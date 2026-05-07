use std::net::SocketAddr;

use axum::Router;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use futures::SinkExt;
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::auth::WebsocketAuthPolicy;
use super::auth::authorize_upgrade;
use super::auth::should_warn_about_unauthenticated_non_loopback_listener;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::QueuedOutgoingMessage;
use crate::transport::CHANNEL_CAPACITY;
use crate::transport::ConnectionOrigin;
use crate::transport::TransportEvent;
use crate::transport::forward_incoming_message;
use crate::transport::next_connection_id;
use crate::transport::send_connection_closed;
use crate::transport::send_connection_opened;

#[derive(Clone)]
struct WebSocketState {
    transport_event_tx: mpsc::Sender<TransportEvent>,
    auth_policy: WebsocketAuthPolicy,
}

pub async fn start_websocket_acceptor(
    bind_address: SocketAddr,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    auth_policy: WebsocketAuthPolicy,
    shutdown_token: CancellationToken,
) -> anyhow::Result<()> {
    if should_warn_about_unauthenticated_non_loopback_listener(bind_address, &auth_policy) {
        warn!("websocket listener {bind_address} is not loopback and has no auth configured");
    }
    let listener = TcpListener::bind(bind_address).await?;
    let local_addr = listener.local_addr()?;
    info!("app-server websocket listening on ws://{local_addr}");
    let state = WebSocketState {
        transport_event_tx,
        auth_policy,
    };
    let app = Router::new()
        .route("/", get(websocket_handler))
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_token.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn websocket_handler(
    State(state): State<WebSocketState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if let Err(err) = authorize_upgrade(&headers, &state.auth_policy) {
        return (err.status_code(), err.message()).into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state))
        .into_response()
}

async fn handle_socket(socket: WebSocket, state: WebSocketState) {
    let connection_id = next_connection_id();
    let (writer_tx, mut writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(CHANNEL_CAPACITY);
    let disconnect_sender = CancellationToken::new();
    if !send_connection_opened(
        &state.transport_event_tx,
        connection_id,
        ConnectionOrigin::WebSocket,
        writer_tx.clone(),
        Some(disconnect_sender.clone()),
    )
    .await
    {
        return;
    }

    let (mut sender, mut receiver) = socket.split();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = disconnect_sender.cancelled() => break,
                queued = writer_rx.recv() => {
                    let Some(queued) = queued else { break };
                    let serialized = match queued.message {
                        OutgoingMessage::AppServerNotification(notification) => serde_json::to_string(&notification.into_jsonrpc()),
                        OutgoingMessage::RawNotification(notification) => serde_json::to_string(&notification),
                        OutgoingMessage::RawResponse(response) => serde_json::to_string(&response),
                        other => serde_json::to_string(&other),
                    };
                    match serialized {
                        Ok(payload) => {
                            if sender.send(Message::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => error!("failed to serialize outgoing websocket message: {err}"),
                    }
                    if let Some(tx) = queued.write_complete_tx {
                        let _ = tx.send(());
                    }
                }
            }
        }
    });

    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Text(text)) => {
                if !forward_incoming_message(
                    &state.transport_event_tx,
                    &writer_tx,
                    connection_id,
                    &text,
                )
                .await
                {
                    break;
                }
            }
            Ok(Message::Binary(bytes)) => {
                if let Ok(text) = std::str::from_utf8(&bytes)
                    && !forward_incoming_message(
                        &state.transport_event_tx,
                        &writer_tx,
                        connection_id,
                        text,
                    )
                    .await
                {
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(err) => {
                error!("websocket receive error: {err}");
                break;
            }
        }
    }
    writer_task.abort();
    send_connection_closed(&state.transport_event_tx, connection_id).await;
}

#[allow(dead_code)]
fn unauthorized_response() -> (StatusCode, &'static str) {
    (StatusCode::UNAUTHORIZED, "unauthorized")
}
