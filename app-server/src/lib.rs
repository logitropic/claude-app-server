use std::collections::HashMap;

use claude_app_server_transport::TransportEvent;
use claude_app_server_transport::auth::policy_from_settings;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::message_processor::MessageProcessor;
use crate::outgoing_message::OutboundConnectionState;
use crate::outgoing_message::OutboundControlEvent;
use crate::outgoing_message::route_outgoing_envelope;

mod claude_runner;
mod message_processor;
mod outgoing_message;
mod request_processors;
mod thread_state;
mod transport;

pub use claude_app_server_transport::AppServerTransport;
pub use claude_app_server_transport::auth::AppServerWebsocketAuthArgs;
pub use claude_app_server_transport::auth::AppServerWebsocketAuthSettings;

pub use thread_state::ThreadStore;

#[derive(Debug, Clone, Default)]
pub struct AppServerRuntimeOptions {
    pub claude_path: Option<std::path::PathBuf>,
    pub debug: bool,
}

pub async fn run_main_with_transport_options(
    transport: AppServerTransport,
    auth: AppServerWebsocketAuthSettings,
    runtime_options: AppServerRuntimeOptions,
) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "claude_app_server=info,claude_app_server_transport=info".into()
            }),
        )
        .with_writer(std::io::stderr)
        .init();

    if matches!(transport, AppServerTransport::Off) {
        info!("app-server transport is off");
        return Ok(());
    }

    let claude_path = runtime_options
        .claude_path
        .unwrap_or_else(|| std::path::PathBuf::from("claude"));
    let (transport_event_tx, mut transport_event_rx) =
        mpsc::channel::<TransportEvent>(claude_app_server_transport::CHANNEL_CAPACITY);
    let (outbound_tx, mut outbound_rx) =
        mpsc::channel::<OutboundControlEvent>(claude_app_server_transport::CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();

    start_transport(
        transport,
        transport_event_tx.clone(),
        auth,
        shutdown_token.clone(),
    )
    .await?;

    let processor = MessageProcessor::new(claude_path, runtime_options.debug, outbound_tx.clone());
    let processor_task = tokio::spawn(async move {
        while let Some(event) = transport_event_rx.recv().await {
            match event {
                TransportEvent::ConnectionOpened {
                    connection_id,
                    origin,
                    writer,
                    disconnect_sender,
                } => {
                    processor.open_connection(connection_id, origin).await;
                    let _ = outbound_tx
                        .send(OutboundControlEvent::Opened {
                            connection_id,
                            writer,
                            disconnect_sender,
                        })
                        .await;
                }
                TransportEvent::ConnectionClosed { connection_id } => {
                    processor.close_connection(connection_id).await;
                    let _ = outbound_tx
                        .send(OutboundControlEvent::Closed { connection_id })
                        .await;
                }
                TransportEvent::IncomingMessage {
                    connection_id,
                    message,
                } => {
                    processor.process_message(connection_id, message).await;
                }
            }
        }
    });

    let outbound_task = tokio::spawn(async move {
        let mut connections: HashMap<
            claude_app_server_transport::ConnectionId,
            OutboundConnectionState,
        > = HashMap::new();
        while let Some(event) = outbound_rx.recv().await {
            route_outgoing_envelope(&mut connections, event).await;
        }
    });

    tokio::select! {
        signal = shutdown_signal() => signal?,
        _ = shutdown_token.cancelled() => {},
    }
    shutdown_token.cancel();
    processor_task.abort();
    outbound_task.abort();
    Ok(())
}

async fn start_transport(
    transport: AppServerTransport,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    auth: AppServerWebsocketAuthSettings,
    shutdown_token: CancellationToken,
) -> anyhow::Result<()> {
    match transport {
        AppServerTransport::Stdio => {
            let shutdown_token = shutdown_token.clone();
            tokio::spawn(async move {
                claude_app_server_transport::start_stdio_connection(transport_event_tx).await;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                shutdown_token.cancel();
            });
        }
        AppServerTransport::UnixSocket { socket_path } => {
            tokio::spawn(async move {
                if let Err(err) = claude_app_server_transport::start_control_socket_acceptor(
                    socket_path,
                    transport_event_tx,
                    shutdown_token,
                )
                .await
                {
                    tracing::error!("unix socket transport failed: {err}");
                }
            });
        }
        AppServerTransport::WebSocket { bind_address } => {
            let auth_policy = policy_from_settings(&auth)?;
            tokio::spawn(async move {
                if let Err(err) = claude_app_server_transport::start_websocket_acceptor(
                    bind_address,
                    transport_event_tx,
                    auth_policy,
                    shutdown_token,
                )
                .await
                {
                    tracing::error!("websocket transport failed: {err}");
                }
            });
        }
        AppServerTransport::Off => {}
    }
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::SignalKind;
        use tokio::signal::unix::signal;
        let mut term = signal(SignalKind::terminate())?;
        tokio::select! {
            ctrl_c_result = tokio::signal::ctrl_c() => ctrl_c_result,
            _ = term.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
