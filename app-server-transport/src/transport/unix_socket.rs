use std::path::PathBuf;

use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;

use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::QueuedOutgoingMessage;
use crate::transport::CHANNEL_CAPACITY;
use crate::transport::ConnectionOrigin;
use crate::transport::TransportEvent;
use crate::transport::forward_incoming_message;
use crate::transport::next_connection_id;
use crate::transport::send_connection_closed;
use crate::transport::send_connection_opened;

pub async fn start_control_socket_acceptor(
    socket_path: PathBuf,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    shutdown_token: CancellationToken,
) -> anyhow::Result<()> {
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await?;
    }
    let listener = UnixListener::bind(&socket_path)?;
    info!(
        "app-server unix socket listening on {}",
        socket_path.display()
    );
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let tx = transport_event_tx.clone();
                        tokio::spawn(async move {
                            handle_stream(stream, tx).await;
                        });
                    }
                    Err(err) => error!("failed to accept unix socket connection: {err}"),
                }
            }
        }
    }
    Ok(())
}

async fn handle_stream(
    stream: tokio::net::UnixStream,
    transport_event_tx: mpsc::Sender<TransportEvent>,
) {
    let connection_id = next_connection_id();
    let (writer_tx, mut writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(CHANNEL_CAPACITY);
    let disconnect_sender = CancellationToken::new();
    if !send_connection_opened(
        &transport_event_tx,
        connection_id,
        ConnectionOrigin::UnixSocket,
        writer_tx.clone(),
        Some(disconnect_sender.clone()),
    )
    .await
    {
        return;
    }

    let (read_half, mut write_half) = stream.into_split();
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
                        Ok(line) => {
                            if write_half.write_all(line.as_bytes()).await.is_err()
                                || write_half.write_all(b"\n").await.is_err()
                                || write_half.flush().await.is_err()
                            {
                                break;
                            }
                        }
                        Err(err) => error!("failed to serialize outgoing unix socket message: {err}"),
                    }
                    if let Some(tx) = queued.write_complete_tx {
                        let _ = tx.send(());
                    }
                }
            }
        }
    });

    let mut lines = BufReader::new(read_half).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if !forward_incoming_message(&transport_event_tx, &writer_tx, connection_id, &line)
                    .await
                {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                error!("failed to read unix socket connection: {err}");
                break;
            }
        }
    }
    writer_task.abort();
    send_connection_closed(&transport_event_tx, connection_id).await;
}
