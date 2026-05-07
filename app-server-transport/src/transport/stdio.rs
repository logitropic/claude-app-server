use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tracing::error;

use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::QueuedOutgoingMessage;
use crate::transport::CHANNEL_CAPACITY;
use crate::transport::ConnectionOrigin;
use crate::transport::TransportEvent;
use crate::transport::forward_incoming_message;
use crate::transport::next_connection_id;
use crate::transport::send_connection_closed;
use crate::transport::send_connection_opened;

pub async fn start_stdio_connection(transport_event_tx: mpsc::Sender<TransportEvent>) {
    let connection_id = next_connection_id();
    let (writer_tx, mut writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(CHANNEL_CAPACITY);
    if !send_connection_opened(
        &transport_event_tx,
        connection_id,
        ConnectionOrigin::Stdio,
        writer_tx.clone(),
        None,
    )
    .await
    {
        return;
    }

    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(queued) = writer_rx.recv().await {
            let serialized = match queued.message {
                OutgoingMessage::AppServerNotification(notification) => {
                    serde_json::to_string(&notification.into_jsonrpc())
                }
                OutgoingMessage::RawNotification(notification) => {
                    serde_json::to_string(&notification)
                }
                OutgoingMessage::RawResponse(response) => serde_json::to_string(&response),
                other => serde_json::to_string(&other),
            };
            match serialized {
                Ok(line) => {
                    if stdout.write_all(line.as_bytes()).await.is_err()
                        || stdout.write_all(b"\n").await.is_err()
                        || stdout.flush().await.is_err()
                    {
                        break;
                    }
                }
                Err(err) => error!("failed to serialize outgoing stdio message: {err}"),
            }
            if let Some(tx) = queued.write_complete_tx {
                let _ = tx.send(());
            }
        }
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
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
                error!("failed to read stdin: {err}");
                break;
            }
        }
    }
    send_connection_closed(&transport_event_tx, connection_id).await;
}
