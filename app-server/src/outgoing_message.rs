use std::collections::HashMap;

use claude_app_server_transport::ConnectionId;
use claude_app_server_transport::OutgoingMessage;
use claude_app_server_transport::QueuedOutgoingMessage;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(Debug)]
pub enum OutgoingEnvelope {
    ToConnection {
        connection_id: ConnectionId,
        message: OutgoingMessage,
    },
    #[allow(dead_code)]
    Broadcast { message: OutgoingMessage },
}

#[derive(Debug)]
pub enum OutboundControlEvent {
    Opened {
        connection_id: ConnectionId,
        writer: mpsc::Sender<QueuedOutgoingMessage>,
        disconnect_sender: Option<CancellationToken>,
    },
    Closed {
        connection_id: ConnectionId,
    },
    Envelope(OutgoingEnvelope),
}

pub struct OutboundConnectionState {
    writer: mpsc::Sender<QueuedOutgoingMessage>,
    disconnect_sender: Option<CancellationToken>,
}

impl OutboundConnectionState {
    fn new(
        writer: mpsc::Sender<QueuedOutgoingMessage>,
        disconnect_sender: Option<CancellationToken>,
    ) -> Self {
        Self {
            writer,
            disconnect_sender,
        }
    }

    fn request_disconnect(&self) {
        if let Some(token) = &self.disconnect_sender {
            token.cancel();
        }
    }
}

pub type OutgoingMessageSender = mpsc::Sender<OutboundControlEvent>;

pub async fn route_outgoing_envelope(
    connections: &mut HashMap<ConnectionId, OutboundConnectionState>,
    event: OutboundControlEvent,
) {
    match event {
        OutboundControlEvent::Opened {
            connection_id,
            writer,
            disconnect_sender,
        } => {
            connections.insert(
                connection_id,
                OutboundConnectionState::new(writer, disconnect_sender),
            );
        }
        OutboundControlEvent::Closed { connection_id } => {
            if let Some(connection) = connections.remove(&connection_id) {
                connection.request_disconnect();
            }
        }
        OutboundControlEvent::Envelope(OutgoingEnvelope::ToConnection {
            connection_id,
            message,
        }) => {
            send_message(connections, connection_id, message).await;
        }
        OutboundControlEvent::Envelope(OutgoingEnvelope::Broadcast { message }) => {
            let targets: Vec<ConnectionId> = connections.keys().copied().collect();
            for target in targets {
                send_message(connections, target, message.clone()).await;
            }
        }
    }
}

async fn send_message(
    connections: &mut HashMap<ConnectionId, OutboundConnectionState>,
    connection_id: ConnectionId,
    message: OutgoingMessage,
) {
    let Some(connection) = connections.get(&connection_id) else {
        warn!("dropping message for disconnected connection: {connection_id}");
        return;
    };
    if connection
        .writer
        .send(QueuedOutgoingMessage::new(message))
        .await
        .is_err()
        && let Some(connection) = connections.remove(&connection_id)
    {
        connection.request_disconnect();
    }
}
