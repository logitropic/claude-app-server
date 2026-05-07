pub mod auth;

mod stdio;
mod unix_socket;
mod websocket;

use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use claude_app_server_protocol::JSONRPCErrorError;
use claude_app_server_protocol::JSONRPCMessage;
use claude_app_server_protocol::JSONRPCResponse;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::QueuedOutgoingMessage;

pub use stdio::start_stdio_connection;
pub use unix_socket::start_control_socket_acceptor;
pub use websocket::start_websocket_acceptor;

pub const CHANNEL_CAPACITY: usize = 128;
const OVERLOADED_ERROR_CODE: i64 = -32001;
const APP_SERVER_CONTROL_SOCKET_DIR_NAME: &str = "app-server-control";
const APP_SERVER_CONTROL_SOCKET_FILE_NAME: &str = "app-server-control.sock";

pub fn app_server_control_socket_path(home: &Path) -> std::io::Result<PathBuf> {
    Ok(home
        .join(APP_SERVER_CONTROL_SOCKET_DIR_NAME)
        .join(APP_SERVER_CONTROL_SOCKET_FILE_NAME))
}

pub fn find_app_server_home() -> std::io::Result<PathBuf> {
    if let Some(home) = std::env::var_os("CLAUDE_APP_SERVER_HOME") {
        return Ok(PathBuf::from(home));
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".claude-app-server"));
    }
    std::env::current_dir()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppServerTransport {
    Stdio,
    UnixSocket { socket_path: PathBuf },
    WebSocket { bind_address: SocketAddr },
    Off,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum AppServerTransportParseError {
    #[error(
        "unsupported --listen URL `{0}`; expected `stdio://`, `unix://`, `unix://PATH`, `ws://IP:PORT`, or `off`"
    )]
    UnsupportedListenUrl(String),
    #[error(
        "invalid unix socket --listen URL `{listen_url}`; failed to resolve socket path: {message}"
    )]
    InvalidUnixSocketPath { listen_url: String, message: String },
    #[error("invalid websocket --listen URL `{0}`; expected `ws://IP:PORT`")]
    InvalidWebSocketListenUrl(String),
}

impl AppServerTransport {
    pub const DEFAULT_LISTEN_URL: &'static str = "stdio://";

    pub fn from_listen_url(listen_url: &str) -> Result<Self, AppServerTransportParseError> {
        if listen_url == Self::DEFAULT_LISTEN_URL {
            return Ok(Self::Stdio);
        }
        if listen_url == "off" {
            return Ok(Self::Off);
        }
        if let Some(raw_socket_path) = listen_url.strip_prefix("unix://") {
            let socket_path = if raw_socket_path.is_empty() {
                let home = find_app_server_home().map_err(|err| {
                    AppServerTransportParseError::InvalidUnixSocketPath {
                        listen_url: listen_url.to_string(),
                        message: err.to_string(),
                    }
                })?;
                app_server_control_socket_path(&home).map_err(|err| {
                    AppServerTransportParseError::InvalidUnixSocketPath {
                        listen_url: listen_url.to_string(),
                        message: err.to_string(),
                    }
                })?
            } else {
                let path = PathBuf::from(raw_socket_path);
                if path.is_absolute() {
                    path
                } else {
                    std::env::current_dir()
                        .map_err(|err| AppServerTransportParseError::InvalidUnixSocketPath {
                            listen_url: listen_url.to_string(),
                            message: err.to_string(),
                        })?
                        .join(path)
                }
            };
            return Ok(Self::UnixSocket { socket_path });
        }
        if let Some(socket_addr) = listen_url.strip_prefix("ws://") {
            let bind_address = socket_addr.parse::<SocketAddr>().map_err(|_| {
                AppServerTransportParseError::InvalidWebSocketListenUrl(listen_url.to_string())
            })?;
            return Ok(Self::WebSocket { bind_address });
        }
        Err(AppServerTransportParseError::UnsupportedListenUrl(
            listen_url.to_string(),
        ))
    }
}

impl FromStr for AppServerTransport {
    type Err = AppServerTransportParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_listen_url(s)
    }
}

#[derive(Debug)]
pub enum TransportEvent {
    ConnectionOpened {
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
        writer: mpsc::Sender<QueuedOutgoingMessage>,
        disconnect_sender: Option<CancellationToken>,
    },
    ConnectionClosed {
        connection_id: ConnectionId,
    },
    IncomingMessage {
        connection_id: ConnectionId,
        message: JSONRPCMessage,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionOrigin {
    Stdio,
    WebSocket,
    UnixSocket,
}

static CONNECTION_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_connection_id() -> ConnectionId {
    ConnectionId(CONNECTION_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
}

async fn forward_incoming_message(
    transport_event_tx: &mpsc::Sender<TransportEvent>,
    writer: &mpsc::Sender<QueuedOutgoingMessage>,
    connection_id: ConnectionId,
    payload: &str,
) -> bool {
    match serde_json::from_str::<JSONRPCMessage>(payload) {
        Ok(message) => {
            enqueue_incoming_message(transport_event_tx, writer, connection_id, message).await
        }
        Err(err) => {
            error!("failed to deserialize JSONRPCMessage: {err}");
            true
        }
    }
}

async fn enqueue_incoming_message(
    transport_event_tx: &mpsc::Sender<TransportEvent>,
    writer: &mpsc::Sender<QueuedOutgoingMessage>,
    connection_id: ConnectionId,
    message: JSONRPCMessage,
) -> bool {
    let event = TransportEvent::IncomingMessage {
        connection_id,
        message,
    };
    match transport_event_tx.try_send(event) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Closed(_)) => false,
        Err(mpsc::error::TrySendError::Full(TransportEvent::IncomingMessage {
            message: JSONRPCMessage::Request(request),
            ..
        })) => {
            let response = JSONRPCResponse::error(
                request.id,
                JSONRPCErrorError {
                    code: OVERLOADED_ERROR_CODE,
                    message: "server overloaded".to_string(),
                    data: None,
                },
            );
            let _ = writer
                .send(QueuedOutgoingMessage::new(OutgoingMessage::RawResponse(
                    response,
                )))
                .await;
            true
        }
        Err(mpsc::error::TrySendError::Full(_)) => true,
    }
}

async fn send_connection_opened(
    transport_event_tx: &mpsc::Sender<TransportEvent>,
    connection_id: ConnectionId,
    origin: ConnectionOrigin,
    writer: mpsc::Sender<QueuedOutgoingMessage>,
    disconnect_sender: Option<CancellationToken>,
) -> bool {
    transport_event_tx
        .send(TransportEvent::ConnectionOpened {
            connection_id,
            origin,
            writer,
            disconnect_sender,
        })
        .await
        .is_ok()
}

async fn send_connection_closed(
    transport_event_tx: &mpsc::Sender<TransportEvent>,
    connection_id: ConnectionId,
) {
    let _ = transport_event_tx
        .send(TransportEvent::ConnectionClosed { connection_id })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_listen_urls() {
        assert_eq!(
            AppServerTransport::from_listen_url("stdio://").unwrap(),
            AppServerTransport::Stdio
        );
        assert_eq!(
            AppServerTransport::from_listen_url("off").unwrap(),
            AppServerTransport::Off
        );
        assert!(matches!(
            AppServerTransport::from_listen_url("ws://127.0.0.1:3284").unwrap(),
            AppServerTransport::WebSocket { .. }
        ));
        assert!(matches!(
            AppServerTransport::from_listen_url("unix:///tmp/claude.sock").unwrap(),
            AppServerTransport::UnixSocket { .. }
        ));
    }
}
