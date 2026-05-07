use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use claude_app_server_protocol::INTERNAL_ERROR_CODE;
use claude_app_server_protocol::JSONRPCError;
use claude_app_server_protocol::JSONRPCMessage;
use claude_app_server_protocol::JSONRPCNotification;
use claude_app_server_protocol::JSONRPCRequest;
use claude_app_server_protocol::JSONRPCResponse;
use claude_app_server_protocol::METHOD_NOT_FOUND_ERROR_CODE;
use claude_app_server_protocol::NOT_INITIALIZED_ERROR_CODE;
use claude_app_server_protocol::ServerNotification;
use claude_app_server_transport::ConnectionId;
use claude_app_server_transport::ConnectionOrigin;
use claude_app_server_transport::OutgoingMessage;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::claude_runner::ClaudeRunner;
use crate::outgoing_message::OutboundControlEvent;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessageSender;
use crate::request_processors::apps_processor;
use crate::request_processors::initialize_processor;
use crate::request_processors::model_processor;
use crate::request_processors::thread_processor;
use crate::request_processors::turn_processor;
use crate::thread_state::ThreadStore;

#[derive(Clone)]
pub struct MessageProcessor {
    connections: Arc<Mutex<HashMap<ConnectionId, ConnectionSessionState>>>,
    thread_store: ThreadStore,
    claude_runner: ClaudeRunner,
    outbound_tx: OutgoingMessageSender,
}

#[derive(Debug, Default)]
struct ConnectionSessionState {
    initialized: bool,
    origin: Option<ConnectionOrigin>,
}

impl MessageProcessor {
    pub fn new(claude_path: PathBuf, debug: bool, outbound_tx: OutgoingMessageSender) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            thread_store: ThreadStore::default(),
            claude_runner: ClaudeRunner::new(claude_path, debug, outbound_tx.clone()),
            outbound_tx,
        }
    }

    pub async fn open_connection(&self, connection_id: ConnectionId, origin: ConnectionOrigin) {
        self.connections.lock().await.insert(
            connection_id,
            ConnectionSessionState {
                initialized: false,
                origin: Some(origin),
            },
        );
    }

    pub async fn close_connection(&self, connection_id: ConnectionId) {
        self.connections.lock().await.remove(&connection_id);
    }

    pub async fn process_message(&self, connection_id: ConnectionId, message: JSONRPCMessage) {
        let JSONRPCMessage::Request(request) = message else {
            return;
        };
        let response = self.handle_request(connection_id, request).await;
        self.send_response(connection_id, response).await;
    }

    async fn handle_request(
        &self,
        connection_id: ConnectionId,
        request: JSONRPCRequest,
    ) -> JSONRPCResponse {
        let result = self.dispatch(connection_id, &request).await;
        match result {
            Ok(value) => JSONRPCResponse::success(request.id, value),
            Err(err) => JSONRPCResponse::error(request.id, err.into_error()),
        }
    }

    async fn dispatch(
        &self,
        connection_id: ConnectionId,
        request: &JSONRPCRequest,
    ) -> Result<serde_json::Value, JSONRPCError> {
        let initialized = self
            .connections
            .lock()
            .await
            .get(&connection_id)
            .map(|connection| connection.initialized)
            .unwrap_or(false);
        if !initialized && request.method != "initialize" {
            return Err(JSONRPCError::new(
                NOT_INITIALIZED_ERROR_CODE,
                "Not initialized. Send initialize first.",
            ));
        }

        match request.method.as_str() {
            "initialize" => {
                if let Some(connection) = self.connections.lock().await.get_mut(&connection_id) {
                    connection.initialized = true;
                    let _ = connection.origin;
                }
                self.send_notification(
                    connection_id,
                    "initialized",
                    serde_json::json!({ "server": initialize_processor::SERVER_NAME }),
                )
                .await;
                Ok(initialize_processor::initialize_response())
            }
            "thread/start" => to_value(
                thread_processor::thread_start(&self.thread_store, request.params.clone()).await,
            ),
            "thread/resume" => to_value(
                thread_processor::thread_resume(&self.thread_store, request.params.clone()).await,
            ),
            "thread/fork" => to_value(
                thread_processor::thread_fork(&self.thread_store, request.params.clone()).await,
            ),
            "turn/start" => to_value(
                turn_processor::turn_start(
                    self.thread_store.clone(),
                    self.outbound_tx.clone(),
                    self.claude_runner.clone(),
                    connection_id,
                    request.params.clone(),
                )
                .await,
            ),
            "turn/steer" => {
                turn_processor::turn_steer(&self.thread_store, request.params.clone()).await
            }
            "turn/interrupt" => {
                turn_processor::turn_interrupt(&self.thread_store, request.params.clone()).await
            }
            "approval/respond" => {
                turn_processor::approval_respond(&self.thread_store, request.params.clone()).await
            }
            "model/list" => to_value(Ok(model_processor::model_list_response())),
            "skills/list" => Ok(apps_processor::skills_list_response()),
            "app/list" => to_value(Ok(apps_processor::app_list_response())),
            _ => Err(JSONRPCError::new(
                METHOD_NOT_FOUND_ERROR_CODE,
                format!("Unknown method: {}", request.method),
            )),
        }
    }

    async fn send_response(&self, connection_id: ConnectionId, response: JSONRPCResponse) {
        let _ = self
            .outbound_tx
            .send(OutboundControlEvent::Envelope(
                OutgoingEnvelope::ToConnection {
                    connection_id,
                    message: OutgoingMessage::RawResponse(response),
                },
            ))
            .await;
    }

    async fn send_notification(
        &self,
        connection_id: ConnectionId,
        method: &str,
        params: impl Serialize,
    ) {
        let notification = ServerNotification::new(method, params).into_jsonrpc();
        let _ = self
            .outbound_tx
            .send(OutboundControlEvent::Envelope(
                OutgoingEnvelope::ToConnection {
                    connection_id,
                    message: OutgoingMessage::RawNotification(JSONRPCNotification {
                        jsonrpc: notification.jsonrpc,
                        method: notification.method,
                        params: notification.params,
                    }),
                },
            ))
            .await;
    }
}

fn to_value<T: Serialize>(
    result: Result<T, JSONRPCError>,
) -> Result<serde_json::Value, JSONRPCError> {
    result.and_then(|value| {
        serde_json::to_value(value).map_err(|err| {
            JSONRPCError::new(
                INTERNAL_ERROR_CODE,
                format!("failed to serialize response: {err}"),
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use claude_app_server_protocol::JSONRPCRequest;
    use claude_app_server_transport::QueuedOutgoingMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn requires_initialize() {
        let (tx, mut rx) = mpsc::channel::<OutboundControlEvent>(8);
        let processor = MessageProcessor::new(PathBuf::from("claude"), false, tx.clone());
        let (writer_tx, _writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(8);
        processor
            .open_connection(ConnectionId(1), ConnectionOrigin::Stdio)
            .await;
        let _ = tx
            .send(OutboundControlEvent::Opened {
                connection_id: ConnectionId(1),
                writer: writer_tx,
                disconnect_sender: None,
            })
            .await;
        processor
            .process_message(
                ConnectionId(1),
                JSONRPCMessage::Request(JSONRPCRequest {
                    jsonrpc: "2.0".to_string(),
                    id: serde_json::json!(1),
                    method: "model/list".to_string(),
                    params: None,
                }),
            )
            .await;
        let event = rx.recv().await.unwrap();
        assert!(matches!(event, OutboundControlEvent::Opened { .. }));
    }
}
