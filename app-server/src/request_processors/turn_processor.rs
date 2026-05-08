use claude_app_server_protocol::INVALID_PARAMS_ERROR_CODE;
use claude_app_server_protocol::JSONRPCError;
use claude_app_server_protocol::NO_ACTIVE_TURN_ERROR_CODE;
use claude_app_server_protocol::PermissionMode;
use claude_app_server_protocol::THREAD_NOT_FOUND_ERROR_CODE;
use claude_app_server_protocol::TURN_BUSY_ERROR_CODE;
use claude_app_server_protocol::TurnStartResponse;
use claude_app_server_protocol::TurnStartTurn;
use claude_app_server_protocol::TurnStartedNotification;
use serde_json::Value;

use crate::claude_runner::ClaudeRunner;
use crate::claude_runner::RunTurnArgs;
use crate::outgoing_message::OutboundControlEvent;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessageSender;
use crate::request_processors::thread_processor::get_thread_id;
use crate::thread_state::ThreadStore;
use crate::thread_state::TurnState;
use claude_app_server_protocol::ServerNotification;
use claude_app_server_transport::ConnectionId;
use claude_app_server_transport::OutgoingMessage;

pub async fn turn_start(
    store: ThreadStore,
    outbound_tx: OutgoingMessageSender,
    claude_runner: ClaudeRunner,
    connection_id: ConnectionId,
    params: Option<Value>,
) -> Result<TurnStartResponse, JSONRPCError> {
    let params = params.unwrap_or_default();
    let thread_id = get_thread_id(Some(&params))?;
    let content = get_user_content(&params)?;
    let model = params
        .get("model")
        .and_then(Value::as_str)
        .map(ToString::to_string);

    let (turn_id, turn_snapshot) = store
        .with_thread_mut(&thread_id, |thread| {
            if thread.active_turn_id.is_some() {
                return Err(JSONRPCError::new(
                    TURN_BUSY_ERROR_CODE,
                    "Thread already has an active turn. Interrupt it first.",
                ));
            }
            let turn = TurnState::new(thread.id.clone(), content);
            let turn_id = turn.id.clone();
            let turn_snapshot = turn.snapshot();
            thread.active_turn_id = Some(turn_id.clone());
            thread.turns.push(turn);
            Ok((turn_id, turn_snapshot))
        })
        .await
        .ok_or_else(|| {
            JSONRPCError::new(
                THREAD_NOT_FOUND_ERROR_CODE,
                format!("Thread not found: {thread_id}"),
            )
        })??;

    let notification = TurnStartedNotification {
        thread_id: thread_id.clone(),
        turn: turn_snapshot,
    };
    let _ = outbound_tx
        .send(OutboundControlEvent::Envelope(
            OutgoingEnvelope::ToConnection {
                connection_id,
                message: OutgoingMessage::AppServerNotification(ServerNotification::new(
                    "turn/started",
                    notification,
                )),
            },
        ))
        .await;

    let runner_turn_id = turn_id.clone();
    tokio::spawn(async move {
        claude_runner
            .run_turn(
                store,
                RunTurnArgs {
                    thread_id,
                    turn_id: runner_turn_id,
                    model,
                    connection_id,
                },
            )
            .await;
    });

    Ok(TurnStartResponse {
        turn: TurnStartTurn { id: turn_id },
    })
}

pub async fn turn_steer(store: &ThreadStore, params: Option<Value>) -> Result<Value, JSONRPCError> {
    let params = params.unwrap_or_default();
    let thread_id = get_thread_id(Some(&params))?;
    let content = params
        .get("content")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| input_text(&params))
        .ok_or_else(|| {
            JSONRPCError::new(INVALID_PARAMS_ERROR_CODE, "content or input is required")
        })?;
    let turn_id = store
        .with_thread_mut(&thread_id, |thread| {
            let Some(turn) = thread.active_turn_mut() else {
                return Err(JSONRPCError::new(
                    NO_ACTIVE_TURN_ERROR_CODE,
                    "No active turn to steer.",
                ));
            };
            turn.steer_queue.push(content);
            Ok(turn.id.clone())
        })
        .await
        .ok_or_else(|| {
            JSONRPCError::new(
                THREAD_NOT_FOUND_ERROR_CODE,
                format!("Thread not found: {thread_id}"),
            )
        })??;
    Ok(serde_json::json!({
        "turn_id": turn_id,
        "note": "queued: will be prepended to the next user message",
    }))
}

pub async fn turn_interrupt(
    store: &ThreadStore,
    params: Option<Value>,
) -> Result<Value, JSONRPCError> {
    let thread_id = get_thread_id(params.as_ref())?;
    let turn_id = store
        .with_thread_mut(&thread_id, |thread| {
            let Some(turn) = thread.active_turn_mut() else {
                return Err(JSONRPCError::new(
                    NO_ACTIVE_TURN_ERROR_CODE,
                    "No active turn to interrupt.",
                ));
            };
            turn.cancel.cancel();
            Ok(turn.id.clone())
        })
        .await
        .ok_or_else(|| {
            JSONRPCError::new(
                THREAD_NOT_FOUND_ERROR_CODE,
                format!("Thread not found: {thread_id}"),
            )
        })??;
    Ok(serde_json::json!({
        "turn_id": turn_id,
        "status": "interrupted",
    }))
}

pub async fn approval_respond(
    store: &ThreadStore,
    params: Option<Value>,
) -> Result<Value, JSONRPCError> {
    let params = params.unwrap_or_default();
    let thread_id = get_thread_id(Some(&params))?;
    let approved = params
        .get("approved")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let permission_mode = params
        .get("permissionMode")
        .or_else(|| params.get("permission_mode"))
        .cloned()
        .map(serde_json::from_value::<PermissionMode>)
        .transpose()
        .map_err(|err| JSONRPCError::new(INVALID_PARAMS_ERROR_CODE, err.to_string()))?
        .unwrap_or(PermissionMode::AcceptEdits);
    let active_permission_mode = store
        .with_thread_mut(&thread_id, |thread| {
            if approved {
                thread.permission_mode = permission_mode;
            }
            thread.permission_mode
        })
        .await
        .ok_or_else(|| {
            JSONRPCError::new(
                THREAD_NOT_FOUND_ERROR_CODE,
                format!("Thread not found: {thread_id}"),
            )
        })?;
    Ok(serde_json::json!({
        "thread_id": thread_id,
        "approved": approved,
        "permission_mode": active_permission_mode,
        "note": if approved {
            format!("Permission mode updated to \"{}\". Retry your turn/start.", active_permission_mode.as_claude_arg())
        } else {
            "Approval denied. Permission mode unchanged.".to_string()
        },
    }))
}

fn get_user_content(params: &Value) -> Result<String, JSONRPCError> {
    params
        .get("content")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| input_text(params))
        .filter(|content| !content.is_empty())
        .ok_or_else(|| JSONRPCError::new(INVALID_PARAMS_ERROR_CODE, "content or input is required"))
}

fn input_text(params: &Value) -> Option<String> {
    let input = params.get("input")?.as_array()?;
    let joined = input
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if joined.is_empty() {
        None
    } else {
        Some(joined)
    }
}
