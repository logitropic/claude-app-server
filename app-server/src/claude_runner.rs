use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use claude_app_server_protocol::Item;
use claude_app_server_protocol::ItemCreatedNotification;
use claude_app_server_protocol::ItemProgressNotification;
use claude_app_server_protocol::ServerNotification;
use claude_app_server_protocol::TurnCompletedNotification;
use claude_app_server_protocol::TurnFailedNotification;
use claude_app_server_protocol::TurnPermissionDeniedNotification;
use claude_app_server_protocol::TurnStatus;
use claude_app_server_protocol::UsageUpdateNotification;
use claude_app_server_transport::OutgoingMessage;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::debug;

use crate::outgoing_message::OutboundControlEvent;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessageSender;
use crate::thread_state::ThreadStore;
use crate::thread_state::now_millis;

#[derive(Clone)]
pub struct ClaudeRunner {
    claude_path: PathBuf,
    debug: bool,
    outbound_tx: OutgoingMessageSender,
}

pub struct RunTurnArgs {
    pub thread_id: String,
    pub turn_id: String,
    pub model: Option<String>,
    pub connection_id: claude_app_server_transport::ConnectionId,
}

impl ClaudeRunner {
    pub fn new(claude_path: PathBuf, debug: bool, outbound_tx: OutgoingMessageSender) -> Self {
        Self {
            claude_path,
            debug,
            outbound_tx,
        }
    }

    pub async fn run_turn(self, store: ThreadStore, args: RunTurnArgs) {
        let result = self.run_turn_inner(store.clone(), &args).await;
        if let Err(err) = result {
            let _ = store
                .with_thread_mut(&args.thread_id, |thread| {
                    if let Some(turn) = thread.active_turn_mut() {
                        turn.status = TurnStatus::Error;
                        turn.error = Some(err.to_string());
                        turn.completed_at = Some(now_millis());
                    }
                    thread.active_turn_id = None;
                })
                .await;
            self.notify(
                args.connection_id,
                "turn/failed",
                TurnFailedNotification {
                    turn_id: args.turn_id,
                    error: err.to_string(),
                },
            )
            .await;
        }
    }

    async fn run_turn_inner(&self, store: ThreadStore, args: &RunTurnArgs) -> anyhow::Result<()> {
        let launch = store
            .with_thread_mut(&args.thread_id, |thread| {
                let cli_session_id = thread.cli_session_id.clone();
                let fork_from = thread.fork_from.clone();
                let permission_mode = thread.permission_mode;
                let cwd = thread.cwd.clone();
                let turn = thread
                    .turns
                    .iter_mut()
                    .find(|turn| turn.id == args.turn_id)
                    .expect("active turn should exist");
                (
                    cwd,
                    permission_mode,
                    cli_session_id,
                    fork_from,
                    turn.user_content.clone(),
                    turn.cancel.clone(),
                )
            })
            .await
            .ok_or_else(|| anyhow::anyhow!("thread not found: {}", args.thread_id))?;

        let (cwd, permission_mode, cli_session_id, fork_from, user_content, cancel) = launch;
        let mut claude_args = vec![
            "--print".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--verbose".to_string(),
            "--include-partial-messages".to_string(),
            "--permission-mode".to_string(),
            permission_mode.as_claude_arg().to_string(),
        ];
        if let Some(model) = &args.model {
            claude_args.push("--model".to_string());
            claude_args.push(model.clone());
        }
        if let Some(fork_from) = fork_from.filter(|_| cli_session_id.is_none()) {
            claude_args.push("--resume".to_string());
            claude_args.push(fork_from.cli_session_id);
            claude_args.push("--fork-session".to_string());
        } else if let Some(cli_session_id) = cli_session_id {
            claude_args.push("--resume".to_string());
            claude_args.push(cli_session_id);
        } else {
            claude_args.push("--session-id".to_string());
            claude_args.push(args.thread_id.clone());
        }

        if self.debug {
            debug!(
                "spawn: {} {}",
                self.claude_path.display(),
                claude_args.join(" ")
            );
            debug!("cwd: {}", cwd.display());
        }

        let mut child = Command::new(&self.claude_path)
            .args(&claude_args)
            .current_dir(&cwd)
            .env_remove("CLAUDECODE")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("claude stdout unavailable"))?;
        let stderr = child.stderr.take();
        let child: Arc<Mutex<tokio::process::Child>> = Arc::new(Mutex::new(child));
        let child_for_cancel: Arc<Mutex<tokio::process::Child>> = Arc::clone(&child);
        let _ = store
            .with_thread_mut(&args.thread_id, |thread| {
                if let Some(turn) = thread.active_turn_mut() {
                    turn.process = Some(Arc::clone(&child));
                }
            })
            .await;

        if let Some(mut stdin) = stdin {
            stdin.write_all(user_content.as_bytes()).await?;
        }

        if let Some(stderr) = stderr {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    debug!("claude stderr: {line}");
                }
            });
        }

        let cancel_task = tokio::spawn(async move {
            cancel.cancelled().await;
            let _ = child_for_cancel.lock().await.kill().await;
        });

        let mut partial_text: HashMap<String, String> = HashMap::new();
        let mut partial_thinking: HashMap<String, String> = HashMap::new();
        let mut lines = BufReader::new(stdout).lines();
        while let Some(line) = lines.next_line().await? {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if self.debug {
                debug!("claude stdout: {trimmed}");
            }
            let Ok(event) = serde_json::from_str::<ClaudeStreamEvent>(trimmed) else {
                continue;
            };
            self.process_claude_event(
                &store,
                &args.thread_id,
                &args.turn_id,
                args.connection_id,
                event,
                &mut partial_text,
                &mut partial_thinking,
            )
            .await?;
        }
        let exit_status = child.lock().await.wait().await?;
        cancel_task.abort();
        let interrupted = store
            .with_thread_mut(&args.thread_id, |thread| {
                let Some(turn) = thread.active_turn_mut() else {
                    return false;
                };
                turn.cancel.is_cancelled()
            })
            .await
            .unwrap_or(false);
        if !interrupted && !exit_status.success() {
            return Err(anyhow::anyhow!("claude exited with status {exit_status}"));
        }

        let completed = store
            .with_thread_mut(&args.thread_id, |thread| {
                let notification_thread_id = thread.id.clone();
                let turn = thread.active_turn_mut()?;
                turn.status = if interrupted {
                    TurnStatus::Interrupted
                } else {
                    TurnStatus::Completed
                };
                turn.completed_at = Some(now_millis());
                let notification = TurnCompletedNotification {
                    turn_id: turn.id.clone(),
                    thread_id: notification_thread_id,
                    status: turn.status.clone(),
                    items_count: turn.items.len(),
                    completed_at: turn.completed_at.unwrap_or_default(),
                    usage: turn.usage.clone(),
                    cost_usd: turn.cost_usd,
                };
                thread.active_turn_id = None;
                Some(notification)
            })
            .await
            .flatten();
        if let Some(notification) = completed {
            self.notify(args.connection_id, "turn/completed", notification)
                .await;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_claude_event(
        &self,
        store: &ThreadStore,
        thread_id: &str,
        turn_id: &str,
        connection_id: claude_app_server_transport::ConnectionId,
        event: ClaudeStreamEvent,
        partial_text: &mut HashMap<String, String>,
        partial_thinking: &mut HashMap<String, String>,
    ) -> anyhow::Result<()> {
        match event {
            ClaudeStreamEvent::System {
                subtype,
                session_id,
                ..
            } => {
                if subtype == "init"
                    && let Some(session_id) = session_id
                {
                    let _ = store
                        .with_thread_mut(thread_id, |thread| {
                            thread.cli_session_id = Some(session_id);
                        })
                        .await;
                }
            }
            ClaudeStreamEvent::Assistant {
                message,
                is_partial,
                ..
            } => {
                let msg_id = message.id.unwrap_or_else(|| "unknown".to_string());
                let partial = is_partial.unwrap_or(false);
                for block in message.content.unwrap_or_default() {
                    match block {
                        ClaudeContentBlock::Text { text } => {
                            let prev = partial_text.get(&msg_id).cloned().unwrap_or_default();
                            let delta = text.strip_prefix(&prev).unwrap_or(&text).to_string();
                            if !delta.is_empty() {
                                self.notify(
                                    connection_id,
                                    "item/progress",
                                    ItemProgressNotification {
                                        turn_id: turn_id.to_string(),
                                        delta: json!({ "type": "text", "text": delta }),
                                    },
                                )
                                .await;
                                partial_text.insert(msg_id.clone(), text.clone());
                            }
                            if !partial {
                                let created = store
                                    .with_thread_mut(thread_id, |thread| {
                                        thread
                                            .active_turn_mut()
                                            .map(|turn| turn.push_item(Item::Text { text }))
                                    })
                                    .await
                                    .flatten();
                                if let Some(item) = created {
                                    self.notify(
                                        connection_id,
                                        "item/created",
                                        ItemCreatedNotification {
                                            turn_id: turn_id.to_string(),
                                            item,
                                        },
                                    )
                                    .await;
                                }
                                partial_text.remove(&msg_id);
                            }
                        }
                        ClaudeContentBlock::Thinking { thinking } if !partial => {
                            let prev = partial_thinking.get(&msg_id).cloned().unwrap_or_default();
                            let delta = thinking
                                .strip_prefix(&prev)
                                .unwrap_or(&thinking)
                                .to_string();
                            if !delta.is_empty() {
                                self.notify(
                                    connection_id,
                                    "item/progress",
                                    ItemProgressNotification {
                                        turn_id: turn_id.to_string(),
                                        delta: json!({ "type": "thinking", "thinking": delta }),
                                    },
                                )
                                .await;
                            }
                            let created = store
                                .with_thread_mut(thread_id, |thread| {
                                    thread
                                        .active_turn_mut()
                                        .map(|turn| turn.push_item(Item::Thinking { thinking }))
                                })
                                .await
                                .flatten();
                            if let Some(item) = created {
                                self.notify(
                                    connection_id,
                                    "item/created",
                                    ItemCreatedNotification {
                                        turn_id: turn_id.to_string(),
                                        item,
                                    },
                                )
                                .await;
                            }
                            partial_thinking.remove(&msg_id);
                        }
                        ClaudeContentBlock::ToolUse { id, name, input } if !partial => {
                            let created = store
                                .with_thread_mut(thread_id, |thread| {
                                    thread.active_turn_mut().map(|turn| {
                                        turn.push_item(Item::ToolCall {
                                            tool_use_id: id,
                                            name,
                                            input: input.unwrap_or(Value::Null),
                                        })
                                    })
                                })
                                .await
                                .flatten();
                            if let Some(item) = created {
                                self.notify(
                                    connection_id,
                                    "item/created",
                                    ItemCreatedNotification {
                                        turn_id: turn_id.to_string(),
                                        item,
                                    },
                                )
                                .await;
                            }
                        }
                        _ => {}
                    }
                }
            }
            ClaudeStreamEvent::User { message, .. } => {
                for block in message.content.unwrap_or_default() {
                    if let ClaudeContentBlock::ToolResult {
                        tool_use_id,
                        content,
                        is_error,
                    } = block
                    {
                        let content = match content {
                            Some(Value::Array(parts)) => parts
                                .into_iter()
                                .filter_map(|part| {
                                    part.get("text")
                                        .and_then(Value::as_str)
                                        .map(ToString::to_string)
                                })
                                .collect::<Vec<_>>()
                                .join(""),
                            Some(value) => value.to_string(),
                            None => String::new(),
                        };
                        let created = store
                            .with_thread_mut(thread_id, |thread| {
                                thread.active_turn_mut().map(|turn| {
                                    turn.push_item(Item::ToolResult {
                                        tool_use_id,
                                        content,
                                        is_error: is_error.unwrap_or(false),
                                    })
                                })
                            })
                            .await
                            .flatten();
                        if let Some(item) = created {
                            self.notify(
                                connection_id,
                                "item/created",
                                ItemCreatedNotification {
                                    turn_id: turn_id.to_string(),
                                    item,
                                },
                            )
                            .await;
                        }
                    }
                }
            }
            ClaudeStreamEvent::StreamEvent { event, .. } => {
                if event.event_type == "message_delta"
                    && let Some(mut usage) = event.usage
                {
                    add_total_tokens(&mut usage);
                    let _ = store
                        .with_thread_mut(thread_id, |thread| {
                            if let Some(turn) = thread.active_turn_mut() {
                                turn.usage = Some(usage.clone());
                            }
                        })
                        .await;
                    self.notify(
                        connection_id,
                        "usage/update",
                        UsageUpdateNotification {
                            turn_id: turn_id.to_string(),
                            thread_id: thread_id.to_string(),
                            usage,
                        },
                    )
                    .await;
                }
            }
            ClaudeStreamEvent::Result {
                subtype,
                session_id,
                error,
                usage,
                cost_usd,
                total_cost_usd,
                permission_denials,
                ..
            } => {
                let denials_for_notification = permission_denials.clone();
                let _ = store
                    .with_thread_mut(thread_id, |thread| {
                        if let Some(session_id) = session_id {
                            thread.cli_session_id = Some(session_id);
                        }
                        if let Some(turn) = thread.active_turn_mut() {
                            if subtype == "error" {
                                turn.status = TurnStatus::Error;
                                turn.error = error;
                            }
                            if let Some(mut usage) = usage {
                                add_total_tokens(&mut usage);
                                turn.usage = Some(usage);
                            }
                            turn.cost_usd = total_cost_usd.or(cost_usd);
                        }
                    })
                    .await;
                if let Some(denials) = denials_for_notification
                    && denials.as_array().is_some_and(|items| !items.is_empty())
                {
                    self.notify(
                        connection_id,
                        "turn/permission_denied",
                        TurnPermissionDeniedNotification {
                            turn_id: turn_id.to_string(),
                            thread_id: thread_id.to_string(),
                            denials,
                        },
                    )
                    .await;
                }
            }
            ClaudeStreamEvent::RateLimitEvent { .. } => {}
        }
        Ok(())
    }

    async fn notify(
        &self,
        connection_id: claude_app_server_transport::ConnectionId,
        method: &str,
        params: impl serde::Serialize,
    ) {
        let _ = self
            .outbound_tx
            .send(OutboundControlEvent::Envelope(
                OutgoingEnvelope::ToConnection {
                    connection_id,
                    message: OutgoingMessage::AppServerNotification(ServerNotification::new(
                        method, params,
                    )),
                },
            ))
            .await;
    }
}

fn add_total_tokens(usage: &mut Value) {
    let Some(object) = usage.as_object_mut() else {
        return;
    };
    let total = [
        "input_tokens",
        "output_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
    ]
    .iter()
    .filter_map(|key| object.get(*key).and_then(Value::as_i64))
    .sum::<i64>();
    object.insert("total_tokens".to_string(), Value::from(total));
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClaudeStreamEvent {
    #[serde(rename = "system")]
    System {
        subtype: String,
        session_id: Option<String>,
    },
    #[serde(rename = "assistant")]
    Assistant {
        message: ClaudeMessage,
        is_partial: Option<bool>,
    },
    #[serde(rename = "user")]
    User { message: ClaudeMessage },
    #[serde(rename = "stream_event")]
    StreamEvent { event: ClaudeInnerStreamEvent },
    #[serde(rename = "result")]
    Result {
        subtype: String,
        session_id: Option<String>,
        error: Option<String>,
        usage: Option<Value>,
        cost_usd: Option<f64>,
        total_cost_usd: Option<f64>,
        permission_denials: Option<Value>,
    },
    #[serde(rename = "rate_limit_event")]
    RateLimitEvent {},
}

#[derive(Debug, Deserialize)]
struct ClaudeMessage {
    id: Option<String>,
    content: Option<Vec<ClaudeContentBlock>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClaudeContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "thinking")]
    Thinking { thinking: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Option<Value>,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: Option<Value>,
        is_error: Option<bool>,
    },
}

#[derive(Debug, Deserialize)]
struct ClaudeInnerStreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    usage: Option<Value>,
}
