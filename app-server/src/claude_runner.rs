use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use claude_app_server_protocol::AgentMessageDeltaNotification;
use claude_app_server_protocol::DynamicToolCallOutputContentItem;
use claude_app_server_protocol::DynamicToolCallStatus;
use claude_app_server_protocol::Item;
use claude_app_server_protocol::ItemCompletedNotification;
use claude_app_server_protocol::ItemStartedNotification;
use claude_app_server_protocol::ReasoningTextDeltaNotification;
use claude_app_server_protocol::ServerNotification;
use claude_app_server_protocol::ThreadItem;
use claude_app_server_protocol::ThreadStartedNotification;
use claude_app_server_protocol::ThreadTokenUsageUpdatedNotification;
use claude_app_server_protocol::TokenUsageBreakdown;
use claude_app_server_protocol::TurnCompletedNotification;
use claude_app_server_protocol::TurnPermissionDeniedNotification;
use claude_app_server_protocol::TurnStatus;
use claude_app_server_transport::OutgoingMessage;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::debug;
use uuid::Uuid;

use crate::outgoing_message::OutboundControlEvent;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessageSender;
use crate::thread_state::ThreadStore;
use crate::thread_state::now_millis;

#[derive(Debug, Default, Clone)]
struct ClaudeStreamState {
    usage: Option<ClaudeUsageSnapshot>,
    last_total_tokens: Option<i64>,
    context_window_size: i64,
    active_blocks: HashMap<usize, ActiveStreamBlock>,
    pending_tool_calls: HashMap<String, PendingToolCall>,
}

#[derive(Debug, Clone)]
struct ActiveStreamBlock {
    item_id: String,
    kind: ActiveStreamBlockKind,
}

#[derive(Debug, Clone)]
enum ActiveStreamBlockKind {
    AgentMessage {
        text: String,
    },
    Reasoning {
        thinking: String,
    },
    ToolUse {
        tool_use_id: String,
        name: String,
        input: Value,
        input_json_delta: String,
    },
}

#[derive(Debug, Clone)]
struct PendingToolCall {
    item_id: String,
    name: String,
    input: Value,
}

#[derive(Debug, Default, Clone)]
struct ClaudeUsageSnapshot {
    input_tokens: i64,
    output_tokens: i64,
    cached_input_tokens: i64,
    cache_creation_input_tokens: i64,
    reasoning_output_tokens: i64,
}

impl ClaudeUsageSnapshot {
    fn total_tokens(&self) -> i64 {
        self.input_tokens
            + self.output_tokens
            + self.cached_input_tokens
            + self.cache_creation_input_tokens
    }
}

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
            let completed = store
                .with_thread_mut(&args.thread_id, |thread| {
                    let thread_id = thread.id.clone();
                    if let Some(turn) = thread.active_turn_mut() {
                        turn.status = TurnStatus::Failed;
                        turn.error = Some(err.to_string());
                        turn.completed_at = Some(now_millis());
                        let notification = TurnCompletedNotification {
                            thread_id,
                            turn: turn.snapshot(),
                        };
                        thread.active_turn_id = None;
                        return Some(notification);
                    }
                    None
                })
                .await
                .flatten();
            if let Some(notification) = completed {
                self.notify(args.connection_id, "turn/completed", notification)
                    .await;
            }
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

        let mut stream_state = ClaudeStreamState {
            context_window_size: 200_000,
            ..ClaudeStreamState::default()
        };
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
                &mut stream_state,
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
                    thread_id: notification_thread_id,
                    turn: turn.snapshot(),
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
        stream_state: &mut ClaudeStreamState,
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
                    let notification = store
                        .with_thread_mut(thread_id, |thread| {
                            thread.cli_session_id = Some(session_id);
                            ThreadStartedNotification {
                                thread: thread.snapshot(),
                            }
                        })
                        .await;
                    if let Some(notification) = notification {
                        self.notify(connection_id, "thread/started", notification)
                            .await;
                    }
                }
            }
            ClaudeStreamEvent::Assistant { .. } => {}
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
                        let is_error = is_error.unwrap_or(false);
                        let created = store
                            .with_thread_mut(thread_id, |thread| {
                                thread.active_turn_mut().map(|turn| {
                                    turn.push_item(Item::ToolResult {
                                        tool_use_id: tool_use_id.clone(),
                                        content: content.clone(),
                                        is_error,
                                    })
                                })
                            })
                            .await
                            .flatten();
                        if created.is_some() {
                            let pending = stream_state.pending_tool_calls.remove(&tool_use_id);
                            let item = if let Some(pending) = pending {
                                ThreadItem::DynamicToolCall {
                                    id: pending.item_id,
                                    namespace: None,
                                    tool: pending.name,
                                    arguments: pending.input,
                                    status: if is_error {
                                        DynamicToolCallStatus::Failed
                                    } else {
                                        DynamicToolCallStatus::Completed
                                    },
                                    content_items: Some(vec![
                                        DynamicToolCallOutputContentItem::InputText {
                                            text: content,
                                        },
                                    ]),
                                    success: Some(!is_error),
                                    duration_ms: None,
                                }
                            } else {
                                ThreadItem::DynamicToolCall {
                                    id: Uuid::now_v7().to_string(),
                                    namespace: None,
                                    tool: tool_use_id,
                                    arguments: Value::Null,
                                    status: if is_error {
                                        DynamicToolCallStatus::Failed
                                    } else {
                                        DynamicToolCallStatus::Completed
                                    },
                                    content_items: Some(vec![
                                        DynamicToolCallOutputContentItem::InputText {
                                            text: content,
                                        },
                                    ]),
                                    success: Some(!is_error),
                                    duration_ms: None,
                                }
                            };
                            self.notify(
                                connection_id,
                                "item/completed",
                                ItemCompletedNotification {
                                    thread_id: thread_id.to_string(),
                                    turn_id: turn_id.to_string(),
                                    item,
                                    completed_at_ms: now_millis(),
                                },
                            )
                            .await;
                        }
                    }
                }
            }
            ClaudeStreamEvent::StreamEvent { event, .. } => match event {
                ClaudeInnerStreamEvent::MessageStart { message } => {
                    stream_state.usage = None;
                    stream_state.last_total_tokens = None;
                    if let Some(model) = message.model.as_deref()
                        && let Some(inferred) = infer_context_window_from_model(model)
                    {
                        stream_state.context_window_size = inferred;
                    }
                    if let Some(usage_value) = message.usage.as_ref()
                        && let Some(token_breakdown) =
                            update_token_usage_snapshot(stream_state, usage_value)
                    {
                        self.notify(
                            connection_id,
                            "thread/tokenUsage/updated",
                            ThreadTokenUsageUpdatedNotification {
                                thread_id: thread_id.to_string(),
                                turn_id: turn_id.to_string(),
                                token_usage: claude_app_server_protocol::ThreadTokenUsage {
                                    total: token_breakdown.clone(),
                                    last: token_breakdown,
                                    model_context_window: Some(stream_state.context_window_size),
                                },
                            },
                        )
                        .await;
                    }
                }
                ClaudeInnerStreamEvent::MessageDelta { usage } => {
                    if let Some(usage_value) = usage
                        && let Some(token_breakdown) =
                            update_token_usage_snapshot(stream_state, &usage_value)
                    {
                        self.notify(
                            connection_id,
                            "thread/tokenUsage/updated",
                            ThreadTokenUsageUpdatedNotification {
                                thread_id: thread_id.to_string(),
                                turn_id: turn_id.to_string(),
                                token_usage: claude_app_server_protocol::ThreadTokenUsage {
                                    total: token_breakdown.clone(),
                                    last: token_breakdown,
                                    model_context_window: Some(stream_state.context_window_size),
                                },
                            },
                        )
                        .await;
                    }
                }
                ClaudeInnerStreamEvent::ContentBlockStart {
                    index,
                    content_block,
                } => {
                    let item_id = Uuid::now_v7().to_string();
                    let (item, block) = match content_block {
                        ClaudeContentBlock::Text { text } => (
                            Some(ThreadItem::AgentMessage {
                                id: item_id.clone(),
                                text: text.clone(),
                                phase: None,
                                memory_citation: None,
                            }),
                            Some(ActiveStreamBlock {
                                item_id,
                                kind: ActiveStreamBlockKind::AgentMessage { text },
                            }),
                        ),
                        ClaudeContentBlock::Thinking { thinking } => (
                            Some(ThreadItem::Reasoning {
                                id: item_id.clone(),
                                summary: Vec::new(),
                                content: if thinking.is_empty() {
                                    Vec::new()
                                } else {
                                    vec![thinking.clone()]
                                },
                            }),
                            Some(ActiveStreamBlock {
                                item_id,
                                kind: ActiveStreamBlockKind::Reasoning { thinking },
                            }),
                        ),
                        ClaudeContentBlock::ToolUse { id, name, input } => {
                            let input = input.unwrap_or(Value::Null);
                            (
                                Some(ThreadItem::DynamicToolCall {
                                    id: item_id.clone(),
                                    namespace: None,
                                    tool: name.clone(),
                                    arguments: input.clone(),
                                    status: DynamicToolCallStatus::InProgress,
                                    content_items: None,
                                    success: None,
                                    duration_ms: None,
                                }),
                                Some(ActiveStreamBlock {
                                    item_id,
                                    kind: ActiveStreamBlockKind::ToolUse {
                                        tool_use_id: id,
                                        name,
                                        input,
                                        input_json_delta: String::new(),
                                    },
                                }),
                            )
                        }
                        ClaudeContentBlock::ToolResult { .. } => (None, None),
                    };
                    if let Some(block) = block {
                        stream_state.active_blocks.insert(index, block);
                    }
                    if let Some(item) = item {
                        self.notify(
                            connection_id,
                            "item/started",
                            ItemStartedNotification {
                                item,
                                thread_id: thread_id.to_string(),
                                turn_id: turn_id.to_string(),
                                started_at_ms: now_millis(),
                            },
                        )
                        .await;
                    }
                }
                ClaudeInnerStreamEvent::ContentBlockDelta { index, delta } => {
                    let Some(active_block) = stream_state.active_blocks.get_mut(&index) else {
                        return Ok(());
                    };
                    match &mut active_block.kind {
                        ActiveStreamBlockKind::AgentMessage { text } => {
                            if let Some(delta_text) = delta.get("text").and_then(Value::as_str) {
                                text.push_str(delta_text);
                                self.notify(
                                    connection_id,
                                    "item/agentMessage/delta",
                                    AgentMessageDeltaNotification {
                                        thread_id: thread_id.to_string(),
                                        turn_id: turn_id.to_string(),
                                        item_id: active_block.item_id.clone(),
                                        delta: delta_text.to_string(),
                                    },
                                )
                                .await;
                            }
                        }
                        ActiveStreamBlockKind::Reasoning { thinking } => {
                            if let Some(delta_text) = delta.get("thinking").and_then(Value::as_str)
                            {
                                thinking.push_str(delta_text);
                                self.notify(
                                    connection_id,
                                    "item/reasoningText/delta",
                                    ReasoningTextDeltaNotification {
                                        thread_id: thread_id.to_string(),
                                        turn_id: turn_id.to_string(),
                                        item_id: active_block.item_id.clone(),
                                        delta: delta_text.to_string(),
                                        content_index: 0,
                                    },
                                )
                                .await;
                            }
                        }
                        ActiveStreamBlockKind::ToolUse {
                            input_json_delta, ..
                        } => {
                            if let Some(partial_json) =
                                delta.get("partial_json").and_then(Value::as_str)
                            {
                                input_json_delta.push_str(partial_json);
                            }
                        }
                    }
                }
                ClaudeInnerStreamEvent::ContentBlockStop { index } => {
                    let Some(active_block) = stream_state.active_blocks.remove(&index) else {
                        return Ok(());
                    };
                    match active_block.kind {
                        ActiveStreamBlockKind::AgentMessage { text } => {
                            let item_id = active_block.item_id;
                            let item = ThreadItem::AgentMessage {
                                id: item_id.clone(),
                                text: text.clone(),
                                phase: None,
                                memory_citation: None,
                            };
                            let created = store
                                .with_thread_mut(thread_id, |thread| {
                                    thread.active_turn_mut().map(|turn| {
                                        turn.push_item_with_id(item_id, Item::Text { text })
                                    })
                                })
                                .await
                                .flatten();
                            if created.is_some() {
                                self.notify(
                                    connection_id,
                                    "item/completed",
                                    ItemCompletedNotification {
                                        item,
                                        thread_id: thread_id.to_string(),
                                        turn_id: turn_id.to_string(),
                                        completed_at_ms: now_millis(),
                                    },
                                )
                                .await;
                            }
                        }
                        ActiveStreamBlockKind::Reasoning { thinking } => {
                            let item_id = active_block.item_id;
                            let item = ThreadItem::Reasoning {
                                id: item_id.clone(),
                                summary: Vec::new(),
                                content: if thinking.is_empty() {
                                    Vec::new()
                                } else {
                                    vec![thinking.clone()]
                                },
                            };
                            let created = store
                                .with_thread_mut(thread_id, |thread| {
                                    thread.active_turn_mut().map(|turn| {
                                        turn.push_item_with_id(item_id, Item::Thinking { thinking })
                                    })
                                })
                                .await
                                .flatten();
                            if created.is_some() {
                                self.notify(
                                    connection_id,
                                    "item/completed",
                                    ItemCompletedNotification {
                                        item,
                                        thread_id: thread_id.to_string(),
                                        turn_id: turn_id.to_string(),
                                        completed_at_ms: now_millis(),
                                    },
                                )
                                .await;
                            }
                        }
                        ActiveStreamBlockKind::ToolUse {
                            tool_use_id,
                            name,
                            input,
                            input_json_delta,
                        } => {
                            let item_id = active_block.item_id;
                            let input = if input_json_delta.is_empty() {
                                input
                            } else {
                                serde_json::from_str(&input_json_delta).unwrap_or(input)
                            };
                            let created = store
                                .with_thread_mut(thread_id, |thread| {
                                    thread.active_turn_mut().map(|turn| {
                                        turn.push_item_with_id(
                                            item_id.clone(),
                                            Item::ToolCall {
                                                tool_use_id: tool_use_id.clone(),
                                                name: name.clone(),
                                                input: input.clone(),
                                            },
                                        )
                                    })
                                })
                                .await
                                .flatten();
                            if created.is_some() {
                                stream_state.pending_tool_calls.insert(
                                    tool_use_id,
                                    PendingToolCall {
                                        item_id,
                                        name,
                                        input,
                                    },
                                );
                            }
                        }
                    }
                }
                ClaudeInnerStreamEvent::MessageStop => {}
            },
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
                                turn.status = TurnStatus::Failed;
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
    let total = usage_snapshot_from_value(usage, None).total_tokens();
    let Some(object) = usage.as_object_mut() else {
        return;
    };
    object.insert("total_tokens".to_string(), Value::from(total));
}

fn update_token_usage_snapshot(
    stream_state: &mut ClaudeStreamState,
    usage: &Value,
) -> Option<TokenUsageBreakdown> {
    let snapshot = usage_snapshot_from_value(usage, stream_state.usage.as_ref());
    let total_tokens = snapshot.total_tokens();
    if stream_state.last_total_tokens == Some(total_tokens) {
        stream_state.usage = Some(snapshot);
        return None;
    }
    stream_state.last_total_tokens = Some(total_tokens);
    stream_state.usage = Some(snapshot.clone());
    Some(TokenUsageBreakdown {
        total_tokens,
        input_tokens: snapshot.input_tokens,
        cached_input_tokens: snapshot.cached_input_tokens,
        output_tokens: snapshot.output_tokens,
        reasoning_output_tokens: snapshot.reasoning_output_tokens,
    })
}

fn usage_snapshot_from_value(
    usage: &Value,
    previous: Option<&ClaudeUsageSnapshot>,
) -> ClaudeUsageSnapshot {
    let empty_map = serde_json::Map::new();
    let obj = usage.as_object().unwrap_or(&empty_map);
    let previous = previous.cloned().unwrap_or_default();
    ClaudeUsageSnapshot {
        input_tokens: obj
            .get("input_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(previous.input_tokens),
        output_tokens: obj
            .get("output_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(previous.output_tokens),
        cached_input_tokens: obj
            .get("cache_read_input_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(previous.cached_input_tokens),
        cache_creation_input_tokens: obj
            .get("cache_creation_input_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(previous.cache_creation_input_tokens),
        reasoning_output_tokens: obj
            .get("reasoning_output_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(previous.reasoning_output_tokens),
    }
}

fn infer_context_window_from_model(model: &str) -> Option<i64> {
    model
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|part| part.eq_ignore_ascii_case("1m"))
        .then_some(1_000_000)
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
    Assistant {},
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
struct ClaudeMessageStart {
    model: Option<String>,
    usage: Option<Value>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClaudeInnerStreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: ClaudeMessageStart },
    #[serde(rename = "message_delta")]
    MessageDelta { usage: Option<Value> },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: usize,
        content_block: ClaudeContentBlock,
    },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { index: usize, delta: Value },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: usize },
    #[serde(rename = "message_stop")]
    MessageStop,
}

#[derive(Debug, Deserialize)]
struct ClaudeMessage {
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

#[cfg(test)]
mod tests {
    use super::*;
    use claude_app_server_protocol::PermissionMode;
    use claude_app_server_transport::ConnectionId;
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    async fn collect_jsonrpc_notification(
        rx: &mut mpsc::Receiver<OutboundControlEvent>,
    ) -> claude_app_server_protocol::JSONRPCNotification {
        let event = rx.recv().await.expect("expected notification");
        let OutboundControlEvent::Envelope(OutgoingEnvelope::ToConnection {
            message: OutgoingMessage::AppServerNotification(notification),
            ..
        }) = event
        else {
            panic!("unexpected outbound event");
        };
        notification.into_jsonrpc()
    }

    async fn make_runner() -> (
        ClaudeRunner,
        ThreadStore,
        mpsc::Receiver<OutboundControlEvent>,
        String,
        String,
    ) {
        let (tx, rx) = mpsc::channel(8);
        let runner = ClaudeRunner::new(PathBuf::from("claude"), false, tx);
        let store = ThreadStore::default();
        let thread =
            crate::thread_state::ThreadState::new(PathBuf::from("/tmp"), PermissionMode::Default);
        let thread_id = thread.id.clone();
        let turn = crate::thread_state::TurnState::new(thread.id.clone(), "hello".to_string());
        let turn_id = turn.id.clone();
        let mut thread = thread;
        thread.active_turn_id = Some(turn_id.clone());
        thread.turns.push(turn);
        store.insert(thread).await;
        (runner, store, rx, thread_id, turn_id)
    }

    #[tokio::test]
    async fn message_stream_updates_token_usage() {
        let (runner, store, mut rx, thread_id, turn_id) = make_runner().await;
        let mut state = ClaudeStreamState {
            context_window_size: 200_000,
            ..ClaudeStreamState::default()
        };

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::MessageStart {
                        message: ClaudeMessageStart {
                            model: Some("claude-opus-4-6-1m".to_string()),
                            usage: Some(serde_json::json!({
                                "input_tokens": 1000,
                                "output_tokens": 0,
                                "cache_read_input_tokens": 200,
                                "cache_creation_input_tokens": 100
                            })),
                        },
                    },
                },
                &mut state,
            )
            .await
            .unwrap();

        let notification = collect_jsonrpc_notification(&mut rx).await;
        assert_eq!(notification.method, "thread/tokenUsage/updated");
        let params = notification.params.expect("params");
        assert_eq!(
            params.get("threadId").and_then(Value::as_str),
            Some(thread_id.as_str())
        );
        assert_eq!(
            params.get("turnId").and_then(Value::as_str),
            Some(turn_id.as_str())
        );
        assert_eq!(
            params["tokenUsage"]["total"]["totalTokens"].as_i64(),
            Some(1300)
        );
        assert_eq!(
            params["tokenUsage"]["modelContextWindow"].as_i64(),
            Some(1_000_000)
        );

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::MessageDelta {
                        usage: Some(serde_json::json!({
                            "output_tokens": 500
                        })),
                    },
                },
                &mut state,
            )
            .await
            .unwrap();

        let notification = collect_jsonrpc_notification(&mut rx).await;
        assert_eq!(notification.method, "thread/tokenUsage/updated");
        let params = notification.params.expect("params");
        assert_eq!(
            params["tokenUsage"]["total"]["totalTokens"].as_i64(),
            Some(1800)
        );
    }

    #[tokio::test]
    async fn tool_use_block_emits_item_started() {
        let (runner, store, mut rx, thread_id, turn_id) = make_runner().await;
        let mut state = ClaudeStreamState {
            context_window_size: 200_000,
            ..ClaudeStreamState::default()
        };

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::ContentBlockStart {
                        index: 0,
                        content_block: ClaudeContentBlock::ToolUse {
                            id: "tool_1".to_string(),
                            name: "Bash".to_string(),
                            input: Some(serde_json::json!({"command":"echo hi"})),
                        },
                    },
                },
                &mut state,
            )
            .await
            .unwrap();

        let notification = collect_jsonrpc_notification(&mut rx).await;
        assert_eq!(notification.method, "item/started");
        let params = notification.params.expect("params");
        assert_eq!(
            params.get("threadId").and_then(Value::as_str),
            Some(thread_id.as_str())
        );
        assert_eq!(
            params.get("turnId").and_then(Value::as_str),
            Some(turn_id.as_str())
        );
        assert_eq!(
            params
                .get("item")
                .and_then(|item| item.get("type"))
                .and_then(Value::as_str),
            Some("dynamicToolCall")
        );
    }

    #[tokio::test]
    async fn text_block_emits_codex_delta_and_completed_item() {
        let (runner, store, mut rx, thread_id, turn_id) = make_runner().await;
        let mut state = ClaudeStreamState {
            context_window_size: 200_000,
            ..ClaudeStreamState::default()
        };

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::ContentBlockStart {
                        index: 0,
                        content_block: ClaudeContentBlock::Text {
                            text: String::new(),
                        },
                    },
                },
                &mut state,
            )
            .await
            .unwrap();

        let started = collect_jsonrpc_notification(&mut rx).await;
        assert_eq!(started.method, "item/started");
        let started_params = started.params.expect("params");
        let item_id = started_params["item"]["id"]
            .as_str()
            .expect("item id")
            .to_string();

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::ContentBlockDelta {
                        index: 0,
                        delta: serde_json::json!({
                            "type": "text_delta",
                            "text": "hello"
                        }),
                    },
                },
                &mut state,
            )
            .await
            .unwrap();

        let delta = collect_jsonrpc_notification(&mut rx).await;
        assert_eq!(delta.method, "item/agentMessage/delta");
        let delta_params = delta.params.expect("params");
        assert_eq!(delta_params["itemId"].as_str(), Some(item_id.as_str()));
        assert_eq!(delta_params["delta"].as_str(), Some("hello"));

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::ContentBlockStop { index: 0 },
                },
                &mut state,
            )
            .await
            .unwrap();

        let completed = collect_jsonrpc_notification(&mut rx).await;
        assert_eq!(completed.method, "item/completed");
        let completed_params = completed.params.expect("params");
        assert_eq!(
            completed_params["item"]["id"].as_str(),
            Some(item_id.as_str())
        );
        assert_eq!(completed_params["item"]["text"].as_str(), Some("hello"));
    }

    #[tokio::test]
    async fn duplicate_usage_snapshot_is_deduped() {
        let (runner, store, mut rx, thread_id, turn_id) = make_runner().await;
        let mut state = ClaudeStreamState {
            context_window_size: 200_000,
            ..ClaudeStreamState::default()
        };

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::MessageStart {
                        message: ClaudeMessageStart {
                            model: Some("claude-opus-4-20250514".to_string()),
                            usage: Some(serde_json::json!({
                                "input_tokens": 1000,
                                "output_tokens": 0,
                                "cache_read_input_tokens": 200,
                                "cache_creation_input_tokens": 100
                            })),
                        },
                    },
                },
                &mut state,
            )
            .await
            .unwrap();

        let _ = collect_jsonrpc_notification(&mut rx).await;

        runner
            .process_claude_event(
                &store,
                &thread_id,
                &turn_id,
                ConnectionId(1),
                ClaudeStreamEvent::StreamEvent {
                    event: ClaudeInnerStreamEvent::MessageDelta {
                        usage: Some(serde_json::json!({
                            "output_tokens": 0
                        })),
                    },
                },
                &mut state,
            )
            .await
            .unwrap();

        assert!(rx.try_recv().is_err());
    }
}
