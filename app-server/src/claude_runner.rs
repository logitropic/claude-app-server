use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use claude_app_server_protocol::AgentMessageDeltaNotification;
use claude_app_server_protocol::CommandExecutionOutputDeltaNotification;
use claude_app_server_protocol::HookCompletedNotification;
use claude_app_server_protocol::HookStartedNotification;
use claude_app_server_protocol::Item;
use claude_app_server_protocol::ItemCompletedNotification;
use claude_app_server_protocol::ItemCreatedNotification;
use claude_app_server_protocol::ItemProgressNotification;
use claude_app_server_protocol::ItemStartedNotification;
use claude_app_server_protocol::ReasoningSummaryTextDeltaNotification;
use claude_app_server_protocol::RichItemStatus;
use claude_app_server_protocol::ServerNotification;
use claude_app_server_protocol::ThreadItem;
use claude_app_server_protocol::TurnCompletedNotification;
use claude_app_server_protocol::TurnFailedNotification;
use claude_app_server_protocol::TurnPermissionDeniedNotification;
use claude_app_server_protocol::TurnPlanStep;
use claude_app_server_protocol::TurnPlanStepStatus;
use claude_app_server_protocol::TurnPlanUpdatedNotification;
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
            "--include-hook-events".to_string(),
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
        let mut rich_events = RichEventState::default();
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
                &mut rich_events,
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
                    turn: Some(turn.snapshot()),
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
        rich_events: &mut RichEventState,
    ) -> anyhow::Result<()> {
        match event {
            ClaudeStreamEvent::System {
                subtype,
                session_id,
                hook_id,
                hook_name,
                hook_event,
                output,
                stdout,
                stderr,
                exit_code,
                outcome,
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
                match subtype.as_str() {
                    "hook_started" => {
                        if let (Some(hook_id), Some(hook_name), Some(hook_event)) =
                            (hook_id, hook_name, hook_event)
                        {
                            self.notify(
                                connection_id,
                                "hook/started",
                                HookStartedNotification {
                                    thread_id: thread_id.to_string(),
                                    turn_id: turn_id.to_string(),
                                    hook_id,
                                    hook_name,
                                    hook_event,
                                    started_at_ms: now_millis(),
                                },
                            )
                            .await;
                        }
                    }
                    "hook_response" => {
                        if let (Some(hook_id), Some(hook_name), Some(hook_event), Some(outcome)) =
                            (hook_id, hook_name, hook_event, outcome)
                        {
                            self.notify(
                                connection_id,
                                "hook/completed",
                                HookCompletedNotification {
                                    thread_id: thread_id.to_string(),
                                    turn_id: turn_id.to_string(),
                                    hook_id,
                                    hook_name,
                                    hook_event,
                                    outcome,
                                    output: output.unwrap_or_default(),
                                    stdout: stdout.unwrap_or_default(),
                                    stderr: stderr.unwrap_or_default(),
                                    exit_code,
                                    completed_at_ms: now_millis(),
                                },
                            )
                            .await;
                        }
                    }
                    _ => {}
                }
            }
            ClaudeStreamEvent::Assistant {
                message,
                is_partial,
                ..
            } => {
                let msg_id = message.id.unwrap_or_else(|| "unknown".to_string());
                let partial = is_partial.unwrap_or(false);
                for (index, block) in message.content.unwrap_or_default().into_iter().enumerate() {
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
                                rich_events
                                    .complete_agent_message_from_assistant(
                                        self,
                                        connection_id,
                                        thread_id,
                                        turn_id,
                                        &msg_id,
                                        index,
                                        text.clone(),
                                    )
                                    .await;
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
                            rich_events
                                .complete_reasoning_from_assistant(
                                    self,
                                    connection_id,
                                    thread_id,
                                    turn_id,
                                    &msg_id,
                                    index,
                                    thinking.clone(),
                                )
                                .await;
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
                            let input = input.unwrap_or(Value::Null);
                            rich_events
                                .start_tool_call(
                                    self,
                                    connection_id,
                                    thread_id,
                                    turn_id,
                                    id.clone(),
                                    name.clone(),
                                    input.clone(),
                                )
                                .await;
                            rich_events
                                .maybe_notify_plan_update(
                                    self,
                                    connection_id,
                                    thread_id,
                                    turn_id,
                                    &id,
                                    &name,
                                    &input,
                                )
                                .await;
                            let created = store
                                .with_thread_mut(thread_id, |thread| {
                                    thread.active_turn_mut().map(|turn| {
                                        turn.push_item(Item::ToolCall {
                                            tool_use_id: id,
                                            name,
                                            input,
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
                        let raw_content = content.unwrap_or(Value::Null);
                        let content = tool_result_text(&raw_content);
                        rich_events
                            .complete_tool_call(
                                self,
                                connection_id,
                                thread_id,
                                turn_id,
                                tool_use_id.clone(),
                                raw_content,
                                content.clone(),
                                is_error.unwrap_or(false),
                            )
                            .await;
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
            ClaudeStreamEvent::StreamEvent { event, .. } => match event {
                ClaudeInnerStreamEvent::MessageDelta { usage } => {
                    let Some(mut usage) = usage else {
                        return Ok(());
                    };
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
                ClaudeInnerStreamEvent::ContentBlockStart {
                    index,
                    content_block,
                } => {
                    rich_events
                        .start_content_block(
                            self,
                            connection_id,
                            thread_id,
                            turn_id,
                            index,
                            content_block,
                        )
                        .await;
                }
                ClaudeInnerStreamEvent::ContentBlockDelta { index, delta } => {
                    rich_events
                        .apply_content_delta(self, connection_id, thread_id, turn_id, index, delta)
                        .await;
                }
                ClaudeInnerStreamEvent::ContentBlockStop { index } => {
                    rich_events
                        .complete_content_block(self, connection_id, thread_id, turn_id, index)
                        .await;
                }
                ClaudeInnerStreamEvent::Other => {}
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
        hook_id: Option<String>,
        hook_name: Option<String>,
        hook_event: Option<String>,
        output: Option<String>,
        stdout: Option<String>,
        stderr: Option<String>,
        exit_code: Option<i32>,
        outcome: Option<String>,
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
#[serde(tag = "type")]
enum ClaudeInnerStreamEvent {
    #[serde(rename = "message_delta")]
    MessageDelta { usage: Option<Value> },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: usize,
        content_block: ClaudeContentBlock,
    },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta {
        index: usize,
        delta: ClaudeContentBlockDelta,
    },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: usize },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClaudeContentBlockDelta {
    #[serde(rename = "text_delta")]
    TextDelta { text: String },
    #[serde(rename = "thinking_delta")]
    ThinkingDelta { thinking: String },
    #[serde(rename = "input_json_delta")]
    InputJsonDelta {
        #[serde(rename = "partial_json")]
        _partial_json: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Default)]
struct RichEventState {
    content_blocks: HashMap<usize, RichContentBlock>,
    tool_calls: HashMap<String, RichToolCall>,
    completed_tools: HashSet<String>,
    plan_tool_calls: HashSet<String>,
    saw_content_block_stream: bool,
}

struct RichToolCall {
    name: String,
    arguments: Value,
}

enum RichContentBlock {
    AgentMessage {
        item_id: String,
        text: String,
        completed: bool,
    },
    Reasoning {
        item_id: String,
        thinking: String,
        completed: bool,
    },
    ToolCall {
        tool_use_id: String,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RichContentBlockKind {
    AgentMessage,
    Reasoning,
    ToolCall,
}

impl RichContentBlock {
    fn kind(&self) -> RichContentBlockKind {
        match self {
            RichContentBlock::AgentMessage { .. } => RichContentBlockKind::AgentMessage,
            RichContentBlock::Reasoning { .. } => RichContentBlockKind::Reasoning,
            RichContentBlock::ToolCall { .. } => RichContentBlockKind::ToolCall,
        }
    }
}

impl RichEventState {
    async fn start_content_block(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        index: usize,
        content_block: ClaudeContentBlock,
    ) {
        self.saw_content_block_stream = true;
        match content_block {
            ClaudeContentBlock::Text { text } => {
                let item_id = stream_item_id(turn_id, index);
                runner
                    .notify(
                        connection_id,
                        "item/started",
                        ItemStartedNotification {
                            thread_id: thread_id.to_string(),
                            turn_id: turn_id.to_string(),
                            started_at_ms: now_millis(),
                            item: ThreadItem::AgentMessage {
                                id: item_id.clone(),
                                text: String::new(),
                            },
                        },
                    )
                    .await;
                if !text.is_empty() {
                    runner
                        .notify(
                            connection_id,
                            "item/agentMessage/delta",
                            AgentMessageDeltaNotification {
                                thread_id: thread_id.to_string(),
                                turn_id: turn_id.to_string(),
                                item_id: item_id.clone(),
                                delta: text.clone(),
                            },
                        )
                        .await;
                }
                self.content_blocks.insert(
                    index,
                    RichContentBlock::AgentMessage {
                        item_id,
                        text,
                        completed: false,
                    },
                );
            }
            ClaudeContentBlock::Thinking { thinking } => {
                let item_id = stream_item_id(turn_id, index);
                runner
                    .notify(
                        connection_id,
                        "item/started",
                        ItemStartedNotification {
                            thread_id: thread_id.to_string(),
                            turn_id: turn_id.to_string(),
                            started_at_ms: now_millis(),
                            item: ThreadItem::Reasoning {
                                id: item_id.clone(),
                                summary: Vec::new(),
                                content: Vec::new(),
                            },
                        },
                    )
                    .await;
                if !thinking.is_empty() {
                    runner
                        .notify(
                            connection_id,
                            "item/reasoning/summaryTextDelta",
                            ReasoningSummaryTextDeltaNotification {
                                thread_id: thread_id.to_string(),
                                turn_id: turn_id.to_string(),
                                item_id: item_id.clone(),
                                delta: thinking.clone(),
                                summary_index: 0,
                            },
                        )
                        .await;
                }
                self.content_blocks.insert(
                    index,
                    RichContentBlock::Reasoning {
                        item_id,
                        thinking,
                        completed: false,
                    },
                );
            }
            ClaudeContentBlock::ToolUse { id, name, input } => {
                let input = input.unwrap_or(Value::Null);
                self.start_tool_call(
                    runner,
                    connection_id,
                    thread_id,
                    turn_id,
                    id.clone(),
                    name.clone(),
                    input.clone(),
                )
                .await;
                self.maybe_notify_plan_update(
                    runner,
                    connection_id,
                    thread_id,
                    turn_id,
                    &id,
                    &name,
                    &input,
                )
                .await;
                self.content_blocks
                    .insert(index, RichContentBlock::ToolCall { tool_use_id: id });
            }
            ClaudeContentBlock::ToolResult { .. } => {}
        }
    }

    async fn apply_content_delta(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        index: usize,
        delta: ClaudeContentBlockDelta,
    ) {
        match (self.content_blocks.get_mut(&index), delta) {
            (
                Some(RichContentBlock::AgentMessage { item_id, text, .. }),
                ClaudeContentBlockDelta::TextDelta { text: delta },
            ) => {
                text.push_str(&delta);
                runner
                    .notify(
                        connection_id,
                        "item/agentMessage/delta",
                        AgentMessageDeltaNotification {
                            thread_id: thread_id.to_string(),
                            turn_id: turn_id.to_string(),
                            item_id: item_id.clone(),
                            delta,
                        },
                    )
                    .await;
            }
            (
                Some(RichContentBlock::Reasoning {
                    item_id, thinking, ..
                }),
                ClaudeContentBlockDelta::ThinkingDelta { thinking: delta },
            ) => {
                thinking.push_str(&delta);
                runner
                    .notify(
                        connection_id,
                        "item/reasoning/summaryTextDelta",
                        ReasoningSummaryTextDeltaNotification {
                            thread_id: thread_id.to_string(),
                            turn_id: turn_id.to_string(),
                            item_id: item_id.clone(),
                            delta,
                            summary_index: 0,
                        },
                    )
                    .await;
            }
            _ => {}
        }
    }

    async fn complete_content_block(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        index: usize,
    ) {
        let Some(block) = self.content_blocks.get_mut(&index) else {
            return;
        };
        match block {
            RichContentBlock::AgentMessage {
                item_id,
                text,
                completed,
            } => {
                if *completed {
                    return;
                }
                *completed = true;
                runner
                    .notify(
                        connection_id,
                        "item/completed",
                        ItemCompletedNotification {
                            thread_id: thread_id.to_string(),
                            turn_id: turn_id.to_string(),
                            completed_at_ms: now_millis(),
                            item: ThreadItem::AgentMessage {
                                id: item_id.clone(),
                                text: text.clone(),
                            },
                        },
                    )
                    .await;
            }
            RichContentBlock::Reasoning {
                item_id,
                thinking,
                completed,
            } => {
                if *completed {
                    return;
                }
                *completed = true;
                runner
                    .notify(
                        connection_id,
                        "item/completed",
                        ItemCompletedNotification {
                            thread_id: thread_id.to_string(),
                            turn_id: turn_id.to_string(),
                            completed_at_ms: now_millis(),
                            item: ThreadItem::Reasoning {
                                id: item_id.clone(),
                                summary: vec![thinking.clone()],
                                content: Vec::new(),
                            },
                        },
                    )
                    .await;
            }
            RichContentBlock::ToolCall { tool_use_id } => {
                let _ = tool_use_id;
            }
        }
    }

    async fn complete_agent_message_from_assistant(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        message_id: &str,
        index: usize,
        text: String,
    ) {
        if self.saw_content_block_stream {
            return;
        }
        if let Some(stream_index) =
            self.find_content_block_index(index, RichContentBlockKind::AgentMessage)
        {
            if !self.content_block_completed(stream_index) {
                self.complete_content_block(
                    runner,
                    connection_id,
                    thread_id,
                    turn_id,
                    stream_index,
                )
                .await;
            }
            self.content_blocks.remove(&stream_index);
            return;
        }
        self.emit_agent_message_from_final(
            runner,
            connection_id,
            thread_id,
            turn_id,
            message_id,
            index,
            text,
        )
        .await;
    }

    fn find_content_block_index(
        &self,
        preferred_index: usize,
        kind: RichContentBlockKind,
    ) -> Option<usize> {
        if self
            .content_blocks
            .get(&preferred_index)
            .is_some_and(|block| block.kind() == kind)
        {
            return Some(preferred_index);
        }
        self.content_blocks
            .iter()
            .find_map(|(index, block)| (block.kind() == kind).then_some(*index))
    }

    fn content_block_completed(&self, index: usize) -> bool {
        match self.content_blocks.get(&index) {
            Some(RichContentBlock::AgentMessage { completed, .. })
            | Some(RichContentBlock::Reasoning { completed, .. }) => *completed,
            Some(RichContentBlock::ToolCall { .. }) | None => false,
        }
    }

    async fn emit_agent_message_from_final(
        &self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        message_id: &str,
        index: usize,
        text: String,
    ) {
        let item_id = assistant_item_id(message_id, index);
        runner
            .notify(
                connection_id,
                "item/started",
                ItemStartedNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    started_at_ms: now_millis(),
                    item: ThreadItem::AgentMessage {
                        id: item_id.clone(),
                        text: String::new(),
                    },
                },
            )
            .await;
        if !text.is_empty() {
            runner
                .notify(
                    connection_id,
                    "item/agentMessage/delta",
                    AgentMessageDeltaNotification {
                        thread_id: thread_id.to_string(),
                        turn_id: turn_id.to_string(),
                        item_id: item_id.clone(),
                        delta: text.clone(),
                    },
                )
                .await;
        }
        runner
            .notify(
                connection_id,
                "item/completed",
                ItemCompletedNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    completed_at_ms: now_millis(),
                    item: ThreadItem::AgentMessage { id: item_id, text },
                },
            )
            .await;
    }

    async fn complete_reasoning_from_assistant(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        message_id: &str,
        index: usize,
        thinking: String,
    ) {
        if self.saw_content_block_stream {
            return;
        }
        if let Some(stream_index) =
            self.find_content_block_index(index, RichContentBlockKind::Reasoning)
        {
            if !self.content_block_completed(stream_index) {
                self.complete_content_block(
                    runner,
                    connection_id,
                    thread_id,
                    turn_id,
                    stream_index,
                )
                .await;
            }
            self.content_blocks.remove(&stream_index);
            return;
        }
        self.emit_reasoning_from_final(
            runner,
            connection_id,
            thread_id,
            turn_id,
            message_id,
            index,
            thinking,
        )
        .await;
    }

    async fn emit_reasoning_from_final(
        &self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        message_id: &str,
        index: usize,
        thinking: String,
    ) {
        let item_id = assistant_item_id(message_id, index);
        runner
            .notify(
                connection_id,
                "item/started",
                ItemStartedNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    started_at_ms: now_millis(),
                    item: ThreadItem::Reasoning {
                        id: item_id.clone(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            )
            .await;
        if !thinking.is_empty() {
            runner
                .notify(
                    connection_id,
                    "item/reasoning/summaryTextDelta",
                    ReasoningSummaryTextDeltaNotification {
                        thread_id: thread_id.to_string(),
                        turn_id: turn_id.to_string(),
                        item_id: item_id.clone(),
                        delta: thinking.clone(),
                        summary_index: 0,
                    },
                )
                .await;
        }
        runner
            .notify(
                connection_id,
                "item/completed",
                ItemCompletedNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    completed_at_ms: now_millis(),
                    item: ThreadItem::Reasoning {
                        id: item_id,
                        summary: vec![thinking],
                        content: Vec::new(),
                    },
                },
            )
            .await;
    }

    async fn start_tool_call(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        id: String,
        name: String,
        arguments: Value,
    ) {
        if self.tool_calls.contains_key(&id) {
            return;
        }
        self.tool_calls.insert(
            id.clone(),
            RichToolCall {
                name: name.clone(),
                arguments: arguments.clone(),
            },
        );
        let item = if name == "Bash" {
            ThreadItem::CommandExecution {
                id: id.clone(),
                command: arguments
                    .get("command")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                cwd: None,
                status: RichItemStatus::InProgress,
                aggregated_output: None,
                exit_code: None,
            }
        } else {
            ThreadItem::ToolCall {
                id: id.clone(),
                name,
                arguments,
                status: RichItemStatus::InProgress,
                result: None,
                error: None,
            }
        };
        runner
            .notify(
                connection_id,
                "item/started",
                ItemStartedNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    started_at_ms: now_millis(),
                    item,
                },
            )
            .await;
    }

    async fn maybe_notify_plan_update(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        tool_use_id: &str,
        tool_name: &str,
        input: &Value,
    ) {
        if !self.plan_tool_calls.insert(tool_use_id.to_string()) {
            return;
        }
        maybe_notify_plan_update(runner, connection_id, thread_id, turn_id, tool_name, input).await;
    }

    async fn complete_tool_call(
        &mut self,
        runner: &ClaudeRunner,
        connection_id: claude_app_server_transport::ConnectionId,
        thread_id: &str,
        turn_id: &str,
        tool_use_id: String,
        raw_content: Value,
        text_content: String,
        is_error: bool,
    ) {
        if !self.completed_tools.insert(tool_use_id.clone()) {
            return;
        }
        let tool = self
            .tool_calls
            .remove(&tool_use_id)
            .unwrap_or(RichToolCall {
                name: "unknown".to_string(),
                arguments: Value::Null,
            });
        let status = if is_error {
            RichItemStatus::Failed
        } else {
            RichItemStatus::Completed
        };
        let item = if tool.name == "Bash" {
            if !text_content.is_empty() {
                runner
                    .notify(
                        connection_id,
                        "item/commandExecution/outputDelta",
                        CommandExecutionOutputDeltaNotification {
                            thread_id: thread_id.to_string(),
                            turn_id: turn_id.to_string(),
                            item_id: tool_use_id.clone(),
                            delta: text_content.clone(),
                        },
                    )
                    .await;
            }
            ThreadItem::CommandExecution {
                id: tool_use_id,
                command: tool
                    .arguments
                    .get("command")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                cwd: None,
                status,
                aggregated_output: if text_content.is_empty() {
                    None
                } else {
                    Some(text_content)
                },
                exit_code: None,
            }
        } else {
            ThreadItem::ToolCall {
                id: tool_use_id,
                name: tool.name,
                arguments: tool.arguments,
                status,
                result: if is_error { None } else { Some(raw_content) },
                error: if is_error { Some(text_content) } else { None },
            }
        };
        runner
            .notify(
                connection_id,
                "item/completed",
                ItemCompletedNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    completed_at_ms: now_millis(),
                    item,
                },
            )
            .await;
    }
}

fn stream_item_id(turn_id: &str, index: usize) -> String {
    format!("{turn_id}:content:{index}")
}

fn assistant_item_id(message_id: &str, index: usize) -> String {
    format!("{message_id}:content:{index}")
}

fn tool_result_text(content: &Value) -> String {
    match content {
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        value => value.to_string(),
    }
}

async fn maybe_notify_plan_update(
    runner: &ClaudeRunner,
    connection_id: claude_app_server_transport::ConnectionId,
    thread_id: &str,
    turn_id: &str,
    tool_name: &str,
    input: &Value,
) {
    if tool_name != "TodoWrite" {
        return;
    }
    let Some(plan) = plan_steps_from_todo_input(input) else {
        return;
    };
    runner
        .notify(
            connection_id,
            "turn/plan/updated",
            TurnPlanUpdatedNotification {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                explanation: None,
                plan,
            },
        )
        .await;
}

fn plan_steps_from_todo_input(input: &Value) -> Option<Vec<TurnPlanStep>> {
    let todos = input.get("todos").and_then(Value::as_array)?;
    let plan = todos
        .iter()
        .filter_map(|todo| {
            let step = todo
                .get("content")
                .or_else(|| todo.get("activeForm"))
                .and_then(Value::as_str)?
                .to_string();
            let status = match todo.get("status").and_then(Value::as_str) {
                Some("in_progress") => TurnPlanStepStatus::InProgress,
                Some("completed") => TurnPlanStepStatus::Completed,
                _ => TurnPlanStepStatus::Pending,
            };
            Some(TurnPlanStep { step, status })
        })
        .collect::<Vec<_>>();
    Some(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_text_delta_stream_event() {
        let event = serde_json::from_value::<ClaudeStreamEvent>(json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "hello" }
            }
        }))
        .unwrap();

        match event {
            ClaudeStreamEvent::StreamEvent {
                event:
                    ClaudeInnerStreamEvent::ContentBlockDelta {
                        index,
                        delta: ClaudeContentBlockDelta::TextDelta { text },
                    },
            } => {
                assert_eq!(index, 0);
                assert_eq!(text, "hello");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_message_delta_without_usage() {
        let event = serde_json::from_value::<ClaudeStreamEvent>(json!({
            "type": "stream_event",
            "event": { "type": "message_delta" }
        }))
        .unwrap();

        match event {
            ClaudeStreamEvent::StreamEvent {
                event: ClaudeInnerStreamEvent::MessageDelta { usage },
            } => assert!(usage.is_none()),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_hook_response_event() {
        let event = serde_json::from_value::<ClaudeStreamEvent>(json!({
            "type": "system",
            "subtype": "hook_response",
            "hook_id": "hook-1",
            "hook_name": "PreToolUse:Bash",
            "hook_event": "PreToolUse",
            "outcome": "success",
            "output": "ok",
            "stdout": "ok",
            "stderr": "",
            "exit_code": 0
        }))
        .unwrap();

        match event {
            ClaudeStreamEvent::System {
                subtype,
                hook_id,
                hook_name,
                hook_event,
                outcome,
                exit_code,
                ..
            } => {
                assert_eq!(subtype, "hook_response");
                assert_eq!(hook_id.as_deref(), Some("hook-1"));
                assert_eq!(hook_name.as_deref(), Some("PreToolUse:Bash"));
                assert_eq!(hook_event.as_deref(), Some("PreToolUse"));
                assert_eq!(outcome.as_deref(), Some("success"));
                assert_eq!(exit_code, Some(0));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn maps_todo_write_input_to_plan_steps() {
        let steps = plan_steps_from_todo_input(&json!({
            "todos": [
                { "content": "Read code", "status": "completed" },
                { "content": "Patch runner", "status": "in_progress" },
                { "content": "Update docs", "status": "pending" }
            ]
        }))
        .unwrap();

        assert_eq!(
            steps,
            vec![
                TurnPlanStep {
                    step: "Read code".to_string(),
                    status: TurnPlanStepStatus::Completed,
                },
                TurnPlanStep {
                    step: "Patch runner".to_string(),
                    status: TurnPlanStepStatus::InProgress,
                },
                TurnPlanStep {
                    step: "Update docs".to_string(),
                    status: TurnPlanStepStatus::Pending,
                },
            ]
        );
    }

    #[test]
    fn extracts_text_from_tool_result_parts() {
        assert_eq!(
            tool_result_text(&json!([
                { "type": "text", "text": "one" },
                { "type": "text", "text": "two" },
                { "type": "image", "source": {} }
            ])),
            "onetwo"
        );
    }
}
