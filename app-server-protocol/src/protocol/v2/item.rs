use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Item {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
    },
    ToolCall {
        tool_use_id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StoredItem {
    pub id: String,
    pub created_at: u128,
    #[serde(flatten)]
    pub item: Item,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ItemProgressNotification {
    pub turn_id: String,
    pub delta: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ItemCreatedNotification {
    pub turn_id: String,
    pub item: StoredItem,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RichItemStatus {
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ThreadItem {
    AgentMessage {
        id: String,
        text: String,
    },
    Reasoning {
        id: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        summary: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        content: Vec<String>,
    },
    CommandExecution {
        id: String,
        command: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        status: RichItemStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        aggregated_output: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: Value,
        status: RichItemStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ItemStartedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item: ThreadItem,
    pub started_at_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ItemCompletedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item: ThreadItem,
    pub completed_at_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentMessageDeltaNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReasoningSummaryTextDeltaNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
    pub summary_index: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CommandExecutionOutputDeltaNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_rich_item_lifecycle_notifications() {
        let notification = ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            started_at_ms: 123,
            item: ThreadItem::ToolCall {
                id: "tool-1".to_string(),
                name: "Read".to_string(),
                arguments: json!({ "file_path": "src/lib.rs" }),
                status: RichItemStatus::InProgress,
                result: None,
                error: None,
            },
        };

        assert_eq!(
            serde_json::to_value(notification).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "started_at_ms": 123,
                "item": {
                    "type": "tool_call",
                    "id": "tool-1",
                    "name": "Read",
                    "arguments": { "file_path": "src/lib.rs" },
                    "status": "in_progress"
                }
            })
        );
    }

    #[test]
    fn serializes_typed_delta_notifications() {
        let agent_delta = AgentMessageDeltaNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item_id: "msg-1".to_string(),
            delta: "hello".to_string(),
        };

        assert_eq!(
            serde_json::to_value(agent_delta).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "item_id": "msg-1",
                "delta": "hello"
            })
        );

        let reasoning_delta = ReasoningSummaryTextDeltaNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item_id: "thinking-1".to_string(),
            delta: "think".to_string(),
            summary_index: 0,
        };

        assert_eq!(
            serde_json::to_value(reasoning_delta).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "item_id": "thinking-1",
                "delta": "think",
                "summary_index": 0
            })
        );

        let command_delta = CommandExecutionOutputDeltaNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item_id: "cmd-1".to_string(),
            delta: "ok\n".to_string(),
        };

        assert_eq!(
            serde_json::to_value(command_delta).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "item_id": "cmd-1",
                "delta": "ok\n"
            })
        );
    }

    #[test]
    fn serializes_rich_item_completed_notification() {
        let notification = ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            completed_at_ms: 456,
            item: ThreadItem::CommandExecution {
                id: "cmd-1".to_string(),
                command: "pwd".to_string(),
                cwd: None,
                status: RichItemStatus::Completed,
                aggregated_output: Some("/tmp\n".to_string()),
                exit_code: None,
            },
        };

        assert_eq!(
            serde_json::to_value(notification).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "completed_at_ms": 456,
                "item": {
                    "type": "command_execution",
                    "id": "cmd-1",
                    "command": "pwd",
                    "status": "completed",
                    "aggregated_output": "/tmp\n"
                }
            })
        );
    }
}
