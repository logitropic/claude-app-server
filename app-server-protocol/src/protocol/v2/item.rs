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
#[serde(rename_all = "camelCase")]
pub struct StoredItem {
    pub id: String,
    pub created_at: u128,
    #[serde(flatten)]
    pub item: Item,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserInput {
    Text { text: String },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DynamicToolCallStatus {
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DynamicToolCallOutputContentItem {
    InputText { text: String },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ThreadItem {
    UserMessage {
        id: String,
        content: Vec<UserInput>,
    },
    AgentMessage {
        id: String,
        text: String,
        #[serde(default)]
        phase: Option<Value>,
        #[serde(default)]
        memory_citation: Option<Value>,
    },
    Reasoning {
        id: String,
        #[serde(default)]
        summary: Vec<String>,
        #[serde(default)]
        content: Vec<String>,
    },
    DynamicToolCall {
        id: String,
        namespace: Option<String>,
        tool: String,
        arguments: Value,
        status: DynamicToolCallStatus,
        content_items: Option<Vec<DynamicToolCallOutputContentItem>>,
        success: Option<bool>,
        duration_ms: Option<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemStartedNotification {
    pub item: ThreadItem,
    pub thread_id: String,
    pub turn_id: String,
    pub started_at_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemCompletedNotification {
    pub item: ThreadItem,
    pub thread_id: String,
    pub turn_id: String,
    pub completed_at_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentMessageDeltaNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningTextDeltaNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
    pub content_index: i64,
}

impl StoredItem {
    pub fn to_thread_item(&self) -> ThreadItem {
        match &self.item {
            Item::Text { text } => ThreadItem::AgentMessage {
                id: self.id.clone(),
                text: text.clone(),
                phase: None,
                memory_citation: None,
            },
            Item::Thinking { thinking } => ThreadItem::Reasoning {
                id: self.id.clone(),
                summary: Vec::new(),
                content: vec![thinking.clone()],
            },
            Item::ToolCall {
                tool_use_id: _,
                name,
                input,
            } => ThreadItem::DynamicToolCall {
                id: self.id.clone(),
                namespace: None,
                tool: name.clone(),
                arguments: input.clone(),
                status: DynamicToolCallStatus::InProgress,
                content_items: None,
                success: None,
                duration_ms: None,
            },
            Item::ToolResult {
                tool_use_id,
                content,
                is_error,
            } => ThreadItem::DynamicToolCall {
                id: self.id.clone(),
                namespace: None,
                tool: tool_use_id.clone(),
                arguments: Value::Null,
                status: if *is_error {
                    DynamicToolCallStatus::Failed
                } else {
                    DynamicToolCallStatus::Completed
                },
                content_items: Some(vec![DynamicToolCallOutputContentItem::InputText {
                    text: content.clone(),
                }]),
                success: Some(!*is_error),
                duration_ms: None,
            },
        }
    }
}
