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
