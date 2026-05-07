use serde::Serialize;
use serde_json::Value;

use super::item::StoredItem;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Active,
    Completed,
    Interrupted,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Turn {
    pub id: String,
    pub thread_id: String,
    pub status: TurnStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<StoredItem>,
    pub created_at: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnStartResponse {
    pub turn: TurnStartTurn,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnStartTurn {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnStartedNotification {
    pub turn_id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnCompletedNotification {
    pub turn_id: String,
    pub thread_id: String,
    pub status: TurnStatus,
    pub items_count: usize,
    pub completed_at: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnFailedNotification {
    pub turn_id: String,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UsageUpdateNotification {
    pub turn_id: String,
    pub thread_id: String,
    pub usage: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnPermissionDeniedNotification {
    pub turn_id: String,
    pub thread_id: String,
    pub denials: Value,
}
