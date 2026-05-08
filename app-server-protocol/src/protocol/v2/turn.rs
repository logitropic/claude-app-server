use serde::Serialize;
use serde_json::Value;

use super::item::StoredItem;
use super::item::ThreadItem;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn: Option<Turn>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn: Option<Turn>,
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

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPlanStepStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnPlanStep {
    pub step: String,
    pub status: TurnPlanStepStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnPlanUpdatedNotification {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explanation: Option<String>,
    pub plan: Vec<TurnPlanStep>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HookStartedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub hook_id: String,
    pub hook_name: String,
    pub hook_event: String,
    pub started_at_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HookCompletedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub hook_id: String,
    pub hook_name: String,
    pub hook_event: String,
    pub outcome: String,
    pub output: String,
    pub stdout: String,
    pub stderr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub completed_at_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RichTurn {
    pub id: String,
    pub thread_id: String,
    pub status: TurnStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<ThreadItem>,
    pub created_at: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_plan_update_notification() {
        let notification = TurnPlanUpdatedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            explanation: None,
            plan: vec![TurnPlanStep {
                step: "Implement events".to_string(),
                status: TurnPlanStepStatus::InProgress,
            }],
        };

        assert_eq!(
            serde_json::to_value(notification).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "plan": [
                    { "step": "Implement events", "status": "in_progress" }
                ]
            })
        );
    }

    #[test]
    fn serializes_hook_completed_notification() {
        let started = HookStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            hook_id: "hook-1".to_string(),
            hook_name: "PreToolUse:Bash".to_string(),
            hook_event: "PreToolUse".to_string(),
            started_at_ms: 123,
        };

        assert_eq!(
            serde_json::to_value(started).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "hook_id": "hook-1",
                "hook_name": "PreToolUse:Bash",
                "hook_event": "PreToolUse",
                "started_at_ms": 123
            })
        );

        let notification = HookCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            hook_id: "hook-1".to_string(),
            hook_name: "PostToolUse:Edit".to_string(),
            hook_event: "PostToolUse".to_string(),
            outcome: "success".to_string(),
            output: "ok".to_string(),
            stdout: "ok".to_string(),
            stderr: String::new(),
            exit_code: Some(0),
            completed_at_ms: 456,
        };

        assert_eq!(
            serde_json::to_value(notification).unwrap(),
            json!({
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "hook_id": "hook-1",
                "hook_name": "PostToolUse:Edit",
                "hook_event": "PostToolUse",
                "outcome": "success",
                "output": "ok",
                "stdout": "ok",
                "stderr": "",
                "exit_code": 0,
                "completed_at_ms": 456
            })
        );
    }
}
