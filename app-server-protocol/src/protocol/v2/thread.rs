use serde::Deserialize;
use serde::Serialize;

use super::turn::Turn;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermissionMode {
    #[serde(rename = "default")]
    #[default]
    Default,
    #[serde(rename = "acceptEdits")]
    AcceptEdits,
    #[serde(rename = "bypassPermissions")]
    BypassPermissions,
    #[serde(rename = "dontAsk")]
    DontAsk,
}

impl PermissionMode {
    pub fn as_claude_arg(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::AcceptEdits => "acceptEdits",
            Self::BypassPermissions => "bypassPermissions",
            Self::DontAsk => "dontAsk",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Thread {
    pub id: String,
    pub created_at: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<PermissionMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cli_session_id: Option<String>,
    #[serde(default)]
    pub turns: Vec<Turn>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadStartResponse {
    pub thread: ThreadStartThread,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadStartThread {
    pub id: String,
    pub created_at: u128,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadResumeResponse {
    pub thread: Thread,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadForkResponse {
    pub thread: ForkedThread,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ForkedThread {
    pub id: String,
    pub forked_from: String,
    pub created_at: u128,
}
