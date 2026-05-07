use serde_json::json;

use super::BUILTIN_SKILLS;
use claude_app_server_protocol::AVAILABLE_MODELS;

pub const SERVER_NAME: &str = "claude-app-server";
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn initialize_response() -> serde_json::Value {
    json!({
        "server": {
            "name": SERVER_NAME,
            "version": SERVER_VERSION,
        },
        "capabilities": {
            "threads": ["start", "resume", "fork"],
            "turns": ["start", "steer", "interrupt"],
            "models": AVAILABLE_MODELS.iter().map(|model| model.id).collect::<Vec<_>>(),
            "skills": BUILTIN_SKILLS.iter().map(|skill| skill.name).collect::<Vec<_>>(),
        },
    })
}
