use claude_app_server_protocol::AppListResponse;
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Serialize)]
pub struct BuiltinSkill {
    pub name: &'static str,
    pub description: &'static str,
    pub required: &'static [&'static str],
}

pub const BUILTIN_SKILLS: &[BuiltinSkill] = &[
    BuiltinSkill {
        name: "Read",
        description: "Read the contents of a file.",
        required: &["file_path"],
    },
    BuiltinSkill {
        name: "Write",
        description: "Create a new file with specified content.",
        required: &["file_path", "content"],
    },
    BuiltinSkill {
        name: "Edit",
        description: "Make targeted edits to an existing file.",
        required: &["file_path", "old_string", "new_string"],
    },
    BuiltinSkill {
        name: "Bash",
        description: "Execute a shell command in the working directory.",
        required: &["command"],
    },
    BuiltinSkill {
        name: "Glob",
        description: "Find files matching a glob pattern.",
        required: &["pattern"],
    },
    BuiltinSkill {
        name: "Grep",
        description: "Search file contents using regex.",
        required: &["pattern"],
    },
    BuiltinSkill {
        name: "WebFetch",
        description: "Fetch and analyze the contents of a URL.",
        required: &["url", "prompt"],
    },
    BuiltinSkill {
        name: "WebSearch",
        description: "Search the web.",
        required: &["query"],
    },
    BuiltinSkill {
        name: "Task",
        description: "Spawn a sub-agent to handle a parallel task.",
        required: &["description", "prompt"],
    },
];

pub fn skills_list_response() -> serde_json::Value {
    let skills: Vec<_> = BUILTIN_SKILLS
        .iter()
        .map(|skill| {
            json!({
                "name": skill.name,
                "description": skill.description,
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": skill.required,
                },
            })
        })
        .collect();
    json!({ "skills": skills })
}

pub fn app_list_response() -> AppListResponse {
    AppListResponse { apps: Vec::new() }
}
