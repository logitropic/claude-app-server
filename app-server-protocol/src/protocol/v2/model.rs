use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Model {
    pub id: &'static str,
    pub name: &'static str,
    pub aliases: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModelListResponse {
    pub models: Vec<Model>,
}

pub const AVAILABLE_MODELS: &[Model] = &[
    Model {
        id: "claude-opus-4-6",
        name: "Claude Opus 4.6",
        aliases: &["opus"],
    },
    Model {
        id: "claude-sonnet-4-6",
        name: "Claude Sonnet 4.6",
        aliases: &["sonnet"],
    },
    Model {
        id: "claude-haiku-4-5",
        name: "Claude Haiku 4.5",
        aliases: &["haiku"],
    },
];
