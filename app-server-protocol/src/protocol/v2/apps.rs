use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AppListResponse {
    pub apps: Vec<Value>,
}
