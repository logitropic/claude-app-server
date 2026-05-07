use serde::Serialize;
use serde_json::Value;

use crate::jsonrpc_lite::JSONRPCErrorError;
use crate::jsonrpc_lite::JSONRPCNotification;
use crate::jsonrpc_lite::server_notification;

pub const PARSE_ERROR_CODE: i64 = -32700;
pub const INVALID_REQUEST_ERROR_CODE: i64 = -32600;
pub const METHOD_NOT_FOUND_ERROR_CODE: i64 = -32601;
pub const INVALID_PARAMS_ERROR_CODE: i64 = -32602;
pub const INTERNAL_ERROR_CODE: i64 = -32603;
pub const NOT_INITIALIZED_ERROR_CODE: i64 = -32000;
pub const THREAD_NOT_FOUND_ERROR_CODE: i64 = -32001;
pub const TURN_BUSY_ERROR_CODE: i64 = -32003;
pub const NO_ACTIVE_TURN_ERROR_CODE: i64 = -32004;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct JSONRPCError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl JSONRPCError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(code: i64, message: impl Into<String>, data: Value) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data),
        }
    }

    pub fn into_error(self) -> JSONRPCErrorError {
        JSONRPCErrorError {
            code: self.code,
            message: self.message,
            data: self.data,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ServerNotification {
    Raw(JSONRPCNotification),
}

impl ServerNotification {
    pub fn new(method: impl Into<String>, params: impl Serialize) -> Self {
        Self::Raw(server_notification(method, params))
    }

    pub fn method(&self) -> &str {
        match self {
            Self::Raw(notification) => notification.method.as_str(),
        }
    }

    pub fn into_jsonrpc(self) -> JSONRPCNotification {
        match self {
            Self::Raw(notification) => notification,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    pub id: Value,
}
