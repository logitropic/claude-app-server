use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

pub type RequestId = Value;
pub type Result = Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JSONRPCMessage {
    Request(JSONRPCRequest),
    Notification(JSONRPCNotification),
    Response(JSONRPCResponse),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JSONRPCRequest {
    pub jsonrpc: String,
    pub id: RequestId,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JSONRPCNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JSONRPCResponse {
    pub jsonrpc: String,
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JSONRPCErrorError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JSONRPCErrorError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JSONRPCResponse {
    pub fn success(id: RequestId, result: impl Serialize) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(serde_json::to_value(result).unwrap_or(Value::Null)),
            error: None,
        }
    }

    pub fn error(id: RequestId, error: JSONRPCErrorError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

pub fn server_notification(
    method: impl Into<String>,
    params: impl Serialize,
) -> JSONRPCNotification {
    JSONRPCNotification {
        jsonrpc: "2.0".to_string(),
        method: method.into(),
        params: Some(serde_json::to_value(params).unwrap_or(Value::Null)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jsonrpc_request() {
        let msg: JSONRPCMessage =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)
                .unwrap();
        assert!(matches!(msg, JSONRPCMessage::Request(_)));
    }
}
