use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcCall {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    pub params: Value,
}

impl RpcCall {
    pub fn new(method: &str, params: &Value) -> Self {
        RpcCall {
            jsonrpc: "1.0".to_string(),
            id: "mapi-xbt".to_string(),
            method: method.to_string(),
            params: params.clone(),
        }
    }
}
