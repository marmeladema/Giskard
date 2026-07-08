use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::ServerRequestId;

/// Harness-neutral representation of a server-initiated request that needs a browser response.
///
/// The method and params are intentionally preserved as protocol-shaped JSON. Codex evolves this
/// surface faster than Giskard's domain model, so the UI can add first-class handling for known
/// methods while still exposing unknown requests without blocking the harness forever.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerRequest {
    pub id: ServerRequestId,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerRequestResponse {
    Result { value: Value },
    Error { code: i64, message: String },
}

impl ServerRequestResponse {
    pub fn result(value: Value) -> Self {
        Self::Result { value }
    }

    pub fn error(code: i64, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
        }
    }
}
