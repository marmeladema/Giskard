use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::ids::ItemId;

/// Kind of item — discriminant only; payload fills in on completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemKind {
    UserMessage,
    AgentMessage,
    Reasoning,
    CommandExecution,
    FileChange,
    ToolCall,
}

/// What kind of file change occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeKind {
    Created,
    Modified,
    Deleted,
}

/// Sent on `AgentEvent::ItemStarted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStarted {
    pub id: ItemId,
    pub kind: ItemKind,
}

/// The finalized item persisted in thread history and sent on `ItemCompleted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub id: ItemId,
    pub payload: ItemPayload,
    pub created_at: DateTime<Utc>,
}

/// Discriminated union of item payloads (spec §4.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ItemPayload {
    UserMessage {
        text: String,
    },
    AgentMessage {
        text: String,
    },
    Reasoning {
        text: String,
    },
    CommandExecution {
        command: String,
        cwd: PathBuf,
        output: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
    FileChange {
        path: PathBuf,
        change: FileChangeKind,
    },
    ToolCall {
        name: String,
        input: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<serde_json::Value>,
    },
}

/// Incremental delta streamed during an item's lifecycle (spec §4.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ItemDelta {
    Text { text: String },
    CommandOutput { chunk: String },
}

impl ItemKind {
    /// Returns the matching `ItemPayload` discriminant.
    pub fn as_payload_kind(&self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::AgentMessage => "agent_message",
            Self::Reasoning => "reasoning",
            Self::CommandExecution => "command_execution",
            Self::FileChange => "file_change",
            Self::ToolCall => "tool_call",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_payload_serde_roundtrip() {
        let item = Item {
            id: ItemId("it_1".into()),
            payload: ItemPayload::AgentMessage {
                text: "Hello!".into(),
            },
            created_at: DateTime::parse_from_rfc3339("2026-07-06T10:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        };
        let json = serde_json::to_string(&item).unwrap();
        let back: Item = serde_json::from_str(&json).unwrap();
        assert_eq!(item, back);
    }

    #[test]
    fn command_execution_serde() {
        let payload = ItemPayload::CommandExecution {
            command: "cargo test".into(),
            cwd: "/tmp/project".into(),
            output: "all passed".into(),
            exit_code: Some(0),
        };
        let json = serde_json::to_string(&payload).unwrap();
        let back: ItemPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(payload, back);
    }

    #[test]
    fn item_delta_text_serde() {
        let delta = ItemDelta::Text {
            text: "Hello".into(),
        };
        let json = serde_json::to_string(&delta).unwrap();
        assert!(json.contains("\"type\":\"text\""));
    }
}
