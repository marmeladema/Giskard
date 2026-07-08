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
    Activity,
}

/// What kind of file change occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeKind {
    Created,
    Modified,
    Deleted,
}

/// One file touched by a finalized file-change item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChangeEntry {
    pub path: PathBuf,
    pub change: FileChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<String>,
}

/// Sent on `AgentEvent::ItemStarted` (spec §4.5, B5: renamed from `ItemStarted` to avoid
/// colliding with the `AgentEvent::ItemStarted` variant).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStart {
    /// Giskard-owned id (B2), stable across resume.
    pub id: ItemId,
    /// Harness-native item id, used to correlate deltas/completion.
    pub harness_item_id: String,
    pub kind: ItemKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandExecutionStart>,
}

/// Normalize a command-execution status string for comparison (lowercase, `-` → `_`).
///
/// Codex reports statuses like `inProgress` / `in_progress` / `in-progress`; normalizing here
/// keeps the running/terminal classification consistent across the harness, server registry, and
/// live-turn buffer.
pub fn normalized_command_status(status: &str) -> String {
    status.to_ascii_lowercase().replace('-', "_")
}

/// Returns true when a command-execution status string denotes a still-running command.
pub fn command_status_is_running(status: &str) -> bool {
    matches!(
        normalized_command_status(status).as_str(),
        "in_progress" | "inprogress" | "running"
    )
}

/// Command metadata available when a command item starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandExecutionStart {
    pub command: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<i64>,
}

/// The finalized item persisted in thread history and sent on `ItemCompleted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    /// Giskard-owned id (B2): stable across resume, addressable by the diff viewer and code overlay.
    pub id: ItemId,
    /// Harness-native item id (opaque; not relied on for stability).
    pub harness_item_id: String,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        process_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<i64>,
    },
    FileChange {
        /// Back-compat summary path for older persisted files and compact renderers.
        path: PathBuf,
        change: FileChangeKind,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changes: Vec<FileChangeEntry>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
    },
    ToolCall {
        name: String,
        input: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        server: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Activity {
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
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
            Self::Activity => "activity",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_payload_serde_roundtrip() {
        let item = Item {
            id: ItemId::new(),
            harness_item_id: "it_1".into(),
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
            status: Some("completed".into()),
            process_id: Some("proc_1".into()),
            duration_ms: Some(1250),
        };
        let json = serde_json::to_string(&payload).unwrap();
        let back: ItemPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(payload, back);
    }

    #[test]
    fn old_file_change_payload_deserializes() {
        let json = r#"{"kind":"file_change","path":"/tmp/a.rs","change":"modified"}"#;
        let back: ItemPayload = serde_json::from_str(json).unwrap();
        assert_eq!(
            back,
            ItemPayload::FileChange {
                path: PathBuf::from("/tmp/a.rs"),
                change: FileChangeKind::Modified,
                changes: vec![],
                status: None,
            }
        );
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
