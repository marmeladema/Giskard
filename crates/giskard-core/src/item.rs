use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::diff::CapturedDiffDescriptor;
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
    /// Present after the server extracts `diff` into turn-owned lazy storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_diff: Option<CapturedDiffDescriptor>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolCallStart>,
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

/// Tool-call metadata available when a tool item starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallStart {
    pub name: String,
    pub input: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent: Option<SubagentLink>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<i64>,
}

/// Harness-neutral link from a transcript item to a related agent thread.
///
/// The owning thread and referenced thread may be related in either direction. Harness events do
/// not always state that direction, so the registry resolves it from authoritative persisted
/// ownership before materializing a child or navigating to a parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubagentLink {
    /// Harness-native thread id of the related agent thread.
    pub harness_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Initial task prompt used to start the child thread, when the harness exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_prompt: Option<String>,
    pub action: SubagentAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SubagentStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubagentAction {
    Spawned,
    Started,
    Interacted,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubagentStatus {
    Pending,
    Running,
    Completed,
    Interrupted,
    Failed,
    Shutdown,
    NotFound,
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
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        output_truncated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_original_bytes: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_original_lines: Option<u64>,
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
        metadata: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent: Option<SubagentLink>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Activity {
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent: Option<SubagentLink>,
    },
}

/// Bounded projection of completed command output used by browser-facing protocols.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandOutputDescriptor {
    pub preview: String,
    pub preview_truncated: bool,
    pub durable_truncated: bool,
    pub original_bytes: u64,
    pub original_lines: u64,
    pub durable_bytes: u64,
    pub durable_lines: u64,
    pub preview_bytes: u64,
    pub preview_lines: u64,
    pub output_available: bool,
}

impl CommandOutputDescriptor {
    pub const PREVIEW_MAX_BYTES: usize = 8 * 1024;

    /// Project durable output into the bounded tail descriptor shared by every wire path.
    pub fn from_durable(
        output: &str,
        durable_truncated: bool,
        original_bytes: u64,
        original_lines: u64,
        output_available: bool,
    ) -> Self {
        let (preview, preview_truncated) =
            command_output_tail_preview(output, original_bytes, Self::PREVIEW_MAX_BYTES);
        Self {
            preview_bytes: preview.len() as u64,
            preview_lines: command_output_logical_lines(&preview),
            durable_bytes: output.len() as u64,
            durable_lines: command_output_logical_lines(output),
            original_bytes,
            original_lines,
            preview,
            preview_truncated,
            durable_truncated,
            output_available,
        }
    }
}

/// Resolve authoritative command-output counts from a durable representation.
///
/// Complete output is authoritative, so redundant persisted metadata is ignored. For truncated
/// output, callers must validate that both supplied counts are present before using this helper.
pub fn resolve_command_output_counts(
    output: &str,
    output_truncated: bool,
    output_original_bytes: Option<u64>,
    output_original_lines: Option<u64>,
) -> (u64, u64) {
    if output_truncated {
        (
            output_original_bytes.unwrap_or(output.len() as u64),
            output_original_lines.unwrap_or_else(|| command_output_logical_lines(output)),
        )
    } else {
        (output.len() as u64, command_output_logical_lines(output))
    }
}

pub fn command_output_logical_lines(text: &str) -> u64 {
    if text.is_empty() {
        0
    } else {
        text.bytes().filter(|byte| *byte == b'\n').count() as u64 + u64::from(!text.ends_with('\n'))
    }
}

pub fn command_output_tail_preview(
    text: &str,
    original_bytes: u64,
    max_bytes: usize,
) -> (String, bool) {
    if original_bytes == text.len() as u64 && text.len() <= max_bytes {
        return (text.to_owned(), false);
    }
    let marker = |omitted| format!("[… {omitted} bytes omitted from command output preview …]\n");
    let mut omitted = original_bytes;
    loop {
        let prefix = marker(omitted);
        let budget = max_bytes.saturating_sub(prefix.len());
        let mut start = text.len().saturating_sub(budget);
        while !text.is_char_boundary(start) {
            start += 1;
        }
        let actual = original_bytes.saturating_sub((text.len() - start) as u64);
        if actual == omitted {
            return (format!("{prefix}{}", &text[start..]), true);
        }
        omitted = actual;
    }
}

/// The complete result of normalizing provider command output at the ingestion boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedCommandOutput {
    pub output: String,
    pub output_truncated: bool,
    pub output_original_bytes: Option<u64>,
    pub output_original_lines: Option<u64>,
    pub descriptor: CommandOutputDescriptor,
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
            output_truncated: false,
            output_original_bytes: None,
            output_original_lines: None,
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
    fn legacy_command_execution_defaults_retention_metadata() {
        let json =
            r#"{"kind":"command_execution","command":"echo ok","cwd":"/tmp","output":"ok\n"}"#;
        let payload: ItemPayload = serde_json::from_str(json).unwrap();
        assert!(matches!(
            payload,
            ItemPayload::CommandExecution {
                output_truncated: false,
                output_original_bytes: None,
                output_original_lines: None,
                ..
            }
        ));
    }

    #[test]
    fn item_start_tool_metadata_is_optional_and_roundtrips() {
        let minimal =
            r#"{"id":"01ARYZ6S41TSV4RRFFQ69G5FAV","harness_item_id":"tool_1","kind":"tool_call"}"#;
        let back: ItemStart = serde_json::from_str(minimal).unwrap();
        assert_eq!(back.tool, None);

        let start = ItemStart {
            id: ItemId::new(),
            harness_item_id: "tool_1".into(),
            kind: ItemKind::ToolCall,
            command: None,
            tool: Some(ToolCallStart {
                name: "jira_search".into(),
                input: serde_json::json!({ "jql": "project = ERE" }),
                server: Some("cf-tools".into()),
                status: Some("in_progress".into()),
                metadata: None,
                subagent: None,
                started_at_ms: Some(1_700_000_000_000),
            }),
        };
        let json = serde_json::to_string(&start).unwrap();
        let back: ItemStart = serde_json::from_str(&json).unwrap();
        assert_eq!(start, back);
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
