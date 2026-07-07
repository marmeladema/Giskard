use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::item::FileChangeKind;

/// A structured file diff for the side-by-side viewer (spec §11.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDiff {
    pub path: PathBuf,
    pub change: FileChangeKind,
    /// None for created files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_text: Option<String>,
    /// None for deleted files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_text: Option<String>,
    /// Precomputed hunks for rendering; may be empty if full-text only.
    #[serde(default)]
    pub hunks: Vec<DiffHunk>,
    #[serde(default)]
    pub binary: bool,
}

/// A single diff hunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffHunk {
    pub old_start: u32,
    pub old_lines: u32,
    pub new_start: u32,
    pub new_lines: u32,
    pub lines: Vec<DiffLine>,
}

/// A single line within a hunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "text", rename_all = "lowercase")]
pub enum DiffLine {
    Context(String),
    Added(String),
    Removed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_diff_roundtrip() {
        let diff = FileDiff {
            path: "/src/main.rs".into(),
            change: FileChangeKind::Modified,
            old_text: Some("old".into()),
            new_text: Some("new".into()),
            hunks: vec![DiffHunk {
                old_start: 1,
                old_lines: 1,
                new_start: 1,
                new_lines: 1,
                lines: vec![
                    DiffLine::Removed("old".into()),
                    DiffLine::Added("new".into()),
                ],
            }],
            binary: false,
        };
        let json = serde_json::to_string(&diff).unwrap();
        let back: FileDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(diff, back);
    }
}
