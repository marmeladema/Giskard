use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

use crate::ids::{DiffId, ItemId};
use crate::item::FileChangeKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffContentKind {
    Unified,
    Structured,
}

/// Bounded projection of captured diff content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedDiffDescriptor {
    pub id: DiffId,
    pub path: PathBuf,
    pub change: FileChangeKind,
    pub content_kind: DiffContentKind,
    pub available: bool,
    /// UTF-8 bytes for unified text, or canonical JSON bytes for structured content.
    pub byte_size: u64,
    pub additions: u64,
    pub deletions: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub binary: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<ItemId>,
}

/// Full immutable content served only through the lazy diff endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CapturedDiffContent {
    Unified { text: String },
    Structured { diff: FileDiff },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedDiffRecord {
    pub id: DiffId,
    pub content: CapturedDiffContent,
}

pub fn capture_unified_diff(
    path: PathBuf,
    change: FileChangeKind,
    item_id: Option<ItemId>,
    text: String,
) -> (CapturedDiffDescriptor, CapturedDiffRecord) {
    let (additions, deletions) = unified_stats(&text);
    let byte_size = text.len() as u64;
    let content = CapturedDiffContent::Unified { text };
    let id = captured_diff_id(&path, change, &content);
    let descriptor = CapturedDiffDescriptor {
        id: id.clone(),
        path,
        change,
        content_kind: DiffContentKind::Unified,
        available: true,
        byte_size,
        additions,
        deletions,
        binary: false,
        item_id,
    };
    let record = CapturedDiffRecord { id, content };
    (descriptor, record)
}

pub fn capture_structured_diff(mut diff: FileDiff) -> (FileDiff, CapturedDiffRecord) {
    diff.captured = None;
    let (additions, deletions) = if diff.hunks.is_empty() && !diff.binary {
        (
            full_text_line_count(diff.new_text.as_deref()),
            full_text_line_count(diff.old_text.as_deref()),
        )
    } else {
        let additions = diff
            .hunks
            .iter()
            .flat_map(|hunk| &hunk.lines)
            .filter(|line| matches!(line, DiffLine::Added(_)))
            .count() as u64;
        let deletions = diff
            .hunks
            .iter()
            .flat_map(|hunk| &hunk.lines)
            .filter(|line| matches!(line, DiffLine::Removed(_)))
            .count() as u64;
        (additions, deletions)
    };
    let content = CapturedDiffContent::Structured { diff: diff.clone() };
    let canonical_content = canonical_content(&content);
    let content_bytes = serialized_bytes(&canonical_content);
    let byte_size = content_bytes.len() as u64;
    let id = captured_diff_id_from_content_bytes(&diff.path, diff.change, &content_bytes);
    let descriptor = CapturedDiffDescriptor {
        id: id.clone(),
        path: diff.path.clone(),
        change: diff.change,
        content_kind: DiffContentKind::Structured,
        available: true,
        byte_size,
        additions,
        deletions,
        binary: diff.binary,
        item_id: None,
    };
    let projected = FileDiff {
        path: diff.path,
        change: diff.change,
        old_text: None,
        new_text: None,
        hunks: Vec::new(),
        binary: diff.binary,
        captured: Some(descriptor),
    };
    (projected, CapturedDiffRecord { id, content })
}

fn full_text_line_count(text: Option<&str>) -> u64 {
    let Some(text) = text else {
        return 0;
    };
    let without_final_newline = text
        .strip_suffix("\r\n")
        .or_else(|| text.strip_suffix('\n'))
        .unwrap_or(text);
    if without_final_newline.is_empty() {
        return 0;
    }
    without_final_newline.split('\n').count() as u64
}

/// Stable identity for the canonical, complete captured representation.
pub fn captured_diff_id(
    path: &std::path::Path,
    change: FileChangeKind,
    content: &CapturedDiffContent,
) -> DiffId {
    let normalized_content = canonical_content(content);
    let content_bytes = serialized_bytes(&normalized_content);
    captured_diff_id_from_content_bytes(path, change, &content_bytes)
}

fn captured_diff_id_from_content_bytes(
    path: &std::path::Path,
    change: FileChangeKind,
    content_bytes: &[u8],
) -> DiffId {
    let normalized_path = path.to_string_lossy();
    let mut digest = Sha256::new();
    // This is the exact field order and encoding produced by serializing the canonical identity
    // struct. Feeding the already-serialized content avoids encoding a large structured diff twice.
    digest.update(b"{\"path\":");
    digest.update(serialized_bytes(&normalized_path));
    digest.update(b",\"change\":");
    digest.update(serialized_bytes(&change));
    digest.update(b",\"content\":");
    digest.update(content_bytes);
    digest.update(b"}");
    DiffId::from_digest(format!("sha256_{:x}", digest.finalize()))
}

fn canonical_content(content: &CapturedDiffContent) -> CapturedDiffContent {
    let mut normalized = content.clone();
    if let CapturedDiffContent::Structured { diff } = &mut normalized {
        diff.path = diff.path.to_string_lossy().into_owned().into();
        diff.captured = None;
    }
    normalized
}

fn serialized_bytes(value: &impl Serialize) -> Vec<u8> {
    // Canonical paths have already been normalized to UTF-8 and these domain types otherwise
    // contain only JSON-supported strings, integers, booleans, sequences, structs, and enums.
    serde_json::to_vec(value).expect("captured diff domain types always serialize as JSON")
}

fn unified_stats(text: &str) -> (u64, u64) {
    text.lines().fold((0, 0), |(additions, deletions), line| {
        if line.starts_with('+') && !line.starts_with("+++") {
            (additions + 1, deletions)
        } else if line.starts_with('-') && !line.starts_with("---") {
            (additions, deletions + 1)
        } else {
            (additions, deletions)
        }
    })
}

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
    /// Present after the server extracts the full body into turn-owned lazy storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured: Option<CapturedDiffDescriptor>,
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

    fn structured_diff() -> FileDiff {
        FileDiff {
            path: "/src/main.rs".into(),
            change: FileChangeKind::Modified,
            old_text: None,
            new_text: None,
            hunks: Vec::new(),
            binary: false,
            captured: None,
        }
    }

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
            captured: None,
        };
        let json = serde_json::to_string(&diff).unwrap();
        let back: FileDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(diff, back);
    }

    #[test]
    fn structured_byte_size_includes_hunk_only_content() {
        let mut diff = structured_diff();
        diff.hunks.push(DiffHunk {
            old_start: 1,
            old_lines: 1,
            new_start: 1,
            new_lines: 1,
            lines: vec![
                DiffLine::Removed("before".into()),
                DiffLine::Added("after".into()),
            ],
        });

        let (projected, record) = capture_structured_diff(diff);
        let descriptor = projected.captured.as_ref().unwrap();
        assert_eq!(
            descriptor.byte_size,
            serialized_bytes(&record.content).len() as u64
        );
        assert!(descriptor.byte_size > 0);
    }

    #[test]
    fn structured_byte_size_counts_unicode_utf8_bytes() {
        let mut ascii = structured_diff();
        ascii.old_text = Some("a".into());
        ascii.new_text = Some("b".into());
        let mut unicode = ascii.clone();
        unicode.old_text = Some("é".into());
        unicode.new_text = Some("🐍".into());

        let (ascii_projected, _) = capture_structured_diff(ascii);
        let (unicode_projected, unicode_record) = capture_structured_diff(unicode);
        let ascii_size = ascii_projected.captured.as_ref().unwrap().byte_size;
        let unicode_size = unicode_projected.captured.as_ref().unwrap().byte_size;

        assert_eq!(
            unicode_size,
            serialized_bytes(&unicode_record.content).len() as u64
        );
        assert!(unicode_size > ascii_size);
    }

    #[test]
    fn empty_structured_diff_has_serialized_byte_size() {
        let (projected, record) = capture_structured_diff(structured_diff());
        assert_eq!(
            projected.captured.as_ref().unwrap().byte_size,
            serialized_bytes(&record.content).len() as u64
        );
    }

    #[test]
    fn full_text_only_structured_diff_stats_match_rendered_lines() {
        let cases = [
            ("created", None, Some("one\ntwo\n"), 2, 0),
            ("deleted", Some("one\ntwo"), None, 0, 2),
            ("modified", Some("before\n"), Some("after\nnext"), 2, 1),
            ("empty", Some(""), Some(""), 0, 0),
            (
                "crlf",
                Some("before\r\nsecond\r\n"),
                Some("after\r\n"),
                1,
                2,
            ),
            ("unicode", Some("旧\n"), Some("新\n✅\n"), 2, 1),
            (
                "trailing newline",
                Some("one\ntwo\n"),
                Some("three\n"),
                1,
                2,
            ),
            ("only newline", Some("\r\n"), Some("\n"), 0, 0),
            ("standalone carriage return", Some("\r"), Some("\r"), 1, 1),
        ];

        for (name, old_text, new_text, additions, deletions) in cases {
            let mut diff = structured_diff();
            diff.old_text = old_text.map(str::to_owned);
            diff.new_text = new_text.map(str::to_owned);

            let (projected, _) = capture_structured_diff(diff);
            let descriptor = projected.captured.as_ref().unwrap();
            assert_eq!(descriptor.additions, additions, "{name} additions");
            assert_eq!(descriptor.deletions, deletions, "{name} deletions");
        }
    }

    #[test]
    fn binary_full_text_does_not_report_text_stats() {
        let mut diff = structured_diff();
        diff.old_text = Some("before\n".into());
        diff.new_text = Some("after\n".into());
        diff.binary = true;

        let (projected, _) = capture_structured_diff(diff);
        let descriptor = projected.captured.as_ref().unwrap();
        assert_eq!((descriptor.additions, descriptor.deletions), (0, 0));
    }

    #[test]
    fn structured_id_distinguishes_hunk_partitioning() {
        let mut one_hunk = structured_diff();
        one_hunk.hunks = vec![DiffHunk {
            old_start: 1,
            old_lines: 0,
            new_start: 1,
            new_lines: 2,
            lines: vec![DiffLine::Added("a".into()), DiffLine::Added("b".into())],
        }];
        let mut two_hunks = structured_diff();
        two_hunks.hunks = vec![
            DiffHunk {
                old_start: 1,
                old_lines: 0,
                new_start: 1,
                new_lines: 1,
                lines: vec![DiffLine::Added("a".into())],
            },
            DiffHunk {
                old_start: 1,
                old_lines: 0,
                new_start: 2,
                new_lines: 1,
                lines: vec![DiffLine::Added("b".into())],
            },
        ];

        let one_content = CapturedDiffContent::Structured { diff: one_hunk };
        let two_content = CapturedDiffContent::Structured { diff: two_hunks };
        assert_ne!(
            captured_diff_id(
                std::path::Path::new("/src/main.rs"),
                FileChangeKind::Modified,
                &one_content
            ),
            captured_diff_id(
                std::path::Path::new("/src/main.rs"),
                FileChangeKind::Modified,
                &two_content
            )
        );
    }

    #[test]
    fn structured_id_ignores_existing_capture_descriptor() {
        let diff = structured_diff();
        let (_, record) = capture_structured_diff(diff.clone());
        let mut with_descriptor = diff;
        with_descriptor.captured = Some(CapturedDiffDescriptor {
            id: DiffId::from_digest("sha256_placeholder"),
            path: "/different".into(),
            change: FileChangeKind::Created,
            content_kind: DiffContentKind::Unified,
            available: false,
            byte_size: 999,
            additions: 99,
            deletions: 99,
            binary: true,
            item_id: None,
        });
        let content = CapturedDiffContent::Structured {
            diff: with_descriptor,
        };

        assert_eq!(
            captured_diff_id(
                std::path::Path::new("/src/main.rs"),
                FileChangeKind::Modified,
                &record.content
            ),
            captured_diff_id(
                std::path::Path::new("/src/main.rs"),
                FileChangeKind::Modified,
                &content
            )
        );
    }
}
