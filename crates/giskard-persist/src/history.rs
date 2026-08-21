//! The records of a format 2 thread directory.
//!
//! The split this file encodes: `history.jsonl` holds one **bounded** record per turn, and
//! `turns/<turn_id>.jsonl` holds everything whose size is a function of what the agent did. The
//! index is therefore strictly bounded per row except for the prompt preview and attachment
//! descriptors, which are human-scaled — never agent-driven. That is the property the split exists
//! to create, so nothing agent-driven may be added to a turn record later.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;

use giskard_core::PersistError;
use giskard_core::diff::FileDiff;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::Item;
use giskard_core::model::ModelRef;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{Mode, Turn, TurnStatus};
use giskard_core::user_input::{AttachmentKind, UserInput};

use crate::layout::{HISTORY_FORMAT, TURN_PAYLOAD_FORMAT};
use crate::preview::{PROMPT_PREVIEW_MAX_BYTES, STATUS_MESSAGE_MAX_BYTES, bounded_preview};

// ---- history.jsonl ----

/// First line of `history.jsonl`, written once and never touched again.
///
/// "Once" means at thread creation, with one fallback: a thread that reaches its first append
/// without having gone through `create_thread` gets the header prepended to that append. So a
/// format 2 directory can briefly exist with no index at all — between a first turn's payload write
/// and its index append — which is a state layout detection and the orphan sweep both have to
/// reason about, and neither may read as "this thread has no turns".
///
/// The layout version lives here rather than in `thread.json` because `thread.json` is rewritten on
/// every metadata mutation: a marker there would be rewritten hundreds of times per thread and
/// would have to stay consistent with the history file across a crash. A file that carries its own
/// format claim has no cross-file consistency to maintain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryHeader {
    pub format: u32,
    pub thread_id: ThreadId,
    pub created_at: DateTime<Utc>,
}

/// An attachment as the index records it: strictly bounded per attachment, human-scaled in count.
///
/// Attachment *bytes* have never been written to the history — `UserInput`'s persisted form drops
/// `data_base64` — so this is a descriptor of something materialised elsewhere, not a truncation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedAttachment {
    pub name: String,
    pub mime_type: String,
    pub size: u64,
    pub kind: AttachmentKind,
}

/// One bounded row of the history index.
///
/// Everything here is either strictly bounded (scalars, ids, timestamps) or human-scaled (the
/// capped preview, the attachment descriptors). No full prompt text, no `items`, no `diffs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnRecord {
    pub turn_id: TurnId,
    pub model: ModelRef,
    pub mode: Mode,
    /// The turn's outcome. `kind` is strictly bounded and authoritative — the ledger folds it, so
    /// aggregate repair must never have to open a payload file to learn it. `message` is **not**
    /// bounded at the source: it is composed from provider error text, which has no ceiling, so the
    /// copy here is capped to [`STATUS_MESSAGE_MAX_BYTES`] and is a display hint in the same
    /// category as [`Self::prompt_preview`]. The payload file holds what the harness reported.
    pub status: TurnStatus,
    pub usage: TokenUsage,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// The item count **as of this record** — a display hint, never a cross-file invariant.
    ///
    /// A late payload amendment may make this stale by adding a genuinely new item. Amendments do
    /// not touch the bounded index merely to refresh a display hint, and replacements normally
    /// preserve the count.
    ///
    /// Two rules keep it from becoming a consistency trap: never validate against it (a mismatch is
    /// logged and the payload file wins), and never let corruption detection depend on it.
    #[serde(default)]
    pub item_count: usize,
    /// A capped, derived, never-authoritative rendering of the prompt. The payload file holds the
    /// record. Never used for search, comparison, or validation.
    #[serde(default)]
    pub prompt_preview: String,
    /// Whether [`Self::prompt_preview`] dropped anything — what a renderer needs to decide whether
    /// to offer expansion.
    #[serde(default, skip_serializing_if = "is_false")]
    pub prompt_truncated: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<PersistedAttachment>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// A line of `history.jsonl`, tagged so an unknown `kind` can be skipped instead of being fatal.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HistoryLine<'a> {
    HistoryHeader(&'a HistoryHeader),
    Turn(&'a TurnRecord),
}

/// A line of `turns/<turn_id>.jsonl`.
///
/// `index` is **display order**, not identity, and it is carried explicitly because file order is
/// *write* order: the two diverge the moment anything is appended after the turn committed. A
/// command that settles late is necessarily appended at the end of the file, and folding by file
/// order would move it to the bottom of the turn; folding by `index` keeps it where it happened.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PayloadLine<'a> {
    TurnHeader { format: u32, turn_id: TurnId },
    UserInput { user_input: &'a UserInput },
    Status { status: &'a TurnStatus },
    Item { index: usize, item: &'a Item },
    Diff { index: usize, diff: &'a FileDiff },
}

#[derive(Debug, Clone, Deserialize)]
struct PayloadTurnHeader {
    format: u32,
    #[allow(dead_code)]
    turn_id: TurnId,
}

#[derive(Debug, Clone, Deserialize)]
struct PayloadUserInput {
    user_input: UserInput,
}

#[derive(Debug, Clone, Deserialize)]
struct PayloadStatus {
    status: TurnStatus,
}

/// `index` is optional on the read side so "absent" stays distinguishable from "0": a replacement
/// record carrying no index of its own keeps the slot its first record established, instead of
/// defaulting to zero and jumping to the top of the turn.
#[derive(Debug, Clone, Deserialize)]
struct PayloadItem {
    #[serde(default)]
    index: Option<usize>,
    item: Item,
}

#[derive(Debug, Clone, Deserialize)]
struct PayloadDiff {
    #[serde(default)]
    index: Option<usize>,
    diff: FileDiff,
}

/// The agent-driven half of a turn, as folded from one payload file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnPayload {
    /// Not optional: a payload without it is incomplete, and reassembling the turn with an empty
    /// prompt would be indistinguishable from a turn where the user genuinely submitted nothing.
    /// The parser rejects such a file, so this field cannot be absent by the time anyone holds one.
    pub user_input: UserInput,
    /// The status the harness reported, message and all. Absent from a payload written before the
    /// status moved here, in which case the index's capped copy is all there is.
    pub status: Option<TurnStatus>,
    pub items: Vec<Item>,
    pub diffs: Vec<FileDiff>,
}

impl HistoryHeader {
    pub fn new(thread_id: ThreadId, created_at: DateTime<Utc>) -> Self {
        Self {
            format: HISTORY_FORMAT,
            thread_id,
            created_at,
        }
    }

    /// The header line, newline included.
    pub fn line(&self) -> Result<String, PersistError> {
        line_of(&HistoryLine::HistoryHeader(self))
    }
}

impl TurnRecord {
    /// The bounded projection of a completed turn.
    pub fn from_turn(turn: &Turn) -> Self {
        let (prompt_preview, prompt_truncated) = turn
            .user_input
            .as_text()
            .map(|text| bounded_preview(text, PROMPT_PREVIEW_MAX_BYTES))
            .unwrap_or_default();
        Self {
            turn_id: turn.id,
            model: turn.model.clone(),
            mode: turn.mode,
            status: TurnStatus {
                kind: turn.status.kind,
                message: turn
                    .status
                    .message
                    .as_deref()
                    .map(|message| bounded_preview(message, STATUS_MESSAGE_MAX_BYTES).0),
            },
            usage: turn.usage,
            started_at: turn.started_at,
            completed_at: turn.completed_at,
            item_count: turn.items.len(),
            prompt_preview,
            prompt_truncated,
            attachments: turn
                .user_input
                .attachments()
                .iter()
                .map(|attachment| PersistedAttachment {
                    name: attachment.name.clone(),
                    mime_type: attachment.mime_type.clone(),
                    size: attachment.size,
                    kind: attachment.kind.clone(),
                })
                .collect(),
        }
    }

    /// The record line, newline included.
    pub fn line(&self) -> Result<String, PersistError> {
        line_of(&HistoryLine::Turn(self))
    }

    /// Reassemble the whole `Turn` this record indexes from its payload file.
    pub fn into_turn(self, payload: TurnPayload, payload_path: &Path) -> Turn {
        if payload.items.len() != self.item_count {
            // A hint, never an invariant: the payload file wins and the turn is still returned.
            // Still a warning, because by construction the two cannot disagree unless something
            // was lost.
            tracing::warn!(
                path = %payload_path.display(),
                turn_id = %self.turn_id,
                recorded = self.item_count,
                actual = payload.items.len(),
                "turn record item_count differs from its payload; using the payload"
            );
        }
        Turn {
            id: self.turn_id,
            user_input: payload.user_input,
            items: payload.items,
            model: self.model,
            mode: self.mode,
            // The payload holds what the harness reported; the record's copy is capped.
            status: payload.status.unwrap_or(self.status),
            usage: self.usage,
            diffs: payload.diffs,
            started_at: self.started_at,
            completed_at: self.completed_at,
        }
    }
}

fn line_of<T: Serialize>(value: &T) -> Result<String, PersistError> {
    let mut line =
        serde_json::to_string(value).map_err(|e| PersistError::Serialize(e.to_string()))?;
    line.push('\n');
    Ok(line)
}

/// Serialize a turn's initial whole payload file, header first.
///
/// This initial value is written with temp file + fsync + rename, so a committed turn starts with a
/// complete payload or no payload. Late completed-item replacements use the append protocol in the
/// store and can leave an ignored unterminated tail after an interrupted write.
pub fn payload_file_bytes(turn: &Turn) -> Result<Vec<u8>, PersistError> {
    let mut out = line_of(&PayloadLine::TurnHeader {
        format: TURN_PAYLOAD_FORMAT,
        turn_id: turn.id,
    })?;
    out.push_str(&line_of(&PayloadLine::UserInput {
        user_input: &turn.user_input,
    })?);
    out.push_str(&line_of(&PayloadLine::Status {
        status: &turn.status,
    })?);
    for (index, item) in turn.items.iter().enumerate() {
        out.push_str(&line_of(&PayloadLine::Item { index, item })?);
    }
    for (index, diff) in turn.diffs.iter().enumerate() {
        out.push_str(&line_of(&PayloadLine::Diff { index, diff })?);
    }
    Ok(out.into_bytes())
}

/// Serialize one ordinary item record, newline included.
///
/// Late item amendments use the exact same record shape as the initial payload. Keeping this
/// constructor beside [`payload_file_bytes`] prevents the amendment path from acquiring a second,
/// subtly different JSON representation.
pub(crate) fn payload_item_line(index: usize, item: &Item) -> Result<String, PersistError> {
    line_of(&PayloadLine::Item { index, item })
}

/// The `kind` of a JSONL record, or `None` when the line is not a tagged object.
fn record_kind(value: &serde_json::Value) -> Option<&str> {
    value.get("kind")?.as_str()
}

/// Parse the bounded history index.
///
/// Turn records fold by turn id, **first-wins**: a repeated id is a turn that is already durable,
/// and the first durable record stays authoritative. Payload amendments deliberately do not write
/// another index record, so silently preferring a later duplicate would only change how ambiguous
/// append retries resolve. The same first-wins rule is what the format 1 reader applies.
pub fn parse_history_index(path: &Path, data: &str) -> Result<Vec<TurnRecord>, PersistError> {
    let lines: Vec<&str> = data.lines().filter(|l| !l.trim().is_empty()).collect();
    let last = lines.len().saturating_sub(1);
    let mut records: Vec<TurnRecord> = Vec::with_capacity(lines.len());
    let mut seen: std::collections::HashSet<TurnId> = std::collections::HashSet::new();
    let mut header_seen = false;

    for (i, line) in lines.iter().enumerate() {
        let value: serde_json::Value = match serde_json::from_str(line) {
            Ok(value) => value,
            // Turn records are strictly bounded, so the index keeps the cheap `O_APPEND` write and
            // with it the tolerance for a single torn *final* line. A bad interior line is real
            // corruption.
            Err(e) if i == last => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "skipping torn final line in history index"
                );
                continue;
            }
            Err(e) => {
                return Err(PersistError::Corrupt(format!(
                    "{}: line {}: {}",
                    path.display(),
                    i + 1,
                    e
                )));
            }
        };

        match record_kind(&value) {
            Some("history_header") => {
                let header: HistoryHeader = serde_json::from_value(value).map_err(|e| {
                    PersistError::Corrupt(format!("{}: line {}: {}", path.display(), i + 1, e))
                })?;
                // Without the index there is nothing to partially recover, so a newer layout fails
                // the whole thread rather than silently reading it as something it is not.
                if header.format > HISTORY_FORMAT {
                    return Err(PersistError::Invalid(format!(
                        "{}: history format {} is newer than this build understands ({HISTORY_FORMAT})",
                        path.display(),
                        header.format
                    )));
                }
                header_seen = true;
            }
            Some("turn") => {
                let record: TurnRecord = match serde_json::from_value(value) {
                    Ok(record) => record,
                    Err(e) if i == last => {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "skipping torn final turn record in history index"
                        );
                        continue;
                    }
                    Err(e) => {
                        return Err(PersistError::Corrupt(format!(
                            "{}: line {}: {}",
                            path.display(),
                            i + 1,
                            e
                        )));
                    }
                };
                if !seen.insert(record.turn_id) {
                    tracing::warn!(
                        path = %path.display(),
                        turn_id = %record.turn_id,
                        line = i + 1,
                        "skipping duplicate turn id in history index"
                    );
                    continue;
                }
                records.push(record);
            }
            // A future downgrade must be non-destructive, so an unrecognised record is skipped
            // rather than failing a thread this build could otherwise read.
            other => {
                tracing::warn!(
                    path = %path.display(),
                    line = i + 1,
                    kind = other.unwrap_or("<untagged>"),
                    "skipping unknown record kind in history index"
                );
            }
        }
    }

    if !header_seen && !lines.is_empty() {
        tracing::warn!(
            path = %path.display(),
            "history index has no header; reading it as the current format"
        );
    }
    Ok(records)
}

/// Fold one payload file into the agent-driven half of a turn.
///
/// A payload file holds two classes of record, and each needs its own stated rule:
///
/// - **Collections** — `item` and `diff` — fold by **identity, last-wins**: `ItemId` for an item,
///   path for a diff. `index` answers a different question, "where does it render", and is not
///   identity: keying a collection by position would let two records at one index collide while the
///   same subject at two indices survived twice.
/// - **Singletons** — `turn_header`, `user_input`, `status` — fold **last-wins, with a warning**.
///   Last-wins keeps a damaged-but-readable file deterministic; the warning remains because the
///   amendment writer adds only ordinary `item` records, so no valid writer duplicates a singleton.
///
/// The history index folds turn records *first*-wins, which is not an inconsistency: that answers
/// "which turn record is authoritative when an append is retried", a question about the index's own
/// retry safety that does not arise inside a payload file.
pub fn parse_turn_payload(path: &Path, data: &str) -> Result<TurnPayload, PersistError> {
    parse_turn_payload_bytes(path, data.as_bytes())
}

/// Byte-oriented payload parser used by disk reads and amendments.
///
/// A torn append can stop inside a multi-byte UTF-8 character. Decoding the whole file would then
/// hide every earlier valid record, so record boundaries are found in bytes first and UTF-8 is
/// validated independently for each newline-terminated record.
pub fn parse_turn_payload_bytes(path: &Path, data: &[u8]) -> Result<TurnPayload, PersistError> {
    let mut user_input = None;
    let mut status = None;
    // Insertion order is preserved so a replacement that carries no index of its own keeps the slot
    // its first record established — "a late item that replaces one already present keeps its
    // original index", read from the other side.
    let mut items: Vec<(usize, Item)> = Vec::new();
    let mut item_slots: HashMap<ItemId, usize> = HashMap::new();
    let mut diffs: Vec<(usize, FileDiff)> = Vec::new();
    let mut diff_slots: HashMap<PathBuf, usize> = HashMap::new();
    let mut header_seen = false;

    /// Replace a singleton record, warning if one was already there.
    macro_rules! set_singleton {
        ($slot:ident, $value:expr, $line:expr) => {{
            if $slot.is_some() {
                tracing::warn!(
                    path = %path.display(),
                    line = $line,
                    record = stringify!($slot),
                    "duplicate singleton record in turn payload; the last one wins"
                );
            }
            $slot = Some($value);
        }};
    }

    /// Decode a known record without letting one malformed line hide the rest of the turn.
    macro_rules! payload_record {
        ($value:expr, $ty:ty, $kind:literal, $line:expr) => {{
            match serde_json::from_value::<$ty>($value) {
                Ok(record) => record,
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        line = $line,
                        kind = $kind,
                        %error,
                        "skipping malformed record in turn payload"
                    );
                    continue;
                }
            }
        }};
    }

    for (i, raw_line) in data.split_inclusive(|byte| *byte == b'\n').enumerate() {
        if !raw_line.ends_with(b"\n") {
            tracing::warn!(
                path = %path.display(),
                line = i + 1,
                "ignoring unterminated final record in turn payload"
            );
            break;
        }
        let line = &raw_line[..raw_line.len() - 1];
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let line = match std::str::from_utf8(line) {
            Ok(line) => line,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    line = i + 1,
                    %error,
                    "skipping non-UTF-8 record in turn payload"
                );
                continue;
            }
        };
        let value: serde_json::Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    line = i + 1,
                    %error,
                    "skipping malformed JSON record in turn payload"
                );
                continue;
            }
        };

        match record_kind(&value) {
            Some("turn_header") => {
                let header = payload_record!(value, PayloadTurnHeader, "turn_header", i + 1);
                // A payload newer than this build understands fails **that one turn**; the index
                // and every other turn stay readable. That containment is the point of per-file
                // headers.
                if header_seen {
                    tracing::warn!(
                        path = %path.display(),
                        line = i + 1,
                        record = "turn_header",
                        "duplicate singleton record in turn payload; the last one wins"
                    );
                }
                if header.format > TURN_PAYLOAD_FORMAT {
                    return Err(PersistError::Invalid(format!(
                        "{}: turn payload format {} is newer than this build understands ({TURN_PAYLOAD_FORMAT})",
                        path.display(),
                        header.format
                    )));
                }
                header_seen = true;
            }
            Some("user_input") => {
                let record = payload_record!(value, PayloadUserInput, "user_input", i + 1);
                set_singleton!(user_input, record.user_input, i + 1);
            }
            Some("status") => {
                let record = payload_record!(value, PayloadStatus, "status", i + 1);
                set_singleton!(status, record.status, i + 1);
            }
            Some("item") => {
                let record = payload_record!(value, PayloadItem, "item", i + 1);
                match item_slots.get(&record.item.id) {
                    Some(&slot) => {
                        if let Some(index) = record.index {
                            items[slot].0 = index;
                        }
                        items[slot].1 = record.item;
                    }
                    None => {
                        let index = record.index.unwrap_or(items.len());
                        item_slots.insert(record.item.id, items.len());
                        items.push((index, record.item));
                    }
                }
            }
            Some("diff") => {
                let record = payload_record!(value, PayloadDiff, "diff", i + 1);
                // Keyed by path, the only identity a `FileDiff` has. The same reasoning as items:
                // `index` is where it renders, not which diff it is.
                match diff_slots.get(&record.diff.path) {
                    Some(&slot) => {
                        if let Some(index) = record.index {
                            diffs[slot].0 = index;
                        }
                        diffs[slot].1 = record.diff;
                    }
                    None => {
                        let index = record.index.unwrap_or(diffs.len());
                        diff_slots.insert(record.diff.path.clone(), diffs.len());
                        diffs.push((index, record.diff));
                    }
                }
            }
            other => {
                tracing::warn!(
                    path = %path.display(),
                    line = i + 1,
                    kind = other.unwrap_or("<untagged>"),
                    "skipping unknown record kind in turn payload"
                );
            }
        }
    }

    if !header_seen {
        tracing::warn!(
            path = %path.display(),
            "turn payload has no header; reading it as the current format"
        );
    }

    items.sort_by_key(|(index, _)| *index);
    // After folding by item id the indices must be contiguous from 0. A gap means a record was
    // lost, and saying so beats silently rendering a shorter turn.
    for (position, (index, _)) in items.iter().enumerate() {
        if *index != position {
            tracing::warn!(
                path = %path.display(),
                expected = position,
                found = *index,
                "gap in turn payload item indices; a record appears to be missing"
            );
            break;
        }
    }

    diffs.sort_by_key(|(index, _)| *index);

    // A well-formed file that is missing a required record is still incomplete. Failing the turn
    // keeps it in the same class as a truncated payload; substituting an empty prompt would
    // manufacture a turn that reads as one the user submitted blank. `Invalid` rather than
    // `Corrupt` because the bytes parsed — the content is what is wrong — so the caller reports it
    // instead of quarantining a file whose surviving records may still be recoverable by hand.
    let Some(user_input) = user_input else {
        return Err(PersistError::Invalid(format!(
            "{}: turn payload has no user_input record",
            path.display()
        )));
    };

    Ok(TurnPayload {
        user_input,
        status,
        items: items.into_iter().map(|(_, item)| item).collect(),
        diffs: diffs.into_iter().map(|(_, diff)| diff).collect(),
    })
}

/// Read one turn's payload file.
///
/// `Ok(None)` means the file is absent — a turn record whose payload never landed, or was removed.
/// Malformed newline-terminated records and an unterminated final record are preserved and skipped
/// with a warning. A payload still fails its turn when no committed `user_input` record survives.
pub async fn read_turn_payload(path: &Path) -> Result<Option<TurnPayload>, PersistError> {
    let data = match fs::read(path).await {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(PersistError::Io(e.to_string())),
    };

    parse_turn_payload_bytes(path, &data).map(Some)
}
