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
use giskard_core::diff::{CapturedDiffContent, CapturedDiffRecord, FileDiff};
use giskard_core::ids::{DiffId, ItemId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
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
    /// Nothing in this format appends to a payload file after its turn commits, so today the count
    /// cannot go stale. The semantics are pinned now so the scheduled amendment work inherits a
    /// field that already means something: a superseding turn record carries the current count, and
    /// no payload append is forced to touch the index just to keep a counter honest.
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
    pub diff_contents: HashMap<DiffId, CapturedDiffContent>,
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

/// Serialize a turn's whole payload file, header first.
///
/// Written with temp file + fsync + rename, so a payload is complete or absent — never the torn
/// half-line a shared append-only file can leave behind.
pub fn payload_file_bytes(turn: &Turn) -> Result<Vec<u8>, PersistError> {
    payload_file_bytes_with_diffs(turn, &[])
}

pub fn payload_file_bytes_with_diffs(
    turn: &Turn,
    captured: &[CapturedDiffRecord],
) -> Result<Vec<u8>, PersistError> {
    let projected = turn_with_inline_diffs(turn, captured)?;
    let turn = &projected;
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

/// Reconstruct the unchanged format-1 representation from a bounded runtime projection.
///
/// Both supported storage layouts persist full diff bodies inline. Keeping the reconstruction in
/// one place prevents the degraded flat-layout append path from accidentally persisting only the
/// descriptors that are meant for runtime and browser delivery.
pub(crate) fn turn_with_inline_diffs(
    turn: &Turn,
    captured: &[CapturedDiffRecord],
) -> Result<Turn, PersistError> {
    let mut projected = turn.clone();
    let contents: HashMap<DiffId, CapturedDiffContent> = captured
        .iter()
        .map(|record| (record.id.clone(), record.content.clone()))
        .collect();
    for item in &mut projected.items {
        if let ItemPayload::FileChange { changes, .. } = &mut item.payload {
            for change in changes {
                if let Some(descriptor) = change.captured_diff.take() {
                    let Some(CapturedDiffContent::Unified { text }) = contents.get(&descriptor.id)
                    else {
                        return Err(PersistError::Invalid(format!(
                            "diff descriptor {} has no unified content",
                            descriptor.id
                        )));
                    };
                    change.diff = Some(text.clone());
                }
            }
        }
    }
    for diff in &mut projected.diffs {
        if let Some(descriptor) = diff.captured.take() {
            let Some(CapturedDiffContent::Structured { diff: content }) =
                contents.get(&descriptor.id)
            else {
                return Err(PersistError::Invalid(format!(
                    "diff descriptor {} has no structured content",
                    descriptor.id
                )));
            };
            *diff = content.clone();
        }
    }
    Ok(projected)
}

/// Derive lazy content records from a format-1 turn whose full bodies are inline.
pub(crate) fn captured_diff_contents(turn: &Turn) -> HashMap<DiffId, CapturedDiffContent> {
    let mut contents = HashMap::new();
    for item in &turn.items {
        if let ItemPayload::FileChange { changes, .. } = &item.payload {
            for change in changes {
                let Some(text) = &change.diff else {
                    continue;
                };
                let (_, record) = giskard_core::capture_unified_diff(
                    change.path.clone(),
                    change.change,
                    Some(item.id),
                    text.clone(),
                );
                contents.entry(record.id).or_insert(record.content);
            }
        }
    }
    for diff in &turn.diffs {
        if diff.captured.is_none() {
            let (_, record) = giskard_core::capture_structured_diff(diff.clone());
            contents.entry(record.id).or_insert(record.content);
        }
    }
    contents
}

/// The `kind` of a JSONL record, or `None` when the line is not a tagged object.
fn record_kind(value: &serde_json::Value) -> Option<&str> {
    value.get("kind")?.as_str()
}

/// Parse the bounded history index.
///
/// Turn records fold by turn id, **first-wins**: a repeated id is a turn that is already durable,
/// and the first durable record stays authoritative. (Superseding records — a later record that
/// *replaces* an earlier one — arrive with the amendment work, which is out of scope here; until a
/// producer exists, silently preferring a later duplicate would change how today's duplicates
/// resolve.) The same first-wins rule is what the format 1 reader applies.
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
///   Last-wins because a late `status` is the amendment case this format is meant to absorb; the
///   warning because nothing today writes a payload file twice, so a duplicate is a corruption
///   signal and must not vanish silently.
///
/// The history index folds turn records *first*-wins, which is not an inconsistency: that answers
/// "which turn record is authoritative when an append is retried", a question about the index's own
/// retry safety that does not arise inside a payload file.
pub fn parse_turn_payload(path: &Path, data: &str) -> Result<TurnPayload, PersistError> {
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
    let mut diff_contents = HashMap::new();

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

    for (i, line) in data.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            PersistError::Corrupt(format!("{}: line {}: {}", path.display(), i + 1, e))
        })?;

        match record_kind(&value) {
            Some("turn_header") => {
                let header: PayloadTurnHeader = serde_json::from_value(value).map_err(|e| {
                    PersistError::Corrupt(format!("{}: line {}: {}", path.display(), i + 1, e))
                })?;
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
                let record: PayloadUserInput = serde_json::from_value(value).map_err(|e| {
                    PersistError::Corrupt(format!("{}: line {}: {}", path.display(), i + 1, e))
                })?;
                set_singleton!(user_input, record.user_input, i + 1);
            }
            Some("status") => {
                let record: PayloadStatus = serde_json::from_value(value).map_err(|e| {
                    PersistError::Corrupt(format!("{}: line {}: {}", path.display(), i + 1, e))
                })?;
                set_singleton!(status, record.status, i + 1);
            }
            Some("item") => {
                let record: PayloadItem = serde_json::from_value(value).map_err(|e| {
                    PersistError::Corrupt(format!("{}: line {}: {}", path.display(), i + 1, e))
                })?;
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
                let record: PayloadDiff = serde_json::from_value(value).map_err(|e| {
                    PersistError::Corrupt(format!("{}: line {}: {}", path.display(), i + 1, e))
                })?;
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

    for (_, item) in &mut items {
        crate::command_output::validate_command_output_payload(&item.payload).map_err(|error| {
            PersistError::Invalid(format!(
                "{}: item {} has invalid command-output metadata: {error}",
                path.display(),
                item.id
            ))
        })?;
        let (ignored_bytes, ignored_lines) =
            crate::command_output::ignored_command_output_metadata(&item.payload);
        if ignored_bytes {
            tracing::warn!(
                path = %path.display(),
                item_id = %item.id,
                field = "output_original_bytes",
                "ignoring command-output metadata field because output is not truncated"
            );
        }
        if ignored_lines {
            tracing::warn!(
                path = %path.display(),
                item_id = %item.id,
                field = "output_original_lines",
                "ignoring command-output metadata field because output is not truncated"
            );
        }
        if let ItemPayload::FileChange { changes, .. } = &mut item.payload {
            for change in changes {
                let Some(text) = change.diff.take() else {
                    continue;
                };
                let (descriptor, record) = giskard_core::capture_unified_diff(
                    change.path.clone(),
                    change.change,
                    Some(item.id),
                    text,
                );
                change.captured_diff = Some(descriptor);
                diff_contents.entry(record.id).or_insert(record.content);
            }
        }
    }
    for (_, diff) in &mut diffs {
        if diff.captured.is_none() {
            let (projected, record) = giskard_core::capture_structured_diff(diff.clone());
            *diff = projected;
            diff_contents.entry(record.id).or_insert(record.content);
        }
    }

    for descriptor in items
        .iter()
        .flat_map(|(_, item)| match &item.payload {
            ItemPayload::FileChange { changes, .. } => changes
                .iter()
                .filter_map(|change| change.captured_diff.as_ref())
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .chain(diffs.iter().filter_map(|(_, diff)| diff.captured.as_ref()))
    {
        if descriptor.available && !diff_contents.contains_key(&descriptor.id) {
            return Err(PersistError::Invalid(format!(
                "{}: diff descriptor {} has no content record",
                path.display(),
                descriptor.id
            )));
        }
    }

    Ok(TurnPayload {
        user_input,
        status,
        items: items.into_iter().map(|(_, item)| item).collect(),
        diffs: diffs.into_iter().map(|(_, diff)| diff).collect(),
        diff_contents,
    })
}

/// Read one turn's payload file.
///
/// `Ok(None)` means the file is absent — a turn record whose payload never landed, or was removed.
/// A file that will not parse is quarantined to `<path>.corrupt-<ts>` exactly as unreadable JSON is
/// elsewhere, which leaves the bad bytes on disk for inspection and fails that one turn.
pub async fn read_turn_payload(path: &Path) -> Result<Option<TurnPayload>, PersistError> {
    let data = match fs::read_to_string(path).await {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(PersistError::Io(e.to_string())),
    };

    match parse_turn_payload(path, &data) {
        Ok(payload) => Ok(Some(payload)),
        // A newer format is not damage: leave the file alone so a later build can read it.
        Err(e @ PersistError::Invalid(_)) => Err(e),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "corrupt turn payload, quarantining");
            let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S");
            let corrupt_path = PathBuf::from(format!("{}.corrupt-{ts}", path.display()));
            if let Err(rename_error) = fs::rename(path, &corrupt_path).await {
                tracing::warn!(
                    path = %path.display(),
                    quarantine_path = %corrupt_path.display(),
                    error = %rename_error,
                    "failed to quarantine corrupt turn payload"
                );
            }
            Err(e)
        }
    }
}

#[cfg(test)]
mod lazy_diff_tests {
    use super::*;
    use giskard_core::item::FileChangeEntry;
    use giskard_core::{
        FileChangeKind, Item, ItemId, ItemPayload, Mode, ModelRef, TokenUsage, TurnId, TurnStatus,
        TurnStatusKind, UserInput,
    };

    #[test]
    fn payload_load_rejects_incomplete_command_output_metadata() {
        let now = chrono::Utc::now();
        let turn = Turn {
            id: TurnId::new(),
            user_input: UserInput::text("show output"),
            items: vec![Item {
                id: ItemId::new(),
                harness_item_id: "malformed-command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "printf output".into(),
                    cwd: ".".into(),
                    output:
                        "first\nsecond\n[… 20 bytes omitted from durable command output …]\nlast\n"
                            .into(),
                    output_truncated: true,
                    output_original_bytes: Some(1),
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
                    duration_ms: Some(1),
                },
                created_at: now,
            }],
            model: ModelRef {
                provider: "test".into(),
                model: "test".into(),
                reasoning_effort: None,
            },
            mode: Mode::Build,
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: TokenUsage::default(),
            diffs: Vec::new(),
            started_at: now,
            completed_at: Some(now),
        };

        let serialized = String::from_utf8(payload_file_bytes(&turn).unwrap()).unwrap();
        let error = parse_turn_payload(Path::new("turn.jsonl"), &serialized).unwrap_err();
        assert!(error.to_string().contains(
            "invalid command-output metadata: truncated command output is missing original-size metadata"
        ));
    }

    #[test]
    fn legacy_diff_id_is_deterministic_and_content_sensitive() {
        let unified = |text: &str| CapturedDiffContent::Unified { text: text.into() };
        let path = Path::new("src/main.rs");
        assert_eq!(
            giskard_core::captured_diff_id(path, FileChangeKind::Modified, &unified("same")),
            giskard_core::captured_diff_id(path, FileChangeKind::Modified, &unified("same"))
        );
        assert_ne!(
            giskard_core::captured_diff_id(path, FileChangeKind::Modified, &unified("same")),
            giskard_core::captured_diff_id(path, FileChangeKind::Modified, &unified("changed"))
        );
        assert_ne!(
            giskard_core::captured_diff_id(path, FileChangeKind::Modified, &unified("same")),
            giskard_core::captured_diff_id(
                Path::new("src/other.rs"),
                FileChangeKind::Modified,
                &unified("same")
            )
        );

        let diff = FileDiff {
            path: "src/main.rs".into(),
            change: FileChangeKind::Modified,
            old_text: Some("old".into()),
            new_text: Some("new".into()),
            hunks: Vec::new(),
            binary: false,
            captured: None,
        };
        let structured = |diff: FileDiff| CapturedDiffContent::Structured { diff };
        assert_eq!(
            giskard_core::captured_diff_id(&diff.path, diff.change, &structured(diff.clone())),
            giskard_core::captured_diff_id(&diff.path, diff.change, &structured(diff.clone()))
        );
        let mut changed = diff.clone();
        changed.new_text = Some("newer".into());
        assert_ne!(
            giskard_core::captured_diff_id(&diff.path, diff.change, &structured(diff.clone())),
            giskard_core::captured_diff_id(
                &changed.path,
                changed.change,
                &structured(changed.clone())
            )
        );
    }

    #[test]
    fn empty_inline_diff_roundtrips_through_format_one_payload() {
        let item_id = ItemId::new();
        let (descriptor, record) = giskard_core::capture_unified_diff(
            "src/empty.rs".into(),
            FileChangeKind::Modified,
            Some(item_id),
            String::new(),
        );
        let now = chrono::Utc::now();
        let turn = Turn {
            id: TurnId::new(),
            user_input: UserInput::text("empty diff"),
            items: vec![Item {
                id: item_id,
                harness_item_id: "empty-diff-item".into(),
                payload: ItemPayload::FileChange {
                    path: "src/empty.rs".into(),
                    change: FileChangeKind::Modified,
                    changes: vec![FileChangeEntry {
                        path: "src/empty.rs".into(),
                        change: FileChangeKind::Modified,
                        diff: None,
                        captured_diff: Some(descriptor.clone()),
                    }],
                    status: None,
                },
                created_at: now,
            }],
            model: ModelRef {
                provider: "test".into(),
                model: "test".into(),
                reasoning_effort: None,
            },
            mode: Mode::Build,
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: TokenUsage::default(),
            diffs: Vec::new(),
            started_at: now,
            completed_at: Some(now),
        };

        let bytes = payload_file_bytes_with_diffs(&turn, &[record]).unwrap();
        let serialized = String::from_utf8(bytes).unwrap();
        assert!(serialized.contains(r#""diff":"""#));
        let payload = parse_turn_payload(Path::new("turn.jsonl"), &serialized).unwrap();
        let change = match &payload.items[0].payload {
            ItemPayload::FileChange { changes, .. } => &changes[0],
            _ => panic!("expected file-change payload"),
        };
        assert_eq!(change.captured_diff.as_ref().unwrap().id, descriptor.id);
        assert!(matches!(
            payload.diff_contents.get(&descriptor.id),
            Some(CapturedDiffContent::Unified { text }) if text.is_empty()
        ));
    }
}
