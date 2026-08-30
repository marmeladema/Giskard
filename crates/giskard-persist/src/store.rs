//! The persistence store: load/save projects, threads, token ledgers (spec §5).

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use tokio::sync::{Mutex, RwLock};

use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::ItemPayload;
use giskard_core::model::{Effort, ModelRef};
use giskard_core::thread::ThreadKind;
use giskard_core::token::{DailyTokenLedger, TokenLedger};
use giskard_core::turn::{PermissionPreset, Turn, TurnMode, TurnModel};

use crate::PersistError;
use crate::atomic::{atomic_write, atomic_write_json, read_json, read_json_or_quarantine};
use crate::config::Config;
use crate::history::{self, HistoryHeader, TurnRecord};
use crate::layout::{ThreadLayout, ThreadPaths};
use crate::migrate::{self, MigrationOutcome};
use giskard_core::diff::CapturedDiffRecord;
use giskard_core::{CapturedDiffContent, DiffId};

const SCHEMA_VERSION: u32 = 1;
pub const THREAD_METADATA_VERSION: u32 = 2;

// ---- Persisted types ----

/// `projects.json` — index of all projects (spec §5.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectIndex {
    pub version: u32,
    pub projects: Vec<ProjectEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectEntry {
    pub id: ProjectId,
    pub name: String,
    pub dir: String,
    pub created_at: DateTime<Utc>,
    pub order: usize,
}

/// `projects/<id>/project.json` — per-project config (spec §5.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub version: u32,
    pub id: ProjectId,
    pub name: String,
    pub dir: String,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
    /// Absorbs the `default_model` key of a `project.json` written before a thread's starting model
    /// moved to the project catalog (§8.3). **Never read** — the leading underscore is the
    /// reminder — and `skip_serializing` drops it the next time this file is written.
    ///
    /// It has to exist at all only because of `deny_unknown_fields` above, which is load-bearing
    /// here: it is what makes an obsolete *`permission_preset`* fail loudly instead of silently
    /// reverting a user's sandboxing to the default. Keeping that guard means every key a previous
    /// version wrote has to be named, even the ones that no longer mean anything.
    #[serde(default, skip_serializing, rename = "default_model")]
    _default_model: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `projects/<id>/threads/<thread_id>.json` — thread metadata and recomputable caches (§5.3/H1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadFile {
    pub version: u32,
    pub id: ThreadId,
    pub project_id: ProjectId,
    /// Monotonic revision of this thread's durable metadata.
    ///
    /// Existing files predate the field and start at zero. The first real mutation advances to
    /// one under the same per-thread lock that commits the change.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub revision: u64,
    pub title: String,
    pub harness_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<ThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawned_by_turn_id: Option<TurnId>,
    #[serde(default, skip_serializing_if = "is_primary_thread")]
    pub kind: ThreadKind,
    pub mode: TurnMode,
    pub current_model: TurnModel,
    /// Effective context window for `current_model`. This starts from catalog/config metadata and
    /// is replaced when the harness reports an authoritative runtime value.
    #[serde(default)]
    pub context_window: u32,
    /// Harness-reported effective windows nested by provider and model. These survive reloads and
    /// model switches without making Giskard maintain model-specific built-in metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub model_context_windows: HashMap<String, HashMap<String, u32>>,
    /// Per-thread permission preset (P3).
    #[serde(
        alias = "approval_policy",
        deserialize_with = "deserialize_persisted_permission_preset"
    )]
    pub permission_preset: PermissionPreset,
    /// Per-model effort retention (C7): maps `"provider/model"` → stored `Effort`, so switching
    /// back to a reasoning model restores the user's last effort choice.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub model_efforts: HashMap<String, Effort>,
    /// Token aggregates (total + nested by_model). A **recomputable cache** (H3): the authoritative
    /// history is the `.jsonl`, so these can be rebuilt by folding it (`recompute_aggregates`).
    pub tokens: TokenLedger,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub archived: bool,
    /// The Git workspace this thread was created with, when it was created with one. Absent for
    /// ordinary threads, which work in the project's own workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_workspace: Option<ThreadGitWorkspace>,
}

/// Whether and how a metadata mutation affects the thread's sidebar recency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadRecency {
    /// Background repair, normalization, imports, and cache updates preserve ordering.
    Preserve,
    /// A user-visible mutation touches recency only when another durable field changed.
    TouchIfChanged,
    /// A completed turn is itself visible activity, even when no aggregate changed.
    RecordActivity,
    /// Crash repair restores the latest durable turn time without moving an old thread to now.
    RestoreActivity(DateTime<Utc>),
}

/// Largest metadata revision represented exactly by the paired JavaScript client.
///
/// Revisions are JSON numbers on the wire. Advancing past JavaScript's maximum safe integer would
/// make distinct durable revisions compare equal in the browser, so exhaustion is deliberately
/// earlier than `u64::MAX`.
const MAX_THREAD_REVISION: u64 = 9_007_199_254_740_991;

/// How many unreferenced payload files one thread can plausibly have accumulated honestly.
///
/// A turn commit writes its payload, then appends its index record; only a crash landing between
/// those two writes leaves an orphan, so they accrue one per crash. Well past that, the likelier
/// explanation is that the *index* lost a tail of appends and the payloads it no longer names are
/// the surviving history — so the sweep refuses instead of deleting. Deliberately not a parameter:
/// a guard against destroying transcript history should not be callable away.
const MAX_PLAUSIBLE_ORPHANS: usize = 8;

/// Result of an attempted metadata mutation under the per-thread store lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadMutation {
    Missing,
    Unchanged {
        current: Box<ThreadFile>,
    },
    Changed {
        before: Box<ThreadFile>,
        after: Box<ThreadFile>,
    },
}

/// What an orphan sweep found, and why it declined to delete if it did.
///
/// A refusal is carried rather than raised because it is a statement *about* the payload files —
/// "these may be the surviving history" — so it has to reach the caller together with the list it
/// describes. Raising it would also make `--dry-run` fail, which is precisely the run whose job is
/// to show an operator what a refusal is talking about.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrphanSweep {
    /// Payload files no turn record references: deleted, or the ones that would be.
    pub payloads: Vec<PathBuf>,
    /// Why nothing was deleted. `None` means the sweep was willing to delete `payloads` — and did,
    /// unless this was a dry run.
    pub refusal: Option<String>,
}

/// Result after a turn was appended to authoritative history.
///
/// A metadata failure is distinct from an append failure because the history line remains durable
/// and must be repaired from JSONL later.
#[derive(Debug)]
pub enum TurnCommitOutcome {
    MetadataMutation(ThreadMutation),
    MetadataFailed(PersistError),
}

impl ThreadMutation {
    pub fn into_current(self) -> Option<ThreadFile> {
        match self {
            Self::Missing => None,
            Self::Unchanged { current } => Some(*current),
            Self::Changed { after, .. } => Some(*after),
        }
    }
}

impl ThreadFile {
    /// Record a harness-reported window for an exact model and update the visible capacity only
    /// when that provider/model is selected. Reasoning effort is not part of capacity identity.
    pub fn record_model_context_window(&mut self, model: &ModelRef, context_window: u32) {
        self.model_context_windows
            .entry(model.provider.clone())
            .or_default()
            .insert(model.model.clone(), context_window);
        if self.current_model.as_known().is_some_and(|current| {
            current.provider == model.provider && current.model == model.model
        }) {
            self.context_window = context_window;
        }
    }
}

/// A Git workspace a thread owns, tagged by the strategy that produced it.
///
/// Tagged rather than a bare struct because the strategies are meant to *coexist*: a worktree and a
/// thread-owned repository answer different needs — one shares the project's history and delivers
/// work by simply existing, the other trades that for a boundary — and a user picking per thread
/// wants both available. So this is not a placeholder for one shape replacing another. A new
/// strategy is a new variant, old records keep parsing because their tag still names their own
/// variant, and nothing has to be migrated.
///
/// The tag values match `GitStrategy` on the wire, so a record says which choice produced it in the
/// same vocabulary the request used.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum ThreadGitWorkspace {
    Worktree(ThreadWorktree),
}

impl ThreadGitWorkspace {
    /// Where the thread works — the one question every strategy must answer, and the only thing
    /// most callers need. Everything else about a workspace is strategy-specific.
    pub fn workspace_root(&self) -> &str {
        match self {
            Self::Worktree(worktree) => worktree.workspace_root(),
        }
    }

    /// The worktree behind this workspace, if that is what it is.
    ///
    /// Deliberately fallible rather than a field: creation, removal and deletion impact are
    /// strategy-specific, so a caller doing one of those has to say which strategy it handles and
    /// what it does about the others.
    pub fn as_worktree(&self) -> Option<&ThreadWorktree> {
        match self {
            Self::Worktree(worktree) => Some(worktree),
        }
    }
}

/// A linked Git worktree owned by one thread, so its file changes never touch the project's
/// checkout or another thread's.
///
/// The Git directories are recorded rather than derived: `<workspace>/.git` is only the repository
/// when the project directory is an ordinary checkout, and is a pointer *file* when the project is
/// itself a linked worktree. Both paths come from `git rev-parse` inside the worktree at creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadWorktree {
    /// Absolute path of the worktree — the checkout Git created, and what `git worktree remove`
    /// and the impact probes operate on. Not necessarily where the thread works: see `workspace`.
    pub path: String,
    /// Absolute path the thread actually works in, and what every path resolves against.
    ///
    /// Equal to `path` when the project directory is the repository's top level. When the project
    /// is rooted in a *subdirectory* of its repository, Git can only check out the whole repository,
    /// so the worktree is its root while the thread works in the same subdirectory beneath it —
    /// otherwise an isolated thread would silently work one or more levels above the directory the
    /// project scoped it to, and a path would name a different file than it does for an ordinary
    /// thread of the same project.
    ///
    /// Absent for the top-level case, which is the common one; read it through
    /// [`ThreadWorktree::workspace_root`] rather than directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// The branch created for this thread. Only its *starting* branch: the agent may switch away,
    /// so this names what to clean up, never what is currently checked out.
    pub branch: String,
    /// The commit the branch started from, or `None` in a repository with no commits, where the
    /// worktree is created on an orphan branch and there is nothing to count from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_commit: Option<String>,
    /// The checkout that owns the worktree — where `git worktree remove` and `git branch -D` run.
    pub repo_root: String,
    /// `git rev-parse --git-common-dir`: the shared repository (objects, refs, config, hooks).
    pub common_dir: String,
    /// `git rev-parse --git-dir`: this worktree's private directory (its index, HEAD, reflog).
    pub git_dir: String,
}

impl ThreadWorktree {
    /// Where the thread works. The whole point of the pair: `path` is the checkout Git manages,
    /// this is the directory inside it that stands in for the project's own.
    pub fn workspace_root(&self) -> &str {
        self.workspace.as_deref().unwrap_or(&self.path)
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn next_recency(previous: DateTime<Utc>) -> Result<DateTime<Utc>, PersistError> {
    let now = Utc::now();
    if now > previous {
        return Ok(now);
    }
    previous
        .checked_add_signed(TimeDelta::nanoseconds(1))
        .ok_or_else(|| PersistError::Invalid("thread recency timestamp exhausted".into()))
}

fn is_primary_thread(value: &ThreadKind) -> bool {
    *value == ThreadKind::Primary
}

fn deserialize_persisted_permission_preset<'de, D>(
    deserializer: D,
) -> Result<PermissionPreset, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "ask_first" | "ask" | "read_only" => Ok(PermissionPreset::AskFirst),
        "auto_approve" | "auto" => Ok(PermissionPreset::AutoApprove),
        "full_access" => Ok(PermissionPreset::FullAccess),
        other => Err(serde::de::Error::unknown_variant(
            other,
            &[
                "ask_first",
                "auto_approve",
                "full_access",
                "ask",
                "auto",
                "read_only",
            ],
        )),
    }
}

pub(crate) fn parse_turn_history(path: &Path, data: &str) -> Result<Vec<Turn>, PersistError> {
    let lines: Vec<&str> = data.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut turns = Vec::with_capacity(lines.len());
    let mut seen_turn_ids = HashSet::new();
    let last = lines.len().saturating_sub(1);
    for (i, line) in lines.iter().enumerate() {
        match serde_json::from_str::<Turn>(line) {
            Ok(turn) => {
                if !seen_turn_ids.insert(turn.id) {
                    tracing::warn!(
                        path = %path.display(),
                        turn_id = %turn.id,
                        line = i + 1,
                        "skipping duplicate turn id in history"
                    );
                    continue;
                }
                for item in &turn.items {
                    crate::command_output::validate_command_output_payload(&item.payload).map_err(
                        |error| {
                            PersistError::Invalid(format!(
                                "{}: line {} item {} has invalid command-output metadata: {error}",
                                path.display(),
                                i + 1,
                                item.id
                            ))
                        },
                    )?;
                    let (ignored_bytes, ignored_lines) =
                        crate::command_output::ignored_command_output_metadata(&item.payload);
                    if ignored_bytes {
                        tracing::warn!(
                            path = %path.display(),
                            turn_id = %turn.id,
                            item_id = %item.id,
                            field = "output_original_bytes",
                            "ignoring command-output metadata field because output is not truncated"
                        );
                    }
                    if ignored_lines {
                        tracing::warn!(
                            path = %path.display(),
                            turn_id = %turn.id,
                            item_id = %item.id,
                            field = "output_original_lines",
                            "ignoring command-output metadata field because output is not truncated"
                        );
                    }
                }
                turns.push(turn);
            }
            Err(e) if i == last => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "skipping torn final turn line in history"
                );
            }
            Err(e) => {
                return Err(PersistError::Corrupt(format!(
                    "{}: line {}: {}",
                    path.display(),
                    i + 1,
                    e
                )));
            }
        }
    }
    Ok(turns)
}

/// Reassemble whole `Turn` values from a format 2 index plus one payload file per turn.
///
/// A turn whose payload is missing, damaged, or written in a format this build does not understand
/// fails **alone**: it is dropped from the returned history with an error logged, while the index
/// and every other turn stay readable. That containment is the reason a turn is its own file.
async fn assemble_turns(
    paths: &ThreadPaths,
    index_path: &Path,
    index_data: &str,
) -> Result<Vec<Turn>, PersistError> {
    let records = history::parse_history_index(index_path, index_data)?;
    Ok(assemble_turn_records(paths, records).await)
}

/// Reassemble only the selected index records from their payload files.
///
/// Selection happens before this function is called. Keeping that boundary explicit prevents a
/// bounded display read from accidentally regressing into opening every agent-driven payload in a
/// thread before slicing the result.
async fn assemble_turn_records(paths: &ThreadPaths, records: Vec<TurnRecord>) -> Vec<Turn> {
    let mut turns = Vec::with_capacity(records.len());
    for record in records {
        if let Some(turn) = assemble_turn_record(paths, record).await {
            turns.push(turn);
        }
    }
    turns
}

async fn assemble_turn_record(paths: &ThreadPaths, record: TurnRecord) -> Option<Turn> {
    let payload_path = paths.turn_payload(record.turn_id);
    match history::read_turn_payload(&payload_path).await {
        Ok(Some(payload)) => Some(record.into_turn(payload, &payload_path)),
        Ok(None) => {
            tracing::error!(
                path = %payload_path.display(),
                turn_id = %record.turn_id,
                action = "load_turn_payload",
                "turn payload file is missing; skipping the turn and keeping the rest of the thread"
            );
            None
        }
        Err(error) => {
            tracing::error!(
                path = %payload_path.display(),
                turn_id = %record.turn_id,
                %error,
                action = "load_turn_payload",
                "unreadable turn payload; skipping the turn and keeping the rest of the thread"
            );
            None
        }
    }
}

async fn remove_dir_all_if_present(path: &Path) -> Result<(), PersistError> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(PersistError::Io(e.to_string())),
    }
}

async fn path_exists(path: &Path) -> bool {
    tokio::fs::metadata(path).await.is_ok()
}

async fn is_dir(path: &Path) -> bool {
    matches!(tokio::fs::metadata(path).await, Ok(meta) if meta.is_dir())
}

// ---- Store ----

/// The flat-file persistence store.
///
/// Owns the data directory path. Each file is guarded by a per-file async mutex
/// for single-writer discipline (spec §5.4): the project index has its own lock, and thread
/// files are serialized per id via [`PersistStore::update_thread`].
pub struct PersistStore {
    data_dir: PathBuf,
    config: Mutex<Option<Config>>,
    project_index_lock: Mutex<()>,
    /// Per-thread-id write locks so read-modify-write of a thread file is single-writer (§5.4).
    thread_locks: Mutex<HashMap<ThreadId, Weak<Mutex<()>>>>,
    /// Parsed JSONL history cache, keyed by `(project, thread)`.
    ///
    /// The JSONL remains authoritative. This per-process cache only avoids reparsing unchanged
    /// histories when the user switches between already-opened threads.
    history_cache: RwLock<HashMap<(ProjectId, ThreadId), Arc<HistoryCacheEntry>>>,
    /// Threads whose migration to the current layout was attempted and could not proceed.
    ///
    /// Without this, a thread with an unparseable format 1 history re-reads and re-parses that
    /// whole file, takes the per-thread lock, and logs an error on *every* store call — including
    /// the read paths that previously took no lock at all. Nothing here changes what a caller sees;
    /// it only stops the store from retrying a decision it has already made this process.
    unmigratable: RwLock<HashSet<(ProjectId, ThreadId)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCommandOutput {
    pub output: String,
    pub output_truncated: bool,
    pub original_bytes: u64,
    pub original_lines: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredToolOutput {
    pub bytes: Vec<u8>,
    pub descriptor: giskard_core::item::ToolOutputDescriptor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HistoryFileMeta {
    len: u64,
    modified: Option<SystemTime>,
}

struct HistoryCacheEntry {
    turns: RwLock<Vec<Turn>>,
    meta: Mutex<HistoryFileMeta>,
}

impl PersistStore {
    /// Create a new store rooted at `data_dir`.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            config: Mutex::new(None),
            project_index_lock: Mutex::new(()),
            thread_locks: Mutex::new(HashMap::new()),
            history_cache: RwLock::new(HashMap::new()),
            unmigratable: RwLock::new(HashSet::new()),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    // ---- Config ----

    pub async fn load_config(&self) -> Result<Config, PersistError> {
        let mut guard = self.config.lock().await;
        if let Some(cfg) = guard.as_ref() {
            return Ok(cfg.clone());
        }
        let path = self.data_dir.join("config.toml");
        let cfg = if path.exists() {
            let data = tokio::fs::read_to_string(&path)
                .await
                .map_err(|e| PersistError::Io(e.to_string()))?;
            toml::from_str(&data).map_err(|e| PersistError::Invalid(e.to_string()))?
        } else {
            Config::default()
        };
        *guard = Some(cfg.clone());
        Ok(cfg)
    }

    // ---- Project index ----

    fn projects_json_path(&self) -> PathBuf {
        self.data_dir.join("projects.json")
    }

    fn project_dir(&self, id: ProjectId) -> PathBuf {
        self.data_dir.join("projects").join(id.to_string())
    }

    fn project_json_path(&self, id: ProjectId) -> PathBuf {
        self.project_dir(id).join("project.json")
    }

    fn threads_dir(&self, id: ProjectId) -> PathBuf {
        self.project_dir(id).join("threads")
    }

    // ---- Layout resolution (§5.2) ----
    //
    // The store holds a mix of format 1 and format 2 threads for as long as an unmigrated thread
    // survives, and *every* thread file operation resolves differently between them. Resolution is
    // centralized here so no call site can branch on only one of the two shapes.

    /// Which layout this thread is actually stored in.
    ///
    /// Structural, not recorded anywhere: the directory either exists or it does not. A thread with
    /// nothing on disk resolves to the current layout, so a first write creates format 2.
    async fn thread_layout(&self, project: ProjectId, thread: ThreadId) -> ThreadLayout {
        let threads_dir = self.threads_dir(project);
        if is_dir(&threads_dir.join(thread.to_string())).await {
            return ThreadLayout::Directory;
        }
        if path_exists(&threads_dir.join(format!("{thread}.json"))).await
            || path_exists(&threads_dir.join(format!("{thread}.jsonl"))).await
        {
            return ThreadLayout::Flat;
        }
        ThreadLayout::Directory
    }

    async fn thread_paths(&self, project: ProjectId, thread: ThreadId) -> ThreadPaths {
        ThreadPaths::new(
            self.threads_dir(project),
            thread,
            self.thread_layout(project, thread).await,
        )
    }

    /// Paths for a thread already known to be in the current layout, without re-resolving.
    fn current_thread_paths(&self, project: ProjectId, thread: ThreadId) -> ThreadPaths {
        ThreadPaths::new(self.threads_dir(project), thread, ThreadLayout::Directory)
    }

    /// Migrate this thread to the current layout if it is not already there.
    ///
    /// Called at the top of every public thread entry point, *before* that entry point takes the
    /// per-thread lock, because migration itself runs under that lock and the lock is not
    /// reentrant. Migration is monotonic, so releasing the lock in between cannot un-migrate.
    ///
    /// Returns the layout the thread is actually in afterwards: a migration that cannot proceed
    /// (an unparseable format 1 history, a failed rename) leaves the thread readable and writable
    /// exactly as it is today rather than failing the caller's operation.
    async fn ensure_migrated(&self, project: ProjectId, thread: ThreadId) -> ThreadLayout {
        let threads_dir = self.threads_dir(project);
        let has_flat = path_exists(&threads_dir.join(format!("{thread}.json"))).await
            || path_exists(&threads_dir.join(format!("{thread}.jsonl"))).await;
        if !has_flat {
            // The overwhelmingly common case: a format 2 thread, or one with nothing on disk yet.
            // One `stat` and no lock.
            return ThreadLayout::Directory;
        }
        if self.unmigratable.read().await.contains(&(project, thread)) {
            return self.thread_layout(project, thread).await;
        }

        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        self.migrate_thread_unlocked(project, thread).await
    }

    /// Migrate under an already-held thread lock, and report the layout that resulted.
    ///
    /// Never fails the caller: a migration that cannot proceed is logged, remembered, and left on
    /// the layout it could not leave.
    async fn migrate_thread_unlocked(&self, project: ProjectId, thread: ThreadId) -> ThreadLayout {
        match migrate::migrate_thread(&self.threads_dir(project), thread).await {
            Ok(MigrationOutcome::Migrated | MigrationOutcome::FinishedLegacyMove) => {
                // History moved file; anything parsed from the old path is stale.
                self.invalidate_history_cache(project, thread).await;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(
                    %project,
                    %thread,
                    %error,
                    action = "migrate_thread",
                    "thread storage migration failed; continuing on the existing layout"
                );
                self.unmigratable.write().await.insert((project, thread));
            }
        }
        self.thread_layout(project, thread).await
    }

    /// Forget that a thread could not be migrated, so the next open tries again.
    ///
    /// Called wherever the reason might have changed: an explicit `giskard-admin` run, and deletion
    /// (which frees the id for a thread that has nothing to do with the one that failed).
    async fn clear_unmigratable(&self, project: ProjectId, thread: ThreadId) {
        self.unmigratable.write().await.remove(&(project, thread));
    }

    /// The thread metadata record, wherever this thread keeps it.
    async fn thread_json_path(&self, project: ProjectId, thread: ThreadId) -> PathBuf {
        self.thread_paths(project, thread).await.metadata()
    }

    async fn history_file_meta(
        &self,
        path: &Path,
    ) -> Result<Option<HistoryFileMeta>, PersistError> {
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(Some(HistoryFileMeta {
                len: meta.len(),
                modified: meta.modified().ok(),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PersistError::Io(e.to_string())),
        }
    }

    async fn history_cache_entry(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Option<Arc<HistoryCacheEntry>> {
        self.history_cache
            .read()
            .await
            .get(&(project, thread))
            .cloned()
    }

    async fn install_history_cache(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turns: Vec<Turn>,
        meta: HistoryFileMeta,
    ) -> Arc<HistoryCacheEntry> {
        let entry = Arc::new(HistoryCacheEntry {
            turns: RwLock::new(turns),
            meta: Mutex::new(meta),
        });
        self.history_cache
            .write()
            .await
            .insert((project, thread), entry.clone());
        entry
    }

    /// The parsed history for a thread, reusing the cache when the index file is unchanged.
    ///
    /// Cache validity is keyed on the **index** file's metadata alone. That is sound because
    /// nothing appends to a payload file after its turn commits, so a turn's payload cannot change
    /// under a cache entry the index still validates. When amendments start appending to payload
    /// files, this key has to grow with them.
    ///
    /// Note what this trades away. Caching whole `Turn` values keeps a warm `load_all_turns` at
    /// zero payload opens, but it also keeps resident memory per open thread *unbounded*, exactly
    /// as before this change — so the split's "the index is bounded, so it can stay resident"
    /// benefit is available but not yet taken. Claiming it means caching only the parsed index and
    /// reading payloads per turn, which is worth doing once the bounded read APIs exist and the
    /// callers that need whole turns are the exception rather than the rule.
    async fn current_history_cache_entry(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Option<Arc<HistoryCacheEntry>>, PersistError> {
        let paths = self.thread_paths(project, thread).await;
        let Some(meta) = self.history_file_meta(&paths.history()).await? else {
            self.invalidate_history_cache(project, thread).await;
            return Ok(None);
        };

        if let Some(entry) = self.history_cache_entry(project, thread).await {
            let cached_meta = *entry.meta.lock().await;
            if cached_meta == meta {
                return Ok(Some(entry));
            }
        }

        let Some((turns, meta)) = self.load_all_turns_uncached(&paths, meta).await? else {
            self.invalidate_history_cache(project, thread).await;
            return Ok(None);
        };
        Ok(Some(
            self.install_history_cache(project, thread, turns, meta)
                .await,
        ))
    }

    async fn invalidate_history_cache(&self, project: ProjectId, thread: ThreadId) {
        self.history_cache.write().await.remove(&(project, thread));
    }

    async fn invalidate_project_history_cache(&self, project: ProjectId) {
        self.history_cache
            .write()
            .await
            .retain(|(cached_project, _), _| *cached_project != project);
        self.unmigratable
            .write()
            .await
            .retain(|(cached_project, _)| *cached_project != project);
    }

    async fn update_history_cache_after_append(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: &Turn,
        meta_before: Option<HistoryFileMeta>,
        meta_after: Option<HistoryFileMeta>,
        appended_len: u64,
    ) {
        let Some(entry) = self.history_cache_entry(project, thread).await else {
            return;
        };
        let Some(meta_after) = meta_after else {
            tracing::error!(
                %project,
                %thread,
                "history append succeeded but metadata refresh failed; invalidating parsed history cache"
            );
            self.invalidate_history_cache(project, thread).await;
            return;
        };

        let cached_meta = *entry.meta.lock().await;

        let Some(meta_before) = meta_before else {
            tracing::error!(
                %project,
                %thread,
                "parsed history cache existed but history file metadata was missing before append; invalidating cache"
            );
            self.invalidate_history_cache(project, thread).await;
            return;
        };

        if cached_meta != meta_before {
            tracing::debug!(
                %project,
                %thread,
                "history cache was stale before append; invalidating instead of appending in memory"
            );
            self.invalidate_history_cache(project, thread).await;
            return;
        };

        if meta_after.len != meta_before.len + appended_len {
            tracing::warn!(
                %project,
                %thread,
                cached_len = meta_before.len,
                appended_len,
                actual_len = meta_after.len,
                "history file changed by more than the appended turn; invalidating parsed history cache"
            );
            self.invalidate_history_cache(project, thread).await;
            return;
        }

        {
            let mut turns = entry.turns.write().await;
            if turns.iter().any(|cached| cached.id == turn.id) {
                tracing::warn!(
                    %project,
                    %thread,
                    turn_id = %turn.id,
                    "skipping duplicate turn id in parsed history cache"
                );
            } else {
                let mut cached_turn = turn.clone();
                cached_turn.user_input = cached_turn.user_input.without_attachment_data();
                turns.push(cached_turn);
            }
        }
        *entry.meta.lock().await = meta_after;
    }

    fn tokens_json_path(&self, project: ProjectId) -> PathBuf {
        self.project_dir(project).join("tokens.json")
    }

    fn global_tokens_path(&self) -> PathBuf {
        self.data_dir.join("tokens-global.json")
    }

    /// Load the project index, or return an empty one if it doesn't exist.
    pub async fn load_project_index(&self) -> Result<ProjectIndex, PersistError> {
        let _lock = self.project_index_lock.lock().await;
        Ok(read_json_or_quarantine(&self.projects_json_path())
            .await?
            .unwrap_or(ProjectIndex {
                version: SCHEMA_VERSION,
                projects: vec![],
            }))
    }

    /// Save the project index atomically.
    pub async fn save_project_index(&self, index: &ProjectIndex) -> Result<(), PersistError> {
        let _lock = self.project_index_lock.lock().await;
        atomic_write_json(&self.projects_json_path(), index).await
    }

    // ---- Project config ----

    /// Load a single project's config.
    pub async fn load_project(&self, id: ProjectId) -> Result<Option<ProjectConfig>, PersistError> {
        read_json_or_quarantine(&self.project_json_path(id)).await
    }

    /// Save a project's config atomically. Also creates the project directory.
    pub async fn save_project(&self, config: &ProjectConfig) -> Result<(), PersistError> {
        atomic_write_json(&self.project_json_path(config.id), config).await
    }

    /// Create a new project: add to index + write project.json.
    pub async fn create_project(
        &self,
        id: ProjectId,
        name: &str,
        dir: &str,
    ) -> Result<ProjectConfig, PersistError> {
        let now = Utc::now();
        let mut index = self.load_project_index().await?;
        let order = index.projects.len();
        let entry = ProjectEntry {
            id,
            name: name.into(),
            dir: dir.into(),
            created_at: now,
            order,
        };
        index.projects.push(entry);
        self.save_project_index(&index).await?;

        let config = ProjectConfig {
            version: SCHEMA_VERSION,
            id,
            name: name.into(),
            dir: dir.into(),
            harness: "codex".into(),
            workspace_root: None,
            _default_model: None,
            created_at: now,
            updated_at: now,
        };
        self.save_project(&config).await?;
        Ok(config)
    }

    /// Delete a project: remove from index + delete its directory.
    pub async fn delete_project(&self, id: ProjectId) -> Result<(), PersistError> {
        let mut index = self.load_project_index().await?;
        index.projects.retain(|p| p.id != id);
        self.save_project_index(&index).await?;

        let dir = self.project_dir(id);
        if dir.exists() {
            tokio::fs::remove_dir_all(&dir)
                .await
                .map_err(|e| PersistError::Io(e.to_string()))?;
        }
        self.invalidate_project_history_cache(id).await;
        Ok(())
    }

    // ---- Threads ----

    /// Load a thread file.
    pub async fn load_thread(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Option<ThreadFile>, PersistError> {
        self.ensure_migrated(project, thread).await;
        self.load_thread_unlocked(project, thread).await
    }

    /// Load a thread file without migrating first — for callers already holding the thread lock,
    /// which migration itself takes.
    async fn load_thread_unlocked(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Option<ThreadFile>, PersistError> {
        let path = self.thread_json_path(project, thread).await;
        let loaded: Option<ThreadFile> = read_json_or_quarantine(&path).await?;
        if let Some(thread_file) = loaded.as_ref()
            && thread_file.version > THREAD_METADATA_VERSION
        {
            return Err(PersistError::Invalid(format!(
                "{}: thread metadata version {} is newer than this build understands ({THREAD_METADATA_VERSION})",
                path.display(),
                thread_file.version
            )));
        }
        Ok(loaded)
    }

    /// Save a thread file atomically.
    pub async fn save_thread(
        &self,
        project: ProjectId,
        thread: &ThreadFile,
    ) -> Result<(), PersistError> {
        self.ensure_migrated(project, thread.id).await;
        let lock = self.thread_lock(thread.id).await;
        let _guard = lock.lock().await;
        self.save_thread_unlocked(project, thread).await
    }

    /// Write the metadata record. The caller must hold this thread's lock — the read-modify-write
    /// in [`Self::update_thread_with_recency_unlocked`] is only single-writer because of it.
    async fn save_thread_unlocked(
        &self,
        project: ProjectId,
        thread: &ThreadFile,
    ) -> Result<(), PersistError> {
        atomic_write_json(&self.thread_json_path(project, thread.id).await, thread).await
    }

    /// Create a thread without overwriting an existing record. New durable records begin at
    /// revision one; revision zero is reserved for files written before revisions existed.
    pub async fn create_thread(
        &self,
        project: ProjectId,
        mut thread: ThreadFile,
    ) -> Result<ThreadFile, PersistError> {
        if thread.project_id != project {
            return Err(PersistError::Invalid(format!(
                "thread {} belongs to project {}, not {project}",
                thread.id, thread.project_id
            )));
        }
        self.ensure_migrated(project, thread.id).await;
        let lock = self.thread_lock(thread.id).await;
        let _guard = lock.lock().await;
        if self
            .load_thread_unlocked(project, thread.id)
            .await?
            .is_some()
        {
            return Err(PersistError::Invalid(format!(
                "thread {} already exists in project {project}",
                thread.id
            )));
        }
        thread.revision = 1;
        self.save_thread_unlocked(project, &thread).await?;
        // The history header is written here, once, and never touched again. That is
        // what lets it carry the layout version: unlike `thread.json` it inherits no bug from the
        // metadata write path, and there is no second file its claim has to stay consistent with.
        self.ensure_history_header_unlocked(project, thread.id, thread.created_at)
            .await?;
        Ok(thread)
    }

    /// Acquire (creating if needed) the per-thread write lock.
    async fn thread_lock(&self, thread: ThreadId) -> Arc<Mutex<()>> {
        let mut locks = self.thread_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&thread).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(thread, Arc::downgrade(&lock));
        lock
    }

    /// Atomically read-modify-write a thread file under its per-thread lock (spec §5.4
    /// single-writer discipline). `f` mutates the loaded [`ThreadFile`]; the result is written
    /// back atomically before the lock is released, so concurrent mutations (a turn completing
    /// while the user switches model/mode/preset) cannot lose each other's updates.
    ///
    /// Background mutations preserve recency by default. User actions and turn completion must
    /// use [`Self::update_thread_with_recency`] with the appropriate explicit intent.
    pub async fn update_thread<F>(
        &self,
        project: ProjectId,
        thread: ThreadId,
        f: F,
    ) -> Result<ThreadMutation, PersistError>
    where
        F: FnOnce(&mut ThreadFile),
    {
        self.update_thread_with_recency(project, thread, ThreadRecency::Preserve, f)
            .await
    }

    /// Atomically mutate thread metadata, allocate its revision, and apply an explicit recency
    /// policy. No-op mutations perform no write and do not advance either field.
    pub async fn update_thread_with_recency<F>(
        &self,
        project: ProjectId,
        thread: ThreadId,
        recency: ThreadRecency,
        f: F,
    ) -> Result<ThreadMutation, PersistError>
    where
        F: FnOnce(&mut ThreadFile),
    {
        self.ensure_migrated(project, thread).await;
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        self.update_thread_with_recency_unlocked(project, thread, recency, f)
            .await
    }

    /// The read-modify-write half of [`Self::update_thread_with_recency`], for callers already
    /// holding the thread lock — a turn commit folds its usage through here inside the same lock
    /// that ordered its history append.
    async fn update_thread_with_recency_unlocked<F>(
        &self,
        project: ProjectId,
        thread: ThreadId,
        recency: ThreadRecency,
        f: F,
    ) -> Result<ThreadMutation, PersistError>
    where
        F: FnOnce(&mut ThreadFile),
    {
        let Some(before) = self.load_thread_unlocked(project, thread).await? else {
            return Ok(ThreadMutation::Missing);
        };
        let mut after = before.clone();
        f(&mut after);

        // The store, not mutation closures, owns both ordering fields.
        after.revision = before.revision;
        after.updated_at = before.updated_at;
        if after.id != before.id
            || after.project_id != before.project_id
            || after.version != before.version
            || after.created_at != before.created_at
        {
            return Err(PersistError::Invalid(format!(
                "thread metadata mutation attempted to change store-owned identity for {thread}"
            )));
        }
        let durable_fields_changed = after != before;
        match recency {
            ThreadRecency::Preserve => {}
            ThreadRecency::TouchIfChanged if durable_fields_changed => {
                after.updated_at = next_recency(before.updated_at)?;
            }
            ThreadRecency::RecordActivity => after.updated_at = next_recency(before.updated_at)?,
            ThreadRecency::RestoreActivity(activity_at) if activity_at > before.updated_at => {
                after.updated_at = activity_at;
            }
            ThreadRecency::RestoreActivity(_) => {}
            ThreadRecency::TouchIfChanged => {}
        }

        if after == before {
            return Ok(ThreadMutation::Unchanged {
                current: Box::new(before),
            });
        }
        after.revision = before
            .revision
            .checked_add(1)
            .filter(|revision| *revision <= MAX_THREAD_REVISION)
            .ok_or_else(|| {
                PersistError::Invalid(format!("thread metadata revision exhausted for {thread}"))
            })?;
        self.save_thread_unlocked(project, &after).await?;
        Ok(ThreadMutation::Changed {
            before: Box::new(before),
            after: Box::new(after),
        })
    }

    // ---- Authoritative turn history (spec §5.4 H1) ----
    //
    // Split across two files by how their size behaves. `history.jsonl` is the **index**: one
    // strictly bounded record per turn, appended. `turns/<turn_id>.jsonl` is the **payload**:
    // everything whose size is a function of what the agent did, written atomically.
    //
    // The split closes a live corruption path. A whole turn appended with one `write_all` can be
    // torn by a crash or a full disk; the next append concatenates onto the partial line, the
    // merged garbage line is no longer last, and the tolerance for a torn *final* line no longer
    // covers it — so the thread's entire history becomes unreadable. Probability scales with turn
    // size, so it was likeliest on the threads with the largest command output. Temp file + fsync +
    // rename makes a payload complete or absent, and what remains on the append-only path is a
    // bounded record the torn-final-line tolerance is adequate for.

    /// Append a completed `Turn`: payload first, index last.
    ///
    /// A crash between the two leaves a payload file no turn record references. It is invisible to
    /// every read path, because reads start from the index — so the worst case is a wasted file
    /// rather than an unreadable thread.
    ///
    /// The index record is a pre-serialized `JSON + "\n"` written with a **single** `write_all` to
    /// a file opened `O_APPEND`, so on a local POSIX filesystem the offset-seek + write is atomic
    /// against concurrent writers and a process kill leaves the line all-or-nothing. The per-thread
    /// lock is still used to order appends against aggregate repair. This does not survive power
    /// loss (page cache) — the tolerant loader handles a torn final line. On NFS/network storage
    /// the atomicity guarantee does not hold (out of scope, §1.2 local-first).
    pub async fn append_turn(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: &Turn,
    ) -> Result<(), PersistError> {
        self.append_turn_with_diffs(project, thread, turn, &[])
            .await
    }

    pub async fn append_turn_with_diffs(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: &Turn,
        captured: &[CapturedDiffRecord],
    ) -> Result<(), PersistError> {
        self.ensure_migrated(project, thread).await;
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        self.append_turn_unlocked(project, thread, turn, captured)
            .await
    }

    /// Write the history header if this thread has no index file yet.
    ///
    /// Idempotent and called under the thread lock, so the header is written exactly once even
    /// though both thread creation and a first append can be the one to need it.
    async fn ensure_history_header_unlocked(
        &self,
        project: ProjectId,
        thread: ThreadId,
        created_at: DateTime<Utc>,
    ) -> Result<(), PersistError> {
        let paths = self.thread_paths(project, thread).await;
        if paths.layout() == ThreadLayout::Flat || path_exists(&paths.history()).await {
            return Ok(());
        }
        atomic_write(
            &paths.history(),
            HistoryHeader::new(thread, created_at).line()?.as_bytes(),
        )
        .await
    }

    /// The two-file commit described on [`Self::append_turn`], for callers already holding the
    /// thread lock.
    async fn append_turn_unlocked(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: &Turn,
        captured: &[CapturedDiffRecord],
    ) -> Result<(), PersistError> {
        let paths = self.thread_paths(project, thread).await;
        let path = paths.history();

        let line = match paths.layout() {
            // A thread whose format 1 history could not be migrated (an unparseable interior line)
            // keeps behaving exactly as it does today rather than losing the append.
            ThreadLayout::Flat => {
                let persisted = history::turn_with_inline_diffs(turn, captured)?;
                let mut line = serde_json::to_string(&persisted)
                    .map_err(|e| PersistError::Serialize(e.to_string()))?;
                line.push('\n');
                line
            }
            ThreadLayout::Directory => {
                let payload_path = paths.turn_payload(turn.id);
                if path_exists(&payload_path).await {
                    // A repeat of a turn whose payload is already durable. The index folds turn
                    // records first-wins, so replacing the payload here would leave the winning
                    // record describing bytes it never indexed.
                    //
                    // The one case where the two diverge is a *retry* after the index append
                    // failed: the second attempt skips the payload write and appends its own
                    // record, so the record that wins was not the one whose write produced the
                    // file. That is sound only because a retry re-serializes the same turn, making
                    // the bytes identical — a caller that re-appended a *different* turn under an
                    // id already on disk would be defeating the first-wins rule, not exercising
                    // this path.
                    tracing::warn!(
                        %project,
                        %thread,
                        turn_id = %turn.id,
                        action = "append_turn",
                        "turn payload already exists; keeping the durable one"
                    );
                } else {
                    atomic_write(
                        &payload_path,
                        &history::payload_file_bytes_with_diffs(turn, captured)?,
                    )
                    .await?;
                }
                let mut line = String::new();
                if !path_exists(&path).await {
                    line.push_str(&HistoryHeader::new(thread, turn.started_at).line()?);
                }
                line.push_str(&TurnRecord::from_turn(turn).line()?);
                line
            }
        };
        let appended_len = line.len() as u64;
        let bytes = line.into_bytes();

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| PersistError::Io(e.to_string()))?;
        }

        let meta_before = self.history_file_meta(&path).await.ok().flatten();
        let write_path = path.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&write_path)?;
            file.write_all(&bytes)
        })
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?
        .map_err(|e| PersistError::Io(e.to_string()))?;

        let meta_after = self.history_file_meta(&path).await.ok().flatten();
        self.update_history_cache_after_append(
            project,
            thread,
            turn,
            meta_before,
            meta_after,
            appended_len,
        )
        .await;
        Ok(())
    }

    /// Append a completed turn and fold its usage into metadata under one per-thread lock.
    ///
    /// The JSONL append still happens first (H3). If the following atomic metadata write fails,
    /// the returned outcome records that history was appended so callers can report the degraded
    /// state accurately and a later aggregate repair can recover it.
    pub async fn append_turn_and_update_aggregates(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: &Turn,
    ) -> Result<TurnCommitOutcome, PersistError> {
        self.append_turn_with_diffs_and_update_aggregates(project, thread, turn, &[])
            .await
    }

    pub async fn append_turn_with_diffs_and_update_aggregates(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: &Turn,
        captured: &[CapturedDiffRecord],
    ) -> Result<TurnCommitOutcome, PersistError> {
        self.ensure_migrated(project, thread).await;
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        self.append_turn_unlocked(project, thread, turn, captured)
            .await?;

        let should_record = matches!(
            turn.status.kind,
            giskard_core::turn::TurnStatusKind::Completed
                | giskard_core::turn::TurnStatusKind::Interrupted
        );
        let model = turn.model.clone();
        let usage = turn.usage;
        let mutation = self
            .update_thread_with_recency_unlocked(
                project,
                thread,
                ThreadRecency::RecordActivity,
                move |thread| {
                    if should_record {
                        match model.as_known() {
                            Some(model) => {
                                thread.tokens.record(&model.provider, &model.model, &usage)
                            }
                            None => thread.tokens.record_unattributed(&usage),
                        }
                    }
                },
            )
            .await;
        Ok(match mutation {
            Ok(mutation) => TurnCommitOutcome::MetadataMutation(mutation),
            Err(error) => TurnCommitOutcome::MetadataFailed(error),
        })
    }

    /// Read and parse the history from disk, retrying while the index file changes underneath.
    ///
    /// Format 1 parses whole turns from the one file; format 2 parses the index and then opens one
    /// payload file per turn.
    async fn load_all_turns_uncached(
        &self,
        paths: &ThreadPaths,
        mut meta_before: HistoryFileMeta,
    ) -> Result<Option<(Vec<Turn>, HistoryFileMeta)>, PersistError> {
        let path = paths.history();
        for attempt in 0..3 {
            let data = match tokio::fs::read_to_string(&path).await {
                Ok(d) => d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(PersistError::Io(e.to_string())),
            };
            let Some(meta_after) = self.history_file_meta(&path).await? else {
                return Ok(None);
            };
            if meta_after != meta_before {
                if attempt < 2 {
                    meta_before = meta_after;
                    continue;
                }
                return Err(PersistError::Io(
                    "history file changed while loading; retry limit exceeded".into(),
                ));
            }
            let turns = match paths.layout() {
                ThreadLayout::Flat => parse_turn_history(&path, &data)?,
                ThreadLayout::Directory => assemble_turns(paths, &path, &data).await?,
            };
            return Ok(Some((turns, meta_after)));
        }
        Err(PersistError::Io(
            "history file changed while loading; retry limit exceeded".into(),
        ))
    }

    /// The bounded index rows for a thread, without opening a single payload file.
    ///
    /// Everything aggregate repair needs — `usage`, `model`, `status`, and the turn timestamps —
    /// is a turn-record field, so repair never has to read the agent-driven half of history.
    async fn load_turn_records_unlocked(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Vec<TurnRecord>, PersistError> {
        let started_at = std::time::Instant::now();
        let paths = self.thread_paths(project, thread).await;
        let path = paths.history();
        let data = match tokio::fs::read_to_string(&path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(PersistError::Io(e.to_string())),
        };
        let records = match paths.layout() {
            ThreadLayout::Directory => history::parse_history_index(&path, &data),
            ThreadLayout::Flat => Ok(parse_turn_history(&path, &data)?
                .iter()
                .map(TurnRecord::from_turn)
                .collect()),
        }?;
        tracing::debug!(
            %project,
            %thread,
            action = "load_turn_records",
            records = records.len(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "loaded bounded history index"
        );
        Ok(records)
    }

    /// Load the bounded history index without opening any per-turn payload files.
    ///
    /// This is an index snapshot from one read, not the consistency boundary for M5's transaction:
    /// that milestone still owns the history snapshot/range API and its relationship to a live cut.
    ///
    /// M5's transactional bootstrap will take ranges from this projection and fetch only their
    /// corresponding payloads. Exposing the primitive now also keeps ordinary pagination bounded
    /// on cold reads.
    pub async fn load_turn_records(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Vec<TurnRecord>, PersistError> {
        self.ensure_migrated(project, thread).await;
        self.load_turn_records_unlocked(project, thread).await
    }

    /// Load one captured diff from an indexed immutable turn payload.
    pub async fn load_captured_diff(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: TurnId,
        diff_id: &DiffId,
    ) -> Result<Option<CapturedDiffContent>, PersistError> {
        self.ensure_migrated(project, thread).await;
        let records = self.load_turn_records_unlocked(project, thread).await?;
        if !records.iter().any(|record| record.turn_id == turn) {
            return Ok(None);
        }
        let paths = self.thread_paths(project, thread).await;
        if paths.layout() == ThreadLayout::Flat {
            let path = paths.history();
            let data = match tokio::fs::read_to_string(&path).await {
                Ok(data) => data,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(PersistError::Io(error.to_string())),
            };
            let turns = parse_turn_history(&path, &data)?;
            let Some(turn) = turns.iter().find(|candidate| candidate.id == turn) else {
                return Ok(None);
            };
            return Ok(history::captured_diff_contents(turn).remove(diff_id));
        }
        let payload_path = paths.turn_payload(turn);
        let Some(payload) = history::read_turn_payload(&payload_path).await? else {
            return Ok(None);
        };
        Ok(payload.diff_contents.get(diff_id).cloned())
    }

    /// Load one terminal command's retained output from an indexed immutable turn payload.
    pub async fn load_command_output(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: TurnId,
        item_id: ItemId,
    ) -> Result<Option<StoredCommandOutput>, PersistError> {
        self.ensure_migrated(project, thread).await;
        let records = self.load_turn_records_unlocked(project, thread).await?;
        if !records.iter().any(|record| record.turn_id == turn) {
            return Ok(None);
        }
        let paths = self.thread_paths(project, thread).await;
        let loaded_items = if paths.layout() == ThreadLayout::Flat {
            let path = paths.history();
            let data = match tokio::fs::read_to_string(&path).await {
                Ok(data) => data,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(PersistError::Io(error.to_string())),
            };
            parse_turn_history(&path, &data)?
                .into_iter()
                .find(|candidate| candidate.id == turn)
                .map(|turn| turn.items)
        } else {
            let payload_path = paths.turn_payload(turn);
            history::read_turn_payload(&payload_path)
                .await?
                .map(|payload| payload.items)
        };
        let Some(item) =
            loaded_items.and_then(|items| items.into_iter().find(|item| item.id == item_id))
        else {
            return Ok(None);
        };
        let ItemPayload::CommandExecution {
            output,
            output_truncated,
            output_original_bytes,
            output_original_lines,
            status,
            ..
        } = item.payload
        else {
            return Ok(None);
        };
        if status
            .as_deref()
            .is_some_and(giskard_core::item::command_status_is_running)
        {
            return Ok(None);
        }
        let descriptor = crate::command_output_descriptor(
            &output,
            output_truncated,
            output_original_bytes,
            output_original_lines,
            true,
        )
        .map_err(|error| {
            PersistError::Invalid(format!(
                "command output {item_id} in turn {turn} has invalid metadata: {error}"
            ))
        })?;
        Ok(Some(StoredCommandOutput {
            output,
            output_truncated,
            original_bytes: descriptor.original_bytes,
            original_lines: descriptor.original_lines,
        }))
    }

    /// Load one terminal tool's complete JSON output from an indexed immutable turn payload.
    pub async fn load_tool_output(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: TurnId,
        item_id: ItemId,
    ) -> Result<Option<StoredToolOutput>, PersistError> {
        self.ensure_migrated(project, thread).await;
        let records = self.load_turn_records_unlocked(project, thread).await?;
        if !records.iter().any(|record| record.turn_id == turn) {
            return Ok(None);
        }
        let paths = self.thread_paths(project, thread).await;
        let loaded_items = if paths.layout() == ThreadLayout::Flat {
            let path = paths.history();
            let data = match tokio::fs::read_to_string(&path).await {
                Ok(data) => data,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(PersistError::Io(error.to_string())),
            };
            parse_turn_history(&path, &data)?
                .into_iter()
                .find(|candidate| candidate.id == turn)
                .map(|turn| turn.items)
        } else {
            let payload_path = paths.turn_payload(turn);
            history::read_turn_payload(&payload_path)
                .await?
                .map(|payload| payload.items)
        };
        let Some(item) =
            loaded_items.and_then(|items| items.into_iter().find(|item| item.id == item_id))
        else {
            return Ok(None);
        };
        let ItemPayload::ToolCall { output, status, .. } = item.payload else {
            return Ok(None);
        };
        if status
            .as_deref()
            .is_some_and(giskard_core::item::tool_status_is_running)
        {
            return Ok(None);
        }
        let Some(output) = output else {
            return Ok(None);
        };
        let (bytes, descriptor) = giskard_core::item::serialize_tool_output(&output)
            .map_err(|error| PersistError::Invalid(error.to_string()))?;
        Ok(Some(StoredToolOutput { bytes, descriptor }))
    }

    /// Load up to `limit` readable turns ending before `end`, backfilling across isolated damaged
    /// payloads so a page cannot be empty while older readable history is stranded behind it.
    async fn load_history_page_records(
        &self,
        project: ProjectId,
        thread: ThreadId,
        records: &[TurnRecord],
        end: usize,
        limit: usize,
    ) -> (Vec<Turn>, usize, bool) {
        let paths = self.thread_paths(project, thread).await;
        let mut turns = Vec::with_capacity(limit.min(end));
        let mut cursor = end;
        let mut attempted_records = 0;
        while cursor > 0 && turns.len() < limit {
            cursor -= 1;
            attempted_records += 1;
            if let Some(turn) = assemble_turn_record(&paths, records[cursor].clone()).await {
                turns.push(turn);
            }
        }
        turns.reverse();
        (turns, attempted_records, cursor > 0)
    }

    async fn load_selected_turn_records(
        &self,
        project: ProjectId,
        thread: ThreadId,
        records: Vec<TurnRecord>,
    ) -> Vec<Turn> {
        let started_at = std::time::Instant::now();
        let selected_records = records.len();
        let paths = self.thread_paths(project, thread).await;
        let turns = assemble_turn_records(&paths, records).await;
        tracing::debug!(
            %project,
            %thread,
            action = "load_selected_turn_payloads",
            selected_records,
            returned_turns = turns.len(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "loaded selected turn payloads"
        );
        turns
    }

    /// Load every persisted turn from the JSONL history, in order (H4).
    ///
    /// Tolerates a single unparseable **final** line (a torn append after power loss): it is
    /// skipped with a warning. A bad **interior** line is real corruption and returns `Corrupt`.
    pub async fn load_all_turns(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Vec<Turn>, PersistError> {
        self.ensure_migrated(project, thread).await;
        self.load_all_turns_unlocked(project, thread).await
    }

    /// [`Self::load_all_turns`] without the migration check, for callers already holding the
    /// thread lock — which migration itself takes, so calling the public one there would deadlock.
    async fn load_all_turns_unlocked(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Vec<Turn>, PersistError> {
        let Some(entry) = self.current_history_cache_entry(project, thread).await? else {
            return Ok(vec![]);
        };
        Ok(entry.turns.read().await.clone())
    }

    /// Load a page of history for display (H4): the last `limit` turns ending just before the
    /// `before` cursor (a `TurnId`), or the tail when `before` is `None`. Returns the page (oldest
    /// first) and `has_more` (whether older turns exist before the page).
    pub async fn load_history(
        &self,
        project: ProjectId,
        thread: ThreadId,
        before: Option<TurnId>,
        limit: usize,
    ) -> Result<(Vec<Turn>, bool), PersistError> {
        self.ensure_migrated(project, thread).await;
        let paths = self.thread_paths(project, thread).await;
        if paths.layout() == ThreadLayout::Directory {
            let started_at = std::time::Instant::now();
            let records = self.load_turn_records_unlocked(project, thread).await?;
            let total_records = records.len();
            let end = match before {
                Some(cursor) => records
                    .iter()
                    .position(|record| record.turn_id == cursor)
                    .unwrap_or(records.len()),
                None => records.len(),
            };
            let (turns, attempted_records, has_more) = self
                .load_history_page_records(project, thread, &records, end, limit)
                .await;
            tracing::debug!(
                %project,
                %thread,
                action = "load_history_page",
                total_records,
                attempted_records,
                returned_turns = turns.len(),
                elapsed_ms = started_at.elapsed().as_millis(),
                "loaded bounded history page"
            );
            return Ok((turns, has_more));
        }
        let Some(entry) = self.current_history_cache_entry(project, thread).await? else {
            return Ok((vec![], false));
        };
        let all = entry.turns.read().await;
        let end = match before {
            Some(cursor) => all.iter().position(|t| t.id == cursor).unwrap_or(all.len()),
            None => all.len(),
        };
        let start = end.saturating_sub(limit);
        Ok((all[start..end].to_vec(), start > 0))
    }

    /// Load the turns persisted strictly after the `after` cursor (a `TurnId`), oldest-first — the
    /// delta an incremental reconnect needs. Returns `None` when the cursor is not found in history
    /// (the client's cursor is stale or from another thread), signalling the caller to fall back to
    /// a full snapshot rather than guessing.
    pub async fn load_turns_after(
        &self,
        project: ProjectId,
        thread: ThreadId,
        after: TurnId,
    ) -> Result<Option<Vec<Turn>>, PersistError> {
        self.ensure_migrated(project, thread).await;
        let paths = self.thread_paths(project, thread).await;
        if paths.layout() == ThreadLayout::Directory {
            let started_at = std::time::Instant::now();
            let records = self.load_turn_records_unlocked(project, thread).await?;
            let total_records = records.len();
            let Some(index) = records.iter().position(|record| record.turn_id == after) else {
                tracing::debug!(
                    %project,
                    %thread,
                    %after,
                    action = "load_turns_after",
                    total_records,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "history cursor was not found"
                );
                return Ok(None);
            };
            let selected = records[index + 1..].to_vec();
            let selected_records = selected.len();
            let turns = self
                .load_selected_turn_records(project, thread, selected)
                .await;
            tracing::debug!(
                %project,
                %thread,
                %after,
                action = "load_turns_after",
                total_records,
                selected_records,
                returned_turns = turns.len(),
                elapsed_ms = started_at.elapsed().as_millis(),
                "loaded selected history suffix"
            );
            return Ok(Some(turns));
        }
        let Some(entry) = self.current_history_cache_entry(project, thread).await? else {
            return Ok(None);
        };
        let all = entry.turns.read().await;
        match all.iter().position(|t| t.id == after) {
            Some(index) => Ok(Some(all[index + 1..].to_vec())),
            None => Ok(None),
        }
    }

    /// Rebuild the metadata token aggregates from the authoritative history (H3), for repair when a
    /// crash landed between the history append and the metadata update.
    ///
    /// Reads the **index only**: the ledger needs `usage`, `model` and `status`, and restoring the
    /// thread's recency needs the turn timestamps — all of them turn-record fields. No payload file
    /// is opened. An internal optimization, with no change to what repair produces.
    pub async fn recompute_aggregates(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<ThreadMutation, PersistError> {
        self.ensure_migrated(project, thread).await;
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        let records = self.load_turn_records_unlocked(project, thread).await?;
        let latest_activity = records
            .iter()
            .map(|record| record.completed_at.unwrap_or(record.started_at))
            .max();
        let recency = latest_activity
            .map(ThreadRecency::RestoreActivity)
            .unwrap_or(ThreadRecency::Preserve);
        self.update_thread_with_recency_unlocked(project, thread, recency, move |tf| {
            let mut ledger = TokenLedger::default();
            for record in &records {
                if matches!(
                    record.status.kind,
                    giskard_core::turn::TurnStatusKind::Completed
                        | giskard_core::turn::TurnStatusKind::Interrupted
                ) {
                    match record.model.as_known() {
                        Some(model) => ledger.record(&model.provider, &model.model, &record.usage),
                        None => ledger.record_unattributed(&record.usage),
                    }
                }
            }
            tf.tokens = ledger;
        })
        .await
    }

    /// List all threads for a project (by reading the directory), in either layout, once each.
    ///
    /// A `<ulid>/` directory is a format 2 thread and a `<ulid>.json` file is a format 1 one; both
    /// exist at once only in the window between a migration's commit rename and its relocation of
    /// the originals, so the two are deduped by id. Everything else in the directory —
    /// `<ulid>.migrating/`, `<ulid>.deleting/`, `<ulid>.jsonl`, any `*.corrupt-*` — carries a
    /// suffix that stops the name parsing as a bare ULID, so none of it is ever enumerated.
    pub async fn list_threads(&self, project: ProjectId) -> Result<Vec<ThreadId>, PersistError> {
        let dir = self.threads_dir(project);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(PersistError::Io(e.to_string())),
        };
        let mut ids = vec![];
        let mut seen = HashSet::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?
        {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_dir = matches!(entry.file_type().await, Ok(file_type) if file_type.is_dir());
            let parsed = if is_dir {
                name.parse::<ulid::Ulid>().ok()
            } else {
                name.strip_suffix(".json")
                    .and_then(|stem| stem.parse::<ulid::Ulid>().ok())
            };
            if let Some(ulid) = parsed
                && seen.insert(ulid)
            {
                ids.push(ThreadId(ulid));
            }
        }
        Ok(ids)
    }

    /// Delete a thread: its directory, and any format 1 originals still beside it.
    ///
    /// A thread is a directory now, but this is deliberately **not** a naive `remove_dir_all`. The
    /// catalog is derived from metadata, so a partial failure must leave the thread visible and
    /// retryable rather than deleting the catalog record while history survives as an invisible
    /// orphan — and `remove_dir_all` gives no ordering guarantee.
    ///
    /// Instead the directory is renamed to `<thread_id>.deleting/` first. The rename is atomic and
    /// immediately drops the thread out of enumeration, after which the recursive removal can fail
    /// and be retried harmlessly. Format 1 originals keep the ordering they always had: history
    /// first, metadata last.
    pub async fn delete_thread(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<(), PersistError> {
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        let paths = self.current_thread_paths(project, thread);
        let pending = paths.deleting_dir();

        if is_dir(&paths.dir()).await {
            // A leftover from an interrupted delete would make the rename fail with `ENOTEMPTY`
            // and strand the thread; it holds nothing this delete is not about to remove anyway.
            remove_dir_all_if_present(&pending).await?;
            tokio::fs::rename(paths.dir(), &pending)
                .await
                .map_err(|e| PersistError::Io(e.to_string()))?;
        }
        remove_dir_all_if_present(&paths.migrating_dir()).await?;

        for path in [paths.flat_history(), paths.flat_metadata()] {
            match tokio::fs::remove_file(&path).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(PersistError::Io(e.to_string())),
            }
        }
        remove_dir_all_if_present(&pending).await?;

        self.invalidate_history_cache(project, thread).await;
        self.clear_unmigratable(project, thread).await;
        Ok(())
    }

    // ---- Storage-layout maintenance (`giskard-admin`, spec §5.5) ----

    /// Whether this thread still holds pre-migration originals under `legacy/`.
    pub async fn has_legacy_data(&self, project: ProjectId, thread: ThreadId) -> bool {
        is_dir(&self.current_thread_paths(project, thread).legacy_dir()).await
    }

    /// What [`Self::migrate_thread_layout`] would do to this thread, without doing it.
    ///
    /// The same classifier the migration itself uses, so a dry run cannot disagree with the run it
    /// is previewing. It cannot predict a *failure*: a thread whose format 1 history will not parse
    /// plans as `Migrated` and reports as an error only when the work is actually attempted.
    pub async fn planned_migration(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> MigrationOutcome {
        migrate::planned_outcome(&self.threads_dir(project), thread).await
    }

    /// Migrate one thread to the current layout, reporting what it did.
    ///
    /// The same work `ensure_migrated` does on open, exposed so an operator can do it in bulk (and
    /// see the failures) instead of discovering it one thread at a time.
    pub async fn migrate_thread_layout(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<MigrationOutcome, PersistError> {
        self.clear_unmigratable(project, thread).await;
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        let outcome = migrate::migrate_thread(&self.threads_dir(project), thread).await?;
        if matches!(
            outcome,
            MigrationOutcome::Migrated | MigrationOutcome::FinishedLegacyMove
        ) {
            self.invalidate_history_cache(project, thread).await;
        }
        Ok(outcome)
    }

    /// Delete one thread's retained format 1 originals.
    ///
    /// Separate and explicit because it is the only step in this layout change that can destroy
    /// transcript history — the part of a user's data nothing else can reconstruct. Returns whether
    /// anything was removed.
    pub async fn prune_legacy_data(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<bool, PersistError> {
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        let legacy = self.current_thread_paths(project, thread).legacy_dir();
        if !is_dir(&legacy).await {
            return Ok(false);
        }
        tokio::fs::remove_dir_all(&legacy)
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?;
        Ok(true)
    }

    /// Remove payload files no turn record references.
    ///
    /// An orphan is a payload written by a turn commit that crashed before appending its index
    /// record. It is harmless — reads start from the index, so nothing can see it — so this only
    /// reclaims space, and nothing calls it automatically on open.
    ///
    /// **The caller must hold the data-directory lock** ([`crate::lock::DataDirLock`]) for a real
    /// run. Deleting an unreferenced payload would otherwise race a turn commit in progress and
    /// delete the file between its two writes; the per-thread lock taken below cannot prevent that,
    /// because it is an in-process `Mutex` and `giskard-admin` is a different process from
    /// `giskard-server`. With the directory locked, unreferenced means unreferenced: there is no
    /// in-flight commit an orphan could belong to.
    ///
    /// Returns the paths swept, or the paths that *would* be swept when `dry_run`.
    pub async fn sweep_orphan_payloads(
        &self,
        project: ProjectId,
        thread: ThreadId,
        dry_run: bool,
    ) -> Result<OrphanSweep, PersistError> {
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        let paths = self.current_thread_paths(project, thread);
        if !is_dir(&paths.dir()).await {
            return Ok(OrphanSweep::default());
        }
        let index_missing = !path_exists(&paths.history()).await;
        let referenced: HashSet<TurnId> = self
            .load_turn_records_unlocked(project, thread)
            .await?
            .into_iter()
            .map(|record| record.turn_id)
            .collect();

        let mut entries = match tokio::fs::read_dir(paths.turns_dir()).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(OrphanSweep::default());
            }
            Err(e) => return Err(PersistError::Io(e.to_string())),
        };
        let mut payloads = vec![];
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?
        {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(turn) = name
                .strip_suffix(".jsonl")
                .and_then(|stem| stem.parse::<ulid::Ulid>().ok())
                .map(TurnId)
            else {
                continue;
            };
            if referenced.contains(&turn) {
                continue;
            }
            payloads.push(entry.path());
        }

        // Every refusal below is a judgment about the *thread* — "these payloads may be the
        // surviving history" — not a failure of the sweep. So it is reported alongside the list it
        // is about, rather than raised in place of it. Raising would make a dry run refuse too,
        // and the whole point of the dry run is to show an operator the files a refusal names.
        let refusal = if index_missing {
            // The index is the *less* durable of the two files by design: bounded records on a
            // page-cached `O_APPEND` write, against payloads fsynced before their rename. A lost
            // index beside intact payloads is a recoverable transcript, and "nothing is referenced"
            // must never be read as "everything is unreferenced" — that would turn a
            // space-reclaiming command into the one thing in this store that can destroy history.
            Some(format!(
                "{}: thread has no history index",
                paths.dir().display()
            ))
        } else if referenced.is_empty() && !payloads.is_empty() {
            // Same signal as a missing index: an empty thread has no payload files in the first
            // place, so payloads beside an index that names no turns means the index lost them.
            Some(format!(
                "{}: history index references no turns, but {} payload file(s) exist",
                paths.dir().display(),
                payloads.len()
            ))
        } else if payloads.len() > MAX_PLAUSIBLE_ORPHANS {
            // Total index loss is the rarer failure. The durability asymmetry above loses a *tail
            // of appends* far more often than the whole file, and a tail of lost records leaves
            // that many fsynced payloads unreferenced. An orphan is otherwise produced one at a
            // time, by one crash landing between a turn's two writes, so a large set is itself the
            // evidence that the index is what was damaged.
            Some(format!(
                "{}: {} unreferenced payload file(s) at once is evidence of a truncated history \
                 index, not of {MAX_PLAUSIBLE_ORPHANS} or fewer orphaned commits",
                paths.dir().display(),
                payloads.len()
            ))
        } else {
            None
        };

        if refusal.is_none() && !dry_run {
            for path in &payloads {
                tokio::fs::remove_file(path)
                    .await
                    .map_err(|e| PersistError::Io(e.to_string()))?;
            }
        }
        Ok(OrphanSweep { payloads, refusal })
    }

    // ---- Token ledgers ----

    /// Load a project's token ledger.
    pub async fn load_project_tokens(
        &self,
        project: ProjectId,
    ) -> Result<Option<DailyTokenLedger>, PersistError> {
        read_json(&self.tokens_json_path(project)).await
    }

    /// Save a project's token ledger atomically.
    pub async fn save_project_tokens(
        &self,
        project: ProjectId,
        ledger: &DailyTokenLedger,
    ) -> Result<(), PersistError> {
        atomic_write_json(&self.tokens_json_path(project), ledger).await
    }

    /// Load the global token ledger.
    pub async fn load_global_tokens(&self) -> Result<Option<DailyTokenLedger>, PersistError> {
        read_json(&self.global_tokens_path()).await
    }

    /// Save the global token ledger atomically.
    pub async fn save_global_tokens(&self, ledger: &DailyTokenLedger) -> Result<(), PersistError> {
        atomic_write_json(&self.global_tokens_path(), ledger).await
    }

    // ---- Validation ----

    /// Validate all files, returning a list of errors for corrupt ones.
    pub async fn validate_all(&self) -> Vec<(PathBuf, String)> {
        let mut errors = vec![];

        // Project index.
        if let Err(e) = self.load_project_index().await {
            errors.push((self.projects_json_path(), e.to_string()));
        }

        // Each project.
        let index = self
            .load_project_index()
            .await
            .unwrap_or_else(|_| ProjectIndex {
                version: SCHEMA_VERSION,
                projects: vec![],
            });
        for entry in &index.projects {
            if let Err(e) = self.load_project(entry.id).await {
                errors.push((self.project_json_path(entry.id), e.to_string()));
            }

            // Thread metadata + authoritative history (H7: report the first bad JSONL line per
            // thread rather than quarantining whole histories).
            if let Ok(thread_ids) = self.list_threads(entry.id).await {
                for tid in thread_ids {
                    // Deliberately the non-migrating read: `validate` reports what is on disk, and
                    // migrating every format 1 thread as a side effect of inspecting it would make
                    // the report describe a store the operator did not ask to change.
                    if let Err(e) = self.load_thread_unlocked(entry.id, tid).await {
                        errors.push((self.thread_json_path(entry.id, tid).await, e.to_string()));
                    }
                    errors.extend(self.history_validation_errors(entry.id, tid).await);
                }
            }
        }

        errors
    }

    /// Every readability problem in one thread's history, named at the file that has it.
    ///
    /// Reported per turn rather than per thread, because that is how the layout fails: a damaged or
    /// missing payload takes down one turn while the index and every other turn stay readable, and
    /// an operator needs to be told *which* turn.
    async fn history_validation_errors(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Vec<(PathBuf, String)> {
        let paths = self.thread_paths(project, thread).await;
        let index_path = paths.history();
        let data = match tokio::fs::read_to_string(&index_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return vec![],
            Err(e) => return vec![(index_path, e.to_string())],
        };

        if paths.layout() == ThreadLayout::Flat {
            return match parse_turn_history(&index_path, &data) {
                Ok(_) => vec![],
                Err(e) => vec![(index_path, e.to_string())],
            };
        }

        let records = match history::parse_history_index(&index_path, &data) {
            Ok(records) => records,
            Err(e) => return vec![(index_path, e.to_string())],
        };

        let mut errors = vec![];
        for record in records {
            let payload_path = paths.turn_payload(record.turn_id);
            // Reported, not quarantined. Renaming the bad file aside here would make a *second*
            // `validate` run describe the same damage as a missing payload, losing the parse error
            // that says what is actually wrong with it.
            let payload = match tokio::fs::read_to_string(&payload_path).await {
                Ok(payload) => payload,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    errors.push((
                        payload_path,
                        format!("turn {} has no payload file", record.turn_id),
                    ));
                    continue;
                }
                Err(e) => {
                    errors.push((payload_path, e.to_string()));
                    continue;
                }
            };
            if let Err(e) = history::parse_turn_payload(&payload_path, &payload) {
                errors.push((payload_path, e.to_string()));
            }
        }
        errors
    }
}

#[cfg(test)]
mod git_workspace_tests {
    use super::*;

    /// The tag is what makes another strategy additive rather than a migration, so it has to be
    /// written, and the variant's own fields have to survive sitting beside it — `ThreadWorktree`
    /// denies unknown fields, and an internally tagged enum is exactly where that can go wrong.
    #[test]
    fn git_workspace_round_trips_with_its_strategy_tag() {
        let workspace = ThreadGitWorkspace::Worktree(ThreadWorktree {
            path: "/data/wt".into(),
            workspace: Some("/data/wt/packages/api".into()),
            branch: "giskard/worktree-01test".into(),
            base_commit: Some("e17b742".into()),
            repo_root: "/home/me/project".into(),
            common_dir: "/home/me/project/.git".into(),
            git_dir: "/home/me/project/.git/worktrees/t".into(),
        });

        let json = serde_json::to_value(&workspace).unwrap();
        assert_eq!(
            json["strategy"], "worktree",
            "the record names the strategy that produced it"
        );
        assert_eq!(json["path"], "/data/wt");
        assert_eq!(
            serde_json::from_value::<ThreadGitWorkspace>(json).unwrap(),
            workspace,
            "and reads back as the same variant"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::turn::Mode;
    use tempfile::TempDir;

    fn make_store() -> (TempDir, PersistStore) {
        let tmp = TempDir::new().unwrap();
        let store = PersistStore::new(tmp.path().to_path_buf());
        (tmp, store)
    }

    pub(super) fn test_model() -> ModelRef {
        ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        }
    }

    /// Write raw metadata bytes wherever this thread's metadata record belongs.
    pub(super) async fn write_thread_metadata(
        store: &PersistStore,
        project: ProjectId,
        thread: ThreadId,
        bytes: &[u8],
    ) {
        let path = store.thread_json_path(project, thread).await;
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(path, bytes).await.unwrap();
    }

    /// A whole `history.jsonl`: the header, then one bounded record per turn.
    pub(super) fn history_index_of(thread: ThreadId, turns: &[&Turn]) -> String {
        let mut out = HistoryHeader::new(thread, Utc::now()).line().unwrap();
        for turn in turns {
            out.push_str(&TurnRecord::from_turn(turn).line().unwrap());
        }
        out
    }

    /// Lay a thread out the way this store did before the per-turn payload split: a flat
    /// `threads/<id>.json` beside a flat `threads/<id>.jsonl` holding one whole `Turn` per line.
    pub(super) async fn write_format1_thread(
        store: &PersistStore,
        project: ProjectId,
        thread: &ThreadFile,
        turns: &[Turn],
    ) {
        let threads_dir = store.threads_dir(project);
        tokio::fs::create_dir_all(&threads_dir).await.unwrap();
        tokio::fs::write(
            threads_dir.join(format!("{}.json", thread.id)),
            serde_json::to_vec_pretty(thread).unwrap(),
        )
        .await
        .unwrap();
        let mut history = String::new();
        for turn in turns {
            history.push_str(&serde_json::to_string(turn).unwrap());
            history.push('\n');
        }
        tokio::fs::write(
            threads_dir.join(format!("{}.jsonl", thread.id)),
            history.as_bytes(),
        )
        .await
        .unwrap();
    }

    pub(super) fn test_thread(project_id: ProjectId, thread_id: ThreadId) -> ThreadFile {
        let now = Utc::now();
        ThreadFile {
            revision: 0,
            version: SCHEMA_VERSION,
            id: thread_id,
            project_id,
            title: "Thread".into(),
            harness_thread_id: "native-thread".into(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind: ThreadKind::Primary,
            mode: TurnMode::Known(Mode::Build),
            current_model: TurnModel::Known(test_model()),
            context_window: 128_000,
            model_context_windows: HashMap::new(),
            permission_preset: PermissionPreset::AskFirst,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
            archived: false,
            git_workspace: None,
        }
    }

    #[tokio::test]
    async fn create_thread_allocates_revision_one_and_rejects_collision() {
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let created = store
            .create_thread(project_id, test_thread(project_id, thread_id))
            .await
            .unwrap();
        assert_eq!(created.revision, 1);

        let error = store
            .create_thread(project_id, test_thread(project_id, thread_id))
            .await
            .unwrap_err();
        assert!(matches!(error, PersistError::Invalid(_)));
        assert_eq!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap()
                .revision,
            1
        );
    }

    /// The format 1 ordering has to survive: history first, metadata last, so a partial failure
    /// leaves the thread visible rather than deleting the catalog record while history remains.
    #[tokio::test]
    async fn delete_thread_keeps_metadata_when_format1_history_removal_fails() {
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        write_format1_thread(&store, project_id, &test_thread(project_id, thread_id), &[]).await;
        // A directory where the history file belongs makes its removal fail without making the
        // metadata removal fail.
        let history = store
            .threads_dir(project_id)
            .join(format!("{thread_id}.jsonl"));
        tokio::fs::remove_file(&history).await.unwrap();
        tokio::fs::create_dir_all(&history).await.unwrap();

        let error = store
            .delete_thread(project_id, thread_id)
            .await
            .unwrap_err();
        assert!(matches!(error, PersistError::Io(_)));
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some(),
            "a partial delete must leave the catalog record visible for retry"
        );
    }

    /// Deleting a thread directory renames it out of enumeration *first*, so the recursive removal
    /// that follows can fail and be retried without ever leaving an invisible orphan behind.
    #[tokio::test]
    async fn delete_thread_renames_the_directory_out_of_enumeration_before_removing_it() {
        use giskard_core::token::TokenUsage;
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        store
            .create_thread(project_id, test_thread(project_id, thread_id))
            .await
            .unwrap();
        let turn = make_turn(TokenUsage::new(100, 10));
        store
            .append_turn(project_id, thread_id, &turn)
            .await
            .unwrap();
        let paths = store.current_thread_paths(project_id, thread_id);
        assert!(paths.turn_payload(turn.id).exists());

        // A leftover from an earlier interrupted delete must not strand the retry.
        tokio::fs::create_dir_all(paths.deleting_dir().join("turns"))
            .await
            .unwrap();

        store.delete_thread(project_id, thread_id).await.unwrap();
        assert!(!paths.dir().exists());
        assert!(!paths.deleting_dir().exists());
        assert!(store.list_threads(project_id).await.unwrap().is_empty());
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn no_op_mutation_does_not_write_or_advance_revision() {
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        store
            .create_thread(project_id, test_thread(project_id, thread_id))
            .await
            .unwrap();
        let path = store.thread_json_path(project_id, thread_id).await;
        let before = tokio::fs::read(&path).await.unwrap();

        let mutation = store
            .update_thread(project_id, thread_id, |_| {})
            .await
            .unwrap();
        assert!(matches!(
            mutation,
            ThreadMutation::Unchanged { ref current } if current.revision == 1
        ));
        assert_eq!(tokio::fs::read(path).await.unwrap(), before);
    }

    #[tokio::test]
    async fn recency_policy_is_monotonic_and_revision_is_checked() {
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let mut thread = test_thread(project_id, thread_id);
        thread.updated_at = Utc::now() + TimeDelta::days(1);
        store.create_thread(project_id, thread).await.unwrap();

        let preserved = store
            .update_thread(project_id, thread_id, |thread| {
                thread.title = "Background".into()
            })
            .await
            .unwrap();
        let preserved = preserved.into_current().unwrap();
        let original_recency = preserved.updated_at;
        assert_eq!(preserved.revision, 2);

        let touched = store
            .update_thread_with_recency(
                project_id,
                thread_id,
                ThreadRecency::TouchIfChanged,
                |thread| thread.mode = TurnMode::Known(Mode::Plan),
            )
            .await
            .unwrap()
            .into_current()
            .unwrap();
        assert!(touched.updated_at > original_recency);
        assert_eq!(touched.revision, 3);

        let activity = store
            .update_thread_with_recency(
                project_id,
                thread_id,
                ThreadRecency::RecordActivity,
                |_| {},
            )
            .await
            .unwrap()
            .into_current()
            .unwrap();
        assert!(activity.updated_at > touched.updated_at);
        assert_eq!(activity.revision, 4);

        let mut exhausted = activity;
        exhausted.revision = MAX_THREAD_REVISION - 1;
        store.save_thread(project_id, &exhausted).await.unwrap();
        let last = store
            .update_thread(project_id, thread_id, |thread| {
                thread.title = "Last exact revision".into()
            })
            .await
            .unwrap()
            .into_current()
            .unwrap();
        assert_eq!(last.revision, MAX_THREAD_REVISION);
        let error = store
            .update_thread(project_id, thread_id, |thread| {
                thread.title = "Overflow".into()
            })
            .await
            .unwrap_err();
        assert!(matches!(error, PersistError::Invalid(_)));
    }

    #[tokio::test]
    async fn mutation_rejects_store_owned_identity_changes() {
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        store
            .create_thread(project_id, test_thread(project_id, thread_id))
            .await
            .unwrap();
        let error = store
            .update_thread(project_id, thread_id, |thread| thread.id = ThreadId::new())
            .await
            .unwrap_err();
        assert!(matches!(error, PersistError::Invalid(_)));
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn non_selected_model_window_is_cached_without_changing_visible_capacity() {
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let mut thread = test_thread(project_id, thread_id);
        let other = ModelRef {
            provider: "proxy".into(),
            model: "other".into(),
            reasoning_effort: None,
        };
        thread.record_model_context_window(&other, 64_000);
        assert_eq!(thread.context_window, 128_000);
        assert_eq!(thread.model_context_windows["proxy"]["other"], 64_000);
    }

    #[tokio::test]
    async fn create_and_load_project() {
        let (_tmp, store) = make_store();
        let id = ProjectId::new();
        store
            .create_project(id, "test-project", "/tmp/test")
            .await
            .unwrap();

        let index = store.load_project_index().await.unwrap();
        assert_eq!(index.projects.len(), 1);
        assert_eq!(index.projects[0].name, "test-project");

        let config = store.load_project(id).await.unwrap().unwrap();
        assert_eq!(config.name, "test-project");
        assert_eq!(config.harness, "codex");
    }

    #[tokio::test]
    async fn load_project_rejects_obsolete_permission_preset() {
        let (_tmp, store) = make_store();
        let id = ProjectId::new();
        let project = store
            .create_project(id, "test-project", "/tmp/test")
            .await
            .unwrap();

        let mut value = serde_json::to_value(&project).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("permission_preset".into(), serde_json::json!("auto"));
        tokio::fs::write(
            store.project_json_path(id),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .await
        .unwrap();

        let result = store.load_project(id).await;
        assert!(matches!(result.unwrap_err(), PersistError::Corrupt(_)));
    }

    #[tokio::test]
    async fn delete_project() {
        let (_tmp, store) = make_store();
        let id = ProjectId::new();
        store
            .create_project(id, "to-delete", "/tmp/test")
            .await
            .unwrap();

        store.delete_project(id).await.unwrap();

        let index = store.load_project_index().await.unwrap();
        assert!(index.projects.is_empty());
        assert!(store.load_project(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn newer_thread_metadata_version_is_rejected_without_quarantine() {
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let mut thread = test_thread(project_id, thread_id);
        thread.version = THREAD_METADATA_VERSION + 1;
        write_thread_metadata(
            &store,
            project_id,
            thread_id,
            &serde_json::to_vec(&thread).unwrap(),
        )
        .await;

        let error = store.load_thread(project_id, thread_id).await.unwrap_err();
        assert!(error.to_string().contains("newer than this build"));
        assert!(store.thread_json_path(project_id, thread_id).await.exists());
    }

    #[tokio::test]
    async fn save_and_load_thread() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        let tid = ThreadId::new();
        let now = Utc::now();
        let thread = ThreadFile {
            revision: 0,
            version: SCHEMA_VERSION,
            id: tid,
            project_id: pid,
            title: "Fix auth".into(),
            harness_thread_id: "th_abc".into(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind: ThreadKind::Primary,
            mode: TurnMode::Known(Mode::Build),
            current_model: TurnModel::Known(test_model()),
            context_window: 262_144,
            model_context_windows: HashMap::new(),
            permission_preset: PermissionPreset::AskFirst,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
            archived: false,
            git_workspace: None,
        };
        store.save_thread(pid, &thread).await.unwrap();
        let raw = tokio::fs::read_to_string(store.thread_json_path(pid, tid).await)
            .await
            .unwrap();
        assert!(!raw.contains("\"revision\""));

        let loaded = store.load_thread(pid, tid).await.unwrap().unwrap();
        assert_eq!(loaded.revision, 0);
        assert_eq!(loaded.title, "Fix auth");
        assert_eq!(loaded.harness_thread_id, "th_abc");
        assert_eq!(loaded.mode, TurnMode::Known(Mode::Build));

        let raw = tokio::fs::read_to_string(store.thread_json_path(pid, tid).await)
            .await
            .unwrap();
        assert!(raw.contains("\"permission_preset\""));
        assert!(!raw.contains("\"approval_policy\""));
    }

    #[tokio::test]
    async fn load_project_ignores_a_default_model_left_by_an_older_version() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        // Put the file back the way a Giskard that still stored a per-project model wrote it.
        let mut value =
            serde_json::to_value(store.load_project(pid).await.unwrap().unwrap()).unwrap();
        value.as_object_mut().unwrap().insert(
            "default_model".into(),
            serde_json::json!({"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null}),
        );
        tokio::fs::write(
            store.project_json_path(pid),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .await
        .unwrap();

        // Every project on disk predates the removal, and `deny_unknown_fields` would otherwise
        // make each one unloadable.
        let loaded = store
            .load_project(pid)
            .await
            .expect("a project written by an older Giskard still loads")
            .expect("project exists");
        assert_eq!(loaded.name, "proj");

        // The key is not carried forward — but only a write drops it, and nothing writes this file
        // on startup, so it survives on disk until the project is next modified.
        store.save_project(&loaded).await.unwrap();
        let raw = tokio::fs::read_to_string(store.project_json_path(pid))
            .await
            .unwrap();
        assert!(
            !raw.contains("default_model"),
            "rewriting the project drops the stale key: {raw}"
        );
    }

    #[tokio::test]
    async fn load_thread_accepts_legacy_approval_policy_field() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        let tid = ThreadId::new();
        let now = Utc::now();
        let thread = ThreadFile {
            revision: 0,
            version: SCHEMA_VERSION,
            id: tid,
            project_id: pid,
            title: "Fix auth".into(),
            harness_thread_id: "th_abc".into(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind: ThreadKind::Primary,
            mode: TurnMode::Known(Mode::Build),
            current_model: TurnModel::Known(test_model()),
            context_window: 262_144,
            model_context_windows: HashMap::new(),
            permission_preset: PermissionPreset::AskFirst,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
            archived: false,
            git_workspace: None,
        };
        let mut value = serde_json::to_value(&thread).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("permission_preset");
        object.insert("approval_policy".into(), serde_json::json!("read_only"));

        write_thread_metadata(
            &store,
            pid,
            tid,
            &serde_json::to_vec_pretty(&value).unwrap(),
        )
        .await;

        let loaded = store.load_thread(pid, tid).await.unwrap().unwrap();
        assert_eq!(loaded.permission_preset, PermissionPreset::AskFirst);

        let legacy_auto =
            deserialize_persisted_permission_preset(serde_json::Value::String("auto".into()))
                .unwrap();
        assert_eq!(legacy_auto, PermissionPreset::AutoApprove);
    }

    #[tokio::test]
    async fn load_thread_requires_permission_preset() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        let tid = ThreadId::new();
        let now = Utc::now();
        let thread = ThreadFile {
            revision: 0,
            version: SCHEMA_VERSION,
            id: tid,
            project_id: pid,
            title: "Fix auth".into(),
            harness_thread_id: "th_abc".into(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind: ThreadKind::Primary,
            mode: TurnMode::Known(Mode::Build),
            current_model: TurnModel::Known(test_model()),
            context_window: 262_144,
            model_context_windows: HashMap::new(),
            permission_preset: PermissionPreset::AskFirst,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
            archived: false,
            git_workspace: None,
        };
        let mut value = serde_json::to_value(&thread).unwrap();
        value.as_object_mut().unwrap().remove("permission_preset");
        write_thread_metadata(
            &store,
            pid,
            tid,
            &serde_json::to_vec_pretty(&value).unwrap(),
        )
        .await;

        let result = store.load_thread(pid, tid).await;
        assert!(matches!(result.unwrap_err(), PersistError::Corrupt(_)));
    }

    #[tokio::test]
    async fn list_threads() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        let t1 = ThreadId::new();
        let t2 = ThreadId::new();
        let now = Utc::now();
        for tid in [t1, t2] {
            let thread = ThreadFile {
                revision: 0,
                version: SCHEMA_VERSION,
                id: tid,
                project_id: pid,
                title: "t".into(),
                harness_thread_id: "th".into(),
                parent_thread_id: None,
                spawned_by_turn_id: None,
                kind: ThreadKind::Primary,
                mode: TurnMode::Known(Mode::Plan),
                current_model: TurnModel::Known(test_model()),
                context_window: 128_000,
                model_context_windows: HashMap::new(),
                permission_preset: PermissionPreset::AskFirst,
                model_efforts: HashMap::new(),
                tokens: TokenLedger::default(),
                created_at: now,
                updated_at: now,
                archived: false,
                git_workspace: None,
            };
            store.save_thread(pid, &thread).await.unwrap();
        }

        let threads = store.list_threads(pid).await.unwrap();
        assert_eq!(threads.len(), 2);
    }

    #[tokio::test]
    async fn corrupt_file_quarantined() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        // Write corrupt JSON to the project file.
        let path = store.project_json_path(pid);
        tokio::fs::write(&path, b"{ not valid json").await.unwrap();

        let result = store.load_project(pid).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), PersistError::Corrupt(_)));

        // The corrupt file should have been moved aside.
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn token_ledger_roundtrip() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        let mut ledger = DailyTokenLedger::default();
        ledger.record(
            "2026-07-06",
            "openai",
            "gpt-5.5",
            &giskard_core::token::TokenUsage::new(1000, 500),
        );

        store.save_project_tokens(pid, &ledger).await.unwrap();
        let loaded = store.load_project_tokens(pid).await.unwrap().unwrap();
        assert_eq!(loaded.total.input, 1000);
        assert_eq!(loaded.by_day.len(), 1);
        assert_eq!(loaded.by_model.len(), 1);
    }

    #[tokio::test]
    async fn global_tokens_roundtrip() {
        let (_tmp, store) = make_store();
        let mut ledger = DailyTokenLedger::default();
        ledger.record(
            "2026-07-06",
            "openai",
            "gpt-5.5",
            &giskard_core::token::TokenUsage::new(2000, 1000),
        );

        store.save_global_tokens(&ledger).await.unwrap();
        let loaded = store.load_global_tokens().await.unwrap().unwrap();
        assert_eq!(loaded.total.input, 2000);
    }

    #[tokio::test]
    async fn validate_all_clean() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        let errors = store.validate_all().await;
        assert!(errors.is_empty(), "expected no errors, got: {errors:?}");
    }

    #[tokio::test]
    async fn load_project_index_empty() {
        let (_tmp, store) = make_store();
        let index = store.load_project_index().await.unwrap();
        assert!(index.projects.is_empty());
    }

    pub(super) fn make_turn(usage: giskard_core::token::TokenUsage) -> Turn {
        Turn {
            id: TurnId::new(),
            user_input: giskard_core::user_input::UserInput::text("hi"),
            items: vec![],
            model: TurnModel::Known(test_model()),
            mode: TurnMode::Known(Mode::Build),
            status: giskard_core::turn::TurnStatus {
                kind: giskard_core::turn::TurnStatusKind::Completed,
                message: None,
            },
            usage,
            diffs: vec![],
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
        }
    }

    #[tokio::test]
    async fn turn_commit_and_aggregate_repair_share_the_thread_lock() {
        use giskard_core::token::TokenUsage;
        use tokio::time::{Duration, timeout};

        let (_tmp, store) = make_store();
        let store = Arc::new(store);
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        store
            .create_thread(project_id, test_thread(project_id, thread_id))
            .await
            .unwrap();

        let lock = store.thread_lock(thread_id).await;
        let guard = lock.lock().await;
        let turn = make_turn(TokenUsage::new(100, 10));
        let commit = tokio::spawn({
            let store = store.clone();
            let turn = turn.clone();
            async move {
                store
                    .append_turn_and_update_aggregates(project_id, thread_id, &turn)
                    .await
            }
        });
        tokio::pin!(commit);
        assert!(
            timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "turn history must not append outside the aggregate transaction lock"
        );
        assert!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .is_empty()
        );
        drop(guard);
        assert!(matches!(
            commit.await.unwrap().unwrap(),
            TurnCommitOutcome::MetadataMutation(ThreadMutation::Changed { .. })
        ));

        // Model a crash after the JSONL append but before its metadata fold became durable.
        let mut stale = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        stale.tokens = TokenLedger::default();
        stale.updated_at = turn.started_at - TimeDelta::days(1);
        store.save_thread(project_id, &stale).await.unwrap();

        let guard = lock.lock().await;
        let repair = tokio::spawn({
            let store = store.clone();
            async move { store.recompute_aggregates(project_id, thread_id).await }
        });
        tokio::pin!(repair);
        assert!(
            timeout(Duration::from_millis(20), &mut repair)
                .await
                .is_err(),
            "aggregate repair must not read history outside the transaction lock"
        );
        drop(guard);
        let repaired = repair.await.unwrap().unwrap().into_current().unwrap();
        assert_eq!(repaired.tokens.total, turn.usage);
        assert!(repaired.updated_at >= turn.completed_at.unwrap());
    }

    #[tokio::test]
    async fn turn_commit_reports_metadata_failure_after_durable_history_append() {
        use giskard_core::token::TokenUsage;

        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let mut thread = store
            .create_thread(project_id, test_thread(project_id, thread_id))
            .await
            .unwrap();
        thread.revision = u64::MAX;
        store.save_thread(project_id, &thread).await.unwrap();

        let turn = make_turn(TokenUsage::new(100, 10));
        assert!(matches!(
            store
                .append_turn_and_update_aggregates(project_id, thread_id, &turn)
                .await
                .unwrap(),
            TurnCommitOutcome::MetadataFailed(PersistError::Invalid(_))
        ));
        assert_eq!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .iter()
                .map(|turn| turn.id)
                .collect::<Vec<_>>(),
            vec![turn.id]
        );
        assert_eq!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap()
                .tokens,
            TokenLedger::default()
        );
    }

    #[tokio::test]
    async fn inactive_thread_locks_are_pruned() {
        let (_tmp, store) = make_store();
        let project_id = ProjectId::new();
        let first = ThreadId::new();
        let second = ThreadId::new();

        store
            .create_thread(project_id, test_thread(project_id, first))
            .await
            .unwrap();
        assert_eq!(store.thread_locks.lock().await.len(), 1);

        store
            .create_thread(project_id, test_thread(project_id, second))
            .await
            .unwrap();
        let locks = store.thread_locks.lock().await;
        assert_eq!(locks.len(), 1);
        assert!(!locks.contains_key(&first));
        assert!(locks.contains_key(&second));
    }

    #[tokio::test]
    async fn jsonl_history_append_load_page_and_recompute() {
        use giskard_core::token::TokenUsage;
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();

        // Three appended turns become three JSONL lines.
        let mut ids = vec![];
        for i in 0..3 {
            let t = make_turn(TokenUsage::new(100 * (i + 1), 10));
            ids.push(t.id);
            store.append_turn(pid, tid, &t).await.unwrap();
        }
        let all = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all.iter().map(|t| t.id).collect::<Vec<_>>(), ids);

        // Tail page + cursor pagination.
        let (tail, more) = store.load_history(pid, tid, None, 2).await.unwrap();
        assert_eq!(tail.len(), 2);
        assert!(more, "an older turn remains before the tail");
        let (older, more2) = store
            .load_history(pid, tid, Some(tail[0].id), 2)
            .await
            .unwrap();
        assert_eq!(older.len(), 1);
        assert!(!more2);

        // A torn final line is tolerated, not fatal.
        let path = store.thread_paths(pid, tid).await.history();
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        tokio::fs::write(&path, {
            let mut s = tokio::fs::read_to_string(&path).await.unwrap();
            s.push_str("{ this is a torn half-written line");
            s
        })
        .await
        .unwrap();
        assert_eq!(store.load_all_turns(pid, tid).await.unwrap().len(), 3);

        // recompute_aggregates rebuilds the metadata token totals from the JSONL.
        store
            .save_thread(
                pid,
                &ThreadFile {
                    revision: 0,
                    version: SCHEMA_VERSION,
                    id: tid,
                    project_id: pid,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(test_model()),
                    context_window: 0,
                    model_context_windows: HashMap::new(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: HashMap::new(),
                    tokens: TokenLedger::default(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();
        let tf = store
            .recompute_aggregates(pid, tid)
            .await
            .unwrap()
            .into_current()
            .unwrap();
        // 100+200+300 input, 30 output.
        assert_eq!(tf.tokens.total.input, 600);
        assert_eq!(tf.tokens.total.output, 30);
    }

    #[tokio::test]
    async fn load_turns_after_returns_delta_or_none_for_stale_cursor() {
        use giskard_core::token::TokenUsage;
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();

        let mut ids = vec![];
        for i in 0..4 {
            let t = make_turn(TokenUsage::new(100 * (i + 1), 10));
            ids.push(t.id);
            store.append_turn(pid, tid, &t).await.unwrap();
        }

        // A reconnect suffix must select records before opening payloads just like a tail page.
        // Damage before the cursor is unrelated to the requested suffix and must remain untouched.
        let paths = store.current_thread_paths(pid, tid);
        let unselected_payload = paths.turn_payload(ids[0]);
        tokio::fs::write(&unselected_payload, "{ definitely not json")
            .await
            .unwrap();

        // After a middle turn → the turns strictly after it, oldest-first.
        let after = store.load_turns_after(pid, tid, ids[1]).await.unwrap();
        assert_eq!(
            after.map(|turns| turns.iter().map(|t| t.id).collect::<Vec<_>>()),
            Some(vec![ids[2], ids[3]])
        );
        assert!(
            unselected_payload.exists(),
            "a payload before the reconnect cursor must not be opened or quarantined"
        );

        // Damage inside the requested suffix fails that turn alone. Later readable turns still
        // reconnect, and the selected damaged payload follows the normal quarantine path.
        let damaged_selected_payload = paths.turn_payload(ids[2]);
        tokio::fs::write(&damaged_selected_payload, "{ definitely not json")
            .await
            .unwrap();
        let after_damage = store.load_turns_after(pid, tid, ids[1]).await.unwrap();
        assert_eq!(
            after_damage.map(|turns| turns.iter().map(|turn| turn.id).collect::<Vec<_>>()),
            Some(vec![ids[3]])
        );
        assert!(
            !damaged_selected_payload.exists(),
            "a damaged selected payload should be quarantined"
        );
        let damaged_name = damaged_selected_payload
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let mut entries = tokio::fs::read_dir(paths.turns_dir()).await.unwrap();
        let mut quarantined = false;
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(&format!("{damaged_name}.corrupt-"))
            {
                quarantined = true;
                break;
            }
        }
        assert!(quarantined, "the damaged selected payload was not retained");

        // After the newest turn → an empty delta (the client is already up to date), not None.
        let after_last = store.load_turns_after(pid, tid, ids[3]).await.unwrap();
        assert_eq!(after_last, Some(vec![]));

        // A cursor not in history → None, so the caller falls back to a full snapshot.
        let stale = store
            .load_turns_after(pid, tid, TurnId::new())
            .await
            .unwrap();
        assert!(stale.is_none());
    }

    #[tokio::test]
    async fn bounded_history_page_does_not_open_unselected_payloads() {
        use giskard_core::token::TokenUsage;

        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let mut turns = Vec::new();
        for i in 0..128 {
            let turn = make_turn(TokenUsage::new(100 * (i + 1), 10));
            store.append_turn(pid, tid, &turn).await.unwrap();
            turns.push(turn);
        }

        let paths = store.current_thread_paths(pid, tid);
        let unselected_payload = paths.turn_payload(turns[0].id);
        tokio::fs::write(&unselected_payload, "{ definitely not json")
            .await
            .unwrap();
        let selected_payload = paths.turn_payload(turns[126].id);
        tokio::fs::write(&selected_payload, "{ also definitely not json")
            .await
            .unwrap();

        let records = store.load_turn_records(pid, tid).await.unwrap();
        assert_eq!(records.len(), turns.len());
        assert!(
            unselected_payload.exists() && selected_payload.exists(),
            "the index-only read must not open any payload"
        );

        let (tail, has_more) = store.load_history(pid, tid, None, 5).await.unwrap();
        assert!(has_more);
        assert_eq!(
            tail.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            [122usize, 123, 124, 125, 127]
                .map(|index| turns[index].id)
                .to_vec(),
            "a damaged selected payload is skipped and the page is backfilled"
        );
        assert!(
            unselected_payload.exists(),
            "an unselected corrupt payload must not be opened or quarantined"
        );
        let payload_name = unselected_payload
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let mut entries = tokio::fs::read_dir(paths.turns_dir()).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name();
            assert!(
                !name
                    .to_string_lossy()
                    .starts_with(&format!("{payload_name}.corrupt-")),
                "bounded tail read quarantined an unselected payload"
            );
        }
        assert!(
            !selected_payload.exists(),
            "a damaged selected payload should retain the existing quarantine behavior"
        );
    }

    #[tokio::test]
    async fn bounded_cursor_page_does_not_open_payloads_outside_its_range() {
        use giskard_core::token::TokenUsage;

        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let mut turns = Vec::new();
        for i in 0..8 {
            let turn = make_turn(TokenUsage::new(100 * (i + 1), 10));
            store.append_turn(pid, tid, &turn).await.unwrap();
            turns.push(turn);
        }

        let paths = store.current_thread_paths(pid, tid);
        let older_payload = paths.turn_payload(turns[0].id);
        let newer_payload = paths.turn_payload(turns[7].id);
        tokio::fs::write(&older_payload, "{ corrupt older payload")
            .await
            .unwrap();
        tokio::fs::write(&newer_payload, "{ corrupt newer payload")
            .await
            .unwrap();

        let (page, has_more) = store
            .load_history(pid, tid, Some(turns[6].id), 2)
            .await
            .unwrap();
        assert!(has_more);
        assert_eq!(
            page.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![turns[4].id, turns[5].id]
        );
        assert!(
            older_payload.exists() && newer_payload.exists(),
            "cursor pagination must not open payloads outside the selected range"
        );
    }

    #[tokio::test]
    async fn jsonl_history_cache_updates_on_append_and_invalidates_on_file_change() {
        use giskard_core::token::TokenUsage;
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();

        let first = make_turn(TokenUsage::new(100, 10));
        let second = make_turn(TokenUsage::new(200, 20));
        store.append_turn(pid, tid, &first).await.unwrap();
        store.append_turn(pid, tid, &second).await.unwrap();

        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            loaded.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![first.id, second.id,]
        );

        let entry = store.history_cache_entry(pid, tid).await.unwrap();
        assert_eq!(entry.turns.read().await.len(), 2);

        let mut third = make_turn(TokenUsage::new(300, 30));
        third.user_input = giskard_core::UserInput::text_with_attachments(
            "inspect",
            vec![giskard_core::UserAttachment {
                name: "diagram.png".into(),
                mime_type: "image/png".into(),
                size: 5,
                kind: giskard_core::AttachmentKind::Image,
                data_base64: "aW1hZ2U=".into(),
            }],
        );
        store.append_turn(pid, tid, &third).await.unwrap();
        assert_eq!(third.user_input.attachments()[0].data_base64, "aW1hZ2U=");
        assert_eq!(
            entry
                .turns
                .read()
                .await
                .iter()
                .map(|turn| turn.id)
                .collect::<Vec<_>>(),
            vec![first.id, second.id, third.id]
        );
        assert!(
            entry.turns.read().await[2].user_input.attachments()[0]
                .data_base64
                .is_empty()
        );

        let (tail, has_more) = store.load_history(pid, tid, None, 2).await.unwrap();
        assert!(has_more);
        assert_eq!(
            tail.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![second.id, third.id]
        );

        let path = store.thread_paths(pid, tid).await.history();
        tokio::fs::write(&path, history_index_of(tid, &[&first]))
            .await
            .unwrap();
        let reloaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            reloaded.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![first.id]
        );

        let entry = store.history_cache_entry(pid, tid).await.unwrap();
        let cached_meta = *entry.meta.lock().await;
        store
            .update_history_cache_after_append(
                pid,
                tid,
                &second,
                Some(cached_meta),
                Some(HistoryFileMeta {
                    len: cached_meta.len + 2,
                    modified: cached_meta.modified,
                }),
                1,
            )
            .await;
        assert!(store.history_cache_entry(pid, tid).await.is_none());

        store.delete_thread(pid, tid).await.unwrap();
        assert!(store.history_cache_entry(pid, tid).await.is_none());
    }

    #[tokio::test]
    async fn jsonl_history_skips_duplicate_turn_ids_on_read_and_recompute() {
        use giskard_core::token::TokenUsage;
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();

        let original = make_turn(TokenUsage::new(100, 10));
        let mut duplicate = original.clone();
        duplicate.user_input = giskard_core::user_input::UserInput::text("stale input");
        duplicate.usage = TokenUsage::new(999, 99);
        let second = make_turn(TokenUsage::new(200, 20));

        store.append_turn(pid, tid, &original).await.unwrap();
        store.append_turn(pid, tid, &duplicate).await.unwrap();
        store.append_turn(pid, tid, &second).await.unwrap();

        let all = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, original.id);
        assert_eq!(all[0].user_input, original.user_input);
        assert_eq!(all[0].usage, original.usage);
        assert_eq!(all[1].id, second.id);

        store
            .save_thread(
                pid,
                &ThreadFile {
                    revision: 0,
                    version: SCHEMA_VERSION,
                    id: tid,
                    project_id: pid,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(test_model()),
                    context_window: 0,
                    model_context_windows: HashMap::new(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: HashMap::new(),
                    tokens: TokenLedger::default(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();
        let tf = store
            .recompute_aggregates(pid, tid)
            .await
            .unwrap()
            .into_current()
            .unwrap();
        assert_eq!(tf.tokens.total.input, 300);
        assert_eq!(tf.tokens.total.output, 30);
    }

    #[tokio::test]
    async fn update_thread_serializes_concurrent_writes() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test")
            .await
            .unwrap();

        let tid = ThreadId::new();
        let now = Utc::now();
        store
            .save_thread(
                pid,
                &ThreadFile {
                    revision: 0,
                    version: SCHEMA_VERSION,
                    id: tid,
                    project_id: pid,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(test_model()),
                    context_window: 0,
                    model_context_windows: HashMap::new(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: HashMap::new(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        // 20 concurrent read-modify-write increments. Without the per-thread lock these would
        // race on load→save and lose updates; with it, every increment lands.
        let store = std::sync::Arc::new(store);
        let mut handles = Vec::new();
        for _ in 0..20 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                s.update_thread(pid, tid, |tf| tf.context_window += 1)
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let tf = store.load_thread(pid, tid).await.unwrap().unwrap();
        assert_eq!(tf.context_window, 20, "all concurrent increments must land");
        assert_eq!(
            tf.revision, 20,
            "each committed increment gets one revision"
        );
    }
}

/// The format 2 layout: the bounded index, the per-turn payloads, and the migration onto it.
#[cfg(test)]
mod layout_tests {
    use super::tests::*;
    use super::*;
    use giskard_core::diff::FileDiff;
    use giskard_core::item::{FileChangeEntry, FileChangeKind, Item, ItemPayload};
    use giskard_core::token::TokenUsage;
    use giskard_core::user_input::{AttachmentKind, UserAttachment, UserInput};
    use tempfile::TempDir;

    fn make_store() -> (TempDir, PersistStore) {
        let tmp = TempDir::new().unwrap();
        let store = PersistStore::new(tmp.path().to_path_buf());
        (tmp, store)
    }

    fn item(text: &str) -> Item {
        Item {
            id: giskard_core::ids::ItemId(ulid::Ulid::new()),
            harness_item_id: format!("native-{text}"),
            payload: ItemPayload::AgentMessage { text: text.into() },
            created_at: Utc::now(),
        }
    }

    /// A payload file's required opening records, for fixtures that hand-write the rest.
    fn payload_prologue() -> String {
        let mut data = String::new();
        data.push_str(
            r#"{"kind":"turn_header","format":1,"turn_id":"01JQ0000000000000000000000"}"#,
        );
        data.push('\n');
        data.push_str(r#"{"kind":"user_input","user_input":{"type":"text","text":"hi"}}"#);
        data.push('\n');
        data
    }

    fn turn_with_items(prompt: &str, items: Vec<Item>) -> Turn {
        let mut turn = make_turn(TokenUsage::new(100, 10));
        turn.user_input = UserInput::text(prompt);
        turn.items = items;
        turn
    }

    // ---- Formats ----

    /// The whole point of the split: the index row stays bounded no matter how large the turn is,
    /// and everything agent-driven lands in the payload file.
    #[tokio::test]
    async fn the_index_stays_bounded_while_the_payload_carries_the_agent_driven_half() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();

        let prompt = "fix the flaky test in ".to_string() + &"x".repeat(4096);
        let huge_output = "y".repeat(64 * 1024);
        let mut turn = turn_with_items(&prompt, vec![item(&huge_output), item("done")]);
        turn.user_input = UserInput::text_with_attachments(
            prompt.clone(),
            vec![UserAttachment {
                name: "trace.log".into(),
                mime_type: "text/plain".into(),
                size: 48_213,
                kind: AttachmentKind::File,
                data_base64: "dHJhY2U=".into(),
            }],
        );
        store.append_turn(pid, tid, &turn).await.unwrap();

        let paths = store.current_thread_paths(pid, tid);
        let index = tokio::fs::read_to_string(paths.history()).await.unwrap();
        assert!(
            index.len() < 2048,
            "a 64 KiB turn must not produce a 64 KiB index row: {} bytes",
            index.len()
        );
        assert!(
            !index.contains(&huge_output),
            "no agent output in the index"
        );
        assert!(!index.contains(&prompt), "no full prompt text in the index");
        assert!(
            index.contains("trace.log"),
            "attachment descriptors are bounded, so they stay"
        );

        let payload = tokio::fs::read_to_string(paths.turn_payload(turn.id))
            .await
            .unwrap();
        assert!(payload.contains(&huge_output));
        assert!(payload.contains(&prompt));
        assert!(
            !payload.contains("dHJhY2U="),
            "attachment bytes have never been written to history"
        );

        // And the whole turn still round-trips.
        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].user_input,
            turn.user_input.without_attachment_data()
        );
        assert_eq!(loaded[0].items, turn.items);
        assert_eq!(loaded[0].usage, turn.usage);
    }

    /// The preview is a capped display hint on a UTF-8 boundary; the payload holds the record.
    #[tokio::test]
    async fn the_prompt_preview_is_capped_and_never_authoritative() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let prompt = "🙂".repeat(400);
        let turn = turn_with_items(&prompt, vec![]);
        store.append_turn(pid, tid, &turn).await.unwrap();

        let paths = store.current_thread_paths(pid, tid);
        let data = tokio::fs::read_to_string(paths.history()).await.unwrap();
        let records = crate::history::parse_history_index(&paths.history(), &data).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].prompt_truncated);
        assert!(records[0].prompt_preview.len() <= crate::preview::PROMPT_PREVIEW_MAX_BYTES);
        assert!(prompt.starts_with(&records[0].prompt_preview));
        assert_eq!(records[0].item_count, 0);

        // Reading the thread yields the full prompt, not the preview.
        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(loaded[0].user_input.as_text(), Some(prompt.as_str()));
    }

    /// The header is written once, when the thread is created, and never rewritten afterwards.
    #[tokio::test]
    async fn the_history_header_is_written_once_at_thread_creation() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let created = store
            .create_thread(pid, test_thread(pid, tid))
            .await
            .unwrap();

        let paths = store.current_thread_paths(pid, tid);
        let header_only = tokio::fs::read_to_string(paths.history()).await.unwrap();
        assert_eq!(header_only.lines().count(), 1);
        let header: serde_json::Value = serde_json::from_str(header_only.trim()).unwrap();
        assert_eq!(header["kind"], "history_header");
        assert_eq!(header["format"], crate::layout::HISTORY_FORMAT);
        assert_eq!(header["thread_id"], created.id.to_string());

        for _ in 0..3 {
            store
                .append_turn(pid, tid, &make_turn(TokenUsage::new(1, 1)))
                .await
                .unwrap();
        }
        let after = tokio::fs::read_to_string(paths.history()).await.unwrap();
        assert!(
            after.starts_with(&header_only),
            "the header is never rewritten"
        );
        assert_eq!(after.lines().count(), 4);
    }

    /// File order is write order; `index` is display order. They diverge the moment anything is
    /// appended after the turn committed, which is why the field is carried explicitly.
    ///
    /// Nothing in this change produces a late append — amendments are out of scope — so the
    /// scenario is written by hand: a build that was still running at item 1 of 3 settles later and
    /// is appended at the *end* of the file. Folded by file order it would render last; folded by
    /// `index` it stays where it happened.
    #[test]
    fn a_late_payload_record_folds_back_into_its_own_slot() {
        let path = std::path::Path::new("turns/T1.jsonl");
        let items = [item("first"), item("cargo build"), item("third")];
        let mut settled = items[1].clone();
        settled.payload = ItemPayload::AgentMessage {
            text: "cargo build (finished)".into(),
        };

        let mut data = payload_prologue();
        for (index, item) in items.iter().enumerate() {
            data.push_str(
                &serde_json::to_string(
                    &serde_json::json!({"kind":"item","index":index,"item":item}),
                )
                .unwrap(),
            );
            data.push('\n');
        }
        // Appended after the turn committed: last in the file, index 1.
        data.push_str(
            &serde_json::to_string(&serde_json::json!({"kind":"item","index":1,"item":settled}))
                .unwrap(),
        );
        data.push('\n');

        let payload = crate::history::parse_turn_payload(path, &data).unwrap();
        assert_eq!(
            payload.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            items.iter().map(|item| item.id).collect::<Vec<_>>(),
            "last-wins by item id must not move the item to the bottom of the turn"
        );
        assert_eq!(payload.items[1].payload, settled.payload);

        // A genuinely new late item takes max(existing) + 1 and renders after the rest.
        let appended = item("follow-up");
        let mut with_new = data.clone();
        with_new.push_str(
            &serde_json::to_string(&serde_json::json!({"kind":"item","index":3,"item":appended}))
                .unwrap(),
        );
        with_new.push('\n');
        let payload = crate::history::parse_turn_payload(path, &with_new).unwrap();
        assert_eq!(payload.items.len(), 4);
        assert_eq!(payload.items[3].id, appended.id);
    }

    // ---- Containment ----

    /// Atomic payload writes mean this code cannot itself truncate a payload file, so the damage is
    /// injected directly. The turn must fail alone.
    #[tokio::test]
    async fn a_damaged_payload_fails_that_turn_alone_and_is_quarantined() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let first = turn_with_items("first", vec![item("one")]);
        let damaged = turn_with_items("damaged", vec![item("two")]);
        let last = turn_with_items("last", vec![item("three")]);
        for turn in [&first, &damaged, &last] {
            store.append_turn(pid, tid, turn).await.unwrap();
        }

        let paths = store.current_thread_paths(pid, tid);
        let payload = paths.turn_payload(damaged.id);
        tokio::fs::write(&payload, b"{\"kind\":\"item\",\"index\":0,\"it")
            .await
            .unwrap();

        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            loaded.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![first.id, last.id],
            "the damaged turn fails alone; the index and every other turn stay readable"
        );
        assert!(
            !payload.exists(),
            "the bad file is quarantined, not left in place"
        );
        let quarantined: Vec<_> = std::fs::read_dir(paths.turns_dir())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".corrupt-"))
            .collect();
        assert_eq!(
            quarantined.len(),
            1,
            "left on disk for inspection: {quarantined:?}"
        );

        // `validate_all` names the turn that failed, not the whole thread.
        let errors = store.history_validation_errors(pid, tid).await;
        assert_eq!(errors.len(), 1);
        assert!(errors[0].0.ends_with(format!("{}.jsonl", damaged.id)));
    }

    /// A turn record whose payload file is simply absent behaves the same way.
    #[tokio::test]
    async fn a_missing_payload_fails_that_turn_alone() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let first = turn_with_items("first", vec![item("one")]);
        let vanished = turn_with_items("vanished", vec![item("two")]);
        for turn in [&first, &vanished] {
            store.append_turn(pid, tid, turn).await.unwrap();
        }

        let paths = store.current_thread_paths(pid, tid);
        tokio::fs::remove_file(paths.turn_payload(vanished.id))
            .await
            .unwrap();

        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            loaded.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![first.id]
        );
        let errors = store.history_validation_errors(pid, tid).await;
        assert_eq!(errors.len(), 1);
        assert!(errors[0].1.contains("no payload file"));
    }

    /// A future downgrade must be non-destructive: an unrecognised record is skipped, not fatal.
    #[tokio::test]
    async fn an_unknown_index_record_kind_is_skipped_with_a_warning() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let turn = turn_with_items("kept", vec![]);
        store.append_turn(pid, tid, &turn).await.unwrap();

        let path = store.current_thread_paths(pid, tid).history();
        let mut data = tokio::fs::read_to_string(&path).await.unwrap();
        data.push_str("{\"kind\":\"turn_superseded\",\"turn_id\":\"x\"}\n");
        data.push_str("{\"no_kind_at_all\":true}\n");
        tokio::fs::write(&path, data).await.unwrap();

        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            loaded.iter().map(|t| t.id).collect::<Vec<_>>(),
            vec![turn.id]
        );
    }

    /// A payload newer than this build understands fails that turn only, and is left alone rather
    /// than quarantined — a newer format is not damage.
    #[tokio::test]
    async fn a_newer_payload_format_fails_that_turn_only() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let future = turn_with_items("from the future", vec![]);
        let ordinary = turn_with_items("ordinary", vec![]);
        for turn in [&future, &ordinary] {
            store.append_turn(pid, tid, turn).await.unwrap();
        }

        let paths = store.current_thread_paths(pid, tid);
        let payload = paths.turn_payload(future.id);
        tokio::fs::write(
            &payload,
            format!(
                "{{\"kind\":\"turn_header\",\"format\":{},\"turn_id\":\"{}\"}}\n",
                crate::layout::TURN_PAYLOAD_FORMAT + 1,
                future.id
            ),
        )
        .await
        .unwrap();

        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            loaded.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![ordinary.id]
        );
        assert!(
            payload.exists(),
            "a newer format is not damage; leave it readable"
        );
    }

    /// Without the index there is nothing to partially recover, so a newer layout fails the thread.
    #[tokio::test]
    async fn a_newer_history_format_fails_the_whole_thread() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        store
            .append_turn(pid, tid, &turn_with_items("hi", vec![]))
            .await
            .unwrap();

        let path = store.current_thread_paths(pid, tid).history();
        let data = tokio::fs::read_to_string(&path).await.unwrap();
        let bumped = data.replacen(
            &format!("\"format\":{}", crate::layout::HISTORY_FORMAT),
            &format!("\"format\":{}", crate::layout::HISTORY_FORMAT + 1),
            1,
        );
        tokio::fs::write(&path, bumped).await.unwrap();

        assert!(matches!(
            store.load_all_turns(pid, tid).await.unwrap_err(),
            PersistError::Invalid(_)
        ));
    }

    // ---- Write ordering ----

    /// Payload first, index last: a crash between them leaves a file no turn record references, and
    /// nothing can see it because every read starts from the index.
    #[tokio::test]
    async fn a_payload_no_turn_record_references_is_invisible_and_sweepable() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let committed = turn_with_items("committed", vec![]);
        store.append_turn(pid, tid, &committed).await.unwrap();

        let paths = store.current_thread_paths(pid, tid);
        let orphan = TurnId::new();
        tokio::fs::write(
            paths.turn_payload(orphan),
            crate::history::payload_file_bytes(&turn_with_items("orphan", vec![])).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            store
                .load_all_turns(pid, tid)
                .await
                .unwrap()
                .iter()
                .map(|turn| turn.id)
                .collect::<Vec<_>>(),
            vec![committed.id],
            "reads start from the index, so an unreferenced payload is invisible"
        );

        // With the data directory locked there is no in-flight commit an orphan could belong to,
        // so a freshly written one is swept on sight — no wall-clock guess about another process.
        let sweep = store.sweep_orphan_payloads(pid, tid, true).await.unwrap();
        assert_eq!(sweep.payloads, vec![paths.turn_payload(orphan)]);
        assert_eq!(sweep.refusal, None);
        assert!(
            paths.turn_payload(orphan).exists(),
            "dry run removes nothing"
        );
        store.sweep_orphan_payloads(pid, tid, false).await.unwrap();
        assert!(!paths.turn_payload(orphan).exists());
        assert!(paths.turn_payload(committed.id).exists());
    }

    // ---- Enumeration ----

    #[tokio::test]
    async fn list_threads_reports_migrated_and_unmigrated_threads_exactly_once() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let migrated = ThreadId::new();
        let unmigrated = ThreadId::new();
        let mid_migration = ThreadId::new();

        store
            .create_thread(pid, test_thread(pid, migrated))
            .await
            .unwrap();
        write_format1_thread(&store, pid, &test_thread(pid, unmigrated), &[]).await;

        // A thread caught between the commit rename and the legacy move has *both* shapes on disk.
        let threads_dir = store.threads_dir(pid);
        tokio::fs::create_dir_all(threads_dir.join(mid_migration.to_string()))
            .await
            .unwrap();
        tokio::fs::write(threads_dir.join(format!("{mid_migration}.json")), b"{}")
            .await
            .unwrap();

        // Working state and quarantine files are never threads.
        for name in [
            format!("{}.migrating", ThreadId::new()),
            format!("{}.deleting", ThreadId::new()),
        ] {
            tokio::fs::create_dir_all(threads_dir.join(name))
                .await
                .unwrap();
        }
        tokio::fs::write(
            threads_dir.join(format!("{}.json.corrupt-20260101T000000", ThreadId::new())),
            b"{}",
        )
        .await
        .unwrap();

        let mut listed = store.list_threads(pid).await.unwrap();
        listed.sort_by_key(|id| id.0);
        let mut expected = vec![migrated, unmigrated, mid_migration];
        expected.sort_by_key(|id| id.0);
        assert_eq!(listed, expected);
    }

    #[tokio::test]
    async fn delete_project_cascades_to_thread_directories() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/proj")
            .await
            .unwrap();
        let tid = ThreadId::new();
        store
            .create_thread(pid, test_thread(pid, tid))
            .await
            .unwrap();
        store
            .append_turn(pid, tid, &turn_with_items("hi", vec![item("there")]))
            .await
            .unwrap();
        assert!(store.current_thread_paths(pid, tid).turns_dir().exists());

        store.delete_project(pid).await.unwrap();
        assert!(!store.threads_dir(pid).exists());
        assert!(store.list_threads(pid).await.unwrap().is_empty());
    }

    // ---- Migration ----

    #[tokio::test]
    async fn a_format1_thread_migrates_turn_for_turn_and_re_running_is_a_no_op() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let source: Vec<Turn> = (0..3)
            .map(|i| turn_with_items(&format!("prompt {i}"), vec![item(&format!("item {i}"))]))
            .collect();
        let metadata = test_thread(pid, tid);
        write_format1_thread(&store, pid, &metadata, &source).await;
        assert_eq!(store.thread_layout(pid, tid).await.history_format(), 1);

        // Any read is enough to migrate; nothing has to be run beforehand.
        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            loaded.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            source.iter().map(|turn| turn.id).collect::<Vec<_>>()
        );
        assert_eq!(loaded[1].items, source[1].items);
        assert_eq!(loaded[2].user_input, source[2].user_input);
        assert_eq!(
            store.thread_layout(pid, tid).await.history_format(),
            crate::layout::HISTORY_FORMAT
        );

        // Metadata survives byte-for-byte, and the originals are relocated, never deleted.
        let paths = store.current_thread_paths(pid, tid);
        assert_eq!(
            store.load_thread(pid, tid).await.unwrap().unwrap(),
            metadata
        );
        assert!(!paths.flat_metadata().exists());
        assert!(!paths.flat_history().exists());
        assert!(paths.legacy_dir().join("thread.json").exists());
        assert!(paths.legacy_dir().join("history.jsonl").exists());
        assert!(store.has_legacy_data(pid, tid).await);

        // Re-running changes nothing.
        let committed = tokio::fs::read_to_string(paths.history()).await.unwrap();
        assert_eq!(
            store.migrate_thread_layout(pid, tid).await.unwrap(),
            MigrationOutcome::AlreadyCurrent
        );
        assert_eq!(
            tokio::fs::read_to_string(paths.history()).await.unwrap(),
            committed
        );

        // Pruning is separate and explicit, because it is the step that destroys transcript data.
        assert!(store.prune_legacy_data(pid, tid).await.unwrap());
        assert!(!store.has_legacy_data(pid, tid).await);
        assert!(!store.prune_legacy_data(pid, tid).await.unwrap());
        assert_eq!(store.load_all_turns(pid, tid).await.unwrap().len(), 3);
    }

    /// A dry run previews through the migration's own classifier, so it names every case the real
    /// run acts on — including the thread caught between the commit rename and the legacy move,
    /// which a bare format check reports as having nothing to do.
    #[tokio::test]
    async fn a_planned_migration_names_every_case_the_real_one_acts_on() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();

        let absent = ThreadId::new();
        assert_eq!(
            store.planned_migration(pid, absent).await,
            MigrationOutcome::Absent
        );

        let current = ThreadId::new();
        store
            .create_thread(pid, test_thread(pid, current))
            .await
            .unwrap();
        assert_eq!(
            store.planned_migration(pid, current).await,
            MigrationOutcome::AlreadyCurrent
        );

        let flat = ThreadId::new();
        write_format1_thread(&store, pid, &test_thread(pid, flat), &[]).await;
        assert_eq!(
            store.planned_migration(pid, flat).await,
            MigrationOutcome::Migrated
        );

        // Both shapes on disk: the rebuild is committed, the relocation is not.
        let interrupted = ThreadId::new();
        let paths = store.current_thread_paths(pid, interrupted);
        tokio::fs::create_dir_all(paths.dir()).await.unwrap();
        tokio::fs::write(paths.flat_metadata(), b"{}")
            .await
            .unwrap();
        assert_eq!(
            store.planned_migration(pid, interrupted).await,
            MigrationOutcome::FinishedLegacyMove
        );

        // Every plan matches what the run actually does.
        for thread in [absent, current, flat, interrupted] {
            let planned = store.planned_migration(pid, thread).await;
            assert_eq!(
                store.migrate_thread_layout(pid, thread).await.unwrap(),
                planned,
                "the plan and the act disagreed about {thread}"
            );
        }
    }

    /// The one thing a plan cannot know is whether the work will succeed.
    #[tokio::test]
    async fn a_plan_cannot_predict_a_migration_that_will_fail() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        write_format1_thread(&store, pid, &test_thread(pid, tid), &[]).await;
        let good = serde_json::to_string(&make_turn(TokenUsage::new(1, 1))).unwrap();
        tokio::fs::write(
            store.current_thread_paths(pid, tid).flat_history(),
            format!("torn interior line\n{good}\n"),
        )
        .await
        .unwrap();

        assert_eq!(
            store.planned_migration(pid, tid).await,
            MigrationOutcome::Migrated,
            "the plan reads the layout, not the history"
        );
        assert!(store.migrate_thread_layout(pid, tid).await.is_err());
    }

    /// A crash between the commit rename and the legacy move leaves both shapes on disk. Readers
    /// prefer the directory, and the next open finishes the move.
    #[tokio::test]
    async fn a_migration_interrupted_after_the_commit_rename_is_finished_on_the_next_open() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let source = vec![turn_with_items("prompt", vec![item("output")])];
        write_format1_thread(&store, pid, &test_thread(pid, tid), &source).await;

        // Migrate, then put the originals back where the crash would have left them.
        let paths = store.current_thread_paths(pid, tid);
        store.migrate_thread_layout(pid, tid).await.unwrap();
        for (from, to) in [
            (
                paths.legacy_dir().join("thread.json"),
                paths.flat_metadata(),
            ),
            (
                paths.legacy_dir().join("history.jsonl"),
                paths.flat_history(),
            ),
        ] {
            tokio::fs::rename(from, to).await.unwrap();
        }
        tokio::fs::remove_dir_all(paths.legacy_dir()).await.unwrap();
        assert!(paths.flat_metadata().exists() && paths.dir().exists());

        // The directory wins, and the move finishes.
        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, source[0].id);
        assert!(!paths.flat_metadata().exists());
        assert!(!paths.flat_history().exists());
        assert!(paths.legacy_dir().join("history.jsonl").exists());
    }

    /// A staged migration that never committed is discarded and rebuilt, not adopted.
    #[tokio::test]
    async fn a_staged_migration_left_by_a_crash_is_discarded_and_rebuilt() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let source = vec![turn_with_items("prompt", vec![item("output")])];
        write_format1_thread(&store, pid, &test_thread(pid, tid), &source).await;

        let paths = store.current_thread_paths(pid, tid);
        tokio::fs::create_dir_all(paths.migrating_dir().join("turns"))
            .await
            .unwrap();
        tokio::fs::write(paths.migrating_dir().join("history.jsonl"), b"garbage\n")
            .await
            .unwrap();

        assert_eq!(
            store.migrate_thread_layout(pid, tid).await.unwrap(),
            MigrationOutcome::Migrated
        );
        assert!(!paths.migrating_dir().exists());
        assert_eq!(
            store
                .load_all_turns(pid, tid)
                .await
                .unwrap()
                .iter()
                .map(|turn| turn.id)
                .collect::<Vec<_>>(),
            vec![source[0].id]
        );
    }

    /// The index is the less durable of the two files, so an empty or missing one beside intact
    /// payloads is a recoverable transcript — never a thread whose every payload is unreferenced.
    #[tokio::test]
    async fn the_sweep_refuses_to_read_a_lost_index_as_a_directory_full_of_orphans() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let turns: Vec<Turn> = (0..3)
            .map(|i| turn_with_items(&format!("prompt {i}"), vec![item(&format!("item {i}"))]))
            .collect();
        for turn in &turns {
            store.append_turn(pid, tid, turn).await.unwrap();
        }
        let paths = store.current_thread_paths(pid, tid);

        // A power loss can leave a zero-length index (page-cached `O_APPEND`) beside payloads that
        // were fsynced before their rename.
        tokio::fs::write(paths.history(), b"").await.unwrap();
        let sweep = store.sweep_orphan_payloads(pid, tid, false).await.unwrap();
        assert!(sweep.refusal.is_some(), "{sweep:?}");

        // Same for an index that is gone outright.
        tokio::fs::remove_file(paths.history()).await.unwrap();
        let sweep = store.sweep_orphan_payloads(pid, tid, false).await.unwrap();
        assert!(sweep.refusal.is_some(), "{sweep:?}");
        for turn in &turns {
            assert!(paths.turn_payload(turn.id).exists(), "nothing was deleted");
        }

        // A thread that genuinely has no turns has no payloads to sweep either, so the guard costs
        // a legitimate sweep nothing.
        let empty = ThreadId::new();
        store
            .create_thread(pid, test_thread(pid, empty))
            .await
            .unwrap();
        let sweep = store
            .sweep_orphan_payloads(pid, empty, false)
            .await
            .unwrap();
        assert!(
            sweep.payloads.is_empty() && sweep.refusal.is_none(),
            "{sweep:?}"
        );
    }

    /// Total index loss is the rarer failure; losing a *tail* of page-cached appends is the likely
    /// one, and it leaves that many fsynced payloads unreferenced. Deleting them is the same
    /// history destruction the empty-index guard exists to prevent, through the more probable door.
    #[tokio::test]
    async fn the_sweep_refuses_a_partially_truncated_index_too() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let turns: Vec<Turn> = (0..12)
            .map(|i| turn_with_items(&format!("prompt {i}"), vec![item(&format!("item {i}"))]))
            .collect();
        for turn in &turns {
            store.append_turn(pid, tid, turn).await.unwrap();
        }
        let paths = store.current_thread_paths(pid, tid);

        // Keep the header and the first two records; the rest of the appends never reached disk.
        let index = tokio::fs::read_to_string(paths.history()).await.unwrap();
        let kept: String = index
            .lines()
            .take(3)
            .map(|line| format!("{line}\n"))
            .collect();
        tokio::fs::write(paths.history(), kept).await.unwrap();

        let sweep = store.sweep_orphan_payloads(pid, tid, false).await.unwrap();
        assert!(
            sweep
                .refusal
                .as_deref()
                .is_some_and(|reason| reason.contains("truncated")),
            "{sweep:?}"
        );
        for turn in &turns {
            assert!(paths.turn_payload(turn.id).exists(), "nothing was deleted");
        }

        // The refusal points an operator at `--dry-run`, so `--dry-run` has to be the one thing
        // that still works here: it names the same refusal *and* lists the files it is about.
        let previewed = store.sweep_orphan_payloads(pid, tid, true).await.unwrap();
        assert_eq!(previewed.refusal, sweep.refusal);
        assert_eq!(previewed.payloads.len(), 10, "the list the refusal names");
        for turn in &turns {
            assert!(
                paths.turn_payload(turn.id).exists(),
                "and a dry run still removes nothing"
            );
        }

        // The bound is on magnitude, not on the existence of orphans: a handful still sweeps, which
        // is what an orphan actually looks like when it is one crash per commit.
        let few = ThreadId::new();
        store
            .append_turn(pid, few, &turn_with_items("kept", vec![]))
            .await
            .unwrap();
        let few_paths = store.current_thread_paths(pid, few);
        let mut orphans = vec![];
        for _ in 0..MAX_PLAUSIBLE_ORPHANS {
            let orphan = TurnId::new();
            tokio::fs::write(
                few_paths.turn_payload(orphan),
                crate::history::payload_file_bytes(&turn_with_items("orphan", vec![])).unwrap(),
            )
            .await
            .unwrap();
            orphans.push(orphan);
        }
        assert_eq!(
            store
                .sweep_orphan_payloads(pid, few, false)
                .await
                .unwrap()
                .payloads
                .len(),
            MAX_PLAUSIBLE_ORPHANS
        );
        for orphan in orphans {
            assert!(!few_paths.turn_payload(orphan).exists());
        }
    }

    /// A payload missing a required record is incomplete. Reassembling it with an empty prompt
    /// would manufacture a turn indistinguishable from one the user submitted blank.
    #[tokio::test]
    async fn a_payload_with_no_user_input_fails_that_turn_instead_of_inventing_one() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        let incomplete = turn_with_items("what was asked", vec![item("one")]);
        let ordinary = turn_with_items("ordinary", vec![item("two")]);
        for turn in [&incomplete, &ordinary] {
            store.append_turn(pid, tid, turn).await.unwrap();
        }

        // Well-formed lines, one required record missing — not a truncation.
        let paths = store.current_thread_paths(pid, tid);
        let payload = paths.turn_payload(incomplete.id);
        let data = tokio::fs::read_to_string(&payload).await.unwrap();
        let stripped: String = data
            .lines()
            .filter(|line| !line.contains(r#""kind":"user_input""#))
            .map(|line| format!("{line}\n"))
            .collect();
        tokio::fs::write(&payload, stripped).await.unwrap();

        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(
            loaded.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            vec![ordinary.id],
            "the incomplete turn fails alone rather than reassembling with an empty prompt"
        );
        assert!(
            payload.exists(),
            "the bytes parsed, so the file is reported rather than quarantined — its surviving \
             records may still be recoverable"
        );
        let errors = store.history_validation_errors(pid, tid).await;
        assert_eq!(errors.len(), 1);
        assert!(errors[0].1.contains("no user_input record"), "{errors:?}");
    }

    /// The payload file holds two classes of record, and each needs its own stated rule.
    #[test]
    fn collections_fold_by_identity_and_singletons_last_win() {
        use giskard_core::diff::FileDiff;
        use giskard_core::item::FileChangeKind;
        let path = std::path::Path::new("turns/T1.jsonl");

        let diff = |file: &str, text: &str| FileDiff {
            path: file.into(),
            change: FileChangeKind::Modified,
            old_text: None,
            new_text: Some(text.into()),
            hunks: vec![],
            binary: false,
            captured: None,
        };

        let mut data = payload_prologue();
        // The same file twice at different indices, and a second file — keyed by path, the first
        // must survive once, at the slot its first record established.
        for (index, record) in [
            (0usize, diff("src/a.rs", "first")),
            (1, diff("src/b.rs", "other")),
            (7, diff("src/a.rs", "revised")),
        ] {
            data.push_str(
                &serde_json::to_string(
                    &serde_json::json!({"kind":"diff","index":index,"diff":record}),
                )
                .unwrap(),
            );
            data.push('\n');
        }
        // A duplicate singleton: last wins, and it is warned about rather than vanishing.
        data.push_str(r#"{"kind":"user_input","user_input":{"type":"text","text":"superseding"}}"#);
        data.push('\n');

        let payload = crate::history::parse_turn_payload(path, &data).unwrap();
        assert_eq!(
            payload
                .diffs
                .iter()
                .map(|d| {
                    let id = &d.captured.as_ref().unwrap().id;
                    let text = match payload.diff_contents.get(id).unwrap() {
                        giskard_core::CapturedDiffContent::Structured { diff } => {
                            diff.new_text.clone()
                        }
                        other => panic!("expected structured diff, got {other:?}"),
                    };
                    (d.path.to_string_lossy().into_owned(), text)
                })
                .collect::<Vec<_>>(),
            vec![
                ("src/b.rs".to_string(), Some("other".into())),
                ("src/a.rs".to_string(), Some("revised".into())),
            ],
            "one entry per path, last-wins, rendered at the index the last record gave it"
        );
        assert_eq!(payload.user_input.as_text(), Some("superseding"));
    }

    /// A turn's status *kind* is bounded and belongs in the index; its *message* is composed from
    /// provider error text and has no ceiling, so the index keeps only a capped rendering.
    #[tokio::test]
    async fn an_unbounded_status_message_is_capped_in_the_index_and_whole_in_the_payload() {
        use giskard_core::turn::{TurnStatus, TurnStatusKind};
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();

        let detail = "provider said: ".to_string() + &"z".repeat(32 * 1024);
        let mut turn = turn_with_items("run it", vec![]);
        turn.status = TurnStatus {
            kind: TurnStatusKind::Failed,
            message: Some(detail.clone()),
        };
        store.append_turn(pid, tid, &turn).await.unwrap();

        let paths = store.current_thread_paths(pid, tid);
        let index = tokio::fs::read_to_string(paths.history()).await.unwrap();
        assert!(
            index.len() < 2048,
            "a 32 KiB provider error must not land on the append-only index: {} bytes",
            index.len()
        );
        assert!(!index.contains(&detail));

        // The kind stays authoritative in the index, so aggregate repair still reads no payload.
        let records = crate::history::parse_history_index(&paths.history(), &index).unwrap();
        assert_eq!(records[0].status.kind, TurnStatusKind::Failed);
        assert!(records[0].status.message.as_deref().is_some_and(|message| {
            message.len() <= crate::preview::STATUS_MESSAGE_MAX_BYTES && detail.starts_with(message)
        }));

        // And the message the harness reported survives in full.
        let loaded = store.load_all_turns(pid, tid).await.unwrap();
        assert_eq!(loaded[0].status, turn.status);
    }

    /// A replacement record that carries no index of its own keeps the slot its first record
    /// established, rather than defaulting to zero and jumping to the top of the turn.
    #[test]
    fn a_replacement_without_an_index_keeps_its_original_slot() {
        let path = std::path::Path::new("turns/T1.jsonl");
        let items = [item("first"), item("second"), item("third")];
        let mut settled = items[2].clone();
        settled.payload = ItemPayload::AgentMessage {
            text: "third (settled)".into(),
        };

        let mut data = payload_prologue();
        for (index, item) in items.iter().enumerate() {
            data.push_str(
                &serde_json::to_string(
                    &serde_json::json!({"kind":"item","index":index,"item":item}),
                )
                .unwrap(),
            );
            data.push('\n');
        }
        data.push_str(
            &serde_json::to_string(&serde_json::json!({"kind":"item","item":settled})).unwrap(),
        );
        data.push('\n');

        let payload = crate::history::parse_turn_payload(path, &data).unwrap();
        assert_eq!(
            payload.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            items.iter().map(|item| item.id).collect::<Vec<_>>()
        );
        assert_eq!(payload.items[2].payload, settled.payload);
    }

    /// `validate` reports what is on disk. Migrating every format 1 thread as a side effect of
    /// inspecting it would make the report describe a store the operator did not ask to change.
    #[tokio::test]
    async fn validate_all_inspects_without_migrating_or_quarantining() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/proj")
            .await
            .unwrap();

        let flat = ThreadId::new();
        write_format1_thread(
            &store,
            pid,
            &test_thread(pid, flat),
            &[make_turn(giskard_core::token::TokenUsage::new(1, 1))],
        )
        .await;

        let damaged = ThreadId::new();
        let turn = turn_with_items("damaged", vec![item("one")]);
        store.append_turn(pid, damaged, &turn).await.unwrap();
        let payload = store
            .current_thread_paths(pid, damaged)
            .turn_payload(turn.id);
        tokio::fs::write(&payload, b"{not json").await.unwrap();

        let errors = store.validate_all().await;
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(errors[0].0, payload);
        assert_eq!(
            store.thread_layout(pid, flat).await.history_format(),
            1,
            "validate must not migrate the store it is inspecting"
        );
        assert!(
            payload.exists(),
            "validate reports damage, it does not move it aside"
        );

        // So a second run says the same thing rather than degrading to "no payload file".
        let again = store.validate_all().await;
        assert_eq!(again.len(), 1);
        assert_eq!(again[0], errors[0]);
    }

    /// A format 1 history with a bad interior line cannot be rebuilt without losing turns, so the
    /// migration aborts and the thread keeps behaving exactly as it does today.
    #[tokio::test]
    async fn an_unmigratable_history_leaves_the_thread_on_format_1_rather_than_losing_turns() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        write_format1_thread(&store, pid, &test_thread(pid, tid), &[]).await;
        let flat_history = store.current_thread_paths(pid, tid).flat_history();
        let good = serde_json::to_string(&make_turn(TokenUsage::new(1, 1))).unwrap();
        tokio::fs::write(&flat_history, format!("torn interior line\n{good}\n"))
            .await
            .unwrap();

        assert!(matches!(
            store.load_all_turns(pid, tid).await.unwrap_err(),
            PersistError::Corrupt(_)
        ));
        assert_eq!(store.thread_layout(pid, tid).await.history_format(), 1);
        // Metadata still reads and writes, exactly as before this change.
        assert!(store.load_thread(pid, tid).await.unwrap().is_some());
        assert!(matches!(
            store
                .update_thread(pid, tid, |thread| thread.title = "renamed".into())
                .await
                .unwrap(),
            ThreadMutation::Changed { .. }
        ));
        // The decision is remembered so every later call does not re-parse the whole flat file,
        // take the lock, and log again — but it is a memo, not a verdict: repairing the file and
        // asking explicitly migrates.
        assert!(store.unmigratable.read().await.contains(&(pid, tid)));
        tokio::fs::write(&flat_history, format!("{good}\n"))
            .await
            .unwrap();
        assert_eq!(
            store.migrate_thread_layout(pid, tid).await.unwrap(),
            MigrationOutcome::Migrated
        );
        assert_eq!(store.load_all_turns(pid, tid).await.unwrap().len(), 1);
    }

    /// A migration failure leaves a readable flat thread in service. Its writes and lazy reads
    /// must retain the same inline format-1 bodies as an ordinary migrated payload.
    #[tokio::test]
    async fn degraded_flat_layout_preserves_and_loads_captured_diff_bodies() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        let tid = ThreadId::new();
        write_format1_thread(&store, pid, &test_thread(pid, tid), &[]).await;
        store.unmigratable.write().await.insert((pid, tid));

        let item_id = giskard_core::ItemId::new();
        let unified_text = "@@ -1 +1 @@\n-old\n+new\n".to_string();
        let (unified_descriptor, unified_record) = giskard_core::capture_unified_diff(
            "src/inline.rs".into(),
            FileChangeKind::Modified,
            Some(item_id),
            unified_text.clone(),
        );
        let structured_body = FileDiff {
            path: "src/structured.rs".into(),
            change: FileChangeKind::Modified,
            old_text: Some("before\n".into()),
            new_text: Some("after\n".into()),
            hunks: Vec::new(),
            binary: false,
            captured: None,
        };
        let (structured_projection, structured_record) =
            giskard_core::capture_structured_diff(structured_body.clone());
        let mut turn = make_turn(TokenUsage::new(1, 1));
        turn.items = vec![Item {
            id: item_id,
            harness_item_id: "native-file-change".into(),
            payload: ItemPayload::FileChange {
                path: "src/inline.rs".into(),
                change: FileChangeKind::Modified,
                changes: vec![FileChangeEntry {
                    path: "src/inline.rs".into(),
                    change: FileChangeKind::Modified,
                    diff: None,
                    captured_diff: Some(unified_descriptor.clone()),
                }],
                status: None,
            },
            created_at: Utc::now(),
        }];
        turn.diffs = vec![structured_projection];

        store
            .append_turn_with_diffs(
                pid,
                tid,
                &turn,
                &[unified_record, structured_record.clone()],
            )
            .await
            .unwrap();

        assert_eq!(store.thread_layout(pid, tid).await, ThreadLayout::Flat);
        let flat_history = store.thread_paths(pid, tid).await.history();
        let persisted = parse_turn_history(
            &flat_history,
            &tokio::fs::read_to_string(&flat_history).await.unwrap(),
        )
        .unwrap();
        let persisted_turn = persisted
            .iter()
            .find(|candidate| candidate.id == turn.id)
            .unwrap();
        let persisted_change = match &persisted_turn.items[0].payload {
            ItemPayload::FileChange { changes, .. } => &changes[0],
            _ => panic!("expected file change"),
        };
        assert_eq!(
            persisted_change.diff.as_deref(),
            Some(unified_text.as_str())
        );
        assert!(persisted_change.captured_diff.is_none());
        assert_eq!(persisted_turn.diffs, vec![structured_body.clone()]);

        assert_eq!(
            store
                .load_captured_diff(pid, tid, turn.id, &unified_descriptor.id)
                .await
                .unwrap(),
            Some(giskard_core::CapturedDiffContent::Unified { text: unified_text })
        );
        assert_eq!(
            store
                .load_captured_diff(pid, tid, turn.id, &structured_record.id)
                .await
                .unwrap(),
            Some(giskard_core::CapturedDiffContent::Structured {
                diff: structured_body
            })
        );
    }
}
