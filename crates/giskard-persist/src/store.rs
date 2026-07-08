//! The persistence store: load/save projects, threads, token ledgers (spec §5).

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::model::{Effort, ModelRef};
use giskard_core::token::{DailyTokenLedger, TokenLedger};
use giskard_core::turn::{ApprovalPolicy, Mode, Turn};

use crate::PersistError;
use crate::atomic::{atomic_write_json, read_json, read_json_or_quarantine};
use crate::config::Config;

const SCHEMA_VERSION: u32 = 1;

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
    pub default_model: ModelRef,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `projects/<id>/threads/<thread_id>.json` — thread state + history (spec §5.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadFile {
    pub version: u32,
    pub id: ThreadId,
    pub project_id: ProjectId,
    pub title: String,
    pub harness_thread_id: String,
    pub mode: Mode,
    pub current_model: ModelRef,
    /// Cache only (C4): derived from `current_model`'s descriptor and recomputed on load — not a
    /// source of truth. `#[serde(default)]` so older files (or a deliberately omitted value) load;
    /// callers should recompute it from `current_model` against the live model config.
    #[serde(default)]
    pub context_window: u32,
    /// Per-thread approval policy (P3).
    pub approval_policy: ApprovalPolicy,
    /// Per-model effort retention (C7): maps `"provider/model"` → stored `Effort`, so switching
    /// back to a reasoning model restores the user's last effort choice.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub model_efforts: HashMap<String, Effort>,
    /// Token aggregates (total + nested by_model). A **recomputable cache** (H3): the authoritative
    /// history is the `.jsonl`, so these can be rebuilt by folding it (`recompute_aggregates`).
    pub tokens: TokenLedger,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
    thread_locks: Mutex<HashMap<ThreadId, Arc<Mutex<()>>>>,
}

impl PersistStore {
    /// Create a new store rooted at `data_dir`.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            config: Mutex::new(None),
            project_index_lock: Mutex::new(()),
            thread_locks: Mutex::new(HashMap::new()),
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

    fn thread_json_path(&self, project: ProjectId, thread: ThreadId) -> PathBuf {
        self.threads_dir(project).join(format!("{}.json", thread))
    }

    fn thread_jsonl_path(&self, project: ProjectId, thread: ThreadId) -> PathBuf {
        self.threads_dir(project).join(format!("{}.jsonl", thread))
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
        default_model: ModelRef,
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
            default_model,
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
        Ok(())
    }

    // ---- Threads ----

    /// Load a thread file.
    pub async fn load_thread(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Option<ThreadFile>, PersistError> {
        read_json_or_quarantine(&self.thread_json_path(project, thread)).await
    }

    /// Save a thread file atomically.
    pub async fn save_thread(
        &self,
        project: ProjectId,
        thread: &ThreadFile,
    ) -> Result<(), PersistError> {
        atomic_write_json(&self.thread_json_path(project, thread.id), thread).await
    }

    /// Acquire (creating if needed) the per-thread write lock.
    async fn thread_lock(&self, thread: ThreadId) -> Arc<Mutex<()>> {
        self.thread_locks
            .lock()
            .await
            .entry(thread)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Atomically read-modify-write a thread file under its per-thread lock (spec §5.4
    /// single-writer discipline). `f` mutates the loaded [`ThreadFile`]; the result is written
    /// back atomically before the lock is released, so concurrent mutations (a turn completing
    /// while the user switches model/mode/policy) cannot lose each other's updates.
    ///
    /// Returns the updated file, or `Ok(None)` if the thread file does not exist.
    pub async fn update_thread<F>(
        &self,
        project: ProjectId,
        thread: ThreadId,
        f: F,
    ) -> Result<Option<ThreadFile>, PersistError>
    where
        F: FnOnce(&mut ThreadFile),
    {
        let lock = self.thread_lock(thread).await;
        let _guard = lock.lock().await;
        let Some(mut tf) = self.load_thread(project, thread).await? else {
            return Ok(None);
        };
        f(&mut tf);
        self.save_thread(project, &tf).await?;
        Ok(Some(tf))
    }

    // ---- Authoritative turn history (`<thread_id>.jsonl`, one Turn per line, spec §5.4 H1) ----

    /// Append a completed `Turn` as one line to the thread's authoritative JSONL history (H1/H2).
    ///
    /// The pre-serialized `JSON + "\n"` is written with a **single** `write_all` to a file opened
    /// `O_APPEND`, so on a local POSIX filesystem the offset-seek + write is atomic against
    /// concurrent writers and a process kill leaves the line all-or-nothing (no app lock needed for
    /// append ordering). This does not survive power loss (page cache) — the tolerant loader
    /// (`load_all_turns`) handles a torn final line. On NFS/network storage the atomicity guarantee
    /// does not hold (out of scope, §1.2 local-first).
    pub async fn append_turn(
        &self,
        project: ProjectId,
        thread: ThreadId,
        turn: &Turn,
    ) -> Result<(), PersistError> {
        let path = self.thread_jsonl_path(project, thread);
        let mut line =
            serde_json::to_string(turn).map_err(|e| PersistError::Serialize(e.to_string()))?;
        line.push('\n');

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| PersistError::Io(e.to_string()))?;
        }

        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            file.write_all(line.as_bytes())
        })
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?
        .map_err(|e| PersistError::Io(e.to_string()))
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
        let path = self.thread_jsonl_path(project, thread);
        let data = match tokio::fs::read_to_string(&path).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(PersistError::Io(e.to_string())),
        };

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
        let all = self.load_all_turns(project, thread).await?;
        let end = match before {
            Some(cursor) => all.iter().position(|t| t.id == cursor).unwrap_or(all.len()),
            None => all.len(),
        };
        let start = end.saturating_sub(limit);
        Ok((all[start..end].to_vec(), start > 0))
    }

    /// Rebuild the metadata token aggregates from the authoritative JSONL history (H3), for repair
    /// when a crash landed between the history append and the metadata update.
    pub async fn recompute_aggregates(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<Option<ThreadFile>, PersistError> {
        let turns = self.load_all_turns(project, thread).await?;
        self.update_thread(project, thread, move |tf| {
            let mut ledger = TokenLedger::default();
            for t in &turns {
                if matches!(
                    t.status.kind,
                    giskard_core::turn::TurnStatusKind::Completed
                        | giskard_core::turn::TurnStatusKind::Interrupted
                ) {
                    ledger.record(&t.model.provider, &t.model.model, &t.usage);
                }
            }
            tf.tokens = ledger;
        })
        .await
    }

    /// List all thread files for a project (by reading the directory).
    pub async fn list_threads(&self, project: ProjectId) -> Result<Vec<ThreadId>, PersistError> {
        let dir = self.threads_dir(project);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(PersistError::Io(e.to_string())),
        };
        let mut ids = vec![];
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?
        {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".json") {
                if let Ok(ulid) = stem.parse::<ulid::Ulid>() {
                    ids.push(ThreadId(ulid));
                }
            }
        }
        Ok(ids)
    }

    /// Delete a thread file.
    pub async fn delete_thread(
        &self,
        project: ProjectId,
        thread: ThreadId,
    ) -> Result<(), PersistError> {
        // Remove both the metadata and the authoritative history (H1).
        for path in [
            self.thread_json_path(project, thread),
            self.thread_jsonl_path(project, thread),
        ] {
            match tokio::fs::remove_file(&path).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(PersistError::Io(e.to_string())),
            }
        }
        Ok(())
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
                    if let Err(e) = self.load_thread(entry.id, tid).await {
                        errors.push((self.thread_json_path(entry.id, tid), e.to_string()));
                    }
                    if let Err(e) = self.load_all_turns(entry.id, tid).await {
                        errors.push((self.thread_jsonl_path(entry.id, tid), e.to_string()));
                    }
                }
            }
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_store() -> (TempDir, PersistStore) {
        let tmp = TempDir::new().unwrap();
        let store = PersistStore::new(tmp.path().to_path_buf());
        (tmp, store)
    }

    fn test_model() -> ModelRef {
        ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        }
    }

    #[tokio::test]
    async fn create_and_load_project() {
        let (_tmp, store) = make_store();
        let id = ProjectId::new();
        store
            .create_project(id, "test-project", "/tmp/test", test_model())
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
    async fn load_project_rejects_obsolete_approval_policy() {
        let (_tmp, store) = make_store();
        let id = ProjectId::new();
        let project = store
            .create_project(id, "test-project", "/tmp/test", test_model())
            .await
            .unwrap();

        let mut value = serde_json::to_value(&project).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("approval_policy".into(), serde_json::json!("auto"));
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
            .create_project(id, "to-delete", "/tmp/test", test_model())
            .await
            .unwrap();

        store.delete_project(id).await.unwrap();

        let index = store.load_project_index().await.unwrap();
        assert!(index.projects.is_empty());
        assert!(store.load_project(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_and_load_thread() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test", test_model())
            .await
            .unwrap();

        let tid = ThreadId::new();
        let now = Utc::now();
        let thread = ThreadFile {
            version: SCHEMA_VERSION,
            id: tid,
            project_id: pid,
            title: "Fix auth".into(),
            harness_thread_id: "th_abc".into(),
            mode: Mode::Build,
            current_model: test_model(),
            context_window: 262_144,
            approval_policy: ApprovalPolicy::Ask,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
        };
        store.save_thread(pid, &thread).await.unwrap();

        let loaded = store.load_thread(pid, tid).await.unwrap().unwrap();
        assert_eq!(loaded.title, "Fix auth");
        assert_eq!(loaded.harness_thread_id, "th_abc");
        assert_eq!(loaded.mode, Mode::Build);
    }

    #[tokio::test]
    async fn load_thread_requires_approval_policy() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test", test_model())
            .await
            .unwrap();

        let tid = ThreadId::new();
        let now = Utc::now();
        let thread = ThreadFile {
            version: SCHEMA_VERSION,
            id: tid,
            project_id: pid,
            title: "Fix auth".into(),
            harness_thread_id: "th_abc".into(),
            mode: Mode::Build,
            current_model: test_model(),
            context_window: 262_144,
            approval_policy: ApprovalPolicy::Ask,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
        };
        let mut value = serde_json::to_value(&thread).unwrap();
        value.as_object_mut().unwrap().remove("approval_policy");
        tokio::fs::create_dir_all(store.threads_dir(pid))
            .await
            .unwrap();
        tokio::fs::write(
            store.thread_json_path(pid, tid),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .await
        .unwrap();

        let result = store.load_thread(pid, tid).await;
        assert!(matches!(result.unwrap_err(), PersistError::Corrupt(_)));
    }

    #[tokio::test]
    async fn list_threads() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test", test_model())
            .await
            .unwrap();

        let t1 = ThreadId::new();
        let t2 = ThreadId::new();
        let now = Utc::now();
        for tid in [t1, t2] {
            let thread = ThreadFile {
                version: SCHEMA_VERSION,
                id: tid,
                project_id: pid,
                title: "t".into(),
                harness_thread_id: "th".into(),
                mode: Mode::Plan,
                current_model: test_model(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: HashMap::new(),
                tokens: TokenLedger::default(),
                created_at: now,
                updated_at: now,
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
            .create_project(pid, "proj", "/tmp/test", test_model())
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
            .create_project(pid, "proj", "/tmp/test", test_model())
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
            .create_project(pid, "proj", "/tmp/test", test_model())
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

    fn make_turn(usage: giskard_core::token::TokenUsage) -> Turn {
        Turn {
            id: TurnId::new(),
            user_input: giskard_core::user_input::UserInput::text("hi"),
            items: vec![],
            model: test_model(),
            mode: Mode::Build,
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
        let path = store.thread_jsonl_path(pid, tid);
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
                    version: SCHEMA_VERSION,
                    id: tid,
                    project_id: pid,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    mode: Mode::Build,
                    current_model: test_model(),
                    context_window: 0,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: HashMap::new(),
                    tokens: TokenLedger::default(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        let tf = store.recompute_aggregates(pid, tid).await.unwrap().unwrap();
        // 100+200+300 input, 30 output.
        assert_eq!(tf.tokens.total.input, 600);
        assert_eq!(tf.tokens.total.output, 30);
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
                    version: SCHEMA_VERSION,
                    id: tid,
                    project_id: pid,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    mode: Mode::Build,
                    current_model: test_model(),
                    context_window: 0,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: HashMap::new(),
                    tokens: TokenLedger::default(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        let tf = store.recompute_aggregates(pid, tid).await.unwrap().unwrap();
        assert_eq!(tf.tokens.total.input, 300);
        assert_eq!(tf.tokens.total.output, 30);
    }

    #[tokio::test]
    async fn update_thread_serializes_concurrent_writes() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test", test_model())
            .await
            .unwrap();

        let tid = ThreadId::new();
        let now = Utc::now();
        store
            .save_thread(
                pid,
                &ThreadFile {
                    version: SCHEMA_VERSION,
                    id: tid,
                    project_id: pid,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    mode: Mode::Build,
                    current_model: test_model(),
                    context_window: 0,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: HashMap::new(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
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
    }
}
