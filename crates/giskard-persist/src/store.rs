//! The persistence store: load/save projects, threads, token ledgers (spec §5).

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
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
pub struct ProjectConfig {
    pub version: u32,
    pub id: ProjectId,
    pub name: String,
    pub dir: String,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
    pub default_model: ModelRef,
    pub approval_policy: ApprovalPolicy,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `projects/<id>/threads/<thread_id>.json` — thread state + history (spec §5.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub tokens: TokenLedger,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Ordered `Turn` objects, each holding its completed items (B1).
    #[serde(default)]
    pub turns: Vec<Turn>,
}

// ---- Store ----

/// The flat-file persistence store.
///
/// Owns the data directory path. Each file is guarded by a per-file async mutex
/// for single-writer discipline (spec §5.4).
pub struct PersistStore {
    data_dir: PathBuf,
    config: Mutex<Option<Config>>,
    project_index_lock: Mutex<()>,
}

impl PersistStore {
    /// Create a new store rooted at `data_dir`.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            config: Mutex::new(None),
            project_index_lock: Mutex::new(()),
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
        approval_policy: ApprovalPolicy,
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
            approval_policy,
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
        let path = self.thread_json_path(project, thread);
        match tokio::fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PersistError::Io(e.to_string())),
        }
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

            // Thread files.
            if let Ok(thread_ids) = self.list_threads(entry.id).await {
                for tid in thread_ids {
                    if let Err(e) = self.load_thread(entry.id, tid).await {
                        errors.push((self.thread_json_path(entry.id, tid), e.to_string()));
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
            .create_project(
                id,
                "test-project",
                "/tmp/test",
                test_model(),
                ApprovalPolicy::Ask,
            )
            .await
            .unwrap();

        let index = store.load_project_index().await.unwrap();
        assert_eq!(index.projects.len(), 1);
        assert_eq!(index.projects[0].name, "test-project");

        let config = store.load_project(id).await.unwrap().unwrap();
        assert_eq!(config.name, "test-project");
        assert_eq!(config.harness, "codex");
        assert_eq!(config.approval_policy, ApprovalPolicy::Ask);
    }

    #[tokio::test]
    async fn delete_project() {
        let (_tmp, store) = make_store();
        let id = ProjectId::new();
        store
            .create_project(
                id,
                "to-delete",
                "/tmp/test",
                test_model(),
                ApprovalPolicy::Auto,
            )
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
            .create_project(pid, "proj", "/tmp/test", test_model(), ApprovalPolicy::Ask)
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
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
            turns: vec![],
        };
        store.save_thread(pid, &thread).await.unwrap();

        let loaded = store.load_thread(pid, tid).await.unwrap().unwrap();
        assert_eq!(loaded.title, "Fix auth");
        assert_eq!(loaded.harness_thread_id, "th_abc");
        assert_eq!(loaded.mode, Mode::Build);
    }

    #[tokio::test]
    async fn list_threads() {
        let (_tmp, store) = make_store();
        let pid = ProjectId::new();
        store
            .create_project(pid, "proj", "/tmp/test", test_model(), ApprovalPolicy::Ask)
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
                tokens: TokenLedger::default(),
                created_at: now,
                updated_at: now,
                turns: vec![],
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
            .create_project(pid, "proj", "/tmp/test", test_model(), ApprovalPolicy::Ask)
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
            .create_project(pid, "proj", "/tmp/test", test_model(), ApprovalPolicy::Ask)
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
            .create_project(pid, "proj", "/tmp/test", test_model(), ApprovalPolicy::Ask)
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
}
