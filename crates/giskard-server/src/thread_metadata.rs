use std::sync::Arc;

use chrono::{DateTime, Utc};
use tracing::debug;

use giskard_core::CapturedDiffRecord;
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{Mode, Turn};
use giskard_persist::PersistError;
use giskard_persist::store::{
    PersistStore, ThreadFile, ThreadGitWorkspace, ThreadMutation, ThreadRecency, TurnCommitOutcome,
};
use giskard_proto::{ThreadMetadata, ThreadState};

use crate::hub::Hub;

/// Owns committed thread-metadata mutation, browser projection, and publication.
///
/// Persistence stays independent of WebSockets. Callers cannot publish a candidate which has not
/// committed because the service derives every message from [`ThreadMutation`].
pub struct ThreadMetadataService {
    store: Arc<PersistStore>,
    hub: Arc<Hub>,
}

impl ThreadMetadataService {
    pub fn new(store: Arc<PersistStore>, hub: Arc<Hub>) -> Self {
        Self { store, hub }
    }

    pub async fn mutate<F>(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
        f: F,
    ) -> Result<ThreadMutation, PersistError>
    where
        F: FnOnce(&mut ThreadFile),
    {
        self.mutate_with_recency(project_id, thread_id, ThreadRecency::Preserve, f)
            .await
    }

    pub async fn mutate_with_recency<F>(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
        recency: ThreadRecency,
        f: F,
    ) -> Result<ThreadMutation, PersistError>
    where
        F: FnOnce(&mut ThreadFile),
    {
        let mutation = self
            .store
            .update_thread_with_recency(project_id, thread_id, recency, f)
            .await?;
        self.publish(project_id, &mutation).await;
        Ok(mutation)
    }

    pub async fn recompute_aggregates(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
    ) -> Result<ThreadMutation, PersistError> {
        let mutation = self
            .store
            .recompute_aggregates(project_id, thread_id)
            .await?;
        self.publish(project_id, &mutation).await;
        Ok(mutation)
    }

    pub async fn append_turn(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
        turn: &Turn,
    ) -> Result<TurnCommitOutcome, PersistError> {
        self.append_turn_with_diffs(project_id, thread_id, turn, &[])
            .await
    }

    pub async fn append_turn_with_diffs(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
        turn: &Turn,
        captured: &[CapturedDiffRecord],
    ) -> Result<TurnCommitOutcome, PersistError> {
        let outcome = self
            .store
            .append_turn_with_diffs_and_update_aggregates(project_id, thread_id, turn, captured)
            .await?;
        if let TurnCommitOutcome::MetadataMutation(mutation) = &outcome {
            self.publish(project_id, mutation).await;
        }
        Ok(outcome)
    }

    pub async fn create(
        &self,
        project_id: ProjectId,
        thread: ThreadFile,
    ) -> Result<ThreadFile, PersistError> {
        self.store.create_thread(project_id, thread).await
    }

    /// Publish a creation only after its surrounding native/worktree setup has committed.
    pub async fn publish_created(&self, project_id: ProjectId, thread: &ThreadFile) {
        self.invalidate_thread_catalog(project_id, thread.id, thread.revision)
            .await;
    }

    pub async fn delete(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
    ) -> Result<(), PersistError> {
        self.store.delete_thread(project_id, thread_id).await?;
        self.invalidate_thread_catalog(project_id, thread_id, 0)
            .await;
        Ok(())
    }

    pub fn thread_state(thread: &ThreadFile, active_turn: Option<bool>) -> ThreadState {
        ThreadState {
            metadata: Self::metadata(thread),
            active_turn,
        }
    }

    pub fn metadata(thread: &ThreadFile) -> ThreadMetadata {
        ThreadMetadata {
            thread_id: thread.id,
            revision: thread.revision,
            title: thread.title.clone(),
            mode: thread.mode,
            current_model: thread.current_model.clone(),
            context_window: thread.context_window,
            permission_preset: thread.permission_preset,
            tokens: thread.tokens.clone(),
        }
    }

    async fn publish(&self, project_id: ProjectId, mutation: &ThreadMutation) {
        let ThreadMutation::Changed { before, after } = mutation else {
            return;
        };

        if ThreadDetailProjection::from(before.as_ref())
            != ThreadDetailProjection::from(after.as_ref())
        {
            debug!(
                %project_id,
                thread_id = %after.id,
                metadata_revision = after.revision,
                action = "publish_thread_metadata",
                "publishing committed thread metadata"
            );
            self.hub
                .publish_thread_metadata(after.id, Self::thread_state(after, None))
                .await;
        }

        // `updated_at` is part of this projection, so every completed turn invalidates the
        // catalog and each connected browser refetches the project's full thread list. We accept
        // that local-first scaling tradeoff until measurement shows it is material; removing
        // `updated_at` would break sidebar recency. The two intended escalation paths are to
        // debounce catalog refetches in the browser, or cache the projected catalog on the server.
        if ThreadCatalogProjection::from(before.as_ref())
            != ThreadCatalogProjection::from(after.as_ref())
        {
            self.invalidate_thread_catalog(project_id, after.id, after.revision)
                .await;
        }
    }

    async fn invalidate_thread_catalog(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
        metadata_revision: u64,
    ) {
        debug!(
            %project_id,
            %thread_id,
            metadata_revision,
            action = "invalidate_thread_catalog",
            "publishing thread-catalog invalidation"
        );
        self.hub.invalidate_thread_catalog(project_id).await;
    }
}

#[derive(PartialEq, Eq)]
struct ThreadDetailProjection {
    // `title` and `mode` are deliberately the complete intersection with
    // `ThreadCatalogProjection`. The browser compares that same common set when projections with
    // one revision arrive over different transports. Keep both definitions and the browser
    // reconciliation invariant synchronized when adding a browser-visible field.
    title: String,
    mode: Mode,
    current_model: giskard_core::model::ModelRef,
    context_window: u32,
    permission_preset: giskard_core::turn::PermissionPreset,
    tokens: giskard_core::token::TokenLedger,
}

impl From<&ThreadFile> for ThreadDetailProjection {
    fn from(thread: &ThreadFile) -> Self {
        Self {
            title: thread.title.clone(),
            mode: thread.mode,
            current_model: thread.current_model.clone(),
            context_window: thread.context_window,
            permission_preset: thread.permission_preset,
            tokens: thread.tokens.clone(),
        }
    }
}

#[derive(PartialEq, Eq)]
struct ThreadCatalogProjection {
    // See `ThreadDetailProjection`: `title` and `mode` are the only fields common to both browser
    // projections and form the cross-transport same-revision consistency check.
    title: String,
    parent_thread_id: Option<ThreadId>,
    spawned_by_turn_id: Option<TurnId>,
    kind: ThreadKind,
    mode: Mode,
    archived: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    git_workspace: Option<ThreadGitWorkspace>,
}

impl From<&ThreadFile> for ThreadCatalogProjection {
    fn from(thread: &ThreadFile) -> Self {
        Self {
            title: thread.title.clone(),
            parent_thread_id: thread.parent_thread_id,
            spawned_by_turn_id: thread.spawned_by_turn_id,
            kind: thread.kind,
            mode: thread.mode,
            archived: thread.archived,
            created_at: thread.created_at,
            updated_at: thread.updated_at,
            git_workspace: thread.git_workspace.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::PermissionPreset;
    use giskard_proto::ServerMessage;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    use super::*;

    fn thread(project_id: ProjectId, thread_id: ThreadId) -> ThreadFile {
        let now = Utc::now();
        ThreadFile {
            revision: 0,
            version: 1,
            id: thread_id,
            project_id,
            title: "Thread".into(),
            harness_thread_id: "native".into(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind: ThreadKind::Primary,
            mode: Mode::Build,
            current_model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
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
    async fn publishes_only_changed_browser_projections() {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(temp.path().to_path_buf()));
        let hub = Arc::new(Hub::new());
        let service = ThreadMetadataService::new(store, hub.clone());
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        service
            .create(project_id, thread(project_id, thread_id))
            .await
            .unwrap();
        let (tx, _rx) = mpsc::channel(8);
        let replacements = hub.register_client(1, tx.clone()).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let other_model = ModelRef {
            provider: "proxy".into(),
            model: "other".into(),
            reasoning_effort: None,
        };
        service
            .mutate(project_id, thread_id, |thread| {
                thread.record_model_context_window(&other_model, 64_000);
            })
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(20), replacements.recv())
                .await
                .is_err()
        );

        service
            .mutate(project_id, thread_id, |thread| {
                let selected = thread.current_model.clone();
                thread.record_model_context_window(&selected, 258_400);
            })
            .await
            .unwrap();
        let message = timeout(Duration::from_secs(1), replacements.recv())
            .await
            .unwrap();
        match message {
            ServerMessage::ThreadState(state) => {
                assert_eq!(state.metadata.revision, 3);
                assert_eq!(state.metadata.context_window, 258_400);
                assert_eq!(state.active_turn, None);
            }
            other => panic!("expected thread state, got {other:?}"),
        }
        assert!(
            timeout(Duration::from_millis(20), replacements.recv())
                .await
                .is_err()
        );

        service
            .mutate_with_recency(
                project_id,
                thread_id,
                ThreadRecency::TouchIfChanged,
                |thread| thread.title = "Renamed".into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            replacements.recv().await,
            ServerMessage::ThreadState(_)
        ));
        assert!(matches!(
            replacements.recv().await,
            ServerMessage::ThreadCatalogChanged
        ));
    }

    #[tokio::test]
    async fn turn_commit_and_crash_repair_publish_committed_metadata() {
        use giskard_core::ids::TurnId;
        use giskard_core::token::TokenUsage;
        use giskard_core::turn::{TurnStatus, TurnStatusKind};
        use giskard_core::user_input::UserInput;

        let temp = TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(temp.path().to_path_buf()));
        let hub = Arc::new(Hub::new());
        let service = ThreadMetadataService::new(store.clone(), hub.clone());
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        service
            .create(project_id, thread(project_id, thread_id))
            .await
            .unwrap();
        let (tx, _rx) = mpsc::channel(8);
        let replacements = hub.register_client(1, tx.clone()).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let turn = Turn {
            id: TurnId::new(),
            user_input: UserInput::text("hello"),
            items: vec![],
            model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
            mode: Mode::Build,
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: TokenUsage::new(100, 10),
            diffs: vec![],
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
        };
        assert!(matches!(
            service
                .append_turn(project_id, thread_id, &turn)
                .await
                .unwrap(),
            TurnCommitOutcome::MetadataMutation(ThreadMutation::Changed { .. })
        ));
        let ServerMessage::ThreadState(committed) = replacements.recv().await else {
            panic!("turn commit should publish thread metadata");
        };
        assert_eq!(committed.metadata.tokens.total.total, 110);
        assert!(matches!(
            replacements.recv().await,
            ServerMessage::ThreadCatalogChanged
        ));

        store
            .update_thread(project_id, thread_id, |thread| {
                thread.tokens = TokenLedger::default();
            })
            .await
            .unwrap();
        assert!(matches!(
            service
                .recompute_aggregates(project_id, thread_id)
                .await
                .unwrap(),
            ThreadMutation::Changed { .. }
        ));
        let ServerMessage::ThreadState(repaired) = replacements.recv().await else {
            panic!("aggregate repair should publish thread metadata");
        };
        assert_eq!(repaired.metadata.tokens.total.total, 110);
    }
}
