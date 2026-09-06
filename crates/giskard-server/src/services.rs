//! The process-wide service handles, split out of the registry (§B3).
//!
//! `RegistryShared` owns the identity indexes, the harness transition gate, and the background-task
//! tracker; those are the registry's own state. The five handles here are not: they are process-wide
//! and every component that reduces events needs them whether or not it needs a registry. Keeping
//! them in their own struct lets the thread event forwarder take an `Arc<Services>` and hold no
//! registry at all.

use std::sync::Arc;

use giskard_persist::PersistStore;

use crate::hub::Hub;
use crate::ledger::LedgerHandle;
use crate::thread_metadata::ThreadMetadataService;
use crate::thread_runtime::ThreadRuntimeSupport;

/// The process-wide services a thread's event owner needs, independent of the registry that
/// spawned it: where outbound messages go, where runtime state lives, where turns and metadata
/// persist, where token usage is tallied.
pub(crate) struct Services {
    pub(crate) hub: Arc<Hub>,
    pub(crate) runtime: Arc<ThreadRuntimeSupport>,
    pub(crate) store: Arc<PersistStore>,
    pub(crate) thread_metadata: Arc<ThreadMetadataService>,
    pub(crate) ledger: LedgerHandle,
}

impl Services {
    pub(crate) fn new(
        hub: Arc<Hub>,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
        max_command_output_bytes: usize,
    ) -> Self {
        // The metadata service must read and write the same store and publish to the same hub the
        // struct hands out; cloning here, before the moves below, is what keeps them the same.
        let thread_metadata = Arc::new(ThreadMetadataService::new(store.clone(), hub.clone()));
        Self {
            hub,
            runtime: Arc::new(ThreadRuntimeSupport::with_max_command_output_bytes(
                max_command_output_bytes,
            )),
            store,
            thread_metadata,
            ledger,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(hub: Arc<Hub>, store: Arc<PersistStore>, ledger: LedgerHandle) -> Self {
        Self::new(
            hub,
            store,
            ledger,
            giskard_persist::config::RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES,
        )
    }

    /// Publish the current cross-thread overview on the replacement lane.
    pub(crate) async fn publish_runtime_overview(&self) {
        self.hub
            .publish_runtime_overview(self.runtime.current_overview())
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
    use giskard_core::ids::{ApprovalId, ProjectId, ThreadId};
    use giskard_core::model::ModelRef;
    use giskard_core::thread::ThreadKind;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::{Mode, PermissionPreset, TurnMode, TurnModel};
    use giskard_persist::store::ThreadFile;
    use giskard_proto::ServerMessage;

    use crate::ledger;
    use crate::registry::ThreadAuthority;

    /// The five handles are one set: the metadata service persists into the store the struct hands
    /// out, and `publish_runtime_overview` publishes the struct's runtime through the struct's hub.
    #[tokio::test]
    async fn services_share_one_store_and_hub() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(temp.path().to_path_buf()));
        let hub = Arc::new(Hub::new());
        let services = Services::for_test(hub.clone(), store.clone(), ledger::spawn(store.clone()));

        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        store
            .create_project(project_id, "services", "/tmp/test")
            .await
            .unwrap();

        // The metadata service writes where the struct's own store reads.
        let now = Utc::now();
        services
            .thread_metadata
            .create(
                project_id,
                ThreadFile {
                    revision: 0,
                    version: giskard_persist::store::THREAD_METADATA_VERSION,
                    id: thread_id,
                    project_id,
                    title: "services".into(),
                    harness_thread_id: format!("native-{thread_id}"),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(ModelRef {
                        provider: "test".into(),
                        model: "test".into(),
                        reasoning_effort: None,
                    }),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();
        assert!(
            services
                .store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some(),
            "the metadata service persisted into the store the struct hands out"
        );

        // The overview published is the struct's runtime's own, on the replacement lane.
        let (tx, _rx) = mpsc::channel(4);
        let replacements = hub.register_client(1, tx).await;
        let authority = Arc::new(ThreadAuthority::new_for_test(thread_id, project_id));
        services.runtime.register_approval(
            &authority,
            ApprovalRequest {
                id: ApprovalId("approval-1".into()),
                kind: ApprovalKind::Permission {
                    detail: "test".into(),
                },
                reason: None,
                metadata: Vec::new(),
                available: vec![ApprovalDecision::Accept],
            },
        );
        let expected = services.runtime.current_overview();
        assert_eq!(expected.threads.len(), 1);

        services.publish_runtime_overview().await;

        match timeout(Duration::from_secs(1), replacements.recv())
            .await
            .unwrap()
        {
            ServerMessage::ThreadRuntimeOverview(overview) => {
                assert_eq!(overview.revision, expected.revision);
                assert_eq!(overview.threads.len(), 1);
                assert_eq!(overview.threads[0].thread_id, thread_id);
            }
            other => panic!("expected a runtime overview, got {other:?}"),
        }
    }
}
