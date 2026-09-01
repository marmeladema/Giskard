use std::collections::HashMap;

use chrono::Utc;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
use giskard_core::thread::ThreadKind;
use giskard_core::token::TokenLedger;
use giskard_core::turn::{Mode, PermissionPreset, TurnMode, TurnModel};
use giskard_persist::PersistStore;
use giskard_persist::store::{THREAD_METADATA_VERSION, ThreadFile};

/// Persist a primary thread that an integration test will reopen by Giskard thread id.
pub async fn persist_primary_thread(
    store: &PersistStore,
    project_id: ProjectId,
    thread_id: ThreadId,
    harness_thread_id: impl Into<String>,
    model: ModelRef,
) -> ThreadId {
    let now = Utc::now();
    store
        .create_thread(
            project_id,
            ThreadFile {
                version: THREAD_METADATA_VERSION,
                id: thread_id,
                project_id,
                revision: 0,
                title: "Test thread".into(),
                harness_thread_id: harness_thread_id.into(),
                parent_thread_id: None,
                spawned_by_turn_id: None,
                kind: ThreadKind::Primary,
                mode: TurnMode::Known(Mode::Build),
                current_model: TurnModel::Known(model),
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
        .expect("persist primary thread fixture");
    thread_id
}
