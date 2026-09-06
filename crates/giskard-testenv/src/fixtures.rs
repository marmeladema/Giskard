use std::collections::HashMap;

use chrono::Utc;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::model::ModelRef;
use giskard_core::thread::ThreadKind;
use giskard_core::token::{TokenLedger, TokenUsage};
use giskard_core::turn::{
    Mode, PermissionPreset, Turn, TurnMode, TurnModel, TurnStatus, TurnStatusKind,
};
use giskard_harness_replay::ReplayFixture;
use giskard_persist::PersistStore;
use giskard_persist::store::{THREAD_METADATA_VERSION, ThreadFile, ThreadGitWorkspace};

pub const TINY_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D', b'R',
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0a, b'I', b'D', b'A', b'T', 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, b'I', b'E', b'N', b'D', 0xae,
    0x42, 0x60, 0x82,
];

pub const COMPLETED_TURN_HARNESS_THREAD_ID: &str = "th_tok";

pub fn fake_native_model() -> ModelRef {
    ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    }
}

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

pub fn completed_turn_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let item = ItemId::new();
    let now = Utc::now();
    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: COMPLETED_TURN_HARNESS_THREAD_ID.into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: item,
                harness_item_id: "it_1".into(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "done".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(100, 50),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

pub fn completed_turn(text: &str, model: ModelRef) -> Turn {
    let now = Utc::now();
    Turn {
        id: TurnId::new(),
        user_input: giskard_core::user_input::UserInput::text(text),
        items: vec![Item {
            id: ItemId::new(),
            harness_item_id: String::new(),
            payload: ItemPayload::AgentMessage {
                text: text.to_string(),
            },
            created_at: now,
        }],
        model: TurnModel::Known(model),
        mode: TurnMode::Known(Mode::Build),
        status: TurnStatus {
            kind: TurnStatusKind::Completed,
            message: None,
        },
        usage: TokenUsage::new(1, 1),
        diffs: vec![],
        started_at: now,
        completed_at: Some(now),
    }
}

pub fn orphaned_thread(
    project_id: ProjectId,
    thread_id: ThreadId,
    model: ModelRef,
    git_workspace: Option<ThreadGitWorkspace>,
) -> ThreadFile {
    let now = Utc::now();
    ThreadFile {
        revision: 0,
        version: THREAD_METADATA_VERSION,
        id: thread_id,
        project_id,
        title: "Orphaned thread".into(),
        harness_thread_id: format!("harness-{thread_id}"),
        parent_thread_id: None,
        spawned_by_turn_id: None,
        kind: ThreadKind::Primary,
        mode: TurnMode::Known(Mode::Build),
        current_model: TurnModel::Known(model),
        context_window: 131_072,
        model_context_windows: Default::default(),
        permission_preset: PermissionPreset::AskFirst,
        model_efforts: HashMap::new(),
        tokens: TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived: false,
        git_workspace,
    }
}

#[cfg(test)]
mod tests {
    use giskard_core::event::AgentEvent;
    use giskard_core::token::TokenUsage;

    #[test]
    fn completed_fixture_has_the_expected_bounds() {
        let fixture = super::completed_turn_fixture();
        assert!(
            matches!(fixture.events.first(), Some(AgentEvent::ThreadOpened { harness_thread_id, .. }) if harness_thread_id == super::COMPLETED_TURN_HARNESS_THREAD_ID)
        );
        assert!(
            matches!(fixture.events.last(), Some(AgentEvent::TurnCompleted { usage, .. }) if *usage == TokenUsage::new(100, 50))
        );
    }
}
