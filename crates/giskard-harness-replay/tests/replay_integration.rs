//! Phase 1 integration test: open thread, one turn, assert persisted state.
//!
//! Uses `ReplayHarness` with a fixture — no live LLM calls (spec §14.2).

use std::sync::Arc;

use chrono::Utc;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
use giskard_core::model::ModelRef;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{AgentHarness, OpenThreadOptions};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};

fn make_fixture() -> (ReplayFixture, ThreadId, TurnId) {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let it_1 = ItemId::new();
    let it_2 = ItemId::new();
    let now = Utc::now();

    let events = vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_test_001".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: it_1,
                harness_item_id: "it_1".into(),
                kind: ItemKind::UserMessage,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: it_1,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::UserMessage {
                    text: "Fix the auth module".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: it_2,
                harness_item_id: "it_2".into(),
                kind: ItemKind::AgentMessage,
            },
        },
        AgentEvent::ItemDelta {
            thread,
            turn,
            item_id: it_2,
            delta: ItemDelta::Text {
                text: "I'll start by reading auth.rs".into(),
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: it_2,
                harness_item_id: "it_2".into(),
                payload: ItemPayload::AgentMessage {
                    text: "I'll start by reading auth.rs".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(1200, 340),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ];

    (ReplayFixture::from_events(events), thread, turn)
}

#[tokio::test]
async fn open_thread_one_turn_assert_state() {
    let (fixture, expected_thread, _expected_turn) = make_fixture();
    let harness = Arc::new(ReplayHarness::from_fixture(fixture));

    // Capabilities
    let caps = harness.capabilities();
    assert!(caps.plan_build_modes);
    assert!(caps.token_usage);
    assert!(caps.live_approvals);

    // Open thread
    let handle = harness
        .open_thread(OpenThreadOptions {
            project: giskard_core::ProjectId::new(),
            thread: None,
            workspace_root: "/tmp/test".into(),
            resume: Some("th_test_001".into()),
            initial_model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        })
        .await
        .expect("open_thread failed");

    assert_eq!(handle.thread, expected_thread);
    assert_eq!(handle.harness_thread_id, "th_test_001");

    // Subscribe before starting turn
    let mut stream = harness.subscribe(&handle);

    // Start turn
    let _turn_id = harness
        .start_turn(
            &handle,
            UserInput::text("Fix the auth module"),
            giskard_core::turn::TurnOverrides {
                model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                }),
                mode: Mode::Build,
                approval_policy: ApprovalPolicy::Auto,
            },
        )
        .await
        .expect("start_turn failed");

    // Collect events until TurnCompleted
    let mut events = Vec::new();
    let mut final_usage = None;
    loop {
        match stream.recv().await {
            Ok(event) => {
                if let AgentEvent::TurnCompleted { usage, .. } = &event {
                    final_usage = Some(*usage);
                }
                let is_done = matches!(event, AgentEvent::TurnCompleted { .. });
                events.push(event);
                if is_done {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }

    // Assert event sequence
    assert_eq!(events.len(), 8, "expected 8 events");

    // Event 0: ThreadOpened
    assert!(
        matches!(&events[0], AgentEvent::ThreadOpened { thread, harness_thread_id }
        if *thread == expected_thread && harness_thread_id == "th_test_001")
    );

    // Event 1: TurnStarted
    assert!(matches!(&events[1], AgentEvent::TurnStarted { thread, .. }
        if *thread == expected_thread));

    // Event 2: ItemStarted (UserMessage)
    assert!(matches!(&events[2], AgentEvent::ItemStarted { item, .. }
        if item.kind == ItemKind::UserMessage));

    // Event 3: ItemCompleted (UserMessage)
    if let AgentEvent::ItemCompleted { item, .. } = &events[3] {
        match &item.payload {
            ItemPayload::UserMessage { text } => assert_eq!(text, "Fix the auth module"),
            _ => panic!("expected UserMessage"),
        }
    } else {
        panic!("expected ItemCompleted");
    }

    // Event 4: ItemStarted (AgentMessage)
    assert!(matches!(&events[4], AgentEvent::ItemStarted { item, .. }
        if item.kind == ItemKind::AgentMessage));

    // Event 5: ItemDelta (Text)
    if let AgentEvent::ItemDelta { delta, .. } = &events[5] {
        match delta {
            ItemDelta::Text { text } => assert_eq!(text, "I'll start by reading auth.rs"),
            _ => panic!("expected Text delta"),
        }
    } else {
        panic!("expected ItemDelta");
    }

    // Event 6: ItemCompleted (AgentMessage)
    if let AgentEvent::ItemCompleted { item, .. } = &events[6] {
        match &item.payload {
            ItemPayload::AgentMessage { text } => {
                assert_eq!(text, "I'll start by reading auth.rs")
            }
            _ => panic!("expected AgentMessage"),
        }
    } else {
        panic!("expected ItemCompleted");
    }

    // Event 7: TurnCompleted with token usage
    assert!(
        matches!(&events[7], AgentEvent::TurnCompleted { usage, status, .. }
        if usage.input == 1200 && usage.output == 340 && usage.total == 1540
        && status.kind == TurnStatusKind::Completed)
    );

    // Verify token usage
    let usage = final_usage.expect("missing token usage");
    assert_eq!(usage.input, 1200);
    assert_eq!(usage.output, 340);
    assert_eq!(usage.total, 1540);

    // Shutdown
    harness.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn replay_persisted_state_roundtrip() {
    // Verify that a replayed turn's state can be persisted and reloaded.
    let (fixture, thread_id, _turn_id) = make_fixture();
    let harness = Arc::new(ReplayHarness::from_fixture(fixture));

    let handle = harness
        .open_thread(OpenThreadOptions {
            project: giskard_core::ProjectId::new(),
            thread: None,
            workspace_root: "/tmp/test".into(),
            resume: Some("th_test_001".into()),
            initial_model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        })
        .await
        .unwrap();

    let mut stream = harness.subscribe(&handle);
    let _ = harness
        .start_turn(
            &handle,
            UserInput::text("test"),
            giskard_core::turn::TurnOverrides {
                model: None,
                mode: Mode::Plan,
                approval_policy: ApprovalPolicy::ReadOnly,
            },
        )
        .await
        .unwrap();

    // Collect all events
    let mut usage = TokenUsage::default();
    loop {
        match stream.recv().await {
            Ok(AgentEvent::TurnCompleted { usage: u, .. }) => {
                usage = u;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }

    // Simulate persisting thread state (as the server would do on TurnCompleted)
    let tmp = tempfile::TempDir::new().unwrap();
    let store = giskard_persist::PersistStore::new(tmp.path().to_path_buf());

    let pid = giskard_core::ProjectId::new();
    store
        .create_project(
            pid,
            "test-proj",
            "/tmp/test",
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
            ApprovalPolicy::Ask,
        )
        .await
        .unwrap();

    let now = Utc::now();
    let thread_file = giskard_persist::store::ThreadFile {
        version: 1,
        id: thread_id,
        project_id: pid,
        title: "Fix auth".into(),
        harness_thread_id: handle.harness_thread_id.clone(),
        mode: Mode::Plan,
        current_model: ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        },
        context_window: 262_144,
        approval_policy: None,
        model_efforts: std::collections::HashMap::new(),
        tokens: giskard_core::token::TokenLedger {
            total: usage,
            by_model: Default::default(),
        },
        created_at: now,
        updated_at: now,
    };

    store.save_thread(pid, &thread_file).await.unwrap();

    // Reload and verify
    let loaded = store.load_thread(pid, thread_id).await.unwrap().unwrap();
    assert_eq!(loaded.title, "Fix auth");
    assert_eq!(loaded.mode, Mode::Plan);
    assert_eq!(loaded.tokens.total.input, 1200);
    assert_eq!(loaded.tokens.total.output, 340);
}
