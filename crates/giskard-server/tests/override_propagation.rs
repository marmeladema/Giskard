//! Regression test for the turn-override snapshot the server hands the harness.
//!
//! Guards two fixes: (1) the thread's current model + reasoning effort must reach `start_turn`
//! so mid-thread model/effort changes take effect (§8.4/§8.5); (2) the thread's permission preset
//! must reach the harness (§9). A capturing harness records every `TurnOverrides` it is handed.

use async_trait::async_trait;
use futures_util::SinkExt;
use giskard_core::event::AgentEvent;
use giskard_core::model::{Effort, ModelRef};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{Mode, PermissionPreset, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_proto::ClientMessage;
use giskard_testenv::fake::{self, Call, FakeCore, FakeHarness, Script, TurnCall};
use giskard_testenv::{TestServer, fixtures, ws};

/// Harness that records the overrides passed to `start_turn` and emits a trivial completed turn.
struct CapturingScript;

#[async_trait]
impl Script for CapturingScript {
    fn native_thread_id(&self, _thread: giskard_core::ids::ThreadId) -> String {
        "cap".into()
    }

    async fn start_turn(
        &self,
        _core: &FakeCore,
        call: &TurnCall,
    ) -> Result<(), giskard_core::HarnessError> {
        // Drive a minimal turn so the server-side forwarder completes and persists.
        call.log.append(AgentEvent::TurnStarted {
            thread: call.thread,
            turn: call.turn,
        });
        call.log.append(AgentEvent::TurnCompleted {
            thread: call.thread,
            turn: call.turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        });
        Ok(())
    }
}

#[tokio::test]
async fn send_input_snapshot_carries_model_effort_and_permission_preset() {
    let harness = FakeHarness::new(CapturingScript);
    let server = TestServer::builder(fake::factory(harness.clone()))
        .config(
            r#"[providers.openai]
  [[providers.openai.models]]
  id = "gpt-5.5"
  context_window = 258400
  supports_reasoning_effort = true
"#,
        )
        .start()
        .await;
    let project = server.create_project("proj").await;
    let pid = project.id;
    let thread_id = server.register_thread(pid, "th_cap").await;
    assert_eq!(
        harness
            .core
            .calls()
            .iter()
            .filter_map(|call| match call {
                Call::Open { model, .. } => Some(model.clone()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![fixtures::fake_native_model()],
        "reopening a persisted thread passes its effective model"
    );
    let state = &server.state;
    let mut ws = server.ws().await;

    // Select a reasoning model with High effort (gpt-5.5 is declared in this test's config).
    ws.send(ws::text(&ClientMessage::SelectModel {
        thread_id,
        request_id: "select-model-1".into(),
        model_ref: ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: Some(Effort::new("high")),
        },
    }))
    .await
    .unwrap();
    // Switch to Plan mode.
    ws.send(ws::text(&ClientMessage::SwitchMode {
        thread_id,
        request_id: "switch-mode-1".into(),
        mode: Mode::Plan,
    }))
    .await
    .unwrap();
    // First turn.
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "plan it".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    let first = captured(&harness.core, 1).await;
    assert_eq!(
        first.model,
        Some(ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: Some(Effort::new("high")),
        }),
        "fix #1: current model + effort must reach the harness"
    );
    assert_eq!(first.mode, Mode::Plan);
    assert_eq!(
        first.permission_preset,
        PermissionPreset::AskFirst,
        "new threads default to ask first"
    );

    // Now set the thread permission preset and send again.
    ws.send(ws::text(&ClientMessage::SetPermissionPreset {
        thread_id,
        request_id: "set-permission-1".into(),
        preset: PermissionPreset::FullAccess,
    }))
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let tf = state
            .store
            .load_thread(pid, thread_id)
            .await
            .unwrap()
            .unwrap();
        if tf.permission_preset == PermissionPreset::FullAccess {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("thread permission preset was not persisted");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "again".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    let second = captured(&harness.core, 2).await;
    assert_eq!(
        second.permission_preset,
        PermissionPreset::FullAccess,
        "thread permission preset changes must reach the harness"
    );

    // Clearing effort on the same model should mean "model default", not "restore the previous
    // remembered effort".
    ws.send(ws::text(&ClientMessage::SelectModel {
        thread_id,
        request_id: "select-model-2".into(),
        model_ref: ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        },
    }))
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let tf = state
            .store
            .load_thread(pid, thread_id)
            .await
            .unwrap()
            .unwrap();
        if tf
            .current_model
            .as_known()
            .is_some_and(|model| model.reasoning_effort.is_none())
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("thread reasoning effort was not cleared");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "default effort".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    let third = captured(&harness.core, 3).await;
    assert_eq!(
        third.model,
        Some(ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        }),
        "cleared reasoning effort should not be sent to the harness"
    );
}

/// Wait until at least `n` overrides have been captured, returning the `n`-th (1-based).
async fn captured(core: &FakeCore, n: usize) -> TurnOverrides {
    core.wait_for_calls(|call| matches!(call, Call::StartTurn { .. }), n)
        .await;
    core.calls()
        .iter()
        .filter_map(|call| match call {
            Call::StartTurn { overrides, .. } => Some(overrides.clone()),
            _ => None,
        })
        .nth(n - 1)
        .unwrap()
}
