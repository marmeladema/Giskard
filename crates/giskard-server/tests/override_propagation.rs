//! Regression test for the turn-override snapshot the server hands the harness.
//!
//! Guards two fixes: (1) the thread's current model + reasoning effort must reach `start_turn`
//! so mid-thread model/effort changes take effect (§8.4/§8.5); (2) the thread's permission preset
//! must reach the harness (§9). A capturing harness records every `TurnOverrides` it is handed.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use futures_util::SinkExt;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{Mode, PermissionPreset, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_proto::ClientMessage;
use giskard_testenv::{TestServer, factory, fixtures, ws};
use tokio::sync::Mutex as TokioMutex;

/// Harness that records the overrides passed to `start_turn` and emits a trivial completed turn.
struct CapturingHarness {
    captured: Arc<TokioMutex<Vec<TurnOverrides>>>,
    tx: Arc<EventLog>,
    thread_id: StdMutex<Option<ThreadId>>,
    /// What each `open_thread` asked for.
    requested_models: Arc<StdMutex<Vec<ModelRef>>>,
}

impl CapturingHarness {
    fn with_requests(
        captured: Arc<TokioMutex<Vec<TurnOverrides>>>,
        requested_models: Arc<StdMutex<Vec<ModelRef>>>,
    ) -> Self {
        let tx = Arc::new(EventLog::new());
        Self {
            captured,
            tx,
            requested_models,
            thread_id: StdMutex::new(None),
        }
    }
}

#[async_trait]
impl AgentHarness for CapturingHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            live_approvals: true,
            plan_build_modes: true,
            per_turn_model: true,
            reasoning_effort: true,
            structured_diffs: true,
            resumable_threads: true,
            model_listing: false,
            provider_listing: false,
            token_usage: true,
            mcp_status: false,
            mcp_reload: false,
            mcp_oauth_login: false,
            context_compaction: false,
        }
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        Ok(vec![])
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let tid = opts.thread;
        *self.thread_id.lock().unwrap() = Some(tid);
        self.requested_models
            .lock()
            .unwrap()
            .push(opts.initial_model.clone());
        Ok(ThreadHandle {
            resumed_model: Some(opts.initial_model.clone()),
            ..ThreadHandle::opened(
                tid,
                opts.resume.unwrap_or_else(|| "cap".into()),
                opts.workspace_root.clone(),
            )
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        self.captured.lock().await.push(overrides);
        let tid = thread.thread;
        let turn = TurnId::new();
        // Drive a minimal turn so the server-side forwarder completes and persists.
        let _ = self
            .tx
            .append(AgentEvent::TurnStarted { thread: tid, turn });
        let _ = self.tx.append(AgentEvent::TurnCompleted {
            thread: tid,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        });
        Ok(turn)
    }

    fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
        AgentEventStream::new(self.tx.reader())
    }

    async fn respond_approval(
        &self,
        _req: ApprovalId,
        _decision: giskard_core::approval::ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn respond_server_request(
        &self,
        _req: ServerRequestId,
        _response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

#[tokio::test]
async fn send_input_snapshot_carries_model_effort_and_permission_preset() {
    let captured = Arc::new(TokioMutex::new(Vec::<TurnOverrides>::new()));
    let requested_models = Arc::new(StdMutex::new(Vec::new()));
    let captured_for_factory = captured.clone();
    let models_for_factory = requested_models.clone();
    let factory = factory::from_fn(move |_, _| {
        Ok(Arc::new(CapturingHarness::with_requests(
            captured_for_factory.clone(),
            models_for_factory.clone(),
        )))
    });
    let server = TestServer::builder(factory)
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
        requested_models.lock().unwrap().as_slice(),
        &[fixtures::fake_native_model()],
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

    let first = wait_for_capture(&captured, 1).await;
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

    let second = wait_for_capture(&captured, 2).await;
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

    let third = wait_for_capture(&captured, 3).await;
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
async fn wait_for_capture(
    captured: &Arc<TokioMutex<Vec<TurnOverrides>>>,
    n: usize,
) -> TurnOverrides {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        {
            let guard = captured.lock().await;
            if guard.len() >= n {
                return guard[n - 1].clone();
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("expected {n} captured overrides");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }
}
