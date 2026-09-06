//! Regression coverage for live-turn and running-command control through the browser WebSocket
//! protocol.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    CommandExecutionStart, Item, ItemDelta, ItemKind, ItemPayload, ItemStart,
};
use giskard_core::model::ModelDescriptor;
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_proto::{ClientMessage, RunningTask, ServerMessage, WireAgentEvent};
use giskard_testenv::{TestServer, TestWs, factory, ws};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum TerminateBehavior {
    Succeed,
    NoActiveCommand,
    NoActiveTurn,
    TransportError,
    Unsupported,
}

struct InterruptHarness {
    tx: Arc<EventLog>,
    active: Mutex<Option<(ThreadId, TurnId)>>,
    command: Mutex<Option<(ThreadId, TurnId, ItemId)>>,
    interrupted: Mutex<Vec<ThreadId>>,
    interrupt_delay: Mutex<Option<Duration>>,
    terminated: Mutex<Vec<String>>,
    terminate_behavior: Mutex<TerminateBehavior>,
}

impl InterruptHarness {
    fn new() -> Self {
        let tx = Arc::new(EventLog::new());
        Self {
            tx,
            active: Mutex::new(None),
            command: Mutex::new(None),
            interrupted: Mutex::new(Vec::new()),
            interrupt_delay: Mutex::new(None),
            terminated: Mutex::new(Vec::new()),
            terminate_behavior: Mutex::new(TerminateBehavior::Succeed),
        }
    }

    async fn wait_until_active(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.active.lock().await.is_some() {
                return;
            }
            if Instant::now() >= deadline {
                panic!("turn did not become active");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn interrupted_threads(&self) -> Vec<ThreadId> {
        self.interrupted.lock().await.clone()
    }

    async fn set_interrupt_delay(&self, delay: Duration) {
        *self.interrupt_delay.lock().await = Some(delay);
    }

    async fn terminated_processes(&self) -> Vec<String> {
        self.terminated.lock().await.clone()
    }

    async fn wait_until_terminated(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if !self.terminated.lock().await.is_empty() {
                return;
            }
            if Instant::now() >= deadline {
                panic!("command termination did not reach harness");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn set_terminate_behavior(&self, behavior: TerminateBehavior) {
        *self.terminate_behavior.lock().await = behavior;
    }

    async fn complete_command(&self) {
        let Some((thread, turn, item_id)) = *self.command.lock().await else {
            panic!("command did not start");
        };
        let _ = self.tx.append(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: "cmd1".into(),
                payload: ItemPayload::CommandExecution {
                    command: "sleep 60".into(),
                    cwd: "/tmp/project".into(),
                    output: "started\nfinished".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: Some("proc_1".into()),
                    duration_ms: Some(60_000),
                },
                created_at: Utc::now(),
            },
        });
    }
}

#[async_trait]
impl AgentHarness for InterruptHarness {
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
        let thread = opts.thread;
        Ok(ThreadHandle {
            resumed_model: Some(opts.initial_model.clone()),
            ..ThreadHandle::opened(
                thread,
                opts.resume.unwrap_or_else(|| "interrupt_harness".into()),
                opts.workspace_root.clone(),
            )
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn = TurnId::new();
        *self.active.lock().await = Some((thread.thread, turn));
        let _ = self.tx.append(AgentEvent::TurnStarted {
            thread: thread.thread,
            turn,
        });
        let command_item = ItemId::new();
        *self.command.lock().await = Some((thread.thread, turn, command_item));
        let _ = self.tx.append(AgentEvent::ItemStarted {
            thread: thread.thread,
            turn,
            item: ItemStart {
                id: command_item,
                harness_item_id: "cmd1".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 60".into(),
                    cwd: "/tmp/project".into(),
                    status: Some("in_progress".into()),
                    process_id: Some("proc_1".into()),
                    started_at_ms: Some(Utc::now().timestamp_millis()),
                }),
                tool: None,
            },
        });
        let _ = self.tx.append(AgentEvent::ItemDelta {
            thread: thread.thread,
            turn,
            item_id: command_item,
            delta: ItemDelta::CommandOutput {
                chunk: "started".into(),
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

    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.interrupted.lock().await.push(thread.thread);
        if let Some(delay) = *self.interrupt_delay.lock().await {
            tokio::time::sleep(delay).await;
        }
        let turn = self
            .active
            .lock()
            .await
            .take()
            .map(|(_, turn)| turn)
            .unwrap_or_default();
        let _ = self.tx.append(AgentEvent::TurnCompleted {
            thread: thread.thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Interrupted,
                message: Some("Interrupted by user.".into()),
            },
        });
        Ok(())
    }

    async fn terminate_command(
        &self,
        _thread: &ThreadHandle,
        process_id: &str,
    ) -> Result<(), HarnessError> {
        self.terminated.lock().await.push(process_id.to_owned());
        match *self.terminate_behavior.lock().await {
            TerminateBehavior::Succeed => Ok(()),
            TerminateBehavior::NoActiveCommand => Err(HarnessError::Transport(
                "JSON-RPC error (-32600): no active command/exec for process id \"proc_1\"".into(),
            )),
            TerminateBehavior::NoActiveTurn => Err(HarnessError::Transport(
                "JSON-RPC error (-32600): no active turn to interrupt".into(),
            )),
            TerminateBehavior::TransportError => {
                Err(HarnessError::Transport("terminate failed".into()))
            }
            TerminateBehavior::Unsupported => Err(HarnessError::Unsupported(
                "command termination disabled".into(),
            )),
        }
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct TestApp {
    server: TestServer,
    harness: Arc<InterruptHarness>,
    thread_id: ThreadId,
}

impl TestApp {
    async fn connect_ws(&self) -> TestWs {
        self.server.ws().await
    }
}

async fn spawn_test_app() -> TestApp {
    let harness = Arc::new(InterruptHarness::new());
    let server = TestServer::spawn(factory::shared(harness.clone())).await;
    let project = server.create_project("proj").await;
    let thread_id = server.register_thread(project.id, "interrupt_thread").await;
    TestApp {
        server,
        harness,
        thread_id,
    }
}

#[tokio::test]
async fn server_shutdown_closes_websocket_with_away_frame() {
    let app = spawn_test_app().await;
    let mut ws = app.connect_ws().await;

    app.server.state.shutdown.trigger();

    let frame = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = ws
                .next()
                .await
                .expect("server should send a close frame")
                .expect("close frame should be readable");
            if let tokio_tungstenite::tungstenite::Message::Close(Some(frame)) = message {
                break frame;
            }
        }
    })
    .await
    .expect("server should close WebSocket promptly");
    assert_eq!(
        frame.code,
        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away
    );
    assert_eq!(frame.reason, "server shutting down");
}

#[tokio::test]
async fn websocket_interrupt_reaches_live_harness_turn() {
    let app = spawn_test_app().await;
    let mut ws = app.connect_ws().await;
    let thread_id = app.thread_id;

    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "run for a while".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    app.harness.wait_until_active().await;
    wait_for_running_command(&mut ws).await;

    ws.send(ws::text(&ClientMessage::Interrupt { thread_id }))
        .await
        .unwrap();
    wait_for_interrupted_turn(&mut ws).await;

    app.harness.complete_command().await;
    wait_for_completed_command_after_interrupted_turn(&mut ws).await;

    ws.send(ws::text(&ClientMessage::TerminateCommand {
        thread_id,
        process_id: "proc_1".into(),
    }))
    .await
    .unwrap();

    app.harness.wait_until_terminated().await;
    assert_eq!(app.harness.terminated_processes().await, vec!["proc_1"]);
    assert_eq!(app.harness.interrupted_threads().await, vec![thread_id]);
}

#[tokio::test]
async fn websocket_interrupt_timeout_surfaces_error() {
    let app = spawn_test_app().await;
    app.harness
        .set_interrupt_delay(Duration::from_secs(10))
        .await;
    let mut ws = app.connect_ws().await;
    let thread_id = app.thread_id;

    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "sleep".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();
    app.harness.wait_until_active().await;

    ws.send(ws::text(&ClientMessage::Interrupt { thread_id }))
        .await
        .unwrap();

    let error = ws::expect_error_for(&mut ws, "interrupt", "harness_timeout").await;
    assert_eq!(error.thread_id, Some(thread_id));
    assert_eq!(app.harness.interrupted_threads().await, vec![thread_id]);
}

#[tokio::test]
async fn websocket_terminate_running_command_marks_terminating_until_terminal_event() {
    let app = spawn_test_app().await;
    let mut ws = app.connect_ws().await;
    let thread_id = app.thread_id;

    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "run for a while".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    app.harness.wait_until_active().await;
    wait_for_running_command(&mut ws).await;

    ws.send(ws::text(&ClientMessage::TerminateCommand {
        thread_id,
        process_id: "proc_1".into(),
    }))
    .await
    .unwrap();

    app.harness.wait_until_terminated().await;
    wait_for_terminating_command(&mut ws).await;
    let snapshot = app
        .server
        .state
        .registry
        .thread_runtime(thread_id)
        .await
        .unwrap()
        .tasks_snapshot()
        .1;
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot[0].terminating);

    app.harness.complete_command().await;
    wait_for_completed_command_after_interrupted_turn(&mut ws).await;
    assert!(
        app.server
            .state
            .registry
            .thread_runtime(thread_id)
            .await
            .unwrap()
            .tasks_snapshot()
            .1
            .is_empty()
    );
}

#[tokio::test]
async fn websocket_subscribe_replays_running_command_snapshot() {
    let app = spawn_test_app().await;
    let mut first = app.connect_ws().await;
    let thread_id = app.thread_id;

    first
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    first
        .send(ws::text(&ClientMessage::SendInput {
            thread_id,
            text: "run for a while".into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

    app.harness.wait_until_active().await;
    wait_for_running_command(&mut first).await;

    let mut second = app.connect_ws().await;
    second
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    let replayed = wait_for_running_command(&mut second).await;
    assert_eq!(replayed.process_id.as_deref(), Some("proc_1"));
}

#[tokio::test]
async fn websocket_terminate_transport_failure_preserves_snapshot() {
    terminate_failure_preserves_snapshot(
        TerminateBehavior::TransportError,
        "harness_transport_error",
    )
    .await;
}

#[tokio::test]
async fn websocket_terminate_unsupported_preserves_snapshot() {
    terminate_failure_preserves_snapshot(TerminateBehavior::Unsupported, "harness_unsupported")
        .await;
}

#[tokio::test]
async fn websocket_no_active_for_live_command_surfaces_error() {
    terminate_failure_preserves_snapshot(
        TerminateBehavior::NoActiveCommand,
        "harness_transport_error",
    )
    .await;
}

#[tokio::test]
async fn websocket_no_active_turn_for_live_command_surfaces_error() {
    terminate_failure_preserves_snapshot(
        TerminateBehavior::NoActiveTurn,
        "harness_transport_error",
    )
    .await;
}

#[tokio::test]
async fn websocket_no_active_command_for_after_turn_clears_stale_snapshot() {
    no_active_for_after_turn_command_clears_stale_snapshot(TerminateBehavior::NoActiveCommand)
        .await;
}

#[tokio::test]
async fn websocket_no_active_turn_for_after_turn_clears_stale_snapshot() {
    no_active_for_after_turn_command_clears_stale_snapshot(TerminateBehavior::NoActiveTurn).await;
}

async fn no_active_for_after_turn_command_clears_stale_snapshot(behavior: TerminateBehavior) {
    let app = spawn_test_app().await;
    app.harness.set_terminate_behavior(behavior).await;
    let mut ws = app.connect_ws().await;
    let thread_id = app.thread_id;

    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "run for a while".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    app.harness.wait_until_active().await;
    wait_for_running_command(&mut ws).await;

    ws.send(ws::text(&ClientMessage::Interrupt { thread_id }))
        .await
        .unwrap();
    wait_for_interrupted_turn(&mut ws).await;
    assert!(
        app.server
            .state
            .registry
            .thread_runtime(thread_id)
            .await
            .unwrap()
            .tasks_snapshot()
            .1[0]
            .after_turn
    );

    ws.send(ws::text(&ClientMessage::TerminateCommand {
        thread_id,
        process_id: "proc_1".into(),
    }))
    .await
    .unwrap();

    wait_for_empty_running_commands(&mut ws).await;
    let warning =
        ws::expect_error_for(&mut ws, "terminate_command", "harness_command_unmanaged").await;
    assert_eq!(warning.severity, giskard_proto::ErrorSeverity::Warning);
    assert_eq!(warning.thread_id, Some(thread_id));
    assert_eq!(warning.process_id.as_deref(), Some("proc_1"));
    assert!(warning.detail.as_deref().is_some_and(|detail| {
        detail.contains("may still be running in the harness environment")
    }));
    assert!(
        app.server
            .state
            .registry
            .thread_runtime(thread_id)
            .await
            .unwrap()
            .tasks_snapshot()
            .1
            .is_empty()
    );
}

#[tokio::test]
async fn websocket_terminate_unknown_thread_surfaces_error() {
    let app = spawn_test_app().await;
    let mut ws = app.connect_ws().await;
    let unknown_thread = ThreadId::new();

    ws.send(ws::text(&ClientMessage::TerminateCommand {
        thread_id: unknown_thread,
        process_id: "proc_missing".into(),
    }))
    .await
    .unwrap();

    let error = ws::expect_error_for(&mut ws, "terminate_command", "thread_not_open").await;
    assert_eq!(error.thread_id, Some(unknown_thread));
}

async fn terminate_failure_preserves_snapshot(behavior: TerminateBehavior, expected_code: &str) {
    let app = spawn_test_app().await;
    app.harness.set_terminate_behavior(behavior).await;
    let mut ws = app.connect_ws().await;
    let thread_id = app.thread_id;

    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "run for a while".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    app.harness.wait_until_active().await;
    wait_for_running_command(&mut ws).await;

    ws.send(ws::text(&ClientMessage::TerminateCommand {
        thread_id,
        process_id: "proc_1".into(),
    }))
    .await
    .unwrap();

    let error = ws::expect_error_for(&mut ws, "terminate_command", expected_code).await;
    assert_eq!(error.thread_id, Some(thread_id));
    assert_eq!(error.process_id.as_deref(), Some("proc_1"));
    let snapshot = app
        .server
        .state
        .registry
        .thread_runtime(thread_id)
        .await
        .unwrap()
        .tasks_snapshot()
        .1;
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].process_id.as_deref(), Some("proc_1"));
    assert!(!snapshot[0].terminating);
}

async fn wait_for_running_command(ws: &mut TestWs) -> RunningTask {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("running command snapshot was not observed");
        }

        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for running command snapshot"))
        else {
            continue;
        };

        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
        if let ServerMessage::RunningTasks { tasks, .. } = server_msg
            && let Some(cmd) = tasks
                .iter()
                .find(|cmd| cmd.process_id.as_deref() == Some("proc_1"))
        {
            assert_eq!(cmd.command, "sleep 60");
            return cmd.clone();
        }
    }
}

async fn wait_for_empty_running_commands(ws: &mut TestWs) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("empty running command snapshot was not observed");
        }

        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for empty running command snapshot"))
        else {
            continue;
        };

        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        if let ServerMessage::RunningTasks { tasks, .. } =
            serde_json::from_str::<ServerMessage>(&text).unwrap()
            && tasks.is_empty()
        {
            return;
        }
    }
}

async fn wait_for_terminating_command(ws: &mut TestWs) -> RunningTask {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("terminating running command snapshot was not observed");
        }

        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for terminating command snapshot"))
        else {
            continue;
        };

        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
        if let ServerMessage::RunningTasks { tasks, .. } = server_msg
            && let Some(cmd) = tasks
                .iter()
                .find(|cmd| cmd.process_id.as_deref() == Some("proc_1") && cmd.terminating)
        {
            return cmd.clone();
        }
    }
}

async fn wait_for_completed_command_after_interrupted_turn(ws: &mut TestWs) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_completed_command = false;
    let mut saw_empty_running_commands = false;
    loop {
        if saw_completed_command && saw_empty_running_commands {
            return;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("late command completion was not reflected in websocket messages");
        }

        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for late command completion"))
        else {
            continue;
        };

        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        match serde_json::from_str::<ServerMessage>(&text).unwrap() {
            ServerMessage::Event { agent_event, .. } => {
                if let WireAgentEvent::ItemCompleted { item, .. } = *agent_event
                    && let giskard_proto::WireItemPayload::CommandExecution {
                        status,
                        exit_code,
                        duration_ms,
                        ..
                    } = item.payload
                {
                    saw_completed_command = status.as_deref() == Some("completed")
                        && exit_code == Some(0)
                        && duration_ms == Some(60_000);
                }
            }
            ServerMessage::RunningTasks { tasks, .. } => {
                saw_empty_running_commands = tasks.is_empty();
            }
            _ => {}
        }
    }
}

async fn wait_for_interrupted_turn(ws: &mut TestWs) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("interrupted turn completion was not observed");
        }

        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for interrupted turn"))
        else {
            continue;
        };

        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
        if let ServerMessage::Event { agent_event, .. } = server_msg
            && let WireAgentEvent::TurnCompleted { status, .. } = *agent_event
        {
            assert_eq!(status.kind, TurnStatusKind::Interrupted);
            return;
        }
    }
}
