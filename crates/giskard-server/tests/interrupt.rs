//! Regression coverage for live-turn and running-command control through the browser WebSocket
//! protocol.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{
    CommandExecutionStart, Item, ItemDelta, ItemKind, ItemPayload, ItemStart,
};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness::ThreadHandle;
use giskard_proto::{ClientMessage, RunningTask, ServerMessage, WireAgentEvent};
use giskard_testenv::fake::{self, Call, FakeCore, FakeHarness, Script, TurnCall};
use giskard_testenv::{TestServer, TestWs, ws};
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

struct InterruptScript {
    active: Mutex<Option<(ThreadId, TurnId)>>,
    command: Mutex<Option<(ThreadId, TurnId, ItemId)>>,
    interrupt_delay: Mutex<Option<Duration>>,
    terminate_behavior: Mutex<TerminateBehavior>,
}

impl InterruptScript {
    fn new() -> Self {
        Self {
            active: Mutex::new(None),
            command: Mutex::new(None),
            interrupt_delay: Mutex::new(None),
            terminate_behavior: Mutex::new(TerminateBehavior::Succeed),
        }
    }

    async fn set_interrupt_delay(&self, delay: Duration) {
        *self.interrupt_delay.lock().await = Some(delay);
    }

    async fn set_terminate_behavior(&self, behavior: TerminateBehavior) {
        *self.terminate_behavior.lock().await = behavior;
    }

    async fn complete_command(&self, core: &FakeCore) {
        let Some((thread, turn, item_id)) = *self.command.lock().await else {
            panic!("command did not start");
        };
        core.append(
            thread,
            AgentEvent::ItemCompleted {
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
            },
        );
    }
}

#[async_trait]
impl Script for InterruptScript {
    fn native_thread_id(&self, _thread: ThreadId) -> String {
        "interrupt_harness".into()
    }

    async fn start_turn(&self, _core: &FakeCore, call: &TurnCall) -> Result<(), HarnessError> {
        *self.active.lock().await = Some((call.thread, call.turn));
        call.log.append(AgentEvent::TurnStarted {
            thread: call.thread,
            turn: call.turn,
        });
        let command_item = ItemId::new();
        *self.command.lock().await = Some((call.thread, call.turn, command_item));
        call.log.append(AgentEvent::ItemStarted {
            thread: call.thread,
            turn: call.turn,
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
        call.log.append(AgentEvent::ItemDelta {
            thread: call.thread,
            turn: call.turn,
            item_id: command_item,
            delta: ItemDelta::CommandOutput {
                chunk: "started".into(),
            },
        });
        Ok(())
    }

    async fn interrupt(&self, core: &FakeCore, thread: &ThreadHandle) -> Result<(), HarnessError> {
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
        core.append(
            thread.thread,
            AgentEvent::TurnCompleted {
                thread: thread.thread,
                turn,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Interrupted,
                    message: Some("Interrupted by user.".into()),
                },
            },
        );
        Ok(())
    }

    async fn terminate_command(
        &self,
        _core: &FakeCore,
        _thread: &ThreadHandle,
        _process_id: &str,
    ) -> Result<(), HarnessError> {
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
}

struct TestApp {
    server: TestServer,
    harness: Arc<FakeHarness<InterruptScript>>,
    thread_id: ThreadId,
}

impl TestApp {
    async fn connect_ws(&self) -> TestWs {
        self.server.ws().await
    }
}

async fn spawn_test_app() -> TestApp {
    let harness = FakeHarness::new(InterruptScript::new());
    let server = TestServer::spawn(fake::factory(harness.clone())).await;
    let project = server.create_project("proj").await;
    let thread_id = server.register_thread(project.id, "interrupt_thread").await;
    TestApp {
        server,
        harness,
        thread_id,
    }
}

async fn wait_until_active(core: &FakeCore) {
    core.wait_for_call(|call| matches!(call, Call::StartTurn { .. }).then_some(()))
        .await;
}

async fn wait_until_terminated(core: &FakeCore) {
    core.wait_for_call(|call| matches!(call, Call::TerminateCommand { .. }).then_some(()))
        .await;
}

fn interrupted_threads(core: &FakeCore) -> Vec<ThreadId> {
    core.calls()
        .iter()
        .filter_map(|call| match call {
            Call::Interrupt { thread } => Some(*thread),
            _ => None,
        })
        .collect()
}

fn terminated_processes(core: &FakeCore) -> Vec<String> {
    core.calls()
        .iter()
        .filter_map(|call| match call {
            Call::TerminateCommand { process_id, .. } => Some(process_id.clone()),
            _ => None,
        })
        .collect()
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

    wait_until_active(&app.harness.core).await;
    wait_for_running_command(&mut ws).await;

    ws.send(ws::text(&ClientMessage::Interrupt { thread_id }))
        .await
        .unwrap();
    wait_for_interrupted_turn(&mut ws).await;

    app.harness.script.complete_command(&app.harness.core).await;
    wait_for_completed_command_after_interrupted_turn(&mut ws).await;

    ws.send(ws::text(&ClientMessage::TerminateCommand {
        thread_id,
        process_id: "proc_1".into(),
    }))
    .await
    .unwrap();

    wait_until_terminated(&app.harness.core).await;
    assert_eq!(terminated_processes(&app.harness.core), vec!["proc_1"]);
    assert_eq!(interrupted_threads(&app.harness.core), vec![thread_id]);
}

#[tokio::test]
async fn websocket_interrupt_timeout_surfaces_error() {
    let app = spawn_test_app().await;
    app.harness
        .script
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
    wait_until_active(&app.harness.core).await;

    ws.send(ws::text(&ClientMessage::Interrupt { thread_id }))
        .await
        .unwrap();

    let error = ws::expect_error_for(&mut ws, "interrupt", "harness_timeout").await;
    assert_eq!(error.thread_id, Some(thread_id));
    assert_eq!(interrupted_threads(&app.harness.core), vec![thread_id]);
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

    wait_until_active(&app.harness.core).await;
    wait_for_running_command(&mut ws).await;

    ws.send(ws::text(&ClientMessage::TerminateCommand {
        thread_id,
        process_id: "proc_1".into(),
    }))
    .await
    .unwrap();

    wait_until_terminated(&app.harness.core).await;
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

    app.harness.script.complete_command(&app.harness.core).await;
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

    wait_until_active(&app.harness.core).await;
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
    app.harness.script.set_terminate_behavior(behavior).await;
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

    wait_until_active(&app.harness.core).await;
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
    app.harness.script.set_terminate_behavior(behavior).await;
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

    wait_until_active(&app.harness.core).await;
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
