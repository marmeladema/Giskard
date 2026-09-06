//! Regression coverage for Codex-style server-initiated browser requests.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ServerRequestId, ThreadId, TurnId};
use giskard_core::model::ModelDescriptor;
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_proto::{ClientMessage, LiveTurnSnapshot, ServerMessage, WireAgentEvent};
use giskard_testenv::{TestServer, TestWs, factory, ws};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

struct ServerRequestHarness {
    tx: Arc<EventLog>,
    active: Mutex<Option<(ThreadId, TurnId)>>,
    responses: Mutex<Vec<(ServerRequestId, ServerRequestResponse)>>,
    fail_next_response: Mutex<Option<HarnessError>>,
    hang_next_response: Mutex<bool>,
    resolve_before_reply: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    /// When set, routing a response does not emit `ServerRequestResolved`/`TurnCompleted`. Real
    /// harnesses resolve on their own schedule and may never resolve at all, and that window is
    /// exactly what the reconnect snapshot has to survive.
    suppress_resolution: Mutex<bool>,
}

impl ServerRequestHarness {
    fn new() -> Self {
        let tx = Arc::new(EventLog::new());
        Self {
            tx,
            active: Mutex::new(None),
            responses: Mutex::new(Vec::new()),
            fail_next_response: Mutex::new(None),
            hang_next_response: Mutex::new(false),
            resolve_before_reply: Mutex::new(None),
            suppress_resolution: Mutex::new(false),
        }
    }

    async fn suppress_resolution(&self) {
        *self.suppress_resolution.lock().await = true;
    }

    async fn fail_next_response(&self, error: HarnessError) {
        *self.fail_next_response.lock().await = Some(error);
    }

    async fn hang_next_response(&self) {
        *self.hang_next_response.lock().await = true;
    }

    async fn resolve_before_reply(&self) -> tokio::sync::oneshot::Sender<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self.resolve_before_reply.lock().await = Some(receiver);
        sender
    }

    async fn wait_for_response(&self) -> (ServerRequestId, ServerRequestResponse) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(response) = self.responses.lock().await.first().cloned() {
                return response;
            }
            if Instant::now() >= deadline {
                panic!("server request response did not reach harness");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

#[async_trait]
impl AgentHarness for ServerRequestHarness {
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
                opts.resume
                    .unwrap_or_else(|| "server_request_harness".into()),
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
        let _ = self.tx.append(AgentEvent::ServerRequestReceived {
            thread: thread.thread,
            turn: Some(turn),
            request: ServerRequest {
                id: ServerRequestId("srv_1".into()),
                method: "item/tool/requestUserInput".into(),
                params: serde_json::json!({
                    "questions": [{
                        "id": "confirm",
                        "header": "Confirm",
                        "question": "Continue?",
                        "options": [{ "label": "Yes", "description": "Continue" }],
                    }]
                }),
                received_at: Utc::now(),
            },
        });
        Ok(turn)
    }

    fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
        AgentEventStream::new(self.tx.reader())
    }

    async fn respond_approval(
        &self,
        _req: giskard_core::ids::ApprovalId,
        _decision: giskard_core::approval::ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn respond_server_request(
        &self,
        req: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        let suppress_resolution = *self.suppress_resolution.lock().await;
        let receiver = if suppress_resolution {
            None
        } else {
            self.resolve_before_reply.lock().await.take()
        };
        let mut active = None;
        if let Some(receiver) = receiver {
            let (thread, turn) = self.active.lock().await.take().unwrap_or_default();
            active = Some((thread, turn));
            let _ = self.tx.append(AgentEvent::ServerRequestResolved {
                thread,
                turn: Some(turn),
                request_id: req.clone(),
            });
            let _ = self.tx.append(AgentEvent::Notice {
                thread,
                turn: Some(turn),
                message: "resolution-fence".into(),
            });
            let _ = receiver.await;
        }
        if let Some(error) = self.fail_next_response.lock().await.take() {
            return Err(error);
        }
        if std::mem::take(&mut *self.hang_next_response.lock().await) {
            std::future::pending::<()>().await;
        }
        self.responses
            .lock()
            .await
            .push((req.clone(), response.clone()));
        if suppress_resolution {
            return Ok(());
        }
        let (thread, turn) = match active {
            Some(active) => active,
            None => {
                let active = self.active.lock().await.take().unwrap_or_default();
                let _ = self.tx.append(AgentEvent::ServerRequestResolved {
                    thread: active.0,
                    turn: Some(active.1),
                    request_id: req,
                });
                active
            }
        };
        let _ = self.tx.append(AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        });
        Ok(())
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct TestApp {
    server: TestServer,
    harness: Arc<ServerRequestHarness>,
    thread_id: ThreadId,
}

async fn spawn_test_app() -> TestApp {
    let harness = Arc::new(ServerRequestHarness::new());
    let server = TestServer::spawn(factory::shared(harness.clone())).await;
    let project = server.create_project("proj").await;
    let thread_id = server
        .register_thread(project.id, "server_request_thread")
        .await;
    TestApp {
        server,
        harness,
        thread_id,
    }
}

#[tokio::test]
async fn websocket_server_request_response_routes_to_harness() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;
    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({
            "answers": { "confirm": { "answers": ["Yes"] } }
        })),
    }))
    .await
    .unwrap();

    let (request_id, response) = harness.wait_for_response().await;
    assert_eq!(request_id, ServerRequestId("srv_1".into()));
    match response {
        ServerRequestResponse::Result { value } => {
            assert_eq!(value["answers"]["confirm"]["answers"][0], "Yes");
        }
        ServerRequestResponse::Error { .. } => panic!("expected result response"),
    }
}

#[tokio::test]
async fn websocket_server_request_error_response_routes_to_harness() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;
    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::error(-32000, "cancelled"),
    }))
    .await
    .unwrap();

    let (request_id, response) = harness.wait_for_response().await;
    assert_eq!(request_id, ServerRequestId("srv_1".into()));
    match response {
        ServerRequestResponse::Error { code, message } => {
            assert_eq!(code, -32000);
            assert_eq!(message, "cancelled");
        }
        ServerRequestResponse::Result { .. } => panic!("expected error response"),
    }
}

#[tokio::test]
async fn websocket_server_request_response_failure_can_be_retried() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;
    harness
        .fail_next_response(HarnessError::Protocol("temporary failure".into()))
        .await;

    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({
            "answers": { "confirm": { "answers": ["Yes"] } }
        })),
    }))
    .await
    .unwrap();

    let error = ws::expect_error(&mut ws).await;
    assert_eq!(error.code, "harness_protocol_error");
    assert_eq!(error.action.as_deref(), Some("server_request_response"));
    assert!(
        error
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("temporary failure")
    );

    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({
            "answers": { "confirm": { "answers": ["Yes"] } }
        })),
    }))
    .await
    .unwrap();

    let (request_id, response) = harness.wait_for_response().await;
    assert_eq!(request_id, ServerRequestId("srv_1".into()));
    match response {
        ServerRequestResponse::Result { value } => {
            assert_eq!(value["answers"]["confirm"]["answers"][0], "Yes");
        }
        ServerRequestResponse::Error { .. } => panic!("expected retry result response"),
    }
}

#[tokio::test]
async fn timed_out_server_request_response_republishes_pending_to_peer_tabs() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut claimant = app.server.ws().await;
    let mut peer = app.server.ws().await;
    for ws in [&mut claimant, &mut peer] {
        ws.send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    }
    claimant
        .send(ws::text(&ClientMessage::SendInput {
            thread_id,
            text: "ask me".into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();
    wait_for_server_request(&mut claimant).await;
    let pending = wait_for_request_state(&mut peer, "pending").await;
    assert_eq!(pending.revision, 1);

    harness.hang_next_response().await;
    claimant
        .send(ws::text(&ClientMessage::ServerRequestResponse {
            thread_id,
            request_id: "srv_1".into(),
            response: ServerRequestResponse::result(serde_json::json!({ "answers": ["Yes"] })),
        }))
        .await
        .unwrap();

    let responding = wait_for_request_state(&mut peer, "responding").await;
    assert_eq!(responding.revision, 2);
    let error = ws::expect_error(&mut claimant).await;
    assert_eq!(error.code, "harness_timeout");
    let rolled_back = wait_for_request_state(&mut peer, "pending").await;
    assert_eq!(rolled_back.revision, 3);
}

#[tokio::test]
async fn websocket_subscribe_replays_pending_server_request_snapshot() {
    let app = spawn_test_app().await;
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;

    let mut reconnect = app.server.ws().await;
    reconnect
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();

    let snapshot = ws::expect_live_snapshot(&mut reconnect).await;
    assert_eq!(snapshot.thread_id, thread_id);
    // The outstanding server request is derived from `accumulated` plus
    // `answered_server_requests`, so the still-open one is reported as pending.
    let rows = server_request_rows(&snapshot);
    let [(request, resolved)] = &rows[..] else {
        panic!("expected exactly one server request row, got {rows:?}");
    };
    assert_eq!(request.id, ServerRequestId("srv_1".into()));
    assert_eq!(request.method, "item/tool/requestUserInput");
    assert!(
        !resolved,
        "nobody answered it, so its row must still read open"
    );
}

/// A server request the user answered must not come back actionable on reconnect, even when the
/// harness has not (or will never) emit its resolved event. Nothing recorded the answer server-side
/// before this, so the replayed `ServerRequestReceived` re-prompted and answering again routed a
/// stale id to the harness, which errors — the same defect already fixed for approvals.
#[tokio::test]
async fn answered_server_request_is_not_pending_after_reconnect() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    harness.suppress_resolution().await;

    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();
    wait_for_server_request(&mut ws).await;

    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({ "answers": ["main"] })),
    }))
    .await
    .unwrap();
    // The harness confirms it received the answer; it deliberately never resolves it.
    let (answered_id, _) = harness.wait_for_response().await;
    assert_eq!(answered_id, ServerRequestId("srv_1".into()));

    let mut reconnect = app.server.ws().await;
    reconnect
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();

    let snapshot = ws::expect_live_snapshot(&mut reconnect).await;
    // The request is still replayed — its own row is what says it was answered, so the card renders
    // resolved instead of re-prompting.
    let rows = server_request_rows(&snapshot);
    let [(request, resolved)] = &rows[..] else {
        panic!("expected exactly one server request row, got {rows:?}");
    };
    assert_eq!(request.id, ServerRequestId("srv_1".into()));
    assert!(
        resolved,
        "an answered request's row must be stamped resolved so it is not replayed as actionable"
    );
    assert_eq!(
        snapshot.answered_server_requests,
        vec![ServerRequestId("srv_1".into())],
        "the reconnect snapshot must name the answered request so its card renders resolved"
    );
    // It is still in the replayed events; naming it answered is what stops it re-prompting.
    assert!(
        snapshot.accumulated.iter().any(|event| matches!(
            event,
            giskard_proto::WireAgentEvent::ServerRequestReceived { .. }
        )),
        "the request should still appear in the accumulated events"
    );
}

#[tokio::test]
async fn server_request_answer_succeeds_when_the_harness_resolves_first() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();
    wait_for_server_request(&mut ws).await;

    let gate = harness.resolve_before_reply().await;
    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({ "answers": ["Yes"] })),
    }))
    .await
    .unwrap();
    wait_for_notice(&mut ws, "resolution-fence").await;
    gate.send(()).unwrap();

    let resolved = wait_for_resolved_and_completion_without_error(&mut ws).await;
    assert_eq!(resolved.revision, 3);
    let (request_id, response) = harness.wait_for_response().await;
    assert_eq!(request_id, ServerRequestId("srv_1".into()));
    assert_eq!(
        response,
        ServerRequestResponse::result(serde_json::json!({ "answers": ["Yes"] }))
    );
}

#[tokio::test]
async fn harness_failure_after_a_native_resolution_leaves_the_request_resolved() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    let mut peer = app.server.ws().await;
    for socket in [&mut ws, &mut peer] {
        socket
            .send(ws::text(&ClientMessage::Subscribe {
                thread_id,
                since: None,
            }))
            .await
            .unwrap();
    }
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();
    wait_for_server_request(&mut ws).await;
    assert_eq!(
        wait_for_request_state(&mut peer, "pending").await.revision,
        1
    );

    harness
        .fail_next_response(HarnessError::Protocol("late failure".into()))
        .await;
    let gate = harness.resolve_before_reply().await;
    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({ "answers": ["Yes"] })),
    }))
    .await
    .unwrap();
    assert_eq!(
        wait_for_request_state(&mut peer, "responding")
            .await
            .revision,
        2
    );
    wait_for_notice(&mut ws, "resolution-fence").await;
    gate.send(()).unwrap();

    let error = ws::expect_error(&mut ws).await;
    assert!(
        error
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("late failure")
    );
    let resolved = wait_for_request_state(&mut peer, "resolved").await;
    assert_eq!(resolved.revision, 3);

    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({ "answers": ["again"] })),
    }))
    .await
    .unwrap();
    let error = ws::expect_error(&mut ws).await;
    assert!(
        error
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("is not pending")
    );
}

#[tokio::test]
async fn timeout_after_a_native_resolution_republishes_resolved_to_peer_tabs() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut claimant = app.server.ws().await;
    let mut peer = app.server.ws().await;
    for ws in [&mut claimant, &mut peer] {
        ws.send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    }
    claimant
        .send(ws::text(&ClientMessage::SendInput {
            thread_id,
            text: "ask me".into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();
    wait_for_server_request(&mut claimant).await;
    assert_eq!(
        wait_for_request_state(&mut peer, "pending").await.revision,
        1
    );

    harness.hang_next_response().await;
    let gate = harness.resolve_before_reply().await;
    claimant
        .send(ws::text(&ClientMessage::ServerRequestResponse {
            thread_id,
            request_id: "srv_1".into(),
            response: ServerRequestResponse::result(serde_json::json!({ "answers": ["Yes"] })),
        }))
        .await
        .unwrap();
    assert_eq!(
        wait_for_request_state(&mut peer, "responding")
            .await
            .revision,
        2
    );
    wait_for_notice(&mut peer, "resolution-fence").await;
    gate.send(()).unwrap();
    assert_eq!(
        ws::expect_error(&mut claimant).await.code,
        "harness_timeout"
    );
    peer.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({ "answers": ["again"] })),
    }))
    .await
    .unwrap();
    let resolved = wait_for_resolved_and_rejection_without_pending(&mut peer).await;
    assert_eq!(resolved.revision, 3);
}

#[tokio::test]
async fn reconnect_after_a_native_resolution_during_a_claim_does_not_re_prompt() {
    let app = spawn_test_app().await;
    let harness = app.harness.clone();
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();
    wait_for_server_request(&mut ws).await;

    let gate = harness.resolve_before_reply().await;
    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({ "answers": ["Yes"] })),
    }))
    .await
    .unwrap();
    wait_for_notice(&mut ws, "resolution-fence").await;

    let mut reconnect = app.server.ws().await;
    reconnect
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    let snapshot = ws::expect_live_snapshot(&mut reconnect).await;
    let rows = server_request_rows(&snapshot);
    let [(request, resolved)] = &rows[..] else {
        panic!("expected exactly one server request row, got {rows:?}");
    };
    assert_eq!(request.id, ServerRequestId("srv_1".into()));
    assert!(*resolved);

    gate.send(()).unwrap();
    assert_eq!(
        wait_for_resolved_and_completion_without_error(&mut ws)
            .await
            .revision,
        3
    );
}

#[tokio::test]
async fn websocket_unknown_server_request_response_surfaces_error() {
    let app = spawn_test_app().await;
    let thread_id = app.thread_id;
    let mut ws = app.server.ws().await;
    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "missing".into(),
        response: ServerRequestResponse::error(-32000, "missing"),
    }))
    .await
    .unwrap();

    let error = ws::expect_error(&mut ws).await;
    assert_eq!(error.code, "harness_protocol_error");
    assert_eq!(error.action.as_deref(), Some("server_request_response"));
    assert!(error.message.contains("protocol error"));
}

async fn wait_for_server_request(ws: &mut TestWs) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&text)
                    && matches!(*agent_event, WireAgentEvent::ServerRequestReceived { .. })
                {
                    return;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("server request event not observed");
}

async fn wait_for_notice(ws: &mut TestWs, expected_message: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&text)
                    && matches!(&*agent_event, WireAgentEvent::Notice { message, .. } if message == expected_message)
                {
                    return;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("notice {expected_message:?} not observed");
}

async fn wait_for_request_state(
    ws: &mut TestWs,
    expected_status: &str,
) -> giskard_proto::RequestState {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::RequestState(state)) = serde_json::from_str(&text) {
                    let matches = matches!(
                        (&state.status, expected_status),
                        (giskard_proto::RequestStatus::Pending, "pending")
                            | (giskard_proto::RequestStatus::Responding, "responding")
                            | (giskard_proto::RequestStatus::Resolved { .. }, "resolved")
                    );
                    if state.request_id == "srv_1" && matches {
                        return state;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("request state {expected_status} not observed");
}

async fn wait_for_resolved_and_completion_without_error(
    ws: &mut TestWs,
) -> giskard_proto::RequestState {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut resolved = None;
    let mut completed = false;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                match serde_json::from_str::<ServerMessage>(&text) {
                    Ok(ServerMessage::Error { error }) => {
                        panic!("unexpected websocket error before completion: {error:?}")
                    }
                    Ok(ServerMessage::RequestState(state))
                        if state.request_id == "srv_1"
                            && matches!(
                                state.status,
                                giskard_proto::RequestStatus::Resolved { .. }
                            ) =>
                    {
                        resolved = Some(state);
                    }
                    Ok(ServerMessage::Event { agent_event, .. })
                        if matches!(*agent_event, WireAgentEvent::TurnCompleted { .. }) =>
                    {
                        completed = true;
                    }
                    _ => {}
                }
                if completed && let Some(resolved) = resolved {
                    return resolved;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("resolved request state and turn completion not observed");
}

async fn wait_for_resolved_and_rejection_without_pending(
    ws: &mut TestWs,
) -> giskard_proto::RequestState {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut resolved = None;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                match serde_json::from_str::<ServerMessage>(&text) {
                    Ok(ServerMessage::RequestState(state)) if state.request_id == "srv_1" => {
                        match state.status {
                            giskard_proto::RequestStatus::Pending => {
                                panic!(
                                    "request was republished as pending at revision {}",
                                    state.revision
                                )
                            }
                            giskard_proto::RequestStatus::Resolved { .. } => resolved = Some(state),
                            giskard_proto::RequestStatus::Responding => {}
                        }
                    }
                    Ok(ServerMessage::Error { error })
                        if error
                            .detail
                            .as_deref()
                            .unwrap_or_default()
                            .contains("is not pending") =>
                    {
                        return resolved.unwrap_or_else(|| {
                            panic!("response rejection arrived before resolved request state")
                        });
                    }
                    _ => {}
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("resolved request state and ordered response rejection not observed");
}

/// Every server-request row in the snapshot, paired with whether a reconnecting client would
/// render it resolved (answered by the user or closed by the harness). Mirrors the client's
/// `outstandingServerRequests` derivation.
fn server_request_rows(snapshot: &LiveTurnSnapshot) -> Vec<(ServerRequest, bool)> {
    let answered: std::collections::HashSet<ServerRequestId> =
        snapshot.answered_server_requests.iter().cloned().collect();
    let mut closed = std::collections::HashSet::new();
    for ev in &snapshot.accumulated {
        if let WireAgentEvent::ServerRequestResolved { request_id, .. } = ev {
            closed.insert(request_id.clone());
        }
    }
    snapshot
        .accumulated
        .iter()
        .filter_map(|event| match event {
            WireAgentEvent::ServerRequestReceived { request, .. } => Some((
                request.clone(),
                answered.contains(&request.id) || closed.contains(&request.id),
            )),
            _ => None,
        })
        .collect()
}
