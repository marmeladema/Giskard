//! Regression coverage for Codex-style server-initiated browser requests.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::ModelDescriptor;
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_proto::{
    BootstrapSection, ClientMessage, RequestKind, RequestPayload, RequestResolution, RequestState,
    RequestStatus, RuntimeTurnState, ServerMessage, ThreadBootstrapFrame, ThreadBootstrapPayload,
    ThreadEventPayload, ThreadRuntimeOverview,
};
use giskard_server::{AppState, HarnessFactory, build_app};
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, Instant};

use common::fake_native_model;

type TestWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct ServerRequestHarness {
    tx: broadcast::Sender<AgentEvent>,
    active: Mutex<Option<(ThreadId, TurnId)>>,
    responses: Mutex<Vec<(ServerRequestId, ServerRequestResponse)>>,
    fail_next_response: Mutex<Option<HarnessError>>,
    delay_next_response: Mutex<Option<Duration>>,
    /// When set, routing a response does not emit `ServerRequestResolved`/`TurnCompleted`. Real
    /// harnesses resolve on their own schedule and may never resolve at all, and that window is
    /// exactly what the reconnect snapshot has to survive.
    suppress_resolution: Mutex<bool>,
}

impl ServerRequestHarness {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            tx,
            active: Mutex::new(None),
            responses: Mutex::new(Vec::new()),
            fail_next_response: Mutex::new(None),
            delay_next_response: Mutex::new(None),
            suppress_resolution: Mutex::new(false),
        }
    }

    async fn suppress_resolution(&self) {
        *self.suppress_resolution.lock().await = true;
    }

    async fn fail_next_response(&self, error: HarnessError) {
        *self.fail_next_response.lock().await = Some(error);
    }

    async fn delay_next_response(&self, delay: Duration) {
        *self.delay_next_response.lock().await = Some(delay);
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
        let thread = opts.thread.unwrap_or_default();
        Ok(ThreadHandle {
            resumed_model: opts
                .initial_model
                .clone()
                .or_else(|| Some(fake_native_model())),
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
        let _ = self.tx.send(AgentEvent::TurnStarted {
            thread: thread.thread,
            turn,
        });
        let _ = self.tx.send(AgentEvent::ServerRequestReceived {
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
        AgentEventStream::new(self.tx.subscribe())
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
        if let Some(delay) = self.delay_next_response.lock().await.take() {
            tokio::time::sleep(delay).await;
        }
        if let Some(error) = self.fail_next_response.lock().await.take() {
            return Err(error);
        }
        self.responses
            .lock()
            .await
            .push((req.clone(), response.clone()));
        if *self.suppress_resolution.lock().await {
            return Ok(());
        }
        let (thread, turn) = self.active.lock().await.take().unwrap_or_default();
        let _ = self.tx.send(AgentEvent::ServerRequestResolved {
            thread,
            turn: Some(turn),
            request_id: req,
        });
        let _ = self.tx.send(AgentEvent::TurnCompleted {
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

struct ServerRequestFactory {
    harness: Arc<ServerRequestHarness>,
}

#[async_trait]
impl HarnessFactory for ServerRequestFactory {
    async fn create(&self, _config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(self.harness.clone())
    }
}

fn generate_password_hash(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand::rngs::OsRng;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn ws_text(msg: &ClientMessage) -> tokio_tungstenite::tungstenite::Message {
    tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(msg).unwrap().into())
}

async fn spawn_test_app() -> (
    tempfile::TempDir,
    Arc<ServerRequestHarness>,
    SocketAddr,
    String,
    ThreadId,
) {
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = generate_password_hash("testpass");
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            "[server]\nbind = \"127.0.0.1:0\"\nsecure_cookies = false\n\n[auth]\npassword_hash = \"{hash}\"\nsession_days = 30\n"
        ),
    )
    .await
    .unwrap();

    let harness = Arc::new(ServerRequestHarness::new());
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(ServerRequestFactory {
            harness: harness.clone(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let proj_dir = tempfile::TempDir::new().unwrap();
    let pid = ProjectId::new();
    state
        .store
        .create_project(pid, "proj", &proj_dir.path().to_string_lossy())
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let cookie = {
        let resp = client
            .post(format!("http://{addr}/api/login"))
            .json(&serde_json::json!({"password": "testpass"}))
            .send()
            .await
            .unwrap();
        resp.headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    };

    let thread_id = {
        let resp: serde_json::Value = client
            .post(format!("http://{addr}/api/projects/{pid}/threads"))
            .header("cookie", &cookie)
            .json(&serde_json::json!({ "resume": "server_request_thread" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        serde_json::from_value(resp["thread_id"].clone()).unwrap()
    };

    (tmp, harness, addr, cookie, thread_id)
}

async fn connect_ws(addr: SocketAddr, cookie: &str) -> TestWs {
    let ws_request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://{addr}/api/ws"))
        .header("host", addr.to_string())
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");
    ws
}

#[tokio::test]
async fn websocket_server_request_response_routes_to_harness() {
    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws, thread_id).await;
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({
            "answers": { "confirm": { "answers": ["Yes"] } }
        })),
    }))
    .await
    .unwrap();

    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(status, RequestStatus::Responding)
    })
    .await;
    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(
            status,
            RequestStatus::Resolved {
                resolution: RequestResolution::Server
            }
        )
    })
    .await;
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
    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws, thread_id).await;
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::error(-32000, "cancelled"),
    }))
    .await
    .unwrap();

    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(status, RequestStatus::Responding)
    })
    .await;
    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(
            status,
            RequestStatus::Resolved {
                resolution: RequestResolution::Server
            }
        )
    })
    .await;
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
    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws, thread_id).await;
    harness
        .fail_next_response(HarnessError::Protocol("temporary failure".into()))
        .await;

    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({
            "answers": { "confirm": { "answers": ["Yes"] } }
        })),
    }))
    .await
    .unwrap();

    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(status, RequestStatus::Responding)
    })
    .await;
    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(status, RequestStatus::Pending)
    })
    .await;
    let error = wait_for_ws_error(&mut ws).await;
    assert_eq!(error.code, "harness_protocol_error");
    assert_eq!(error.action.as_deref(), Some("server_request_response"));
    assert!(
        error
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("temporary failure")
    );

    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({
            "answers": { "confirm": { "answers": ["Yes"] } }
        })),
    }))
    .await
    .unwrap();

    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(status, RequestStatus::Responding)
    })
    .await;
    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(
            status,
            RequestStatus::Resolved {
                resolution: RequestResolution::Server
            }
        )
    })
    .await;
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
async fn timed_out_server_request_response_publishes_rollback_to_every_tab() {
    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut first = connect_ws(addr, &cookie).await;
    let mut second = connect_ws(addr, &cookie).await;
    for ws in [&mut first, &mut second] {
        ws.send(ws_text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    }
    first
        .send(ws_text(&ClientMessage::SendInput {
            thread_id,
            text: "ask me".into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();
    wait_for_server_request(&mut first, thread_id).await;
    wait_for_server_request(&mut second, thread_id).await;
    harness.delay_next_response(Duration::from_secs(3)).await;

    first
        .send(ws_text(&ClientMessage::ServerRequestResponse {
            thread_id,
            request_id: "srv_1".into(),
            response: ServerRequestResponse::result(serde_json::json!({
                "answers": { "confirm": { "answers": ["Yes"] } }
            })),
        }))
        .await
        .unwrap();

    for ws in [&mut first, &mut second] {
        wait_for_server_request_status(ws, thread_id, |status| {
            matches!(status, RequestStatus::Responding)
        })
        .await;
        wait_for_server_request_status(ws, thread_id, |status| {
            matches!(status, RequestStatus::Pending)
        })
        .await;
    }
    let error = wait_for_ws_error(&mut first).await;
    assert_eq!(error.code, "harness_timeout");
    assert_eq!(error.action.as_deref(), Some("server_request_response"));
}

#[tokio::test]
async fn websocket_subscribe_replays_pending_server_request_snapshot() {
    let (_tmp, _harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws, thread_id).await;

    let mut reconnect = connect_ws(addr, &cookie).await;
    let overview = wait_for_runtime_overview(&mut reconnect).await;
    let runtime = runtime_for_thread(&overview, thread_id);
    assert!(matches!(
        runtime.turn_state,
        RuntimeTurnState::Active { .. }
    ));
    assert!(runtime.outstanding_requests.iter().any(|request| {
        request.request_id == "srv_1" && request.kind == RequestKind::Server && !request.responding
    }));
    reconnect
        .send(ws_text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();

    let bootstrap = wait_for_bootstrap(&mut reconnect, thread_id).await;
    let request = request_from_final_runtime(&bootstrap, "srv_1");
    assert_server_request(request, thread_id);
    assert!(matches!(request.status, RequestStatus::Pending));
}

/// A server request the user answered must not come back actionable on reconnect, even when the
/// harness has not (or will never) emit its resolved event. Nothing recorded the answer server-side
/// before this, so the replayed `ServerRequestReceived` re-prompted and answering again routed a
/// stale id to the harness, which errors — the same defect already fixed for approvals.
#[tokio::test]
async fn answered_server_request_is_not_pending_after_reconnect() {
    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    harness.suppress_resolution().await;

    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();
    wait_for_server_request(&mut ws, thread_id).await;

    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({ "answers": ["main"] })),
    }))
    .await
    .unwrap();
    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(status, RequestStatus::Responding)
    })
    .await;
    wait_for_server_request_status(&mut ws, thread_id, |status| {
        matches!(
            status,
            RequestStatus::Resolved {
                resolution: RequestResolution::Server
            }
        )
    })
    .await;
    // The harness confirms it received the answer; it deliberately never emits its own resolution.
    let (answered_id, _) = harness.wait_for_response().await;
    assert_eq!(answered_id, ServerRequestId("srv_1".into()));

    let mut reconnect = connect_ws(addr, &cookie).await;
    let overview = wait_for_runtime_overview(&mut reconnect).await;
    let runtime = runtime_for_thread(&overview, thread_id);
    assert!(
        runtime.outstanding_requests.is_empty(),
        "the committed response must no longer be outstanding"
    );
    reconnect
        .send(ws_text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();

    let bootstrap = wait_for_bootstrap(&mut reconnect, thread_id).await;
    let request = request_from_final_runtime(&bootstrap, "srv_1");
    assert_server_request(request, thread_id);
    assert!(
        matches!(
            request.status,
            RequestStatus::Resolved {
                resolution: RequestResolution::Server
            }
        ),
        "an answered request must reconnect as resolved rather than actionable"
    );
}

#[tokio::test]
async fn websocket_unknown_server_request_response_surfaces_error() {
    let (_tmp, _harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: "missing".into(),
        response: ServerRequestResponse::error(-32000, "missing"),
    }))
    .await
    .unwrap();

    let error = wait_for_ws_error(&mut ws).await;
    assert_eq!(error.code, "harness_protocol_error");
    assert_eq!(error.action.as_deref(), Some("server_request_response"));
    assert!(error.message.contains("protocol error"));
}

async fn wait_for_server_request(ws: &mut TestWs, thread_id: ThreadId) -> RequestState {
    wait_for_server_request_status(ws, thread_id, |status| {
        matches!(status, RequestStatus::Pending)
    })
    .await
}

async fn wait_for_server_request_status<F>(
    ws: &mut TestWs,
    thread_id: ThreadId,
    status_matches: F,
) -> RequestState
where
    F: Fn(&RequestStatus) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::ThreadEvent {
                    thread_id: message_thread_id,
                    event,
                    ..
                }) = serde_json::from_str(&text)
                    && message_thread_id == thread_id
                    && let ThreadEventPayload::Request { request } = event.event
                    && request.request_id == "srv_1"
                    && status_matches(&request.status)
                {
                    assert_server_request(&request, thread_id);
                    return *request;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("matching server request transition not observed");
}

async fn wait_for_ws_error(ws: &mut TestWs) -> giskard_proto::ErrorInfo {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Error { error }) = serde_json::from_str(&text) {
                    return error;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("error message not observed");
}

fn assert_server_request(request: &RequestState, thread_id: ThreadId) {
    assert_eq!(request.thread_id, thread_id);
    let RequestPayload::Server { request } = &request.payload else {
        panic!("expected server request payload");
    };
    assert_eq!(request.id, ServerRequestId("srv_1".into()));
    assert_eq!(request.method, "item/tool/requestUserInput");
}

async fn wait_for_runtime_overview(ws: &mut TestWs) -> ThreadRuntimeOverview {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::ThreadRuntimeOverview(overview)) =
                    serde_json::from_str(&text)
                {
                    return overview;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("runtime overview not observed");
}

fn runtime_for_thread(
    overview: &ThreadRuntimeOverview,
    thread_id: ThreadId,
) -> &giskard_proto::ThreadRuntimeSummary {
    overview
        .threads
        .iter()
        .find(|runtime| runtime.thread_id == thread_id)
        .expect("active thread should be present in runtime overview")
}

async fn wait_for_bootstrap(ws: &mut TestWs, thread_id: ThreadId) -> ThreadBootstrapPayload {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut generation = None;
    let mut expected = HashMap::new();
    let mut chunks: HashMap<BootstrapSection, BTreeMap<u32, Vec<u8>>> = HashMap::new();
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let Ok(ServerMessage::ThreadBootstrap {
                    thread_id: message_thread_id,
                    subscription_generation,
                    frame,
                }) = serde_json::from_str(&text)
                else {
                    continue;
                };
                if message_thread_id != thread_id {
                    continue;
                }
                match frame {
                    ThreadBootstrapFrame::Start { sections } => {
                        generation = Some(subscription_generation);
                        expected = sections
                            .into_iter()
                            .map(|section| (section.section, section.chunk_count))
                            .collect();
                        chunks.clear();
                    }
                    ThreadBootstrapFrame::Chunk {
                        section,
                        index,
                        payload_base64,
                    } if generation == Some(subscription_generation) => {
                        let payload = BASE64
                            .decode(payload_base64)
                            .expect("bootstrap chunks should contain valid base64");
                        chunks.entry(section).or_default().insert(index, payload);
                    }
                    ThreadBootstrapFrame::Commit if generation == Some(subscription_generation) => {
                        for (section, chunk_count) in &expected {
                            assert_eq!(
                                chunks.get(section).map(BTreeMap::len),
                                Some(*chunk_count as usize),
                                "bootstrap section {section:?} was incomplete at commit"
                            );
                        }
                        return decode_bootstrap_sections(&mut chunks);
                    }
                    ThreadBootstrapFrame::Chunk { .. } | ThreadBootstrapFrame::Commit => {}
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("committed thread bootstrap not observed");
}

fn decode_bootstrap_sections(
    chunks: &mut HashMap<BootstrapSection, BTreeMap<u32, Vec<u8>>>,
) -> ThreadBootstrapPayload {
    ThreadBootstrapPayload {
        metadata: take_bootstrap_section(chunks, BootstrapSection::Metadata),
        history: take_bootstrap_section(chunks, BootstrapSection::History),
        live_turn: take_bootstrap_section(chunks, BootstrapSection::LiveTurn),
        ordered_suffix: take_bootstrap_section(chunks, BootstrapSection::OrderedSuffix),
        final_runtime: take_bootstrap_section(chunks, BootstrapSection::FinalRuntime),
        notices: take_bootstrap_section(chunks, BootstrapSection::Notices),
    }
}

fn take_bootstrap_section<T>(
    chunks: &mut HashMap<BootstrapSection, BTreeMap<u32, Vec<u8>>>,
    section: BootstrapSection,
) -> T
where
    T: serde::de::DeserializeOwned,
{
    let section_chunks = chunks
        .remove(&section)
        .unwrap_or_else(|| panic!("bootstrap section {section:?} was absent"));
    let mut encoded = Vec::new();
    for (expected_index, (index, chunk)) in section_chunks.into_iter().enumerate() {
        assert_eq!(index as usize, expected_index, "bootstrap chunk gap");
        encoded.extend(chunk);
    }
    serde_json::from_slice(&encoded)
        .unwrap_or_else(|error| panic!("bootstrap section {section:?} was invalid: {error}"))
}

fn request_from_final_runtime<'a>(
    bootstrap: &'a ThreadBootstrapPayload,
    request_id: &str,
) -> &'a RequestState {
    bootstrap
        .final_runtime
        .requests
        .iter()
        .find(|request| request.request_id == request_id)
        .expect("server request should be present in final runtime")
}
