//! Regression coverage for Codex-style server-initiated browser requests.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_proto::{ClientMessage, ServerMessage, WireAgentEvent};
use giskard_server::{AppState, HarnessFactory, build_app};
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, Instant};

type TestWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct ServerRequestHarness {
    tx: broadcast::Sender<AgentEvent>,
    active: Mutex<Option<(ThreadId, TurnId)>>,
    responses: Mutex<Vec<(ServerRequestId, ServerRequestResponse)>>,
    fail_next_response: Mutex<Option<HarnessError>>,
}

impl ServerRequestHarness {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            tx,
            active: Mutex::new(None),
            responses: Mutex::new(Vec::new()),
            fail_next_response: Mutex::new(None),
        }
    }

    async fn fail_next_response(&self, error: HarnessError) {
        *self.fail_next_response.lock().await = Some(error);
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
            thread,
            harness_thread_id: opts
                .resume
                .unwrap_or_else(|| "server_request_harness".into()),
            warning: None,
            resumed_model: Some(opts.initial_model.clone()),
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
        if let Some(error) = self.fail_next_response.lock().await.take() {
            return Err(error);
        }
        self.responses
            .lock()
            .await
            .push((req.clone(), response.clone()));
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
        .create_project(
            pid,
            "proj",
            &proj_dir.path().to_string_lossy(),
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5".into(),
                reasoning_effort: None,
            },
        )
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
    ws.send(ws_text(&ClientMessage::Subscribe { thread_id }))
        .await
        .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
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
    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe { thread_id }))
        .await
        .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
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
    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe { thread_id }))
        .await
        .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;
    harness
        .fail_next_response(HarnessError::Protocol("temporary failure".into()))
        .await;

    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        request_id: "srv_1".into(),
        response: ServerRequestResponse::result(serde_json::json!({
            "answers": { "confirm": { "answers": ["Yes"] } }
        })),
    }))
    .await
    .unwrap();

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
async fn websocket_subscribe_replays_pending_server_request_snapshot() {
    let (_tmp, _harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe { thread_id }))
        .await
        .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "ask me".into(),
    }))
    .await
    .unwrap();

    wait_for_server_request(&mut ws).await;

    let mut reconnect = connect_ws(addr, &cookie).await;
    reconnect
        .send(ws_text(&ClientMessage::Subscribe { thread_id }))
        .await
        .unwrap();

    let snapshot = wait_for_live_snapshot(&mut reconnect).await;
    assert_eq!(snapshot.thread_id, thread_id);
    assert_eq!(snapshot.pending_server_requests.len(), 1);
    assert_eq!(
        snapshot.pending_server_requests[0].id,
        ServerRequestId("srv_1".into())
    );
    assert_eq!(
        snapshot.pending_server_requests[0].method,
        "item/tool/requestUserInput"
    );
}

#[tokio::test]
async fn websocket_unknown_server_request_response_surfaces_error() {
    let (_tmp, _harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe { thread_id }))
        .await
        .unwrap();
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
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

async fn wait_for_server_request(ws: &mut TestWs) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&text) {
                    if matches!(agent_event, WireAgentEvent::ServerRequestReceived { .. }) {
                        return;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("server request event not observed");
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

async fn wait_for_live_snapshot(ws: &mut TestWs) -> giskard_proto::LiveTurnSnapshot {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::LiveTurnSnapshot(snapshot)) = serde_json::from_str(&text) {
                    return snapshot;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("live turn snapshot not observed");
}
