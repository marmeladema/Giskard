//! Regression coverage for request state surviving a browser reconnect (spec §9, §13.6).
//!
//! This test drives the real WebSocket API: raise an approval, answer it, reconnect, and assert
//! that both runtime projections report the request as resolved rather than actionable.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ProjectId, ThreadId, TurnId};
use giskard_core::model::ModelDescriptor;
use giskard_core::turn::TurnOverrides;
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

const APPROVAL_ID: &str = "ap_reconnect_1";
const SERVER_REQUEST_ID: &str = "req_reconnect_1";

/// A harness that raises a single approval and keeps the turn in-flight forever (never sends
/// `TurnCompleted`), so the live buffer is still present when the reconnect snapshot is taken.
struct ApprovalHarness {
    tx: broadcast::Sender<AgentEvent>,
    active: Mutex<Option<(ThreadId, TurnId)>>,
    answered: Mutex<Vec<(ApprovalId, ApprovalDecision)>>,
}

impl ApprovalHarness {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            tx,
            active: Mutex::new(None),
            answered: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl AgentHarness for ApprovalHarness {
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
                opts.resume.unwrap_or_else(|| "approval_harness".into()),
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
        let _ = self.tx.send(AgentEvent::ApprovalRequested {
            thread: thread.thread,
            turn,
            request: ApprovalRequest {
                id: ApprovalId(APPROVAL_ID.into()),
                kind: ApprovalKind::CommandExecution {
                    command: "rm -rf ./build".into(),
                    cwd: "/tmp/project".into(),
                },
                reason: Some("Remove the build directory?".into()),
                metadata: vec![],
                available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
            },
        });
        // A non-approval server request blocks the turn the same way an approval does, and is just
        // as invisible to a browser that was not connected, so the connect replay carries both.
        let _ = self.tx.send(AgentEvent::ServerRequestReceived {
            thread: thread.thread,
            turn: Some(turn),
            request: giskard_core::server_request::ServerRequest {
                id: giskard_core::ids::ServerRequestId(SERVER_REQUEST_ID.into()),
                method: "requestUserInput".into(),
                params: serde_json::json!({ "question": "Which branch?" }),
                received_at: chrono::Utc::now(),
            },
        });
        Ok(turn)
    }

    fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
        AgentEventStream::new(self.tx.subscribe())
    }

    async fn respond_approval(
        &self,
        req: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        // Record the routed decision but leave the turn in-flight so the reconnect still has a live
        // buffer to snapshot.
        self.answered.lock().await.push((req, decision));
        Ok(())
    }

    async fn respond_server_request(
        &self,
        req: giskard_core::ids::ServerRequestId,
        _response: giskard_core::server_request::ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        // Mirror a real harness: answering the request resolves it, which is what clears it from the
        // live buffer. Without this it would stay outstanding and keep being replayed.
        if let Some((thread, turn)) = *self.active.lock().await {
            let _ = self.tx.send(AgentEvent::ServerRequestResolved {
                thread,
                turn: Some(turn),
                request_id: req,
            });
        }
        Ok(())
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct ApprovalFactory {
    harness: Arc<ApprovalHarness>,
}

#[async_trait]
impl HarnessFactory for ApprovalFactory {
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

async fn spawn_test_app() -> (tempfile::TempDir, SocketAddr, String, ThreadId) {
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

    let harness = Arc::new(ApprovalHarness::new());
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(ApprovalFactory {
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
            .json(&serde_json::json!({ "resume": "approval_thread" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        serde_json::from_value(resp["thread_id"].clone()).unwrap()
    };

    (tmp, addr, cookie, thread_id)
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
async fn answered_approval_is_not_pending_after_reconnect() {
    use futures_util::SinkExt;

    let (_tmp, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "please".into(),
        attachments: vec![],
    }))
    .await
    .unwrap();

    // Wait for the approval to be surfaced.
    wait_for_approval(&mut ws).await;

    // Answer it.
    ws.send(ws_text(&ClientMessage::ApprovalDecision {
        thread_id,
        request_id: APPROVAL_ID.into(),
        decision: ApprovalDecision::Accept,
    }))
    .await
    .unwrap();

    // The resolved request transition is published only after the runtime authority records it, so
    // observing it guarantees the reconnect below sees the answered state.
    wait_for_approval_resolved(&mut ws).await;

    // Reconnect with a fresh socket, as a browser reload would.
    let mut reconnect = connect_ws(addr, &cookie).await;
    reconnect
        .send(ws_text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();

    let bootstrap = wait_for_bootstrap(&mut reconnect, thread_id).await;
    let live_turn = bootstrap
        .live_turn
        .expect("the in-flight turn should have a live projection");
    let live_request = live_turn
        .events
        .iter()
        .find_map(|event| request_with_id(event, APPROVAL_ID))
        .expect("the approval card should be reconstructed from the live projection");
    assert_resolved_approval(live_request);

    let final_request = bootstrap
        .final_runtime
        .requests
        .iter()
        .find(|request| request.request_id == APPROVAL_ID)
        .expect("the final runtime authority should contain the approval");
    assert_resolved_approval(final_request);
}

async fn wait_for_approval(ws: &mut TestWs) {
    use futures_util::StreamExt;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::ThreadEvent { event, .. }) = serde_json::from_str(&text)
                    && matches!(
                        event.event,
                        ThreadEventPayload::Request { request }
                            if request.request_id == APPROVAL_ID
                                && matches!(request.payload, RequestPayload::Approval { .. })
                                && matches!(request.status, RequestStatus::Pending)
                    )
                {
                    return;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("approval request event not observed");
}

async fn wait_for_approval_resolved(ws: &mut TestWs) {
    use futures_util::StreamExt;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::ThreadEvent { event, .. }) = serde_json::from_str(&text)
                    && matches!(
                        event.event,
                        ThreadEventPayload::Request { request }
                            if request.request_id == APPROVAL_ID
                                && matches!(
                                    request.status,
                                    RequestStatus::Resolved {
                                        resolution: RequestResolution::Approval {
                                            decision: ApprovalDecision::Accept,
                                        },
                                    }
                                )
                    )
                {
                    return;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("approval resolved message not observed");
}

/// A browser that was not connected when an approval was raised must still learn the thread is
/// blocked. The complete runtime overview is sent on every connection, without requiring a thread
/// subscription, so this also covers managed sub-agents that have no sidebar row of their own.
#[tokio::test]
async fn pending_approval_is_replayed_to_a_client_that_connects_later() {
    use futures_util::SinkExt;

    let (_tmp, addr, cookie, thread_id) = spawn_test_app().await;
    let mut ws = connect_ws(addr, &cookie).await;
    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "please".into(),
        attachments: vec![],
    }))
    .await
    .unwrap();
    wait_for_approval(&mut ws).await;

    // A second browser connects while the approval is still outstanding, and never subscribes.
    let mut latecomer = connect_ws(addr, &cookie).await;
    let overview = wait_for_runtime_overview(&mut latecomer).await;
    let runtime = overview
        .threads
        .iter()
        .find(|runtime| runtime.thread_id == thread_id)
        .expect("the active thread should appear in the runtime overview");
    assert!(matches!(
        runtime.turn_state,
        RuntimeTurnState::Active { .. }
    ));
    assert!(runtime.outstanding_requests.iter().any(|request| {
        request.request_id == APPROVAL_ID && request.kind == RequestKind::Approval
    }));
    // A non-approval server request blocks the turn just as invisibly, so it is present too.
    assert!(runtime.outstanding_requests.iter().any(|request| {
        request.request_id == SERVER_REQUEST_ID && request.kind == RequestKind::Server
    }));

    // Answer both, then connect again: neither is outstanding any more, so neither may be replayed.
    // Re-alerting for something the user already dealt with is worse than silence.
    ws.send(ws_text(&ClientMessage::ApprovalDecision {
        thread_id,
        request_id: APPROVAL_ID.into(),
        decision: ApprovalDecision::Accept,
    }))
    .await
    .unwrap();
    wait_for_approval_resolved(&mut ws).await;
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: SERVER_REQUEST_ID.into(),
        response: giskard_proto::ServerRequestResponse::result(serde_json::json!("main")),
    }))
    .await
    .unwrap();
    wait_for_server_request_resolved(&mut ws).await;

    let mut after_answer = connect_ws(addr, &cookie).await;
    let after_answer = wait_for_runtime_overview(&mut after_answer).await;
    let outstanding = after_answer
        .threads
        .iter()
        .find(|runtime| runtime.thread_id == thread_id)
        .map(|runtime| runtime.outstanding_requests.as_slice())
        .unwrap_or_default();
    assert!(
        outstanding.is_empty(),
        "answered requests must not be outstanding for a later client, got {outstanding:?}"
    );
}

async fn wait_for_server_request_resolved(ws: &mut TestWs) {
    use futures_util::StreamExt;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::ThreadEvent { event, .. }) = serde_json::from_str(&text)
                    && matches!(
                        event.event,
                        ThreadEventPayload::Request { request }
                            if request.request_id == SERVER_REQUEST_ID
                                && matches!(
                                    request.status,
                                    RequestStatus::Resolved {
                                        resolution: RequestResolution::Server,
                                    }
                                )
                    )
                {
                    return;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("server request resolved event not observed");
}

async fn wait_for_runtime_overview(ws: &mut TestWs) -> ThreadRuntimeOverview {
    use futures_util::StreamExt;
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

async fn wait_for_bootstrap(ws: &mut TestWs, thread_id: ThreadId) -> ThreadBootstrapPayload {
    use futures_util::StreamExt;

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

fn request_with_id<'a>(
    event: &'a ThreadEventPayload,
    request_id: &str,
) -> Option<&'a RequestState> {
    match event {
        ThreadEventPayload::Request { request } if request.request_id == request_id => {
            Some(request)
        }
        ThreadEventPayload::Agent { .. } | ThreadEventPayload::Request { .. } => None,
    }
}

fn assert_resolved_approval(request: &RequestState) {
    assert!(matches!(
        &request.payload,
        RequestPayload::Approval { request: approval }
            if approval.id == ApprovalId(APPROVAL_ID.into())
    ));
    assert!(matches!(
        &request.status,
        RequestStatus::Resolved {
            resolution: RequestResolution::Approval { decision },
        } if *decision == ApprovalDecision::Accept
    ));
}
