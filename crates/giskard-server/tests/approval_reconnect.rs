//! Regression coverage for approval state surviving a browser reconnect (spec §9, §13.6).
//!
//! Approval resolution lives only in browser memory. Before this was fixed, a reconnect's live-turn
//! snapshot re-surfaced an already-answered approval as `pending_approval`, so the reloaded UI
//! showed it as actionable again — and answering it a second time routed a stale id to the harness,
//! which errored. This test drives the real WebSocket API: raise an approval, answer it, reconnect,
//! and assert the snapshot reports it as answered (not pending).

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
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
use giskard_proto::{ClientMessage, ServerMessage, WireAgentEvent};
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
    hang_next_approval: Mutex<bool>,
}

impl ApprovalHarness {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            tx,
            active: Mutex::new(None),
            answered: Mutex::new(Vec::new()),
            hang_next_approval: Mutex::new(false),
        }
    }

    async fn hang_next_approval(&self) {
        *self.hang_next_approval.lock().await = true;
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
        if std::mem::take(&mut *self.hang_next_approval.lock().await) {
            std::future::pending::<()>().await;
        }
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

async fn spawn_test_app() -> (
    tempfile::TempDir,
    Arc<ApprovalHarness>,
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
async fn answered_approval_is_not_pending_after_reconnect() {
    use futures_util::SinkExt;

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

    // The server publishes the resolved `RequestState` only after it has recorded the resolution
    // against the live buffer, so waiting for it guarantees the reconnect below sees the answered
    // state.
    wait_for_approval_request_state(&mut ws, "resolved").await;

    // Reconnect with a fresh socket, as a browser reload would.
    let mut reconnect = connect_ws(addr, &cookie).await;
    reconnect
        .send(ws_text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();

    let snapshot = wait_for_live_snapshot(&mut reconnect).await;
    assert_eq!(snapshot.thread_id, thread_id);
    // The answered approval must NOT be re-surfaced as actionable. The client derives the
    // outstanding set from `accumulated` plus `answered_approvals`, so the answered approval is
    // present in `accumulated` (so its resolved card can be drawn) but not in the outstanding set.
    let outstanding: Vec<ApprovalId> = snapshot
        .accumulated
        .iter()
        .filter_map(|e| match e {
            WireAgentEvent::ApprovalRequested { request, .. } => Some(request.id.clone()),
            _ => None,
        })
        .filter(|id| {
            !snapshot
                .answered_approvals
                .iter()
                .any(|a| a.request_id == *id)
        })
        .collect();
    assert!(
        outstanding.is_empty(),
        "answered approval should not be outstanding after reconnect, got {outstanding:?}",
    );
    // It is reported as answered so the reconnecting client renders it in its resolved state.
    assert_eq!(snapshot.answered_approvals.len(), 1);
    assert_eq!(
        snapshot.answered_approvals[0].request_id,
        ApprovalId(APPROVAL_ID.into())
    );
    assert_eq!(
        snapshot.answered_approvals[0].decision,
        ApprovalDecision::Accept
    );
    // The original request is still replayed in the accumulated stream (so the card can be drawn).
    assert!(
        snapshot.accumulated.iter().any(|e| matches!(
            e,
            WireAgentEvent::ApprovalRequested { request, .. } if request.id == ApprovalId(APPROVAL_ID.into())
        )),
        "the approval request should still be present in the accumulated events"
    );
}

async fn wait_for_approval(ws: &mut TestWs) {
    use futures_util::StreamExt;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&text)
                    && matches!(*agent_event, WireAgentEvent::ApprovalRequested { .. })
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

/// A browser that was not connected when an approval was raised must still learn the thread is
/// blocked. The replacement runtime overview must include it without requiring the browser to open
/// or subscribe to that thread first, which matters for managed sub-agents with no sidebar row.
#[tokio::test]
async fn pending_approval_is_replayed_to_a_client_that_connects_later() {
    use futures_util::SinkExt;

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
        text: "please".into(),
        attachments: vec![],
    }))
    .await
    .unwrap();
    wait_for_approval(&mut ws).await;

    // A second browser connects while the approval is still outstanding, and never subscribes.
    let mut latecomer = connect_ws(addr, &cookie).await;
    let overview = wait_for_runtime_overview(&mut latecomer).await;
    let mine = overview
        .threads
        .iter()
        .find(|summary| summary.thread_id == thread_id)
        .expect("the active thread should appear in the runtime overview");
    assert!(
        mine.outstanding_requests
            .iter()
            .any(|request| request.request_id == APPROVAL_ID)
    );
    // A non-approval server request blocks the turn just as invisibly, so it is replayed too.
    assert!(
        mine.outstanding_requests
            .iter()
            .any(|request| request.request_id == SERVER_REQUEST_ID)
    );

    // Answer both, then connect again: neither is outstanding any more, so neither may be replayed.
    // Re-alerting for something the user already dealt with is worse than silence.
    ws.send(ws_text(&ClientMessage::ApprovalDecision {
        thread_id,
        request_id: APPROVAL_ID.into(),
        decision: ApprovalDecision::Accept,
    }))
    .await
    .unwrap();
    wait_for_approval_request_state(&mut ws, "resolved").await;
    ws.send(ws_text(&ClientMessage::ServerRequestResponse {
        thread_id,
        request_id: SERVER_REQUEST_ID.into(),
        response: giskard_proto::ServerRequestResponse::result(serde_json::json!("main")),
    }))
    .await
    .unwrap();
    wait_for_server_request_resolved(&mut ws).await;

    let mut after_answer = connect_ws(addr, &cookie).await;
    let replayed = wait_for_runtime_overview(&mut after_answer).await;
    assert!(
        !replayed
            .threads
            .iter()
            .any(|summary| summary.thread_id == thread_id
                && summary
                    .outstanding_requests
                    .iter()
                    .any(|request| request.request_id == APPROVAL_ID)),
        "an answered approval must not be replayed to a later client, got {replayed:?}"
    );
    assert!(
        !replayed
            .threads
            .iter()
            .any(|summary| summary.thread_id == thread_id
                && summary
                    .outstanding_requests
                    .iter()
                    .any(|request| request.request_id == SERVER_REQUEST_ID)),
        "an answered server request must not be replayed to a later client, got {replayed:?}"
    );
}

#[tokio::test]
async fn timed_out_approval_republishes_pending_to_peer_tabs() {
    use futures_util::SinkExt;

    let (_tmp, harness, addr, cookie, thread_id) = spawn_test_app().await;
    let mut claimant = connect_ws(addr, &cookie).await;
    let mut peer = connect_ws(addr, &cookie).await;
    for ws in [&mut claimant, &mut peer] {
        ws.send(ws_text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    }
    claimant
        .send(ws_text(&ClientMessage::SendInput {
            thread_id,
            text: "please".into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();
    wait_for_approval(&mut claimant).await;
    let pending = wait_for_approval_request_state(&mut peer, "pending").await;
    assert_eq!(pending.revision, 1);

    harness.hang_next_approval().await;
    claimant
        .send(ws_text(&ClientMessage::ApprovalDecision {
            thread_id,
            request_id: APPROVAL_ID.into(),
            decision: ApprovalDecision::Accept,
        }))
        .await
        .unwrap();

    let responding = wait_for_approval_request_state(&mut peer, "responding").await;
    assert_eq!(responding.revision, 2);
    let error = wait_for_ws_error(&mut claimant).await;
    assert_eq!(error.code, "harness_timeout");
    let rolled_back = wait_for_approval_request_state(&mut peer, "pending").await;
    assert_eq!(rolled_back.revision, 3);
}

async fn wait_for_approval_request_state(
    ws: &mut TestWs,
    expected: &str,
) -> giskard_proto::RequestState {
    use futures_util::StreamExt;

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(Duration::from_secs(1), ws.next()).await
            && let Ok(ServerMessage::RequestState(state)) = serde_json::from_str(&text)
        {
            let status = match state.status {
                giskard_proto::RequestStatus::Pending => "pending",
                giskard_proto::RequestStatus::Responding => "responding",
                giskard_proto::RequestStatus::Resolved { .. } => "resolved",
            };
            if state.request_id == APPROVAL_ID && status == expected {
                return state;
            }
        }
    }
    panic!("approval request state {expected} not observed");
}

async fn wait_for_ws_error(ws: &mut TestWs) -> giskard_proto::ErrorInfo {
    use futures_util::StreamExt;

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(Duration::from_secs(1), ws.next()).await
            && let Ok(ServerMessage::Error { error }) = serde_json::from_str(&text)
        {
            return error;
        }
    }
    panic!("websocket error not observed");
}

async fn wait_for_server_request_resolved(ws: &mut TestWs) {
    use futures_util::StreamExt;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&text)
                    && matches!(*agent_event, WireAgentEvent::ServerRequestResolved { .. })
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

async fn wait_for_runtime_overview(ws: &mut TestWs) -> giskard_proto::ThreadRuntimeOverview {
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

async fn wait_for_live_snapshot(ws: &mut TestWs) -> giskard_proto::LiveTurnSnapshot {
    use futures_util::StreamExt;
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
