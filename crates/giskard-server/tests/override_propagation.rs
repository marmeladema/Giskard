//! Regression test for the turn-override snapshot the server hands the harness.
//!
//! Guards two fixes: (1) the thread's current model + reasoning effort must reach `start_turn`
//! so mid-thread model/effort changes take effect (§8.4/§8.5); (2) the thread's approval policy
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
use giskard_core::turn::{ApprovalPolicy, Mode, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_proto::ClientMessage;
use giskard_server::{AppState, HarnessFactory, build_app};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::broadcast;

/// Harness that records the overrides passed to `start_turn` and emits a trivial completed turn.
struct CapturingHarness {
    captured: Arc<TokioMutex<Vec<TurnOverrides>>>,
    tx: broadcast::Sender<AgentEvent>,
    thread_id: StdMutex<Option<ThreadId>>,
}

impl CapturingHarness {
    fn new(captured: Arc<TokioMutex<Vec<TurnOverrides>>>) -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            captured,
            tx,
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
        let tid = opts.thread.unwrap_or_default();
        *self.thread_id.lock().unwrap() = Some(tid);
        Ok(ThreadHandle {
            thread: tid,
            harness_thread_id: opts.resume.unwrap_or_else(|| "cap".into()),
            warning: None,
            resumed_model: Some(opts.initial_model.clone()),
            agent_name: None,
            parent_harness_thread_id: None,
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
        let _ = self.tx.send(AgentEvent::TurnStarted { thread: tid, turn });
        let _ = self.tx.send(AgentEvent::TurnCompleted {
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
        AgentEventStream::new(self.tx.subscribe())
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

struct CapFactory {
    captured: Arc<TokioMutex<Vec<TurnOverrides>>>,
}

#[async_trait::async_trait]
impl HarnessFactory for CapFactory {
    async fn create(&self, _config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(Arc::new(CapturingHarness::new(self.captured.clone())))
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

#[tokio::test]
async fn send_input_snapshot_carries_model_effort_and_thread_policy() {
    let port = 19100;
    let captured = Arc::new(TokioMutex::new(Vec::<TurnOverrides>::new()));

    // Server with a capturing harness.
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = generate_password_hash("testpass");
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            r#"
[server]
bind = "127.0.0.1:{port}"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

[[providers]]
id = "openai"
name = "OpenAI"
wire_api = "responses"
  [[providers.models]]
  id = "gpt-5.5"
  context_window = 258400
  supports_reasoning_effort = true
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(CapFactory {
            captured: captured.clone(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let proj_dir = tempfile::TempDir::new().unwrap();
    let pid = giskard_core::ids::ProjectId::new();
    state
        .store
        .create_project(
            pid,
            "proj",
            &proj_dir.path().to_string_lossy(),
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let cookie = {
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/login"))
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

    let thread_id: ThreadId = {
        let resp: serde_json::Value = client
            .post(format!(
                "http://127.0.0.1:{port}/api/projects/{pid}/threads"
            ))
            .header("cookie", &cookie)
            .json(&serde_json::json!({ "resume": "th_cap" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        serde_json::from_value(resp["thread_id"].clone()).unwrap()
    };

    let ws_request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    // Select a reasoning model with High effort (gpt-5.5 is declared in this test's config).
    ws.send(ws_text(&ClientMessage::SelectModel {
        thread_id,
        model_ref: ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: Some(Effort::new("high")),
        },
    }))
    .await
    .unwrap();
    // Switch to Plan mode.
    ws.send(ws_text(&ClientMessage::SwitchMode {
        thread_id,
        mode: Mode::Plan,
    }))
    .await
    .unwrap();
    // First turn.
    ws.send(ws_text(&ClientMessage::SendInput {
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
        first.approval_policy,
        ApprovalPolicy::Ask,
        "new threads default to ask"
    );

    // Now set the thread approval policy and send again.
    ws.send(ws_text(&ClientMessage::SetApprovalPolicy {
        thread_id,
        policy: ApprovalPolicy::ReadOnly,
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
        if tf.approval_policy == ApprovalPolicy::ReadOnly {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("thread approval policy was not persisted");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "again".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    let second = wait_for_capture(&captured, 2).await;
    assert_eq!(
        second.approval_policy,
        ApprovalPolicy::ReadOnly,
        "thread approval policy changes must reach the harness"
    );

    // Clearing effort on the same model should mean "model default", not "restore the previous
    // remembered effort".
    ws.send(ws_text(&ClientMessage::SelectModel {
        thread_id,
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
        if tf.current_model.reasoning_effort.is_none() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("thread reasoning effort was not cleared");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }
    ws.send(ws_text(&ClientMessage::SendInput {
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
