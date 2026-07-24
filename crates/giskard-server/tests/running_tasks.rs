//! End-to-end coverage: a running tool/MCP call surfaces in the `RunningTasks` snapshot through the
//! real server path (registry forward → broadcast → WebSocket), the same way commands do (TK1).

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{ItemKind, ItemStart, ToolCallStart};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_proto::{ClientMessage, ServerMessage, TaskKind};
use giskard_server::{AppState, HarnessFactory, build_app};
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, Instant};

/// Harness that, on `start_turn`, emits `TurnStarted` + an in-progress tool `ItemStarted` and
/// leaves the turn open (the tool blocks the turn), so the server keeps a running tool task.
struct ToolHarness {
    tx: broadcast::Sender<AgentEvent>,
    active_turn: Mutex<Option<TurnId>>,
}

impl ToolHarness {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            tx,
            active_turn: Mutex::new(None),
        }
    }
}

#[async_trait]
impl AgentHarness for ToolHarness {
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
        Ok(ThreadHandle {
            thread: opts.thread.unwrap_or_default(),
            harness_thread_id: opts.resume.unwrap_or_else(|| "tool_harness".into()),
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
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn = TurnId::new();
        let tid = thread.thread;
        *self.active_turn.lock().await = Some(turn);
        let _ = self.tx.send(AgentEvent::TurnStarted { thread: tid, turn });
        let _ = self.tx.send(AgentEvent::ItemStarted {
            thread: tid,
            turn,
            item: ItemStart {
                id: ItemId::new(),
                harness_item_id: "tool1".into(),
                kind: ItemKind::ToolCall,
                command: None,
                tool: Some(ToolCallStart {
                    name: "search".into(),
                    input: serde_json::json!({ "q": "cats" }),
                    server: Some("wiki".into()),
                    status: Some("in_progress".into()),
                    metadata: None,
                    subagent: None,
                    started_at_ms: Some(1_785_000_000_000),
                }),
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

    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        // Interrupting the turn ends it; the still-running tool is then dropped by the registry.
        let turn = self
            .active_turn
            .lock()
            .await
            .take()
            .unwrap_or_else(TurnId::new);
        let _ = self.tx.send(AgentEvent::TurnCompleted {
            thread: thread.thread,
            turn,
            usage: giskard_core::token::TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Interrupted,
                message: None,
            },
        });
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct ToolFactory;

#[async_trait::async_trait]
impl HarnessFactory for ToolFactory {
    async fn create(&self, _config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(Arc::new(ToolHarness::new()))
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
async fn running_tool_call_surfaces_in_running_tasks_snapshot() {
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

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(store, Arc::new(ToolFactory), (0..32u8).collect());
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = {
        let resp = client
            .post(format!("{base}/api/login"))
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

    let proj_dir = tempfile::TempDir::new().unwrap();
    let pid = giskard_core::ids::ProjectId::new();
    state
        .store
        .create_project(
            pid,
            "tool-proj",
            &proj_dir.path().to_string_lossy(),
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        )
        .await
        .unwrap();

    let thread_id: ThreadId = {
        let resp: serde_json::Value = client
            .post(format!("{base}/api/projects/{pid}/threads"))
            .header("cookie", &cookie)
            .json(&serde_json::json!({ "resume": "th_tool" }))
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

    ws.send(ws_text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id,
        text: "search wikipedia".into(),
    }))
    .await
    .unwrap();

    // Read snapshots until the running tool call appears.
    let deadline = Instant::now() + Duration::from_secs(5);
    let tool_item_id = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "running tool task was not observed");
        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for a running tool task"))
        else {
            continue;
        };
        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        if let ServerMessage::RunningTasks { tasks, .. } =
            serde_json::from_str::<ServerMessage>(&text).unwrap()
            && let Some(task) = tasks.iter().find(|t| t.kind == TaskKind::Tool)
        {
            assert_eq!(task.command, "search");
            assert_eq!(task.server.as_deref(), Some("wiki"));
            assert_eq!(task.process_id, None);
            assert_eq!(task.started_at_ms, 1_785_000_000_000);
            break task.item_id;
        }
    };

    ws.send(ws_text(&ClientMessage::Interrupt { thread_id }))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "interrupted running tool task was not cleared"
        );
        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for the running tool task to clear"))
        else {
            continue;
        };
        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        if let ServerMessage::RunningTasks { tasks, .. } =
            serde_json::from_str::<ServerMessage>(&text).unwrap()
            && tasks.iter().all(|task| task.item_id != tool_item_id)
        {
            assert!(state.running_commands.snapshot(thread_id).await.is_empty());
            return;
        }
    }
}
