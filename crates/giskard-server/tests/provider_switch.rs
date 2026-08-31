//! Verified cold-resume provider switching (spec PS1): a thread whose provider was removed from
//! config loads read-only, and selecting a model from a configured provider re-resumes the native
//! thread under it — but only when the harness *confirms* the switch. An unconfirmed switch is
//! rejected with `thread_provider_switch_ignored` and persists nothing.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::token::{TokenLedger, TokenUsage};
use giskard_core::turn::{Mode, PermissionPreset, Turn, TurnStatus, TurnStatusKind};
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::{ProjectConfig, ThreadFile, ThreadGitWorkspace, ThreadWorktree};
use giskard_proto::ClientMessage;
use giskard_server::{AppState, HarnessFactory, build_app};
use tokio::sync::{Mutex, broadcast};

use common::fake_native_model;

const DEAD_PROVIDER: &str = "cloudflare-litellm";
const NEW_PROVIDER: &str = "opencodex";

/// Opens fail for the removed provider; for any other provider the open succeeds and the handle
/// reports an effective model — either an echo of the request, or `report_provider` to simulate
/// Codex ignoring the override (the loaded-thread rejoin behavior the verification must catch).
struct SwitchHarness {
    report_provider: Option<String>,
    opened_workspace_roots: Arc<Mutex<Vec<String>>>,
    events: broadcast::Sender<giskard_core::event::AgentEvent>,
}

#[async_trait::async_trait]
impl AgentHarness for SwitchHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities::default()
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.opened_workspace_roots
            .lock()
            .await
            .push(opts.workspace_root.to_string_lossy().into_owned());
        // An import names no model, so the fake answers with the one it is already on —
        // the same thing a real harness reports from its own record.
        let mut effective = opts.initial_model.clone().unwrap_or_else(fake_native_model);
        if effective.provider == DEAD_PROVIDER {
            return Err(HarnessError::Transport(format!(
                "JSON-RPC error (-32600): failed to load configuration: Model provider \
                 {:?} not found",
                DEAD_PROVIDER
            )));
        }
        if let Some(provider) = &self.report_provider {
            effective.provider = provider.clone();
        }
        let thread = opts.thread.unwrap_or_default();
        Ok(ThreadHandle {
            resumed_model: Some(effective),
            ..ThreadHandle::opened(
                thread,
                opts.resume.unwrap_or_else(|| "fresh".into()),
                opts.workspace_root.clone(),
            )
        })
    }

    async fn start_turn(
        &self,
        _thread: &ThreadHandle,
        _input: giskard_core::user_input::UserInput,
        _overrides: giskard_core::turn::TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        Err(HarnessError::Unsupported("no turns in this test".into()))
    }

    fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
        AgentEventStream::new(self.events.subscribe())
    }

    async fn respond_approval(
        &self,
        _req: giskard_core::ids::ApprovalId,
        _decision: giskard_core::approval::ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "no approvals in this test".into(),
        ))
    }

    async fn respond_server_request(
        &self,
        _req: giskard_core::ids::ServerRequestId,
        _response: giskard_core::server_request::ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("no requests in this test".into()))
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("no turns in this test".into()))
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct SwitchFactory {
    report_provider: Option<String>,
    opened_workspace_roots: Arc<Mutex<Vec<String>>>,
    events: broadcast::Sender<giskard_core::event::AgentEvent>,
}

#[async_trait::async_trait]
impl HarnessFactory for SwitchFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
        _bootstrap: giskard_harness::HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(Arc::new(SwitchHarness {
            report_provider: self.report_provider.clone(),
            opened_workspace_roots: self.opened_workspace_roots.clone(),
            events: self.events.clone(),
        }))
    }
}

fn password_hash(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand::rngs::OsRng;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn dead_model() -> ModelRef {
    ModelRef {
        provider: DEAD_PROVIDER.into(),
        model: "@cf/z-ai/glm-4.7".into(),
        reasoning_effort: None,
    }
}

fn new_model() -> ModelRef {
    ModelRef {
        provider: NEW_PROVIDER.into(),
        model: "glm-5.2".into(),
        reasoning_effort: None,
    }
}

fn make_turn(text: &str) -> Turn {
    let now = Utc::now();
    Turn {
        id: TurnId::new(),
        user_input: giskard_core::user_input::UserInput::text(text),
        items: vec![Item {
            id: ItemId::new(),
            harness_item_id: String::new(),
            payload: ItemPayload::AgentMessage {
                text: text.to_string(),
            },
            created_at: now,
        }],
        model: giskard_core::turn::TurnModel::Known(dead_model()),
        mode: giskard_core::turn::TurnMode::Known(Mode::Build),
        status: TurnStatus {
            kind: TurnStatusKind::Completed,
            message: None,
        },
        usage: TokenUsage::new(1, 1),
        diffs: vec![],
        started_at: now,
        completed_at: Some(now),
    }
}

fn seeded_thread(
    project_id: ProjectId,
    thread_id: ThreadId,
    git_workspace: Option<ThreadGitWorkspace>,
) -> ThreadFile {
    let now = Utc::now();
    ThreadFile {
        revision: 0,
        version: 1,
        id: thread_id,
        project_id,
        title: "Orphaned thread".into(),
        harness_thread_id: format!("harness-{thread_id}"),
        parent_thread_id: None,
        spawned_by_turn_id: None,
        kind: giskard_core::ThreadKind::Primary,
        mode: giskard_core::turn::TurnMode::Known(Mode::Build),
        current_model: giskard_core::turn::TurnModel::Known(dead_model()),
        context_window: 131_072,
        model_context_windows: Default::default(),
        permission_preset: PermissionPreset::AskFirst,
        model_efforts: HashMap::new(),
        tokens: TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived: false,
        git_workspace,
    }
}

struct TestServer {
    state: AppState,
    base: String,
    port: u16,
    pid: ProjectId,
    tid: ThreadId,
    opened_workspace_roots: Arc<Mutex<Vec<String>>>,
    _tmp: tempfile::TempDir,
    _proj_dir: tempfile::TempDir,
    _worktree_dir: Option<tempfile::TempDir>,
}

/// Server whose config declares only the *new* providers; the thread is seeded under the dead one.
async fn start_server(report_provider: Option<String>) -> TestServer {
    start_server_inner(report_provider, false).await
}

async fn start_server_with_worktree(report_provider: Option<String>) -> TestServer {
    start_server_inner(report_provider, true).await
}

async fn start_server_inner(report_provider: Option<String>, seed_worktree: bool) -> TestServer {
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

[providers.{NEW_PROVIDER}]
model_listing = false
  [[providers.{NEW_PROVIDER}.models]]
  id = "glm-5.2"
  display_name = "GLM-5.2"
  context_window = 262144
  supports_reasoning_effort = false

[providers.openai]
model_listing = false
  [[providers.openai.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));

    let pid = ProjectId::new();
    let tid = ThreadId::new();
    let proj_dir = tempfile::TempDir::new().unwrap();
    let worktree_dir = seed_worktree.then(|| tempfile::TempDir::new().unwrap());
    let git_workspace = worktree_dir.as_ref().map(|worktree_dir| {
        let path = worktree_dir.path().to_string_lossy().into_owned();
        ThreadGitWorkspace::Worktree(ThreadWorktree {
            path,
            workspace: None,
            branch: "giskard/worktree-test".into(),
            base_commit: None,
            repo_root: "/repo".into(),
            common_dir: "/repo/.git".into(),
            git_dir: "/repo/.git/worktrees/test".into(),
        })
    });
    store
        .create_project(pid, "proj", &proj_dir.path().to_string_lossy())
        .await
        .unwrap();
    store
        .save_thread(pid, &seeded_thread(pid, tid, git_workspace))
        .await
        .unwrap();
    store
        .append_turn(pid, tid, &make_turn("hello from the old provider"))
        .await
        .unwrap();

    let opened_workspace_roots = Arc::new(Mutex::new(Vec::new()));
    let (events, _) = broadcast::channel(8);
    let state = AppState::new(
        store,
        Arc::new(SwitchFactory {
            report_provider,
            opened_workspace_roots: opened_workspace_roots.clone(),
            events,
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    TestServer {
        state,
        base: format!("http://{addr}"),
        port,
        pid,
        tid,
        opened_workspace_roots,
        _tmp: tmp,
        _proj_dir: proj_dir,
        _worktree_dir: worktree_dir,
    }
}

async fn login_cookie(base: &str) -> String {
    let client = reqwest::Client::new();
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
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_ws(port: u16, cookie: &str) -> Ws {
    let req = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

async fn send_msg(ws: &mut Ws, msg: &ClientMessage) {
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(msg).unwrap().into(),
    ))
    .await
    .unwrap();
}

/// Await the next WS message matching `pred` within 5 seconds.
async fn next_matching(
    ws: &mut Ws,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if pred(&v) {
                    return Some(v);
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => return None,
        }
    }
    None
}

#[tokio::test]
async fn cold_provider_switch_succeeds_and_binds_the_thread() {
    let srv = start_server(None).await;
    let cookie = login_cookie(&srv.base).await;
    let mut ws = connect_ws(srv.port, &cookie).await;

    // The thread loads read-only under the dead provider.
    send_msg(
        &mut ws,
        &ClientMessage::Subscribe {
            thread_id: srv.tid,
            since: None,
        },
    )
    .await;
    next_matching(&mut ws, |v| v["code"] == "thread_read_only")
        .await
        .expect("read-only warning");

    // Selecting a model from a configured provider triggers the verified cold re-resume…
    send_msg(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-1".into(),
            model_ref: new_model(),
        },
    )
    .await;

    // …and the broadcast thread state reports the new provider.
    let state_msg = next_matching(&mut ws, |v| {
        v["type"] == "thread_state" && v["current_model"]["provider"] == NEW_PROVIDER
    })
    .await
    .expect("thread state under the new provider");
    assert_eq!(state_msg["current_model"]["model"], "glm-5.2");

    // Persisted and natively bound.
    let tf = srv
        .state
        .store
        .load_thread(srv.pid, srv.tid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tf.current_model.as_known().unwrap().provider, NEW_PROVIDER);
    let native = srv
        .state
        .registry
        .loaded_thread_binding(srv.tid)
        .await
        .and_then(|binding| binding.native_model().cloned())
        .expect("thread must be warm after a confirmed switch");
    assert_eq!(native.provider, NEW_PROVIDER);

    // The thread is now provider-bound again: switching to yet another provider is rejected.
    send_msg(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-2".into(),
            model_ref: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        },
    )
    .await;
    let error = next_matching(&mut ws, |v| v["code"] == "thread_provider_locked")
        .await
        .expect("warm thread rejects a second provider change");
    assert_eq!(error["request_id"], "select-model-2");
}

#[tokio::test]
async fn cold_provider_switch_reopens_worktree_thread_in_its_worktree() {
    let srv = start_server_with_worktree(None).await;
    let thread = srv
        .state
        .store
        .load_thread(srv.pid, srv.tid)
        .await
        .unwrap()
        .unwrap();
    let worktree_root = thread
        .git_workspace
        .as_ref()
        .unwrap()
        .workspace_root()
        .to_string();
    let project_root = srv._proj_dir.path().to_string_lossy().into_owned();
    let cookie = login_cookie(&srv.base).await;
    let mut ws = connect_ws(srv.port, &cookie).await;

    send_msg(
        &mut ws,
        &ClientMessage::Subscribe {
            thread_id: srv.tid,
            since: None,
        },
    )
    .await;
    next_matching(&mut ws, |v| v["code"] == "thread_read_only")
        .await
        .expect("read-only warning");

    send_msg(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-3".into(),
            model_ref: new_model(),
        },
    )
    .await;
    next_matching(&mut ws, |v| {
        v["type"] == "thread_state" && v["current_model"]["provider"] == NEW_PROVIDER
    })
    .await
    .expect("thread state under the new provider");

    let opened_roots = srv.opened_workspace_roots.lock().await.clone();
    assert_eq!(
        opened_roots,
        vec![worktree_root.clone(), worktree_root],
        "both the failed dead-provider attach and the confirmed provider-switch reopen must use the worktree"
    );
    assert!(
        !opened_roots.contains(&project_root),
        "provider switch must not reopen a worktree thread in the project checkout"
    );
}

#[tokio::test]
async fn unconfirmed_provider_switch_is_rejected_and_persists_nothing() {
    // The harness claims the *old* provider stayed effective — simulating Codex ignoring the
    // resume overrides. Verification must fail the switch.
    let srv = start_server(Some(DEAD_PROVIDER.into())).await;
    let cookie = login_cookie(&srv.base).await;
    let mut ws = connect_ws(srv.port, &cookie).await;

    send_msg(
        &mut ws,
        &ClientMessage::Subscribe {
            thread_id: srv.tid,
            since: None,
        },
    )
    .await;
    next_matching(&mut ws, |v| v["code"] == "thread_read_only")
        .await
        .expect("read-only warning");

    send_msg(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-4".into(),
            model_ref: new_model(),
        },
    )
    .await;
    let err = next_matching(&mut ws, |v| v["code"] == "thread_provider_switch_ignored")
        .await
        .expect("unconfirmed switch must surface a structured error");
    assert_eq!(err["severity"], "error");

    // Nothing persisted; the thread is still cold under the old provider.
    let tf = srv
        .state
        .store
        .load_thread(srv.pid, srv.tid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tf.current_model.as_known().unwrap().provider, DEAD_PROVIDER);
    assert!(
        srv.state
            .registry
            .loaded_thread_binding(srv.tid)
            .await
            .and_then(|binding| binding.native_model().cloned())
            .is_none(),
        "failed switch must leave the thread cold"
    );
}
