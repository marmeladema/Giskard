//! Deterministic, Codex-free giskard-server for end-to-end browser tests (Playwright).
//!
//! The production `giskard-server` binary spawns a real `codex app-server` per project, so it can
//! only run where Codex is installed and authenticated. Browser tests need a server that behaves
//! like the real one — same REST + WebSocket API, same static UI — but is fully self-contained and
//! deterministic. This binary provides exactly that:
//!
//! * a `ScriptedHarness` that never touches the network and emits a fixed, streamed agent reply on
//!   every turn (so the transcript/streaming UI can be asserted on);
//! * a fresh data directory, a known password, and one pre-seeded "Demo" project, so tests can log
//!   in and drive a thread without any host-side setup.
//!
//! It is a test/dev tool: it is not installed by `cargo install` (which targets `--bin
//! giskard-server`) and must never back a real user's data. Configure it with:
//!
//! * `GISKARD_DATA_DIR`   — data dir (created if missing; defaults to a fresh temp dir);
//! * `GISKARD_BIND`       — bind address (default `127.0.0.1:8787`);
//! * `GISKARD_REPLAY_PASSWORD` — the app password (default `giskard`);
//! * `GISKARD_REPLAY_WORKSPACE` — the demo project's workspace dir (created if missing).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use argon2::PasswordHasher;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::info;

use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
use giskard_core::model::ModelRef;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};

/// The scripted agent's fixed reply. Tests assert on this exact string, so keep it stable.
const SCRIPTED_REPLY: &str = "Hello from the scripted replay harness!";

/// A harness that speaks the neutral protocol but has no backend: every turn streams the same
/// canned agent message, so the browser-visible transcript is fully deterministic.
struct ScriptedHarness {
    capabilities: HarnessCapabilities,
    threads: tokio::sync::Mutex<Vec<(ThreadId, broadcast::Sender<AgentEvent>)>>,
}

impl ScriptedHarness {
    fn new() -> Self {
        Self {
            capabilities: HarnessCapabilities {
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
            },
            threads: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    async fn sender_for(&self, thread: ThreadId) -> Option<broadcast::Sender<AgentEvent>> {
        let threads = self.threads.lock().await;
        threads
            .iter()
            .find(|(id, _)| *id == thread)
            .map(|(_, tx)| tx.clone())
    }
}

#[async_trait]
impl AgentHarness for ScriptedHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        self.capabilities
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::model::ModelDescriptor>, HarnessError> {
        Ok(vec![])
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let thread = opts.thread.unwrap_or_default();
        let harness_thread_id = opts
            .resume
            .clone()
            .unwrap_or_else(|| format!("scripted_{thread}"));

        let (tx, _rx) = broadcast::channel(256);
        let mut threads = self.threads.lock().await;
        if let Some((_, existing)) = threads.iter_mut().find(|(id, _)| *id == thread) {
            *existing = tx.clone();
        } else {
            threads.push((thread, tx.clone()));
        }

        Ok(ThreadHandle {
            thread,
            harness_thread_id,
            warning: None,
            resumed_model: Some(opts.initial_model),
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn = TurnId::new();
        let thread_id = thread.thread;
        let Some(sender) = self.sender_for(thread_id).await else {
            return Err(HarnessError::ThreadNotFound(thread_id));
        };

        // Stream the canned reply the way a real harness would: start, incremental deltas, then a
        // completed item and a turn-completed with token usage. Emitted off-task with yields so the
        // WebSocket layer observes distinct frames (the transcript renders progressively).
        tokio::spawn(async move {
            let item_id = ItemId::new();
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ItemStarted {
                thread: thread_id,
                turn,
                item: ItemStart {
                    id: item_id,
                    harness_item_id: "scripted_1".into(),
                    kind: ItemKind::AgentMessage,
                    command: None,
                    tool: None,
                },
            });
            tokio::task::yield_now().await;
            for word in SCRIPTED_REPLY.split_inclusive(' ') {
                let _ = sender.send(AgentEvent::ItemDelta {
                    thread: thread_id,
                    turn,
                    item_id,
                    delta: ItemDelta::Text { text: word.into() },
                });
                tokio::task::yield_now().await;
            }
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "scripted_1".into(),
                    payload: ItemPayload::AgentMessage {
                        text: SCRIPTED_REPLY.into(),
                    },
                    created_at: chrono::Utc::now(),
                },
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn,
                usage: TokenUsage::new(120, 34),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            });
        });

        Ok(turn)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(threads) = self.threads.try_lock() {
            if let Some((_, tx)) = threads.iter().find(|(id, _)| *id == thread.thread) {
                return AgentEventStream::new(tx.subscribe());
            }
        }
        let (_, rx) = broadcast::channel(1);
        AgentEventStream::new(rx)
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
        _req: giskard_core::ids::ServerRequestId,
        _response: giskard_core::server_request::ServerRequestResponse,
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

struct ScriptedFactory;

#[async_trait]
impl HarnessFactory for ScriptedFactory {
    async fn create(&self, _config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(Arc::new(ScriptedHarness::new()))
    }
}

fn env_path(var: &str) -> Option<PathBuf> {
    std::env::var_os(var).map(PathBuf::from)
}

/// Argon2 hash of the given password, in the PHC string form the login path expects.
fn hash_password(password: &str) -> Result<String, String> {
    use argon2::password_hash::SaltString;
    let salt = SaltString::generate(&mut rand::rngs::OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("failed to hash replay password: {e}"))
}

/// Write a `config.toml` into `data_dir` so the standard loader reads it back: this keeps the
/// replay server on the exact same config path as production instead of hand-building `Config`.
fn write_config(data_dir: &Path, bind: &str, password_hash: &str) -> Result<(), String> {
    let config = format!(
        r#"[server]
bind = "{bind}"
# Plain HTTP for local/CI tests: browsers refuse a Secure cookie over http://.
secure_cookies = false

[auth]
password_hash = "{password_hash}"

[harness]
kind = "replay"

[[providers]]
id = "replay"
name = "Replay (scripted)"
wire_api = "responses"
model_listing = false
  [[providers.models]]
  id = "replay-model"
  display_name = "Replay Model"
  context_window = 131072
  supports_reasoning_effort = true
"#
    );
    std::fs::write(data_dir.join("config.toml"), config)
        .map_err(|e| format!("cannot write config.toml in {}: {e}", data_dir.display()))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "giskard=info,tower_http=info".into()),
        )
        .init();

    if let Err(e) = run().await {
        eprintln!("giskard-server-replay: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let data_dir = env_path("GISKARD_DATA_DIR").unwrap_or_else(|| {
        std::env::temp_dir().join(format!("giskard-replay-{}", std::process::id()))
    });
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("cannot create data dir {}: {e}", data_dir.display()))?;

    let bind = std::env::var("GISKARD_BIND").unwrap_or_else(|_| "127.0.0.1:8787".to_string());
    let password =
        std::env::var("GISKARD_REPLAY_PASSWORD").unwrap_or_else(|_| "giskard".to_string());
    let password_hash = hash_password(&password)?;
    write_config(&data_dir, &bind, &password_hash)?;

    let store = Arc::new(giskard_persist::PersistStore::new(data_dir.clone()));
    let config = store
        .load_config()
        .await
        .map_err(|e| format!("cannot load generated config: {e}"))?;

    // Seed one project so tests have a thread to drive without exercising the folder picker. The
    // scripted harness ignores the workspace path, but we still create it so any file endpoints
    // resolve to a real directory.
    let workspace =
        env_path("GISKARD_REPLAY_WORKSPACE").unwrap_or_else(|| data_dir.join("demo-workspace"));
    std::fs::create_dir_all(&workspace)
        .map_err(|e| format!("cannot create workspace {}: {e}", workspace.display()))?;

    let projects = store
        .load_project_index()
        .await
        .map_err(|e| format!("cannot read project index: {e}"))?;
    if projects.projects.is_empty() {
        let default_model = ModelRef {
            provider: "replay".into(),
            model: "replay-model".into(),
            reasoning_effort: None,
        };
        store
            .create_project(
                giskard_core::ids::ProjectId::new(),
                "Demo",
                &workspace.to_string_lossy(),
                default_model,
            )
            .await
            .map_err(|e| format!("cannot seed demo project: {e}"))?;
        info!(workspace = %workspace.display(), "seeded demo project");
    }

    // A fresh random session key each boot is fine: the replay server holds no durable sessions.
    let mut session_key = [0u8; 32];
    {
        use rand::RngCore;
        rand::rngs::OsRng.fill_bytes(&mut session_key);
    }

    let factory = Arc::new(ScriptedFactory);
    let state = AppState::new_with_config(store, factory, session_key.to_vec(), Some(&config.viz));
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("cannot bind {bind}: {e}"))?;
    info!(bind = %bind, data_dir = %data_dir.display(), "giskard-server-replay listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("server error: {e}"))?;
    Ok(())
}
