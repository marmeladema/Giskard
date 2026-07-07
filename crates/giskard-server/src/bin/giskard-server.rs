use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStarted};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};
use tracing::info;

struct ReplayFactory {
    fixture: ReplayFixture,
}

impl HarnessFactory for ReplayFactory {
    fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(Arc::new(ReplayHarness::from_fixture(self.fixture.clone())))
    }
}

fn make_demo_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let now = chrono::Utc::now();

    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_demo".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStarted {
                id: ItemId("it_1".into()),
                kind: ItemKind::AgentMessage,
            },
        },
        AgentEvent::ItemDelta {
            thread,
            turn,
            item_id: ItemId("it_1".into()),
            delta: ItemDelta::Text {
                text: "Hello! I'm a replay harness in demo mode. ".into(),
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: ItemId("it_1".into()),
                payload: ItemPayload::AgentMessage {
                    text: "Hello! I'm a replay harness in demo mode.".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(100, 50),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

fn default_data_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("GISKARD_DATA_DIR") {
        return std::path::PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    std::path::PathBuf::from(format!("{home}/.local/share/giskard"))
}

fn load_or_create_session_key(data_dir: &std::path::Path) -> Vec<u8> {
    let key_path = data_dir.join("session.key");
    if key_path.exists() {
        if let Ok(key) = std::fs::read(&key_path) {
            if key.len() == 32 {
                return key;
            }
        }
    }
    use rand::RngCore;
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    std::fs::create_dir_all(data_dir).ok();
    let perms = std::fs::Permissions::from_mode(0o700);
    std::fs::set_permissions(data_dir, std::fs::Permissions::clone(&perms)).ok();
    std::fs::write(&key_path, key).ok();
    let _ = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(&key_path, perms).ok();
    key.to_vec()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "giskard=info,tower_http=info".into()),
        )
        .init();

    let data_dir = default_data_dir();
    std::fs::create_dir_all(&data_dir).expect("cannot create data dir");
    info!(data_dir = ?data_dir, "starting giskard server");

    let store = Arc::new(giskard_persist::PersistStore::new(data_dir.clone()));
    let session_key = load_or_create_session_key(&data_dir);

    let factory = Arc::new(ReplayFactory {
        fixture: make_demo_fixture(),
    });

    let state = AppState::new(store, factory, session_key);
    let config = state.store.load_config().await.unwrap_or_default();
    let bind = config.server.bind.clone();

    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .expect("cannot bind");
    info!(bind = %bind, "listening");
    axum::serve(listener, app).await.expect("server error");
}
