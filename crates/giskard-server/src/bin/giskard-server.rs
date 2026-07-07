use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use async_trait::async_trait;
use giskard_core::error::HarnessError;
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};
use tracing::{error, info, warn};

struct CodexFactory;

#[async_trait]
impl HarnessFactory for CodexFactory {
    async fn create(
        &self,
        config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        if config.harness != "codex" {
            return Err(HarnessError::Unsupported(format!(
                "unsupported harness kind: {}",
                config.harness
            )));
        }

        let workspace_root =
            std::path::PathBuf::from(config.workspace_root.as_deref().unwrap_or(&config.dir));
        let harness = giskard_harness_codex::CodexHarness::start(workspace_root).await?;
        Ok(harness)
    }
}

fn default_data_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("GISKARD_DATA_DIR") {
        return std::path::PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    std::path::PathBuf::from(format!("{home}/.local/share/giskard"))
}

fn load_or_create_session_key(data_dir: &std::path::Path) -> std::io::Result<Vec<u8>> {
    let key_path = data_dir.join("session.key");
    if key_path.exists() {
        match std::fs::read(&key_path) {
            Ok(key) if key.len() == 32 => return Ok(key),
            Ok(key) => {
                warn!(
                    path = ?key_path,
                    len = key.len(),
                    "ignoring invalid session key length"
                );
            }
            Err(e) => {
                warn!(path = ?key_path, "failed to read session key: {e}");
            }
        }
    }
    use rand::RngCore;
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    std::fs::create_dir_all(data_dir)?;
    std::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(0o700))?;
    std::fs::write(&key_path, key)?;
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
    Ok(key.to_vec())
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
        error!("{e}");
        eprintln!("giskard-server: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let data_dir = default_data_dir();
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("cannot create data dir {}: {e}", data_dir.display()))?;
    info!(data_dir = ?data_dir, "starting giskard server");

    let store = Arc::new(giskard_persist::PersistStore::new(data_dir.clone()));
    let session_key = load_or_create_session_key(&data_dir)
        .map_err(|e| format!("cannot load session key from {}: {e}", data_dir.display()))?;
    let config = match store.load_config().await {
        Ok(config) => config,
        Err(e) => {
            warn!("failed to load config, using defaults: {e}");
            Default::default()
        }
    };
    let bind = config.server.bind.clone();

    let factory = Arc::new(CodexFactory);

    let state = AppState::new_with_config(store, factory, session_key, Some(&config.viz));

    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("cannot bind {bind}: {e}"))?;
    info!(bind = %bind, "listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("server error: {e}"))?;
    Ok(())
}
