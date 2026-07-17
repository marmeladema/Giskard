use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use async_trait::async_trait;
use giskard_core::error::{HarnessError, PersistError};
use giskard_persist::Config;
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

async fn load_required_config(
    store: &giskard_persist::PersistStore,
    data_dir: &std::path::Path,
) -> Result<Config, String> {
    let config_path = data_dir.join("config.toml");
    let metadata = tokio::fs::metadata(&config_path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            format!(
                "missing config file {}. GISKARD_DATA_DIR is {}. Copy config.example.toml there, \
                 edit it, and restart giskard-server.",
                config_path.display(),
                data_dir.display()
            )
        } else {
            format!(
                "cannot access config file {}: {e}. Check permissions and GISKARD_DATA_DIR.",
                config_path.display()
            )
        }
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "config path {} exists but is not a regular file. GISKARD_DATA_DIR must point to a \
             data directory containing config.toml.",
            config_path.display()
        ));
    }

    store.load_config().await.map_err(|e| match e {
        PersistError::Io(message) => format!(
            "cannot read config file {}: {message}. Check file permissions and restart \
             giskard-server.",
            config_path.display()
        ),
        PersistError::Invalid(message) => format!(
            "invalid config file {}: {message}. Fix the TOML syntax or unsupported values and \
             restart giskard-server.",
            config_path.display()
        ),
        other => format!(
            "cannot load config file {}: {other}. Fix the config and restart giskard-server.",
            config_path.display()
        ),
    })
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
    rand::rngs::OsRng.fill_bytes(&mut key);
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
    let config = load_required_config(store.as_ref(), &data_dir).await?;
    let session_key = load_or_create_session_key(&data_dir)
        .map_err(|e| format!("cannot load session key from {}: {e}", data_dir.display()))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn required_config_rejects_missing_file() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let store = giskard_persist::PersistStore::new(tmp.path().to_path_buf());

        let error = load_required_config(&store, tmp.path())
            .await
            .expect_err("missing config.toml should fail startup");

        assert!(
            error.contains("missing config file"),
            "unexpected error: {error}"
        );
        assert!(error.contains("config.toml"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn required_config_accepts_existing_empty_file() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        tokio::fs::write(tmp.path().join("config.toml"), "")
            .await
            .expect("write config");
        let store = giskard_persist::PersistStore::new(tmp.path().to_path_buf());

        let config = load_required_config(&store, tmp.path())
            .await
            .expect("existing empty config should use defaults");

        assert_eq!(config.server.bind, "127.0.0.1:8787");
        assert!(config.providers.is_empty());
    }

    #[tokio::test]
    async fn required_config_reports_invalid_toml() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        tokio::fs::write(tmp.path().join("config.toml"), "[server\nbind = 1")
            .await
            .expect("write config");
        let store = giskard_persist::PersistStore::new(tmp.path().to_path_buf());

        let error = load_required_config(&store, tmp.path())
            .await
            .expect_err("invalid config.toml should fail startup");

        assert!(
            error.contains("invalid config file"),
            "unexpected error: {error}"
        );
        assert!(error.contains("config.toml"), "unexpected error: {error}");
        assert!(
            error.contains("restart giskard-server"),
            "unexpected error: {error}"
        );
    }
}
