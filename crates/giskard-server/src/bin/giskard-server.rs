use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use async_trait::async_trait;
use giskard_core::error::{HarnessError, PersistError};
use giskard_persist::Config;
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};
use tracing::{error, info, warn};
use tracing_subscriber::prelude::*;

mod common;

const HTTP_GRACEFUL_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

struct CodexFactory;

#[async_trait]
impl HarnessFactory for CodexFactory {
    async fn create(
        &self,
        config: &ProjectConfig,
        bootstrap: giskard_harness::HarnessBootstrap,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        if config.harness != "codex" {
            return Err(HarnessError::Unsupported(format!(
                "unsupported harness kind: {}",
                config.harness
            )));
        }

        let workspace_root =
            std::path::PathBuf::from(config.workspace_root.as_deref().unwrap_or(&config.dir));
        Ok(
            giskard_harness_codex::CodexHarness::start_with_bootstrap(workspace_root, bootstrap)
                .await?,
        )
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

/// Take the data-directory lock, or refuse to start.
///
/// Two servers on one data directory would interleave writes that each believes are serialized by
/// its own in-process per-thread locks — which order nothing between processes. Refusing here is
/// also what makes `giskard-admin`'s destructive commands able to assume no server is running.
fn acquire_data_dir_lock(
    data_dir: &std::path::Path,
) -> Result<giskard_persist::DataDirLock, String> {
    match giskard_persist::DataDirLock::try_acquire(data_dir) {
        Ok(Some(lock)) => Ok(lock),
        Ok(None) => Err(format!(
            "another Giskard process is using the data directory {}. Stop it (or set \
             GISKARD_DATA_DIR to a different directory) and start giskard-server again.",
            data_dir.display()
        )),
        Err(e) => Err(format!(
            "cannot lock data directory {}: {e}",
            data_dir.display()
        )),
    }
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
    let startup = match prepare_startup().await {
        Ok(startup) => startup,
        Err(error) => {
            eprintln!("giskard-server: {error}");
            std::process::exit(1);
        }
    };
    // Acquire exclusion before the rolling appender opens files at the configured location. Keep
    // the guard in `main` so it outlives the server and the file logger.
    let _data_dir_lock = match acquire_data_dir_lock(&startup.data_dir) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("giskard-server: {error}");
            std::process::exit(1);
        }
    };
    let configured_file =
        match configured_file_writer(&startup.config.logging.file, &startup.data_dir) {
            Ok(configured) => configured,
            Err(error) => {
                eprintln!("giskard-server: {error}");
                std::process::exit(1);
            }
        };
    let (file_writer, file_log_guard, file_log_path) = match configured_file {
        Some(configured) => (
            Some(configured.writer),
            Some(configured.guard),
            Some(configured.path),
        ),
        None => (None, None, None),
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "giskard=info,tower_http=info".into());
    let file_layer = file_writer.map(|writer| {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(writer)
    });
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(file_layer)
        .init();

    if let Some(path) = file_log_path {
        info!(path = %path.display(), "file logging enabled");
    }
    info!(data_dir = ?startup.data_dir, "starting giskard server");

    let shutdown = common::shutdown::install_signal_handler();
    match common::shutdown::run_until_forced(run(startup, shutdown.clone()), shutdown).await {
        common::shutdown::RunOutcome::Completed(Ok(())) => {}
        common::shutdown::RunOutcome::Completed(Err(error)) => {
            error!(%error, "giskard server stopped with an error");
            eprintln!("giskard-server: {error}");
            drop(file_log_guard);
            std::process::exit(1);
        }
        common::shutdown::RunOutcome::Forced(signal) => {
            error!(
                signal,
                "second shutdown signal received; forcing process exit"
            );
            eprintln!("giskard-server: second {signal} received; forcing process exit");
            drop(file_log_guard);
            std::process::exit(1);
        }
    }
}

struct Startup {
    data_dir: std::path::PathBuf,
    store: Arc<giskard_persist::PersistStore>,
    config: giskard_persist::Config,
}

async fn prepare_startup() -> Result<Startup, String> {
    let data_dir = default_data_dir();
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("cannot create data dir {}: {e}", data_dir.display()))?;

    let store = Arc::new(giskard_persist::PersistStore::new(data_dir.clone()));
    let config = load_required_config(store.as_ref(), &data_dir).await?;
    Ok(Startup {
        data_dir,
        store,
        config,
    })
}

struct ConfiguredFileWriter {
    writer: tracing_appender::non_blocking::NonBlocking,
    guard: tracing_appender::non_blocking::WorkerGuard,
    path: std::path::PathBuf,
}

fn configured_file_writer(
    config: &giskard_persist::config::FileLoggingConfig,
    data_dir: &std::path::Path,
) -> Result<Option<ConfiguredFileWriter>, String> {
    if !config.enabled {
        return Ok(None);
    }
    let configured_path = std::path::PathBuf::from(&config.path);
    let path = if configured_path.is_absolute() {
        configured_path
    } else {
        data_dir.join(configured_path)
    };
    let directory = path
        .parent()
        .ok_or_else(|| format!("file log path {} has no parent directory", path.display()))?;
    let prefix = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| {
            format!(
                "file log path {} has no valid UTF-8 file name",
                path.display()
            )
        })?;
    let appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix(prefix)
        .build(directory)
        .map_err(|error| {
            format!(
                "cannot initialize file logging at {}: {error}",
                path.display()
            )
        })?;
    let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .lossy(false)
        .finish(appender);
    Ok(Some(ConfiguredFileWriter {
        writer,
        guard,
        path,
    }))
}

async fn run(
    startup: Startup,
    shutdown: tokio::sync::watch::Receiver<common::shutdown::Phase>,
) -> Result<(), String> {
    let session_key = load_or_create_session_key(&startup.data_dir).map_err(|e| {
        format!(
            "cannot load session key from {}: {e}",
            startup.data_dir.display()
        )
    })?;
    let bind = startup.config.server.bind.clone();
    let viz = startup.config.viz.clone();
    let retention = startup.config.retention.clone();

    let factory = Arc::new(CodexFactory);

    let state = AppState::new_with_config(
        startup.store,
        factory,
        session_key,
        Some(&viz),
        Some(&retention),
    );
    let registry = state.registry.clone();
    let app_shutdown = state.shutdown.clone();

    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("cannot bind {bind}: {e}"))?;
    info!(bind = %bind, "listening");
    common::shutdown::serve_then_shutdown_registry(
        listener,
        app,
        app_shutdown,
        shutdown,
        HTTP_GRACEFUL_SHUTDOWN_TIMEOUT,
        "giskard-server",
        &registry,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn configured_file_writer_writes_without_pruning_prefix_matches() {
        let tmp = tempfile::tempdir().expect("create temporary data directory");
        let log_dir = tmp.path().join("logs");
        std::fs::create_dir(&log_dir).expect("create log directory");
        let unrelated = log_dir.join("server.log.backup");
        std::fs::write(&unrelated, "keep me").expect("seed unrelated prefix match");
        let config = giskard_persist::config::FileLoggingConfig {
            enabled: true,
            path: "logs/server.log".into(),
        };
        let configured = configured_file_writer(&config, tmp.path())
            .expect("configure")
            .expect("enabled file logging");
        assert_eq!(configured.path, tmp.path().join("logs/server.log"));

        let mut writer = configured.writer;
        writeln!(writer, "file logging probe").expect("enqueue log record");
        drop(writer);
        drop(configured.guard);

        let entries = std::fs::read_dir(&log_dir)
            .expect("read log directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("read log entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(
            std::fs::read_to_string(unrelated).expect("read unrelated file"),
            "keep me"
        );
        let generated = entries
            .iter()
            .find(|entry| entry.file_name() != "server.log.backup")
            .expect("generated daily log");
        assert!(
            generated
                .file_name()
                .to_string_lossy()
                .starts_with("server.log.")
        );
        let contents = std::fs::read_to_string(generated.path()).expect("read log file");
        assert_eq!(contents, "file logging probe\n");
    }

    /// A second server on one data directory would interleave writes that each believes its own
    /// in-process per-thread locks serialize — and those order nothing between processes.
    #[test]
    fn startup_refuses_a_data_directory_another_process_holds() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let held = acquire_data_dir_lock(tmp.path()).expect("first server takes the directory");

        let error =
            acquire_data_dir_lock(tmp.path()).expect_err("a second server must refuse to start");
        assert!(
            error.contains("another Giskard process is using the data directory"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("GISKARD_DATA_DIR"),
            "unexpected error: {error}"
        );

        drop(held);
        assert!(
            acquire_data_dir_lock(tmp.path()).is_ok(),
            "the directory is takeable once the first server is gone"
        );
    }

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
