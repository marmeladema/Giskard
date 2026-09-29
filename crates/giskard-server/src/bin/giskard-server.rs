use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use async_trait::async_trait;
use giskard_core::error::{HarnessError, PersistError};
use giskard_harness::EnvOverlay;
use giskard_harness_codex::{CodexDeclarationOptions, CodexHarness, CodexLaunchOptions};
use giskard_persist::{Config, HarnessCatalog, HarnessDeclaration};
use giskard_server::{
    AppState, HarnessInstanceSpec, HarnessKind, HarnessKindFactory, LogDriverEventSink, build_app,
};
use tracing::{error, info, warn};
use tracing_subscriber::prelude::*;

mod common;

const HTTP_GRACEFUL_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

struct CodexKind;

/// Type-check a `codex` declaration's kind-specific keys.
fn codex_options(declaration: &HarnessDeclaration) -> Result<CodexDeclarationOptions, String> {
    let options: CodexDeclarationOptions = toml::Value::Table(declaration.options.clone())
        .try_into()
        .map_err(|error: toml::de::Error| error.message().to_owned())?;
    options.validate()?;
    Ok(options)
}

#[async_trait]
impl HarnessKind for CodexKind {
    fn name(&self) -> &str {
        "codex"
    }

    fn validate(&self, declaration: &HarnessDeclaration) -> Result<(), String> {
        codex_options(declaration).map(|_| ())
    }

    async fn create(
        &self,
        spec: HarnessInstanceSpec<'_>,
        bootstrap: giskard_harness::HarnessBootstrap,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        let declaration = spec.declaration;
        // Validated at boot; re-checked rather than trusted so a failure is an error, not a panic.
        let options = codex_options(declaration).map_err(|message| {
            HarnessError::Unsupported(format!("[harnesses.{}] {message}", spec.name))
        })?;
        let launch = CodexLaunchOptions {
            command: declaration.command.as_ref().map(std::path::PathBuf::from),
            args: declaration.args.clone(),
            env: EnvOverlay::new(
                declaration
                    .env
                    .iter()
                    .map(|(name, value)| (name.to_owned(), value.to_owned())),
            ),
            profile: options.profile,
            project_id: Some(spec.project_id),
            declaration: Some(spec.name.to_owned()),
        };
        Ok(CodexHarness::launch(spec.workspace_root, launch, bootstrap).await?)
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
    use rand::TryRng;
    let mut key = [0u8; 32];
    rand::rngs::SysRng
        .try_fill_bytes(&mut key)
        .map_err(std::io::Error::other)?;
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

fn harness_catalog(config: &Config) -> Result<HarnessCatalog, String> {
    HarnessCatalog::resolve(config).map_err(|error| format!("invalid config.toml: {error}"))
}

/// The production factory: the `codex` kind over the given catalog, every declaration validated.
fn codex_factory(catalog: HarnessCatalog) -> Result<HarnessKindFactory, String> {
    let factory = HarnessKindFactory::new()
        .register(Arc::new(CodexKind))
        .map_err(|error| error.to_string())?
        .with_catalog(catalog);
    factory
        .validate()
        .map_err(|error| format!("invalid config.toml: {error}"))?;
    for (name, declaration) in factory.catalog().iter() {
        info!(
            harness = name,
            kind = %declaration.kind,
            default = declaration.default,
            env_names = ?declaration.env.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            "declared harness"
        );
    }
    Ok(factory)
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

    // Declarations are read once, from the startup config, and every one is checked before the
    // listener binds, so a typo refuses startup instead of surfacing on the first project open.
    let catalog = harness_catalog(&startup.config)?;
    let factory = Arc::new(codex_factory(catalog)?);

    let state = AppState::new_with_config(
        startup.store,
        factory,
        session_key,
        Some(&viz),
        Some(&retention),
        Arc::new(LogDriverEventSink),
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

    fn startup_factory(src: &str) -> Result<HarnessKindFactory, String> {
        let config: Config = toml::from_str(src).expect("config parses");
        harness_catalog(&config).and_then(codex_factory)
    }

    #[test]
    fn startup_accepts_no_harnesses_table_and_two_codex_declarations() {
        let factory = startup_factory("").expect("the synthesized codex is valid");
        assert_eq!(factory.catalog().default_name(), "codex");

        let factory = startup_factory(
            r#"
[harnesses.codex-stable]
kind = "codex"
default = true

[harnesses.codex-nightly]
kind = "codex"
command = "/opt/codex-nightly/bin/codex"
profile = "nightly"
[harnesses.codex-nightly.env]
CODEX_HOME = "/home/you/.codex-nightly"
"#,
        )
        .expect("the README example is valid");
        assert_eq!(factory.catalog().default_name(), "codex-stable");
    }

    #[test]
    fn startup_refuses_invalid_declarations_naming_the_key() {
        for (src, expected) in [
            (
                "[harnesses.x]\nkind = \"nope\"\n",
                "invalid config.toml: [harnesses.x] names kind \"nope\"",
            ),
            (
                "[harnesses.x]\nkind = \"codex\"\nprofile = \"\"\n",
                "invalid config.toml: [harnesses.x] `profile` must not be blank",
            ),
            (
                "[harnesses.x]\nkind = \"codex\"\nprofiel = \"p\"\n",
                "invalid config.toml: [harnesses.x] unknown field `profiel`",
            ),
            (
                "[harnesses.x]\nkind = \"codex\"\ndefault = true\n\
                 [harnesses.y]\nkind = \"codex\"\ndefault = true\n",
                "invalid config.toml: [harnesses.x], [harnesses.y] are all marked",
            ),
            (
                "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\n\"A=B\" = \"v\"\n",
                "invalid config.toml: [harnesses.x.env] has an invalid variable name",
            ),
        ] {
            let error = match startup_factory(src) {
                Err(error) => error,
                Ok(_) => panic!("{src:?} must refuse startup"),
            };
            assert!(error.starts_with(expected), "{src:?}: {error}");
        }
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
