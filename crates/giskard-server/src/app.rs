use std::sync::Arc;

use axum::{Router, middleware};

use giskard_persist::PersistStore;

use crate::headers::security_headers_middleware;
use crate::highlight::Highlighter;
use crate::hub::Hub;
use crate::ledger::{self, LedgerHandle};
use crate::registry::{HarnessFactory, HarnessRegistry};
use crate::routes::{http_request_context_middleware, protected_routes, public_routes};
use crate::thread_metadata::ThreadMetadataService;
use crate::thread_runtime::ThreadRuntimeSupport;
use crate::throttle::LoginThrottle;

#[derive(Clone)]
pub struct AppShutdown {
    sender: tokio::sync::watch::Sender<bool>,
}

impl Default for AppShutdown {
    fn default() -> Self {
        let (sender, _) = tokio::sync::watch::channel(false);
        Self { sender }
    }
}

impl AppShutdown {
    pub fn trigger(&self) {
        self.sender.send_replace(true);
    }

    pub async fn wait(&self) {
        let mut receiver = self.sender.subscribe();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Shared application state passed to all Axum handlers and middleware.
///
/// Created once at startup and cloned (cheaply — everything is behind `Arc`)
/// into each request handler.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<PersistStore>,
    pub hub: Arc<Hub>,
    pub thread_metadata: Arc<ThreadMetadataService>,
    pub registry: Arc<HarnessRegistry>,
    pub runtime: Arc<ThreadRuntimeSupport>,
    pub highlighter: Arc<Highlighter>,
    /// Single-writer token-ledger actor handle (§5.4).
    pub ledger: LedgerHandle,
    pub session_key: Arc<[u8]>,
    /// Global brute-force throttle for `/api/login`.
    pub login_throttle: Arc<LoginThrottle>,
    /// Process-shutdown notification for long-lived upgraded connections.
    pub shutdown: AppShutdown,
}

impl AppState {
    /// Create a new `AppState` with default settings (10 MiB highlight limit).
    pub fn new(
        store: Arc<PersistStore>,
        factory: Arc<dyn HarnessFactory>,
        session_key: Vec<u8>,
    ) -> Self {
        Self::new_with_config(store, factory, session_key, None, None)
    }

    /// Create a new `AppState` with visualization config from `config.toml`.
    ///
    /// When `viz_config` is `None`, defaults are used (10 MiB highlight limit).
    pub fn new_with_config(
        store: Arc<PersistStore>,
        factory: Arc<dyn HarnessFactory>,
        session_key: Vec<u8>,
        viz_config: Option<&giskard_persist::config::VizConfig>,
        retention_config: Option<&giskard_persist::config::RetentionConfig>,
    ) -> Self {
        let hub = Arc::new(Hub::new());
        let runtime = Arc::new(ThreadRuntimeSupport::with_max_command_output_bytes(
            retention_config.map_or(
                giskard_persist::config::RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES,
                |retention| retention.max_command_output_bytes,
            ),
        ));
        let highlighter = match viz_config {
            Some(viz) => Arc::new(Highlighter::with_max_size(viz.max_highlight_size)),
            None => Arc::new(Highlighter::new()),
        };
        let ledger = ledger::spawn(store.clone());
        let registry = Arc::new(HarnessRegistry::new_with_runtime(
            factory,
            hub.clone(),
            runtime.clone(),
            store.clone(),
            ledger.clone(),
        ));
        let thread_metadata = registry.thread_metadata_service();
        Self {
            store,
            hub,
            thread_metadata,
            registry,
            runtime,
            highlighter,
            ledger,
            session_key: session_key.into(),
            login_throttle: Arc::new(LoginThrottle::new()),
            shutdown: AppShutdown::default(),
        }
    }
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .merge(public_routes())
        .merge(protected_routes(state.clone()))
        .layer(middleware::from_fn(security_headers_middleware))
        .layer(middleware::from_fn(http_request_context_middleware))
        .with_state(state)
}
