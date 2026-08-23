use std::sync::Arc;

use axum::{Router, middleware};

use giskard_persist::PersistStore;

use crate::headers::security_headers_middleware;
use crate::highlight::Highlighter;
use crate::hub::Hub;
use crate::ledger::{self, LedgerHandle};
use crate::models::ProjectModelCatalogStore;
use crate::registry::{HarnessFactory, HarnessRegistry};
use crate::routes::{protected_routes, public_routes};
use crate::thread_metadata::ThreadMetadataService;
use crate::thread_runtime::ThreadRuntimeRegistry;
use crate::throttle::LoginThrottle;

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
    pub runtime: Arc<ThreadRuntimeRegistry>,
    pub model_catalogs: Arc<ProjectModelCatalogStore>,
    pub highlighter: Arc<Highlighter>,
    /// Single-writer token-ledger actor handle (§5.4).
    pub ledger: LedgerHandle,
    pub session_key: Arc<[u8]>,
    /// Global brute-force throttle for `/api/login`.
    pub login_throttle: Arc<LoginThrottle>,
}

impl AppState {
    /// Create a new `AppState` with default settings (10 MiB highlight limit).
    pub fn new(
        store: Arc<PersistStore>,
        factory: Arc<dyn HarnessFactory>,
        session_key: Vec<u8>,
    ) -> Self {
        Self::new_with_config(store, factory, session_key, None)
    }

    /// Create a new `AppState` with visualization config from `config.toml`.
    ///
    /// When `viz_config` is `None`, defaults are used (10 MiB highlight limit).
    pub fn new_with_config(
        store: Arc<PersistStore>,
        factory: Arc<dyn HarnessFactory>,
        session_key: Vec<u8>,
        viz_config: Option<&giskard_persist::config::VizConfig>,
    ) -> Self {
        let hub = Arc::new(Hub::new());
        let runtime = Arc::new(ThreadRuntimeRegistry::new());
        let model_catalogs = Arc::new(ProjectModelCatalogStore::default());
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
            model_catalogs,
            highlighter,
            ledger,
            session_key: session_key.into(),
            login_throttle: Arc::new(LoginThrottle::new()),
        }
    }
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .merge(public_routes())
        .merge(protected_routes(state.clone()))
        .layer(middleware::from_fn(security_headers_middleware))
        .with_state(state)
}
