use std::sync::Arc;

use axum::Router;

use giskard_persist::PersistStore;

use crate::highlight::Highlighter;
use crate::hub::Hub;
use crate::ledger::{self, LedgerHandle};
use crate::live_buffer::LiveBufferStore;
use crate::registry::{HarnessFactory, HarnessRegistry};
use crate::routes::{protected_routes, public_routes};
use crate::running_commands::RunningTaskStore;

/// Shared application state passed to all Axum handlers and middleware.
///
/// Created once at startup and cloned (cheaply — everything is behind `Arc`)
/// into each request handler.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<PersistStore>,
    pub hub: Arc<Hub>,
    pub registry: Arc<HarnessRegistry>,
    pub live_buffers: Arc<LiveBufferStore>,
    pub running_commands: Arc<RunningTaskStore>,
    pub highlighter: Arc<Highlighter>,
    /// Single-writer token-ledger actor handle (§5.4).
    pub ledger: LedgerHandle,
    pub session_key: Arc<[u8]>,
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
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let highlighter = match viz_config {
            Some(viz) => Arc::new(Highlighter::with_max_size(viz.max_highlight_size)),
            None => Arc::new(Highlighter::new()),
        };
        let ledger = ledger::spawn(store.clone());
        let registry = Arc::new(HarnessRegistry::new(
            factory,
            hub.clone(),
            live_buffers.clone(),
            running_commands.clone(),
            store.clone(),
            ledger.clone(),
        ));
        Self {
            store,
            hub,
            registry,
            live_buffers,
            running_commands,
            highlighter,
            ledger,
            session_key: session_key.into(),
        }
    }
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .merge(public_routes())
        .merge(protected_routes(state.clone()))
        .with_state(state)
}
