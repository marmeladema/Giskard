use std::sync::Arc;

use axum::Router;

use giskard_persist::PersistStore;

use crate::hub::Hub;
use crate::live_buffer::LiveBufferStore;
use crate::registry::{HarnessFactory, HarnessRegistry};
use crate::routes::{protected_routes, public_routes};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<PersistStore>,
    pub hub: Arc<Hub>,
    pub registry: Arc<HarnessRegistry>,
    pub live_buffers: Arc<LiveBufferStore>,
    pub session_key: Arc<[u8]>,
}

impl AppState {
    pub fn new(
        store: Arc<PersistStore>,
        factory: Arc<dyn HarnessFactory>,
        session_key: Vec<u8>,
    ) -> Self {
        let hub = Arc::new(Hub::new());
        let live_buffers = Arc::new(LiveBufferStore::new());
        let registry = Arc::new(HarnessRegistry::new(
            factory,
            hub.clone(),
            live_buffers.clone(),
        ));
        Self {
            store,
            hub,
            registry,
            live_buffers,
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
