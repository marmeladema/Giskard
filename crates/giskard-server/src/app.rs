use std::sync::Arc;

use axum::{Router, middleware};

use giskard_persist::PersistStore;

use crate::headers::security_headers_middleware;
use crate::highlight::Highlighter;
use crate::hub::Hub;
use crate::ledger::{self, LedgerHandle};
use crate::live_buffer::LiveBufferStore;
use crate::registry::{HarnessFactory, HarnessRegistry};
use crate::routes::{protected_routes, public_routes};
use crate::running_commands::RunningTaskStore;
use crate::throttle::LoginThrottle;
use crate::trace::TraceHandle;

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
    /// Global brute-force throttle for `/api/login`.
    pub login_throttle: Arc<LoginThrottle>,
    /// On-demand tracing capture handle (spec §17).
    pub trace: TraceHandle,
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
        Self::new_full(store, factory, session_key, viz_config, None, None)
    }

    /// Create an `AppState` carrying both visualization and tracing configuration.
    pub fn new_full(
        store: Arc<PersistStore>,
        factory: Arc<dyn HarnessFactory>,
        session_key: Vec<u8>,
        viz_config: Option<&giskard_persist::config::VizConfig>,
        tracing_config: Option<&giskard_persist::config::TracingConfig>,
        trace_handle: Option<TraceHandle>,
    ) -> Self {
        let trace = trace_handle.unwrap_or_else(|| {
            TraceHandle::new(
                tracing_config.map(|c| c.buffer_max_traces).unwrap_or(256),
                tracing_config
                    .map(|c| c.capture == giskard_persist::config::TracingCapture::Armed)
                    .unwrap_or(false),
            )
        });
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
            login_throttle: Arc::new(LoginThrottle::new()),
            trace,
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

/// HTTP request span middleware (spec §17). Opens a `tracing` span per request with `method` and
/// the matched template route (when available) so the on-demand trace shows one span per
/// request. The literal id is kept out of the `route` label via `MatchedPath`; when no matched
/// path is available the raw path is used (acceptable since capture is on-demand).
///
/// When the browser sends a W3C `traceparent` header, its `trace_id`/`parent_span_id` are recorded
/// on this span so server-side spans join the browser's trace (§17.4). Applied as a `route_layer`
/// inside each router so `MatchedPath` is populated when the span is created.
pub(crate) async fn request_span_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::extract::MatchedPath;
    let method = req.method().clone();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    // Extract W3C traceparent (if present) so this request joins the browser's trace.
    let (trace_id, parent_span_id) = req
        .headers()
        .get("traceparent")
        .and_then(|h| h.to_str().ok())
        .and_then(crate::trace::parse_traceparent)
        .map(|(tid, pspan)| (Some(tid), Some(pspan)))
        .unwrap_or((None, None));
    // Record the W3C trace context as fields directly in the span (captured by `on_new_span`),
    // so server-side spans join the browser's trace. We record the bare strings via `display`
    // (empty when no header is present); the capture layer treats an empty `trace_id` as "no
    // propagated context".
    let span = tracing::info_span!(
        "http.request",
        %method,
        %route,
        trace_id = tracing::field::display(trace_id.clone().unwrap_or_default()),
        parent_span_id = tracing::field::display(parent_span_id.clone().unwrap_or_default()),
        // Declare `status` up front as Empty so the post-response `span.record("status", …)`
        // below actually emits. tracing drops `.record()` calls for fields not in the fieldset,
        // so an undeclared `status` would never reach on_record / the export (F1a).
        status = tracing::field::Empty,
    );
    // Use the span via `Instrument` rather than a held `EnteredSpan` guard so the middleware
    // future stays `Send` (axum requires it).
    use tracing::Instrument;
    let response = next.run(req).instrument(span.clone()).await;
    let status = response.status().as_u16();
    span.record("status", status);
    response
}
