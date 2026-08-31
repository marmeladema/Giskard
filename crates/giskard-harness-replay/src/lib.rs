//! Deterministic replay harness for testing (spec §14.2).
//!
//! Reads a recorded fixture (JSONL of `AgentEvent`s) and replays them through the
//! `AgentHarness` trait with deterministic timing. No real LLM, no network.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, mpsc, watch};
use tokio::task::JoinHandle;

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
use giskard_core::mcp::McpServerStatus;
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::token::TokenUsage;
use giskard_core::turn::TurnOverrides;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, ClaimedNativeRoute, HarnessBootstrap, HarnessCapabilities,
    HarnessProvider, HarnessSignal, HarnessSignalStream, OpenThreadOptions, ThreadActivationCause,
    ThreadHandle, thread_activation,
};

const ROUTE_CAPACITY: usize = 256;
const SIGNAL_CAPACITY: usize = 64;

/// A recorded fixture: an ordered list of `AgentEvent`s to replay.
#[derive(Clone)]
pub struct ReplayFixture {
    pub events: Vec<AgentEvent>,
}

impl ReplayFixture {
    /// Load a fixture from a JSONL file (one `AgentEvent` per line).
    pub fn load(path: &Path) -> Result<Self, String> {
        let data = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let mut events = Vec::new();
        for (i, line) in data.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let event: AgentEvent =
                serde_json::from_str(line).map_err(|e| format!("line {}: {}", i + 1, e))?;
            events.push(event);
        }
        Ok(Self { events })
    }

    /// Create a fixture from a list of events.
    pub fn from_events(events: Vec<AgentEvent>) -> Self {
        Self { events }
    }

    /// Save as JSONL.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        use std::io::Write;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let mut file = std::fs::File::create(path).map_err(|e| e.to_string())?;
        for event in &self.events {
            let json = serde_json::to_string(event).map_err(|e| e.to_string())?;
            writeln!(file, "{json}").map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

struct ThreadState {
    route: ClaimedNativeRoute,
    sender: mpsc::Sender<AgentEvent>,
    receiver: Option<mpsc::Receiver<AgentEvent>>,
    pending: Vec<AgentEvent>,
    ready: bool,
}

#[derive(Default)]
struct ReplayRoutes {
    by_native: HashMap<String, ThreadState>,
    native_by_thread: HashMap<ThreadId, String>,
    next_epoch: u64,
}

struct PreloadedFixture {
    thread_id: ThreadId,
    events: Vec<AgentEvent>,
}

enum ReplayLifecycle {
    Running { producers: Vec<JoinHandle<()>> },
    Stopping,
    Stopped,
}

/// A harness that replays recorded events deterministically.
pub struct ReplayHarness {
    capabilities: HarnessCapabilities,
    routes: StdMutex<ReplayRoutes>,
    signal_tx: StdMutex<Option<mpsc::Sender<HarnessSignal>>>,
    signals: StdMutex<Option<mpsc::Receiver<HarnessSignal>>>,
    fixtures: Mutex<HashMap<String, PreloadedFixture>>,
    /// Catalog returned by `list_models` (empty unless set via [`ReplayHarness::with_models`]),
    /// standing in for a real harness's model catalog (e.g. Codex `model/list`).
    models: Vec<ModelDescriptor>,
    /// When set, `list_models` fails with this message instead of returning `models` — used to
    /// exercise the server's best-effort degradation when a harness catalog query errors.
    models_error: Option<String>,
    /// Provider table returned by `list_providers` (empty unless set via
    /// [`ReplayHarness::with_providers`]), standing in for a real harness's own provider config.
    providers: Vec<HarnessProvider>,
    /// When set, `list_providers` fails with this message instead of returning `providers`.
    providers_error: Option<String>,
    /// Version reported by `client_version`, standing in for a real harness's own. `None` by
    /// default: a harness that cannot say must not have one invented for it.
    client_version: Option<String>,
    /// The model reported for a thread imported by native id, where the caller names none and a
    /// real harness answers from the thread's own record. Defaults to `openai/gpt-5.5`; a test
    /// whose config does not offer that model sets its own with
    /// [`ReplayHarness::with_imported_model`].
    imported_model: ModelRef,
    lifecycle: StdMutex<ReplayLifecycle>,
    stop_tx: watch::Sender<bool>,
    shutdown_complete: Notify,
}

impl ReplayHarness {
    /// Create a new replay harness with full Codex-like capabilities.
    pub fn new() -> Self {
        Self::with_fixtures(HashMap::new())
    }

    /// Create a replay harness with every durable native/local binding installed before use.
    pub fn new_with_bootstrap(bootstrap: HarnessBootstrap) -> Result<Self, HarnessError> {
        Self::new().with_bootstrap(bootstrap)
    }

    /// Install a complete bootstrap while the harness is still exclusively owned.
    ///
    /// Consuming `self` prevents a partially installed route table from being published when a
    /// binding is invalid or conflicts with another binding.
    pub fn with_bootstrap(mut self, bootstrap: HarnessBootstrap) -> Result<Self, HarnessError> {
        {
            let routes = self.routes.get_mut().map_err(|poisoned| {
                HarnessError::Protocol(format!(
                    "replay route lock was poisoned during bootstrap: {poisoned}"
                ))
            })?;
            for binding in bootstrap.known_threads {
                if let Some(existing) = routes.by_native.get(binding.harness_thread_id.trim())
                    && existing.route.thread_id != binding.thread_id
                {
                    return Err(HarnessError::Protocol(format!(
                        "replay bootstrap native route {} maps to both {} and {}",
                        binding.harness_thread_id, existing.route.thread_id, binding.thread_id
                    )));
                }
                claim_route(routes, &binding.harness_thread_id, binding.thread_id)?;
            }
        }
        Ok(self)
    }

    fn with_fixtures(fixtures: HashMap<String, PreloadedFixture>) -> Self {
        let (signal_tx, signals) = mpsc::channel(SIGNAL_CAPACITY);
        let (stop_tx, _) = watch::channel(false);
        Self {
            capabilities: HarnessCapabilities {
                live_approvals: true,
                plan_build_modes: true,
                per_turn_model: true,
                reasoning_effort: true,
                structured_diffs: true,
                resumable_threads: true,
                model_listing: false,
                provider_listing: false,
                token_usage: true,
                mcp_status: true,
                mcp_reload: true,
                mcp_oauth_login: false,
                context_compaction: true,
            },
            routes: StdMutex::new(ReplayRoutes::default()),
            signal_tx: StdMutex::new(Some(signal_tx)),
            signals: StdMutex::new(Some(signals)),
            fixtures: Mutex::new(fixtures),
            models: Vec::new(),
            models_error: None,
            providers: Vec::new(),
            providers_error: None,
            client_version: None,
            imported_model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
            lifecycle: StdMutex::new(ReplayLifecycle::Running {
                producers: Vec::new(),
            }),
            stop_tx,
            shutdown_complete: Notify::new(),
        }
    }

    fn lock_routes(&self) -> Result<StdMutexGuard<'_, ReplayRoutes>, HarnessError> {
        self.routes
            .lock()
            .map_err(|_| HarnessError::Transport("replay route lock was poisoned".into()))
    }

    fn lock_lifecycle(&self) -> Result<StdMutexGuard<'_, ReplayLifecycle>, HarnessError> {
        self.lifecycle
            .lock()
            .map_err(|_| HarnessError::Transport("replay lifecycle lock was poisoned".into()))
    }

    fn ensure_running(&self) -> Result<(), HarnessError> {
        if matches!(&*self.lock_lifecycle()?, ReplayLifecycle::Running { .. }) {
            Ok(())
        } else {
            Err(HarnessError::Transport(
                "replay harness is shut down".into(),
            ))
        }
    }

    fn signal_sender(&self) -> Result<mpsc::Sender<HarnessSignal>, HarnessError> {
        let lifecycle = self.lock_lifecycle()?;
        if !matches!(&*lifecycle, ReplayLifecycle::Running { .. }) {
            return Err(HarnessError::Transport(
                "replay harness is shut down".into(),
            ));
        }
        self.signal_tx
            .lock()
            .map_err(|_| HarnessError::Transport("replay signal lock was poisoned".into()))?
            .clone()
            .ok_or_else(|| HarnessError::Transport("replay harness is shut down".into()))
    }

    fn claim_route_while_running(
        &self,
        harness_thread_id: &str,
        suggested_thread_id: ThreadId,
    ) -> Result<ClaimedNativeRoute, HarnessError> {
        let lifecycle = self.lock_lifecycle()?;
        if !matches!(&*lifecycle, ReplayLifecycle::Running { .. }) {
            return Err(HarnessError::Transport(
                "replay harness is shut down".into(),
            ));
        }
        let mut routes = self.lock_routes()?;
        claim_route(&mut routes, harness_thread_id, suggested_thread_id)
    }

    fn spawn_producer<F>(&self, producer: F) -> Result<(), HarnessError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut lifecycle = self.lock_lifecycle()?;
        let ReplayLifecycle::Running { producers } = &mut *lifecycle else {
            return Err(HarnessError::Transport(
                "replay harness is shut down".into(),
            ));
        };
        producers.retain(|producer| !producer.is_finished());
        producers.push(tokio::spawn(producer));
        Ok(())
    }

    /// Advertise a model catalog: sets the list returned by `list_models` and turns on the
    /// `model_listing` capability, so the server's per-project model overlay runs against it.
    pub fn with_models(mut self, models: Vec<ModelDescriptor>) -> Self {
        self.capabilities.model_listing = true;
        self.models = models;
        self
    }

    /// Advertise `model_listing` but make `list_models` fail, to exercise the server's best-effort
    /// degradation (the picker should still get the config + discovery list).
    pub fn with_failing_models(mut self, message: impl Into<String>) -> Self {
        self.capabilities.model_listing = true;
        self.models_error = Some(message.into());
        self
    }

    /// Report a harness version, as Codex does from its initialize handshake. Discovery sends it
    /// to a provider's `/models` endpoint as `client_version` (§8.3).
    pub fn with_client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = Some(version.into());
        self
    }

    /// Advertise a provider table: sets what `list_providers` returns and turns on the
    /// `provider_listing` capability, so the server resolves discovery endpoints and validates
    /// configured provider ids against it.
    pub fn with_providers(mut self, providers: Vec<HarnessProvider>) -> Self {
        self.capabilities.provider_listing = true;
        self.providers = providers;
        self
    }

    /// Advertise `provider_listing` but make `list_providers` fail, to exercise the server's
    /// best-effort degradation (the picker should still get the static config list).
    pub fn with_failing_providers(mut self, message: impl Into<String>) -> Self {
        self.capabilities.provider_listing = true;
        self.providers_error = Some(message.into());
        self
    }

    /// Set the model this harness reports for a thread imported by native id — the stand-in for
    /// the model that thread was already on. Tests whose configured providers do not include the
    /// default `openai/gpt-5.5` need this, or the imported thread lands on a provider their config
    /// never declared.
    pub fn with_imported_model(mut self, model: ModelRef) -> Self {
        self.imported_model = model;
        self
    }

    /// Load a fixture and create a harness pre-loaded with those events
    /// for a single thread.
    pub fn from_fixture(fixture: ReplayFixture) -> Self {
        let (thread_id, harness_thread_id) = fixture
            .events
            .iter()
            .find_map(|e| match e {
                AgentEvent::ThreadOpened {
                    thread,
                    harness_thread_id,
                } => Some((*thread, harness_thread_id.clone())),
                _ => None,
            })
            .unwrap_or_else(|| (ThreadId::new(), format!("replay_{}", ThreadId::new())));

        let mut fixtures = HashMap::new();
        fixtures.insert(
            harness_thread_id,
            PreloadedFixture {
                thread_id,
                events: fixture.events,
            },
        );
        Self::with_fixtures(fixtures)
    }
}

impl Default for ReplayHarness {
    fn default() -> Self {
        Self::new()
    }
}

fn claim_route(
    routes: &mut ReplayRoutes,
    harness_thread_id: &str,
    suggested_thread_id: ThreadId,
) -> Result<ClaimedNativeRoute, HarnessError> {
    let harness_thread_id = harness_thread_id.trim();
    if harness_thread_id.is_empty() {
        return Err(HarnessError::Protocol(
            "cannot claim an empty replay native thread id".into(),
        ));
    }
    if let Some(existing) = routes.by_native.get(harness_thread_id) {
        return Ok(existing.route.clone());
    }
    if let Some(existing) = routes.native_by_thread.get(&suggested_thread_id) {
        return Err(HarnessError::Protocol(format!(
            "replay thread {suggested_thread_id} is already bound to native route {existing}"
        )));
    }
    routes.next_epoch = routes
        .next_epoch
        .checked_add(1)
        .ok_or_else(|| HarnessError::Protocol("replay route epoch space exhausted".into()))?;
    let route = ClaimedNativeRoute {
        thread_id: suggested_thread_id,
        harness_thread_id: harness_thread_id.to_owned(),
        route_epoch: routes.next_epoch,
    };
    let (sender, receiver) = mpsc::channel(ROUTE_CAPACITY);
    routes
        .native_by_thread
        .insert(route.thread_id, route.harness_thread_id.clone());
    routes.by_native.insert(
        route.harness_thread_id.clone(),
        ThreadState {
            route: route.clone(),
            sender,
            receiver: Some(receiver),
            pending: Vec::new(),
            ready: false,
        },
    );
    Ok(route)
}

#[async_trait]
impl AgentHarness for ReplayHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        self.capabilities
    }

    fn client_version(&self) -> Option<String> {
        self.client_version.clone()
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        self.ensure_running()?;
        match &self.models_error {
            Some(message) => Err(HarnessError::Transport(message.clone())),
            None => Ok(self.models.clone()),
        }
    }

    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        self.ensure_running()?;
        match &self.providers_error {
            Some(message) => Err(HarnessError::Transport(message.clone())),
            None => Ok(self.providers.clone()),
        }
    }

    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError> {
        self.ensure_running()?;
        Ok(vec![])
    }

    async fn reload_mcp_servers(&self) -> Result<(), HarnessError> {
        self.ensure_running()?;
        Ok(())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.ensure_running()?;
        let harness_thread_id = opts
            .resume
            .clone()
            .unwrap_or_else(|| format!("replay_{}", ThreadId::new()));

        let (suggested_thread_id, mut pending) = if let Some(resume) = &opts.resume {
            let mut fixtures = self.fixtures.lock().await;
            if let Some(fixture) = fixtures.remove(resume) {
                (opts.thread.unwrap_or(fixture.thread_id), fixture.events)
            } else {
                (opts.thread.unwrap_or_default(), Vec::new())
            }
        } else {
            (opts.thread.unwrap_or_default(), Vec::new())
        };
        let route = self.claim_route_while_running(&harness_thread_id, suggested_thread_id)?;
        for event in &mut pending {
            remap_event_thread(event, route.thread_id);
        }
        {
            let mut routes = self.lock_routes()?;
            let entry = routes
                .by_native
                .get_mut(&route.harness_thread_id)
                .filter(|entry| entry.route == route)
                .ok_or_else(|| HarnessError::Protocol("stale replay native route".into()))?;
            if !entry.pending.is_empty() && !pending.is_empty() {
                return Err(HarnessError::Protocol(format!(
                    "replay route {} already has pending fixture events",
                    route.harness_thread_id
                )));
            }
            entry.pending.extend(pending);
        }

        let effective_model = opts
            .initial_model
            .clone()
            .unwrap_or_else(|| self.imported_model.clone());
        if let Some(generation) = opts.identity_generation {
            let already_ready = self
                .lock_routes()?
                .by_native
                .get(&route.harness_thread_id)
                .is_some_and(|entry| entry.ready);
            if !already_ready {
                let (activation, readiness) = thread_activation(
                    route.clone(),
                    ThreadActivationCause::IdentityResponse {
                        method: "replay/open".into(),
                        generation,
                        reported_model: Some(effective_model.clone()),
                    },
                );
                let signal_tx = self.signal_sender()?;
                let mut stop = self.stop_tx.subscribe();
                tokio::select! {
                    biased;
                    _ = wait_for_stop(&mut stop) => {
                        return Err(HarnessError::Transport("replay harness is shut down".into()));
                    }
                    result = signal_tx.send(HarnessSignal::Activate(activation)) => {
                        result.map_err(|_| {
                            HarnessError::Transport("replay harness signal receiver closed".into())
                        })?;
                    }
                }
                tokio::select! {
                    biased;
                    _ = wait_for_stop(&mut stop) => {
                        return Err(HarnessError::Transport("replay harness is shut down".into()));
                    }
                    result = readiness => {
                        result.map_err(|_| {
                            HarnessError::Transport(
                                "replay Primary activation acknowledgement dropped".into(),
                            )
                        })??;
                    }
                }
                let mut routes = self.lock_routes()?;
                let entry = routes
                    .by_native
                    .get_mut(&route.harness_thread_id)
                    .filter(|entry| entry.route == route)
                    .ok_or_else(|| HarnessError::Protocol("stale replay native route".into()))?;
                if entry.receiver.is_some() {
                    return Err(HarnessError::Protocol(format!(
                        "replay route {} was acknowledged before its receiver was claimed",
                        route.harness_thread_id
                    )));
                }
                entry.ready = true;
            }
        }

        Ok(ThreadHandle {
            // A deterministic replay applies exactly the requested model, so echo it as
            // effective — this is what lets server tests exercise verified provider switches.
            // An import names no model, and a harness answers that from the thread itself, so
            // the fake stands in with its configured one rather than reporting nothing.
            resumed_model: Some(effective_model),
            ..ThreadHandle::opened(
                route.thread_id,
                route.harness_thread_id,
                opts.workspace_root.clone(),
            )
        })
    }

    async fn claim_native_thread(
        &self,
        suggested_thread_id: ThreadId,
        harness_thread_id: String,
        workspace_root: std::path::PathBuf,
    ) -> Result<ThreadHandle, HarnessError> {
        let route = self
            .claim_native_route(harness_thread_id, suggested_thread_id)
            .await?;
        Ok(ThreadHandle::opened(
            route.thread_id,
            route.harness_thread_id,
            workspace_root,
        ))
    }

    fn take_harness_signals(&self) -> Result<HarnessSignalStream, HarnessError> {
        self.ensure_running()?;
        self.signals
            .lock()
            .map_err(|_| HarnessError::Transport("replay signal lock was poisoned".into()))?
            .take()
            .map(HarnessSignalStream::new)
            .ok_or_else(|| {
                HarnessError::Protocol("replay harness signal stream was already taken".into())
            })
    }

    async fn claim_native_route(
        &self,
        harness_thread_id: String,
        suggested_thread_id: ThreadId,
    ) -> Result<ClaimedNativeRoute, HarnessError> {
        self.claim_route_while_running(&harness_thread_id, suggested_thread_id)
    }

    fn claim_event_receiver(
        &self,
        route: &ClaimedNativeRoute,
    ) -> Result<AgentEventStream, HarnessError> {
        let lifecycle = self.lock_lifecycle()?;
        if !matches!(&*lifecycle, ReplayLifecycle::Running { .. }) {
            return Err(HarnessError::Transport(
                "replay harness is shut down".into(),
            ));
        }
        let mut routes = self.lock_routes()?;
        let entry = routes
            .by_native
            .get_mut(&route.harness_thread_id)
            .filter(|entry| entry.route == *route)
            .ok_or_else(|| HarnessError::Protocol("stale or unknown replay route".into()))?;
        let receiver = entry.receiver.take().ok_or_else(|| {
            HarnessError::Protocol(format!(
                "replay route {} event receiver was already claimed",
                route.harness_thread_id
            ))
        })?;
        Ok(AgentEventStream::new(receiver))
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        self.ensure_running()?;
        let turn_id = TurnId::new();

        // Emit all pending events for this thread into its exclusive route.
        let (sender, events) = {
            let mut routes = self.lock_routes()?;
            let native = routes
                .native_by_thread
                .get(&thread.thread)
                .cloned()
                .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
            let state = routes
                .by_native
                .get_mut(&native)
                .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
            (state.sender.clone(), std::mem::take(&mut state.pending))
        };

        // Send events asynchronously.
        self.spawn_producer(async move {
            for event in events {
                let _ = sender.send(event).await;
                tokio::task::yield_now().await;
            }
        })?;

        Ok(turn_id)
    }

    async fn respond_approval(
        &self,
        _req: ApprovalId,
        _decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        self.ensure_running()?;
        Ok(())
    }

    async fn respond_server_request(
        &self,
        _req: ServerRequestId,
        _response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        self.ensure_running()?;
        Ok(())
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.ensure_running()?;
        Ok(())
    }

    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.ensure_running()?;
        let sender = {
            let routes = self.lock_routes()?;
            let native = routes
                .native_by_thread
                .get(&thread.thread)
                .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
            routes
                .by_native
                .get(native)
                .ok_or(HarnessError::ThreadNotFound(thread.thread))?
                .sender
                .clone()
        };
        let thread_id = thread.thread;

        self.spawn_producer(async move {
            let turn = TurnId::new();
            let item_id = ItemId::new();
            let _ = sender
                .send(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                })
                .await;
            tokio::task::yield_now().await;
            let _ = sender
                .send(AgentEvent::ItemCompleted {
                    thread: thread_id,
                    turn,
                    item: Item {
                        id: item_id,
                        harness_item_id: "replay_context_compaction".into(),
                        payload: ItemPayload::Activity {
                            title: "Context compacted".into(),
                            detail: None,
                            metadata: None,
                            subagent: None,
                        },
                        created_at: chrono::Utc::now(),
                    },
                })
                .await;
            tokio::task::yield_now().await;
            let _ = sender
                .send(AgentEvent::TurnCompleted {
                    thread: thread_id,
                    turn,
                    usage: TokenUsage::default(),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                })
                .await;
        })?;
        Ok(())
    }

    async fn set_thread_name(
        &self,
        _thread: &ThreadHandle,
        _name: &str,
    ) -> Result<(), HarnessError> {
        self.ensure_running()?;
        Ok(())
    }

    async fn set_thread_archived(
        &self,
        _thread: &ThreadHandle,
        _archived: bool,
    ) -> Result<(), HarnessError> {
        self.ensure_running()?;
        Ok(())
    }

    async fn delete_thread(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.ensure_running()?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        let producers = {
            let mut lifecycle = self.lock_lifecycle()?;
            match &mut *lifecycle {
                ReplayLifecycle::Running { producers } => {
                    let producers = std::mem::take(producers);
                    *lifecycle = ReplayLifecycle::Stopping;
                    Some(producers)
                }
                ReplayLifecycle::Stopping => None,
                ReplayLifecycle::Stopped => return Ok(()),
            }
        };
        let Some(producers) = producers else {
            loop {
                let completed = self.shutdown_complete.notified();
                if matches!(&*self.lock_lifecycle()?, ReplayLifecycle::Stopped) {
                    return Ok(());
                }
                completed.await;
            }
        };

        self.stop_tx.send_replace(true);
        let signal_result = self
            .signal_tx
            .lock()
            .map(|mut sender| {
                sender.take();
            })
            .map_err(|_| HarnessError::Transport("replay signal lock was poisoned".into()));

        for producer in &producers {
            producer.abort();
        }
        for producer in producers {
            let _ = producer.await;
        }

        let route_result = self.lock_routes().map(|mut routes| {
            routes.by_native.clear();
            routes.native_by_thread.clear();
        });

        *self.lock_lifecycle()? = ReplayLifecycle::Stopped;
        self.shutdown_complete.notify_waiters();
        match (signal_result, route_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(signal_error), Err(route_error)) => Err(HarnessError::Transport(format!(
                "replay shutdown failed to close signal and route authorities: {signal_error}; \
                 {route_error}"
            ))),
        }
    }
}

async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow_and_update() {
        return;
    }
    let _ = stop.changed().await;
}

fn remap_event_thread(event: &mut AgentEvent, thread_id: ThreadId) {
    match event {
        AgentEvent::ThreadOpened { thread, .. }
        | AgentEvent::TurnStarted { thread, .. }
        | AgentEvent::ContextWindowUpdated { thread, .. }
        | AgentEvent::ItemStarted { thread, .. }
        | AgentEvent::ItemDelta { thread, .. }
        | AgentEvent::ItemCompleted { thread, .. }
        | AgentEvent::DiffUpdated { thread, .. }
        | AgentEvent::ApprovalRequested { thread, .. }
        | AgentEvent::ServerRequestReceived { thread, .. }
        | AgentEvent::ServerRequestResolved { thread, .. }
        | AgentEvent::TurnCompleted { thread, .. }
        | AgentEvent::Error { thread, .. }
        | AgentEvent::Notice { thread, .. } => *thread = thread_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::ids::{ItemId, ThreadId, TurnId};
    use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenUsage;
    use giskard_core::turn::{Mode, TurnStatus, TurnStatusKind};
    use giskard_harness::KnownThreadBinding;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    fn make_simple_fixture() -> ReplayFixture {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let now = Utc::now();

        ReplayFixture::from_events(vec![
            AgentEvent::ThreadOpened {
                thread,
                harness_thread_id: "th_test".into(),
            },
            AgentEvent::TurnStarted { thread, turn },
            AgentEvent::ItemStarted {
                thread,
                turn,
                item: ItemStart {
                    id: item_id,
                    harness_item_id: "it_1".into(),
                    kind: ItemKind::AgentMessage,
                    command: None,
                    tool: None,
                },
            },
            AgentEvent::ItemDelta {
                thread,
                turn,
                item_id,
                delta: ItemDelta::Text {
                    text: "Hello!".into(),
                },
            },
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "it_1".into(),
                    payload: ItemPayload::AgentMessage {
                        text: "Hello!".into(),
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

    #[tokio::test]
    async fn replay_basic_turn() {
        let fixture = make_simple_fixture();
        let _thread_id = fixture
            .events
            .iter()
            .find_map(|e| match e {
                AgentEvent::ThreadOpened { thread, .. } => Some(*thread),
                _ => None,
            })
            .unwrap();

        let harness = Arc::new(ReplayHarness::from_fixture(fixture));

        let handle = harness
            .open_thread(giskard_harness::OpenThreadOptions {
                project: giskard_core::ProjectId::new(),
                thread: None,
                workspace_root: "/tmp".into(),
                resume: Some("th_test".into()),
                identity_generation: None,
                updates: giskard_harness::thread_update_channel().0,
                initial_model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                }),
            })
            .await
            .unwrap();

        // Claim the route and its event receiver before starting the turn.
        let route = harness
            .claim_native_route(handle.harness_thread_id.clone(), handle.thread)
            .await
            .expect("replay route should be claimable");
        let mut stream = harness
            .claim_event_receiver(&route)
            .expect("replay event receiver should be claimable");

        let _turn_id = harness
            .start_turn(
                &handle,
                UserInput::text("test"),
                TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: giskard_core::turn::PermissionPreset::AutoApprove,
                },
            )
            .await
            .unwrap();

        // Collect events.
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            let is_completed = matches!(event, AgentEvent::TurnCompleted { .. });
            events.push(event);
            if is_completed {
                break;
            }
        }

        assert_eq!(events.len(), 6);
        assert!(matches!(events[0], AgentEvent::ThreadOpened { .. }));
        assert!(matches!(
            events[5],
            AgentEvent::TurnCompleted { ref status, .. } if status.kind == TurnStatusKind::Completed
        ));

        // Verify token usage.
        if let AgentEvent::TurnCompleted { usage, .. } = &events[5] {
            assert_eq!(usage.input, 100);
            assert_eq!(usage.output, 50);
            assert_eq!(usage.total, 150);
        }
    }

    #[tokio::test]
    async fn replay_resume_remaps_fixture_events_to_requested_thread() {
        let fixture = make_simple_fixture();
        let requested_thread = ThreadId::new();
        let harness = Arc::new(ReplayHarness::from_fixture(fixture));

        let handle = harness
            .open_thread(giskard_harness::OpenThreadOptions {
                project: giskard_core::ProjectId::new(),
                thread: Some(requested_thread),
                workspace_root: "/tmp".into(),
                resume: Some("th_test".into()),
                identity_generation: None,
                updates: giskard_harness::thread_update_channel().0,
                initial_model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                }),
            })
            .await
            .unwrap();
        assert_eq!(handle.thread, requested_thread);

        let route = harness
            .claim_native_route(handle.harness_thread_id.clone(), handle.thread)
            .await
            .expect("replay route should be claimable");
        let mut stream = harness
            .claim_event_receiver(&route)
            .expect("replay event receiver should be claimable");
        harness
            .start_turn(
                &handle,
                UserInput::text("test"),
                TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: giskard_core::turn::PermissionPreset::AutoApprove,
                },
            )
            .await
            .unwrap();

        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            let is_completed = matches!(event, AgentEvent::TurnCompleted { .. });
            events.push(event);
            if is_completed {
                break;
            }
        }

        assert_eq!(events.len(), 6);
        for event in events {
            assert_eq!(event_thread(&event), requested_thread);
        }
    }

    #[tokio::test]
    async fn replay_shutdown_idempotent() {
        let harness = ReplayHarness::new();
        harness.shutdown().await.unwrap();
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replay_shutdown_closes_signal_and_event_streams_after_buffered_events() {
        let thread = ThreadId::new();
        let native = "native-shutdown";
        let mut fixture_events = vec![AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: native.into(),
        }];
        fixture_events.extend((0..ROUTE_CAPACITY).map(|sequence| AgentEvent::Notice {
            thread,
            turn: None,
            message: sequence.to_string(),
        }));
        let harness = Arc::new(ReplayHarness::from_fixture(ReplayFixture::from_events(
            fixture_events,
        )));
        let mut signals = harness.take_harness_signals().unwrap();
        let handle = harness
            .open_thread(OpenThreadOptions {
                project: giskard_core::ProjectId::new(),
                thread: Some(thread),
                workspace_root: "/tmp".into(),
                resume: Some(native.into()),
                identity_generation: None,
                updates: giskard_harness::thread_update_channel().0,
                initial_model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                }),
            })
            .await
            .unwrap();
        let route = harness
            .claim_native_route(native.into(), thread)
            .await
            .unwrap();
        let mut events = harness.claim_event_receiver(&route).unwrap();
        harness
            .start_turn(
                &handle,
                UserInput::text("fill route"),
                TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: giskard_core::turn::PermissionPreset::AutoApprove,
                },
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                let capacity = harness
                    .lock_routes()
                    .expect("shutdown route lock should remain available")
                    .by_native
                    .get(native)
                    .expect("shutdown route should exist")
                    .sender
                    .capacity();
                if capacity == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replay producer should fill the bounded route");

        let first_harness = harness.clone();
        let first_shutdown = tokio::spawn(async move { first_harness.shutdown().await });
        let second_harness = harness.clone();
        let second_shutdown = tokio::spawn(async move { second_harness.shutdown().await });
        timeout(Duration::from_secs(1), async {
            first_shutdown.await.unwrap().unwrap();
            second_shutdown.await.unwrap().unwrap();
        })
        .await
        .expect("concurrent shutdowns should share producer teardown");
        assert!(signals.recv().await.is_none());

        let mut drained = 0;
        while events.recv().await.is_some() {
            drained += 1;
        }
        assert_eq!(drained, ROUTE_CAPACITY);
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replay_rejects_new_routes_after_shutdown() {
        let harness = ReplayHarness::new();
        harness.shutdown().await.unwrap();

        assert!(matches!(
            harness
                .claim_native_route("native-after-shutdown".into(), ThreadId::new())
                .await,
            Err(HarnessError::Transport(message)) if message.contains("shut down")
        ));
    }

    #[test]
    fn replay_signal_lock_poisoning_is_a_typed_error() {
        let harness = Arc::new(ReplayHarness::new());
        let poisoning_harness = harness.clone();
        let poisoning = std::thread::spawn(move || {
            let _guard = poisoning_harness
                .signals
                .lock()
                .expect("fresh replay signal lock should be available");
            panic!("poison replay signal lock");
        });
        assert!(poisoning.join().is_err());

        assert!(matches!(
            harness.take_harness_signals(),
            Err(HarnessError::Transport(message))
                if message == "replay signal lock was poisoned"
        ));
    }

    #[tokio::test]
    async fn replay_route_lock_poisoning_does_not_strand_shutdown_waiters() {
        let harness = Arc::new(ReplayHarness::new());
        let mut signals = harness.take_harness_signals().unwrap();
        let poisoning_harness = harness.clone();
        let poisoning = std::thread::spawn(move || {
            let _guard = poisoning_harness
                .routes
                .lock()
                .expect("fresh replay route lock should be available");
            panic!("poison replay route lock");
        });
        assert!(poisoning.join().is_err());

        assert!(matches!(
            harness.shutdown().await,
            Err(HarnessError::Transport(message))
                if message == "replay route lock was poisoned"
        ));
        assert!(signals.recv().await.is_none());
        harness
            .shutdown()
            .await
            .expect("completed replay shutdown should remain idempotent");
        assert!(matches!(
            harness
                .claim_native_route("native-after-poison".into(), ThreadId::new())
                .await,
            Err(HarnessError::Transport(message)) if message.contains("shut down")
        ));
    }

    #[tokio::test]
    async fn replay_capabilities() {
        let harness = ReplayHarness::new();
        let caps = harness.capabilities();
        assert!(caps.live_approvals);
        assert!(caps.plan_build_modes);
        assert!(caps.token_usage);
    }

    #[tokio::test]
    async fn bootstrap_routes_are_authoritative_and_monotonic() {
        let first = ThreadId::new();
        let second = ThreadId::new();
        let harness = ReplayHarness::new_with_bootstrap(HarnessBootstrap {
            known_threads: vec![
                KnownThreadBinding {
                    harness_thread_id: "native-first".into(),
                    thread_id: first,
                },
                KnownThreadBinding {
                    harness_thread_id: "native-second".into(),
                    thread_id: second,
                },
            ],
        })
        .unwrap();

        let first_route = harness
            .claim_native_route("native-first".into(), ThreadId::new())
            .await
            .unwrap();
        let second_route = harness
            .claim_native_route("native-second".into(), second)
            .await
            .unwrap();
        assert_eq!(first_route.thread_id, first);
        assert_eq!(first_route.route_epoch, 1);
        assert_eq!(second_route.route_epoch, 2);

        let conflict = ReplayHarness::new_with_bootstrap(HarnessBootstrap {
            known_threads: vec![
                KnownThreadBinding {
                    harness_thread_id: "native-conflict".into(),
                    thread_id: ThreadId::new(),
                },
                KnownThreadBinding {
                    harness_thread_id: "native-conflict".into(),
                    thread_id: ThreadId::new(),
                },
            ],
        });
        assert!(matches!(conflict, Err(HarnessError::Protocol(_))));
    }

    #[tokio::test]
    async fn event_receiver_can_only_be_claimed_once() {
        let harness = ReplayHarness::new();
        let route = harness
            .claim_native_route("native-exclusive".into(), ThreadId::new())
            .await
            .unwrap();

        let _receiver = harness.claim_event_receiver(&route).unwrap();
        assert!(matches!(
            harness.claim_event_receiver(&route),
            Err(HarnessError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn native_thread_claim_converges_on_an_authoritative_route() {
        let harness = ReplayHarness::new();
        let authoritative = ThreadId::new();
        let suggestion = ThreadId::new();
        let route = harness
            .claim_native_route("native-converged".into(), authoritative)
            .await
            .unwrap();

        let handle = harness
            .claim_native_thread(
                suggestion,
                "native-converged".into(),
                "/tmp/converged".into(),
            )
            .await
            .unwrap();
        let repeated = harness
            .claim_native_route("native-converged".into(), suggestion)
            .await
            .unwrap();

        assert_eq!(handle.thread, authoritative);
        assert_eq!(handle.harness_thread_id, "native-converged");
        assert_eq!(handle.workspace_root, Path::new("/tmp/converged"));
        assert_eq!(repeated, route);
    }

    #[tokio::test]
    async fn native_thread_claim_rejects_a_local_identity_bound_elsewhere() {
        let harness = ReplayHarness::new();
        let bound = ThreadId::new();
        harness
            .claim_native_route("native-first".into(), bound)
            .await
            .unwrap();

        assert!(matches!(
            harness
                .claim_native_thread(bound, "native-conflict".into(), "/tmp".into())
                .await,
            Err(HarnessError::Protocol(message)) if message.contains("already bound")
        ));
    }

    #[tokio::test]
    async fn primary_open_waits_for_receiver_claim_and_live_acknowledgement() {
        let harness = Arc::new(ReplayHarness::new());
        let mut signals = harness.take_harness_signals().unwrap();
        assert!(harness.take_harness_signals().is_err());
        let intended_thread = ThreadId::new();
        let opening_harness = harness.clone();
        let mut opening = tokio::spawn(async move {
            opening_harness
                .open_thread(OpenThreadOptions {
                    project: giskard_core::ProjectId::new(),
                    thread: Some(intended_thread),
                    workspace_root: "/tmp".into(),
                    resume: None,
                    identity_generation: Some(7),
                    updates: giskard_harness::thread_update_channel().0,
                    initial_model: Some(ModelRef {
                        provider: "openai".into(),
                        model: "gpt-5.5".into(),
                        reasoning_effort: None,
                    }),
                })
                .await
        });

        let signal = timeout(Duration::from_secs(1), signals.recv())
            .await
            .expect("Primary replay open should request activation")
            .expect("replay signal stream should remain open");
        let activation = match signal {
            HarnessSignal::Activate(activation) => activation,
            HarnessSignal::PrimaryIdentityFailed { error, .. } => {
                panic!("expected Primary activation, got identity failure: {error}")
            }
        };
        assert_eq!(activation.route.thread_id, intended_thread);
        assert!(matches!(
            activation.cause,
            ThreadActivationCause::IdentityResponse { generation: 7, .. }
        ));
        assert!(
            timeout(Duration::from_millis(10), &mut opening)
                .await
                .is_err()
        );

        let _receiver = harness
            .claim_event_receiver(&activation.route)
            .expect("activation route receiver should be exclusively claimable");
        activation.readiness.acknowledge(Ok(()));
        let handle = timeout(Duration::from_secs(1), opening)
            .await
            .expect("Live acknowledgement should release replay open")
            .unwrap()
            .unwrap();
        assert_eq!(handle.thread, intended_thread);
    }

    #[tokio::test]
    async fn fixture_save_load_roundtrip() {
        let fixture = make_simple_fixture();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        fixture.save(tmp.path()).unwrap();
        let loaded = ReplayFixture::load(tmp.path()).unwrap();
        assert_eq!(loaded.events.len(), fixture.events.len());
    }

    fn event_thread(event: &AgentEvent) -> ThreadId {
        match event {
            AgentEvent::ThreadOpened { thread, .. }
            | AgentEvent::TurnStarted { thread, .. }
            | AgentEvent::ContextWindowUpdated { thread, .. }
            | AgentEvent::ItemStarted { thread, .. }
            | AgentEvent::ItemDelta { thread, .. }
            | AgentEvent::ItemCompleted { thread, .. }
            | AgentEvent::DiffUpdated { thread, .. }
            | AgentEvent::ApprovalRequested { thread, .. }
            | AgentEvent::ServerRequestReceived { thread, .. }
            | AgentEvent::ServerRequestResolved { thread, .. }
            | AgentEvent::TurnCompleted { thread, .. }
            | AgentEvent::Error { thread, .. }
            | AgentEvent::Notice { thread, .. } => *thread,
        }
    }
}
