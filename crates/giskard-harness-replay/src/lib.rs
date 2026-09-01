//! Deterministic replay harness for testing (spec §14.2).
//!
//! Reads a recorded fixture (JSONL of `AgentEvent`s) and replays them through the
//! `AgentHarness` trait with deterministic timing. No real LLM, no network.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
use giskard_core::mcp::McpServerStatus;
use giskard_core::model::ModelDescriptor;
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::token::TokenUsage;
use giskard_core::turn::TurnOverrides;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, HarnessProvider, OpenThreadOptions,
    ThreadAttachment, ThreadDeletion, ThreadHandle,
};

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
    harness_thread_id: String,
    activation: u64,
    sender: Option<broadcast::Sender<AgentEvent>>,
    receiver: Option<AgentEventStream>,
    phase: RoutePhase,
    pending: Vec<AgentEvent>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RoutePhase {
    Idle,
    Attaching,
    Owned,
    Tombstoned,
}

struct PreloadedFixture {
    events: Vec<AgentEvent>,
}

/// Replay-owned native thread identity used to consume one matching fixture on resume.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ReplayNativeThreadId(String);

impl ReplayNativeThreadId {
    fn new(value: String) -> Self {
        Self(value)
    }
}

impl Borrow<str> for ReplayNativeThreadId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// A harness that replays recorded events deterministically.
pub struct ReplayHarness {
    capabilities: HarnessCapabilities,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Route deterministic replay events to harness threads opened by the server.
    // Source of truth: Replay harness open/resume operations establish each thread state.
    // Structural reason: This non-test-gated harness adapter cannot depend on server authorities.
    // Synchronization: The mutex protects linear lookup, insertion, and removal.
    // Invalidation/removal: Delete tombstones delivery; harness drop removes all entries.
    threads: Arc<StdMutex<Vec<(ThreadId, ThreadState)>>>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Own preloaded replay fixtures until one matching native identity consumes them.
    // Source of truth: Replay fixture construction and explicit resume selection.
    // Structural reason: Fixture identity is harness-native and has no server authority owner.
    // Synchronization: The mutex serializes lookup and one-time fixture removal.
    // Invalidation/removal: Successful resume consumes an entry; harness drop removes the rest.
    fixtures: Mutex<HashMap<ReplayNativeThreadId, PreloadedFixture>>,
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
    shutdown_called: AtomicBool,
    next_activation: AtomicU64,
}

impl ReplayHarness {
    /// Create a new replay harness with full Codex-like capabilities.
    pub fn new() -> Self {
        Self::with_fixtures(HashMap::new())
    }

    fn with_fixtures(fixtures: HashMap<ReplayNativeThreadId, PreloadedFixture>) -> Self {
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
            threads: Arc::new(StdMutex::new(Vec::new())),
            fixtures: Mutex::new(fixtures),
            models: Vec::new(),
            models_error: None,
            providers: Vec::new(),
            providers_error: None,
            client_version: None,
            shutdown_called: AtomicBool::new(false),
            next_activation: AtomicU64::new(1),
        }
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

    /// Load a fixture and create a harness pre-loaded with those events
    /// for a single thread.
    pub fn from_fixture(fixture: ReplayFixture) -> Self {
        let harness_thread_id = fixture
            .events
            .iter()
            .find_map(|e| match e {
                AgentEvent::ThreadOpened {
                    harness_thread_id, ..
                } => Some(harness_thread_id.clone()),
                _ => None,
            })
            .unwrap_or_else(|| format!("replay_{}", ThreadId::new()));

        let mut fixtures = HashMap::new();
        fixtures.insert(
            ReplayNativeThreadId::new(harness_thread_id),
            PreloadedFixture {
                events: fixture.events,
            },
        );
        Self::with_fixtures(fixtures)
    }

    fn route_lock(
        routes: &Arc<StdMutex<Vec<(ThreadId, ThreadState)>>>,
    ) -> Result<std::sync::MutexGuard<'_, Vec<(ThreadId, ThreadState)>>, HarnessError> {
        match routes.lock() {
            Ok(routes) => Ok(routes),
            Err(poisoned) => {
                Self::close_poisoned_routes(poisoned);
                Err(HarnessError::Transport(
                    "replay route authority lock poisoned; authority closed".into(),
                ))
            }
        }
    }

    /// A capability drop cannot report an error. Close all delivery state while preserving the
    /// poisoned mutex so the next fallible harness operation surfaces the fatal authority error.
    fn close_poisoned_routes(
        poisoned: std::sync::PoisonError<std::sync::MutexGuard<'_, Vec<(ThreadId, ThreadState)>>>,
    ) {
        poisoned.into_inner().clear();
    }

    fn attachment(
        &self,
        handle: ThreadHandle,
        stream: AgentEventStream,
        activation: u64,
    ) -> ThreadAttachment {
        let routes_for_commit = Arc::downgrade(&self.threads);
        let routes_for_attachment_drop = Arc::downgrade(&self.threads);
        let thread_id = handle.thread;
        ThreadAttachment::from_route(
            handle,
            stream,
            move || {
                let Some(route_authority) = routes_for_commit.upgrade() else {
                    return Err(HarnessError::Protocol(
                        "replay harness route authority closed".into(),
                    ));
                };
                let mut routes = match ReplayHarness::route_lock(&route_authority) {
                    Ok(routes) => routes,
                    Err(error) => return Err(error),
                };
                let Some((_, state)) = routes.iter_mut().find(|(id, state)| {
                    *id == thread_id
                        && state.activation == activation
                        && state.phase == RoutePhase::Attaching
                }) else {
                    return Err(HarnessError::Protocol(format!(
                        "replay thread {thread_id} attachment is stale"
                    )));
                };
                state.phase = RoutePhase::Owned;
                drop(routes);

                let routes_for_owner_drop = Arc::downgrade(&route_authority);
                Ok(Box::new(move |stream| {
                    let Some(routes) = routes_for_owner_drop.upgrade() else {
                        return;
                    };
                    let mut routes = match routes.lock() {
                        Ok(routes) => routes,
                        Err(poisoned) => {
                            ReplayHarness::close_poisoned_routes(poisoned);
                            return;
                        }
                    };
                    if let Some((_, state)) = routes.iter_mut().find(|(id, state)| {
                        *id == thread_id
                            && state.activation == activation
                            && state.phase == RoutePhase::Owned
                    }) {
                        state.receiver = Some(stream);
                        state.phase = RoutePhase::Idle;
                    }
                })
                    as Box<dyn FnOnce(AgentEventStream) + Send>)
            },
            move |stream| {
                let Some(routes) = routes_for_attachment_drop.upgrade() else {
                    return;
                };
                let mut routes = match routes.lock() {
                    Ok(routes) => routes,
                    Err(poisoned) => {
                        ReplayHarness::close_poisoned_routes(poisoned);
                        return;
                    }
                };
                if let Some((_, state)) = routes.iter_mut().find(|(id, state)| {
                    *id == thread_id
                        && state.activation == activation
                        && state.phase == RoutePhase::Attaching
                }) {
                    state.receiver = Some(stream);
                    state.phase = RoutePhase::Idle;
                }
            },
        )
    }

    fn claim_route(
        &self,
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
        reactivate_tombstone: bool,
    ) -> Result<ThreadAttachment, HarnessError> {
        if self.shutdown_called.load(Ordering::SeqCst) {
            return Err(HarnessError::Transport(
                "replay harness is shut down".into(),
            ));
        }
        let mut routes = Self::route_lock(&self.threads)?;
        if let Some((_, state)) = routes.iter().find(|(bound_thread, state)| {
            *bound_thread == thread && state.harness_thread_id != harness_thread_id
        }) {
            return Err(HarnessError::Protocol(format!(
                "replay thread {thread} is already bound to native thread {}",
                state.harness_thread_id
            )));
        }
        let authoritative_thread = routes
            .iter()
            .find(|(_, state)| state.harness_thread_id == harness_thread_id)
            .map(|(bound_thread, state)| (*bound_thread, state.phase));
        let thread = match authoritative_thread {
            Some((bound_thread, RoutePhase::Tombstoned)) if bound_thread != thread => {
                return Err(HarnessError::Protocol(format!(
                    "native replay thread {harness_thread_id} is tombstoned for {bound_thread}, not {thread}"
                )));
            }
            Some((bound_thread, _)) => bound_thread,
            None => thread,
        };

        let (handle, stream, activation) =
            if let Some((_, state)) = routes.iter_mut().find(|(id, _)| *id == thread) {
                match state.phase {
                    RoutePhase::Idle => {
                        let Some(stream) = state.receiver.take() else {
                            return Err(HarnessError::Protocol(format!(
                                "idle replay thread {thread} lost its retained receiver"
                            )));
                        };
                        state.phase = RoutePhase::Attaching;
                        (
                            ThreadHandle::opened(thread, harness_thread_id, workspace_root),
                            stream,
                            state.activation,
                        )
                    }
                    RoutePhase::Tombstoned if reactivate_tombstone => {
                        let activation = self.next_activation.fetch_add(1, Ordering::Relaxed);
                        let (sender, receiver) = broadcast::channel(256);
                        state.sender = Some(sender);
                        state.receiver = None;
                        state.phase = RoutePhase::Attaching;
                        state.activation = activation;
                        (
                            ThreadHandle::opened(thread, harness_thread_id, workspace_root),
                            AgentEventStream::new(receiver),
                            activation,
                        )
                    }
                    RoutePhase::Tombstoned => {
                        return Err(HarnessError::Protocol(format!(
                            "replay thread {thread} route is tombstoned"
                        )));
                    }
                    RoutePhase::Attaching | RoutePhase::Owned => {
                        return Err(HarnessError::Protocol(format!(
                            "replay thread {thread} already has an event owner"
                        )));
                    }
                }
            } else {
                let activation = self.next_activation.fetch_add(1, Ordering::Relaxed);
                let (sender, receiver) = broadcast::channel(256);
                routes.push((
                    thread,
                    ThreadState {
                        harness_thread_id: harness_thread_id.clone(),
                        activation,
                        sender: Some(sender),
                        receiver: None,
                        phase: RoutePhase::Attaching,
                        pending: Vec::new(),
                    },
                ));
                (
                    ThreadHandle::opened(thread, harness_thread_id, workspace_root),
                    AgentEventStream::new(receiver),
                    activation,
                )
            };
        drop(routes);
        Ok(self.attachment(handle, stream, activation))
    }
}

impl Default for ReplayHarness {
    fn default() -> Self {
        Self::new()
    }
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
        match &self.models_error {
            Some(message) => Err(HarnessError::Transport(message.clone())),
            None => Ok(self.models.clone()),
        }
    }

    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        match &self.providers_error {
            Some(message) => Err(HarnessError::Transport(message.clone())),
            None => Ok(self.providers.clone()),
        }
    }

    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError> {
        Ok(vec![])
    }

    async fn reload_mcp_servers(&self) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadAttachment, HarnessError> {
        if self.shutdown_called.load(Ordering::SeqCst) {
            return Err(HarnessError::Transport(
                "replay harness is shut down".into(),
            ));
        }
        let harness_thread_id = opts
            .resume
            .clone()
            .unwrap_or_else(|| format!("replay_{}", ThreadId::new()));

        let (thread_id, mut pending) = if let Some(resume) = &opts.resume {
            let mut fixtures = self.fixtures.lock().await;
            if let Some(fixture) = fixtures.remove(resume.as_str()) {
                (opts.thread, fixture.events)
            } else {
                (opts.thread, Vec::new())
            }
        } else {
            (opts.thread, Vec::new())
        };
        for event in &mut pending {
            remap_event_thread(event, thread_id);
        }

        let (tx, rx) = broadcast::channel(256);
        let activation = self.next_activation.fetch_add(1, Ordering::Relaxed);
        let mut threads = Self::route_lock(&self.threads)?;
        if let Some((bound_thread, _)) = threads.iter().find(|(bound_thread, state)| {
            *bound_thread != thread_id && state.harness_thread_id == harness_thread_id
        }) {
            return Err(HarnessError::Protocol(format!(
                "native replay thread {harness_thread_id} is already bound to {bound_thread}"
            )));
        }
        if let Some((_, state)) = threads.iter().find(|(bound_thread, state)| {
            *bound_thread == thread_id && state.harness_thread_id != harness_thread_id
        }) {
            return Err(HarnessError::Protocol(format!(
                "replay thread {thread_id} is already bound to native thread {}",
                state.harness_thread_id
            )));
        }
        if threads.iter().any(|(id, state)| {
            *id == thread_id && state.phase != RoutePhase::Tombstoned
                || state.harness_thread_id == harness_thread_id
                    && state.phase != RoutePhase::Tombstoned
        }) {
            return Err(HarnessError::Protocol(format!(
                "replay route for thread {thread_id} or native thread {harness_thread_id} already exists"
            )));
        }
        threads.retain(|(id, state)| {
            !(*id == thread_id
                && state.harness_thread_id == harness_thread_id
                && state.phase == RoutePhase::Tombstoned)
        });
        threads.push((
            thread_id,
            ThreadState {
                harness_thread_id: harness_thread_id.clone(),
                activation,
                sender: Some(tx),
                receiver: None,
                phase: RoutePhase::Attaching,
                pending,
            },
        ));
        drop(threads);
        let handle = ThreadHandle {
            // A deterministic replay applies exactly the requested model, so echo it as
            // effective — this is what lets server tests exercise verified provider switches.
            resumed_model: Some(opts.initial_model.clone()),
            ..ThreadHandle::opened(thread_id, harness_thread_id, opts.workspace_root.clone())
        };
        Ok(self.attachment(handle, AgentEventStream::new(rx), activation))
    }

    async fn claim_native_thread(
        &self,
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
    ) -> Result<ThreadAttachment, HarnessError> {
        self.claim_route(thread, harness_thread_id, workspace_root, false)
    }

    async fn reattach_native_thread(
        &self,
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
    ) -> Result<ThreadAttachment, HarnessError> {
        self.claim_route(thread, harness_thread_id, workspace_root, true)
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn_id = TurnId::new();

        // Emit all pending events for this thread into the broadcast channel.
        let mut threads = Self::route_lock(&self.threads)?;
        if let Some((_, state)) = threads.iter_mut().find(|(id, _)| *id == thread.thread) {
            let Some(sender) = state.sender.clone() else {
                return Err(HarnessError::ThreadNotFound(thread.thread));
            };
            let events = std::mem::take(&mut state.pending);
            drop(threads);

            // Send events asynchronously.
            tokio::spawn(async move {
                for event in events {
                    let _ = sender.send(event);
                    tokio::task::yield_now().await;
                }
            });
        }

        Ok(turn_id)
    }

    async fn respond_approval(
        &self,
        _req: ApprovalId,
        _decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn respond_server_request(
        &self,
        _req: ServerRequestId,
        _response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let mut threads = Self::route_lock(&self.threads)?;
        let Some((_, state)) = threads.iter_mut().find(|(id, _)| *id == thread.thread) else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        let Some(sender) = state.sender.clone() else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        let thread_id = thread.thread;
        drop(threads);

        tokio::spawn(async move {
            let turn = TurnId::new();
            let item_id = ItemId::new();
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ItemCompleted {
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
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            });
        });
        Ok(())
    }

    async fn set_thread_name(
        &self,
        _thread: &ThreadHandle,
        _name: &str,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn set_thread_archived(
        &self,
        _thread: &ThreadHandle,
        _archived: bool,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn begin_delete_thread<'a>(
        &'a self,
        thread: &'a ThreadHandle,
    ) -> Result<giskard_harness::ThreadRetirement<'a>, HarnessError> {
        let mut threads = Self::route_lock(&self.threads)?;
        let Some((_, state)) = threads.iter_mut().find(|(id, state)| {
            *id == thread.thread && state.harness_thread_id == thread.harness_thread_id
        }) else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        state.sender = None;
        state.receiver = None;
        state.pending.clear();
        state.phase = RoutePhase::Tombstoned;
        Ok(giskard_harness::ThreadRetirement::new(Box::pin(async {
            Ok(ThreadDeletion::Retired)
        })))
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        if self.shutdown_called.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut threads = Self::route_lock(&self.threads)?;
        for (_, state) in threads.iter_mut() {
            state.sender = None;
            state.receiver = None;
            state.pending.clear();
            state.phase = RoutePhase::Tombstoned;
        }
        Ok(())
    }
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
    use std::sync::Arc;

    async fn delete_test_thread(
        harness: &ReplayHarness,
        handle: &ThreadHandle,
    ) -> Result<ThreadDeletion, HarnessError> {
        harness.begin_delete_thread(handle).await?.finish().await
    }

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

        let attachment = harness
            .open_thread(giskard_harness::OpenThreadOptions {
                project: giskard_core::ProjectId::new(),
                thread: _thread_id,
                workspace_root: "/tmp".into(),
                resume: Some("th_test".into()),
                updates: giskard_harness::thread_update_channel().0,
                initial_model: ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                },
            })
            .await
            .unwrap();
        let handle = attachment.handle().clone();
        let mut owner = attachment.commit().unwrap();

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
        while let Ok(event) = owner.recv().await {
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

        let attachment = harness
            .open_thread(giskard_harness::OpenThreadOptions {
                project: giskard_core::ProjectId::new(),
                thread: requested_thread,
                workspace_root: "/tmp".into(),
                resume: Some("th_test".into()),
                updates: giskard_harness::thread_update_channel().0,
                initial_model: ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                },
            })
            .await
            .unwrap();
        let handle = attachment.handle().clone();
        assert_eq!(handle.thread, requested_thread);
        let mut owner = attachment.commit().unwrap();
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
        while let Ok(event) = owner.recv().await {
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

    fn open_options(thread: ThreadId) -> OpenThreadOptions {
        OpenThreadOptions {
            project: giskard_core::ProjectId::new(),
            thread,
            workspace_root: "/tmp".into(),
            resume: None,
            updates: giskard_harness::thread_update_channel().0,
            initial_model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        }
    }

    #[tokio::test]
    async fn dropped_attachment_restores_the_exact_buffered_receiver() {
        let harness = ReplayHarness::new();
        let thread = ThreadId::new();
        let attachment = harness.open_thread(open_options(thread)).await.unwrap();
        let handle = attachment.handle().clone();
        let sender = {
            let routes = ReplayHarness::route_lock(&harness.threads).unwrap();
            routes
                .iter()
                .find(|(id, _)| *id == thread)
                .and_then(|(_, state)| state.sender.clone())
                .unwrap()
        };
        sender
            .send(AgentEvent::ThreadOpened {
                thread,
                harness_thread_id: handle.harness_thread_id.clone(),
            })
            .unwrap();

        drop(attachment);
        let reattached = harness
            .claim_native_thread(
                thread,
                handle.harness_thread_id.clone(),
                handle.workspace_root.clone(),
            )
            .await
            .unwrap();
        let mut owner = reattached.commit().unwrap();
        assert!(matches!(
            owner.recv().await.unwrap(),
            AgentEvent::ThreadOpened { thread: event_thread, .. } if event_thread == thread
        ));
    }

    #[tokio::test]
    async fn repeated_native_claim_converges_when_proposed_thread_is_unbound() {
        let harness = ReplayHarness::new();
        let authoritative = ThreadId::new();
        let first = harness
            .open_thread(open_options(authoritative))
            .await
            .unwrap();
        let native = first.handle().harness_thread_id.clone();
        drop(first);

        let converged = harness
            .claim_native_thread(ThreadId::new(), native.clone(), "/tmp".into())
            .await
            .unwrap();
        assert_eq!(converged.handle().thread, authoritative);
        assert_eq!(converged.handle().harness_thread_id, native);
    }

    #[tokio::test]
    async fn attaching_and_owned_replay_routes_allow_only_one_owner() {
        let harness = ReplayHarness::new();
        let thread = ThreadId::new();
        let attachment = harness.open_thread(open_options(thread)).await.unwrap();
        let handle = attachment.handle().clone();
        assert!(
            harness
                .claim_native_thread(
                    ThreadId::new(),
                    handle.harness_thread_id.clone(),
                    "/tmp".into(),
                )
                .await
                .is_err()
        );
        let owner = attachment.commit().unwrap();
        assert!(
            harness
                .claim_native_thread(
                    ThreadId::new(),
                    handle.harness_thread_id.clone(),
                    "/tmp".into(),
                )
                .await
                .is_err()
        );
        drop(owner);
        assert!(
            harness
                .claim_native_thread(ThreadId::new(), handle.harness_thread_id, "/tmp".into())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn explicit_open_cannot_rebind_a_tombstoned_native_identity() {
        let harness = ReplayHarness::new();
        let original = ThreadId::new();
        let attachment = harness.open_thread(open_options(original)).await.unwrap();
        let handle = attachment.handle().clone();
        delete_test_thread(&harness, &handle).await.unwrap();
        drop(attachment);

        let mut mismatched = open_options(ThreadId::new());
        mismatched.resume = Some(handle.harness_thread_id.clone());
        assert!(harness.open_thread(mismatched).await.is_err());

        let mut exact = open_options(original);
        exact.resume = Some(handle.harness_thread_id);
        assert!(harness.open_thread(exact).await.is_ok());
    }

    #[tokio::test]
    async fn delete_closes_delivery_and_late_owner_drop_cannot_reactivate_it() {
        let harness = ReplayHarness::new();
        let thread = ThreadId::new();
        let attachment = harness.open_thread(open_options(thread)).await.unwrap();
        let handle = attachment.handle().clone();
        let mut owner = attachment.commit().unwrap();

        assert!(matches!(
            delete_test_thread(&harness, &handle).await.unwrap(),
            ThreadDeletion::Retired
        ));
        assert!(matches!(
            owner.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        drop(owner);
        let error = harness
            .claim_native_thread(thread, handle.harness_thread_id, handle.workspace_root)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("tombstoned"), "{error}");
    }

    #[tokio::test]
    async fn shutdown_closes_delivery_and_late_attachment_drop_is_inert() {
        let harness = ReplayHarness::new();
        let thread = ThreadId::new();
        let attachment = harness.open_thread(open_options(thread)).await.unwrap();
        let handle = attachment.handle().clone();

        harness.shutdown().await.unwrap();
        drop(attachment);
        let error = harness
            .claim_native_thread(thread, handle.harness_thread_id, handle.workspace_root)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("shut down"), "{error}");
    }

    #[tokio::test]
    async fn poisoned_route_drop_closes_state_and_next_operation_reports_fatal_error() {
        let harness = ReplayHarness::new();
        let thread = ThreadId::new();
        let attachment = harness.open_thread(open_options(thread)).await.unwrap();
        let authority = harness.threads.clone();
        let poisoner = std::thread::spawn(move || {
            let _guard = authority.lock().unwrap();
            panic!("poison replay route authority");
        });
        assert!(poisoner.join().is_err());

        drop(attachment);
        {
            let routes = match harness.threads.lock() {
                Ok(_) => panic!("replay authority should remain poisoned"),
                Err(poisoned) => poisoned.into_inner(),
            };
            assert!(routes.is_empty());
        }
        let error = harness
            .open_thread(open_options(ThreadId::new()))
            .await
            .unwrap_err();
        assert!(matches!(error, HarnessError::Transport(_)), "{error}");
        assert!(error.to_string().contains("lock poisoned"), "{error}");
    }

    #[tokio::test]
    async fn poisoned_route_operation_closes_live_delivery_before_returning_error() {
        let harness = ReplayHarness::new();
        let attachment = harness
            .open_thread(open_options(ThreadId::new()))
            .await
            .unwrap();
        let mut owner = attachment.commit().unwrap();
        let authority = harness.threads.clone();
        let poisoner = std::thread::spawn(move || {
            let _guard = authority.lock().unwrap();
            panic!("poison replay route authority");
        });
        assert!(poisoner.join().is_err());

        let error = harness
            .open_thread(open_options(ThreadId::new()))
            .await
            .unwrap_err();
        assert!(matches!(error, HarnessError::Transport(_)), "{error}");
        assert!(error.to_string().contains("authority closed"), "{error}");
        assert!(matches!(
            owner.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        let routes = match harness.threads.lock() {
            Ok(_) => panic!("replay authority should remain poisoned"),
            Err(poisoned) => poisoned.into_inner(),
        };
        assert!(routes.is_empty());
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
