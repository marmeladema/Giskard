//! Codex CLI harness adapter (spec §4.6).
//!
//! Uses `codex-codes` protocol types with a single-reader, bounded JSON-RPC transport and
//! implements the `AgentHarness` trait.
//! All Codex-specific types are confined to this crate and mapped to
//! `giskard-core` types at the boundary.
//!
//! See the crate README for Codex-native identifier scopes, item and process
//! lifecycles, background-command ownership, and termination routing.

mod log_fields;
mod mapping;
mod transport;

use crate::log_fields::display_opt;

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::RngCore;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::mcp::{
    McpAuthStatus, McpOauthStart, McpResource, McpResourceTemplate, McpServerInfo, McpServerStatus,
    McpTool,
};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::text::trimmed_non_empty;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{PermissionPreset, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::{AttachmentKind, UserAttachment, UserInput};
use giskard_harness::{
    AgentEventStream, AgentHarness, ClaimedNativeRoute, HarnessBootstrap, HarnessCapabilities,
    HarnessNotice, HarnessProvider, HarnessSignal, HarnessSignalStream, OpenThreadOptions,
    ProviderAuth, ProviderAuthCommand, ThreadActivationCause, ThreadHandle, ThreadUpdate,
    thread_activation,
};

use mapping::CodexMapper;

const ROUTE_CAPACITY: usize = 256;
const HARNESS_SIGNAL_CAPACITY: usize = 64;
const TURN_FIRST_EVENT_WARN_AFTER: Duration = Duration::from_secs(15);
#[cfg(not(test))]
const CODEX_JSON_RPC_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const CODEX_JSON_RPC_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const CODEX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const CODEX_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const WORKER_QUEUE_WARN_AFTER: Duration = Duration::from_secs(10);
#[cfg(test)]
const WORKER_QUEUE_WARN_AFTER: Duration = Duration::from_millis(50);
const THREAD_BACKGROUND_TERMINALS_TERMINATE: &str = "thread/backgroundTerminals/terminate";
const CODEX_UPLOAD_DIR_NAME: &str = "giskard-codex-uploads";

struct PendingContextRestore {
    thread: ThreadId,
    model: ModelRef,
    sink: giskard_harness::ThreadUpdateSink,
}

struct OpenThreadOutcome {
    handle: ThreadHandle,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigReadParams {
    include_layers: bool,
    cwd: String,
}

#[derive(Debug, Deserialize)]
struct ConfigReadResponse {
    config: CodexConfig,
}

#[derive(Debug, Deserialize)]
struct CodexConfig {
    sandbox_workspace_write: Option<SandboxWorkspaceWrite>,
    /// `model_provider` — the routing id Codex uses when nothing overrides it. Absent whenever the
    /// user never set the key, which is the common case; Codex then routes to its `openai`
    /// built-in.
    #[serde(default)]
    model_provider: Option<String>,
    /// User-declared `[model_providers.<id>]` entries. Codex's `config/read` serializes its whole
    /// effective config and the app-server `Config` type forwards every key it does not model
    /// itself, so this table arrives even though the generated protocol types omit it. Built-in
    /// providers are *not* included here — see [`CODEX_BUILT_IN_PROVIDER_IDS`].
    #[serde(default)]
    model_providers: HashMap<String, CodexModelProvider>,
}

/// The subset of Codex's `ModelProviderInfo` Giskard needs: a name for the picker and the endpoint
/// plus key location for `/v1/models` discovery.
///
/// `experimental_bearer_token` is deliberately not read. Codex discourages it, and an inline secret
/// is the one field worth leaving where it already lives.
#[derive(Debug, Deserialize)]
struct CodexModelProvider {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    env_key: Option<String>,
    /// `[model_providers.<id>.auth]` — a command whose stdout is the provider's bearer token.
    #[serde(default)]
    auth: Option<CodexProviderAuth>,
}

/// Codex's `ModelProviderAuthInfo`. `refresh_interval_ms` is not read: Giskard reruns the command
/// each time it needs a token rather than caching one (see [`HarnessProvider::resolve_api_key`]),
/// so there is no cached token for an interval to age out.
///
/// Every field is optional even though Codex requires `command`, because this type is only ever
/// deserialized from *Codex's* output and a required field here would make one unfamiliar provider
/// entry fail the whole `config/read`. That response is shared: the same call supplies
/// `sandbox_workspace_write.writable_roots`, so a parse failure would silently narrow the sandbox
/// as a side effect of a model-discovery field. An `auth` table Giskard cannot make sense of is
/// treated as no command auth instead.
#[derive(Debug, Deserialize)]
struct CodexProviderAuth {
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// Codex's own default when `[model_providers.<id>.auth] timeout_ms` is unset
/// (`DEFAULT_PROVIDER_AUTH_TIMEOUT_MS`). `config/read` reports the effective config, so the field
/// should always arrive; this covers a Codex that stops serializing its defaults.
const DEFAULT_PROVIDER_AUTH_TIMEOUT_MS: u64 = 5_000;

impl CodexModelProvider {
    /// Codex rejects a provider declaring both `auth` and `env_key`, so at most one arm applies;
    /// preferring `auth` keeps a config Codex would refuse to load from silently authenticating
    /// discovery a different way than turns.
    fn auth(self) -> Option<ProviderAuth> {
        // Codex rejects an empty `auth.command`, so an absent or blank one is not a provider to
        // authenticate by command — fall through rather than queue up a command that cannot run.
        if let Some(auth) = self.auth
            && let Some(command) = non_empty(auth.command)
        {
            return Some(ProviderAuth::Command(ProviderAuthCommand {
                command,
                args: auth.args,
                cwd: auth.cwd,
                timeout: Duration::from_millis(
                    auth.timeout_ms
                        .filter(|ms| *ms > 0)
                        .unwrap_or(DEFAULT_PROVIDER_AUTH_TIMEOUT_MS),
                ),
            }));
        }
        non_empty(self.env_key).map(ProviderAuth::Env)
    }
}

/// Provider ids Codex ships built in. They never appear in the `[model_providers]` table (which
/// only carries user-declared entries), so Giskard would otherwise report a project pinned to
/// `openai` as pointing at an unknown provider. Mirrors `built_in_model_providers` in Codex's
/// `model-provider-info` crate.
const CODEX_BUILT_IN_PROVIDER_IDS: [&str; 5] = [
    "openai",
    "amazon-bedrock",
    "amazon-bedrock-runtime",
    "ollama",
    "lmstudio",
];

#[derive(Debug, Deserialize)]
struct SandboxWorkspaceWrite {
    #[serde(default)]
    writable_roots: Vec<PathBuf>,
}

struct QueuedHarnessCommand {
    token: WorkerQueueToken,
    command: HarnessCommand,
}

struct QueuedControlCommand {
    token: WorkerQueueToken,
    command: ControlCommand,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadBackgroundTerminalsTerminateParams {
    thread_id: String,
    process_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadBackgroundTerminalsTerminateResponse {
    terminated: bool,
}

enum HarnessCommand {
    OpenThread {
        opts: OpenThreadOptions,
        response: oneshot::Sender<Result<ThreadHandle, HarnessError>>,
    },
    StartTurn {
        thread: Box<ThreadHandle>,
        input: UserInput,
        overrides: TurnOverrides,
        response: oneshot::Sender<Result<TurnId, HarnessError>>,
    },
}

enum ControlCommand {
    ClaimNativeThread {
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
        response: oneshot::Sender<Result<ThreadHandle, HarnessError>>,
    },
    RespondApproval {
        id: ApprovalId,
        decision: ApprovalDecision,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    RespondServerRequest {
        id: ServerRequestId,
        response_payload: ServerRequestResponse,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    Interrupt {
        thread: ThreadHandle,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    TerminateCommand {
        thread: ThreadHandle,
        process_id: String,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    CompactThread {
        thread: ThreadHandle,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    SetThreadName {
        thread: ThreadHandle,
        name: String,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    SetThreadArchived {
        thread: ThreadHandle,
        archived: bool,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    DeleteThread {
        thread: ThreadHandle,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    ListMcpServers {
        response: oneshot::Sender<Result<Vec<McpServerStatus>, HarnessError>>,
    },
    ReloadMcpServers {
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    StartMcpOauthLogin {
        name: String,
        response: oneshot::Sender<Result<McpOauthStart, HarnessError>>,
    },
    ListProviders {
        cwd: String,
        response: oneshot::Sender<Result<Vec<HarnessProvider>, HarnessError>>,
    },
    ListModels {
        /// Codex layers config per directory, so the routing provider its catalog belongs to is
        /// only correct when asked for this project's root.
        cwd: String,
        response: oneshot::Sender<Result<Vec<ModelDescriptor>, HarnessError>>,
    },
}

struct WorkerReceivers {
    commands: mpsc::Receiver<QueuedHarnessCommand>,
    controls: mpsc::Receiver<QueuedControlCommand>,
    urgent_controls: mpsc::Receiver<QueuedControlCommand>,
    shutdown: watch::Receiver<bool>,
}

struct NativeRouteEntry {
    route: ClaimedNativeRoute,
    event_tx: mpsc::Sender<AgentEvent>,
    receiver: Option<mpsc::Receiver<AgentEvent>>,
    ready: bool,
}

#[derive(Default)]
struct NativeRoutes {
    by_native: HashMap<String, NativeRouteEntry>,
    native_by_thread: HashMap<ThreadId, String>,
    next_route_epoch: u64,
}

type RouteMap = Arc<StdMutex<NativeRoutes>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerQueueKind {
    Command,
    Control,
}

impl WorkerQueueKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Control => "control",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WorkerQueueToken {
    id: u64,
    kind: WorkerQueueKind,
    action: &'static str,
    project_id: Option<ProjectId>,
    thread_id: Option<ThreadId>,
    enqueued_at: Instant,
}

#[derive(Debug, Clone)]
struct WorkerQueueEntrySnapshot {
    id: u64,
    kind: WorkerQueueKind,
    action: &'static str,
    project_id: Option<ProjectId>,
    thread_id: Option<ThreadId>,
    elapsed_ms: u128,
}

#[derive(Debug, Clone)]
struct WorkerQueueSnapshot {
    active: Option<WorkerQueueEntrySnapshot>,
    oldest_pending: Option<WorkerQueueEntrySnapshot>,
    command_pending: usize,
    control_pending: usize,
}

#[derive(Debug)]
struct WorkerQueueState {
    next_id: u64,
    pending: HashMap<u64, WorkerQueueToken>,
    active: HashMap<u64, WorkerQueueToken>,
    closed: bool,
}

#[derive(Debug)]
struct WorkerQueueWatchdog {
    state: StdMutex<WorkerQueueState>,
}

impl WorkerQueueWatchdog {
    fn new() -> Self {
        Self {
            state: StdMutex::new(WorkerQueueState {
                next_id: 1,
                pending: HashMap::new(),
                active: HashMap::new(),
                closed: false,
            }),
        }
    }

    fn enqueue(
        &self,
        kind: WorkerQueueKind,
        action: &'static str,
        project_id: Option<ProjectId>,
        thread_id: Option<ThreadId>,
    ) -> WorkerQueueToken {
        let mut state = self.lock_state();
        let token = WorkerQueueToken {
            id: state.next_id,
            kind,
            action,
            project_id,
            thread_id,
            enqueued_at: Instant::now(),
        };
        state.next_id = state.next_id.saturating_add(1);
        state.pending.insert(token.id, token);
        token
    }

    fn cancel(&self, token: WorkerQueueToken) {
        self.lock_state().pending.remove(&token.id);
    }

    fn mark_started(&self, token: WorkerQueueToken) {
        let mut state = self.lock_state();
        state.pending.remove(&token.id);
        state.active.insert(token.id, token);
    }

    fn mark_finished(&self, token: WorkerQueueToken) {
        let mut state = self.lock_state();
        state.active.remove(&token.id);
    }

    fn close(&self) {
        self.lock_state().closed = true;
    }

    fn snapshot(&self) -> WorkerQueueSnapshot {
        let state = self.lock_state();
        let now = Instant::now();
        let mut command_pending = 0;
        let mut control_pending = 0;
        let mut oldest_pending: Option<WorkerQueueToken> = None;
        for token in state.pending.values().copied() {
            match token.kind {
                WorkerQueueKind::Command => command_pending += 1,
                WorkerQueueKind::Control => control_pending += 1,
            }
            if oldest_pending.is_none_or(|oldest| token.enqueued_at < oldest.enqueued_at) {
                oldest_pending = Some(token);
            }
        }

        WorkerQueueSnapshot {
            active: state
                .active
                .values()
                .copied()
                .min_by_key(|token| token.enqueued_at)
                .map(|token| snapshot_queue_token(token, now)),
            oldest_pending: oldest_pending.map(|token| snapshot_queue_token(token, now)),
            command_pending,
            control_pending,
        }
    }

    fn is_closed(&self) -> bool {
        self.lock_state().closed
    }

    fn lock_state(&self) -> StdMutexGuard<'_, WorkerQueueState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("Codex worker queue watchdog lock was poisoned; recovering state");
                poisoned.into_inner()
            }
        }
    }
}

fn snapshot_queue_token(token: WorkerQueueToken, now: Instant) -> WorkerQueueEntrySnapshot {
    WorkerQueueEntrySnapshot {
        id: token.id,
        kind: token.kind,
        action: token.action,
        project_id: token.project_id,
        thread_id: token.thread_id,
        elapsed_ms: now.duration_since(token.enqueued_at).as_millis(),
    }
}

async fn run_worker_queue_watchdog(watchdog: Weak<WorkerQueueWatchdog>) {
    let mut tick = tokio::time::interval(WORKER_QUEUE_WARN_AFTER);
    loop {
        tick.tick().await;
        let Some(watchdog) = watchdog.upgrade() else {
            break;
        };
        if watchdog.is_closed() {
            break;
        }
        let snapshot = watchdog.snapshot();
        let active_is_slow = snapshot
            .active
            .as_ref()
            .is_some_and(|active| active.elapsed_ms >= WORKER_QUEUE_WARN_AFTER.as_millis());
        let pending_is_slow = snapshot
            .oldest_pending
            .as_ref()
            .is_some_and(|pending| pending.elapsed_ms >= WORKER_QUEUE_WARN_AFTER.as_millis());
        if active_is_slow || pending_is_slow {
            warn!(
                active_id = display_opt(snapshot.active.as_ref().map(|entry| entry.id)),
                active_kind =
                    display_opt(snapshot.active.as_ref().map(|entry| entry.kind.as_str())),
                active_action = display_opt(snapshot.active.as_ref().map(|entry| entry.action)),
                active_project_id =
                    display_opt(snapshot.active.as_ref().and_then(|entry| entry.project_id)),
                active_thread_id =
                    display_opt(snapshot.active.as_ref().and_then(|entry| entry.thread_id)),
                active_elapsed_ms =
                    display_opt(snapshot.active.as_ref().map(|entry| entry.elapsed_ms)),
                oldest_pending_id =
                    display_opt(snapshot.oldest_pending.as_ref().map(|entry| entry.id)),
                oldest_pending_kind = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .map(|entry| entry.kind.as_str())
                ),
                oldest_pending_action =
                    display_opt(snapshot.oldest_pending.as_ref().map(|entry| entry.action)),
                oldest_pending_project_id = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .and_then(|entry| entry.project_id)
                ),
                oldest_pending_thread_id = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .and_then(|entry| entry.thread_id)
                ),
                oldest_pending_elapsed_ms = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .map(|entry| entry.elapsed_ms)
                ),
                command_pending = snapshot.command_pending,
                control_pending = snapshot.control_pending,
                "Codex worker queue has slow active or pending work"
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CodexOperationContext<'a> {
    action: &'static str,
    project_id: Option<ProjectId>,
    thread_id: Option<ThreadId>,
    turn_id: Option<TurnId>,
    harness_thread_id: Option<&'a str>,
    native_turn_id: Option<&'a str>,
    process_id: Option<&'a str>,
    server: Option<&'a str>,
    request_id: Option<&'a codex_codes::jsonrpc::RequestId>,
}

impl<'a> CodexOperationContext<'a> {
    fn new(action: &'static str) -> Self {
        Self {
            action,
            project_id: None,
            thread_id: None,
            turn_id: None,
            harness_thread_id: None,
            native_turn_id: None,
            process_id: None,
            server: None,
            request_id: None,
        }
    }

    fn for_project(action: &'static str, project_id: ProjectId) -> Self {
        Self::new(action).with_project_id(project_id)
    }

    fn for_thread(action: &'static str, thread: &'a ThreadHandle) -> Self {
        Self::new(action)
            .with_thread_id(thread.thread)
            .with_harness_thread_id(&thread.harness_thread_id)
    }

    fn with_project_id(mut self, project_id: ProjectId) -> Self {
        self.project_id = Some(project_id);
        self
    }

    fn with_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.thread_id = Some(thread_id);
        self
    }

    fn with_giskard_turn_id(mut self, turn_id: TurnId) -> Self {
        self.turn_id = Some(turn_id);
        self
    }

    fn with_harness_thread_id(mut self, harness_thread_id: &'a str) -> Self {
        self.harness_thread_id = Some(harness_thread_id);
        self
    }

    fn with_native_turn_id(mut self, native_turn_id: &'a str) -> Self {
        self.native_turn_id = Some(native_turn_id);
        self
    }

    fn with_process_id(mut self, process_id: &'a str) -> Self {
        self.process_id = Some(process_id);
        self
    }

    fn with_server(mut self, server: &'a str) -> Self {
        self.server = Some(server);
        self
    }

    fn with_request_id(mut self, request_id: &'a codex_codes::jsonrpc::RequestId) -> Self {
        self.request_id = Some(request_id);
        self
    }

    fn log_timeout(self, method: Option<&str>, elapsed: Duration, message: &'static str) {
        warn!(
            action = self.action,
            method = display_opt(method),
            project_id = display_opt(self.project_id),
            thread_id = display_opt(self.thread_id),
            turn_id = display_opt(self.turn_id),
            harness_thread_id = display_opt(self.harness_thread_id),
            native_turn_id = display_opt(self.native_turn_id),
            process_id = display_opt(self.process_id),
            server = display_opt(self.server),
            request_id = display_opt(self.request_id),
            elapsed_ms = elapsed.as_millis(),
            timeout_ms = CODEX_JSON_RPC_TIMEOUT.as_millis(),
            "{message}"
        );
    }
}

#[async_trait]
trait CodexTransport: Send {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError>;

    async fn request_json_with_hook(
        &mut self,
        method: &str,
        params: serde_json::Value,
        hook: transport::SuccessResponseHook,
        _lifetime: transport::CorrelationLifetime,
        _abandoned_error_hook: Option<transport::AbandonedErrorHook>,
    ) -> Result<serde_json::Value, HarnessError> {
        let response = self.request_json(method, params).await?;
        hook(response).await
    }

    async fn submit_control_request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<PendingCodexResponse, HarnessError>;

    async fn respond_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError>;

    async fn respond_error_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError>;

    async fn shutdown_transport(self, deadline: tokio::time::Instant) -> Result<(), HarnessError>
    where
        Self: Sized;
}

type PendingCodexResponse =
    Pin<Box<dyn Future<Output = Result<serde_json::Value, HarnessError>> + Send>>;

#[async_trait]
trait CodexFrameReceiver: Send {
    async fn next_message(
        &mut self,
    ) -> Result<Option<transport::ProductionFrame>, CodexStreamError>;
}

#[derive(Debug)]
enum CodexStreamError {
    /// A non-JSON line was consumed from app-server stdout. Since JSON-RPC is
    /// newline-delimited, the next read starts at a fresh frame boundary.
    NonJsonStdout {
        parse_error: String,
        raw_preview: String,
        raw_bytes: usize,
    },
    Fatal(HarnessError),
}

const NON_JSON_STDOUT_PREVIEW_BYTES: usize = 4 * 1024;

fn bounded_utf8_preview(value: &str, max_bytes: usize) -> String {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[async_trait]
impl CodexTransport for transport::DispatchClient {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError> {
        self.request(method, params).await
    }

    async fn request_json_with_hook(
        &mut self,
        method: &str,
        params: serde_json::Value,
        hook: transport::SuccessResponseHook,
        lifetime: transport::CorrelationLifetime,
        abandoned_error_hook: Option<transport::AbandonedErrorHook>,
    ) -> Result<serde_json::Value, HarnessError> {
        self.request_with_hook(method, params, hook, lifetime, abandoned_error_hook)
            .await
    }

    async fn submit_control_request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<PendingCodexResponse, HarnessError> {
        let response = self.submit_control_request(method, params)?;
        let method = method.to_owned();
        Ok(Box::pin(async move { response.receive(&method).await }))
    }

    async fn respond_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError> {
        self.respond(id, value).await
    }

    async fn respond_error_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        self.respond_error(id, code, message).await
    }

    async fn shutdown_transport(self, deadline: tokio::time::Instant) -> Result<(), HarnessError> {
        self.shutdown(deadline).await
    }
}

#[async_trait]
impl CodexFrameReceiver for transport::ProductionFrameReceiver {
    async fn next_message(
        &mut self,
    ) -> Result<Option<transport::ProductionFrame>, CodexStreamError> {
        transport::ProductionFrameReceiver::next_message(self).await
    }
}

async fn codex_request<P, R>(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    method: &str,
    params: &P,
) -> Result<R, HarnessError>
where
    P: Serialize + Sync,
    R: DeserializeOwned,
{
    let params = serde_json::to_value(params).map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let started = Instant::now();
    let response =
        tokio::time::timeout(CODEX_JSON_RPC_TIMEOUT, client.request_json(method, params))
            .await
            .map_err(|_| {
                context.log_timeout(
                    Some(method),
                    started.elapsed(),
                    "Codex JSON-RPC request timed out; worker will resume processing commands",
                );
                HarnessError::Timeout(format!("Codex JSON-RPC request {method} timed out"))
            })??;
    serde_json::from_value(response).map_err(|e| HarnessError::Protocol(e.to_string()))
}

async fn codex_request_with_hook<P, R>(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    method: &str,
    params: &P,
    hook: transport::SuccessResponseHook,
    lifetime: transport::CorrelationLifetime,
    abandoned_error_hook: Option<transport::AbandonedErrorHook>,
) -> Result<R, HarnessError>
where
    P: Serialize + Sync,
    R: DeserializeOwned,
{
    let params = serde_json::to_value(params).map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let started = Instant::now();
    let response = tokio::time::timeout(
        CODEX_JSON_RPC_TIMEOUT,
        client.request_json_with_hook(method, params, hook, lifetime, abandoned_error_hook),
    )
    .await
    .map_err(|_| {
        context.log_timeout(
            Some(method),
            started.elapsed(),
            "Codex JSON-RPC request or identity activation timed out",
        );
        HarnessError::Timeout(format!(
            "Codex JSON-RPC request or identity activation {method} timed out"
        ))
    })??;
    serde_json::from_value(response).map_err(|e| HarnessError::Protocol(e.to_string()))
}

async fn codex_respond_json(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    id: codex_codes::jsonrpc::RequestId,
    value: serde_json::Value,
) -> Result<(), HarnessError> {
    let started = Instant::now();
    let id_for_log = id.clone();
    tokio::time::timeout(CODEX_JSON_RPC_TIMEOUT, client.respond_json(id, value))
        .await
        .map_err(|_| {
            context.with_request_id(&id_for_log).log_timeout(
                None,
                started.elapsed(),
                "Codex JSON-RPC response timed out; worker will resume processing commands",
            );
            HarnessError::Timeout(format!("Codex JSON-RPC response {id_for_log} timed out"))
        })?
}

async fn codex_respond_error_json(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    id: codex_codes::jsonrpc::RequestId,
    code: i64,
    message: &str,
) -> Result<(), HarnessError> {
    let started = Instant::now();
    let id_for_log = id.clone();
    tokio::time::timeout(
        CODEX_JSON_RPC_TIMEOUT,
        client.respond_error_json(id, code, message),
    )
    .await
    .map_err(|_| {
        context.with_request_id(&id_for_log).log_timeout(
            None,
            started.elapsed(),
            "Codex JSON-RPC error response timed out; worker will resume processing commands",
        );
        HarnessError::Timeout(format!(
            "Codex JSON-RPC error response {id_for_log} timed out"
        ))
    })?
}

/// Codex CLI harness adapter (one app-server process per project).
pub struct CodexHarness {
    /// Kept so `list_providers` can resolve Codex's config for this project's directory: config is
    /// layered per-cwd, so the provider table is only correct when asked for the right root.
    workspace_root: PathBuf,
    /// The running Codex's version, read from the initialize handshake's user agent. Sent as
    /// `client_version` on `/models` discovery so a provider serving Codex's catalog answers
    /// Giskard the way it would answer Codex (§8.3). `None` when the user agent did not parse.
    client_version: Option<String>,
    cmd_tx: mpsc::Sender<QueuedHarnessCommand>,
    control_tx: mpsc::Sender<QueuedControlCommand>,
    urgent_control_tx: mpsc::Sender<QueuedControlCommand>,
    routes: RouteMap,
    signals: StdMutex<Option<mpsc::Receiver<HarnessSignal>>>,
    worker_queue: Arc<WorkerQueueWatchdog>,
    shutdown_tx: watch::Sender<bool>,
    worker_done: watch::Receiver<bool>,
    capabilities: HarnessCapabilities,
}

impl CodexHarness {
    pub async fn start(workspace_root: PathBuf) -> Result<Arc<Self>, HarnessError> {
        Self::start_with_bootstrap(workspace_root, HarnessBootstrap::default()).await
    }

    pub async fn start_with_bootstrap(
        workspace_root: PathBuf,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<Self>, HarnessError> {
        let workspace_root = normalize_workspace_root(workspace_root)?;
        let (client, frames, client_version) =
            start_codex_client(codex_codes::AppServerBuilder::new()).await?;
        Self::spawn_harness(
            client,
            frames,
            workspace_root,
            None,
            client_version,
            bootstrap,
        )
    }

    pub async fn start_with(
        workspace_root: PathBuf,
        codex_path: PathBuf,
    ) -> Result<Arc<Self>, HarnessError> {
        let workspace_root = normalize_workspace_root(workspace_root)?;
        let builder = codex_codes::cli::AppServerBuilder::new().command(codex_path);
        let (client, frames, client_version) = start_codex_client(builder).await?;
        Self::spawn_harness(
            client,
            frames,
            workspace_root,
            None,
            client_version,
            HarnessBootstrap::default(),
        )
    }

    fn spawn_harness<C, R>(
        client: C,
        frames: R,
        workspace_root: PathBuf,
        writable_roots: Option<Vec<PathBuf>>,
        client_version: Option<String>,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<Self>, HarnessError>
    where
        C: CodexTransport + Clone + 'static,
        R: CodexFrameReceiver + 'static,
    {
        let mut mapper = CodexMapper::new(workspace_root.clone());
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(64);
        let (urgent_control_tx, urgent_control_rx) = mpsc::channel(64);
        let routes: RouteMap = Arc::new(StdMutex::new(NativeRoutes::default()));
        for binding in bootstrap.known_threads {
            let native_id = binding.harness_thread_id;
            let route = claim_native_route(&routes, &native_id, binding.thread_id)?;
            mapper.claim_thread(native_id, route.thread_id)?;
        }
        let (signal_tx, signal_rx) = mpsc::channel(HARNESS_SIGNAL_CAPACITY);
        let worker_queue = Arc::new(WorkerQueueWatchdog::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (worker_done_tx, worker_done) = watch::channel(false);

        let harness = Arc::new(Self {
            workspace_root: workspace_root.clone(),
            client_version,
            cmd_tx,
            control_tx,
            urgent_control_tx,
            routes: routes.clone(),
            signals: StdMutex::new(Some(signal_rx)),
            worker_queue: worker_queue.clone(),
            shutdown_tx,
            worker_done,
            capabilities: HarnessCapabilities {
                live_approvals: true,
                plan_build_modes: true,
                per_turn_model: true,
                reasoning_effort: true,
                structured_diffs: true,
                resumable_threads: true,
                model_listing: true,
                provider_listing: true,
                token_usage: true,
                mcp_status: true,
                mcp_reload: true,
                mcp_oauth_login: true,
                context_compaction: true,
            },
        });

        tokio::spawn(run_worker_queue_watchdog(Arc::downgrade(&worker_queue)));
        tokio::spawn(async move {
            background_task(
                client,
                frames,
                WorkerReceivers {
                    commands: cmd_rx,
                    controls: control_rx,
                    urgent_controls: urgent_control_rx,
                    shutdown: shutdown_rx,
                },
                routes,
                signal_tx,
                worker_queue,
                workspace_root,
                writable_roots,
                mapper,
            )
            .await;
            worker_done_tx.send_replace(true);
        });
        Ok(harness)
    }

    async fn enqueue_command(
        &self,
        action: &'static str,
        command: HarnessCommand,
    ) -> Result<(), HarnessError> {
        let (project_id, thread_id) = match &command {
            HarnessCommand::OpenThread { opts, .. } => (Some(opts.project), opts.thread),
            HarnessCommand::StartTurn { thread, .. } => (None, Some(thread.thread)),
        };
        let token =
            self.worker_queue
                .enqueue(WorkerQueueKind::Command, action, project_id, thread_id);
        self.cmd_tx
            .send(QueuedHarnessCommand { token, command })
            .await
            .map_err(|_| {
                self.worker_queue.cancel(token);
                HarnessError::Transport("background task closed".into())
            })
    }

    async fn enqueue_control(
        &self,
        action: &'static str,
        command: ControlCommand,
    ) -> Result<(), HarnessError> {
        let thread_id = match &command {
            ControlCommand::ClaimNativeThread { thread, .. } => Some(*thread),
            ControlCommand::Interrupt { thread, .. }
            | ControlCommand::TerminateCommand { thread, .. }
            | ControlCommand::CompactThread { thread, .. }
            | ControlCommand::SetThreadName { thread, .. }
            | ControlCommand::SetThreadArchived { thread, .. }
            | ControlCommand::DeleteThread { thread, .. } => Some(thread.thread),
            _ => None,
        };
        let token = self
            .worker_queue
            .enqueue(WorkerQueueKind::Control, action, None, thread_id);
        let tx = match &command {
            ControlCommand::RespondApproval { .. }
            | ControlCommand::RespondServerRequest { .. }
            | ControlCommand::Interrupt { .. }
            | ControlCommand::TerminateCommand { .. } => &self.urgent_control_tx,
            _ => &self.control_tx,
        };
        tx.send(QueuedControlCommand { token, command })
            .await
            .map_err(|_| {
                self.worker_queue.cancel(token);
                HarnessError::Transport("background task closed".into())
            })
    }
}

fn normalize_workspace_root(workspace_root: PathBuf) -> Result<PathBuf, HarnessError> {
    std::path::absolute(&workspace_root).map_err(|error| {
        warn!(
            workspace_root = %workspace_root.display(),
            error = %error,
            "Rejecting Codex project workspace root that could not be made absolute"
        );
        HarnessError::Protocol(format!(
            "could not make Codex project workspace root {} absolute: {error}",
            workspace_root.display()
        ))
    })
}

async fn start_codex_client(
    builder: codex_codes::AppServerBuilder,
) -> Result<
    (
        transport::DispatchClient,
        transport::ProductionFrameReceiver,
        Option<String>,
    ),
    HarnessError,
> {
    let (client, mut frames) = transport::DispatchClient::spawn(builder).await?;
    let params = serde_json::to_value(build_initialize_params())
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    let initialize_client = client.clone();
    let initialize = initialize_client.request(codex_codes::protocol::methods::INITIALIZE, params);
    tokio::pin!(initialize);
    let value = tokio::select! {
        biased;
        result = &mut initialize => {
            result.map_err(|error| HarnessError::Spawn(error.to_string()))?
        }
        frame = frames.next_message() => {
            let error = match frame {
                Ok(Some(frame)) => {
                    let (_, acknowledgement) = frame.into_parts();
                    let error = HarnessError::Protocol(
                        "Codex emitted a production frame before initialize completed".into(),
                    );
                    acknowledgement.acknowledge(Err(error.clone()));
                    error
                }
                Ok(None) => HarnessError::Transport(
                    "Codex stdout closed before initialize completed".into(),
                ),
                Err(CodexStreamError::NonJsonStdout { parse_error, .. }) => {
                    HarnessError::Transport(format!(
                        "Codex wrote non-JSON stdout before initialize completed: {parse_error}"
                    ))
                }
                Err(CodexStreamError::Fatal(error)) => error,
            };
            return Err(HarnessError::Spawn(error.to_string()));
        }
    };
    let response: codex_codes::InitializeResponse =
        serde_json::from_value(value).map_err(|error| HarnessError::Spawn(error.to_string()))?;
    let version = codex_version_from_user_agent(&response.user_agent);
    match version.as_deref() {
        Some(version) => warn_if_codex_is_newer_than_tested(version),
        None => warn!(
            user_agent = %response.user_agent,
            action = "start_codex_client",
            "could not read a Codex version out of the app-server user agent; \
             /models discovery will not identify a client version"
        ),
    }
    Ok((client, frames, version))
}

/// Warn when the running Codex CLI is newer than the release the `codex-codes` bindings were last
/// tested against, which the crate exposes as `version::tested_cli_version()`.
///
/// The bindings' own `check_codex_version` is deliberately not used: it shells out to `codex
/// --version` on `PATH`, ignoring the configured `codex_path`, and reports through the `log` crate,
/// which Giskard does not bridge into `tracing`. Giskard already read the running version out of
/// the initialize user agent, so the comparison costs nothing extra and the warning lands in
/// Giskard's own structured log.
///
/// Drift past the tested release is not fatal — unmapped protocol additions arrive as unknown
/// notifications and requests — but it is the first thing to check when Codex behavior looks
/// truncated, so it is stated once per spawned app-server.
fn warn_if_codex_is_newer_than_tested(version: &str) {
    let tested = codex_codes::version::tested_cli_version();
    if version_is_newer(version, tested) {
        warn!(
            action = "start_codex_client",
            codex_version = version,
            tested_codex_version = tested,
            "running Codex CLI is newer than the release the codex-codes bindings were tested \
             against; protocol additions it makes are not mapped and are reported as unknown \
             notifications and requests"
        );
    }
}

/// Order two `MAJOR.MINOR.PATCH` versions numerically. Anything that does not parse as three
/// numeric components compares as "not newer" so an unreadable version never raises a false alarm.
fn version_is_newer(version: &str, baseline: &str) -> bool {
    fn triple(value: &str) -> Option<(u64, u64, u64)> {
        let mut parts = value.split('.');
        let mut next = || parts.next()?.parse::<u64>().ok();
        let (major, minor, patch) = (next()?, next()?, next()?);
        parts.next().is_none().then_some((major, minor, patch))
    }
    match (triple(version), triple(baseline)) {
        (Some(version), Some(baseline)) => version > baseline,
        _ => false,
    }
}

/// Pull the Codex version out of the user agent the app-server reports at initialize, in the form
/// Codex would send it as `client_version`.
///
/// Codex builds the user agent as `{originator}/{version} ({os}…) …`, so the version is the tail of
/// the first token. This is the only place the running Codex states its own version over the
/// protocol.
///
/// It is **not** used verbatim. The user agent carries the full `CARGO_PKG_VERSION`, while Codex's
/// own `client_version_to_whole` reduces the same version to `MAJOR.MINOR.PATCH` — its doc gives
/// `"1.2.3-alpha.4" -> "1.2.3"`. Forwarding the pre-release suffix would make Giskard ask a
/// provider a question Codex never asks, so the suffix is dropped here to send exactly what Codex
/// sends. Anything that does not reduce to three numeric components yields `None`, and discovery
/// omits the parameter rather than guessing.
fn codex_version_from_user_agent(user_agent: &str) -> Option<String> {
    let (_originator, version) = user_agent.split_whitespace().next()?.rsplit_once('/')?;
    // Pre-release (`-alpha.4`) and build metadata (`+abc`) are both suffixes on the whole version.
    let whole = version
        .split_once(['-', '+'])
        .map_or(version, |(whole, _suffix)| whole);
    let mut parts = whole.split('.');
    let (major, minor, patch) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let numeric = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !numeric(major) || !numeric(minor) || !numeric(patch) {
        return None;
    }
    Some(format!("{major}.{minor}.{patch}"))
}

fn build_initialize_params() -> codex_codes::InitializeParams {
    codex_codes::InitializeParams {
        client_info: codex_codes::ClientInfo {
            name: "giskard".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            title: Some("Giskard".into()),
        },
        capabilities: Some(codex_codes::InitializeCapabilities {
            experimental_api: Some(true),
            extensions: None,
            mcp_server_openai_form_elicitation: None,
            opt_out_notification_methods: None,
            request_attestation: None,
        }),
    }
}

/// Read Codex's effective config for `cwd`.
///
/// Three callers need it — writable roots, the provider table, and the routing provider — and each
/// was building the same params and naming the same method. `action` distinguishes them in the
/// request log, since a failure means different things to each.
async fn read_codex_config(
    client: &mut dyn CodexTransport,
    cwd: String,
    action: &'static str,
) -> Result<CodexConfig, HarnessError> {
    let params = ConfigReadParams {
        include_layers: false,
        cwd,
    };
    let response: ConfigReadResponse = codex_request(
        client,
        CodexOperationContext::new(action),
        "config/read",
        &params,
    )
    .await?;
    Ok(response.config)
}

async fn configured_workspace_write_roots(
    client: &mut dyn CodexTransport,
    workspace_root: &std::path::Path,
) -> Vec<PathBuf> {
    let response = read_codex_config(
        client,
        workspace_root.to_string_lossy().into_owned(),
        "config_read_workspace_roots",
    )
    .await;

    match response {
        Ok(config) => {
            let mut roots = Vec::new();
            if let Some(sandbox) = config.sandbox_workspace_write {
                for root in sandbox.writable_roots {
                    if root.is_absolute() {
                        roots.push(root);
                    } else {
                        warn!(
                            workspace_root = %workspace_root.display(),
                            configured_root = %root.display(),
                            "Ignoring relative Codex configured workspace-write root"
                        );
                    }
                }
            }
            roots.sort();
            roots.dedup();
            roots
        }
        Err(error) => {
            warn!(
                workspace_root = %workspace_root.display(),
                error = %error,
                "Could not read Codex workspace-write roots; omitting configured extra runtime workspace roots"
            );
            Vec::new()
        }
    }
}

#[async_trait]
impl AgentHarness for CodexHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        self.capabilities
    }

    fn client_version(&self) -> Option<String> {
        self.client_version.clone()
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "list_models",
            ControlCommand::ListModels {
                cwd: self.workspace_root.to_string_lossy().into_owned(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "list_providers",
            ControlCommand::ListProviders {
                cwd: self.workspace_root.to_string_lossy().into_owned(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "list_mcp_servers",
            ControlCommand::ListMcpServers { response: tx },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn reload_mcp_servers(&self) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "reload_mcp_servers",
            ControlCommand::ReloadMcpServers { response: tx },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn start_mcp_oauth_login(&self, name: &str) -> Result<McpOauthStart, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "start_mcp_oauth_login",
            ControlCommand::StartMcpOauthLogin {
                name: name.to_owned(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_command(
            "open_thread",
            HarnessCommand::OpenThread { opts, response: tx },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn claim_native_thread(
        &self,
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
    ) -> Result<ThreadHandle, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "claim_native_thread",
            ControlCommand::ClaimNativeThread {
                thread,
                harness_thread_id,
                workspace_root,
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    fn take_harness_signals(&self) -> Result<HarnessSignalStream, HarnessError> {
        let mut signals = self.signals.lock().map_err(|_| {
            HarnessError::Transport("Codex harness signal lock was poisoned".into())
        })?;
        signals
            .take()
            .map(HarnessSignalStream::new)
            .ok_or_else(|| HarnessError::Protocol("harness signal stream was already taken".into()))
    }

    async fn claim_native_route(
        &self,
        harness_thread_id: String,
        suggested_thread_id: ThreadId,
    ) -> Result<ClaimedNativeRoute, HarnessError> {
        self.claim_native_thread(
            suggested_thread_id,
            harness_thread_id.clone(),
            PathBuf::new(),
        )
        .await?;
        route_for_native(&self.routes, &harness_thread_id)?.ok_or_else(|| {
            HarnessError::Protocol(format!(
                "native thread {harness_thread_id} was claimed without a route"
            ))
        })
    }

    fn claim_event_receiver(
        &self,
        route: &ClaimedNativeRoute,
    ) -> Result<AgentEventStream, HarnessError> {
        let mut routes = lock_routes(&self.routes)?;
        let entry = routes
            .by_native
            .get_mut(&route.harness_thread_id)
            .filter(|entry| entry.route == *route)
            .ok_or_else(|| HarnessError::Protocol("stale or unknown native route".into()))?;
        let receiver = entry.receiver.take().ok_or_else(|| {
            HarnessError::Protocol(format!(
                "native route {} event receiver was already claimed",
                route.harness_thread_id
            ))
        })?;
        Ok(AgentEventStream::new(receiver))
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_command(
            "start_turn",
            HarnessCommand::StartTurn {
                thread: Box::new(thread.clone()),
                input,
                overrides,
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn respond_approval(
        &self,
        id: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "respond_approval",
            ControlCommand::RespondApproval {
                id,
                decision,
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn respond_server_request(
        &self,
        id: ServerRequestId,
        response_payload: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "respond_server_request",
            ControlCommand::RespondServerRequest {
                id,
                response_payload,
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "interrupt",
            ControlCommand::Interrupt {
                thread: thread.clone(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "compact_thread",
            ControlCommand::CompactThread {
                thread: thread.clone(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn terminate_command(
        &self,
        thread: &ThreadHandle,
        process_id: &str,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "terminate_command",
            ControlCommand::TerminateCommand {
                thread: thread.clone(),
                process_id: process_id.to_owned(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn set_thread_name(&self, thread: &ThreadHandle, name: &str) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "set_thread_name",
            ControlCommand::SetThreadName {
                thread: thread.clone(),
                name: name.to_owned(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn set_thread_archived(
        &self,
        thread: &ThreadHandle,
        archived: bool,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "set_thread_archived",
            ControlCommand::SetThreadArchived {
                thread: thread.clone(),
                archived,
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control(
            "delete_thread",
            ControlCommand::DeleteThread {
                thread: thread.clone(),
                response: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        // This update is synchronous and idempotent. Unlike enqueueing on the bounded control
        // queue, shutdown initiation therefore survives cancellation of this caller.
        self.shutdown_tx.send_replace(true);
        let mut worker_done = self.worker_done.clone();
        while !*worker_done.borrow_and_update() {
            if worker_done.changed().await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn background_task<C, R>(
    mut client: C,
    frames: R,
    receivers: WorkerReceivers,
    routes: RouteMap,
    signal_tx: mpsc::Sender<HarnessSignal>,
    worker_queue: Arc<WorkerQueueWatchdog>,
    workspace_root: PathBuf,
    configured_writable_roots: Option<Vec<PathBuf>>,
    mapper: CodexMapper,
) where
    C: CodexTransport + Clone + 'static,
    R: CodexFrameReceiver + 'static,
{
    let WorkerReceivers {
        commands,
        controls,
        urgent_controls,
        mut shutdown,
    } = receivers;
    let state = Arc::new(StdMutex::new(BackgroundState {
        mapper,
        pending_compactions: HashMap::new(),
        pending_context_restores: HashMap::new(),
        active_turns: HashMap::new(),
    }));
    let inbound = run_inbound_dispatch(
        client.clone(),
        frames,
        state.clone(),
        routes.clone(),
        signal_tx.clone(),
        workspace_root.clone(),
    );
    tokio::pin!(inbound);

    // `config/read` may have ordinary production frames ahead of its response. Start the sole
    // production consumer first so the ACK barrier can activate and deliver those frames instead
    // of deadlocking harness construction.
    let writable_roots = match configured_writable_roots {
        Some(roots) => roots,
        None => {
            let mut config_client = client.clone();
            let read_roots = configured_workspace_write_roots(&mut config_client, &workspace_root);
            tokio::pin!(read_roots);
            let roots = tokio::select! {
                biased;
                _ = wait_for_shutdown_request(&mut shutdown) => None,
                message = &mut inbound => {
                    let (cleanup_deadline, deadline) = codex_shutdown_deadlines();
                    finish_background_state(
                        &mut client,
                        &state,
                        &routes,
                        message,
                        cleanup_deadline,
                    )
                    .await;
                    shutdown_codex_transport(client, &workspace_root, deadline).await;
                    worker_queue.close();
                    return;
                }
                roots = &mut read_roots => Some(roots),
            };
            let Some(roots) = roots else {
                let (cleanup_deadline, deadline) = codex_shutdown_deadlines();
                finish_background_state(&mut client, &state, &routes, None, cleanup_deadline).await;
                shutdown_codex_transport(client, &workspace_root, deadline).await;
                worker_queue.close();
                return;
            };
            roots
        }
    };

    let command_worker = run_command_worker(
        client.clone(),
        commands,
        controls,
        state.clone(),
        routes.clone(),
        signal_tx.clone(),
        worker_queue.clone(),
        writable_roots,
    );
    let urgent_control_worker = run_urgent_control_worker(
        client.clone(),
        urgent_controls,
        state.clone(),
        routes.clone(),
        worker_queue.clone(),
    );
    let first_event_watchdog = run_first_event_watchdog(state.clone());
    tokio::pin!(command_worker);
    tokio::pin!(urgent_control_worker);
    tokio::pin!(first_event_watchdog);

    let incomplete_message = tokio::select! {
        biased;
        _ = wait_for_shutdown_request(&mut shutdown) => None,
        message = &mut inbound => message,
        () = &mut command_worker => None,
        () = &mut urgent_control_worker => None,
        () = &mut first_event_watchdog => None,
    };

    let (cleanup_deadline, deadline) = codex_shutdown_deadlines();
    finish_background_state(
        &mut client,
        &state,
        &routes,
        incomplete_message,
        cleanup_deadline,
    )
    .await;
    shutdown_codex_transport(client, &workspace_root, deadline).await;
    worker_queue.close();
}

fn codex_shutdown_deadlines() -> (tokio::time::Instant, tokio::time::Instant) {
    let started = tokio::time::Instant::now();
    let deadline = started + CODEX_SHUTDOWN_TIMEOUT;
    let cleanup_deadline = deadline
        .checked_sub(transport::FORCED_KILL_REAP_RESERVE)
        .unwrap_or(started);
    (cleanup_deadline, deadline)
}

struct BackgroundState {
    mapper: CodexMapper,
    pending_compactions: HashMap<ThreadId, PendingCompaction>,
    pending_context_restores: HashMap<String, PendingContextRestore>,
    active_turns: ActiveTurns,
}

type SharedBackgroundState = Arc<StdMutex<BackgroundState>>;

fn lock_background_state(
    state: &SharedBackgroundState,
) -> Result<StdMutexGuard<'_, BackgroundState>, HarnessError> {
    state
        .lock()
        .map_err(|_| HarnessError::Transport("Codex background-state lock was poisoned".into()))
}

async fn run_inbound_dispatch<C, R>(
    mut client: C,
    mut frames: R,
    state: SharedBackgroundState,
    routes: RouteMap,
    signal_tx: mpsc::Sender<HarnessSignal>,
    workspace_root: PathBuf,
) -> Option<String>
where
    C: CodexTransport + Clone + 'static,
    R: CodexFrameReceiver,
{
    macro_rules! lock_state_or_stop {
        () => {
            match lock_background_state(&state) {
                Ok(state) => state,
                Err(error) => return Some(error.to_string()),
            }
        };
    }
    loop {
        match frames.next_message().await {
            Ok(Some(frame)) => {
                let (message, acknowledgement) = frame.into_parts();
                {
                    let mut state = lock_state_or_stop!();
                    observe_pending_context_restore(&mut state.pending_context_restores, &message);
                }
                match handle_background_server_message(
                    &mut client,
                    &state,
                    &routes,
                    &signal_tx,
                    message,
                )
                .await
                {
                    Ok(StreamOutcome::TurnEnded) => acknowledgement.acknowledge(Ok(())),
                    Ok(StreamOutcome::CompactionCompleted { thread, elapsed_ms }) => {
                        acknowledgement.acknowledge(Ok(()));
                        let pending_compactions = lock_state_or_stop!().pending_compactions.len();
                        info!(
                            %thread,
                            elapsed_ms,
                            pending_compactions,
                            "Codex context compaction completion observed"
                        );
                    }
                    Err(error) => {
                        acknowledgement.acknowledge(Err(error.clone()));
                        return Some(format!(
                            "Codex stream failed before turn completion: {error}"
                        ));
                    }
                }
            }
            Ok(None) => {
                let state = lock_state_or_stop!();
                if !state.pending_compactions.is_empty() {
                    warn!(
                        action = "read_codex_stream",
                        workspace_root = %workspace_root.display(),
                        pending_compactions = state.pending_compactions.len(),
                        pending_compaction_states = ?pending_compaction_states(&state.pending_compactions),
                        "Codex message stream ended with pending context compactions"
                    );
                }
                return Some("Codex stream ended before turn completion".into());
            }
            Err(CodexStreamError::NonJsonStdout {
                parse_error,
                raw_preview,
                raw_bytes,
            }) => {
                let state = lock_state_or_stop!();
                warn!(
                    active_turns = state.active_turns.len(),
                    pending_compactions = state.pending_compactions.len(),
                    pending_compaction_states = ?pending_compaction_states(&state.pending_compactions),
                    workspace_root = %workspace_root.display(),
                    error = %parse_error,
                    raw_bytes,
                    raw_preview = ?raw_preview,
                    "Ignoring non-JSON line from Codex app-server stdout"
                );
            }
            Err(CodexStreamError::Fatal(error)) => {
                let message = error.to_string();
                let state = lock_state_or_stop!();
                if state.active_turns.is_empty() {
                    warn!(
                        action = "read_codex_stream",
                        error = %message,
                        pending_compactions = state.pending_compactions.len(),
                        pending_compaction_states = ?pending_compaction_states(&state.pending_compactions),
                        workspace_root = %workspace_root.display(),
                        "Codex idle stream failed while background work was running"
                    );
                } else {
                    warn!(
                        action = "read_codex_stream",
                        error = %message,
                        active_turns = state.active_turns.len(),
                        active_turn_states = ?active_turn_states(&state.active_turns),
                        pending_compactions = state.pending_compactions.len(),
                        pending_compaction_states = ?pending_compaction_states(&state.pending_compactions),
                        workspace_root = %workspace_root.display(),
                        "Codex stream failed before all active turns completed"
                    );
                }
                return Some(format!(
                    "Codex stream failed before turn completion: {message}"
                ));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_command_worker<C>(
    mut client: C,
    mut commands: mpsc::Receiver<QueuedHarnessCommand>,
    mut controls: mpsc::Receiver<QueuedControlCommand>,
    state: SharedBackgroundState,
    routes: RouteMap,
    signal_tx: mpsc::Sender<HarnessSignal>,
    worker_queue: Arc<WorkerQueueWatchdog>,
    writable_roots: Vec<PathBuf>,
) where
    C: CodexTransport + Clone,
{
    loop {
        tokio::select! {
            biased;
            queued = controls.recv() => {
                let Some(queued) = queued else { break };
                worker_queue.mark_started(queued.token);
                let token = queued.token;
                if handle_control_command(&mut client, &state, &routes, queued.command)
                    .await
                    .is_err()
                {
                    worker_queue.mark_finished(token);
                    break;
                }
                worker_queue.mark_finished(token);
            }
            queued = commands.recv() => {
                let Some(queued) = queued else { break };
                worker_queue.mark_started(queued.token);
                handle_harness_command(
                    &mut client,
                    &state,
                    &routes,
                    &signal_tx,
                    &writable_roots,
                    queued.command,
                )
                .await;
                worker_queue.mark_finished(queued.token);
            }
        }
    }
}

async fn run_urgent_control_worker<C>(
    mut client: C,
    mut controls: mpsc::Receiver<QueuedControlCommand>,
    state: SharedBackgroundState,
    routes: RouteMap,
    worker_queue: Arc<WorkerQueueWatchdog>,
) where
    C: CodexTransport + Clone + 'static,
{
    while let Some(queued) = controls.recv().await {
        worker_queue.mark_started(queued.token);
        let token = queued.token;
        match queued.command {
            ControlCommand::Interrupt { thread, response } => {
                let native_turn_id = match lock_background_state(&state) {
                    Ok(state) => state
                        .mapper
                        .active_native_turn_for_thread(thread.thread)
                        .map(str::to_owned),
                    Err(error) => {
                        let _ = response.send(Err(error));
                        worker_queue.mark_finished(token);
                        break;
                    }
                };
                let Some(native_turn_id) = native_turn_id else {
                    let _ = response.send(Err(HarnessError::Unsupported(
                        "no active Codex turn to interrupt".into(),
                    )));
                    worker_queue.mark_finished(token);
                    continue;
                };
                let pending =
                    submit_interrupt_turn(&mut client, &thread.harness_thread_id, &native_turn_id)
                        .await;
                let pending = match pending {
                    Ok(pending) => pending,
                    Err(error) => {
                        let _ = response.send(Err(error));
                        worker_queue.mark_finished(token);
                        continue;
                    }
                };

                let mut completion_client = client.clone();
                let completion_state = state.clone();
                let completion_routes = routes.clone();
                let completion_queue = worker_queue.clone();
                tokio::spawn(async move {
                    let result = timeout_codex_control(
                        "interrupt",
                        Some(&thread),
                        None,
                        Some(&native_turn_id),
                        await_interrupt_response(
                            pending,
                            CodexOperationContext::for_thread("interrupt", &thread)
                                .with_native_turn_id(&native_turn_id),
                        ),
                    )
                    .await;
                    if result.is_ok() {
                        reject_pending_requests_for_interrupted_thread_ordered(
                            &mut completion_client,
                            &completion_state,
                            &completion_routes,
                            thread.thread,
                        )
                        .await;
                    }
                    let _ = response.send(result);
                    completion_queue.mark_finished(token);
                });
            }
            ControlCommand::TerminateCommand {
                thread,
                process_id,
                response,
            } => {
                let native_turn_id = {
                    let state = match lock_background_state(&state) {
                        Ok(state) => state,
                        Err(error) => {
                            let _ = response.send(Err(error));
                            worker_queue.mark_finished(token);
                            break;
                        }
                    };
                    state
                        .mapper
                        .native_turn_for_process(thread.thread, &process_id)
                        .or_else(|| state.mapper.active_native_turn_for_thread(thread.thread))
                        .map(str::to_owned)
                };
                let initial = submit_terminate_command(&mut client, &thread, &process_id).await;
                let initial = match initial {
                    Ok(initial) => initial,
                    Err(error) => {
                        let _ = response.send(Err(error));
                        worker_queue.mark_finished(token);
                        continue;
                    }
                };
                let mut completion_client = client.clone();
                let completion_queue = worker_queue.clone();
                tokio::spawn(async move {
                    let result = complete_submitted_termination(
                        &mut completion_client,
                        &thread,
                        &process_id,
                        initial,
                    )
                    .await;
                    if let Err(error) = &result {
                        warn!(
                            thread_id = %thread.thread,
                            harness_thread_id = %thread.harness_thread_id,
                            native_turn_id = display_opt(native_turn_id.as_deref()),
                            process_id,
                            %error,
                            "Codex command termination failed"
                        );
                    }
                    let _ = response.send(result);
                    completion_queue.mark_finished(token);
                });
            }
            command => {
                if handle_control_command(&mut client, &state, &routes, command)
                    .await
                    .is_err()
                {
                    worker_queue.mark_finished(token);
                    break;
                }
                worker_queue.mark_finished(token);
            }
        }
    }
}

async fn run_first_event_watchdog(state: SharedBackgroundState) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        let Ok(mut state) = lock_background_state(&state) else {
            return;
        };
        if !state.active_turns.is_empty() {
            warn_slow_first_events(&mut state.active_turns);
        }
    }
}

async fn finish_background_state(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    routes: &RouteMap,
    incomplete_message: Option<String>,
    deadline: tokio::time::Instant,
) {
    let active = {
        let mut state = match lock_background_state(state) {
            Ok(state) => state,
            Err(error) => {
                warn!(%error, "could not finalize poisoned Codex background state");
                return;
            }
        };
        let active_turns = std::mem::take(&mut state.active_turns);
        active_turns
            .into_iter()
            .map(|(thread_id, mut active)| {
                state.mapper.clear_active_turn(thread_id);
                let upload_dir = active.upload_dir.take();
                (active.thread, active.active_turn, upload_dir)
            })
            .collect::<Vec<_>>()
    };

    for (thread, _, upload_dir) in &active {
        if tokio::time::timeout_at(
            deadline,
            cleanup_codex_upload_dir(client, thread, upload_dir.as_ref()),
        )
        .await
        .is_err()
        {
            warn!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                "shutdown deadline reached while cleaning Codex upload directories"
            );
            break;
        }
    }
    if let Some(message) = incomplete_message {
        for (thread, turn, _) in active {
            if tokio::time::timeout_at(
                deadline,
                emit_incomplete_turn(routes, thread.thread, turn, message.clone()),
            )
            .await
            .is_err()
            {
                warn!(
                    thread_id = %thread.thread,
                    harness_thread_id = %thread.harness_thread_id,
                    "shutdown deadline reached while finalizing an incomplete Codex turn"
                );
                break;
            }
        }
    }
}

async fn handle_harness_command(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
    writable_roots: &[PathBuf],
    command: HarnessCommand,
) {
    match command {
        HarnessCommand::OpenThread { opts, response } => {
            let result = handle_open_thread(client, state, &opts, routes, signal_tx).await;
            if let (Some(generation), Err(error)) = (opts.identity_generation, &result)
                && !matches!(error, HarnessError::Timeout(_))
            {
                warn!(
                    action = "primary_identity_failed",
                    project_id = %opts.project,
                    thread_id = display_opt(opts.thread),
                    generation,
                    %error,
                    "Codex Primary identity operation failed definitively"
                );
                if let Some(thread_id) = opts.thread
                    && signal_tx
                        .send(HarnessSignal::PrimaryIdentityFailed {
                            thread_id,
                            generation,
                            error: error.to_string(),
                        })
                        .await
                        .is_err()
                {
                    warn!(
                        action = "primary_identity_failed",
                        project_id = %opts.project,
                        %thread_id,
                        generation,
                        "could not publish definitive Primary identity failure because the harness signal stream closed"
                    );
                }
            }
            let _ = response.send(result.map(|outcome| outcome.handle));
        }
        HarnessCommand::StartTurn {
            thread,
            input,
            overrides,
            response,
        } => {
            let result = handle_start_turn_ordered(
                client,
                state,
                &thread,
                &input,
                &overrides,
                writable_roots,
            )
            .await;
            let _ = response.send(result.map(|started| started.turn));
        }
    }
}

async fn wait_for_shutdown_request(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow_and_update() {
        return;
    }
    // A closed sender also means the harness owner disappeared, so the worker should tear down.
    let _ = shutdown.changed().await;
}

async fn shutdown_codex_transport<C>(
    client: C,
    workspace_root: &std::path::Path,
    deadline: tokio::time::Instant,
) where
    C: CodexTransport,
{
    let started = Instant::now();
    match client.shutdown_transport(deadline).await {
        Ok(()) => {
            info!(
                action = "shutdown_codex_transport",
                workspace_root = %workspace_root.display(),
                elapsed_ms = started.elapsed().as_millis(),
                "Codex transport shutdown completed"
            );
        }
        Err(error) => {
            warn!(
                action = "shutdown_codex_transport",
                workspace_root = %workspace_root.display(),
                error = %error,
                elapsed_ms = started.elapsed().as_millis(),
                "Codex transport shutdown failed"
            );
        }
    }
}

#[derive(Debug)]
struct ActiveTurn {
    thread: ThreadHandle,
    acknowledged_turn: TurnId,
    active_turn: Option<TurnId>,
    started_at: Instant,
    saw_server_message: bool,
    warned_no_server_message: bool,
    upload_dir: Option<PathBuf>,
}

impl ActiveTurn {
    fn new(thread: ThreadHandle, acknowledged_turn: TurnId) -> Self {
        Self {
            thread,
            acknowledged_turn,
            active_turn: Some(acknowledged_turn),
            started_at: Instant::now(),
            saw_server_message: false,
            warned_no_server_message: false,
            upload_dir: None,
        }
    }

    fn with_upload_dir(mut self, upload_dir: Option<PathBuf>) -> Self {
        self.upload_dir = upload_dir;
        self
    }

    fn mark_server_message(&mut self) {
        self.saw_server_message = true;
    }

    fn event_is_current_turn(&self, event: &AgentEvent) -> bool {
        agent_event_turn(event).is_none_or(|turn| turn == self.acknowledged_turn)
    }
}

type ActiveTurns = HashMap<ThreadId, ActiveTurn>;

fn active_turn_states(active_turns: &ActiveTurns) -> Vec<String> {
    active_turns
        .values()
        .map(|active| {
            format!(
                "thread_id={},harness_thread_id={},acknowledged_turn={},active_turn={}",
                active.thread.thread,
                active.thread.harness_thread_id,
                active.acknowledged_turn,
                active
                    .active_turn
                    .map_or_else(|| "none".into(), |turn| turn.to_string())
            )
        })
        .collect()
}

struct StartedTurn {
    turn: TurnId,
    #[allow(dead_code)]
    upload_dir: Option<PathBuf>,
}

fn fallback_thread(mapper: &CodexMapper, active_turns: &ActiveTurns) -> ThreadId {
    mapper
        .running_command_fallback_thread()
        .or_else(|| {
            (active_turns.len() == 1)
                .then(|| active_turns.keys().next().copied())
                .flatten()
        })
        .unwrap_or_default()
}

fn warn_slow_first_events(active_turns: &mut ActiveTurns) {
    for active in active_turns.values_mut() {
        if !active.saw_server_message
            && !active.warned_no_server_message
            && active.started_at.elapsed() >= TURN_FIRST_EVENT_WARN_AFTER
        {
            active.warned_no_server_message = true;
            warn!(
                thread_id = %active.thread.thread,
                harness_thread_id = %active.thread.harness_thread_id,
                acknowledged_turn = %active.acknowledged_turn,
                active_turn = display_opt(active.active_turn),
                elapsed_ms = active.started_at.elapsed().as_millis(),
                "Codex accepted turn/start but has not emitted a server message yet"
            );
        }
    }
}

fn completed_current_active_turn(
    active_turns: &ActiveTurns,
    event: &AgentEvent,
) -> Option<(ThreadId, TurnId)> {
    let AgentEvent::TurnCompleted { thread, turn, .. } = event else {
        return None;
    };
    let active = active_turns.get(thread)?;
    (*turn == active.acknowledged_turn).then_some((*thread, *turn))
}

async fn handle_background_server_message<C>(
    client: &mut C,
    state: &SharedBackgroundState,
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
    msg: codex_codes::ServerMessage,
) -> Result<StreamOutcome, HarnessError>
where
    C: CodexTransport + Clone + 'static,
{
    match msg {
        codex_codes::ServerMessage::Notification(notif) => {
            let native_thread_id = notification_native_thread_id(&notif);
            if let Some(native_thread_id) = native_thread_id.as_deref()
                && !native_thread_id.trim().is_empty()
            {
                let cause = ThreadActivationCause::Notification {
                    method: notif.method().to_owned(),
                };
                ensure_frame_route_ready(state, routes, signal_tx, native_thread_id, cause)
                    .await
                    .inspect_err(|error| {
                        warn!(
                            harness_thread_id = native_thread_id,
                            method = notif.method(),
                            %error,
                            "failed to activate native route before notification delivery"
                        );
                    })?;
            }

            let mapped = {
                let mut state = lock_background_state(state)?;
                let fallback_thread = fallback_thread(&state.mapper, &state.active_turns);
                let event = state.mapper.map_notification(&notif, fallback_thread);
                let Some(event) = event else {
                    if let Some(message) = mapping::fatal_turn_error(&notif) {
                        warn!(
                            action = "map_fatal_notification",
                            method = notif.method(),
                            fallback_thread = %fallback_thread,
                            error = %message,
                            "dropping fatal Codex error notification that could not be mapped to a known thread"
                        );
                    }
                    return Ok(StreamOutcome::TurnEnded);
                };
                let thread = event_thread(&event);
                if let Some(active) = state.active_turns.get_mut(&thread) {
                    active.mark_server_message();
                    if let AgentEvent::TurnStarted { turn, .. } = &event
                        && *turn == active.acknowledged_turn
                    {
                        active.active_turn = Some(*turn);
                    }
                }
                let completed_compaction =
                    observe_pending_compaction(&mut state.pending_compactions, thread, &event);
                let completed_active_turn =
                    completed_current_active_turn(&state.active_turns, &event)
                        .map(|(_, turn)| turn);
                if state.active_turns.contains_key(&thread)
                    && matches!(&event, AgentEvent::TurnCompleted { .. })
                    && completed_active_turn.is_none()
                {
                    debug!(
                        %thread,
                        acknowledged_turn = display_opt(state.active_turns.get(&thread).map(|active| active.acknowledged_turn)),
                        event_turn = display_opt(agent_event_turn(&event)),
                        "ignoring Codex turn completion for a non-current turn"
                    );
                }
                let fatal_completion = state.active_turns.get(&thread).and_then(|active| {
                    active
                        .event_is_current_turn(&event)
                        .then(|| {
                            mapping::fatal_turn_error(&notif)
                                .map(|message| (active.active_turn, message))
                        })
                        .flatten()
                });
                (
                    event,
                    thread,
                    completed_compaction,
                    completed_active_turn,
                    fatal_completion,
                )
            };
            let (event, thread, completed_compaction, completed_active_turn, fatal_completion) =
                mapped;
            route_event(routes, thread, event).await?;

            let mut cleanup = None;
            if let Some(turn) = completed_active_turn {
                let remaining_active_turns = {
                    let mut state = lock_background_state(state)?;
                    cleanup = state
                        .active_turns
                        .remove(&thread)
                        .map(|active| (active.thread, active.upload_dir));
                    state.mapper.clear_active_turn(thread);
                    state.active_turns.len()
                };
                debug!(
                    %thread,
                    %turn,
                    remaining_active_turns,
                    "Codex turn completion observed"
                );
            } else if let Some((turn, message)) = fatal_completion
                && emit_fatal_turn_completion(routes, thread, turn, message).await
            {
                let mut state = lock_background_state(state)?;
                cleanup = state
                    .active_turns
                    .remove(&thread)
                    .map(|active| (active.thread, active.upload_dir));
                state.mapper.clear_active_turn(thread);
            }
            if let Some((thread, upload_dir)) = cleanup {
                let mut cleanup_client = client.clone();
                tokio::spawn(async move {
                    cleanup_codex_upload_dir(&mut cleanup_client, &thread, upload_dir.as_ref())
                        .await;
                });
            }
            Ok(
                completed_compaction.map_or(StreamOutcome::TurnEnded, |elapsed_ms| {
                    StreamOutcome::CompactionCompleted { thread, elapsed_ms }
                }),
            )
        }
        codex_codes::ServerMessage::Request { id, request } => {
            let (native_thread_id, _) = server_request_native_scope(&request);
            if let Some(native_thread_id) = native_thread_id.as_deref()
                && !native_thread_id.trim().is_empty()
            {
                let cause = ThreadActivationCause::ServerRequest {
                    method: request.method().to_owned(),
                };
                ensure_frame_route_ready(state, routes, signal_tx, native_thread_id, cause)
                    .await
                    .inspect_err(|error| {
                        warn!(
                            harness_thread_id = native_thread_id,
                            method = request.method(),
                            request_id = %id,
                            %error,
                            "failed to activate native route before server-request delivery"
                        );
                    })?;
            }
            let event = {
                let mut state = lock_background_state(state)?;
                let fallback_thread = fallback_thread(&state.mapper, &state.active_turns);
                state
                    .mapper
                    .map_server_request(&id, &request, fallback_thread)
            };
            let Some(event) = event else {
                let mut response_client = client.clone();
                tokio::spawn(async move {
                    respond_unroutable_server_request(&mut response_client, &id, &request).await;
                });
                return Ok(StreamOutcome::TurnEnded);
            };
            let thread = event_thread(&event);
            {
                let mut state = lock_background_state(state)?;
                if let Some(active) = state.active_turns.get_mut(&thread) {
                    active.mark_server_message();
                }
            }
            route_event(routes, thread, event).await?;
            Ok(StreamOutcome::TurnEnded)
        }
    }
}

async fn ensure_frame_route_ready(
    state: &SharedBackgroundState,
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
    native_thread_id: &str,
    cause: ThreadActivationCause,
) -> Result<ClaimedNativeRoute, HarnessError> {
    let native_thread_id = native_thread_id.trim();
    let suggested_thread_id = lock_background_state(state)?
        .mapper
        .thread_for_native(native_thread_id)
        .unwrap_or_default();
    let route = claim_mapped_route(state, routes, native_thread_id, suggested_thread_id)?;
    activate_route(routes, signal_tx, &route, cause).await?;
    Ok(route)
}

fn claim_mapped_route(
    state: &SharedBackgroundState,
    routes: &RouteMap,
    native_thread_id: &str,
    suggested_thread_id: ThreadId,
) -> Result<ClaimedNativeRoute, HarnessError> {
    let route = claim_native_route(routes, native_thread_id, suggested_thread_id)?;
    lock_background_state(state)?
        .mapper
        .claim_thread(native_thread_id.to_owned(), route.thread_id)?;
    Ok(route)
}

async fn activate_primary_identity_response(
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
    intended_thread_id: ThreadId,
    native_thread_id: String,
    generation: u64,
    method: &'static str,
    reported_model: Option<ModelRef>,
) -> Result<(), HarnessError> {
    let route = claim_native_route(routes, &native_thread_id, intended_thread_id)?;
    if route.thread_id != intended_thread_id {
        return Err(HarnessError::Protocol(format!(
            "native thread {native_thread_id} is already bound to {}, not intended Primary {intended_thread_id}",
            route.thread_id
        )));
    }
    let cause = ThreadActivationCause::IdentityResponse {
        method: method.to_owned(),
        generation,
        reported_model,
    };
    activate_route(routes, signal_tx, &route, cause).await
}

fn primary_identity_failure_hook(
    signal_tx: mpsc::Sender<HarnessSignal>,
    thread_id: ThreadId,
    generation: u64,
    method: &'static str,
) -> transport::AbandonedErrorHook {
    Box::new(move |error| {
        Box::pin(async move {
            warn!(
                action = "primary_identity_failed",
                %thread_id,
                generation,
                method,
                %error,
                "abandoned Codex Primary identity operation failed definitively"
            );
            if signal_tx
                .send(HarnessSignal::PrimaryIdentityFailed {
                    thread_id,
                    generation,
                    error: error.to_string(),
                })
                .await
                .is_err()
            {
                warn!(
                    action = "primary_identity_failed",
                    %thread_id,
                    generation,
                    method,
                    "could not publish definitive Primary identity failure because the harness signal stream closed"
                );
            }
        })
    })
}

async fn activate_route(
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
    route: &ClaimedNativeRoute,
    cause: ThreadActivationCause,
) -> Result<(), HarnessError> {
    if lock_routes(routes)?
        .by_native
        .get(&route.harness_thread_id)
        .is_some_and(|entry| entry.ready)
    {
        return Ok(());
    }

    let (activation, readiness) = thread_activation(route.clone(), cause);
    signal_tx
        .send(HarnessSignal::Activate(activation))
        .await
        .map_err(|_| HarnessError::Transport("harness signal receiver closed".into()))?;
    readiness.await.map_err(|_| {
        HarnessError::Transport("thread activation acknowledgement dropped".into())
    })??;
    let mut routes = lock_routes(routes)?;
    let entry = routes
        .by_native
        .get_mut(&route.harness_thread_id)
        .filter(|entry| entry.route == *route)
        .ok_or_else(|| HarnessError::Protocol("stale or unknown native route".into()))?;
    if entry.receiver.is_some() {
        return Err(HarnessError::Protocol(format!(
            "native route {} was acknowledged before its receiver was claimed",
            route.harness_thread_id
        )));
    }
    entry.ready = true;
    Ok(())
}

async fn handle_control_command(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    routes: &RouteMap,
    control: ControlCommand,
) -> Result<StreamOutcome, HarnessError> {
    match control {
        ControlCommand::ClaimNativeThread {
            thread,
            harness_thread_id,
            workspace_root,
            response,
        } => {
            let route = match claim_native_route(routes, &harness_thread_id, thread) {
                Ok(route) => route,
                Err(error) => {
                    let _ = response.send(Err(error.clone()));
                    return Err(error);
                }
            };
            let mut state = match lock_background_state(state) {
                Ok(state) => state,
                Err(error) => {
                    let _ = response.send(Err(error.clone()));
                    return Err(error);
                }
            };
            let result = state
                .mapper
                .claim_thread(harness_thread_id.clone(), route.thread_id)
                .map(|()| ThreadHandle {
                    parent_harness_thread_id: state.mapper.native_parent(&harness_thread_id),
                    ..ThreadHandle::opened(route.thread_id, harness_thread_id, workspace_root)
                });
            let _ = response.send(result);
        }
        ControlCommand::RespondApproval {
            id,
            decision,
            response,
        } => {
            let result = handle_respond_approval_ordered(client, state, &id, &decision).await;
            let _ = response.send(result);
        }
        ControlCommand::RespondServerRequest {
            id,
            response_payload,
            response,
        } => {
            let result =
                handle_respond_server_request_ordered(client, state, routes, &id, response_payload)
                    .await;
            let _ = response.send(result);
        }
        ControlCommand::Interrupt { thread, response } => {
            warn!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                action = "interrupt",
                "interrupt reached the non-urgent Codex control dispatcher"
            );
            let _ = response.send(Err(HarnessError::Protocol(
                "interrupt was routed to the non-urgent Codex control dispatcher".into(),
            )));
        }
        ControlCommand::TerminateCommand {
            thread,
            process_id,
            response,
        } => {
            warn!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                process_id,
                action = "terminate_command",
                "command termination reached the non-urgent Codex control dispatcher"
            );
            let _ = response.send(Err(HarnessError::Protocol(
                "command termination was routed to the non-urgent Codex control dispatcher".into(),
            )));
        }
        ControlCommand::CompactThread { thread, response } => {
            let active = match lock_background_state(state) {
                Ok(state) => state.active_turns.contains_key(&thread.thread),
                Err(error) => {
                    let _ = response.send(Err(error.clone()));
                    return Err(error);
                }
            };
            let result = if active {
                Err(HarnessError::Unsupported(
                    "context compaction is not available during an active turn".into(),
                ))
            } else {
                handle_compact_thread_ordered(client, state, &thread).await
            };
            let _ = response.send(result);
        }
        ControlCommand::SetThreadName {
            thread,
            name,
            response,
        } => {
            let result = handle_set_thread_name(client, &thread, &name).await;
            let _ = response.send(result);
        }
        ControlCommand::SetThreadArchived {
            thread,
            archived,
            response,
        } => {
            let active = match lock_background_state(state) {
                Ok(state) => state.active_turns.contains_key(&thread.thread),
                Err(error) => {
                    let _ = response.send(Err(error.clone()));
                    return Err(error);
                }
            };
            let result = if active {
                Err(HarnessError::Unsupported(
                    "thread archiving is not available during an active turn".into(),
                ))
            } else {
                handle_set_thread_archived(client, &thread, archived).await
            };
            let _ = response.send(result);
        }
        ControlCommand::DeleteThread { thread, response } => {
            let active = match lock_background_state(state) {
                Ok(state) => state.active_turns.contains_key(&thread.thread),
                Err(error) => {
                    let _ = response.send(Err(error.clone()));
                    return Err(error);
                }
            };
            let result = if active {
                Err(HarnessError::Unsupported(
                    "thread deletion is not available during an active turn".into(),
                ))
            } else {
                handle_delete_thread(client, &thread).await
            };
            if result.is_ok() {
                if let Err(error) = remove_native_route(routes, thread.thread) {
                    let _ = response.send(Err(error.clone()));
                    return Err(error);
                }
                match lock_background_state(state) {
                    Ok(mut state) => state
                        .pending_context_restores
                        .remove(&thread.harness_thread_id),
                    Err(error) => {
                        let _ = response.send(Err(error.clone()));
                        return Err(error);
                    }
                };
            }
            let _ = response.send(result);
        }
        ControlCommand::ListMcpServers { response } => {
            let result = timeout_codex_control(
                "list_mcp_servers",
                None,
                None,
                None,
                handle_list_mcp_servers(client),
            )
            .await;
            let _ = response.send(result);
        }
        ControlCommand::ReloadMcpServers { response } => {
            let result = timeout_codex_control(
                "reload_mcp_servers",
                None,
                None,
                None,
                handle_reload_mcp_servers(client),
            )
            .await;
            let _ = response.send(result);
        }
        ControlCommand::StartMcpOauthLogin { name, response } => {
            let result = timeout_codex_control(
                "start_mcp_oauth_login",
                None,
                Some(&name),
                None,
                handle_start_mcp_oauth_login(client, &name),
            )
            .await;
            let _ = response.send(result);
        }
        ControlCommand::ListProviders { cwd, response } => {
            let result = timeout_codex_control(
                "list_providers",
                None,
                None,
                None,
                handle_list_providers(client, cwd),
            )
            .await;
            let _ = response.send(result);
        }
        ControlCommand::ListModels { cwd, response } => {
            let result = timeout_codex_control(
                "list_models",
                None,
                None,
                None,
                handle_list_models(client, cwd),
            )
            .await;
            let _ = response.send(result);
        }
    }
    Ok(StreamOutcome::TurnEnded)
}

async fn timeout_codex_control<T>(
    action: &'static str,
    thread: Option<&ThreadHandle>,
    process_id: Option<&str>,
    native_turn_id: Option<&str>,
    future: impl std::future::Future<Output = Result<T, HarnessError>>,
) -> Result<T, HarnessError> {
    let started = Instant::now();
    let result = future.await;
    if matches!(result, Err(HarnessError::Timeout(_))) {
        warn!(
            thread_id = display_opt(thread.map(|thread| thread.thread)),
            harness_thread_id = display_opt(thread.map(|thread| thread.harness_thread_id.as_str())),
            action,
            process_id = display_opt(process_id),
            native_turn_id = display_opt(native_turn_id),
            elapsed_ms = started.elapsed().as_millis(),
            timeout_ms = CODEX_JSON_RPC_TIMEOUT.as_millis(),
            "Codex control request timed out; worker will resume processing commands"
        );
    }
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamOutcome {
    TurnEnded,
    CompactionCompleted { thread: ThreadId, elapsed_ms: u128 },
}

#[derive(Debug)]
struct PendingCompaction {
    started_at: Instant,
    saw_turn_started: bool,
}

impl PendingCompaction {
    fn new(started_at: Instant) -> Self {
        Self {
            started_at,
            saw_turn_started: false,
        }
    }

    fn observe(&mut self, event: &AgentEvent) -> bool {
        match event {
            AgentEvent::TurnStarted { .. } => {
                self.saw_turn_started = true;
                false
            }
            AgentEvent::ItemCompleted { item, .. }
                if is_context_compaction_activity(item) && !self.saw_turn_started =>
            {
                true
            }
            AgentEvent::TurnCompleted { .. } => true,
            _ => false,
        }
    }
}

fn observe_pending_compaction(
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    thread: ThreadId,
    event: &AgentEvent,
) -> Option<u128> {
    let event_name = compaction_event_name(event)?;
    let event_turn = agent_event_turn(event);
    let pending = pending_compactions.get_mut(&thread)?;
    let saw_turn_started_before = pending.saw_turn_started;
    let elapsed_ms = pending.started_at.elapsed().as_millis();
    let completed = pending.observe(event);
    info!(
        %thread,
        event_turn = display_opt(event_turn),
        event = event_name,
        saw_turn_started_before,
        saw_turn_started_after = pending.saw_turn_started,
        completed,
        elapsed_ms,
        "observed Codex context compaction event"
    );
    if !completed {
        return None;
    }
    pending_compactions
        .remove(&thread)
        .map(|pending| pending.started_at.elapsed().as_millis())
}

fn compaction_event_name(event: &AgentEvent) -> Option<&'static str> {
    match event {
        AgentEvent::TurnStarted { .. } => Some("turn_started"),
        AgentEvent::ItemCompleted { item, .. } if is_context_compaction_activity(item) => {
            Some("context_compacted_item")
        }
        AgentEvent::TurnCompleted { .. } => Some("turn_completed"),
        _ => None,
    }
}

fn agent_event_turn(event: &AgentEvent) -> Option<TurnId> {
    match event {
        AgentEvent::TurnStarted { turn, .. }
        | AgentEvent::ContextWindowUpdated { turn, .. }
        | AgentEvent::ItemStarted { turn, .. }
        | AgentEvent::ItemDelta { turn, .. }
        | AgentEvent::ItemCompleted { turn, .. }
        | AgentEvent::DiffUpdated { turn, .. }
        | AgentEvent::ApprovalRequested { turn, .. }
        | AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
        AgentEvent::ServerRequestReceived { turn, .. }
        | AgentEvent::ServerRequestResolved { turn, .. }
        | AgentEvent::Error { turn, .. }
        | AgentEvent::Notice { turn, .. } => *turn,
        AgentEvent::ThreadOpened { .. } => None,
    }
}

fn pending_compaction_states(
    pending_compactions: &HashMap<ThreadId, PendingCompaction>,
) -> Vec<String> {
    pending_compactions
        .iter()
        .map(|(thread, pending)| {
            format!(
                "{thread}:saw_turn_started={},elapsed_ms={}",
                pending.saw_turn_started,
                pending.started_at.elapsed().as_millis()
            )
        })
        .collect()
}

fn is_context_compaction_activity(item: &giskard_core::item::Item) -> bool {
    matches!(
        &item.payload,
        giskard_core::item::ItemPayload::Activity { title, .. } if title == "Context compacted"
    )
}

async fn handle_open_thread(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    opts: &OpenThreadOptions,
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
) -> Result<OpenThreadOutcome, HarnessError> {
    let cwd = opts.workspace_root.to_string_lossy().to_string();
    // An explicit id wins — the caller knows this thread's durable identity. Otherwise, if the
    // native thread being resumed is already bound, reuse that binding rather than inventing an
    // id: a caller passing `None` is saying it has no opinion, not that this is a new thread, and
    // minting here would give one thread two identities for everything downstream to reconcile.
    let thread_id = if let Some(thread) = opts.thread {
        thread
    } else if let Some(native) = opts.resume.as_deref() {
        lock_background_state(state)?
            .mapper
            .thread_for_native(native)
            .unwrap_or_default()
    } else {
        ThreadId::default()
    };

    // Track whether resume-by-id failed and we fell back to a fresh native thread (C5), so we can
    // warn the caller that agent context was lost while keeping the Giskard-side history.
    let mut resume_warning = None;

    let opened = if let Some(ref resume_id) = opts.resume {
        let context = CodexOperationContext::for_project("thread_resume", opts.project)
            .with_thread_id(thread_id)
            .with_harness_thread_id(resume_id);
        match resume_thread(
            client,
            state,
            context,
            resume_id,
            &cwd,
            opts.initial_model.as_ref(),
            thread_id,
            opts.identity_generation,
            routes,
            signal_tx,
            opts.updates.clone(),
        )
        .await
        {
            Ok(opened) => opened,
            // Recovery needs a model to start on, and importing a thread by native id supplies
            // none — the model was the resumed thread's to report. Nothing sensible to start.
            Err(error) if opts.initial_model.is_none() => return Err(error),
            Err(e @ HarnessError::ProviderRejected { .. }) => {
                // C5: Codex thread store purged/rotated. Start fresh instead of hard-failing.
                resume_warning = Some(HarnessNotice {
                    code: "codex_resume_failed".into(),
                    message:
                        "Agent context was lost; started a fresh Codex session. History is intact."
                            .into(),
                    detail: Some(e.to_string()),
                });
                let context = CodexOperationContext::for_project(
                    "thread_start_after_resume_failed",
                    opts.project,
                )
                .with_thread_id(thread_id);
                start_thread(
                    client,
                    state,
                    context,
                    &cwd,
                    &fresh_model(opts)?,
                    thread_id,
                    opts.identity_generation,
                    routes,
                    signal_tx,
                )
                .await?
            }
            Err(error) => return Err(error),
        }
    } else {
        let context = CodexOperationContext::for_project("thread_start", opts.project)
            .with_thread_id(thread_id);
        start_thread(
            client,
            state,
            context,
            &cwd,
            &fresh_model(opts)?,
            thread_id,
            opts.identity_generation,
            routes,
            signal_tx,
        )
        .await?
    };

    let route = claim_mapped_route(state, routes, &opened.harness_thread_id, thread_id)?;
    if route.thread_id != thread_id {
        return Err(HarnessError::Protocol(format!(
            "native thread {} is already bound to {}, not intended Primary {thread_id}",
            opened.harness_thread_id, route.thread_id
        )));
    }
    route_event(
        routes,
        thread_id,
        AgentEvent::ThreadOpened {
            thread: thread_id,
            harness_thread_id: opened.harness_thread_id.clone(),
        },
    )
    .await?;

    if let Some(warning) = &resume_warning {
        let message = warning.message.clone();
        route_event(
            routes,
            thread_id,
            AgentEvent::Error {
                thread: thread_id,
                turn: None,
                error: HarnessError::Transport(message),
            },
        )
        .await?;
    }

    Ok(OpenThreadOutcome {
        handle: ThreadHandle {
            warning: resume_warning,
            resumed_model: opened.model,
            agent_name: opened.agent_name,
            parent_harness_thread_id: opened.parent_harness_thread_id,
            ..ThreadHandle::opened(
                thread_id,
                opened.harness_thread_id,
                opts.workspace_root.clone(),
            )
        },
    })
}

fn observe_pending_context_restore(
    pending: &mut HashMap<String, PendingContextRestore>,
    message: &codex_codes::ServerMessage,
) {
    let codex_codes::ServerMessage::Notification(
        codex_codes::messages::Notification::ThreadTokenUsageUpdated(notification),
    ) = message
    else {
        return;
    };
    if !pending.contains_key(&notification.thread_id) {
        return;
    }
    if notification.turn_id.is_empty() {
        debug!(harness_thread_id = %notification.thread_id,
            "pending context-window restore ignored usage without a turn id");
        return;
    }
    let Some(reported) = notification.token_usage.model_context_window else {
        debug!(harness_thread_id = %notification.thread_id,
            native_turn_id = %notification.turn_id,
            "pending context-window restore observed usage without a context window");
        return;
    };
    let Ok(context_window) = u32::try_from(reported) else {
        debug!(harness_thread_id = %notification.thread_id,
            native_turn_id = %notification.turn_id, reported,
            "pending context-window restore rejected an out-of-range context window");
        return;
    };
    if context_window == 0 {
        debug!(harness_thread_id = %notification.thread_id,
            native_turn_id = %notification.turn_id,
            "pending context-window restore rejected a zero context window");
        return;
    }
    let Some(restore) = pending.remove(&notification.thread_id) else {
        return;
    };
    let update = ThreadUpdate::ContextWindowRestored {
        model: restore.model,
        context_window,
    };
    if restore.sink.send(update).is_err() {
        warn!(
            action = "forward_resumed_context_window",
            thread_id = %restore.thread,
            harness_thread_id = %notification.thread_id,
            native_turn_id = %notification.turn_id,
            cause = "thread update receiver closed",
            "failed to forward resumed context window"
        );
    }
}

struct OpenedNativeThread {
    harness_thread_id: String,
    model: Option<giskard_core::model::ModelRef>,
    agent_name: Option<String>,
    parent_harness_thread_id: Option<String>,
}

/// The model/provider a `thread/start` / `thread/resume` response reports as effective. Codex can
/// intentionally ignore resume overrides for an already-loaded thread while still answering
/// success, so callers switching providers must compare this against what they requested (see
/// `specs/model-provider-switching-analysis.md`). Empty response fields (older servers) yield
/// `None`.
///
/// `reasoning_effort` is the thread's, so a reported one wins over the request: `thread/resume`
/// answers with the effort the thread is actually on, and an import names no effort at all.
/// `thread/start` does not report one, and there the request is the only source.
fn effective_model(
    model: &str,
    model_provider: &str,
    reported_effort: Option<giskard_core::model::Effort>,
    requested: Option<&giskard_core::model::ModelRef>,
    context: CodexOperationContext<'_>,
    reported_harness_thread_id: &str,
) -> Option<giskard_core::model::ModelRef> {
    let effective = identity_response_model(model, model_provider, reported_effort, requested);
    if effective.is_none() {
        // An import has no requested model to fall back on, so this is the difference between a
        // thread whose model Codex declined to report and one Giskard dropped on the floor. The
        // caller turns it into a refusal; without this the refusal has no cause attached.
        warn!(
            model_empty = model.is_empty(),
            model_provider_empty = model_provider.is_empty(),
            requested = display_opt(requested.map(giskard_core::model::ModelRef::key)),
            action = context.action,
            project_id = display_opt(context.project_id),
            thread_id = display_opt(context.thread_id),
            harness_thread_id = reported_harness_thread_id,
            "Codex reported no effective model for the opened thread"
        );
    }
    effective
}

fn identity_response_model(
    model: &str,
    model_provider: &str,
    reported_effort: Option<giskard_core::model::Effort>,
    requested: Option<&giskard_core::model::ModelRef>,
) -> Option<giskard_core::model::ModelRef> {
    if model.is_empty() || model_provider.is_empty() {
        return None;
    }
    Some(giskard_core::model::ModelRef {
        provider: model_provider.to_owned(),
        model: model.to_owned(),
        reasoning_effort: reported_effort
            .or_else(|| requested.and_then(|model| model.reasoning_effort.clone())),
    })
}

#[allow(clippy::too_many_arguments)]
async fn resume_thread(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    context: CodexOperationContext<'_>,
    resume_id: &str,
    cwd: &str,
    model: Option<&giskard_core::model::ModelRef>,
    intended_thread_id: ThreadId,
    identity_generation: Option<u64>,
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
    updates: giskard_harness::ThreadUpdateSink,
) -> Result<OpenedNativeThread, HarnessError> {
    // `model`/`modelProvider` are overrides, not preferences: Codex stops applying the thread's
    // own persisted model the moment either is present (`merge_persisted_resume_metadata` returns
    // early on `has_model_resume_override`). Omitting them is how a caller says "keep whatever this
    // thread was already using".
    let params = codex_codes::ThreadResumeParams {
        thread_id: resume_id.to_owned(),
        cwd: Some(cwd.to_owned()),
        model: model.map(|model| model.model.clone()),
        model_provider: model.map(|model| model.provider.clone()),
        ..Default::default()
    };
    let hook = {
        let routes = routes.clone();
        let signal_tx = signal_tx.clone();
        let state = state.clone();
        let requested_model = model.cloned();
        Box::new(move |value: serde_json::Value| {
            Box::pin(async move {
                let response: codex_codes::ThreadResumeResponse =
                    serde_json::from_value(value.clone())
                        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
                let reported_model = identity_response_model(
                    &response.model,
                    &response.model_provider,
                    response
                        .reasoning_effort
                        .as_ref()
                        .map(|effort| giskard_core::model::Effort::new(effort.0.clone())),
                    requested_model.as_ref(),
                );
                if let Some(generation) = identity_generation {
                    activate_primary_identity_response(
                        &routes,
                        &signal_tx,
                        intended_thread_id,
                        response.thread.id.clone(),
                        generation,
                        codex_codes::protocol::methods::THREAD_RESUME,
                        reported_model.clone(),
                    )
                    .await?;
                }
                claim_mapped_route(&state, &routes, &response.thread.id, intended_thread_id)?;
                let mut state = lock_background_state(&state)?;
                if let Some(model) = reported_model {
                    let replaced = state.pending_context_restores.insert(
                        response.thread.id.clone(),
                        PendingContextRestore {
                            thread: intended_thread_id,
                            model,
                            sink: updates,
                        },
                    );
                    if let Some(replaced) = replaced {
                        warn!(
                            thread_id = %intended_thread_id,
                            replaced_thread_id = %replaced.thread,
                            harness_thread_id = %response.thread.id,
                            "replaced an overlapping pending context restore"
                        );
                    }
                }
                Ok(value)
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send>>
        }) as transport::SuccessResponseHook
    };
    let resp: codex_codes::ThreadResumeResponse = codex_request_with_hook(
        client,
        context,
        codex_codes::protocol::methods::THREAD_RESUME,
        &params,
        hook,
        transport::CorrelationLifetime::Retained,
        identity_generation.map(|generation| {
            primary_identity_failure_hook(
                signal_tx.clone(),
                intended_thread_id,
                generation,
                codex_codes::protocol::methods::THREAD_RESUME,
            )
        }),
    )
    .await?;
    let resumed = effective_model(
        &resp.model,
        &resp.model_provider,
        resp.reasoning_effort
            .as_ref()
            .map(|effort| giskard_core::model::Effort::new(effort.0.clone())),
        model,
        context,
        &resp.thread.id,
    );
    Ok(OpenedNativeThread {
        harness_thread_id: resp.thread.id,
        model: resumed,
        agent_name: resp
            .thread
            .agent_nickname
            .as_deref()
            .and_then(trimmed_non_empty)
            .map(ToOwned::to_owned),
        parent_harness_thread_id: resp
            .thread
            .parent_thread_id
            .as_deref()
            .and_then(trimmed_non_empty)
            .map(ToOwned::to_owned),
    })
}

/// The model a *fresh* native thread starts on. Unlike a resume, there is no existing thread whose
/// model Codex could report, so the caller has to have named one.
fn fresh_model(opts: &OpenThreadOptions) -> Result<giskard_core::model::ModelRef, HarnessError> {
    opts.initial_model
        .clone()
        .ok_or_else(|| HarnessError::Protocol("starting a new thread requires a model".into()))
}

#[allow(clippy::too_many_arguments)]
async fn start_thread(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    context: CodexOperationContext<'_>,
    cwd: &str,
    initial_model: &giskard_core::model::ModelRef,
    intended_thread_id: ThreadId,
    identity_generation: Option<u64>,
    routes: &RouteMap,
    signal_tx: &mpsc::Sender<HarnessSignal>,
) -> Result<OpenedNativeThread, HarnessError> {
    let params = codex_codes::ThreadStartParams {
        cwd: Some(cwd.to_owned()),
        model: Some(initial_model.model.clone()),
        model_provider: Some(initial_model.provider.clone()),
        ..Default::default()
    };
    let hook = {
        let routes = routes.clone();
        let signal_tx = signal_tx.clone();
        let state = state.clone();
        let requested_model = initial_model.clone();
        Box::new(move |value: serde_json::Value| {
            Box::pin(async move {
                let response: codex_codes::ThreadStartResponse =
                    serde_json::from_value(value.clone())
                        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
                let reported_model = identity_response_model(
                    &response.model,
                    &response.model_provider,
                    None,
                    Some(&requested_model),
                );
                if let Some(generation) = identity_generation {
                    activate_primary_identity_response(
                        &routes,
                        &signal_tx,
                        intended_thread_id,
                        response.thread.id.clone(),
                        generation,
                        codex_codes::protocol::methods::THREAD_START,
                        reported_model,
                    )
                    .await?;
                }
                claim_mapped_route(&state, &routes, &response.thread.id, intended_thread_id)?;
                Ok(value)
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send>>
        }) as transport::SuccessResponseHook
    };
    let resp: codex_codes::ThreadStartResponse = codex_request_with_hook(
        client,
        context,
        codex_codes::protocol::methods::THREAD_START,
        &params,
        hook,
        transport::CorrelationLifetime::Retained,
        identity_generation.map(|generation| {
            primary_identity_failure_hook(
                signal_tx.clone(),
                intended_thread_id,
                generation,
                codex_codes::protocol::methods::THREAD_START,
            )
        }),
    )
    .await?;
    let started = effective_model(
        &resp.model,
        &resp.model_provider,
        None,
        Some(initial_model),
        context,
        &resp.thread.id,
    );
    Ok(OpenedNativeThread {
        harness_thread_id: resp.thread.id,
        model: started,
        agent_name: resp
            .thread
            .agent_nickname
            .as_deref()
            .and_then(trimmed_non_empty)
            .map(ToOwned::to_owned),
        parent_harness_thread_id: resp
            .thread
            .parent_thread_id
            .as_deref()
            .and_then(trimmed_non_empty)
            .map(ToOwned::to_owned),
    })
}

#[cfg(test)]
async fn handle_start_turn(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    thread: &ThreadHandle,
    input: &UserInput,
    overrides: &TurnOverrides,
    writable_roots: &[PathBuf],
) -> Result<StartedTurn, HarnessError> {
    let prepared = prepare_user_input_for_codex_uploads(client, thread, input).await?;
    let params = match build_turn_start_params(thread, &prepared.input, overrides, writable_roots) {
        Ok(params) => params,
        Err(error) => {
            cleanup_codex_upload_dir(client, thread, prepared.upload_dir.as_ref()).await;
            return Err(error);
        }
    };
    let resp: codex_codes::TurnStartResponse = match codex_request(
        client,
        CodexOperationContext::for_thread("turn_start", thread),
        codex_codes::protocol::methods::TURN_START,
        &params,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            cleanup_codex_upload_dir(client, thread, prepared.upload_dir.as_ref()).await;
            return Err(error);
        }
    };

    let turn = if let Some(model) = overrides.model.clone() {
        mapper.register_active_turn_with_model(thread.thread, &resp.turn.id, model)
    } else {
        mapper.register_active_turn(thread.thread, &resp.turn.id)
    };
    match turn {
        Some(turn) => Ok(StartedTurn {
            turn,
            upload_dir: prepared.upload_dir,
        }),
        None => {
            cleanup_codex_upload_dir(client, thread, prepared.upload_dir.as_ref()).await;
            Err(HarnessError::Protocol(
                "turn/start response did not include a turn id".into(),
            ))
        }
    }
}

async fn handle_start_turn_ordered(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    thread: &ThreadHandle,
    input: &UserInput,
    overrides: &TurnOverrides,
    writable_roots: &[PathBuf],
) -> Result<StartedTurn, HarnessError> {
    let prepared = prepare_user_input_for_codex_uploads(client, thread, input).await?;
    let params = match build_turn_start_params(thread, &prepared.input, overrides, writable_roots) {
        Ok(params) => params,
        Err(error) => {
            cleanup_codex_upload_dir(client, thread, prepared.upload_dir.as_ref()).await;
            return Err(error);
        }
    };
    let state_for_hook = state.clone();
    let thread_for_hook = thread.clone();
    let model_for_hook = overrides.model.clone();
    let upload_dir_for_hook = prepared.upload_dir.clone();
    let hook: transport::SuccessResponseHook = Box::new(move |value| {
        Box::pin(async move {
            let response: codex_codes::TurnStartResponse = serde_json::from_value(value.clone())
                .map_err(|error| HarnessError::Protocol(error.to_string()))?;
            let mut state = lock_background_state(&state_for_hook)?;
            let turn = if let Some(model) = model_for_hook {
                state.mapper.register_active_turn_with_model(
                    thread_for_hook.thread,
                    &response.turn.id,
                    model,
                )
            } else {
                state
                    .mapper
                    .register_active_turn(thread_for_hook.thread, &response.turn.id)
            }
            .ok_or_else(|| {
                HarnessError::Protocol("turn/start response did not include a turn id".into())
            })?;
            state.active_turns.insert(
                thread_for_hook.thread,
                ActiveTurn::new(thread_for_hook, turn).with_upload_dir(upload_dir_for_hook),
            );
            Ok(value)
        })
    });
    let _response: codex_codes::TurnStartResponse = match codex_request_with_hook(
        client,
        CodexOperationContext::for_thread("turn_start", thread),
        codex_codes::protocol::methods::TURN_START,
        &params,
        hook,
        transport::CorrelationLifetime::Retained,
        None,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            cleanup_codex_upload_dir(client, thread, prepared.upload_dir.as_ref()).await;
            return Err(error);
        }
    };
    let turn = lock_background_state(state)?
        .active_turns
        .get(&thread.thread)
        .map(|active| active.acknowledged_turn)
        .ok_or_else(|| {
            HarnessError::Protocol(
                "turn/start hook completed without registering the active turn".into(),
            )
        })?;
    Ok(StartedTurn {
        turn,
        upload_dir: prepared.upload_dir,
    })
}

struct PreparedUserInput {
    input: UserInput,
    upload_dir: Option<PathBuf>,
}

async fn prepare_user_input_for_codex_uploads(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    input: &UserInput,
) -> Result<PreparedUserInput, HarnessError> {
    let UserInput::Text { text, attachments } = input;
    if attachments.is_empty() {
        return Ok(PreparedUserInput {
            input: input.clone(),
            upload_dir: None,
        });
    }

    let mut prepared_text = text.clone();
    let mut image_attachments = Vec::new();
    let mut uploaded_files = Vec::new();
    let upload_dir = codex_upload_dir(thread);
    let mut ensured_upload_dir = false;

    let upload_result: Result<(), HarnessError> = async {
        for (index, attachment) in attachments.iter().enumerate() {
            match attachment.kind {
                AttachmentKind::Image => image_attachments.push(attachment.clone()),
                AttachmentKind::File => {
                    if !ensured_upload_dir {
                        let params = codex_codes::FsCreateDirectoryParams {
                            path: serde_json::json!(upload_dir.to_string_lossy()),
                            recursive: Some(true),
                        };
                        let _: codex_codes::FsCreateDirectoryResponse = codex_request(
                            client,
                            CodexOperationContext::for_thread("upload_attachment_mkdir", thread),
                            codex_codes::protocol::methods::FS_CREATEDIRECTORY,
                            &params,
                        )
                        .await?;
                        ensured_upload_dir = true;
                    }
                    let path = codex_upload_path(&upload_dir, index, attachment);
                    let path_string = path.to_string_lossy().to_string();
                    let params = codex_codes::FsWriteFileParams {
                        data_base64: attachment.data_base64.clone(),
                        path: serde_json::json!(path_string),
                    };
                    let _: codex_codes::FsWriteFileResponse = codex_request(
                        client,
                        CodexOperationContext::for_thread("upload_attachment_write", thread),
                        codex_codes::protocol::methods::FS_WRITEFILE,
                        &params,
                    )
                    .await?;
                    uploaded_files.push((
                        safe_upload_file_name(&attachment.name),
                        path.to_string_lossy().to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = upload_result {
        cleanup_codex_upload_dir(client, thread, Some(&upload_dir)).await;
        return Err(error);
    }

    if !uploaded_files.is_empty() {
        if !prepared_text.trim().is_empty() {
            prepared_text.push_str("\n\n");
        }
        prepared_text.push_str("Attached files available on the harness host:\n");
        for (name, path) in uploaded_files {
            prepared_text.push_str("- ");
            prepared_text.push_str(&name);
            prepared_text.push_str(": ");
            prepared_text.push_str(&path);
            prepared_text.push('\n');
        }
    }

    Ok(PreparedUserInput {
        input: UserInput::text_with_attachments(prepared_text, image_attachments),
        upload_dir: ensured_upload_dir.then_some(upload_dir),
    })
}

#[cfg(test)]
async fn cleanup_active_turn_upload(
    client: &mut dyn CodexTransport,
    active_turns: &mut ActiveTurns,
    thread_id: ThreadId,
) {
    let Some(active) = active_turns.get_mut(&thread_id) else {
        return;
    };
    let upload_dir = active.upload_dir.take();
    cleanup_codex_upload_dir(client, &active.thread, upload_dir.as_ref()).await;
}

#[cfg(test)]
async fn cleanup_all_active_turn_uploads(
    client: &mut dyn CodexTransport,
    active_turns: &mut ActiveTurns,
) {
    let thread_ids: Vec<ThreadId> = active_turns.keys().copied().collect();
    for thread_id in thread_ids {
        cleanup_active_turn_upload(client, active_turns, thread_id).await;
    }
}

async fn cleanup_codex_upload_dir(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    upload_dir: Option<&PathBuf>,
) {
    let Some(upload_dir) = upload_dir else {
        return;
    };
    let params = codex_codes::FsRemoveParams {
        path: serde_json::json!(upload_dir.to_string_lossy()),
        recursive: Some(true),
        force: Some(true),
    };
    if let Err(error) = codex_request::<_, codex_codes::FsRemoveResponse>(
        client,
        CodexOperationContext::for_thread("upload_attachment_cleanup", thread),
        codex_codes::protocol::methods::FS_REMOVE,
        &params,
    )
    .await
    {
        warn!(
            thread_id = %thread.thread,
            harness_thread_id = %thread.harness_thread_id,
            path = %upload_dir.display(),
            error = %error,
            "failed to remove Codex attachment upload directory"
        );
    }
}

fn codex_upload_dir(thread: &ThreadHandle) -> PathBuf {
    let mut rng = rand::thread_rng();
    let nonce_high = rng.next_u64();
    let nonce_low = rng.next_u64();
    std::env::temp_dir()
        .join(CODEX_UPLOAD_DIR_NAME)
        .join(format!(
            "{}-{:016x}{:016x}",
            thread.thread, nonce_high, nonce_low
        ))
}

fn codex_upload_path(dir: &std::path::Path, index: usize, attachment: &UserAttachment) -> PathBuf {
    let mut nonce = [0_u8; 8];
    rand::thread_rng().fill_bytes(&mut nonce);
    dir.join(format!(
        "{index:02}-{}-{}",
        u64::from_le_bytes(nonce),
        safe_upload_file_name(&attachment.name)
    ))
}

fn safe_upload_file_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches(|ch| ch == '.' || ch == '_').trim();
    if trimmed.is_empty() {
        "attachment".into()
    } else {
        trimmed.chars().take(96).collect()
    }
}

fn build_turn_start_params(
    thread: &ThreadHandle,
    input: &UserInput,
    overrides: &TurnOverrides,
    configured_writable_roots: &[PathBuf],
) -> Result<serde_json::Value, HarnessError> {
    let codex_input = mapping::map_user_input(input);
    let codex_approval_policy =
        mapping::map_permission_preset_to_codex_approval(overrides.permission_preset);
    let permissions =
        mapping::map_permission_preset_to_codex_permissions(overrides.permission_preset);
    let effort = overrides
        .model
        .as_ref()
        .and_then(|m| m.reasoning_effort.clone())
        .map(mapping::map_effort);

    let mut params = serde_json::json!({
        "threadId": thread.harness_thread_id,
        "input": codex_input,
        "approvalPolicy": codex_approval_policy,
        "permissions": permissions,
    });
    let Some(map) = params.as_object_mut() else {
        return Err(HarnessError::Protocol(
            "turn/start params must serialize as an object".into(),
        ));
    };

    if overrides.permission_preset == PermissionPreset::AutoApprove {
        map.insert(
            "runtimeWorkspaceRoots".into(),
            serde_json::to_value(runtime_workspace_roots(thread, configured_writable_roots))
                .map_err(|error| HarnessError::Protocol(error.to_string()))?,
        );
    }

    if let Some(model) = overrides.model.as_ref() {
        map.insert("model".into(), serde_json::json!(model.model));
        if let Some(effort) = effort.as_ref() {
            map.insert("effort".into(), serde_json::json!(effort));
        }
        map.insert(
            "collaborationMode".into(),
            serde_json::json!({
                "mode": mapping::map_mode_to_collaboration_mode(overrides.mode),
                "settings": {
                    "model": model.model,
                    "reasoning_effort": effort,
                    "developer_instructions": null,
                }
            }),
        );
    }

    Ok(params)
}

fn runtime_workspace_roots(
    thread: &ThreadHandle,
    configured_writable_roots: &[PathBuf],
) -> Vec<PathBuf> {
    let mut roots = Vec::with_capacity(configured_writable_roots.len() + 1);
    roots.push(thread.workspace_root.clone());
    roots.extend(configured_writable_roots.iter().cloned());
    roots.sort();
    roots.dedup();
    roots
}

async fn route_event(
    routes: &RouteMap,
    thread: ThreadId,
    event: AgentEvent,
) -> Result<(), HarnessError> {
    let event_tx = event_sender_for_thread(routes, thread)?.ok_or_else(|| {
        HarnessError::Protocol(format!("no native event route exists for thread {thread}"))
    })?;
    event_tx.send(event).await.map_err(|_| {
        HarnessError::Transport(format!("native event route for thread {thread} is closed"))
    })
}

fn lock_routes(routes: &RouteMap) -> Result<StdMutexGuard<'_, NativeRoutes>, HarnessError> {
    routes
        .lock()
        .map_err(|_| HarnessError::Transport("Codex route map lock was poisoned".into()))
}

fn event_sender_for_thread(
    routes: &RouteMap,
    thread: ThreadId,
) -> Result<Option<mpsc::Sender<AgentEvent>>, HarnessError> {
    let routes = lock_routes(routes)?;
    let Some(native) = routes.native_by_thread.get(&thread) else {
        return Ok(None);
    };
    Ok(routes
        .by_native
        .get(native)
        .map(|entry| entry.event_tx.clone()))
}

fn route_for_native(
    routes: &RouteMap,
    native_thread_id: &str,
) -> Result<Option<ClaimedNativeRoute>, HarnessError> {
    Ok(lock_routes(routes)?
        .by_native
        .get(native_thread_id)
        .map(|entry| entry.route.clone()))
}

fn remove_native_route(routes: &RouteMap, thread_id: ThreadId) -> Result<(), HarnessError> {
    let mut routes = lock_routes(routes)?;
    if let Some(native_id) = routes.native_by_thread.remove(&thread_id) {
        routes.by_native.remove(&native_id);
    }
    Ok(())
}

#[cfg(test)]
fn install_native_route(routes: &RouteMap, route: ClaimedNativeRoute) -> Result<(), HarnessError> {
    let mut routes = lock_routes(routes)?;
    if let Some(existing) = routes.by_native.get(&route.harness_thread_id) {
        return (existing.route == route).then_some(()).ok_or_else(|| {
            HarnessError::Protocol(format!(
                "native route {} conflicts with its existing route",
                route.harness_thread_id
            ))
        });
    }
    if let Some(existing) = routes.native_by_thread.get(&route.thread_id) {
        return Err(HarnessError::Protocol(format!(
            "thread {} already has native route {existing}",
            route.thread_id
        )));
    }
    let (event_tx, receiver) = mpsc::channel(ROUTE_CAPACITY);
    routes.next_route_epoch = routes.next_route_epoch.max(route.route_epoch);
    routes
        .native_by_thread
        .insert(route.thread_id, route.harness_thread_id.clone());
    routes.by_native.insert(
        route.harness_thread_id.clone(),
        NativeRouteEntry {
            route,
            event_tx,
            receiver: Some(receiver),
            ready: false,
        },
    );
    Ok(())
}

fn claim_native_route(
    routes: &RouteMap,
    harness_thread_id: &str,
    suggested_thread_id: ThreadId,
) -> Result<ClaimedNativeRoute, HarnessError> {
    let harness_thread_id = harness_thread_id.trim();
    if harness_thread_id.is_empty() {
        return Err(HarnessError::Protocol(
            "cannot claim an empty native thread id".into(),
        ));
    }
    let mut routes = lock_routes(routes)?;
    if let Some(existing) = routes.by_native.get(harness_thread_id) {
        return Ok(existing.route.clone());
    }
    if let Some(existing) = routes.native_by_thread.get(&suggested_thread_id) {
        return Err(HarnessError::Protocol(format!(
            "thread {suggested_thread_id} is already bound to native route {existing}"
        )));
    }
    routes.next_route_epoch = routes
        .next_route_epoch
        .checked_add(1)
        .ok_or_else(|| HarnessError::Protocol("native route epoch space exhausted".into()))?;
    let route = ClaimedNativeRoute {
        thread_id: suggested_thread_id,
        harness_thread_id: harness_thread_id.to_owned(),
        route_epoch: routes.next_route_epoch,
    };
    let (event_tx, receiver) = mpsc::channel(ROUTE_CAPACITY);
    routes
        .native_by_thread
        .insert(route.thread_id, route.harness_thread_id.clone());
    routes.by_native.insert(
        route.harness_thread_id.clone(),
        NativeRouteEntry {
            route: route.clone(),
            event_tx,
            receiver: Some(receiver),
            ready: false,
        },
    );
    Ok(route)
}

async fn respond_unroutable_server_request(
    client: &mut dyn CodexTransport,
    id: &codex_codes::jsonrpc::RequestId,
    request: &codex_codes::messages::ServerRequest,
) {
    let message = "Giskard cannot route this Codex server request to a known thread.";
    let context =
        CodexOperationContext::new("reject_unroutable_server_request").with_request_id(id);
    if let Err(error) = codex_respond_error_json(client, context, id.clone(), -32000, message).await
    {
        let (harness_thread_id, native_turn_id) = server_request_native_scope(request);
        warn!(
            action = "reject_unroutable_server_request",
            method = request.method(),
            request_id = %id,
            harness_thread_id = display_opt(harness_thread_id.as_deref()),
            native_turn_id = display_opt(native_turn_id.as_deref()),
            error = %error,
            "failed to reject unroutable Codex server request"
        );
    } else {
        let (harness_thread_id, native_turn_id) = server_request_native_scope(request);
        warn!(
            action = "reject_unroutable_server_request",
            method = request.method(),
            request_id = %id,
            harness_thread_id = display_opt(harness_thread_id.as_deref()),
            native_turn_id = display_opt(native_turn_id.as_deref()),
            "rejected unroutable Codex server request"
        );
    }
}

fn server_request_native_scope(
    request: &codex_codes::messages::ServerRequest,
) -> (Option<String>, Option<String>) {
    use codex_codes::messages::ServerRequest;
    match request {
        ServerRequest::CmdExecApproval(params) => {
            (Some(params.thread_id.clone()), Some(params.turn_id.clone()))
        }
        ServerRequest::FileChangeApproval(params) => {
            (Some(params.thread_id.clone()), Some(params.turn_id.clone()))
        }
        ServerRequest::ToolRequestUserInput(params) => {
            (Some(params.thread_id.clone()), Some(params.turn_id.clone()))
        }
        ServerRequest::PermissionsRequestApproval(params) => {
            (Some(params.thread_id.clone()), Some(params.turn_id.clone()))
        }
        ServerRequest::ItemToolCall(params) => {
            (Some(params.thread_id.clone()), Some(params.turn_id.clone()))
        }
        ServerRequest::ApplyPatchApproval(params) => (Some(params.conversation_id.0.clone()), None),
        ServerRequest::ExecCommandApproval(params) => {
            (Some(params.conversation_id.0.clone()), None)
        }
        ServerRequest::Unknown { params, .. } => {
            let params = params.as_ref();
            (
                params.and_then(native_thread_id_from_value),
                params.and_then(|value| string_field(value, &["turnId", "turn_id"])),
            )
        }
        ServerRequest::McpServerElicitationRequest(params) => {
            let value = serde_json::to_value(params).ok();
            (
                value.as_ref().and_then(native_thread_id_from_value),
                value
                    .as_ref()
                    .and_then(|value| string_field_or_meta(value, &["turnId", "turn_id"])),
            )
        }
        ServerRequest::ChatgptAuthTokensRefresh(_) | ServerRequest::AttestationGenerate(_) => {
            (None, None)
        }
    }
}

fn notification_native_thread_id(
    notification: &codex_codes::messages::Notification,
) -> Option<String> {
    let envelope = serde_json::to_value(notification).ok()?;
    let params = envelope.get("params").unwrap_or(&envelope);
    native_thread_id_from_value(params)
}

fn native_thread_id_from_value(value: &serde_json::Value) -> Option<String> {
    string_field(
        value,
        &["threadId", "thread_id", "conversationId", "conversation_id"],
    )
    .or_else(|| {
        let thread = value.get("thread")?;
        thread
            .as_str()
            .and_then(trimmed_non_empty)
            .map(ToOwned::to_owned)
            .or_else(|| string_field(thread, &["id", "threadId", "thread_id"]))
    })
    .or_else(|| value.get("_meta").and_then(native_thread_id_from_value))
}

fn string_field(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(key).and_then(serde_json::Value::as_str))
        .and_then(trimmed_non_empty)
        .map(ToOwned::to_owned)
}

fn string_field_or_meta(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    string_field(value, keys)
        .or_else(|| value.get("_meta").and_then(|meta| string_field(meta, keys)))
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

async fn emit_incomplete_turn(
    routes: &RouteMap,
    thread: ThreadId,
    turn: Option<TurnId>,
    message: impl Into<String>,
) {
    let message = message.into();
    if let Some(turn) = turn {
        if let Err(error) = route_event(
            routes,
            thread,
            AgentEvent::TurnCompleted {
                thread,
                turn,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Failed,
                    message: Some(message),
                },
            },
        )
        .await
        {
            warn!(%thread, %error, "failed to route incomplete turn completion during teardown");
        }
    } else {
        if let Err(error) = route_event(
            routes,
            thread,
            AgentEvent::Error {
                thread,
                turn: None,
                error: HarnessError::Transport(message),
            },
        )
        .await
        {
            warn!(%thread, %error, "failed to route incomplete stream error during teardown");
        }
    }
}

async fn emit_fatal_turn_completion(
    routes: &RouteMap,
    thread: ThreadId,
    turn: Option<TurnId>,
    message: impl Into<String>,
) -> bool {
    let message = message.into();
    let Some(turn) = turn else {
        warn!(
            %thread,
            error = %message,
            "fatal Codex error notification arrived without an active turn; not synthesizing turn completion"
        );
        return false;
    };

    warn!(
        %thread,
        %turn,
        error = %message,
        "synthesizing failed turn completion from fatal Codex error notification"
    );
    if let Err(error) = route_event(
        routes,
        thread,
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Failed,
                message: Some(message),
            },
        },
    )
    .await
    {
        warn!(%thread, %turn, %error, "failed to route fatal turn completion");
        return false;
    }
    true
}

async fn handle_respond_approval_ordered(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    id: &ApprovalId,
    decision: &ApprovalDecision,
) -> Result<(), HarnessError> {
    let attempt = lock_background_state(state)?
        .mapper
        .prepare_approval_response(id, decision)
        .map_err(HarnessError::Protocol)?;
    let mapping::ApprovalResponseAttempt { response, token } = attempt;
    let result = match response {
        mapping::ApprovalResponse::Result {
            request_id,
            owner,
            value,
        } => {
            codex_respond_json(
                client,
                CodexOperationContext::new("respond_approval")
                    .with_thread_id(owner.thread)
                    .with_giskard_turn_id(owner.turn)
                    .with_request_id(&request_id),
                request_id.clone(),
                value,
            )
            .await
        }
        mapping::ApprovalResponse::Error {
            request_id,
            owner,
            code,
            message,
        } => {
            codex_respond_error_json(
                client,
                CodexOperationContext::new("respond_approval")
                    .with_thread_id(owner.thread)
                    .with_giskard_turn_id(owner.turn)
                    .with_request_id(&request_id),
                request_id.clone(),
                code,
                &message,
            )
            .await
        }
    };
    let mut state = lock_background_state(state)?;
    if result.is_ok() {
        state.mapper.commit_approval_response(&token);
    } else {
        state.mapper.rollback_approval_response(&token);
    }
    result
}

async fn handle_respond_server_request_ordered(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    routes: &RouteMap,
    id: &ServerRequestId,
    response: ServerRequestResponse,
) -> Result<(), HarnessError> {
    let attempt = {
        let mut state = lock_background_state(state)?;
        state
            .mapper
            .prepare_server_request_response(id)
            .map_err(HarnessError::Protocol)?
    };
    let mapping::ServerRequestResponseAttempt { pending, token } = attempt;
    let request_id = pending.request_id.clone();
    let context = CodexOperationContext::new("respond_server_request")
        .with_thread_id(pending.thread)
        .with_request_id(&request_id);
    let write_result = match response {
        ServerRequestResponse::Result { value } => {
            codex_respond_json(client, context, request_id.clone(), value).await
        }
        ServerRequestResponse::Error { code, message } => {
            codex_respond_error_json(client, context, request_id.clone(), code, &message).await
        }
    };
    {
        let mut state = lock_background_state(state)?;
        if write_result.is_ok() {
            state.mapper.commit_server_request_response(&token);
        } else {
            state.mapper.rollback_server_request_response(&token);
        }
    }
    write_result?;
    let thread = pending.thread;
    let turn = pending.turn;
    let request_id = id.clone();
    route_event(
        routes,
        thread,
        AgentEvent::ServerRequestResolved {
            thread,
            turn,
            request_id,
        },
    )
    .await?;
    Ok(())
}

async fn reject_pending_requests_for_interrupted_thread_ordered(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    routes: &RouteMap,
    thread: ThreadId,
) {
    let (approval_ids, server_request_ids) = {
        let state = match lock_background_state(state) {
            Ok(state) => state,
            Err(error) => {
                warn!(%thread, %error, "could not reject requests from poisoned Codex mapper state");
                return;
            }
        };
        (
            state.mapper.pending_approval_ids_for_thread(thread),
            state.mapper.pending_server_request_ids_for_thread(thread),
        )
    };
    for approval_id in approval_ids {
        if let Err(error) =
            handle_respond_approval_ordered(client, state, &approval_id, &ApprovalDecision::Cancel)
                .await
        {
            warn!(%thread, request_id = %approval_id, %error, "failed to cancel pending approval after interrupt");
        }
    }
    for server_request_id in server_request_ids {
        let response = ServerRequestResponse::Error {
            code: -32000,
            message: "Turn interrupted before this server request was answered.".into(),
        };
        if let Err(error) = handle_respond_server_request_ordered(
            client,
            state,
            routes,
            &server_request_id,
            response,
        )
        .await
        {
            warn!(%thread, request_id = %server_request_id, %error, "failed to reject pending server request after interrupt");
        }
    }
}

enum PendingTermination {
    BackgroundTerminal(PendingCodexResponse),
    CommandExec(PendingCodexResponse),
}

async fn submit_terminate_command(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    process_id: &str,
) -> Result<PendingTermination, HarnessError> {
    if process_id.parse::<i32>().is_ok() {
        return submit_terminate_background_terminal(client, thread, process_id)
            .await
            .map(PendingTermination::BackgroundTerminal);
    }
    submit_terminate_command_exec(client, process_id)
        .await
        .map(PendingTermination::CommandExec)
}

async fn complete_submitted_termination(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    process_id: &str,
    initial: PendingTermination,
) -> Result<(), HarnessError> {
    match initial {
        PendingTermination::CommandExec(pending) => {
            let _: codex_codes::CommandExecTerminateResponse = await_reserved_control_response(
                pending,
                CodexOperationContext::for_thread("terminate_command_exec", thread)
                    .with_process_id(process_id),
                codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE,
            )
            .await?;
            Ok(())
        }
        PendingTermination::BackgroundTerminal(pending) => {
            let response: Result<ThreadBackgroundTerminalsTerminateResponse, HarnessError> =
                await_reserved_control_response(
                    pending,
                    CodexOperationContext::for_thread("terminate_background_terminal", thread)
                        .with_process_id(process_id),
                    THREAD_BACKGROUND_TERMINALS_TERMINATE,
                )
                .await;
            match response {
                Ok(response) if response.terminated => return Ok(()),
                Ok(_) => debug!(
                    thread_id = %thread.thread,
                    harness_thread_id = %thread.harness_thread_id,
                    process_id,
                    "Codex did not find a background terminal for command process"
                ),
                Err(error) => debug!(
                    thread_id = %thread.thread,
                    harness_thread_id = %thread.harness_thread_id,
                    process_id,
                    %error,
                    "Codex background-terminal termination failed; trying command/exec"
                ),
            }
            let pending = submit_terminate_command_exec(client, process_id).await?;
            let _: codex_codes::CommandExecTerminateResponse = await_reserved_control_response(
                pending,
                CodexOperationContext::for_thread("terminate_command_exec", thread)
                    .with_process_id(process_id),
                codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE,
            )
            .await?;
            Ok(())
        }
    }
}

async fn submit_terminate_background_terminal(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    process_id: &str,
) -> Result<PendingCodexResponse, HarnessError> {
    let params = ThreadBackgroundTerminalsTerminateParams {
        thread_id: thread.harness_thread_id.clone(),
        process_id: process_id.to_owned(),
    };
    submit_reserved_control_request(client, THREAD_BACKGROUND_TERMINALS_TERMINATE, &params).await
}

async fn submit_terminate_command_exec(
    client: &mut dyn CodexTransport,
    process_id: &str,
) -> Result<PendingCodexResponse, HarnessError> {
    let params = codex_codes::CommandExecTerminateParams {
        process_id: process_id.to_owned(),
    };
    submit_reserved_control_request(
        client,
        codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE,
        &params,
    )
    .await
}

async fn handle_compact_thread_ordered(
    client: &mut dyn CodexTransport,
    state: &SharedBackgroundState,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let started = Instant::now();
    let params = codex_codes::ThreadCompactStartParams {
        thread_id: thread.harness_thread_id.clone(),
    };
    let state_for_hook = state.clone();
    let thread_id = thread.thread;
    let hook: transport::SuccessResponseHook = Box::new(move |value| {
        Box::pin(async move {
            lock_background_state(&state_for_hook)?
                .pending_compactions
                .insert(thread_id, PendingCompaction::new(started));
            Ok(value)
        })
    });
    let _: codex_codes::ThreadCompactStartResponse = codex_request_with_hook(
        client,
        CodexOperationContext::for_thread("compact_thread", thread),
        codex_codes::protocol::methods::THREAD_COMPACT_START,
        &params,
        hook,
        transport::CorrelationLifetime::Retained,
        None,
    )
    .await?;
    Ok(())
}

async fn handle_set_thread_archived(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    archived: bool,
) -> Result<(), HarnessError> {
    if archived {
        let params = codex_codes::ThreadArchiveParams {
            thread_id: thread.harness_thread_id.clone(),
        };
        let _: codex_codes::ThreadArchiveResponse = codex_request(
            client,
            CodexOperationContext::for_thread("archive_thread", thread),
            codex_codes::protocol::methods::THREAD_ARCHIVE,
            &params,
        )
        .await?;
    } else {
        let params = codex_codes::ThreadUnarchiveParams {
            thread_id: thread.harness_thread_id.clone(),
        };
        let _: codex_codes::ThreadUnarchiveResponse = codex_request(
            client,
            CodexOperationContext::for_thread("unarchive_thread", thread),
            codex_codes::protocol::methods::THREAD_UNARCHIVE,
            &params,
        )
        .await?;
    }
    Ok(())
}

async fn handle_set_thread_name(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    name: &str,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadSetNameParams {
        thread_id: thread.harness_thread_id.clone(),
        name: name.to_owned(),
    };
    let _: codex_codes::ThreadSetNameResponse = codex_request(
        client,
        CodexOperationContext::for_thread("set_thread_name", thread),
        codex_codes::protocol::methods::THREAD_NAME_SET,
        &params,
    )
    .await?;
    Ok(())
}

async fn handle_list_mcp_servers(
    client: &mut dyn CodexTransport,
) -> Result<Vec<McpServerStatus>, HarnessError> {
    let mut out = Vec::new();
    let mut cursor = None;

    loop {
        let params = codex_codes::ListMcpServerStatusParams {
            cursor: cursor.clone(),
            detail: Some(codex_codes::McpServerStatusDetail::Full),
            limit: None,
            thread_id: None,
        };
        let page: codex_codes::ListMcpServerStatusResponse = codex_request(
            client,
            CodexOperationContext::new("list_mcp_servers"),
            codex_codes::protocol::methods::MCPSERVERSTATUS_LIST,
            &params,
        )
        .await?;

        out.extend(page.data.into_iter().map(map_mcp_server_status));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    Ok(out)
}

/// Read the providers Codex is configured to route to (§8.2), from the `config/read` RPC.
///
/// Codex owns provider configuration: `~/.codex/config.toml` holds each provider's display name,
/// `base_url`, and `env_key`, and Giskard reads them back here instead of asking the user to
/// restate them. `config/read` returns the whole effective config, so the `[model_providers]`
/// table arrives as an unmodeled key that our own [`CodexConfig`] picks up.
///
/// The result is the built-in ids Codex always accepts plus every user-declared entry. Built-ins
/// come first so a user entry that somehow shadows one wins on `id` collision; Codex itself
/// rejects that at load time for all but the Bedrock ids.
///
/// Config is resolved per directory, so `cwd` must be the project's workspace root: a project
/// layer can add providers the home config does not have.
async fn handle_list_providers(
    client: &mut dyn CodexTransport,
    cwd: String,
) -> Result<Vec<HarnessProvider>, HarnessError> {
    let config = read_codex_config(client, cwd, "list_providers").await?;

    let mut providers: Vec<HarnessProvider> = CODEX_BUILT_IN_PROVIDER_IDS
        .iter()
        .map(|id| HarnessProvider {
            id: (*id).to_string(),
            name: None,
            base_url: None,
            auth: None,
        })
        .collect();

    for (id, provider) in config.model_providers {
        let entry = HarnessProvider {
            id: id.clone(),
            name: non_empty(provider.name.clone()),
            base_url: non_empty(provider.base_url.clone()),
            auth: provider.auth(),
        };
        match providers.iter_mut().find(|existing| existing.id == id) {
            Some(existing) => *existing = entry,
            None => providers.push(entry),
        }
    }

    Ok(providers)
}

/// Treat an absent and an empty string alike: Codex defaults `name` to `""` rather than omitting
/// it, and an empty `base_url`/`env_key` is a misconfiguration, not a value.
fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

/// The provider id Codex routes to when nothing overrides it, for attributing its own catalog.
///
/// An **absent** `model_provider` key is the common case and not a problem: Codex then routes to
/// its `openai` built-in, the same id `CODEX_BUILT_IN_PROVIDER_IDS` leads with, so that is the
/// answer.
///
/// A **failed** `config/read` is different and fails the listing. Guessing the default would
/// attribute every model to `openai` for a user whose effective provider is something else,
/// putting routes in the picker that do not exist — and doing it silently, since nothing
/// downstream can tell an invented attribution from a real one. The caller already reports a
/// failed catalog as a warning, which is the honest outcome: no models rather than wrong ones.
async fn default_model_provider(
    client: &mut dyn CodexTransport,
    cwd: String,
) -> Result<String, HarnessError> {
    const CODEX_DEFAULT_PROVIDER: &str = "openai";
    let config = read_codex_config(client, cwd, "list_models").await?;
    Ok(non_empty(config.model_provider).unwrap_or_else(|| CODEX_DEFAULT_PROVIDER.to_string()))
}

/// List the models Codex advertises over the app-server `model/list` RPC, mapped to Giskard
/// [`ModelDescriptor`]s so the picker can show Codex's friendly `display_name` instead of raw
/// model ids.
///
/// The `model/list` catalog is provider-agnostic — each entry carries only a model slug, no
/// provider — but Giskard routes by `(provider, model)`, so an unattributed descriptor can only
/// ever enrich an entry some other source already produced. That leaves a stock Codex, whose
/// built-in providers carry no `base_url` to discover against, with nothing in the picker at all.
///
/// So the catalog is attributed to the provider Codex itself routes to, read from the same
/// `config/read` that supplies the provider table. Codex omits the context window from this RPC,
/// so descriptors still use the conservative default; these entries size no gauge until the
/// harness reports a real window at turn time.
async fn handle_list_models(
    client: &mut dyn CodexTransport,
    cwd: String,
) -> Result<Vec<ModelDescriptor>, HarnessError> {
    let provider = default_model_provider(client, cwd).await?;
    let mut out = Vec::new();
    let mut cursor = None;

    loop {
        let params = codex_codes::ModelListParams {
            cursor: cursor.clone(),
            // Default (false): only models Codex shows in its own picker.
            include_hidden: None,
            limit: None,
        };
        let page: codex_codes::ModelListResponse = codex_request(
            client,
            CodexOperationContext::new("list_models"),
            codex_codes::protocol::methods::MODEL_LIST,
            &params,
        )
        .await?;

        out.extend(
            page.data
                .into_iter()
                .filter(|m| !m.hidden)
                .map(|m| map_model(m, &provider)),
        );
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    Ok(out)
}

/// Map a Codex `model/list` entry to a Giskard [`ModelDescriptor`] under `provider`. See
/// [`handle_list_models`] for where that provider comes from — the entry itself names none — and
/// why the context window is conservative.
fn map_model(model: codex_codes::Model, provider: &str) -> ModelDescriptor {
    // `model` is the wire slug used in a ModelRef; `id` is the preset id. Prefer the slug, but fall
    // back to the id if an older/edge payload leaves it empty.
    let id = if model.model.is_empty() {
        model.id
    } else {
        model.model
    };
    let display_name = if model.display_name.is_empty() {
        None
    } else {
        Some(model.display_name)
    };
    // Codex separates the default effort from selectable alternatives. Its TUI treats a non-`none`
    // default as the sole valid choice when the alternatives list is empty, so normalize that case
    // here instead of incorrectly classifying a default-only reasoning model as non-reasoning.
    let default_reasoning_effort = model.default_reasoning_effort.0;
    let mut reasoning_efforts: Vec<String> = model
        .supported_reasoning_efforts
        .into_iter()
        .map(|option| option.reasoning_effort.0)
        .collect();
    if reasoning_efforts.is_empty() && default_reasoning_effort != "none" {
        reasoning_efforts.push(default_reasoning_effort);
    }
    ModelDescriptor {
        provider: provider.to_string(),
        model: id,
        context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
        supports_reasoning_effort: !reasoning_efforts.is_empty(),
        reasoning_efforts,
        display_name,
        is_default: model.is_default,
    }
}

async fn handle_reload_mcp_servers(client: &mut dyn CodexTransport) -> Result<(), HarnessError> {
    let _: codex_codes::McpServerRefreshResponse = codex_request(
        client,
        CodexOperationContext::new("reload_mcp_servers"),
        codex_codes::protocol::methods::CONFIG_MCPSERVER_RELOAD,
        &serde_json::json!({}),
    )
    .await?;
    Ok(())
}

async fn handle_start_mcp_oauth_login(
    client: &mut dyn CodexTransport,
    name: &str,
) -> Result<McpOauthStart, HarnessError> {
    let params = codex_codes::McpServerOauthLoginParams {
        // Dynamic client registration details are supplied by Codex's own config; Giskard does not
        // register an OAuth client on the server's behalf.
        client_registration: None,
        name: name.to_owned(),
        scopes: None,
        thread_id: None,
        timeout_secs: None,
    };
    let response: codex_codes::McpServerOauthLoginResponse = codex_request(
        client,
        CodexOperationContext::new("start_mcp_oauth_login").with_server(name),
        codex_codes::protocol::methods::MCPSERVER_OAUTH_LOGIN,
        &params,
    )
    .await?;
    Ok(McpOauthStart {
        authorization_url: response.authorization_url,
    })
}

fn map_mcp_server_status(status: codex_codes::McpServerStatus) -> McpServerStatus {
    McpServerStatus {
        name: status.name,
        auth_status: map_mcp_auth_status(status.auth_status),
        server_info: status.server_info.map(map_mcp_server_info),
        tools: status.tools.into_values().map(map_mcp_tool).collect(),
        resources: status.resources.into_iter().map(map_mcp_resource).collect(),
        resource_templates: status
            .resource_templates
            .into_iter()
            .map(map_mcp_resource_template)
            .collect(),
    }
}

fn map_mcp_auth_status(status: codex_codes::McpAuthStatus) -> McpAuthStatus {
    match status {
        codex_codes::McpAuthStatus::Unknown => McpAuthStatus::Unknown,
        codex_codes::McpAuthStatus::Unsupported => McpAuthStatus::Unsupported,
        codex_codes::McpAuthStatus::NotLoggedIn => McpAuthStatus::NotLoggedIn,
        codex_codes::McpAuthStatus::BearerToken => McpAuthStatus::BearerToken,
        codex_codes::McpAuthStatus::OAuth => McpAuthStatus::OAuth,
    }
}

fn map_mcp_server_info(info: codex_codes::McpServerInfo) -> McpServerInfo {
    McpServerInfo {
        name: info.name,
        title: info.title,
        description: info.description,
        version: (!info.version.is_empty()).then_some(info.version),
        website_url: info.website_url,
    }
}

fn map_mcp_tool(tool: codex_codes::Tool) -> McpTool {
    McpTool {
        name: tool.name,
        title: tool.title,
        description: tool.description,
        input_schema: tool.input_schema,
        output_schema: tool.output_schema,
    }
}

fn map_mcp_resource(resource: codex_codes::Resource) -> McpResource {
    McpResource {
        name: resource.name,
        uri: resource.uri,
        title: resource.title,
        description: resource.description,
        mime_type: resource.mime_type,
        size: resource.size,
    }
}

fn map_mcp_resource_template(template: codex_codes::ResourceTemplate) -> McpResourceTemplate {
    McpResourceTemplate {
        name: template.name,
        uri_template: template.uri_template,
        title: template.title,
        description: template.description,
        mime_type: template.mime_type,
    }
}

async fn handle_delete_thread(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadDeleteParams {
        thread_id: thread.harness_thread_id.clone(),
    };
    let result: Result<codex_codes::ThreadDeleteResponse, HarnessError> = codex_request(
        client,
        CodexOperationContext::for_thread("delete_thread", thread),
        codex_codes::protocol::methods::THREAD_DELETE,
        &params,
    )
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) if codex_reports_missing_rollout(&error, &thread.harness_thread_id) => {
            warn!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                action = "delete_thread",
                "native Codex rollout is already absent; completing thread deletion idempotently"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn codex_reports_missing_rollout(error: &HarnessError, harness_thread_id: &str) -> bool {
    // This is intentionally coupled to the pinned Codex/codex-codes protocol chain: Codex emits
    // `InvalidRequest("no rollout found for thread id …")`, app-server preserves it as JSON-RPC
    // -32600. Keep the match fail-closed if any layer changes; a different "thread not found"
    // error must remain visible to the caller.
    const PREFIX: &str = "no rollout found for thread id ";
    let HarnessError::ProviderRejected {
        code: -32600,
        message,
    } = error
    else {
        return false;
    };
    message
        .strip_prefix(PREFIX)
        .is_some_and(|missing_id| missing_id == harness_thread_id)
}

async fn submit_interrupt_turn(
    client: &mut dyn CodexTransport,
    native_thread_id: &str,
    native_turn_id: &str,
) -> Result<PendingCodexResponse, HarnessError> {
    let params = codex_codes::TurnInterruptParams {
        thread_id: native_thread_id.to_owned(),
        turn_id: native_turn_id.to_owned(),
    };
    submit_reserved_control_request(
        client,
        codex_codes::protocol::methods::TURN_INTERRUPT,
        &params,
    )
    .await
}

async fn submit_reserved_control_request<P: Serialize + Sync>(
    client: &mut dyn CodexTransport,
    method: &str,
    params: &P,
) -> Result<PendingCodexResponse, HarnessError> {
    let params =
        serde_json::to_value(params).map_err(|error| HarnessError::Protocol(error.to_string()))?;
    client.submit_control_request_json(method, params).await
}

async fn await_reserved_control_response<R: DeserializeOwned>(
    pending: PendingCodexResponse,
    context: CodexOperationContext<'_>,
    method: &str,
) -> Result<R, HarnessError> {
    let started = Instant::now();
    let response = tokio::time::timeout(CODEX_JSON_RPC_TIMEOUT, pending)
        .await
        .map_err(|_| {
            context.log_timeout(
                Some(method),
                started.elapsed(),
                "Codex reserved control response timed out",
            );
            HarnessError::Timeout(format!("Codex JSON-RPC request {method} timed out"))
        })??;
    serde_json::from_value(response).map_err(|error| HarnessError::Protocol(error.to_string()))
}

async fn await_interrupt_response(
    pending: PendingCodexResponse,
    context: CodexOperationContext<'_>,
) -> Result<(), HarnessError> {
    let method = codex_codes::protocol::methods::TURN_INTERRUPT;
    let _: codex_codes::TurnInterruptResponse =
        await_reserved_control_response(pending, context, method).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::ids::ItemId;
    use giskard_core::item::{Item, ItemPayload};
    use giskard_core::model::{Effort, ModelRef};
    use giskard_core::turn::{Mode, PermissionPreset};
    use serde_json::{Value, json};
    use std::collections::HashSet;

    fn test_routes(thread: ThreadId) -> (RouteMap, mpsc::Receiver<AgentEvent>) {
        let routes = Arc::new(StdMutex::new(NativeRoutes::default()));
        install_native_route(
            &routes,
            ClaimedNativeRoute {
                thread_id: thread,
                harness_thread_id: format!("native-{thread}"),
                route_epoch: 1,
            },
        )
        .unwrap();
        let receiver = {
            let mut routes_guard = lock_routes(&routes).expect("test route lock should be healthy");
            routes_guard
                .by_native
                .get_mut(&format!("native-{thread}"))
                .and_then(|entry| entry.receiver.take())
                .unwrap()
        };
        (routes, receiver)
    }

    #[test]
    fn event_routes_own_authoritative_monotonic_epochs() {
        let routes = Arc::new(StdMutex::new(NativeRoutes::default()));
        let first_thread = ThreadId::new();
        let second_thread = ThreadId::new();
        let first = claim_native_route(&routes, "native-first", first_thread)
            .expect("first native route should be claimable");
        let repeated = claim_native_route(&routes, "native-first", ThreadId::new())
            .expect("an existing native route should remain authoritative");
        let second = claim_native_route(&routes, "native-second", second_thread)
            .expect("second native route should be claimable");

        assert_eq!(repeated, first);
        assert!(second.route_epoch > first.route_epoch);
        assert!(claim_native_route(&routes, "native-conflict", first_thread).is_err());
    }
    use tokio::sync::Mutex;
    use tokio::time::timeout;

    fn test_thread() -> ThreadHandle {
        ThreadHandle::opened(
            ThreadId::new(),
            "native-thread".into(),
            PathBuf::from("/tmp/test-workspace"),
        )
    }

    fn test_model(effort: Option<Effort>) -> ModelRef {
        ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: effort,
        }
    }

    fn turn_overrides(mode: Mode, effort: Option<Effort>) -> TurnOverrides {
        TurnOverrides {
            model: Some(test_model(effort)),
            mode,
            permission_preset: PermissionPreset::AskFirst,
        }
    }

    #[test]
    fn worker_queue_snapshot_preserves_operation_identity() {
        let watchdog = WorkerQueueWatchdog::new();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let token = watchdog.enqueue(
            WorkerQueueKind::Command,
            "open_thread",
            Some(project_id),
            Some(thread_id),
        );
        watchdog.mark_started(token);

        let active = watchdog.snapshot().active.expect("active queue entry");
        assert_eq!(active.project_id, Some(project_id));
        assert_eq!(active.thread_id, Some(thread_id));
        assert_eq!(active.action, "open_thread");
    }

    #[test]
    fn active_turn_diagnostics_preserve_thread_identities() {
        let thread = test_thread();
        let thread_id = thread.thread;
        let harness_thread_id = thread.harness_thread_id.clone();
        let turn = TurnId::new();
        let active_turns = HashMap::from([(thread_id, ActiveTurn::new(thread, turn))]);

        let states = active_turn_states(&active_turns);
        assert_eq!(states.len(), 1);
        assert!(states[0].contains(&format!("thread_id={thread_id}")));
        assert!(states[0].contains(&format!("harness_thread_id={harness_thread_id}")));
        assert!(states[0].contains(&format!("acknowledged_turn={turn}")));
    }

    #[tokio::test]
    async fn poisoned_signal_receiver_lock_returns_typed_error() {
        let (harness, _controller) = spawn_fake_harness();
        let poisoning_harness = harness.clone();
        let poisoning = std::thread::spawn(move || {
            let _guard = poisoning_harness
                .signals
                .lock()
                .expect("fresh signal lock should be available");
            panic!("poison signal receiver lock");
        });
        assert!(poisoning.join().is_err());
        assert!(matches!(
            harness.take_harness_signals(),
            Err(HarnessError::Transport(message)) if message.contains("signal lock was poisoned")
        ));
    }

    #[tokio::test]
    async fn poisoned_route_lock_returns_typed_error_without_claiming() {
        let (harness, _controller) = spawn_fake_harness();
        let routes = harness.routes.clone();
        let poisoning = std::thread::spawn(move || {
            let _guard = routes.lock().expect("fresh route lock should be available");
            panic!("poison route lock");
        });
        assert!(poisoning.join().is_err());
        assert!(matches!(
            harness
                .claim_native_route("native-poisoned".into(), ThreadId::new())
                .await,
            Err(HarnessError::Transport(message)) if message.contains("route map lock was poisoned")
        ));
    }

    #[test]
    fn poisoned_mapper_state_lock_returns_typed_error() {
        let state = Arc::new(StdMutex::new(BackgroundState {
            mapper: CodexMapper::new(PathBuf::from("/tmp")),
            pending_compactions: HashMap::new(),
            pending_context_restores: HashMap::new(),
            active_turns: HashMap::new(),
        }));
        let state_to_poison = state.clone();
        let poisoning = std::thread::spawn(move || {
            let _guard = state_to_poison
                .lock()
                .expect("fresh mapper-state lock should be available");
            panic!("poison mapper-state lock");
        });
        assert!(poisoning.join().is_err());
        assert!(matches!(
            lock_background_state(&state),
            Err(HarnessError::Transport(message))
                if message.contains("background-state lock was poisoned")
        ));
    }

    #[test]
    fn unknown_server_request_diagnostics_extract_only_native_scope() {
        let request = codex_codes::messages::ServerRequest::Unknown {
            method: "future/request".into(),
            params: Some(json!({
                "threadId": "native-thread-7",
                "turnId": "native-turn-9",
                "prompt": "sensitive and intentionally ignored"
            })),
        };

        assert_eq!(
            server_request_native_scope(&request),
            (Some("native-thread-7".into()), Some("native-turn-9".into()))
        );
    }

    #[test]
    fn unknown_server_request_scope_accepts_all_protocol_spellings() {
        for (key, native_id) in [
            ("threadId", "camel-thread"),
            ("thread_id", "snake-thread"),
            ("conversationId", "camel-conversation"),
            ("conversation_id", "snake-conversation"),
        ] {
            let request = codex_codes::messages::ServerRequest::Unknown {
                method: "future/request".into(),
                params: Some(json!({ (key): native_id, "turn_id": "native-turn" })),
            };
            assert_eq!(
                server_request_native_scope(&request),
                (Some(native_id.into()), Some("native-turn".into()))
            );
        }
    }

    #[test]
    fn mcp_elicitation_scope_uses_meta_thread_and_turn() {
        let request = codex_codes::messages::ServerRequest::McpServerElicitationRequest(
            serde_json::from_value(json!({
                "mode": "form",
                "_meta": { "threadId": "native-mcp", "turn_id": "native-turn" },
                "message": "Need input",
                "requestedSchema": { "type": "object" }
            }))
            .unwrap(),
        );

        assert_eq!(
            server_request_native_scope(&request),
            (Some("native-mcp".into()), Some("native-turn".into()))
        );
    }

    #[test]
    fn thread_started_scope_uses_nested_thread_identity() {
        let notification = codex_codes::messages::Notification::from_envelope(
            codex_codes::protocol::methods::THREAD_STARTED,
            Some(json!({ "thread": { "id": "native-started" } })),
        )
        .expect("minimal thread/started notification should decode");

        assert_eq!(
            notification_native_thread_id(&notification).as_deref(),
            Some("native-started")
        );
    }

    #[test]
    fn unknown_notification_scope_accepts_meta_and_snake_case() {
        let notification = codex_codes::messages::Notification::Unknown {
            method: "future/notification".into(),
            params: Some(json!({ "_meta": { "thread_id": "native-meta" } })),
        };

        assert_eq!(
            notification_native_thread_id(&notification).as_deref(),
            Some("native-meta")
        );
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeRequest {
        method: String,
        params: Value,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeResponse {
        id: codex_codes::jsonrpc::RequestId,
        value: Value,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeResponseError {
        id: codex_codes::jsonrpc::RequestId,
        code: i64,
        message: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeStartedTurn {
        native_thread_id: String,
        native_turn_id: String,
    }

    #[derive(Debug, Default)]
    struct FakeCodexState {
        thread_counter: usize,
        turn_counter: usize,
        hang_methods: HashSet<String>,
        background_terminal_terminate_result: Option<bool>,
        command_exec_terminate_error: Option<String>,
        thread_delete_error: Option<String>,
        thread_resume_missing_rollout_failures: usize,
        thread_resume_error: Option<HarnessError>,
        model_list_error: Option<String>,
        config_read_error: Option<String>,
        /// `model_provider` in the `config/read` payload; `None` omits the key, as a config that
        /// never set it does.
        config_model_provider: Option<String>,
        hang_response_json: bool,
        hang_shutdown: bool,
        block_shutdown: bool,
        shutdown_release: Arc<tokio::sync::Notify>,
        requests: Vec<FakeRequest>,
        responses: Vec<FakeResponse>,
        response_errors: Vec<FakeResponseError>,
        started_turns: Vec<FakeStartedTurn>,
        shutdowns: usize,
        shutdown_remaining: Option<Duration>,
    }

    #[derive(Clone)]
    struct FakeCodexTransport {
        state: Arc<Mutex<FakeCodexState>>,
    }

    struct FakeCodexFrameReceiver {
        events_rx: mpsc::Receiver<Result<codex_codes::ServerMessage, CodexStreamError>>,
    }

    #[derive(Clone)]
    struct FakeCodexController {
        state: Arc<Mutex<FakeCodexState>>,
        events_tx: mpsc::Sender<Result<codex_codes::ServerMessage, CodexStreamError>>,
    }

    impl FakeCodexController {
        async fn send_server_message(&self, msg: codex_codes::ServerMessage) {
            self.events_tx
                .send(Ok(msg))
                .await
                .expect("fake Codex event receiver should be open");
        }

        async fn send_stream_error(&self, error: CodexStreamError) {
            self.events_tx
                .send(Err(error))
                .await
                .expect("fake Codex event receiver should be open");
        }

        async fn requests(&self) -> Vec<FakeRequest> {
            self.state.lock().await.requests.clone()
        }

        async fn responses(&self) -> Vec<FakeResponse> {
            self.state.lock().await.responses.clone()
        }

        async fn response_errors(&self) -> Vec<FakeResponseError> {
            self.state.lock().await.response_errors.clone()
        }

        async fn started_turns(&self) -> Vec<FakeStartedTurn> {
            self.state.lock().await.started_turns.clone()
        }

        async fn shutdowns(&self) -> usize {
            self.state.lock().await.shutdowns
        }

        async fn shutdown_remaining(&self) -> Option<Duration> {
            self.state.lock().await.shutdown_remaining
        }

        async fn hang_method(&self, method: &'static str) {
            self.state.lock().await.hang_methods.insert(method.into());
        }

        async fn resume_method(&self, method: &'static str) {
            self.state.lock().await.hang_methods.remove(method);
        }

        async fn background_terminal_terminate_result(&self, result: bool) {
            self.state.lock().await.background_terminal_terminate_result = Some(result);
        }

        async fn fail_command_exec_terminate(&self, message: &str) {
            self.state.lock().await.command_exec_terminate_error = Some(message.into());
        }

        async fn fail_thread_delete(&self, message: &str) {
            self.state.lock().await.thread_delete_error = Some(message.into());
        }

        async fn fail_thread_resume_missing_rollout(&self, failures: usize) {
            self.state
                .lock()
                .await
                .thread_resume_missing_rollout_failures = failures;
        }

        async fn fail_next_thread_resume(&self, error: HarnessError) {
            self.state.lock().await.thread_resume_error = Some(error);
        }

        async fn fail_model_list(&self, message: &str) {
            self.state.lock().await.model_list_error = Some(message.into());
        }

        async fn fail_config_read(&self, message: &str) {
            self.state.lock().await.config_read_error = Some(message.into());
        }

        async fn set_config_model_provider(&self, provider: impl Into<String>) {
            self.state.lock().await.config_model_provider = Some(provider.into());
        }

        async fn hang_json_responses(&self) {
            self.state.lock().await.hang_response_json = true;
        }

        async fn resume_json_responses(&self) {
            self.state.lock().await.hang_response_json = false;
        }

        async fn hang_shutdown(&self) {
            self.state.lock().await.hang_shutdown = true;
        }

        async fn block_shutdown(&self) {
            self.state.lock().await.block_shutdown = true;
        }

        async fn release_shutdown(&self) {
            self.state.lock().await.shutdown_release.notify_one();
        }
    }

    fn fake_codex() -> (
        FakeCodexTransport,
        FakeCodexFrameReceiver,
        FakeCodexController,
    ) {
        let (events_tx, events_rx) = mpsc::channel(32);
        let state = Arc::new(Mutex::new(FakeCodexState::default()));
        (
            FakeCodexTransport {
                state: state.clone(),
            },
            FakeCodexFrameReceiver { events_rx },
            FakeCodexController { state, events_tx },
        )
    }

    #[async_trait]
    impl CodexTransport for FakeCodexTransport {
        async fn request_json(
            &mut self,
            method: &str,
            params: Value,
        ) -> Result<Value, HarnessError> {
            let mut state = self.state.lock().await;
            state.requests.push(FakeRequest {
                method: method.to_owned(),
                params: params.clone(),
            });

            if state.hang_methods.contains(method) {
                drop(state);
                std::future::pending().await
            } else {
                match method {
                    codex_codes::protocol::methods::THREAD_START => {
                        state.thread_counter += 1;
                        let native_thread_id = format!("native-thread-{}", state.thread_counter);
                        Ok(thread_open_response(
                            &native_thread_id,
                            params["model"].as_str().unwrap_or("gpt-5.5"),
                            params["modelProvider"].as_str().unwrap_or("openai"),
                        ))
                    }
                    codex_codes::protocol::methods::THREAD_RESUME => {
                        let native_thread_id = params["threadId"]
                            .as_str()
                            .filter(|id| !id.is_empty())
                            .unwrap_or("native-resumed");
                        if let Some(error) = state.thread_resume_error.take() {
                            Err(error)
                        } else if state.thread_resume_missing_rollout_failures > 0 {
                            state.thread_resume_missing_rollout_failures -= 1;
                            Err(HarnessError::ProviderRejected {
                                code: -32600,
                                message: format!(
                                    "no rollout found for thread id {native_thread_id}"
                                ),
                            })
                        } else {
                            let mut response = thread_open_response(
                                // An import sends no override, and Codex then answers with the
                                // thread's own persisted model rather than anything requested.
                                native_thread_id,
                                params["model"].as_str().unwrap_or("resumed-model"),
                                params["modelProvider"]
                                    .as_str()
                                    .unwrap_or("resumed-provider"),
                            );
                            response["reasoningEffort"] = json!("high");
                            Ok(response)
                        }
                    }
                    codex_codes::protocol::methods::TURN_START => {
                        state.turn_counter += 1;
                        let native_thread_id =
                            params["threadId"].as_str().unwrap_or_default().to_owned();
                        let native_turn_id = format!("native-turn-{}", state.turn_counter);
                        state.started_turns.push(FakeStartedTurn {
                            native_thread_id,
                            native_turn_id: native_turn_id.clone(),
                        });
                        Ok(json!({
                            "turn": {
                                "id": native_turn_id,
                                "status": "inProgress"
                            }
                        }))
                    }
                    codex_codes::protocol::methods::THREAD_COMPACT_START
                    | codex_codes::protocol::methods::THREAD_ARCHIVE
                    | codex_codes::protocol::methods::THREAD_UNARCHIVE
                    | codex_codes::protocol::methods::THREAD_NAME_SET
                    | codex_codes::protocol::methods::CONFIG_MCPSERVER_RELOAD
                    | codex_codes::protocol::methods::FS_CREATEDIRECTORY
                    | codex_codes::protocol::methods::FS_WRITEFILE
                    | codex_codes::protocol::methods::FS_REMOVE
                    | codex_codes::protocol::methods::TURN_INTERRUPT => Ok(json!({})),
                    codex_codes::protocol::methods::THREAD_DELETE => {
                        if let Some(message) = state.thread_delete_error.clone() {
                            Err(HarnessError::ProviderRejected {
                                code: -32600,
                                message,
                            })
                        } else {
                            Ok(json!({}))
                        }
                    }
                    THREAD_BACKGROUND_TERMINALS_TERMINATE => {
                        let terminated = state.background_terminal_terminate_result.unwrap_or(true);
                        Ok(json!({ "terminated": terminated }))
                    }
                    codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE => {
                        if let Some(message) = state.command_exec_terminate_error.clone() {
                            Err(HarnessError::Transport(message))
                        } else {
                            Ok(json!({}))
                        }
                    }
                    codex_codes::protocol::methods::MCPSERVERSTATUS_LIST => Ok(json!({
                        "data": [],
                        "nextCursor": null
                    })),
                    "config/read" => {
                        if let Some(message) = state.config_read_error.clone() {
                            Err(HarnessError::Transport(message))
                        } else {
                            let mut config = json!({
                                    "sandbox_workspace_write": {
                                        "writable_roots": [
                                            "/home/test/.cache/sccache",
                                            "relative/cache"
                                        ]
                                    },
                                    // Codex forwards every config key it does not model itself,
                                    // so the provider table arrives alongside the modeled ones.
                                    "model_providers": {
                                        "litellm": {
                                            "name": "LiteLLM",
                                            "base_url": "http://127.0.0.1:4000/v1",
                                            "env_key": "LITELLM_KEY",
                                            "wire_api": "responses"
                                        },
                                        "unnamed": {
                                            "name": "",
                                            "base_url": "http://127.0.0.1:9000/v1"
                                        },
                                        "unfamiliar-auth": {
                                            "base_url": "http://127.0.0.1:7000/v1",
                                            "env_key": "FALLBACK_KEY",
                                            "auth": {
                                                "args": ["--whatever"],
                                                "some_future_key": true
                                            }
                                        },
                                        "opencodex": {
                                            "name": "OpenCodex",
                                            "base_url": "http://127.0.0.1:5000/v1",
                                            "auth": {
                                                "command": "sh",
                                                "args": ["-c", "printf %s \"$OPENCODEX_KEY\""],
                                                "timeout_ms": 2500,
                                                "refresh_interval_ms": 300000,
                                                "cwd": "/tmp"
                                            }
                                        }
                                    }
                            });
                            if let Some(provider) = state.config_model_provider.clone() {
                                config["model_provider"] = json!(provider);
                            }
                            Ok(json!({ "config": config, "origins": {} }))
                        }
                    }
                    codex_codes::protocol::methods::MODEL_LIST
                        if state.model_list_error.is_some() =>
                    {
                        Err(HarnessError::Transport(
                            state.model_list_error.clone().unwrap(),
                        ))
                    }
                    codex_codes::protocol::methods::MODEL_LIST => Ok(json!({
                        "data": [
                            {
                                "id": "gpt-5.5",
                                "model": "gpt-5.5",
                                "displayName": "GPT-5.5",
                                "description": "Flagship model",
                                "hidden": false,
                                "supportedReasoningEfforts": [
                                    { "reasoningEffort": "medium", "description": "" },
                                    { "reasoningEffort": "high", "description": "" }
                                ],
                                "defaultReasoningEffort": "medium",
                                "isDefault": true
                            },
                            {
                                "id": "gpt-5.5-mini",
                                "model": "gpt-5.5-mini",
                                "displayName": "GPT-5.5 mini",
                                "description": "",
                                "hidden": false,
                                "supportedReasoningEfforts": [],
                                "defaultReasoningEffort": "medium",
                                "isDefault": false
                            },
                            {
                                "id": "internal-secret",
                                "model": "internal-secret",
                                "displayName": "Internal",
                                "description": "",
                                "hidden": true,
                                "supportedReasoningEfforts": [],
                                "defaultReasoningEffort": "medium",
                                "isDefault": false
                            }
                        ],
                        "nextCursor": null
                    })),
                    codex_codes::protocol::methods::MCPSERVER_OAUTH_LOGIN => Ok(json!({
                        "authorizationUrl": "https://example.invalid/oauth"
                    })),
                    other => Err(HarnessError::Unsupported(format!(
                        "fake Codex transport has no response for {other}"
                    ))),
                }
            }
        }

        async fn submit_control_request_json(
            &mut self,
            method: &str,
            params: Value,
        ) -> Result<PendingCodexResponse, HarnessError> {
            let mut state = self.state.lock().await;
            state.requests.push(FakeRequest {
                method: method.to_owned(),
                params,
            });
            if state.hang_methods.contains(method) {
                return Ok(Box::pin(std::future::pending()));
            }
            let response = match method {
                codex_codes::protocol::methods::TURN_INTERRUPT => Ok(json!({})),
                THREAD_BACKGROUND_TERMINALS_TERMINATE => Ok(json!({
                    "terminated": state.background_terminal_terminate_result.unwrap_or(true)
                })),
                codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE => {
                    state.command_exec_terminate_error.clone().map_or_else(
                        || Ok(json!({})),
                        |message| Err(HarnessError::Transport(message)),
                    )
                }
                _ => {
                    return Err(HarnessError::Unsupported(format!(
                        "fake Codex transport has no control request for {method}"
                    )));
                }
            };
            Ok(Box::pin(async move { response }))
        }

        async fn respond_json(
            &mut self,
            id: codex_codes::jsonrpc::RequestId,
            value: Value,
        ) -> Result<(), HarnessError> {
            let mut state = self.state.lock().await;
            if state.hang_response_json {
                drop(state);
                std::future::pending().await
            } else {
                state.responses.push(FakeResponse { id, value });
                Ok(())
            }
        }

        async fn respond_error_json(
            &mut self,
            id: codex_codes::jsonrpc::RequestId,
            code: i64,
            message: &str,
        ) -> Result<(), HarnessError> {
            self.state
                .lock()
                .await
                .response_errors
                .push(FakeResponseError {
                    id,
                    code,
                    message: message.to_owned(),
                });
            Ok(())
        }

        async fn shutdown_transport(
            self,
            deadline: tokio::time::Instant,
        ) -> Result<(), HarnessError> {
            let mut state = self.state.lock().await;
            state.shutdowns += 1;
            state.shutdown_remaining =
                Some(deadline.saturating_duration_since(tokio::time::Instant::now()));
            if state.hang_shutdown {
                drop(state);
                return tokio::time::timeout_at(deadline, std::future::pending())
                    .await
                    .map_err(|_| HarnessError::Timeout("fake shutdown timed out".into()));
            } else if state.block_shutdown {
                let release = state.shutdown_release.clone();
                drop(state);
                tokio::time::timeout_at(deadline, release.notified())
                    .await
                    .map_err(|_| HarnessError::Timeout("fake shutdown timed out".into()))?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl CodexFrameReceiver for FakeCodexFrameReceiver {
        async fn next_message(
            &mut self,
        ) -> Result<Option<transport::ProductionFrame>, CodexStreamError> {
            self.events_rx
                .recv()
                .await
                .transpose()
                .map(|message| message.map(transport::ProductionFrame::ungated))
        }
    }

    fn thread_open_response(native_thread_id: &str, model: &str, provider: &str) -> Value {
        let parent_thread_id = (native_thread_id == "native-existing").then_some("native-parent");
        json!({
            "approvalPolicy": "never",
            "approvalsReviewer": null,
            "cwd": "/tmp",
            "model": model,
            "modelProvider": provider,
            "sandbox": {},
            "thread": {
                "id": native_thread_id,
                "parentThreadId": parent_thread_id
            }
        })
    }

    fn open_opts(thread: Option<ThreadId>, resume: Option<&str>) -> OpenThreadOptions {
        let (updates, _) = giskard_harness::thread_update_channel();
        OpenThreadOptions {
            project: ProjectId::new(),
            thread,
            workspace_root: PathBuf::from("/tmp"),
            resume: resume.map(str::to_owned),
            initial_model: Some(test_model(None)),
            identity_generation: None,
            updates,
        }
    }

    fn build_turn_overrides() -> TurnOverrides {
        turn_overrides(Mode::Build, None)
    }

    fn spawn_fake_harness() -> (Arc<CodexHarness>, FakeCodexController) {
        spawn_fake_harness_with_bootstrap(HarnessBootstrap::default())
    }

    fn spawn_fake_harness_with_bootstrap(
        bootstrap: HarnessBootstrap,
    ) -> (Arc<CodexHarness>, FakeCodexController) {
        let (transport, frames, controller) = fake_codex();
        let harness = CodexHarness::spawn_harness(
            transport,
            frames,
            PathBuf::from("/tmp"),
            Some(Vec::new()),
            Some("1.2.3".into()),
            bootstrap,
        )
        .expect("fake harness should spawn");
        (harness, controller)
    }

    async fn claim_event_stream(harness: &CodexHarness, thread: &ThreadHandle) -> AgentEventStream {
        let route = harness
            .claim_native_route(thread.harness_thread_id.clone(), thread.thread)
            .await
            .expect("test thread route should be claimable");
        harness
            .claim_event_receiver(&route)
            .expect("test thread event receiver should be claimable")
    }

    fn acknowledge_test_activations(harness: &CodexHarness) {
        let mut signals = harness
            .take_harness_signals()
            .expect("test activation signal stream should be available");
        let _activation_owner = tokio::spawn(async move {
            while let Some(HarnessSignal::Activate(activation)) = signals.recv().await {
                activation.readiness.acknowledge(Ok(()));
            }
        });
    }

    /// A sub-agent Giskard persisted in an earlier run already has a `ThreadId` — the one its
    /// history is filed under. Handing that binding over before anything opens means the adapter
    /// reuses it rather than inventing a second identity for the same thread.
    #[tokio::test]
    async fn a_known_binding_is_reused_instead_of_inventing_a_second_identity() {
        let persisted = ThreadId::new();
        let (harness, _controller) = spawn_fake_harness_with_bootstrap(HarnessBootstrap {
            known_threads: vec![giskard_harness::KnownThreadBinding {
                harness_thread_id: "native-child".to_owned(),
                thread_id: persisted,
            }],
        });

        // The caller has no id to offer — it is resuming by native id alone.
        let opened = harness
            .open_thread(open_opts(None, Some("native-child")))
            .await
            .expect("child resumes");

        assert_eq!(
            opened.thread, persisted,
            "a thread Giskard already named must not be given a second id"
        );
    }

    /// The converse, and the property that makes reuse safe rather than merely convenient: a
    /// native thread nobody has bound gets a fresh id, and *that* id is then the binding — so
    /// reopening the same native thread keeps it, while a different one gets its own.
    #[tokio::test]
    async fn an_unbound_native_thread_gets_one_id_and_then_keeps_it() {
        let (harness, _controller) = spawn_fake_harness();

        let first = harness
            .open_thread(open_opts(None, Some("native-unbound")))
            .await
            .expect("thread opens");
        let reopened = harness
            .open_thread(open_opts(None, Some("native-unbound")))
            .await
            .expect("thread reopens");
        assert_eq!(
            reopened.thread, first.thread,
            "the id chosen on the first open is the thread's identity from then on"
        );

        let other = harness
            .open_thread(open_opts(None, Some("native-other")))
            .await
            .expect("other thread opens");
        assert_ne!(
            other.thread, first.thread,
            "a different native thread must not inherit another's id"
        );
    }

    #[tokio::test]
    async fn unknown_status_change_waits_for_exclusive_owner_activation() {
        let (harness, controller) = spawn_fake_harness();
        let mut signals = harness.take_harness_signals().unwrap();
        assert!(harness.take_harness_signals().is_err());

        controller
            .send_server_message(codex_codes::ServerMessage::Notification(
                codex_codes::Notification::ThreadStatusChanged(
                    codex_codes::ThreadStatusChangedNotification {
                        status: codex_codes::ThreadStatus::Idle,
                        thread_id: "native-unknown-status".into(),
                    },
                ),
            ))
            .await;

        let activation = timeout(Duration::from_secs(1), signals.recv())
            .await
            .expect("status change should request activation")
            .expect("signal stream should remain open");
        let HarnessSignal::Activate(activation) = activation else {
            panic!("expected activation signal");
        };
        assert_eq!(activation.route.harness_thread_id, "native-unknown-status");
        let route = activation.route.clone();
        let _events = harness.claim_event_receiver(&route).unwrap();
        assert!(harness.claim_event_receiver(&route).is_err());
        activation.readiness.acknowledge(Ok(()));
    }

    #[tokio::test]
    async fn primary_identity_response_waits_for_receiver_and_owner_acknowledgement() {
        let (harness, _controller) = spawn_fake_harness();
        let mut signals = harness.take_harness_signals().unwrap();
        let intended_thread = ThreadId::new();
        let mut opts = open_opts(Some(intended_thread), None);
        opts.identity_generation = Some(41);
        let opening_harness = harness.clone();
        let mut opening = tokio::spawn(async move { opening_harness.open_thread(opts).await });

        let signal = timeout(Duration::from_secs(1), signals.recv())
            .await
            .expect("thread/start response should request Primary activation")
            .expect("signal stream should remain open");
        let HarnessSignal::Activate(activation) = signal else {
            panic!("expected activation signal");
        };
        assert_eq!(activation.route.thread_id, intended_thread);
        assert!(matches!(
            &activation.cause,
            ThreadActivationCause::IdentityResponse {
                method,
                generation: 41,
                reported_model: Some(model),
            } if method == codex_codes::protocol::methods::THREAD_START
                && model == &test_model(None)
        ));
        assert!(
            timeout(Duration::from_millis(10), &mut opening)
                .await
                .is_err(),
            "open_thread must remain held before owner readiness"
        );

        let route = activation.route.clone();
        let _events = harness.claim_event_receiver(&route).unwrap();
        assert!(
            !lock_routes(&harness.routes)
                .expect("test route lock should be healthy")
                .by_native
                .get(&route.harness_thread_id)
                .unwrap()
                .ready,
            "claiming the receiver does not establish a Live owner"
        );
        activation.readiness.acknowledge(Ok(()));
        let handle = timeout(Duration::from_secs(1), opening)
            .await
            .expect("owner acknowledgement should release open_thread")
            .unwrap()
            .unwrap();
        assert_eq!(handle.thread, intended_thread);
        assert_eq!(handle.harness_thread_id, route.harness_thread_id);
        assert!(
            lock_routes(&harness.routes)
                .expect("test route lock should be healthy")
                .by_native
                .get(&route.harness_thread_id)
                .unwrap()
                .ready,
            "successful activation acknowledgement establishes route readiness"
        );
    }

    #[tokio::test]
    async fn start_turn_maps_image_attachment_to_codex_data_url() {
        let (mut transport, _frames, controller) = fake_codex();
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let thread = test_thread();
        let input = UserInput::text_with_attachments(
            "Inspect this",
            vec![UserAttachment {
                name: "diagram.png".into(),
                mime_type: "image/png".into(),
                size: 5,
                kind: AttachmentKind::Image,
                data_base64: "aW1hZ2U=".into(),
            }],
        );

        handle_start_turn(
            &mut transport,
            &mut mapper,
            &thread,
            &input,
            &build_turn_overrides(),
            &[],
        )
        .await
        .unwrap();

        let requests = controller.requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].method,
            codex_codes::protocol::methods::TURN_START
        );
        assert_eq!(requests[0].params["input"][0]["type"], "text");
        assert_eq!(requests[0].params["input"][0]["text"], "Inspect this");
        assert_eq!(requests[0].params["input"][1]["type"], "image");
        assert_eq!(
            requests[0].params["input"][1]["url"],
            "data:image/png;base64,aW1hZ2U="
        );
    }

    #[tokio::test]
    async fn start_turn_uploads_and_removes_file_attachment() {
        let (mut transport, _frames, controller) = fake_codex();
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let thread = test_thread();
        let input = UserInput::text_with_attachments(
            "Read this",
            vec![UserAttachment {
                name: "notes.pdf".into(),
                mime_type: "application/pdf".into(),
                size: 5,
                kind: AttachmentKind::File,
                data_base64: "ZmlsZQ==".into(),
            }],
        );

        let started = handle_start_turn(
            &mut transport,
            &mut mapper,
            &thread,
            &input,
            &build_turn_overrides(),
            &[],
        )
        .await
        .unwrap();

        let mut active_turns = ActiveTurns::new();
        active_turns.insert(
            thread.thread,
            ActiveTurn::new(thread.clone(), started.turn).with_upload_dir(started.upload_dir),
        );
        cleanup_active_turn_upload(&mut transport, &mut active_turns, thread.thread).await;

        let requests = controller.requests().await;
        assert_eq!(
            requests
                .iter()
                .map(|r| r.method.as_str())
                .collect::<Vec<_>>(),
            vec![
                codex_codes::protocol::methods::FS_CREATEDIRECTORY,
                codex_codes::protocol::methods::FS_WRITEFILE,
                codex_codes::protocol::methods::TURN_START,
                codex_codes::protocol::methods::FS_REMOVE,
            ]
        );
        assert_eq!(requests[1].params["dataBase64"], "ZmlsZQ==");
        let upload_path = requests[1].params["path"].as_str().unwrap();
        assert!(upload_path.contains("giskard-codex-uploads"));
        assert!(upload_path.ends_with("notes.pdf"));
        let turn_text = requests[2].params["input"][0]["text"].as_str().unwrap();
        assert!(
            turn_text.starts_with("Read this\n\nAttached files available on the harness host:")
        );
        assert!(turn_text.contains("notes.pdf: "));
        assert!(turn_text.contains(upload_path));
        assert_eq!(requests[3].params["path"], requests[0].params["path"]);
        assert_eq!(requests[3].params["recursive"], true);
        assert_eq!(requests[3].params["force"], true);
    }

    #[tokio::test]
    async fn cleanup_all_active_turn_uploads_removes_every_directory() {
        let (mut transport, _frames, controller) = fake_codex();
        let first = test_thread();
        let second = test_thread();
        let mut active_turns = ActiveTurns::new();
        active_turns.insert(
            first.thread,
            ActiveTurn::new(first, TurnId::new()).with_upload_dir(Some(PathBuf::from("/tmp/a"))),
        );
        active_turns.insert(
            second.thread,
            ActiveTurn::new(second, TurnId::new()).with_upload_dir(Some(PathBuf::from("/tmp/b"))),
        );

        cleanup_all_active_turn_uploads(&mut transport, &mut active_turns).await;

        let requests = controller.requests().await;
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == codex_codes::protocol::methods::FS_REMOVE)
                .count(),
            2
        );
        assert!(
            active_turns
                .values()
                .all(|active| active.upload_dir.is_none())
        );
    }

    fn generic_user_input_request(
        id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> codex_codes::ServerMessage {
        codex_codes::ServerMessage::Request {
            id: codex_codes::jsonrpc::RequestId::String(id.to_owned()),
            request: codex_codes::messages::ServerRequest::ToolRequestUserInput(
                serde_json::from_value(json!({
                    "itemId": format!("input-{id}"),
                    "threadId": native_thread_id,
                    "turnId": native_turn_id,
                    "questions": [{
                        "id": "confirm",
                        "header": "Confirm",
                        "question": "Continue?"
                    }]
                }))
                .expect("test user input request should deserialize"),
            ),
        }
    }

    fn command_approval_request(
        id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> codex_codes::ServerMessage {
        codex_codes::ServerMessage::Request {
            id: codex_codes::jsonrpc::RequestId::String(id.to_owned()),
            request: codex_codes::messages::ServerRequest::CmdExecApproval(
                serde_json::from_value(json!({
                    "approvalId": id,
                    "commandActions": [],
                    "cwd": "/tmp",
                    "environmentId": "env_1",
                    "itemId": format!("cmd-{id}"),
                    "threadId": native_thread_id,
                    "turnId": native_turn_id,
                    "startedAtMs": 123
                }))
                .expect("test approval request should deserialize"),
            ),
        }
    }

    async fn recv_matching_event(
        stream: &mut AgentEventStream,
        label: &str,
        matches: impl Fn(&AgentEvent) -> bool,
    ) -> AgentEvent {
        timeout(Duration::from_secs(1), async {
            loop {
                let event = stream.recv().await.expect("event stream should stay open");
                if matches(&event) {
                    break event;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
    }

    fn context_compacted_event(thread: ThreadId, turn: TurnId) -> AgentEvent {
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: ItemId::new(),
                harness_item_id: format!("context_compacted:{turn}"),
                payload: ItemPayload::Activity {
                    title: "Context compacted".into(),
                    detail: None,
                    metadata: None,
                    subagent: None,
                },
                created_at: Utc::now(),
            },
        }
    }

    fn completed_event(thread: ThreadId, turn: TurnId) -> AgentEvent {
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }
    }

    #[test]
    fn active_turn_table_completes_only_matching_thread_and_turn() {
        let first_thread = test_thread();
        let second_thread = ThreadHandle::opened(
            ThreadId::new(),
            "native-thread-2".into(),
            PathBuf::from("/tmp/test-workspace-2"),
        );
        let first_turn = TurnId::new();
        let second_turn = TurnId::new();
        let stale_turn = TurnId::new();
        let mut active_turns = ActiveTurns::new();
        active_turns.insert(
            first_thread.thread,
            ActiveTurn::new(first_thread.clone(), first_turn),
        );
        active_turns.insert(
            second_thread.thread,
            ActiveTurn::new(second_thread.clone(), second_turn),
        );

        assert_eq!(
            completed_current_active_turn(
                &active_turns,
                &completed_event(second_thread.thread, second_turn)
            ),
            Some((second_thread.thread, second_turn))
        );
        assert_eq!(
            completed_current_active_turn(
                &active_turns,
                &completed_event(first_thread.thread, stale_turn)
            ),
            None
        );
        assert_eq!(
            completed_current_active_turn(
                &active_turns,
                &completed_event(ThreadId::new(), first_turn)
            ),
            None
        );
    }

    #[tokio::test]
    async fn codex_worker_opens_new_thread_while_turn_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("keep running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();

        let second = timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(None, None)),
        )
        .await
        .expect("opening another thread must not wait for the active turn")
        .unwrap();

        assert_eq!(second.harness_thread_id, "native-thread-2");
        assert_eq!(
            controller
                .requests()
                .await
                .iter()
                .filter(|req| req.method == codex_codes::protocol::methods::THREAD_START)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn codex_worker_ignores_non_json_stdout_during_an_active_turn() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &thread,
                UserInput::text("keep running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut stream = claim_event_stream(&harness, &thread).await;

        controller
            .send_stream_error(CodexStreamError::NonJsonStdout {
                parse_error: "expected value at line 1 column 1".into(),
                raw_preview: "leaked command output".into(),
                raw_bytes: 21,
            })
            .await;
        controller
            .send_server_message(codex_codes::ServerMessage::Notification(
                codex_codes::messages::Notification::TurnCompleted(
                    serde_json::from_value(json!({
                        "threadId": thread.harness_thread_id,
                        "turn": { "id": native_turn, "status": "completed" }
                    }))
                    .expect("test completion should deserialize"),
                ),
            ))
            .await;
        recv_matching_event(
            &mut stream,
            "turn completion after non-JSON stdout",
            |event| {
                matches!(event, AgentEvent::TurnCompleted { status, .. }
                if status.kind == TurnStatusKind::Completed)
            },
        )
        .await;

        let models = timeout(Duration::from_secs(1), harness.list_models())
            .await
            .expect("a consumed non-JSON line must not close the worker")
            .unwrap();
        assert_eq!(models.len(), 2);

        let second = timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(None, None)),
        )
        .await
        .expect("the worker must continue accepting thread operations")
        .unwrap();
        assert_eq!(second.harness_thread_id, "native-thread-2");
    }

    #[tokio::test]
    async fn fatal_stream_error_closes_worker_while_idle() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        controller
            .send_stream_error(CodexStreamError::Fatal(HarnessError::Transport(
                "connection lost".into(),
            )))
            .await;

        timeout(Duration::from_secs(1), async {
            while !harness.worker_queue.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fatal stream error must close the worker");

        assert!(harness.compact_thread(&thread).await.is_err());

        assert!(matches!(
            harness.list_models().await,
            Err(HarnessError::Transport(message)) if message == "background task closed"
        ));
    }

    #[tokio::test]
    async fn codex_worker_resumes_thread_while_turn_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("keep running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let resumed_thread = ThreadId::new();

        let resumed = timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(Some(resumed_thread), Some("native-existing"))),
        )
        .await
        .expect("resuming another thread must not wait for the active turn")
        .unwrap();

        assert_eq!(resumed.thread, resumed_thread);
        assert_eq!(resumed.harness_thread_id, "native-existing");
        assert_eq!(
            resumed.parent_harness_thread_id.as_deref(),
            Some("native-parent")
        );
        assert!(controller.requests().await.iter().any(|req| {
            req.method == codex_codes::protocol::methods::THREAD_RESUME
                && req.params["threadId"] == "native-existing"
        }));
    }

    #[tokio::test]
    async fn normal_resume_keeps_fresh_thread_recovery_after_missing_rollout() {
        let (harness, controller) = spawn_fake_harness();
        controller.fail_thread_resume_missing_rollout(1).await;

        let opened = harness
            .open_thread(open_opts(None, Some("native-missing")))
            .await
            .unwrap();

        assert_eq!(opened.harness_thread_id, "native-thread-1");
        assert_eq!(
            opened.warning.as_ref().map(|warning| warning.code.as_str()),
            Some("codex_resume_failed")
        );
        let requests = controller.requests().await;
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == codex_codes::protocol::methods::THREAD_RESUME)
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == codex_codes::protocol::methods::THREAD_START)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn consumed_resume_rejection_does_not_fail_successful_primary_fallback() {
        let (harness, controller) = spawn_fake_harness();
        controller.fail_thread_resume_missing_rollout(1).await;
        let mut signals = harness
            .take_harness_signals()
            .expect("signal stream should be available");
        let thread_id = ThreadId::new();
        let mut opts = open_opts(Some(thread_id), Some("native-missing"));
        opts.identity_generation = Some(7);
        let opening_harness = harness.clone();
        let opening = tokio::spawn(async move { opening_harness.open_thread(opts).await });

        let activation = signals
            .recv()
            .await
            .expect("fallback identity should request activation");
        let HarnessSignal::Activate(activation) = activation else {
            panic!("consumed resume rejection must not publish identity failure");
        };
        assert_eq!(activation.route.thread_id, thread_id);
        assert!(matches!(
            &activation.cause,
            ThreadActivationCause::IdentityResponse {
                method,
                generation: 7,
                ..
            } if method == codex_codes::protocol::methods::THREAD_START
        ));
        let _events = harness
            .claim_event_receiver(&activation.route)
            .expect("activation owner should claim the exclusive event receiver");
        activation.readiness.acknowledge(Ok(()));

        let opened = opening.await.unwrap().unwrap();
        assert_eq!(opened.thread, thread_id);
        assert_eq!(opened.harness_thread_id, "native-thread-1");
        harness.shutdown().await.unwrap();
        while let Some(signal) = signals.recv().await {
            assert!(
                !matches!(signal, HarnessSignal::PrimaryIdentityFailed { .. }),
                "successful fresh-thread fallback must not be failed by the consumed resume rejection"
            );
        }
    }

    #[tokio::test]
    async fn normal_resume_does_not_start_fresh_thread_after_ambiguous_failure() {
        for error in [
            HarnessError::Timeout("thread/resume timed out".into()),
            HarnessError::Transport("connection closed before thread/resume response".into()),
            HarnessError::Protocol("thread/resume response was malformed".into()),
        ] {
            let (harness, controller) = spawn_fake_harness();
            controller.fail_next_thread_resume(error.clone()).await;

            let result = harness
                .open_thread(open_opts(None, Some("native-ambiguous")))
                .await;
            assert_eq!(
                result
                    .expect_err("ambiguous resume failure must surface")
                    .to_string(),
                error.to_string()
            );

            let requests = controller.requests().await;
            assert_eq!(
                requests
                    .iter()
                    .filter(
                        |request| request.method == codex_codes::protocol::methods::THREAD_RESUME
                    )
                    .count(),
                1
            );
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.method == codex_codes::protocol::methods::THREAD_START)
                    .count(),
                0,
                "ambiguous resume failure must not start a second native thread"
            );
        }
    }

    #[tokio::test]
    async fn codex_worker_starts_other_thread_turn_while_first_turn_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("keep running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();

        let second_turn = timeout(
            Duration::from_secs(1),
            harness.start_turn(
                &second,
                UserInput::text("run concurrently"),
                build_turn_overrides(),
            ),
        )
        .await
        .expect("starting another thread turn must not wait for the first turn")
        .unwrap();

        let started = controller.started_turns().await;
        assert_eq!(started.len(), 2);
        assert_eq!(started[0].native_thread_id, first.harness_thread_id);
        assert_eq!(started[1].native_thread_id, second.harness_thread_id);
        assert_ne!(started[0].native_turn_id, started[1].native_turn_id);
        assert!(second_turn != TurnId::default());
    }

    #[tokio::test]
    async fn codex_worker_pending_server_request_does_not_block_other_thread_start() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let first_turn = harness
            .start_turn(&first, UserInput::text("ask later"), build_turn_overrides())
            .await
            .unwrap();
        let first_native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut first_stream = claim_event_stream(&harness, &first).await;

        controller
            .send_server_message(generic_user_input_request(
                "server_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;

        let event = recv_matching_event(&mut first_stream, "server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived {
                    thread,
                    turn,
                    request,
                } if *thread == first.thread
                    && *turn == Some(first_turn)
                    && request.id == ServerRequestId("server_req".into())
            )
        })
        .await;
        assert!(matches!(event, AgentEvent::ServerRequestReceived { .. }));

        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        timeout(
            Duration::from_secs(1),
            harness.start_turn(
                &second,
                UserInput::text("not blocked"),
                build_turn_overrides(),
            ),
        )
        .await
        .expect("pending server request in one thread must not block another thread")
        .unwrap();
    }

    #[tokio::test]
    async fn codex_worker_routes_server_request_response_while_other_thread_is_active() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("ask a question"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        harness
            .start_turn(
                &second,
                UserInput::text("also running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let first_native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut first_stream = claim_event_stream(&harness, &first).await;
        controller
            .send_server_message(generic_user_input_request(
                "server_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived { request, .. }
                    if request.id == ServerRequestId("server_req".into())
            )
        })
        .await;

        timeout(
            Duration::from_secs(1),
            harness.respond_server_request(
                ServerRequestId("server_req".into()),
                ServerRequestResponse::result(json!({"answer": true})),
            ),
        )
        .await
        .expect("server request response must be routed while another thread is active")
        .unwrap();

        let responses = controller.responses().await;
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].id,
            codex_codes::jsonrpc::RequestId::String("server_req".into())
        );
        assert_eq!(responses[0].value, json!({"answer": true}));
        recv_matching_event(&mut first_stream, "server request resolution", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestResolved { request_id, .. }
                    if *request_id == ServerRequestId("server_req".into())
            )
        })
        .await;
    }

    #[tokio::test]
    async fn codex_worker_terminates_numeric_process_with_background_terminal_api() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();

        timeout(
            Duration::from_secs(1),
            harness.terminate_command(&thread, "123"),
        )
        .await
        .expect("terminate command should complete")
        .unwrap();

        let requests = controller.requests().await;
        assert!(requests.iter().any(|req| {
            req.method == THREAD_BACKGROUND_TERMINALS_TERMINATE
                && req.params["threadId"] == thread.harness_thread_id
                && req.params["processId"] == "123"
        }));
        assert!(!requests.iter().any(|req| {
            req.method == codex_codes::protocol::methods::TURN_INTERRUPT
                || req.method == codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE
        }));
    }

    #[tokio::test]
    async fn codex_worker_terminates_non_numeric_process_with_command_exec_api() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();

        timeout(
            Duration::from_secs(1),
            harness.terminate_command(&thread, "session-a"),
        )
        .await
        .expect("terminate command should complete")
        .unwrap();

        let requests = controller.requests().await;
        assert!(requests.iter().any(|req| {
            req.method == codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE
                && req.params["processId"] == "session-a"
        }));
        assert!(!requests.iter().any(|req| {
            req.method == THREAD_BACKGROUND_TERMINALS_TERMINATE
                || req.method == codex_codes::protocol::methods::TURN_INTERRUPT
        }));
    }

    #[tokio::test]
    async fn importing_a_thread_takes_its_model_and_effort_from_codex() {
        let (harness, controller) = spawn_fake_harness();

        // No model named: this is importing a native thread whose model is Codex's to report.
        let handle = timeout(
            Duration::from_secs(1),
            harness.open_thread(OpenThreadOptions {
                project: ProjectId::new(),
                thread: Some(ThreadId::new()),
                workspace_root: PathBuf::from("/tmp"),
                resume: Some("native-existing".into()),
                initial_model: None,
                identity_generation: None,
                updates: giskard_harness::thread_update_channel().0,
            }),
        )
        .await
        .expect("open_thread should complete")
        .expect("open_thread should succeed");

        assert_eq!(
            handle.resumed_model,
            Some(giskard_core::model::ModelRef {
                provider: "resumed-provider".into(),
                model: "resumed-model".into(),
                // The picker has to land on the effort the thread is actually running, not
                // "Default": Codex reports it on resume, so dropping it would understate the
                // thread's own settings.
                reasoning_effort: Some(giskard_core::model::Effort::new("high")),
            })
        );

        let resume = controller
            .requests()
            .await
            .into_iter()
            .find(|req| req.method == codex_codes::protocol::methods::THREAD_RESUME)
            .expect("a thread/resume request");
        assert!(
            resume.params.get("model").is_none() && resume.params.get("modelProvider").is_none(),
            "an import must send no model override, which would suppress the thread's own: {:?}",
            resume.params
        );
    }

    #[tokio::test]
    async fn resumed_usage_is_forwarded_as_a_turnless_thread_update() {
        let (harness, controller) = spawn_fake_harness();
        let (updates, mut update_stream) = giskard_harness::thread_update_channel();
        let thread = ThreadId::new();
        harness
            .open_thread(OpenThreadOptions {
                project: ProjectId::new(),
                thread: Some(thread),
                workspace_root: PathBuf::from("/tmp"),
                resume: Some("native-existing".into()),
                initial_model: Some(test_model(None)),
                identity_generation: None,
                updates,
            })
            .await
            .unwrap();
        controller
            .send_server_message(codex_codes::ServerMessage::Notification(
                codex_codes::messages::Notification::ThreadTokenUsageUpdated(
                    serde_json::from_value(json!({
                        "threadId": "native-existing", "turnId": "historical-turn",
                        "tokenUsage": { "last": { "cachedInputTokens": 0, "inputTokens": 1,
                            "outputTokens": 1, "reasoningOutputTokens": 0, "totalTokens": 2 },
                            "total": { "cachedInputTokens": 0, "inputTokens": 1,
                            "outputTokens": 1, "reasoningOutputTokens": 0, "totalTokens": 2 },
                            "modelContextWindow": 258400 }
                    }))
                    .unwrap(),
                ),
            ))
            .await;
        let update = timeout(Duration::from_secs(1), update_stream.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            update,
            ThreadUpdate::ContextWindowRestored {
                model: test_model(Some(giskard_core::model::Effort("high".into()))),
                context_window: 258_400,
            }
        );
    }

    #[tokio::test]
    async fn codex_list_providers_reads_the_config_model_providers_table() {
        let (harness, controller) = spawn_fake_harness();

        assert!(
            harness.capabilities().provider_listing,
            "Codex harness should advertise provider listing"
        );

        let providers = timeout(Duration::from_secs(1), harness.list_providers())
            .await
            .expect("list_providers should complete")
            .expect("list_providers should succeed");

        let by_id = |id: &str| {
            providers
                .iter()
                .find(|p| p.id == id)
                .unwrap_or_else(|| panic!("provider {id} missing from {providers:?}"))
                .clone()
        };

        // Built-ins are always routable even though Codex never lists them in `model_providers`.
        for built_in in CODEX_BUILT_IN_PROVIDER_IDS {
            assert!(
                providers.iter().any(|p| p.id == built_in),
                "built-in {built_in} should be routable: {providers:?}"
            );
        }

        let litellm = by_id("litellm");
        assert_eq!(litellm.name.as_deref(), Some("LiteLLM"));
        assert_eq!(
            litellm.base_url.as_deref(),
            Some("http://127.0.0.1:4000/v1")
        );
        assert_eq!(
            litellm.auth,
            Some(ProviderAuth::Env("LITELLM_KEY".into())),
            "env_key should map to the env arm"
        );

        // Codex defaults an omitted `name` to "", which is absence, not a display name.
        let unnamed = by_id("unnamed");
        assert_eq!(unnamed.name, None);
        assert_eq!(unnamed.auth, None);

        // `[model_providers.opencodex.auth]` — a command whose stdout is the bearer token.
        let opencodex = by_id("opencodex");
        assert_eq!(
            opencodex.auth,
            Some(ProviderAuth::Command(ProviderAuthCommand {
                command: "sh".into(),
                args: vec!["-c".into(), "printf %s \"$OPENCODEX_KEY\"".into()],
                cwd: Some(PathBuf::from("/tmp")),
                timeout: Duration::from_millis(2500),
            })),
            "the auth table should map to the command arm, timeout included"
        );

        // An `auth` table Giskard cannot make sense of must not fail the whole `config/read`: the
        // same response carries `sandbox_workspace_write.writable_roots`, so a strict parse here
        // would silently narrow the sandbox. The provider still resolves by its `env_key`.
        let unfamiliar = by_id("unfamiliar-auth");
        assert_eq!(
            unfamiliar.auth,
            Some(ProviderAuth::Env("FALLBACK_KEY".into())),
            "an auth table with no usable command falls back rather than failing the read"
        );
        assert_eq!(
            unfamiliar.base_url.as_deref(),
            Some("http://127.0.0.1:7000/v1"),
            "the rest of the entry survives"
        );

        let requests = controller.requests().await;
        assert!(
            requests.iter().any(|req| req.method == "config/read"),
            "list_providers should issue a config/read request"
        );
    }

    #[tokio::test]
    async fn codex_list_providers_surfaces_config_read_failure() {
        let (harness, controller) = spawn_fake_harness();
        controller.fail_config_read("config/read exploded").await;

        let result = timeout(Duration::from_secs(1), harness.list_providers())
            .await
            .expect("list_providers should complete");

        match result {
            Err(HarnessError::Transport(message)) => assert!(
                message.contains("config/read exploded"),
                "transport failure should carry the cause: {message}"
            ),
            other => panic!("expected a transport failure, got {other:?}"),
        }
    }

    /// Without the routing provider there is no correct attribution, and guessing the default
    /// would put routes in the picker that do not exist for anyone whose effective provider is
    /// something else. The listing fails instead, which the caller already surfaces as a warning.
    #[tokio::test]
    async fn codex_list_models_fails_rather_than_guess_the_provider() {
        let (harness, controller) = spawn_fake_harness();
        controller.fail_config_read("config/read exploded").await;

        let result = timeout(Duration::from_secs(1), harness.list_models())
            .await
            .expect("list_models should complete");

        let err = result.expect_err("an unattributable catalog is not served");
        assert!(
            err.to_string().contains("config/read exploded"),
            "the real cause is reported: {err}"
        );
    }

    /// Codex's `model/list` names no provider, so the adapter attributes the catalog to the one
    /// Codex routes to. A config that sets `model_provider` is followed rather than defaulted.
    #[tokio::test]
    async fn codex_list_models_attributes_the_catalog_to_the_configured_provider() {
        let (harness, controller) = spawn_fake_harness();
        controller.set_config_model_provider("opencodex").await;

        let models = timeout(Duration::from_secs(1), harness.list_models())
            .await
            .expect("list_models should complete")
            .unwrap();

        assert!(!models.is_empty());
        assert!(
            models.iter().all(|m| m.provider == "opencodex"),
            "every entry follows Codex's routing provider: {:?}",
            models
                .iter()
                .map(|m| m.provider.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn codex_list_models_maps_model_list_rpc_to_descriptors() {
        let (harness, controller) = spawn_fake_harness();

        assert!(
            harness.capabilities().model_listing,
            "Codex harness should advertise model listing"
        );

        let models = timeout(Duration::from_secs(1), harness.list_models())
            .await
            .expect("list_models should complete")
            .unwrap();

        // The hidden Codex model is filtered out; only picker-visible models remain.
        assert_eq!(models.len(), 2);

        let flagship = &models[0];
        assert_eq!(flagship.model, "gpt-5.5");
        assert_eq!(flagship.display_name.as_deref(), Some("GPT-5.5"));
        assert!(
            flagship.supports_reasoning_effort,
            "gpt-5.5 advertises reasoning efforts"
        );
        // The exact effort levels from the catalog are preserved for the picker.
        assert_eq!(flagship.reasoning_efforts, vec!["medium", "high"]);
        // model/list carries no provider of its own, so entries are attributed to the provider
        // Codex routes to — `openai` here, the built-in default, since this config sets no
        // `model_provider`. Without that a stock setup has nothing to put in the picker.
        assert_eq!(flagship.provider, "openai");
        // The context window is still absent from this RPC.
        assert_eq!(
            flagship.context_window,
            ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW
        );

        let mini = &models[1];
        assert_eq!(mini.model, "gpt-5.5-mini");
        assert_eq!(mini.display_name.as_deref(), Some("GPT-5.5 mini"));
        assert!(
            mini.supports_reasoning_effort,
            "a non-none default is the sole effort when alternatives are empty"
        );
        assert_eq!(mini.reasoning_efforts, vec!["medium"]);

        assert!(
            controller
                .requests()
                .await
                .iter()
                .any(|req| req.method == codex_codes::protocol::methods::MODEL_LIST),
            "list_models should issue a model/list request"
        );
    }

    #[tokio::test]
    async fn codex_list_models_surfaces_transport_failure() {
        let (harness, controller) = spawn_fake_harness();
        controller.fail_model_list("model/list exploded").await;

        let result = timeout(Duration::from_secs(1), harness.list_models())
            .await
            .expect("list_models should complete");

        match result {
            Err(HarnessError::Transport(message)) => {
                assert!(
                    message.contains("model/list exploded"),
                    "unexpected error message: {message}"
                );
            }
            other => panic!("expected a transport error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn codex_delete_is_idempotent_when_matching_rollout_is_missing() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        controller
            .fail_thread_delete(&format!(
                "no rollout found for thread id {}",
                thread.harness_thread_id
            ))
            .await;

        timeout(Duration::from_secs(1), harness.delete_thread(&thread))
            .await
            .expect("delete_thread should complete")
            .expect("an already-absent matching rollout should be idempotent success");

        assert!(controller.requests().await.iter().any(|request| {
            request.method == codex_codes::protocol::methods::THREAD_DELETE
                && request.params["threadId"] == thread.harness_thread_id
        }));
    }

    #[tokio::test]
    async fn codex_delete_preserves_nonmatching_transport_failure() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        controller
            .fail_thread_delete("no rollout found for thread id different-thread")
            .await;

        let error = timeout(Duration::from_secs(1), harness.delete_thread(&thread))
            .await
            .expect("delete_thread should complete")
            .expect_err("a nonmatching missing-rollout error must remain fatal");
        assert!(matches!(
            error,
            HarnessError::ProviderRejected { code: -32600, message }
                if message.ends_with("different-thread")
        ));
    }

    #[tokio::test]
    async fn codex_worker_surfaces_process_terminate_failure_without_interrupting_turn() {
        let (harness, controller) = spawn_fake_harness();
        controller.background_terminal_terminate_result(false).await;
        controller
            .fail_command_exec_terminate("no active command/exec for process id 123")
            .await;
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &thread,
                UserInput::text("run command"),
                build_turn_overrides(),
            )
            .await
            .unwrap();

        let err = timeout(
            Duration::from_secs(1),
            harness.terminate_command(&thread, "123"),
        )
        .await
        .expect("terminate command should complete")
        .expect_err("failed process termination should surface to the caller");
        assert!(
            matches!(err, HarnessError::Transport(message) if message.contains("no active command/exec"))
        );

        let requests = controller.requests().await;
        assert!(requests.iter().any(|req| {
            req.method == THREAD_BACKGROUND_TERMINALS_TERMINATE
                && req.params["threadId"] == thread.harness_thread_id
                && req.params["processId"] == "123"
        }));
        assert!(requests.iter().any(|req| {
            req.method == codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE
                && req.params["processId"] == "123"
        }));
        assert!(
            !requests
                .iter()
                .any(|req| req.method == codex_codes::protocol::methods::TURN_INTERRUPT)
        );
    }

    #[tokio::test]
    async fn codex_worker_routes_approval_response_while_other_thread_is_active() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("needs approval"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        harness
            .start_turn(
                &second,
                UserInput::text("also running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let first_native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut first_stream = claim_event_stream(&harness, &first).await;
        controller
            .send_server_message(command_approval_request(
                "approval_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "approval request", |event| {
            matches!(
                event,
                AgentEvent::ApprovalRequested { request, .. }
                    if request.id == ApprovalId("approval_req".into())
            )
        })
        .await;

        timeout(
            Duration::from_secs(1),
            harness.respond_approval(ApprovalId("approval_req".into()), ApprovalDecision::Accept),
        )
        .await
        .expect("approval response must be routed while another thread is active")
        .unwrap();

        let responses = controller.responses().await;
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].id,
            codex_codes::jsonrpc::RequestId::String("approval_req".into())
        );
        assert_eq!(responses[0].value, json!({"decision": "accept"}));
    }

    #[tokio::test]
    async fn codex_worker_interrupt_rejects_only_interrupted_thread_requests() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("waits on input"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        harness
            .start_turn(
                &second,
                UserInput::text("also waits"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let started = controller.started_turns().await;
        let first_native_turn = started[0].native_turn_id.clone();
        let second_native_turn = started[1].native_turn_id.clone();
        let mut first_stream = claim_event_stream(&harness, &first).await;
        let mut second_stream = claim_event_stream(&harness, &second).await;

        controller
            .send_server_message(generic_user_input_request(
                "first_server_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "first server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived { request, .. }
                    if request.id == ServerRequestId("first_server_req".into())
            )
        })
        .await;
        controller
            .send_server_message(command_approval_request(
                "first_approval_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "first approval request", |event| {
            matches!(
                event,
                AgentEvent::ApprovalRequested { request, .. }
                    if request.id == ApprovalId("first_approval_req".into())
            )
        })
        .await;
        controller
            .send_server_message(generic_user_input_request(
                "second_server_req",
                &second.harness_thread_id,
                &second_native_turn,
            ))
            .await;
        recv_matching_event(&mut second_stream, "second server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived { request, .. }
                    if request.id == ServerRequestId("second_server_req".into())
            )
        })
        .await;

        timeout(Duration::from_secs(1), harness.interrupt(&first))
            .await
            .expect("interrupt must be processed while another thread is active")
            .unwrap();

        let requests = controller.requests().await;
        assert!(requests.iter().any(|req| {
            req.method == codex_codes::protocol::methods::TURN_INTERRUPT
                && req.params["threadId"] == first.harness_thread_id
                && req.params["turnId"] == first_native_turn
        }));
        let responses = controller.responses().await;
        assert!(responses.iter().any(|response| {
            response.id == codex_codes::jsonrpc::RequestId::String("first_approval_req".into())
                && response.value == json!({"decision": "cancel"})
        }));
        let response_errors = controller.response_errors().await;
        assert!(response_errors.iter().any(|error| {
            error.id == codex_codes::jsonrpc::RequestId::String("first_server_req".into())
        }));
        assert!(!response_errors.iter().any(|error| {
            error.id == codex_codes::jsonrpc::RequestId::String("second_server_req".into())
        }));

        timeout(
            Duration::from_secs(1),
            harness.respond_server_request(
                ServerRequestId("second_server_req".into()),
                ServerRequestResponse::result(json!({"still": "routable"})),
            ),
        )
        .await
        .expect("interrupting one thread must not discard another thread request")
        .unwrap();
        let responses = controller.responses().await;
        assert!(responses.iter().any(|response| {
            response.id == codex_codes::jsonrpc::RequestId::String("second_server_req".into())
                && response.value == json!({"still": "routable"})
        }));
    }

    #[tokio::test]
    async fn codex_worker_recovers_after_hung_interrupt_request() {
        let (harness, controller) = spawn_fake_harness();
        controller
            .hang_method(codex_codes::protocol::methods::TURN_INTERRUPT)
            .await;
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(&thread, UserInput::text("first"), build_turn_overrides())
            .await
            .unwrap();

        let err = timeout(Duration::from_secs(1), harness.interrupt(&thread))
            .await
            .expect("worker-side timeout should answer the harness caller")
            .expect_err("hung interrupt should return a timeout");
        assert!(matches!(err, HarnessError::Timeout(_)));

        timeout(
            Duration::from_secs(1),
            harness.start_turn(&thread, UserInput::text("second"), build_turn_overrides()),
        )
        .await
        .expect("worker must keep processing commands after a hung interrupt")
        .unwrap();

        assert_eq!(controller.started_turns().await.len(), 2);
    }

    #[tokio::test]
    async fn held_interrupt_response_does_not_block_approval_reply() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &thread,
                UserInput::text("needs approval"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut stream = claim_event_stream(&harness, &thread).await;
        controller
            .send_server_message(command_approval_request(
                "approval-during-interrupt",
                &thread.harness_thread_id,
                &native_turn,
            ))
            .await;
        recv_matching_event(&mut stream, "approval request", |event| {
            matches!(
                event,
                AgentEvent::ApprovalRequested { request, .. }
                    if request.id == ApprovalId("approval-during-interrupt".into())
            )
        })
        .await;
        controller
            .hang_method(codex_codes::protocol::methods::TURN_INTERRUPT)
            .await;

        let interrupt_harness = harness.clone();
        let interrupt_thread = thread.clone();
        let interrupt =
            tokio::spawn(async move { interrupt_harness.interrupt(&interrupt_thread).await });
        timeout(Duration::from_secs(1), async {
            while !controller
                .requests()
                .await
                .iter()
                .any(|request| request.method == codex_codes::protocol::methods::TURN_INTERRUPT)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("interrupt must be submitted before its response is available");
        assert!(!interrupt.is_finished());

        timeout(
            Duration::from_secs(1),
            harness.respond_approval(
                ApprovalId("approval-during-interrupt".into()),
                ApprovalDecision::Accept,
            ),
        )
        .await
        .expect("approval reply must bypass the held interrupt response")
        .unwrap();
        assert!(controller.responses().await.iter().any(|response| {
            response.id
                == codex_codes::jsonrpc::RequestId::String("approval-during-interrupt".into())
                && response.value == json!({"decision": "accept"})
        }));

        let error = timeout(Duration::from_secs(1), interrupt)
            .await
            .expect("held interrupt should retain its bounded timeout")
            .expect("interrupt task should not panic")
            .expect_err("held interrupt response should time out");
        assert!(matches!(error, HarnessError::Timeout(_)));
    }

    #[tokio::test]
    async fn held_termination_response_does_not_block_approval_reply() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &thread,
                UserInput::text("needs approval"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut stream = claim_event_stream(&harness, &thread).await;
        controller
            .send_server_message(command_approval_request(
                "approval-during-termination",
                &thread.harness_thread_id,
                &native_turn,
            ))
            .await;
        recv_matching_event(&mut stream, "approval request", |event| {
            matches!(event, AgentEvent::ApprovalRequested { request, .. }
                if request.id == ApprovalId("approval-during-termination".into()))
        })
        .await;
        controller
            .hang_method(THREAD_BACKGROUND_TERMINALS_TERMINATE)
            .await;

        let terminating_harness = harness.clone();
        let terminating_thread = thread.clone();
        let termination = tokio::spawn(async move {
            terminating_harness
                .terminate_command(&terminating_thread, "123")
                .await
        });
        timeout(Duration::from_secs(1), async {
            while !controller
                .requests()
                .await
                .iter()
                .any(|request| request.method == THREAD_BACKGROUND_TERMINALS_TERMINATE)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("termination must be submitted before its response is available");

        harness
            .respond_approval(
                ApprovalId("approval-during-termination".into()),
                ApprovalDecision::Accept,
            )
            .await
            .expect("approval reply must bypass the held termination response");
        assert!(controller.responses().await.iter().any(|response| {
            response.id
                == codex_codes::jsonrpc::RequestId::String("approval-during-termination".into())
        }));
        termination
            .await
            .expect("termination task should not panic")
            .expect("command/exec fallback should complete termination");
    }

    #[tokio::test]
    async fn termination_write_bypasses_blocked_normal_rpc() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        controller
            .hang_method(codex_codes::protocol::methods::TURN_START)
            .await;
        let turn_harness = harness.clone();
        let turn_thread = thread.clone();
        let starting = tokio::spawn(async move {
            turn_harness
                .start_turn(
                    &turn_thread,
                    UserInput::text("blocked"),
                    build_turn_overrides(),
                )
                .await
        });
        timeout(Duration::from_secs(1), async {
            while !controller
                .requests()
                .await
                .iter()
                .any(|request| request.method == codex_codes::protocol::methods::TURN_START)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("normal turn request should be in flight");

        harness
            .terminate_command(&thread, "session-urgent")
            .await
            .expect("termination should use the urgent reserved transport");
        assert!(controller.requests().await.iter().any(|request| {
            request.method == codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE
                && request.params["processId"] == "session-urgent"
        }));
        assert!(matches!(
            starting.await.unwrap(),
            Err(HarnessError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn codex_worker_recovers_after_hung_turn_start_request() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        controller
            .hang_method(codex_codes::protocol::methods::TURN_START)
            .await;

        let err = timeout(
            Duration::from_secs(1),
            harness.start_turn(&thread, UserInput::text("first"), build_turn_overrides()),
        )
        .await
        .expect("worker-side timeout should answer the start-turn caller")
        .expect_err("hung turn/start should return a timeout");
        assert!(matches!(err, HarnessError::Timeout(_)));

        controller
            .resume_method(codex_codes::protocol::methods::TURN_START)
            .await;
        timeout(
            Duration::from_secs(1),
            harness.start_turn(&thread, UserInput::text("second"), build_turn_overrides()),
        )
        .await
        .expect("worker must keep processing commands after a hung turn/start")
        .unwrap();

        assert_eq!(controller.started_turns().await.len(), 1);
    }

    #[tokio::test]
    async fn codex_worker_recovers_after_hung_approval_response() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &thread,
                UserInput::text("needs approval"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut stream = claim_event_stream(&harness, &thread).await;

        controller
            .send_server_message(command_approval_request(
                "approval_req",
                &thread.harness_thread_id,
                &native_turn,
            ))
            .await;
        recv_matching_event(&mut stream, "approval request", |event| {
            matches!(
                event,
                AgentEvent::ApprovalRequested { request, .. }
                    if request.id == ApprovalId("approval_req".into())
            )
        })
        .await;

        controller.hang_json_responses().await;
        let err = timeout(
            Duration::from_secs(1),
            harness.respond_approval(ApprovalId("approval_req".into()), ApprovalDecision::Accept),
        )
        .await
        .expect("worker-side timeout should answer the approval caller")
        .expect_err("hung approval response should return a timeout");
        assert!(matches!(err, HarnessError::Timeout(_)));

        controller.resume_json_responses().await;
        timeout(
            Duration::from_secs(1),
            harness.respond_approval(ApprovalId("approval_req".into()), ApprovalDecision::Accept),
        )
        .await
        .expect("approval retry should complete")
        .expect("approval retry should reuse the pending native request");
        let matching = controller
            .responses()
            .await
            .into_iter()
            .filter(|response| {
                response.id == codex_codes::jsonrpc::RequestId::String("approval_req".into())
            })
            .count();
        assert_eq!(
            matching, 1,
            "the native approval must be answered exactly once"
        );

        timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(None, None)),
        )
        .await
        .expect("worker must keep processing commands after a hung approval response")
        .unwrap();
    }

    #[tokio::test]
    async fn failed_server_response_write_can_retry_same_native_request() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &thread,
                UserInput::text("needs input"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut stream = claim_event_stream(&harness, &thread).await;
        controller
            .send_server_message(generic_user_input_request(
                "server_retry",
                &thread.harness_thread_id,
                &native_turn,
            ))
            .await;
        recv_matching_event(&mut stream, "server request", |event| {
            matches!(event, AgentEvent::ServerRequestReceived { request, .. }
                if request.id == ServerRequestId("server_retry".into()))
        })
        .await;

        controller.hang_json_responses().await;
        let first = harness
            .respond_server_request(
                ServerRequestId("server_retry".into()),
                ServerRequestResponse::result(json!({"answer": 1})),
            )
            .await;
        assert!(matches!(first, Err(HarnessError::Timeout(_))));

        controller.resume_json_responses().await;
        harness
            .respond_server_request(
                ServerRequestId("server_retry".into()),
                ServerRequestResponse::result(json!({"answer": 2})),
            )
            .await
            .expect("server response retry should reuse the pending native request");
        let matching = controller
            .responses()
            .await
            .into_iter()
            .filter(|response| {
                response.id == codex_codes::jsonrpc::RequestId::String("server_retry".into())
            })
            .count();
        assert_eq!(
            matching, 1,
            "the native server request must be answered once"
        );
    }

    #[tokio::test]
    async fn blocked_cleanups_leave_forced_kill_reap_reserve_for_transport() {
        let (mut client, _frames, controller) = fake_codex();
        controller
            .hang_method(codex_codes::protocol::methods::FS_REMOVE)
            .await;
        let mut active_turns = HashMap::new();
        for index in 0..32 {
            let thread = ThreadHandle::opened(
                ThreadId::new(),
                format!("native-shutdown-{index}"),
                PathBuf::from("/tmp/test-workspace"),
            );
            active_turns.insert(
                thread.thread,
                ActiveTurn::new(thread, TurnId::new())
                    .with_upload_dir(Some(PathBuf::from(format!("/tmp/codex-upload-{index}")))),
            );
        }
        let state = Arc::new(StdMutex::new(BackgroundState {
            mapper: CodexMapper::new(PathBuf::from("/tmp/test-workspace")),
            pending_compactions: HashMap::new(),
            pending_context_restores: HashMap::new(),
            active_turns,
        }));
        let routes = Arc::new(StdMutex::new(NativeRoutes::default()));
        let (cleanup_deadline, deadline) = codex_shutdown_deadlines();
        assert_eq!(
            deadline.duration_since(cleanup_deadline),
            transport::FORCED_KILL_REAP_RESERVE
        );

        finish_background_state(&mut client, &state, &routes, None, cleanup_deadline).await;
        shutdown_codex_transport(
            client,
            std::path::Path::new("/tmp/test-workspace"),
            deadline,
        )
        .await;

        let remaining = controller
            .shutdown_remaining()
            .await
            .expect("transport shutdown should record its remaining deadline");
        assert!(
            !remaining.is_zero(),
            "cleanup must leave time for the transport to issue kill and reap"
        );
        assert!(remaining <= transport::FORCED_KILL_REAP_RESERVE);
        assert_eq!(controller.shutdowns().await, 1);
    }

    #[tokio::test]
    async fn codex_worker_drops_transport_after_hung_shutdown() {
        let (harness, controller) = spawn_fake_harness();
        controller.hang_shutdown().await;

        timeout(Duration::from_secs(1), harness.shutdown())
            .await
            .expect("bounded transport shutdown should complete")
            .unwrap();
        assert_eq!(controller.shutdowns().await, 1);

        let err = timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(None, None)),
        )
        .await
        .expect("bounded shutdown should eventually drop the worker receiver")
        .expect_err("worker should be closed after shutdown");
        assert!(matches!(err, HarnessError::Transport(_)));
    }

    #[tokio::test]
    async fn cancelled_shutdown_caller_does_not_cancel_worker_teardown() {
        let (harness, controller) = spawn_fake_harness();
        controller.block_shutdown().await;

        let first_harness = harness.clone();
        let first = tokio::spawn(async move { first_harness.shutdown().await });
        timeout(Duration::from_secs(1), async {
            while controller.shutdowns().await == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("transport shutdown should start");
        assert!(!first.is_finished());
        first.abort();

        let second_harness = harness.clone();
        let second = tokio::spawn(async move { second_harness.shutdown().await });
        tokio::task::yield_now().await;
        assert!(
            !second.is_finished(),
            "later callers must await transport completion"
        );
        controller.release_shutdown().await;
        second.await.unwrap().unwrap();
        assert_eq!(controller.shutdowns().await, 1);
    }

    #[tokio::test]
    async fn concurrent_shutdown_callers_wait_for_one_worker_teardown() {
        let (harness, controller) = spawn_fake_harness();
        controller.block_shutdown().await;

        let first_harness = harness.clone();
        let first = tokio::spawn(async move { first_harness.shutdown().await });
        let second_harness = harness.clone();
        let second = tokio::spawn(async move { second_harness.shutdown().await });
        timeout(Duration::from_secs(1), async {
            while controller.shutdowns().await == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("transport shutdown should start");
        assert!(!first.is_finished());
        assert!(!second.is_finished());

        controller.release_shutdown().await;
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(controller.shutdowns().await, 1);
    }

    #[tokio::test]
    async fn opening_thread_preserves_existing_sender() {
        let thread = ThreadId::new();
        let (routes, mut first_rx) = test_routes(thread);
        let route = route_for_native(&routes, &format!("native-{thread}"))
            .unwrap()
            .unwrap();
        install_native_route(&routes, route).unwrap();

        let turn = TurnId::new();
        event_sender_for_thread(&routes, thread)
            .unwrap()
            .expect("sender exists")
            .send(AgentEvent::TurnStarted { thread, turn })
            .await
            .unwrap();
        assert!(matches!(
            first_rx.recv().await,
            Some(AgentEvent::TurnStarted { thread: got_thread, turn: got_turn })
                if got_thread == thread && got_turn == turn
        ));
    }

    #[tokio::test]
    async fn bounded_native_route_backpressures_without_overwriting_events() {
        let thread = ThreadId::new();
        let (routes, mut receiver) = test_routes(thread);
        let sender = event_sender_for_thread(&routes, thread).unwrap().unwrap();
        for sequence in 0..ROUTE_CAPACITY {
            sender
                .try_send(AgentEvent::Notice {
                    thread,
                    turn: None,
                    message: sequence.to_string(),
                })
                .unwrap();
        }
        assert!(matches!(
            sender.try_send(AgentEvent::Notice {
                thread,
                turn: None,
                message: "full".into(),
            }),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        for expected in 0..ROUTE_CAPACITY {
            let AgentEvent::Notice { message, .. } = receiver.recv().await.unwrap() else {
                panic!("expected notice");
            };
            assert_eq!(message, expected.to_string());
        }
    }

    #[tokio::test]
    async fn full_route_does_not_block_public_control_response_write() {
        let (harness, controller) = spawn_fake_harness();
        acknowledge_test_activations(&harness);
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &thread,
                UserInput::text("keep the native turn active"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut stream = claim_event_stream(&harness, &thread).await;
        let _ = stream
            .recv()
            .await
            .expect("thread-opened event should exist");

        let sender = event_sender_for_thread(&harness.routes, thread.thread)
            .unwrap()
            .unwrap();
        for sequence in 0..ROUTE_CAPACITY {
            sender
                .try_send(AgentEvent::Notice {
                    thread: thread.thread,
                    turn: None,
                    message: format!("filler-{sequence}"),
                })
                .unwrap();
        }
        controller
            .send_server_message(generic_user_input_request(
                "blocked-request",
                &thread.harness_thread_id,
                &native_turn,
            ))
            .await;

        controller
            .hang_method(codex_codes::protocol::methods::TURN_INTERRUPT)
            .await;
        let interrupt_harness = harness.clone();
        let interrupt_thread = thread.clone();
        let interrupt =
            tokio::spawn(async move { interrupt_harness.interrupt(&interrupt_thread).await });
        timeout(Duration::from_secs(1), async {
            while !controller
                .requests()
                .await
                .iter()
                .any(|request| request.method == codex_codes::protocol::methods::TURN_INTERRUPT)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("interrupt write must not wait for route capacity");
        assert!(matches!(
            sender.try_send(AgentEvent::Notice {
                thread: thread.thread,
                turn: None,
                message: "full-after-interrupt".into(),
            }),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        controller
            .hang_method(codex_codes::protocol::methods::THREAD_START)
            .await;
        let opening_harness = harness.clone();
        let blocked_normal_rpc =
            tokio::spawn(async move { opening_harness.open_thread(open_opts(None, None)).await });
        timeout(Duration::from_secs(1), async {
            while controller
                .requests()
                .await
                .iter()
                .filter(|request| request.method == codex_codes::protocol::methods::THREAD_START)
                .count()
                < 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("normal RPC should be in flight before the urgent response");

        let responding_harness = harness.clone();
        let response_task = tokio::spawn(async move {
            loop {
                match responding_harness
                    .respond_server_request(
                        ServerRequestId("blocked-request".into()),
                        ServerRequestResponse::result(json!({"answer": "accepted"})),
                    )
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(HarnessError::Protocol(message))
                        if message.contains("no pending server request") =>
                    {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => return Err(error),
                }
            }
        });

        timeout(Duration::from_secs(1), async {
            loop {
                if controller.responses().await.iter().any(|response| {
                    response.id == codex_codes::jsonrpc::RequestId::String("blocked-request".into())
                        && response.value == json!({"answer": "accepted"})
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("control write must not wait for route capacity");
        assert!(matches!(
            sender.try_send(AgentEvent::Notice {
                thread: thread.thread,
                turn: None,
                message: "still-full".into(),
            }),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        interrupt.abort();
        response_task.abort();
        blocked_normal_rpc.abort();
    }

    #[test]
    fn pending_compaction_marker_only_completes_without_turn_started() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let mut pending = HashMap::new();
        pending.insert(thread, PendingCompaction::new(Instant::now()));

        let elapsed_ms = observe_pending_compaction(
            &mut pending,
            thread,
            &context_compacted_event(thread, turn),
        );

        assert!(elapsed_ms.is_some());
        assert!(!pending.contains_key(&thread));
    }

    #[test]
    fn pending_compaction_marker_after_turn_started_waits_for_turn_completed() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let mut pending = HashMap::new();
        pending.insert(thread, PendingCompaction::new(Instant::now()));

        let started = AgentEvent::TurnStarted { thread, turn };
        assert!(observe_pending_compaction(&mut pending, thread, &started).is_none());
        assert!(pending.get(&thread).unwrap().saw_turn_started);

        let marker = observe_pending_compaction(
            &mut pending,
            thread,
            &context_compacted_event(thread, turn),
        );
        assert!(marker.is_none());
        assert!(pending.contains_key(&thread));

        let completed =
            observe_pending_compaction(&mut pending, thread, &completed_event(thread, turn));
        assert!(completed.is_some());
        assert!(!pending.contains_key(&thread));
    }

    #[tokio::test]
    async fn incomplete_stream_without_turn_emits_error_event() {
        let thread = ThreadId::new();
        let (routes, mut rx) = test_routes(thread);

        emit_incomplete_turn(&routes, thread, None, "stream ended").await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::Error {
                thread: got_thread,
                turn: None,
                error: HarnessError::Transport(message),
            } => {
                assert_eq!(got_thread, thread);
                assert_eq!(message, "stream ended");
            }
            other => panic!("expected error event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn incomplete_stream_with_turn_emits_failed_completion() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (routes, mut rx) = test_routes(thread);

        emit_incomplete_turn(&routes, thread, Some(turn), "stream failed").await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::TurnCompleted {
                thread: got_thread,
                turn: got_turn,
                status,
                ..
            } => {
                assert_eq!(got_thread, thread);
                assert_eq!(got_turn, turn);
                assert_eq!(status.kind, TurnStatusKind::Failed);
                assert_eq!(status.message.as_deref(), Some("stream failed"));
            }
            other => panic!("expected failed turn completion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fatal_error_with_turn_emits_failed_completion() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (routes, mut rx) = test_routes(thread);

        assert!(emit_fatal_turn_completion(&routes, thread, Some(turn), "quota exceeded").await);

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::TurnCompleted {
                thread: got_thread,
                turn: got_turn,
                status,
                ..
            } => {
                assert_eq!(got_thread, thread);
                assert_eq!(got_turn, turn);
                assert_eq!(status.kind, TurnStatusKind::Failed);
                assert_eq!(status.message.as_deref(), Some("quota exceeded"));
            }
            other => panic!("expected failed turn completion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fatal_error_without_turn_does_not_synthesize_completion() {
        let thread = ThreadId::new();
        let (routes, mut rx) = test_routes(thread);

        assert!(!emit_fatal_turn_completion(&routes, thread, None, "quota exceeded").await);

        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn mcp_server_status_maps_codex_metadata() {
        let mut tools = std::collections::BTreeMap::new();
        tools.insert(
            "jira_search".into(),
            codex_codes::Tool {
                _meta: None,
                annotations: None,
                description: Some("Search Jira".into()),
                icons: None,
                input_schema: serde_json::json!({"type": "object"}),
                name: "jira_search".into(),
                output_schema: Some(serde_json::json!({"type": "object"})),
                title: Some("Jira Search".into()),
            },
        );

        let mapped = map_mcp_server_status(codex_codes::McpServerStatus {
            auth_status: codex_codes::McpAuthStatus::NotLoggedIn,
            runtime_status: None,
            name: "cf-mcp".into(),
            plugin_id: None,
            resource_templates: vec![codex_codes::ResourceTemplate {
                annotations: None,
                description: Some("Issue by key".into()),
                mime_type: Some("application/json".into()),
                name: "jira issue".into(),
                title: Some("Jira Issue".into()),
                uri_template: "jira://issue/{key}".into(),
            }],
            resources: vec![codex_codes::Resource {
                _meta: None,
                annotations: None,
                description: Some("Project metadata".into()),
                icons: None,
                mime_type: Some("application/json".into()),
                name: "project".into(),
                size: Some(42),
                title: Some("Project".into()),
                uri: "gitlab://project/group/name".into(),
            }],
            server_info: Some(codex_codes::McpServerInfo {
                description: Some("Cloudflare tools".into()),
                icons: None,
                name: "cf-mcp".into(),
                title: Some("Cloudflare MCP".into()),
                version: "1.2.3".into(),
                website_url: Some("https://example.invalid".into()),
            }),
            tools,
        });

        assert_eq!(mapped.name, "cf-mcp");
        assert_eq!(mapped.auth_status, McpAuthStatus::NotLoggedIn);
        assert_eq!(mapped.server_info.unwrap().title.unwrap(), "Cloudflare MCP");
        assert_eq!(mapped.tools[0].name, "jira_search");
        assert_eq!(mapped.tools[0].description.as_deref(), Some("Search Jira"));
        assert_eq!(mapped.resources[0].uri, "gitlab://project/group/name");
        assert_eq!(
            mapped.resource_templates[0].uri_template,
            "jira://issue/{key}"
        );
    }

    #[test]
    fn maps_unknown_mcp_auth_status() {
        assert_eq!(
            map_mcp_auth_status(codex_codes::McpAuthStatus::Unknown),
            McpAuthStatus::Unknown
        );
    }

    /// Codex's app-server states its version exactly once over the protocol, in the initialize
    /// handshake's user agent, and that is the value it would send as `client_version` — both come
    /// from the same workspace version.
    #[test]
    fn codex_version_is_read_out_of_the_user_agent() {
        assert_eq!(
            codex_version_from_user_agent(
                "codex_cli_rs/0.58.0 (Linux 6.1.0; x86_64) codex_vscode/1.2.3"
            )
            .as_deref(),
            Some("0.58.0")
        );
        // A pre-release reduces to the whole version, because that is what Codex sends: its
        // `client_version_to_whole` documents `"1.2.3-alpha.4" -> "1.2.3"`, while the user agent
        // carries the full `CARGO_PKG_VERSION`. Forwarding the suffix would ask a question Codex
        // never asks.
        assert_eq!(
            codex_version_from_user_agent("codex_cli_rs/0.59.0-alpha.1 (Mac)").as_deref(),
            Some("0.59.0")
        );
        // Build metadata is a suffix on the whole version too.
        assert_eq!(
            codex_version_from_user_agent("codex_cli_rs/1.2.3+build.7 (Mac)").as_deref(),
            Some("1.2.3")
        );
        // An originator containing a slash: the version is the tail, not the head.
        assert_eq!(
            codex_version_from_user_agent("vendor/codex_cli_rs/1.0.0 (Linux)").as_deref(),
            Some("1.0.0")
        );

        // Anything that is not shaped like `{originator}/{version}` yields nothing, and discovery
        // then omits the parameter rather than forwarding a stray token.
        for unparseable in [
            "",
            "codex_cli_rs",
            "   ",
            "codex_cli_rs/ (Linux)",
            "codex_cli_rs/1.2 (Linux)",     // not three components
            "codex_cli_rs/1.2.3.4 (Linux)", // four
            "codex_cli_rs/1.2.x (Linux)",   // not numeric
        ] {
            assert_eq!(
                codex_version_from_user_agent(unparseable),
                None,
                "{unparseable:?} should not yield a version"
            );
        }
    }

    /// The drift warning fires only for a Codex that is strictly newer than the tested release, so
    /// an exact match and every older release stay quiet.
    #[test]
    fn codex_newer_than_the_tested_release_is_ordered_ahead_of_it() {
        assert!(version_is_newer("0.151.0", "0.150.1"));
        assert!(version_is_newer("0.150.2", "0.150.1"));
        assert!(version_is_newer("1.0.0", "0.150.1"));
        assert!(!version_is_newer("0.150.1", "0.150.1"));
        assert!(!version_is_newer("0.150.0", "0.150.1"));
        assert!(!version_is_newer("0.9.9", "0.150.1"));
        // Components are compared numerically, not lexically.
        assert!(version_is_newer("0.150.10", "0.150.9"));

        // A version that is not three numeric components never raises the alarm.
        assert!(!version_is_newer("0.150", "0.150.1"));
        assert!(!version_is_newer("0.150.1.2", "0.150.1"));
        assert!(!version_is_newer("0.150.x", "0.150.1"));
        assert!(!version_is_newer("", "0.150.1"));
    }

    /// The pin the warning compares against comes from the bindings, so it must stay readable as
    /// the same `MAJOR.MINOR.PATCH` shape Giskard parses out of the user agent.
    #[test]
    fn tested_codex_release_is_comparable_to_a_reported_version() {
        let tested = codex_codes::version::tested_cli_version();

        assert!(!version_is_newer(tested, tested), "{tested} vs itself");
        assert!(
            version_is_newer("999.0.0", tested),
            "{tested} should order below a clearly newer release"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn initialization_failure_kills_and_reaps_spawned_codex() {
        use std::os::unix::fs::PermissionsExt;

        let test_dir =
            std::env::temp_dir().join(format!("giskard-codex-init-failure-{}", ThreadId::new()));
        std::fs::create_dir_all(&test_dir).expect("create startup-failure test directory");
        let script = test_dir.join("fake-codex");
        let pid_file = test_dir.join("pid");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$GISKARD_TEST_PID_FILE\"\nprintf '%s\\n' 'not-json'\nexec sleep 60\n",
        )
        .expect("write fake Codex executable");
        let mut permissions = std::fs::metadata(&script)
            .expect("read fake Codex permissions")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script, permissions)
            .expect("make fake Codex executable runnable");

        let result = start_codex_client(
            codex_codes::AppServerBuilder::new()
                .command(&script)
                .env("GISKARD_TEST_PID_FILE", &pid_file),
        )
        .await;
        assert!(matches!(result, Err(HarnessError::Spawn(_))));
        let pid = std::fs::read_to_string(&pid_file)
            .expect("fake Codex should record its pid")
            .trim()
            .to_owned();
        let proc_entry = PathBuf::from(format!("/proc/{pid}"));
        timeout(Duration::from_secs(1), async {
            while proc_entry.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("last transport owner should kill and reap failed startup child");
        std::fs::remove_dir_all(test_dir).expect("remove startup-failure test directory");
    }

    #[test]
    fn initialize_params_enable_experimental_app_server_api() {
        let params = serde_json::to_value(build_initialize_params()).unwrap();

        assert_eq!(params["clientInfo"]["name"], "giskard");
        assert_eq!(params["capabilities"]["experimentalApi"], true);
        assert!(params["capabilities"].get("extensions").is_none());
    }

    #[test]
    fn relative_project_workspace_root_is_normalized() {
        let relative = PathBuf::from("relative/project");
        let expected = std::path::absolute(&relative).unwrap();

        assert_eq!(normalize_workspace_root(relative).unwrap(), expected);
    }

    #[tokio::test]
    async fn config_read_resolves_extra_workspace_write_roots_for_project_cwd() {
        let (mut transport, _frames, controller) = fake_codex();
        let workspace = PathBuf::from("/tmp/project");

        let roots = configured_workspace_write_roots(&mut transport, &workspace).await;

        assert_eq!(roots, vec![PathBuf::from("/home/test/.cache/sccache")]);
        let requests = controller.requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "config/read");
        assert_eq!(requests[0].params["cwd"], "/tmp/project");
        assert_eq!(requests[0].params["includeLayers"], false);
    }

    #[tokio::test]
    async fn config_read_failure_omits_configured_extra_workspace_roots() {
        let (mut transport, _frames, controller) = fake_codex();
        controller.fail_config_read("unsupported method").await;

        let roots =
            configured_workspace_write_roots(&mut transport, std::path::Path::new("/tmp/project"))
                .await;

        assert!(roots.is_empty());
    }

    #[test]
    fn auto_approve_turn_includes_thread_workspace_root_and_configured_roots() {
        let roots = [
            PathBuf::from("/home/test/.cache/cargo"),
            PathBuf::from("/home/test/.cache/sccache"),
        ];
        let mut overrides = turn_overrides(Mode::Build, None);
        overrides.permission_preset = PermissionPreset::AutoApprove;

        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("implement it"),
            &overrides,
            &roots,
        )
        .unwrap();

        assert_eq!(params["permissions"], ":workspace");
        assert_eq!(
            params["runtimeWorkspaceRoots"],
            json!([
                "/home/test/.cache/cargo",
                "/home/test/.cache/sccache",
                "/tmp/test-workspace"
            ])
        );

        overrides.permission_preset = PermissionPreset::AskFirst;
        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("inspect it"),
            &overrides,
            &roots,
        )
        .unwrap();
        assert!(params.get("runtimeWorkspaceRoots").is_none());

        overrides.permission_preset = PermissionPreset::FullAccess;
        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("run it"),
            &overrides,
            &roots,
        )
        .unwrap();
        assert!(params.get("runtimeWorkspaceRoots").is_none());

        overrides.permission_preset = PermissionPreset::AutoApprove;
        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("continue"),
            &overrides,
            &roots,
        )
        .unwrap();
        assert_eq!(
            params["runtimeWorkspaceRoots"],
            json!([
                "/home/test/.cache/cargo",
                "/home/test/.cache/sccache",
                "/tmp/test-workspace"
            ])
        );
    }

    #[test]
    fn auto_approve_turn_includes_thread_workspace_root_without_configured_roots() {
        let mut overrides = turn_overrides(Mode::Build, None);
        overrides.permission_preset = PermissionPreset::AutoApprove;

        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("implement it"),
            &overrides,
            &[],
        )
        .unwrap();

        assert_eq!(params["permissions"], ":workspace");
        assert_eq!(
            params["runtimeWorkspaceRoots"],
            json!(["/tmp/test-workspace"])
        );
    }

    #[test]
    fn plan_turn_start_params_include_plan_collaboration_mode() {
        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("make a plan"),
            &turn_overrides(Mode::Plan, Some(Effort::new("medium"))),
            &[],
        )
        .unwrap();

        assert_eq!(params["threadId"], "native-thread");
        assert!(params.get("sandboxPolicy").is_none());
        assert_eq!(params["approvalPolicy"], "on-request");
        assert_eq!(params["permissions"], ":read-only");
        assert_eq!(params["model"], "gpt-5.5");
        assert_eq!(params["effort"], "medium");
        assert_eq!(params["collaborationMode"]["mode"], "plan");
        assert_eq!(params["collaborationMode"]["settings"]["model"], "gpt-5.5");
        assert_eq!(
            params["collaborationMode"]["settings"]["reasoning_effort"],
            "medium"
        );
        assert!(params["collaborationMode"]["settings"]["developer_instructions"].is_null());
    }

    #[test]
    fn build_turn_start_params_reset_collaboration_mode_to_default() {
        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("implement it"),
            &turn_overrides(Mode::Build, None),
            &[],
        )
        .unwrap();

        assert_eq!(params["collaborationMode"]["mode"], "default");
        assert!(params.get("sandboxPolicy").is_none());
        assert_eq!(params["approvalPolicy"], "on-request");
        assert_eq!(params["permissions"], ":read-only");
        assert_eq!(params["collaborationMode"]["settings"]["model"], "gpt-5.5");
        assert!(params.get("effort").is_none());
        assert!(params["collaborationMode"]["settings"]["reasoning_effort"].is_null());
    }

    #[test]
    fn full_access_turn_start_params_disable_codex_approval_and_sandbox() {
        let mut overrides = turn_overrides(Mode::Build, None);
        overrides.permission_preset = PermissionPreset::FullAccess;

        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("implement it"),
            &overrides,
            &[],
        )
        .unwrap();

        assert!(params.get("sandboxPolicy").is_none());
        assert_eq!(params["approvalPolicy"], "never");
        assert_eq!(params["permissions"], ":danger-full-access");
    }
}
