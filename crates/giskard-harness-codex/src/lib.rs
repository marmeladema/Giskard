//! Codex CLI harness adapter (spec §4.6).
//!
//! Wraps `codex-codes::AsyncClient` and implements the `AgentHarness` trait.
//! All Codex-specific types are confined to this crate and mapped to
//! `giskard-core` types at the boundary.
//!
//! See the crate README for Codex-native identifier scopes, item and process
//! lifecycles, background-command ownership, and termination routing.

mod log_fields;
mod mapping;

use crate::log_fields::display_opt;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::RngCore;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
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
    AgentEventStream, AgentHarness, HarnessBootstrap, HarnessCapabilities, HarnessNotice,
    HarnessProvider, OpenThreadOptions, ProviderAuth, ProviderAuthCommand, ThreadHandle,
    ThreadUpdate,
};

use mapping::CodexMapper;

const BROADCAST_CAPACITY: usize = 256;
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
    resume_replay_model: Option<ModelRef>,
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
    shutdown: watch::Receiver<bool>,
}

type SenderMap = Arc<StdMutex<HashMap<ThreadId, broadcast::Sender<AgentEvent>>>>;

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
    active: Option<WorkerQueueToken>,
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
                active: None,
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
        state.active = Some(token);
    }

    fn mark_finished(&self, token: WorkerQueueToken) {
        let mut state = self.lock_state();
        if state.active.is_some_and(|active| active.id == token.id) {
            state.active = None;
        }
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
            active: state.active.map(|token| snapshot_queue_token(token, now)),
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

    async fn next_message(
        &mut self,
    ) -> Result<Option<codex_codes::ServerMessage>, CodexStreamError>;

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

    async fn shutdown_transport(self) -> Result<(), HarnessError>
    where
        Self: Sized;
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

fn classify_codex_stream_error(error: codex_codes::Error) -> CodexStreamError {
    match error {
        codex_codes::Error::Deserialization(parse_error)
            if parse_error.method.is_none()
                && parse_error.raw_json.is_none()
                && !parse_error.raw_line.trim_start().starts_with('{') =>
        {
            CodexStreamError::NonJsonStdout {
                raw_preview: bounded_utf8_preview(
                    &parse_error.raw_line,
                    NON_JSON_STDOUT_PREVIEW_BYTES,
                ),
                raw_bytes: parse_error.raw_line.len(),
                parse_error: parse_error.error_message,
            }
        }
        codex_codes::Error::Deserialization(parse_error) => {
            let raw_preview =
                bounded_utf8_preview(&parse_error.raw_line, NON_JSON_STDOUT_PREVIEW_BYTES);
            let method = parse_error.method.as_deref().unwrap_or("unknown");
            CodexStreamError::Fatal(HarnessError::Transport(format!(
                "Codex JSON-RPC deserialization error for method {method}: {} \
                 (raw_bytes: {}, raw_preview: {raw_preview:?})",
                parse_error.error_message,
                parse_error.raw_line.len(),
            )))
        }
        error => CodexStreamError::Fatal(HarnessError::Transport(error.to_string())),
    }
}

#[async_trait]
impl CodexTransport for codex_codes::AsyncClient {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError> {
        self.request(method, &params)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }

    async fn next_message(
        &mut self,
    ) -> Result<Option<codex_codes::ServerMessage>, CodexStreamError> {
        self.next_message()
            .await
            .map_err(classify_codex_stream_error)
    }

    async fn respond_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError> {
        self.respond(id, &value)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }

    async fn respond_error_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        self.respond_error(id, code, message)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }

    async fn shutdown_transport(self) -> Result<(), HarnessError> {
        self.shutdown()
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
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
    senders: SenderMap,
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
        let (mut client, client_version) =
            start_codex_client(codex_codes::AppServerBuilder::new()).await?;
        let writable_roots = configured_workspace_write_roots(&mut client, &workspace_root).await;
        Self::spawn_harness(
            client,
            workspace_root,
            writable_roots,
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
        let (mut client, client_version) = start_codex_client(builder).await?;
        let writable_roots = configured_workspace_write_roots(&mut client, &workspace_root).await;
        Self::spawn_harness(
            client,
            workspace_root,
            writable_roots,
            client_version,
            HarnessBootstrap::default(),
        )
    }

    fn spawn_harness<C>(
        client: C,
        workspace_root: PathBuf,
        writable_roots: Vec<PathBuf>,
        client_version: Option<String>,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<Self>, HarnessError>
    where
        C: CodexTransport + 'static,
    {
        let mut mapper = CodexMapper::new(workspace_root.clone());
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(64);
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        for binding in bootstrap.known_threads {
            mapper.claim_thread(binding.harness_thread_id, binding.thread_id)?;
            let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
            ensure_thread_sender(&senders, binding.thread_id, sender);
        }
        let worker_queue = Arc::new(WorkerQueueWatchdog::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (worker_done_tx, worker_done) = watch::channel(false);

        let harness = Arc::new(Self {
            workspace_root: workspace_root.clone(),
            client_version,
            cmd_tx,
            control_tx,
            senders: senders.clone(),
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
                WorkerReceivers {
                    commands: cmd_rx,
                    controls: control_rx,
                    shutdown: shutdown_rx,
                },
                senders,
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
        self.control_tx
            .send(QueuedControlCommand { token, command })
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
) -> Result<(codex_codes::AsyncClient, Option<String>), HarnessError> {
    let mut client = codex_codes::AsyncClient::spawn(builder)
        .await
        .map_err(|e| HarnessError::Spawn(e.to_string()))?;
    let response = client
        .initialize(&build_initialize_params())
        .await
        .map_err(|e| HarnessError::Spawn(e.to_string()))?;
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
    Ok((client, version))
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

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Some(sender) = sender_for_thread(&self.senders, thread.thread) {
            return AgentEventStream::new(sender.subscribe());
        }
        let (_, rx) = broadcast::channel(1);
        AgentEventStream::new(rx)
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

async fn background_task<C>(
    mut client: C,
    receivers: WorkerReceivers,
    senders: SenderMap,
    worker_queue: Arc<WorkerQueueWatchdog>,
    workspace_root: PathBuf,
    writable_roots: Vec<PathBuf>,
    mut mapper: CodexMapper,
) where
    C: CodexTransport,
{
    let WorkerReceivers {
        mut commands,
        mut controls,
        mut shutdown,
    } = receivers;
    let mut pending_compactions: HashMap<ThreadId, PendingCompaction> = HashMap::new();
    let mut pending_context_restores: HashMap<String, PendingContextRestore> = HashMap::new();
    let mut active_turns: ActiveTurns = HashMap::new();
    let mut first_event_warn_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            biased;
            _ = wait_for_shutdown_request(&mut shutdown) => {
                cleanup_all_active_turn_uploads(&mut client, &mut active_turns).await;
                shutdown_codex_transport(client, &workspace_root).await;
                break;
            }
            msg = client.next_message(), if should_poll_codex_messages(&mapper, &active_turns, &pending_compactions) || !pending_context_restores.is_empty() => {
                match msg {
                    Ok(Some(msg)) => {
                        observe_pending_context_restore(&mut pending_context_restores, &msg);
                        match handle_background_server_message(
                                &mut client,
                                &mut mapper,
                                &senders,
                                &mut pending_compactions,
                                &mut active_turns,
                                msg,
                            )
                            .await
                        {
                            StreamOutcome::TurnEnded => {}
                            StreamOutcome::CompactionCompleted { thread, elapsed_ms } => {
                                info!(
                                    %thread,
                                    elapsed_ms,
                                    pending_compactions = pending_compactions.len(),
                                    "Codex context compaction completion observed"
                                );
                            }
                            StreamOutcome::Shutdown => {
                                cleanup_all_active_turn_uploads(&mut client, &mut active_turns).await;
                                shutdown_codex_transport(client, &workspace_root).await;
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        cleanup_all_active_turn_uploads(&mut client, &mut active_turns).await;
                        emit_incomplete_active_turns(
                            &senders,
                            &mut mapper,
                            &mut active_turns,
                            "Codex stream ended before turn completion",
                        )
                        .await;
                        if !pending_compactions.is_empty() {
                            warn!(
                                action = "read_codex_stream",
                                workspace_root = %workspace_root.display(),
                                pending_compactions = pending_compactions.len(),
                                pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                                "Codex message stream ended with pending context compactions"
                            );
                        }
                        break;
                    }
                    Err(CodexStreamError::NonJsonStdout {
                        parse_error,
                        raw_preview,
                        raw_bytes,
                    }) => {
                        warn!(
                            active_turns = active_turns.len(),
                            pending_compactions = pending_compactions.len(),
                            pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                            workspace_root = %workspace_root.display(),
                            error = %parse_error,
                            raw_bytes,
                            raw_preview = ?raw_preview,
                            "Ignoring non-JSON line from Codex app-server stdout"
                        );
                    }
                    Err(CodexStreamError::Fatal(e)) => {
                        let message = e.to_string();
                        if active_turns.is_empty() {
                            warn!(
                                action = "read_codex_stream",
                                error = %message,
                                pending_compactions = pending_compactions.len(),
                                pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                                workspace_root = %workspace_root.display(),
                                "Codex idle stream failed while background work was running"
                            );
                        } else {
                            warn!(
                                action = "read_codex_stream",
                                error = %message,
                                active_turns = active_turns.len(),
                                active_turn_states = ?active_turn_states(&active_turns),
                                pending_compactions = pending_compactions.len(),
                                pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                                workspace_root = %workspace_root.display(),
                                "Codex stream failed before all active turns completed"
                            );
                            cleanup_all_active_turn_uploads(&mut client, &mut active_turns).await;
                            emit_incomplete_active_turns(
                                &senders,
                                &mut mapper,
                                &mut active_turns,
                                format!("Codex stream failed before turn completion: {message}"),
                            )
                            .await;
                        }
                        break;
                    }
                }
            }
            queued = commands.recv() => {
                let queued = match queued {
                    Some(queued) => queued,
                    None => {
                        cleanup_all_active_turn_uploads(&mut client, &mut active_turns).await;
                        break;
                    }
                };
                worker_queue.mark_started(queued.token);

                match queued.command {
                    HarnessCommand::OpenThread { opts, response } => {
                        let result =
                            handle_open_thread(&mut client, &mut mapper, &opts, &senders).await;
                        match result {
                            Ok(outcome) => {
                                let handle = outcome.handle;
                                if let Some(model) = outcome.resume_replay_model {
                                    let replaced = pending_context_restores.insert(handle.harness_thread_id.clone(), PendingContextRestore {
                                        thread: handle.thread,
                                        model,
                                        sink: opts.updates.clone(),
                                    });
                                    if let Some(replaced) = replaced {
                                        warn!(thread_id = %handle.thread,
                                            replaced_thread_id = %replaced.thread,
                                            harness_thread_id = %handle.harness_thread_id,
                                            "replaced an overlapping pending context restore");
                                    }
                                }
                                let _ = response.send(Ok(handle));
                            }
                            Err(error) => { let _ = response.send(Err(error)); }
                        }
                    }
                    HarnessCommand::StartTurn {
                        thread,
                        input,
                        overrides,
                        response,
                    } => {
                        match handle_start_turn(
                            &mut client,
                            &mut mapper,
                            &thread,
                            &input,
                            &overrides,
                            &writable_roots,
                        )
                        .await
                        {
                            Ok(started) => {
                                let _ = response.send(Ok(started.turn));
                                active_turns.insert(
                                    thread.thread,
                                    ActiveTurn::new(*thread, started.turn)
                                        .with_upload_dir(started.upload_dir),
                                );
                            }
                            Err(error) => {
                                let _ = response.send(Err(error));
                            }
                        }
                    }
                }
                worker_queue.mark_finished(queued.token);
            }
            queued = controls.recv() => {
                let Some(queued) = queued else {
                    cleanup_all_active_turn_uploads(&mut client, &mut active_turns).await;
                    break;
                };
                worker_queue.mark_started(queued.token);
                let token = queued.token;
                let outcome =
                    handle_control_command(
                        &mut client,
                        &mut mapper,
                        &senders,
                        &mut pending_compactions,
                        &mut pending_context_restores,
                        &active_turns,
                        Some(queued.command),
                    )
                    .await;
                worker_queue.mark_finished(token);
                if matches!(outcome, StreamOutcome::Shutdown) {
                    cleanup_all_active_turn_uploads(&mut client, &mut active_turns).await;
                    shutdown_codex_transport(client, &workspace_root).await;
                    break;
                }
            }
            _ = first_event_warn_tick.tick(), if !active_turns.is_empty() => {
                warn_slow_first_events(&mut active_turns);
            }
        }
    }
    worker_queue.close();
}

async fn wait_for_shutdown_request(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow_and_update() {
        return;
    }
    // A closed sender also means the harness owner disappeared, so the worker should tear down.
    let _ = shutdown.changed().await;
}

async fn shutdown_codex_transport<C>(client: C, workspace_root: &std::path::Path)
where
    C: CodexTransport,
{
    let started = Instant::now();
    match tokio::time::timeout(CODEX_SHUTDOWN_TIMEOUT, client.shutdown_transport()).await {
        Ok(Ok(())) => {
            info!(
                action = "shutdown_codex_transport",
                workspace_root = %workspace_root.display(),
                elapsed_ms = started.elapsed().as_millis(),
                "Codex transport shutdown completed"
            );
        }
        Ok(Err(error)) => {
            warn!(
                action = "shutdown_codex_transport",
                workspace_root = %workspace_root.display(),
                error = %error,
                elapsed_ms = started.elapsed().as_millis(),
                "Codex transport shutdown failed"
            );
        }
        Err(_) => {
            warn!(
                action = "shutdown_codex_transport",
                workspace_root = %workspace_root.display(),
                elapsed_ms = started.elapsed().as_millis(),
                timeout_ms = CODEX_SHUTDOWN_TIMEOUT.as_millis(),
                "Codex transport shutdown timed out; dropping transport"
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
    upload_dir: Option<PathBuf>,
}

fn should_poll_codex_messages(
    mapper: &CodexMapper,
    active_turns: &ActiveTurns,
    pending_compactions: &HashMap<ThreadId, PendingCompaction>,
) -> bool {
    !active_turns.is_empty()
        || mapper.has_active_turns()
        || mapper.has_running_commands()
        || !pending_compactions.is_empty()
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

async fn handle_background_server_message(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    active_turns: &mut ActiveTurns,
    msg: codex_codes::ServerMessage,
) -> StreamOutcome {
    let fallback_thread = fallback_thread(mapper, active_turns);
    match msg {
        codex_codes::ServerMessage::Notification(notif) => {
            if let Some(event) = mapper.map_notification(&notif, fallback_thread) {
                let thread = event_thread(&event);
                if let Some(active) = active_turns.get_mut(&thread) {
                    active.mark_server_message();
                    if let AgentEvent::TurnStarted { turn, .. } = &event
                        && *turn == active.acknowledged_turn
                    {
                        active.active_turn = Some(*turn);
                    }
                }
                let completed_compaction =
                    observe_pending_compaction(pending_compactions, thread, &event);
                let completed_active_turn =
                    completed_current_active_turn(active_turns, &event).map(|(_, turn)| turn);
                if active_turns.contains_key(&thread)
                    && matches!(&event, AgentEvent::TurnCompleted { .. })
                    && completed_active_turn.is_none()
                {
                    debug!(
                        %thread,
                        acknowledged_turn = display_opt(active_turns.get(&thread).map(|active| active.acknowledged_turn)),
                        event_turn = display_opt(agent_event_turn(&event)),
                        "ignoring Codex turn completion for a non-current turn"
                    );
                }
                let fatal_completion = active_turns.get(&thread).and_then(|active| {
                    active
                        .event_is_current_turn(&event)
                        .then(|| {
                            mapping::fatal_turn_error(&notif)
                                .map(|message| (active.active_turn, message))
                        })
                        .flatten()
                });
                let _ = broadcast_event(senders, thread, || event).await;
                if let Some(turn) = completed_active_turn {
                    cleanup_active_turn_upload(client, active_turns, thread).await;
                    active_turns.remove(&thread);
                    mapper.clear_active_turn(thread);
                    debug!(
                        %thread,
                        %turn,
                        remaining_active_turns = active_turns.len(),
                        "Codex turn completion observed"
                    );
                } else if let Some((turn, message)) = fatal_completion
                    && emit_fatal_turn_completion(senders, thread, turn, message).await
                {
                    cleanup_active_turn_upload(client, active_turns, thread).await;
                    active_turns.remove(&thread);
                    mapper.clear_active_turn(thread);
                }
                if let Some(elapsed_ms) = completed_compaction {
                    return StreamOutcome::CompactionCompleted { thread, elapsed_ms };
                }
            } else if let Some(message) = mapping::fatal_turn_error(&notif) {
                let (harness_thread_id, native_turn_id) = match &notif {
                    codex_codes::messages::Notification::Error(error) => {
                        (Some(error.thread_id.as_str()), Some(error.turn_id.as_str()))
                    }
                    _ => (None, notif.turn_id()),
                };
                warn!(
                    action = "map_fatal_notification",
                    method = notif.method(),
                    harness_thread_id,
                    native_turn_id,
                    fallback_thread = %fallback_thread,
                    error = %message,
                    "dropping fatal Codex error notification that could not be mapped to a known thread"
                );
            }
            StreamOutcome::TurnEnded
        }
        codex_codes::ServerMessage::Request { id, request } => {
            let Some(event) = mapper.map_server_request(&id, &request, fallback_thread) else {
                respond_unroutable_server_request(client, &id, &request).await;
                return StreamOutcome::TurnEnded;
            };
            let thread = event_thread(&event);
            if let Some(active) = active_turns.get_mut(&thread) {
                active.mark_server_message();
            }
            let _ = broadcast_event(senders, thread, || event).await;
            StreamOutcome::TurnEnded
        }
    }
}

async fn handle_control_command(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    pending_context_restores: &mut HashMap<String, PendingContextRestore>,
    active_turns: &ActiveTurns,
    control: Option<ControlCommand>,
) -> StreamOutcome {
    match control {
        Some(ControlCommand::ClaimNativeThread {
            thread,
            harness_thread_id,
            workspace_root,
            response,
        }) => {
            let result = mapper
                .claim_thread(harness_thread_id.clone(), thread)
                .map(|_| {
                    let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
                    ensure_thread_sender(senders, thread, sender);
                    // A claim answers with the identity facts this harness lifetime already
                    // attested through its own events. It must not resume the thread to learn
                    // more: the native model stays unreported until an event names it.
                    let parent_harness_thread_id = mapper.native_parent(&harness_thread_id);
                    ThreadHandle {
                        parent_harness_thread_id,
                        ..ThreadHandle::opened(thread, harness_thread_id, workspace_root)
                    }
                });
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::RespondApproval {
            id,
            decision,
            response,
        }) => {
            let result = handle_respond_approval(client, mapper, &id, &decision).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::RespondServerRequest {
            id,
            response_payload,
            response,
        }) => {
            let result =
                handle_respond_server_request(client, mapper, senders, &id, response_payload).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::Interrupt { thread, response }) => {
            let native_turn_id = mapper
                .active_native_turn_for_thread(thread.thread)
                .map(str::to_owned);
            let result = timeout_codex_control(
                "interrupt",
                Some(&thread),
                None,
                native_turn_id.as_deref(),
                handle_interrupt(client, mapper, &thread),
            )
            .await;
            if result.is_ok() {
                reject_pending_requests_for_interrupted_thread(
                    client,
                    mapper,
                    senders,
                    thread.thread,
                )
                .await;
            }
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::TerminateCommand {
            thread,
            process_id,
            response,
        }) => {
            let native_turn_id = mapper
                .native_turn_for_process(thread.thread, &process_id)
                .or_else(|| mapper.active_native_turn_for_thread(thread.thread))
                .map(str::to_owned);
            let result = timeout_codex_control(
                "terminate_command",
                Some(&thread),
                Some(&process_id),
                native_turn_id.as_deref(),
                handle_terminate_command(client, &thread, &process_id),
            )
            .await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::CompactThread { thread, response }) => {
            if active_turns.contains_key(&thread.thread) {
                let _ = response.send(Err(HarnessError::Unsupported(
                    "context compaction is not available during an active turn".into(),
                )));
                return StreamOutcome::TurnEnded;
            }
            let started = Instant::now();
            info!(
                thread = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                pending_compactions = pending_compactions.len(),
                "requesting Codex context compaction"
            );
            let result = handle_compact_thread(client, &thread).await;
            match &result {
                Ok(()) => {
                    pending_compactions.insert(thread.thread, PendingCompaction::new(started));
                    info!(
                        thread = %thread.thread,
                        harness_thread_id = %thread.harness_thread_id,
                        ack_elapsed_ms = started.elapsed().as_millis(),
                        pending_compactions = pending_compactions.len(),
                        "Codex accepted context compaction request"
                    );
                }
                Err(error) => {
                    warn!(
                        action = "compact_thread",
                        thread_id = %thread.thread,
                        harness_thread_id = %thread.harness_thread_id,
                        error = %error,
                        elapsed_ms = started.elapsed().as_millis(),
                        "Codex context compaction request failed"
                    );
                }
            }
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::SetThreadName {
            thread,
            name,
            response,
        }) => {
            let result = handle_set_thread_name(client, &thread, &name).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::SetThreadArchived {
            thread,
            archived,
            response,
        }) => {
            let result = if active_turns.contains_key(&thread.thread) {
                Err(HarnessError::Unsupported(
                    "thread archiving is not available during an active turn".into(),
                ))
            } else {
                handle_set_thread_archived(client, &thread, archived).await
            };
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::DeleteThread { thread, response }) => {
            let result = if active_turns.contains_key(&thread.thread) {
                Err(HarnessError::Unsupported(
                    "thread deletion is not available during an active turn".into(),
                ))
            } else {
                handle_delete_thread(client, &thread).await
            };
            if result.is_ok() {
                lock_senders(senders).remove(&thread.thread);
                pending_context_restores.remove(&thread.harness_thread_id);
            }
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::ListMcpServers { response }) => {
            let result = timeout_codex_control(
                "list_mcp_servers",
                None,
                None,
                None,
                handle_list_mcp_servers(client),
            )
            .await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::ReloadMcpServers { response }) => {
            let result = timeout_codex_control(
                "reload_mcp_servers",
                None,
                None,
                None,
                handle_reload_mcp_servers(client),
            )
            .await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::StartMcpOauthLogin { name, response }) => {
            let result = timeout_codex_control(
                "start_mcp_oauth_login",
                None,
                Some(&name),
                None,
                handle_start_mcp_oauth_login(client, &name),
            )
            .await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::ListProviders { cwd, response }) => {
            let result = timeout_codex_control(
                "list_providers",
                None,
                None,
                None,
                handle_list_providers(client, cwd),
            )
            .await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::ListModels { cwd, response }) => {
            let result = timeout_codex_control(
                "list_models",
                None,
                None,
                None,
                handle_list_models(client, cwd),
            )
            .await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        None => StreamOutcome::Shutdown,
    }
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
    Shutdown,
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
    mapper: &mut CodexMapper,
    opts: &OpenThreadOptions,
    senders: &SenderMap,
) -> Result<OpenThreadOutcome, HarnessError> {
    let cwd = opts.workspace_root.to_string_lossy().to_string();
    // An explicit id wins — the caller knows this thread's durable identity. Otherwise, if the
    // native thread being resumed is already bound, reuse that binding rather than inventing an
    // id: a caller passing `None` is saying it has no opinion, not that this is a new thread, and
    // minting here would give one thread two identities for everything downstream to reconcile.
    let thread_id = opts
        .thread
        .or_else(|| {
            opts.resume
                .as_deref()
                .and_then(|native| mapper.thread_for_native(native))
        })
        .unwrap_or_default();

    // Track whether resume-by-id failed and we fell back to a fresh native thread (C5), so we can
    // warn the caller that agent context was lost while keeping the Giskard-side history.
    let mut resume_warning = None;

    let opened = if let Some(ref resume_id) = opts.resume {
        let context = CodexOperationContext::for_project("thread_resume", opts.project)
            .with_thread_id(thread_id)
            .with_harness_thread_id(resume_id);
        match resume_thread(
            client,
            context,
            resume_id,
            &cwd,
            opts.initial_model.as_ref(),
        )
        .await
        {
            Ok(opened) => opened,
            // Recovery needs a model to start on, and importing a thread by native id supplies
            // none — the model was the resumed thread's to report. Nothing sensible to start.
            Err(error) if opts.initial_model.is_none() => return Err(error),
            Err(e) => {
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
                start_thread(client, context, &cwd, &fresh_model(opts)?).await?
            }
        }
    } else {
        let context = CodexOperationContext::for_project("thread_start", opts.project)
            .with_thread_id(thread_id);
        start_thread(client, context, &cwd, &fresh_model(opts)?).await?
    };

    // B4: bind the (possibly re-established) native id to the durable ThreadId.
    mapper.claim_thread(opened.harness_thread_id.clone(), thread_id)?;

    let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
    ensure_thread_sender(senders, thread_id, tx);

    let _ = broadcast_event(senders, thread_id, || AgentEvent::ThreadOpened {
        thread: thread_id,
        harness_thread_id: opened.harness_thread_id.clone(),
    })
    .await;

    if let Some(warning) = &resume_warning {
        let message = warning.message.clone();
        let _ = broadcast_event(senders, thread_id, || AgentEvent::Error {
            thread: thread_id,
            turn: None,
            error: HarnessError::Transport(message),
        })
        .await;
    }

    let resume_replay_model = (opts.resume.is_some() && resume_warning.is_none())
        .then(|| opened.model.clone())
        .flatten();
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
        resume_replay_model,
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
    if model.is_empty() || model_provider.is_empty() {
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
        return None;
    }
    Some(giskard_core::model::ModelRef {
        provider: model_provider.to_string(),
        model: model.to_string(),
        reasoning_effort: reported_effort
            .or_else(|| requested.and_then(|r| r.reasoning_effort.clone())),
    })
}

async fn resume_thread(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    resume_id: &str,
    cwd: &str,
    model: Option<&giskard_core::model::ModelRef>,
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
    let resp: codex_codes::ThreadResumeResponse = codex_request(
        client,
        context,
        codex_codes::protocol::methods::THREAD_RESUME,
        &params,
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

async fn start_thread(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    cwd: &str,
    initial_model: &giskard_core::model::ModelRef,
) -> Result<OpenedNativeThread, HarnessError> {
    let params = codex_codes::ThreadStartParams {
        cwd: Some(cwd.to_owned()),
        model: Some(initial_model.model.clone()),
        model_provider: Some(initial_model.provider.clone()),
        ..Default::default()
    };
    let resp: codex_codes::ThreadStartResponse = codex_request(
        client,
        context,
        codex_codes::protocol::methods::THREAD_START,
        &params,
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

async fn broadcast_event<F: FnOnce() -> AgentEvent>(senders: &SenderMap, thread: ThreadId, f: F) {
    let sender = sender_for_thread(senders, thread);
    if let Some(sender) = sender {
        let _ = sender.send(f());
    }
}

fn lock_senders(
    senders: &SenderMap,
) -> StdMutexGuard<'_, HashMap<ThreadId, broadcast::Sender<AgentEvent>>> {
    match senders.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("Codex sender map lock was poisoned; recovering sender state");
            poisoned.into_inner()
        }
    }
}

fn sender_for_thread(
    senders: &SenderMap,
    thread: ThreadId,
) -> Option<broadcast::Sender<AgentEvent>> {
    lock_senders(senders).get(&thread).cloned()
}

fn ensure_thread_sender(
    senders: &SenderMap,
    thread: ThreadId,
    sender: broadcast::Sender<AgentEvent>,
) {
    lock_senders(senders).entry(thread).or_insert(sender);
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
            let string_at = |key: &str| {
                params
                    .as_ref()
                    .and_then(|value| value.get(key))
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
            };
            (string_at("threadId"), string_at("turnId"))
        }
        ServerRequest::McpServerElicitationRequest(_)
        | ServerRequest::ChatgptAuthTokensRefresh(_)
        | ServerRequest::AttestationGenerate(_) => (None, None),
    }
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

#[cfg(test)]
fn event_belongs_to_stream(stream_thread: ThreadId, event: &AgentEvent) -> bool {
    event_thread(event) == stream_thread
}

#[cfg(test)]
fn event_belongs_to_current_turn(
    stream_thread: ThreadId,
    current_turn: TurnId,
    event: &AgentEvent,
) -> bool {
    event_belongs_to_stream(stream_thread, event)
        && agent_event_turn(event).is_none_or(|turn| turn == current_turn)
}

#[cfg(test)]
fn event_completes_stream(
    stream_thread: ThreadId,
    current_turn: TurnId,
    event: &AgentEvent,
) -> bool {
    event_belongs_to_stream(stream_thread, event)
        && matches!(event, AgentEvent::TurnCompleted { turn, .. } if *turn == current_turn)
}

async fn emit_incomplete_turn(
    senders: &SenderMap,
    thread: ThreadId,
    turn: Option<TurnId>,
    message: impl Into<String>,
) {
    let message = message.into();
    if let Some(turn) = turn {
        let _ = broadcast_event(senders, thread, || AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Failed,
                message: Some(message),
            },
        })
        .await;
    } else {
        let _ = broadcast_event(senders, thread, || AgentEvent::Error {
            thread,
            turn: None,
            error: HarnessError::Transport(message),
        })
        .await;
    }
}

async fn emit_incomplete_active_turns(
    senders: &SenderMap,
    mapper: &mut CodexMapper,
    active_turns: &mut ActiveTurns,
    message: impl Into<String>,
) {
    let message = message.into();
    let turns: Vec<(ThreadId, Option<TurnId>)> = active_turns
        .iter()
        .map(|(thread, active)| (*thread, active.active_turn))
        .collect();
    for (thread, turn) in turns {
        emit_incomplete_turn(senders, thread, turn, message.clone()).await;
        mapper.clear_active_turn(thread);
    }
    active_turns.clear();
}

async fn emit_fatal_turn_completion(
    senders: &SenderMap,
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
    let _ = broadcast_event(senders, thread, || AgentEvent::TurnCompleted {
        thread,
        turn,
        usage: TokenUsage::default(),
        status: TurnStatus {
            kind: TurnStatusKind::Failed,
            message: Some(message),
        },
    })
    .await;
    true
}

async fn handle_respond_approval(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    id: &ApprovalId,
    decision: &ApprovalDecision,
) -> Result<(), HarnessError> {
    match mapper
        .map_approval_response(id, decision)
        .map_err(HarnessError::Protocol)?
    {
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
    }
}

async fn handle_respond_server_request(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    id: &ServerRequestId,
    response: ServerRequestResponse,
) -> Result<(), HarnessError> {
    let pending = mapper
        .pending_server_request(id)
        .map_err(HarnessError::Protocol)?;
    let request_id = pending.request_id.clone();
    let context = CodexOperationContext::new("respond_server_request")
        .with_thread_id(pending.thread)
        .with_request_id(&request_id);
    match response {
        ServerRequestResponse::Result { value } => {
            codex_respond_json(client, context, request_id.clone(), value).await?
        }
        ServerRequestResponse::Error { code, message } => {
            codex_respond_error_json(client, context, request_id.clone(), code, &message).await?
        }
    }
    mapper.resolve_server_request(id);
    let thread = pending.thread;
    let turn = pending.turn;
    let request_id = id.clone();
    let _ = broadcast_event(senders, thread, || AgentEvent::ServerRequestResolved {
        thread,
        turn,
        request_id,
    })
    .await;
    Ok(())
}

async fn reject_pending_requests_for_interrupted_thread(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    thread: ThreadId,
) {
    let approval_ids = mapper.pending_approval_ids_for_thread(thread);
    let server_request_ids = mapper.pending_server_request_ids_for_thread(thread);

    if approval_ids.is_empty() && server_request_ids.is_empty() {
        debug!(
            %thread,
            "interrupt accepted with no pending Codex approval/server request to reject"
        );
        return;
    }

    for approval_id in approval_ids {
        if let Err(error) =
            handle_respond_approval(client, mapper, &approval_id, &ApprovalDecision::Cancel).await
        {
            warn!(
                %thread,
                request_id = %approval_id,
                %error,
                "failed to cancel pending approval after interrupt"
            );
        }
    }

    for server_request_id in server_request_ids {
        let response = ServerRequestResponse::Error {
            code: -32000,
            message: "Turn interrupted before this server request was answered.".into(),
        };
        if let Err(error) =
            handle_respond_server_request(client, mapper, senders, &server_request_id, response)
                .await
        {
            warn!(
                %thread,
                request_id = %server_request_id,
                %error,
                "failed to reject pending server request after interrupt"
            );
        }
    }
}

async fn handle_interrupt(
    client: &mut dyn CodexTransport,
    mapper: &CodexMapper,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let native_turn_id = mapper
        .active_native_turn_for_thread(thread.thread)
        .ok_or_else(|| HarnessError::Unsupported("no active Codex turn to interrupt".into()))?;
    handle_interrupt_turn(
        client,
        CodexOperationContext::for_thread("interrupt", thread).with_native_turn_id(native_turn_id),
        &thread.harness_thread_id,
        native_turn_id,
    )
    .await
}

async fn handle_terminate_command(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    process_id: &str,
) -> Result<(), HarnessError> {
    if process_id.parse::<i32>().is_ok() {
        match handle_terminate_background_terminal(client, thread, process_id).await {
            Ok(true) => return Ok(()),
            Ok(false) => {
                debug!(
                    thread_id = %thread.thread,
                    harness_thread_id = %thread.harness_thread_id,
                    process_id,
                    "Codex did not find a background terminal for command process"
                );
            }
            Err(error) => {
                debug!(
                    thread_id = %thread.thread,
                    harness_thread_id = %thread.harness_thread_id,
                    process_id,
                    error = %error,
                    "Codex background-terminal termination failed; trying command/exec"
                );
            }
        }
    }

    handle_terminate_command_exec(client, thread, process_id).await
}

async fn handle_terminate_background_terminal(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    process_id: &str,
) -> Result<bool, HarnessError> {
    let params = ThreadBackgroundTerminalsTerminateParams {
        thread_id: thread.harness_thread_id.clone(),
        process_id: process_id.to_owned(),
    };
    let response: ThreadBackgroundTerminalsTerminateResponse = codex_request(
        client,
        CodexOperationContext::for_thread("terminate_background_terminal", thread)
            .with_process_id(process_id),
        THREAD_BACKGROUND_TERMINALS_TERMINATE,
        &params,
    )
    .await?;
    Ok(response.terminated)
}

async fn handle_terminate_command_exec(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    process_id: &str,
) -> Result<(), HarnessError> {
    let params = codex_codes::CommandExecTerminateParams {
        process_id: process_id.to_owned(),
    };
    let _: codex_codes::CommandExecTerminateResponse = codex_request(
        client,
        CodexOperationContext::for_thread("terminate_command_exec", thread)
            .with_process_id(process_id),
        codex_codes::protocol::methods::COMMAND_EXEC_TERMINATE,
        &params,
    )
    .await?;
    Ok(())
}

async fn handle_compact_thread(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadCompactStartParams {
        thread_id: thread.harness_thread_id.clone(),
    };
    let _: codex_codes::ThreadCompactStartResponse = codex_request(
        client,
        CodexOperationContext::for_thread("compact_thread", thread),
        codex_codes::protocol::methods::THREAD_COMPACT_START,
        &params,
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
    // -32600, and codex-codes formats that response with this prefix. Keep the match fail-closed if
    // any layer changes; a different "thread not found" error must remain visible to the caller.
    const PREFIX: &str = "JSON-RPC error (-32600): no rollout found for thread id ";
    let HarnessError::Transport(message) = error else {
        return false;
    };
    message
        .strip_prefix(PREFIX)
        .is_some_and(|missing_id| missing_id == harness_thread_id)
}

async fn handle_interrupt_turn(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    native_thread_id: &str,
    native_turn_id: &str,
) -> Result<(), HarnessError> {
    let params = codex_codes::TurnInterruptParams {
        thread_id: native_thread_id.to_owned(),
        turn_id: native_turn_id.to_owned(),
    };
    let _: codex_codes::TurnInterruptResponse = codex_request(
        client,
        context,
        codex_codes::protocol::methods::TURN_INTERRUPT,
        &params,
    )
    .await?;
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
    }

    struct FakeCodexTransport {
        state: Arc<Mutex<FakeCodexState>>,
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

    fn fake_codex() -> (FakeCodexTransport, FakeCodexController) {
        let (events_tx, events_rx) = mpsc::channel(32);
        let state = Arc::new(Mutex::new(FakeCodexState::default()));
        (
            FakeCodexTransport {
                state: state.clone(),
                events_rx,
            },
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
                        if state.thread_resume_missing_rollout_failures > 0 {
                            state.thread_resume_missing_rollout_failures -= 1;
                            Err(HarnessError::Transport(format!(
                                "JSON-RPC error (-32600): no rollout found for thread id \
                                 {native_thread_id}"
                            )))
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
                            Err(HarnessError::Transport(message))
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

        async fn next_message(
            &mut self,
        ) -> Result<Option<codex_codes::ServerMessage>, CodexStreamError> {
            self.events_rx.recv().await.transpose()
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

        async fn shutdown_transport(self) -> Result<(), HarnessError> {
            let mut state = self.state.lock().await;
            state.shutdowns += 1;
            if state.hang_shutdown {
                drop(state);
                std::future::pending().await
            } else if state.block_shutdown {
                let release = state.shutdown_release.clone();
                drop(state);
                release.notified().await;
            }
            Ok(())
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
        let (transport, controller) = fake_codex();
        let harness = CodexHarness::spawn_harness(
            transport,
            PathBuf::from("/tmp"),
            Vec::new(),
            Some("1.2.3".into()),
            bootstrap,
        )
        .expect("fake harness should spawn");
        (harness, controller)
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
    async fn start_turn_maps_image_attachment_to_codex_data_url() {
        let (mut transport, controller) = fake_codex();
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
        let (mut transport, controller) = fake_codex();
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
        let (mut transport, controller) = fake_codex();
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
    fn bare_non_json_stream_error_is_recoverable() {
        let serde_error = serde_json::from_str::<Value>("not JSON").unwrap_err();
        let parse_error = codex_codes::ParseError::from_line("not JSON", serde_error);

        assert!(matches!(
            classify_codex_stream_error(codex_codes::Error::Deserialization(parse_error)),
            CodexStreamError::NonJsonStdout { .. }
        ));
    }

    #[test]
    fn non_json_stdout_preview_is_bounded_on_a_utf8_boundary() {
        let raw = format!("{}é", "x".repeat(NON_JSON_STDOUT_PREVIEW_BYTES - 1));
        let serde_error = serde_json::from_str::<Value>(&raw).unwrap_err();
        let parse_error = codex_codes::ParseError::from_line(&raw, serde_error);

        let CodexStreamError::NonJsonStdout {
            raw_preview,
            raw_bytes,
            ..
        } = classify_codex_stream_error(codex_codes::Error::Deserialization(parse_error))
        else {
            panic!("expected recoverable non-JSON stdout");
        };
        assert_eq!(raw_preview.len(), NON_JSON_STDOUT_PREVIEW_BYTES - 1);
        assert_eq!(raw_bytes, raw.len());
    }

    #[test]
    fn parseable_json_stream_error_remains_fatal() {
        let raw = r#"{"unexpected":true}"#;
        let serde_error = serde_json::from_str::<i32>(raw).unwrap_err();
        let parse_error = codex_codes::ParseError::from_line(raw, serde_error);

        assert!(matches!(
            classify_codex_stream_error(codex_codes::Error::Deserialization(parse_error)),
            CodexStreamError::Fatal(HarnessError::Transport(_))
        ));
    }

    #[test]
    fn truncated_json_rpc_object_remains_fatal() {
        let raw = r#"{"method":"turn/completed""#;
        let serde_error = serde_json::from_str::<Value>(raw).unwrap_err();
        let parse_error = codex_codes::ParseError::from_line(raw, serde_error);

        assert!(matches!(
            classify_codex_stream_error(codex_codes::Error::Deserialization(parse_error)),
            CodexStreamError::Fatal(HarnessError::Transport(_))
        ));
    }

    #[test]
    fn typed_json_rpc_decode_error_remains_fatal() {
        let serde_error = serde_json::from_value::<i32>(json!({"unexpected": true})).unwrap_err();
        let parse_error = codex_codes::ParseError::from_envelope(
            "turn/completed",
            Some(json!({"unexpected": true})),
            serde_error,
        );

        assert!(matches!(
            classify_codex_stream_error(codex_codes::Error::Deserialization(parse_error)),
            CodexStreamError::Fatal(HarnessError::Transport(_))
        ));
    }

    #[test]
    fn foreign_turn_completion_does_not_end_live_stream() {
        let stream_thread = ThreadId::new();
        let foreign_thread = ThreadId::new();
        let turn = TurnId::new();
        let current_turn = TurnId::new();
        let event = completed_event(foreign_thread, turn);

        assert!(!event_belongs_to_stream(stream_thread, &event));
        assert!(!event_belongs_to_current_turn(
            stream_thread,
            current_turn,
            &event
        ));
        assert!(!event_completes_stream(stream_thread, current_turn, &event));
        assert!(event_completes_stream(foreign_thread, turn, &event));
    }

    #[test]
    fn same_thread_stale_turn_completion_does_not_end_live_stream() {
        let thread = ThreadId::new();
        let current_turn = TurnId::new();
        let previous_turn = TurnId::new();
        let event = completed_event(thread, previous_turn);

        assert!(event_belongs_to_stream(thread, &event));
        assert!(!event_belongs_to_current_turn(thread, current_turn, &event));
        assert!(!event_completes_stream(thread, current_turn, &event));
        assert!(event_completes_stream(thread, previous_turn, &event));
    }

    #[test]
    fn same_thread_stale_turn_error_is_not_current_turn() {
        let thread = ThreadId::new();
        let current_turn = TurnId::new();
        let previous_turn = TurnId::new();
        let stale_error = AgentEvent::Error {
            thread,
            turn: Some(previous_turn),
            error: HarnessError::Protocol("previous failure".into()),
        };

        assert!(!event_belongs_to_current_turn(
            thread,
            current_turn,
            &stale_error
        ));

        let turnless_error = AgentEvent::Error {
            thread,
            turn: None,
            error: HarnessError::Protocol("unscoped failure".into()),
        };
        assert!(event_belongs_to_current_turn(
            thread,
            current_turn,
            &turnless_error
        ));
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

    #[test]
    fn codex_messages_are_polled_while_any_turn_is_active() {
        let mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let mut active_turns = ActiveTurns::new();
        let thread = test_thread();
        active_turns.insert(thread.thread, ActiveTurn::new(thread, TurnId::new()));

        assert!(should_poll_codex_messages(
            &mapper,
            &active_turns,
            &HashMap::new()
        ));
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
        let mut stream = harness.subscribe(&thread);

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
    async fn fatal_stream_error_closes_worker_with_only_pending_compaction() {
        let (harness, controller) = spawn_fake_harness();
        let thread = harness.open_thread(open_opts(None, None)).await.unwrap();
        controller
            .send_stream_error(CodexStreamError::Fatal(HarnessError::Transport(
                "connection lost".into(),
            )))
            .await;

        harness.compact_thread(&thread).await.unwrap();
        timeout(Duration::from_secs(1), async {
            while !harness.worker_queue.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fatal stream error must close the worker");

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
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let first_turn = harness
            .start_turn(&first, UserInput::text("ask later"), build_turn_overrides())
            .await
            .unwrap();
        let first_native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut first_stream = harness.subscribe(&first);

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
        let mut first_stream = harness.subscribe(&first);
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
                "JSON-RPC error (-32600): no rollout found for thread id {}",
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
            .fail_thread_delete(
                "JSON-RPC error (-32600): no rollout found for thread id different-thread",
            )
            .await;

        let error = timeout(Duration::from_secs(1), harness.delete_thread(&thread))
            .await
            .expect("delete_thread should complete")
            .expect_err("a nonmatching missing-rollout error must remain fatal");
        assert!(matches!(
            error,
            HarnessError::Transport(message) if message.ends_with("different-thread")
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
        let mut first_stream = harness.subscribe(&first);
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
        let mut first_stream = harness.subscribe(&first);
        let mut second_stream = harness.subscribe(&second);

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
        let mut stream = harness.subscribe(&thread);

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

        timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(None, None)),
        )
        .await
        .expect("worker must keep processing commands after a hung approval response")
        .unwrap();
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

    #[test]
    fn opening_thread_preserves_existing_sender() {
        let thread = ThreadId::new();
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (first_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let mut first_rx = first_tx.subscribe();
        ensure_thread_sender(&senders, thread, first_tx);

        let (replacement_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, replacement_tx);

        let turn = TurnId::new();
        sender_for_thread(&senders, thread)
            .expect("sender exists")
            .send(AgentEvent::TurnStarted { thread, turn })
            .unwrap();
        assert!(matches!(
            first_rx.try_recv(),
            Ok(AgentEvent::TurnStarted { thread: got_thread, turn: got_turn })
                if got_thread == thread && got_turn == turn
        ));
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
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        emit_incomplete_turn(&senders, thread, None, "stream ended").await;

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
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        emit_incomplete_turn(&senders, thread, Some(turn), "stream failed").await;

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
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        assert!(emit_fatal_turn_completion(&senders, thread, Some(turn), "quota exceeded").await);

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
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        assert!(!emit_fatal_turn_completion(&senders, thread, None, "quota exceeded").await);

        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
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
        let (mut transport, controller) = fake_codex();
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
        let (mut transport, controller) = fake_codex();
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
