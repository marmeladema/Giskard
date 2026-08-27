//! The `AgentHarness` abstraction — the keystone of the harness-agnostic design (spec §4).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::mcp::{McpOauthStart, McpServerStatus};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::TurnOverrides;
use giskard_core::user_input::UserInput;

/// What a harness can do (spec §4.2). Different harnesses advertise different capabilities;
/// the UI adapts accordingly (§13.5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HarnessCapabilities {
    /// Server-initiated, per-action approval requests (accept/decline while a turn is live).
    pub live_approvals: bool,
    /// Distinct read-only (plan) vs read-write (build) sandbox modes switchable per turn.
    pub plan_build_modes: bool,
    /// Per-turn model override (change model between turns of one thread).
    pub per_turn_model: bool,
    /// Reasoning-effort control (medium/high/xhigh, model-dependent).
    pub reasoning_effort: bool,
    /// Structured, per-file diff stream (for the side-by-side viewer).
    pub structured_diffs: bool,
    /// Durable thread resume across process/app restarts.
    pub resumable_threads: bool,
    /// A queryable model list (e.g. GET /v1/models via the provider).
    pub model_listing: bool,
    /// The harness can report the providers it is configured to route to (§8.2).
    pub provider_listing: bool,
    /// Token usage reported on turn completion.
    pub token_usage: bool,
    /// MCP server status can be listed through the harness.
    pub mcp_status: bool,
    /// MCP server config can be reloaded through the harness.
    pub mcp_reload: bool,
    /// MCP OAuth login can be started through the harness.
    pub mcp_oauth_login: bool,
    /// Manual context compaction can be requested for a thread.
    pub context_compaction: bool,
}

/// A provider the harness is configured to route turns to (spec §8.2).
///
/// Giskard declares providers by `id` alone; the endpoint, display name, and key location live in
/// the harness's own configuration and are read back through [`AgentHarness::list_providers`]
/// rather than restated in `config.toml`.
///
/// Deliberately carries *where the key comes from*, never the key itself: a harness config may
/// hold an inline secret, and copying it into Giskard would spread it across another process's
/// memory and logs for no benefit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessProvider {
    /// Routing id. A Giskard `[providers.<id>]` key must match one of these to be reachable.
    pub id: String,
    /// Friendly name for the picker, when the harness supplies one.
    pub name: Option<String>,
    /// OpenAI-compatible base URL, used for Giskard's own `/v1/models` discovery (§8.3).
    pub base_url: Option<String>,
    /// Where this provider's discovery key comes from, when the harness names a source.
    pub auth: Option<ProviderAuth>,
}

/// Where a provider's discovery key comes from.
///
/// One variant or none, never both: Codex's own `ModelProviderInfo::validate` rejects a provider
/// that declares `auth` alongside `env_key`, so the two sources are mutually exclusive upstream and
/// an enum is the honest shape rather than two optional fields that could disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderAuth {
    /// The key is the value of this environment variable.
    Env(String),
    /// The key is whatever this command prints to stdout.
    Command(ProviderAuthCommand),
}

/// A command whose stdout is a provider's bearer token (Codex's `[model_providers.<id>.auth]`).
///
/// Giskard runs this itself when composing a project's model list, because `/v1/models` discovery
/// is Giskard's own HTTP request rather than one the harness makes. That means executing a command
/// named in the harness's config file — the same file that already tells Giskard which endpoint to
/// call, and the same command the harness runs on every turn, so this widens *when* it runs rather
/// than *whether* it does.
///
/// It is worth being precise about where that config can come from, since opening a project points
/// the harness at a directory: for Codex, provider tables are read only from the user-level,
/// packaged-default, and enterprise-managed layers. `model_providers` sits on Codex's
/// project-local denylist, so a `.codex/config.toml` committed to a cloned repository cannot
/// introduce a provider — and therefore cannot introduce a command to run. A harness that does not
/// hold that line should not report `ProviderAuth::Command`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAuthCommand {
    /// Program to run. A bare name resolves via `PATH`; a relative path resolves against `cwd`.
    pub command: String,
    pub args: Vec<String>,
    /// Working directory, when the harness names one.
    pub cwd: Option<PathBuf>,
    /// How long to wait for the command before giving up.
    pub timeout: Duration,
}

/// A provider's key source that could not produce a usable token.
///
/// Distinct from "no key configured", which is not an error: an unset environment variable leaves
/// discovery to try unauthenticated. This is the source being *present and broken* — a command that
/// fails, or a value that cannot be sent.
///
/// `origin` names which one, phrased to read as the subject of `reason`, because a provider has two
/// and a message blaming the wrong one sends the user to the wrong place.
#[derive(Debug, thiserror::Error)]
#[error("{origin} {reason}")]
pub struct ProviderAuthError {
    /// Named `origin` rather than `source`, which `thiserror` reserves for a nested error.
    pub origin: String,
    pub reason: String,
}

impl ProviderAuthError {
    fn command(command: &str, reason: impl Into<String>) -> Self {
        Self {
            origin: format!("auth command `{command}`"),
            reason: reason.into(),
        }
    }

    fn env(var: &str, reason: impl Into<String>) -> Self {
        Self {
            origin: format!("environment variable ${var}"),
            reason: reason.into(),
        }
    }
}

impl HarnessProvider {
    /// Resolve the discovery API key from whichever source this provider names.
    ///
    /// `Ok(None)` covers both "no source" and an environment variable that is unset or empty —
    /// discovery then tries unauthenticated, which is what it did before any of this existed. A
    /// command that fails, times out, or prints nothing is `Err`, so the caller can say what went
    /// wrong instead of reporting the 401 that would follow.
    ///
    /// The token is recomputed on each call. Codex caches it with a refresh interval; discovery
    /// runs when a project's model list is composed, which is rare enough that a cache would mostly
    /// hold a stale token.
    pub async fn resolve_api_key(&self) -> Result<Option<String>, ProviderAuthError> {
        match self.auth.as_ref() {
            None => Ok(None),
            // An unset variable and one holding only whitespace mean the same thing.
            Some(ProviderAuth::Env(var)) => match std::env::var(var).ok() {
                None => Ok(None),
                Some(value) => usable_token(value.trim()).map_err(|tail| {
                    ProviderAuthError::env(var, format!("holds a value that {tail}"))
                }),
            },
            Some(ProviderAuth::Command(auth)) => run_auth_command(auth).await.map(Some),
        }
    }
}

/// Vet a candidate token, whichever source produced it.
///
/// `Ok(None)` is "there is no token here". `Err` carries why a non-empty one is unusable: it goes
/// out verbatim in an `Authorization` header, which cannot hold control characters or non-ASCII, so
/// letting it through would surface as an opaque HTTP-client "builder error" that reads as if the
/// provider were unreachable — from either source, which is why both go through here.
fn usable_token(candidate: &str) -> Result<Option<String>, &'static str> {
    if candidate.is_empty() {
        return Ok(None);
    }
    if candidate.bytes().any(|b| !(0x20..=0x7e).contains(&b)) {
        return Err("is not plain visible ASCII, so it cannot be sent in an Authorization header");
    }
    Ok(Some(candidate.to_string()))
}

/// How much of a failing command's stderr is worth repeating.
const MAX_AUTH_STDERR: usize = 400;

/// The command's stderr, trimmed and bounded, or `None` when it said nothing.
fn truncated_stderr(stderr: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(stderr);
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let Some((cut, _)) = text.char_indices().nth(MAX_AUTH_STDERR) else {
        return Some(text.to_string());
    };
    Some(format!("{}…", &text[..cut]))
}

async fn run_auth_command(auth: &ProviderAuthCommand) -> Result<String, ProviderAuthError> {
    // The arguments are not logged: they are a credential helper's invocation, and a helper that
    // takes its secret as an argument would otherwise put it in the log. The count is enough to
    // tell one configured provider's command from another.
    debug!(
        action = "provider_auth_command",
        command = %auth.command,
        args = auth.args.len(),
        timeout_ms = auth.timeout.as_millis(),
        "running provider auth command"
    );
    let started = std::time::Instant::now();
    let mut command = tokio::process::Command::new(&auth.command);
    command
        .args(&auth.args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // Without this a command that ignores the timeout outlives the request that wanted it.
        .kill_on_drop(true);
    if let Some(cwd) = &auth.cwd {
        command.current_dir(cwd);
    }

    let output = tokio::time::timeout(auth.timeout, command.output())
        .await
        .map_err(|_| {
            // Logged here as well as returned: the caller reports the failure against a provider,
            // but only this boundary knows the command sat for the whole budget rather than
            // failing fast, which is what a hanging helper looks like.
            warn!(
                action = "provider_auth_command",
                command = %auth.command,
                timeout_ms = auth.timeout.as_millis(),
                "provider auth command timed out"
            );
            ProviderAuthError::command(
                &auth.command,
                format!("timed out after {} ms", auth.timeout.as_millis()),
            )
        })?
        .map_err(|e| ProviderAuthError::command(&auth.command, format!("failed to start: {e}")))?;

    if !output.status.success() {
        // stderr is the command's own diagnostic and the only clue to why it refused, so it is
        // quoted back the way Codex quotes it. The token itself is on stdout and never is. Bounded
        // because this reaches both a log line and an API response, and a chatty helper should not
        // decide how big either gets.
        let detail = match truncated_stderr(&output.stderr) {
            Some(stderr) => format!(": {stderr}"),
            None => String::new(),
        };
        return Err(ProviderAuthError::command(
            &auth.command,
            format!("exited with {}{detail}", output.status),
        ));
    }

    let token = String::from_utf8(output.stdout)
        .map_err(|_| ProviderAuthError::command(&auth.command, "wrote non-UTF-8 data to stdout"))?
        .trim()
        .to_string();
    let token = match usable_token(&token) {
        Ok(Some(token)) => token,
        Ok(None) => {
            return Err(ProviderAuthError::command(
                &auth.command,
                "produced an empty token",
            ));
        }
        Err(tail) => {
            return Err(ProviderAuthError::command(
                &auth.command,
                format!("produced a token that {tail}; it should print the token and nothing else"),
            ));
        }
    };
    debug!(
        action = "provider_auth_command",
        command = %auth.command,
        elapsed_ms = started.elapsed().as_millis(),
        "provider auth command produced a token"
    );
    Ok(token)
}

/// Options for opening (or resuming) a thread.
#[derive(Debug, Clone)]
pub struct OpenThreadOptions {
    pub project: ProjectId,
    pub thread: Option<ThreadId>,
    pub workspace_root: PathBuf,
    /// Some(native id) ⇒ resume; None ⇒ fresh thread.
    pub resume: Option<String>,
    /// Whether a failed native resume may recover by starting a replacement thread.
    ///
    /// Linked sub-agent imports must use `RequireExisting`: their advertised native id is the
    /// ownership and event-routing identity, so silently replacing it would attach Giskard to a
    /// different thread. Normal primary-thread reopen keeps the historical fresh-session recovery.
    pub resume_policy: ResumePolicy,
    /// The model to open on, or `None` to take whatever the harness already has for a resumed
    /// thread.
    ///
    /// `None` is only meaningful together with `resume`: importing a native thread Giskard has no
    /// record of, where the harness is the only one who knows which model that thread was using.
    /// Requesting one there does not express a preference, it *overrides* — Codex stops applying
    /// the thread's persisted model as soon as `model`/`modelProvider` is supplied — so a guess
    /// would silently move an existing conversation onto a different model. Starting a fresh
    /// thread, and reopening one Giskard already tracks, both pass `Some`.
    pub initial_model: Option<ModelRef>,
    /// Bounded, non-blocking destination for metadata discovered after open returns.
    pub updates: ThreadUpdateSink,
    /// Re-arm retention of this thread's events for a caller that will subscribe only after
    /// further work.
    ///
    /// A thread retains from the moment the harness opens its channel, so a *first* open needs
    /// nothing from this flag. It matters on reopen: the previous forwarder consumed the earlier
    /// retention, and a thread that is already running will produce events before a new one can
    /// attach. Opening and subscribing are back to back for most callers, on a thread that is not
    /// running, so nothing is produced in between; a linked sub-agent is the exception, because
    /// its work is already under way while its metadata and ownership are still being settled.
    ///
    /// Has no effect while a subscriber is attached: retention holds events back from the
    /// channel, so re-arming under a live forwarder would starve it.
    pub retain_until_subscribed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadUpdate {
    ContextWindowRestored {
        model: ModelRef,
        context_window: u32,
    },
}

#[derive(Debug, Clone)]
pub struct ThreadUpdateSink(mpsc::Sender<ThreadUpdate>);

impl ThreadUpdateSink {
    pub fn send(&self, update: ThreadUpdate) -> Result<(), ThreadUpdateSendError> {
        self.0.try_send(update).map_err(|error| match error {
            mpsc::error::TrySendError::Full(update) => ThreadUpdateSendError::Full(update),
            mpsc::error::TrySendError::Closed(update) => ThreadUpdateSendError::Closed(update),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadUpdateSendError {
    Full(ThreadUpdate),
    Closed(ThreadUpdate),
}

pub struct ThreadUpdateStream(mpsc::Receiver<ThreadUpdate>);

impl ThreadUpdateStream {
    pub async fn recv(&mut self) -> Option<ThreadUpdate> {
        self.0.recv().await
    }
}

pub fn thread_update_channel() -> (ThreadUpdateSink, ThreadUpdateStream) {
    let (tx, rx) = mpsc::channel(1);
    (ThreadUpdateSink(tx), ThreadUpdateStream(rx))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ResumePolicy {
    #[default]
    AllowFreshFallback,
    RequireExisting,
}

/// Handle to an opened thread.
#[derive(Debug, Clone)]
pub struct ThreadHandle {
    pub thread: ThreadId,
    pub harness_thread_id: String,
    /// Workspace root the harness opened this thread against. Turns must use this exact runtime
    /// root when rebinding workspace-scoped permissions; for isolated threads it is the worktree,
    /// not the project's checkout.
    pub workspace_root: PathBuf,
    pub warning: Option<HarnessNotice>,
    /// The model/provider the harness reports as *effective* for the opened thread, when the
    /// native protocol exposes it (Codex `thread/resume` echoes `model`/`modelProvider`). Callers
    /// switching a thread's provider must verify this against what they requested: Codex can
    /// intentionally ignore resume overrides for already-loaded threads while still answering
    /// success (see `specs/model-provider-switching-analysis.md`). `None` ⇒ the harness gave no
    /// signal and the requested model must be assumed.
    pub resumed_model: Option<ModelRef>,
    /// Optional user-facing sub-agent name reported by the harness, such as Codex's random
    /// AgentControl nickname.
    pub agent_name: Option<String>,
    /// Harness-native parent thread id when the native protocol exposes the relationship.
    /// Servers use this only to validate a proposed Giskard parent; it never replaces a durable
    /// Giskard `ThreadId`.
    pub parent_harness_thread_id: Option<String>,
}

impl ThreadHandle {
    /// Build a handle for an opened, turn-capable harness thread.
    pub fn opened(thread: ThreadId, harness_thread_id: String, workspace_root: PathBuf) -> Self {
        Self {
            thread,
            harness_thread_id,
            workspace_root,
            warning: None,
            resumed_model: None,
            agent_name: None,
            parent_harness_thread_id: None,
        }
    }

    /// Build a minimal handle for native operations on a persisted thread that is not attached.
    pub fn detached(thread: ThreadId, harness_thread_id: String) -> Self {
        Self {
            thread,
            harness_thread_id,
            workspace_root: PathBuf::new(),
            warning: None,
            resumed_model: None,
            agent_name: None,
            parent_harness_thread_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HarnessNotice {
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
}

/// A typed wrapper around a `broadcast::Receiver<AgentEvent>`, optionally preceded by events the
/// harness retained for this thread before any subscriber existed.
pub struct AgentEventStream {
    /// Drained before the live receiver, so a subscriber that attaches after the thread started
    /// producing still sees its opening events, in order.
    backlog: VecDeque<AgentEvent>,
    rx: broadcast::Receiver<AgentEvent>,
}

impl AgentEventStream {
    pub fn new(rx: broadcast::Receiver<AgentEvent>) -> Self {
        Self {
            backlog: VecDeque::new(),
            rx,
        }
    }

    /// Build a stream that replays `backlog` before switching to live events.
    pub fn with_backlog(
        backlog: VecDeque<AgentEvent>,
        rx: broadcast::Receiver<AgentEvent>,
    ) -> Self {
        Self { backlog, rx }
    }

    /// How many replayed events are still queued ahead of the live stream.
    ///
    /// Non-zero means the next `recv` returns history the thread produced before this stream
    /// existed, rather than something happening now. Consumers whose behaviour depends on an
    /// event being live — anything reading an event as evidence of concurrent activity — have to
    /// tell the two apart.
    pub fn backlog_len(&self) -> usize {
        self.backlog.len()
    }

    /// Recv next event (awaits).
    pub async fn recv(&mut self) -> Result<AgentEvent, broadcast::error::RecvError> {
        if let Some(event) = self.backlog.pop_front() {
            return Ok(event);
        }
        self.rx.recv().await
    }

    /// Take the next event if one is already available, without awaiting.
    pub fn try_recv(&mut self) -> Result<AgentEvent, broadcast::error::TryRecvError> {
        if let Some(event) = self.backlog.pop_front() {
            return Ok(event);
        }
        self.rx.try_recv()
    }

    /// Convert to a `BoxStream` for ergonomic use with `futures`.
    pub fn into_stream(self) -> BoxStream<'static, AgentEvent> {
        use futures::StreamExt;
        futures::stream::unfold(self, |mut stream| async move {
            match stream.recv().await {
                Ok(event) => Some((event, stream)),
                Err(_) => None,
            }
        })
        .boxed()
    }
}

/// The neutral harness contract (spec §4.3).
///
/// Every method is dyn-compatible: `&self` receivers, no generic method params, no `Self`-by-value.
/// The whole application holds harnesses as `Arc<dyn AgentHarness>`.
#[async_trait]
pub trait AgentHarness: Send + Sync {
    fn capabilities(&self) -> HarnessCapabilities;

    /// List models available through this harness/provider, if supported.
    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError>;

    /// List the providers this harness can route to (§8.2), when it can introspect its own
    /// configuration. Giskard uses the result to resolve discovery endpoints and to tell the user
    /// when a configured provider id is one the harness has never heard of.
    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        Err(HarnessError::Unsupported(
            "provider listing is not supported by this harness".into(),
        ))
    }

    /// The harness's own version, when it knows it.
    ///
    /// Only used to identify Giskard to a provider's `/models` endpoint as the harness would
    /// (§8.3): a provider that serves the harness's richer catalog keys off the *harness* version,
    /// not Giskard's, so this is the harness answering for itself rather than Giskard guessing.
    fn client_version(&self) -> Option<String> {
        None
    }

    /// List configured MCP servers and their visible tools/resources.
    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError> {
        Err(HarnessError::Unsupported(
            "MCP server status is not supported by this harness".into(),
        ))
    }

    /// Reload MCP server configuration.
    async fn reload_mcp_servers(&self) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "MCP server reload is not supported by this harness".into(),
        ))
    }

    /// Start an OAuth login flow for one MCP server.
    async fn start_mcp_oauth_login(&self, name: &str) -> Result<McpOauthStart, HarnessError> {
        Err(HarnessError::Unsupported(format!(
            "MCP OAuth login is not supported for server {name:?}"
        )))
    }

    /// Open (or resume) a thread.
    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError>;

    /// Start a turn: send user input, applying per-turn overrides.
    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError>;

    /// Subscribe to the stream of neutral events for a thread.
    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream;

    /// Tell the harness which native threads Giskard already has durable ids for.
    ///
    /// A harness can be told about a thread by the agent before Giskard opens it — Codex creates a
    /// sub-agent's thread and starts its first turn before the tool call naming the child returns.
    /// A harness routing those events has no id to route them to unless it invents one, and an
    /// invented id for a thread Giskard already persisted is a *second* identity for one thread:
    /// two ids, one of them wrong, and everything keyed by either has to be reconciled afterwards.
    ///
    /// Supplying the bindings up front removes that problem rather than making it cheaper. Every
    /// `(native id, ThreadId)` pair Giskard already knows is one the harness never has to guess.
    ///
    /// Called once per harness, before any thread is opened on it. Idempotent, and harnesses that
    /// do not translate ids ignore it.
    async fn bind_known_threads(&self, bindings: Vec<(String, ThreadId)>) {
        let _ = bindings;
    }

    /// Respond to a pending approval request.
    async fn respond_approval(
        &self,
        req: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError>;

    /// Respond to a pending non-approval server request.
    async fn respond_server_request(
        &self,
        req: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError>;

    /// Interrupt the active turn of a thread.
    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

    /// Ask the harness to compact the thread context, when supported.
    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!(
            "context compaction is not supported for thread {}",
            thread.harness_thread_id
        )))
    }

    /// Ask the harness to terminate a running command process, if it exposes a process handle.
    async fn terminate_command(
        &self,
        _thread: &ThreadHandle,
        process_id: &str,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!(
            "command termination is not supported for process {process_id}"
        )))
    }

    /// Rename a durable thread in the underlying harness, when supported.
    async fn set_thread_name(&self, thread: &ThreadHandle, name: &str) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!(
            "renaming thread {} to {name:?} is not supported",
            thread.harness_thread_id
        )))
    }

    /// Archive or unarchive a durable thread in the underlying harness, when supported.
    async fn set_thread_archived(
        &self,
        thread: &ThreadHandle,
        archived: bool,
    ) -> Result<(), HarnessError> {
        let action = if archived { "archiving" } else { "unarchiving" };
        Err(HarnessError::Unsupported(format!(
            "{action} thread {} is not supported",
            thread.harness_thread_id
        )))
    }

    /// Delete a durable thread in the underlying harness, when supported.
    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!(
            "deleting thread {} is not supported",
            thread.harness_thread_id
        )))
    }

    /// Cleanly shut down the harness.
    ///
    /// Takes `&self` (not `self: Arc<Self>`) so the trait stays object-safe.
    /// Idempotent: implementations perform teardown once and treat further calls as no-ops.
    async fn shutdown(&self) -> Result<(), HarnessError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command_provider(args: &[&str], timeout: Duration) -> HarnessProvider {
        HarnessProvider {
            id: "opencodex".into(),
            name: None,
            base_url: None,
            auth: Some(ProviderAuth::Command(ProviderAuthCommand {
                command: "sh".into(),
                args: args.iter().map(|a| (*a).to_string()).collect(),
                cwd: None,
                timeout,
            })),
        }
    }

    #[tokio::test]
    async fn a_command_token_is_its_trimmed_stdout() {
        let provider = command_provider(&["-c", "echo  spaced-token  "], Duration::from_secs(5));
        assert_eq!(
            provider.resolve_api_key().await.unwrap(),
            Some("spaced-token".to_string()),
            "surrounding whitespace and the trailing newline are not part of the token"
        );
    }

    /// Only stdout is the token. A command that chats on stderr and still succeeds is fine.
    #[tokio::test]
    async fn stderr_is_not_part_of_the_token() {
        let provider = command_provider(
            &["-c", "echo noise >&2; printf %s real-token"],
            Duration::from_secs(5),
        );
        assert_eq!(
            provider.resolve_api_key().await.unwrap(),
            Some("real-token".to_string())
        );
    }

    #[tokio::test]
    async fn a_command_that_prints_nothing_is_an_error() {
        let provider = command_provider(&["-c", "true"], Duration::from_secs(5));
        let err = provider
            .resolve_api_key()
            .await
            .expect_err("an empty token is not a token");
        assert!(err.to_string().contains("empty token"), "{err}");
    }

    /// A helper that prints the token plus a stray line cannot go in a header. Reporting it against
    /// the command beats the HTTP client's opaque "builder error", which reads as if the provider
    /// were unreachable. Only the interior matters — `trim` has already taken the edges off.
    #[tokio::test]
    async fn a_token_that_cannot_be_a_header_value_is_an_error() {
        for script in [
            r"printf 'tok\nnoise'",
            r"printf 'tok\ttab'",
            "printf 'tok\u{e9}n'",
        ] {
            let provider = command_provider(&["-c", script], Duration::from_secs(5));
            let err = provider
                .resolve_api_key()
                .await
                .expect_err("a token that cannot be a header value is not usable")
                .to_string();
            assert!(
                err.contains("visible ASCII"),
                "{script:?} should be rejected: {err}"
            );
        }
    }

    #[tokio::test]
    async fn a_failing_command_reports_its_status_and_stderr() {
        let provider = command_provider(
            &["-c", "echo vault locked >&2; exit 7"],
            Duration::from_secs(5),
        );
        let err = provider
            .resolve_api_key()
            .await
            .expect_err("exit 7 is a failure");
        let message = err.to_string();
        assert!(
            message.contains("vault locked"),
            "stderr should be quoted: {message}"
        );
        assert!(
            message.contains('7'),
            "the exit status should be named: {message}"
        );
    }

    /// A chatty helper should not decide how long a warning or a log line gets.
    #[tokio::test]
    async fn a_flood_of_stderr_is_bounded() {
        let provider = command_provider(
            &["-c", "printf 'x%.0s' $(seq 1 5000) >&2; exit 1"],
            Duration::from_secs(5),
        );
        let message = provider
            .resolve_api_key()
            .await
            .expect_err("exit 1 is a failure")
            .to_string();
        assert!(message.ends_with('…'), "output should be cut: {message}");
        assert!(
            message.len() < 600,
            "the whole 5000-char flood should not be repeated (len {})",
            message.len()
        );
    }

    /// A command that hangs must not hang discovery with it.
    #[tokio::test]
    async fn a_hanging_command_times_out() {
        let provider = command_provider(&["-c", "sleep 30"], Duration::from_millis(150));
        let err = provider
            .resolve_api_key()
            .await
            .expect_err("the command outlives its timeout");
        assert!(err.to_string().contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn a_command_that_does_not_exist_reports_that() {
        let mut provider = command_provider(&[], Duration::from_secs(5));
        provider.auth = Some(ProviderAuth::Command(ProviderAuthCommand {
            command: "giskard-no-such-program".into(),
            args: Vec::new(),
            cwd: None,
            timeout: Duration::from_secs(5),
        }));
        let err = provider
            .resolve_api_key()
            .await
            .expect_err("a missing program cannot produce a token");
        assert!(err.to_string().contains("failed to start"), "{err}");
    }

    fn env_provider(var: &str) -> HarnessProvider {
        HarnessProvider {
            id: "litellm".into(),
            name: None,
            base_url: None,
            auth: Some(ProviderAuth::Env(var.into())),
        }
    }

    /// An environment-supplied key gets the same treatment as a command's stdout: a variable set
    /// from a file or a here-doc routinely carries a trailing newline, and that is not part of it.
    #[tokio::test]
    async fn an_env_key_is_trimmed() {
        // Supplied by `.cargo/config.toml`; a test cannot set one without racing every other reader.
        let provider = env_provider("GISKARD_TEST_PADDED_KEY");
        assert_eq!(
            provider.resolve_api_key().await.unwrap(),
            Some("padded-key".to_string())
        );
    }

    /// And the same vetting: an interior newline cannot go in a header from either source. The
    /// message has to name the variable, not a command the provider does not have.
    #[tokio::test]
    async fn an_env_key_that_cannot_be_a_header_value_names_the_variable() {
        let provider = env_provider("GISKARD_TEST_UNUSABLE_KEY");
        let err = provider
            .resolve_api_key()
            .await
            .expect_err("a value with an interior newline cannot be sent")
            .to_string();
        assert!(
            err.contains("environment variable $GISKARD_TEST_UNUSABLE_KEY"),
            "the variable should be named: {err}"
        );
        assert!(err.contains("visible ASCII"), "{err}");
        assert!(
            !err.contains("command"),
            "an env-backed provider has no command to blame: {err}"
        );
    }

    #[tokio::test]
    async fn an_unset_env_var_is_absence_rather_than_failure() {
        let provider = env_provider("GISKARD_DEFINITELY_UNSET_KEY");
        assert_eq!(
            provider.resolve_api_key().await.unwrap(),
            None,
            "discovery still tries unauthenticated when no key is in the environment"
        );
    }

    #[tokio::test]
    async fn a_provider_naming_no_source_has_no_key() {
        let provider = HarnessProvider {
            id: "openai".into(),
            name: None,
            base_url: None,
            auth: None,
        };
        assert_eq!(provider.resolve_api_key().await.unwrap(), None);
    }
}
