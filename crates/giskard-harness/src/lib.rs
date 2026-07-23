//! The `AgentHarness` abstraction — the keystone of the harness-agnostic design (spec §4).

use std::path::PathBuf;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::sync::broadcast;

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
    pub initial_model: ModelRef,
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
    /// Build a minimal handle for native operations on a persisted thread that is not attached.
    pub fn detached(thread: ThreadId, harness_thread_id: String) -> Self {
        Self {
            thread,
            harness_thread_id,
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

/// A typed wrapper around a `broadcast::Receiver<AgentEvent>`.
pub struct AgentEventStream {
    rx: broadcast::Receiver<AgentEvent>,
}

impl AgentEventStream {
    pub fn new(rx: broadcast::Receiver<AgentEvent>) -> Self {
        Self { rx }
    }

    /// Returns the underlying receiver.
    pub fn into_inner(self) -> broadcast::Receiver<AgentEvent> {
        self.rx
    }

    /// Recv next event (awaits).
    pub async fn recv(&mut self) -> Result<AgentEvent, broadcast::error::RecvError> {
        self.rx.recv().await
    }

    /// Convert to a `BoxStream` for ergonomic use with `futures`.
    pub fn into_stream(self) -> BoxStream<'static, AgentEvent> {
        use futures::StreamExt;
        let rx = self.rx;
        futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(event) => Some((event, rx)),
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
