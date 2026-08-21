use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::thread::ThreadKind;
use giskard_core::user_input::UserInput;

pub mod wire;
pub use wire::{
    WireAgentEvent, WireApprovalKind, WireApprovalMetadata, WireApprovalRequest, WireFileDiff,
    WireHarnessError, WireItem, WireItemPayload, WireTurn,
};

pub use giskard_core::approval::{
    ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest,
};
pub use giskard_core::diff::{DiffHunk, DiffLine};
pub use giskard_core::error::HarnessError;
pub use giskard_core::event::AgentEvent;
pub use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId};
pub use giskard_core::item::{
    CommandExecutionStart, FileChangeEntry, FileChangeKind, ItemDelta, ItemKind, ItemStart,
    SubagentAction, SubagentLink, SubagentStatus,
};
pub use giskard_core::mcp::{
    McpAuthStatus, McpOauthStart, McpResource, McpResourceTemplate, McpServerInfo, McpServerStatus,
    McpTool,
};
pub use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
pub use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
pub use giskard_core::token::{ByModel, DailyTokenLedger, TokenLedger, TokenUsage};
pub use giskard_core::turn::{Mode, PermissionPreset, TurnStatus, TurnStatusKind};
pub use giskard_core::user_input::{AttachmentKind, UserAttachment};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Subscribe {
        thread_id: ThreadId,
        /// Process-scoped persisted-history cursor. A server epoch or amendment mismatch resets the
        /// authoritative page instead of guessing from turn order alone.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<ThreadHistoryCursor>,
    },
    Unsubscribe {
        thread_id: ThreadId,
    },
    SendInput {
        thread_id: ThreadId,
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<UserAttachment>,
    },
    SwitchMode {
        thread_id: ThreadId,
        request_id: String,
        mode: Mode,
    },
    SelectModel {
        thread_id: ThreadId,
        request_id: String,
        model_ref: ModelRef,
    },
    SetPermissionPreset {
        thread_id: ThreadId,
        request_id: String,
        preset: PermissionPreset,
    },
    Interrupt {
        thread_id: ThreadId,
    },
    CompactContext {
        thread_id: ThreadId,
    },
    RetryTurnPersistence {
        thread_id: ThreadId,
        turn_id: TurnId,
    },
    DiscardUnpersistedTurn {
        thread_id: ThreadId,
        turn_id: TurnId,
    },
    RetryHistoryAmendment {
        thread_id: ThreadId,
        recovery_id: String,
    },
    DiscardHistoryAmendment {
        thread_id: ThreadId,
        recovery_id: String,
    },
    TerminateCommand {
        thread_id: ThreadId,
        process_id: String,
    },
    ApprovalDecision {
        thread_id: ThreadId,
        request_id: String,
        decision: ApprovalDecision,
    },
    ServerRequestResponse {
        thread_id: ThreadId,
        request_id: String,
        response: ServerRequestResponse,
    },
    SavePlan {
        thread_id: ThreadId,
        path: String,
    },
    Ping,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadMetadata {
    pub thread_id: ThreadId,
    pub revision: u64,
    pub title: String,
    pub mode: Mode,
    pub current_model: ModelRef,
    pub context_window: u32,
    pub permission_preset: PermissionPreset,
    pub tokens: TokenLedger,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequencedThreadEvent {
    pub seq: u64,
    pub event: ThreadEventPayload,
    /// Present only when this event was appended to an already-persisted turn payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_amendment: Option<HistoryAmendment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryAmendment {
    pub server_epoch: String,
    pub sequence: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadEventPayload {
    Agent { agent_event: Box<WireAgentEvent> },
    Request { request: Box<RequestState> },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RequestPayload {
    Approval { request: WireApprovalRequest },
    Server { request: ServerRequest },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Approval,
    Server,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RequestResolution {
    Approval { decision: ApprovalDecision },
    Server,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RequestStatus {
    Pending,
    Responding,
    Resolved { resolution: RequestResolution },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestState {
    pub thread_id: ThreadId,
    pub request_id: String,
    pub payload: RequestPayload,
    pub status: RequestStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutstandingRequest {
    pub request_id: String,
    pub kind: RequestKind,
    pub responding: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RuntimeTurnState {
    Idle,
    Active {
        /// Absent only while the harness has accepted a turn but has not named it yet.
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<TurnId>,
    },
    PersistenceBlocked {
        turn_id: TurnId,
        attempts: u32,
        error: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadRuntimeSummary {
    pub thread_id: ThreadId,
    pub turn_state: RuntimeTurnState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_recovery: Option<HistoryAmendmentRecovery>,
    pub outstanding_requests: Vec<OutstandingRequest>,
}

/// Actionable failure while making a late completed item durable in an older turn.
///
/// This is deliberately independent of `turn_state`: a newer turn may be active while an older
/// background command is waiting for persistence recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryAmendmentRecovery {
    pub recovery_id: String,
    pub turn_id: TurnId,
    pub item_id: ItemId,
    pub attempts: u32,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadRuntimeOverview {
    pub revision: u64,
    pub threads: Vec<ThreadRuntimeSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTurnProjection {
    pub turn_id: TurnId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_input: Option<UserInput>,
    pub represented_through: u64,
    pub events: Vec<ThreadEventPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadHistoryCursor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_turn_id: Option<TurnId>,
    pub server_epoch: String,
    pub amendment_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BootstrapHistory {
    FullPage {
        cursor: ThreadHistoryCursor,
        turns: Vec<WireTurn>,
        has_more: bool,
    },
    Delta {
        after: ThreadHistoryCursor,
        cursor: ThreadHistoryCursor,
        turns: Vec<WireTurn>,
    },
    CursorReset {
        requested_after: ThreadHistoryCursor,
        cursor: ThreadHistoryCursor,
        turns: Vec<WireTurn>,
        has_more: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadTaskSnapshot {
    pub thread_id: ThreadId,
    pub revision: u64,
    pub tasks: Vec<RunningTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadFinalRuntime {
    pub through_seq: u64,
    pub turn_state: RuntimeTurnState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_recovery: Option<HistoryAmendmentRecovery>,
    pub tasks: ThreadTaskSnapshot,
    pub requests: Vec<RequestState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadNotice {
    pub kind: String,
    pub revision: u64,
    pub severity: ErrorSeverity,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadNoticeSnapshot {
    pub thread_id: ThreadId,
    pub revision: u64,
    pub notices: Vec<ThreadNotice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadBootstrapPayload {
    pub metadata: ThreadMetadata,
    pub history: BootstrapHistory,
    pub live_turn: Option<LiveTurnProjection>,
    pub ordered_suffix: Vec<SequencedThreadEvent>,
    pub final_runtime: ThreadFinalRuntime,
    pub notices: ThreadNoticeSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapSection {
    Metadata,
    History,
    LiveTurn,
    OrderedSuffix,
    FinalRuntime,
    Notices,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapSectionDescriptor {
    pub section: BootstrapSection,
    pub encoded_bytes: u64,
    pub chunk_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ThreadBootstrapFrame {
    Start {
        sections: Vec<BootstrapSectionDescriptor>,
    },
    Chunk {
        section: BootstrapSection,
        index: u32,
        payload_base64: String,
    },
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncReason {
    OrderedOverflow,
    ReplacementOverflow,
    BootstrapCapacity,
    SequenceGap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    #[default]
    Command,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningTask {
    pub kind: TaskKind,
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub item_id: ItemId,
    pub harness_item_id: String,
    pub command: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    pub started_at_ms: i64,
    pub output: String,
    pub after_turn: bool,
    pub terminating: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: String,
    pub severity: ErrorSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<ThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    ThreadEvent {
        thread_id: ThreadId,
        subscription_generation: u64,
        #[serde(flatten)]
        event: SequencedThreadEvent,
    },
    ThreadMetadata(ThreadMetadata),
    /// Metadata mutation result, sent even for no-ops so pending UI state always settles.
    ThreadMetadataResult {
        request_id: String,
        #[serde(flatten)]
        metadata: ThreadMetadata,
    },
    /// The committed catalog changed; the browser coalesces refetches of its authoritative list.
    ThreadCatalogChanged,
    ThreadRuntimeOverview(ThreadRuntimeOverview),
    ThreadTasks(ThreadTaskSnapshot),
    ThreadBootstrap {
        thread_id: ThreadId,
        subscription_generation: u64,
        frame: ThreadBootstrapFrame,
    },
    ThreadNotices(ThreadNoticeSnapshot),
    ResyncRequired {
        thread_id: ThreadId,
        subscription_generation: u64,
        reason: ResyncReason,
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
    Error {
        #[serde(flatten)]
        error: ErrorInfo,
    },
    Pong,
}

// ---- HTTP API types ----

#[derive(Debug, Clone, Deserialize)]
pub struct LoginRequest {
    pub password: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoginResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct WsTicketResponse {
    pub ticket: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectSummary {
    pub id: ProjectId,
    pub name: String,
    pub dir: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GitFileStatus {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_path: Option<String>,
    pub index_status: String,
    pub worktree_status: String,
    pub kind: String,
    /// Added/deleted line counts from `git diff --numstat`, kept separate for the index and the
    /// worktree so a file that is both staged and modified reports each side accurately instead of
    /// showing one combined figure twice. `None` where there is no countable diff: the side that
    /// has no changes, untracked files, and binary files (which numstat reports as `-`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_added: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_deleted: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unstaged_added: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unstaged_deleted: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GitStatusResponse {
    pub is_repository: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    pub detached: bool,
    pub ahead: u32,
    pub behind: u32,
    pub dirty: bool,
    pub staged_count: usize,
    pub unstaged_count: usize,
    pub untracked_count: usize,
    pub conflicted_count: usize,
    /// Totals across `files`; binary and untracked files contribute nothing.
    pub added_total: u32,
    pub deleted_total: u32,
    pub files: Vec<GitFileStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GitDiffResponse {
    /// The workspace-relative path that was diffed, or `None` for the whole working tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub diff: String,
    pub is_empty: bool,
}

/// What deleting a thread would destroy, per worktree in the subtree it cascades to (spec §7.1).
#[derive(Debug, Clone, Serialize)]
pub struct ThreadDeletionImpactResponse {
    /// Empty when no thread in the subtree has a worktree, which is the ordinary case.
    pub worktrees: Vec<WorktreeImpactResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorktreeImpactResponse {
    pub thread_id: ThreadId,
    pub branch: String,
    /// Modified or untracked files in the worktree. Ignored files are excluded: they do not block
    /// removal and are not work.
    pub uncommitted_changes: usize,
    /// Commits on the thread's branch that no other ref reaches, which deleting it destroys.
    pub unreachable_commits: usize,
    /// A sentence naming what would be lost, or `None` when nothing would be.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListProjectsResponse {
    pub projects: Vec<ProjectSummary>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateProjectRequest {
    pub name: String,
    pub dir: String,
    pub workspace_root: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateProjectResponse {
    pub id: ProjectId,
}

#[derive(Debug, Clone, Serialize)]
pub struct ThreadSummary {
    pub id: ThreadId,
    /// Same durable metadata revision carried by `ThreadMetadata`.
    pub revision: u64,
    pub title: String,
    /// Workspace root this thread reads and writes through. For isolated threads this is the
    /// worktree workspace, inherited by sub-agents; otherwise it is the project's workspace.
    pub workspace_root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<ThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawned_by_turn_id: Option<TurnId>,
    #[serde(default, skip_serializing_if = "is_primary_thread")]
    pub kind: ThreadKind,
    pub mode: Mode,
    pub archived: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn is_primary_thread(value: &ThreadKind) -> bool {
    *value == ThreadKind::Primary
}

#[derive(Debug, Clone, Serialize)]
pub struct ListThreadsResponse {
    pub threads: Vec<ThreadSummary>,
}

/// One older page of the authoritative completed transcript.
///
/// Pagination is ordinary authenticated HTTP rather than part of the ordered WebSocket lane. The
/// cursor is echoed so an asynchronous browser response can be correlated with the request that
/// produced it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryPageResponse {
    pub before: TurnId,
    pub turns: Vec<WireTurn>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenThreadRequest {
    pub thread_id: Option<ThreadId>,
    pub resume: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenSubagentLinkResponse {
    pub thread_id: ThreadId,
    pub title: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenThreadResponse {
    pub thread_id: ThreadId,
    pub harness_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<ErrorInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartThreadRequest {
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<UserAttachment>,
    pub model_ref: ModelRef,
    pub mode: Mode,
    pub permission_preset: PermissionPreset,
    /// How this thread gets the working tree it runs in (spec §7.1).
    ///
    /// Only creation carries this: a thread's workspace is fixed once it exists, so there is no
    /// endpoint that changes it afterwards.
    #[serde(default)]
    pub git_strategy: GitStrategy,
}

/// Where a thread's working tree comes from.
///
/// An enum rather than a flag because the question has more than two answers and the set is open —
/// giving a thread a checkout it genuinely owns, rather than a second view of the project's
/// repository, is a different strategy again. A boolean could never carry a third choice, and a
/// client that had learned to send `true` could not be told about one.
///
/// Serde rejects a variant it does not know, so a client asking for a strategy this server does not
/// implement is refused rather than quietly started in the shared checkout — which is the failure
/// worth designing against here, since it looks like it worked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitStrategy {
    /// The project's own checkout, shared with every other thread of the project. The default, and
    /// the only possibility when the workspace is not a Git repository.
    #[default]
    Shared,
    /// A linked Git worktree of the project's repository, private to this thread and the sub-agents
    /// it spawns. Isolates files; shares the repository (`docs/git-worktrees.md`).
    Worktree,
}

#[derive(Debug, Clone, Serialize)]
pub struct StartThreadResponse {
    pub thread_id: ThreadId,
    pub title: String,
    pub harness_thread_id: String,
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<ErrorInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArchiveThreadRequest {
    pub archived: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RenameThreadRequest {
    pub title: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BrowseResponse {
    pub path: String,
    pub entries: Vec<DirEntry>,
}

/// A non-fatal failure from one source participating in model-list composition (§8.3).
///
/// `source` identifies either a configured provider (`provider:<id>`) or the project harness
/// (`harness:<kind>`), so the browser can report degraded discovery without conflating the two.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelListingWarning {
    pub source: String,
    pub message: String,
}

/// Static model list for the model picker (spec §8.3).
#[derive(Debug, Clone, Serialize)]
pub struct ListModelsResponse {
    pub models: Vec<ModelDescriptor>,
    /// Non-fatal provider or harness listing failures (empty for the static listing).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ModelListingWarning>,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpCapabilitiesResponse {
    pub status: bool,
    pub reload: bool,
    pub oauth_login: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListMcpServersResponse {
    pub servers: Vec<McpServerStatus>,
    pub capabilities: McpCapabilitiesResponse,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartMcpOauthLoginRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReloadMcpServersResponse {
    pub ok: bool,
}

/// Token dashboard report for a project or the global scope (spec §10.2). All figures reuse the
/// one [`TokenUsage`] struct (B3); the day/week/month windows are derived from `by_day` on read.
#[derive(Debug, Clone, Serialize)]
pub struct TokenReport {
    pub total: TokenUsage,
    pub today: TokenUsage,
    pub this_week: TokenUsage,
    pub this_month: TokenUsage,
    pub by_day: std::collections::BTreeMap<String, TokenUsage>,
    pub by_model: ByModel,
    /// Estimated spend in euros, present only when cost estimation is enabled (§10.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost_eur: Option<f64>,
}

/// Result of a "Save plan to project" action (spec §7.4.1).
#[derive(Debug, Clone, Serialize)]
pub struct SavePlanResponse {
    /// Path the plan markdown was written to (relative to the project dir when possible).
    pub path: String,
}

/// Syntax-highlighted file content (spec §11.2).
///
/// The overlay displays the file's path, size, and language alongside the
/// highlighted HTML. When `is_binary` is true or the file exceeds the size
/// threshold, `html` is empty and the UI shows a fallback message.
#[derive(Debug, Clone, Serialize)]
pub struct HighlightResponse {
    /// Syntax-highlighted HTML (empty for binary or oversized files).
    pub html: String,
    /// Detected language name (e.g. "Rust", "Python").
    pub language: Option<String>,
    /// True if the file contains null bytes (§11.3 binary detection).
    pub is_binary: bool,
    /// Total number of lines in the file (before range slicing).
    pub total_lines: usize,
    /// File size in bytes (spec §11.2: overlay shows path, size, and language).
    pub file_size: u64,
}

/// A linkified span within agent text (spec §11.2).
#[derive(Debug, Clone, Serialize)]
pub struct LinkSpanResponse {
    pub start: usize,
    pub end: usize,
    pub path: String,
    /// Optional 1-based source line parsed from `path#<line>`, `path:<line>`,
    /// or `path:<line>:<column>` suffixes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
}

/// Result of path linkification (spec §11.2).
#[derive(Debug, Clone, Serialize)]
pub struct LinkifyResponse {
    pub links: Vec<LinkSpanResponse>,
}

/// Request body for linkification (spec §11.2).
#[derive(Debug, Clone, Deserialize)]
pub struct LinkifyRequest {
    pub text: String,
}

/// Request body for Markdown rendering of agent text (spec §11.2).
#[derive(Debug, Clone, Deserialize)]
pub struct RenderRequest {
    pub text: String,
}

/// Result of rendering agent Markdown to sanitized HTML with embedded path links.
#[derive(Debug, Clone, Serialize)]
pub struct RenderResponse {
    /// Sanitized HTML: agent-authored raw HTML is escaped, link URLs are scheme-checked, and
    /// detected workspace paths are wrapped in `.path-link` buttons the client wires up.
    pub html: String,
}

/// Request to create a directory under `parent` (filesystem picker "New folder"). `name` is a
/// single path segment; the server rejects separators and `.`/`..` and enforces the browse roots.
#[derive(Debug, Clone, Deserialize)]
pub struct MkdirRequest {
    pub parent: String,
    pub name: String,
}

/// The canonical path of the directory created via [`MkdirRequest`].
#[derive(Debug, Clone, Serialize)]
pub struct MkdirResponse {
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_message_send_input_serde() {
        let msg = ClientMessage::SendInput {
            thread_id: ThreadId::new(),
            text: "Refactor the auth module".into(),
            attachments: Vec::new(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"send_input\""));
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        match back {
            ClientMessage::SendInput { text, .. } => assert_eq!(text, "Refactor the auth module"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_send_input_serde_with_attachments() {
        let msg = ClientMessage::SendInput {
            thread_id: ThreadId::new(),
            text: "Inspect this".into(),
            attachments: vec![UserAttachment {
                name: "diagram.png".into(),
                mime_type: "image/png".into(),
                size: 12,
                kind: AttachmentKind::Image,
                data_base64: "aW1hZ2U=".into(),
            }],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"data_base64\":\"aW1hZ2U=\""));
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        match back {
            ClientMessage::SendInput {
                text, attachments, ..
            } => {
                assert_eq!(text, "Inspect this");
                assert_eq!(attachments.len(), 1);
                assert_eq!(attachments[0].kind, AttachmentKind::Image);
                assert_eq!(attachments[0].data_base64, "aW1hZ2U=");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn open_thread_request_rejects_client_asserted_subagent_metadata() {
        let result = serde_json::from_value::<OpenThreadRequest>(serde_json::json!({
            "thread_id": null,
            "resume": "native-child",
            "subagent_action": "spawned",
            "subagent_status": "completed",
            "subagent_message": "done"
        }));
        assert!(result.is_err());

        let request: OpenThreadRequest = serde_json::from_value(serde_json::json!({
            "thread_id": null,
            "resume": "native-child"
        }))
        .unwrap();
        assert_eq!(request.thread_id, None);
        assert_eq!(request.resume.as_deref(), Some("native-child"));
    }

    #[test]
    fn client_message_terminate_command_serde() {
        let tid = ThreadId::new();
        let msg = ClientMessage::TerminateCommand {
            thread_id: tid,
            process_id: "proc_1".into(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "terminate_command");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["process_id"], "proc_1");
        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::TerminateCommand { process_id, .. } => {
                assert_eq!(process_id, "proc_1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_compact_context_serde() {
        let tid = ThreadId::new();
        let msg = ClientMessage::CompactContext { thread_id: tid };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "compact_context");
        assert_eq!(json["thread_id"], tid.to_string());

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::CompactContext { thread_id } => assert_eq!(thread_id, tid),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_messages_for_runtime_recovery_are_turn_scoped() {
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let messages = [
            (
                ClientMessage::RetryTurnPersistence { thread_id, turn_id },
                "retry_turn_persistence",
            ),
            (
                ClientMessage::DiscardUnpersistedTurn { thread_id, turn_id },
                "discard_unpersisted_turn",
            ),
        ];
        for (message, kind) in messages {
            let value = serde_json::to_value(message).unwrap();
            assert_eq!(value["type"], kind);
            assert_eq!(value["thread_id"], thread_id.to_string());
            assert_eq!(value["turn_id"], turn_id.to_string());
        }
    }

    #[test]
    fn history_amendment_recovery_is_identified_independently_of_turn_activity() {
        let thread_id = ThreadId::new();
        let recovery_id = "history-7".to_string();
        let messages = [
            (
                ClientMessage::RetryHistoryAmendment {
                    thread_id,
                    recovery_id: recovery_id.clone(),
                },
                "retry_history_amendment",
            ),
            (
                ClientMessage::DiscardHistoryAmendment {
                    thread_id,
                    recovery_id: recovery_id.clone(),
                },
                "discard_history_amendment",
            ),
        ];
        for (message, kind) in messages {
            let value = serde_json::to_value(message).unwrap();
            assert_eq!(value["type"], kind);
            assert_eq!(value["thread_id"], thread_id.to_string());
            assert_eq!(value["recovery_id"], recovery_id);
            assert!(value.get("turn_id").is_none());
        }
    }

    #[test]
    fn client_message_set_permission_preset_is_thread_scoped() {
        let tid = ThreadId::new();
        let msg = ClientMessage::SetPermissionPreset {
            thread_id: tid,
            request_id: "metadata-1".into(),
            preset: PermissionPreset::AskFirst,
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "set_permission_preset");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["preset"], "ask_first");
        assert!(json.get("project_id").is_none());

        let mut missing_request_id = json.clone();
        missing_request_id
            .as_object_mut()
            .unwrap()
            .remove("request_id");
        assert!(serde_json::from_value::<ClientMessage>(missing_request_id).is_err());

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::SetPermissionPreset {
                thread_id,
                request_id,
                preset,
            } => {
                assert_eq!(thread_id, tid);
                assert_eq!(request_id, "metadata-1");
                assert_eq!(preset, PermissionPreset::AskFirst);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn live_apis_reject_legacy_permission_names() {
        let thread_id = ThreadId::new();
        let legacy_ws = serde_json::json!({
            "type": "set_permission_preset",
            "thread_id": thread_id,
            "preset": "auto"
        });
        assert!(serde_json::from_value::<ClientMessage>(legacy_ws).is_err());

        let legacy_http = serde_json::json!({
            "text": "hello",
            "model_ref": {
                "provider": "openai",
                "model": "gpt-5.5"
            },
            "mode": "build",
            "approval_policy": "ask"
        });
        assert!(serde_json::from_value::<StartThreadRequest>(legacy_http).is_err());
    }

    #[test]
    fn client_message_server_request_response_serde() {
        let thread_id = ThreadId::new();
        let msg = ClientMessage::ServerRequestResponse {
            thread_id,
            request_id: "req_1".into(),
            response: ServerRequestResponse::result(serde_json::json!({
                "success": true,
                "contentItems": [],
            })),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "server_request_response");
        assert_eq!(json["thread_id"], thread_id.to_string());
        assert_eq!(json["request_id"], "req_1");
        assert_eq!(json["response"]["kind"], "result");
        assert_eq!(json["response"]["value"]["success"], true);

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::ServerRequestResponse {
                thread_id: decoded_thread_id,
                request_id,
                response: ServerRequestResponse::Result { value },
            } => {
                assert_eq!(decoded_thread_id, thread_id);
                assert_eq!(request_id, "req_1");
                assert_eq!(value["contentItems"], serde_json::json!([]));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_server_request_error_response_serde() {
        let thread_id = ThreadId::new();
        let msg = ClientMessage::ServerRequestResponse {
            thread_id,
            request_id: "req_1".into(),
            response: ServerRequestResponse::error(-32000, "unsupported"),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "server_request_response");
        assert_eq!(json["request_id"], "req_1");
        assert_eq!(json["response"]["kind"], "error");
        assert_eq!(json["response"]["code"], -32000);
        assert_eq!(json["response"]["message"], "unsupported");

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::ServerRequestResponse {
                thread_id: decoded_thread_id,
                request_id,
                response: ServerRequestResponse::Error { code, message },
            } => {
                assert_eq!(decoded_thread_id, thread_id);
                assert_eq!(request_id, "req_1");
                assert_eq!(code, -32000);
                assert_eq!(message, "unsupported");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_ping() {
        let json = serde_json::to_string(&ClientMessage::Ping).unwrap();
        assert_eq!(json, "{\"type\":\"ping\"}");
    }

    #[test]
    fn server_message_pong() {
        let json = serde_json::to_string(&ServerMessage::Pong).unwrap();
        assert_eq!(json, "{\"type\":\"pong\"}");
    }

    #[test]
    fn thread_catalog_invalidation_is_one_global_signal() {
        let json = serde_json::to_string(&ServerMessage::ThreadCatalogChanged).unwrap();
        assert_eq!(json, "{\"type\":\"thread_catalog_changed\"}");
        assert!(matches!(
            serde_json::from_str::<ServerMessage>(&json).unwrap(),
            ServerMessage::ThreadCatalogChanged
        ));
    }

    #[test]
    fn server_message_thread_metadata_is_a_typed_revisioned_projection() {
        let tid = ThreadId::new();
        let msg = ServerMessage::ThreadMetadata(ThreadMetadata {
            thread_id: tid,
            revision: 7,
            title: "Typed state".into(),
            mode: Mode::Build,
            current_model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
            context_window: 258_400,
            permission_preset: PermissionPreset::AskFirst,
            tokens: TokenLedger::default(),
        });

        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "thread_metadata");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["revision"], 7);
        assert_eq!(json["context_window"], 258_400);

        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::ThreadMetadata(metadata) => {
                assert_eq!(metadata.thread_id, tid);
                assert_eq!(metadata.revision, 7);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn metadata_action_result_is_correlated_and_authoritative() {
        let tid = ThreadId::new();
        let msg = ServerMessage::ThreadMetadataResult {
            request_id: "metadata-7".into(),
            metadata: ThreadMetadata {
                thread_id: tid,
                revision: 7,
                title: "Committed state".into(),
                mode: Mode::Plan,
                current_model: ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                },
                context_window: 258_400,
                permission_preset: PermissionPreset::AskFirst,
                tokens: TokenLedger::default(),
            },
        };

        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "thread_metadata_result");
        assert_eq!(json["request_id"], "metadata-7");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["revision"], 7);

        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::ThreadMetadataResult {
                request_id,
                metadata,
            } => {
                assert_eq!(request_id, "metadata-7");
                assert_eq!(metadata.thread_id, tid);
                assert_eq!(metadata.revision, 7);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn runtime_overview_is_an_authoritative_replacement() {
        let tid = ThreadId::new();
        let msg = ServerMessage::ThreadRuntimeOverview(ThreadRuntimeOverview {
            revision: 9,
            threads: vec![ThreadRuntimeSummary {
                thread_id: tid,
                turn_state: RuntimeTurnState::Active { turn_id: None },
                history_recovery: None,
                outstanding_requests: vec![OutstandingRequest {
                    request_id: "approval-1".into(),
                    kind: RequestKind::Approval,
                    responding: false,
                }],
            }],
        });

        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "thread_runtime_overview");
        assert_eq!(json["revision"], 9);
        assert_eq!(json["threads"][0]["thread_id"], tid.to_string());
        assert_eq!(json["threads"][0]["turn_state"]["state"], "active");
        assert_eq!(
            json["threads"][0]["outstanding_requests"][0]["kind"],
            "approval"
        );

        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::ThreadRuntimeOverview(overview) => {
                assert_eq!(overview.revision, 9);
                assert_eq!(overview.threads[0].thread_id, tid);
                assert_eq!(overview.threads[0].outstanding_requests.len(), 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn runtime_overview_requires_request_identity() {
        let json = serde_json::json!({
            "type": "thread_runtime_overview",
            "revision": 1,
            "threads": [{
                "thread_id": ThreadId::new().to_string(),
                "turn_state": { "state": "active", "turn_id": null },
                "outstanding_requests": [{ "kind": "approval", "responding": false }]
            }]
        });

        let err = serde_json::from_value::<ServerMessage>(json).unwrap_err();
        assert!(
            err.to_string().contains("missing field `request_id`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn ordered_request_state_is_thread_scoped_and_resolved() {
        let tid = ThreadId::new();
        let request = RequestState {
            thread_id: tid,
            request_id: "approval-1".into(),
            payload: RequestPayload::Approval {
                request: WireApprovalRequest {
                    id: ApprovalId("approval-1".into()),
                    kind: WireApprovalKind::Permission {
                        detail: "run command".into(),
                    },
                    reason: None,
                    metadata: Vec::new(),
                    available: Vec::new(),
                },
            },
            status: RequestStatus::Resolved {
                resolution: RequestResolution::Approval {
                    decision: ApprovalDecision::Accept,
                },
            },
        };
        let msg = ServerMessage::ThreadEvent {
            thread_id: tid,
            subscription_generation: 4,
            event: SequencedThreadEvent {
                seq: 12,
                event: ThreadEventPayload::Request {
                    request: Box::new(request),
                },
                history_amendment: None,
            },
        };

        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "thread_event");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["subscription_generation"], 4);
        assert_eq!(json["seq"], 12);
        assert_eq!(json["event"]["kind"], "request");
        assert_eq!(json["event"]["request"]["request_id"], "approval-1");
        assert_eq!(json["event"]["request"]["status"]["status"], "resolved");

        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::ThreadEvent {
                thread_id, event, ..
            } => {
                assert_eq!(thread_id, tid);
                assert_eq!(event.seq, 12);
                assert!(matches!(event.event, ThreadEventPayload::Request { .. }));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn server_message_thread_tasks_is_revisioned() {
        let tid = ThreadId::new();
        let turn_id = TurnId::new();
        let item_id = ItemId::new();
        let tool_item = ItemId::new();
        let msg = ServerMessage::ThreadTasks(ThreadTaskSnapshot {
            thread_id: tid,
            revision: 6,
            tasks: vec![
                RunningTask {
                    kind: TaskKind::Command,
                    thread_id: tid,
                    turn_id,
                    item_id,
                    harness_item_id: "cmd1".into(),
                    command: "sleep 60".into(),
                    cwd: "/tmp/project".into(),
                    server: None,
                    status: "in_progress".into(),
                    process_id: Some("proc_1".into()),
                    started_at_ms: 1_785_000_000_000,
                    output: "waiting".into(),
                    after_turn: true,
                    terminating: true,
                },
                RunningTask {
                    kind: TaskKind::Tool,
                    thread_id: tid,
                    turn_id,
                    item_id: tool_item,
                    harness_item_id: "tool1".into(),
                    command: "search".into(),
                    cwd: String::new(),
                    server: Some("wiki".into()),
                    status: "in_progress".into(),
                    process_id: None,
                    started_at_ms: 1_785_000_000_500,
                    output: String::new(),
                    after_turn: false,
                    terminating: false,
                },
            ],
        });
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "thread_tasks");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["revision"], 6);
        assert_eq!(json["tasks"][0]["kind"], "command");
        assert_eq!(json["tasks"][0]["process_id"], "proc_1");
        assert_eq!(json["tasks"][0]["after_turn"], true);
        assert_eq!(json["tasks"][1]["kind"], "tool");
        assert_eq!(json["tasks"][1]["server"], "wiki");
        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::ThreadTasks(snapshot) => {
                assert_eq!(snapshot.revision, 6);
                assert_eq!(snapshot.tasks[0].item_id, item_id);
                assert_eq!(snapshot.tasks[0].kind, TaskKind::Command);
                assert!(snapshot.tasks[0].terminating);
                assert_eq!(snapshot.tasks[1].kind, TaskKind::Tool);
                assert_eq!(snapshot.tasks[1].server.as_deref(), Some("wiki"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn server_message_error_is_flattened() {
        let tid = ThreadId::new();
        let msg = ServerMessage::Error {
            error: ErrorInfo {
                code: "thread_not_found".into(),
                severity: ErrorSeverity::Error,
                message: "Thread not found.".into(),
                detail: Some("missing".into()),
                thread_id: Some(tid),
                action: Some("subscribe".into()),
                request_id: None,
                process_id: None,
            },
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "error");
        assert_eq!(json["code"], "thread_not_found");
        assert_eq!(json["message"], "Thread not found.");
        assert_eq!(json["thread_id"], tid.to_string());
        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::Error { error } => {
                assert_eq!(error.code, "thread_not_found");
                assert_eq!(error.action.as_deref(), Some("subscribe"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn thread_bootstrap_frames_use_one_chunked_transaction_shape() {
        let tid = ThreadId::new();
        let start = ServerMessage::ThreadBootstrap {
            thread_id: tid,
            subscription_generation: 3,
            frame: ThreadBootstrapFrame::Start {
                sections: vec![BootstrapSectionDescriptor {
                    section: BootstrapSection::History,
                    encoded_bytes: 11,
                    chunk_count: 2,
                }],
            },
        };
        let chunk = ServerMessage::ThreadBootstrap {
            thread_id: tid,
            subscription_generation: 3,
            frame: ThreadBootstrapFrame::Chunk {
                section: BootstrapSection::History,
                index: 1,
                payload_base64: "d29ybGQ=".into(),
            },
        };
        let commit = ServerMessage::ThreadBootstrap {
            thread_id: tid,
            subscription_generation: 3,
            frame: ThreadBootstrapFrame::Commit,
        };

        let start_json = serde_json::to_value(&start).unwrap();
        assert_eq!(start_json["type"], "thread_bootstrap");
        assert_eq!(start_json["frame"]["phase"], "start");
        assert_eq!(start_json["frame"]["sections"][0]["encoded_bytes"], 11);
        assert_eq!(start_json["frame"]["sections"][0]["chunk_count"], 2);

        let chunk_json = serde_json::to_value(&chunk).unwrap();
        assert_eq!(chunk_json["frame"]["phase"], "chunk");
        assert_eq!(chunk_json["frame"]["index"], 1);
        assert_eq!(chunk_json["frame"]["payload_base64"], "d29ybGQ=");

        let commit_json = serde_json::to_value(&commit).unwrap();
        assert_eq!(commit_json["frame"]["phase"], "commit");

        for message in [start_json, chunk_json, commit_json] {
            let decoded: ServerMessage = serde_json::from_value(message).unwrap();
            assert!(matches!(decoded, ServerMessage::ThreadBootstrap { .. }));
        }
    }
}
