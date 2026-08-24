//! Giskard core domain types.
//!
//! Harness-neutral types shared across the entire application. No I/O, no async —
//! pure data structures, fully unit-testable. All types live here per spec §3.2 / §4.5.

pub mod approval;
pub mod diff;
pub mod error;
pub mod event;
pub mod ids;
pub mod item;
pub mod mcp;
pub mod model;
pub mod server_request;
pub mod text;
pub mod thread;
pub mod token;
pub mod turn;
pub mod user_input;

pub use approval::{ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest};
pub use diff::{
    CapturedDiffContent, CapturedDiffDescriptor, CapturedDiffRecord, DiffContentKind, DiffHunk,
    DiffLine, FileDiff, capture_structured_diff, capture_unified_diff, captured_diff_id,
};
pub use error::{GiskardError, HarnessError, PersistError};
pub use event::AgentEvent;
pub use ids::{ApprovalId, DiffId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
pub use item::{
    CommandOutputDescriptor, FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload, ItemStart,
    NormalizedCommandOutput, SubagentAction, SubagentLink, SubagentStatus,
    command_output_logical_lines, command_output_tail_preview,
};
pub use mcp::{
    McpAuthStatus, McpOauthStart, McpResource, McpResourceTemplate, McpServerInfo, McpServerStatus,
    McpTool,
};
pub use model::{Effort, ModelDescriptor, ModelRef};
pub use server_request::{ServerRequest, ServerRequestResponse};
pub use thread::ThreadKind;
pub use token::{ByModel, DailyTokenLedger, TokenLedger, TokenUsage};
pub use turn::{Mode, PermissionPreset, Turn, TurnOverrides, TurnStatus, TurnStatusKind};
pub use user_input::{AttachmentKind, UserAttachment, UserInput};
