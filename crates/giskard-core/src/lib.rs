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
pub mod model;
pub mod token;
pub mod turn;
pub mod user_input;

pub use approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
pub use diff::{DiffHunk, DiffLine, FileDiff};
pub use error::{GiskardError, HarnessError, PersistError};
pub use event::AgentEvent;
pub use ids::{ApprovalId, ItemId, ProjectId, ThreadId, TurnId};
pub use item::{FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
pub use model::{Effort, ModelDescriptor, ModelRef, default_descriptor};
pub use token::{ByModel, DailyTokenLedger, TokenLedger, TokenUsage};
pub use turn::{ApprovalPolicy, Mode, Turn, TurnOverrides, TurnStatus, TurnStatusKind};
pub use user_input::UserInput;
