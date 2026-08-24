//! Flat-file persistence with atomic writes (spec §5).

pub mod atomic;
pub mod command_output;
pub mod config;
pub mod history;
pub mod layout;
pub mod lock;
mod migrate;
pub mod preview;
pub mod store;

pub use command_output::{command_output_descriptor, normalize_command_output};
pub use config::{
    Config, HarnessConfig, HistoryConfig, ModelConfig, ModelRate, ProviderConfig, RetentionConfig,
};
pub use giskard_core::PersistError;
pub use layout::ThreadLayout;
pub use lock::{DataDirLock, LOCK_FILE_NAME};
pub use migrate::MigrationOutcome;
pub use preview::{
    COMMAND_OUTPUT_PREVIEW_MAX_BYTES, PROMPT_PREVIEW_MAX_BYTES, STATUS_MESSAGE_MAX_BYTES,
    bounded_head_tail, bounded_preview, bounded_tail_preview, bounded_tail_preview_for_original,
    logical_line_count,
};
pub use store::{OrphanSweep, PersistStore, ProjectEntry, ProjectIndex};
