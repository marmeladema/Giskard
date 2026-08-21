//! Flat-file persistence with atomic writes (spec §5).

pub mod atomic;
pub mod config;
pub mod history;
pub mod layout;
pub mod lock;
mod migrate;
pub mod preview;
pub mod store;

pub use config::{Config, HarnessConfig, HistoryConfig, ModelConfig, ModelRate, ProviderConfig};
pub use giskard_core::PersistError;
pub use layout::ThreadLayout;
pub use lock::{DataDirLock, LOCK_FILE_NAME};
pub use migrate::MigrationOutcome;
pub use preview::{PROMPT_PREVIEW_MAX_BYTES, STATUS_MESSAGE_MAX_BYTES, bounded_preview};
pub use store::{
    HistoryCursor, HistorySnapshot, HistorySnapshotKind, ItemAmendmentOutcome, OrphanSweep,
    PersistStore, ProjectEntry, ProjectIndex,
};
