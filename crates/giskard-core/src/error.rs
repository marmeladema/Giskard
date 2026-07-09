use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ids::ThreadId;

/// Errors from the harness layer (spec §4.5).
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HarnessError {
    #[error("failed to start/locate harness binary: {0}")]
    Spawn(String),
    #[error("harness used before handshake completed")]
    NotInitialized,
    #[error("harness reports missing/invalid credentials")]
    Unauthenticated,
    #[error("transport error: {0}")]
    Transport(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("harness overloaded after retries exhausted")]
    Overloaded,
    #[error("capability not offered: {0}")]
    Unsupported(String),
    #[error("thread not found: {0}")]
    ThreadNotFound(ThreadId),
    #[error("thread already has an active turn: {thread}")]
    ThreadBusy { thread: ThreadId },
    #[error("operation timed out: {0}")]
    Timeout(String),
}

/// Errors from the persistence layer (spec §5).
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PersistError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialize(String),
    #[error("deserialization error: {0}")]
    Deserialize(String),
    #[error("file not found: {0}")]
    NotFound(String),
    #[error("corrupt file quarantined: {0}")]
    Corrupt(String),
    #[error("invalid data: {0}")]
    Invalid(String),
}

/// Top-level error type for the application.
#[derive(Debug, Clone, Error)]
pub enum GiskardError {
    #[error(transparent)]
    Harness(#[from] HarnessError),

    #[error(transparent)]
    Persist(#[from] PersistError),

    #[error("{0}")]
    Other(String),
}
