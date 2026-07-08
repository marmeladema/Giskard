use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// ULID-backed project identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectId(pub ulid::Ulid);

/// ULID-backed thread identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ThreadId(pub ulid::Ulid);

/// ULID-backed turn identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TurnId(pub ulid::Ulid);

/// Giskard-owned item identifier (spec §4.5, B2).
///
/// ULID-backed and stable across resume. The harness-native item id lives in a separate
/// `harness_item_id` field on `Item`/`ItemStart`, so persistence, the diff viewer, and the code
/// overlay never depend on the stability of Codex item ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ItemId(pub ulid::Ulid);

/// Harness-native approval request identifier (opaque string; short-lived, not persisted).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApprovalId(pub String);

/// Harness-native server request identifier (opaque string; short-lived, not persisted).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServerRequestId(pub String);

// --- Display impls ---

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl fmt::Display for ThreadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl fmt::Display for TurnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl fmt::Display for ItemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl fmt::Display for ApprovalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl fmt::Display for ServerRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// --- Constructors ---

impl ProjectId {
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }
}
impl ThreadId {
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }
}
impl TurnId {
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }
}

impl FromStr for ProjectId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(ulid::Ulid::from_string(s)?))
    }
}
impl FromStr for ThreadId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(ulid::Ulid::from_string(s)?))
    }
}
impl FromStr for TurnId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(ulid::Ulid::from_string(s)?))
    }
}
impl ItemId {
    /// Mint a fresh Giskard-owned item id.
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }
}
impl FromStr for ItemId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(ulid::Ulid::from_string(s)?))
    }
}
impl ApprovalId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}
impl ServerRequestId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl Default for ProjectId {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for ThreadId {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for TurnId {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for ItemId {
    fn default() -> Self {
        Self::new()
    }
}
