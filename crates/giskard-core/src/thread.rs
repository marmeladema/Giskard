use serde::{Deserialize, Serialize};

/// Durable thread origin/type metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadKind {
    #[default]
    Primary,
    Subagent,
}
