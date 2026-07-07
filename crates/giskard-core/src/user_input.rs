use serde::{Deserialize, Serialize};

/// User input sent to the agent (spec §4.5).
///
/// Note: image/file attachments are NOT in v1 scope. If added later, extend here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserInput {
    Text { text: String },
}

impl UserInput {
    pub fn text(s: impl Into<String>) -> Self {
        Self::Text { text: s.into() }
    }

    /// Returns the text content if this is a `Text` variant.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text { text } => Some(text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_input_text() {
        let input = UserInput::text("Hello");
        assert_eq!(input.as_text(), Some("Hello"));
    }

    #[test]
    fn user_input_serde() {
        let input = UserInput::text("Refactor the auth module");
        let json = serde_json::to_string(&input).unwrap();
        let back: UserInput = serde_json::from_str(&json).unwrap();
        assert_eq!(input, back);
    }
}
