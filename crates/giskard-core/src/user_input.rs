use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAttachment {
    pub name: String,
    pub mime_type: String,
    pub size: u64,
    pub kind: AttachmentKind,
    #[serde(default)]
    pub data_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentKind {
    Image,
    File,
}

/// User input sent to the agent (spec §4.5).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserInput {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<UserAttachment>,
    },
}

impl Serialize for UserInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct PersistedAttachment<'a> {
            name: &'a str,
            mime_type: &'a str,
            size: u64,
            kind: &'a AttachmentKind,
        }

        #[derive(Serialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum PersistedUserInput<'a> {
            Text {
                text: &'a str,
                #[serde(skip_serializing_if = "Vec::is_empty")]
                attachments: Vec<PersistedAttachment<'a>>,
            },
        }

        match self {
            Self::Text { text, attachments } => PersistedUserInput::Text {
                text,
                attachments: attachments
                    .iter()
                    .map(|attachment| PersistedAttachment {
                        name: &attachment.name,
                        mime_type: &attachment.mime_type,
                        size: attachment.size,
                        kind: &attachment.kind,
                    })
                    .collect(),
            }
            .serialize(serializer),
        }
    }
}

impl UserInput {
    pub fn text(s: impl Into<String>) -> Self {
        Self::text_with_attachments(s, Vec::new())
    }

    pub fn text_with_attachments(s: impl Into<String>, attachments: Vec<UserAttachment>) -> Self {
        Self::Text {
            text: s.into(),
            attachments,
        }
    }

    /// Returns the text content if this is a `Text` variant.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text { text, .. } => Some(text),
        }
    }

    pub fn attachments(&self) -> &[UserAttachment] {
        match self {
            Self::Text { attachments, .. } => attachments,
        }
    }

    pub fn without_attachment_data(&self) -> Self {
        match self {
            Self::Text { text, attachments } => Self::Text {
                text: text.clone(),
                attachments: attachments
                    .iter()
                    .map(|attachment| UserAttachment {
                        name: attachment.name.clone(),
                        mime_type: attachment.mime_type.clone(),
                        size: attachment.size,
                        kind: attachment.kind.clone(),
                        data_base64: String::new(),
                    })
                    .collect(),
            },
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
        let input = UserInput::text_with_attachments(
            "Refactor the auth module",
            vec![UserAttachment {
                name: "diagram.png".into(),
                mime_type: "image/png".into(),
                size: 12,
                kind: AttachmentKind::Image,
                data_base64: "aW1hZ2U=".into(),
            }],
        );
        let json = serde_json::to_string(&input).unwrap();
        assert!(!json.contains("aW1hZ2U="));
        let back: UserInput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.as_text(), input.as_text());
        assert_eq!(back.attachments().len(), 1);
        assert_eq!(back.attachments()[0].data_base64, "");
    }

    #[test]
    fn attachment_serde_includes_transient_bytes_for_wire_messages() {
        let attachment = UserAttachment {
            name: "diagram.png".into(),
            mime_type: "image/png".into(),
            size: 5,
            kind: AttachmentKind::Image,
            data_base64: "aW1hZ2U=".into(),
        };

        let json = serde_json::to_string(&attachment).unwrap();
        assert!(json.contains("aW1hZ2U="));
        assert_eq!(
            serde_json::from_str::<UserAttachment>(&json).unwrap(),
            attachment
        );
    }

    #[test]
    fn attachment_data_can_be_redacted_without_mutating_the_input() {
        let input = UserInput::text_with_attachments(
            "Inspect this",
            vec![UserAttachment {
                name: "diagram.png".into(),
                mime_type: "image/png".into(),
                size: 5,
                kind: AttachmentKind::Image,
                data_base64: "aW1hZ2U=".into(),
            }],
        );

        let redacted = input.without_attachment_data();
        assert_eq!(input.attachments()[0].data_base64, "aW1hZ2U=");
        assert!(redacted.attachments()[0].data_base64.is_empty());
    }

    #[test]
    fn old_user_input_text_deserializes_without_attachments() {
        let input: UserInput =
            serde_json::from_str(r#"{"type":"text","text":"Refactor the auth module"}"#).unwrap();
        assert_eq!(input.as_text(), Some("Refactor the auth module"));
        assert!(input.attachments().is_empty());
    }
}
