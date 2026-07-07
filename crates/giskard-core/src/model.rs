use serde::{Deserialize, Serialize};

/// A model identified by the pair (provider, model_id) plus optional reasoning effort.
///
/// The same model name on two providers is two distinct entries (spec §8.1).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelRef {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<Effort>,
}

/// Reasoning effort level (model-dependent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Medium,
    High,
    XHigh,
}

/// Metadata describing a model, used by the UI and context gauge (spec §8.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub provider: String,
    pub model: String,
    /// Token limit; drives the context gauge (§10.3).
    pub context_window: u32,
    /// Whether the effort selector is shown (§8.5).
    pub supports_reasoning_effort: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

impl ModelRef {
    /// Returns the composite key "provider/model" used in token ledgers.
    pub fn key(&self) -> String {
        format!("{}/{}", self.provider, self.model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_ref_key() {
        let m = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        assert_eq!(m.key(), "openai/gpt-5.5");
    }

    #[test]
    fn model_ref_equality_provider_significant() {
        let a = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        let b = ModelRef {
            provider: "cloudflare-litellm".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        assert_ne!(a, b, "same model on different providers must be distinct");
    }

    #[test]
    fn effort_serde() {
        let e = Effort::XHigh;
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(json, "\"xhigh\"");
        let back: Effort = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn model_ref_serde_roundtrip() {
        let m = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: Some(Effort::High),
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: ModelRef = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }
}
