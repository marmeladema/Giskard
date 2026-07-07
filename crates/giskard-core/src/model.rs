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
///
/// S4: mirrors the pinned Codex `ModelReasoningEffort` (verified against codex-codes 0.143.0:
/// minimal | low | medium | high | xhigh). Not hardcoded to a smaller set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Minimal,
    Low,
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

impl ModelDescriptor {
    /// Conservative context window used when the model's size is unknown (spec §8.3 step 4).
    pub const CONSERVATIVE_CONTEXT_WINDOW: u32 = 128_000;

    /// A conservative fallback descriptor for an otherwise-unknown model (spec §8.3 step 4).
    ///
    /// The UI shows a "context size unknown — using default" badge for such a descriptor
    /// (which is any descriptor whose `context_window == CONSERVATIVE_CONTEXT_WINDOW` produced by
    /// this constructor); the gauge still renders and the model is still usable.
    pub fn conservative(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            context_window: Self::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: false,
            display_name: None,
        }
    }
}

/// Built-in defaults table keyed by well-known model ids (spec §8.3 step 3).
///
/// This is the third precedence source (after a typed config entry and a `/v1/models` response),
/// before the conservative fallback. Returns `None` for unknown models so the caller can fall
/// back. Provider is preserved on the returned descriptor since the same model id may be served by
/// several providers (§8.1).
pub fn default_descriptor(provider: &str, model: &str) -> Option<ModelDescriptor> {
    let (context_window, supports_reasoning_effort, display_name) = match model {
        "gpt-5.5" => (262_144, true, "GPT-5.5"),
        "gpt-5.4" => (262_144, true, "GPT-5.4"),
        "@cf/z-ai/glm-4.7" => (131_072, false, "GLM-4.7 (Workers AI)"),
        _ => return None,
    };
    Some(ModelDescriptor {
        provider: provider.to_string(),
        model: model.to_string(),
        context_window,
        supports_reasoning_effort,
        display_name: Some(display_name.to_string()),
    })
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
    fn default_descriptor_known_and_unknown() {
        let d = default_descriptor("openai", "gpt-5.5").unwrap();
        assert_eq!(d.context_window, 262_144);
        assert!(d.supports_reasoning_effort);
        assert!(default_descriptor("openai", "totally-unknown").is_none());
    }

    #[test]
    fn conservative_fallback() {
        let d = ModelDescriptor::conservative("acme", "mystery-1");
        assert_eq!(
            d.context_window,
            ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW
        );
        assert!(!d.supports_reasoning_effort);
        assert_eq!(d.provider, "acme");
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
