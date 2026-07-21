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

/// A reasoning-effort level (model-dependent).
///
/// Reasoning efforts are open-ended and model-specific — the set a model accepts comes from the
/// harness's catalog (Codex's own `ReasoningEffort` is likewise a bare string), not a fixed list.
/// Giskard is a pass-through: it never branches on the value, it just carries the user's selection to
/// the harness. So this is a transparent string newtype, not a closed enum. Common values are
/// `minimal | low | medium | high | xhigh`, but any string a model advertises is valid.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Effort(pub String);

impl Effort {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Effort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
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
    /// The exact reasoning-effort levels this model advertises (e.g. from Codex's `model/list`),
    /// used to populate the effort selector. Empty means "unknown" — the UI falls back to the
    /// default effort set when `supports_reasoning_effort` is true.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasoning_efforts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

impl ModelRef {
    /// Returns the composite key "provider/model" used by the per-thread effort-retention map.
    pub fn key(&self) -> String {
        format!("{}/{}", self.provider, self.model)
    }
}

impl ModelDescriptor {
    /// Conservative context window used when the model's size is unknown (spec §8.3 step 3).
    pub const CONSERVATIVE_CONTEXT_WINDOW: u32 = 128_000;

    /// A conservative fallback descriptor for an otherwise-unknown model (spec §8.3 step 3).
    /// A harness-reported runtime window replaces this fallback when one becomes available.
    pub fn conservative(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            context_window: Self::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: false,
            reasoning_efforts: Vec::new(),
            display_name: None,
        }
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
        // Serializes transparently as its string, so persisted/wire values are unchanged from the
        // old enum ("xhigh"), and any model-defined value round-trips.
        let e = Effort::new("xhigh");
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(json, "\"xhigh\"");
        let back: Effort = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);

        // A value outside the old closed set is now valid and round-trips.
        let custom: Effort = serde_json::from_str("\"ultra\"").unwrap();
        assert_eq!(custom, Effort::new("ultra"));
        assert_eq!(custom.as_str(), "ultra");
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
    fn model_descriptor_reasoning_efforts_serde() {
        // Missing field deserializes to an empty vec (serde default), for older payloads.
        let missing: ModelDescriptor = serde_json::from_str(
            r#"{"provider":"p","model":"m","context_window":1000,"supports_reasoning_effort":false}"#,
        )
        .unwrap();
        assert!(missing.reasoning_efforts.is_empty());

        // An explicit empty array also deserializes to empty, and is omitted on serialize.
        let empty: ModelDescriptor = serde_json::from_str(
            r#"{"provider":"p","model":"m","context_window":1000,"supports_reasoning_effort":false,"reasoning_efforts":[]}"#,
        )
        .unwrap();
        assert!(empty.reasoning_efforts.is_empty());
        let json = serde_json::to_value(&empty).unwrap();
        assert!(
            json.get("reasoning_efforts").is_none(),
            "empty efforts are omitted (skip_serializing_if): {json}"
        );

        // A populated list serializes and round-trips.
        let mut d = ModelDescriptor::conservative("p", "m");
        d.reasoning_efforts = vec!["low".into(), "high".into()];
        let json = serde_json::to_value(&d).unwrap();
        assert_eq!(
            json["reasoning_efforts"],
            serde_json::json!(["low", "high"])
        );
        let back: ModelDescriptor = serde_json::from_value(json).unwrap();
        assert_eq!(back.reasoning_efforts, vec!["low", "high"]);

        // The conservative constructor initializes the field empty.
        assert!(
            ModelDescriptor::conservative("p", "m")
                .reasoning_efforts
                .is_empty()
        );
    }

    #[test]
    fn model_ref_serde_roundtrip() {
        let m = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: Some(Effort::new("high")),
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: ModelRef = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }
}
