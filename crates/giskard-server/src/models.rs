//! Model descriptor resolution & listing (spec §8.3).
//!
//! Applies the metadata-source precedence for a `(provider, model)` pair:
//! 1. the typed `[[providers.models]]` config entry;
//! 2. (`/v1/models` dynamic listing — Phase 5, not resolved here);
//! 3. the built-in defaults table in `giskard-core`;
//! 4. a conservative fallback (`context_window = 128000`, no reasoning effort).

use giskard_core::model::{ModelDescriptor, ModelRef, default_descriptor};
use giskard_persist::Config;

/// Build a `ModelDescriptor` from a typed config entry, if the provider + model are declared.
fn from_config(config: &Config, provider: &str, model: &str) -> Option<ModelDescriptor> {
    let p = config.providers.iter().find(|p| p.id == provider)?;
    let m = p.models.iter().find(|m| m.id == model)?;
    Some(ModelDescriptor {
        provider: provider.to_string(),
        model: model.to_string(),
        context_window: m.context_window,
        supports_reasoning_effort: m.supports_reasoning_effort,
        display_name: m.display_name.clone(),
    })
}

/// Resolve the descriptor for a model, following the §8.3 precedence.
pub fn resolve_descriptor(config: &Config, model: &ModelRef) -> ModelDescriptor {
    from_config(config, &model.provider, &model.model)
        .or_else(|| default_descriptor(&model.provider, &model.model))
        .unwrap_or_else(|| ModelDescriptor::conservative(&model.provider, &model.model))
}

/// The context window (gauge denominator, §10.3) for a model, per the §8.3 precedence.
pub fn context_window_for(config: &Config, model: &ModelRef) -> u32 {
    resolve_descriptor(config, model).context_window
}

/// The full static model list offered by the model picker (§8.3): every declared
/// `[[providers.models]]` entry, resolved to a `ModelDescriptor`.
pub fn list_descriptors(config: &Config) -> Vec<ModelDescriptor> {
    let mut out = Vec::new();
    for p in &config.providers {
        for m in &p.models {
            out.push(ModelDescriptor {
                provider: p.id.clone(),
                model: m.id.clone(),
                context_window: m.context_window,
                supports_reasoning_effort: m.supports_reasoning_effort,
                display_name: m.display_name.clone(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_glm() -> Config {
        let toml = r#"
[[providers]]
id = "cloudflare-litellm"
name = "Cloudflare Workers AI (via LiteLLM)"
wire_api = "responses"
model_listing = true
  [[providers.models]]
  id = "@cf/z-ai/glm-4.7"
  display_name = "GLM-4.7"
  context_window = 131072
  supports_reasoning_effort = false
"#;
        toml::from_str(toml).unwrap()
    }

    #[test]
    fn resolves_from_config_first() {
        let config = config_with_glm();
        let m = ModelRef {
            provider: "cloudflare-litellm".into(),
            model: "@cf/z-ai/glm-4.7".into(),
            reasoning_effort: None,
        };
        let d = resolve_descriptor(&config, &m);
        assert_eq!(d.context_window, 131_072);
        assert!(!d.supports_reasoning_effort);
    }

    #[test]
    fn falls_back_to_defaults_table() {
        let config = Config::default();
        let m = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        assert_eq!(context_window_for(&config, &m), 262_144);
    }

    #[test]
    fn conservative_when_unknown() {
        let config = Config::default();
        let m = ModelRef {
            provider: "acme".into(),
            model: "mystery-1".into(),
            reasoning_effort: None,
        };
        let d = resolve_descriptor(&config, &m);
        assert_eq!(
            d.context_window,
            ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW
        );
    }

    #[test]
    fn lists_declared_models() {
        let config = config_with_glm();
        let list = list_descriptors(&config);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].model, "@cf/z-ai/glm-4.7");
    }
}
