//! Model descriptor resolution & listing (spec §8.3).
//!
//! Applies the metadata-source precedence for a `(provider, model)` pair:
//! 1. the typed `[[providers.models]]` config entry;
//! 2. the `/v1/models` dynamic listing (merged over the static list by [`refresh_models`]);
//! 3. the built-in defaults table in `giskard-core`;
//! 4. a conservative fallback (`context_window = 128000`, no reasoning effort).

use serde::Deserialize;
use tracing::warn;

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

// ---- Dynamic /v1/models refresh (spec §8.3) ----

/// OpenAI-compatible `GET /v1/models` response shape (`{ "data": [ { "id": "…" }, … ] }`).
#[derive(Deserialize)]
struct OpenAiModelsResponse {
    #[serde(default)]
    data: Vec<OpenAiModel>,
}

#[derive(Deserialize)]
struct OpenAiModel {
    id: String,
}

/// Merge dynamically-listed `(provider, model)` ids over a static descriptor list. Static/config
/// entries win (metadata precedence, §8.3); ids only present dynamically are added via the defaults
/// table or the conservative fallback. Result is deduped by `(provider, model)`.
pub fn merge_models(
    mut base: Vec<ModelDescriptor>,
    dynamic: &[(String, String)],
) -> Vec<ModelDescriptor> {
    use std::collections::BTreeSet;
    let mut seen: BTreeSet<(String, String)> = base
        .iter()
        .map(|d| (d.provider.clone(), d.model.clone()))
        .collect();
    for (provider, model) in dynamic {
        if seen.insert((provider.clone(), model.clone())) {
            base.push(
                default_descriptor(provider, model)
                    .unwrap_or_else(|| ModelDescriptor::conservative(provider, model)),
            );
        }
    }
    base
}

/// Refresh the model list by querying `GET {base_url}/models` for every provider that advertises
/// `model_listing`, merging the results over the static list (§8.3). Best-effort: a provider whose
/// endpoint errors or returns unparseable JSON is skipped (its static entries remain), so the call
/// always returns at least the static list.
pub async fn refresh_models(config: &Config) -> Vec<ModelDescriptor> {
    let base = list_descriptors(config);

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(%e, "cannot build HTTP client; returning static model list");
            return base;
        }
    };

    let mut dynamic: Vec<(String, String)> = Vec::new();
    for p in &config.providers {
        if !p.model_listing {
            continue;
        }
        let Some(base_url) = &p.base_url else {
            continue;
        };
        let url = format!("{}/models", base_url.trim_end_matches('/'));
        match client.get(&url).send().await {
            Ok(resp) => match resp.json::<OpenAiModelsResponse>().await {
                Ok(body) => {
                    for m in body.data {
                        dynamic.push((p.id.clone(), m.id));
                    }
                }
                Err(e) => warn!(%e, provider = %p.id, "unparseable /models response; skipping"),
            },
            Err(e) => warn!(%e, provider = %p.id, "failed to fetch /models; skipping"),
        }
    }

    merge_models(base, &dynamic)
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

    #[test]
    fn merge_keeps_static_and_adds_dynamic() {
        let base = list_descriptors(&config_with_glm()); // one static config entry
        let dynamic = vec![
            // Already present ⇒ must not duplicate or override the config metadata.
            ("cloudflare-litellm".into(), "@cf/z-ai/glm-4.7".into()),
            // New id ⇒ added (unknown ⇒ conservative descriptor).
            ("cloudflare-litellm".into(), "@cf/meta/llama-4".into()),
        ];
        let merged = merge_models(base, &dynamic);
        assert_eq!(merged.len(), 2);

        let glm = merged
            .iter()
            .find(|d| d.model == "@cf/z-ai/glm-4.7")
            .unwrap();
        assert_eq!(glm.context_window, 131_072, "config metadata preserved");

        let llama = merged
            .iter()
            .find(|d| d.model == "@cf/meta/llama-4")
            .unwrap();
        assert_eq!(
            llama.context_window,
            ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
            "dynamic-only id gets a conservative descriptor"
        );
    }
}
