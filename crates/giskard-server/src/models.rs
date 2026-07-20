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
use giskard_proto::ProviderListingWarning;

/// Build a `ModelDescriptor` from a typed config entry, if the provider + model are declared.
fn from_config(config: &Config, provider: &str, model: &str) -> Option<ModelDescriptor> {
    let p = config.providers.iter().find(|p| p.id == provider)?;
    let m = p.models.iter().find(|m| m.id == model)?;
    Some(ModelDescriptor {
        provider: provider.to_string(),
        model: model.to_string(),
        context_window: m.context_window,
        supports_reasoning_effort: m.supports_reasoning_effort,
        reasoning_efforts: Vec::new(),
        display_name: m.display_name.clone(),
    })
}

/// Resolve the descriptor for a model, following the §8.3 precedence.
pub fn resolve_descriptor(config: &Config, model: &ModelRef) -> ModelDescriptor {
    from_config(config, &model.provider, &model.model)
        .or_else(|| default_descriptor(&model.provider, &model.model))
        .unwrap_or_else(|| ModelDescriptor::conservative(&model.provider, &model.model))
}

pub fn normalize_model_ref(config: &Config, model: &ModelRef) -> ModelRef {
    if config.providers.is_empty() || from_config(config, &model.provider, &model.model).is_some() {
        return model.clone();
    }

    let mut matches = config.providers.iter().filter_map(|provider| {
        provider
            .models
            .iter()
            .find(|candidate| candidate.id == model.model)
            .map(|candidate| (provider, candidate))
    });

    let Some((provider, candidate)) = matches.next() else {
        return model.clone();
    };
    if matches.next().is_some() {
        return model.clone();
    }

    let mut normalized = model.clone();
    normalized.provider = provider.id.clone();
    if !candidate.supports_reasoning_effort {
        normalized.reasoning_effort = None;
    }
    normalized
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
                reasoning_efforts: Vec::new(),
                display_name: m.display_name.clone(),
            });
        }
    }
    out
}

/// Overlay a harness model catalog's metadata onto a descriptor list, keyed by model id
/// (provider-independent — Codex's `model/list` carries no provider):
///
/// - **Display names** fill a descriptor whose `display_name` is unset, so an explicit
///   `[[providers.models]] display_name` always wins.
/// - **Reasoning efforts** (the exact levels a model advertises) are applied only to models the
///   config did **not** explicitly declare. A `[[providers.models]]` entry keeps its configured
///   effort setting; for discovery-only / built-in models the catalog is the source of truth.
///
/// The harness never supplies context window (Codex's `model/list` omits it).
pub fn apply_harness_metadata(
    mut base: Vec<ModelDescriptor>,
    harness_models: &[ModelDescriptor],
    config: &Config,
) -> Vec<ModelDescriptor> {
    use std::collections::{HashMap, HashSet};
    let by_id: HashMap<&str, &ModelDescriptor> = harness_models
        .iter()
        .map(|m| (m.model.as_str(), m))
        .collect();
    let declared: HashSet<&str> = config
        .providers
        .iter()
        .flat_map(|p| p.models.iter().map(|m| m.id.as_str()))
        .collect();
    for d in &mut base {
        let Some(h) = by_id.get(d.model.as_str()) else {
            continue;
        };
        if d.display_name.is_none() {
            d.display_name = h.display_name.clone();
        }
        if !declared.contains(d.model.as_str()) && !h.reasoning_efforts.is_empty() {
            d.supports_reasoning_effort = true;
            d.reasoning_efforts = h.reasoning_efforts.clone();
        }
    }
    base
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
/// endpoint errors, returns a non-success status, or sends unparseable JSON is skipped (its static
/// entries remain), so the call always returns at least the static list. Each such failure is
/// logged **and** returned as a [`ProviderListingWarning`] so it can be surfaced to the user rather
/// than silently yielding no models (e.g. a 401 from a proxy whose api_key is missing/wrong).
pub async fn refresh_models(
    config: &Config,
) -> (Vec<ModelDescriptor>, Vec<ProviderListingWarning>) {
    let base = list_descriptors(config);
    let mut warnings: Vec<ProviderListingWarning> = Vec::new();

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(%e, "cannot build HTTP client; returning static model list");
            return (base, warnings);
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
        // Attach the provider's discovery key (inline or from an env var) for endpoints that
        // require auth — e.g. a LiteLLM proxy with a master key returns 401 otherwise.
        let mut request = client.get(&url);
        if let Some(key) = p.resolve_api_key() {
            request = request.bearer_auth(key);
        }

        let mut fail = |message: String| {
            warn!(provider = %p.id, %url, %message, "model discovery failed; skipping provider");
            warnings.push(ProviderListingWarning {
                provider: p.id.clone(),
                message,
            });
        };

        match request.send().await {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    // A 401/403 almost always means the discovery api_key is missing or wrong.
                    let hint = if status.as_u16() == 401 || status.as_u16() == 403 {
                        " — check the provider's api_key / api_key_env"
                    } else {
                        ""
                    };
                    fail(format!("model listing returned HTTP {status}{hint}"));
                    continue;
                }
                match resp.json::<OpenAiModelsResponse>().await {
                    Ok(body) => {
                        for m in body.data {
                            dynamic.push((p.id.clone(), m.id));
                        }
                    }
                    Err(e) => fail(format!("unparseable /models response: {e}")),
                }
            }
            Err(e) => fail(format!("could not reach {url}: {e}")),
        }
    }

    (merge_models(base, &dynamic), warnings)
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

    #[test]
    fn harness_metadata_fills_names_and_efforts_by_model_id() {
        // config declares `@cf/z-ai/glm-4.7` (no efforts); `gpt-5.5` is not declared (discovered).
        let config = config_with_glm();
        let base = vec![
            ModelDescriptor {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                context_window: 262_144,
                supports_reasoning_effort: false,
                reasoning_efforts: Vec::new(),
                display_name: None,
            },
            ModelDescriptor {
                provider: "cloudflare-litellm".into(),
                model: "@cf/z-ai/glm-4.7".into(),
                context_window: 131_072,
                supports_reasoning_effort: false,
                reasoning_efforts: Vec::new(),
                display_name: Some("GLM-4.7".into()),
            },
        ];
        // Harness catalog is provider-agnostic (empty provider), keyed by model id.
        let harness = vec![
            ModelDescriptor {
                provider: String::new(),
                model: "gpt-5.5".into(),
                context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
                supports_reasoning_effort: true,
                reasoning_efforts: vec!["low".into(), "high".into()],
                display_name: Some("GPT-5.5".into()),
            },
            ModelDescriptor {
                provider: String::new(),
                model: "@cf/z-ai/glm-4.7".into(),
                context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
                supports_reasoning_effort: true,
                reasoning_efforts: vec!["medium".into()],
                display_name: Some("GLM 4.7".into()),
            },
        ];

        let merged = apply_harness_metadata(base, &harness, &config);

        // Not config-declared: name filled and the catalog's exact efforts applied.
        let gpt = merged.iter().find(|d| d.model == "gpt-5.5").unwrap();
        assert_eq!(gpt.display_name.as_deref(), Some("GPT-5.5"));
        assert!(gpt.supports_reasoning_effort);
        assert_eq!(gpt.reasoning_efforts, vec!["low", "high"]);
        // Names only: the harness never changes context window.
        assert_eq!(gpt.context_window, 262_144);

        // Config-declared: display_name already set stays (config wins), and its configured effort
        // setting is preserved — the catalog does not override a declared model's efforts.
        let glm = merged
            .iter()
            .find(|d| d.model == "@cf/z-ai/glm-4.7")
            .unwrap();
        assert_eq!(glm.display_name.as_deref(), Some("GLM-4.7"));
        assert!(!glm.supports_reasoning_effort);
        assert!(glm.reasoning_efforts.is_empty());
    }

    #[test]
    fn harness_metadata_precedence_matrix() {
        // config declares two models: one with a name, one without. Both have efforts off.
        let config: Config = toml::from_str(
            r#"
[[providers]]
id = "p"
name = "P"
wire_api = "responses"
  [[providers.models]]
  id = "declared-named"
  display_name = "Config Name"
  context_window = 1000
  supports_reasoning_effort = false
  [[providers.models]]
  id = "declared-noname"
  context_window = 1000
  supports_reasoning_effort = false
"#,
        )
        .unwrap();

        // Base descriptor with the given name/effort-support (no effort list, like config/discovery).
        let base_desc = |model: &str, name: Option<&str>, supports: bool| ModelDescriptor {
            provider: "p".into(),
            model: model.into(),
            context_window: 1000,
            supports_reasoning_effort: supports,
            reasoning_efforts: Vec::new(),
            display_name: name.map(str::to_string),
        };
        // Harness catalog entry (empty provider) with a name and effort list.
        let cat = |model: &str, name: &str, efforts: &[&str]| ModelDescriptor {
            provider: String::new(),
            model: model.into(),
            context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: !efforts.is_empty(),
            reasoning_efforts: efforts.iter().map(|e| (*e).to_string()).collect(),
            display_name: Some(name.into()),
        };

        let base = vec![
            base_desc("declared-named", Some("Config Name"), false),
            base_desc("declared-noname", None, false),
            base_desc("discovered-named", Some("Already Named"), false),
            base_desc("discovered-unnamed", None, false),
            base_desc("discovered-no-catalog-efforts", None, false),
            base_desc("unknown-to-harness", None, false),
        ];
        let harness = vec![
            cat("declared-named", "Catalog Name", &["low", "high"]),
            cat("declared-noname", "Catalog NoName", &["low"]),
            cat("discovered-named", "Catalog Named", &["medium"]),
            cat("discovered-unnamed", "Catalog Unnamed", &["high", "xhigh"]),
            cat("discovered-no-catalog-efforts", "Catalog NoEfforts", &[]),
            // no entry for "unknown-to-harness"
        ];

        let merged = apply_harness_metadata(base, &harness, &config);
        let get = |m: &str| merged.iter().find(|d| d.model == m).cloned().unwrap();

        // Declared + config name: name kept; efforts NOT overlaid (config wins for declared models).
        let d = get("declared-named");
        assert_eq!(d.display_name.as_deref(), Some("Config Name"));
        assert!(!d.supports_reasoning_effort);
        assert!(d.reasoning_efforts.is_empty());

        // Declared + no config name: name fills from catalog; efforts still NOT overlaid (declared).
        let d = get("declared-noname");
        assert_eq!(d.display_name.as_deref(), Some("Catalog NoName"));
        assert!(!d.supports_reasoning_effort);
        assert!(d.reasoning_efforts.is_empty());

        // Not declared + already named: name preserved (not overridden); efforts applied.
        let d = get("discovered-named");
        assert_eq!(d.display_name.as_deref(), Some("Already Named"));
        assert!(d.supports_reasoning_effort);
        assert_eq!(d.reasoning_efforts, vec!["medium"]);

        // Not declared + unnamed: name fills and the exact catalog efforts apply (flips support on).
        let d = get("discovered-unnamed");
        assert_eq!(d.display_name.as_deref(), Some("Catalog Unnamed"));
        assert!(d.supports_reasoning_effort);
        assert_eq!(d.reasoning_efforts, vec!["high", "xhigh"]);

        // Not declared, but the catalog lists no efforts for it: support stays off, list stays empty.
        let d = get("discovered-no-catalog-efforts");
        assert_eq!(d.display_name.as_deref(), Some("Catalog NoEfforts"));
        assert!(!d.supports_reasoning_effort);
        assert!(d.reasoning_efforts.is_empty());

        // In the list but unknown to the harness: left entirely unchanged.
        let d = get("unknown-to-harness");
        assert_eq!(d.display_name, None);
        assert!(!d.supports_reasoning_effort);
        assert!(d.reasoning_efforts.is_empty());
    }

    #[test]
    fn normalizes_stale_provider_when_model_has_one_configured_provider() {
        let config = config_with_glm();
        let normalized = normalize_model_ref(
            &config,
            &ModelRef {
                provider: "openai".into(),
                model: "@cf/z-ai/glm-4.7".into(),
                reasoning_effort: Some(giskard_core::model::Effort::new("high")),
            },
        );
        assert_eq!(normalized.provider, "cloudflare-litellm");
        assert_eq!(normalized.model, "@cf/z-ai/glm-4.7");
        assert_eq!(normalized.reasoning_effort, None);
    }

    #[test]
    fn does_not_normalize_ambiguous_model_provider() {
        let mut config = config_with_glm();
        config.providers.push(giskard_persist::ProviderConfig {
            id: "other".into(),
            name: "Other".into(),
            base_url: None,
            wire_api: "responses".into(),
            model_listing: false,
            api_key: None,
            api_key_env: None,
            models: vec![giskard_persist::ModelConfig {
                id: "@cf/z-ai/glm-4.7".into(),
                display_name: None,
                context_window: 131_072,
                supports_reasoning_effort: false,
            }],
        });
        let original = ModelRef {
            provider: "openai".into(),
            model: "@cf/z-ai/glm-4.7".into(),
            reasoning_effort: None,
        };
        assert_eq!(normalize_model_ref(&config, &original), original);
    }
}
