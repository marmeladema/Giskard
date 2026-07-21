//! Model descriptor resolution & listing (spec §8.3).
//!
//! Applies the metadata-source precedence for a `(provider, model)` pair:
//! 1. the typed `[[providers.models]]` config entry;
//! 2. the `/v1/models` dynamic listing (merged over the static list by [`refresh_models`]);
//! 3. a conservative fallback (`context_window = 128000`, no reasoning effort).

use std::collections::HashMap;

use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::warn;

use giskard_core::ids::ProjectId;
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_persist::Config;
use giskard_proto::ModelListingWarning;

/// Last successfully composed model list for each project.
///
/// The browser and model-mutation routes must resolve against the same descriptors: otherwise a
/// catalog-only reasoning effort can be displayed by the picker and then discarded when a turn is
/// created. The project endpoint refreshes this store; mutation routes load it on demand when the
/// browser has not fetched the catalog yet.
#[derive(Default)]
pub struct ProjectModelCatalogStore {
    catalogs: RwLock<HashMap<ProjectId, Vec<ModelDescriptor>>>,
}

impl ProjectModelCatalogStore {
    pub async fn get(&self, project_id: ProjectId) -> Option<Vec<ModelDescriptor>> {
        self.catalogs.read().await.get(&project_id).cloned()
    }

    pub async fn replace(&self, project_id: ProjectId, models: Vec<ModelDescriptor>) {
        self.catalogs.write().await.insert(project_id, models);
    }

    pub async fn remove(&self, project_id: ProjectId) {
        self.catalogs.write().await.remove(&project_id);
    }
}

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
        .unwrap_or_else(|| ModelDescriptor::conservative(&model.provider, &model.model))
}

/// Resolve against a composed project catalog before falling back to static metadata.
pub fn resolve_catalog_descriptor(
    catalog: &[ModelDescriptor],
    config: &Config,
    model: &ModelRef,
) -> ModelDescriptor {
    from_config(config, &model.provider, &model.model)
        .or_else(|| {
            catalog
                .iter()
                .find(|descriptor| {
                    descriptor.provider == model.provider && descriptor.model == model.model
                })
                .cloned()
        })
        .unwrap_or_else(|| resolve_descriptor(config, model))
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

/// Prefer a harness-reported effective window retained for this exact model over catalog/config
/// metadata. Runtime values are the closest representation of what the harness actually enforces.
pub fn context_window_with_runtime(
    model: &ModelRef,
    descriptor: &ModelDescriptor,
    runtime_windows: &HashMap<String, HashMap<String, u32>>,
) -> u32 {
    runtime_windows
        .get(&model.provider)
        .and_then(|models| models.get(&model.model))
        .copied()
        .filter(|window| *window > 0)
        .unwrap_or(descriptor.context_window)
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
///   effort setting; for discovery-only models the catalog is the source of truth.
///
/// The harness never supplies context window (Codex's `model/list` omits it).
pub fn apply_harness_metadata(
    mut base: Vec<ModelDescriptor>,
    harness_models: &[ModelDescriptor],
    config: &Config,
) -> Vec<ModelDescriptor> {
    use std::collections::HashSet;
    let by_id: HashMap<&str, &ModelDescriptor> = harness_models
        .iter()
        .map(|m| (m.model.as_str(), m))
        .collect();
    let declared: HashSet<(&str, &str)> = config
        .providers
        .iter()
        .flat_map(|p| p.models.iter().map(|m| (p.id.as_str(), m.id.as_str())))
        .collect();
    for d in &mut base {
        let Some(h) = by_id.get(d.model.as_str()) else {
            continue;
        };
        if d.display_name.is_none() {
            d.display_name = h.display_name.clone();
        }
        if !declared.contains(&(d.provider.as_str(), d.model.as_str())) {
            d.supports_reasoning_effort = h.supports_reasoning_effort;
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
    #[serde(default)]
    context_window: Option<serde_json::Value>,
    #[serde(default)]
    max_input_tokens: Option<serde_json::Value>,
}

fn parse_discovered_capacity(value: Option<&serde_json::Value>) -> Result<Option<u32>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    let capacity = value.as_u64().ok_or(())?;
    let capacity = u32::try_from(capacity).map_err(|_| ())?;
    if capacity == 0 {
        return Err(());
    }
    Ok(Some(capacity))
}

/// One model discovered from a provider's OpenAI-compatible `/v1/models` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredModel {
    pub provider: String,
    pub model: String,
    pub context_window: Option<u32>,
}

/// Merge dynamically discovered models over a static descriptor list. Static/config entries win;
/// provider context metadata is used when present, otherwise the conservative fallback remains.
/// Result is deduped by `(provider, model)`.
pub fn merge_models(
    mut base: Vec<ModelDescriptor>,
    dynamic: &[DiscoveredModel],
) -> Vec<ModelDescriptor> {
    use std::collections::BTreeSet;
    let mut seen: BTreeSet<(String, String)> = base
        .iter()
        .map(|d| (d.provider.clone(), d.model.clone()))
        .collect();
    for discovered in dynamic {
        if seen.insert((discovered.provider.clone(), discovered.model.clone())) {
            let mut descriptor = ModelDescriptor::conservative(
                discovered.provider.clone(),
                discovered.model.clone(),
            );
            if let Some(context_window) = discovered.context_window.filter(|window| *window > 0) {
                descriptor.context_window = context_window;
            }
            base.push(descriptor);
        }
    }
    base
}

/// Refresh the model list by querying `GET {base_url}/models` for every provider that advertises
/// `model_listing`, merging the results over the static list (§8.3). Best-effort: a provider whose
/// endpoint errors, returns a non-success status, or sends unparseable JSON is skipped (its static
/// entries remain), so the call always returns at least the static list. Each such failure is
/// logged **and** returned as a [`ModelListingWarning`] so it can be surfaced to the user rather
/// than silently yielding no models (e.g. a 401 from a proxy whose api_key is missing/wrong).
pub async fn refresh_models(config: &Config) -> (Vec<ModelDescriptor>, Vec<ModelListingWarning>) {
    let base = list_descriptors(config);
    let mut warnings: Vec<ModelListingWarning> = Vec::new();

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

    let mut dynamic: Vec<DiscoveredModel> = Vec::new();
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
            warnings.push(ModelListingWarning {
                source: format!("provider:{}", p.id),
                message,
            });
        };
        let mut invalid_metadata_models = Vec::new();

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
                        for model in body.data {
                            let context_window =
                                parse_discovered_capacity(model.context_window.as_ref());
                            let max_input_tokens =
                                parse_discovered_capacity(model.max_input_tokens.as_ref());
                            if context_window.is_err() || max_input_tokens.is_err() {
                                warn!(
                                    provider = %p.id,
                                    model = %model.id,
                                    context_window = ?model.context_window,
                                    max_input_tokens = ?model.max_input_tokens,
                                    "ignoring invalid model capacity metadata"
                                );
                                invalid_metadata_models.push(model.id.clone());
                            }
                            dynamic.push(DiscoveredModel {
                                provider: p.id.clone(),
                                model: model.id,
                                context_window: context_window
                                    .ok()
                                    .flatten()
                                    .or_else(|| max_input_tokens.ok().flatten()),
                            });
                        }
                    }
                    Err(e) => fail(format!("unparseable /models response: {e}")),
                }
            }
            Err(e) => fail(format!("could not reach {url}: {e}")),
        }
        if !invalid_metadata_models.is_empty() {
            warnings.push(ModelListingWarning {
                source: format!("provider:{}", p.id),
                message: format!(
                    "ignored invalid context capacity metadata for {} model(s)",
                    invalid_metadata_models.len()
                ),
            });
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
    fn explicit_config_overrides_a_stale_cached_catalog_descriptor() {
        let config = config_with_glm();
        let model = ModelRef {
            provider: "cloudflare-litellm".into(),
            model: "@cf/z-ai/glm-4.7".into(),
            reasoning_effort: None,
        };
        let stale_catalog = vec![ModelDescriptor {
            provider: model.provider.clone(),
            model: model.model.clone(),
            context_window: 64_000,
            supports_reasoning_effort: true,
            reasoning_efforts: vec!["high".into()],
            display_name: Some("Stale".into()),
        }];

        let descriptor = resolve_catalog_descriptor(&stale_catalog, &config, &model);
        assert_eq!(descriptor.context_window, 131_072);
        assert!(!descriptor.supports_reasoning_effort);
    }

    #[test]
    fn known_model_names_do_not_bypass_conservative_fallback() {
        let config = Config::default();
        let m = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        assert_eq!(
            context_window_for(&config, &m),
            ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW
        );
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
            DiscoveredModel {
                provider: "cloudflare-litellm".into(),
                model: "@cf/z-ai/glm-4.7".into(),
                context_window: Some(1),
            },
            // New id with no metadata ⇒ added with a conservative descriptor.
            DiscoveredModel {
                provider: "cloudflare-litellm".into(),
                model: "@cf/meta/llama-4".into(),
                context_window: None,
            },
            // A provider-advertised context window is retained for a new model.
            DiscoveredModel {
                provider: "cloudflare-litellm".into(),
                model: "provider-sized".into(),
                context_window: Some(258_400),
            },
        ];
        let merged = merge_models(base, &dynamic);
        assert_eq!(merged.len(), 3);

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
        let provider_sized = merged.iter().find(|d| d.model == "provider-sized").unwrap();
        assert_eq!(provider_sized.context_window, 258_400);
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
    fn empty_catalog_efforts_use_the_harness_support_flag() {
        let config = Config::default();
        let base = vec![ModelDescriptor::conservative("openai", "gpt-5.5")];
        let unsupported = vec![ModelDescriptor {
            provider: String::new(),
            model: "gpt-5.5".into(),
            context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: false,
            reasoning_efforts: Vec::new(),
            display_name: Some("GPT-5.5".into()),
        }];
        let merged = apply_harness_metadata(base.clone(), &unsupported, &config);
        assert!(!merged[0].supports_reasoning_effort);
        assert!(merged[0].reasoning_efforts.is_empty());

        let default_only = vec![ModelDescriptor {
            provider: String::new(),
            model: "gpt-5.5".into(),
            context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: true,
            reasoning_efforts: Vec::new(),
            display_name: Some("GPT-5.5".into()),
        }];
        let merged = apply_harness_metadata(base, &default_only, &config);
        assert!(merged[0].supports_reasoning_effort);
        assert!(merged[0].reasoning_efforts.is_empty());
    }

    #[test]
    fn harness_runtime_context_window_wins_for_exact_model() {
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: None,
        };
        let descriptor = ModelDescriptor::conservative("openai", "gpt-5.6-sol");
        let runtime = HashMap::from([
            (
                "openai".into(),
                HashMap::from([("gpt-5.6-sol".into(), 258_400)]),
            ),
            (
                "other".into(),
                HashMap::from([("gpt-5.6-sol".into(), 1_000)]),
            ),
        ]);

        assert_eq!(
            context_window_with_runtime(&model, &descriptor, &runtime),
            258_400
        );
        assert_eq!(
            context_window_with_runtime(&model, &descriptor, &HashMap::new()),
            ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW
        );
    }

    #[test]
    fn harness_runtime_context_windows_keep_slash_bearing_ids_distinct() {
        let runtime = HashMap::from([
            ("a".into(), HashMap::from([("b/c".into(), 100_000)])),
            ("a/b".into(), HashMap::from([("c".into(), 200_000)])),
        ]);
        let first = ModelRef {
            provider: "a".into(),
            model: "b/c".into(),
            reasoning_effort: None,
        };
        let second = ModelRef {
            provider: "a/b".into(),
            model: "c".into(),
            reasoning_effort: None,
        };

        assert_eq!(
            context_window_with_runtime(
                &first,
                &ModelDescriptor::conservative("a", "b/c"),
                &runtime,
            ),
            100_000
        );
        assert_eq!(
            context_window_with_runtime(
                &second,
                &ModelDescriptor::conservative("a/b", "c"),
                &runtime,
            ),
            200_000
        );
    }

    #[test]
    fn configured_effort_precedence_is_scoped_to_provider_and_model() {
        let config: Config = toml::from_str(
            r#"
[[providers]]
id = "configured"
name = "Configured"
wire_api = "responses"
  [[providers.models]]
  id = "shared-model"
  context_window = 1000
  supports_reasoning_effort = false

[[providers]]
id = "discovered"
name = "Discovered"
wire_api = "responses"
"#,
        )
        .unwrap();
        let base = vec![ModelDescriptor::conservative("discovered", "shared-model")];
        let harness = vec![ModelDescriptor {
            provider: String::new(),
            model: "shared-model".into(),
            context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: true,
            reasoning_efforts: vec!["focused".into()],
            display_name: Some("Shared Model".into()),
        }];

        let merged = apply_harness_metadata(base, &harness, &config);
        assert!(merged[0].supports_reasoning_effort);
        assert_eq!(merged[0].reasoning_efforts, vec!["focused"]);
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
