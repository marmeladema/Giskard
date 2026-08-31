//! Model descriptor resolution & listing (spec §8.3).
//!
//! Applies the metadata-source precedence for a `(provider, model)` pair:
//! 1. the typed `[[providers.<id>.models]]` config entry;
//! 2. the `/v1/models` dynamic listing (merged over the static list by [`discover_models`]);
//! 3. a conservative fallback (`context_window = 128000`, no reasoning effort).

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;

use serde::Deserialize;
use tracing::warn;

use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_harness::{HarnessProvider, ProviderAuth};
use giskard_persist::Config;
use giskard_proto::ModelListingWarning;

/// Build a `ModelDescriptor` from a typed config entry, if the provider + model are declared.
fn from_config(config: &Config, provider: &str, model: &str) -> Option<ModelDescriptor> {
    let p = config.providers.get(provider)?;
    let m = p.models.iter().find(|m| m.id == model)?;
    Some(ModelDescriptor {
        provider: provider.to_string(),
        model: model.to_string(),
        context_window: m.context_window,
        supports_reasoning_effort: m.supports_reasoning_effort,
        reasoning_efforts: Vec::new(),
        display_name: m.display_name.clone(),
        is_default: false,
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

pub fn normalize_model_ref(
    config: &Config,
    catalog: &[ModelDescriptor],
    model: &ModelRef,
) -> ModelRef {
    if config.providers.is_empty() || from_config(config, &model.provider, &model.model).is_some() {
        return model.clone();
    }
    // A pair the catalog offers is not stale, whatever config says. This repairs a provider that
    // has gone away; since the harness catalog became a source of picker entries in its own right,
    // "absent from config" stopped implying "wrong" — and rewriting a model the user just picked
    // would silently route the turn to a different provider than the one they chose.
    if catalog
        .iter()
        .any(|d| d.provider == model.provider && d.model == model.model)
    {
        return model.clone();
    }

    let mut matches = config.providers.iter().filter_map(|(id, provider)| {
        provider
            .models
            .iter()
            .find(|candidate| candidate.id == model.model)
            .map(|candidate| (id, candidate))
    });

    let Some((provider_id, candidate)) = matches.next() else {
        return model.clone();
    };
    if matches.next().is_some() {
        return model.clone();
    }

    let mut normalized = model.clone();
    normalized.provider = provider_id.clone();
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
/// `[[providers.<id>.models]]` entry, resolved to a `ModelDescriptor`.
pub fn list_descriptors(config: &Config) -> Vec<ModelDescriptor> {
    let mut out = Vec::new();
    for (id, p) in &config.providers {
        for m in &p.models {
            out.push(ModelDescriptor {
                provider: id.clone(),
                model: m.id.clone(),
                context_window: m.context_window,
                supports_reasoning_effort: m.supports_reasoning_effort,
                reasoning_efforts: Vec::new(),
                display_name: m.display_name.clone(),
                is_default: false,
            });
        }
    }
    out
}

/// Put the composed picker list in provider order (§8.3).
///
/// Declared providers lead, in the order config writes them; everything else follows by id. The
/// same rule `discovery_targets` uses, applied once at the end because the list is assembled from
/// three sources that each contribute at a different point — config's declared models, discovery,
/// and the harness catalog. Ordering only the first two would let a provider supplied solely by
/// the catalog, as a stock `openai` is, land behind one the user never declared.
///
/// Stable, so each provider keeps the order its own models arrived in: an explicit
/// `[[providers.<id>.models]]` entry still precedes that provider's discovered ones, and a
/// provider-stated `priority` still orders its catalog.
pub fn order_for_picker(mut models: Vec<ModelDescriptor>, config: &Config) -> Vec<ModelDescriptor> {
    let rank = |provider: &str| {
        config
            .providers
            .get_index_of(provider)
            .unwrap_or(usize::MAX)
    };
    models.sort_by(|a, b| {
        rank(&a.provider)
            .cmp(&rank(&b.provider))
            // Undeclared providers all share the sentinel rank, so their relative order is the id's
            // — the harness table has none of its own to inherit.
            .then_with(|| a.provider.cmp(&b.provider))
    });
    models
}

/// Overlay a harness model catalog's metadata onto a descriptor list, and append the entries it
/// alone supplies.
///
/// Metadata is keyed by model id, not by route: a catalog entry describes a model, and the same
/// model reached through a different provider is still that model. `is_default` is the exception —
/// it names one route — and so is the append below, which needs a provider to produce an entry at
/// all:
///
/// - **Display names** fill a descriptor whose `display_name` is unset, so an explicit
///   `[[providers.<id>.models]] display_name` always wins.
/// - **`is_default`** marks the model the harness would start from, used only to seed a project's
///   default (§8.3). Config has no way to express it, so the catalog is its only source.
/// - **Reasoning efforts** (the exact levels a model advertises) fill a gap rather than overwrite:
///   they are applied only to models the config did **not** declare *and* that discovery did not
///   already describe. A `[[providers.<id>.models]]` entry keeps its configured effort setting, a
///   provider that advertised its own levels keeps those, and only a model nothing else has
///   described takes the harness catalog's.
///
/// The harness never supplies context window (Codex's `model/list` omits it).
pub fn apply_harness_metadata(
    mut base: Vec<ModelDescriptor>,
    harness_models: &[ModelDescriptor],
    config: &Config,
    efforts_from_discovery: &HashSet<(String, String)>,
) -> Vec<ModelDescriptor> {
    let by_id: HashMap<&str, &ModelDescriptor> = harness_models
        .iter()
        .map(|m| (m.model.as_str(), m))
        .collect();
    let declared: HashSet<(&str, &str)> = config
        .providers
        .iter()
        .flat_map(|(id, p)| p.models.iter().map(|m| (id.as_str(), m.id.as_str())))
        .collect();
    for d in &mut base {
        let Some(h) = by_id.get(d.model.as_str()) else {
            continue;
        };
        if d.display_name.is_none() {
            d.display_name = h.display_name.clone();
        }
        // Which model to start from is the harness's to state, and it applies to a declared entry
        // too, unlike the effort metadata below. But it names one *route*, not one model id: now
        // that the catalog carries a provider, honouring it by id alone would flag a same-named
        // model under an unrelated provider as the default — and with the real entry appended
        // below, both would claim it and the picker would start on whichever came first.
        if h.provider.is_empty() || h.provider == d.provider {
            d.is_default = h.is_default;
        }
        // Config wins, and so does anything discovery already learned: a provider's own catalog
        // names efforts for *its* model, while `model/list` is keyed by model id alone and knows
        // nothing about providers, so it would otherwise replace an advertised list with the one
        // some same-named model happens to have (§8.3 puts the `/v1/models` response above the
        // harness catalog). Silence here still means the harness fills the gap.
        if !declared.contains(&(d.provider.as_str(), d.model.as_str()))
            && !efforts_from_discovery.contains(&(d.provider.clone(), d.model.clone()))
        {
            d.supports_reasoning_effort = h.supports_reasoning_effort;
            d.reasoning_efforts = h.reasoning_efforts.clone();
        }
    }

    // The catalog is also a *source*, not only an overlay. A harness that names the provider its
    // models route to has said everything a picker entry needs, and for a stock Codex it is the
    // only thing that has: its built-in providers carry no `base_url`, so discovery finds nothing
    // and config declaring nothing is now the expected case. Without this the picker is empty.
    //
    // Appended, then ordered with everything else by `order_for_picker` — a provider supplied
    // only by the catalog is still a provider the user may have pinned. Skipped when the harness
    // could not say which provider a model belongs to: an unattributed entry is not routable and
    // would fail at turn time instead of here.
    // `model_listing = false` is an opt-out from *listing*, whatever the source: a provider told
    // to offer only its declared models must not have the harness's catalog appended under it
    // either.
    let present: HashSet<(&str, &str)> = base
        .iter()
        .map(|d| (d.provider.as_str(), d.model.as_str()))
        .collect();
    let mut from_catalog: Vec<ModelDescriptor> = harness_models
        .iter()
        .filter(|h| !h.provider.is_empty())
        .filter(|h| {
            config
                .providers
                .get(&h.provider)
                .and_then(|configured| configured.model_listing)
                != Some(false)
        })
        .filter(|h| !present.contains(&(h.provider.as_str(), h.model.as_str())))
        .cloned()
        .collect();
    base.append(&mut from_catalog);
    base
}

// ---- Dynamic /v1/models refresh (spec §8.3) ----

/// A `GET /v1/models` body, which may carry either list or both.
///
/// `data` is the OpenAI-compatible shape; `models` is the harness catalog a provider serves to a
/// caller that identified itself. Both are optional so a body carrying one is not rejected for
/// lacking the other — and both being absent is what distinguishes a body in neither shape from a
/// legitimately empty listing, which still sends `{"data": []}`.
#[derive(Deserialize)]
struct ModelsBody {
    #[serde(default)]
    models: Option<serde_json::Value>,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

/// The entries of one of the two lists, or `None` when the key was absent or was not a list.
///
/// Both lists are taken loosely and read entry by entry. Typing either strictly would let a single
/// unrecognisable entry — or a key a provider happens to use for something else — discard the other
/// list along with it, and the two are independent claims about the same catalog.
fn model_entries(
    value: Option<serde_json::Value>,
    provider: &str,
    key: &str,
) -> Option<Vec<serde_json::Value>> {
    match value {
        None => None,
        Some(serde_json::Value::Array(entries)) => Some(entries),
        Some(_) => {
            warn!(
                provider = %provider,
                key,
                action = "discover_models",
                "ignoring a /models response key that is not a list"
            );
            None
        }
    }
}

#[derive(Deserialize)]
struct OpenAiModel {
    id: String,
    #[serde(default)]
    context_window: Option<serde_json::Value>,
    #[serde(default)]
    max_input_tokens: Option<serde_json::Value>,
}

/// The harness catalog shape the same endpoint serves to a harness that identifies itself
/// (`{ "models": [ … ] }`, Codex's `ModelsResponse`).
///
/// Worth parsing because it answers the one question the OpenAI shape cannot: `context_window`,
/// which no harness reports over its own protocol — Codex drops it between this response and
/// `model/list`. It also carries display names and the exact reasoning levels a model advertises,
/// so a provider serving this needs no `[[providers.<id>.models]]` entries at all.
///
/// Only `slug` is typed: without one there is no model to speak of. Everything else is taken as raw
/// JSON and interpreted below, so a `display_name` that arrives as a number costs that name rather
/// than the whole entry — the same entry-by-entry, field-by-field tolerance the lists get.
#[derive(Deserialize)]
struct HarnessCatalogModel {
    slug: String,
    #[serde(default)]
    display_name: Option<serde_json::Value>,
    #[serde(default)]
    context_window: Option<serde_json::Value>,
    /// Codex resolves the effective window as `context_window.or(max_context_window)`
    /// (`ModelInfo::resolved_context_window`), so an entry carrying only the maximum still has one.
    #[serde(default)]
    max_context_window: Option<serde_json::Value>,
    /// `None` is "said nothing about efforts"; `Some([])` is "said there are none". The two must
    /// stay apart: the harness-catalog overlay fills the first and must not touch the second.
    ///
    /// Taken as raw JSON like the rest: a value that is not a list is silence, not an answer —
    /// suppressing the overlay on the strength of a malformed field would be worse than filling it.
    #[serde(default)]
    supported_reasoning_levels: Option<serde_json::Value>,
    /// Codex separates the default effort from the selectable alternatives, and its TUI treats a
    /// non-`none` default as the sole valid choice when the alternatives are empty. `map_model`
    /// already normalizes that for the `model/list` path; this is the same rule for this one.
    #[serde(default)]
    default_reasoning_level: Option<serde_json::Value>,
    /// `"list"` shows in a picker; `"hide"`/`"none"` do not. Absent is treated as listed, so a
    /// server that omits the field does not silently empty the picker.
    #[serde(default)]
    visibility: Option<serde_json::Value>,
    /// Codex sorts the catalog by this before building its picker list, so a provider serving this
    /// shape is stating an order, not just a set.
    #[serde(default)]
    priority: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct HarnessCatalogEffort {
    effort: String,
}

impl HarnessCatalogModel {
    fn listed(&self) -> bool {
        !matches!(
            self.visibility.as_ref().and_then(|v| v.as_str()),
            Some("hide") | Some("none")
        )
    }

    fn priority(&self) -> Option<i64> {
        self.priority.as_ref().and_then(serde_json::Value::as_i64)
    }

    /// The window this entry claims, following Codex's own resolution order, and whether either
    /// field was present but unusable.
    ///
    /// The two are separate answers on purpose: a fallback that works should still be used, and the
    /// malformed field it stepped around should still be reported.
    fn context_window(&self) -> (Option<u32>, bool) {
        let primary = parse_discovered_capacity(self.context_window.as_ref());
        if let Ok(Some(window)) = primary {
            return (Some(window), false);
        }
        let max = parse_discovered_capacity(self.max_context_window.as_ref());
        let invalid = primary.is_err() || max.is_err();
        (max.unwrap_or(None), invalid)
    }

    fn display_name(&self) -> Option<String> {
        self.display_name
            .as_ref()
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
    }

    /// The effort levels this model advertises, or `None` when it said nothing about them.
    ///
    /// A level that is not an object with a string `effort` is skipped rather than costing the
    /// list. An empty list stays empty — "no efforts" is an answer — except that a non-`none`
    /// default with nothing else is that default, matching `map_model` on the `model/list` side.
    fn reasoning_efforts(&self) -> Option<Vec<String>> {
        let default = self
            .default_reasoning_level
            .as_ref()
            .and_then(|value| value.as_str())
            .filter(|effort| !effort.is_empty() && *effort != "none");
        let stated = self
            .supported_reasoning_levels
            .as_ref()
            .and_then(serde_json::Value::as_array);
        if stated.is_none() && default.is_none() {
            return None;
        }
        let mut efforts: Vec<String> = stated
            .map(|levels| {
                levels
                    .iter()
                    .filter_map(|level| {
                        serde_json::from_value::<HarnessCatalogEffort>(level.clone())
                            .ok()
                            .map(|level| level.effort)
                    })
                    .collect()
            })
            .unwrap_or_default();
        if efforts.is_empty()
            && let Some(default) = default
        {
            efforts.push(default.to_string());
        }
        Some(efforts)
    }
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

/// One model discovered from a provider's `/v1/models` endpoint.
///
/// Everything past `provider`/`model` is optional because the two response shapes carry different
/// amounts: the OpenAI-compatible one usually just names ids, while a harness catalog fills all of
/// it in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiscoveredModel {
    pub provider: String,
    pub model: String,
    pub context_window: Option<u32>,
    pub display_name: Option<String>,
    /// `None` is "the endpoint said nothing about efforts"; `Some([])` is "it said there are none".
    /// Collapsing the two would let the harness-catalog overlay hand a model effort levels its own
    /// provider just said it does not have.
    pub reasoning_efforts: Option<Vec<String>>,
    /// The catalog's ordering key, when it gave one. Codex sorts by this before building its
    /// picker list, so a provider serving that shape is stating an order and not just a set.
    pub priority: Option<i64>,
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
            descriptor.display_name = discovered.display_name.clone();
            // An advertised effort list is also the answer to whether the selector is shown at all,
            // so the two move together — including when the answer is "none", which is why this
            // turns on `Some` rather than on a non-empty list. Silence leaves the conservative
            // default for the harness catalog overlay (§8.3) to fill in later.
            if let Some(efforts) = &discovered.reasoning_efforts {
                descriptor.supports_reasoning_effort = !efforts.is_empty();
                descriptor.reasoning_efforts = efforts.clone();
            }
            base.push(descriptor);
        }
    }
    base
}

/// Check each configured provider id against the providers the harness actually knows (§8.2),
/// returning one warning per id the harness has never heard of.
///
/// A mismatch is not fatal — the models stay in the picker — but it is the difference between
/// finding out at config time and finding out mid-turn, when the provider answers
/// `model_not_found` for a routing id it was never configured with.
pub fn validate_provider_ids(
    config: &Config,
    harness_providers: &[HarnessProvider],
    harness_kind: &str,
) -> Vec<ModelListingWarning> {
    config
        .providers
        .iter()
        .filter(|(id, _)| !harness_providers.iter().any(|known| &known.id == *id))
        .map(|(id, _)| {
            warn!(
                provider = %id,
                harness = %harness_kind,
                action = "validate_provider_ids",
                "configured provider is not one the harness knows; its models cannot be routed"
            );
            ModelListingWarning {
                source: format!("provider:{id}"),
                message: format!(
                    "provider id is not configured in {harness_kind}; models under it cannot \
                     be routed until it is added there"
                ),
            }
        })
        .collect()
}

/// What one discovery pass produced.
pub struct Discovery {
    /// The static list with everything discovered merged over it.
    pub models: Vec<ModelDescriptor>,
    pub warnings: Vec<ModelListingWarning>,
    /// `(provider, model)` pairs whose reasoning efforts a provider stated — including stating that
    /// there are none. The harness catalog overlay leaves these alone: `model/list` is keyed by
    /// model id and knows nothing about providers, so it can only ever describe some model of that
    /// name (§8.3).
    pub efforts_from_discovery: HashSet<(String, String)>,
}

/// The discovery URL, identifying the harness to the provider when the harness knows its own
/// version.
///
/// A provider that serves the harness's catalog decides what to answer from `client_version` — it
/// is how the harness asks for the richer shape (§8.3). It is sent to every provider, not only the
/// ones known to serve that shape: an OpenAI-compatible endpoint ignores a query parameter it does
/// not know, and gating on how a provider authenticates would deny the richer catalog to one that
/// serves it with a plain `env_key`.
fn models_url(base_url: &str, client_version: Option<&str>) -> String {
    // Split any query off first: appending the path to a base_url that already carries one would
    // bury `/models` inside the query string.
    let (path, query) = match base_url.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (base_url, None),
    };
    let mut url = format!("{}/models", path.trim_end_matches('/'));
    if let Some(query) = query.filter(|query| !query.is_empty()) {
        url.push('?');
        url.push_str(query);
    }
    // Only a harness that reports something usable gets to shape the URL. The trait lets a harness
    // return any string, and one carrying a space or an `&` would either corrupt the query or be
    // rejected by the HTTP client — failing discovery for every provider over a value that is
    // nothing but an identifier.
    if let Some(version) = client_version.filter(|version| is_url_safe_version(version)) {
        url.push(if url.contains('?') { '&' } else { '?' });
        url.push_str("client_version=");
        url.push_str(version);
    }
    url
}

/// Whether a harness-reported version can go in a query string as-is.
///
/// Deliberately narrow — versions are digits, dots, and at most a dash or plus suffix — so this
/// needs no percent-encoding and a harness cannot inject query structure through it.
fn is_url_safe_version(version: &str) -> bool {
    !version.is_empty()
        && version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+' | b'_' | b'~'))
}

/// Read a `/models` body, taking whatever each of the two shapes has to offer.
///
/// Sending `client_version` asks for the harness catalog; it does not guarantee one. A provider may
/// answer the OpenAI-compatible shape regardless, or — with `data` and `models` being different
/// keys — answer both at once. So both are read from a single parse and combined per model id,
/// with the catalog winning field by field: it is the shape that actually carries metadata, and
/// where it stays silent the OpenAI entry still fills the gap rather than being discarded.
///
/// A body carrying neither key is an error, not an empty listing.
fn parse_models_body(body: &[u8], provider: &str) -> Result<Vec<(DiscoveredModel, bool)>, String> {
    let parsed: ModelsBody =
        serde_json::from_slice(body).map_err(|e| format!("unparseable /models response: {e}"))?;
    let catalog = model_entries(parsed.models, provider, "models");
    let openai = model_entries(parsed.data, provider, "data");
    if catalog.is_none() && openai.is_none() {
        return Err("/models response has neither a `models` nor a `data` list".to_string());
    }

    // Insertion-ordered so the picker keeps the order the provider listed, and so a catalog entry
    // refines an OpenAI one in place rather than jumping to the end.
    let mut out: IndexMap<String, (DiscoveredModel, bool)> = IndexMap::new();

    for entry in openai.unwrap_or_default() {
        let model: OpenAiModel = match serde_json::from_value(entry) {
            Ok(model) => model,
            Err(e) => {
                warn!(
                    provider = %provider,
                    error = %e,
                    action = "discover_models",
                    "skipping an unreadable entry in the /models data list"
                );
                continue;
            }
        };
        let context_window = parse_discovered_capacity(model.context_window.as_ref());
        let max_input_tokens = parse_discovered_capacity(model.max_input_tokens.as_ref());
        let invalid = context_window.is_err() || max_input_tokens.is_err();
        if invalid {
            warn!(
                provider = %provider,
                model = %model.id,
                context_window = ?model.context_window,
                max_input_tokens = ?model.max_input_tokens,
                action = "discover_models",
                "ignoring invalid model capacity metadata"
            );
        }
        out.insert(
            model.id.clone(),
            (
                DiscoveredModel {
                    provider: provider.to_string(),
                    model: model.id,
                    context_window: context_window
                        .ok()
                        .flatten()
                        .or_else(|| max_input_tokens.ok().flatten()),
                    ..DiscoveredModel::default()
                },
                invalid,
            ),
        );
    }

    for entry in catalog.unwrap_or_default() {
        let model: HarnessCatalogModel = match serde_json::from_value(entry) {
            Ok(model) => model,
            Err(e) => {
                // One entry Giskard cannot read is not a reason to drop the rest, nor the `data`
                // list beside it.
                warn!(
                    provider = %provider,
                    error = %e,
                    action = "discover_models",
                    "skipping an unreadable entry in the /models catalog list"
                );
                continue;
            }
        };
        if !model.listed() {
            // Hidden in the catalog means hidden, even if the OpenAI list named it.
            out.shift_remove(&model.slug);
            continue;
        }
        let (context_window, invalid_window) = model.context_window();
        if invalid_window {
            warn!(
                provider = %provider,
                model = %model.slug,
                context_window = ?model.context_window,
                max_context_window = ?model.max_context_window,
                action = "discover_models",
                "ignoring invalid model capacity metadata"
            );
        }
        let entry = out.entry(model.slug.clone()).or_insert_with(|| {
            (
                DiscoveredModel {
                    provider: provider.to_string(),
                    model: model.slug.clone(),
                    ..DiscoveredModel::default()
                },
                false,
            )
        });
        // Field by field: the catalog wins where it says something, and leaves what it does not
        // mention as the OpenAI entry had it.
        if let Some(window) = context_window {
            entry.0.context_window = Some(window);
        }
        if let Some(name) = model.display_name() {
            entry.0.display_name = Some(name);
        }
        if let Some(efforts) = model.reasoning_efforts() {
            entry.0.reasoning_efforts = Some(efforts);
        }
        if let Some(priority) = model.priority() {
            entry.0.priority = Some(priority);
        }
        entry.1 |= invalid_window;
    }

    // Codex sorts the catalog by `priority` before building its picker list, so a provider serving
    // that shape is stating an order. Sorted per provider rather than globally: the picker's
    // top-level order is the `[providers.<id>]` declaration order, which a global sort would
    // interleave. Stable, so anything without a priority keeps the order the provider listed it in.
    out.sort_by(|_, a, _, b| {
        a.0.priority
            .unwrap_or(i64::MAX)
            .cmp(&b.0.priority.unwrap_or(i64::MAX))
    });
    Ok(out.into_values().collect())
}

/// One provider a catalog refresh will query.
struct DiscoveryTarget<'a> {
    provider: &'a HarnessProvider,
    /// Whether config asked for listing in so many words.
    ///
    /// Only an explicit request earns a warning when the provider cannot be discovered. Listing is
    /// on for everyone now, and most of what the harness reports has nothing to query — Codex's
    /// built-in provider ids arrive with no `base_url` at all — so warning on the default would
    /// hand every user a fistful of complaints about providers they never mentioned.
    requested: bool,
}

/// The providers to query, in picker order.
///
/// Listing is on unless a provider's config entry turns it off (§8.3). A provider the harness
/// reports is one the user already declared *to the harness*; making them declare it again to
/// Giskard bought nothing, and in practice almost nobody writes `[[providers.<id>.models]]` by
/// hand — discovery is what makes a new model appear under the right slug at all.
///
/// Order is config's where config names a provider, then everything else by id. The harness table
/// has no order to inherit: Codex parses `[model_providers]` into a `HashMap` before the config
/// ever reaches the wire, so declaration order is gone upstream and what arrives is a hash order
/// that changes between app-server restarts. Sorting the remainder keeps the picker — and the
/// first-model fallback a draft starts on — from reshuffling underneath the user.
fn discovery_targets<'a>(
    config: &Config,
    harness_providers: &'a [HarnessProvider],
) -> Vec<DiscoveryTarget<'a>> {
    let mut ordered: Vec<&HarnessProvider> = config
        .providers
        .keys()
        .filter_map(|id| harness_providers.iter().find(|known| &known.id == id))
        .collect();
    let mut rest: Vec<&HarnessProvider> = harness_providers
        .iter()
        .filter(|known| !config.providers.contains_key(&known.id))
        .collect();
    rest.sort_by(|a, b| a.id.cmp(&b.id));
    ordered.append(&mut rest);

    ordered
        .into_iter()
        .filter_map(|provider| {
            let listing = config
                .providers
                .get(&provider.id)
                .and_then(|configured| configured.model_listing);
            (listing != Some(false)).then_some(DiscoveryTarget {
                provider,
                requested: listing == Some(true),
            })
        })
        .collect()
}

/// Whether a refresh would actually query anything.
///
/// Used to decide whether it is worth asking the harness for its version: listing is on by default
/// now, so the old test — "does any config entry request listing?" — is both false for the common
/// empty config and beside the point. What matters is whether some provider has an endpoint.
pub fn will_query_any_provider(config: &Config, harness_providers: &[HarnessProvider]) -> bool {
    discovery_targets(config, harness_providers)
        .iter()
        .any(|target| target.provider.base_url.is_some())
}

/// Refresh the model list by querying `GET {base_url}/models` for each provider the harness
/// reports, merging the results over the static list (§8.3).
///
/// The endpoint and key location come from `harness_providers` — the harness owns provider
/// configuration (§8.2), so a provider the harness does not know contributes no discovery, and one
/// it knows without a `base_url` has nothing to query.
///
/// Providers are queried concurrently. Serially, one slow endpoint delayed every provider behind
/// it, which mattered little while listing was opt-in per provider and matters now that it is on
/// by default. Results are collected back in picker order, so concurrency changes the timing and
/// nothing else.
///
/// Best-effort: a provider whose endpoint errors, returns a non-success status, or sends
/// unparseable JSON is skipped (its static entries remain), so the call always returns at least
/// the static list. Each such failure is logged **and** returned as a [`ModelListingWarning`] so
/// it can be surfaced to the user rather than silently yielding no models (e.g. a 401 from a proxy
/// whose key is missing or wrong).
pub async fn discover_models(
    config: &Config,
    harness_providers: &[HarnessProvider],
    client_version: Option<&str>,
) -> Discovery {
    let base = list_descriptors(config);
    let mut warnings: Vec<ModelListingWarning> = Vec::new();

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(%e, "cannot build HTTP client; returning static model list");
            return Discovery {
                models: base,
                warnings,
                efforts_from_discovery: HashSet::new(),
            };
        }
    };

    // Capped rather than an unbounded `join_all`. A provider with a command-backed key spawns a
    // process to produce it, and listing is on for everyone now — a gateway-heavy
    // `[model_providers]` table would otherwise mean one HTTP request *and* one process per
    // provider, all at once, on something as routine as opening a project. Providers with no
    // `base_url` return before either and cost nothing.
    use futures::stream::StreamExt as _;
    const MAX_CONCURRENT_PROVIDERS: usize = 8;
    let targets = discovery_targets(config, harness_providers);
    let queries: Vec<_> = targets
        .iter()
        .map(|target| discover_provider(&client, target, client_version))
        .collect();
    let results: Vec<_> = futures::stream::iter(queries)
        .buffered(MAX_CONCURRENT_PROVIDERS)
        .collect()
        .await;

    let mut dynamic: Vec<DiscoveredModel> = Vec::new();
    for (models, provider_warnings) in results {
        dynamic.extend(models);
        warnings.extend(provider_warnings);
    }

    // Which pairs a provider actually spoke about, so the harness-catalog overlay can tell "nobody
    // said" from "the provider said none" (§8.3).
    let efforts_from_discovery = dynamic
        .iter()
        .filter(|model| model.reasoning_efforts.is_some())
        .map(|model| (model.provider.clone(), model.model.clone()))
        .collect();
    Discovery {
        models: merge_models(base, &dynamic),
        warnings,
        efforts_from_discovery,
    }
}

/// Query one provider's `/models`, returning what it offered and anything worth telling the user.
async fn discover_provider(
    client: &reqwest::Client,
    target: &DiscoveryTarget<'_>,
    client_version: Option<&str>,
) -> (Vec<DiscoveredModel>, Vec<ModelListingWarning>) {
    let known = target.provider;
    let id = &known.id;
    let mut models: Vec<DiscoveredModel> = Vec::new();
    let mut warnings: Vec<ModelListingWarning> = Vec::new();

    let Some(base_url) = known.base_url.as_deref() else {
        // Nothing to query. Worth saying only if the user asked for this provider by name.
        if target.requested {
            warn!(
                provider = %id,
                action = "discover_models",
                "provider requests model listing but the harness reports no base_url for it"
            );
            warnings.push(ModelListingWarning {
                source: format!("provider:{id}"),
                message: "model_listing is on but the harness reports no base_url for this \
                          provider; only declared models are offered"
                    .into(),
            });
        }
        return (models, warnings);
    };
    let url = models_url(base_url, client_version);

    let mut fail = |message: String| {
        warn!(provider = %id, %url, %message, "model discovery failed; skipping provider");
        warnings.push(ModelListingWarning {
            source: format!("provider:{id}"),
            message,
        });
    };
    let mut invalid_metadata_models = Vec::new();

    // Attach the provider's discovery key for endpoints that require auth — e.g. a LiteLLM
    // proxy with a master key returns 401 otherwise. A command-backed key that cannot be
    // produced is reported as itself: sending the request unauthenticated would bury the real
    // cause under a 401 blaming the endpoint.
    let key = match known.resolve_api_key().await {
        Ok(key) => key,
        Err(e) => {
            fail(e.to_string());
            return (models, warnings);
        }
    };
    let mut request = client.get(&url);
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }

    match request.send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                // A 401/403 is about the key, so point at whichever source this provider
                // actually names rather than assuming an environment variable.
                let hint = if status.as_u16() == 401 || status.as_u16() == 403 {
                    match &known.auth {
                        Some(ProviderAuth::Env(var)) => {
                            format!(
                                " — check ${var}, the env var the harness names for this \
                                     provider's key"
                            )
                        }
                        Some(ProviderAuth::Command(auth)) => format!(
                            " — `{}` produced a token the provider rejected",
                            auth.command
                        ),
                        None => " — the harness names no key for this provider".to_string(),
                    }
                } else {
                    String::new()
                };
                fail(format!("model listing returned HTTP {status}{hint}"));
                return (models, warnings);
            }
            match resp.bytes().await {
                Ok(body) => match parse_models_body(&body, id) {
                    Ok(parsed) => {
                        for (model, invalid_metadata) in parsed {
                            if invalid_metadata {
                                invalid_metadata_models.push(model.model.clone());
                            }
                            models.push(model);
                        }
                    }
                    Err(e) => fail(e),
                },
                Err(e) => fail(format!("could not read the /models response: {e}")),
            }
        }
        Err(e) => fail(format!("could not reach {url}: {e}")),
    }
    if !invalid_metadata_models.is_empty() {
        warnings.push(ModelListingWarning {
            source: format!("provider:{id}"),
            message: format!(
                "ignored invalid context capacity metadata for {} model(s)",
                invalid_metadata_models.len()
            ),
        });
    }
    (models, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn harness_provider(id: &str, base_url: Option<&str>) -> HarnessProvider {
        HarnessProvider {
            id: id.into(),
            name: None,
            base_url: base_url.map(str::to_string),
            auth: None,
        }
    }

    fn target_ids(config: &Config, table: &[HarnessProvider]) -> Vec<String> {
        discovery_targets(config, table)
            .iter()
            .map(|t| t.provider.id.clone())
            .collect()
    }

    /// The headline: a provider the harness reports is discovered without config naming it. The
    /// harness table is where a provider is declared; repeating it here bought nothing.
    #[test]
    fn a_provider_absent_from_config_is_still_queried() {
        let config: Config = toml::from_str("").unwrap();
        let table = vec![harness_provider("opencodex", Some("http://x"))];
        assert_eq!(target_ids(&config, &table), ["opencodex"]);
    }

    /// The opt-out is the only thing config has to say about listing now.
    #[test]
    fn model_listing_false_opts_a_provider_out() {
        let config: Config =
            toml::from_str("[providers.opencodex]\nmodel_listing = false\n[providers.other]\n")
                .unwrap();
        let table = vec![
            harness_provider("opencodex", Some("http://x")),
            harness_provider("other", Some("http://y")),
        ];
        assert_eq!(
            target_ids(&config, &table),
            ["other"],
            "an explicit false is respected; a bare entry is not an opt-out"
        );
    }

    /// Config orders the picker where it names providers; the rest follow by id, because the
    /// harness table arrives in a hash order that changes between app-server restarts.
    #[test]
    fn config_declaration_order_leads_then_the_rest_by_id() {
        let config: Config = toml::from_str("[providers.zeta]\n[providers.alpha]\n").unwrap();
        // Deliberately not alphabetical, and not config order either.
        let table = vec![
            harness_provider("mid", Some("http://m")),
            harness_provider("alpha", Some("http://a")),
            harness_provider("beta", Some("http://b")),
            harness_provider("zeta", Some("http://z")),
        ];
        assert_eq!(
            target_ids(&config, &table),
            ["zeta", "alpha", "beta", "mid"],
            "declared providers keep config's order; undeclared ones sort by id"
        );
    }

    /// A config entry naming a provider the harness has never heard of cannot be queried, and must
    /// not wedge itself into the order.
    #[test]
    fn a_config_only_provider_is_not_a_target() {
        let config: Config = toml::from_str("[providers.ghost]\n").unwrap();
        let table = vec![harness_provider("real", Some("http://r"))];
        assert_eq!(target_ids(&config, &table), ["real"]);
    }

    /// Whether a failure to discover is worth reporting depends on whether the user asked. Codex
    /// reports five built-in provider ids with no `base_url`; on-by-default must not turn those
    /// into five complaints per refresh.
    #[test]
    fn only_an_explicit_request_is_worth_warning_about() {
        let config: Config = toml::from_str("[providers.asked]\nmodel_listing = true\n").unwrap();
        let table = vec![
            harness_provider("asked", None),
            harness_provider("defaulted", None),
        ];
        let targets = discovery_targets(&config, &table);
        let requested: Vec<(&str, bool)> = targets
            .iter()
            .map(|t| (t.provider.id.as_str(), t.requested))
            .collect();
        assert_eq!(requested, [("asked", true), ("defaulted", false)]);
    }

    /// The `client_version` gate: asking the harness for its version is only worth it when some
    /// provider actually has an endpoint to query.
    #[test]
    fn the_version_gate_follows_the_harness_not_config() {
        let empty: Config = toml::from_str("").unwrap();
        assert!(
            will_query_any_provider(&empty, &[harness_provider("p", Some("http://x"))]),
            "an empty config must not stop us identifying the harness"
        );
        assert!(
            !will_query_any_provider(&empty, &[harness_provider("builtin", None)]),
            "a provider with no endpoint is not worth starting a harness for"
        );
        assert!(!will_query_any_provider(&empty, &[]));
    }

    fn config_with_glm() -> Config {
        let toml = r#"
[providers.cloudflare-litellm]
model_listing = true
  [[providers.cloudflare-litellm.models]]
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
            is_default: false,
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
    fn models_url_identifies_the_harness_when_it_knows_its_version() {
        assert_eq!(
            models_url("https://api.example/v1", Some("0.58.0")),
            "https://api.example/v1/models?client_version=0.58.0"
        );
        // A trailing slash must not produce `//models`.
        assert_eq!(
            models_url("https://api.example/v1/", Some("0.58.0")),
            "https://api.example/v1/models?client_version=0.58.0"
        );
        // A base_url that already carries a query keeps it.
        assert_eq!(
            models_url("https://api.example/v1?tenant=a", Some("0.58.0")),
            "https://api.example/v1/models?tenant=a&client_version=0.58.0"
        );
        // Unknown version ⇒ no parameter at all rather than an empty one.
        assert_eq!(
            models_url("https://api.example/v1", None),
            "https://api.example/v1/models"
        );
        // A harness may return any string; one that cannot go in a query as-is is dropped rather
        // than corrupting the URL for every provider.
        for unusable in ["", "0.1 0", "0.1&x=y", "0.1?x", "0.1/../"] {
            assert_eq!(
                models_url("https://api.example/v1", Some(unusable)),
                "https://api.example/v1/models",
                "{unusable:?} should not reach the query"
            );
        }
    }

    /// The shape a provider serves a harness that identified itself: the metadata Giskard otherwise
    /// has to be told by hand, including the context window no harness reports over its protocol.
    #[test]
    fn a_harness_catalog_body_supplies_the_metadata_config_would_have_to_declare() {
        let body = br#"{
            "models": [
                {
                    "slug": "gpt-5.5",
                    "display_name": "GPT-5.5",
                    "context_window": 262144,
                    "supported_reasoning_levels": [
                        { "effort": "low", "description": "fast" },
                        { "effort": "high", "description": "thorough" }
                    ],
                    "visibility": "list"
                },
                { "slug": "internal-eval", "visibility": "hide" },
                { "slug": "retired", "visibility": "none" },
                { "slug": "unlabelled" }
            ]
        }"#;
        let parsed = parse_models_body(body, "opencodex").expect("the catalog shape should parse");
        let ids: Vec<&str> = parsed.iter().map(|(m, _)| m.model.as_str()).collect();
        assert_eq!(
            ids,
            ["gpt-5.5", "unlabelled"],
            "hidden models stay out of the picker; an absent visibility is not hidden"
        );

        let (rich, invalid) = &parsed[0];
        assert!(!invalid);
        assert_eq!(rich.provider, "opencodex");
        assert_eq!(rich.context_window, Some(262_144));
        assert_eq!(rich.display_name.as_deref(), Some("GPT-5.5"));
        assert_eq!(
            rich.reasoning_efforts.as_deref(),
            Some(&["low".to_string(), "high".to_string()][..])
        );
    }

    /// Codex separates the default effort from the selectable alternatives and treats a non-`none`
    /// default as the sole choice when there are no alternatives. `map_model` already normalizes
    /// that for the `model/list` path; the catalog path must agree, or a default-only reasoning
    /// model reads as non-reasoning.
    #[test]
    fn a_default_only_reasoning_model_keeps_its_effort() {
        let parsed = parse_models_body(
            br#"{ "models": [
                { "slug": "default-only", "default_reasoning_level": "medium",
                  "supported_reasoning_levels": [] },
                { "slug": "default-and-list", "default_reasoning_level": "medium",
                  "supported_reasoning_levels": [ { "effort": "low" }, { "effort": "high" } ] },
                { "slug": "explicitly-none", "default_reasoning_level": "none",
                  "supported_reasoning_levels": [] },
                { "slug": "said-nothing" }
            ] }"#,
            "p",
        )
        .unwrap();
        let by_id: std::collections::HashMap<&str, &DiscoveredModel> =
            parsed.iter().map(|(m, _)| (m.model.as_str(), m)).collect();

        assert_eq!(
            by_id["default-only"].reasoning_efforts.as_deref(),
            Some(&["medium".to_string()][..]),
            "a default with no alternatives is the sole effort"
        );
        assert_eq!(
            by_id["default-and-list"].reasoning_efforts.as_deref(),
            Some(&["low".to_string(), "high".to_string()][..]),
            "alternatives win when present; the default is not appended"
        );
        assert_eq!(
            by_id["explicitly-none"].reasoning_efforts.as_deref(),
            Some(&[][..]),
            "`none` is not an effort, and an empty list is still an answer"
        );
        assert_eq!(
            by_id["said-nothing"].reasoning_efforts, None,
            "no keys at all is silence, which the harness catalog may fill"
        );
    }

    /// Codex resolves a model's window as `context_window.or(max_context_window)`
    /// (`ModelInfo::resolved_context_window`, with a test of its own upstream), so an entry
    /// carrying only the maximum has a window — not the conservative fallback this whole shape
    /// exists to avoid.
    #[test]
    fn a_catalog_window_falls_back_to_max_context_window() {
        let parsed = parse_models_body(
            br#"{ "models": [
                { "slug": "both", "context_window": 262144, "max_context_window": 400000 },
                { "slug": "max-only", "max_context_window": 400000 },
                { "slug": "neither" },
                { "slug": "bad-window-good-max", "context_window": "lots",
                  "max_context_window": 400000 }
            ] }"#,
            "p",
        )
        .unwrap();
        let by_id: std::collections::HashMap<&str, &(DiscoveredModel, bool)> = parsed
            .iter()
            .map(|entry| (entry.0.model.as_str(), entry))
            .collect();

        assert_eq!(
            by_id["both"].0.context_window,
            Some(262_144),
            "the specific window wins over the maximum"
        );
        assert_eq!(by_id["max-only"].0.context_window, Some(400_000));
        assert_eq!(by_id["neither"].0.context_window, None);
        assert_eq!(
            by_id["bad-window-good-max"].0.context_window,
            Some(400_000),
            "an unusable window still falls back"
        );
        assert!(
            by_id["bad-window-good-max"].1,
            "but the unusable field is still reported rather than hidden by the fallback"
        );
    }

    /// The last structurally-typed field: a `supported_reasoning_levels` that is not a list must
    /// cost that field, not the model — and must read as silence, since suppressing the harness
    /// overlay on the strength of a malformed value is worse than filling it.
    #[test]
    fn a_non_list_effort_field_is_silence_not_a_dropped_model() {
        let parsed = parse_models_body(
            br#"{ "models": [
                { "slug": "kept", "context_window": 262144,
                  "supported_reasoning_levels": "high" }
            ] }"#,
            "p",
        )
        .unwrap();
        assert_eq!(parsed.len(), 1, "the model survives");
        assert_eq!(parsed[0].0.context_window, Some(262_144));
        assert_eq!(
            parsed[0].0.reasoning_efforts, None,
            "a malformed field is not an answer about efforts"
        );
    }

    /// Codex sorts the catalog by `priority` before building its picker list, so a provider serving
    /// that shape is stating an order. Ties and unprioritized entries keep the listed order.
    #[test]
    fn a_stated_priority_orders_the_catalog() {
        let parsed = parse_models_body(
            br#"{ "models": [
                { "slug": "third", "priority": 30 },
                { "slug": "first", "priority": 10 },
                { "slug": "unranked-a" },
                { "slug": "second", "priority": 20 },
                { "slug": "unranked-b" }
            ] }"#,
            "p",
        )
        .unwrap();
        let ids: Vec<&str> = parsed.iter().map(|(m, _)| m.model.as_str()).collect();
        assert_eq!(
            ids,
            ["first", "second", "third", "unranked-a", "unranked-b"],
            "ranked models come first in priority order, the rest keep the provider's order"
        );
    }

    /// Field-by-field tolerance has to hold inside an entry too: a `display_name` that arrives as a
    /// number costs the name, not the model.
    #[test]
    fn malformed_optional_metadata_costs_only_that_field() {
        let parsed = parse_models_body(
            br#"{ "models": [ {
                "slug": "kept",
                "display_name": 7,
                "visibility": 1,
                "priority": "high",
                "context_window": 262144,
                "supported_reasoning_levels": [ "low", { "effort": "high" }, { "nope": 1 } ]
            } ] }"#,
            "p",
        )
        .unwrap();
        assert_eq!(parsed.len(), 1, "the entry survives its bad fields");
        let model = &parsed[0].0;
        assert_eq!(model.model, "kept");
        assert_eq!(
            model.context_window,
            Some(262_144),
            "the good fields still land"
        );
        assert_eq!(model.display_name, None, "a non-string name is no name");
        assert_eq!(
            model.reasoning_efforts.as_deref(),
            Some(&["high".to_string()][..]),
            "unreadable levels are skipped, the readable one is kept"
        );
        assert_eq!(
            model.priority, None,
            "a non-numeric priority is no priority"
        );
    }

    /// The OpenAI-compatible shape still parses, and still carries only what it carried before.
    #[test]
    fn an_openai_body_still_parses() {
        let body = br#"{ "data": [
            { "id": "plain" },
            { "id": "sized", "max_input_tokens": 131072 }
        ] }"#;
        let parsed = parse_models_body(body, "litellm").expect("the OpenAI shape should parse");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0.model, "plain");
        assert_eq!(parsed[0].0.context_window, None);
        assert_eq!(parsed[0].0.display_name, None);
        assert_eq!(
            parsed[0].0.reasoning_efforts, None,
            "the OpenAI shape says nothing about efforts"
        );
        assert_eq!(parsed[1].0.context_window, Some(131_072));
    }

    /// Capacity metadata that is present but unusable is reported, not silently dropped — in the
    /// catalog shape the same way as in the OpenAI one.
    #[test]
    fn unusable_capacity_metadata_is_flagged_in_either_shape() {
        let catalog = parse_models_body(
            br#"{ "models": [ { "slug": "m", "context_window": "lots" } ] }"#,
            "p",
        )
        .unwrap();
        assert!(catalog[0].1, "catalog: invalid capacity should be flagged");
        assert_eq!(catalog[0].0.context_window, None);

        let openai = parse_models_body(
            br#"{ "data": [ { "id": "m", "context_window": "lots" } ] }"#,
            "p",
        )
        .unwrap();
        assert!(openai[0].1, "openai: invalid capacity should be flagged");
        assert_eq!(openai[0].0.context_window, None);
    }

    /// Asking for the catalog does not guarantee one, and `data`/`models` are different keys, so a
    /// provider may answer both. Neither list may be dropped: the catalog refines, it does not
    /// replace.
    #[test]
    fn both_lists_combine_with_the_catalog_taking_precedence() {
        let body = br#"{
            "data": [
                { "id": "shared", "context_window": 8192 },
                { "id": "openai-only", "max_input_tokens": 4096 }
            ],
            "models": [
                {
                    "slug": "shared",
                    "display_name": "Shared",
                    "context_window": 262144,
                    "supported_reasoning_levels": [ { "effort": "high", "description": "d" } ]
                },
                { "slug": "catalog-only", "display_name": "Catalog Only" }
            ]
        }"#;
        let parsed = parse_models_body(body, "opencodex").expect("both lists should parse");
        let by_id: std::collections::HashMap<&str, &DiscoveredModel> =
            parsed.iter().map(|(m, _)| (m.model.as_str(), m)).collect();
        assert_eq!(by_id.len(), 3, "no model may be dropped: {parsed:?}");

        // Present in both ⇒ the catalog's richer values win.
        let shared = by_id["shared"];
        assert_eq!(shared.context_window, Some(262_144));
        assert_eq!(shared.display_name.as_deref(), Some("Shared"));
        assert_eq!(
            shared.reasoning_efforts.as_deref(),
            Some(&["high".to_string()][..])
        );

        // Present only in `data` ⇒ kept as it was.
        let openai_only = by_id["openai-only"];
        assert_eq!(openai_only.context_window, Some(4096));
        assert_eq!(openai_only.display_name, None);

        // Present only in `models` ⇒ added.
        assert_eq!(
            by_id["catalog-only"].display_name.as_deref(),
            Some("Catalog Only")
        );

        // The provider's ordering survives, with a refined entry staying where it first appeared.
        let ids: Vec<&str> = parsed.iter().map(|(m, _)| m.model.as_str()).collect();
        assert_eq!(ids, ["shared", "openai-only", "catalog-only"]);
    }

    /// "Precedence" is per field, not wholesale: a catalog entry that says nothing about a window
    /// must not erase one the OpenAI entry supplied.
    #[test]
    fn a_silent_catalog_field_does_not_erase_the_openai_value() {
        let body = br#"{
            "data": [ { "id": "m", "context_window": 8192 } ],
            "models": [ { "slug": "m", "display_name": "M" } ]
        }"#;
        let parsed = parse_models_body(body, "p").unwrap();
        assert_eq!(
            parsed[0].0.context_window,
            Some(8192),
            "the window survives"
        );
        assert_eq!(parsed[0].0.display_name.as_deref(), Some("M"));
    }

    /// Hidden in the catalog means hidden, even when the OpenAI list named the same model.
    #[test]
    fn a_catalog_hidden_model_is_dropped_from_the_openai_list_too() {
        let body = br#"{
            "data": [ { "id": "internal" }, { "id": "public" } ],
            "models": [ { "slug": "internal", "visibility": "hide" } ]
        }"#;
        let parsed = parse_models_body(body, "p").unwrap();
        let ids: Vec<&str> = parsed.iter().map(|(m, _)| m.model.as_str()).collect();
        assert_eq!(ids, ["public"]);
    }

    /// The two lists are independent claims. One unreadable entry — or a key a provider happens to
    /// use for something else — must not take the other list down with it.
    #[test]
    fn a_bad_entry_or_key_does_not_discard_the_other_list() {
        // `models` is not a list at all; `data` still serves.
        let parsed = parse_models_body(
            br#"{ "models": { "count": 2 }, "data": [ { "id": "kept" } ] }"#,
            "p",
        )
        .expect("a non-list `models` should not fail the body");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0.model, "kept");

        // One catalog entry is unreadable (no `slug`); its siblings and `data` survive.
        let parsed = parse_models_body(
            br#"{
                "models": [ { "no_slug": true }, { "slug": "good" } ],
                "data": [ { "id": "from-data" } ]
            }"#,
            "p",
        )
        .expect("one bad entry should not fail the body");
        let ids: Vec<&str> = parsed.iter().map(|(m, _)| m.model.as_str()).collect();
        assert_eq!(ids, ["from-data", "good"]);

        // Same in the other direction: an entry with no `id` does not cost the catalog.
        let parsed = parse_models_body(
            br#"{ "data": [ { "nope": 1 } ], "models": [ { "slug": "good" } ] }"#,
            "p",
        )
        .expect("one bad data entry should not fail the body");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0.model, "good");
    }

    #[test]
    fn a_body_in_neither_shape_is_an_error() {
        parse_models_body(br#"{ "unexpected": true }"#, "p")
            .expect_err("neither shape ⇒ report it rather than silently finding no models");
    }

    /// Discovery metadata reaches the picker: a catalog-sourced model arrives with its window,
    /// name, and effort list rather than the conservative default.
    #[test]
    fn merge_carries_catalog_metadata_onto_new_models() {
        let discovered = DiscoveredModel {
            provider: "opencodex".into(),
            model: "gpt-5.5".into(),
            context_window: Some(262_144),
            display_name: Some("GPT-5.5".into()),
            reasoning_efforts: Some(vec!["low".into(), "high".into()]),
            priority: None,
        };
        let merged = merge_models(Vec::new(), std::slice::from_ref(&discovered));
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].context_window, 262_144);
        assert_eq!(merged[0].display_name.as_deref(), Some("GPT-5.5"));
        assert!(merged[0].supports_reasoning_effort);
        assert_eq!(merged[0].reasoning_efforts, ["low", "high"]);
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
                ..DiscoveredModel::default()
            },
            // New id with no metadata ⇒ added with a conservative descriptor.
            DiscoveredModel {
                provider: "cloudflare-litellm".into(),
                model: "@cf/meta/llama-4".into(),
                context_window: None,
                ..DiscoveredModel::default()
            },
            // A provider-advertised context window is retained for a new model.
            DiscoveredModel {
                provider: "cloudflare-litellm".into(),
                model: "provider-sized".into(),
                context_window: Some(258_400),
                ..DiscoveredModel::default()
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
                is_default: false,
            },
            ModelDescriptor {
                provider: "cloudflare-litellm".into(),
                model: "@cf/z-ai/glm-4.7".into(),
                context_window: 131_072,
                supports_reasoning_effort: false,
                reasoning_efforts: Vec::new(),
                display_name: Some("GLM-4.7".into()),
                is_default: false,
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
                is_default: false,
            },
            ModelDescriptor {
                provider: String::new(),
                model: "@cf/z-ai/glm-4.7".into(),
                context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
                supports_reasoning_effort: true,
                reasoning_efforts: vec!["medium".into()],
                display_name: Some("GLM 4.7".into()),
                is_default: false,
            },
        ];

        let merged = apply_harness_metadata(base, &harness, &config, &HashSet::new());

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

    /// Declaring a provider is how a user pins it first (§8.3). That has to hold for a provider
    /// whose models come only from the harness catalog — a stock `openai` has no `base_url`, so
    /// nothing else supplies it, and appending the catalog last would park the pinned provider
    /// behind an undeclared one's discovered models.
    #[test]
    fn a_pinned_provider_leads_even_when_only_the_catalog_supplies_it() {
        let config: Config = toml::from_str("[providers.openai]\n").unwrap();
        // What discovery produced: an undeclared provider, so it sorts after the declared one.
        let discovered = vec![ModelDescriptor::conservative(
            "zzz".to_string(),
            "model-a".to_string(),
        )];
        let harness = vec![ModelDescriptor {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            context_window: 128_000,
            supports_reasoning_effort: false,
            reasoning_efforts: Vec::new(),
            display_name: None,
            is_default: false,
        }];

        let out = order_for_picker(
            apply_harness_metadata(discovered, &harness, &config, &HashSet::new()),
            &config,
        );
        let order: Vec<(&str, &str)> = out
            .iter()
            .map(|d| (d.provider.as_str(), d.model.as_str()))
            .collect();
        assert_eq!(
            order,
            [("openai", "gpt-5.5"), ("zzz", "model-a")],
            "the declared provider leads whatever supplied its models"
        );
    }

    /// `model_listing = false` is an opt-out from listing, whatever the source. Without this the
    /// example config's own guarantee — declare `false` and only your declared models are offered
    /// — would be false for the provider the harness routes to.
    #[test]
    fn the_catalog_is_not_appended_under_a_provider_that_opted_out() {
        let config: Config = toml::from_str(
            "[providers.openai]\nmodel_listing = false\n  [[providers.openai.models]]\n  id = \"declared\"\n  context_window = 1000\n",
        )
        .unwrap();
        let harness = vec![ModelDescriptor {
            provider: "openai".into(),
            model: "from-catalog".into(),
            context_window: 128_000,
            supports_reasoning_effort: false,
            reasoning_efforts: Vec::new(),
            display_name: None,
            is_default: false,
        }];
        let out = apply_harness_metadata(
            list_descriptors(&config),
            &harness,
            &config,
            &HashSet::new(),
        );
        let ids: Vec<&str> = out.iter().map(|d| d.model.as_str()).collect();
        assert_eq!(
            ids,
            ["declared"],
            "the opt-out holds for the catalog too: {ids:?}"
        );
    }

    /// `is_default` names one route. Applying it by model id alone would flag a same-named model
    /// under an unrelated provider, and with the catalog's own entry appended both would claim it
    /// — the picker starts on whichever comes first, which is not the one the harness meant.
    #[test]
    fn is_default_follows_the_route_not_just_the_model_id() {
        let config: Config = toml::from_str(
            "[providers.litellm]\n  [[providers.litellm.models]]\n  id = \"gpt-5.5\"\n  context_window = 1000\n",
        )
        .unwrap();
        let harness = vec![ModelDescriptor {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            context_window: 128_000,
            supports_reasoning_effort: false,
            reasoning_efforts: Vec::new(),
            display_name: Some("GPT-5.5".into()),
            is_default: true,
        }];
        let out = apply_harness_metadata(
            list_descriptors(&config),
            &harness,
            &config,
            &HashSet::new(),
        );

        let litellm = out
            .iter()
            .find(|d| d.provider == "litellm")
            .expect("the declared entry survives");
        assert!(
            !litellm.is_default,
            "another provider's route must not inherit the default flag"
        );
        assert_eq!(
            litellm.display_name.as_deref(),
            Some("GPT-5.5"),
            "but the name still crosses providers — it describes the model, not the route"
        );
        let openai = out
            .iter()
            .find(|d| d.provider == "openai")
            .expect("the catalog's own entry is appended");
        assert!(openai.is_default, "and it is the one the harness marked");
    }

    /// A pair the catalog offers is not a stale provider. Rewriting it would send the turn to a
    /// provider the user did not pick.
    #[test]
    fn a_catalog_pair_is_not_normalized_away() {
        let config = config_with_glm();
        let picked = ModelRef {
            provider: "openai".into(),
            model: "@cf/z-ai/glm-4.7".into(),
            reasoning_effort: None,
        };
        let catalog = vec![ModelDescriptor::conservative(
            "openai".to_string(),
            "@cf/z-ai/glm-4.7".to_string(),
        )];
        assert_eq!(
            normalize_model_ref(&config, &catalog, &picked),
            picked,
            "the catalog offers this exact route, so it stands"
        );
        assert_eq!(
            normalize_model_ref(&config, &[], &picked).provider,
            "cloudflare-litellm",
            "with no catalog entry it is still repaired as before"
        );
    }

    /// A provider's own catalog outranks `model/list` on effort levels, per §8.3: `model/list` is
    /// keyed by model id and knows nothing about providers, so it must not replace the levels a
    /// specific provider advertised for its own model. Without this, the whole point of reading the
    /// richer shape is undone for any model whose id the harness happens to share.
    #[test]
    fn discovered_efforts_survive_the_harness_overlay() {
        let config: Config = toml::from_str("").unwrap();
        // What discovery produced: the provider advertised three levels for its own model.
        let discovered = ModelDescriptor {
            provider: "opencodex".into(),
            model: "gpt-5.5".into(),
            context_window: 262_144,
            supports_reasoning_effort: true,
            reasoning_efforts: vec!["low".into(), "high".into(), "xhigh".into()],
            display_name: Some("GPT-5.5".into()),
            is_default: false,
        };
        // What `model/list` says about a model with the same id, under no provider at all.
        let harness = vec![ModelDescriptor {
            provider: String::new(),
            model: "gpt-5.5".into(),
            context_window: 0,
            supports_reasoning_effort: true,
            reasoning_efforts: vec!["low".into(), "medium".into()],
            display_name: Some("Other".into()),
            is_default: false,
        }];

        // Discovery spoke about this pair, so the overlay must leave its efforts alone.
        let stated = HashSet::from([("opencodex".to_string(), "gpt-5.5".to_string())]);
        let out = apply_harness_metadata(vec![discovered], &harness, &config, &stated);
        assert_eq!(
            out[0].reasoning_efforts,
            ["low", "high", "xhigh"],
            "the provider's advertised levels must survive"
        );
        assert!(out[0].supports_reasoning_effort);
        assert_eq!(
            out[0].display_name.as_deref(),
            Some("GPT-5.5"),
            "and its name, which was already guarded"
        );
    }

    /// A provider that says a model has *no* effort levels is making a statement, not leaving a
    /// gap. The harness catalog must not answer it — `model/list` is keyed by model id and would
    /// hand the model levels its own provider just disclaimed.
    #[test]
    fn a_provider_saying_no_efforts_is_not_a_gap_to_fill() {
        let config: Config = toml::from_str("").unwrap();
        let discovered = ModelDescriptor {
            provider: "opencodex".into(),
            model: "gpt-5.5".into(),
            context_window: 262_144,
            supports_reasoning_effort: false,
            reasoning_efforts: Vec::new(),
            display_name: None,
            is_default: false,
        };
        let harness = vec![ModelDescriptor {
            provider: String::new(),
            model: "gpt-5.5".into(),
            context_window: 0,
            supports_reasoning_effort: true,
            reasoning_efforts: vec!["low".into(), "medium".into()],
            display_name: None,
            is_default: false,
        }];

        let stated = HashSet::from([("opencodex".to_string(), "gpt-5.5".to_string())]);
        let out = apply_harness_metadata(vec![discovered], &harness, &config, &stated);
        assert!(
            out[0].reasoning_efforts.is_empty(),
            "an explicit 'none' must not be topped up: {:?}",
            out[0].reasoning_efforts
        );
        assert!(
            !out[0].supports_reasoning_effort,
            "and the selector stays hidden"
        );
    }

    /// The gap-filling still works: a model discovery only named takes the harness's levels.
    #[test]
    fn the_harness_overlay_still_fills_an_empty_effort_list() {
        let config: Config = toml::from_str("").unwrap();
        let discovered = ModelDescriptor {
            provider: "litellm".into(),
            model: "gpt-5.5".into(),
            context_window: 128_000,
            supports_reasoning_effort: false,
            reasoning_efforts: Vec::new(),
            display_name: None,
            is_default: false,
        };
        let harness = vec![ModelDescriptor {
            provider: String::new(),
            model: "gpt-5.5".into(),
            context_window: 0,
            supports_reasoning_effort: true,
            reasoning_efforts: vec!["low".into(), "medium".into()],
            display_name: Some("GPT-5.5".into()),
            is_default: false,
        }];

        let out = apply_harness_metadata(vec![discovered], &harness, &config, &HashSet::new());
        assert_eq!(out[0].reasoning_efforts, ["low", "medium"]);
        assert!(out[0].supports_reasoning_effort);
        assert_eq!(out[0].display_name.as_deref(), Some("GPT-5.5"));
    }

    #[test]
    fn harness_metadata_precedence_matrix() {
        // config declares two models: one with a name, one without. Both have efforts off.
        let config: Config = toml::from_str(
            r#"
[providers.p]
  [[providers.p.models]]
  id = "declared-named"
  display_name = "Config Name"
  context_window = 1000
  supports_reasoning_effort = false
  [[providers.p.models]]
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
            is_default: false,
        };
        // Harness catalog entry (empty provider) with a name and effort list.
        let cat = |model: &str, name: &str, efforts: &[&str]| ModelDescriptor {
            provider: String::new(),
            model: model.into(),
            context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: !efforts.is_empty(),
            reasoning_efforts: efforts.iter().map(|e| (*e).to_string()).collect(),
            display_name: Some(name.into()),
            is_default: false,
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

        let merged = apply_harness_metadata(base, &harness, &config, &HashSet::new());
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
            is_default: false,
        }];
        let merged = apply_harness_metadata(base.clone(), &unsupported, &config, &HashSet::new());
        assert!(!merged[0].supports_reasoning_effort);
        assert!(merged[0].reasoning_efforts.is_empty());

        let default_only = vec![ModelDescriptor {
            provider: String::new(),
            model: "gpt-5.5".into(),
            context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
            supports_reasoning_effort: true,
            reasoning_efforts: Vec::new(),
            display_name: Some("GPT-5.5".into()),
            is_default: false,
        }];
        let merged = apply_harness_metadata(base, &default_only, &config, &HashSet::new());
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
[providers.configured]
  [[providers.configured.models]]
  id = "shared-model"
  context_window = 1000
  supports_reasoning_effort = false

[providers.discovered]
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
            is_default: false,
        }];

        let merged = apply_harness_metadata(base, &harness, &config, &HashSet::new());
        assert!(merged[0].supports_reasoning_effort);
        assert_eq!(merged[0].reasoning_efforts, vec!["focused"]);
    }

    #[test]
    fn normalizes_stale_provider_when_model_has_one_configured_provider() {
        let config = config_with_glm();
        let normalized = normalize_model_ref(
            &config,
            &[],
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
        config.providers.insert(
            "other".into(),
            giskard_persist::ProviderConfig {
                model_listing: Some(false),
                models: vec![giskard_persist::ModelConfig {
                    id: "@cf/z-ai/glm-4.7".into(),
                    display_name: None,
                    context_window: 131_072,
                    supports_reasoning_effort: false,
                }],
            },
        );
        let original = ModelRef {
            provider: "openai".into(),
            model: "@cf/z-ai/glm-4.7".into(),
            reasoning_effort: None,
        };
        assert_eq!(normalize_model_ref(&config, &[], &original), original);
    }
}
