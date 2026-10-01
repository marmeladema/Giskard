//! The model catalog Claude Code reports in its `initialize` response, and the provider report.

use std::time::Instant;

use giskard_core::model::ModelDescriptor;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

/// The single provider a Claude Code instance routes to.
pub const ANTHROPIC_PROVIDER_ID: &str = "anthropic";

/// The catalog alias that names another entry; never offered as a picker row.
const DEFAULT_ALIAS: &str = "default";

/// One `initialize.models` entry. Unknown keys are ignored.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CatalogEntry {
    /// The selector `--model` takes: an alias such as `sonnet` or a full model id.
    pub value: String,
    #[serde(rename = "resolvedModel")]
    pub resolved_model: Option<String>,
    #[serde(rename = "displayName")]
    pub display_name: Option<String>,
    #[serde(default, rename = "supportsEffort")]
    pub supports_effort: bool,
    #[serde(default, rename = "supportedEffortLevels")]
    pub supported_effort_levels: Vec<String>,
}

/// The latest catalog any child reported.
#[derive(Debug, Clone)]
pub(crate) struct CatalogSnapshot {
    pub entries: Vec<CatalogEntry>,
    pub taken_at: Instant,
    /// `"handshake"` or `"probe"`, for the log line.
    pub source: &'static str,
}

impl CatalogSnapshot {
    pub fn new(entries: Vec<CatalogEntry>, source: &'static str) -> Self {
        Self {
            entries,
            taken_at: Instant::now(),
            source,
        }
    }

    /// The model id the CLI resolves `value` to, when the catalog names it.
    pub fn resolved_model(&self, value: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|entry| entry.value == value)
            .and_then(|entry| entry.resolved_model.as_deref())
    }
}

/// Parse `initialize.models` entry by entry, so one odd entry cannot empty the picker.
pub(crate) fn parse_entries(models: &[Value]) -> Vec<CatalogEntry> {
    models
        .iter()
        .enumerate()
        .filter_map(
            |(index, entry)| match CatalogEntry::deserialize(entry.clone()) {
                Ok(entry) => Some(entry),
                Err(error) => {
                    warn!(
                        action = "catalog_entry",
                        index,
                        error = %crate::frame::redact_serde_error(&error),
                        "skipping a model catalog entry that did not parse"
                    );
                    None
                }
            },
        )
        .collect()
}

/// One descriptor per catalog entry except the `default` alias.
///
/// `is_default` marks the first entry resolving to the same model as `default`: which model that
/// is depends on the user's configuration, so it is read, never assumed. The catalog carries no
/// context window; the runtime window arrives through `TurnUsageUpdated` and, on resume,
/// `ContextWindowRestored`.
pub(crate) fn descriptors(snapshot: &CatalogSnapshot) -> Vec<ModelDescriptor> {
    let default_target = snapshot
        .entries
        .iter()
        .find(|entry| entry.value == DEFAULT_ALIAS)
        .and_then(|entry| entry.resolved_model.as_deref());
    let mut default_marked = false;
    snapshot
        .entries
        .iter()
        .filter(|entry| entry.value != DEFAULT_ALIAS)
        .map(|entry| {
            let is_default = !default_marked
                && default_target.is_some()
                && entry.resolved_model.as_deref() == default_target;
            default_marked |= is_default;
            ModelDescriptor {
                provider: ANTHROPIC_PROVIDER_ID.into(),
                model: entry.value.clone(),
                context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
                supports_reasoning_effort: entry.supports_effort,
                reasoning_efforts: entry.supported_effort_levels.clone(),
                display_name: entry.display_name.clone(),
                is_default,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture_models() -> Vec<Value> {
        let path = format!(
            "{}/tests/fixtures/initialize.out.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).unwrap();
        let first: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        first["response"]["response"]["models"]
            .as_array()
            .unwrap()
            .clone()
    }

    #[test]
    fn the_default_alias_is_dropped_and_its_target_is_marked() {
        let snapshot = CatalogSnapshot::new(parse_entries(&fixture_models()), "handshake");
        let descriptors = descriptors(&snapshot);
        assert!(descriptors.iter().all(|d| d.model != "default"));
        let defaults: Vec<_> = descriptors.iter().filter(|d| d.is_default).collect();
        assert_eq!(defaults.len(), 1);
        assert_eq!(defaults[0].model, "opus");
        assert!(
            descriptors
                .iter()
                .all(|d| d.provider == ANTHROPIC_PROVIDER_ID)
        );
        assert!(
            descriptors
                .iter()
                .all(|d| d.context_window == ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW)
        );
        let haiku = descriptors.iter().find(|d| d.model == "haiku").unwrap();
        assert!(!haiku.supports_reasoning_effort);
        assert!(haiku.reasoning_efforts.is_empty());
        let sonnet = descriptors.iter().find(|d| d.model == "sonnet").unwrap();
        assert!(sonnet.supports_reasoning_effort);
        assert!(sonnet.reasoning_efforts.contains(&"xhigh".to_string()));
        assert_eq!(snapshot.resolved_model("sonnet"), Some("claude-sonnet-5-5"));
    }

    #[test]
    fn an_odd_entry_is_skipped_not_fatal() {
        let entries = parse_entries(&[json!({"value": "sonnet"}), json!({"value": 3}), json!(7)]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value, "sonnet");
    }
}
