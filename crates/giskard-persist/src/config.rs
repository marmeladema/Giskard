//! Configuration loading from `config.toml` (spec Appendix C).

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// Global application configuration (spec Appendix C).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub browse: BrowseConfig,
    pub plan: PlanConfig,
    pub tokens: TokensConfig,
    pub viz: VizConfig,
    pub history: HistoryConfig,
    /// Declared providers, keyed by routing id — the same shape Codex uses for
    /// `[model_providers.<id>]`. An `IndexMap` rather than a `HashMap` because the declaration
    /// order is the model picker's order (§8.3): a hashed order would reshuffle the picker on
    /// every restart and change which model a draft starts on when none is marked default.
    pub providers: IndexMap<String, ProviderConfig>,
    pub harness: HarnessConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind: String,
    pub secure_cookies: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8787".into(),
            secure_cookies: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub password_hash: Option<String>,
    pub session_days: u32,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            password_hash: None,
            session_days: 30,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BrowseConfig {
    /// Empty/unset ⇒ entire filesystem browsable.
    pub roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PlanConfig {
    pub default_dir: String,
    pub filename_template: String,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            default_dir: "docs".into(),
            filename_template: "plan-{slug}-{ts}.md".into(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TokensConfig {
    pub cost_estimation: bool,
    /// Per-model €/Mtok rates, keyed by `"provider/model"` (spec §10.4, Appendix C). Only used
    /// when `cost_estimation` is true. Human-authored config, so the interpolated string key is
    /// fine here (unlike the persisted `by_model` ledger, which is nested — C3).
    #[serde(default)]
    pub rates: std::collections::HashMap<String, ModelRate>,
}

/// Per-model cost rate in euros per million tokens (spec §10.4).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelRate {
    pub input_per_mtok_eur: f64,
    pub output_per_mtok_eur: f64,
}

/// History paging configuration (spec §13.6, H4/H6).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryConfig {
    /// Turns loaded when a thread is first opened. Kept deliberately small: a turn can contain an
    /// arbitrary number of items, so a turn count is a poor proxy for screen height. The browser
    /// renders the live turn first, then tops this initial page up to fill roughly two viewports
    /// (see `HISTORY_FILL_SCREENS` in `app.js`), so most threads never fetch more than this.
    pub initial: usize,
    /// Turns loaded per "scroll up" page. Small for the same reason as `initial`: a turn is not a
    /// fixed amount of content, so loading many at once can pull far more than a screen.
    pub page: usize,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            initial: 5,
            page: 5,
        }
    }
}

/// Visualization configuration (spec §11.3).
///
/// Controls the maximum file size for syntax highlighting. Files exceeding
/// this threshold return an empty HTML body with metadata only.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VizConfig {
    /// Maximum file size in bytes for syntax highlighting (default: 10 MiB).
    pub max_highlight_size: usize,
}

impl Default for VizConfig {
    fn default() -> Self {
        Self {
            max_highlight_size: 10 * 1024 * 1024,
        }
    }
}

/// A provider declaration (spec Appendix C).
///
/// Deliberately minimal: a provider's display name, endpoint, and key location are the harness's
/// configuration (for Codex, `~/.codex/config.toml`), and Giskard reads them back through
/// `AgentHarness::list_providers` rather than asking for them a second time here. What is left is
/// what no harness can supply — which `(provider, model)` pairs to offer, and the context window
/// for each (§8.3).
///
/// Unknown keys are rejected: this file is written by hand, so a key Giskard does not recognise is
/// a typo, an `id` left over from the array-of-tables form this replaced, or — the one the table
/// key introduced — an id with a dot in it left unquoted, where `[providers.openrouter.ai]` is a
/// provider `openrouter` with a sub-table rather than a provider `openrouter.ai`. Reporting the
/// key beats silently offering no models under a provider the user did not name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Whether to merge `GET {base_url}/models` discovery over the declared models, using the
    /// endpoint the harness reports for this provider.
    ///
    /// Unset means on (§8.3). A provider the harness reports is one the user already declared to
    /// the harness; making them name it again here was ceremony, and in practice almost nobody
    /// declares models by hand — discovery is how a new model shows up under the right slug at all.
    /// `false` turns it off.
    ///
    /// Tri-state rather than a `true` default because "asked for" and "on by default" want
    /// different behaviour when a provider cannot be discovered: an explicit `true` that cannot
    /// work is worth a warning, while a defaulted-on provider with nothing to query is not.
    #[serde(default)]
    pub model_listing: Option<bool>,
    #[serde(default)]
    pub models: Vec<ModelConfig>,
}

/// A typed model entry within a provider (spec §8.3 / Appendix C).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub context_window: u32,
    #[serde(default)]
    pub supports_reasoning_effort: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HarnessConfig {
    pub kind: String,
    pub idle_shutdown_secs: u64,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            kind: "codex".into(),
            idle_shutdown_secs: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let toml = r#"
[server]
bind = "127.0.0.1:8787"
secure_cookies = true

[auth]
password_hash = "$argon2id$v=19$m=…"
session_days = 30

[browse]
roots = ["/home/user/dev"]

[plan]
default_dir = "docs"
filename_template = "plan-{slug}-{ts}.md"

[tokens]
cost_estimation = false

[providers.openai]
model_listing = false

  [[providers.openai.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true

  [[providers.openai.models]]
  id = "gpt-5.4"
  display_name = "GPT-5.4"
  context_window = 262144
  supports_reasoning_effort = true

[providers.cloudflare-litellm]
model_listing = true

  [[providers.cloudflare-litellm.models]]
  id = "@cf/z-ai/glm-4.7"
  display_name = "GLM-4.7 (Workers AI)"
  context_window = 131072
  supports_reasoning_effort = false

[harness]
kind = "codex"
idle_shutdown_secs = 0
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.server.bind, "127.0.0.1:8787");
        assert_eq!(config.browse.roots, vec!["/home/user/dev"]);
        // Declaration order, not hash order: the picker lists providers as the file does.
        assert_eq!(
            config.providers.keys().collect::<Vec<_>>(),
            ["openai", "cloudflare-litellm"]
        );
        let openai = &config.providers["openai"];
        assert_eq!(openai.models.len(), 2);
        assert_eq!(openai.models[0].context_window, 262144);
        assert!(openai.models[0].supports_reasoning_effort);
        let litellm = &config.providers["cloudflare-litellm"];
        assert_eq!(litellm.models[0].id, "@cf/z-ai/glm-4.7");
        assert!(!litellm.models[0].supports_reasoning_effort);
        assert_eq!(config.harness.kind, "codex");
    }

    /// Two providers with the same routing id is a config mistake, and keying the table by id
    /// makes TOML itself catch it. The array-of-tables form this replaced accepted the duplicate
    /// and silently used whichever came first.
    #[test]
    fn a_duplicate_provider_id_is_a_parse_error() {
        let err = toml::from_str::<Config>(
            r#"
[providers.openai]
model_listing = false

[providers.openai]
model_listing = true
"#,
        )
        .expect_err("a repeated provider id must not parse");
        assert!(
            err.to_string().contains("openai"),
            "the error should name the duplicated id: {err}"
        );
    }

    /// A provider id that is not a bare TOML key has to be quoted, and the unquoted form is a
    /// dotted path rather than an id: `[providers.openrouter.ai]` declares a provider `openrouter`
    /// with a sub-table `ai`. `deny_unknown_fields` on [`ProviderConfig`] is what turns that into
    /// an error pointing at the offending segment instead of a provider silently missing its
    /// models. (The array-of-tables form this replaced carried the id as a string value, where a
    /// dot meant nothing.)
    #[test]
    fn a_dotted_provider_id_must_be_quoted() {
        let err = toml::from_str::<Config>(
            r#"
[providers.openrouter.ai]
model_listing = true
"#,
        )
        .expect_err("an unquoted dotted id must not be read as a provider named `openrouter`");
        assert!(
            err.to_string().contains("unknown field `ai`"),
            "the error should name the stray path segment: {err}"
        );

        let config: Config = toml::from_str(
            r#"
[providers."openrouter.ai"]
model_listing = true
  [[providers."openrouter.ai".models]]
  id = "z-ai/glm-4.7"
  context_window = 131072
"#,
        )
        .expect("the quoted form is the way to write it");
        let provider = &config.providers["openrouter.ai"];
        assert_eq!(provider.model_listing, Some(true));
        assert_eq!(provider.models.len(), 1);
    }

    /// `config.toml` is written by hand, so an unrecognised key is a mistake worth reporting rather
    /// than ignoring — including an `id` left behind by a config half-converted from the
    /// array-of-tables form, which would otherwise be silently dropped while the table key it
    /// disagrees with is what actually routes.
    #[test]
    fn an_unknown_provider_key_is_a_parse_error() {
        for src in [
            "[providers.openai]\nid = \"totally-different\"\n",
            "[providers.openai]\nmodel_listings = true\n",
        ] {
            let err = toml::from_str::<Config>(src)
                .expect_err("an unrecognised provider key must not parse");
            assert!(
                err.to_string().contains("unknown field"),
                "expected an unknown-field error for {src:?}, got: {err}"
            );
        }
    }

    #[test]
    fn default_config() {
        let config = Config::default();
        assert_eq!(config.server.bind, "127.0.0.1:8787");
        assert!(config.server.secure_cookies);
        assert_eq!(config.auth.session_days, 30);
        assert!(config.providers.is_empty());
        assert_eq!(config.harness.kind, "codex");
    }

    #[test]
    fn empty_config_uses_defaults() {
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config.server.bind, "127.0.0.1:8787");
        assert_eq!(config.harness.kind, "codex");
    }

    #[test]
    fn missing_browse_table_defaults_to_unrestricted_roots() {
        let config: Config = toml::from_str(
            r#"
[server]
bind = "127.0.0.1:8787"

[auth]
password_hash = "hash"
"#,
        )
        .unwrap();

        assert!(config.browse.roots.is_empty());
    }

    #[test]
    fn empty_browse_roots_is_unrestricted_roots() {
        let config: Config = toml::from_str(
            r#"
[browse]
roots = []
"#,
        )
        .unwrap();

        assert!(config.browse.roots.is_empty());
    }

    /// The annotated `config.example.toml` shipped at the repo root must always parse against the
    /// current `Config` structs, so the documented example can't silently drift from the code.
    #[test]
    fn shipped_example_config_parses() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config.example.toml");
        let toml = std::fs::read_to_string(path).expect("read config.example.toml");
        let config: Config = toml::from_str(&toml).expect("config.example.toml parses as Config");
        assert_eq!(config.server.bind, "127.0.0.1:8787");
        // Example intentionally documents plain-HTTP local dev.
        assert!(!config.server.secure_cookies);
        assert_eq!(config.harness.kind, "codex");
        assert_eq!(config.providers.len(), 2);
    }
}
