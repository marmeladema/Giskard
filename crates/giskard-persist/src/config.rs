//! Configuration loading from `config.toml` (spec Appendix C).

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
    pub providers: Vec<ProviderConfig>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    pub wire_api: String,
    #[serde(default)]
    pub model_listing: bool,
    /// API key sent as `Authorization: Bearer …` on the `/v1/models` discovery request (§8.3),
    /// for endpoints that require auth (e.g. a LiteLLM proxy with a master key). Inline secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Name of an environment variable to read the discovery API key from, so the secret can be
    /// kept out of `config.toml`. Used only when `api_key` is unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub models: Vec<ModelConfig>,
}

impl ProviderConfig {
    /// Resolve the discovery API key: the inline `api_key`, else the value of the env var named by
    /// `api_key_env`. Empty values are treated as unset.
    pub fn resolve_api_key(&self) -> Option<String> {
        if let Some(key) = self.api_key.as_deref() {
            if !key.is_empty() {
                return Some(key.to_string());
            }
        }
        if let Some(var) = self.api_key_env.as_deref() {
            if let Ok(val) = std::env::var(var) {
                if !val.is_empty() {
                    return Some(val);
                }
            }
        }
        None
    }
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

[[providers]]
id = "openai"
name = "OpenAI (Codex built-in)"
wire_api = "responses"
model_listing = false

  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true

  [[providers.models]]
  id = "gpt-5.4"
  display_name = "GPT-5.4"
  context_window = 262144
  supports_reasoning_effort = true

[[providers]]
id = "cloudflare-litellm"
name = "Cloudflare Workers AI (via LiteLLM)"
base_url = "http://127.0.0.1:4000/v1"
wire_api = "responses"
model_listing = true

  [[providers.models]]
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
        assert_eq!(config.providers.len(), 2);
        assert_eq!(config.providers[0].models.len(), 2);
        assert_eq!(config.providers[0].models[0].context_window, 262144);
        assert!(config.providers[0].models[0].supports_reasoning_effort);
        assert_eq!(config.providers[1].models[0].id, "@cf/z-ai/glm-4.7");
        assert!(!config.providers[1].models[0].supports_reasoning_effort);
        assert_eq!(config.harness.kind, "codex");
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
