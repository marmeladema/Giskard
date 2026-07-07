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
roots = ["/home/elie/dev"]

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
        assert_eq!(config.browse.roots, vec!["/home/elie/dev"]);
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
}
