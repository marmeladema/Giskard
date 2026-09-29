//! Configuration loading from `config.toml` (spec Appendix C).

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// Global application configuration (spec Appendix C).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub auth: AuthConfig,
    pub browse: BrowseConfig,
    pub plan: PlanConfig,
    pub tokens: TokensConfig,
    pub viz: VizConfig,
    pub history: HistoryConfig,
    pub retention: RetentionConfig,
    /// Declared providers, keyed by routing id — the same shape Codex uses for
    /// `[model_providers.<id>]`. An `IndexMap` rather than a `HashMap` because the declaration
    /// order is the model picker's order (§8.3): a hashed order would reshuffle the picker on
    /// every restart and change which model a draft starts on when none is marked default.
    pub providers: IndexMap<String, ProviderConfig>,
    /// Declared harnesses, keyed by name (design: *Configuration*). An `IndexMap` because
    /// declaration order decides the default when none is marked. Empty means "not declared", which
    /// `HarnessCatalog::resolve` turns into the synthesized `codex` entry.
    pub harnesses: IndexMap<String, HarnessDeclaration>,
}

/// One `[harnesses.<name>]` entry. The neutral keys are parsed here; everything else is kept as
/// an opaque table for the kind's adapter to type-check at boot, because Codex-specific types
/// stay in the adapter crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessDeclaration {
    pub kind: String,
    #[serde(default)]
    pub default: bool,
    /// Program to spawn. `None` leaves it to the adapter (Codex: `codex` on `PATH`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Extra arguments appended after the adapter's own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Environment overlay for every process this instance spawns.
    #[serde(default, skip_serializing_if = "HarnessEnv::is_empty")]
    pub env: HarnessEnv,
    /// Kind-specific keys, validated by the adapter.
    #[serde(default, flatten)]
    pub options: toml::Table,
}

impl HarnessDeclaration {
    fn synthesized_codex() -> Self {
        Self {
            kind: HarnessCatalog::SYNTHESIZED_NAME.to_string(),
            default: true,
            command: None,
            args: Vec::new(),
            env: HarnessEnv::default(),
            options: toml::Table::new(),
        }
    }
}

/// A declaration's environment overlay (`[harnesses.<name>.env]`).
///
/// Values are literal and may be credentials, so the custom `Debug` implementation prints the
/// variable names only, on the `ProviderHttpHeaders` pattern.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HarnessEnv(IndexMap<String, String>);

impl HarnessEnv {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }
}

impl FromIterator<(String, String)> for HarnessEnv {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl std::fmt::Debug for HarnessEnv {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HarnessEnv")
            .field("names", &self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A `[harnesses]` table that breaks one of the declaration rules. Every message names the
/// `[harnesses.<name>]` key it is about, and never an environment value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HarnessConfigError {
    #[error(
        "{} are all marked `default = true`; at most one harness may be the default",
        .0.iter().map(|name| format!("[harnesses.{name}]")).collect::<Vec<_>>().join(", ")
    )]
    MultipleDefaults(Vec<String>),
    /// The name is quoted in the message, since `[harnesses.]` would not say which key it was.
    #[error("[harnesses.{0:?}] has a blank name; a declaration needs a non-blank table key")]
    BlankName(String),
    #[error("[harnesses.{0}] has a blank `kind`")]
    BlankKind(String),
    #[error("[harnesses.{0}] has a blank `command`; omit the key to use the adapter's default")]
    BlankCommand(String),
    #[error(
        "[harnesses.{declaration}.env] has an invalid variable name {name:?}: names must be \
         non-empty and contain neither `=` nor NUL"
    )]
    InvalidEnvName { declaration: String, name: String },
    #[error("[harnesses.{declaration}.env] variable {name:?} has a value containing NUL")]
    InvalidEnvValue { declaration: String, name: String },
}

/// The declarations after the rules are applied: every name is declared, exactly one is the
/// default, and an empty table has become the synthesized `codex`.
#[derive(Debug, Clone)]
pub struct HarnessCatalog {
    declarations: IndexMap<String, HarnessDeclaration>,
    default: String,
}

impl HarnessCatalog {
    pub const SYNTHESIZED_NAME: &str = "codex";

    /// Apply the declaration rules to `config.harnesses`.
    pub fn resolve(config: &Config) -> Result<Self, HarnessConfigError> {
        if config.harnesses.is_empty() {
            return Ok(Self::synthesized());
        }
        for (name, declaration) in &config.harnesses {
            // Checked first: every other message names the key, which a blank one cannot.
            if name.trim().is_empty() {
                return Err(HarnessConfigError::BlankName(name.clone()));
            }
            if declaration.kind.trim().is_empty() {
                return Err(HarnessConfigError::BlankKind(name.clone()));
            }
            if declaration
                .command
                .as_deref()
                .is_some_and(|command| command.trim().is_empty())
            {
                return Err(HarnessConfigError::BlankCommand(name.clone()));
            }
            for (variable, value) in declaration.env.iter() {
                if variable.is_empty() || variable.contains('=') || variable.contains('\0') {
                    return Err(HarnessConfigError::InvalidEnvName {
                        declaration: name.clone(),
                        name: variable.to_string(),
                    });
                }
                if value.contains('\0') {
                    return Err(HarnessConfigError::InvalidEnvValue {
                        declaration: name.clone(),
                        name: variable.to_string(),
                    });
                }
            }
        }
        let marked: Vec<String> = config
            .harnesses
            .iter()
            .filter(|(_, declaration)| declaration.default)
            .map(|(name, _)| name.clone())
            .collect();
        if marked.len() > 1 {
            return Err(HarnessConfigError::MultipleDefaults(marked));
        }
        // `harnesses` is non-empty here, so a first entry exists.
        let default = match marked.into_iter().next() {
            Some(name) => name,
            None => match config.harnesses.keys().next() {
                Some(name) => name.clone(),
                None => return Ok(Self::synthesized()),
            },
        };
        let declarations = config
            .harnesses
            .iter()
            .map(|(name, declaration)| {
                let mut declaration = declaration.clone();
                declaration.default = *name == default;
                (name.clone(), declaration)
            })
            .collect();
        Ok(Self {
            declarations,
            default,
        })
    }

    /// The catalog an empty table resolves to: one `codex` of kind `codex`.
    pub fn synthesized() -> Self {
        let mut declarations = IndexMap::new();
        declarations.insert(
            Self::SYNTHESIZED_NAME.to_string(),
            HarnessDeclaration::synthesized_codex(),
        );
        Self {
            declarations,
            default: Self::SYNTHESIZED_NAME.to_string(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&HarnessDeclaration> {
        self.declarations.get(name)
    }

    pub fn default_name(&self) -> &str {
        &self.default
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.declarations.keys().map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &HarnessDeclaration)> {
        self.declarations
            .iter()
            .map(|(name, declaration)| (name.as_str(), declaration))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub file: FileLoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FileLoggingConfig {
    pub enabled: bool,
    /// File-name prefix. Relative paths are resolved from the Giskard data directory using normal
    /// filesystem path semantics, including `..` components.
    #[serde(deserialize_with = "deserialize_non_empty_log_path")]
    pub path: String,
}

fn deserialize_non_empty_log_path<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.trim().is_empty() {
        return Err(serde::de::Error::custom(
            "logging file path must not be empty",
        ));
    }
    Ok(value)
}

impl Default for FileLoggingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "logs/giskard-server.log".into(),
        }
    }
}

/// Retention limits for agent-produced content (spec Appendix C).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RetentionConfig {
    /// Maximum retained UTF-8 bytes for completed command output (default: 128 MiB).
    #[serde(deserialize_with = "deserialize_command_output_limit")]
    pub max_command_output_bytes: usize,
}

impl RetentionConfig {
    pub const DEFAULT_MAX_COMMAND_OUTPUT_BYTES: usize = 128 * 1024 * 1024;
    pub const MIN_MAX_COMMAND_OUTPUT_BYTES: usize = 32 * 1024;
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_command_output_bytes: Self::DEFAULT_MAX_COMMAND_OUTPUT_BYTES,
        }
    }
}

fn deserialize_command_output_limit<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    if value < RetentionConfig::MIN_MAX_COMMAND_OUTPUT_BYTES {
        return Err(serde::de::Error::custom(format_args!(
            "max_command_output_bytes must be at least {} bytes",
            RetentionConfig::MIN_MAX_COMMAND_OUTPUT_BYTES
        )));
    }
    Ok(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_logging_defaults_disabled_and_validates_path() {
        let defaults: Config = toml::from_str("").unwrap();
        assert!(!defaults.logging.file.enabled);
        assert_eq!(defaults.logging.file.path, "logs/giskard-server.log");

        let configured: Config = toml::from_str(
            "[logging.file]\nenabled = true\npath = \"/var/log/giskard/server.log\"\n",
        )
        .unwrap();
        assert!(configured.logging.file.enabled);
        assert_eq!(configured.logging.file.path, "/var/log/giskard/server.log");

        let error = toml::from_str::<Config>("[logging.file]\npath = \"  \"\n").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("logging file path must not be empty")
        );
    }

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
        assert_eq!(
            config.retention.max_command_output_bytes,
            RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES
        );
        assert!(config.providers.is_empty());
    }

    /// A `config.toml` written before the `[harness]` section was removed keeps parsing: `Config`
    /// ignores unknown tables, so the stale section is dropped rather than refusing startup.
    #[test]
    fn a_removed_harness_table_is_ignored() {
        let config: Config =
            toml::from_str("[harness]\nkind = \"codex\"\nidle_shutdown_secs = 0\n").unwrap();
        let defaults = Config::default();
        assert_eq!(config.server.bind, defaults.server.bind);
        assert_eq!(config.server.secure_cookies, defaults.server.secure_cookies);
        assert_eq!(config.auth.session_days, defaults.auth.session_days);
        assert!(config.providers.is_empty());
        assert_eq!(
            config.retention.max_command_output_bytes,
            defaults.retention.max_command_output_bytes
        );
    }

    #[test]
    fn empty_config_uses_defaults() {
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config.server.bind, "127.0.0.1:8787");
        assert_eq!(
            config.retention.max_command_output_bytes,
            RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES
        );
    }

    #[test]
    fn command_output_retention_limit_accepts_minimum_and_override() {
        for value in [
            RetentionConfig::MIN_MAX_COMMAND_OUTPUT_BYTES,
            RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES + 1,
        ] {
            let config: Config = toml::from_str(&format!(
                "[retention]\nmax_command_output_bytes = {value}\n"
            ))
            .unwrap();
            assert_eq!(config.retention.max_command_output_bytes, value);
        }
    }

    #[test]
    fn command_output_retention_limit_rejects_value_below_minimum() {
        let err = toml::from_str::<Config>("[retention]\nmax_command_output_bytes = 32767\n")
            .expect_err("a command output limit below 32 KiB must not parse");
        assert!(
            err.to_string()
                .contains("max_command_output_bytes must be at least 32768 bytes"),
            "the error should identify the invalid retention limit: {err}"
        );
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
        assert_eq!(
            config.retention.max_command_output_bytes,
            RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES
        );
        assert_eq!(config.providers.len(), 2);
        let catalog = HarnessCatalog::resolve(&config).expect("the example's harnesses resolve");
        assert_eq!(catalog.default_name(), HarnessCatalog::SYNTHESIZED_NAME);
    }

    #[test]
    fn no_harnesses_table_synthesizes_codex() {
        let config: Config = toml::from_str("").unwrap();
        let catalog = HarnessCatalog::resolve(&config).unwrap();
        let entries: Vec<_> = catalog.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "codex");
        assert_eq!(entries[0].1.kind, "codex");
        assert!(entries[0].1.default);
        assert_eq!(catalog.default_name(), "codex");
    }

    #[test]
    fn declared_table_is_exactly_what_it_declares() {
        let config: Config = toml::from_str(
            "[harnesses.stable]\nkind = \"codex\"\n[harnesses.nightly]\nkind = \"codex\"\n",
        )
        .unwrap();
        let catalog = HarnessCatalog::resolve(&config).unwrap();
        assert_eq!(catalog.names().collect::<Vec<_>>(), ["stable", "nightly"]);
        assert_eq!(catalog.default_name(), "stable");
        assert!(catalog.get("codex").is_none());
        assert!(catalog.get("stable").is_some_and(|d| d.default));
        assert!(catalog.get("nightly").is_some_and(|d| !d.default));
    }

    #[test]
    fn a_marked_default_wins_over_order() {
        let config: Config = toml::from_str(
            "[harnesses.stable]\nkind = \"codex\"\n\
             [harnesses.nightly]\nkind = \"codex\"\ndefault = true\n",
        )
        .unwrap();
        let catalog = HarnessCatalog::resolve(&config).unwrap();
        assert_eq!(catalog.default_name(), "nightly");
        assert!(catalog.get("stable").is_some_and(|d| !d.default));
    }

    #[test]
    fn two_defaults_are_an_error() {
        let config: Config = toml::from_str(
            "[harnesses.stable]\nkind = \"codex\"\ndefault = true\n\
             [harnesses.nightly]\nkind = \"codex\"\ndefault = true\n",
        )
        .unwrap();
        let err = HarnessCatalog::resolve(&config).unwrap_err();
        assert_eq!(
            err,
            HarnessConfigError::MultipleDefaults(vec!["stable".into(), "nightly".into()])
        );
        let message = err.to_string();
        assert!(message.contains("[harnesses.stable]"), "{message}");
        assert!(message.contains("[harnesses.nightly]"), "{message}");
    }

    #[test]
    fn blank_declaration_names_are_errors() {
        for (key, name) in [("\"\"", ""), ("\" \"", " ")] {
            let config: Config =
                toml::from_str(&format!("[harnesses.{key}]\nkind = \"codex\"\n")).unwrap();
            let err = HarnessCatalog::resolve(&config).unwrap_err();
            assert_eq!(err, HarnessConfigError::BlankName(name.into()));
            let message = err.to_string();
            assert!(
                message.starts_with(&format!("[harnesses.{name:?}] has a blank name")),
                "{message}"
            );
        }
    }

    #[test]
    fn blank_kind_and_command_are_errors() {
        let config: Config = toml::from_str("[harnesses.x]\nkind = \" \"\n").unwrap();
        let err = HarnessCatalog::resolve(&config).unwrap_err();
        assert_eq!(err, HarnessConfigError::BlankKind("x".into()));
        assert!(err.to_string().contains("[harnesses.x]"));

        let config: Config =
            toml::from_str("[harnesses.x]\nkind = \"codex\"\ncommand = \"\"\n").unwrap();
        let err = HarnessCatalog::resolve(&config).unwrap_err();
        assert_eq!(err, HarnessConfigError::BlankCommand("x".into()));
        assert!(err.to_string().contains("[harnesses.x]"));
    }

    #[test]
    fn kind_specific_keys_are_kept_opaque() {
        let config: Config =
            toml::from_str("[harnesses.x]\nkind = \"codex\"\nprofile = \"p\"\n").unwrap();
        let declaration = &config.harnesses["x"];
        assert_eq!(declaration.kind, "codex");
        assert_eq!(declaration.options["profile"].as_str(), Some("p"));
        assert!(!declaration.options.contains_key("kind"));
    }

    #[test]
    fn env_names_are_validated() {
        for (name, expected) in [("A=B", "A=B"), ("", "")] {
            let src = format!(
                "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\n\"{name}\" = \"secret-value\"\n"
            );
            let config: Config = toml::from_str(&src).unwrap();
            let err = HarnessCatalog::resolve(&config).unwrap_err();
            assert_eq!(
                err,
                HarnessConfigError::InvalidEnvName {
                    declaration: "x".into(),
                    name: expected.into(),
                }
            );
            let message = err.to_string();
            assert!(message.contains("[harnesses.x.env]"), "{message}");
            assert!(message.contains(&format!("{expected:?}")), "{message}");
            assert!(!message.contains("secret-value"), "{message}");
        }

        let config: Config = toml::from_str(
            "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\nTOKEN = \"secret\\u0000value\"\n",
        )
        .unwrap();
        let err = HarnessCatalog::resolve(&config).unwrap_err();
        assert_eq!(
            err,
            HarnessConfigError::InvalidEnvValue {
                declaration: "x".into(),
                name: "TOKEN".into(),
            }
        );
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn env_debug_redacts_values() {
        let config: Config = toml::from_str(
            "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\nCODEX_HOME = \"/very/secret/home\"\n",
        )
        .unwrap();
        let env = &config.harnesses["x"].env;
        assert_eq!(env.get("CODEX_HOME"), Some("/very/secret/home"));
        let debug = format!("{env:?}");
        assert!(debug.contains("CODEX_HOME"), "{debug}");
        assert!(!debug.contains("/very/secret/home"), "{debug}");
        let declaration_debug = format!("{:?}", config.harnesses["x"]);
        assert!(!declaration_debug.contains("/very/secret/home"));
    }
}
