//! Configuration loading from `config.toml` (spec Appendix C).

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::env_template::{EnvTemplate, EnvTemplateContext, ResolvedEnv, UnsetVariable};

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
    pub disk_usage: DiskUsageConfig,
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
    /// The adapter. Optional on a declaration named after a kind in
    /// [`HarnessCatalog::KINDS_IMPLIED_BY_NAME`], where it is that name;
    /// `HarnessCatalog::resolve` fills it in or refuses the entry, so read it through
    /// [`HarnessDeclaration::kind`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
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
    /// The declaration's kind. Always set on a declaration from a [`HarnessCatalog`]; empty on a
    /// raw one that omits it.
    pub fn kind(&self) -> &str {
        self.kind.as_deref().unwrap_or_default()
    }

    fn synthesized_codex() -> Self {
        Self {
            kind: Some(HarnessCatalog::SYNTHESIZED_NAME.to_string()),
            default: true,
            command: None,
            args: Vec::new(),
            env: HarnessEnv::default(),
            options: toml::Table::new(),
        }
    }
}

/// A declaration's environment overlay (`[harnesses.<name>.env]`), parsed.
///
/// Each value is an [`EnvTemplate`], parsed while `config.toml` is loaded: a value that is not a
/// valid template, or that contains NUL, fails the load like any other bad value, and `toml`
/// reports it with its line. Serializing writes each template back as its source string. Values
/// may be credentials, so the custom `Debug` implementation prints the variable names only, on the
/// `ProviderHttpHeaders` pattern.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct HarnessEnv(IndexMap<String, EnvTemplate>);

impl HarnessEnv {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    pub fn get(&self, name: &str) -> Option<&EnvTemplate> {
        self.0.get(name)
    }

    /// Render every value against one instance's facts, in table order. A value naming a
    /// placeholder with no usable value is skipped: left out of `entries` (never set to an empty
    /// string) and reported in `unset`.
    ///
    /// `{env:NAME}` reads `NAME` as it stands at that line, as a shell would: the value of a
    /// variable rendered earlier in this table, else `lookup_env` (Giskard's environment). A
    /// variable defined *later* is not yet set, so `ORIG_HOME = "{env:HOME}"` above
    /// `HOME = "/sandbox"` keeps Giskard's `HOME`, and a variable's own name reads Giskard's value,
    /// so `PATH = "/x:{env:PATH}"` extends Giskard's `PATH`. A skipped variable does not count as
    /// defined: a later `{env:NAME}` reads it as if its line were not there.
    pub fn resolve(
        &self,
        context: &EnvTemplateContext<'_>,
        lookup_env: &dyn Fn(&str) -> Result<String, std::env::VarError>,
    ) -> ResolvedEnv {
        let mut resolved = ResolvedEnv::default();
        for (name, template) in &self.0 {
            let earlier = &resolved.entries;
            let lookup = |variable: &str| {
                earlier
                    .iter()
                    .find(|(defined, _)| defined == variable)
                    .map(|(_, value)| Ok(value.clone()))
                    .unwrap_or_else(|| lookup_env(variable))
            };
            match template.render(context, &lookup) {
                Ok(value) => resolved.entries.push((name.clone(), value)),
                Err(placeholder) => resolved.unset.push(UnsetVariable {
                    name: name.clone(),
                    placeholder,
                }),
            }
        }
        resolved
    }
}

impl Serialize for HarnessEnv {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, template) in &self.0 {
            map.serialize_entry(name, &template.source())?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for HarnessEnv {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let raw = IndexMap::<String, String>::deserialize(deserializer)?;
        let mut parsed = IndexMap::with_capacity(raw.len());
        for (name, value) in raw {
            if value.contains('\0') {
                return Err(D::Error::custom(format!(
                    "variable {name:?} has a value containing NUL"
                )));
            }
            let template = EnvTemplate::parse(&value).map_err(|error| {
                D::Error::custom(format!(
                    "variable {name:?} is not a valid template: {error}"
                ))
            })?;
            parsed.insert(name, template);
        }
        Ok(Self(parsed))
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
    #[error(
        "[harnesses.{0}] has no `kind`; only a declaration named after its kind ({names}) may \
         omit it",
        names = HarnessCatalog::KINDS_IMPLIED_BY_NAME
            .iter()
            .map(|kind| format!("`{kind}`"))
            .collect::<Vec<_>>()
            .join(", ")
    )]
    MissingKind(String),
    #[error("[harnesses.{0}] has a blank `kind`")]
    BlankKind(String),
    #[error("[harnesses.{0}] has a blank `command`; omit the key to use the adapter's default")]
    BlankCommand(String),
    #[error(
        "[harnesses.{declaration}.env] has an invalid variable name {name:?}: names must be \
         non-empty and contain neither `=` nor NUL"
    )]
    InvalidEnvName { declaration: String, name: String },
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
    /// Kinds a declaration may leave out when its name is the kind (`[harnesses.codex]`).
    /// Unrelated to what is synthesized when nothing is declared.
    pub const KINDS_IMPLIED_BY_NAME: &[&str] = &["codex", "claude-code"];

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
            match declaration.kind.as_deref() {
                None if !Self::KINDS_IMPLIED_BY_NAME.contains(&name.as_str()) => {
                    return Err(HarnessConfigError::MissingKind(name.clone()));
                }
                Some(kind) if kind.trim().is_empty() => {
                    return Err(HarnessConfigError::BlankKind(name.clone()));
                }
                _ => {}
            }
            if declaration
                .command
                .as_deref()
                .is_some_and(|command| command.trim().is_empty())
            {
                return Err(HarnessConfigError::BlankCommand(name.clone()));
            }
            // Values were parsed and checked when the config was loaded (`HarnessEnv`'s
            // `Deserialize`); names are checked here, where the message can name the declaration.
            for variable in declaration.env.names() {
                if variable.is_empty() || variable.contains('=') || variable.contains('\0') {
                    return Err(HarnessConfigError::InvalidEnvName {
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
                // Only a name in `KINDS_IMPLIED_BY_NAME` gets past the check above without one.
                declaration.kind.get_or_insert_with(|| name.clone());
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

/// Filesystem-capacity monitoring. Omitted sections enable monitoring with these defaults. Unknown
/// keys are rejected: a misspelled threshold would otherwise silently fall back to the default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiskUsageConfig {
    pub enabled: bool,
    #[serde(deserialize_with = "deserialize_check_interval")]
    pub check_interval_seconds: u64,
    #[serde(deserialize_with = "deserialize_available_gib")]
    pub min_available_gib: u64,
    /// Extra paths to watch, keyed by the path itself (`[disk_usage.paths."/mnt/builds"]`), the
    /// same shape as `[harnesses.<name>]`: a path is an identity, so a duplicate is a parse
    /// error rather than a second threshold. An `IndexMap` keeps declaration order, which is the
    /// order the status route reports them in.
    #[serde(deserialize_with = "deserialize_disk_usage_paths")]
    pub paths: IndexMap<std::path::PathBuf, DiskUsagePathConfig>,
}

impl Default for DiskUsageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_seconds: 60,
            min_available_gib: 10,
            paths: IndexMap::new(),
        }
    }
}

/// Per-path settings; an empty table inherits the global threshold. Unknown keys are rejected so
/// a stray `path = ...` (the path is the table key) fails at startup instead of being ignored.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiskUsagePathConfig {
    #[serde(deserialize_with = "deserialize_optional_available_gib")]
    pub min_available_gib: Option<u64>,
}

fn validate_available_gib<E: serde::de::Error>(value: u64) -> Result<u64, E> {
    if value == 0 || value.checked_mul(1_u64 << 30).is_none() {
        return Err(E::custom(
            "min_available_gib must be positive and fit in bytes (u64)",
        ));
    }
    Ok(value)
}

fn deserialize_available_gib<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    validate_available_gib(u64::deserialize(deserializer)?)
}

fn deserialize_optional_available_gib<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(deserializer)?
        .map(validate_available_gib)
        .transpose()
}

fn deserialize_check_interval<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value == 0
        || std::time::Instant::now()
            .checked_add(std::time::Duration::from_secs(value))
            .is_none()
    {
        return Err(serde::de::Error::custom(
            "check_interval_seconds must be positive and fit a monotonic deadline",
        ));
    }
    Ok(value)
}

fn deserialize_disk_usage_paths<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<IndexMap<std::path::PathBuf, DiskUsagePathConfig>, D::Error> {
    let paths = IndexMap::<std::path::PathBuf, DiskUsagePathConfig>::deserialize(deserializer)?;
    if let Some(path) = paths.keys().find(|path| !path.is_absolute()) {
        return Err(serde::de::Error::custom(format!(
            "disk usage path {path:?} must be absolute"
        )));
    }
    Ok(paths)
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
    /// Items of a turn sent at once: the newest ones of the live turn in its reconnect snapshot,
    /// and the default size of an items page (spec §13.6, LP6). Items still running or waiting on
    /// the user are sent regardless. Read as at least 1.
    pub turn_items: usize,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            initial: 5,
            page: 5,
            turn_items: 100,
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
    use crate::EnvTemplateContext;

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
        assert_eq!(entries[0].1.kind(), "codex");
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
    fn a_declaration_named_after_its_kind_may_omit_it() {
        let config: Config = toml::from_str(
            "[harnesses.claude-code]\n[harnesses.codex.env]\nA = \"1\"\n\
             [harnesses.claude]\nkind = \"claude-code\"\n",
        )
        .unwrap();
        let catalog = HarnessCatalog::resolve(&config).unwrap();
        let kinds: Vec<_> = catalog
            .iter()
            .map(|(name, declaration)| (name, declaration.kind()))
            .collect();
        assert_eq!(
            kinds,
            [
                ("claude-code", "claude-code"),
                ("codex", "codex"),
                ("claude", "claude-code")
            ]
        );
        assert!(
            catalog
                .get("codex")
                .is_some_and(|d| d.env.get("A").is_some())
        );

        // An explicit kind is taken as written, even on a kind's name.
        let config: Config = toml::from_str("[harnesses.codex]\nkind = \"claude-code\"\n").unwrap();
        let catalog = HarnessCatalog::resolve(&config).unwrap();
        assert_eq!(
            catalog.get("codex").map(HarnessDeclaration::kind),
            Some("claude-code")
        );
    }

    #[test]
    fn other_declarations_need_a_kind() {
        let config: Config = toml::from_str("[harnesses.stable.env]\nA = \"1\"\n").unwrap();
        let err = HarnessCatalog::resolve(&config).unwrap_err();
        assert_eq!(err, HarnessConfigError::MissingKind("stable".into()));
        assert_eq!(
            err.to_string(),
            "[harnesses.stable] has no `kind`; only a declaration named after its kind \
             (`codex`, `claude-code`) may omit it"
        );

        // A blank kind is not an omitted one, even on a kind's name.
        let config: Config = toml::from_str("[harnesses.codex]\nkind = \"\"\n").unwrap();
        let err = HarnessCatalog::resolve(&config).unwrap_err();
        assert_eq!(err, HarnessConfigError::BlankKind("codex".into()));
    }

    #[test]
    fn kind_specific_keys_are_kept_opaque() {
        let config: Config =
            toml::from_str("[harnesses.x]\nkind = \"codex\"\nprofile = \"p\"\n").unwrap();
        let declaration = &config.harnesses["x"];
        assert_eq!(declaration.kind(), "codex");
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
    }

    #[test]
    fn env_values_are_parsed_when_the_config_loads() {
        for (value, reason) in [
            ("secret\\u0000value", "has a value containing NUL"),
            (
                "s3cr3t-{projet_dir}",
                "is not a valid template: unknown placeholder `{projet_dir}`",
            ),
        ] {
            let src = format!(
                "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\nTOKEN = \"{value}\"\n"
            );
            let message = toml::from_str::<Config>(&src).unwrap_err().to_string();
            // The whole table is read before any value is checked, so `toml` points at the
            // table's header, and the quoted source line is not the one holding the value.
            assert!(message.contains("[harnesses.x.env]"), "{message}");
            assert!(
                !message.contains("secret") && !message.contains("s3cr3t"),
                "{message}"
            );
            assert!(
                message.contains(&format!("variable \"TOKEN\" {reason}")),
                "{message}"
            );
        }
    }

    #[test]
    fn env_serializes_back_to_its_source() {
        let src = "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\n\
                   B = \"{{lit}}{project_dir}\"\nA = \"{env:HOME}/x\"\n";
        let config: Config = toml::from_str(src).unwrap();
        let written = toml::to_string(&config.harnesses["x"]).unwrap();
        let reread: HarnessDeclaration = toml::from_str(&written).unwrap();
        assert_eq!(reread.env, config.harnesses["x"].env);
        assert!(written.contains("{{lit}}{project_dir}"), "{written}");
        assert_eq!(reread.env.names().collect::<Vec<_>>(), ["B", "A"]);
    }

    #[test]
    fn env_resolves_per_instance_and_leaves_unresolvable_values_unset() {
        let config: Config = toml::from_str(
            "[harnesses.x]\nkind = \"claude-code\"\n[harnesses.x.env]\n\
             CACHE = \"{project_dir}/.cache/{harness_name}\"\n\
             LITERAL = \"{{x}}\"\n\
             MISSING = \"{env:GISKARD_TEST_SURELY_UNSET}\"\n\
             NO_DATA = \"{data_dir}/x\"\n\
             HOME_BIN = \"{env:HOME}/bin\"\n",
        )
        .unwrap();
        HarnessCatalog::resolve(&config).unwrap();
        let context = EnvTemplateContext {
            project_dir: Some("/src/demo"),
            harness_name: Some("x"),
            ..EnvTemplateContext::default()
        };
        let lookup = |name: &str| {
            (name == "HOME")
                .then(|| "/home/me".to_owned())
                .ok_or(std::env::VarError::NotPresent)
        };
        let resolved = config.harnesses["x"].env.resolve(&context, &lookup);
        assert_eq!(
            resolved.entries,
            [
                ("CACHE".to_owned(), "/src/demo/.cache/x".to_owned()),
                ("LITERAL".to_owned(), "{x}".to_owned()),
                ("HOME_BIN".to_owned(), "/home/me/bin".to_owned()),
            ]
        );
        let unset: Vec<String> = resolved.unset.iter().map(ToString::to_string).collect();
        assert_eq!(
            unset,
            [
                "MISSING is skipped: {env:GISKARD_TEST_SURELY_UNSET} has no value",
                "NO_DATA is skipped: {data_dir} has no value",
            ]
        );
        let debug = format!("{resolved:?}");
        assert!(!debug.contains("/src/demo"), "{debug}");
    }

    #[test]
    fn env_values_read_earlier_variables_and_giskard_environment() {
        let config: Config = toml::from_str(
            "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\n\
             ROOT = \"{project_dir}/.tools\"\n\
             PATH = \"{env:ROOT}/bin:{env:PATH}\"\n\
             CACHE = \"{env:ROOT}/cache\"\n\
             GONE = \"{env:GISKARD_TEST_SURELY_UNSET}\"\n\
             HOME_DIR = \"{env:GONE}{env:HOME}\"\n",
        )
        .unwrap();
        HarnessCatalog::resolve(&config).unwrap();
        let context = EnvTemplateContext {
            project_dir: Some("/src/demo"),
            ..EnvTemplateContext::default()
        };
        let lookup = |name: &str| {
            match name {
                "PATH" => Some("/usr/bin".to_owned()),
                "HOME" => Some("/home/me".to_owned()),
                // Giskard's own ROOT is shadowed by the table's.
                "ROOT" => Some("/giskard-root".to_owned()),
                _ => None,
            }
            .ok_or(std::env::VarError::NotPresent)
        };
        let resolved = config.harnesses["x"].env.resolve(&context, &lookup);
        assert_eq!(
            resolved.entries,
            [
                ("ROOT".to_owned(), "/src/demo/.tools".to_owned()),
                (
                    "PATH".to_owned(),
                    "/src/demo/.tools/bin:/usr/bin".to_owned()
                ),
                ("CACHE".to_owned(), "/src/demo/.tools/cache".to_owned()),
            ]
        );
        // GONE is unset, so HOME_DIR, reading it, is unset too.
        let unset: Vec<&str> = resolved
            .unset
            .iter()
            .map(|unset| unset.name.as_str())
            .collect();
        assert_eq!(unset, ["GONE", "HOME_DIR"]);
    }

    #[test]
    fn env_values_read_names_as_they_stand_at_their_line() {
        let config: Config = toml::from_str(
            "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\n\
             ORIG_HOME = \"{env:HOME}\"\n\
             HOME = \"/sandbox/home\"\n\
             CONFIG = \"{env:HOME}/.config\"\n",
        )
        .unwrap();
        HarnessCatalog::resolve(&config).unwrap();
        let lookup = |name: &str| {
            (name == "HOME")
                .then(|| "/home/me".to_owned())
                .ok_or(std::env::VarError::NotPresent)
        };
        let resolved = config.harnesses["x"]
            .env
            .resolve(&EnvTemplateContext::default(), &lookup);
        assert_eq!(
            resolved.entries,
            [
                ("ORIG_HOME".to_owned(), "/home/me".to_owned()),
                ("HOME".to_owned(), "/sandbox/home".to_owned()),
                ("CONFIG".to_owned(), "/sandbox/home/.config".to_owned()),
            ]
        );
        assert!(resolved.unset.is_empty());
    }

    #[test]
    fn env_debug_redacts_values() {
        let config: Config = toml::from_str(
            "[harnesses.x]\nkind = \"codex\"\n[harnesses.x.env]\nCODEX_HOME = \"/very/secret/home\"\n",
        )
        .unwrap();
        let env = &config.harnesses["x"].env;
        assert_eq!(
            env.get("CODEX_HOME").map(EnvTemplate::source).as_deref(),
            Some("/very/secret/home")
        );
        let debug = format!("{env:?}");
        assert!(debug.contains("CODEX_HOME"), "{debug}");
        assert!(!debug.contains("/very/secret/home"), "{debug}");
        let declaration_debug = format!("{:?}", config.harnesses["x"]);
        assert!(!declaration_debug.contains("/very/secret/home"));
    }
}

#[cfg(test)]
mod disk_usage_tests {
    use super::Config;

    #[test]
    fn old_configs_and_partial_sections_enable_default_monitoring() {
        for text in ["", "[disk_usage]", "[disk_usage]\nenabled = true"] {
            let config: Config = toml::from_str(text).unwrap();
            assert!(config.disk_usage.enabled);
            assert_eq!(config.disk_usage.check_interval_seconds, 60);
            assert_eq!(config.disk_usage.min_available_gib, 10);
            assert!(config.disk_usage.paths.is_empty());
        }
    }

    #[test]
    fn explicit_paths_can_inherit_or_override_thresholds_in_declaration_order() {
        let config: Config = toml::from_str(
            r#"
[disk_usage]
enabled = false
check_interval_seconds = 5
min_available_gib = 20
[disk_usage.paths."/mnt/small"]
min_available_gib = 2
[disk_usage.paths."/mnt/builds"]
"#,
        )
        .unwrap();
        assert!(!config.disk_usage.enabled);
        assert_eq!(config.disk_usage.check_interval_seconds, 5);
        assert_eq!(config.disk_usage.min_available_gib, 20);
        let paths: Vec<_> = config
            .disk_usage
            .paths
            .iter()
            .map(|(path, entry)| (path.to_str().unwrap(), entry.min_available_gib))
            .collect();
        assert_eq!(paths, [("/mnt/small", Some(2)), ("/mnt/builds", None)]);

        let inline: Config = toml::from_str(
            r#"
[disk_usage]
paths = { "/mnt/builds" = { min_available_gib = 30 }, "/mnt/scratch" = {} }
"#,
        )
        .unwrap();
        assert_eq!(inline.disk_usage.paths.len(), 2);
        assert_eq!(
            inline.disk_usage.paths[std::path::Path::new("/mnt/builds")].min_available_gib,
            Some(30)
        );
    }

    #[test]
    fn a_path_declared_twice_is_rejected() {
        let text =
            "[disk_usage.paths.\"/mnt\"]\nmin_available_gib = 2\n[disk_usage.paths.\"/mnt\"]\n";
        assert!(toml::from_str::<Config>(text).is_err());
    }

    #[test]
    fn invalid_thresholds_intervals_and_paths_fail_deserialization() {
        for text in [
            "[disk_usage]\nmin_available_gib = 0",
            "[disk_usage]\nmin_available_gib = 17179869184",
            "[disk_usage]\ncheck_interval_seconds = 0",
            "[disk_usage]\ncheck_interval_seconds = 9223372036854775807",
            "[disk_usage]\nmin_avail_gib = 5",
            "[disk_usage.paths.relative]",
            "[disk_usage.paths.\"\"]",
            "[disk_usage.paths.\"/mnt\"]\nmin_available_gib = 0",
            "[disk_usage.paths.\"/mnt\"]\nmin_available_gib = 17179869184",
            "[disk_usage.paths.\"/mnt\"]\npath = \"/mnt\"",
        ] {
            assert!(toml::from_str::<Config>(text).is_err(), "accepted {text}");
        }
    }
}
