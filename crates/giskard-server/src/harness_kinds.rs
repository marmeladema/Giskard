//! Harness kinds a binary can construct, and the declarations (`[harnesses.<name>]`) that name
//! them. A project names a declaration; the declaration names a kind.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use giskard_core::error::HarnessError;
use giskard_core::ids::ProjectId;
use giskard_harness::{AgentHarness, HarnessBootstrap};
use giskard_persist::store::ProjectConfig;
use giskard_persist::{HarnessCatalog, HarnessDeclaration};
use indexmap::IndexMap;
use tracing::warn;

use crate::HarnessFactory;

/// What a kind needs to construct one instance.
pub struct HarnessInstanceSpec<'a> {
    pub project_id: ProjectId,
    pub workspace_root: PathBuf,
    /// The declaration's `[harnesses.<name>]` key.
    pub name: &'a str,
    pub declaration: &'a HarnessDeclaration,
}

#[async_trait]
pub trait HarnessKind: Send + Sync {
    /// The kind string a declaration names in its `kind` key.
    fn name(&self) -> &str;
    /// Type-check a declaration's kind-specific `options` at boot. The error is shown to the
    /// operator with the `[harnesses.<name>]` key prepended by the caller.
    fn validate(&self, declaration: &HarnessDeclaration) -> Result<(), String>;
    async fn create(
        &self,
        spec: HarnessInstanceSpec<'_>,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError>;
}

/// A `HarnessFactory` that resolves `ProjectConfig::harness` to a declaration in its catalog and
/// dispatches on the declaration's kind. Nothing outside this factory reads a declaration.
pub struct HarnessKindFactory {
    // Insertion order is the order kinds are listed in errors; `indexmap` is already a dependency.
    kinds: IndexMap<String, Arc<dyn HarnessKind>>,
    catalog: HarnessCatalog,
    /// Undeclared declaration names already reported at `warn`, so a project whose threads keep
    /// retrying an attach does not repeat the same operator message. Keyed by declaration name,
    /// which is configuration, not entity identity; it lives as long as the factory.
    reported_undeclared: Mutex<HashSet<String>>,
}

impl Default for HarnessKindFactory {
    fn default() -> Self {
        Self {
            kinds: IndexMap::new(),
            catalog: HarnessCatalog::synthesized(),
            reported_undeclared: Mutex::new(HashSet::new()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("harness kind {0:?} is registered twice")]
pub struct DuplicateHarnessKind(pub String);

/// A declaration this binary cannot construct. Found at boot, before the server listens.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HarnessValidationError {
    #[error(
        "[harnesses.{declaration}] names kind {kind:?}, which this server cannot construct; \
         supported kinds: {supported}"
    )]
    UnknownKind {
        declaration: String,
        kind: String,
        supported: String,
    },
    #[error("[harnesses.{declaration}] {message}")]
    InvalidOptions {
        declaration: String,
        message: String,
    },
}

impl HarnessKindFactory {
    /// A factory with no kinds and the synthesized single-`codex` catalog.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, kind: Arc<dyn HarnessKind>) -> Result<Self, DuplicateHarnessKind> {
        let name = kind.name().to_owned();
        if self.kinds.contains_key(&name) {
            return Err(DuplicateHarnessKind(name));
        }
        self.kinds.insert(name, kind);
        Ok(self)
    }

    /// Replace the catalog projects resolve against.
    pub fn with_catalog(mut self, catalog: HarnessCatalog) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.kinds.keys().map(String::as_str)
    }

    pub fn catalog(&self) -> &HarnessCatalog {
        &self.catalog
    }

    /// Check every declaration against the registered kinds: its kind must be registered, and
    /// that kind must accept its options.
    pub fn validate(&self) -> Result<(), HarnessValidationError> {
        for (name, declaration) in self.catalog.iter() {
            let Some(kind) = self.kinds.get(&declaration.kind) else {
                return Err(HarnessValidationError::UnknownKind {
                    declaration: name.to_owned(),
                    kind: declaration.kind.clone(),
                    supported: self.kinds().collect::<Vec<_>>().join(", "),
                });
            };
            kind.validate(declaration).map_err(|message| {
                HarnessValidationError::InvalidOptions {
                    declaration: name.to_owned(),
                    message,
                }
            })?;
        }
        Ok(())
    }

    /// `true` the first time a name is reported, so the caller logs it once per process.
    fn first_report_of(&self, name: &str) -> bool {
        let mut reported = self
            .reported_undeclared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reported.insert(name.to_owned())
    }
}

#[async_trait]
impl HarnessFactory for HarnessKindFactory {
    async fn create(
        &self,
        config: &ProjectConfig,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        let Some(declaration) = self.catalog.get(&config.harness) else {
            let declared = self.catalog.names().collect::<Vec<_>>().join(", ");
            if self.first_report_of(&config.harness) {
                warn!(
                    project_id = %config.id,
                    harness = %config.harness,
                    action = "create_harness",
                    declared = %declared,
                    "project names a harness that config.toml does not declare"
                );
            }
            return Err(HarnessError::Unsupported(format!(
                "project {} names harness {:?}, which config.toml does not declare under \
                 [harnesses]; declared: {}",
                config.id, config.harness, declared
            )));
        };
        // `validate` refuses boot for an unregistered kind, so this is unreachable in a server
        // that started; it still degrades to `Unsupported` rather than panicking.
        let Some(kind) = self.kinds.get(&declaration.kind) else {
            let supported = self.kinds().collect::<Vec<_>>().join(", ");
            warn!(
                project_id = %config.id,
                harness = %config.harness,
                kind = %declaration.kind,
                action = "create_harness",
                supported = %supported,
                "project's harness declaration names a kind this server cannot construct"
            );
            return Err(HarnessError::Unsupported(format!(
                "project {} names harness {:?} of kind {:?}, which this server cannot construct; \
                 supported kinds: {}",
                config.id, config.harness, declaration.kind, supported
            )));
        };
        let spec = HarnessInstanceSpec {
            project_id: config.id,
            workspace_root: PathBuf::from(config.workspace_root.as_deref().unwrap_or(&config.dir)),
            name: &config.harness,
            declaration,
        };
        kind.create(spec, bootstrap).await
    }

    fn catalog(&self) -> HarnessCatalog {
        self.catalog.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_logs::CapturedLogWriter;

    /// A kind whose `create` fails recognisably, so dispatch is observable without a harness. The
    /// error carries the declaration name, workspace root, and command the spec delivered.
    struct StubKind {
        name: &'static str,
        reject_options: bool,
    }

    fn stub(name: &'static str) -> Arc<StubKind> {
        Arc::new(StubKind {
            name,
            reject_options: false,
        })
    }

    #[async_trait]
    impl HarnessKind for StubKind {
        fn name(&self) -> &str {
            self.name
        }
        fn validate(&self, declaration: &HarnessDeclaration) -> Result<(), String> {
            if self.reject_options && !declaration.options.is_empty() {
                return Err("has options this stub rejects".into());
            }
            Ok(())
        }
        async fn create(
            &self,
            spec: HarnessInstanceSpec<'_>,
            _bootstrap: HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            Err(HarnessError::Protocol(format!(
                "stub reached: {} as {} in {} with {:?}",
                self.name,
                spec.name,
                spec.workspace_root.display(),
                spec.declaration.command
            )))
        }
    }

    fn project(harness: &str) -> ProjectConfig {
        serde_json::from_value(serde_json::json!({
            "version": 1,
            "id": giskard_core::ids::ProjectId::new().to_string(),
            "name": "proj",
            "dir": "/tmp/giskard-harness-kinds-test",
            "harness": harness,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }))
        .expect("project config fixture parses")
    }

    fn catalog(src: &str) -> HarnessCatalog {
        let config: giskard_persist::Config = toml::from_str(src).expect("config parses");
        HarnessCatalog::resolve(&config).expect("catalog resolves")
    }

    fn factory() -> HarnessKindFactory {
        HarnessKindFactory::new()
            .register(stub("codex"))
            .and_then(|factory| factory.register(stub("other")))
            .expect("distinct kinds register")
            .with_catalog(catalog(
                r#"
[harnesses.stable]
kind = "codex"

[harnesses.nightly]
kind = "other"
command = "/opt/nightly/bin/codex"
"#,
            ))
    }

    async fn create_error(factory: &HarnessKindFactory, config: &ProjectConfig) -> HarnessError {
        match factory.create(config, HarnessBootstrap::default()).await {
            Err(error) => error,
            Ok(_) => panic!("expected an error, got a harness"),
        }
    }

    #[tokio::test]
    async fn a_declared_name_reaches_its_kind_with_the_declaration() {
        let error = create_error(&factory(), &project("nightly")).await;
        match error {
            HarnessError::Protocol(message) => assert_eq!(
                message,
                "stub reached: other as nightly in /tmp/giskard-harness-kinds-test with \
                 Some(\"/opt/nightly/bin/codex\")"
            ),
            other => panic!("expected the stub's error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_default_catalog_resolves_codex_to_the_codex_kind() {
        let factory = HarnessKindFactory::new()
            .register(stub("codex"))
            .expect("kind registers");
        let error = create_error(&factory, &project("codex")).await;
        assert!(
            matches!(&error, HarnessError::Protocol(message) if message.starts_with("stub reached: codex as codex")),
            "{error:?}"
        );
    }

    /// One test for both the error and its warning: tracing caches a callsite's interest globally,
    /// so a second test reaching this `warn!` on another thread with no subscriber could race
    /// this thread's `set_default` and switch the callsite off, losing the captured event.
    #[tokio::test]
    async fn an_undeclared_name_is_unsupported_and_warned_about_once() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);

        let factory = factory();
        let config = project("codex");
        let message = match create_error(&factory, &config).await {
            HarnessError::Unsupported(message) => message,
            other => panic!("expected Unsupported, got {other:?}"),
        };
        assert!(message.contains("\"codex\""), "{message}");
        assert!(message.contains(&config.id.to_string()), "{message}");
        assert!(message.contains("[harnesses]"), "{message}");
        assert!(message.contains("stable, nightly"), "{message}");
        create_error(&factory, &project("codex")).await;
        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert_eq!(
            output
                .matches("project names a harness that config.toml does not declare")
                .count(),
            1,
            "{output}"
        );
        assert!(output.contains("harness=codex"), "{output}");
        assert!(output.contains("action=\"create_harness\""), "{output}");
    }

    #[test]
    fn validate_accepts_declarations_of_registered_kinds() {
        factory()
            .validate()
            .expect("every declared kind is registered");
    }

    #[test]
    fn validate_refuses_an_unregistered_kind_naming_the_key() {
        let factory = HarnessKindFactory::new()
            .register(stub("codex"))
            .expect("kind registers")
            .with_catalog(catalog("[harnesses.x]\nkind = \"nope\"\n"));
        let error = factory
            .validate()
            .expect_err("kind `nope` is not registered");
        assert_eq!(
            error,
            HarnessValidationError::UnknownKind {
                declaration: "x".into(),
                kind: "nope".into(),
                supported: "codex".into(),
            }
        );
        assert!(error.to_string().starts_with("[harnesses.x] "), "{error}");
    }

    #[test]
    fn validate_prepends_the_key_to_a_kind_rejecting_its_options() {
        let factory = HarnessKindFactory::new()
            .register(Arc::new(StubKind {
                name: "codex",
                reject_options: true,
            }))
            .expect("kind registers")
            .with_catalog(catalog("[harnesses.x]\nkind = \"codex\"\nprofil = \"p\"\n"));
        let error = factory.validate().expect_err("the stub rejects options");
        assert_eq!(
            error.to_string(),
            "[harnesses.x] has options this stub rejects"
        );
    }

    #[test]
    fn registering_a_name_twice_is_refused() {
        let error = match factory().register(stub("codex")) {
            Err(error) => error,
            Ok(_) => panic!("a duplicate kind must be refused"),
        };
        assert_eq!(error.0, "codex");
    }
}
