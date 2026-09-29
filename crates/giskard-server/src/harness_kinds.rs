//! Harness kinds a binary can construct, keyed by the kind string a project names.

use std::sync::Arc;

use async_trait::async_trait;
use giskard_core::error::HarnessError;
use giskard_harness::{AgentHarness, HarnessBootstrap};
use giskard_persist::store::ProjectConfig;
use indexmap::IndexMap;
use tracing::warn;

use crate::HarnessFactory;

#[async_trait]
pub trait HarnessKind: Send + Sync {
    /// The kind string a `project.json` names in its `harness` field.
    fn name(&self) -> &str;
    async fn create(
        &self,
        config: &ProjectConfig,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError>;
}

/// A `HarnessFactory` that dispatches on `ProjectConfig::harness`.
#[derive(Default)]
pub struct HarnessKindFactory {
    // Insertion order is the order kinds are listed in errors; `indexmap` is already a dependency.
    kinds: IndexMap<String, Arc<dyn HarnessKind>>,
}

#[derive(Debug, thiserror::Error)]
#[error("harness kind {0:?} is registered twice")]
pub struct DuplicateHarnessKind(pub String);

impl HarnessKindFactory {
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

    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.kinds.keys().map(String::as_str)
    }
}

#[async_trait]
impl HarnessFactory for HarnessKindFactory {
    async fn create(
        &self,
        config: &ProjectConfig,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        let Some(kind) = self.kinds.get(&config.harness) else {
            let supported = self.kinds().collect::<Vec<_>>().join(", ");
            warn!(
                project_id = %config.id,
                harness = %config.harness,
                action = "create_harness",
                supported = %supported,
                "project names a harness kind this server cannot construct"
            );
            return Err(HarnessError::Unsupported(format!(
                "unsupported harness kind {:?} for project {}; this server supports: {}",
                config.harness, config.id, supported
            )));
        };
        kind.create(config, bootstrap).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A kind whose `create` fails recognisably, so dispatch is observable without a harness.
    struct StubKind(&'static str);

    #[async_trait]
    impl HarnessKind for StubKind {
        fn name(&self) -> &str {
            self.0
        }
        async fn create(
            &self,
            _config: &ProjectConfig,
            _bootstrap: HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            Err(HarnessError::Protocol(format!("stub reached: {}", self.0)))
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

    fn factory() -> HarnessKindFactory {
        HarnessKindFactory::new()
            .register(Arc::new(StubKind("codex")))
            .and_then(|factory| factory.register(Arc::new(StubKind("other"))))
            .expect("distinct kinds register")
    }

    #[tokio::test]
    async fn a_registered_kind_receives_its_projects() {
        let result = factory()
            .create(&project("other"), HarnessBootstrap::default())
            .await;
        match result {
            Err(HarnessError::Protocol(message)) => assert_eq!(message, "stub reached: other"),
            Err(other) => panic!("expected the stub's error, got {other:?}"),
            Ok(_) => panic!("expected the stub's error, got a harness"),
        }
    }

    #[tokio::test]
    async fn an_unregistered_kind_is_unsupported_and_names_the_alternatives() {
        let config = project("claude");
        let result = factory().create(&config, HarnessBootstrap::default()).await;
        let message = match result {
            Err(HarnessError::Unsupported(message)) => message,
            Err(other) => panic!("expected Unsupported, got {other:?}"),
            Ok(_) => panic!("expected Unsupported, got a harness"),
        };
        assert!(message.contains("\"claude\""), "{message}");
        assert!(message.contains(&config.id.to_string()), "{message}");
        assert!(message.contains("codex, other"), "{message}");
    }

    #[test]
    fn registering_a_name_twice_is_refused() {
        let error = match factory().register(Arc::new(StubKind("codex"))) {
            Err(error) => error,
            Ok(_) => panic!("a duplicate kind must be refused"),
        };
        assert_eq!(error.0, "codex");
    }
}
