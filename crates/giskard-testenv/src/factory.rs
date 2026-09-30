use std::sync::Arc;

use async_trait::async_trait;
use giskard_core::HarnessError;
use giskard_harness::{AgentHarness, HarnessBootstrap};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_server::HarnessFactory;

/// A factory over a closure that also receives the requested `[harnesses.<name>]` declaration.
struct FnFactory<F>(F);

#[async_trait]
impl<F> HarnessFactory for FnFactory<F>
where
    F: Fn(&ProjectConfig, &str, HarnessBootstrap) -> Result<Arc<dyn AgentHarness>, HarnessError>
        + Send
        + Sync
        + 'static,
{
    async fn create(
        &self,
        config: &ProjectConfig,
        harness: &str,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        (self.0)(config, harness, bootstrap)
    }
}

/// A factory that ignores the declaration name: every instance comes from `f`.
pub fn from_fn<F>(f: F) -> Arc<dyn HarnessFactory>
where
    F: Fn(&ProjectConfig, HarnessBootstrap) -> Result<Arc<dyn AgentHarness>, HarnessError>
        + Send
        + Sync
        + 'static,
{
    Arc::new(FnFactory(
        move |config: &ProjectConfig, _: &str, bootstrap| f(config, bootstrap),
    ))
}

/// A factory that constructs a different instance per declaration name, for multi-harness tests.
pub fn from_fn_by_harness<F>(f: F) -> Arc<dyn HarnessFactory>
where
    F: Fn(&ProjectConfig, &str, HarnessBootstrap) -> Result<Arc<dyn AgentHarness>, HarnessError>
        + Send
        + Sync
        + 'static,
{
    Arc::new(FnFactory(f))
}

pub fn shared(harness: Arc<dyn AgentHarness>) -> Arc<dyn HarnessFactory> {
    from_fn(move |_, _| Ok(harness.clone()))
}

pub fn fixture(fixture: ReplayFixture) -> Arc<dyn HarnessFactory> {
    from_fn(move |_, _| Ok(Arc::new(ReplayHarness::from_fixture(fixture.clone()))))
}

pub fn failing(error: HarnessError) -> Arc<dyn HarnessFactory> {
    from_fn(move |_, _| Err(error.clone()))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use giskard_core::HarnessError;
    use giskard_core::ids::{ProjectId, ThreadId};
    use giskard_harness::{HarnessBootstrap, KnownThreadBinding};
    use giskard_harness_replay::ReplayHarness;
    use giskard_persist::PersistStore;

    async fn config() -> giskard_persist::store::ProjectConfig {
        let dir = tempfile::tempdir().unwrap();
        let store = PersistStore::new(dir.path().to_path_buf());
        store
            .create_project(ProjectId::new(), "test", "/tmp", "codex")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn failing_returns_the_given_error() {
        let factory = super::failing(HarnessError::Spawn("given".into()));
        let error = match factory
            .create(&config().await, "codex", HarnessBootstrap::default())
            .await
        {
            Ok(_) => panic!("failing factory created a harness"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            HarnessError::Spawn("given".into()).to_string()
        );
    }

    #[tokio::test]
    async fn shared_returns_the_same_arc() {
        let harness: Arc<dyn giskard_harness::AgentHarness> = Arc::new(ReplayHarness::new());
        let factory = super::shared(harness.clone());
        let config = config().await;
        let first = factory
            .create(&config, "codex", HarnessBootstrap::default())
            .await
            .unwrap();
        let second = factory
            .create(&config, "codex", HarnessBootstrap::default())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&harness, &first));
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn from_fn_receives_the_bootstrap() {
        let binding = KnownThreadBinding {
            harness_thread_id: "native".into(),
            thread_id: ThreadId::new(),
        };
        let received = Arc::new(Mutex::new(None));
        let recorded = received.clone();
        let factory = super::from_fn(move |_, bootstrap| {
            *recorded.lock().unwrap() = Some(bootstrap);
            Ok(Arc::new(ReplayHarness::new()))
        });
        let bootstrap = HarnessBootstrap {
            known_threads: vec![binding],
        };
        factory
            .create(&config().await, "codex", bootstrap.clone())
            .await
            .unwrap();
        assert_eq!(*received.lock().unwrap(), Some(bootstrap));
    }

    #[tokio::test]
    async fn from_fn_by_harness_receives_the_declaration_name() {
        let received = Arc::new(Mutex::new(None));
        let recorded = received.clone();
        let factory = super::from_fn_by_harness(move |_, harness, _| {
            *recorded.lock().unwrap() = Some(harness.to_string());
            Ok(Arc::new(ReplayHarness::new()))
        });
        factory
            .create(&config().await, "nightly", HarnessBootstrap::default())
            .await
            .unwrap();
        assert_eq!(received.lock().unwrap().as_deref(), Some("nightly"));
    }
}
