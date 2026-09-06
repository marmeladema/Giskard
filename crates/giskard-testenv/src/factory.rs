use std::sync::Arc;

use async_trait::async_trait;
use giskard_core::HarnessError;
use giskard_harness::{AgentHarness, HarnessBootstrap};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_server::HarnessFactory;

struct FnFactory<F>(F);

#[async_trait]
impl<F> HarnessFactory for FnFactory<F>
where
    F: Fn(&ProjectConfig, HarnessBootstrap) -> Result<Arc<dyn AgentHarness>, HarnessError>
        + Send
        + Sync
        + 'static,
{
    async fn create(
        &self,
        config: &ProjectConfig,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        (self.0)(config, bootstrap)
    }
}

pub fn from_fn<F>(f: F) -> Arc<dyn HarnessFactory>
where
    F: Fn(&ProjectConfig, HarnessBootstrap) -> Result<Arc<dyn AgentHarness>, HarnessError>
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
            .create_project(ProjectId::new(), "test", "/tmp")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn failing_returns_the_given_error() {
        let factory = super::failing(HarnessError::Spawn("given".into()));
        let error = match factory
            .create(&config().await, HarnessBootstrap::default())
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
            .create(&config, HarnessBootstrap::default())
            .await
            .unwrap();
        let second = factory
            .create(&config, HarnessBootstrap::default())
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
            .create(&config().await, bootstrap.clone())
            .await
            .unwrap();
        assert_eq!(*received.lock().unwrap(), Some(bootstrap));
    }
}
