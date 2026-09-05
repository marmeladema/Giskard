use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use giskard_core::ids::{ProjectId, ThreadId};
use giskard_persist::PersistStore;
use giskard_server::{AppState, DriverEventSink, HarnessFactory, LogDriverEventSink, build_app};
use tempfile::TempDir;

use crate::{TestWs, auth, fixtures, ws};

type SeedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type Seed = Box<dyn FnOnce(Arc<PersistStore>) -> SeedFuture + Send>;

pub struct TestServer {
    pub state: AppState,
    pub addr: SocketAddr,
    pub base: String,
    pub client: reqwest::Client,
    pub cookie: String,
    data_dir: Option<TempDir>,
}

pub struct TestProject {
    pub id: ProjectId,
    pub dir: TempDir,
}

pub struct TestServerBuilder {
    factory: Arc<dyn HarnessFactory>,
    extra_config: String,
    authenticated: bool,
    data_dir: Option<std::path::PathBuf>,
    driver_events: Arc<dyn DriverEventSink>,
    seed: Option<Seed>,
}

impl TestServer {
    pub async fn spawn(factory: Arc<dyn HarnessFactory>) -> Self {
        Self::builder(factory).start().await
    }

    pub fn builder(factory: Arc<dyn HarnessFactory>) -> TestServerBuilder {
        TestServerBuilder {
            factory,
            extra_config: String::new(),
            authenticated: true,
            data_dir: None,
            driver_events: Arc::new(LogDriverEventSink),
            seed: None,
        }
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir
            .as_ref()
            .map(TempDir::path)
            .unwrap_or_else(|| self.state.store.data_dir())
    }

    pub fn store(&self) -> &Arc<PersistStore> {
        &self.state.store
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    pub async fn login(&self) -> String {
        auth::login(&self.client, &self.base).await
    }

    pub async fn create_project(&self, name: &str) -> TestProject {
        let dir = tempfile::tempdir().unwrap();
        let id = self.create_project_in(name, dir.path()).await;
        TestProject { id, dir }
    }

    pub async fn create_project_in(&self, name: &str, dir: &Path) -> ProjectId {
        let id = ProjectId::new();
        self.state
            .store
            .create_project(id, name, dir.to_str().unwrap())
            .await
            .unwrap();
        id
    }

    pub async fn create_project_via_api(&self, name: &str, dir: &str) -> ProjectId {
        let response = self
            .client
            .post(self.url("/api/projects"))
            .header("cookie", &self.cookie)
            .json(&serde_json::json!({"name": name, "dir": dir}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        serde_json::from_value(body["id"].clone()).unwrap()
    }

    pub async fn register_thread(&self, project: ProjectId, harness_thread_id: &str) -> ThreadId {
        let thread_id = ThreadId::new();
        fixtures::persist_primary_thread(
            &self.state.store,
            project,
            thread_id,
            harness_thread_id,
            fixtures::fake_native_model(),
        )
        .await;
        let response = self
            .client
            .post(self.url(&format!("/api/projects/{project}/threads")))
            .header("cookie", &self.cookie)
            .json(&serde_json::json!({"thread_id": thread_id}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let opened: serde_json::Value = response.json().await.unwrap();
        assert_eq!(opened["thread_id"], serde_json::json!(thread_id));
        thread_id
    }

    pub async fn ws(&self) -> TestWs {
        self.ws_with_cookie(&self.cookie).await
    }

    pub async fn ws_with_cookie(&self, cookie: &str) -> TestWs {
        ws::connect(self.addr, cookie).await
    }
}

impl TestServerBuilder {
    pub fn config(mut self, extra: &str) -> Self {
        self.extra_config = extra.to_string();
        self
    }
    pub fn unauthenticated(mut self) -> Self {
        self.authenticated = false;
        self
    }
    pub fn data_dir(mut self, path: &Path) -> Self {
        self.data_dir = Some(path.to_path_buf());
        self
    }
    pub fn driver_events(mut self, sink: Arc<dyn DriverEventSink>) -> Self {
        self.driver_events = sink;
        self
    }

    pub fn seed<F, Fut>(mut self, seed: F) -> Self
    where
        F: FnOnce(Arc<PersistStore>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.seed = Some(Box::new(move |store| Box::pin(seed(store))));
        self
    }

    pub async fn start(self) -> TestServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let owned_data_dir = if self.data_dir.is_none() {
            Some(tempfile::tempdir().unwrap())
        } else {
            None
        };
        let data_path = self
            .data_dir
            .as_deref()
            .or_else(|| owned_data_dir.as_ref().map(TempDir::path))
            .unwrap();
        // A restart points `data_dir` at a directory whose `config.toml` an earlier server wrote;
        // rewriting it would silently drop that server's extra sections. Write only when there is
        // nothing to preserve.
        let config_path = data_path.join("config.toml");
        let write_config = self.authenticated && !config_path.exists();
        assert!(
            write_config || self.extra_config.is_empty(),
            "config() would be ignored here: an unauthenticated server writes no config.toml, and \
             a restart over an existing data directory keeps the config already in it"
        );
        if write_config {
            let hash = auth::password_hash(auth::PASSWORD);
            let config = format!(
                "[server]\nbind = \"127.0.0.1:{}\"\nsecure_cookies = false\n\n[auth]\npassword_hash = \"{hash}\"\nsession_days = 30\n\n{}",
                addr.port(),
                self.extra_config
            );
            tokio::fs::write(&config_path, config).await.unwrap();
        }
        let store = Arc::new(PersistStore::new(data_path.to_path_buf()));
        if let Some(seed) = self.seed {
            seed(store.clone()).await;
        }
        let state = AppState::new_with_config(
            store,
            self.factory,
            auth::session_key(),
            None,
            None,
            self.driver_events,
        );
        let app = build_app(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let base = format!("http://{addr}");
        let cookie = if self.authenticated {
            auth::login(&client, &base).await
        } else {
            String::new()
        };
        TestServer {
            state,
            addr,
            base,
            client,
            cookie,
            data_dir: owned_data_dir,
        }
    }
}
