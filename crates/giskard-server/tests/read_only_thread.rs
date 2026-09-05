//! A thread whose harness can no longer attach — e.g. its provider was removed from config — must
//! still load **read-only**: the persisted history is served and a non-fatal `thread_read_only`
//! warning is surfaced, instead of the whole subscribe failing with a JSON-RPC/harness error.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::model::ModelRef;
use giskard_harness_replay::ReplayHarness;
use giskard_proto::ClientMessage;
use giskard_server::HarnessFactory;
use giskard_testenv::{TestServer, factory, fixtures};

struct AttachFails {
    inner: ReplayHarness,
}

#[async_trait::async_trait]
impl giskard_harness::AgentHarness for AttachFails {
    fn capabilities(&self) -> giskard_harness::HarnessCapabilities {
        self.inner.capabilities()
    }
    async fn list_models(
        &self,
    ) -> Result<Vec<giskard_core::model::ModelDescriptor>, giskard_core::HarnessError> {
        self.inner.list_models().await
    }
    async fn list_providers(
        &self,
    ) -> Result<Vec<giskard_harness::HarnessProvider>, giskard_core::HarnessError> {
        self.inner.list_providers().await
    }
    async fn open_thread(
        &self,
        _opts: giskard_harness::OpenThreadOptions,
    ) -> Result<giskard_harness::ThreadHandle, giskard_core::HarnessError> {
        Err(giskard_core::HarnessError::Spawn(
            "unknown provider: cloudflare-litellm".into(),
        ))
    }
    fn subscribe(
        &self,
        thread: &giskard_harness::ThreadHandle,
    ) -> giskard_harness::AgentEventStream {
        self.inner.subscribe(thread)
    }
    async fn interrupt(
        &self,
        thread: &giskard_harness::ThreadHandle,
    ) -> Result<(), giskard_core::HarnessError> {
        self.inner.interrupt(thread).await
    }
    async fn shutdown(&self) -> Result<(), giskard_core::HarnessError> {
        self.inner.shutdown().await
    }
    async fn start_turn(
        &self,
        thread: &giskard_harness::ThreadHandle,
        input: giskard_core::user_input::UserInput,
        overrides: giskard_core::turn::TurnOverrides,
    ) -> Result<TurnId, giskard_core::HarnessError> {
        self.inner.start_turn(thread, input, overrides).await
    }
    async fn respond_approval(
        &self,
        req: giskard_core::ids::ApprovalId,
        decision: giskard_core::approval::ApprovalDecision,
    ) -> Result<(), giskard_core::HarnessError> {
        self.inner.respond_approval(req, decision).await
    }
    async fn respond_server_request(
        &self,
        req: giskard_core::ids::ServerRequestId,
        response: giskard_core::server_request::ServerRequestResponse,
    ) -> Result<(), giskard_core::HarnessError> {
        self.inner.respond_server_request(req, response).await
    }
}

/// A model referencing a provider that no longer exists in config.
fn orphaned_model() -> ModelRef {
    ModelRef {
        provider: "cloudflare-litellm".into(),
        model: "@cf/z-ai/glm-4.7".into(),
        reasoning_effort: None,
    }
}

/// The other direction: when the harness *can* answer and does not list the provider, the thread
/// really is pinned to something unroutable, and the warning should say so precisely.
/// Open a seeded thread whose harness cannot attach, and return the parsed read-only response.
///
/// The three tests below differ only in the factory they hand in and what they assert about the
/// warning; everything up to the open — config, store, project, thread, login — is identical.
async fn open_read_only_thread(
    factory: Arc<dyn HarnessFactory>,
) -> (serde_json::Value, TestServer, tempfile::TempDir) {
    let pid = ProjectId::new();
    let tid = ThreadId::new();
    let proj_dir = tempfile::TempDir::new().unwrap();
    let path = proj_dir.path().to_string_lossy().to_string();
    let server = TestServer::builder(factory)
        .seed(move |store| async move {
            store.create_project(pid, "proj", &path).await.unwrap();
            store
                .save_thread(
                    pid,
                    &fixtures::orphaned_thread(pid, tid, orphaned_model(), None),
                )
                .await
                .unwrap();
        })
        .start()
        .await;
    let resp = server
        .client
        .post(server.url(&format!("/api/projects/{pid}/threads")))
        .header("cookie", &server.cookie)
        .json(&serde_json::json!({"thread_id": tid}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "an orphaned thread must degrade to a read-only open, not a hard failure"
    );
    (resp.json().await.unwrap(), server, proj_dir)
}

#[tokio::test]
async fn a_provider_the_harness_does_not_know_is_named_as_the_cause() {
    // The harness knows some other provider, but not the one the thread is pinned to.
    let (open, _server, _tmp) = open_read_only_thread({
        let providers = vec![giskard_harness::HarnessProvider {
            id: "something-else".into(),
            name: None,
            base_url: None,
            auth: None,
        }];
        let advertises = true;
        factory::from_fn(move |_, _| {
            let inner = if advertises {
                ReplayHarness::new().with_providers(providers.clone())
            } else {
                ReplayHarness::new()
            };
            Ok(Arc::new(AttachFails { inner }))
        })
    })
    .await;

    assert_eq!(open["warning"]["code"], "thread_read_only");
    let message = open["warning"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("\"cloudflare-litellm\"") && message.contains("no longer configured"),
        "a harness that can answer and does not know the provider should name it: {message}"
    );
}

/// A harness that never claimed it can list providers may still return an empty table. Reading
/// that as proof would convict every provider — so the capability is checked before the answer is
/// believed, the same way the catalog refresh checks it (§8.2).
#[tokio::test]
async fn an_empty_table_from_a_harness_without_the_capability_convicts_nobody() {
    let (open, _server, _tmp) = open_read_only_thread({
        let providers = Vec::new();
        let advertises = false;
        factory::from_fn(move |_, _| {
            let inner = if advertises {
                ReplayHarness::new().with_providers(providers.clone())
            } else {
                ReplayHarness::new()
            };
            Ok(Arc::new(AttachFails { inner }))
        })
    })
    .await;

    // Positive assertions first: `unwrap_or_default()` yields "" for a missing warning, and ""
    // satisfies the negative assertion below — so on its own it would stay green if the warning
    // vanished entirely.
    assert_eq!(open["warning"]["code"], "thread_read_only");
    let message = open["warning"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("\"cloudflare-litellm\"") && message.contains("Pick a model"),
        "the generic wording must still name the provider and the recovery action: {message}"
    );
    assert!(
        !message.contains("no longer configured"),
        "an unauthoritative empty table must not accuse the provider: {message}"
    );
}

/// A harness that cannot be reached cannot tell us whether the provider still exists, so the
/// warning must not blame config. Model listing is on by default and most configs name no
/// providers at all, so "absent from config" stopped being evidence: accusing it here would send
/// users to edit a file that was never the problem.
#[tokio::test]
async fn an_unreachable_harness_does_not_blame_the_provider_config() {
    let pid = ProjectId::new();
    let tid = ThreadId::new();
    let proj_dir = tempfile::TempDir::new().unwrap();
    let path = proj_dir.path().to_string_lossy().to_string();
    let server = TestServer::builder(factory::failing(giskard_core::HarnessError::Spawn(
        "unknown provider: cloudflare-litellm".into(),
    )))
    .seed(move |store| async move {
        store.create_project(pid, "proj", &path).await.unwrap();
        store
            .save_thread(
                pid,
                &fixtures::orphaned_thread(pid, tid, orphaned_model(), None),
            )
            .await
            .unwrap();
        for i in 0..2 {
            store
                .append_turn(
                    pid,
                    tid,
                    &fixtures::completed_turn(&format!("turn {i}"), orphaned_model()),
                )
                .await
                .unwrap();
        }
    })
    .start()
    .await;
    let base = server.base.clone();
    let client = server.client.clone();
    let cookie = server.cookie.clone();

    // The browser opens a thread over HTTP *before* subscribing on the WebSocket, so this
    // endpoint must also degrade to a read-only open (200 + warning) instead of a 500 when the
    // harness can't attach — otherwise the UI aborts before the WS path is ever reached.
    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "HTTP open of an orphaned thread must not hard-fail"
    );
    let open: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(open["thread_id"].as_str().unwrap(), tid.to_string());
    assert_eq!(open["warning"]["code"], "thread_read_only");
    let http_message = open["warning"]["message"].as_str().unwrap_or_default();
    assert!(
        http_message.contains("\"cloudflare-litellm\"") && http_message.contains("Pick a model"),
        "read-only message must still name the provider and the recovery action: {http_message}"
    );
    assert!(
        !http_message.contains("no longer configured"),
        "with no way to verify, the message must not accuse config: {http_message}"
    );
    assert_eq!(
        open["harness_thread_id"].as_str().unwrap(),
        format!("harness-{tid}")
    );

    let mut ws = server.ws().await;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    // The socket reports the read-only attach warning and still bootstraps persisted history.
    let mut read_only_warning: Option<serde_json::Value> = None;
    let mut history_delta: Option<serde_json::Value> = None;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline
        && (read_only_warning.is_none() || history_delta.is_none())
    {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                // `ServerMessage::Error` flattens `ErrorInfo`, so its fields sit at the top level.
                match v["type"].as_str() {
                    Some("error") if v["code"] == "thread_read_only" => read_only_warning = Some(v),
                    Some("history_delta") => history_delta = Some(v),
                    _ => {}
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    let history_delta =
        history_delta.expect("read-only subscribe must bootstrap persisted history over WebSocket");
    assert_eq!(history_delta["thread_id"], tid.to_string());
    assert_eq!(history_delta["turns"].as_array().unwrap().len(), 2);
    assert_eq!(history_delta["reset"], true);

    // The persisted history also remains available through pagination.
    let page: serde_json::Value = client
        .get(format!("{base}/api/projects/{pid}/threads/{tid}/history"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["turns"].as_array().unwrap().len(), 2);

    // …and the attach failure is surfaced as a non-fatal warning, not a hard error.
    let warning =
        read_only_warning.expect("read-only subscribe must surface a thread_read_only warning");
    assert_eq!(warning["severity"], "warning");
    let message = warning["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("\"cloudflare-litellm\"") && message.contains("Pick a model"),
        "subscribe warning must name the provider and the recovery action: {message}"
    );
    let detail = warning["detail"].as_str().unwrap_or_default();
    assert!(
        detail.contains("cloudflare-litellm"),
        "warning detail should explain the attach failure: {detail}"
    );
}
