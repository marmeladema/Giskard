//! Integration tests for the on-demand tracing endpoints (spec §17).
//!
//! Exercises arm/disarm, Perfetto JSON export, and UI span ingest, including the auth gate and
//! the "capture not armed" degradation path.

use std::sync::Arc;

use giskard_core::error::HarnessError;
use giskard_persist::store::ProjectConfig;
use giskard_proto::UiSpanBatch;
use giskard_server::{AppState, HarnessFactory, build_app};

struct NoopFactory;
#[async_trait::async_trait]
impl HarnessFactory for NoopFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Err(HarnessError::Unsupported("noop".into()))
    }
}

fn generate_password_hash(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand::rngs::OsRng;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

async fn start_server(extra_config: &str) -> (tempfile::TempDir, Arc<AppState>, u16) {
    let tmp = tempfile::tempdir().unwrap();
    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

{extra_config}
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let state = AppState::new(store, Arc::new(NoopFactory), session_key);
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (tmp, Arc::new(state), port)
}

async fn login(client: &reqwest::Client, base: &str) -> String {
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

fn base(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

#[tokio::test]
async fn trace_endpoints_require_auth() {
    let (_tmp, _state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    // Without a session cookie, all trace endpoints reject.
    assert_eq!(
        client
            .get(format!("{b}/admin/trace"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{b}/admin/trace/arm"))
            .json(&serde_json::json!({"armed": true}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{b}/api/traces/ui"))
            .json(&UiSpanBatch::default())
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
}

#[tokio::test]
async fn arm_then_export_returns_perfetto_json() {
    let (_tmp, state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;

    // Capture is off by default → /admin/trace conflicts.
    let r = client
        .get(format!("{b}/admin/trace"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);

    // Arm capture.
    let r = client
        .post(format!("{b}/admin/trace/arm"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"armed": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(state.trace.is_armed());

    // The config endpoint must mirror the armed state so the browser UI can show it.
    let r = client
        .get(format!("{b}/api/traces/config"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let cfg: serde_json::Value = r.json().await.unwrap();
    assert_eq!(cfg["armed"], true);
    assert_eq!(cfg["ui_ingest_enabled"], true);

    // Record a server-side span directly into the shared buffer and export.
    state
        .trace
        .record(vec![giskard_server::trace::RecordedSpan {
            name: "load_history".into(),
            trace_id: "0af7651916cd43dd8448eb211c80319c".into(),
            span_id: "b7ad6b7169203331".into(),
            parent_span_id: None,
            start_us: 1000,
            end_us: 2500,
            labels: std::collections::HashMap::from([("turns".into(), "50".into())]),
        }]);

    let r = client
        .get(format!("{b}/admin/trace"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body = r.text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v.is_array());
    // The span is emitted as an async begin/end pair (ph:b / ph:e) sharing the span id.
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2, "one span -> begin + end event");
    let begin = arr
        .iter()
        .find(|e| e["ph"] == "b")
        .expect("begin event present");
    let end = arr
        .iter()
        .find(|e| e["ph"] == "e")
        .expect("end event present");
    assert_eq!(begin["name"], "load_history");
    assert_eq!(begin["ts"], 1000);
    assert_eq!(begin["id"], "b7ad6b7169203331");
    assert_eq!(begin["args"]["turns"], "50");
    assert_eq!(end["name"], "load_history");
    assert_eq!(end["ts"], 2500);
    assert_eq!(end["id"], "b7ad6b7169203331");

    // Disarm.
    let _ = client
        .post(format!("{b}/admin/trace/arm"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"armed": false}))
        .send()
        .await
        .unwrap();
    assert!(!state.trace.is_armed());
}

#[tokio::test]
async fn ingest_ui_spans_requires_armed() {
    let (_tmp, _state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;
    // Not armed → 409 conflict with a structured message.
    let r = client
        .post(format!("{b}/api/traces/ui"))
        .header("cookie", &cookie)
        .json(&UiSpanBatch::default())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
}

#[tokio::test]
async fn ingest_ui_spans_rejects_oversized_batch() {
    let (_tmp, state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;
    state.trace.set_armed(true);
    let batch = UiSpanBatch {
        spans: (0..giskard_server::trace::UI_SPAN_BATCH_MAX + 1)
            .map(|i| giskard_proto::UiSpan {
                name: format!("s{i}"),
                trace_id: "0af7651916cd43dd8448eb211c80319c".into(),
                span_id: format!("{i:016x}"),
                parent_span_id: None,
                start_us: 0,
                end_us: 1,
                labels: Default::default(),
            })
            .collect(),
    };
    let r = client
        .post(format!("{b}/api/traces/ui"))
        .header("cookie", &cookie)
        .json(&batch)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn ingest_ui_spans_drops_oversized_name_or_labels() {
    let (_tmp, state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;
    state.trace.set_armed(true);
    use giskard_server::trace::{
        UI_SPAN_LABEL_COUNT_MAX, UI_SPAN_LABEL_FIELD_MAX, UI_SPAN_NAME_MAX,
    };
    // One oversized-name span, one with too many labels, one with an oversized label value, and
    // one valid span. The three oversized spans are dropped; the valid one survives and is
    // merged into the buffer.
    let valid = giskard_proto::UiSpan {
        name: "valid_span".into(),
        trace_id: "0af7651916cd43dd8448eb211c80319c".into(),
        span_id: "00000000000000ff".into(),
        parent_span_id: None,
        start_us: 10,
        end_us: 20,
        labels: Default::default(),
    };
    let oversize_name = giskard_proto::UiSpan {
        name: "x".repeat(UI_SPAN_NAME_MAX + 1),
        ..valid.clone()
    };
    let oversize_count = giskard_proto::UiSpan {
        span_id: "00000000000000fe".into(),
        labels: (0..UI_SPAN_LABEL_COUNT_MAX + 1)
            .map(|i| (format!("k{i}"), "v".into()))
            .collect(),
        ..valid.clone()
    };
    let oversize_value = giskard_proto::UiSpan {
        span_id: "00000000000000fd".into(),
        labels: std::collections::HashMap::from([(
            "k".into(),
            "v".repeat(UI_SPAN_LABEL_FIELD_MAX + 1),
        )]),
        ..valid.clone()
    };
    let batch = UiSpanBatch {
        spans: vec![oversize_name, oversize_count, oversize_value, valid],
    };
    let r = client
        .post(format!("{b}/api/traces/ui"))
        .header("cookie", &cookie)
        .json(&batch)
        .send()
        .await
        .unwrap();
    // The batch itself is within the size limit, so the request is accepted (204). The
    // oversized spans are dropped server-side; the valid one is merged.
    assert_eq!(r.status(), 204);
    let traces = state.trace.flush(None);
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].spans.len(), 1, "only the valid span survives");
    assert_eq!(traces[0].spans[0].name, "valid_span");
}

#[tokio::test]
async fn ingest_ui_spans_accepts_numeric_and_bool_labels_as_strings() {
    // B-1 regression: the browser attaches numeric/bool label values (turns: 5, has_more: true)
    // but the server wire contract is HashMap<String,String>. JSON numbers/bools fail to
    // deserialize and axum rejects the WHOLE batch (422), dropping every span in it — so the
    // client↔server waterfall never materializes. The recorder must stringify at the boundary.
    // This test POSTs what the browser actually sends (post-fix, stringified) and asserts 204 +
    // the values are stored as strings. It also POSTs the pre-fix shape (raw numbers) to prove
    // the wire contract itself rejects non-strings, documenting why the recorder must stringify.
    let (_tmp, state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;
    state.trace.set_armed(true);

    // Stringified labels (the recorder's post-fix shape) must be accepted.
    let span = serde_json::json!({
        "name": "ui.await_history_page",
        "trace_id": "0af7651916cd43dd8448eb211c80319c",
        "span_id": "b7ad6b7169203331",
        "parent_span_id": null,
        "start_us": 10,
        "end_us": 20,
        "labels": { "turns": "5", "has_more": "true" }
    });
    let r = client
        .post(format!("{b}/api/traces/ui"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({ "spans": [span] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204, "stringified labels accepted");
    let traces = state.trace.flush(None);
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].spans.len(), 1);
    assert_eq!(
        traces[0].spans[0].labels.get("turns").map(|s| s.as_str()),
        Some("5"),
        "numeric label stored as string"
    );
    assert_eq!(
        traces[0].spans[0]
            .labels
            .get("has_more")
            .map(|s| s.as_str()),
        Some("true"),
        "bool label stored as string"
    );

    // Raw non-string labels (the pre-fix browser shape) must be rejected by the wire contract,
    // documenting why the recorder must stringify. axum returns 422 for the Json extractor.
    state.trace.set_armed(true);
    let bad_span = serde_json::json!({
        "name": "ui.render_turns",
        "trace_id": "0af7651916cd43dd8448eb211c80319d",
        "span_id": "b7ad6b7169203332",
        "parent_span_id": null,
        "start_us": 10,
        "end_us": 20,
        "labels": { "turns": 5, "has_more": true }
    });
    let r = client
        .post(format!("{b}/api/traces/ui"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({ "spans": [bad_span] }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        422,
        "raw non-string labels rejected by the wire contract (recorder must stringify)"
    );
}

#[tokio::test]
async fn ingest_ui_spans_stores_resource_timing_labels_as_strings() {
    // Per-call fetch spans (Part 1.4) attach Resource Timing labels (dns_ms / tcp_ms / tls_ms /
    // ttfb_ms / download_ms / transfer_size) on close. The recorder stringifies them at the
    // boundary (the wire contract is HashMap<String,String>), so the browser sends numbers as
    // strings. This test POSTs a fetch span with the stringified shape the recorder emits and
    // asserts the labels survive ingestion as strings, guarding the numeric-label-to-string
    // coercion on the fetch-span path specifically.
    let (_tmp, state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;
    state.trace.set_armed(true);

    let span = serde_json::json!({
        "name": "ui.fetch.render_markdown",
        "trace_id": "0af7651916cd43dd8448eb211c80319e",
        "span_id": "b7ad6b7169203333",
        "parent_span_id": null,
        "start_us": 1000,
        "end_us": 1420,
        "labels": {
            "route": "/api/projects/{id}/render",
            "dns_ms": "0",
            "tcp_ms": "0",
            "tls_ms": "0",
            "ttfb_ms": "42",
            "download_ms": "378",
            "transfer_size": "1024"
        }
    });
    let r = client
        .post(format!("{b}/api/traces/ui"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({ "spans": [span] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204, "resource-timing fetch span accepted");
    let traces = state.trace.flush(None);
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].spans.len(), 1);
    let labels = &traces[0].spans[0].labels;
    assert_eq!(
        labels.get("route").map(|s| s.as_str()),
        Some("/api/projects/{id}/render")
    );
    assert_eq!(labels.get("ttfb_ms").map(|s| s.as_str()), Some("42"));
    assert_eq!(labels.get("download_ms").map(|s| s.as_str()), Some("378"));
    assert_eq!(
        labels.get("transfer_size").map(|s| s.as_str()),
        Some("1024")
    );
}

#[tokio::test]
async fn ingest_ui_spans_disabled_returns_404() {
    let (_tmp, _state, port) = start_server("[tracing]\nui_ingest_enabled = false\n").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;
    let r = client
        .post(format!("{b}/api/traces/ui"))
        .header("cookie", &cookie)
        .json(&UiSpanBatch::default())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn trace_config_reports_armed_and_ui_flag() {
    let (_tmp, state, port) = start_server("").await;
    let client = reqwest::Client::new();
    let b = base(port);
    let cookie = login(&client, &b).await;
    state.trace.set_armed(true);
    let r = client
        .get(format!("{b}/api/traces/config"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["armed"], true);
    assert_eq!(v["ui_ingest_enabled"], true);
    // The config response carries non-draining buffer counts (spec §17.2 / F2.1) so the UI can
    // show "N spans" without consuming the capture.
    assert_eq!(v["trace_count"], 0);
    assert_eq!(v["span_count"], 0);
    // Record a span, then confirm the counts reflect it and the buffer is NOT drained.
    state
        .trace
        .record(vec![giskard_server::trace::RecordedSpan {
            name: "probe".into(),
            trace_id: "0af7651916cd43dd8448eb211c80319c".into(),
            span_id: "b7ad6b7169203331".into(),
            parent_span_id: None,
            start_us: 1,
            end_us: 2,
            labels: Default::default(),
        }]);
    let r2 = client
        .get(format!("{b}/api/traces/config"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    let v2: serde_json::Value = r2.json().await.unwrap();
    assert_eq!(
        v2["trace_count"], 1,
        "config counts reflect the recorded span"
    );
    assert_eq!(v2["span_count"], 1, "config counts reflect one span");
    assert_eq!(
        state.trace.flush(None).len(),
        1,
        "reading counts did not drain the buffer"
    );
}

#[tokio::test]
async fn http_request_span_carries_status_label() {
    // F1a regression: the `http.request` span must declare `status = field::Empty` up front so
    // the post-response `span.record("status", …)` actually emits (tracing drops `.record()`
    // for fields not in the fieldset). Drive a request through the live middleware in-process
    // (no socket, no global subscriber) and assert the exported span carries the status label.
    //
    // Drive the router with `oneshot` so the middleware runs inline within the request future
    // (no spawned server task). Scope the capture subscriber to that one future via
    // `WithSubscriber`, so it is the default only while polling this future — on whichever
    // thread polls it. No process-global subscriber, no cross-test contamination, no race
    // with parallel tests that hit non-200 paths.
    use axum::body::Body;
    use axum::http::Request;
    use giskard_server::trace::TraceCaptureLayer;
    use tower::ServiceExt;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    // Reuse the config + AppState setup, but do NOT start the network server — we drive the
    // router in-process.
    let tmp = tempfile::tempdir().unwrap();
    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let state = AppState::new(store, Arc::new(NoopFactory), session_key);
    let app = build_app(state.clone());
    state.trace.set_armed(true);

    // Log in WITHOUT the capture subscriber so the login request's span is not recorded.
    let login = app
        .clone()
        .oneshot(
            Request::post("/api/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"password":"testpass"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), 200);
    let cookie = login
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    // Scope the armed capture layer to THIS request's future only.
    let sub = tracing_subscriber::registry::Registry::default()
        .with(TraceCaptureLayer::new(state.trace.clone()));
    let resp = app
        .oneshot(
            Request::get("/api/traces/config")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .with_subscriber(sub)
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let traces = state.trace.flush(None);
    // The buffer holds only this request's spans (the login ran outside the scoped subscriber).
    let http_spans: Vec<&giskard_server::trace::RecordedSpan> = traces
        .iter()
        .flat_map(|t| t.spans.iter())
        .filter(|s| s.name == "http.request")
        .collect();
    assert!(
        !http_spans.is_empty(),
        "http.request span was captured for the armed request"
    );
    for span in &http_spans {
        let status = span
            .labels
            .get("status")
            .map(|s| s.as_str())
            .unwrap_or("(missing)");
        assert_eq!(
            status, "200",
            "http.request span must carry a status label (F1a); got {status}"
        );
        assert!(
            span.labels.contains_key("route"),
            "http.request span carries the matched route template"
        );
        assert!(
            span.labels.contains_key("method"),
            "http.request span carries the request method"
        );
    }
}
