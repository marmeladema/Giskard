//! Integration tests for the hardening surface: login throttling, token domain separation,
//! sliding sessions, security headers, and browse-root confinement of project creation.

use giskard_core::error::HarnessError;
use giskard_server::auth::{SESSION_COOKIE, TokenPurpose, sign_token};
use giskard_testenv::{TestServer, auth, factory};

/// Pull an attribute value (up to the next `"`) that follows `start` — used to read the served,
/// content-hashed asset URLs (`/app.<hash>.js`) out of the index HTML.
fn attr_after(html: &str, start: &str) -> String {
    let s = html.find(start).expect("attribute prefix present") + start.len();
    let e = html[s..].find('"').expect("closing quote") + s;
    html[s..e].to_string()
}

/// Start a server on an ephemeral port with the given extra config sections appended to a
/// baseline `[server]`/`[auth]` config (password: "testpass").
async fn start_server(extra_config: &str) -> TestServer {
    TestServer::builder(factory::failing(HarnessError::Unsupported(
        "no harness in security tests".into(),
    )))
    .config(extra_config)
    .start()
    .await
}

#[tokio::test]
async fn security_headers_are_set_on_all_responses() {
    let server = start_server("").await;
    let base = server.base.clone();
    let client = server.client.clone();

    // The script/stylesheet live at content-hashed URLs; read the current ones from the index.
    let index = client
        .get(format!("{base}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let js = attr_after(&index, "<script src=\"");
    let css = attr_after(&index, "<link rel=\"stylesheet\" href=\"");

    for path in ["/", "/favicon.svg", &js, &css, "/api/projects"] {
        let resp = client.get(format!("{base}{path}")).send().await.unwrap();
        let headers = resp.headers();
        let csp = headers
            .get("content-security-policy")
            .unwrap_or_else(|| panic!("missing CSP on {path}"))
            .to_str()
            .unwrap();
        assert!(csp.contains("script-src 'self'"), "CSP on {path}: {csp}");
        assert!(csp.contains("worker-src 'self'"), "CSP on {path}: {csp}");
        assert!(
            csp.contains("frame-ancestors 'none'"),
            "CSP on {path}: {csp}"
        );
        assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
        assert_eq!(headers.get("x-frame-options").unwrap(), "DENY");
        assert_eq!(headers.get("referrer-policy").unwrap(), "no-referrer");
    }
}

#[tokio::test]
async fn index_page_has_no_inline_script() {
    let server = start_server("").await;
    let base = server.base.clone();
    let body = server
        .client
        .clone()
        .get(format!("{base}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // A strict `script-src 'self'` only protects if the page itself carries no inline code.
    assert!(!body.contains("<script>"), "index.html must not inline JS");
    assert!(!body.contains("<style>"), "index.html must not inline CSS");
    // Script/stylesheet are same-origin assets under content-hashed URLs (cache-busting).
    assert!(
        body.contains(r#"<script src="/app."#) && body.contains(r#".js"></script>"#),
        "script is a same-origin content-hashed asset"
    );
    assert!(
        body.contains(r#"<link rel="stylesheet" href="/app."#) && body.contains(r#".css" />"#),
        "stylesheet is a same-origin content-hashed asset"
    );
    assert!(body.contains(r#"<link rel="icon" href="/favicon.svg" type="image/svg+xml" />"#));
    assert!(
        body.contains(
            r#"<img class="sidebar-logo" src="/favicon.svg" width="24" height="24" alt="" aria-hidden="true" />"#
        )
    );
}

#[tokio::test]
async fn login_locks_out_after_repeated_failures() {
    let server = start_server("").await;
    let base = server.base.clone();
    let client = server.client.clone();

    // The first failures are tolerated (typos) and answered with an in-band `ok: false`.
    for _ in 0..4 {
        let resp = client
            .post(format!("{base}/api/login"))
            .json(&serde_json::json!({"password": "wrong"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["ok"], false);
    }

    // The 5th consecutive failure arms the lockout…
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "wrong"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // …after which even the *correct* password is rejected with 429 + Retry-After until the
    // window elapses (the throttle runs before password verification).
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    let retry_after: u64 = resp
        .headers()
        .get("retry-after")
        .expect("429 must carry Retry-After")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(retry_after >= 1);
    assert!(resp.headers().get("set-cookie").is_none());
}

#[tokio::test]
async fn ws_ticket_is_not_a_session_and_vice_versa() {
    let server = start_server("").await;
    let base = server.base.clone();
    let client = server.client.clone();
    let cookie = auth::login(&client, &base).await;
    let session_token = cookie
        .strip_prefix(&format!("{SESSION_COOKIE}="))
        .unwrap()
        .to_string();

    let ticket_response = client
        .get(format!("{base}/api/ws-ticket"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(
        ticket_response["ui_version"]
            .as_str()
            .is_some_and(|version| !version.is_empty())
    );
    let ticket: String = ticket_response["ticket"].as_str().unwrap().to_string();

    // A ticket presented as a session cookie must not authenticate API requests.
    let resp = client
        .get(format!("{base}/api/projects"))
        .header("cookie", format!("{SESSION_COOKIE}={ticket}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // A session token presented as a WS ticket must not authenticate the upgrade.
    let resp = client
        .get(format!("{base}/api/ws?ticket={session_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Sanity: a real ticket passes auth (the request then fails as a non-upgrade, not as 401).
    let resp = client
        .get(format!("{base}/api/ws?ticket={ticket}"))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), 401);
}

#[tokio::test]
async fn cookie_max_age_follows_session_days() {
    let server = start_server("").await;
    let base = server.base.clone();
    let resp = server
        .client
        .clone()
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let set_cookie = resp.headers().get("set-cookie").unwrap().to_str().unwrap();
    // session_days = 30 in the baseline config.
    assert!(
        set_cookie.contains("Max-Age=2592000"),
        "set-cookie: {set_cookie}"
    );
    assert!(set_cookie.contains("HttpOnly"), "set-cookie: {set_cookie}");
    assert!(
        set_cookie.contains("SameSite=Strict"),
        "set-cookie: {set_cookie}"
    );
}

#[tokio::test]
async fn session_is_renewed_past_the_lifetime_midpoint() {
    let server = start_server("").await;
    let base = server.base.clone();
    let client = server.client.clone();

    // A fresh session (full lifetime remaining) must not be re-issued on every request.
    let cookie = auth::login(&client, &base).await;
    let resp = client
        .get(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("set-cookie").is_none());

    // A valid session in the second half of its lifetime gets a refreshed cookie.
    let nearly_expired = chrono::Utc::now().timestamp() as u64 + 3600;
    let old_token =
        sign_token(TokenPurpose::Session, nearly_expired, &auth::session_key()).unwrap();
    let resp = client
        .get(format!("{base}/api/projects"))
        .header("cookie", format!("{SESSION_COOKIE}={old_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let renewed = resp
        .headers()
        .get("set-cookie")
        .expect("near-expiry session must be renewed")
        .to_str()
        .unwrap();
    assert!(renewed.starts_with(&format!("{SESSION_COOKIE}=")));
    assert!(renewed.contains("Max-Age=2592000"), "renewed: {renewed}");
}

#[tokio::test]
async fn create_project_is_confined_to_browse_roots() {
    let allowed = tempfile::TempDir::new().unwrap();
    let denied = tempfile::TempDir::new().unwrap();
    let allowed_path = allowed.path().canonicalize().unwrap();
    let extra = format!("[browse]\nroots = [{:?}]\n", allowed_path.to_str().unwrap());
    let server = start_server(&extra).await;
    let base = server.base.clone();
    let client = server.client.clone();
    let cookie = auth::login(&client, &base).await;

    let create = |dir: String| {
        let client = client.clone();
        let base = base.clone();
        let cookie = cookie.clone();
        async move {
            client
                .post(format!("{base}/api/projects"))
                .header("cookie", &cookie)
                .json(&serde_json::json!({
                    "name": "proj",
                    "dir": dir,
                }))
                .send()
                .await
                .unwrap()
        }
    };

    let resp = create(denied.path().to_string_lossy().to_string()).await;
    assert_eq!(resp.status(), 403);

    let resp = create(allowed_path.to_string_lossy().to_string()).await;
    assert_eq!(resp.status(), 200);
}
