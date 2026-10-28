//! Disk usage is process-wide HTTP state: reading it needs a session and nothing else — no
//! project selection, no thread subscription, no harness.

use giskard_proto::AppStatus;
use giskard_testenv::fake::{self, FakeHarness};
use giskard_testenv::{TestServer, auth};

#[tokio::test]
async fn status_route_reports_ui_version_and_disk_usage_without_starting_a_harness() {
    let harness = FakeHarness::new(());
    let server = TestServer::spawn(fake::factory(harness.clone())).await;

    let unauthenticated = server
        .client
        .get(format!("{}/api/status", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);

    let cookie = auth::login(&server.client, &server.base).await;
    let status = server
        .client
        .get(format!("{}/api/status", server.base))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<AppStatus>()
        .await
        .unwrap();

    // The page's embedded identity must match, or the browser would refuse to open a socket.
    let index = server
        .client
        .get(format!("{}/", server.base))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!status.ui_version.is_empty());
    assert!(
        index.contains(&format!(
            r#"<meta name="giskard-ui-version" content="{}" />"#,
            status.ui_version
        )),
        "status ui_version {:?} is the page's embedded identity",
        status.ui_version
    );

    // TestServer intentionally does not launch production background monitoring, so the route
    // serves the disabled projection.
    assert_eq!(status.disk_usage, server.state.disk_usage.current());
    assert!(!status.disk_usage.enabled);
    assert_eq!(status.disk_usage.checked_at, None);
    assert!(status.disk_usage.paths.is_empty());
    assert!(
        harness.core.calls().is_empty(),
        "health reads must not start a harness"
    );
}
