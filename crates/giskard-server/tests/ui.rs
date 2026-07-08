//! Smoke test: the desktop UI page is served at `/`, is public, and is self-contained (§13).

use std::sync::Arc;

use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};

struct NoFactory;
#[async_trait::async_trait]
impl HarnessFactory for NoFactory {
    async fn create(
        &self,
        _c: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, giskard_core::HarnessError> {
        Err(giskard_core::HarnessError::Spawn("unused".into()))
    }
}

#[tokio::test]
async fn index_page_is_served_and_public() {
    let port = 19300;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(store, Arc::new(NoFactory), (0..32u8).collect());
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // No cookie: the page must load without authentication.
    let body = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("<title>Giskard</title>"));
    assert!(body.contains("/api/ws"), "app talks to the WS endpoint");
    assert!(
        body.contains("/api/ws-ticket"),
        "app fetches a WS auth ticket"
    );
    assert!(body.contains("send_input"), "composer wired to SendInput");
    assert!(
        body.contains("id=\"stopBtn\""),
        "UI exposes a stop button for live turns"
    );
    assert!(
        body.contains("type:\"interrupt\""),
        "stop button sends the Interrupt client message"
    );
    assert!(
        body.contains("activeTurn"),
        "UI tracks active turns for interrupt controls"
    );
    assert!(
        body.contains("ctxCommands"),
        "right panel exposes a running command summary"
    );
    assert!(
        body.contains("case \"running_commands\""),
        "UI consumes server-owned running command snapshots"
    );
    assert!(
        body.contains("commandBodyElsByItemId"),
        "UI tracks running command transcript rows after finalization"
    );
    assert!(
        body.contains("commandIsRunningStatus"),
        "UI keeps in-progress completed command items visible"
    );
    assert!(
        body.contains("setInterval(updateRunningCommandDurations, 1000)"),
        "UI refreshes running command durations once per second"
    );
    assert!(
        body.contains("terminalCommandStatus"),
        "UI renders terminal command status with elapsed duration"
    );
    assert!(
        body.contains("commandStopRequestedByItemId"),
        "UI remembers stop requests until terminal command status arrives"
    );
    assert!(
        body.contains("stop requested after"),
        "UI labels pending command termination as a stop request"
    );
    assert!(
        body.contains("(stop requested)"),
        "UI annotates terminal command status after a stop request"
    );
    assert!(
        body.contains("No longer tracked"),
        "UI has a non-terminal fallback for stale stopped command snapshots"
    );
    assert!(
        !body.contains("terminatedByUser && p.status"),
        "UI must not rewrite successful terminal status to terminated"
    );
    assert!(
        body.contains("commandVisualStateFromStatus"),
        "UI maps command statuses to visual states"
    );
    assert!(
        body.contains("commandStateSymbol"),
        "UI renders command state symbols"
    );
    assert!(
        body.contains("cmd-symbol"),
        "UI includes a non-color command state cue"
    );
    assert!(
        body.contains("state-running"),
        "UI styles running command state"
    );
    assert!(
        body.contains("state-succeeded"),
        "UI styles succeeded command state"
    );
    assert!(
        body.contains("state-failed"),
        "UI styles failed command state"
    );
    assert!(
        body.contains("state-terminated"),
        "UI styles terminated command state"
    );
    assert!(
        body.contains("s===\"completed\" || s===\"succeeded\" || s===\"success\""),
        "UI maps successful command statuses to succeeded state"
    );
    assert!(
        body.contains("s===\"failed\" || s===\"error\""),
        "UI maps failed command statuses to failed state"
    );
    assert!(
        body.contains("s===\"terminated\" || s===\"declined\""),
        "UI maps terminated and declined command statuses to terminated state"
    );
    assert!(
        body.contains("if (stateName===\"succeeded\") return \"✓\""),
        "UI renders the succeeded command symbol"
    );
    assert!(
        body.contains("if (stateName===\"failed\") return \"✕\""),
        "UI renders the failed command symbol"
    );
    assert!(
        body.contains("if (stateName===\"terminated\") return \"■\""),
        "UI renders the terminated command symbol"
    );
    assert!(
        body.contains("return \"●\""),
        "UI renders the running command symbol"
    );
    assert!(
        !body.contains(".cmd-status::before"),
        "command state symbols are DOM content, not CSS-only decoration"
    );
    assert!(
        body.contains("startedAtMs"),
        "UI tracks command start timestamps for elapsed duration"
    );
    assert!(
        body.contains("selectCommand"),
        "running command summary can select transcript command rows"
    );
    assert!(
        body.contains("type:\"terminate_command\""),
        "running command controls send terminate requests"
    );
    assert!(
        body.contains("running-command"),
        "transcript command rows expose running command state"
    );
    assert!(
        body.contains("pendingClientMsgs"),
        "messages are queued while WS connects"
    );
    assert!(
        body.contains("Connecting to agent"),
        "UI exposes WS connecting state"
    );
    assert!(
        body.contains("WebSocket closed"),
        "UI surfaces WS close failures"
    );
    assert!(
        body.contains("Invalid WebSocket message from server"),
        "UI surfaces malformed WS frames"
    );
    assert!(
        body.contains("renderedHarnessItemIds"),
        "UI de-duplicates rendered harness items"
    );
    assert!(
        body.contains("streamElsByItemId"),
        "UI tracks streamed items independently"
    );
    assert!(
        body.contains("renderedItemIds"),
        "UI de-duplicates completed items by Giskard item id"
    );
    assert!(
        body.contains("finalizeStreamedItem"),
        "UI finalizes streamed agent messages instead of duplicating them"
    );
    assert!(
        body.contains("hasVisiblePayload"),
        "UI has an explicit visibility gate for transcript items"
    );
    assert!(
        body.contains("p.kind===\"file_change\""),
        "UI has explicit file-change transcript handling"
    );
    assert!(
        body.contains("renderFileChange"),
        "UI renders file-change items"
    );
    assert!(
        body.contains("renderToolCall"),
        "UI renders tool-call items"
    );
    assert!(
        body.contains("renderActivity"),
        "UI renders generic activity items"
    );
    assert!(
        body.contains("/linkify"),
        "UI calls the server-side path linkifier"
    );
    assert!(
        body.contains("renderCommandOutputBlock"),
        "UI renders command output through the collapsible output block"
    );
    assert!(
        body.contains("if (phase === \"completed\") return false"),
        "completed command output is collapsed by default"
    );
    assert!(
        body.contains("commandOutputShouldAutoCollapse"),
        "running command output can auto-collapse when it grows large"
    );
    assert!(
        body.contains("COMMAND_AUTO_COLLAPSE_LINES"),
        "UI has a line threshold for running command auto-collapse"
    );
    assert!(
        body.contains("expandedCommandOutputs"),
        "UI tracks command output expansion state by item"
    );
    assert!(
        body.contains("manuallyToggledCommandOutputs"),
        "UI preserves manual command output toggles while the thread is open"
    );
    assert!(
        body.contains("toggleCommandOutput"),
        "command rows can toggle collapsed output"
    );
    assert!(
        body.contains("wireCommandRowToggle"),
        "the transcript command row owns the output toggle handler"
    );
    assert!(
        body.contains("e.target.closest(\"button,a,input,select,textarea\")"),
        "nested controls inside command rows do not trigger collapse"
    );
    assert!(
        !body.contains("cmd-toggle"),
        "command output collapse must not use a separate arrow-style button"
    );
    assert!(
        body.contains("grid-template-columns:78px minmax(0,1fr)"),
        "IDE transcript grid cannot be widened by command output"
    );
    assert!(
        body.contains("grid-template-columns:66px minmax(0,1fr)"),
        "terminal transcript grid cannot be widened by command output"
    );
    assert!(
        body.contains("overflow-x:hidden"),
        "the transcript column suppresses horizontal overflow"
    );
    assert!(
        body.contains("overflow-wrap:anywhere"),
        "long command output can wrap instead of widening the row"
    );
    assert!(
        body.contains("Output collapsed"),
        "collapsed command rows summarize hidden output"
    );
    assert!(
        body.contains("if (opts.linkify) renderLinkedText(out, output)"),
        "UI linkifies command output only when the output block is expanded"
    );
    assert!(
        body.contains("renderLinkedText(body, p.text"),
        "UI linkifies completed agent/reasoning text through the server"
    );
    assert!(
        body.contains("openCodeOverlay"),
        "UI opens a source overlay for linked paths"
    );
    assert!(
        body.contains("openCodeOverlay(value, targetLine)"),
        "UI passes linkified line targets into the source overlay"
    );
    assert!(
        body.contains("makePathLink(c.path"),
        "UI routes structured file-change paths into the source overlay"
    );
    assert!(
        body.contains("projectFileUrl(\"highlight\""),
        "UI requests server-side syntax highlighted source"
    );
    assert!(
        body.contains("projectFileUrl(\"raw\""),
        "UI exposes raw file downloads"
    );
    assert!(
        body.contains("codeDownload"),
        "UI has a download button for source files"
    );
    assert!(
        body.contains("code-line-nos"),
        "UI renders source line numbers in the code overlay"
    );
    assert!(
        body.contains("scrollToCodeLine"),
        "UI scrolls the code overlay to a requested line"
    );
    assert!(
        body.contains("view.clientHeight / 2"),
        "UI centers requested source lines in the code overlay"
    );
    assert!(
        body.contains("live_turn_snapshot"),
        "UI replays active-turn snapshots on reconnect"
    );
    assert!(
        body.contains("itemKindsByItemId"),
        "UI tracks item kinds for streamed deltas"
    );
    assert!(
        body.contains("addItem(ev.item)"),
        "UI renders completed events with full item metadata"
    );
    assert!(
        body.contains("addItem(it)"),
        "UI replays persisted history with full item metadata"
    );
    // Self-contained: no external script/style hosts.
    assert!(!body.contains("http://"), "no external http asset refs");
    assert!(!body.contains("cdn"), "no CDN references");
}
