//! A terminal item that settles after its turn was persisted is recorded durably (LA1–LA4).
//!
//! The harness here is the shape the Codex mapper produces for a real late completion: a turn that
//! finishes while one of its commands is still running, and the command's `ItemCompleted` arriving
//! afterwards, resolved back to the turn it belongs to. Every test drives that through the real
//! server path — registry forward, classification as late, amendment, hub publish — and then asks a
//! reader that a browser would use.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::SinkExt;
use giskard_core::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{CommandExecutionStart, ToolCallStart};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_proto::{ClientMessage, ServerMessage};
use giskard_testenv::fake::{self, FakeCore, FakeHarness, Gate, Script, TurnCall};
use giskard_testenv::ws::TestWs;
use giskard_testenv::{TestProject, TestServer, ws};

const COMMAND: &str = "cargo test --workspace";
const LATE_OUTPUT: &str = "running 214 tests\ntest result: ok. 214 passed\n";
const RE_AMENDED_OUTPUT: &str = "running 214 tests\ntest result: FAILED. 1 failed\n";
const HARNESS_ITEM_ID: &str = "native-late-item";

#[derive(Clone, Copy, PartialEq, Eq)]
enum LateKind {
    Command,
    Tool,
}

/// Starts a turn with one still-running item, completes the turn, and then — once the test opens
/// the gate — appends that item's completion. The completion therefore always arrives after the
/// turn has been persisted, which is what makes it late.
struct LateScript {
    kind: LateKind,
    item_id: ItemId,
    gate: Arc<Gate>,
    /// A second completion for the same item, for the case where an already-amended item settles
    /// again — the one that proves the route does not keep serving the first amendment's version.
    re_amend_gate: Arc<Gate>,
}

impl LateScript {
    fn new(kind: LateKind) -> Self {
        Self {
            kind,
            item_id: ItemId::new(),
            gate: Arc::new(Gate::held()),
            re_amend_gate: Arc::new(Gate::held()),
        }
    }

    fn started_item(&self) -> ItemStart {
        match self.kind {
            LateKind::Command => ItemStart {
                id: self.item_id,
                harness_item_id: HARNESS_ITEM_ID.into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: COMMAND.into(),
                    cwd: ".".into(),
                    status: Some("in_progress".into()),
                    process_id: Some("proc-late".into()),
                    started_at_ms: Some(1_785_000_000_000),
                }),
                tool: None,
            },
            LateKind::Tool => ItemStart {
                id: self.item_id,
                harness_item_id: HARNESS_ITEM_ID.into(),
                kind: ItemKind::ToolCall,
                command: None,
                tool: Some(ToolCallStart {
                    name: "search".into(),
                    input: serde_json::json!({ "q": "late" }),
                    server: Some("wiki".into()),
                    status: Some("in_progress".into()),
                    metadata: None,
                    subagent: None,
                    started_at_ms: Some(1_785_000_000_000),
                }),
            },
        }
    }

    fn settled_item(&self, output: &str) -> Item {
        let payload = match self.kind {
            LateKind::Command => ItemPayload::CommandExecution {
                command: COMMAND.into(),
                cwd: ".".into(),
                output: output.into(),
                output_truncated: false,
                output_original_bytes: None,
                output_original_lines: None,
                exit_code: Some(0),
                status: Some("completed".into()),
                process_id: Some("proc-late".into()),
                duration_ms: Some(92_000),
            },
            LateKind::Tool => ItemPayload::ToolCall {
                name: "search".into(),
                input: serde_json::json!({ "q": "late" }),
                output: Some(serde_json::json!({ "hits": ["late completion"] })),
                server: Some("wiki".into()),
                status: Some("completed".into()),
                metadata: None,
                subagent: None,
                error: None,
            },
        };
        Item {
            id: self.item_id,
            harness_item_id: HARNESS_ITEM_ID.into(),
            payload,
            created_at: chrono::Utc::now(),
        }
    }
}

#[async_trait]
impl Script for LateScript {
    fn native_thread_id(&self, _thread: ThreadId) -> String {
        "late_harness".into()
    }

    async fn start_turn(&self, core: &FakeCore, call: &TurnCall) -> Result<(), HarnessError> {
        call.log.append(AgentEvent::TurnStarted {
            thread: call.thread,
            turn: call.turn,
        });
        call.log.append(AgentEvent::ItemStarted {
            thread: call.thread,
            turn: call.turn,
            item: self.started_item(),
        });
        // The turn ends with the item still running: the forwarder persists it as it stands.
        core.complete_turn(call.thread, call.turn);

        let gate = self.gate.clone();
        let re_amend_gate = self.re_amend_gate.clone();
        let log = call.log.clone();
        let thread = call.thread;
        let turn = call.turn;
        let settled = self.settled_item(LATE_OUTPUT);
        let re_amended = self.settled_item(RE_AMENDED_OUTPUT);
        tokio::spawn(async move {
            gate.pass().await;
            log.append(AgentEvent::ItemCompleted {
                thread,
                turn,
                item: settled,
            });
            re_amend_gate.pass().await;
            log.append(AgentEvent::ItemCompleted {
                thread,
                turn,
                item: re_amended,
            });
        });
        Ok(())
    }
}

struct Fixture {
    server: TestServer,
    project: TestProject,
    harness: Arc<FakeHarness<LateScript>>,
    thread_id: ThreadId,
}

impl Fixture {
    fn pid(&self) -> ProjectId {
        self.project.id
    }

    fn item_id(&self) -> ItemId {
        self.harness.script.item_id
    }

    fn release(&self) {
        self.harness.script.gate.release();
    }

    fn release_re_amendment(&self) {
        self.harness.script.re_amend_gate.release();
    }

    fn url(&self, suffix: &str) -> String {
        format!(
            "http://127.0.0.1:{}/api/projects/{}/threads/{}{suffix}",
            self.server.addr.port(),
            self.pid(),
            self.thread_id
        )
    }

    fn threads_dir(&self) -> std::path::PathBuf {
        self.server
            .state
            .store
            .data_dir()
            .join("projects")
            .join(self.pid().to_string())
            .join("threads")
    }

    fn payload_path(&self, turn: TurnId) -> std::path::PathBuf {
        giskard_persist::layout::ThreadPaths::new(
            self.threads_dir(),
            self.thread_id,
            giskard_persist::ThreadLayout::Directory,
        )
        .turn_payload(turn)
    }

    async fn get(&self, url: &str) -> reqwest::Response {
        reqwest::Client::new()
            .get(url)
            .header("cookie", &self.server.cookie)
            .send()
            .await
            .unwrap()
    }
}

async fn start(kind: LateKind) -> Fixture {
    let harness = FakeHarness::new(LateScript::new(kind));
    let server = TestServer::spawn(fake::factory(harness.clone())).await;
    let project = server.create_project("late-item").await;
    let thread_id = server.register_thread(project.id, "late_harness").await;
    Fixture {
        server,
        project,
        harness,
        thread_id,
    }
}

async fn subscribe(fixture: &Fixture, since: Option<TurnId>) -> TestWs {
    let mut socket = fixture.server.ws().await;
    socket
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id: fixture.thread_id,
            since,
        }))
        .await
        .unwrap();
    socket
}

async fn send_input(fixture: &Fixture, socket: &mut TestWs) {
    socket
        .send(ws::text(&ClientMessage::SendInput {
            thread_id: fixture.thread_id,
            text: "run the suite".into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();
}

/// Runs one turn to completion with its item still running, and returns the persisted turn's id.
async fn run_turn_leaving_the_item_running(fixture: &Fixture, socket: &mut TestWs) -> TurnId {
    let turn = complete_turn_leaving_the_item_running(fixture, socket).await;
    wait_for_persisted_turn(fixture, turn).await;
    turn
}

/// The same, without waiting on the turn index — for a thread whose history this build cannot
/// parse, where the wait has to be made on the file itself.
async fn complete_turn_leaving_the_item_running(fixture: &Fixture, socket: &mut TestWs) -> TurnId {
    send_input(fixture, socket).await;
    ws::recv_until(socket, |message| match message {
        ServerMessage::Event { agent_event, .. } => match *agent_event {
            giskard_proto::WireAgentEvent::TurnCompleted { turn, .. } => Some(turn),
            _ => None,
        },
        _ => None,
    })
    .await
    .expect("the turn completes while its item is still running")
}

async fn wait_for_persisted_turn(fixture: &Fixture, turn: TurnId) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let records = fixture
            .server
            .state
            .store
            .load_turn_records(fixture.pid(), fixture.thread_id)
            .await
            .unwrap();
        if records.iter().any(|record| record.turn_id == turn) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("turn {turn} was not persisted");
}

/// A turn persists only the items that had completed when it ended, so until the amendment lands
/// the payload does not carry this item at all. The amendment is written after the completion is
/// forwarded, so a reader polls for it.
async fn wait_for_settled_item(fixture: &Fixture, turn: TurnId) -> Item {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let found = fixture
            .server
            .state
            .store
            .load_turn_item(fixture.pid(), fixture.thread_id, turn, fixture.item_id())
            .await
            .unwrap();
        if let Some(item) = found.filter(item_is_settled) {
            return item;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the late completion was never persisted"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn item_is_settled(item: &Item) -> bool {
    match &item.payload {
        ItemPayload::CommandExecution { status, .. } => status.as_deref() == Some("completed"),
        ItemPayload::ToolCall { status, .. } => status.as_deref() == Some("completed"),
        _ => false,
    }
}

async fn expect_late_item_completion(socket: &mut TestWs) {
    ws::recv_until(socket, |message| match message {
        ServerMessage::Event { agent_event, .. } => match *agent_event {
            giskard_proto::WireAgentEvent::ItemCompleted { ref item, .. }
                if item.harness_item_id == HARNESS_ITEM_ID =>
            {
                Some(())
            }
            _ => None,
        },
        _ => None,
    })
    .await
    .expect("the late completion still reaches a connected client");
}

fn etag(response: &reqwest::Response) -> String {
    response
        .headers()
        .get("etag")
        .expect("the output carries a version")
        .to_str()
        .unwrap()
        .to_owned()
}

/// Polls until the persisted command output reads `expected`.
async fn wait_for_output(fixture: &Fixture, turn: TurnId, expected: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let item = fixture
            .server
            .state
            .store
            .load_turn_item(fixture.pid(), fixture.thread_id, turn, fixture.item_id())
            .await
            .unwrap();
        if let Some(Item {
            payload: ItemPayload::CommandExecution { output, .. },
            ..
        }) = item
            && output == expected
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the persisted output never became {expected:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn history_delta(socket: &mut TestWs) -> (Vec<giskard_proto::WireTurn>, bool) {
    ws::recv_until(socket, |message| match message {
        ServerMessage::HistoryDelta { turns, reset, .. } => Some((turns, reset)),
        _ => None,
    })
    .await
    .expect("a history delta")
}

// ---- G1: durable ----

/// The live client still sees the completion, and it is now durable: a lazy read of the turn shows
/// the item settled, and the command-output route serves the late output with a fresh `ETag`.
#[tokio::test]
async fn a_late_command_completion_is_persisted_and_served_from_persistence() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;

    // A turn keeps only the items that had completed when it ended, so until the amendment lands
    // there is nothing for the route to serve.
    let url = fixture.url(&format!(
        "/turns/{turn}/items/{}/command-output",
        fixture.item_id()
    ));
    assert_eq!(fixture.get(&url).await.status(), 404);

    fixture.release();
    expect_late_item_completion(&mut socket).await;

    let settled = wait_for_settled_item(&fixture, turn).await;
    let ItemPayload::CommandExecution {
        output, exit_code, ..
    } = &settled.payload
    else {
        panic!("expected a command item, got {:?}", settled.payload);
    };
    assert_eq!(output, LATE_OUTPUT);
    assert_eq!(*exit_code, Some(0));

    let amended = fixture.get(&url).await;
    assert_eq!(amended.status(), 200);
    let etag = etag(&amended);
    assert!(!etag.is_empty(), "the amended output carries a version");
    assert_eq!(amended.text().await.unwrap(), LATE_OUTPUT);
}

/// An item that settles again after it was already amended is re-amended, and the route stops
/// serving the version it cached for the first amendment.
#[tokio::test]
async fn a_second_late_completion_replaces_the_cached_output_version() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;

    fixture.release();
    expect_late_item_completion(&mut socket).await;
    wait_for_settled_item(&fixture, turn).await;

    // Reading it once caches the first amendment's version against the item.
    let url = fixture.url(&format!(
        "/turns/{turn}/items/{}/command-output",
        fixture.item_id()
    ));
    let first = fixture.get(&url).await;
    let first_etag = etag(&first);
    assert_eq!(first.text().await.unwrap(), LATE_OUTPUT);

    fixture.release_re_amendment();
    wait_for_output(&fixture, turn, RE_AMENDED_OUTPUT).await;

    let second = fixture.get(&url).await;
    assert_eq!(second.status(), 200);
    let second_etag = etag(&second);
    assert_eq!(second.text().await.unwrap(), RE_AMENDED_OUTPUT);
    assert_ne!(
        first_etag, second_etag,
        "a cached version must not outlive the output it described"
    );
}

// ---- G2: reconnect ----

/// A client that was away for the amendment is told about it by the ordinary resync delta, with no
/// message of its own — and asking again with the same cursor returns it again.
#[tokio::test]
async fn a_reconnecting_client_receives_the_amended_turn_in_its_resync_delta() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;

    // Away for the amendment.
    drop(socket);
    fixture.release();
    wait_for_settled_item(&fixture, turn).await;

    let mut resumed = subscribe(&fixture, Some(turn)).await;
    let (turns, reset) = history_delta(&mut resumed).await;
    assert!(!reset, "an incremental resync, not a bounded reset");
    assert_eq!(
        turns.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![turn],
        "the delta carries the amended turn and nothing else"
    );
    let item = turns[0]
        .items
        .iter()
        .find(|item| item.harness_item_id == HARNESS_ITEM_ID)
        .expect("the amended item");
    let giskard_proto::WireItemPayload::CommandExecution {
        status, exit_code, ..
    } = &item.payload
    else {
        panic!("expected a command item, got {:?}", item.payload);
    };
    assert_eq!(status.as_deref(), Some("completed"));
    assert_eq!(*exit_code, Some(0));

    // A second reconnect on the same cursor is handed the same amendment: a client may see one
    // twice, it may never miss one.
    drop(resumed);
    let mut again = subscribe(&fixture, Some(turn)).await;
    let (turns, reset) = history_delta(&mut again).await;
    assert!(!reset);
    assert_eq!(turns.iter().map(|t| t.id).collect::<Vec<_>>(), vec![turn]);
}

// ---- G3: reload ----

/// A reload reads the history page, which now shows the item settled.
#[tokio::test]
async fn the_history_page_shows_a_late_command_settled() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;
    fixture.release();
    expect_late_item_completion(&mut socket).await;
    wait_for_settled_item(&fixture, turn).await;

    let page = fixture.get(&fixture.url("/history")).await;
    assert_eq!(page.status(), 200);
    let body: serde_json::Value = page.json().await.unwrap();
    let items = body["turns"]
        .as_array()
        .expect("turns")
        .iter()
        .find(|value| value["id"].as_str() == Some(&turn.to_string()))
        .expect("the amended turn")["items"]
        .as_array()
        .expect("items")
        .clone();
    let item = items
        .iter()
        .find(|item| item["harness_item_id"].as_str() == Some(HARNESS_ITEM_ID))
        .expect("the amended item");
    assert_eq!(item["payload"]["status"].as_str(), Some("completed"));
    assert_eq!(item["payload"]["exit_code"].as_i64(), Some(0));
}

// ---- G4: tool result ----

/// The same flow for a tool call: the tool-output route serves the late output from persistence
/// once the runtime copy is gone.
#[tokio::test]
async fn a_late_tool_completion_is_served_from_persistence() {
    let fixture = start(LateKind::Tool).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;

    // A late tool completion publishes no transcript event of its own; the durable record and the
    // lazy route are what a client sees it through.
    fixture.release();
    wait_for_settled_item(&fixture, turn).await;

    // The runtime copy is dropped as part of the amendment, so this read comes from the payload.
    let response = fixture
        .get(&fixture.url(&format!(
            "/turns/{turn}/items/{}/tool-output",
            fixture.item_id()
        )))
        .await;
    assert_eq!(response.status(), 200);
    assert!(
        response.headers().get("etag").is_some(),
        "the persisted tool output carries a version"
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body, serde_json::json!({ "hits": ["late completion"] }));
}

// ---- G5: write failure ----

/// An amendment that cannot be written keeps the runtime copy: the completion still reaches the
/// socket and the route still serves the fresh output. Only a reload loses it.
#[tokio::test]
async fn a_failed_amendment_keeps_the_runtime_copy_serving_the_late_output() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;

    // A directory where the payload file belongs: the append cannot open it.
    let payload = fixture.payload_path(turn);
    tokio::fs::remove_file(&payload).await.unwrap();
    tokio::fs::create_dir(&payload).await.unwrap();

    fixture.release();
    expect_late_item_completion(&mut socket).await;

    let response = fixture
        .get(&fixture.url(&format!(
            "/turns/{turn}/items/{}/command-output",
            fixture.item_id()
        )))
        .await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.text().await.unwrap(),
        LATE_OUTPUT,
        "the runtime copy outlives a failed amendment"
    );
}

// ---- G6: flat layout ----

/// A thread still on the format 1 flat layout has no per-turn payload to amend. The amendment is
/// reported unsupported, nothing is written, and the late completion behaves exactly as it did
/// before durable amendments existed.
#[tokio::test]
async fn a_flat_layout_thread_is_unsupported_and_behaves_as_before() {
    let fixture = start(LateKind::Command).await;

    // A format 1 history whose interior line cannot be parsed: the migration would lose turns, so
    // the thread stays flat. That is the only way a live thread is still on format 1.
    let threads_dir = fixture.threads_dir();
    let thread_file = fixture
        .server
        .state
        .store
        .load_thread(fixture.pid(), fixture.thread_id)
        .await
        .unwrap()
        .unwrap();
    tokio::fs::remove_dir_all(threads_dir.join(fixture.thread_id.to_string()))
        .await
        .unwrap();
    tokio::fs::write(
        threads_dir.join(format!("{}.json", fixture.thread_id)),
        serde_json::to_vec_pretty(&thread_file).unwrap(),
    )
    .await
    .unwrap();
    let flat_history = threads_dir.join(format!("{}.jsonl", fixture.thread_id));
    tokio::fs::write(&flat_history, "torn interior line\n{}\n")
        .await
        .unwrap();
    let mut socket = subscribe(&fixture, None).await;
    let turn = complete_turn_leaving_the_item_running(&fixture, &mut socket).await;

    // The flat history this build cannot parse is still appended to, so wait on the file.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let data = tokio::fs::read_to_string(&flat_history).await.unwrap();
        if data.contains(&turn.to_string()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the turn was never appended to the flat history"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let before = tokio::fs::read(&flat_history).await.unwrap();

    fixture.release();
    expect_late_item_completion(&mut socket).await;

    // Today's behaviour, unchanged: the live client saw it, and nothing durable moved.
    assert_eq!(
        tokio::fs::read(&flat_history).await.unwrap(),
        before,
        "an unsupported amendment writes nothing"
    );
    assert!(
        !threads_dir
            .join(fixture.thread_id.to_string())
            .join("turns")
            .exists(),
        "an unsupported amendment does not create a turn payload"
    );
    // And the runtime copy was dropped, as it is today: the route has nothing left to serve. (The
    // status is the corrupt-history reader's, not the amendment's — this thread is unreadable by
    // construction, which is the only way a live thread is still on format 1.)
    let response = fixture
        .get(&fixture.url(&format!(
            "/turns/{turn}/items/{}/command-output",
            fixture.item_id()
        )))
        .await;
    assert_ne!(response.status(), 200);
    assert!(!response.text().await.unwrap().contains(LATE_OUTPUT));
}

// ---- H7: damaged record surfaced ----

/// A payload whose last append was torn loses that record, not the turn. Both readers a browser
/// uses deliver the turn with what remains, and say how much was skipped.
#[tokio::test]
async fn a_damaged_payload_record_is_skipped_and_reported_on_the_turn() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;

    // Two amendments, so tearing the last one leaves a readable record behind it.
    fixture.release();
    expect_late_item_completion(&mut socket).await;
    wait_for_settled_item(&fixture, turn).await;
    fixture.release_re_amendment();
    wait_for_output(&fixture, turn, RE_AMENDED_OUTPUT).await;
    drop(socket);

    // Tear the payload's last line, the way a crash mid-append would.
    let payload = fixture.payload_path(turn);
    let whole = tokio::fs::read_to_string(&payload).await.unwrap();
    tokio::fs::write(&payload, &whole[..whole.len() - 20])
        .await
        .unwrap();

    // The history page a reload reads.
    let page = fixture.get(&fixture.url("/history")).await;
    assert_eq!(page.status(), 200);
    let body: serde_json::Value = page.json().await.unwrap();
    let wire_turn = body["turns"]
        .as_array()
        .expect("turns")
        .iter()
        .find(|value| value["id"].as_str() == Some(&turn.to_string()))
        .expect("the damaged turn is still delivered")
        .clone();
    assert_eq!(wire_turn["skipped_records"].as_u64(), Some(1));
    let item = wire_turn["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|item| item["harness_item_id"].as_str() == Some(HARNESS_ITEM_ID))
        .expect("the record behind the torn one still loads");
    assert_eq!(
        item["payload"]["status"].as_str(),
        Some("completed"),
        "the turn now reads the amendment before the torn one: {item}"
    );
    assert!(
        payload.exists(),
        "one unreadable record is not a quarantine"
    );
    let quarantined: Vec<_> = std::fs::read_dir(payload.parent().unwrap())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".corrupt-"))
        .collect();
    assert!(quarantined.is_empty(), "left in service: {quarantined:?}");

    // And the bootstrap a fresh subscribe reads.
    let mut fresh = subscribe(&fixture, None).await;
    let (turns, reset) = history_delta(&mut fresh).await;
    assert!(
        reset,
        "a cursorless subscribe bootstraps with a reset delta"
    );
    let delivered = turns
        .iter()
        .find(|candidate| candidate.id == turn)
        .expect("the damaged turn is in the bootstrap");
    assert_eq!(delivered.skipped_records, 1);

    // The item endpoint still serves what survived.
    let item = fixture
        .get(&fixture.url(&format!("/turns/{turn}/items/{}", fixture.item_id())))
        .await;
    assert_eq!(item.status(), 200);
}

/// A healthy turn carries no `skipped_records` key at all, so nothing that was ever written or
/// delivered changes shape.
#[tokio::test]
async fn a_healthy_turn_carries_no_skipped_records_key() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;
    fixture.release();
    wait_for_settled_item(&fixture, turn).await;

    let body: serde_json::Value = fixture
        .get(&fixture.url("/history"))
        .await
        .json()
        .await
        .unwrap();
    let wire_turn = &body["turns"][0];
    assert_eq!(wire_turn["id"].as_str(), Some(turn.to_string().as_str()));
    assert!(
        wire_turn.get("skipped_records").is_none(),
        "a healthy turn is byte-identical to what it was: {wire_turn}"
    );
}

// ---- H8: amendment after a torn tail ----

/// The amend path appends to a payload whose tail is already torn: a newline first, so the damage
/// stays its own line, and the settled item is read from the line after it.
#[tokio::test]
async fn an_amendment_lands_after_a_torn_tail_without_repairing_it() {
    let fixture = start(LateKind::Command).await;
    let mut socket = subscribe(&fixture, None).await;
    let turn = run_turn_leaving_the_item_running(&fixture, &mut socket).await;

    let payload = fixture.payload_path(turn);
    let whole = tokio::fs::read_to_string(&payload).await.unwrap();
    let torn = whole[..whole.len() - 20].to_string();
    assert!(!torn.ends_with('\n'));
    tokio::fs::write(&payload, &torn).await.unwrap();

    fixture.release();
    expect_late_item_completion(&mut socket).await;
    wait_for_settled_item(&fixture, turn).await;

    let on_disk = tokio::fs::read_to_string(&payload).await.unwrap();
    assert!(
        on_disk.starts_with(&torn),
        "the torn tail is left exactly as it was found"
    );
    assert_eq!(
        on_disk.lines().count(),
        torn.lines().count() + 1,
        "the amendment is its own line"
    );

    let body: serde_json::Value = fixture
        .get(&fixture.url("/history"))
        .await
        .json()
        .await
        .unwrap();
    let wire_turn = body["turns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value["id"].as_str() == Some(&turn.to_string()))
        .expect("the turn still loads");
    assert_eq!(wire_turn["skipped_records"].as_u64(), Some(1));
    let item = wire_turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["harness_item_id"].as_str() == Some(HARNESS_ITEM_ID))
        .expect("the settled item");
    assert_eq!(item["payload"]["status"].as_str(), Some("completed"));
}
