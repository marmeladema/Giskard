# M0 — Loss scenario tests

Implementation plan for milestone M0 of [`event-pipeline-milestones.md`](event-pipeline-milestones.md).
Written against `main` at `b6fdf22`. Every file and line reference below was checked against that
tree; re-check them if the branch has moved.

## Purpose

Make each event-loss window observable as a test that fails today, so M1 and M2 flip tests on
instead of writing them. M0 changes **no production code**: the diff is test modules plus a status
line in the milestones document.

## Verified prototype

The tests below were prototyped on this branch, in the commit titled "Add M0 loss-scenario
prototype tests", and run against `main` at `b6fdf22`. Every one fails for the stated reason:

| Test | Observed failure |
| --- | --- |
| `child_frames_before_claim_reach_the_child_subscriber` | `timed out waiting for child TurnStarted` |
| `events_after_claim_and_before_subscribe_are_retained` | `timed out waiting for child TurnStarted` |
| `slow_subscriber_receives_every_event_in_order` | `stream closed while waiting for child TurnStarted: channel lagged by 46` |
| `resubscribing_after_dropping_the_stream_yields_unconsumed_events` | `timed out waiting for tail after resubscribe` |
| `child_events_are_read_while_no_turn_is_active` | `timed out waiting for child TurnStarted while idle` |
| `replacement_forwarder_persists_events_sent_while_no_forwarder_ran` | `an event sent while no forwarder ran must reach the replacement; persisted items: []` |

`cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings` and
`cargo test --workspace --locked` pass with the six tests reported as ignored.

The implementing agent's job is therefore review rather than authorship: read the prototype
against this plan, keep or tighten the assertions, add the status line (Deliverable 3), and
re-run the verification below. If the branch has moved past `b6fdf22`, re-run the ignored tests
first and confirm each still fails for the listed reason before doing anything else.

## Non-goals

- No fix, no new transport, no new trait method.
- No end-to-end test through the scripted replay binary. Its `ScriptedHarness` waits for a
  receiver before emitting (`bin/giskard-server-replay.rs:207-215`) and spawns a child's turn only
  when the server attaches (`:243-262`), so it cannot express windows A or B. M1 deletes
  `wait_for_receiver` and adds the e2e test then.
- No "child frames before parent link" server test. That needs the discoveries stream M2 adds to
  the trait; it is written in M2.
- No public test-support feature on `giskard-harness-codex`. The adapter tests live inside its
  existing `mod tests`, which already has the fake transport.

## Ground truth the tests rely on

| Fact | Where |
| --- | --- |
| Unknown non-empty native id makes the mapper return `Err(UnknownNativeThread)`; the instance logs "dropping Codex notification for unknown native thread" and drops the frame | `mapping.rs:391-409`, `native_routes.rs:145-163` |
| `thread/started` and `thread/status/changed` are not handled by the mapper; they fall to `_ => None` and are silently ignored, unknown id or not | `mapping.rs`, the catch-all arm of `try_map_notification` |
| Events go to a per-`ThreadId` `tokio::sync::broadcast` of capacity 256; a send with no receiver is discarded | `lib.rs:55`, `lib.rs:272`, `lib.rs:2329-2334` |
| `subscribe` for a thread with no sender returns a receiver on a fresh channel whose sender is dropped immediately, so `recv` returns `Closed` | `lib.rs:1374-1380` |
| The instance reads stdout only while it has an active turn, the mapper has an active turn or running command, a compaction is pending, or a context restore is pending | `instance.rs:106`, `lib.rs:1634-1643` |
| `claim_native_thread` is a control command served by the instance's `select!`, which is `biased` with stdout ahead of the control queue | `instance.rs:94-107`, `lib.rs:1332-1350` |
| `codex_codes::AsyncClient` buffers notifications that arrive during a request in an unbounded `VecDeque` and drains them in order from `next_message`; no loss, no bound | `codex-codes-0.151.2/src/client_async.rs:94,227,415-419` |
| The fake transport delivers `ServerMessage`s from an `mpsc` of capacity 32 and records `turn/start` in `started_turns` with `native-turn-N` ids; fresh threads are `native-thread-N` | `lib.rs:3385-3400`, `3495-3507`, `3527-3580` |
| Test helpers: `spawn_fake_harness`, `open_opts(thread, resume)`, `build_turn_overrides`, `recv_matching_event(stream, label, pred)` with a 1 s timeout, `completed_event` | `lib.rs:3785-3819`, `4023-4038`, `4058-4069` |
| A `subAgentActivity` completed item with `kind = started` maps to `ItemCompleted` with `ItemPayload::Activity { subagent: Some(link) }`, `link.harness_thread_id = agentThreadId` | `mapping.rs` tests at `6182-6212` |
| The forwarder's lag path persists the prefix as `Interrupted` and swallows the rest of the turn; the test that pins this is `forwarder_lag_recovers_but_truncates_the_interrupted_native_turn` | `event_forwarder.rs:920-1022`, `:3972` |
| Forwarder test scaffolding: `spawn_forwarder_handle_with_runtime` prepares a `User` operation on a coordinator and runs a forwarder over a given stream | `event_forwarder.rs:4231-4306` |

## Deliverable 1 — adapter-level scenarios

File: `crates/giskard-harness-codex/src/lib.rs`, inside `mod tests`, in a new nested module
`mod loss_scenarios` placed after the existing route-claim tests (`:5721-5817`) so it can reuse
every helper in scope.

### Shared helpers to add inside `loss_scenarios`

All notification builders go through `serde_json::from_value` exactly like the existing tests
(`lib.rs:4318-4326`). Verified JSON shapes:

```rust
fn notification(n: codex_codes::messages::Notification) -> codex_codes::ServerMessage {
    codex_codes::ServerMessage::Notification(n)
}

fn thread_started(native_thread_id: &str, parent: &str) -> codex_codes::ServerMessage {
    // Every field of `Thread` has a serde default, so this minimal object deserializes.
    notification(codex_codes::messages::Notification::ThreadStarted(
        serde_json::from_value(json!({
            "thread": { "id": native_thread_id, "parentThreadId": parent }
        }))
        .unwrap(),
    ))
}

fn turn_started(native_thread_id: &str, native_turn_id: &str) -> codex_codes::ServerMessage {
    notification(codex_codes::messages::Notification::TurnStarted(
        serde_json::from_value(json!({
            "threadId": native_thread_id,
            "turn": { "id": native_turn_id, "status": "inProgress" }
        }))
        .unwrap(),
    ))
}

fn turn_completed(native_thread_id: &str, native_turn_id: &str) -> codex_codes::ServerMessage {
    notification(codex_codes::messages::Notification::TurnCompleted(
        serde_json::from_value(json!({
            "threadId": native_thread_id,
            "turn": { "id": native_turn_id, "status": "completed" }
        }))
        .unwrap(),
    ))
}

fn agent_message_completed(
    native_thread_id: &str,
    native_turn_id: &str,
    item_id: &str,
    text: &str,
) -> codex_codes::ServerMessage {
    notification(codex_codes::messages::Notification::ItemCompleted(
        serde_json::from_value(json!({
            "threadId": native_thread_id,
            "turnId": native_turn_id,
            "completedAtMs": 1000,
            "item": { "type": "agentMessage", "id": item_id, "text": text }
        }))
        .unwrap(),
    ))
}

/// The parent's `subAgentActivity(started)` item naming the child.
fn subagent_started(
    parent_native_thread_id: &str,
    parent_native_turn_id: &str,
    child_native_thread_id: &str,
) -> codex_codes::ServerMessage {
    notification(codex_codes::messages::Notification::ItemCompleted(
        serde_json::from_value(json!({
            "threadId": parent_native_thread_id,
            "turnId": parent_native_turn_id,
            "completedAtMs": 1000,
            "item": {
                "type": "subAgentActivity",
                "id": "spawn-1",
                "kind": "started",
                "agentThreadId": child_native_thread_id,
                "agentPath": "/root/explorer"
            }
        }))
        .unwrap(),
    ))
}

/// Open a parent thread and start a turn on it so the instance polls stdout. Returns the parent
/// handle, its native turn id, and a subscription on the parent used as an ordering barrier.
async fn parent_with_active_turn(
    harness: &Arc<CodexHarness>,
    controller: &FakeCodexController,
) -> (ThreadHandle, String, AgentEventStream) {
    let parent = harness.open_thread(open_opts(ThreadId::new(), None)).await.unwrap();
    let parent_stream = harness.subscribe(&parent);
    harness
        .start_turn(&parent, UserInput::text("spawn a child"), build_turn_overrides())
        .await
        .unwrap();
    let native_turn = controller.started_turns().await[0].native_turn_id.clone();
    (parent, native_turn, parent_stream)
}

/// Send a parent-scoped marker and wait until it is observed on the parent stream. Every frame
/// sent before the marker has then been processed by the instance, dropped or not.
async fn barrier(
    controller: &FakeCodexController,
    parent: &ThreadHandle,
    native_turn: &str,
    parent_stream: &mut AgentEventStream,
    label: &str,
) {
    let marker = format!("barrier-{label}");
    controller
        .send_server_message(agent_message_completed(
            &parent.harness_thread_id, native_turn, &marker, &marker,
        ))
        .await;
    recv_matching_event(parent_stream, label, |event| {
        matches!(event, AgentEvent::ItemCompleted { item, .. } if item.harness_item_id == marker)
    })
    .await;
}

async fn expect_event(stream: &mut AgentEventStream, label: &str) -> AgentEvent {
    timeout(Duration::from_secs(1), stream.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
        .unwrap_or_else(|error| panic!("stream closed while waiting for {label}: {error}"))
}
```

The barrier is the only ordering device. It works because the instance is a single task: a
parent-scoped item delivered on the parent's channel proves every earlier frame on the fake's
`mpsc` was handled. Do not use `sleep` or `yield_now` as ordering; use the barrier or the fake's
recorded `requests()`.

### Tests

Each test is `#[tokio::test]` followed by `#[ignore = "..."]` with the milestone that enables it.
Every test keeps the parent turn open for its whole duration unless the scenario says otherwise,
because the instance stops reading stdout when nothing is active.

**A. `child_frames_before_claim_reach_the_child_subscriber`** — `#[ignore = "M2: identity minted at ingest"]`

1. `parent_with_active_turn`.
2. Send, in order: `subagent_started(parent, parent_turn, "native-child")`,
   `thread_started("native-child", parent)`, `turn_started("native-child", "child-turn-1")`,
   `agent_message_completed("native-child", "child-turn-1", "child-msg-1", "hello")`,
   `turn_completed("native-child", "child-turn-1")`.
3. `barrier(..., "child frames processed")`. Also consume the parent's `subagent_started`
   `ItemCompleted` from `parent_stream` before or as part of the barrier; assert its
   `subagent.harness_thread_id == "native-child"` so the test also proves the link event survived.
4. `harness.claim_native_thread(ThreadId::new(), "native-child", "/tmp")`, then
   `harness.subscribe(&child_handle)`.
5. Assert `expect_event` yields `TurnStarted`, then `ItemCompleted` with
   `ItemPayload::AgentMessage { text: "hello" }`, then `TurnCompleted { status.kind: Completed }`,
   in that order, for `child_handle.thread`.

Expected failure today: step 5 times out on the first event. Every child frame was dropped at
`mapping.rs:406` because `native-child` had no route (window A). Note that after M2 the claim in
step 4 returns a handle whose `thread` is the adapter-minted id, not the proposed one; assert on
`child_handle.thread`, never on the id passed in.

**B. `events_after_claim_and_before_subscribe_are_retained`** — `#[ignore = "M1: retained event log"]`

1. `parent_with_active_turn`.
2. `claim_native_thread(child_id, "native-child")`; keep the handle; **do not subscribe**.
3. Send `turn_started("native-child", "child-turn-1")` and
   `agent_message_completed("native-child", "child-turn-1", "m1", "early")`.
4. `barrier(..., "child events broadcast without receiver")`.
5. `subscribe(&child_handle)`; assert `TurnStarted` then the `ItemCompleted` arrive.

Expected failure today: step 5 times out. The route and sender exist, so the mapper routes the
frames, but `broadcast::Sender::send` at `lib.rs:2332` had zero receivers and discarded them
(window B).

**C. `slow_subscriber_receives_every_event_in_order`** — `#[ignore = "M1: retained event log"]`

1. `parent_with_active_turn`, `claim_native_thread(child_id, "native-child")`,
   `subscribe(&child_handle)` **before** sending anything.
2. Send `turn_started("native-child", "child-turn-1")`, then 300
   `agent_message_completed("native-child", "child-turn-1", &format!("m{i}"), "x")` for
   `i in 0..300`, then `turn_completed`. 300 exceeds `BROADCAST_CAPACITY` (256).
3. `barrier(..., "burst processed")`. Only now start receiving on the child stream.
4. Assert: `TurnStarted`, then 300 `ItemCompleted` whose `harness_item_id` are `m0..m299` in
   order, then `TurnCompleted`.

Expected failure today: `recv` returns `Err(Lagged(n))` after the first few events (window C).
The fake `mpsc` has capacity 32, so `send_server_message` will block until the instance drains it;
that is fine because the parent turn keeps the instance polling. If the test's own sends block
the current-thread runtime, send the burst from a `tokio::spawn`ed task and join it after the
barrier.

**D. `resubscribing_after_dropping_the_stream_yields_unconsumed_events`** — `#[ignore = "M1: retained event log"]`

1. `parent_with_active_turn`, claim child, `let mut first = subscribe(&child)`.
2. Send `turn_started("native-child", "child-turn-1")`; `expect_event(&mut first)` is
   `TurnStarted`.
3. `drop(first)`.
4. Send `agent_message_completed("native-child", "child-turn-1", "after-drop", "tail")`;
   `barrier(..., "tail processed without receiver")`.
5. `let mut second = subscribe(&child)`; assert `expect_event(&mut second)` is the `ItemCompleted`
   with `harness_item_id == "after-drop"`.

Expected failure today: step 5 times out; the event was sent while the receiver count was zero
(window D, the transport half of owner replacement). After M1 a new reader starts at the oldest
event no reader has consumed, which is exactly `after-drop`: `TurnStarted` was consumed by
`first` and must **not** be replayed. Assert that too.

**E. `child_events_are_read_while_no_turn_is_active`** — `#[ignore = "M5: polling gate removed"]`

1. Open the parent with `open_opts(ThreadId::new(), None)` and **do not** start a turn. Use a
   fresh open, not a resume: a resume installs a pending context restore, which keeps polling on
   (`instance.rs:106`).
2. `claim_native_thread(child_id, "native-child")`, `subscribe(&child)`.
3. Send `turn_started("native-child", "child-turn-1")`.
4. Assert `expect_event` yields `TurnStarted` within the 1 s timeout.

Expected failure today: timeout. `should_poll_codex_messages` is false (no active turn anywhere,
no running command, no pending compaction or restore), so the instance never calls
`next_message` and the frame sits in the fake's channel (window E). No barrier is possible here by
construction, which is the point.

Tag this M5 rather than M1: M1's log retains frames the instance has *read*; it does not make the
instance read. Removing the gate is M5's job. If M1 chooses to remove the gate early, retag.

### Assertions that must not be made

- Do not assert on log output. The warn lines are diagnostics, not contract.
- Do not assert `receiver_count()` or anything about `SenderMap`; M1 deletes both.
- Do not assert the `ThreadId` passed to `claim_native_thread` is the one on the returned handle.

## Deliverable 2 — server-level scenario

File: `crates/giskard-server/src/registry/event_forwarder.rs`, inside `mod tests`, after
`forwarder_lag_recovers_but_truncates_the_interrupted_native_turn` (`:3972`).

**`replacement_forwarder_persists_events_sent_while_no_forwarder_ran`** — `#[ignore = "M1: retained event log"]`

Scaffolding, all existing: the store, project and thread-file setup copied from the lag test
(`:3972-4020`); `spawn_forwarder_handle_with_runtime` (`:4231`), which creates its own
`RegistryShared`, coordinator and authority per call and prepares a `User` operation; a hub client
registered with `hub.register_client(1, client_tx)` and `hub.subscribe(thread_id, 1)` (`:4508-4511`)
as the ordering device; `wait_for_turn_count` (`:4979`).

1. `let (tx, _) = broadcast::channel(256)`. Spawn forwarder 1 over `tx.subscribe()`, keeping
   its `(handle, _runtime, coordinator, _authority)`.
2. Send `TurnStarted { turn }` and `ItemCompleted` with `harness_item_id = "before"` on `tx`.
   Wait until the hub client receives the wire event for `before`; that proves forwarder 1
   consumed it.
3. Retire forwarder 1 the way `forget_thread` does (`registry.rs:1505-1530`):
   `let control = coordinator.begin_retirement().await.unwrap(); control.cancel.send(true)`, then
   `await` the join handle, then `coordinator.finish_retirement().await`. Forwarder 1 exits with
   `StreamEndedWithoutTurn` and releases its lease **without persisting**.
4. Send `ItemCompleted` with `harness_item_id = "during-gap"` on `tx` while nothing is
   subscribed. Write it as `let _ = tx.send(...)`: with zero receivers `broadcast::Sender::send`
   returns `Err(SendError)` and discards the event, and production ignores that result at
   `lib.rs:2332`. Unwrapping it would make the test fail at the send instead of at the assertion.
5. Spawn forwarder 2 with a second call to `spawn_forwarder_handle_with_runtime` over a fresh
   `tx.subscribe()` (it gets its own coordinator; the retired one cannot be reactivated). Send
   `TurnCompleted { turn, Completed }`.
6. `wait_for_turn_count(&store, project_id, thread_id, 1)`, load the turn, and assert its items
   contain `during-gap`.

Expected failure today: the persisted turn has no items. `during-gap` was broadcast with no
receiver, and forwarder 2 subscribed at the tail and claimed the turn from `TurnCompleted` alone.

What this test deliberately does **not** assert: that `before` is in the persisted turn. Forwarder
1 consumed it and was cancelled before persisting, and M1's log trims what a reader has consumed.
Recovering consumed-but-unpersisted events is M7's cursor-committed persistence. Write that as a
separate assertion in a separate test tagged `M7` only if M7 is scheduled; otherwise leave a
comment.

After M1 the test constructs an `EventLog` reader instead of `tx.subscribe()`; that edit is
mechanical and expected. After M4 the same scenario is expressed as detach/attach on the driver.

Keep `forwarder_lag_recovers_but_truncates_the_interrupted_native_turn` untouched. M1 deletes it
together with the behavior it pins.

## Deliverable 3 — status line

Add one line under M0 in `docs/event-pipeline-milestones.md`:

> **Status.** Landed in <PR>. Run `cargo test -p giskard-harness-codex --lib -- --ignored loss_scenarios`
> and `cargo test -p giskard-server --lib -- --ignored replacement_forwarder` to see the current
> failures.

No README, spec, `AGENTS.md` or endpoint documentation changes: nothing user-visible changes.

## Verification the implementer must perform and record in the PR description

1. `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   `cargo test --workspace --locked` all pass. The six new tests are reported as ignored, never as
   failed: five in `giskard-harness-codex --lib`, one in `giskard-server --lib`.
2. Run each new test with `--ignored` and paste the failure line for each. The failure must match
   the "expected failure today" above. A test that passes today is a bug in the test, not a fixed
   window: rework it until it fails for the stated reason.
3. `git diff --stat main` shows changes only under `crates/giskard-harness-codex/src/lib.rs`
   (inside `mod tests`), `crates/giskard-server/src/registry/event_forwarder.rs` (inside
   `mod tests`) and `docs/event-pipeline-milestones.md`.

## Pitfalls

- `#[tokio::test]` uses a current-thread runtime. The instance task only runs while the test
  awaits. Every step that needs the instance to have processed something must await the barrier
  or a recorded request; the ordering is deterministic, never timing-based.
- The fake's event channel has capacity 32. Long bursts must be sent from a spawned task while
  the parent turn keeps the instance polling.
- `recv_matching_event` panics on a closed stream. In B and D, the child stream is expected to be
  live, so a `Closed` error is a real failure and should surface as a panic with the label.
- Never call `open_thread` for the child. Sub-agents are claimed, not opened, and an open would
  issue `thread/start` and mint `native-thread-2`, which is not the child.
- Do not add the `thread/status/changed` frame in A. The mapper ignores it today and M2 treats it
  as a discovery signal; including it would make the test depend on an M2 design choice.
  `thread/started` is included because Codex emits it and it must be harmless.

## Size

About 250 lines of tests in the adapter, about 120 in the server, one documentation line.

