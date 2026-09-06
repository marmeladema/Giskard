# S5 — `Hub::publish(Outbound)`: one implementation of the outbound lanes

Implementation plan for step 5 of [`design-straightening-review.md`](design-straightening-review.md)
(finding C3). Written against `main` at `6f464b0` (S4b merged); every file and line reference below
was checked against that tree. Re-check them if the branch has moved.

Revision 2: the first cut missed two multi-line `.hub` `.broadcast(` chains in `routes.rs` and
one `broadcast_event` call in an integration test; both are in the tables below.

## Goal

Give the hub one typed entry point for everything the server sends to subscribed browsers about a
thread, so that the choice of lane (ordered transcript FIFO, revisioned replacement, direct error)
and the core-to-wire narrowing happen in exactly one place. Today that choice is made in three:
`Hub::broadcast_event`, `registry::broadcast_event_with_user_input`, and the forwarder's
`publish_applied_runtime_effects`, and the three do not agree. After S5 the registry and the
forwarder hand the hub `AgentEvent`s and runtime effects and never name a `WireAgentEvent` or a
`ServerMessage`.

One behaviour changes, deliberately, because the spec requires it: see "The one wire-visible
change" below.

## Non-goals

- No change to `giskard-proto`, to `delivery.rs`, or to the per-connection replacement mechanism.
  `WireAgentEvent::from_agent_event` keeps its `Option` return type even though no arm returns
  `None` today; the hub keeps the one refusal branch for it.
- No change to the process-scoped lanes. `publish_runtime_overview` and
  `invalidate_thread_catalog` are already single implementations and keep their names and
  callers.
- No change to the order in which effects reach a client: request state, then running tasks,
  then the overview, exactly as `publish_applied_runtime_effects` sends them today.
- No change to `routes.rs` beyond its two publish sites (D4). Its direct replies to one client
  (`ServerMessage::RunningTasks` at `:4803`, the error replies) go through the client's own sender,
  not the hub, and stay.
- No spec text change. RT1 already states the rule this step enforces.

## Ground truth

| Fact | Where |
| --- | --- |
| `Hub` public surface today: `new`, `next_client_id`, `register_client`, `subscribe`, `unsubscribe`, `disconnect`, `broadcast_all` (`:101-123`), `broadcast` (`:125-153`), `publish_thread_metadata` (`:159-196`), `invalidate_thread_catalog` (`:199-213`), `publish_runtime_overview` (`:216-230`), `broadcast_event` (`:232-260`); `server_message_kind` (`:269-283`); 8 unit tests from `:286` | `crates/giskard-server/src/hub.rs` |
| `broadcast_all` has no caller outside its own definition | grep `broadcast_all` across `src`: 1 hit |
| `broadcast_event` refuses `ThreadOpened` and `DiffUpdated` with a `debug!` ("keeping internal-only harness event off the browser stream"), then narrows with `WireAgentEvent::from_agent_event` and, on `None`, warns "refusing to broadcast a metadata-only event on the transcript stream" | `hub.rs:232-260` |
| The thread runtime uses the same two-variant predicate to decide which events consume a process-local sequence | `crates/giskard-server/src/thread_runtime.rs:1032-1037` (in `apply_event_locked`, `:1024`) |
| Spec RT1: "metadata-only/internal events do not consume that sequence or reach the transcript stream" | `specs/giskard-specification.md:272-277` |
| `registry::broadcast_event_with_user_input` (`:1907-1941`, wrapped by `broadcast_event_with_context` `:1896-1905`) re-implements the narrowing: it maps `TurnStarted` itself to attach `user_input`, calls `from_agent_event` for the rest, logs a rejection through `log_metadata_only_event_rejection` on `None`, and does **not** apply the internal-only filter. Its one caller is the forwarder's main live-turn path (`event_forwarder.rs:1810`) | `crates/giskard-server/src/registry.rs`; `registry/event_forwarder.rs` |
| Because that path lacks the filter and `from_agent_event` maps every variant, a `DiffUpdated` inside a live turn is broadcast to browsers today (`event_forwarder.rs:1673-1680` records the diff and falls through to `:1810`), in conflict with RT1 and with `hub.rs:232-243`. `ThreadOpened` never reaches `:1810`: it is turnless and the turnless path (`:1494-1542`) broadcasts only `Error`, `Notice`, and `ServerRequestReceived` | read |
| The browser has no handler for a `diff_updated` frame: the agent-event dispatch in `static/app.js` (`:4290-4345`) handles `item_started`, `item_delta`, `item_completed`, `turn_usage_updated`, `turn_completed`, and the string `"diff_updated"` does not occur in the file. Diffs reach the viewer from persisted turns over HTTP. The only integration test that emits `DiffUpdated` (`tests/diff_accumulation.rs`) asserts on persisted turns and HTTP, not on socket frames | grep |
| `WireAgentEvent::from_agent_event` (`wire.rs:331-420`) maps all 13 `AgentEvent` variants and ends in `Some(event)`; it never returns `None`. `TurnStarted` maps with `user_input: None` (`:340-344`) | `crates/giskard-proto/src/wire.rs` |
| `log_metadata_only_event_rejection` (`event_forwarder.rs:131-146`) therefore has no reachable production caller; its other use is a log-format unit test, `dropped_and_rejected_event_logs_include_bare_identity_without_content` (`:2665-2672`) | grep: 4 hits total |
| `publish_applied_runtime_effects(&Hub, ThreadId, AppliedRuntimeEvent)` (`event_forwarder.rs:258-281`): `RequestState` on the ordered lane, then `RunningTasks { thread_id, revision, tasks }` on the ordered lane, then `publish_runtime_overview`. Five callers: `:1442`, `:1498`, `:1762`, `:1894`, `:1910` | read |
| `AppliedRuntimeEvent { sequence, tasks_changed, running_tasks_if_changed: Option<RunningTasksProjection>, request_state: Option<WireRequestState>, overview_if_changed: Option<ThreadRuntimeOverview>, overview_refresh_needed }` and `RunningTasksProjection { revision, tasks: Vec<RunningTask> }` are `pub(crate)`; `RequestTransition { request_state: WireRequestState, overview_if_changed }` likewise; `WireRequestState` is `giskard_proto::RequestState` | `thread_runtime.rs:257-275, 30` |
| Other production publish sites: forwarder `broadcast_event` ×5 (`:1209` completion event, `:1511`/`:1524`/`:1538` turnless error/notice/server request, `:1796` completion event), `broadcast` ×4 (`:264`, `:268` inside the effects helper, `:1462` late command completion, `:1911-1930` a `ServerMessage::Error` with code `turn_persistence_blocked`), `publish_runtime_overview` ×7 (`:279`, `:915`, `:996`, `:1043`, `:1078`, `:1131`, `:1328`); registry `broadcast` ×3 (`:1086-1091` claim settlement `RequestState`, `:1094-1105` `publish_request_transition`, `:1933-1942` the wire event), `publish_runtime_overview` helper `:1671-1676`; `thread_metadata.rs:194-196` `publish_thread_metadata`, `:224` `invalidate_thread_catalog` | grep + read |
| `routes.rs` publishes through the hub at two sites the single-line grep missed because the chain is split over lines: `:5632-5640` broadcasts `ServerMessage::Error { error: warning.clone() }` for a thread reopened with a warning; `broadcast_running_commands` (`:6180-6196`, called at `:5388`, `:5420`, `:5449` after terminate and interrupt actions) broadcasts `ServerMessage::RunningTasks` from `runtime.tasks_snapshot()`. `routes.rs`'s `mod tests` spans `:2100-2749` with production code on both sides | read |
| The integration test `websocket_serializes_harness_error_events` (`tests/e2e_smoke.rs:6045-6055`) calls `state.hub.broadcast_event(tid, AgentEvent::Error { .. })` and asserts the browser receives the wire error frame. It is the one caller outside the crate, so the entry point must be `pub` | read |
| `AppliedRuntimeEvent` and `RunningTasksProjection` are `pub(crate)` with `pub` fields except the private `overview_refresh_needed`; `thread_runtime` is a `pub mod` (`lib.rs:19`). A `pub` enum cannot carry a `pub(crate)` type (E0446) | `thread_runtime.rs:257-269` |
| A second unit test uses the late-completion helper: `late_untruncated_command_completion_ignores_original_counts` (`event_forwarder.rs:2717-2756`) asserts on the wire payload it returns | read |
| `late_command_completion_message` (`event_forwarder.rs:303-345`) builds a `ServerMessage::Event` by hand: it derives a `CommandOutputDescriptor` from the durable item fields (`resolve_command_output_counts`, `CommandOutputDescriptor::from_durable`) and calls `WireItem::from_item_with_command_output(item, descriptor)`. `WireCommandOutput` is a type alias for `giskard_core::CommandOutputDescriptor` | `event_forwarder.rs:303-345`; `wire.rs:131`, `:480-491` |
| Production `WireAgentEvent` references: `registry.rs` 3 (import `:37`, `:1915`, `:1924`), `event_forwarder.rs` 1 (`:336`), `hub.rs` 2. Production `ServerMessage::` uses: `registry.rs` `Event` ×1, `RequestState` ×2; `event_forwarder.rs` `Error`, `Event`, `RequestState`, `RunningTasks` ×1 each. The forwarder imports everything through `use super::*;` (`event_forwarder.rs:1`), so its wire vocabulary is `registry.rs`'s import line | awk over the code before each `mod tests` (`hub.rs:286`, `registry.rs:1975`, `event_forwarder.rs:1951`) |
| Unit tests: `hub.rs` 8, `registry.rs` 29, `event_forwarder.rs` 44, `thread_metadata.rs` 3, `delivery.rs` 8; integration 226. Forwarder tests observe hub output by registering an `mpsc` client (17 `register_client` calls) and matching `ServerMessage` frames (19 `ServerMessage::Event` matches); one test calls `hub.broadcast` directly (`:4903-4907`) | grep |
| Hub tests that call the removed methods: `live_usage_is_broadcast_while_diff_stays_internal` (`:290-337`, `broadcast_event` ×2), `full_client_queue_does_not_unsubscribe_client` (`:371`), `closed_client_queue_removes_subscription` (`:388`), the three metadata/invalidation tests (`publish_thread_metadata` ×3, `invalidate_thread_catalog` ×2) | read |

## Design

### D1. `Outbound`, the thread-scoped lanes

In `hub.rs`:

```rust
/// Everything the server publishes to the browsers subscribed to one thread. Each variant is
/// one row of spec §13.6.1: the hub, not the caller, chooses the lane and does the core→wire
/// narrowing (§3.5).
pub enum Outbound {
    /// An agent event for the ordered transcript FIFO. Internal-only kinds are dropped here.
    Transcript {
        event: AgentEvent,
        /// Attached to `TurnStarted` only: the prompt text for externally started turns.
        user_input: Option<UserInput>,
        /// Attached to `ItemCompleted` only: the durable command output of a late completion.
        command_output: Option<WireCommandOutput>,
    },
    /// Authoritative replacement state for one request, on the ordered lane.
    Request(RequestState),
    /// The running-tasks projection as a revisioned snapshot. On the ordered lane today; the
    /// spec table lists it as revisioned replacement, a lane change this step does not make.
    RunningTasks { revision: u64, tasks: Vec<RunningTask> },
    /// What one applied event changed: request state, running tasks, overview, in that order.
    RuntimeEffects(AppliedRuntimeEvent),
    /// Committed persisted metadata, on the per-connection replacement lane.
    Metadata(ThreadState),
    /// A thread-scoped error for the ordered lane.
    Error(ErrorInfo),
}

impl Hub {
    pub async fn publish(&self, thread_id: ThreadId, outbound: Outbound);
}

`Outbound` and `publish` are `pub`, not `pub(crate)`: one integration test publishes through the
hub (ground truth), and `Hub` is already public API through `AppState.hub`. That requires
`AppliedRuntimeEvent` and `RunningTasksProjection` to become `pub` structs; their fields keep
their visibility, including the private `overview_refresh_needed`.
```

`publish` dispatches:

| Variant | Does |
| --- | --- |
| `Transcript` | if `is_internal_event(&event)`: the existing `debug!` and return. Then narrow: when the event is `ItemCompleted` **and** `command_output` is `Some`, build `WireAgentEvent::ItemCompleted { thread, turn, item: WireItem::from_item_with_command_output(item, command_output) }` directly, as `late_command_completion_message` and `runtime_live.rs:296-303` do today; otherwise `from_agent_event(event)`, and on `None` the existing `warn!` and return. If the wire event is `TurnStarted`, set its `user_input`. Then the ordered send of `ServerMessage::Event { thread_id, agent_event }` |
| `RunningTasks { revision, tasks }` | ordered send of `ServerMessage::RunningTasks { thread_id, revision, tasks }` (a private helper the `RuntimeEffects` arm shares) |
| `Request(state)` | ordered send of `ServerMessage::RequestState(state)` |
| `RuntimeEffects(applied)` | the body of today's `publish_applied_runtime_effects`, verbatim order: the `Request` send if `request_state`, then the `RunningTasks` send if `running_tasks_if_changed`, then `publish_runtime_overview` if `overview_if_changed` |
| `Metadata(state)` | the body of today's `publish_thread_metadata`, including both refusals |
| `Error(error)` | ordered send of `ServerMessage::Error { error }` |

`command_output` on `Transcript` is how the late command completion keeps its descriptor without
the forwarder building a wire item. The pre-narrowing `ItemCompleted` branch is deliberate:
`from_item_with_command_output` takes the core `Item`, so the descriptor has to be applied before
`from_agent_event` consumes it. It is the only event the hub maps outside `from_agent_event`;
`TurnStarted`'s prompt is set on the wire value afterwards.

### D2. One predicate for "internal-only"

`thread_runtime.rs` gains, next to `apply_event_locked`:

```rust
/// Events that carry no client-visible transcript content: they consume no process-local
/// sequence (RT1) and never reach the transcript stream.
pub(crate) fn is_internal_event(event: &AgentEvent) -> bool {
    matches!(event, AgentEvent::ThreadOpened { .. } | AgentEvent::DiffUpdated { .. })
}
```

`apply_event_locked` (`:1032-1037`) and `Hub::publish` both call it. The two-variant pattern
then exists once in production code.

### D3. What becomes private or goes

| Item | After |
| --- | --- |
| `Hub::broadcast` | private `send_ordered(&self, thread_id, ServerMessage)`; same body |
| `Hub::broadcast_event` | deleted; its body is the `Transcript` arm |
| `Hub::broadcast_all` | deleted (no caller) |
| `Hub::publish_thread_metadata` | private `publish_metadata`; called by the `Metadata` arm |
| `registry::broadcast_event_with_context`, `broadcast_event_with_user_input` | deleted |
| `event_forwarder::log_metadata_only_event_rejection` | deleted (unreachable; the hub's `warn!` is the one refusal) |
| `event_forwarder::publish_applied_runtime_effects` | deleted; its body is the `RuntimeEffects` arm |
| `event_forwarder::late_command_completion_message` | becomes `late_command_output(item: &Item) -> Option<WireCommandOutput>`: the descriptor derivation (`:310-332`) without the wire event; the `None` for non-command items stays |
| `registry.rs:37` import | drops `WireAgentEvent`, `WireItem`, `ServerMessage` (the compiler decides `RunningTask`); gains `use crate::hub::{Hub, Outbound};` |
| `routes::broadcast_running_commands` (`:6180-6196`) | keeps its name; its body becomes `hub.publish(thread_id, Outbound::RunningTasks { revision, tasks })` |
| `thread_runtime::{AppliedRuntimeEvent, RunningTasksProjection}` | `pub(crate) struct` → `pub struct`; fields unchanged |

### D4. Call-site migration

| Site | Today | After |
| --- | --- | --- |
| `event_forwarder.rs:1810` | `broadcast_event_with_context(&hub, project_id, thread_id, event, &self.turn.context)` | `hub.publish(thread_id, Outbound::Transcript { event, user_input: live_turn_user_input(&self.turn.context), command_output: None })` (`live_turn_user_input` stays in `registry.rs:124`) |
| `:1209`, `:1796` | `hub.broadcast_event(thread_id, completion_event)` | `Outbound::Transcript { event: completion_event, user_input: None, command_output: None }` |
| `:1511`, `:1524`, `:1538` | `hub.broadcast_event(thread_id, event.clone())` | same shape with `event.clone()` |
| `:1462` | `if let Some(message) = late_command_completion_message(thread_id, event.clone()) { hub.broadcast(thread_id, message) }` | `if let AgentEvent::ItemCompleted { item, .. } = &event { let command_output = late_command_output(item); hub.publish(thread_id, Outbound::Transcript { event: event.clone(), user_input: None, command_output }) }` (the old helper returned `None` for anything but `ItemCompleted`, so the guard is the same) |
| `:1442`, `:1498`, `:1762`, `:1894`, `:1910` | `publish_applied_runtime_effects(&hub, thread_id, applied)` | `hub.publish(thread_id, Outbound::RuntimeEffects(applied))` |
| `:1911-1930` | `hub.broadcast(thread_id, ServerMessage::Error { error: ErrorInfo { .. } })` | `hub.publish(thread_id, Outbound::Error(ErrorInfo { .. }))`, the struct literal unchanged |
| `registry.rs:1086-1091` | `hub.broadcast(request.thread_id, ServerMessage::RequestState(request))` | `hub.publish(request.thread_id, Outbound::Request(request))` |
| `registry.rs:1094-1105` | `RequestState` broadcast then `publish_runtime_overview` | `Outbound::Request(transition.request_state)` then the same `publish_runtime_overview` |
| `thread_metadata.rs:194-196` | `hub.publish_thread_metadata(after.id, Self::thread_state(after, None))` | `hub.publish(after.id, Outbound::Metadata(Self::thread_state(after, None)))` |
| `routes.rs:5632-5640` | `state.hub.broadcast(thread_id, ServerMessage::Error { error: warning.clone() })` | `state.hub.publish(thread_id, Outbound::Error(warning.clone()))` |
| `routes.rs:6185-6195` | `state.hub.broadcast(thread_id, ServerMessage::RunningTasks { thread_id, revision, tasks })` | `state.hub.publish(thread_id, Outbound::RunningTasks { revision, tasks })` |
| `tests/e2e_smoke.rs:6045-6055` | `state.hub.broadcast_event(tid, AgentEvent::Error { .. })` | `state.hub.publish(tid, Outbound::Transcript { event: AgentEvent::Error { .. }, user_input: None, command_output: None })`; the assertions that follow are untouched |
| the 7 forwarder and 1 registry `publish_runtime_overview` sites, `thread_metadata.rs:224` | unchanged | unchanged |

Every `debug!`/`warn!` at the migrated sites stays where it is; only the publish call changes.

### The one wire-visible change

After S5 a `DiffUpdated` inside a live turn is no longer sent to browsers, because the forwarder's
main path now goes through the same filter as every other transcript event. This is what RT1
requires and what the runtime already does for its sequence, what `Hub::broadcast_event` already
did, and what the hub's own unit test asserts. No browser code reads the frame and no test asserts
on it (ground truth). It is called out in the PR description as the one behaviour change. It is
**not** a spec change: the spec required it already.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/hub.rs` | `Outbound`, `publish`, `send_ordered`, `publish_metadata`; `broadcast_event`, `broadcast_all` deleted; imports gain `UserInput`, `WireCommandOutput`, `WireItem`, `RequestState`, `RunningTask`, `ErrorInfo`, `crate::thread_runtime::{AppliedRuntimeEvent, is_internal_event}`; tests per below |
| `crates/giskard-server/src/thread_runtime.rs` | `is_internal_event` (D2); `apply_event_locked:1032-1037` calls it; `AppliedRuntimeEvent` and `RunningTasksProjection` become `pub` |
| `crates/giskard-server/src/routes.rs:5632-5640, 6180-6196` | D4 |
| `crates/giskard-server/tests/e2e_smoke.rs:6045-6055` | D4, one call; no assertion changes |
| `crates/giskard-server/src/registry.rs` | two functions deleted (`:1896-1941`); three publish sites (D4); import line `:37` |
| `crates/giskard-server/src/registry/event_forwarder.rs` | `log_metadata_only_event_rejection` and `publish_applied_runtime_effects` deleted; `late_command_completion_message` → `late_command_output`; eleven publish sites (D4); test `:4903-4907` → `hub.publish(thread_id, Outbound::Request(responding.request_state))`; test `:2655-2672` loses its `log_metadata_only_event_rejection` block; test `:2717-2756` calls `late_command_output(&item)` and asserts `original_bytes`/`original_lines` on the returned descriptor instead of on the wire payload |
| `crates/giskard-server/src/thread_metadata.rs:194-196` | D4 |
| `docs/design-straightening-review.md` | mark C3 (step 5) landed |

## Tests

Existing tests are the specification: 226 integration tests and the 92 unit tests in the five
files above keep their assertions. Two tests change how they call, not what they assert: the
integration test at `e2e_smoke.rs:6045-6055` and the forwarder unit test at `:2717-2756`. Hub tests migrate one-for-one: the two `broadcast_event` calls
become `publish(.., Outbound::Transcript { .. })`, `publish_thread_metadata` calls become
`publish(.., Outbound::Metadata(..))`, and the two `hub.broadcast(thread_id, Pong)` calls become
`send_ordered` (private, reachable from the module's tests).

Three hub tests are added:

1. `transcript_attaches_user_input_to_turn_started`: publish `TurnStarted` with
   `user_input: Some(UserInput::text("prompt"))`; the received `WireAgentEvent::TurnStarted` carries
   it; publish `ItemDelta` with the same `user_input`; the received event is unchanged (the field
   only applies to `TurnStarted`).
2. `transcript_applies_late_command_output`: publish an `ItemCompleted` command item with
   `command_output: Some(descriptor)`; the received `WireItem` payload's output slot is the
   descriptor. Publish the same item with `None`; the slot is whatever plain conversion gives.
3. `runtime_effects_keep_request_tasks_overview_order`: publish `RuntimeEffects` with all three
   present; the client's ordered receiver yields `RequestState` then `RunningTasks`, and the
   replacement receiver yields the `ThreadRuntimeOverview`.

The existing `live_usage_is_broadcast_while_diff_stays_internal` keeps its name and assertions
and gains a `ThreadOpened` publish that is also not received, so both internal kinds are pinned
at the hub. A fourth check lives in `thread_runtime.rs`'s tests: `is_internal_event` is true for
exactly `ThreadOpened` and `DiffUpdated` across all 13 variants.

## Order of work

1. D2 in `thread_runtime.rs`; `apply_event_locked` uses it. `cargo test -p giskard-server --lib thread_runtime`.
2. `Outbound` and `publish` in `hub.rs` beside the existing methods, with the three new tests;
   `cargo test -p giskard-server --lib hub`.
3. Migrate the forwarder (D4), then the registry, then `thread_metadata.rs`; delete the four
   helpers as their last caller goes. `cargo test -p giskard-server --lib` after each file.
4. Delete `broadcast_event`, `broadcast_all`; privatise `broadcast` and `publish_thread_metadata`;
   migrate the hub tests. `cargo test -p giskard-server --lib hub`.
5. `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo fmt --check`; run `e2e_smoke` and `diff_accumulation` twice.

Expected size: about 120 lines added in `hub.rs`, about 130 deleted across the registry and the
forwarder.

## Exit checks

Validated on the base tree; baselines given. `prod` prints a file up to its `mod tests` line.

```sh
S=crates/giskard-server/src
prod() { awk '/^mod tests/{exit} {print}' "$1"; }
# 3 → 0 and 1 → 0: no wire event type outside the hub
prod $S/registry.rs | grep -c WireAgentEvent
prod $S/registry/event_forwarder.rs | grep -c WireAgentEvent
# 3 → 0 and 4 → 0: no wire message construction outside the hub
prod $S/registry.rs | grep -c "ServerMessage::"
prod $S/registry/event_forwarder.rs | grep -c "ServerMessage::"
# 5 → 0 and 7 → 0: no direct lane calls outside the hub
grep -c "broadcast_event(" <(prod $S/registry.rs) <(prod $S/registry/event_forwarder.rs) | awk -F: '{s+=$2} END{print s}'
grep -c "\.broadcast(" <(prod $S/registry.rs) <(prod $S/registry/event_forwarder.rs) | awk -F: '{s+=$2} END{print s}'
# 2 → 0 (routes.rs has production code after its test module, so count the whole file) and 1 → 0
grep -c "\.broadcast(" $S/routes.rs
grep -c "broadcast_event(" crates/giskard-server/tests/*.rs | awk -F: '{s+=$2} END{print s}'
# 3 → 1: the only RunningTasks construction outside the hub is the direct subscribe reply at routes.rs:4803
grep -c "ServerMessage::RunningTasks" $S/routes.rs $S/registry/event_forwarder.rs | awk -F: '{s+=$2} END{print s}'
# 1 → 0, 6 → 0, 4 → 0, 4 → 0: the four helpers are gone
grep -rc "broadcast_all" $S --include=*.rs | awk -F: '{s+=$2} END{print s}'
grep -c "publish_applied_runtime_effects" $S/registry/event_forwarder.rs
grep -cE "broadcast_event_with_(context|user_input)" $S/registry.rs $S/registry/event_forwarder.rs | awk -F: '{s+=$2} END{print s}'
grep -rc "log_metadata_only_event_rejection" $S --include=*.rs | awk -F: '{s+=$2} END{print s}'
# 2 → 1: the internal-only pattern exists once in production code
grep -rlE "ThreadOpened \{ \.\. \} \| AgentEvent::DiffUpdated \{ \.\. \}" $S --include=*.rs | while read f; do prod $f | grep -c "ThreadOpened { .. } | AgentEvent::DiffUpdated { .. }"; done | awk '{s+=$1} END{print s}'
# 0 → 1: the predicate
grep -c "pub(crate) fn is_internal_event" $S/thread_runtime.rs
# 10 → 7: hub's public async surface is register/subscribe/unsubscribe/disconnect/publish/publish_runtime_overview/invalidate_thread_catalog
prod $S/hub.rs | grep -cE "pub(\(crate\))? async fn"
# 0 → 1, 0 → 1, 0 → 1
grep -c "pub async fn publish(" $S/hub.rs
grep -c "^pub struct AppliedRuntimeEvent" $S/thread_runtime.rs
grep -c "^pub struct RunningTasksProjection" $S/thread_runtime.rs
# 8 → 11, 29 → 29, 44 → 44, 226 → 226
grep -cE "^\s*#\[(tokio::)?test" $S/hub.rs
grep -cE "^\s*#\[(tokio::)?test" $S/registry.rs
grep -cE "^\s*#\[(tokio::)?test" $S/registry/event_forwarder.rs
grep -cE "^\s*#\[(tokio::)?test" crates/giskard-server/tests/*.rs | awk -F: '{s+=$2} END{print s}'
# 0 → 0: proto and delivery untouched
git diff --stat origin/main -- crates/giskard-proto crates/giskard-server/src/delivery.rs | wc -l
```

## Pitfalls

- Keep the effects order. A client's ordered lane must see `RequestState` before `RunningTasks`,
  and the overview goes to the replacement lane; five forwarder tests match those frames in
  sequence.
- `user_input` applies to `TurnStarted` only. Today's `broadcast_event_with_user_input` passes
  `live_turn_user_input(ctx)` for every event but only `TurnStarted` uses it; the hub must do the
  same, not attach it elsewhere.
- Set the wire `TurnStarted.user_input` **after** `from_agent_event`, which always writes `None`
  there; do not add a second mapping of `TurnStarted`.
- The late command completion is an `ItemCompleted` for a persisted turn. The descriptor comes from
  the item's durable fields; do not consult the live-turn output maps in `runtime_live.rs`, which
  serve reconnect snapshots.
- The `Error` variant is thread-scoped and ordered. There is no `broadcast_all` replacement because
  nothing broadcasts to every client on the ordered lane; the two process-wide lanes are
  replacement lanes and keep their methods.
- `is_internal_event` belongs in `thread_runtime.rs`, not in the hub: the runtime owns the
  sequence rule and the hub consults it, which is the dependency direction the review's B3/C5
  steps assume.
- `event_forwarder.rs` sees `Outbound` only through `registry.rs`'s imports (`use super::*`); add
  the import there, not in the forwarder.
- Do not touch `runtime_live.rs`'s `WireAgentEvent` use (`:282-306`): the live-turn snapshot is the
  second outbound edge §3.5 names, and it stays where it is.

## Stop rules

Stop and re-cut if the diff:

- changes `giskard-proto` or `delivery.rs`, touches `routes.rs` outside its two publish sites, or
  changes any integration test beyond the one call at `e2e_smoke.rs:6045-6055`;
- changes a log line's level, message, or fields at a migrated site;
- reorders request state, running tasks, and overview within `RuntimeEffects`;
- leaves a `WireAgentEvent`, `WireItem`, or `ServerMessage::` construction in production code
  outside `hub.rs` and `runtime_live.rs`;
- sends `DiffUpdated` or `ThreadOpened` on the transcript lane from any path, or drops any other
  variant;
- adds a second definition of the internal-only predicate, or a cargo feature, `allow`, or `cfg`.

## Follow-on: fewer variants, later

`Outbound`'s five variants are the lanes as the callers compute them today. Two reductions fall
out of later steps and are not attempted here:

- **S7 (C5)** reshapes the runtime's effect types. `RequestTransition` (`thread_runtime.rs:272-275`)
  is a subset of `AppliedRuntimeEvent`; once one struct replaces both, the registry's two
  request-state sites publish `RuntimeEffects` and `Outbound::Request` goes.
- **S8 (C2)** makes the runtime's apply step the one place a harness event becomes its
  client-visible form. When apply returns the enriched transcript event (prompt attached to
  `TurnStarted`, durable output descriptor attached to a late `ItemCompleted`) as part of the
  applied effects, `Transcript` loses its `user_input` and `command_output` fields and can become
  a field of `RuntimeEffects`.

The end state is three variants, one per delivery class the runtime does not own:
`RuntimeEffects`, `Metadata`, `Error`. S5 does not start either change: both alter the runtime's
types and the forwarder's structure, which the review fences for S7 and S8.
