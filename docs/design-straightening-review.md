# Design straightening review

A step back after M0–M8, written against `main` at `37fb77e` plus the follow-up in PR #239. The
milestones fixed the event pipeline's authority and ordering problems. This review asks what is
now *structurally* out of place: state that is grouped by history rather than by ownership,
abstractions that exist in four copies, functions that carry a whole pipeline in one body, and
test hooks that grew one atomic at a time. Every measurement below was taken on that tree.

## What is right and should not move

- **Authorities.** `ProjectAuthority` and `ThreadAuthority` own process-local identity; keyed maps
  outside them carry an `ENTITY-AUTHORITY-EXCEPTION` comment. This rule held through eight
  milestones and is the reason the races could be fixed locally. Keep it, and keep the rule that
  a fix adds no lock, timer, epoch, or peer map.
- **One driver per project, one owner per thread, one Codex task per harness.** The three
  single-consumer loops (`ProjectEventDriver::run`, `ThreadEventForwarder::run`,
  `CodexInstance::run`) are where ordering is decided, and each has exactly one `select!`.
- **Retained logs with typed gaps.** `EventLog` is small (320 production lines) and its contract
  is now complete.
- **Revisioned projections on the wire.** `ThreadMetadata`, `RequestState`, `RunningTasks`,
  `ThreadRuntimeOverview` each have one authority and one clock (spec §13.6.1).

## Measurements that motivate the rest

| Item | Value |
| --- | --- |
| `event_forwarder.rs` production lines / `handle_event` alone | 2002 / 584 |
| `thread_runtime.rs` production lines / `ThreadRuntimeSupport` public methods | 2073 / 40 |
| `mapping.rs` production lines / keyed exception maps on `CodexMapper` | 3355 / 11 |
| `registry.rs` production lines / `Registry` public methods | 1952 / 30 |
| `routes.rs` production lines / handlers | 2100 / 76 |
| `giskard-harness-codex/src/lib.rs` production lines | 3202 |
| `#[cfg(test)]` hooks in production code (server, harness, codex crates) | 119 |
| Test-only atomic counters on `RegistryShared` | 5 |
| `impl AgentHarness for` fakes across the workspace | 21 |
| Copies of `event_kind` / `event_turn_id` / `event_thread` style helpers | 10, in 4 crates |
| `#[allow(clippy::too_many_arguments)]` | 5 |
| Copies of `generate_password_hash` in server integration tests | 12 |
| `static/app.js` | 10 351 lines, one file |

## Findings, grouped by principle

### A. Observation should be a seam, not a scattering of counters

**Status: landed in S3.**

**Now.** `RegistryShared` carries five `#[cfg(test)] AtomicUsize` fields
(`discovery_records_processed`, `link_admissions_processed`, `deferred_link_requeues`,
`failed_owner_removals_warned`, `teardown_owner_exits`), each incremented at one branch in the
driver and polled by tests with `wait_until`. Every new branch worth testing adds another. The
counters are also lies of omission: they say a branch ran, not which thread or admission it ran
for, so tests that need identity fall back to file-system probes.

**Principle.** The driver already makes discrete decisions at known points. Those decisions are
the natural observation seam. Emit them as values on one channel; tests await the value they need
instead of polling a count.

**Proposal.** A `DriverEvent` enum on `ProjectEventDriver`:

```rust
pub(super) enum DriverEvent {
    AdmissionFinished { source: AdmissionKind, native_thread_id: String, outcome: Result<Option<ThreadId>, HarnessError> },
    AdmissionDeferred { native_thread_id: String, attempts: u32, reason: DeferReason },
    OwnerExited { thread_id: ThreadId, reason: ForwarderExitReason, disposition: OwnerExitDisposition },
    AttachRefused { thread_id: ThreadId, reason: &'static str },
}
```

emitted through `fn observe(&self, event: DriverEvent)`. In production the method is a `debug!`
with structured fields, which replaces several ad-hoc log lines. Under `cfg(test)` the driver also
holds an optional `mpsc::UnboundedSender<DriverEvent>` installed by the test fixture, and the
five counters and their `fetch_add` sites go away. Tests become "await `AdmissionDeferred` for
native id X" rather than "spin until counter == 2", which also removes the class of flake fixed
twice in PR #239 (a count that advanced past the awaited value). The same seam fits the forwarder
(`TurnAdmitted`, `EventDropped { reason }`, `Completed`), whose tests currently read the hub or
the store to infer what it decided.

This is not a peer map and not a lock; it is one sender per driver, owned by the driver.

### B. Fields should be grouped by lifetime and owner, not by the order they were added

**B1. `CodexMapper` per-turn state.** Six maps are keyed by `NativeTurnKey` and cleaned up in two
places by hand (`clear_active_turn`, `TurnCompleted`): `turn_ids`, `turn_usage`, `emitted_usage`,
`turn_models`, `invalid_context_window_turns`, plus `file_change_previews` and `running_commands`
keyed by item within a turn. They are one object:

```rust
struct NativeTurnState {
    id: TurnId,
    usage: TokenUsage,
    emitted_usage: Option<(TokenUsage, Option<u32>)>,
    model: Option<ModelRef>,
    invalid_window_warned: bool,
    file_change_previews: HashMap<NativeItemId, Vec<FileChangeEntry>>,
    running_commands: HashSet<NativeItemId>,
}
turns: HashMap<NativeTurnKey, NativeTurnState>,
```

**Status: landed in S2, with the grouping corrected by lifetime.** `turn_ids` remains separate
because it has harness lifetime and preserves identity for late command completion;
`file_change_previews` and `running_commands` also remain separate because their keys and cleanup
sites differ. `NativeTurnState` groups only usage, emitted usage, model, and invalid-window warning
state, which are removed together at turn completion or thread cleanup.

One map, one `ENTITY-AUTHORITY-EXCEPTION` comment instead of six, one `remove` on completion,
and `clear_active_turn` becomes a single `retain`. The usage handler stops doing five lookups on
the same key. This is mechanical and the mapper's 86 functions do not change signature.

**B2. `ThreadRuntimeEntry` per-item outputs.** `command_outputs`, `tool_outputs`, and
`persisted_command_output_versions` are three maps keyed by `(TurnId, ItemId)` that are always
read and written for the same item. Fold them into one `HashMap<(TurnId, ItemId), ItemOutputs>`.

**Status: landed in S2, with the grouping corrected by lifetime.** `ItemOutputs` groups only live
command and tool outputs, which are pruned at turn completion. The persisted command-output version
cache remains separate because durable-output routes need it after the turn has persisted.

**B3. `RegistryShared` is two things.** It holds the identity indexes and the harness transition
gate (registry concerns) and also five service handles (`hub`, `runtime`, `store`,
`thread_metadata`, `ledger`) that the forwarder, driver, and routes reach through it. Split the
services into a `Services` struct owned by `RegistryShared` and passed by `Arc` to the forwarder,
so the forwarder no longer needs the registry at all. That is the first step toward the
forwarder becoming testable without a registry fixture (see D).

**B4. `ForwarderExitReason`, `OwnerPhase`, and the teardown predicate** are already well placed.
Leave them.

### C. The abstractions that are missing

**C1. `AgentEvent` knows its own identity.** Four crates hand-roll the same exhaustive matches:
`event_kind` (forwarder, hub, codex), `event_turn_id` / `agent_event_turn` (forwarder, codex),
`event_thread` (codex, replay), `remap_event_thread` (replay), `event_item_id` (forwarder). Ten
functions, one per-variant table each, all of which had to be edited when `TurnUsageUpdated`
landed. Put them on the type in `giskard-core`:

```rust
impl AgentEvent {
    pub fn kind(&self) -> &'static str;
    pub fn turn(&self) -> Option<TurnId>;
    pub fn item_id(&self) -> Option<ItemId>;
    pub fn set_thread(&mut self, thread: ThreadId);
}
```

**Status: landed in S1.**

`thread_id()` already exists there; this finishes the job. Adding a variant then touches one
file.

**C2. `handle_event` is a pipeline written as one function.** Its 584 lines are a fixed sequence
of gates and effects: foreign-thread drop, duplicate notice, item identity conflict, cross-turn
drop, external turn reservation, item-output preparation, the persisted-turn late path, the
turnless path, diff capture, usage handling, per-kind bookkeeping, live-buffer admission,
completion, broadcast. Each gate is a decision with no side effects; each effect is a call into
the runtime, hub, store, or driver. Separate them:

```rust
enum EventDisposition {
    Drop(DropReason),
    LateForPersistedTurn(TurnId),
    Turnless,
    Owned { first_for_turn: bool, completes: Option<(TurnId, TokenUsage, TurnStatus)> },
}
fn classify(&self, event: &AgentEvent) -> EventDisposition;   // pure, unit-testable
async fn apply(&mut self, event: AgentEvent, disposition: EventDisposition) -> ForwarderControl;
```

`classify` is where the M2/M5/M7 fences live and where every future "which turn does this belong
to" question is answered; it can be tested with a struct and no runtime. `apply` shrinks to the
effects. The current per-kind `match` in the middle (link items to the driver, diff accumulation,
compaction markers) becomes a third small function. The 58 log statements in the file mostly
belong to `classify`'s drop reasons and collapse into one `log_drop(reason, &event)`.

**C3. Outbound lanes are chosen in three places.** `Hub::broadcast_event` narrows to the wire
and refuses internal-only kinds; `registry::broadcast_event_with_context` re-implements the
narrowing to attach `user_input`; `publish_applied_runtime_effects` in the forwarder routes
runtime effects to three other lanes. Give the hub one typed entry point:

```rust
enum Outbound {
    Transcript { event: AgentEvent, user_input: Option<UserInput> },
    Metadata(ThreadState),
    RuntimeEffects(AppliedRuntimeEvent),
    Overview(ThreadRuntimeOverview),
}
impl Hub { async fn publish(&self, thread_id: ThreadId, outbound: Outbound) }
```

so the "which lane, which clock" table in spec §13.6.1 has one implementation, and the
forwarder and registry stop knowing about `WireAgentEvent`.

**C4. `AgentHarness` is three interfaces.** Its 21 methods split cleanly by receiver:

- process-scoped: `capabilities`, `client_version`, `list_models`, `list_providers`,
  `list_mcp_servers`, `reload_mcp_servers`, `start_mcp_oauth_login`, `discoveries`, `shutdown`;
- thread-scoped, take a `ThreadHandle`: `open_thread`, `claim_native_thread`, `subscribe`,
  `set_thread_name`, `set_thread_archived`, `delete_thread`, `compact_thread`, `interrupt`;
- turn-scoped: `start_turn`, `respond_approval`, `respond_server_request`, `terminate_command`.

`Registry` mirrors this with 30 public methods, a third of which are pure pass-throughs
(`list_models`, `list_providers`, `client_version`, `capabilities`, `list_mcp_servers`,
`reload_mcp_servers`, `start_mcp_oauth_login`). Two options, in order of preference:

1. Keep one trait but stop routing the process-scoped calls through `Registry`: expose
   `Registry::harness(project) -> Result<Arc<dyn AgentHarness>>` and let routes call the trait.
   Removes seven facade methods and their tests with no trait change.
2. Split the trait into `HarnessProcess`, `HarnessThreads`, `HarnessTurns` with a blanket
   `AgentHarness: HarnessProcess + HarnessThreads + HarnessTurns`. Fakes then implement only what
   they use, which directly attacks the 21 fake implementations below.

**C5. `ThreadRuntimeSupport` is five components behind one door.** Its 40 public methods cluster
into: request ledger (`register_approval`, `claim_request`, `request_state(s)`), live buffer
(`ensure_live_turn`, `replace_live_turn`, `live_snapshot`, `resolve_live_*`), running tasks
(`tasks_snapshot`, `task_by_*`, `set_task_terminating`), item outputs and captured diffs
(`prepare_item_output`, `command_output`, `tool_output`, `captured_diff*`), and the turn gate and
overview (`reserve_turn`, `settle_completed_turn`, `current_overview`, permits). `ThreadRuntimeEntry`
already stores them as five fields. Make each a type with its own `impl` and its own tests, and
let `ThreadRuntimeSupport` become the lock-and-dispatch layer around `ThreadRuntimeEntry {
requests: RequestLedger, live: LiveTurnState, tasks: RunningTaskState, outputs: ItemOutputs,
gate: TurnGate }`. `runtime_live.rs` and `runtime_tasks.rs` already are this shape; the other
three are inline. `apply_event_locked` then reads as five `apply` calls.

**C6. A test-support crate.** Twenty-one `impl AgentHarness for` fakes, twelve copies of
`generate_password_hash`, six of `ws_text`, three of `spawn_test_app`, and `e2e_smoke.rs` at
8 815 lines say the same thing: there is no shared fixture. A `giskard-testkit` crate (or a
`tests/support` module for the server crate) with one configurable `FakeHarness` (gates for open,
claim, respond, shutdown; per-thread logs; a discoveries log; recorded calls) and the WebSocket
helpers would remove several thousand lines and make every new integration test a page. C4
option 2 makes the fake small; without it the fake is still one place instead of twenty-one.

### D. Cohesion of the largest units

- **`routes.rs`** carries HTTP handlers and the WebSocket dispatch for 13 client message kinds.
  Move the WebSocket session (`ClientMessage` dispatch, ticket handling, subscription bookkeeping)
  to `ws.rs`; it is a different protocol with different error mapping.
- **`giskard-harness-codex/src/lib.rs`** is 3 202 production lines because the transport
  helpers, the worker-queue watchdog (208 lines), upload preparation, and the `AgentHarness`
  impl share a file. `instance.rs` and `transport.rs` show the split that is wanted; finish it
  with `queue.rs` (watchdog), `uploads.rs`, and `rpc.rs` (the `codex_respond_*` helpers).
- **`app.js`** at 10 351 lines is one scope. The no-toolchain constraint does not require one
  file: browsers load `<script type="module">` natively, and `include_str!` embedding works per
  file. A first cut into `ws.js`, `transcript.js`, `requests.js`, `gauge.js`, `composer.js` with
  an explicit `state` module would give the `ui.rs` source tests real boundaries to assert on
  instead of substring searches.

### E. What not to do

- Do not introduce a general event bus, an actor framework, or trait objects for the runtime
  components. The single-consumer loops are the design; the refactors above make their inputs
  and outputs typed, nothing more.
- Do not merge the driver and forwarder loops into one "project actor". The two-level split is
  what lets a thread owner block on persistence without stalling admissions.
- Do not start M9 (cursor-committed persistence) before C2 and C5: its cursor commit lands in
  `complete_forwarded_turn` and `apply_event_locked`, exactly the code these two changes reshape.

## Sequencing

Each step is one PR that stands alone on `main`, mechanical first:

| # | Change | Kind | Size (non-test lines) |
| --- | --- | --- | --- |
| 1 | C1 `AgentEvent` accessors; delete the ten copies — **landed in S1** | mechanical | −150 |
| 2 | B1 `NativeTurnState`; B2 `ItemOutputs` — **landed in S2** | mechanical | −120 |
| 3 | A `DriverEvent` seam; delete the five counters | contract for tests only | ±80 |
| 4 | C6 test-support crate; migrate the server integration tests to it | tests only | −3000 |
| 5 | C3 `Hub::publish(Outbound)` | one seam | ±100 |
| 6 | B3 `Services` split; forwarder takes `Arc<Services>` | mechanical | ±60 |
| 7 | C5 runtime components | structural, no behaviour change | ±300 |
| 8 | C2 `classify` / `apply` in the forwarder | structural, no behaviour change | ±250 |
| 9 | C4 option 1, then option 2 if C6 wants it | API | −200 |
| 10 | D file splits (`ws.rs`, codex modules, `app.js`) | mechanical | 0 |

Steps 1–4 can be given to an agent today; each has a crisp exit (grep returns nothing, counter
fields gone, one fake). Steps 7 and 8 need a plan document in the M-series style because they
touch the code the milestones fenced; the tests from M0–M8 are the safety net and must not be
edited by those steps except for fixture changes.

## Signs a step has gone wrong

- It changes the published `RequestState`, `ThreadState`, or transcript sequence in any test.
- It adds a trait object where a struct was, or a channel where a call was, outside A and C3.
- It edits `AGENTS.md` rules rather than satisfying them.
- Its diff is not almost entirely moves and deletions.
