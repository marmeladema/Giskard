# S7 — The thread runtime as components behind one lock

Implementation plan for step 7 of [`design-straightening-review.md`](design-straightening-review.md)
(finding C5) plus the first reduction recorded under "Follow-on" in
[`s5-hub-publish.md`](s5-hub-publish.md) (`Outbound::Request` goes). Written against `main` at
`7822b02` (S6 merged); every file and line reference below was checked against that tree.
Re-check them if the branch has moved.

## Goal

`ThreadRuntimeSupport` (`crates/giskard-server/src/thread_runtime.rs`) is 2 105 production lines
in which three clusters of per-thread state — the request ledger, the captured diffs, and the item
outputs — are inline `HashMap`s on `ThreadRuntimeEntry`, mutated by free functions that take the
whole entry, next to the turn gate (`active_turn` + `lifecycle_revision`) and the cross-thread
overview. `runtime_live.rs` and `runtime_tasks.rs` already show the intended shape: one type, one
`impl`, one test module, no lock, no authority.

After S7:

- each cluster is a type in its own module with `&mut self` methods and no knowledge of locks,
  authorities, permits, or the overview;
- `ThreadRuntimeEntry` is a struct of components plus its clocks;
- `ThreadRuntimeSupport` is the lock-and-dispatch layer: it resolves the entry through the
  authority, locks it, calls one component method, refreshes the overview when a summary input
  changed, and shapes the result;
- `apply_event_locked` reads as one call per component;
- `Outbound::Request` is gone: every request transition is published as `RuntimeEffects`.

No behaviour change: no log line, error string, wire message, message order, or test assertion
changes.

## Three corrections to the review

1. **The entry has ten fields, not five.** The review says `ThreadRuntimeEntry` "already stores
   them as five fields". It stores `active_turn`, `lifecycle_revision`, `requests`,
   `event_sequence`, `task_revision`, `live`, `tasks`, `captured_diffs`, `item_outputs`,
   `persisted_command_output_versions` (`thread_runtime.rs:56-67`). Two are already types
   (`live`, `tasks`), one is a cache with its own lifetime that S2 deliberately left alone
   (`persisted_command_output_versions`, see `s2-grouped-state.md` correction 2), and three are
   clocks. S7 turns the three inline maps and the gate into four types and folds one clock
   (`task_revision`) into the component it counts. The entry ends with eight fields.
2. **"outputs: ItemOutputs" is two types, and the name is taken.** S2 gave `ItemOutputs` to the
   per-item struct (`:131-135`). Captured diffs are keyed by `TurnId` and item outputs by
   `(TurnId, ItemId)`; each is ~250 lines of logic with no shared code. They become
   `CapturedDiffState` and `ItemOutputState`, each with a `clear_turn` that `settle_completed_turn`
   calls in the order the two inline removals run today.
3. **`RequestTransition` stays; the fold is at the publish edge.** The S5 follow-on said "once one
   struct replaces both, ... `Outbound::Request` goes". Verification: `RequestTransition`'s
   `request_state` is non-optional and 16 test lines assert on it directly
   (`thread_runtime.rs:2766, 2768, 2782, 2784, 2805, 2807, 2850, 2852, 3021, 3095, 3123, 3125`;
   `event_forwarder.rs:4869, 4871, 4924, 4926`), while `AppliedRuntimeEvent::request_state`
   must stay `Option` because most applied events touch no request. A transition
   is the more precise type. S7 keeps it as the ledger's typed result and adds
   `impl From<RequestTransition> for AppliedRuntimeEvent`; the registry publishes
   `Outbound::RuntimeEffects(transition.into())`. `Outbound::Request` still goes, with no test
   assertion touched.

## Non-goals

- No change to `LiveTurnState`: `runtime_live.rs` moves to `thread_runtime/live.rs` unchanged.
  `RunningTaskState` moves to `thread_runtime/tasks.rs` and changes only as D5 says.
- No change to `ThreadRuntimeSlot`, `ThreadAuthority`'s runtime-entry methods
  (`registry/thread.rs:377-397`), `RestorePermit`, `PersistedCommandOutputVersionPermit`,
  `ThreadTurnLease`, `RequestClaim`'s public API, or `ResolvedThreadRuntime`.
- No change to the `Outbound` variants other than deleting `Request`; no change to `Hub` beyond
  that arm. S8 (C2) owns the `Transcript` reshaping.
- No change to `AppliedRuntimeEvent`'s fields or to `RunningTasksProjection`.
- No new `Arc`, `Mutex`, trait object, or channel. Components are plain structs owned by the
  entry; the one lock stays where it is.
- No edit to the M-series tests except the fixture-level edits listed under **Tests**. No test is
  deleted; two pure unit tests move with the code they test.
- No import change outside `thread_runtime.rs`, `hub.rs`, `registry.rs`, `lib.rs`: the runtime's
  crate-facing names are re-exported from `thread_runtime.rs` (D8).

## Ground truth

All paths under `crates/giskard-server/src/`.

| Fact | Where |
| --- | --- |
| `ThreadRuntimeSupport { overview: Arc<Mutex<OverviewState>>, max_command_output_bytes }` | `thread_runtime.rs:35-39` |
| `ThreadRuntimeEntry` ten fields, with the lifetime-class doc comment AGENTS.md requires ("Both `CodexMapper` and `ThreadRuntimeEntry` list their lifetime classes in a doc comment") | `:41-67` |
| `impl ThreadRuntimeSupport` has 41 `pub(crate) fn` and 10 private `fn` | `:473-1395` |
| Production reads of the fields that move (multi-line chains included): `requests` ×11, `captured_diffs` ×5, `item_outputs` ×7, `active_turn` ×9, `task_revision` ×8, `lifecycle_revision` on the entry ×4 (`:716, :733, :979, :1181`; the other reads are `permit.lifecycle_revision`) | `rg -U '\.\s*<field>\b'` over lines 1–2105 |
| Test reads of those fields: `captured_diffs` at `:2346, :2609`; `item_outputs` at `:3657, :3663, :3693, :3699`; `active_turn` at `:3553` | same, lines 2106–4036 |
| The eleven direct `requests` map operations: `:1085` (get), `:1126` (retain), `:1258` (get_mut), `:1315` (get), `:1326` (values), `:1340` (get), `:1806` (entry), `:1838` (get_mut), `:1897` (iter), `:1988` (get_mut), `:2047` (get_mut) | `rg -U 'requests\s*\.\s*(get\|get_mut\|entry\|retain\|values\|iter)\('` |
| Request types: `RuntimeRequestId` `:291-295` (+ `as_str` `:1776-1783`), `RequestPayload` `:297-301`, `RequestStatus` `:303-308`, `RequestRecord` `:310-316`, `RequestResolution` `:318-322`; `RequestTransition` `:271-275`; `RequestCommitError` `:277-281`; `RequestClaim` `:324-332` | read |
| Request logic: `register_request` `:1798-1830`, `resolve_server_request_from_harness` `:1832-1875`, the outstanding-request half of `runtime_summary` `:1895-1914`, `wire_request_state` `:1926-1962`, `claim_request` `:1250-1289`, `RequestClaim::commit` `:1972-2031`, `rollback` / `rollback_inner` `:2033-2072`, `Drop` `:2074-2091`, `next_claim_id` `:2093-2097`, `register_approval` (`cfg(test)`) `:1232-1248`, `request_state` `:1307-1318`, `request_states` `:1320-1330`, `resolution_for_test` `:1332-1344`, settle's prune `:1126-1129` | read |
| Diff types: `ActiveCapturedDiffs` `:212-222`, `CapturedDiffSlot` `:224-232`, `SupersededCapturedDiff` `:234-237`, `RuntimeDiffLookup` `:239-243` | read |
| Diff logic: `capture_event_diffs` `:515-572`, `captured_diff_records` `:574-587`, `captured_diff` `:663-683`, `install_captured_diff` `:1404-1446`, `reconcile_item_captured_diffs` `:1448-1489`, settle's `captured_diffs.remove` `:1121` | read |
| Output types: `RuntimeCommandOutput` `:115-122`, `RuntimeToolOutput` `:124-128`, `ItemOutputs` `:131-141`, `PreparedItemOutput` `:161-170` (all fields private; `command_descriptor`, `tool_descriptor`, `live_event` are read outside the output code at `:1000-1008`), `RuntimeCommandOutputLookup` `:172-175`, `RuntimeToolOutputLookup` `:177-180` | read |
| Output logic: `ThreadRuntimeEntry::set_command_output` / `set_tool_output` `:143-159`, `command_output` `:589-607`, `tool_output` `:609-627`, `remove_tool_output` `:629-639`, `remove_command_output` `:641-651`, `update_command_output_authority` `:1513-1555`, free `prepare_item_output` `:1557-1646`, `update_prepared_item_output_authority` `:1648-1665`, `update_tool_output_authority` `:1667-1710` (reads `entry.active_turn` for the `project_id` log field at `:1694-1697`), `command_output_version` `:1712-1714`, settle's `item_outputs.retain` `:1122-1124` | read |
| Gate: `ActiveTurnOwner` `:283-289`, `TurnReservation` `:334-341`, `reserve_turn` `:1153-1189` (the warn at `:1162-1173`, bump at `:1181`), `has_active_turn` `:1191-1198`, `acknowledge_turn` `:1200-1214`, `release_turn` `:1216-1230`, settle's `active_turn.take()` + debug `:1130-1137` and `persistence_blocked` branch `:1140-1147`, the turn-state half of `runtime_summary` `:1881-1894`, permit reads `:716, :733, :979` | read |
| `task_revision` is bumped exactly when a `RunningTaskState` mutator returned `true`: `set_task_terminating` `:895-900`, `remove_task_by_process` `:914-917`, `apply_event_locked` `:1072-1075`; read at `:836` and `:1094` | read |
| `RunningTaskState`'s three mutators: `apply_event` `runtime_tasks.rs:22`, `set_terminating_by_process` `:180`, `remove_by_process` `:218`; 17 tests, all built with `RunningTaskState::new()` | grep |
| `apply_event_locked` `:1024-1101`: sequence, request match, item-output branch, tasks, live append, request-state shaping | read |
| `settle_completed_turn` `:1103-1151`: order is `live.clear_turn`, `captured_diffs.remove`, `item_outputs.retain`, `requests.retain`, `active_turn.take()` | read |
| `Outbound::Request(RequestState)` `hub.rs:33`, its arm `:304-307`; `RequestState` imported at `hub.rs:12` and otherwise used only as the `ServerMessage::RequestState` variant path (`:305, :313, :361`) | grep |
| Registry request lanes: `publish_request_state` `registry.rs:1073-1091` publishes `Outbound::Request(request)` then `services.publish_runtime_overview()`; `publish_request_transition` `:1093-1104` publishes `Outbound::Request(transition.request_state)` then `hub.publish_runtime_overview(overview)` if changed; eight callers of the latter (`:974, :981, :989, :1007, :1036, :1043, :1051, :1069`) | grep |
| Three tests match on request error strings: `is not pending` (`thread_runtime.rs:2818, :2966`), `no pending request` (`:3393`) | grep |
| `Hub::publish` for `RuntimeEffects` sends `RequestState` if `Some`, then `RunningTasks` if `Some`, then `publish_runtime_overview` if `Some` (`hub.rs:311-323`); `Services::publish_runtime_overview` is `hub.publish_runtime_overview(runtime.current_overview())` (`services.rs:61-65`) | read |
| The one test that publishes `Outbound::Request`: `event_forwarder.rs:4874`, with `responding` from `claim_request` at `:4866-4868`; its `client_rx` is the `register_client` channel (`:4821-4823`), and overviews go to the replacement channel (`hub.rs:226-238`), so an extra overview publish never reaches `client_rx` | read |
| Crate-facing names imported from `thread_runtime` elsewhere (24 `thread_runtime::` lines outside the file): `registry.rs:48-51` (`RequestResolution, RequestTransition, ResolvedThreadRuntime, RestorePermit, RuntimeRequestId, ThreadRuntimeSupport, ThreadTurnLease, TurnReservation`); `event_forwarder.rs:1985` (`RequestResolution, RuntimeRequestId, ThreadRuntimeSupport`), `:3621` (`RuntimeDiffLookup`); `hub.rs:17` (`AppliedRuntimeEvent, is_internal_event`), `:599`; `routes.rs` ×21 (`command_output_version`, `TurnReservation`, `RuntimeDiffLookup`, `RuntimeCommandOutputLookup`, `RuntimeToolOutputLookup`); `services.rs:16`; `registry/thread.rs:12` (`ThreadRuntimeEntry, ThreadRuntimeSlot`) | grep |
| `lib.rs:15-16` declares `mod runtime_live; mod runtime_tasks;` (private modules whose `pub` items are crate-visible); only `thread_runtime.rs:25-26` imports from them | grep |
| `registry.rs` is a file-plus-directory module: `mod admission; mod driver; mod event_forwarder; mod project; mod thread;` at `registry.rs:53-57` load `registry/*.rs`; the crate uses no `mod.rs` under `src/` except `bin/common/mod.rs` | read |
| `thread_runtime.rs` tests: 41; `runtime_tasks.rs`: 17; `runtime_live.rs`: 14 | grep |
| Two thread_runtime tests use only `ActiveCapturedDiffs::default()`, `install_captured_diff`, `CapturedDiffSlot` and `giskard_core` helpers: `identical_unified_text_on_different_paths_has_independent_identity` `:2349-2406`, `item_and_turn_diffs_for_the_same_path_have_independent_authority` `:2407-2470` | read |
| CI lint is `cargo clippy --workspace --all-targets --locked -- -D warnings` (`.github/workflows/ci.yml:41`) | read |

## Design

The runtime becomes a file-plus-directory module like `registry`: `thread_runtime.rs` declares
`mod diffs; mod gate; mod live; mod outputs; mod requests; mod tasks;` and the six files live in
`crates/giskard-server/src/thread_runtime/`. `runtime_live.rs` and `runtime_tasks.rs` are
`git mv`ed there; `lib.rs:15-16` go. The submodules are private to `thread_runtime`, so their
`pub` items are reachable only from `thread_runtime.rs` and its descendants: the module tree,
not convention, fences the components from the rest of the crate, and `thread_runtime.rs` is the
one place that decides what the crate sees (D8). Each carries the bodies named in the ground-truth
table **verbatim**, with `entry.<field>` replaced by `self.<inner>`; the diff must read as moves.
Log lines and error strings move with the code that emits them and keep their text and fields.

### D1. `thread_runtime/requests.rs` — `RequestLedger`

```rust
#[derive(Default)]
pub struct RequestLedger {
    records: HashMap<RuntimeRequestId, RequestRecord>,
}

pub enum ClaimRejection { Missing, NotPending }
pub enum CommitRejection { Missing, StaleClaim, KindMismatch }

impl RequestLedger {
    /// `register_request` (:1798-1830). `true` when the record changed.
    pub fn register(&mut self, request_id: RuntimeRequestId, turn_id: Option<TurnId>, payload: RequestPayload) -> bool;
    /// `resolve_server_request_from_harness` (:1832-1875), logs included.
    pub fn resolve_from_harness(&mut self, thread_id: ThreadId, request_id: &ServerRequestId) -> bool;
    /// `wire_request_state` (:1926-1962) applied to one record; `:1307-1318`.
    pub fn state(&self, thread_id: ThreadId, request_id: &RuntimeRequestId) -> Option<WireRequestState>;
    /// `:1320-1330`.
    pub fn states(&self, thread_id: ThreadId) -> Vec<WireRequestState>;
    /// The sorted outstanding list from `runtime_summary` (:1895-1914).
    pub fn outstanding(&self) -> Vec<OutstandingRequest>;
    /// Settle's prune (:1126-1129): drops resolved records whose `turn_id == turn_id`
    /// — an `Option` compared to an `Option`, so `None` prunes turn-less resolved records,
    /// exactly as today.
    pub fn prune_resolved(&mut self, turn_id: Option<TurnId>);
    /// `claim_request`'s record step (:1258-1272): `Pending` → `Responding { claim, harness_resolved: false }`,
    /// revision +1. Returns the claim id and the new wire state.
    pub fn claim(&mut self, thread_id: ThreadId, request_id: &RuntimeRequestId) -> Result<(u64, WireRequestState), ClaimRejection>;
    /// `RequestClaim::commit`'s record step (:1988-2029), checks in today's order:
    /// record present, claim matches, resolution kind matches payload kind; then `Resolved`, revision +1.
    pub fn commit(&mut self, thread_id: ThreadId, request_id: &RuntimeRequestId, claim_id: u64, resolution: RequestResolution) -> Result<WireRequestState, CommitRejection>;
    /// `rollback_inner`'s record step (:2047-2068): a `Responding` record with this claim goes back to
    /// `Pending`, or to the synthesized `Resolved(Server(Null))` when `harness_resolved`. `None` otherwise.
    pub fn rollback(&mut self, thread_id: ThreadId, request_id: &RuntimeRequestId, claim_id: u64) -> Option<WireRequestState>;
    /// `resolution_for_test`'s record step (:1340-1343).
    #[cfg(test)]
    pub fn resolution(&self, request_id: &RuntimeRequestId) -> Option<RequestResolution>;
}
```

Moves into the module: `RuntimeRequestId` + `as_str` (`pub fn`), `RequestPayload` (`pub`; the
dispatch layer constructs it in `apply_event_locked` and `register_approval`), `RequestStatus`,
`RequestRecord` (private), `RequestResolution` (`pub`), `next_claim_id`. The `wire_request_state`
free function becomes private to the module.

The error **strings** stay in `thread_runtime.rs`, mapped from the rejection enums:
`ClaimRejection::Missing` → `"no pending request for id {}"`, `NotPending` →
`"request {} is not pending"`; `CommitRejection::Missing` → `"request {} disappeared"` with
`rollback: None` and `settled = true`; `StaleClaim` → `"stale claim for request {}"` and
`KindMismatch` → `"response kind does not match request {}"`, both after `drop(entry)` and
`self.rollback_inner()`, as the branches at `:2003` and `:2015` do today. `RequestClaim`
(`:324-332`), `RequestTransition`, `RequestCommitError`, `Drop for RequestClaim`, and the
`"runtime state for request {} disappeared"` branch (`:1981`) stay in `thread_runtime.rs`
unchanged apart from calling the ledger.

### D2. `thread_runtime/diffs.rs` — `CapturedDiffState`

```rust
#[derive(Default)]
pub struct CapturedDiffState {
    turns: HashMap<TurnId, ActiveCapturedDiffs>,
}

impl CapturedDiffState {
    /// The `match` of `capture_event_diffs` (:523-570): mutates the event in place.
    pub fn capture(&mut self, thread_id: ThreadId, event: &mut AgentEvent);
    /// `:580-587`.
    pub fn records(&self, turn_id: TurnId) -> Vec<CapturedDiffRecord>;
    /// `:673-683`.
    pub fn lookup(&self, turn_id: TurnId, diff_id: &DiffId) -> RuntimeDiffLookup;
    /// Settle's `captured_diffs.remove` (:1121).
    pub fn clear_turn(&mut self, turn_id: TurnId);
    /// `current_by_slot.len()` for the one test that reads it (:2610).
    #[cfg(test)]
    pub fn slot_count(&self, turn_id: TurnId) -> usize;
}
```

Moves: `ActiveCapturedDiffs`, `CapturedDiffSlot`, `SupersededCapturedDiff` (private),
`RuntimeDiffLookup` (`pub`), `install_captured_diff`, `reconcile_item_captured_diffs` (private,
signatures unchanged), and the two pure tests named in the ground truth, verbatim, into a
`#[cfg(test)] mod tests` at the end of the module.

`ThreadRuntimeSupport::capture_event_diffs` keeps its signature and becomes: resolve
`entry_or_create`, lock, `entry.diffs.capture(thread_id, &mut event)`, return `event`.

### D3. `thread_runtime/outputs.rs` — `ItemOutputState`

```rust
#[derive(Default)]
pub struct ItemOutputState {
    items: HashMap<(TurnId, ItemId), ItemOutputs>,
}

impl ItemOutputState {
    /// `:144-150` / `:152-158`: drop the map entry when both slots are empty.
    pub fn set_command(&mut self, key: (TurnId, ItemId), output: Option<RuntimeCommandOutput>);
    pub fn set_tool(&mut self, key: (TurnId, ItemId), output: Option<RuntimeToolOutput>);
    /// `:598-607` / `:618-627`.
    pub fn command_output(&self, turn_id: TurnId, item_id: ItemId) -> RuntimeCommandOutputLookup;
    pub fn tool_output(&self, turn_id: TurnId, item_id: ItemId) -> RuntimeToolOutputLookup;
    /// `update_prepared_item_output_authority` (:1648-1665), error log included.
    pub fn apply_prepared(&mut self, prepared: PreparedItemOutput);
    /// `update_command_output_authority` (:1513-1555) then `update_tool_output_authority`
    /// (:1667-1710), in that order. `project_id` is the gate's owner project, read by the caller,
    /// for the `could not serialize completed tool output` log (:1694-1706).
    pub fn apply_completed_item(&mut self, thread_id: ThreadId, project_id: Option<ProjectId>, turn_id: TurnId, item: &Item);
    /// Settle's `item_outputs.retain` (:1122-1124).
    pub fn clear_turn(&mut self, turn_id: TurnId);
    /// `contains_key` for the one test that reads it (:3657-3699).
    #[cfg(test)]
    pub fn contains(&self, turn_id: TurnId, item_id: ItemId) -> bool;
}

pub fn prepare_item_output(event: &AgentEvent) -> Option<PreparedItemOutput>;   // :1557-1646
pub fn command_output_version(output: &str) -> String;                          // :1712-1714
```

Moves: `RuntimeCommandOutput`, `RuntimeToolOutput`, `ItemOutputs` (private), `PreparedItemOutput`
(`pub`, with `command_descriptor`, `tool_descriptor`, `live_event` made `pub(crate)` because
`apply_prepared_event_to_entry` reads them at `:1000-1008`; the other five fields stay private
and are consumed by `apply_prepared`), `RuntimeCommandOutputLookup`, `RuntimeToolOutputLookup`.
`ThreadRuntimeEntry::set_command_output` / `set_tool_output` (`:143-159`) are deleted; the
`impl ThreadRuntimeEntry` block goes with them.

`ThreadRuntimeSupport::normalize_command_output` and `prepare_item_output` (`:475-508`) stay: the
byte limit is support configuration. `prepare_existing_item_output` (`:510-512`) calls the moved
free function.

### D4. `thread_runtime/gate.rs` — `TurnGate`

```rust
#[derive(Default)]
pub struct TurnGate {
    active: Option<ActiveTurnOwner>,
    lifecycle_revision: u64,
}

impl TurnGate {
    /// `:1161-1181`: the `rejecting turn start because thread runtime is already active` warn and
    /// `Err(HarnessError::ThreadBusy { thread: thread_id })`, else install the owner and
    /// advance `lifecycle_revision`.
    pub fn reserve(&mut self, thread_id: ThreadId, reservation: TurnReservation) -> Result<(), HarnessError>;
    /// `:1196`.
    pub fn is_active(&self) -> bool;
    /// `:716, :733, :979`.
    pub fn lifecycle_revision(&self) -> u64;
    /// `:1208-1212` without the warn: `false` when there is no owner (the caller warns).
    pub fn acknowledge(&mut self, turn_id: TurnId) -> bool;
    /// `active.take()`. The two callers keep their different debug lines (:1131-1137, :1221-1227).
    pub fn release(&mut self) -> Option<ActiveTurnOwner>;
    /// `:1141-1143`: `false` when there is no owner (the caller keeps the warn at :1145).
    pub fn block_on_persistence(&mut self, turn: Turn, error: String) -> bool;
    /// `active.as_ref().map(|owner| owner.reservation.project_id)` for the output log (:1694-1697).
    pub fn project_id(&self) -> Option<ProjectId>;
    /// The turn-state half of `runtime_summary` (:1881-1894).
    pub fn turn_state(&self) -> RuntimeTurnState;
    /// `persistence_blocked.map(|(turn, _)| turn.id)` for the one test that reads it (:3551-3560).
    #[cfg(test)]
    pub fn blocked_turn(&self) -> Option<TurnId>;
}
```

Moves: `ActiveTurnOwner` (`pub`, fields `pub(crate)`: the two release sites log
`reservation.project_id`, `acknowledged_turn`, `reserved_at`) and `TurnReservation` (`pub`,
fields already `pub`). The module imports `crate::log_fields::display_opt` for the warn.

### D5. `RunningTaskState` owns its revision

`task_revision` is bumped at three sites (`:899, :916, :1074`), each exactly when a
`RunningTaskState` mutator returned `true`. Move the clock into `thread_runtime/tasks.rs`
(today's `runtime_tasks.rs`):

- field `revision: u64` on `RunningTaskState` (`#[derive(Default)]` already there);
- `pub fn revision(&self) -> u64`;
- `apply_event`, `set_terminating_by_process`, `remove_by_process` do
  `if changed { self.revision = self.revision.saturating_add(1); }` before returning. For
  `apply_event`, whose `match` returns from many arms, rename the current body to
  `fn apply_event_inner(&mut self, event: &AgentEvent) -> bool` and make `apply_event` the
  three-line wrapper. The other two compute `changed` at the end already.

`tasks_snapshot` returns `(entry.tasks.revision(), entry.tasks.snapshot(thread_id))`;
`apply_event_locked` reads `entry.tasks.revision()` for the projection. The three bump sites in
`thread_runtime.rs` are deleted. No `tasks.rs` test changes: none reads a revision.

### D6. The entry and the dispatch layer

```rust
pub(crate) struct ThreadRuntimeEntry {
    gate: TurnGate,
    requests: RequestLedger,
    event_sequence: u64,
    live: LiveTurnState,
    tasks: RunningTaskState,
    diffs: CapturedDiffState,
    outputs: ItemOutputState,
    persisted_command_output_versions: HashMap<(TurnId, ItemId), String>,
}
```

Rewrite the lifetime-class doc comment (`:42-55`) for the new field names; AGENTS.md requires it
to list the classes. Keep today's classes: per-turn (`diffs`, `outputs`, `live`, the resolved
records in `requests`), the owner and clocks cleared only with the entry (`gate`,
`event_sequence`, and now the revision inside `tasks`), caches cleared only with the entry
(`persisted_command_output_versions`), and `tasks`, which outlive turns.

`apply_event_locked` (`:1024-1101`) after S7, same order as today:

```rust
let sequence = /* unchanged (:1031-1034) */;
let (request_id, request_changed) = match event {
    ApprovalRequested { turn, request, .. } => (Some(id), entry.requests.register(id, Some(*turn), RequestPayload::Approval(request.clone()))),
    ServerRequestReceived { turn, request, .. } => (Some(id), entry.requests.register(id, *turn, RequestPayload::Server(request.clone()))),
    ServerRequestResolved { request_id, .. } => (Some(id), entry.requests.resolve_from_harness(thread_id, request_id)),
    _ => (None, false),
};
if let AgentEvent::ItemCompleted { turn, item, .. } = event {
    match prepared_output {
        Some(prepared) => entry.outputs.apply_prepared(prepared),
        None => entry.outputs.apply_completed_item(thread_id, entry.gate.project_id(), *turn, item),
    }
}
let tasks_changed = entry.tasks.apply_event(event);
if append_live && entry.live.is_active(thread_id) { entry.live.append(thread_id, event.clone()); }
let request_state = request_changed.then(|| request_id.as_ref().and_then(|id| entry.requests.state(thread_id, id))).flatten();
AppliedRuntimeEvent { sequence, tasks_changed, running_tasks_if_changed: tasks_changed.then(|| RunningTasksProjection { revision: entry.tasks.revision(), tasks: entry.tasks.snapshot(thread_id) }), request_state, overview_if_changed: None, overview_refresh_needed: request_changed }
```

(`entry.outputs.apply_completed_item(.., entry.gate.project_id(), ..)` borrows two disjoint
fields; it compiles as written.)

`settle_completed_turn` (`:1103-1151`) keeps its order: `entry.live.clear_turn(thread_id)`; if
`completed_turn` is `Some(t)`: `entry.diffs.clear_turn(t)`, `entry.outputs.clear_turn(t)`;
`entry.requests.prune_resolved(completed_turn)`; `if let Some(owner) = entry.gate.release()`
with the `committed persisted turn and released thread runtime` debug; the `Some((turn, error))`
arm becomes `if !entry.gate.block_on_persistence(turn, error) { warn!(.., "cannot retain failed turn without an active owner") }`.

`runtime_summary` (`:1877-1924`) becomes `let turn_state = entry.gate.turn_state(); let
outstanding_requests = entry.requests.outstanding();` plus the unchanged `Idle && empty → None`
rule and the `Some(ThreadRuntimeSummary { .. })`. It stays in `thread_runtime.rs`: the overview is
the one cross-thread projection and belongs to the dispatch layer.

Every other support method is the same three lines around one component call: `live_*` and
`resolve_live_*` (already so), `tasks_*` / `task_by_*` / `set_task_terminating` /
`remove_task_by_process` (drop the bumps), `captured_diff_records` → `entry.diffs.records`,
`captured_diff` → `entry.diffs.lookup`, `command_output` / `tool_output` / `remove_*_output` →
`entry.outputs.*`, `has_active_turn` → `entry.gate.is_active()`, `reserve_turn` →
`entry.gate.reserve(thread_id, reservation)?` then `refresh_overview` and the lease,
`acknowledge_turn` → `if !entry.gate.acknowledge(turn_id) { warn!(..); return None; }`,
`release_turn` → `entry.gate.release()` + its debug, the three permit reads →
`entry.gate.lifecycle_revision()`, `request_state(s)` → `entry.requests.state(s)`,
`register_approval` → `entry.requests.register(..)`, `resolution_for_test` →
`entry.requests.resolution(id)`, `claim_request` / `commit` / `rollback_inner` → D1.

### D7. `Outbound::Request` goes

In `thread_runtime.rs`:

```rust
impl AppliedRuntimeEvent {
    /// Effects that carry only one request's replacement state: a republish of the current record.
    pub(crate) fn for_request(request_state: WireRequestState) -> Self {
        Self { request_state: Some(request_state), ..Self::unchanged() }
    }
}

impl From<RequestTransition> for AppliedRuntimeEvent {
    fn from(transition: RequestTransition) -> Self {
        Self { overview_if_changed: transition.overview_if_changed, ..Self::for_request(transition.request_state) }
    }
}
```

- `registry.rs:1093-1104` `publish_request_transition` body becomes one publish:
  `hub.publish(thread_id, Outbound::RuntimeEffects(transition.into())).await`. The hub's
  `RuntimeEffects` arm sends `RequestState` then the overview if `Some` — the order the two
  publishes had. Its eight callers are untouched.
- `registry.rs:1073-1091` `publish_request_state`: replace `Outbound::Request(request)` with
  `Outbound::RuntimeEffects(AppliedRuntimeEvent::for_request(request))`; keep the following
  `services.publish_runtime_overview().await` exactly as it is (it publishes the *current*
  overview unconditionally, which is not an `overview_if_changed`). Add `AppliedRuntimeEvent` to
  the import at `:48-51`.
- `hub.rs`: delete the `Request(RequestState)` variant (`:33`) with its doc line (`:32`) and the
  arm (`:304-307`); remove `RequestState` from the import at `:12` — it has no other use as a
  type in the file and `-D warnings` rejects the unused import.
- `event_forwarder.rs:4874`: `Outbound::Request(responding.request_state)` →
  `Outbound::RuntimeEffects(responding.into())`. The transition's `overview_if_changed` is
  `Some` (Responding changes the summary), so the hub additionally publishes an overview; it goes
  to the replacement channel, which this test never reads. The assertions on `responding` at
  `:4869-4872` run before the move and are unchanged.

### D8. `thread_runtime.rs` keeps the crate-facing surface

Add, near the top of `thread_runtime.rs`:

```rust
mod diffs;
mod gate;
mod live;
mod outputs;
mod requests;
mod tasks;

pub(crate) use diffs::RuntimeDiffLookup;
pub(crate) use gate::TurnReservation;
pub(crate) use outputs::{
    RuntimeCommandOutput, RuntimeCommandOutputLookup, RuntimeToolOutput, RuntimeToolOutputLookup,
    command_output_version,
};
pub(crate) use requests::{RequestResolution, RuntimeRequestId};
```

(Plus private `use` lines for what only `thread_runtime.rs` needs: `RequestLedger`,
`RequestPayload`, `ClaimRejection`, `CommitRejection`, `CapturedDiffState`, `ItemOutputState`,
`PreparedItemOutput`, `prepare_item_output`, `TurnGate`, `ActiveTurnOwner`.) Every import listed
under "Crate-facing names" in the ground truth then resolves unchanged; `routes.rs`,
`event_forwarder.rs`, `services.rs`, `registry/thread.rs`, `tests/e2e_smoke.rs` are not edited
except `event_forwarder.rs:4874`. Delete any `use` in `thread_runtime.rs` that only the moved
code needed (`sha2`, `command_status_is_running`, `CapturedDiffDescriptor`, `Item`, ...) or
`-D warnings` fails.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/lib.rs` | delete `:15-16` (`mod runtime_live; mod runtime_tasks;`) |
| `crates/giskard-server/src/thread_runtime/requests.rs` | new (D1), ~330 lines |
| `crates/giskard-server/src/thread_runtime/diffs.rs` | new (D2), ~250 lines + the two moved tests |
| `crates/giskard-server/src/thread_runtime/outputs.rs` | new (D3), ~330 lines |
| `crates/giskard-server/src/thread_runtime/gate.rs` | new (D4), ~120 lines |
| `crates/giskard-server/src/thread_runtime/live.rs` | `git mv` of `runtime_live.rs`, content unchanged |
| `crates/giskard-server/src/thread_runtime/tasks.rs` | `git mv` of `runtime_tasks.rs`, then D5: `revision` field, `revision()`, three bumps, `apply_event_inner` |
| `crates/giskard-server/src/thread_runtime.rs` | entry fields and doc comment; every method in D6; D7's two impls; D8's re-exports; the eleven `requests` map operations, five `captured_diffs`, seven `item_outputs`, nine `active_turn`, eight `task_revision`, four entry `lifecycle_revision` reads all go; tests as below |
| `crates/giskard-server/src/hub.rs` | D7: variant `:32-33`, arm `:304-307`, import `:12` |
| `crates/giskard-server/src/registry.rs` | D7: `:1088`, `:1097-1104`, import `:48-51` |
| `crates/giskard-server/src/registry/event_forwarder.rs` | `:4874` |
| `docs/design-straightening-review.md` | C5 heading (`:224`) gains `**Status: landed in S7**` with the three corrections in one sentence; row 7 (`:281`) gains ` — **landed in S7**`; the S5 status paragraph (`:201`) drops `Request` from "two more variants" |

## Tests

No test is deleted and no assertion changes. The only edits, all in `thread_runtime.rs` unless
noted:

| Lines | Today | After |
| --- | --- | --- |
| `:2344-2346` | locks the entry and asserts `entry.captured_diffs[&turn].contents.len() == 1` | `assert_eq!(runtime.captured_diff_records(&authority, turn).len(), 1);` — the existing public query; the two lock lines go |
| `:2349-2470` | two pure diff tests | moved verbatim to `thread_runtime/diffs.rs` `mod tests` (they need `super::*`, `giskard_core`, `ThreadId`, `TurnId`, `ItemId`) |
| `:2607-2612` | `state.current_by_slot.len() == 2`, `state.contents.len() == 2`, `drop(entry)` | `assert_eq!(lock_unpoison(&entry, ..).diffs.slot_count(turn), 2);` and `assert_eq!(runtime.captured_diff_records(&authority, turn).len(), 2);` |
| `:3549-3560` | `entry.active_turn...persistence_blocked...0 == turn` | `assert_eq!(entry.gate.blocked_turn(), Some(turn));` |
| `:3657, :3663, :3693, :3699` | `.item_outputs.contains_key(&(turn, id))` | `.outputs.contains(turn, id)` |
| `event_forwarder.rs:4874` | `Outbound::Request(responding.request_state)` | `Outbound::RuntimeEffects(responding.into())` |

Test counts after: `thread_runtime.rs` 39, `thread_runtime/diffs.rs` 2, `thread_runtime/tasks.rs`
17, `thread_runtime/live.rs` 14. No new tests are required: the 41 existing runtime tests drive every
component through the dispatch layer, and the two moved tests are the component-level diff
tests. Do not add tests that duplicate them.

## Order of work

Each step compiles and passes `cargo test -p giskard-server` on its own.

0. Record the baselines in **Exit checks**.
1. `git mv` `runtime_live.rs` and `runtime_tasks.rs` into `thread_runtime/`, the two `mod` lines
   from `lib.rs` into `thread_runtime.rs`, and `thread_runtime.rs:25-26` become
   `use live::LiveTurnState; use tasks::RunningTaskState;`. Build and test: a pure move.
2. D4 `thread_runtime/gate.rs`; entry field `gate`; the permit reads, `reserve_turn`, `has_active_turn`,
   `acknowledge_turn`, `release_turn`, settle's two arms, `runtime_summary`'s turn half, the
   `update_tool_output_authority` project-id read (`:1694-1697` → a `project_id` parameter, still
   in `thread_runtime.rs` for now); test `:3549-3560`.
3. D1 `thread_runtime/requests.rs`; entry field `requests: RequestLedger`; every site in D1; the D8
   re-export of `RequestResolution` and `RuntimeRequestId`.
4. D2 `thread_runtime/diffs.rs`; entry field `diffs`; `capture_event_diffs`, `captured_diff_records`,
   `captured_diff`, settle; move the two tests; tests `:2344-2346` and `:2607-2612`.
5. D3 `thread_runtime/outputs.rs`; entry field `outputs`; delete `impl ThreadRuntimeEntry`; the item
   branch of `apply_event_locked`, the four output queries, settle; test `:3657-3699`.
6. D5 in `thread_runtime/tasks.rs`; delete `task_revision` and its three bumps.
7. Rewrite the entry doc comment; read `apply_event_locked` and `settle_completed_turn` end to
   end against D6.
8. D7: `for_request`, `From`, the two registry sites, the hub variant and import,
   `event_forwarder.rs:4874`.
9. `cargo fmt --all`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   `cargo test --workspace`; then the review-doc status markers.

## Exit checks

Run from the repository root. "Before" is `main` at `7822b02`.

| Check | Before | After |
| --- | --- | --- |
| `rg -c "Outbound::Request\(" crates/giskard-server/src` | registry.rs 2, event_forwarder.rs 1 | no matches |
| `rg -n "Request\(RequestState\)" crates/giskard-server/src/hub.rs` | 1 | 0 |
| `rg -U -c '\.\s*(active_turn\|captured_diffs\|item_outputs\|task_revision)\b' crates/giskard-server/src/thread_runtime.rs` | 36 | 0 |
| `rg -U -c 'requests\s*\.\s*(get\|get_mut\|entry\|retain\|values\|iter)\(' crates/giskard-server/src/thread_runtime.rs` | 11 | 0 |
| `rg -P -c '\.lifecycle_revision\b(?!\()' crates/giskard-server/src/thread_runtime.rs` (field reads, not the gate's accessor call) | 5 (`:716, :733, :734, :979, :1181`) | 2: the `permit.lifecycle_revision` reads at today's `:734` and `:979` |
| `rg -c "fn (update_command_output_authority\|update_tool_output_authority\|update_prepared_item_output_authority\|register_request\|resolve_server_request_from_harness\|wire_request_state\|install_captured_diff\|reconcile_item_captured_diffs\|next_claim_id)" crates/giskard-server/src/thread_runtime.rs` | 9 | 0 |
| `rg -c "pub fn revision" crates/giskard-server/src/thread_runtime/tasks.rs` | (absent) | 1 |
| `rg -c "^mod runtime_" crates/giskard-server/src/lib.rs` | 2 | 0 |
| `rg -c "^mod (diffs\|gate\|live\|outputs\|requests\|tasks);" crates/giskard-server/src/thread_runtime.rs` | 0 | 6 |
| `ls crates/giskard-server/src/thread_runtime/` | (absent) | `diffs.rs gate.rs live.rs outputs.rs requests.rs tasks.rs` |
| `git diff --stat -M main -- crates/giskard-server/src/runtime_live.rs crates/giskard-server/src/thread_runtime/live.rs` | — | a rename with no content change |
| `rg -c "#\[test\]\|#\[tokio::test\]" crates/giskard-server/src/thread_runtime.rs crates/giskard-server/src/thread_runtime/diffs.rs` | 41, (absent) | 39, 2 |
| `awk '/^mod tests/{print NR; exit}' crates/giskard-server/src/thread_runtime.rs` (production lines) | 2106 | ≤ 1300 (guidance, not a gate) |
| `rg -n "thread_runtime::" crates/giskard-server/src crates/giskard-server/tests crates/giskard-testenv/src \| rg -v "^crates/giskard-server/src/thread_runtime.rs"` | 24 lines | the same 24 lines |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean | clean |
| `cargo test --workspace` | green | green, same test names plus none |
| `git diff --stat main -- crates/giskard-server/src/routes.rs crates/giskard-server/src/services.rs crates/giskard-server/src/registry/thread.rs crates/giskard-server/tests crates/giskard-testenv` | — | empty |

## Pitfalls

- **Multi-line field chains.** Seven of the `item_outputs` reads and two of the `requests` reads
  are `entry\n    .field\n    .op(..)`. Search with `rg -U` (multiline), not a single-line grep,
  when hunting leftovers; the exit checks are written that way.
- **`prune_resolved(None)` prunes.** `record.turn_id == completed_turn` compares two `Option`s;
  when settle runs for a non-`TurnCompleted` event, resolved records with `turn_id: None` are
  dropped today. Keep the `Option` parameter; do not "fix" it into `Some`-only.
- **Two release messages.** `settle_completed_turn` logs `committed persisted turn and released
  thread runtime`; `release_turn` logs `released active thread runtime`. That is why `TurnGate::
  release` returns the owner instead of logging.
- **Error strings stay put.** Three tests match on them (`:2818`, `:2966`, `:3393`) and they are
  the browser-visible protocol errors; keep them in `thread_runtime.rs` word for word and keep
  `settled` / rollback behaviour per rejection exactly as `:1976-2021` has it.
- **Kind check before mutation.** In `RequestLedger::commit`, check the claim, then the
  resolution kind, then mutate; today's code does not touch the record before both checks pass.
- **`PreparedItemOutput` visibility.** Only `command_descriptor`, `tool_descriptor`, `live_event`
  become `pub(crate)`; the forwarder treats the struct as opaque (`event_forwarder.rs:1310-1314`).
- **Unused imports after the moves.** `-D warnings` fails on any `use` left behind in
  `thread_runtime.rs` (`sha2`, `Item`, `command_status_is_running`, `CapturedDiffDescriptor`,
  `CapturedDiffRecord`, `DiffId`, `Instant`, ...) and on `RequestState` in `hub.rs`. Let the
  compiler list them; do not pre-empt with `allow`.
- **`cfg(test)` accessors are the only test-facing additions** (`slot_count`, `contains`,
  `blocked_turn`, `resolution`). Do not make component fields `pub` for the tests.
- **Do not fold `event_sequence` or `persisted_command_output_versions`** into a component: the
  first is the entry's own clock (RT1), the second has its own lifetime (S2 correction 2).
- **Overview refresh stays on the dispatch side.** Components never see `OverviewState`;
  `overview_refresh_needed` / `refresh_overview` calls stay exactly where they are.

## Stop rules

Stop and report instead of improvising if:

- a component method needs the authority, the permit, the overview, or the lock to express a
  behaviour that exists today (the design is wrong, not the code);
- a log line or error string cannot keep its text and fields after the move;
- preserving a behaviour requires editing a test assertion not listed under **Tests**;
- `Outbound::Request` cannot be removed without changing the order of `RequestState` and
  `ThreadRuntimeOverview` messages at either registry site;
- the `-D warnings` build wants an `allow` attribute anywhere.
