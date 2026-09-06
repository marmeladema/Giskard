# S8 — `classify` / `apply` in the event forwarder

Implementation plan for step 8 of [`design-straightening-review.md`](design-straightening-review.md)
(finding C2). Written against `main` at `58a841b` (S7 merged); every file and line reference
below was checked against that tree. Re-check them if the branch has moved.

## Goal

`ThreadEventForwarder::handle_event` (`crates/giskard-server/src/registry/event_forwarder.rs:1182-1816`,
635 lines) is a fixed pipeline: five gates that decide what an event is, then three effect paths
(late-for-persisted-turn, turnless, owned) that call the runtime, hub, store, driver, and
persistence. The decisions and the effects are interleaved, so every "which turn does this belong
to" question is answered by reading 600 lines with side effects.

After S8:

- `classify` is a free function of the forwarder's bookkeeping and the event, returns an
  `EventDisposition`, and touches nothing. It is unit-tested with a `ForwardedTurnState` and no
  runtime.
- `handle_event` is three lines: classify, remember, apply.
- `apply` dispatches on the disposition to one drop logger and three effect paths; each path is
  a method whose body is today's code, moved.
- The two identical "apply through the permit or the authority" blocks become one helper.

No behaviour change: no log line, log field, log level, error string, wire message, message
order, or test assertion changes. Every existing test passes unedited; `hub.rs` and every file
other than `event_forwarder.rs` and the review are untouched, except two `use`-list lines in
`thread_runtime.rs` and one import line in `registry.rs` (D4).

## Corrections to the review and the S5 follow-on

1. **Two gates keep registries, so the pure `classify` needs a `remember` beside it.** The
   duplicate-notice gate inserts into `seen_notices` (`should_skip_duplicate_notice`, `:10-18`)
   and the item-identity gate inserts a first-seen id into `item_ids_by_harness`
   (`track_item_identity`, `:58-74`). Both inserts happen for every non-foreign event, including
   events the cross-turn gate then drops (`:1193-1218` run before `:1220-1240`). S8 splits each
   gate into a read (used by `classify`) and a write (`ForwardedTurnState::remember`, run once for
   every non-foreign event before `apply`). The recorded set is identical: a foreign event
   records nothing today either; a duplicate notice and an identity conflict find their key
   present, so the write is a no-op both today and after.
2. **`first_for_turn: bool` is `attaches: Option<TurnId>`.** The attach path needs the turn id it
   attaches to (`:1239-1293`); a bool would make `apply` re-derive it.
3. **A sixth decision.** The review's enum has no room for the persisted-turn usage update that
   is dropped at `:1336-1343`. It is a `DropReason` variant.
4. **`log_drop` is one entry point, not one function body.** Two log-field tests call
   `log_foreign_thread_event_drop` and `log_cross_turn_event_drop` directly
   (`:2578-2590`, `:2656-2657`). They stay as the two field-heavy arms; `log_drop` owns the other
   three drop logs verbatim and dispatches to those two. The review counts 58 log statements and
   584 lines; today there are 57 production log macros and 635 lines.
5. **`Outbound::Transcript` keeps its fields; the S5 follow-on's fold is not this step.** The
   follow-on (`s5-hub-publish.md` "Follow-on") expected the runtime's apply to return the
   enriched transcript event so `Transcript` could become a field of `RuntimeEffects`. Reading the
   seven publish sites shows the transcript decision is forwarder policy, not runtime state: a
   turnless event reaches the transcript only when it is an `Error`, `Notice`, or
   `ServerRequestReceived` (`:1462-1528`), a persisted-turn event only when it is a terminal
   command completion (`:1401-1426`), a completion only after persistence succeeded
   (`:1778-1791`), and the `user_input` attached to owned events is the forwarder's
   `TurnContext` (`live_turn_user_input`, `registry.rs:125`), which the runtime never holds.
   Moving that into the runtime would move policy, not just a projection. S8 leaves `Outbound`
   and `hub.rs` untouched and makes the policy legible by putting each transcript publish at the
   end of its path. Whether the fold is still wanted is a question for after S8, with the policy
   visible; it is recorded under **What S8 does not do**.

## Non-goals

- No change to `Outbound`, `Hub`, `AppliedRuntimeEvent`, or any runtime method.
- No change to `run`, `admit_intent`, `handle_answer`, `finish`, `handle_stream_error`,
  `complete_forwarded_turn`, `persist_turn`, `persist_model_context_window`, or any free helper
  other than the three named in D1 (`should_skip_duplicate_notice` deleted, `track_item_identity`
  reimplemented on the new read/write pair with its signature and tests unchanged).
- No new log line and no removed one. The five drop logs keep their levels and fields.
- No new `Arc`, channel, trait, or task. `spawn_blocking` for output preparation stays where it
  is in the sequence.
- No edit to any existing test. New tests are the `classify` table only (D6).
- No change to `ForwarderExitReason`, `ForwarderControl`, `ForwardedTurnState`'s fields, or
  `CurrentTurnItems`.

## Ground truth

All lines in `crates/giskard-server/src/registry/event_forwarder.rs` unless stated. `mod tests`
starts at `:1959`; 44 tests; 57 production log macros (one is spelled `tracing::error!`, `:1316`).

| Fact | Where |
| --- | --- |
| Gate helpers: `should_skip_duplicate_notice` (inserts) `:10-18`; `event_item_identity` `:20-31`; `track_item_identity` (inserts on first sight, reports a conflict without inserting) `:58-74`; `log_foreign_thread_event_drop` `:113-129`; `log_cross_turn_event_drop` `:131-150` | read |
| Tests calling gate helpers directly: `log_cross_turn_event_drop` `:2578-2590`, `log_foreign_thread_event_drop` `:2656-2657`, `track_item_identity` `:2865`, `:2879`, `:2894-2896`. No test calls `should_skip_duplicate_notice`, `event_item_identity`, or `handle_event` | grep over `:1959-6353` |
| `ForwardedTurnState` `:605-620`: `owned_turn`, `seen_notices`, `item_ids_by_harness` are the gate inputs it holds; `new` `:622-639` needs only a `TurnContext`; `reset` `:641-655` | read |
| `ThreadEventForwarder` `:676-694`: `seen_turn_ids: HashSet<TurnId>` is the fourth gate input; `admitted: Option<AdmittedIntent>` is consumed by the attach path | read |
| `handle_event` `:1182-1816`, 26 log macros. Stages in order: foreign thread `:1187-1191`; duplicate notice `:1193-1201`; item identity `:1203-1217`; `event_turn` `:1219`; cross-turn drop `:1220-1238`; attach `:1239-1293` (`admitted.take()` `:1242`, store load + classification + `reserve_turn` `:1245-1281`, `acknowledge_turn` `:1282-1284`, state writes `:1285-1287`, debug `:1288-1292`); output preparation `:1295-1331` (`event_application_permit` `:1304`, `spawn_blocking` `:1309`, error log `:1315-1326`); late path `:1333-1447` (usage drop `:1336-1343`, terminal completion `:1345-1400`, transcript `:1401-1426`, tool cleanup `:1427-1445`); turnless path `:1449-1530`; diff capture `:1532-1535`; usage bookkeeping `:1537-1580`; per-kind bookkeeping `:1582-1669`; `completed` `:1671-1680`; live-buffer admission `:1686-1717`; non-completion apply + effects `:1719-1751`; completion `:1753-1804`; final transcript `:1806-1815` | read |
| The permit-or-authority apply block appears twice, identical except `append_live`: `:1352-1369` (`false`) and `:1720-1740` (`append_to_live_buffer`) | read |
| Production `Outbound::Transcript` publishes: 7 (`:1155` stream error, `:1418` late, `:1476`/`:1497`/`:1519` turnless, `:1786` completion, `:1808` owned). The three turnless publishes are byte-identical (`event.clone()`, `None`, `None`); only their preceding logs differ | grep |
| Production `Outbound::RuntimeEffects` publishes: 5 (`:1394`, `:1460`, `:1749`, `:1900`, `:1919`) | grep |
| `RuntimeAuthorityReplaced` in production: 6 lines (`:588` variant, `:601` label, exits at `:1273`, `:1305`, `:1362`, `:1731`) — all six survive; revision 2 corrects the first cut's claim that the two inside the duplicated apply block merge | grep |
| Helpers `handle_event` calls that live in `registry.rs` and reach it through `use super::*`: `TurnContext` `:87-92`, `TurnContextKind` `:95-100`, `turn_context_kind_label` `:102`, `turn_reservation` `:111`, `live_turn_user_input` `:125`, `subagent_activity_info` `:1682`, `subagent_start_info` `:1772`; in `registry/thread.rs`: `external_turn_input_label` `:243`, `ExternalTurnDefaults` `:71` | grep |
| `TokenUsage` is `Copy + PartialEq + Debug` (`giskard-core/src/token.rs:5`); `TurnStatus` is `Clone + PartialEq + Debug` (`turn.rs:135-136`); `TurnId`, `ItemId`, `ThreadId` derive `Debug, PartialEq` (ULIDs) | read |
| Review anchors: C2 paragraph `design-straightening-review.md:159-181`, C3 heads `:183`; sequencing row 8 `:289` | grep |

## Design

Every method below is today's code moved, with `return ForwarderControl::…` becoming the
method's return and `self.turn.…` reads unchanged. Log lines move with the code that emits them.

### D1. Gate reads and the write beside them

```rust
/// Read half of `should_skip_duplicate_notice` (:10-18): `contains` instead of `insert`.
fn is_duplicate_notice(seen_notices: &HashSet<(Option<TurnId>, String)>, event: &AgentEvent) -> bool;

/// Read half of `track_item_identity` (:58-74): the conflict tuple, or `None`; never inserts.
fn item_identity_conflict(
    item_ids_by_harness: &HashMap<HarnessItemKey, ItemId>,
    event: &AgentEvent,
) -> Option<(TurnId, String, ItemId, ItemId)>;

/// Unchanged signature and contract (:58-74), now `item_identity_conflict` followed by an
/// insert when there is no conflict. Its three test call sites (:2865, :2879, :2894) stay.
fn track_item_identity(item_ids_by_harness: &mut HashMap<HarnessItemKey, ItemId>, event: &AgentEvent)
    -> Option<(TurnId, String, ItemId, ItemId)>;

impl ForwardedTurnState {
    /// The two gate writes, in today's order (:1193 then :1203): a notice's `(turn, message)`
    /// into `seen_notices`; a first-seen native item id into `item_ids_by_harness`
    /// (`track_item_identity`, result discarded). Called once per non-foreign event.
    fn remember(&mut self, event: &AgentEvent);
}
```

`should_skip_duplicate_notice` is deleted. `event_item_identity`, `HarnessItemId`,
`HarnessItemKey` are unchanged.

### D2. `DropReason`, `EventDisposition`, `classify`

```rust
#[derive(Debug, PartialEq)]
enum DropReason {
    ForeignThread { event_thread: ThreadId },
    DuplicateNotice,
    ItemIdentityConflict { turn: TurnId, harness_item_id: String, existing: ItemId, conflicting: ItemId },
    CrossTurn { owned: TurnId, event_turn: TurnId },
    UsageForPersistedTurn { turn: TurnId },
}

#[derive(Debug, PartialEq)]
enum EventDisposition {
    Drop(DropReason),
    LateForPersistedTurn(TurnId),
    Turnless,
    Owned {
        /// `Some(turn)` when this event is the first for a turn nobody owns: `apply` attaches
        /// the forwarder to it (`:1239-1293`) before anything else.
        attaches: Option<TurnId>,
        /// The `TurnCompleted` triple (`:1671-1680`), when the event completes the owned turn.
        completes: Option<(TurnId, TokenUsage, TurnStatus)>,
    },
}

/// Pure. Reads `turn.owned_turn`, `turn.seen_notices`, `turn.item_ids_by_harness`,
/// `seen_turn_ids`, and the event; writes nothing.
fn classify(
    thread_id: ThreadId,
    turn: &ForwardedTurnState,
    seen_turn_ids: &HashSet<TurnId>,
    event: &AgentEvent,
) -> EventDisposition
```

`classify` evaluates, in today's order:

1. `event.thread_id() != thread_id` → `Drop(ForeignThread { event_thread })` (`:1187-1191`).
2. `is_duplicate_notice` → `Drop(DuplicateNotice)` (`:1193-1201`).
3. `item_identity_conflict` → `Drop(ItemIdentityConflict { .. })` (`:1203-1217`).
4. With `event_turn = event.turn()`:
   - `(Some(owned), Some(t))` where `t != owned && !seen_turn_ids.contains(&t)` →
     `Drop(CrossTurn { owned, event_turn: t })` (`:1220-1238`);
   - `(_, Some(t))` where `seen_turn_ids.contains(&t)` → `Drop(UsageForPersistedTurn { turn: t })`
     for `TurnUsageUpdated` (`:1336-1343`), else `LateForPersistedTurn(t)` (`:1333-1335`);
   - `(None, None)` → `Turnless` (`:1449`);
   - `(None, Some(t))` → `Owned { attaches: Some(t), completes }` (`:1239-1241`);
   - `(Some(_), _)` → `Owned { attaches: None, completes }` (the fall-through into `:1532`).

   `completes` is `Some((turn, usage, status.clone()))` for `TurnCompleted`, else `None`
   (`:1671-1680`). It is computed from the raw event; preparation and diff capture never touch a
   `TurnCompleted`, so this equals today's value.

Ordering note: today the persisted-turn check (`:1333`) runs after attach and preparation. It
reads only `seen_turn_ids` and the event, which neither stage changes, so evaluating it inside
`classify` is the same decision. The cross-turn arm is checked before the persisted arm, as
today (`:1220-1238` precede `:1333`); the combination `t == owned && seen_turn_ids.contains(&t)`
cannot occur (a turn enters `seen_turn_ids` only in `complete_forwarded_turn` `:1889`, which is
followed by `reset` `:1802`), and both today and after it would take the late path.

### D3. `handle_event`, `apply`, `log_drop`

```rust
async fn handle_event(&mut self, event: AgentEvent) -> ForwarderControl {
    let disposition = classify(self.thread_id(), &self.turn, &self.seen_turn_ids, &event);
    if !matches!(disposition, EventDisposition::Drop(DropReason::ForeignThread { .. })) {
        self.turn.remember(&event);
    }
    self.apply(event, disposition).await
}

async fn apply(&mut self, event: AgentEvent, disposition: EventDisposition) -> ForwarderControl {
    match disposition {
        EventDisposition::Drop(reason) => {
            self.log_drop(reason, &event);
            ForwarderControl::Continue
        }
        EventDisposition::LateForPersistedTurn(_) => self.apply_late(event).await,
        EventDisposition::Turnless => self.apply_turnless(event).await,
        EventDisposition::Owned { attaches, completes } => {
            self.apply_owned(event, attaches, completes).await
        }
    }
}

/// The five drop logs, verbatim. Two keep their free functions because tests call them.
fn log_drop(&self, reason: DropReason, event: &AgentEvent) {
    match reason {
        ForeignThread { event_thread } => log_foreign_thread_event_drop(project_id, thread_id, event_thread, event),
        DuplicateNotice => debug!(.. "skipping duplicate harness notice"),                       // :1194-1199
        ItemIdentityConflict { .. } => error!(.. "dropping harness event because a native item id remapped to a different Giskard item id"), // :1206-1215
        CrossTurn { owned, event_turn } => log_cross_turn_event_drop(project_id, thread_id, owned, event_turn, event, self.forwarder_started.elapsed().as_millis()),
        UsageForPersistedTurn { turn } => debug!(.. "ignoring usage update for an already-persisted turn"), // :1337-1342
    }
}
```

The `DuplicateNotice` log keeps `event_turn_id = display_opt(event.turn())`; the
`ItemIdentityConflict` log keeps `turn_id`, `event_kind`, `harness_item_id`, `existing_item_id`,
`conflicting_item_id`; the `UsageForPersistedTurn` log keeps `%turn`. Same levels.

### D4. The shared effect helpers

```rust
/// The one addressable-output preparation step (:1295-1331): permit, `spawn_blocking`, the
/// `addressable item-output event preparation task failed` log with its fields unchanged
/// (including the field literally named `self.turn.observed_turn`, :1319).
struct PreparedEvent {
    event: AgentEvent,
    output: Option<PreparedItemOutput>,
    permit: Option<RestorePermit>,
}
async fn prepare_output(
    &mut self,
    event: AgentEvent,
) -> Result<PreparedEvent, ForwarderExitReason>;
// Err(RuntimeAuthorityReplaced) from :1305, Err(EventPreparationFailed) from :1326.
// `&mut self`, not `&self` (revision 2): see the note below.

/// The permit-or-authority apply (:1352-1369 and :1720-1740, now once).
/// `None` means the permit no longer names the current runtime entry; each of the two callers
/// turns it into `Exit(RuntimeAuthorityReplaced)` itself (revision 2: see exit check J).
fn apply_to_runtime(
    &self,
    permit: Option<&RestorePermit>,
    event: &AgentEvent,
    append_live: bool,
    output: Option<PreparedItemOutput>,
) -> Option<AppliedRuntimeEvent>;
```

**`PreparedItemOutput` must become nameable from the forwarder** (revision 1; the first cut
claimed it already was). Today nothing outside `thread_runtime.rs` names the type: the forwarder
receives it through let-binding inference at `:1303-1331` and passes it straight back. S7's
re-export list (`thread_runtime.rs:39-41`) carries `RuntimeCommandOutputLookup`,
`RuntimeToolOutputLookup`, `command_output_version` only; the type reaches `thread_runtime.rs`
through a private `use` (`:47`), and `registry.rs:48-51` does not import it. A struct field and a
parameter cannot be inferred, so D4 needs two one-line edits (and one they force):

- `thread_runtime.rs:39-41`: add `PreparedItemOutput` to the `pub(crate) use outputs::{..}` list.
  This is a correction of S7's D8 list, not a widening of the runtime's surface: three
  `pub(crate)` methods already name the type in their signatures (`prepare_item_output`
  `:366-369`, `apply_prepared_event` `:730-735`, `apply_prepared_event_if_current` `:752-757`),
  so it was crate-facing before S8 and merely unnameable.
- `registry.rs:48-51`: add `PreparedItemOutput` to the `use crate::thread_runtime::{..}` list;
  `event_forwarder.rs` sees it through `use super::*` (`:1`). `RestorePermit` is already there.

No other line in either file changes except one forced by the first: `thread_runtime.rs:47`
imports the type privately (`use outputs::{ItemOutputState, PreparedItemOutput,
prepare_item_output};`), which the new `pub(crate)` re-export makes a duplicate definition
(E0252), so `PreparedItemOutput` drops out of that list. Three `use`-list lines in total, all
naming only this type; exit check N pins them.

**`prepare_output` takes `&mut self`, not `&self`** (revision 2). `ThreadEventForwarder` is not
`Sync` — `InflightRequest.request` is a `BoxFuture<'static, ..>`, which is `Send` but not `Sync`
— and an `async fn` stores every argument in its generator, so one holding `&self` can never be
`Send`. `driver.rs:621` boxes the forwarder's future as `dyn Future + Send`, so `&self` here
fails to compile. `&mut self` is `Send` whenever the referent is; the call site in `apply_late` /
`apply_owned` holds no other borrow. Nothing else about D4 changes: the method still touches only
`self.services.runtime`, `self.authority`, `self.binding`, and the event.

### D5. The three paths

```rust
/// :1333-1447 minus the usage drop (now `classify`). Order: `prepare_output`; if terminal
/// command completion: `terminating_command_before_terminal_completion`, `apply_to_runtime`
/// (`append_live: false`), `remove_command_output` + its warn, `log_command_completion_after_terminate`,
/// the `applied late terminal event` debug, publish `RuntimeEffects`; else
/// `log_ignored_seen_turn_running_task_start`; then the transcript block (:1401-1426) with
/// `late_command_output`; then the tool cleanup (:1427-1445). Returns `Continue`, or `Exit` from
/// preparation / a stale permit. It takes no turn id (revision 2): once the usage drop moves to
/// `classify` nothing in this body reads one — every other `turn` here is a binding shadowed out
/// of an `ItemCompleted` pattern — so `apply` discards `LateForPersistedTurn`'s payload. The
/// variant keeps it: it is what `classify` decided, and D6 asserts on it.
async fn apply_late(&mut self, event: AgentEvent) -> ForwarderControl;

/// :1449-1530. `apply_event`, the `applied turnless agent event` debug, publish
/// `RuntimeEffects`; then the per-kind logs for `Error` / `Notice` / `ServerRequestReceived`
/// (:1463-1474, :1484-1494, :1505-1517, verbatim); then one transcript publish for exactly those
/// three kinds (the three publishes at :1476, :1497, :1519 are identical). Returns `Continue`.
async fn apply_turnless(&mut self, event: AgentEvent) -> ForwarderControl;

/// The owned path. Order is today's:
///   1. if `attaches` is `Some(turn)`: `attach_to_turn(turn, &event)?`            (:1239-1293)
///   2. `prepare_output(event)?`                                                  (:1295-1331)
///   3. `event = runtime.capture_event_diffs(&self.authority, event)`             (:1532-1535)
///   4. `record_turn_usage(&event).await`                                          (:1537-1580)
///   5. `note_owned_event(&event).await`                                           (:1582-1669)
///   6. `append_live = admit_to_live_buffer(event_turn, &event)`                   (:1686-1717)
///   7. if `completes.is_none()`: `apply_to_runtime(.., append_live, ..)`, the
///      `applied agent event to thread runtime` debug, publish `RuntimeEffects`  (:1719-1751)
///   8. if `completes` is `Some`: `finish_owned_turn(event, ..)`                   (:1753-1804)
///      else publish `Transcript { event, user_input: live_turn_user_input(&self.turn.context), command_output: None }` (:1806-1815)
async fn apply_owned(
    &mut self,
    event: AgentEvent,
    attaches: Option<TurnId>,
    completes: Option<(TurnId, TokenUsage, TurnStatus)>,
) -> ForwarderControl;

/// :1242-1292: take the admitted intent or build the external context (store load,
/// classification, `reserve_turn` with its `event owner could not reserve an external native
/// turn` error → `Err(RuntimeAuthorityReplaced)`), `acknowledge_turn` + overview publish, the
/// three state writes, the `attached to turn before seeing turn start` debug.
async fn attach_to_turn(&mut self, turn: TurnId, event: &AgentEvent) -> Result<(), ForwarderExitReason>;

/// :1537-1580: live usage, live context window, model adoption, `persist_model_context_window`.
async fn record_turn_usage(&mut self, event: &AgentEvent);

/// :1582-1669, the review's "third small function": `TurnStarted` bookkeeping, driver links for
/// `ItemStarted` / `ItemCompleted`, the compaction marker, `items.upsert`, `DiffUpdated`.
async fn note_owned_event(&mut self, event: &AgentEvent);

/// :1686-1717: `ensure_live_turn`, the `replacing a stale live buffer` / `not buffering an event
/// for a different turn` branches. Returns today's `append_to_live_buffer`.
fn admit_to_live_buffer(&self, event_turn: Option<TurnId>, event: &AgentEvent) -> bool;

/// :1753-1804: the two `turn completion` logs, `complete_forwarded_turn` (`None` →
/// `Exit(PersistenceBlocked)`), the completion transcript publish (no `user_input`), the
/// `monitoring after-turn running commands` info, `self.turn.reset`.
async fn finish_owned_turn(
    &mut self,
    event: AgentEvent,
    completed_turn: TurnId,
    usage: TokenUsage,
    status: TurnStatus,
) -> ForwarderControl;
```

`event_turn` in step 6 is `event.turn()` of the event as classified; preparation and diff
capture do not change a turn id.

### D6. Tests for `classify`

Eight `#[test]` functions in the existing `mod tests`, one per disposition, each built from
`ForwardedTurnState::new(TurnContext { user_input: UserInput::text(""), model: TurnModel::Unknown,
mode: TurnMode::Unknown, kind: TurnContextKind::User })`, an empty or seeded `HashSet<TurnId>`,
and one event. They assert with `assert_eq!` on the derived `PartialEq`:

| Test | State | Event | Expected |
| --- | --- | --- | --- |
| `classify_drops_foreign_thread_events` | any | `Notice` for another `ThreadId` | `Drop(ForeignThread { .. })` |
| `classify_drops_a_repeated_notice_after_remember` | `remember` the notice first | the same `Notice` | `Drop(DuplicateNotice)`; before `remember` it is not a drop |
| `classify_drops_a_remapped_native_item_id` | `remember` an `ItemStarted` with `harness_item_id: "cmd_1"` | `ItemCompleted` with `"cmd_1"` and a different `ItemId` | `Drop(ItemIdentityConflict { .. })` |
| `classify_drops_events_for_another_unpersisted_turn` | `owned_turn = Some(a)` | `Notice` for turn `b` | `Drop(CrossTurn { owned: a, event_turn: b })` |
| `classify_routes_persisted_turn_events_to_the_late_path` | `owned_turn = Some(a)`, `seen = {b}` | `ItemCompleted` for `b` | `LateForPersistedTurn(b)` |
| `classify_drops_usage_updates_for_persisted_turns` | `seen = {b}` | `TurnUsageUpdated` for `b` | `Drop(UsageForPersistedTurn { turn: b })` |
| `classify_marks_turnless_events` | `owned_turn = None` | `Notice { turn: None }` | `Turnless` |
| `classify_attaches_the_first_event_of_a_new_turn_and_completes_the_owned_one` | `owned_turn = None`, then `Some(a)` | `TurnStarted` for `a`; then `TurnCompleted` for `a` | `Owned { attaches: Some(a), completes: None }`; then `Owned { attaches: None, completes: Some((a, usage, status)) }` |

Nothing else: the 44 existing tests already drive every effect path through the real forwarder.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/registry/event_forwarder.rs` | D1 (`:10-18` deleted, `:58-74` reimplemented, `remember` added to `impl ForwardedTurnState` after `reset` `:655`); D2 types and `classify` placed directly above `impl ThreadEventForwarder` `:696`; D3 replaces `:1182-1816`; D4 and D5 methods in the same `impl` block, in pipeline order; D6 tests appended before the end of `mod tests` |
| `crates/giskard-server/src/thread_runtime.rs` | `:39-41`: `PreparedItemOutput` joins the `pub(crate) use outputs::{..}` re-export; `:47`: it leaves the private `use outputs::{..}`, which the re-export makes a duplicate definition (D4) |
| `crates/giskard-server/src/registry.rs` | `:48-51`: `PreparedItemOutput` joins the `use crate::thread_runtime::{..}` import (D4) |
| `docs/design-straightening-review.md` | a `**Status: landed in S8**` paragraph after the C2 paragraph (`:181`, before C3 at `:183`) naming corrections 1–5 in one sentence each; row 8 (`:289`) gains ` — **landed in S8**` |

`hub.rs`, the `thread_runtime/` modules, `services.rs`, `registry/driver.rs`,
`registry/thread.rs`, the integration tests, and `giskard-testenv` are not touched.

## Order of work

Each step compiles and passes `cargo test -p giskard-server` on its own.

0. Record the baselines in **Exit checks**.
1. D4: `apply_to_runtime` replaces the two blocks (`:1352-1369`, `:1720-1740`);
   `prepare_output` replaces `:1295-1331`.
2. D5, bottom up: `finish_owned_turn` (`:1753-1804`), `admit_to_live_buffer` (`:1686-1717`),
   `note_owned_event` (`:1582-1669`), `record_turn_usage` (`:1537-1580`), `attach_to_turn`
   (`:1242-1292`). `handle_event` still holds the gates and calls them in place.
3. D5: `apply_late` from `:1333-1447` (leave the usage drop in it for now), `apply_turnless`
   from `:1449-1530`, `apply_owned` from the remainder; `handle_event` is now gates + three
   calls.
4. D1 and D2: the read/write split, `remember`, `DropReason`, `EventDisposition`, `classify`,
   `log_drop`; move the usage drop out of `apply_late`; D3's `handle_event` and `apply`.
5. D6 tests.
6. `cargo fmt --all`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   `cargo test --workspace`; the review-doc markers.

## Exit checks

Run from the repository root. "Before" is `main` at `58a841b`. The commands, so that no
pipe needs escaping inside the table:

```sh
F=crates/giskard-server/src/registry/event_forwarder.rs
T=$(awk '/^mod tests/{print NR; exit}' $F)                    # first test line
prod() { awk -v t="$T" 'NR<t' $F | rg -c "$1"; }              # count in production lines only
A: awk '/async fn handle_event/{s=NR} s&&/^    }$/{print NR-s+1; exit}' $F
B: rg -c 'fn (classify|log_drop|remember|apply|apply_late|apply_turnless|apply_owned|apply_to_runtime|prepare_output|attach_to_turn|record_turn_usage|note_owned_event|admit_to_live_buffer|finish_owned_turn)\(|enum (EventDisposition|DropReason)' $F
C: rg -c 'fn should_skip_duplicate_notice' $F
D: rg -c 'fn (track_item_identity|log_foreign_thread_event_drop|log_cross_turn_event_drop)' $F
E: prod '(debug|info|warn|error)!\('
F1..F5: prod '<message>' for each of: "dropping harness event for a different thread",
        "skipping duplicate harness notice", "native item id remapped to a different Giskard item id",
        "dropping harness event for a different turn on the same thread",
        "ignoring usage update for an already-persisted turn"
G: prod apply_prepared_event_if_current
H: prod 'Outbound::Transcript'
I: prod 'Outbound::RuntimeEffects'
J: prod RuntimeAuthorityReplaced
K: rg -c '#\[test\]|#\[tokio::test\]' $F
L: git diff --stat main -- crates/giskard-server/src/hub.rs crates/giskard-server/src/thread_runtime crates/giskard-server/src/registry/driver.rs crates/giskard-server/src/registry/thread.rs crates/giskard-server/tests crates/giskard-testenv
N: git diff main -- crates/giskard-server/src/thread_runtime.rs crates/giskard-server/src/registry.rs | rg '^[-+][^-+]'
M: git diff main -- $F | rg '^-.*#\[(test|tokio::test)\]'
```

| Check | Before | After |
| --- | --- | --- |
| A, lines of `handle_event` | 635 | ≤ 12 |
| B, the new items | 0 | 16 (14 functions, 2 enums, each on its own line) |
| C | 1 | 0 |
| D, the three kept helpers | 3 | 3 |
| E, production log macros | 57 | 57 |
| F1..F5, each drop message | 1 each | 1 each |
| G | 2 | 1 |
| H | 7 | 5 |
| I | 5 | 5 |
| J | 6 | 6 (revision 2, was 5) |
| K, tests in the file | 44 | 52 |
| L | — | empty |
| N, the D4 `use` lines | — | only `use`-list lines naming `PreparedItemOutput`: the two lists gain it, and `thread_runtime.rs:47`'s private import loses it (revision 2; rustfmt may rewrap a list: still only those lists) |
| M, removed test attributes | — | no output |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`; `cargo test --workspace` | clean, green | clean, green |

## Pitfalls

- **`remember` runs for every non-foreign event, dropped or not.** That is today's behaviour
  (`:1193-1218` precede the cross-turn gate). Running it only for admitted events would change
  which log line a repeated cross-turn notice or a re-keyed cross-turn item produces.
- **Do not run `remember` for foreign events.** A foreign notice recorded into `seen_notices`
  could later drop a same-thread notice with the same `(None, message)` key.
- **The usage drop moves to `classify`; nothing else about the late path moves.** In
  particular `prepare_output` still precedes the late path's first runtime call, because the
  permit it takes (`:1304`) is what `apply_to_runtime` checks.
- **`attach_to_turn` precedes `prepare_output`**, as `:1239-1293` precede `:1295-1331`. A
  reservation failure must exit before any `spawn_blocking`.
- **The transcript publish is the last effect on every path**, after the runtime effects, and on
  the completion path only after `complete_forwarded_turn` returned `Some`. The owned
  non-completion transcript carries `live_turn_user_input(&self.turn.context)`; the completion
  transcript carries `None`; the late transcript carries `late_command_output(item)`.
- **Keep the odd log field.** The preparation-failure log names a field
  `self.turn.observed_turn` (`:1319`). It is a field key, not an assignment; renaming it is a log
  change and out of scope.
- **`completes` is cloned in `classify`.** `TurnStatus` is cloned once there instead of once at
  `:1678`; same allocation count.
- **`is_terminal_command_completion` is evaluated twice today** (`:1345`, `:1401`). Computing it
  once in `apply_late` is fine; do not reorder the two blocks it guards.
- **No `#[allow]`.** If a helper's parameter list trips `too_many_arguments`, pass `&PreparedEvent`
  or split, as the code already does for `ThreadEventForwarder::new`.
- **Do not touch `handle_stream_error`'s synthesized completion** (`:1143-1167`). It builds its
  own `TurnCompleted` and publishes its own transcript; it is not an event from the stream.

## What S8 does not do

- It does not fold `Transcript` into `RuntimeEffects` (correction 5). After S8 the transcript
  policy is four `hub.publish(.., Outbound::Transcript { .. })` sites, one at the tail of each
  path plus the stream-error one. If the fold is still wanted, it needs its own plan that answers
  where the `user_input` and the "which turnless kinds reach the transcript" rule live.
- It does not move `classify` into its own file. It is ~60 lines and reads two private structs
  of this file; a submodule would need to export them.
- It does not touch the 26 log lines' wording, and it does not add the per-disposition
  `debug!` the review might be read to imply: `log_drop` is the existing five drop logs behind
  one door.

## Stop rules

Stop and report instead of improvising if:

- a decision in `classify` needs the store, the coordinator, the runtime, or `self.admitted`
  (the design is wrong: that input is an effect's, not a gate's);
- preserving a log line's fields needs a value only an effect produces (then the log belongs
  to `apply`, not `log_drop`, and the plan must say so);
- an existing test needs an edit;
- any exit-check count lands elsewhere than the table says and the reason is not a plain
  miscount in this document.
