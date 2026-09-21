# M10 — Late item completion (durable amendments)

Implementation plan for milestone M10 of
[`thread-state-and-bootstrap-reconciliation-plan.md`](../thread-state-and-bootstrap-reconciliation-plan.md).
Written against `main` at `3deab1d` (M9 landed, spec 1.95); every file and line reference below was
checked against that tree. The implementation landed on `main` at `ea5a235`, where the spec had
reached 1.98, so this milestone is 1.99 and the line references are as of `3deab1d` — re-check them
before trusting one. The milestone text is the authority on scope and non-goals; this document says
how to land it and how to prove it landed. Nothing here depends on M9's code, and nothing here
changes the resync read it moved into `run_subscribe_bootstrap`.

## What lands

1. `PersistStore::amend_turn_item`: a settled item appended to its turn's payload file with one
   write to the file opened for append (preceded by a newline when the file's last line is torn),
   with no format bump and **nothing written to the index**. The payload is never rewritten.
2. Best-effort payload reads: a payload record that cannot be parsed, torn or not, is skipped with
   a warning and counted; the turn still loads. The count travels on the turn as
   `skipped_records`, and the transcript shows a warning row under such a turn.
3. The browser reconciles its own stale rows: it already knows which persisted turns had a task
   still running (`RunningTask::after_turn`), and on reconnect it asks the item endpoint about each
   one and upserts the settled item. Nothing in the index or the resync delta changes.
4. `apply_late` persists the normalized item before forgetting the runtime copy of its output,
   for commands and tools alike, and keeps the runtime copy when the write fails.
5. Spec, `docs/api-endpoints.md`, README, and tests for the persisted, lazy-route, reconnect,
   damaged-record and failure paths.

## Facts the plan rests on

- **The late path today.** `classify` returns `LateForPersistedTurn` for a turn the forwarder has
  persisted (`registry/event_forwarder.rs:789`), and `apply_late` (`:1396-1495`) runs
  `prepare_output` (normalization and descriptor preparation, `:1401-1407`), applies a terminal
  command completion to the runtime (`:1413-1418`), removes the command output it just applied with
  the "deferred durable command-output update" warning (`:1420-1428`), publishes the runtime
  effects and the transcript event to the hub (`:1440-1442`, `:1461-1470`), and for a tool item
  removes the tool output and warns "ignoring completed tool output for an already-persisted turn"
  (`:1474-1489`). Connected clients therefore see a late *command* completion live; nothing durable
  changes. A late *tool* completion gets neither the runtime apply nor the transcript publish:
  both are gated on `is_terminal_command_completion` (`:1409`, `:1449`), so the tool branch only
  drops its output. Connected clients learn of a settled late tool only by reloading, which this
  milestone does not change (see *Follow-ups*).
- **What a persisted turn holds.** Only items delivered by `ItemCompleted` reach the payload:
  `CurrentTurnItems` (`:299-348`) is fed by the `upsert` in the `ItemCompleted` arm (`:1811`) and
  drained into the `Turn` at persistence (`:2043-2053`, `items: self.turn.items.take()`). An
  item that has only had `ItemStarted` when the turn ends is therefore absent from the payload; a
  command whose `ItemCompleted` carried a running status (`is_terminal_command_completion`,
  `:245-256`, treats that as non-terminal) is present with that status. In the browser
  `detachRunningCommands` (`app.js:6057-6064`) keeps a running command row on screen after its
  turn completes, so a connected client still shows it; a reloading client renders the persisted
  turn, in which the row is either absent or shows the frozen running state.
- **Which turn a late completion belongs to.** The Codex mapper keys running commands by process
  id and resolves a completion to its turn through `native_turn_for_process`
  (`harness-codex/src/mapping.rs:245`, `:817`), so a completion arriving after `turn/completed`
  still names its turn. Late completions are a supported path, not an anomaly.
- **The payload format.** `turns/<turn_id>.jsonl` is a header line then `user_input`, `status`,
  `item` and `diff` records (`persist/src/history.rs:121-129`). An `item` record carries an
  explicit display `index` because "a command that settles late is necessarily appended at the end
  of the file" (`:115-120`), and the reader folds items by id: a later record for a known id
  replaces the item in its slot and keeps the slot's index when the record carries none
  (`:146-155`, `:596-603`). Unknown record kinds are skipped with a warning (`:627-634`). The file
  is written whole at commit with temp-file, fsync and rename (`:283`), and nothing appends to it
  afterwards today. The reader fails the whole turn on the first line that is not JSON
  (`:541-548`) or whose record does not deserialize (the `from_value` calls at `:551-608`), and
  `read_turn_payload` (`:753-778`) then quarantines the file to `<path>.corrupt-<ts>` and fails
  that one turn; a newer format header is `Invalid` and is not quarantined (`:756-766`). A missing
  `user_input` record is also `Corrupt` today. The store skips a turn whose payload failed and
  keeps the rest of the thread (`store.rs:455-480`).
- **The index format.** `history.jsonl` is a header then one `turn_v3` record per turn
  (`:106-112`, written at `:238`). `TurnRecord` (`:65-104`) carries `item_count` "as of this
  record" and its doc says "a superseding turn record carries the current count" (`:79-89`).
  `parse_history_index` currently skips a duplicate turn id with a warning, first-wins
  (`:466-475`); `append_turn_unlocked` relies on that for its retry case, where a second attempt
  re-appends an identical record (`store.rs:1286-1300`). The index is appended with one
  `write_all` to a file opened with `append(true)` (`store.rs:1331-1338`) and tolerates one torn
  final line (`:411-418`).
- **The resync read.** `load_turns_after` (`store.rs:1776-1826`) finds the cursor's position in the
  deduplicated record list and returns the records *after* it, then loads their payloads through
  `load_selected_turn_records` (`:1674`). It is untouched by this milestone.
- **What a reconnect is handed.** `run_subscribe_bootstrap` (`ws.rs:1309`) sends exactly
  `ThreadState`, one `HistoryDelta` (the suffix after the cursor, or a bounded reset page when the
  cursor will not resolve), then `LiveTurnSnapshot` and `RunningTasks`. There is **no retained
  event replay**: a completed turn's items reach a client only through the delta, the history page,
  or a request the client makes itself. `RunningTasks` is runtime state, so a task that settled
  while the client was away is simply absent from it.
- **What the browser already remembers.** `RunningTask` (`proto/src/lib.rs:277-298`) carries
  `turn_id`, `item_id`, `harness_item_id` and `after_turn` — the last set server-side when a turn
  completes with the task still running (`thread_runtime/tasks.rs:183-185`). The browser folds each
  snapshot into `state.runningTasks` keyed by `scopedItemKey(turn_id, item_id)` and keeps
  `afterTurn` on it (`app.js:6282-6320`), and `detachRunningCommands` (`:6244-6251`) marks the same
  flag on `state.runningCommands` when a turn ends. So "which persisted turn had a task still
  running, and which item it was" is already in the browser, named by the server.
- **The item endpoint.** `GET …/turns/{turn}/items/{item}` (M8, `routes.rs:4996`) answers
  state-tagged `started` / `completed`, runtime first and then the persisted payload.
  `turnItemUrl` and a worked fetch of it already exist in the browser
  (`app.js:9576`, `:9580-9615` for reasoning notes), including the project/thread/generation
  guards a late response has to pass before it may touch the DOM.
- **`item_count` has no validating reader.** Outside tests the only readers are unrelated live-turn
  log fields in the forwarder and the mismatch warning in `TurnRecord::into_turn`
  (`history.rs:246-257`), which the field's own doc already forbids validating against.
- **The write path.** `append_turn_unlocked` (`store.rs:1264-1340`) writes the payload with
  `atomic_write` (`atomic.rs:15`), then appends one index line with `OpenOptions::append`
  (`:1335`). The metadata service wraps the commit in `append_turn_with_diffs`
  (`thread_metadata.rs:90-105`) and publishes a mutation when the commit changed metadata.
- **Runtime output state.** `ItemOutputState` holds prepared outputs keyed by `(turn, item)`;
  `remove_command_output` and `remove_tool_output` drop them (`thread_runtime.rs:440-467`); the
  persisted command-output version cache is `persisted_command_output_versions` on
  `ThreadRuntimeEntry` (`:81`) with `version` and `cache` on its permit (`:136-160`) and no
  removal method today. The lazy routes read runtime first, then persistence
  (`routes.rs:4446-4547`, `:4663-4700`).
- **The browser.** A live `ItemCompleted` for a turn already rendered from history updates the row
  in place: `finalizeStreamedItem` finds the body through `renderedItemBody` (`app.js:7490-7497`)
  and `addItem` upserts a repeated item id (`:7531-7560`). A non-reset `HistoryDelta` renders each
  turn with `renderPersistedTurn` into a new container and inserts it (`:4053-4095`), then advances
  `newestPersistedTurnId` to the last turn in the delta (`:4095`); nothing checks whether a turn in
  the delta is already on screen. `renderPersistedTurn` (`:4275-4304`) renders the items then an
  `errorBubble` row for a failed or interrupted status (`:4298-4301`); `noticeBubble` (`:4446-4449`)
  is the existing transcript-anchored warning row, styled as `.msg.notice` in every appearance
  (`app.css:836-837`, `:1051`). `tests/ui.rs:915` asserts the opening lines of
  `renderPersistedTurn`, which the change below does not alter.
- **The turn types.** `Turn` (`core/src/turn.rs:160-180`) and `WireTurn`
  (`proto/src/wire.rs:831-864`) carry no integrity information; `WireTurn` is built by
  `From<Turn>`, which both the history page and `HistoryDelta` use. `Turn` struct literals exist in
  12 files (`rg -l "\bTurn \{" crates`), most of them tests.
- **Docs that reserve this case.** `specs/giskard-specification.md:176` ("post-persistence late
  completion remains ignored until the durable amendment milestone") and `:2334` (tool output);
  `docs/api-endpoints.md:129-131`; version line `:12` (1.95 on `main`, so this milestone is 1.96).
  The "complete or absent" wording lives at spec `:413-415`, `:2628-2629` ("written once when the
  turn commits"), `README.md:428-430` and the `history.rs:283` doc comment; the history route is
  described at `docs/api-endpoints.md:161-166`.
  The newest amendment blockquote is the cancellable-subscribe one at `:14` and the newest
  changelog block is 1.94 → 1.95 at `:202`; the new entries go above each. Tag series `LA*` is
  unused.
- **Tests.** `giskard-testenv`'s `FakeCore` exposes `append(thread, event)` and
  `complete_turn(thread, turn)` (`fake.rs:111`, `:115`), so a script can complete a turn and then
  append the command's `ItemCompleted`. `tests/e2e_smoke.rs:6208` shows a persisted-history
  fixture; `store.rs:3317` is the `load_turns_after` unit test to extend. The
  command-output-links route has HTTP coverage in `tests/code_overlay.rs`; the tool-output route
  only has substring coverage in `tests/ui.rs`, so G4 below is its first end-to-end test.

## Decisions

- **D1. Append to the payload in place.** Open the payload with `append(true)` (never create: a
  persisted turn must have one), and write the `item` line with one `write_all`, exactly as the
  index is appended. If the file is non-empty and its last byte is not `\n`, the buffer starts with
  a `\n` so the torn tail becomes its own line and the amendment its own; the torn tail is never
  truncated or repaired. No fsync, matching the index append. The payload is never rewritten, so
  the write costs the amendment's size whatever the turn holds.
- **D2. The amendment record carries no index.** It is written as `{"kind":"item","item":…}`.
  For an item already in the payload (a command persisted with a running status, or a
  re-amendment) the reader keeps the slot the first record established (`history.rs:146-155`).
  For an item absent from the payload, the common case since only `ItemCompleted` items are
  persisted, the reader assigns `items.len()`, the end of the turn, which is where something that
  settled last belongs. `PayloadLine::Item` gains `index: Option<usize>` with
  `skip_serializing_if` so existing lines are byte-identical.
- **D3. The index is not touched.** An amendment writes the payload and stops. No superseding turn
  record, so `parse_history_index` keeps folding turn records **first-wins** exactly as it does
  today — a repeated turn id is still only `append_turn_unlocked`'s retry case, and nothing about
  how today's duplicates resolve changes. `load_turns_after` is likewise untouched: it still
  returns the suffix strictly after the cursor.

  The alternative was a superseding record whose line position acts as an amendment clock, so the
  server could tell a reconnecting client which already-delivered turns changed. It was dropped
  because the clock pays for one behaviour only — a reconnecting client's stale row — and the
  browser can reach that behaviour on its own (D4) with no on-disk duplication, no change to how
  duplicate turn ids fold, and no line-position plumbing through `parse_history_index` and its four
  callers. Everything else the milestone delivers (the history page, both lazy output routes, the
  item endpoint, a live client's transcript event) needs nothing from the index.
- **D4. The browser reconciles the rows it knows are stale.** The browser already holds, per
  persisted turn, the item ids that were still running when the turn ended — the server names them
  with `RunningTask::after_turn`. Those pairs go in a watch set that is its own state, not a
  projection of a server snapshot, so it survives the socket drop. When a resync bootstrap's
  `RunningTasks` arrives without a watched pair, that task settled while the client was away: the
  browser asks the item endpoint, and on `completed` feeds the item through the ordinary upsert.
  A pair is dropped from the watch set as soon as its completion is observed, live or fetched.

  This reaches further than the clock it replaces: the watch set is keyed by item, not by the
  client's cursor, so a command that outlived two further turns reconciles like any other. It costs
  one request per watched pair on a reconnect, which is normally zero or one. What it gives up is
  that the server no longer *records* "this turn changed", so a non-browser client would have to
  keep the same watch set; that is accepted while the browser is the only client.
- **D5. Runtime copy outlives the write.** `apply_late` removes the runtime output only after the
  amendment is durable. On a write error it logs at `error!` with project, thread, turn, item,
  action `amend_turn_item` and the error, keeps the runtime copy, and continues; no retry.
- **D6. The version cache is dropped for the amended item.** Add `forget_persisted_command_output_version(turn, item)`
  on the runtime so the route re-hashes the amended output on its next request.
- **D7. `item_count` is allowed to go stale.** With no superseding record the index row keeps the
  count the commit wrote, and an amended turn's payload holds more items than that. The field's doc
  already forbids validating against it, and nothing outside tests does. The one consequence is
  `into_turn`'s mismatch warning, which would otherwise fire on every amended turn: it narrows to
  the direction that means loss — the payload holding **fewer** items than the record — and stays
  suppressed when `skipped_records` already explains the difference.
- **D8. Flat layout skips.** `amend_turn_item` returns `Ok(AmendOutcome::Unsupported)` for
  `ThreadLayout::Flat`; the forwarder logs at `warn!` and keeps today's behaviour for that thread.
- **D9. No new `ServerMessage`.** Live clients are served by the transcript event already
  published; reconnecting clients by the resync delta; reloading clients by the history page.
- **D10. `renderHistoryDelta` is unchanged.** With the index untouched, a non-reset delta still
  carries only turns strictly after the cursor, so it can never name a turn that is already on
  screen. The partition and the resume-cursor rule this plan previously called for are not needed;
  the reconciliation in D4 is the only new browser path, and it lands through `addItem`'s existing
  upsert.
- **D11. Best-effort payload reads.** `parse_turn_payload` no longer fails a turn on a bad line. A
  line that is not JSON, or a known record kind that does not deserialize, is logged at `warn!`
  with path, line and error, counted, and skipped; a torn final line is just the last such line.
  The turn loads from what remains. Two failures stay fatal because nothing can be shown without
  them: a `turn_header` newer than this build (`Invalid`, file left alone) and a missing
  `user_input` record (`Corrupt`, quarantined as today). Neither can be produced by an amendment:
  a commit writes both atomically before any amendment line exists.
- **D12. The skipped count reaches the browser on the turn.** `TurnPayload` gains
  `skipped_records: u32`; `Turn` and `WireTurn` gain the same field, `#[serde(default)]` and
  omitted when zero, so healthy turns are byte-identical on the wire and in the flat legacy
  format. The browser renders a `noticeBubble` row under a turn whose count is non-zero. A count,
  not the reasons: the reasons are in the log, and the wire field stays bounded.

## Changes

Order is the order that keeps the tree compiling and each step testable.

### A. `crates/giskard-persist/src/history.rs` (+90, tests +90)

1. `PayloadLine::Item { index: Option<usize>, item }` (`:126`); update the writer at `:307` to
   pass `Some(index)`; add `pub fn payload_item_line(item: &Item) -> Result<String, PersistError>`
   that serializes `PayloadLine::Item { index: None, item }` through `line_of` (`:274`).
1b. `parse_turn_payload` (`:516`): add `skipped_records: u32` to `TurnPayload` (`:167-178`).
   Replace the `?` on the line parse (`:541-548`) and on each record's `from_value` (`:551-608`)
   with a `warn!` (`path`, `line`, `kind` when known, `error`, `"skipping unreadable record in
   turn payload"`), `skipped_records += 1`, `continue`. The newer-format `Invalid` (`:565-571`)
   and the missing `user_input` `Corrupt` stay. `read_turn_payload` (`:753-778`) is unchanged:
   with D11 its quarantine arm is reached only for a payload with no usable `user_input`. Update
   the `:283` doc comment: a payload is written whole at commit and afterwards only extended by
   appended amendment records; a record that does not parse is skipped and counted. Rewrite the
   doc comment on `parse_turn_payload` (`:500-515`) to state the skip rule beside the fold rules.
1c. `TurnRecord::into_turn` (`:245-270`) copies `payload.skipped_records` onto the `Turn`, and its
   `item_count` warning (`:246-257`) narrows per D7: it fires only when the payload holds **fewer**
   items than the record, and only when `skipped_records == 0`. Note in the comment that more items
   than the record means amendments were appended, which is expected and not loss.
2. `parse_history_index` is **not** changed: turn records keep folding first-wins, and its
   doc comment's note that superseding records "arrive with the amendment work" is reworded to say
   that amendments write no turn record, so a repeated id remains the retry case.
3. Tests: a payload with an appended `item` record without `index` folds into the original slot;
   a payload whose last line is torn loads with `skipped_records == 1` and every earlier record;
   a bad interior line (not JSON, and separately a well-formed `item` object missing a field) is
   skipped and counted while the records after it load; a torn last line followed by an appended
   amendment (as the amend path writes it, `\n` first) yields the amended item and
   `skipped_records == 1`; a missing `user_input` is still `Corrupt`. The existing duplicate-id
   test (`store.rs`, `jsonl_history_skips_duplicate_turn_ids_on_read_and_recompute`) keeps passing
   unchanged, and `store.rs:3938`
   (`a_damaged_payload_fails_that_turn_alone_and_is_quarantined`) too: its damaged file has no
   `user_input`.

### B. `crates/giskard-persist/src/store.rs` (+70, tests +90)

1. `pub async fn amend_turn_item(&self, project, thread, turn, item: &Item) ->
   Result<AmendOutcome, PersistError>` with `enum AmendOutcome { Amended, Unsupported }`. Under the
   thread lock: `ensure_migrated`; `Unsupported` for `ThreadLayout::Flat`; then one
   `spawn_blocking` and nothing else — open `paths.turn_payload(turn)` with
   `OpenOptions::new().read(true).append(true)` and no `create` (absent → an `Io` error naming the
   missing payload, since a persisted turn must have one; `read` only so the last byte can be
   checked on the same handle, writes still go to the end); read the file's length and, when
   non-zero, its last byte; build the buffer as `payload_item_line(item)` prefixed with `\n` if
   that byte is not `\n`; one `write_all`, no fsync, matching the index append. Nothing reads the
   payload back and nothing touches `history.jsonl`, so the whole operation is one open, one small
   read and one write whatever the turn holds. Log at `debug!` with project, thread, turn, item,
   bytes appended and whether a newline was inserted.
2. `load_turns_after` (`:1776`) is **not** changed.
3. No `parse_history_index` caller changes.
4. Tests: `amend_turn_item` then `load_turn_item` returns the settled item with its original
   slot; `load_all_turns` shows the item settled; the payload is only ever extended (the bytes
   before the amendment are a prefix of the bytes after); `amend_turn_item` on a flat-layout thread
   returns `Unsupported` and writes nothing; `amend_turn_item` on a payload whose last line is torn
   inserts the newline first, leaves the torn tail byte-for-byte, and the turn then loads with the
   settled item and `skipped_records == 1`; `amend_turn_item` twice for two items appends two lines
   and both fold, while the index row's `item_count` stays at what the commit wrote and no warning
   is logged for it.

### C. `crates/giskard-core/src/turn.rs` (+8) and `crates/giskard-proto/src/wire.rs` (+6)

1. `Turn` (`turn.rs:160-180`): add `#[serde(default, skip_serializing_if = "is_zero")] pub
   skipped_records: u32` with a doc comment: records of the turn's payload file that could not be
   read and were skipped when the turn was reassembled; zero for a live turn and a healthy file; a
   bounded count, the reasons are in the server log. Add `fn is_zero(n: &u32) -> bool` beside it.
   Every `Turn` literal in the workspace gains `skipped_records: 0`
   (`rg -n "\bTurn \{" crates --type rust`; 12 files).
2. `WireTurn` (`wire.rs:831-843`): the same field and attribute; `From<Turn>` (`:845-864`)
   copies it. No other wire type changes and no `ServerMessage` variant is added.

### D. `crates/giskard-server/src/thread_metadata.rs` (+15)

`pub(crate) async fn amend_turn_item(&self, project_id, thread_id, turn, item) ->
Result<AmendOutcome, PersistError>` delegating to the store. No metadata mutation is published:
an item settling changes no aggregate the catalog shows.

### E. `crates/giskard-server/src/thread_runtime.rs` (+15)

`pub(crate) fn forget_persisted_command_output_version(&self, authority, turn, item)` on the
support-level API beside `remove_command_output` (`:454`), removing the `(turn, item)` key from
`persisted_command_output_versions`. No `ResolvedThreadRuntime` wrapper: the forwarder calls the
support API directly, as it does for `remove_command_output` (`event_forwarder.rs:1424`), and an
uncalled wrapper fails clippy's dead-code gate (exit check J).

### F. `crates/giskard-server/src/registry/event_forwarder.rs` (net about +40)

In `apply_late` (`:1396-1495`):

1. Command branch (`:1420-1428`): replace the removal-and-warn with: call
   `self.services.thread_metadata.amend_turn_item(project_id, thread_id, *turn, item).await`;
   on `Ok(Amended)` remove the runtime command output and forget its cached version, log at
   `info!` (`action = "amend_turn_item"`, kind `command`); on `Ok(Unsupported)` keep today's
   warning text; on `Err` log at `error!` with the error and keep the runtime copy.
2. Tool branch (`:1474-1489`): same shape; `remove_tool_output` only after `Amended`; the
   "ignoring completed tool output" warning survives only for `Unsupported`.
3. The `hub.publish` calls stay where they are; the transcript event still goes out.
4. Non-terminal late events (`log_ignored_seen_turn_running_task_start`) are unchanged.

### G. `crates/giskard-server/static/app.js` (+60) and `tests/ui.rs` (+25)

`renderHistoryDelta` is unchanged (D10).

**The watch set.** Add `state.lateItemWatch`, a `Map` from `scopedItemKey(turnId, itemId)` to
`{ turnId, itemId }`, cleared with the rest of the per-thread state (`:7886` and the reset path).
`renderRunningCommandSnapshot` (`:6282-6320`) already decodes `after_turn` into `afterTurn`; add
each task it sees with that flag set, so the set is populated by the server's own judgement of
which tasks outlived their turn. It is *not* rebuilt from each snapshot: it is the browser's own
memory and has to survive the socket drop that loses the snapshot.

**The reconciliation.** `renderRunningCommandSnapshot` already computes the `seen` key set. After
its existing sweep, hand the watch entries that `seen` does not contain to
`reconcileLateItems(missing)`: for each, `fetch(turnItemUrl(state.projectId, state.threadId,
turnId, itemId))` and, on `state === "completed"`, `addItem(body.item, turnId, true)` — the same
upsert a live completion lands through — then drop the entry. Guard the response exactly as
`fetchReasoningNote` (`:9580-9615`) does: project, thread and `state.activeViewGeneration` must
still match before anything touches the DOM. A non-OK response or a `started` body leaves the entry
in place, so the next snapshot tries again and a still-running task is simply not ready yet. Drop
an entry as soon as its completion is observed live, in `finishRunningCommand` (`:6252`), and keep
one request in flight per entry so a run of snapshots does not refire it. `ws.onopen` also resets
`state.runningTasksRevision`, beside the `runtimeOverviewRevision` it already resets: both number a
server-side snapshot sequence, and a restarted server numbering from zero would otherwise have its
first snapshot discarded — the one signal this reconciliation reads.

That makes the reconnect path: bootstrap's `RunningTasks` arrives without the task that settled
while the client was away → the entry is missing from `seen` → one item fetch → the row upserts.

`renderPersistedTurn` (`:4275-4304`): after the status row (`:4298-4301`), if
`turn.skipped_records > 0` call `noticeBubble` with "N record(s) of this turn could not be read
and were skipped; the turn may be incomplete." The row is stamped with the turn id like every
other row rendered inside the function. `tests/ui.rs` asserts the `noticeBubble` call inside
`renderPersistedTurn`, the watch-set population from `after_turn`, and the fetch-and-upsert in
`reconcileLateItems`; the existing assertion on `renderPersistedTurn`'s opening lines (`:915`) and
the existing `renderHistoryDelta` assertions all still hold unchanged.

### H. Tests: `crates/giskard-server/tests/late_item_completion.rs` (new, about 350 lines)

A script whose `start_turn` appends `TurnStarted`, an `ItemStarted` with a `CommandExecutionStart`,
then completes the turn (`core.complete_turn`) with the command still running; a `Gate` the test
releases to have the script append the command's `ItemCompleted` (full output, exit code) after
completion.

1. **Durable.** Subscribe, run the turn, wait for `TurnCompleted`, release the gate, wait for the
   late `ItemCompleted` on the socket; then `load_turn_item` shows the settled item, and the
   command-output route serves the late output with a fresh `ETag`.
2. **Reconnect.** Same, but drop the socket before releasing the gate; release; reconnect with
   `Subscribe { since: <that turn> }`. The delta is **empty** — the index did not move — and the
   bootstrap's `RunningTasks` no longer lists the task, which is the signal the browser reconciles
   on. Assert the server side of that path: the item endpoint for `(turn, item)` answers
   `completed` with the settled item, which is what the browser fetches, and asking again gives the
   same answer. The browser half is asserted in `tests/ui.rs` (G); this is the end-to-end proof
   that the data the browser will ask for is there and that nothing else is.
3. **Reload.** History page after the amendment shows the item settled.
4. **Tool result.** Same flow with a `ToolCallStart` and a late tool completion carrying JSON
   output: the tool-output route serves it from persistence after the runtime copy is gone.
5. **Write failure.** Make the payload path unwritable for the amendment (a directory in place of
   the file, or read-only permissions); the late completion still reaches the socket, the
   command-output route still serves the fresh output from the runtime, and the log carries
   `action = "amend_turn_item"` at `error!`.
6. **Flat layout.** A thread on `ThreadLayout::Flat` logs `Unsupported` and behaves as today.
7. **Damaged record surfaced.** Persist a turn, truncate its payload's last line on disk, then
   fetch the history page and separately subscribe with a reset bootstrap: the turn is present
   with its readable items and `skipped_records: 1` in both `WireTurn`s, and the item endpoint
   still serves the readable items. A healthy turn's JSON carries no `skipped_records` key.
8. **Amendment after a torn tail.** Truncate the payload's last line, release the gate so the
   late completion is amended: the turn loads with the settled item and `skipped_records: 1`, and
   the log shows the newline insertion at `debug!` and the skipped record at `warn!`.

### I. Documentation

- `specs/giskard-specification.md`: bump the version; amendment blockquote; changelog block
  `late item completion` with **LA1** (a terminal item completing after its turn persisted is
  appended to that turn's payload file as one record, with no format bump and no index write, so
  the index row's `item_count` may read lower than the payload holds and nothing may validate
  against it), **LA2** (the index and the resync delta are unchanged; a client that was shown a
  task still running on a persisted turn reconciles it itself, by asking the item endpoint for that
  item when a reconnect's running-task snapshot no longer lists it), **LA3** (runtime output
  survives until the amendment is durable; a failed write is
  logged and the runtime copy is kept), **LA4** (flat layout unsupported, logged), **LA5** (a
  payload is written whole at commit and afterwards only extended by appended records, one write
  each; a payload record that does not parse is skipped with a warning and counted, the turn still
  loads, and the count is delivered on the turn as `skipped_records` and shown as a warning row; a
  newer payload format or a missing `user_input` still fails that turn alone). Retire the
  reservations at `:176` and `:2334` with "(superseded by LA1)" prefixes in the C-series
  convention, and reword "complete or absent" at `:413-415` and "written once when the turn
  commits" at `:2628-2629` to the LA5 rule.
- `docs/api-endpoints.md:129-131`: replace the sentence with the new behaviour for both routes,
  and add to the history description (`:161-166`) that resync deltas may contain previously
  delivered turns whose items settled late, and that a turn carries `skipped_records` when part of
  its payload could not be read.
- `README.md`: one sentence beside the command-row description (`:137-139`): a command that
  finishes after its turn ended is recorded and shown settled after a reload. Reword the storage
  paragraph (`:428-430`): the payload is written atomically at commit and only ever extended by
  appended amendment lines; a line that cannot be read is skipped and the transcript says so.

### Not touched

`giskard-harness*`, `giskard-testenv`, the live buffer, the bootstrap sequence in `ws.rs`,
`ItemOutputState`'s shape, `routes.rs` handlers, the history and payload format numbers,
`atomic.rs`, the quarantine path in `read_turn_payload`, and `tests/e2e/`. In `giskard-core` and
`giskard-proto` only the `skipped_records` field and its zero-skip helper.

## Exit checks

| # | Check | Expected |
| --- | --- | --- |
| A | `rg -n "fn amend_turn_item" crates` | store, metadata service |
| B | `git diff main -- crates/giskard-persist/src/history.rs \| rg "parse_history_index\|first-wins\|IndexedTurnRecord"` | only the reworded doc comment; the folding rule and the return type are untouched |
| C | `rg -n "deferred durable command-output update\|ignoring completed tool output" crates/giskard-server/src` | only inside the `Unsupported` arms |
| D | `rg -n "index: Option<usize>" crates/giskard-persist/src/history.rs` | the write-side `PayloadLine::Item` and the read-side `PayloadItem` |
| E | `rg -n "forget_persisted_command_output_version" crates/giskard-server/src` | runtime definition and one forwarder call |
| F | `rg -n "HISTORY_FORMAT: u32 = 3\|TURN_PAYLOAD_FORMAT: u32 = 1" crates/giskard-persist/src/layout.rs` | both unchanged |
| G | `rg -c "ServerMessage" crates/giskard-proto/src/lib.rs` and the variant count | 11 variants, unchanged |
| H | `git diff --stat main -- crates/giskard-harness crates/giskard-harness-codex crates/giskard-harness-replay crates/giskard-testenv crates/giskard-server/src/ws.rs crates/giskard-server/src/routes.rs crates/giskard-server/src/thread_runtime/live.rs crates/giskard-persist/src/atomic.rs tests/e2e` | empty |
| H2 | `git diff main -- crates/giskard-core crates/giskard-proto \| rg "^[+-] " \| rg -v "skipped_records\|is_zero\|^[+-]\s*(//|///)"` | nothing but the field, its attribute and the helper |
| H3 | `rg -n "atomic_write\|\.create(true)\|read_turn_payload\|history()" crates/giskard-persist/src/store.rs` around `amend_turn_item` | none of them: the amend path opens one file, appends, and returns |
| H4 | `rg -n "skipped_records" crates static` | core, proto, history.rs (`TurnPayload`, parser, `into_turn`), app.js, tests |
| H5 | `rg -c "quarantining" crates/giskard-persist/src/history.rs` | 1, unchanged |
| I | `rg -n "\*\*LA[1-5]:\*\*\|superseded by LA1" specs/giskard-specification.md` | 7 lines |
| J | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| K | `cargo test --workspace --locked` | green, including `late_item_completion`, `history_sync`, `code_overlay`, `e2e_smoke::replayed_persisted_turn_events_are_not_duplicated` |
| H6 | `rg -n "load_turns_after" crates/giskard-persist/src/store.rs` and `git diff main -- crates/giskard-persist/src/store.rs \| rg "load_turns_after" -A 4` | the function is in the diff only if a comment changed; its selection logic is untouched |
| H7 | `rg -n "lateItemWatch\|reconcileLateItem" crates/giskard-server/static` | the watch set's declaration and reset, its population from `after_turn`, the fetch, and the live-completion drop |
| H8 | `rg -n "runningTasksRevision = -1" crates/giskard-server/static/app.js` | the per-thread reset **and** `ws.onopen`, beside `runtimeOverviewRevision` |
| H9 | `rg -n "invalidate_history_cache" crates/giskard-persist/src/store.rs` around `amend_turn_item` | the amendment drops the parsed-history cache |
| L | Test H2 run against `main` before the change | fails: the item endpoint 404s, because the settled item was never persisted |
| M | Test H7 run against `main` before the change | fails: the turn is missing from the page and its payload is quarantined |

## Signs the step has gone wrong

- The payload read back and rewritten whole by the amend path, or a torn tail truncated or
  "repaired" before appending: the amend path only ever appends.
- A payload reader that still fails a turn, or quarantines a file, over one unreadable record;
  or one that silently drops a record without the `warn!` and the count.
- `skipped_records` carrying anything but a count, or appearing in the JSON of a healthy turn.
- A history or payload format bump, or anything written to `history.jsonl` by the amend path: an
  amendment is a payload append and nothing else.
- `parse_history_index` folding anything but first-wins, or growing line positions: the reconnect
  is reconciled by the client, so the index needs no clock.
- `load_turns_after` changed at all, or a delta that can name a turn already on screen.
- The runtime output removed before the amendment is durable, or a retry loop around the write.
- A new `ServerMessage`, or the browser told about amendments any way other than the events, the
  deltas and the item endpoint it already uses.
- The watch set rebuilt from each running-task snapshot rather than kept as the browser's own
  memory: it has to outlive the socket drop that loses the snapshot.
- A watch entry dropped on a failed or `started` fetch, which would make one lost response
  permanent until a reload; or no in-flight guard on it, so every snapshot refires the same request.
- The amendment leaving the parsed-history cache in place: it was invalidated as a side effect of
  the index write the old draft did, and a payload-only append has to do it on purpose.
- The exception sentences in the spec and API docs left standing.

## Follow-ups (not this milestone)

- A late tool completion is persisted by this milestone but still not published to connected
  clients (`apply_late` gates both the runtime apply and the transcript publish on a command
  completion). A tool call that outlives its turn has no known producer today, so the live
  publish waits for one.

## Size

About +200 production lines (persist 110, core and proto 15, server 15, browser 60) and about
+550 test lines, plus docs. If `ws.rs` or `live.rs` appear in the diff, or `giskard-proto`
changes anything beyond the `skipped_records` field, M13 has crept in. If `parse_history_index` or
`load_turns_after` appear in the diff with anything but a comment change, the amendment clock has
crept back in.
