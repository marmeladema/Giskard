# M10 — Late item completion (durable amendments)

Implementation plan for milestone M10 of
[`thread-state-and-bootstrap-reconciliation-plan.md`](../thread-state-and-bootstrap-reconciliation-plan.md).
Written against `main` at `3deab1d` (M9 landed, spec 1.95). Every file and line reference below
was checked against that tree; re-check them if the branch has moved. The milestone text is the
authority on scope and non-goals; this document says how to land it and how to prove it landed.
Nothing here depends on M9's code: the resync read it changes is still called from the bootstrap,
now inside `run_subscribe_bootstrap`.

## What lands

1. `PersistStore::amend_turn_item`: a settled item appended to its turn's payload file and a
   superseding turn record appended to the index, payload first, index last, with no format bump.
2. The index folds turn records last-wins, and the position of a turn's winning record becomes the
   amendment clock that `load_turns_after` uses, so a resync delta carries amended turns.
3. `apply_late` persists the normalized item before forgetting the runtime copy of its output,
   for commands and tools alike, and keeps the runtime copy when the write fails.
4. The browser refreshes a turn it already rendered when a resync delta names it again.
5. Spec, `docs/api-endpoints.md`, README, and tests for the persisted, lazy-route, reconnect and
   failure paths.

## Facts the plan rests on

- **The late path today.** `classify` returns `LateForPersistedTurn` for a turn the forwarder has
  persisted (`registry/event_forwarder.rs:789`), and `apply_late` (`:1396-1495`) runs
  `prepare_output` (normalization and descriptor preparation, `:1401-1407`), applies a terminal
  command completion to the runtime (`:1413-1418`), removes the command output it just applied with
  the "deferred durable command-output update" warning (`:1420-1428`), publishes the runtime
  effects and the transcript event to the hub (`:1440-1442`, `:1461-1470`), and for a tool item
  removes the tool output and warns "ignoring completed tool output for an already-persisted turn"
  (`:1474-1489`). Connected clients therefore see the completion live; nothing durable changes.
- **Which turn a late completion belongs to.** The Codex mapper keys running commands by process
  id and resolves a completion to its turn through `native_turn_for_process`
  (`harness-codex/src/mapping.rs:245`, `:817`), so a completion arriving after `turn/completed`
  still names its turn. Late completions are a supported path, not an anomaly.
- **The payload format.** `turns/<turn_id>.jsonl` is a header line then `user_input`, `status`,
  `item` and `diff` records (`persist/src/history.rs:121-129`). An `item` record carries an
  explicit display `index` because "a command that settles late is necessarily appended at the end
  of the file" (`:115-120`), and the reader folds items by id: a later record for a known id
  replaces the item in its slot and keeps the slot's index when the record carries none
  (`:146-155`, `:596-603`). Unknown record kinds are skipped with a warning (`:632`). The file is
  written whole with temp-file, fsync and rename so it is "complete or absent" (`:283`); the reader
  has no torn-final-line tolerance for payloads (only the index has it, `:411-418`).
- **The index format.** `history.jsonl` is a header then one `turn_v3` record per turn
  (`:106-112`, written at `:238`). `TurnRecord` (`:65-104`) carries `item_count` "as of this
  record" and its doc says "a superseding turn record carries the current count" (`:79-89`).
  `parse_history_index` currently skips a duplicate turn id with a warning, first-wins
  (`:466-475`); `append_turn_unlocked` relies on that for its retry case, where a second attempt
  re-appends an identical record (`store.rs:1286-1300`). The index is appended with `O_APPEND` and
  tolerates one torn final line (`:411-418`).
- **The resync read.** `load_turns_after` (`store.rs:1776-1826`) finds the cursor's position in the
  deduplicated record list and returns the records after it, then loads their payloads through
  `load_selected_turn_records` (`:1674`). It is position-based, so with last-wins folding the
  clock this plan needs is available without a new field.
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
  the delta is already on screen.
- **Docs that reserve this case.** `specs/giskard-specification.md:176` ("post-persistence late
  completion remains ignored until the durable amendment milestone") and `:2334` (tool output);
  `docs/api-endpoints.md:129-131`; version line `:12` (1.95 on `main`, so this milestone is 1.96).
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

- **D1. Append to the payload by atomic rewrite.** Read the payload bytes, append one `item` line,
  `atomic_write` the result. The payload reader tolerates no torn line, and a late completion is
  rare, so the cost of rewriting one turn's payload is accepted over adding torn-line recovery.
- **D2. The amendment record carries no index.** It is written as `{"kind":"item","item":…}` so
  the reader keeps the slot the first record established (`history.rs:146-155`). `PayloadLine::Item`
  gains `index: Option<usize>` with `skip_serializing_if` so existing lines are byte-identical.
- **D3. Index folds last-wins.** `parse_history_index` keeps the last record for a turn id and
  records the line position of that winning record. The retry case still works: an identical
  re-appended record wins with identical content. A downgraded build keeps the first record, which
  only leaves `item_count` stale; the `TurnRecord` doc already forbids validating against it.
- **D4. The winning line is the clock.** `load_turns_after(cursor)` returns, in turn order, every
  turn whose first record is after the cursor's first record, plus every turn whose winning record
  is after the cursor's first record. A client can receive an amended turn twice; it cannot miss
  one that landed after it last saw the cursor turn.
- **D5. Runtime copy outlives the write.** `apply_late` removes the runtime output only after the
  amendment is durable. On a write error it logs at `error!` with project, thread, turn, item,
  action `amend_turn_item` and the error, keeps the runtime copy, and continues; no retry.
- **D6. The version cache is dropped for the amended item.** Add `forget_persisted_command_output_version(turn, item)`
  on the runtime so the route re-hashes the amended output on its next request.
- **D7. Superseding record content.** Same fields as the original record with `item_count` set to
  the folded item count and `completed_at` unchanged; the turn's status is not altered by an item
  settling. No new field on `TurnRecord`.
- **D8. Flat layout skips.** `amend_turn_item` returns `Ok(AmendOutcome::Unsupported)` for
  `ThreadLayout::Flat`; the forwarder logs at `warn!` and keeps today's behaviour for that thread.
- **D9. No new `ServerMessage`.** Live clients are served by the transcript event already
  published; reconnecting clients by the resync delta; reloading clients by the history page.
- **D10. Browser upsert of a rendered turn.** In `renderHistoryDelta`'s non-reset branch, a turn
  whose id already has rows in the transcript is handled by calling `addItem(item, turn.id, true)`
  for each of its items, which is the existing upsert, and is excluded from the container that is
  inserted; `newestPersistedTurnId` advances only to the newest turn id in the delta by turn order,
  which the server guarantees is last.

## Changes

Order is the order that keeps the tree compiling and each step testable.

### A. `crates/giskard-persist/src/history.rs` (+60, tests +60)

1. `PayloadLine::Item { index: Option<usize>, item }` (`:126`); update the writer at `:307` to
   pass `Some(index)`; add `pub fn payload_item_line(item: &Item) -> Result<String, PersistError>`
   that serializes `PayloadLine::Item { index: None, item }` through `line_of` (`:274`).
2. `parse_history_index` (`:400-490`): replace the `seen` set with a `HashMap<TurnId, usize>` from
   turn id to the record's position in `records`; on a duplicate, overwrite `records[pos]` and log
   at `debug!` ("superseding turn record") instead of `warn!`. Return, beside the records, the line
   index of each turn's first and winning record: change the return type to
   `Vec<IndexedTurnRecord { record, first_line, winning_line }>` or add a sibling
   `parse_history_index_positions` used by the store; pick the former and update the three callers
   (`rg -n "parse_history_index\(" crates/giskard-persist/src`).
3. Tests: a payload with an appended `item` record without `index` folds into the original slot;
   an index with a superseding record folds last-wins and reports its winning line; the existing
   duplicate-id test flips from "skipped" to "superseded"; a downgraded reader is not tested here
   (documented behaviour only).

### B. `crates/giskard-persist/src/store.rs` (+120, tests +100)

1. `pub async fn amend_turn_item(&self, project, thread, turn, item: &Item) ->
   Result<AmendOutcome, PersistError>` with `enum AmendOutcome { Amended, Unsupported }`. Under the
   thread lock: `ensure_migrated`; `Unsupported` for `ThreadLayout::Flat`; read
   `paths.turn_payload(turn)` (absent → an error naming the missing payload, since a persisted
   turn must have one); append `payload_item_line(item)`; `atomic_write`; then build the superseding
   `TurnRecord` from the winning record with the folded `item_count` and append it to the index
   through the same `O_APPEND` write `append_turn_unlocked` uses (`:1335`), including its parent
   fsync. Log at `debug!` with project, thread, turn, item, payload bytes before and after.
2. `load_turns_after` (`:1776`): use the positions from A2. `cursor_first` is the cursor's first
   line; select records with `first_line > cursor_first || winning_line > cursor_first`, keep
   them in record order, and load as today. Update the `debug!` line with `amended_records`.
3. `load_turn_records_unlocked` and the other `parse_history_index` callers adopt the new return
   shape; `load_turn_item` (M8) is unaffected beyond that.
4. Tests: `amend_turn_item` then `load_turn_item` returns the settled item with its original
   slot; `load_all_turns` shows the item settled and the count right; `load_turns_after` with a
   cursor older than the amendment returns the amended turn, with a cursor newer than it does not,
   and never returns the cursor turn itself; `amend_turn_item` on a flat-layout thread returns
   `Unsupported` and writes nothing; a superseding record survives a torn final line on the next
   append (existing index tolerance).

### C. `crates/giskard-server/src/thread_metadata.rs` (+15)

`pub(crate) async fn amend_turn_item(&self, project_id, thread_id, turn, item) ->
Result<AmendOutcome, PersistError>` delegating to the store. No metadata mutation is published:
an item settling changes no aggregate the catalog shows.

### D. `crates/giskard-server/src/thread_runtime.rs` (+15)

`pub(crate) fn forget_persisted_command_output_version(&self, authority, turn, item)` removing the
`(turn, item)` key from `persisted_command_output_versions`, with a `ResolvedThreadRuntime`
wrapper beside `persisted_command_output_version_permit` (`:297`).

### E. `crates/giskard-server/src/registry/event_forwarder.rs` (net about +40)

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

### F. `crates/giskard-server/static/app.js` (+25) and `tests/ui.rs` (+10)

`renderHistoryDelta` non-reset branch (`:4053-4095`): before rendering, partition `turns` into
those with an existing row (`document.querySelector('.msg[data-turn="<id>"]')`, the selector shape
the code uses at `:4331`) and those without. For existing turns call `addItem(item, turn.id, true)`
per item and skip `renderPersistedTurn`. Advance `newestPersistedTurnId` to the last turn of the
delta as today; the server orders the delta by turn order. `tests/ui.rs` gains assertions for the
partition and the upsert call.

### G. Tests: `crates/giskard-server/tests/late_item_completion.rs` (new, about 300 lines)

A script whose `start_turn` appends `TurnStarted`, an `ItemStarted` with a `CommandExecutionStart`,
then completes the turn (`core.complete_turn`) with the command still running; a `Gate` the test
releases to have the script append the command's `ItemCompleted` (full output, exit code) after
completion.

1. **Durable.** Subscribe, run the turn, wait for `TurnCompleted`, release the gate, wait for the
   late `ItemCompleted` on the socket; then `load_turn_item` shows the settled item, and the
   command-output route serves the late output with a fresh `ETag`.
2. **Reconnect.** Same, but drop the socket before releasing the gate; release; reconnect with
   `Subscribe { since: <that turn> }`; the delta contains the amended turn with the settled item and
   nothing else; a second reconnect with the same cursor returns it again.
3. **Reload.** History page after the amendment shows the item settled.
4. **Tool result.** Same flow with a `ToolCallStart` and a late tool completion carrying JSON
   output: the tool-output route serves it from persistence after the runtime copy is gone.
5. **Write failure.** Make the payload path unwritable for the amendment (a directory in place of
   the file, or read-only permissions); the late completion still reaches the socket, the
   command-output route still serves the fresh output from the runtime, and the log carries
   `action = "amend_turn_item"` at `error!`.
6. **Flat layout.** A thread on `ThreadLayout::Flat` logs `Unsupported` and behaves as today.

### H. Documentation

- `specs/giskard-specification.md`: bump the version; amendment blockquote; changelog block
  `late item completion` with **LA1** (a terminal item completing after its turn persisted is
  appended to the payload and supersedes the turn record, payload first, index last, no format
  bump), **LA2** (the index folds turn records last-wins; a resync delta includes turns amended
  after the client's cursor turn, ordered by turn order; a client may see an amendment twice, never
  miss one), **LA3** (runtime output survives until the amendment is durable; a failed write is
  logged and the runtime copy is kept), **LA4** (flat layout unsupported, logged). Retire the
  reservations at `:176` and `:2334` with "(superseded by LA1)" prefixes in the C-series
  convention.
- `docs/api-endpoints.md:129-131`: replace the sentence with the new behaviour for both routes,
  and add to the history description that resync deltas may contain previously delivered turns
  whose items settled late.
- `README.md`: one sentence beside the command-row description (`:137-139`): a command that
  finishes after its turn ended is recorded and shown settled after a reload.

### Not touched

`giskard-proto`, `giskard-core`, `giskard-harness*`, `giskard-testenv`, the live buffer, the
bootstrap sequence in `ws.rs`, `ItemOutputState`'s shape, `routes.rs` handlers, the history and
payload format numbers, and `tests/e2e/`.

## Exit checks

| # | Check | Expected |
| --- | --- | --- |
| A | `rg -n "fn amend_turn_item" crates` | store, metadata service |
| B | `rg -n "skipping duplicate turn id" crates/giskard-persist/src` | nothing (replaced by the superseding log) |
| C | `rg -n "deferred durable command-output update\|ignoring completed tool output" crates/giskard-server/src` | only inside the `Unsupported` arms |
| D | `rg -n "index: Option<usize>" crates/giskard-persist/src/history.rs` | the write-side `PayloadLine::Item` and the read-side `PayloadItem` |
| E | `rg -n "forget_persisted_command_output_version" crates/giskard-server/src` | runtime definition, wrapper, one forwarder call |
| F | `rg -n "HISTORY_FORMAT: u32 = 3\|TURN_PAYLOAD_FORMAT: u32 = 1" crates/giskard-persist/src/layout.rs` | both unchanged |
| G | `rg -c "ServerMessage" crates/giskard-proto/src/lib.rs` and the variant count | 11 variants, unchanged |
| H | `git diff --stat main -- crates/giskard-proto crates/giskard-core crates/giskard-harness crates/giskard-harness-codex crates/giskard-harness-replay crates/giskard-testenv crates/giskard-server/src/ws.rs crates/giskard-server/src/routes.rs crates/giskard-server/src/thread_runtime/live.rs tests/e2e` | empty |
| I | `rg -n "\*\*LA[1-4]:\*\*\|superseded by LA1" specs/giskard-specification.md` | 6 lines |
| J | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| K | `cargo test --workspace --locked` | green, including `late_item_completion`, `history_sync`, `code_overlay`, `e2e_smoke::replayed_persisted_turn_events_are_not_duplicated` |
| L | Test G2 run against `main` before the change | fails: the delta is empty and the item still reads running |

## Signs the step has gone wrong

- A payload written with `OpenOptions::append`: the payload reader cannot recover a torn line.
- A history or payload format bump, or a new `TurnRecord` field used as the clock: the line
  position is the clock and the format admits the amendment as it is.
- The runtime output removed before the amendment is durable, or a retry loop around the write.
- A new `ServerMessage`, or the browser told about amendments any way other than the events and
  deltas it already handles.
- `load_turns_after` returning the cursor turn itself, or returning amended turns out of order,
  which would regress `newestPersistedTurnId`.
- The exception sentences in the spec and API docs left standing.

## Size

About +250 production lines (persist 180, server 55, browser 25) and about +500 test lines, plus
docs. If `ws.rs`, `live.rs` or `giskard-proto` appear in the diff, M13 has crept in.
