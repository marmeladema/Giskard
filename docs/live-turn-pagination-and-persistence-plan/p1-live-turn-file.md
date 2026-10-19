# P1 — The live turn file

Implementation plan for milestone P1 of
[`live-turn-pagination-and-persistence-plan.md`](../live-turn-pagination-and-persistence-plan.md).
Written against `main` at `1787a2c` (L1 and L2 merged, spec 1.101); every file and line reference
below was checked against that tree. Re-check them before trusting one. Where this document
refines the overall plan — one file per thread instead of a `live/` directory, the commit folded
into the store's existing append path, the writer registry — this document wins and the overall
plan's P1 section has been updated to match.

P1 is a **durable-format change** and the only milestone of the plan that is. It gets its own
review, as `docs/thread-state-and-bootstrap-reconciliation-plan.md` asked of amendments: a new file
on disk, a new record kind, two new header fields, and a recovery path that writes history.

## What lands

1. While a turn is live, its payload accumulates in `threads/<thread_id>/live.jsonl`, in the
   **payload format**: the header and prompt written atomically when the forwarder attaches to the
   turn, then one appended `item` record per completion and one `usage` record per usage update.
   From the first event on, a crash can no longer lose the prompt or any completed item.
2. Committing the turn **finishes the file instead of writing it**: the status and diff records are
   appended, the file is fsynced and renamed to `turns/<turn_id>.jsonl`, and the index record is
   appended as today. Payload first, index last, unchanged.
3. **Recovery.** A `live.jsonl` that no running writer owns is a turn the process died inside. On the
   thread's first store access it is committed as an `Interrupted` turn with the items it holds,
   the last usage it recorded, credited to the model the turn ran on, and a completion time taken
   from its last item, never from the clock.
4. Best effort, never worse than today: any failure to write the live file disables it for that
   turn, and the commit falls back to the whole-file write that exists now.
5. `giskard-admin validate` reports an unrecovered live file; `recover-live-turns` recovers them
   under the data-directory lock. Spec 1.102, `README.md`, `AGENTS.md`, and tests at every layer.
6. One rule for the fields the file now duplicates: once a turn has an index record, that record
   is the sole authority for `model`, `mode`, `usage`, `started_at` and `completed_at`; what the
   live file carries of them is a recovery input, read only when there is no index record (D10).

## Why one file and not a directory

The overall plan sketched `live/<turn_id>.jsonl`. One file per thread is simpler and loses nothing:

- **A thread has at most one live turn.** The turn gate admits one owner per thread
  (`thread_runtime/gate.rs`), and the forwarder is the single event owner of its thread. There is
  nothing for a second live file to hold.
- **The file names its own turn.** The first record of every payload is `{"kind":"turn_header",
  "format":1,"turn_id":"…"}` (`history.rs:108-117`, `PayloadLine::TurnHeader`;
  `PayloadTurnHeader` at `:146-150`). Recovery reads the turn id from the file, not from its name,
  and commit knows it from the forwarder.
- **Cheaper checks.** Recovery and validation `stat` one fixed path instead of listing a directory.
  `sweep-orphan-payloads` reads `turns/` only (`store.rs:2219-2244`) and `history_validation_errors`
  walks index records, so neither sees the file; thread enumeration matches `<ulid>/` and
  `<ulid>.json` under `threads/`, so a file inside the thread directory is never mistaken for a
  thread. Deletion renames the whole thread directory to `.deleting` (`store.rs:2087`), taking the
  live file with it.
- **The invariant is visible on disk.** `turns/` holds committed payloads and nothing else;
  `live.jsonl` either exists, meaning a turn was in flight the last time this directory was written
  to, or it does not.

It stays JSONL in the payload format, not a rewritten `live.json`: a whole-file rewrite per item is
quadratic in the turn, it would need a second format, and it would forfeit the one property the
milestone exists to deliver, that finishing a turn is a rename.

## Facts the plan rests on

- **The payload format.** `payload_file_bytes_with_diffs` (`history.rs:318-348`) writes
  `turn_header`, `user_input`, `status`, one `item` per item with its display `index`, one `diff`
  per diff with its index; every line through `line_of` (`:301`). `payload_item_line` (`:351`)
  writes an `item` record without an index for amendments. `TURN_PAYLOAD_FORMAT` is 1
  (`layout.rs:7`).
- **The parser folds by rule.** `parse_turn_payload` (`history.rs:570-760`): `turn_header`,
  `user_input` and `status` are last-wins singletons with a warning on repetition (`:639-670`);
  `item` folds by `ItemId`, an indexless record keeping the slot its first record established
  (`:671-686`); `diff` folds by path (`:687-702`); an unknown `kind` is skipped with a warning
  (`:703-710`); a record that does not parse is counted in `skipped_records`; a format newer than
  the build fails the turn (`:653-658`); a missing `user_input` fails the turn (`:747`).
  `PayloadTurnHeader` (`:146-150`) has no `deny_unknown_fields`, and its `turn_id` is already
  `#[allow(dead_code)]`. `TurnPayload.status` is optional (`:184-186`): a payload without a status
  record reads, and `TurnRecord::into_turn` falls back to the index's capped copy (`:291`).
- **The commit.** `append_turn_unlocked` (`store.rs:1300-1389`): on the directory layout, if
  `turns/<turn_id>.jsonl` already exists the payload write is skipped with a `warn` and only the
  index record is appended (`:1320-1341`, the retry case); otherwise the payload is written whole
  with `atomic_write` (`:1343-1348`). The header line is written when the index is new (`:1351`),
  the record is appended with one `write_all` on an append handle (`:1363-1374`), and the parsed
  cache is updated (`:1377-1385`). `append_turn_with_diffs_and_update_aggregates`
  (`:1525-1562`) takes the thread lock, calls it, then folds usage into `thread.json` with
  `ThreadRecency::RecordActivity`. `ThreadRecency::RestoreActivity(DateTime)` exists for repair
  (`:145-154`). `atomic_write` (`atomic.rs:15-44`) is temp file, `sync_all`, rename.
- **The amendment.** `amend_turn_item` (`store.rs:1432-1512`) opens the payload for append, adds a
  newline if the last byte is not one, writes the record in one `write_all`, no fsync, and
  invalidates the parsed cache. That is the append discipline the live file reuses.
- **Paths and locks.** `ThreadPaths` (`layout.rs`) gives `dir()`, `history()`, `turns_dir()`,
  `turn_payload(turn)`; `thread_paths` resolves the layout (`store.rs:677`) and
  `current_thread_paths` assumes the directory layout (`:686`). `thread_lock` (`:1113`) interns a
  per-thread `Mutex`; it is not reentrant, which is why `ensure_migrated` (`:699-715`) runs before
  the lock at the top of every public entry point and keeps an `unmigratable` set so a failed
  migration is attempted once per process. `PersistStore` (`:536-570`) carries its keyed state with
  `ENTITY-AUTHORITY-EXCEPTION` comments.
- **What the forwarder knows and when.** `attach_to_turn` sets `self.turn.context` (a `TurnContext
  { user_input, model, mode, kind }`, `registry.rs:99-104`), the lease, and `owned_turn`
  (`event_forwarder.rs:1783-1785`); `TurnStarted` sets `observed_turn` and `started_at`
  (`:1852-1854`); `record_turn_usage` keeps `live_usage` (`:1808`); `note_owned_event` upserts
  each completion into `CurrentTurnItems` (`:1903`), whose `items` vector is display order and
  whose `indexes` map gives an item's position (`:304-345`); diffs fold by path into
  `self.turn.diffs` (`:1917-1922`). The order in `apply_owned` is attach, `prepare_output`
  (normalizes command output), `capture_event_diffs`, `record_turn_usage`, `note_owned_event`,
  live buffer, runtime, publish (`:1663-1697`). `complete_forwarded_turn` (`:2110-2220`) builds the
  `Turn` from these and calls `persist_turn` (`:445`), which calls
  `ThreadMetadataService::append_turn_with_diffs` (`thread_metadata.rs:91-104`). On a stream end
  with a turn in flight, `handle_stream_error` (`:1209-1290`) synthesizes an `Interrupted`
  completion through the same path: a clean stop already persists the turn, so P1's target is the
  unclean one. A compaction turn's prompt is `/compact` (`registry.rs:1321`); a discovered native
  turn's is synthesized; `live_turn_user_input` (`:137-148`) is only the sub-agent seed and is not
  involved here.
- **Where each field of a committed turn comes from.** `TurnRecord::into_turn`
  (`history.rs:266-296`) takes `model`, `mode`, `usage`, `started_at` and `completed_at` from the
  index record and `user_input`, `items`, `diffs` and `status` from the payload; the payload has no
  say in the first five today. `recompute_aggregates` (`store.rs:2002-2035`) rebuilds a thread's
  token ledger from index records alone. `history_validation_errors` (`:2371-2422`) already parses
  every payload the index references, so a per-turn comparison costs it nothing new.
- **The model can change mid-turn, the usage cannot be read from the last update.** A turn's
  `TurnContext.model` is `Unknown` when the harness did not acknowledge a model at start.
  `record_turn_usage` (`event_forwarder.rs:1797-1830`) adopts the model a `TurnUsageUpdated` names
  (`AgentEvent::TurnUsageUpdated { usage, context_window: Option<u32>, model: Option<ModelRef> }`,
  `giskard-core/src/event.rs:34-42`) when the context has none and the event also carries a
  window (`:1812-1827`); a known context model that disagrees with the event is an `error` and no
  adoption. `complete_forwarded_turn` then persists `ctx.model` (`:2139`), so the index record
  carries the adopted model, and the `usage` it persists is the `TurnCompleted` event's (`:796-805`,
  `:2142`), not the last `TurnUsageUpdated`'s. The forwarder's `started_at` is reset with the turn
  state (`:644`) and re-stamped when `TurnStarted` is noted (`:1854`); attach runs before that in
  `apply_owned` (`:1663-1693`).
- **Admin and lock.** `giskard-admin` is a `match` on the command name
  (`giskard-persist/src/bin/giskard_admin.rs`); mutating commands call `lock_data_dir` (`:345`),
  read-only ones and `--dry-run` call `warn_if_locked` (`:361`); `sweep-orphan-payloads`
  (`:248-299`) is the model for a per-thread run that reports and never aborts the whole run; the
  usage text is at `:455-487` and the header comment at `:1-18`. `DataDirLock::try_acquire` and
  `is_held` (`lock.rs:55`, `:87`).
- **Validation.** `validate_all` → `history_validation_errors` (`store.rs:2313-2400`) reports per
  turn, never quarantines.
- **Tests and harness.** Store tests live in `mod layout_tests` with `make_store`, `item`,
  `turn_with_items`, `running_command_item`, `settled_command_item`, `write_format1_thread`
  (`store.rs:3935-5060`). `TestServer::builder(...).data_dir(path).seed(...)`
  (`giskard-testenv/src/server.rs:148-163`) starts a server on a prepared directory, which is how
  a crash is simulated: write the files a crash would leave, then start. A persistence failure is
  injected by putting a directory where a file must be (`tests/late_item_completion.rs:617-630`),
  and log lines are asserted with `#[traced_test]` and `logs_assert`.
- **Documents to keep true.** Spec §5.2 layout (`specs/giskard-specification.md:2623-2660`), §5.4
  "Turn history" bullet (`:2792`), "Format identification" (`:2831`), "Deletion" (`:2845`), §5.5
  (`:2866-2900`); the 1.101 changelog at `:268` and amendment at `:14`. `AGENTS.md:125-132` ("Two
  files are appended to… Nothing else appends") and `:146` ("A turn commits payload first, index
  last"). `README.md:485-500` layout block and the admin table around `:559-566`.

## Decisions

- **D1. Location and format.** `threads/<thread_id>/live.jsonl`, payload format 1, same record
  kinds as a committed payload plus one. Flat-layout threads (`ThreadLayout::Flat`) get no live
  file; the writer reports `Unsupported` and logs at `debug`, as LA4 does for amendments.

- **D2. Birth.** When the forwarder attaches to a turn (`attach_to_turn`, after `owned_turn` is
  set), the store writes `live.jsonl` with `atomic_write`: the `turn_header` and the `user_input`
  record. The header gains three optional fields, written only here and read only by recovery:

  ```jsonc
  {"kind":"turn_header","format":1,"turn_id":"01K…",
   "model":{"provider":"openai","model":"gpt-5.5"},   // TurnModel, as the index record stores it
   "mode":"build",                                     // TurnMode
   "started_at":"2026-10-19T09:14:02Z"}
  ```

  They are what the index record needs and the payload does not otherwise carry. `PayloadLine::
  TurnHeader` gets them as `Option`s with `skip_serializing_if`, so the whole-file writer's output
  is byte-identical to today; `PayloadTurnHeader` gets them with `#[serde(default)]`. After the
  rename they sit in the committed file, redundant with the index and ignored by every reader.
  Format stays 1: a 1.101 build reads such a file unchanged.

- **D3. Extension.** One `item` record per `ItemCompleted`, with `index` = the item's position in
  `CurrentTurnItems` after the upsert (so a repeat completion carries the same index and folds into
  its slot). One `usage` record per `TurnUsageUpdated`:

  ```jsonc
  {"kind":"usage","usage":{"input":1200,"output":80,"total":1280},
   "model":{"provider":"openai","model":"gpt-5.5"}}   // the event's ModelRef, when it names one
  ```

  a new **singleton, last-wins** record kind that recovery reads and the whole-file writer never
  emits; a 1.101 build skips it with the standard unknown-kind warning. The record carries the
  event's `model` whenever the event has one (`skip_serializing_if` otherwise) because the
  forwarder adopts that model into the turn when the context started without one: a turn whose
  index record would have named the adopted model must recover with it too, or its tokens land in
  the ledger unattributed. Written whenever the event names a model, not only when the forwarder's
  adoption condition (model *and* window) holds: the record states what the harness said, and
  recovery applies the adoption rule (D7). These records are interim by nature: a committed turn's
  `usage` is the `TurnCompleted` event's, and once the index record exists nothing reads them
  (D10). Deltas are never written:
  §7.3 makes them transient, and `ItemCompleted` carries the canonical form. Diffs are written at
  commit, as today (fold by path would allow per-update appends later). Every append is the
  amendment's discipline: one `write_all` on an append handle, newline guard, no fsync.

- **D4. The writer lives in the store.** `PersistStore` gains

  ```rust
  // ENTITY-AUTHORITY-EXCEPTION: the open live-turn file of each thread, one at most, keyed by
  // thread because the turn gate admits one live turn per thread; registered at birth, removed
  // at commit, discard, or recovery; what tells recovery a live file is in use, not stale.
  live_turns: Mutex<HashMap<ThreadId, LiveTurnFile>>,
  // ENTITY-AUTHORITY-EXCEPTION: threads whose live file this process has already checked.
  live_recovered: RwLock<HashSet<(ProjectId, ThreadId)>>,

  struct LiveTurnFile { turn_id: TurnId, file: Option<std::fs::File>, failed: bool }
  ```

  with three public methods the forwarder calls through `ThreadMetadataService`:
  `begin_live_turn(project, thread, turn_id, &UserInput, &TurnModel, TurnMode, started_at) ->
  LiveTurnOutcome`, `append_live_item(project, thread, turn_id, index, &Item)`,
  `append_live_usage(project, thread, turn_id, TokenUsage, Option<ModelRef>)`. Each takes the
  thread lock. An append
  for a `turn_id` other than the registered one is a protocol error logged at `warn` and ignored.
  Keeping the handle open saves an open per item and is what makes "a writer owns this file"
  checkable.

- **D5. Commit is the store's business.** `append_turn_unlocked` learns a third case before the two
  it has: if `live_turns` holds this thread's entry for `turn.id` and it has not failed, the store
  appends the `status` record and the `diff` records (through `turn_with_inline_diffs`, so bodies
  are inline exactly as the whole-file writer puts them), `sync_all`s the handle, drops it, renames
  `live.jsonl` to `turns/<turn_id>.jsonl`, and falls into the existing "payload exists" branch,
  which appends the index record. Payload first, index last, the cache update untouched. The
  forwarder's commit path does not change: `persist_turn` → `append_turn_with_diffs` → store. The
  comment on the "payload exists" branch (`store.rs:1320-1332`) must name the rename as its second
  producer. A whole-file write remains the path for a turn with no live file (flat layout, failed
  writer, a turn whose forwarder never attached).

- **D6. Failure is contained to the live file.** A failed birth registers `{ failed: true, file:
  None }`, logs once at `warn` with `project_id`, `thread_id`, `turn_id`, `action =
  "begin_live_turn"` and the error, and every later append for the turn is a silent no-op. A failed
  append marks `failed`, logs once at `warn` (`action = "append_live_item"` / `"append_live_usage"`),
  closes the handle and **deletes `live.jsonl`** (best effort, a second `warn` if that fails): a
  file missing items must not be finished into a payload, so the commit sees no usable live file
  and writes whole. A commit whose live entry names a different turn id discards the file the same
  way and logs at `warn`. The commit is therefore never less reliable than today.

- **D7. Recovery.** A `live.jsonl` with no `live_turns` entry for its thread is a crashed turn.
  `ensure_live_recovered(project, thread)` runs beside `ensure_migrated` at the top of every public
  thread entry point, once per `(project, thread)` per process (the `live_recovered` set; one
  `stat` the first time, nothing after). Birth calls it too, so recovery always precedes the first
  turn this process runs on the thread and the recovered record lands in the index before any newer
  turn's. Under the thread lock it:
  1. parses the file with `parse_turn_payload` (best-effort; a lost prologue is `Corrupt` and the
     file is quarantined to `live.jsonl.corrupt-<ts>` like any unreadable payload; a newer format
     is left alone and logged);
  2. reads `turn_id`, `mode`, `started_at` from the header (a header without them, which only a
     hand-edited file can produce, recovers with `TurnModel::Unknown`, `TurnMode::Unknown` and the
     file's modification time); `usage` from the last `usage` record, else zero; and `model` by the
     forwarder's own adoption rule: the header's model when it is known, else the last `usage`
     record's `model` when one names it, else `Unknown`. The second case is the adopted-model turn,
     and it is why the record carries the model (D3): the ledger entry recovery writes must be the
     one the commit would have written;
  3. if `turns/<turn_id>.jsonl` already exists, the turn committed and the live file is a leftover:
     delete it, log at `warn`, done;
  4. builds a `Turn { status: Interrupted, message: Some("Giskard stopped before this turn
     completed; <n> items recovered"), completed_at: last item's created_at else started_at,
     skipped_records }` and commits it through the D5 path with `ThreadRecency::RestoreActivity(
     completed_at)` — the repair rule of §5.4: a thread that died days ago does not jump to the top
     of the list;
  5. logs at `warn` with `project_id`, `thread_id`, `turn_id`, `items`, `skipped_records`, `action
     = "recover_live_turn"`.

  `append_turn_with_diffs_and_update_aggregates` gains a recency parameter on its `_unlocked` half
  so recovery can pass `RestoreActivity`; the public method keeps `RecordActivity`.

- **D8. Operator surface.** `validate` reports `<thread dir>/live.jsonl: unrecovered in-flight
  turn <turn_id> (<n> items)` for every live file, as information, not corruption. A new
  `recover-live-turns [--dry-run]` recovers them under the data-directory lock, per thread, never
  aborting the run, with the sweep command's reporting shape; `--dry-run` lists them with their
  turn id and item count.

- **D9. Spec and conventions.** Spec 1.102: §5.2 gains the `live.jsonl` line; §5.4 gains a "Live
  turn file" bullet (birth, extension, commit as rename, failure policy, recovery, recency) and the
  "Format identification" bullet notes the header's optional fields and the `usage` kind; §5.5 gains
  `recover-live-turns` and the `validate` lines; the "Turn history" bullet states the authority
  rule of D10 in one sentence. `AGENTS.md:125-132` becomes "Three files are
  appended to", naming the live file and that a torn tail costs one record as for an amendment;
  `:146` gains "and a live turn's file is finished and renamed into place before its index record".

- **D10. The index record is the authority for what it duplicates.** D2 and D3 put `model`,
  `mode` and `started_at` in the header and `usage` (with a model) in `usage` records: fields the
  index record also carries once the turn commits. The rule that keeps that duplication harmless:
  for a turn the index references, the index record is the **sole** authority for `model`, `mode`,
  `usage`, `started_at` and `completed_at`, exactly as `into_turn` already behaves (Facts). The
  header fields and the `usage` records are **recovery inputs**: read by `ensure_live_recovered`
  and `recover-live-turns` to build the index record a crash prevented, and by `validate`, and by
  nothing else. `TurnPayload.header` and `TurnPayload.usage` exist for those two readers; `into_turn`,
  `load_turns`, `recompute_aggregates` and every wire read stay index-only and are not touched by
  this milestone. The two copies come from the same `TurnContext` (header at attach, index record at
  commit), so they agree except where the forwarder adopted a model mid-turn, which the `usage`
  records record; no reader reconciles them, and no reader ever has to pick between a header and an
  index record. `validate` reports, per referenced payload, a header whose model is known and
  differs from the record's known model, or whose mode differs from the record's, as damage in the
  same list as a parse failure: the writer cannot produce either, so it means a hand edit or a bug.
  It does not compare a header with `Unknown` against a known record model (adoption), the `usage`
  records against the record's usage (interim by design), or the two `started_at` clocks (the header
  is the attach clock, the record the `TurnStarted` clock, Facts).

## Changes

### A. `crates/giskard-persist/src/layout.rs` (+4)

`ThreadPaths::live_turn(&self) -> PathBuf` = `dir().join("live.jsonl")`.

### B. `crates/giskard-persist/src/history.rs` (+90, tests +120)

- `PayloadLine::TurnHeader { format, turn_id, model: Option<&TurnModel>, mode: Option<TurnMode>,
  started_at: Option<DateTime<Utc>> }` with `skip_serializing_if`; `PayloadTurnHeader` gains the
  three as `#[serde(default)]` `Option`s and loses `#[allow(dead_code)]` on `turn_id`.
- `PayloadLine::Usage { usage: &TokenUsage, model: Option<&ModelRef> }` with `skip_serializing_if`,
  `PayloadUsage { usage: TokenUsage, #[serde(default)] model: Option<ModelRef> }`, parsed as a
  last-wins singleton with the same warning as `status`.
- `TurnPayload` gains `pub header: Option<LiveTurnHeader { turn_id, model, mode, started_at }>` and
  `pub usage: Option<PayloadUsage>`, both `None` for every file written today, both documented as
  recovery inputs that `into_turn` must never read (D10).
- `pub fn live_turn_prologue(turn_id, &UserInput, &TurnModel, TurnMode, started_at) -> Result<Vec<u8>>`,
  `pub fn payload_item_line_indexed(index, &Item)`, `pub fn payload_usage_line(TokenUsage,
  Option<&ModelRef>)`,
  `pub fn payload_status_line(&TurnStatus)`, `pub fn payload_diff_lines(&Turn, &[CapturedDiffRecord])`.
  `payload_file_bytes_with_diffs` is rewritten over these so one writer produces both files.
- Tests: the whole-file writer's bytes are unchanged for a turn (compare against a literal); a file
  built as prologue + items + usage + status + diffs parses to the same `TurnPayload` as
  `payload_file_bytes_with_diffs` for the same turn, repeat completion included; a `usage` record
  folds last-wins with a warning on repetition, with and without a `model`; a header with the three
  fields round-trips and a header without them yields `None`s; `into_turn` of a record over a
  payload whose header and `usage` record name a different model, mode and usage than the record
  yields the record's (D10).

### C. `crates/giskard-persist/src/store.rs` (+260, tests +300)

- The two fields of D4 with their `ENTITY-AUTHORITY-EXCEPTION` comments.
- `begin_live_turn`, `append_live_item`, `append_live_usage` per D4/D6, plus `discard_live_turn
  (project, thread)` for the mismatch case, all under the thread lock, all `ensure_migrated` and
  `ensure_live_recovered` first.
- `ensure_live_recovered` and `recover_live_turn_unlocked` per D7; `recover_live_turns(project,
  thread, dry_run) -> Result<LiveRecovery, PersistError>` for the admin command.
- `append_turn_unlocked`: the D5 branch, and the updated comment.
- `append_turn_with_diffs_and_update_aggregates_unlocked(…, recency)`; the public method passes
  `RecordActivity`.
- `history_validation_errors`: the D8 report line.
- `delete_thread`: nothing, the directory rename covers the file; but `live_turns` must drop the
  thread's entry so a later thread with the same id (tests reuse ids) starts clean.
- Tests (all in `layout_tests` with the existing helpers): birth writes header and prompt
  atomically and the file parses; appends extend; a repeat completion folds into its slot; usage
  last-wins; commit appends status and diffs, renames, appends the index record, and the resulting
  `turns/<id>.jsonl` parses equal to what `payload_file_bytes_with_diffs` would have written for
  the same turn; after commit no `live.jsonl` remains and `live_turns` has no entry; commit of a
  turn with no live file writes whole as before (byte-identical to today); a failed append (a
  directory in the file's place) marks the writer failed, deletes nothing it cannot, and the commit
  writes whole with `#[traced_test]` asserting one `warn`; a mismatched turn id at commit discards
  the live file and writes whole; recovery of a file with k items yields an `Interrupted` turn with
  k items, the recorded usage in `thread.json` credited to the header's model, or to the last
  `usage` record's model when the header has none (the adopted-model turn), or unattributed when
  neither names one; `completed_at` = last item's `created_at`,
  `updated_at` not advanced past it; a torn tail costs one record and `skipped_records` is 1; a
  live file whose turn is already in `turns/` is deleted; a file without `user_input` is
  quarantined; a flat-layout thread returns `Unsupported`; `validate_all` names an unrecovered
  file; `recover_live_turns` dry run changes nothing and a real run recovers; the first
  `ensure_live_recovered` costs one `stat` and later ones none (assert through a counter on a test
  hook, or accept the design and test the set's membership).

### D. `crates/giskard-server/src/thread_metadata.rs` (+30)

`begin_live_turn`, `append_live_item`, `append_live_usage` pass-throughs. No mutation is
published: a live append changes no aggregate the catalog shows.

### E. `crates/giskard-server/src/registry/event_forwarder.rs` (+60, tests +150)

- `attach_to_turn` (`:1783-1785`): after `owned_turn` is set, `begin_live_turn(turn,
  &context.user_input, &context.model, context.mode, self.turn.started_at)`; the outcome is logged
  at `debug` (`Begun`, `Unsupported`, `Failed`).
- `note_owned_event`'s `ItemCompleted` branch (`:1903`): after the upsert, look the item's position
  up in `self.turn.items.indexes` and call `append_live_item`.
- `record_turn_usage` (`:1808`): `append_live_usage` with the event's `usage` and `model.clone()`,
  before the adoption branch, so the record states what the event said.
- Nothing in `complete_forwarded_turn` or `persist_turn` changes (D5).
- Tests with the fake harness: after a start and two completions `live.jsonl` exists with four
  records; after completion it is gone and `turns/<id>.jsonl` parses with both items; a repeat
  completion appends an indexed record; an `Interrupted` stream end commits through the rename; a
  compaction turn behaves the same; a flat-layout thread writes no live file; a turn started with
  `TurnModel::Unknown` whose usage update names a model leaves a `usage` record carrying it, and
  the committed index record names the same model while the header still says `unknown`.

### F. `crates/giskard-persist/src/bin/giskard_admin.rs` (+60)

`recover-live-turns [--dry-run]` per D8, in the sweep command's shape; header comment and usage
text.

`validate` gains the D10 comparison in `history_validation_errors`, with a store test for a
header that names a different known model than its record (reported) and one with `unknown`
against a known record model (not reported).

### G. Integration, `crates/giskard-server/tests/live_turn_recovery.rs` (new, about 220 lines)

Prepare a data directory with a thread whose `live.jsonl` holds a prologue, three items (one a
running command), two usage records and a torn final line, as a crash would leave it; start a
`TestServer` on it; open the thread; assert the history route returns one `Interrupted` turn with
three items and `skipped_records: 1`, the thread file's tokens equal the last usage, `updated_at`
equals the last item's `created_at`, `live.jsonl` is gone and `turns/<id>.jsonl` exists; then start
a new turn on the same thread with the fake harness and assert the recovered record precedes the
new one in `history.jsonl`. A second test puts a live file whose turn is already committed and
asserts it is deleted with a `warn`. A third runs a turn to completion and asserts, through the
store, that `live.jsonl` existed mid-turn and is absent afterwards.

### H. Documentation

- `specs/giskard-specification.md`: version 1.102; amendment note; **Changelog (1.101 → 1.102),
  live turn file**: LT1 the file and its birth, LT2 extension and the `usage` record, LT3 commit as
  rename and the unchanged payload-first rule, LT4 failure policy, LT5 recovery and recency, LT6
  the header fields and format identification, LT7 `validate` and `recover-live-turns`, LT8 the
  authority rule of D10 (index record sole authority for the five fields; header and `usage`
  records recovery inputs only); the §5.2, §5.4, §5.5 edits of D9.
- `AGENTS.md:125-132` and `:146` per D9.
- `README.md`: the layout block gains `live.jsonl  # the in-flight turn's payload, finished and
  renamed into turns/ when the turn ends`; the admin table gains `recover-live-turns`; the storage
  paragraph gains two sentences on what survives a crash.
- The overall plan's P1 section: status line when the PR exists.
- `docs/api-endpoints.md`, `config.example.toml`, adapter READMEs: nothing changes.

### Not touched

- `history.jsonl`, its format, `HISTORY_FORMAT`, `TURN_PAYLOAD_FORMAT`.
- `giskard-proto`, `app.js`, any route: nothing on the wire changes.
- `thread_runtime/live.rs`: the in-memory buffer is unchanged; the live file is written from the
  forwarder's item vector, not from the buffer.
- `amend_turn_item`, `sweep_orphan_payloads`, migration.

## Exit checks

- `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green;
  `tests/e2e/run.sh` green (no e2e changes, but the replay server now writes live files).
- `giskard-admin validate` on a directory with a hand-written `live.jsonl` names it;
  `recover-live-turns --dry-run` lists it; `recover-live-turns` recovers it; `validate` is then
  clean.
- Kill `giskard-server-replay` with `SIGKILL` during the long-turn scenario, restart it on the same
  data directory, open the thread: the turn shows as interrupted with its prompt and every reply
  that had completed, and the open note is absent.
- `git diff` shows no change to `HISTORY_FORMAT` or `TURN_PAYLOAD_FORMAT`.
- A turn committed through the rename parses equal to one written whole (test C), so no reader
  needs to know which path produced a file.

## Signs the step has gone wrong

- `turns/<id>.jsonl` exists for a turn and `live.jsonl` also exists with the same id: the rename
  ran but the entry was not dropped, or a birth reused a thread with a stale entry.
- A recovered thread moves to the top of the thread list: `RecordActivity` was used instead of
  `RestoreActivity`.
- A commit after a failed append produces a turn with fewer items than the forwarder had: the live
  file was finished despite `failed`, instead of being discarded for a whole write.
- The `item_count` in the index record disagrees with the payload for a turn that was never
  amended: the index record was built from something other than `turn.items`.
- A per-item fsync appears, or a `tokio::fs::File` kept across awaits without a lock: the append
  discipline is the amendment's, one `write_all`, no fsync, under the thread lock.
- Any change to the index format or a payload format bump: out of scope, both formats are
  unchanged by design.
- Recovery runs on a thread while its forwarder is writing: the `live_turns` check was skipped.
- `into_turn`, `load_turns`, `recompute_aggregates` or a wire read consults `TurnPayload.header` or
  `TurnPayload.usage`: the authority rule (D10) was broken; two copies of a field now have two
  readers, and the next disagreement is a bug nobody can localize.
- A recovered adopted-model turn credits the ledger unattributed while its `usage` record names a
  model: recovery read the header's `Unknown` and stopped, instead of applying the adoption rule.

## Follow-ups (not this milestone)

- A crash between the rename and the index append still leaves an unreferenced payload, as today.
  A later milestone could adopt such a payload into the index, but only under one rule: the index
  is append-only and never rewritten, so an orphan can only be appended at the end, and that is
  right only if it is the thread's newest turn. There is no invariant that it is: orphans accrue one
  per crash (the sweep's refusal threshold exists for that), and an index append that failed
  without a crash becomes interior once the process restarts and new turns commit. Adoption would
  therefore run at the same gate as live recovery, before any new turn can be born, and adopt only
  a payload whose header `started_at` (D2) is later than the last index record's; a payload without
  the header fields, which is every payload written before P1, stays unadoptable and sweepable. A
  `live.jsonl` has no such problem: it is the newest turn by construction, since every earlier turn
  committed before it started and recovery runs before any newer one exists.
- Per-update `diff` records, so a crash keeps diffs too.
- L4: with items on disk during the turn, the runtime can stop holding completed items beyond the
  tail and `CurrentTurnItems` can go.

## Size

About 350 lines in `history.rs` and 560 in `store.rs` including tests, 30 in `thread_metadata.rs`,
210 in the forwarder including tests, 60 in the admin binary, 220 in a new integration test, 4 in
`layout.rs`, 90 in the spec and docs. One PR; the store commits (A–C) can be reviewed before the
forwarder and admin commits (D–F).
