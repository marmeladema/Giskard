# S11 — Retire the marker-only compaction machinery

Step 11 of [`design-straightening-review.md`](design-straightening-review.md). It replaces the
earlier "typed compaction marker" idea from S9 decision C.

## What was found

The `"Context compacted"` activity title is matched by string in three places (the Codex
adapter, the event forwarder, and `app.js`). Every one of those matches exists for a single
reason: spec entries CC4 and CC5 (changelog 1.30 → 1.31) describe Codex versions that answered
`thread/compact/start` with only a `ContextCompacted` notification and no `turn/started` or
`turn/completed`. Giskard then treated the first marker as the terminal event of the manual
compaction turn.

That code path no longer exists on either side:

- **Giskard** removed the forwarder's marker-driven completion synthesis in commit `9392310`
  ("Implement long-lived native thread ownership"), together with its test
  `"compaction marker should synthesize turn completion"`. What remains on `main` is a flag that
  is set once and only read by log lines, a log field `will_synthesize_completion` that names a
  branch that is gone, an adapter-side `PendingCompaction` map whose completion is only logged,
  and a UI early re-enable that `turn_completed` already performs.
- **Codex** (verified in `openai/codex` at tag `rust-v0.155.1`, the version
  `giskard-harness-codex` pins, and again on `main` at `392f56a`; line numbers below are the
  tag's) runs a manual compaction as an ordinary task: `thread/compact/start` submits `Op::Compact`
  (`codex-rs/app-server/src/request_processors/thread_processor.rs:2407`); every compaction
  implementation emits `TurnStarted` first (`codex-rs/core/src/compact.rs:152`,
  `compact_remote_v2.rs:114`, `compact_token_budget.rs:30`); the compaction item is a
  normal `TurnItem::ContextCompaction` emitted through item started/completed
  (`codex-rs/core/src/compact.rs:251`); and `on_task_finished`
  (`codex-rs/core/src/tasks/mod.rs:588`) emits `TurnComplete` or `TurnAborted` for every task
  kind. The deprecated `thread/compacted` notification is swallowed by the app-server
  (`codex-rs/app-server/src/bespoke_event_handling.rs:1016`, "v2 clients receive the canonical
  ContextCompaction item instead") and is not sent by any code path.

So the wire shape of a manual compaction is always `TurnStarted`, the item, then `TurnCompleted`
or an abort. That is the shape the replay harness (`giskard-harness-replay/src/lib.rs:367-397`)
and the end-to-end test `compact_context_streams_and_persists_compaction_turn` already use.

No other activity is matched by title on the server. Sub-agent activities are recognised through
the typed `subagent: Option<SubagentLink>` field (`registry.rs:1739-1747`). In `app.js` the only
other title match is `"Image viewed"` (`isImageViewActivity`, line 8565), which renders an inline
image preview and is unrelated.

## Decisions

- **D1. Delete, do not type.** The marker has no consumer left; giving it a typed identity would
  formalise a signal nothing reads. The compaction activity becomes an ordinary activity with the
  same title. No `giskard-core`, `giskard-proto`, or persisted-shape change.
- **D2. The canonical item carries no metadata.** The live mapping arm
  (`mapping.rs:2943-2948`) stores the raw Codex payload `{"type":"contextCompaction","id":..}` as
  `metadata`. It has no user value and is the only reason `app.js` hides metadata for this title.
  New rows get `metadata: None`; the mapping test asserts that. Owner-approved. The three other
  arms that copy the whole Codex item (`ImageView`, `Sleep`, `ImageGeneration`) are out of scope
  here; do not touch them.
- **D3. Presentation special-casing stays in the UI; lifecycle special-casing leaves it.**
  Deciding how a known activity looks is the UI's job, so `visibleActivityMetadata` and
  `isContextCompactionPayload` stay: they hide the raw protocol payload that rows persisted
  before this change carry (from either the deprecated notification arm or the item arm). Their
  comment says that. What goes is the lifecycle hook (`item_completed` →
  `finishCompactPending`) and its helper `isContextCompactionItem`, because settling the pending
  state is `turn_completed`'s job and the server no longer treats the item as a signal.
- **D4. The forwarder's manual-compaction logging keys on the turn intent, not the item.** Every
  `TurnContextKind::ManualCompaction` log line stays; only the marker fields inside them go.
- **D5. Spec: supersede, do not rewrite.** CC4 and CC5 keep their text with a
  `(superseded by 1.93/CC6)` prefix, following the C8/LT9/WS2 convention. A 1.92 → 1.93 changelog
  block records the new statement.
- **D6. Replay harness and testenv untouched.** They already emit the three-event shape.

## Changes

Line numbers are for `main` at `1606e93` (S10 landed, steering landed, `codex-codes` 0.155.1).
Re-check each anchor before editing; the order below is the order that keeps the tree compiling
at each step.

### A. `crates/giskard-harness-codex/src/lib.rs` (−100 lines)

Production, in one contiguous region (`:1398-1495`), delete all six items:

| Lines | Item |
| --- | --- |
| `:1398-1402` | `enum MessageOutcome { Handled, CompactionCompleted { .. } }` |
| `:1404-1432` | `struct PendingCompaction` and its `impl` (`new`, `observe`) |
| `:1435-1462` | `fn observe_pending_compaction` |
| `:1464-1473` | `fn compaction_event_name` |
| `:1475-1488` | `fn pending_compaction_states` |
| `:1490-1495` | `fn is_context_compaction_activity` |

Imports at the top of the file keep every name they import: `HashMap` (10 other production uses),
`Instant` (6), `display_opt` (21), `info!` (still used once in production and throughout
`instance.rs`, which imports through `use super::*`).

Tests (`mod tests` starts at `:2766`):

- Delete the helper `fn context_compacted_event` (`:3595-3611`). Keep `fn completed_event`
  (`:3613`), which four other tests use.
- Delete the two `#[test]` functions `pending_compaction_marker_only_completes_without_turn_started`
  and `pending_compaction_marker_after_turn_started_waits_for_turn_completed` (`:5705-5746`,
  attributes and the blank line between them included). The next test,
  `incomplete_stream_without_turn_emits_error_event` at `:5747`, is untouched.
- Rename `fatal_stream_error_closes_worker_with_only_pending_compaction` (`:4090`) to
  `fatal_stream_error_closes_worker_during_compaction`. Its body stays: it still proves that a
  fatal stream error closes the worker while a `compact_thread` request is in flight.
- Fix the test-module imports at `:2770-2772`. After the deletions `Utc`, `ItemId`, and `Item` have
  no use in the module (their only uses were inside `context_compacted_event`), so they would fail
  `-D warnings`. Delete `use chrono::Utc;` and `use giskard_core::ids::ItemId;`, and change
  `use giskard_core::item::{Item, ItemPayload};` to `use giskard_core::item::ItemPayload;`
  (`ItemPayload` still has three uses at `:6359`, `:6364`, `:6409`).

### B. `crates/giskard-harness-codex/src/instance.rs` (−40 lines)

- `:8` doc comment: drop "pending compactions," from the list of task-owned state.
- `:25` delete the field `pending_compactions: HashMap<ThreadId, PendingCompaction>,`.
- `:53` delete the initialiser `pending_compactions: HashMap::new(),`.
- `:129-139` replace the `match self.handle_server_message(msg).await { .. }` with the plain
  statement `self.handle_server_message(msg).await;`.
- `:150-158` delete the `if !self.pending_compactions.is_empty() { warn!(..) }` block.
- `:168-169`, `:183-184`, `:194-195` delete the two log fields `pending_compactions = ..` and
  `pending_compaction_states = ..` from each of the three `warn!` calls.
- `:464` change the return type of `handle_server_message` from `-> MessageOutcome` to none.
- `:484-485` delete the `let completed_compaction = observe_pending_compaction(..);` statement.
- `:537-539` delete `if let Some(elapsed_ms) = completed_compaction { return .. }`.
- `:557` and `:575` delete the tail expressions `MessageOutcome::Handled`.
- `:568` change `return MessageOutcome::Handled;` to `return;`.
- `:815` and `:827` delete the log field `pending_compactions = self.pending_compactions.len(),`.
- `:821-822` delete the `self.pending_compactions.insert(thread.thread, PendingCompaction::new(started));`
  statement. The `started` binding at `:811` stays; `ack_elapsed_ms` and the error branch still
  read it.

`Instant` and `HashMap` remain in use in this file (`PendingSteer`, `active_turns`,
`pending_context_restores`).

### C. `crates/giskard-harness-codex/src/mapping.rs` (−50 lines)

- Delete the `Notification::ContextCompacted(n) => { .. }` arm (`:741-763`, plus the blank line
  `:764`). The `match` has a wildcard `_ => None` at `:815`, so no exhaustiveness change. The
  `codex_codes::messages::Notification::ContextCompacted` variant itself is untouched; it is part
  of the pinned crate.
- In the `ThreadItem::ContextCompaction { .. }` arm (`:2943-2948`) change
  `metadata: json_value(item),` to `metadata: None,` (D2). `json_value` keeps 19 other callers.
- Update the test `context_compaction_item_maps_to_clean_activity` (`:6486-6511`): replace the
  three metadata lines (`let metadata = metadata.expect(..)`, and the two `assert_eq!` on
  `metadata["type"]` and `metadata["id"]`) with `assert_eq!(metadata, None);`.
- Delete the test `context_compacted_notification_maps_to_clean_activity` (`:6513-6543`,
  attribute and trailing blank line included).

### D. `crates/giskard-server/src/registry/event_forwarder.rs` (−30 lines)

- `:3-8` delete `fn is_context_compaction_item`. The `Item` type stays imported through
  `use super::*` and has 30+ other uses in the file.
- `:626` delete the field `saw_context_compaction_marker: bool,`; `:645` its initialiser;
  `:662` its reset.
- `:1198`, `:1239`, `:1940` delete the log field
  `saw_context_compaction_marker = self.turn.saw_context_compaction_marker,`.
- `:1827-1841` delete the whole
  `if self.turn.context.kind == TurnContextKind::ManualCompaction && is_context_compaction_item(item) { .. }`
  block inside the `ItemCompleted` arm, so that `if self.turn.items.upsert(item) {` at `:1842`
  follows the native-identity link directly.
- `:2063` delete `let has_context_compaction_marker = ..;`; `:2072` and `:2108` delete the log
  field `has_context_compaction_marker,`.

Everything else in the manual-compaction path (the `ManualCompaction` `info!` lines at `:1780`,
`:1933`, `:2064`, `:2102`, the stream-end `warn!` at `:1231`, the exit `warn!` at `:1186`) stays.

### E. `crates/giskard-server/static/app.js` and `tests/ui.rs` (−6 lines)

- `app.js:4362` delete `if (isContextCompactionItem(ev.item)) finishCompactPending();`. The
  `turn_completed` case (`:4386`) and the `compact_context` error path (`:3251`) still settle
  `state.compactPending`.
- `app.js:8679-8682` delete `function isContextCompactionItem`.
- Above `function isContextCompactionPayload` (`:8671`) add a two-line comment: this is a
  presentation choice owned by the UI; rows persisted before S11 carry the raw Codex payload as
  activity metadata and this hides it, new rows carry none. Do not change the function body (D3).
- `tests/ui.rs:476` delete the line `&& body.contains("isContextCompactionItem"),` and move the
  comma so the assertion still closes on the `msg.action==="compact_context"` line. The assertion
  at `:1366-1370` (`visibleActivityMetadata` / `isContextCompactionPayload`) stays.
- No screenshot regeneration: there is no visible change. `UI_VERSION` changes automatically
  through `build.rs`.

### F. `specs/giskard-specification.md` (+12 lines)

- `:12` bump `**Version:** 1.92` to `1.93`.
- Insert a new block before the 1.91 → 1.92 block at `:185`:

  ```
  **Changelog (1.92 → 1.93), manual compaction is an ordinary turn:**
  - **CC6:** Codex runs a manual compaction as a normal task: `thread/compact/start` is followed by
    `turn/started`, a `contextCompaction` item, and `turn/completed` (or an abort). Giskard no
    longer recognises a `Context compacted` activity as a lifecycle signal; the turn gate, the
    persisted `/compact` turn, and the browser's pending state all settle on `turn/completed`
    exactly as for a user turn.
  - **CC7:** The `Context compacted` activity is an ordinary activity item. New rows carry no
    metadata; the UI hides the raw protocol metadata only on rows persisted before this version.
  ```

- `:1015` change `- **CC4:**` to `- **CC4 (superseded by 1.93/CC6):**`; `:1020` change
  `- **CC5:**` to `- **CC5 (superseded by 1.93/CC6):**`. Keep their text.

### G. `crates/giskard-harness-codex/README.md` (±5 lines)

- `:16` drop "pending compactions and" from the list of instance-owned state.
- `:22` drop "compaction," from the list of state the transport never touches.
- Add a short paragraph after the "Runtime ownership" section:

  ```
  ## Manual compaction

  `compact_thread` sends `thread/compact/start` and returns once Codex acknowledges it. Codex
  then runs the compaction as an ordinary turn (`turn/started`, a `contextCompaction` item,
  `turn/completed`), which the instance forwards like any other turn. The adapter keeps no
  per-compaction state.
  ```

### Not touched

`giskard-core`, `giskard-proto`, `giskard-harness`, `giskard-harness-replay`, `giskard-persist`,
`giskard-testenv`, `registry.rs`, `thread_runtime/`, `ws.rs`, `routes.rs`, the end-to-end tests,
`tests/e2e/`, and `docs/screenshots/`.

## Exit checks

Run from the repository root on the finished tree. Baselines are from `main` at `1606e93`.

| # | Check | Before | After |
| --- | --- | --- | --- |
| A | `rg -c "Context compacted" crates --type rust` | forwarder 1, e2e_smoke 3, codex lib.rs 2, mapping.rs 4, replay 1 | e2e_smoke 3, mapping.rs 2, replay 1 |
| B | `rg -n "PendingCompaction\|pending_compaction\|MessageOutcome\|is_context_compaction_activity\|compaction_event_name\|context_compacted_event" crates/giskard-harness-codex` | 51 hits | nothing |
| C | `rg -n "saw_context_compaction_marker\|has_context_compaction_marker\|is_context_compaction_item\|will_synthesize_completion" crates/giskard-server` | 13 hits | nothing |
| D | `rg -n "ContextCompacted" crates/giskard-harness-codex/src` | 2 hits | nothing |
| E | `rg -n "isContextCompactionItem" crates/giskard-server` | app.js 3, ui.rs 1 | nothing |
| F | `rg -c "isContextCompactionPayload" crates/giskard-server/static/app.js` | 3 | 2 |
| G | `rg -n -i "pending compaction" crates/giskard-harness-codex` | README 1, instance.rs 1 | nothing |
| H | `rg -n "CC4 \(superseded\|CC5 \(superseded\|\*\*CC6:\*\*\|\*\*CC7:\*\*\|Version:\*\* 1.93" specs/giskard-specification.md` | nothing | 5 lines |
| I | `git diff --stat main -- crates/giskard-core crates/giskard-proto crates/giskard-harness crates/giskard-harness-replay crates/giskard-testenv crates/giskard-persist` | | empty |
| J | `cargo fmt --all --check` | | clean |
| K | `cargo clippy --workspace --all-targets --locked -- -D warnings` | | clean |
| L | `cargo test -p giskard-harness-codex --locked` | | green; `context_compaction_item_maps_to_clean_activity` and `fatal_stream_error_closes_worker_during_compaction` present |
| M | `cargo test -p giskard-server --locked --test e2e_smoke compact_context` | | green (`compact_context_streams_and_persists_compaction_turn` and the three other `compact_context_*` tests) |
| N | `cargo test -p giskard-server --locked --test turn_steering steer_input_rejects_compaction_owner` and `--test ui` | | green |

Check L is where a missed import shows up (A's `Utc`/`ItemId`/`Item`).

## Signs the step has gone wrong

- Any diff under `giskard-core` or `giskard-proto`: the step is deletion only.
- A new `match` on an activity title anywhere, or a `kind` field added to `Activity`: that is the
  broader typed-notice question, which is a separate discussion, not this step.
- `finishCompactPending` no longer called from `turn_completed`: the UI would hang in
  "Compacting..." after a real compaction.
- The `ManualCompaction` log lines removed rather than trimmed: they are the only trace of a
  compaction turn in the server log.

## Size

Roughly −200 non-test lines across the Codex adapter, the forwarder, and `app.js`, plus about
+20 lines of spec and README text.
