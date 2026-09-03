# M7 — Lifecycle fences and reader contracts

Implementation plan for milestone M7 of [`event-pipeline-milestones.md`](event-pipeline-milestones.md).
Written against `main` at `b71aaf1` (M6 merged). Every file and line reference below was checked
against that tree; re-check them if the branch has moved.

## Goal

Close the seven findings of the post-M6 independent review. None of them argues for a redesign; each
is a missing fence or an unstated contract at one of three boundaries the milestones created: the
driver's admission lane, the retained log's reader contract, and the forwarder's intent lane. The
fixes are deterministic, timing-free, and small. They are grouped here so the same reasoning covers
all of them and the tests land together.

| # | Finding | Boundary | Fix in one line |
| --- | --- | --- | --- |
| 1 | Project deletion snapshots owners before quiescing admission | driver | Take the owner snapshot after `quiesce()` returns |
| 2 | A quiesced driver consumes and drops discoveries; a failed deletion cannot recover them | driver | Stop polling the discovery cursor while quiesced |
| 3 | Admission is at-most-once; a failed admission leaves a native route with no file and no owner | driver | Defer a failed reply-less admission and retry it after the next successful admission or a resume |
| 4 | A reader created after evictions is not told about them | log | Hand the first reader after an unobserved eviction a pending `Gap` |
| 5 | Intents are not ordered before retained events, and a cloned intent sender can outrun detach | forwarder | `biased` select with events before intents; refuse an intent once cancel is set |
| 6 | Registry shutdown does not fence admission | driver | Quiesce every driver before shutting its harness down |
| 7 | Caps count entries, not bytes; `read_line` has no maximum | transport | A maximum frame length that ends the transport as fatal; say the caps are counts in the docs |

## Non-goals

- No change to `AgentHarness`, the adapter's mapper, routes, persistence, or the hub.
- No timers, backoff or retries driven by time. Every retry here is triggered by an event.
- No byte accounting inside `EventLog<T>`. The caps stay counts; the frame bound is enforced where
  bytes enter the process.
- No change to the admission algorithm in `admission.rs` beyond what a retry needs.
- Cursor-committed persistence stays M8.

## Ground truth

| Fact | Where |
| --- | --- |
| `delete_project`: `coordinator_snapshot` first, then `begin_delete`, then `driver.quiesce()`, then `harness.shutdown()`, then detach each snapshotted thread and `runtime.forget_threads` | `registry.rs:1492-1560` (snapshot `:1493`, quiesce `:1509`, resume on failure `:1520`) |
| Driver loop: commands and the discovery branch are gated on `admission.is_none()`; `Quiesce` and `Resume` flip `quiesced` and reply at once; the discovery branch is not gated on `quiesced` | `registry/driver.rs:243-291` |
| `begin_discovery` under `quiesced` warns, bumps the test counter, and drops the record | `driver.rs:452-461` |
| `finish_admission` attaches on success; `finish_admission_reply` only warns on a failed discovery and on a failed reply-less link; a link with a reply gets the error | `driver.rs:485-540` |
| The link path claims the native id before any fallible write | `registry/admission.rs:133-140` |
| Registry `shutdown`: `begin_shutdown`, `take_for_shutdown` (returns only the harness; the `DriverHandle` is dropped with the slot), harness shutdowns joined, then `background_tasks.close_and_wait` waits for the driver permits | `registry.rs:1393-1470`; `registry/project.rs:271-278`; `HarnessAndDriver` at `:120` |
| `EventLog::append` evicts above the cap and advances `base` whether or not a reader exists; `reader()` starts a new cursor at the current `base`; `poll_reader` reports `Gap` only when a cursor is behind `base` | `giskard-harness/src/event_log.rs:99-121`, `:144-154`, `:174-192`; `LogState` at `:36-46` |
| The log's module doc promises that the only loss is the cap and that it is reported as `Gap` | `event_log.rs:1-10` |
| Forwarder loop: unbiased `select!` over cancel, intents, the in-flight answer and the stream | `registry/event_forwarder.rs:863-905` |
| `admit_intent` checks classification, then busy, then reserves; it never reads `cancel` | `event_forwarder.rs:908-930` |
| `request_detach` replaces `Live` (dropping the coordinator's intent sender) and then sends `cancel = true`; a clone handed out by `intent_sender()` before the detach stays usable | `registry/thread.rs:190-209` |
| The transport reader uses `BufReader::read_line` with no maximum; a decode error is `Fatal` and closes the inbox | `giskard-harness-codex/src/transport.rs:391-432`; `InboxItem` at `:28-40` |
| Retention caps: `EVENT_LOG_RETAIN_LIMIT = 16_384` per thread and per discovery log (the discovery log uses `EventLog::new`), `CODEX_INBOX_RETAIN_LIMIT = 65_536` frames | `event_log.rs:22`; `instance.rs:17`; `transport.rs:25` |
| The byte-bound wording the review quoted is in the architecture review's proposed design, not in a description of what landed | `docs/event-pipeline-architecture-review.md:245`, `:298` |
| Driver tests that encode today's quiesce behavior | `driver.rs:1646` (`a_quiesced_driver_refuses_links_and_drops_discoveries`), `:1668`, `:1572`, `:1220` |
| Forwarder intent test helper returns the intent sender but not the coordinator | `event_forwarder.rs:2096-2160` |
| Codex control commands are serviced between the instance loop's inline awaits; each inline RPC is bounded by the 10 s JSON-RPC timeout | `instance.rs:106-125`, `:195-235`; `lib.rs:59` |

## Design

### 1. Snapshot after quiesce (`delete_project`)

Reorder `delete_project` to: `project_authority`, `begin_delete`, `driver.quiesce()`, **then**
`coordinator_snapshot` filtered to the project, then `harness.shutdown()`, `finish_delete`, detach
each thread, forget runtime, publish. The quiesce reply is sent from the command branch, which the
loop takes only when no admission is in flight, so when `quiesce().await` returns every owner the
driver will ever attach for this project is already installed. The snapshot taken after that point
is authoritative.

When there is no harness slot (`harness_and_driver` is `None`) the snapshot is taken where it is
today; there is no driver to race.

The failure paths keep their current shape: a failed quiesce rolls back the delete; a failed
harness shutdown resumes the driver and rolls back.

### 2. Do not consume discoveries while quiesced

Gate the discovery branch on `!self.quiesced` in addition to `!self.discoveries_closed` and
`self.admission.is_none()`. A quiesced driver leaves its discovery cursor where it is. A successful
deletion shuts the harness down, which closes the discovery log; the driver never reads those
records and exits with the rest. A failed deletion resumes the driver, which continues from the
unchanged cursor with nothing lost.

Delete the `quiesced` branch of `begin_discovery` (`driver.rs:453-461`); it becomes unreachable.
Links keep their current refusal under quiesce, because a link carries a reply and its sender is
waiting.

### 3. Deferred admission

A reply-less admission (a discovery, or a forwarder link) that fails is not the end of that
identity. Add to the driver:

```rust
struct DeferredAdmission {
    admission: Admission,          // Discovered(record) or Link(link) with reply == None
    attempts: u8,
}
deferred: VecDeque<DeferredAdmission>,           // driver field, not keyed
const DEFERRED_ADMISSION_LIMIT: usize = 64;
const ADMISSION_ATTEMPTS: u8 = 3;
```

Rules, all event-triggered:

- `finish_admission` with `Err` and a reply-less source pushes the admission back with
  `attempts + 1`, unless `attempts` reached `ADMISSION_ATTEMPTS` (log at `error!` and drop) or the
  queue is full (log at `error!` and drop the oldest). A link with a reply is answered with the
  error, as today; the route's caller decides.
- After `finish_admission` with `Ok`, and after `Resume`, if `deferred` is not empty and no
  admission is in flight, pop the head and start it (`begin_link` / `begin_discovery` with the
  stored attempt count). A successful admission is the evidence that the store and the harness are
  working again; a retry chained after a failure would only spin against the same fault.
- `Quiesce` does not clear `deferred`; the driver exit path answers nothing for them (they have no
  reply) and drops them with a `warn!` naming the count.
- `admission::admit` needs no change. Re-admitting a discovery or a link is idempotent: the claim
  adopts, an existing file is found, an existing owner is reused.

To keep the failure reason visible, `AdmissionSource` gains the attempt number for logging.

This is the smallest step that makes admission at-least-once without a timer. It is not a
guarantee against a fault that never clears; that case is logged three times and the identity
heals on restart, as it does today.

### 4. Report evictions to the next reader

`LogState` gains `unreported_evictions: u64`, incremented in `append` when an entry is evicted and
`cursors` is empty. `reader()` moves that count into the new reader as a pending gap and resets it
to zero. The per-reader cursor becomes:

```rust
struct Cursor { next: u64, pending_gap: u64 }
cursors: HashMap<u64, Cursor>,
```

`poll_reader` returns `Err(Gap { dropped: pending_gap })` and clears it before anything else.
`trim` and `reader_count` read `cursor.next` where they read the value before. The first reader
created after an unobserved eviction receives the gap; a second reader created before any append
does not, which matches how the driver creates one reader per attach. The module doc is amended:
"an eviction that happened while no reader existed is reported to the next reader created".

The `error!` in `append` stays; it is the operator's signal, the `Gap` is the consumer's.

### 5. Events before intents, and no intent after cancel

Two changes in the forwarder loop and one in `admit_intent`:

- `tokio::select!` gets `biased;` with branches in this order: cancel, stream, in-flight answer,
  intents. An intent is taken only when no retained event is immediately available, so an event
  that was already in the log when the intent arrived cannot adopt that intent's context. Cancel
  stays first so detach is never delayed by a busy stream. The busy check in `admit_intent`
  already refuses an intent while a turn is attached, so nothing else changes for the ordinary
  flow.
- `admit_intent` starts with `if *self.cancel.borrow() { reject(intent, no live owner); return; }`.
  `request_detach` sets cancel before it releases the coordinator lock, and any intent that a
  registry-held sender clone delivers after the detach began is therefore observed with cancel set.
  An intent already admitted before the detach is the "detach while a harness reply is pending"
  case M5 accepted; nothing changes there.

The error message is the one `intent_sender()` uses for a detaching owner, "thread {} has no live
event owner", so the registry's two refusal paths read the same to a caller.

### 6. Quiesce on registry shutdown

`take_for_shutdown` returns `HarnessAndDriver` instead of the harness alone. `shutdown` quiesces
each driver before joining the harness shutdowns:

```text
for each (project, harness, driver): let _ = driver.quiesce().await   // Err means the driver is gone
join_all(harness.shutdown() with timeout)
drop the driver handles
close_and_wait background tasks
```

Quiesce returns only when no admission is in flight, so no thread file is created and no owner is
attached after this point. A quiesce error means the driver already exited; that is not a shutdown
failure. The order keeps `close_and_wait` last, as today.

### 7. A maximum frame length

`read_stdout` bounds each line:

```rust
pub(super) const CODEX_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

let mut limited = (&mut reader).take(CODEX_MAX_FRAME_BYTES as u64 + 1);
let read = limited.read_until(b'\n', &mut buf).await;   // buf: Vec<u8>, converted once
if buf.len() > CODEX_MAX_FRAME_BYTES {
    inbox.append(InboxItem::Fatal(format!("Codex stdout frame exceeded {CODEX_MAX_FRAME_BYTES} bytes")));
    fail_all_waiters(&waiters, "Codex stream produced an oversized frame");
    inbox.close();
    return;
}
```

`AsyncReadExt::take` on `&mut BufReader<R>` implements `AsyncBufRead`, so `read_until` works and
the underlying buffer position is preserved between lines. The bound is generous by design: Codex
frames carry command output and diffs, and the point is to make a runaway child process fail
loudly rather than exhaust memory, not to tune throughput. Non-UTF-8 input goes through the
existing `NonJson` path after a lossy conversion, as before.

Documentation states plainly that `EVENT_LOG_RETAIN_LIMIT` and `CODEX_INBOX_RETAIN_LIMIT` count
entries, that per-entry size is bounded by the frame limit, and that the byte-bounded journal with
spill in the architecture review is the M8 design, not the landed one.

## Every site that changes

| File | Lines | Change |
| --- | --- | --- |
| `registry.rs` | `1492-1560` | Move the snapshot below `quiesce()` |
| `registry.rs` | `1393-1470` | Quiesce each driver before harness shutdown; hold the handles until after the join |
| `registry/project.rs` | `271-278` | `take_for_shutdown` returns `HarnessAndDriver` |
| `registry/driver.rs` | `96-110` | Fields `deferred: VecDeque<DeferredAdmission>`; constants |
| `registry/driver.rs` | `243-291` | Discovery branch gated on `!quiesced`; after `Resume` and after a successful `finish_admission`, start the head of `deferred`; on exit, drop `deferred` with a `warn!` |
| `registry/driver.rs` | `452-461` | Delete the quiesced branch of `begin_discovery` |
| `registry/driver.rs` | `485-540` | `finish_admission_reply` defers failed reply-less admissions with the attempt count |
| `registry/driver.rs` | `391-450` | `begin_link` / `begin_discovery` take an `attempts` argument for the deferred path (default 0) |
| `giskard-harness/src/event_log.rs` | `1-10`, `36-46`, `99-121`, `144-154`, `163-192`, `225-234` | `unreported_evictions`, `Cursor { next, pending_gap }`, doc amendment |
| `registry/event_forwarder.rs` | `870-884` | `biased;` and branch order cancel, stream, answer, intents |
| `registry/event_forwarder.rs` | `908-930` | Cancel check first in `admit_intent` |
| `giskard-harness-codex/src/transport.rs` | `25`, `391-432` | `CODEX_MAX_FRAME_BYTES`, bounded `read_until`, oversized frame is `Fatal` |

Expected non-test delta: about 150 lines added, 20 removed. Well under budget.

## Tests

### Driver (`driver.rs`)

1. `delete_project_detaches_an_owner_admitted_during_quiesce` (registry test, using the driver's
   gated claim): start a link admission and hold the claim gate; call `delete_project` in a task;
   release the gate; assert the child's coordinator is gone and `runtime.has_active_turn` is false
   for it after deletion returns.
2. `a_quiesced_driver_refuses_links_and_leaves_discoveries_unconsumed` (rewrite of `:1646`):
   quiesce; announce; assert no file and `discovery_records_processed` unchanged; resume; assert
   the orphan file appears.
3. `a_failed_discovery_is_retried_after_the_next_successful_admission`: make the store fail
   creation once (the driver test store already supports a corrupted-file trick; add a
   `fail_next_create` hook on the test harness's store wrapper or corrupt the project file for the
   first attempt); announce; assert no file; run a successful link admission; assert the orphan file
   now exists and `attempts` was 1 (observable through the log line or a `cfg(test)` counter).
4. `a_failed_discovery_is_dropped_after_three_attempts`: persistent failure; three successful
   unrelated admissions; assert the deferred queue is empty and no file exists.
5. `a_failed_link_with_a_reply_is_not_deferred`: reply receives the error; queue stays empty.
6. `a_resume_retries_deferred_admissions`.
7. `registry_shutdown_quiesces_drivers_before_harness_shutdown` (registry test): gate a claim,
   call `shutdown`, release the gate; assert the harness's `shutdown` was called after the
   admission finished (order recorded on the test harness) and no coordinator was attached after
   it.

### Event log (`event_log.rs`)

8. `evictions_before_the_first_reader_are_reported_as_a_gap`: cap 2, append 5, then create the
   reader; first `recv` is `Gap { dropped: 3 }`, then "3", "4".
9. `evictions_between_readers_are_reported_to_the_next_reader`: reader consumes, drops; append
   past the cap; new reader gets the gap.
10. `a_second_reader_created_without_an_intervening_append_gets_no_gap`.

### Forwarder (`event_forwarder.rs`)

11. `a_retained_event_is_processed_before_a_queued_intent`: append an external turn's
    `TurnStarted` to the log before sending an intent; both are ready when the forwarder is polled;
    assert the intent is refused busy and the turn is labelled external. Deterministic under
    `biased`: construct the forwarder, append, send the intent, then start polling.
12. `an_intent_delivered_after_detach_began_is_refused`: extend `running_intent_forwarder` to
    return the coordinator; keep a clone of the intent sender; call `request_detach`; send the
    intent through the clone; assert the reply is the "no live event owner" error and the harness
    saw no call.

### Transport (`transport.rs`)

13. `an_oversized_frame_is_fatal`: write a line of `CODEX_MAX_FRAME_BYTES + 1` bytes; the inbox
    yields `Fatal` and closes; waiters fail.
14. `a_frame_at_the_limit_is_accepted`.

### Existing tests that must keep passing unchanged

All M4, M5 and M6 driver and forwarder tests except the one rewritten above; the e2e suite; the
M0 scenario tests.

## Documentation

- `docs/event-pipeline-milestones.md`: M7 section as landed; M8 keeps the cursor-committed
  persistence text; ordering diagram updated.
- `docs/subagents.md` deletion paragraph: "project deletion quiesces the driver, then takes the
  authoritative owner set".
- `specs/giskard-specification.md`: amendment 1.84 "lifecycle fences": deletion and shutdown
  quiesce admission before touching the harness; a quiesced driver holds its discovery cursor; a
  failed reply-less admission is retried after the next successful one, at most three times; the
  retained log reports evictions that happened with no reader to the next reader; an intent is
  never admitted once its owner is cancelled; frames are bounded in bytes.
- `crates/giskard-harness/src/event_log.rs` module doc as in section 4.
- `docs/m3-single-stdout-reader.md` and `docs/m1-retained-event-log.md`: one sentence each that
  the caps count entries and the frame bound is `CODEX_MAX_FRAME_BYTES`.
- `docs/event-pipeline-architecture-review.md` §6.4: a note that the landed logs are count-bounded
  with a per-frame byte limit; the byte-bounded spill journal is M8.
- `AGENTS.md`: extend the driver rule with "deletion and shutdown quiesce the driver before the
  owner set is taken or the harness is shut down".

## Order of work

1. Fences: sections 1, 2 and 6, with tests 1, 2 and 7. One commit.
2. Contracts: sections 4 and 5, with tests 8 to 12. One commit.
3. Deferred admission: section 3, with tests 3 to 6. One commit.
4. Frame bound: section 7, with tests 13 and 14. One commit.
5. Docs.

Each commit compiles and passes on its own.

## Verification the implementer must perform and record

- `grep -n 'coordinator_snapshot' crates/giskard-server/src/registry.rs` shows the call in
  `delete_project` after the quiesce.
- `grep -n 'biased' crates/giskard-server/src/registry/event_forwarder.rs` shows exactly one
  occurrence, in `run`.
- `grep -rn 'tokio::time::sleep\|Duration::from' crates/giskard-server/src/registry/driver.rs`
  shows nothing outside tests. No timer entered the driver.
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace` with zero ignored tests.
- Manual run against real Codex: delete a project while a sub-agent is being spawned; stop the
  server mid-turn; both complete without a panic or a stray thread directory.

## Pitfalls

- `biased` changes fairness, not correctness, except for the intent branch, which is the point.
  Keep cancel first; a busy stream must never delay detach.
- The deferred retry must be started from the loop body after a successful `finish_admission`
  or a `Resume`, never from inside `finish_admission_reply`, and never immediately after a
  failure. Starting it after a failure is a spin.
- Do not clear `deferred` on `Quiesce`; a failed deletion must be able to resume them.
- `take` on the reader must be re-created per line so the limit applies per frame, not
  cumulatively.
- The per-reader `pending_gap` must be reported before `Closed`: a reader over a closed log that
  evicted while unread should still learn about the eviction.
- Moving the snapshot after quiesce means a project with no harness slot still needs the
  snapshot; keep the `None` arm.

## Stop rules

Stop and re-cut if:

- a timer, sleep or backoff appears anywhere in the driver, forwarder or log;
- a keyed map appears on the driver (the deferred queue is a `VecDeque`, not a map);
- the harness trait or the adapter's mapper needs a change;
- `admission::admit` needs more than an attempt count for logging;
- non-test lines exceed the budget.
