# S3 — A `DriverEvent` seam in place of test counters

Implementation plan for step 3 of [`design-straightening-review.md`](design-straightening-review.md)
(finding A). Written against `main` at `967a00e` (S2 merged); every file and line reference below
was checked against that tree. Re-check them if the branch has moved.

## Goal

Replace the five `#[cfg(test)] AtomicUsize` counters on `RegistryShared`, and the six
`fetch_add` sites in the driver that feed them, with one observation seam: the driver reports
each decision it makes as a `DriverEvent` value, and tests await the event they care about instead
of polling a count. Production behaviour, logging, and the driver's public API do not change.

## Non-goals

- No change to any log line, level, or field. Every existing `debug!`/`warn!` in the driver stays
  where it is. Turning the seam into a production trace is a possible later step, not this one.
- No change to the forwarder, the registry's public API, `RegistryShared`'s non-test fields, or
  any test outside `registry/driver.rs` and `registry.rs`.
- No change to what any test asserts about the store, the coordinator slot, or the harness fakes.
  Only the *synchronisation* moves from counters to events.
- No timers. The probe's wait is a bounded `timeout` around an awaited channel receive, the same
  bound `wait_until` uses today.

## Ground truth

| Fact | Where |
| --- | --- |
| Five counters on `RegistryShared`, each `#[cfg(test)]`: `discovery_records_processed`, `link_admissions_processed`, `deferred_link_requeues`, `failed_owner_removals_warned`, `teardown_owner_exits` | `crates/giskard-server/src/registry.rs:283-292`; initialised `:426-435` |
| `registry.rs` already imports `AtomicUsize`, `Ordering`, and `tokio::sync::mpsc` | `registry.rs:3, 10` |
| Six increment sites in the driver, all under `#[cfg(test)]`: `deferred_link_requeues` in the two keep branches of `begin_link` (`:481-485`, `:499-503`); `discovery_records_processed` and `link_admissions_processed` at the top of the two arms of `finish_admission_reply` (`:624-628`, `:645-649`); `teardown_owner_exits` and `failed_owner_removals_warned` in the two `ClearFailed` branches of `owner_exited` (`:753-757`, `:764-768`) | `crates/giskard-server/src/registry/driver.rs` |
| The driver's production code has 9 `#[cfg(test)]` items: the six above plus `DriverHandle::disconnected` `:128-129`, `DriverHandle::responsive_for_test` `:137-138`, and the `mod tests` attribute `:799-800` | verified by scanning lines before `mod tests` |
| Decision sites with no counter today: attach refused under quiesce `:337-343`; link with a caller refused under quiesce `:441-446`; reply-less link deferred under quiesce `:447-451`; link with a caller refused for a parent without a live owner `:463-468`; reply-less link dropped because the parent file is gone `:489-492`; discovery dropped because the harness is gone `:557-561`; deferred queue crossing the warn threshold `:693-696` | `driver.rs` |
| `HarnessError` derives `Clone` | `crates/giskard-core/src/error.rs:7` |
| `Admission::native_thread_id()` exists; `AdmissionSource::Link` carries `native_thread_id: String`, `parent_thread_id`, `item_id`, `origin` | `registry/admission.rs:25-32`; `driver.rs:76-91` |
| Every test that reads a counter lives in `driver.rs`'s `mod tests` (54 field accesses across 21 tests: 38 `wait_until` on a counter, 9 direct `assert_eq!`/`load` reads) plus one helper in `registry.rs` tests, `wait_for_discovery_records` (`:2540-2552`, one field access) | measured on the base tree |
| Driver tests get their `Arc<RegistryShared>` from `setup()` (`driver.rs:945-969`), which spawns the driver; registry tests reach it as `registry.shared` | |
| Three tests build `ProjectEventDriver` as a struct literal (`:1257`, `:1350`, `:1898`); the struct gains no field in this step, so they do not change | |
| `wait_until` is a 2 s `timeout` around a `yield_now` loop | `driver.rs:1104-1112` |

## Design

### D1. The event type

In `driver.rs`, compiled unconditionally (it documents the driver's decisions and costs nothing):

```rust
/// One decision the project event driver made, reported for observation.
///
/// Emitted at the point the decision is final and after any state it changed has been written,
/// so an observer that sees the event may read the driver's effects.
#[derive(Debug, Clone)]
pub(super) enum DriverEvent {
    /// An admission ran to completion, successfully or not.
    AdmissionFinished {
        kind: AdmissionKind,
        native_thread_id: String,
        outcome: Result<Option<ThreadId>, HarnessError>,
    },
    /// A reply-less link was kept in the deferred queue instead of being admitted.
    LinkDeferred {
        native_thread_id: String,
        reason: DeferReason,
    },
    /// A reply-less link was discarded because its parent's thread file is gone.
    LinkDropped { native_thread_id: String },
    /// An attach or a link with a caller was refused because the driver is quiesced, or a link
    /// with a caller was refused because its parent has no live owner.
    Refused { subject: RefusedSubject },
    /// A live owner exited and the driver settled its coordinator.
    OwnerExited {
        thread_id: ThreadId,
        reason: ForwarderExitReason,
        disposition: OwnerExitDisposition,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AdmissionKind { Discovery, Link }

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DeferReason {
    /// The driver is quiesced; the link waits for a resume.
    Quiesced,
    /// The parent is detaching, failed, or not attached, and its thread file exists.
    ParentNotLive,
    /// The parent's thread file could not be read; the error text is kept for diagnostics.
    ParentUnreadable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RefusedSubject {
    AttachWhileQuiesced { thread_id: ThreadId },
    LinkWhileQuiesced { native_thread_id: String },
    LinkWithoutLiveParent { native_thread_id: String, parent_thread_id: ThreadId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OwnerExitDisposition {
    /// A requested detach completed.
    Detached,
    /// The stream ended without a turn while the driver was quiesced (teardown).
    TeardownExit,
    /// An unexpected exit; the slot was cleared so the thread can be reopened.
    FailedRemoved,
    /// A persistence-blocked owner, retained as failed.
    Retained,
}
```

### D2. The sink and the probe

On `RegistryShared`, one field replaces five:

```rust
#[cfg(test)]
driver_events: DriverEventSink,
```

with, in `driver.rs`:

```rust
#[cfg(test)]
#[derive(Default)]
pub(super) struct DriverEventSink(std::sync::Mutex<Option<mpsc::UnboundedSender<DriverEvent>>>);

#[cfg(test)]
impl DriverEventSink {
    /// Start observing. Replaces any earlier observer; events emitted before this call are not
    /// replayed, so call it before triggering the behaviour under test.
    pub(super) fn observe(&self) -> DriverProbe { /* new unbounded channel, store the sender */ }
    fn emit(&self, event: DriverEvent) { /* send if an observer is installed; ignore a closed receiver */ }
}
```

The driver reports through one method:

```rust
impl ProjectEventDriver {
    fn observe(&self, event: DriverEvent) {
        #[cfg(test)]
        self.shared.driver_events.emit(event);
        #[cfg(not(test))]
        let _ = event;
    }
}
```

`DriverProbe` is test-only and wraps the receiver:

```rust
#[cfg(test)]
pub(super) struct DriverProbe(mpsc::UnboundedReceiver<DriverEvent>);

#[cfg(test)]
impl DriverProbe {
    /// The next event matching `pred`, discarding non-matching events before it. Panics after
    /// 2 s, printing the events it discarded.
    pub(super) async fn expect(&mut self, pred: impl Fn(&DriverEvent) -> bool) -> DriverEvent;
    /// Every event currently queued, without waiting. Use after a driver round trip (a reply to
    /// quiesce, resume, detach, attach, or link) to assert that something did *not* happen: the
    /// driver processes commands in order, so every event from earlier work has been sent by
    /// the time the reply arrives.
    pub(super) fn drain(&mut self) -> Vec<DriverEvent>;
}
```

Why a sink on `RegistryShared` and not a field on the driver: the registry tests spawn drivers
through `get_or_create_harness`, and `setup()` in the driver tests spawns before the test body
runs; a sink the test installs afterwards through the `Arc<RegistryShared>` it already holds
serves both without changing any constructor or the three struct-literal tests.

### D3. Emission sites

Each site emits after its existing log statement and after its state change, with the counter
increment removed where one existed:

| Site | Event |
| --- | --- |
| `attach`, quiesced refusal `:337-343` | `Refused { AttachWhileQuiesced { thread_id } }` (thread id from `attach.binding.handle.thread`, read before the reply is sent) |
| `begin_link`, quiesced with caller `:441-446` | `Refused { LinkWhileQuiesced { native_thread_id } }` |
| `begin_link`, quiesced reply-less `:447-451` | `LinkDeferred { reason: Quiesced }` |
| `begin_link`, not live with caller `:463-468` | `Refused { LinkWithoutLiveParent { .. } }` |
| `begin_link`, `Ok(Some)` `:477-485` | `LinkDeferred { reason: ParentNotLive }`; counter removed |
| `begin_link`, `Ok(None)` `:489-492` | `LinkDropped` |
| `begin_link`, `Err` `:494-503` | `LinkDeferred { reason: ParentUnreadable(error.to_string()) }`; counter removed |
| `finish_admission_reply`, discovery arm `:623-635` | `AdmissionFinished { kind: Discovery, native_thread_id: retry.native_thread_id().to_owned(), outcome: result.clone() }` emitted **after** `defer_admission` has run; counter removed |
| `finish_admission_reply`, link arm `:636-659` | `AdmissionFinished { kind: Link, .. }` emitted after the reply is sent or the retry deferred; counter removed |
| `owner_exited`, `Detached` `:743-749` | `OwnerExited { disposition: Detached }` after `retry_parked` |
| `owner_exited`, `ClearFailed` teardown `:752-762` | `OwnerExited { disposition: TeardownExit }`; counter removed |
| `owner_exited`, `ClearFailed` otherwise `:763-773` | `OwnerExited { disposition: FailedRemoved }`; counter removed |
| `owner_exited`, `RetainFailed` `:775` | `OwnerExited { disposition: Retained }` |

Ordering rule, stated once in the enum doc and honoured at every site: the event is the last
thing a branch does before returning or continuing, so a test that receives it may inspect the
store, the coordinator slot, and the deferred-queue effects without a further wait. This is what
lets the counter-based `wait_until` + `assert_eq!` pairs become a single `expect`.

The `begin_discovery` harness-gone branch (`:557-561`) and the queue threshold warning
(`:693-696`) get no event: neither has a test, and adding events without a consumer is scope
creep.

### D4. Test migration

Add to the driver test module:

```rust
fn observe(shared: &RegistryShared) -> DriverProbe { shared.driver_events.observe() }
fn finished(kind: AdmissionKind, native: &str) -> impl Fn(&DriverEvent) -> bool + '_ { .. }
fn deferred(native: &str) -> impl Fn(&DriverEvent) -> bool + '_ { .. }
fn exited(disposition: OwnerExitDisposition) -> impl Fn(&DriverEvent) -> bool { .. }
```

and convert each of the 21 tests by this table:

| Today | After |
| --- | --- |
| `wait_until(\|\| shared.discovery_records_processed.load(..) == n)` after an `announce` | `probe.expect(finished(Discovery, "<native id>")).await` for that announcement |
| `wait_until(\|\| shared.link_admissions_processed.load(..) == n)` after a link | `probe.expect(finished(Link, "<native id>")).await` |
| `wait_until(\|\| shared.deferred_link_requeues.load(..) >= 1)` | `probe.expect(deferred("<native id>")).await` |
| `wait_until(\|\| shared.teardown_owner_exits.load(..) == 1)` / `failed_owner_removals_warned` | `probe.expect(exited(TeardownExit)).await` / `exited(FailedRemoved)` |
| `assert_eq!(shared.<counter>.load(..), n)` after a quiesce/resume/detach/attach reply | `assert!(!probe.drain().iter().any(<same predicate>))` |

Where a test today counts admissions of *several* native ids through one counter
(`the_deferred_queue_keeps_every_distinct_native_id`, `a_failed_discovery_is_never_dropped`,
`deferred_admissions_are_deduplicated_by_native_id`), it expects one `AdmissionFinished` per
native id it announced, which is stricter than the count and reads as the intent. The dedup test
additionally asserts that exactly one `AdmissionFinished` for `"duplicate-native"` follows the
trigger, using `drain` after the attach reply.

`registry.rs` tests: `wait_for_discovery_records(registry, expected)` (`:2540-2552`) becomes a
probe obtained from `registry.shared.driver_events.observe()` before the announcement, awaiting
`finished(Discovery, ..)` once per expected record. It has one caller (`:2529` area); update it.

No test's assertions about files, coordinators, claims, or replies change. The `is_teardown_exit`
unit test and the three struct-literal tests are untouched.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/registry.rs:283-292, 426-435` | Five counter fields and inits → `#[cfg(test)] driver_events: DriverEventSink` (default) |
| `registry.rs:3` | Drop `AtomicUsize` from the import only if nothing else uses it (`RegistryTaskTracker::count` at `:141` does; keep it) |
| `registry.rs:2540-2552` and its caller | `wait_for_discovery_records` → probe |
| `crates/giskard-server/src/registry/driver.rs` (top) | `DriverEvent` and its three companion enums; `DriverEventSink`, `DriverProbe` under `#[cfg(test)]` |
| `driver.rs:337-343, 441-451, 463-503, 623-659, 743-775` | Emission sites per D3; six counter increments deleted |
| `driver.rs` `mod tests` | Four helpers; 21 tests converted per D4 |
| `docs/design-straightening-review.md` | Mark A (step 3) landed |

## Tests

Existing tests are the specification. Two additions pin the seam itself:

1. `driver_events_are_emitted_after_their_effects`: announce a discovery that succeeds; on
   `AdmissionFinished`, assert the thread file exists and its owner is installed *without* any
   further wait. Then close the owner's log; on `OwnerExited { FailedRemoved }`, assert the
   coordinator slot is already empty.
2. `a_refused_attach_is_observable`: quiesce, attach → `Refused { AttachWhileQuiesced }` arrives
   and the attach reply is the "project is being deleted" error (covers the one site that had no
   test signal before).

## Order of work

1. Add the enums, sink, probe, and `observe` with no emission sites; add the field to
   `RegistryShared`; keep the counters for the moment. `cargo check -p giskard-server`.
2. Add emissions at the thirteen sites of D3, still keeping the counters. Run the driver tests:
   unchanged and green.
3. Convert the tests per D4, one at a time, deleting each counter's `fetch_add` when its last
   reader is gone. Run the driver module after each counter is retired.
4. Delete the five fields; convert `wait_for_discovery_records`.
5. `cargo test -p giskard-server`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo fmt --check`, and the driver module ten times in a row.

Expected size: about 160 lines added (enums, sink, probe, helpers), about 90 deleted, plus the
test conversions, which are line-for-line.

## Exit checks

Validated on the base tree: the first three commands match 10, 1, and 54 lines today and 6
`fetch_add` sites respectively; all must match nothing afterwards. The fourth reports 9 today
and must report 3 (the two `DriverHandle` test constructors and the `mod tests` attribute).

```sh
grep -nE "^\s*(discovery_records_processed|link_admissions_processed|deferred_link_requeues|failed_owner_removals_warned|teardown_owner_exits): AtomicUsize" crates/giskard-server/src/registry.rs
grep -nE "\.(discovery_records_processed|link_admissions_processed|deferred_link_requeues|failed_owner_removals_warned|teardown_owner_exits)\b" crates/giskard-server/src/registry.rs crates/giskard-server/src/registry/driver.rs
grep -n "fetch_add(1, std::sync::atomic::Ordering::SeqCst)" crates/giskard-server/src/registry/driver.rs
awk '/^mod tests/{exit} /#\[cfg\(test\)\]/{c++} END{print c}' crates/giskard-server/src/registry/driver.rs
```

## Pitfalls

- Emit after effects, never before. A test that receives `AdmissionFinished` and immediately reads
  the store must find the file; a test that receives `OwnerExited` must find the slot cleared.
  The D3 table names the last statement of each branch for that reason.
- `expect` must discard non-matching events rather than fail on them: the driver emits
  `Refused`, `LinkDeferred`, and `OwnerExited { Detached }` in flows where the test only cares
  about one of them, and a stricter probe would turn every new event into a test change.
- `drain` is only valid after a driver round trip. Using it right after `announce`, which does
  not reply, would race the driver; keep those cases on `expect`.
- Install the probe before the action it observes. The sink does not replay.
- Do not add `AdmissionFinished` for admissions that never start (harness gone, parent not
  live): those are `Refused`/`LinkDeferred`/`LinkDropped`, and a test that waited for
  `AdmissionFinished` there would hang, exactly as the old counter would not have moved.
- Keep the log statements byte-for-byte; S3 adds a seam beside them, it does not replace them.

## Stop rules

Stop and re-cut if the diff:

- changes a log statement, a reply value, or any assertion about persisted or runtime state;
- adds a field to `ProjectEventDriver` or a parameter to `spawn_project_event_driver`;
- makes `DriverEventSink` or `DriverProbe` available outside `cfg(test)`;
- leaves any `wait_until` polling a counter, or introduces a sleep.
