# S3 — A `DriverEvent` seam in place of test counters

Implementation plan for step 3 of [`design-straightening-review.md`](design-straightening-review.md)
(finding A). Written against `main` at `967a00e` (S2 merged); every file and line reference below
was checked against that tree. Re-check them if the branch has moved.

Revision 2. The first cut kept every log line at its site and made the production side of the
seam a no-op, which leaves every event field unread outside `cfg(test)` and fails
`clippy -D warnings` with `dead_code`. This revision follows finding A as written: in production
the seam *is* where the driver's decisions become log lines, so every field is read in every
build and no lint attribute is needed.

## Goal

Replace the five `#[cfg(test)] AtomicUsize` counters on `RegistryShared`, and the six
`fetch_add` sites in the driver that feed them, with one observation seam: the driver reports
each decision it makes as a `DriverEvent` value. In production, `DriverEvent::log` turns the
value into the log line that site emits today. In tests, the same value is also sent to a probe,
and tests await the event they care about instead of polling a count. Production behaviour and
the driver's public API do not change. The eight existing log lines keep their level, message,
field names, field order, and field formatting; only their location moves.

## Non-goals

- No change to the level, message, field names, field order, or field formatting of any of the
  eight log lines that move (listed in D2). Three decisions that are silent today gain a `debug!`
  line, also listed in D2; nothing else in the driver's logging changes.
- No change to the forwarder beyond adding `Debug` to `ForwarderExitReason`'s derive list, and no
  change to the registry's public API, `RegistryShared`'s non-test fields, or any test outside
  `registry/driver.rs` and `registry.rs`.
- No change to what any test asserts about the store, the coordinator slot, or the harness fakes.
  Only the *synchronisation* moves from counters to events.
- No timers. The probe's wait is a bounded `timeout` around an awaited channel receive, the same
  bound `wait_until` uses today.
- No lint attributes. `dead_code` is avoided by construction: every event field is read by
  `DriverEvent::log`, which compiles in every build.

## Ground truth

| Fact | Where |
| --- | --- |
| Five counters on `RegistryShared`, each `#[cfg(test)]`: `discovery_records_processed`, `link_admissions_processed`, `deferred_link_requeues`, `failed_owner_removals_warned`, `teardown_owner_exits` | `crates/giskard-server/src/registry.rs:283-292`; initialised `:426-435` |
| `registry.rs` already imports `AtomicUsize`, `Ordering`, and `tokio::sync::mpsc` | `registry.rs:3, 10` |
| Six increment sites in the driver, all under `#[cfg(test)]`: `deferred_link_requeues` in the two keep branches of `begin_link` (`:481-485`, `:499-503`); `discovery_records_processed` and `link_admissions_processed` at the top of the two arms of `finish_admission_reply` (`:624-628`, `:645-649`); `teardown_owner_exits` and `failed_owner_removals_warned` in the two `ClearFailed` branches of `owner_exited` (`:753-757`, `:764-768`) | `crates/giskard-server/src/registry/driver.rs` |
| The driver's production code has 9 `#[cfg(test)]` items: the six above plus `DriverHandle::disconnected` `:128-129`, `DriverHandle::responsive_for_test` `:137-138`, and the `mod tests` attribute `:799-800` | verified by scanning lines before `mod tests` |
| Eight log lines sit at the decision sites and move into the seam: `warn!` `:464-465`, `debug!` `:479-481`, `warn!` `:489-491`, `warn!` `:496-499`, `warn!` `:630-633`, `warn!` `:651-654`, `debug!` `:758-763`, `warn!` `:769-774`. Each message string occurs exactly once in `driver.rs` and nowhere else in `crates/` | `grep -c` per message, run on the base tree |
| Decision sites with no log and no counter today: attach refused under quiesce `:337-343`; link with a caller refused under quiesce `:441-446`; reply-less link deferred under quiesce `:447-451`; owner exit `Detached` `:743-749`; owner exit `RetainFailed` `:775` | `driver.rs` |
| Other driver logs that do **not** move: `error!` on a stream gap `:303`, `warn!` on stream end `:326`, `debug!` "installed long-lived native event owner" `:436`, `warn!` discovery dropped because the harness is gone `:559-560`, `warn!` deferred-queue threshold `:693-694` | `driver.rs` |
| `tracing-subscriber`'s fmt layer records a bare `&str` field through `record_str`, which forwards to `record_debug` and therefore prints it quoted; a `%value` field prints its `Display` output unquoted. So today `native_thread_id = retry.native_thread_id()` (`:631`) and every `origin` field print quoted, while `%native_thread_id`, `%parent_thread_id`, `%item_id`, `%thread_id`, and `%error` print unquoted | `tracing-subscriber-0.3.23/src/fmt/format/mod.rs:1264-1273` |
| `Link` fields: `parent_thread_id: ThreadId`, `spawned_by_turn_id`, `item_id: ItemId`, `origin: &'static str`, `info: SubagentActivityInfo` (with `native_thread_id: String`), `reply: Option<oneshot::Sender<..>>` | `driver.rs:52-59` |
| `AdmissionSource::Link` carries `retry`, `attempts`, `reply`, `parent_thread_id`, `item_id`, `origin`, `native_thread_id: String` | `driver.rs:76-91` |
| `Admission::native_thread_id(&self) -> &str` | `registry/admission.rs:26-31` |
| `HarnessError` derives `Debug, Clone`; `ProjectId`, `ThreadId`, `ItemId` derive `Debug, Clone, Copy` | `giskard-core/src/error.rs:7`; `giskard-core/src/ids.rs:6,10,22` |
| `ForwarderExitReason` derives `Clone, Copy, PartialEq, Eq` and **not** `Debug`; `DriverEvent`'s `#[derive(Debug)]` needs it, so `Debug` is added to that derive list | `registry/event_forwarder.rs:637-638` |
| `forwarder_exit_reason_label(ForwarderExitReason) -> &'static str` lives in `event_forwarder.rs` and is already imported by the driver | `registry/event_forwarder.rs:651`; `driver.rs:15-17` (used at `:761`, `:772`) |
| Every test that reads a counter lives in `driver.rs`'s `mod tests` (54 field accesses across 21 tests: 38 `wait_until` on a counter, 9 direct `assert_eq!`/`load` reads) plus one helper in `registry.rs` tests, `wait_for_discovery_records` (`:2540-2552`, one field access) | measured on the base tree |
| Driver tests get their `Arc<RegistryShared>` from `setup()` (`driver.rs:977-1003`), which spawns the driver; registry tests reach it as `registry.shared` | |
| Three tests build `ProjectEventDriver` as a struct literal (`:1257`, `:1350`, `:1898`); the struct gains no field in this step, so they do not change | |
| `wait_until` is a 2 s `timeout` around a `yield_now` loop | `driver.rs:1132-1140` |
| No test in `crates/` asserts on any of the eight log messages | `grep` across `crates/` finds each message only at its site |

## Design

### D1. The event type

In `driver.rs`, compiled unconditionally. Every field is one the log line for that decision
prints; `project_id` is not carried because the driver adds it when logging.

```rust
/// One decision the project event driver made.
///
/// Reported through `ProjectEventDriver::observe` at the point the decision is final and after
/// any state it changed has been written, so an observer that sees the event may read the
/// driver's effects. In production the event becomes the log line for that decision
/// (`DriverEvent::log`); under `cfg(test)` it is also delivered to the test probe.
#[derive(Debug, Clone)]
pub(super) enum DriverEvent {
    /// A discovered native thread's admission ran to completion, successfully or not.
    DiscoveryFinished {
        native_thread_id: String,
        attempts: u32,
        outcome: Result<Option<ThreadId>, HarnessError>,
    },
    /// A linked native thread's admission ran to completion, successfully or not.
    LinkFinished {
        native_thread_id: String,
        parent_thread_id: ThreadId,
        item_id: ItemId,
        origin: &'static str,
        attempts: u32,
        outcome: Result<Option<ThreadId>, HarnessError>,
    },
    /// A reply-less link was kept in the deferred queue instead of being admitted.
    LinkDeferred {
        native_thread_id: String,
        parent_thread_id: ThreadId,
        origin: &'static str,
        reason: DeferReason,
    },
    /// A reply-less link was discarded because its parent's thread file is gone.
    LinkDropped {
        native_thread_id: String,
        parent_thread_id: ThreadId,
        origin: &'static str,
    },
    /// An attach or a link with a caller was refused.
    Refused { subject: RefusedSubject },
    /// A live owner exited and the driver settled its coordinator.
    OwnerExited {
        thread_id: ThreadId,
        reason: ForwarderExitReason,
        disposition: OwnerExitDisposition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DeferReason {
    /// The driver is quiesced; the link waits for a resume.
    Quiesced,
    /// The parent is detaching, failed, or not attached, and its thread file exists.
    ParentNotLive,
    /// The parent's thread file could not be read; the error text is kept for the log.
    ParentUnreadable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RefusedSubject {
    /// An attach arrived while the driver was quiesced.
    AttachWhileQuiesced { thread_id: ThreadId },
    /// A link with a caller arrived while the driver was quiesced.
    LinkWhileQuiesced {
        native_thread_id: String,
        parent_thread_id: ThreadId,
        origin: &'static str,
    },
    /// A link with a caller named a parent that has no live owner.
    LinkWithoutLiveParent {
        parent_thread_id: ThreadId,
        origin: &'static str,
    },
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

`LinkWithoutLiveParent` carries no `native_thread_id` because the existing warn line does not
print one, and adding a field to that line is out of scope. Tests match it on
`parent_thread_id`.

### D2. The production side: `DriverEvent::log`

```rust
impl DriverEvent {
    /// The log line for this decision. Levels, messages, field names, field order, and field
    /// formatting are those of the sites the lines moved from.
    fn log(&self, project_id: ProjectId) { match self { .. } }
}
```

The arms, with the exact macro call each one makes. The recording form of every field (`%x`
for `Display`, bare for `&str`, `= expr` for a computed value) is the form the site uses today,
so the rendered text is unchanged:

| Arm | Call |
| --- | --- |
| `DiscoveryFinished { outcome: Err(error), .. }` | `warn!(project_id = %project_id, native_thread_id = native_thread_id.as_str(), %error, attempt = attempts.saturating_add(1), "failed to admit discovered native thread")` (from `:630-633`) |
| `DiscoveryFinished { outcome: Ok(_), .. }` | nothing (silent today; the installed owner is logged at `:436`) |
| `LinkFinished { outcome: Err(error), .. }` | `warn!(project_id = %project_id, %parent_thread_id, %item_id, origin, %error, %native_thread_id, attempt = attempts.saturating_add(1), "failed to admit linked native thread")` (from `:651-654`) |
| `LinkFinished { outcome: Ok(_), .. }` | nothing (silent today) |
| `LinkDeferred { reason: Quiesced, .. }` | **new** `debug!(project_id = %project_id, %parent_thread_id, %native_thread_id, origin, "deferring native identity link until the project resumes")` |
| `LinkDeferred { reason: ParentNotLive, .. }` | `debug!(project_id = %project_id, %parent_thread_id, %native_thread_id, origin, "deferring native identity link until its parent has a live owner")` (from `:479-481`) |
| `LinkDeferred { reason: ParentUnreadable(error), .. }` | `warn!(project_id = %project_id, %parent_thread_id, %native_thread_id, origin, %error, "keeping deferred native identity link; its parent thread could not be read")` (from `:496-499`; `error` is the stored `String`, whose `Display` is the original error's `Display`) |
| `LinkDropped { .. }` | `warn!(project_id = %project_id, %parent_thread_id, %native_thread_id, origin, "dropping native identity link because its parent thread no longer exists")` (from `:489-491`) |
| `Refused { subject: AttachWhileQuiesced { thread_id } }` | **new** `debug!(project_id = %project_id, %thread_id, "refusing attach because the project is being deleted")` |
| `Refused { subject: LinkWhileQuiesced { .. } }` | **new** `debug!(project_id = %project_id, %parent_thread_id, %native_thread_id, origin, "refusing native identity link because the project is being deleted")` |
| `Refused { subject: LinkWithoutLiveParent { .. } }` | `warn!(project_id = %project_id, %parent_thread_id, origin, "refusing native identity link from a parent without a live owner")` (from `:464-465`) |
| `OwnerExited { disposition: TeardownExit, .. }` | `debug!(project_id = %project_id, %thread_id, exit_reason = forwarder_exit_reason_label(*reason), "event owner ended during project teardown")` (from `:758-763`) |
| `OwnerExited { disposition: FailedRemoved, .. }` | `warn!(project_id = %project_id, %thread_id, exit_reason = forwarder_exit_reason_label(*reason), "removed failed event owner so the thread can be reopened")` (from `:769-774`) |
| `OwnerExited { disposition: Detached \| Retained, .. }` | nothing (silent today) |

Field-order note for the moved lines: today the sites write `parent_thread_id = %link.parent_thread_id`
and `origin = link.origin`; in `log` the bindings are named `parent_thread_id` and `origin`, so
the shorthand `%parent_thread_id` and bare `origin` produce the same field names and the same
rendering. The discovery line keeps `native_thread_id = native_thread_id.as_str()` because the
site records a bare `&str` today and a `%` field would render it without quotes.

Why this satisfies `dead_code` with no attribute: every field of every variant is bound and used
in at least one arm of `log` (`Detached | Retained` share the `OwnerExited` fields with the two
logging arms; `Ok(_)` shares the `*Finished` fields with the `Err` arms), and `log` is called
from `observe` in every build.

### D3. The sink, the probe, and `observe`

On `RegistryShared`, one field replaces five:

```rust
#[cfg(test)]
driver_events: driver::probe::DriverEventSink,
```

In `driver.rs`, one test-only module holds both test types. It must be the item immediately
before `mod tests`, with nothing between them: `clippy::items_after_test_module` treats any
`#[cfg(test)]` module as a test module and rejects items that follow it (verified with
`clippy -D warnings` on a scratch crate laid out this way; both the lib and lib-test builds pass):

```rust
#[cfg(test)]
pub(super) mod probe {
    #[derive(Default)]
    pub(in crate::registry) struct DriverEventSink(
        std::sync::Mutex<Option<mpsc::UnboundedSender<DriverEvent>>>,
    );

    impl DriverEventSink {
        /// Start observing. Replaces any earlier observer; events emitted before this call are
        /// not replayed, so call it before triggering the behaviour under test.
        pub(in crate::registry) fn observe(&self) -> DriverProbe { /* new unbounded channel, store the sender */ }
        pub(super) fn emit(&self, event: DriverEvent) { /* send if an observer is installed; ignore a closed receiver */ }
    }

    pub(in crate::registry) struct DriverProbe(mpsc::UnboundedReceiver<DriverEvent>);

    impl DriverProbe {
        /// The next event matching `pred`, discarding non-matching events before it. Panics
        /// after 2 s, printing the events it discarded.
        pub(in crate::registry) async fn expect(&mut self, pred: impl Fn(&DriverEvent) -> bool) -> DriverEvent;
        /// Every event currently queued, without waiting. Use after a driver round trip (a
        /// reply to quiesce, resume, detach, attach, or link) to assert that something did
        /// *not* happen: the driver processes commands in order, so every event from earlier
        /// work has been sent by the time the reply arrives.
        pub(in crate::registry) fn drain(&mut self) -> Vec<DriverEvent>;
    }
}
```

The driver reports through one method, compiled in every build:

```rust
impl ProjectEventDriver {
    fn observe(&self, event: DriverEvent) {
        event.log(self.project_id);
        #[cfg(test)]
        self.shared.driver_events.emit(event);
    }
}
```

Why a sink on `RegistryShared` and not a field on the driver: the registry tests spawn drivers
through `get_or_create_harness`, and `setup()` in the driver tests spawns before the test body
runs; a sink the test installs afterwards through the `Arc<RegistryShared>` it already holds
serves both without changing any constructor or the three struct-literal tests.

### D4. Emission sites

Thirteen sites. At each, the existing log statement (if any) and counter increment (if any) are
deleted and replaced by one `self.observe(..)` call placed after the branch's state change and
reply, so that a test receiving the event may inspect the effects without a further wait:

| Site | Replaces | Emits |
| --- | --- | --- |
| `attach`, quiesced `:337-343` | nothing | `Refused { AttachWhileQuiesced { thread_id } }` (read `attach.binding.handle.thread` before the reply is sent) |
| `begin_link`, quiesced with caller `:441-446` | nothing | `Refused { LinkWhileQuiesced { .. } }` after the reply is sent |
| `begin_link`, quiesced reply-less `:447-451` | nothing | `LinkDeferred { reason: Quiesced }` after `queue_deferred` (clone the three fields out of `link` before it is boxed) |
| `begin_link`, not live with caller `:463-468` | `warn!` `:464-465` | `Refused { LinkWithoutLiveParent { .. } }` after the reply is sent |
| `begin_link`, `Ok(Some)` `:477-485` | `debug!` `:479-481`, counter `:482-485` | `LinkDeferred { reason: ParentNotLive }` after `queue_deferred` |
| `begin_link`, `Ok(None)` `:489-492` | `warn!` `:489-491` | `LinkDropped { .. }` |
| `begin_link`, `Err` `:494-503` | `warn!` `:496-499`, counter `:500-503` | `LinkDeferred { reason: ParentUnreadable(error.to_string()) }` after `queue_deferred` |
| `finish_admission_reply`, discovery arm `:623-635` | counter `:624-628`, `warn!` `:630-633` | `DiscoveryFinished { native_thread_id: retry.native_thread_id().to_owned(), attempts, outcome: result.clone() }` **after** `defer_admission` has run (the `if let Err` keeps deciding whether to defer; it no longer logs) |
| `finish_admission_reply`, link arm `:636-659` | counter `:645-649`, `warn!` `:651-654` | `LinkFinished { .. , outcome: result.clone() }` after the reply is sent or the retry deferred |
| `owner_exited`, `Detached` `:743-749` | nothing | `OwnerExited { disposition: Detached }` after `retry_parked` |
| `owner_exited`, `ClearFailed` teardown `:752-762` | counter `:753-757`, `debug!` `:758-763` | `OwnerExited { disposition: TeardownExit }` |
| `owner_exited`, `ClearFailed` otherwise `:763-773` | counter `:764-768`, `warn!` `:769-774` | `OwnerExited { disposition: FailedRemoved }` |
| `owner_exited`, `RetainFailed` `:775` | nothing | `OwnerExited { disposition: Retained }` |

The `is_teardown_exit` decision stays where it is; only its two logging branches change.
`begin_discovery`'s harness-gone warning (`:559-560`) and the queue threshold warning
(`:693-694`) are not decisions about a single admission and keep their log statements.

Ordering rule, stated once in the enum doc and honoured at every site: `observe` is the last
thing a branch does before returning or continuing. This is what lets the counter-based
`wait_until` + `assert_eq!` pairs become a single `expect`.

One consequence to accept knowingly: at the sites that log today, the log line now appears
after the branch's reply or queue effect instead of before it. Nothing reads log order relative
to those effects.

### D5. Test migration

Add to the driver test module:

```rust
fn observe(shared: &RegistryShared) -> DriverProbe { shared.driver_events.observe() }
fn discovery_finished(native: &str) -> impl Fn(&DriverEvent) -> bool + '_ { .. }
fn link_finished(native: &str) -> impl Fn(&DriverEvent) -> bool + '_ { .. }
fn deferred(native: &str) -> impl Fn(&DriverEvent) -> bool + '_ { .. }
fn exited(disposition: OwnerExitDisposition) -> impl Fn(&DriverEvent) -> bool { .. }
```

and convert each of the 21 tests by this table:

| Today | After |
| --- | --- |
| `wait_until(\|\| shared.discovery_records_processed.load(..) == n)` after an `announce` | `probe.expect(discovery_finished("<native id>")).await` for that announcement |
| `wait_until(\|\| shared.link_admissions_processed.load(..) == n)` after a link | `probe.expect(link_finished("<native id>")).await` |
| `wait_until(\|\| shared.deferred_link_requeues.load(..) >= 1)` | `probe.expect(deferred("<native id>")).await` |
| `wait_until(\|\| shared.teardown_owner_exits.load(..) == 1)` / `failed_owner_removals_warned` | `probe.expect(exited(TeardownExit)).await` / `exited(FailedRemoved)` |
| `assert_eq!(shared.<counter>.load(..), n)` after a quiesce/resume/detach/attach reply | `assert!(!probe.drain().iter().any(<same predicate>))` |

Where a test today counts admissions of *several* native ids through one counter
(`the_deferred_queue_keeps_every_distinct_native_id`, `a_failed_discovery_is_never_dropped`,
`deferred_admissions_are_deduplicated_by_native_id`), it expects one `*Finished` event per
native id it announced, which is stricter than the count and reads as the intent. The dedup test
additionally asserts that exactly one `DiscoveryFinished` for `"duplicate-native"` follows the
trigger, using `drain` after the attach reply.

`registry.rs` tests: `wait_for_discovery_records(registry, expected)` (`:2540-2552`) becomes a
probe obtained from `registry.shared.driver_events.observe()` before the announcement, awaiting
`discovery_finished(..)` once per expected record. It has three callers (`:2992`, `:3031`,
`:3051`); each installs the probe before its announcement and awaits the expected number of
events.

No test's assertions about files, coordinators, claims, or replies change. The `is_teardown_exit`
unit test and the three struct-literal tests are untouched.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/registry/event_forwarder.rs:637` | `#[derive(Clone, Copy, PartialEq, Eq)]` → `#[derive(Debug, Clone, Copy, PartialEq, Eq)]` on `ForwarderExitReason` |
| `crates/giskard-server/src/registry.rs:283-292, 426-435` | Five counter fields and inits → `#[cfg(test)] driver_events: driver::probe::DriverEventSink` (default) |
| `registry.rs:3` | Keep the `AtomicUsize` import: `RegistryTaskTracker::count` at `:141` still uses it |
| `registry.rs:2540-2552` and its three callers | `wait_for_discovery_records` → probe |
| `crates/giskard-server/src/registry/driver.rs` (top) | `DriverEvent`, `DeferReason`, `RefusedSubject`, `OwnerExitDisposition`, and `DriverEvent::log` |
| `driver.rs`, above `mod tests` | `#[cfg(test)] pub(super) mod probe` with `DriverEventSink` and `DriverProbe` |
| `driver.rs` `impl ProjectEventDriver` | `observe` |
| `driver.rs:337-343, 441-451, 463-503, 623-659, 743-775` | Thirteen sites per D4: eight log statements and six counter increments deleted, thirteen `observe` calls added |
| `driver.rs` `mod tests` | Five helpers; 21 tests converted per D5 |
| `docs/design-straightening-review.md` | Mark A (step 3) landed |

## Tests

Existing tests are the specification. Two additions pin the seam itself:

1. `driver_events_are_emitted_after_their_effects`: announce a discovery that succeeds; on
   `DiscoveryFinished`, assert the thread file exists and its owner is installed *without* any
   further wait. Then close the owner's log; on `OwnerExited { FailedRemoved }`, assert the
   coordinator slot is already empty.
2. `a_refused_attach_is_observable`: quiesce, attach → `Refused { AttachWhileQuiesced }` arrives
   and the attach reply is the "project is being deleted" error (covers the one site that had no
   test signal before).

## Order of work

1. Add the four enums, `DriverEvent::log`, the `probe` module, `observe`, and the field on
   `RegistryShared`, with no emission sites yet; keep the counters. `cargo check -p giskard-server`
   passes; `cargo clippy -p giskard-server --all-targets -- -D warnings` reports `DriverEvent` as
   never constructed, which step 2 resolves.
2. Convert the thirteen sites of D4, deleting each site's log statement but keeping its counter
   increment for now. Run the driver tests: unchanged and green. Run
   `cargo clippy --workspace --all-targets -- -D warnings`: clean.
3. Convert the tests per D5, one at a time, deleting each counter's `fetch_add` when its last
   reader is gone. Run the driver module after each counter is retired.
4. Delete the five fields; convert `wait_for_discovery_records`.
5. `cargo test -p giskard-server`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo fmt --check`, and the driver module ten times in a row.

Expected size: about 220 lines added (enums, `log`, probe module, helpers), about 130 deleted,
plus the test conversions, which are line-for-line.

## Exit checks

Validated on the base tree. The first three commands match 10, 1, and 54 lines today and 6
`fetch_add` sites respectively; all must match nothing afterwards.

```sh
grep -nE "^\s*(discovery_records_processed|link_admissions_processed|deferred_link_requeues|failed_owner_removals_warned|teardown_owner_exits): AtomicUsize" crates/giskard-server/src/registry.rs
grep -nE "\.(discovery_records_processed|link_admissions_processed|deferred_link_requeues|failed_owner_removals_warned|teardown_owner_exits)\b" crates/giskard-server/src/registry.rs crates/giskard-server/src/registry/driver.rs
grep -n "fetch_add(1, std::sync::atomic::Ordering::SeqCst)" crates/giskard-server/src/registry/driver.rs
```

The `#[cfg(test)]` count above `mod tests` reports 9 today and must report 5 afterwards: the
two `DriverHandle` test constructors, the `mod tests` attribute, the `probe` module, and the
`emit` statement in `observe`.

```sh
awk '/^mod tests/{exit} /#\[cfg\(test\)\]/{c++} END{print c}' crates/giskard-server/src/registry/driver.rs
```

`self.observe(` occurs 0 times today and must occur exactly 13 times above `mod tests`
afterwards (rustfmt never splits a call's receiver from its opening parenthesis):

```sh
awk '/^mod tests/{exit} /self\.observe\(/{c++} END{print c}' crates/giskard-server/src/registry/driver.rs
```

Each of the eleven messages below must occur exactly once in `driver.rs` afterwards. The first
eight occur once today (at their sites) and the three new ones zero times:

```sh
for m in \
  "refusing native identity link from a parent without a live owner" \
  "deferring native identity link until its parent has a live owner" \
  "dropping native identity link because its parent thread no longer exists" \
  "keeping deferred native identity link; its parent thread could not be read" \
  "failed to admit discovered native thread" \
  "failed to admit linked native thread" \
  "event owner ended during project teardown" \
  "removed failed event owner so the thread can be reopened" \
  "refusing attach because the project is being deleted" \
  "refusing native identity link because the project is being deleted" \
  "deferring native identity link until the project resumes"; do
  printf '%s: ' "$m"; grep -c "$m" crates/giskard-server/src/registry/driver.rs
done
```

## Pitfalls

- Emit after effects, never before. A test that receives `DiscoveryFinished` and immediately
  reads the store must find the file; a test that receives `OwnerExited` must find the slot
  cleared. The D4 table names what each `observe` follows for that reason.
- Keep each moved line's field recording form. `native_thread_id = native_thread_id.as_str()`
  on the discovery line and bare `origin` everywhere print quoted; `%` fields print unquoted.
  Changing the form changes the rendered log.
- `expect` must discard non-matching events rather than fail on them: the driver emits
  `Refused`, `LinkDeferred`, and `OwnerExited { Detached }` in flows where the test only cares
  about one of them, and a stricter probe would turn every new event into a test change.
- `drain` is only valid after a driver round trip. Using it right after `announce`, which does
  not reply, would race the driver; keep those cases on `expect`.
- Install the probe before the action it observes. The sink does not replay.
- Do not add a `*Finished` event for admissions that never start (harness gone, parent not
  live): those are `Refused`/`LinkDeferred`/`LinkDropped`, and a test that waited for a
  `*Finished` event there would hang, exactly as the old counter would not have moved.
- Keep the `probe` module directly above `mod tests`. Placing it higher trips
  `clippy::items_after_test_module` on every item below it.
- Do not reach for `#[allow(dead_code)]` or `cfg_attr`. If clippy reports an unread field, a
  `log` arm is missing a use of it; fix the arm.

## Stop rules

Stop and re-cut if the diff:

- changes the level, message, field names, field order, or field formatting of any of the
  eight moved log lines, a reply value, or any assertion about persisted or runtime state;
- adds a log line other than the three named in D2;
- adds a field to `ProjectEventDriver` or a parameter to `spawn_project_event_driver`;
- makes `DriverEventSink` or `DriverProbe` available outside `cfg(test)`;
- adds any `allow` or `cfg_attr` attribute;
- leaves any `wait_until` polling a counter, or introduces a sleep.
