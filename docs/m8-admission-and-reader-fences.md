# M8 — Admission and reader fences

Implementation plan for milestone M8 of [`event-pipeline-milestones.md`](event-pipeline-milestones.md).
Written against `main` at `3c804dd` (M7 and the live-usage change merged, spec 1.85; the
server-request claim fix in PR #235 is at 1.86). Every file and line reference below was checked
against that tree; re-check them if the branch has moved.

## Goal

Close three findings of the post-M7 static review. Each is a fence M7 intended and stopped one step
short of: quiesce gates discoveries and links but not owner attachment; a failed admission is
retried only on two triggers and is discarded for the life of the process after three failures;
a reader dropped while lagging takes its unobserved loss with it. All three fixes are
deterministic, timing-free, and small.

| # | Finding | Boundary | Fix in one line |
| --- | --- | --- | --- |
| 1 | A slow explicit open can attach an owner after deletion quiesced and snapshotted | driver | A quiesced driver refuses `Attach` with "project is being deleted" |
| 2 | A failed admission stays deferred until an unrelated success, and is dropped after three failures | driver | No lifetime attempt cap; retry on every driver event; deduplicate by native id |
| 3 | Dropping a lagged reader discards its unreported loss | log | Fold the dropped cursor's deficit into `unreported_evictions` when it was the last reader |

## Non-goals

- No change to `AgentHarness`, the adapter, the forwarder, the hub, routes, persistence, or wire
  types.
- No timers. Every retry is triggered by an event the driver already handles.
- No change to the admission algorithm in `admission.rs`.
- No change to how deletion detaches owners or to the project slot states in `project.rs`;
  `driver()` keeps returning the handle for a `Deleting` slot because deletion itself needs it.
- Cursor-committed persistence stays M9.

## Ground truth

| Fact | Where |
| --- | --- |
| Driver loop: `rx.recv()` is gated on `admission.is_none()`; `Attach` is handled unconditionally; the discovery arm is gated on `!quiesced`; `Quiesce` and `Resume` flip the flag and reply at once; `Resume` calls `start_deferred` | `registry/driver.rs:257-299` |
| On close with no owners and no admission, parked attaches are rejected with "project event driver is gone" and deferred admissions are dropped with a warning | `driver.rs:300-313` |
| `attach`: interns the authority, parks behind a detaching coordinator, reuses a live one, else subscribes and installs an owner. Never reads `quiesced` | `driver.rs:314-412` |
| `begin_link` under `quiesced`: a link with a reply gets "project is being deleted"; a reply-less link is queued deferred | `driver.rs:414-424` |
| `finish_admission` attaches on success and calls `start_deferred` only when the admission succeeded | `driver.rs:519-549` |
| `finish_admission_reply` defers a failed discovery or reply-less link through `defer_admission` | `driver.rs:551-590` |
| `defer_admission` discards after `ADMISSION_ATTEMPTS` (3) with an error log; `queue_deferred` drops the oldest at `DEFERRED_ADMISSION_LIMIT` (64) | `driver.rs:24-26, 592-612` |
| `start_deferred` pops one entry unless quiesced or an admission is in flight | `driver.rs:614-625` |
| `detach` and `owner_exited` call `retry_parked`, which re-runs `attach` for parked entries of that thread | `driver.rs:627-679` |
| `DriverHandle::attach` holds the handle (a strong or upgraded sender) across `response.await` | `driver.rs:176-190` |
| `Admission::Discovered(ThreadDiscovered { thread, harness_thread_id, parent_harness_thread_id })` and `Admission::Link(Box<Link>)` where `Link.info.native_thread_id` is the native id | `registry/admission.rs:20-23`; `giskard-harness/src/lib.rs:441-446`; `registry.rs:1640-1645` |
| `delete_project`: `begin_delete` → `driver.quiesce()` → coordinator snapshot → `harness.shutdown()` → `finish_delete` → detach snapshotted threads → drop the retained handle → `forget_threads` | `registry.rs:1501-1565` |
| Registry shutdown quiesces every driver before harness shutdown | `registry.rs:1423-1431` |
| `open_primary_thread`: `get_or_create_harness` (refuses a `Deleting` slot via `active_or_creatable`) → native open → `install_event_owner` → `event_driver()` → `driver.attach` | `registry.rs:786-870` (install at `:852`), `:1904-1922`, `project.rs:194-210, 217-222` |
| Neither `open_thread` nor `delete_project` takes the project lifecycle lock; only the HTTP delete and thread-delete routes do | `routes.rs:466-472, 1865-1868` |
| Existing quiesce test covers an admission in flight when deletion starts, not an attach that arrives after the snapshot | `registry.rs:2548-2590` (`delete_project_detaches_an_owner_admitted_during_quiesce`) |
| Discovery is announced once, when a native id is first bound from traffic; later frames for that route do not announce again | `giskard-harness-codex/src/instance.rs:415-436` |
| `EventLog::append` counts an eviction in `unreported_evictions` only when no cursor exists | `giskard-harness/src/event_log.rs:108-131` |
| `reader()` starts at `base` and takes `unreported_evictions` as its `pending_gap` | `event_log.rs:156-174` |
| `poll_reader` reports `pending_gap` first, then `base - next` as a `Gap` | `event_log.rs:193-222` |
| `Drop for EventLogReader` removes the cursor and trims; the cursor's `pending_gap` and lag are discarded | `event_log.rs:249-256` |
| Module doc: "An eviction that happened while no reader existed is reported to the next reader created" | `event_log.rs:8` |
| Forwarder reaction to `Gap` with no owned turn: one error log, then continue | `registry/event_forwarder.rs:1252-1259` |
| Driver test fixture: `setup()`, `attach_primary`, `link`, `persist_thread`, `obstruct_thread_creation`, `harness.announce`, `shared.discovery_records_processed`, `wait_until` | `driver.rs:861-1030, 1330` |
| Driver tests to keep or replace: `a_quiesced_driver_refuses_links_and_leaves_discoveries_unconsumed` `:1750`, `a_quiesced_driver_defers_a_replyless_link_until_resume` `:1794`, `a_failed_discovery_is_retried_after_the_next_successful_admission` `:1835`, `a_failed_discovery_is_dropped_after_three_attempts` `:1860`, `a_resume_retries_deferred_admissions` `:1920`, `attach_during_detach_is_parked_until_the_detach_completes` `:1136` | `driver.rs` |
| Registry test fixture: `discovery_registry`, `attach_test_primary`, `start_gated_link`, `wait_for_claim`, `DiscoveryHarness::gate_claims` (claim gate); its `open_thread` returns `Unsupported` | `registry.rs:1974-2010, 2049-2054, 2428-2545` |
| Log tests to mirror: `evictions_between_readers_are_reported_to_the_next_reader` `:405`, `a_second_reader_created_without_an_intervening_append_gets_no_gap` `:426` | `event_log.rs` |
| Spec: 1.84 amendment `:19-30`; normative deletion sentence "Project deletion quiesces the driver before harness shutdown and file removal" `:554`; retained-log amendments 1.78 `:69-73` and §4 note `:2183`; version `:12` | `specs/giskard-specification.md` |
| Rules: admission is a driver input processed one at a time; no peer map keyed by thread identity | `AGENTS.md:132-137` |

## Design

### D1. A quiesced driver refuses attachment

Add, as the first statement of `ProjectEventDriver::attach` (`driver.rs:314`):

```rust
if self.quiesced {
    let _ = attach.reply.send(Err(HarnessError::Protocol(
        "project is being deleted".into(),
    )));
    return;
}
```

This is the same reply a link with a caller gets under quiesce (`:416-419`), so `install_event_owner`
and the open route surface it the way they already surface a refused link.

Why refuse rather than park. The caller of `DriverHandle::attach` holds its handle across
`response.await` (`:176-190`), and a handle is a strong sender to the driver's command channel. A
parked attach would keep the channel open, so the driver could never reach the close branch that
rejects parked entries (`:300-313`) and the open would wait until a resume that, on a successful
deletion, never comes. Refusing is immediate, needs no state, and a failed deletion's `Resume`
makes the very next open succeed.

Which attaches this covers:

- explicit opens through `install_event_owner` that pass `active_or_creatable` before
  `begin_delete` and reach the driver after `quiesce`, the reviewer's case;
- `retry_parked` re-attaches fired by a detach or owner exit during deletion: an attach that was
  parked behind a detaching coordinator before quiesce is now refused instead of installing an
  owner into a project being torn down.

Which it does not touch: admission-originated attaches in `finish_admission`. An admission cannot
be in flight while quiesced (the `Quiesce` command is only received when `admission.is_none()`,
`:257-263`) and `start_deferred` already returns under quiesce (`:615`). Registry shutdown quiesces
first too (`registry.rs:1423-1431`), so an open racing shutdown is refused the same way.

`delete_project` and `project.rs` do not change. With D1 the coordinator snapshot at
`registry.rs:1522-1526` is complete by construction: no owner can be installed between `quiesce()`
returning and the retained handle being dropped.

### D2. Deferred admissions retry on every driver event and are never discarded

Three changes in `driver.rs`:

1. **No lifetime cap.** Delete `ADMISSION_ATTEMPTS` and the early return in `defer_admission`
   (`:592-600`). Keep `attempts` on `DeferredAdmission` and in `AdmissionSource` for the log line
   only; widen it to `u32` with `saturating_add` so a long outage cannot wrap. Each failure logs
   at `warn` with the attempt number and the native id; the "dropping … after repeated failures"
   error goes away.
2. **Deduplicate by native id.** Give `Admission` a helper `fn native_thread_id(&self) -> &str`
   (`Discovered` → `harness_thread_id`; `Link` → `info.native_thread_id`). In `queue_deferred`,
   if an entry with the same native id exists, replace it in place (keep the higher attempt
   count) instead of pushing. The 64-entry limit and its oldest-drop stay as the bound for
   distinct native ids.
3. **More triggers, one retry each.** Call `start_deferred().await` after each of: an `Attach`
   command has been handled, a `Detach` command has been handled, and an owner exit has been
   processed, in the loop at `:257-299` and `:643-665`. The existing triggers (a successful
   admission, `Resume`) stay. `start_deferred` still pops exactly one entry and still returns
   under quiesce or with an admission in flight, so a trigger can never start two admissions and
   a failing admission cannot spin: its failure re-queues it at the back and nothing retries until
   the next trigger.

Why these triggers. They are the events that already wake the driver, and each one signals that
the project is alive again after whatever made the admission fail: a user opened a thread, a
sub-agent linked, an owner finished. During a disk-full outage every retry fails once per trigger
and the entry is kept; after the disk is freed, the next trigger admits it. The explicit-open
recovery path (opening the orphan by native id is itself an admission) and restart discovery are
unchanged.

What this does not do. It does not make the adapter re-announce a native id when frames keep
arriving for an unadmitted route. That would couple the adapter to admission state, and the
triggers above cover the same situations without it.

### D3. A dropped reader hands its unobserved loss to the next reader

In `Drop for EventLogReader` (`event_log.rs:249-256`):

```rust
let mut state = self.log.lock();
if let Some(cursor) = state.cursors.remove(&self.id) {
    let deficit = cursor.pending_gap + state.base.saturating_sub(cursor.next);
    if state.cursors.is_empty() {
        state.unreported_evictions += deficit;
    }
}
EventLog::trim(&mut state);
```

Only when the dropped reader was the last one: a remaining reader observes its own lag through
`poll_reader`, so evictions it will report are not "unobserved". A reader dropped with no lag adds
zero, which keeps `a_replacement_reader_starts_at_the_oldest_unconsumed_event` and
`evictions_between_readers_are_reported_to_the_next_reader` (`:309, :405`) unchanged.

Update the module doc line at `:8` to: "An eviction that no reader consumed, including one a
reader was dropped without reporting, is reported to the next reader created."

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/registry/driver.rs:24-26` | Delete `ADMISSION_ATTEMPTS` |
| `driver.rs:77-92` | `attempts: u32` on `AdmissionSource` and `DeferredAdmission` |
| `driver.rs:257-299` | `start_deferred().await` after the `Attach` and `Detach` arms |
| `driver.rs:314` | D1 quiesce check at the top of `attach` |
| `driver.rs:551-590` | Log lines carry the native id and attempt |
| `driver.rs:592-612` | Cap removed; dedupe in `queue_deferred` |
| `driver.rs:643-665` | `start_deferred().await` at the end of `owner_exited` |
| `crates/giskard-server/src/registry/admission.rs:20-23` | `Admission::native_thread_id` |
| `crates/giskard-harness/src/event_log.rs:8, 249-256` | D3 and module doc |
| `specs/giskard-specification.md`, `docs/event-pipeline-milestones.md`, `AGENTS.md` | See Documentation |

No file outside these compiles against the changed items.

## Tests

### Driver (`driver.rs` tests, fixture at `:861-1030`)

1. `a_quiesced_driver_refuses_attach`
   `attach_primary` succeeds; `quiesce`; a second `driver.attach` for a new persisted thread →
   `Err(Protocol("project is being deleted"))`; the thread has no coordinator; `resume`; the same
   attach → `Ok(Installed)`.
2. `a_parked_attach_is_refused_when_its_retry_runs_under_quiesce`
   Model on `attach_during_detach_is_parked_until_the_detach_completes` (`:1136`): attach A, start
   `detach` A while its owner is slow to exit so a second attach for A parks; `quiesce`; let the
   owner exit → `retry_parked` runs under quiesce → the parked attach's reply is
   "project is being deleted" and no coordinator is installed; `resume`; a fresh attach succeeds.
3. `a_failed_discovery_is_retried_after_an_attach`
   Obstruct thread creation, `announce`, wait for one processed record; remove the obstruction;
   `attach_primary` for another persisted thread → processed count reaches 2 and the discovered
   thread file exists.
4. `a_failed_discovery_is_retried_after_a_detach`
   As 3, but the trigger is `driver.detach` of the primary.
5. `a_failed_discovery_is_retried_after_an_owner_exit`
   As 3, but the trigger is closing the primary's event log so its owner exits.
6. `a_failed_discovery_is_never_dropped`
   Replaces `a_failed_discovery_is_dropped_after_three_attempts` (`:1860`). Keep the obstruction
   through four successful links (processed count 1, 2, 3, 4, 5), remove it, one more link →
   processed count 6 and the file exists.
7. `deferred_admissions_are_deduplicated_by_native_id`
   Obstruct; `announce` the same `harness_thread_id` twice with different `thread` ids; wait for
   processed count 2; remove the obstruction; one successful link → processed count 3 (one
   retry, not two) and exactly one thread file for that native id.
8. `a_failing_retry_does_not_spin`
   Obstruct; `announce`; one successful link → processed count 2 (the retry failed); `quiesce`
   and assert the count is still 2, the obstruction is still a file, and `claim_calls` did not
   grow. Then `resume` → 3.
9. `the_deferred_queue_bound_still_drops_the_oldest`
   Obstruct 65 distinct native ids; announce each; after the last, the first native id is never
   admitted after the obstruction is removed and a trigger fires, while the second is. (Keeps the
   bound honest after dedupe.)
10. Existing `:1750, :1794, :1835, :1920` unchanged and green.

### Registry (`registry.rs` tests, fixture at `:2428-2545`)

11. `an_explicit_open_after_quiesce_is_refused_and_leaves_nothing_behind`
    Add `gate_opens()` to `DiscoveryHarness` mirroring `gate_claims` (`:2006-2010`) and make its
    `open_thread` (`:2049-2054`) return a handle after the gate instead of `Unsupported`. Create
    the project and harness, persist a primary thread, spawn `registry.open_thread` for it and
    wait until the harness reports the open started; spawn `delete_project`; yield until it is
    past `quiesce` (the fake harness `shutdown_calls` is still 0 and `deleting` not finished);
    release the open gate; the open resolves to `Err` whose message contains
    "project is being deleted"; deletion completes; `registry.shared.coordinator(thread)` is
    `None`, `thread_has_active_turn` is false, and `current_overview()` has no row for the
    thread. On `main` this test installs an owner and the overview keeps the row.
12. `an_explicit_open_after_a_failed_deletion_resumes_succeeds`
    As 11 but make `harness.shutdown()` fail once so `delete_project` resumes the driver; the
    gated open is refused; a second open succeeds and the coordinator exists.
13. Existing `delete_project_detaches_an_owner_admitted_during_quiesce` (`:2548`) and
    `registry_shutdown_quiesces_drivers_before_harness_shutdown` unchanged.

### Event log (`event_log.rs` tests)

14. `a_lagged_reader_dropped_last_reports_its_loss_to_the_next_reader`
    `with_limit(2)`; reader A; append five; drop A unread (base 3, A.next 0); reader B →
    `Gap { dropped: 3 }`, then "3", "4". On `main` B gets "3" with no gap.
15. `a_lagged_reader_dropped_with_a_peer_remaining_adds_nothing`
    `with_limit(2)`; readers A and C; append five; drop A; C → `Gap { dropped: 3 }`; reader D →
    "3" with no gap.
16. `a_dropped_reader_with_a_pending_gap_passes_it_on`
    `with_limit(2)`; append five with no reader (unreported 3); reader A created (pending 3);
    drop A unread; reader B → `Gap { dropped: 3 }`.
17. Existing `:309, :405, :426` unchanged.

## Documentation

`specs/giskard-specification.md`:

- Bump `:12` to the next free version (1.87 if PR #235 has merged, else 1.86) and add an
  amendment blockquote under it in the 1.84 form: a quiesced driver refuses new event-owner
  attachment; deferred admissions are retried after every driver event, deduplicated by native id,
  and never discarded for repeated failure; a reader dropped while behind hands its unreported loss
  to the next reader created.
- Add a `Changelog (x → y), admission and reader fences:` block at the top of the changelog region
  (before `:135`) with three items `AF1`, `AF2`, `AF3` stating the three rules.
- Normative text at `:554`: after "Project deletion quiesces the driver before harness shutdown
  and file removal." add "A quiesced driver refuses event-owner attachment, so no owner can be
  installed after the deletion snapshot."
- Leave the 1.84 and 1.78 amendment blocks as history.

`docs/event-pipeline-milestones.md`: M8 section for this milestone (done in this commit), the
cursor-committed persistence milestone renumbered to M9, and the ordering diagram updated.

`AGENTS.md:132-135`: append to the admission bullet: "A quiesced driver refuses attachment and
retries deferred admissions only on driver events; nothing polls."

`crates/giskard-harness/src/event_log.rs:8`: module doc as in D3.

## Order of work

1. D3 and tests 14-16 (`giskard-harness` only; no dependents).
2. D1 and tests 1-2.
3. D2 and tests 3-9; delete `a_failed_discovery_is_dropped_after_three_attempts`.
4. Registry tests 11-12 with the `gate_opens` fixture.
5. Docs.

Expected size: 60-90 non-test lines.

## Verification the implementer must perform and record

- `cargo test -p giskard-harness -p giskard-server`, `cargo clippy --workspace --all-targets --
  -D warnings`, `cargo fmt --check`.
- Tests 11 and 14 fail on `main` and pass on the branch; record both outcomes in the PR.
- `grep -n ADMISSION_ATTEMPTS crates/` returns nothing.
- Manual: with a project open, delete it while an explicit open of one of its threads is in
  flight; the open returns "project is being deleted" and the runtime overview shows no row for
  the deleted project.

## Pitfalls

- Do not park attaches under quiesce; see D1 for why it hangs the caller.
- Do not add a retry on admission failure itself, or on the discovery arm; a failing admission
  would then re-run back to back. One retry per trigger, and only the listed triggers.
- Keep `start_deferred`'s `quiesced` and `admission.is_some()` guards; the new triggers rely on
  them to stay single-flight.
- Dedupe must key on the native id, not the proposed `ThreadId`: two discoveries of one native id
  can carry different proposed ids (the mapper mints the id, the record carries it).
- In `Drop`, compute the deficit before removing the cursor and add it only when no cursor
  remains; adding it unconditionally double-reports once a remaining reader also reports its lag.
- The fake harness `open_thread` gate in test 11 must release only after `delete_project` has
  passed `quiesce`; assert that ordering with the harness's `shutdown_calls` counter, not a sleep.

## Stop rules

Stop and re-cut if the diff:

- adds a lock, timer, epoch, or a map keyed by thread or project identity outside the existing
  `deferred` queue;
- changes `admission.rs` beyond the accessor, or any file in the adapter, forwarder, hub, routes,
  or persistence crates;
- changes the `Deleting` slot semantics or `delete_project`'s sequence;
- makes a `Gap` reach a reader that could have consumed the events itself.
