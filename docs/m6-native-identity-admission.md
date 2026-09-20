# M6 — Materialization off the event path

Implementation plan for milestone M6 of [`event-pipeline-milestones.md`](event-pipeline-milestones.md).
Written against `main` at `fce61f7` (M5 merged). Every file and line reference below was checked
against that tree; re-check them if the branch has moved.

## Goal

Delete the per-parent materialization FIFO, its spawned worker, and the project lifecycle lock from
the path between stdout and persistence. Today a parent's forwarder hands a sub-agent link to a
per-parent queue on the parent's `ThreadAuthority`, a worker task is spawned with a registry permit,
and that worker takes the project lifecycle lock, scans every live coordinator, and may read every
thread file in the project before it classifies or creates the child. A discovered native thread
goes through a second task, the discovery consumer, under the same lock and the same scans. While
either task waits for that lock, the child's retained events are not being consumed by anyone.

After M6 the project's event driver is the single place where a native identity is admitted into
the server: discoveries, sub-agent links from forwarders, and explicit link opens are all
admissions, processed one at a time per project, in arrival order, with no lock, no per-parent
queue, no spawned worker, and no scan. The lookup from native id to thread id is the harness's own
route table through the idempotent `claim_native_thread`, which M2 made the identity authority.
The thread graph is loaded only when a relationship is decided (an orphan is classified, a new child
is created, or a reverse link must be told apart from a cyclic one), never for repeated activity on
an already-classified child.

## Non-goals

- No change to the forwarder's reduction, to `ThreadEventForwarder::run`, or to turn intents (M5).
- No change to `AgentHarness`, the adapter, or the transport. `claim_native_thread` is used as it
  exists.
- No change to what a thread file records, to `classify_orphan`, or to the graph validation rules
  in `thread_graph.rs`.
- No removal of `lock_project_lifecycle` from the routes that create, delete or bootstrap. It
  leaves the admission path only. Two routes change order of operations to keep the exclusion
  they got from the lock; see "What the lifecycle lock protected".
- No change to `attach_subagent_thread` (opening a persisted child by id) beyond sharing a helper.
- No cursor-committed persistence (now M9).

## Ground truth

| Fact | Where |
| --- | --- |
| Forwarder enqueues a job on `ItemStarted` with a sub-agent tool link and on `ItemCompleted` with sub-agent activity, then continues | `registry/event_forwarder.rs:1604-1636` |
| `enqueue_subagent_materialization`: registry permit if the parent authority is not yet interned, `intern_thread_authority`, `enqueue_materialization_job` on the authority's FIFO, spawn the worker if none | `registry.rs:2222-2277` |
| `run_subagent_materialization_queue`: pops jobs until the queue is empty, calls `materialize_subagent_thread`, answers the optional reply | `registry.rs:2299-2347` |
| `materialize_subagent_thread`: takes `lock_project_lifecycle`, loads project and parent, scans `coordinator_snapshot` for the native id, else `load_thread_graph`; existing thread: disposition via `classify_existing_link` (graph) or direct fields, reverse link returns `None`, orphan classified with `classify_orphan`, `ensure_subagent_thread_open`, title refresh; new thread: `parent_chain_is_valid(graph)`, `claim_native_thread`, native-parent check, `thread_metadata.create`, `install_event_owner`, `publish_created` | `registry.rs:1936-2220` (disposition `:2008-2020`, chain check `:2113`, claim `:2138`) |
| `admit_discovered_thread`: takes `lock_project_lifecycle`, loads project, scans `coordinator_snapshot` then `load_thread_graph` for the native id; existing primary ignored, existing other → `ensure_subagent_thread_open`; else creates an `Orphan` file with `record.thread` as id (no claim: the adapter minted the route at ingest) and installs an owner | `registry.rs:546-660` |
| `spawn_discovery_consumer`: one task per harness with a registry permit, reads `harness.discoveries()`, increments `discovery_records_processed` under `cfg(test)` | `registry.rs:501-544`, `:285`, `:525` |
| `ensure_subagent_thread_open`: `reusable_handle` if a coordinator exists, else `claim_native_thread` and `install_event_owner` | `registry.rs:2349-2404` |
| `install_event_owner` is `driver.attach` | `registry.rs:2455-2475` |
| `open_subagent_link` (route-backed): `resolve_subagent_link_info`, `resolve_reverse_subagent_target` (graph), then enqueue with a reply | `registry.rs:1318-1355`, `:1795-1868` |
| `attach_subagent_thread`: `ensure_subagent_thread_open` then `loaded_thread_binding` | `registry.rs:918-941` |
| `coordinator_snapshot` locks the thread index and every authority's coordinator slot | `registry.rs:379-395` |
| `MaterializationSlot` on `ThreadAuthority`; `enqueue_materialization_job`, `next_materialization_job`, two test probes | `registry/thread.rs:310-315`, `:324`, `:336`, `:418-462` |
| `lock_project_lifecycle` interns a weak lock before a project authority exists; routes take it with a 5 s timeout | `registry.rs:1702-1728`, `:795-802`; `routes.rs:53` |
| `delete_project` route: lifecycle lock, liveness preflight, `registry.delete_project`, worktree removal, `store.delete_project` (removes the directory) | `routes.rs:466-541`; `giskard-persist/src/store.rs:970-982` |
| `registry.delete_project`: `coordinator_snapshot` for the project's threads, `begin_delete`, `harness.shutdown`, `finish_delete`, `driver.detach` each thread | `registry.rs:1649-1700` |
| `delete_thread` route: lifecycle lock, `load_thread_graph`, `descendant_deletion_order`, preflight, then per candidate `registry.delete_thread` (native delete then `retire_thread`) and `thread_metadata.delete` | `routes.rs:1863-1995`; `registry.rs:1498-1512` |
| `claim_native_thread` on Codex is `claim_or_adopt`: a known native id returns its existing thread id whatever id was proposed; an unknown one binds the proposed id; the reply carries `parent_harness_thread_id` from the mapper | `giskard-harness-codex/src/instance.rs:577-601`, `:69-79`; `native_routes.rs:83-98` |
| Every persisted `(native id, thread id)` pair is bound into the harness at creation; a duplicate or empty native id refuses to publish the harness. So a native id unknown to the harness has no thread file | `registry.rs:844-900` (`known_thread_bindings` `:864`) |
| `store.create_thread` refuses an existing thread id under the store's per-thread lock; `classify_orphan` is revision-checked under the same lock | `giskard-persist/src/store.rs:1042-1072`; `thread_metadata.rs:107-136` |
| `load_thread_graph` reads every thread file; `classify_existing_link` needs the graph only for the reverse-link and cycle cases; `parent_chain_is_valid` walks the chain | `thread_graph.rs:33-44`, `:120-176` |
| `DiscoveryStream::recv` is a retained-log reader; `ThreadDiscovered { thread, harness_thread_id, parent_harness_thread_id }` | `giskard-harness/src/lib.rs:441-462` |
| Driver: `DriverCommand { Attach, Detach }`, `run` selects on commands and owner exits, `attach` subscribes and installs the coordinator, `harness: Weak` | `registry/driver.rs:20-60`, `:124-147`, `:150-241` |
| tokio 1.52 (`mpsc::WeakSender` available) | `Cargo.lock:1955` |
| Tests touching the deleted machinery | `registry.rs:3717-3760` (shutdown rejection), `:3788-3866` (per-parent FIFO), `:3868-3915` (worker survives coordinator clear), `:3104-3160` (calls `materialize_subagent_thread` directly); discovery tests `:3015-3360` wait on files, coordinators and `discovery_records_processed` |
| End-to-end coverage that must keep passing | `tests/e2e_smoke.rs`: `importing_subagent_thread_records_parent_and_reuses_native_child` `:4155`, `collab_agent_spawn_start_imports_subagent_thread` `:4729`, `server_resolved_subagent_link_uses_agent_name_prompt_and_turn` `:5048`, `subagent_link_open_rejects_unknown_and_non_link_items` `:5181`, `terminal_subagent_link_does_not_synthesize_a_fallback_turn` `:5246`, `persisted_or_interrupted_subagent_keeps_one_event_owner` `:5347`, `reverse_subagent_activity_preserves_parent_and_uses_one_forwarder` `:5526`, `concurrent_subagent_cold_opens_install_one_native_owner` `:8145` |

## Design

### Admission is a driver input

```rust
// registry/driver.rs
enum DriverCommand {
    Attach(Box<Attach>),
    Detach { thread_id: ThreadId, reply: oneshot::Sender<()> },
    Link(Box<Link>),
    Quiesce { reply: oneshot::Sender<()> },
}

struct Link {
    parent_thread_id: ThreadId,
    spawned_by_turn_id: TurnId,
    item_id: ItemId,
    origin: &'static str,
    info: SubagentActivityInfo,
    reply: Option<oneshot::Sender<Result<Option<ThreadId>, HarnessError>>>,
}

enum Admission {
    Discovered(ThreadDiscovered),
    Link(Box<Link>),
}
```

The driver gains three fields: `discoveries: DiscoveryStream` (taken from the strong harness in
`spawn_project_event_driver`), `discoveries_closed: bool`, and `admission: Option<InflightAdmission>`
where

```rust
struct InflightAdmission {
    work: BoxFuture<'static, AdmissionOutcome>,
    source: AdmissionSource,           // for logging and the reply
}

struct AdmissionOutcome {
    result: Result<Option<Admitted>, HarnessError>,
}

struct Admitted {
    binding: LoadedThreadBinding,      // handle from the claim or the record, native model
    classification: ClassificationPhase,
    thread_id: ThreadId,
}
```

plus `quiesced: bool`. There is no queue field: the command channel and the discovery log are the
queues, and they are read only when nothing is in flight.

### The loop

```rust
loop {
    tokio::select! {
        command = self.rx.recv(), if !closed && self.admission.is_none() => match command {
            Some(DriverCommand::Attach(a)) => self.attach(*a).await,
            Some(DriverCommand::Detach { thread_id, reply }) => self.detach(thread_id, reply).await,
            Some(DriverCommand::Link(link)) => self.begin_link(*link),
            Some(DriverCommand::Quiesce { reply }) => { self.quiesced = true; let _ = reply.send(()); }
            None => closed = true,
        },
        record = self.discoveries.recv(), if !self.discoveries_closed && self.admission.is_none() => match record {
            Ok(record) => self.begin_discovery(record),
            Err(EventStreamError::Closed) => self.discoveries_closed = true,
            Err(EventStreamError::Gap { dropped }) => error!(...),
        },
        outcome = async { match self.admission.as_mut() { Some(a) => a.work.as_mut().await, None => pending().await } },
            if self.admission.is_some() => self.finish_admission(outcome).await,
        Some(exit) = self.owners.next(), if !self.owners.is_empty() => self.owner_exited(exit).await,
    }
    ...
}
```

Two properties follow from the guards. First, admissions are strictly sequential and strictly
ordered with attaches and detaches: a command is taken only when no admission is in flight, and an
admission is started only from a command or a record taken that way. Second, owners are never
starved: an admission is a boxed future polled by the same `select!`, exactly as M5's in-flight
harness request, so every forwarder in the project keeps making progress while a link is being
decided. That is why admission cannot be an inline `await` in the loop.

`begin_link` and `begin_discovery` check `quiesced` first: a quiesced driver answers a link with
`Protocol("project is being deleted")` and drops a discovery with a warning. Otherwise they build
the future and store it in `self.admission`. The future captures clones of `shared`, the strong
harness (upgraded from `Weak`; if that fails the link is refused with "project harness is gone" and
a discovery is dropped), and the admission's inputs. It never touches driver state.

`finish_admission` applies the outcome inline: for `Ok(Some(admitted))` it calls `self.attach`
with a synthetic `Attach` whose reply is a local oneshot it does not wait on beyond the immediate
result (attach replies synchronously unless the thread is detaching, in which case the attach is
parked exactly as today), then answers the link's reply with `Ok(Some(thread_id))`. `Ok(None)` is
the reverse-link and incompatible-ownership outcome and answers `Ok(None)`. `Err` is logged and
answered. Under `cfg(test)` a finished discovery increments `discovery_records_processed`.

### What an admission does

Both sources share one function in a new `registry/admission.rs`:

```text
admit(shared, harness, project_id, source) -> Result<Option<Admitted>, HarnessError>:

  project = load_project(project_id)?                    // None → Err("project disappeared")
  (handle, link) = match source {
      Discovered(record) => (ThreadHandle::opened(record.thread, record.harness_thread_id, root)
                             with parent_harness_thread_id = record.parent_harness_thread_id, None),
      Link(link) => {
          parent = load_thread(project_id, link.parent_thread_id)?   // None → Err
          root = effective_thread_workspace_root(store, project, parent)
          handle = harness.claim_native_thread(ThreadId::new(), link.info.native_thread_id, root)?
          if handle.harness_thread_id != native → Err (as today)
          (handle, Some((link, parent)))
      }
  };
  file = load_thread(project_id, handle.thread)
  file = match file {
      Some(file) => file,
      None => create Orphan file for handle.thread                  // as admit_discovered_thread today
  };
  if let Some((link, parent)) = link {
      if file.kind == Primary → warn "incompatible ownership", return Ok(None)
      if parent.parent_thread_id == Some(file.id)                   // possible reverse link
         || file.kind == Orphan                                     // classification decides
         || file.parent_thread_id != Some(parent.id)                // wrong parent or missing parent
      {
          graph = load_thread_graph(store, project_id)
          disposition = classify_existing_link(graph, parent.id, file)
      } else {
          disposition = OwnedChild                                  // repeated activity: no graph
      }
      match disposition {
          Parent   → return Ok(None)
          OwnedChild if file.kind == Orphan → {
              if !parent_chain_is_valid(graph, parent.id) → warn, return Ok(None)
              if handle.parent_harness_thread_id names a different native parent → warn, return Ok(None)
              classify_orphan(revision-checked) → file; if not Subagent under parent → Err (as today)
              coordinator(file.id).classify_orphan_as_subagent() if loaded
              publish_created
          }
          OwnedChild → refresh title if should_refresh_subagent_title (as today)
          other → warn with disposition.reason(), return Ok(None)
      }
  }
  Ok(Some(Admitted { binding: LoadedThreadBinding { project_id, handle, native_model }, classification: file.kind.into(), thread_id: file.id }))
```

Three consequences worth stating:

- **No scan, no snapshot.** The native-to-thread lookup is `claim_native_thread`. It is total
  because the harness was published with every persisted binding installed and because M2 mints a
  route for every native id seen in traffic. A native id the harness does not know has no thread
  file, so a fresh claim is exactly the "new child" case. `coordinator_snapshot` is no longer
  called on this path; the only remaining caller is `delete_project`, which is not hot.
- **A fresh claim always gets a file.** Today an invalid link makes no claim, so the adapter stays
  clean. With claim-as-lookup the claim happens first. If the link then turns out to be invalid, the
  thread must still exist durably, otherwise its later frames map to a route with no owner and sit
  in a log nobody reads. Creating the orphan file in that case is the M2 rule applied consistently:
  identity is always recorded; the relationship is decided separately. The e2e test at `:5246`
  (terminal link does not synthesize a turn) is unaffected, because a file is not a turn.
- **The graph is loaded only to decide a relationship.** Repeated `interacted` activity on a child
  already classified under this parent loads one thread file and nothing else.

The title refresh, `classify_orphan_as_subagent`, `publish_created`, the native-parent check and
every warning message are moved verbatim from `materialize_subagent_thread` and
`admit_discovered_thread`; nothing about what is persisted changes.

### The forwarder sends, it does not wait

`enqueue_subagent_materialization` calls at `event_forwarder.rs:1604-1636` become
`self.driver.link(Link { ..., reply: None }).await`. The forwarder receives a `DriverHandle` at
construction; the driver mints it from a `mpsc::WeakSender<DriverCommand>` it holds (a strong
sender on the driver would keep its own channel open forever). The send waits only when the
channel is full, which bounds the parent's event processing by the driver's admission progress and
never the reverse: the driver polls forwarders, it never waits for one, so there is no cycle.

Reordering is not a concern: a link carries the parent's own turn and item ids, and the child's
events wait in its retained log regardless of when the link is admitted.

### Explicit link open

`open_subagent_link` keeps `resolve_subagent_link_info` and `resolve_reverse_subagent_target`
(route-level, graph read allowed) and replaces the enqueue with
`driver.link(Link { ..., reply: Some(tx) }).await` followed by awaiting `rx`. `DriverHandle::link`
returns `Err(Protocol("project event driver is gone"))` if the channel is closed.

### What the lifecycle lock protected, and what replaces it

Admission no longer takes `lock_project_lifecycle`. The routes that still take it relied on
admission being excluded while they run. Each gets a deterministic replacement:

- **Project deletion.** `registry.delete_project` sends `Quiesce` to the driver immediately after
  `begin_delete` and before `harness.shutdown`. Because commands are taken only when no admission
  is in flight, `quiesce().await` returning means no admission is running and none will start.
  The route then deletes files as today. `harness.shutdown` closes the discovery log, so the
  driver's discovery branch ends on its own.
- **Thread subtree deletion.** Today the lock guarantees that no child is created under a
  candidate between the graph load and its deletion. The replacement is ordering plus a refusal:
  `begin_link` refuses a link whose parent has no live coordinator (none, detaching, or failed)
  with a warning, since a parent that is not live cannot be emitting fresh evidence the server
  should act on. The `delete_thread` route then retires every candidate first (`retire_thread`
  for each id in the deletion order, which is `driver.detach` and therefore ordered after every
  link that parent sent before its cancel), reloads the graph, recomputes the deletion order, and
  deletes. A link from a retired parent that was still queued is refused; a link admitted before
  the retire is in the reloaded graph. The route's preflight (liveness, worktree impact) runs on
  the final order.
- **Harness creation and bootstrap.** Unchanged: the driver does not exist until the harness is
  published, and admission never creates a harness.
- **Concurrent link and explicit open for the same child.** Both are admissions on one driver;
  sequential by construction. The store's per-thread lock and `create_thread`'s existence check are
  the durable backstop if two projects' drivers ever raced on one id, which cannot happen because
  ids are minted once.
- **Concurrent `attach_subagent_thread`.** It claims and attaches outside the driver, as today.
  Both operations are idempotent and the attach is serialized by the driver, so the outcome is a
  reuse.

### Deleted

`enqueue_subagent_materialization`, `reject_materialization_during_shutdown`,
`run_subagent_materialization_queue`, `materialize_subagent_thread`, `SubagentMaterializationJob`,
`SubagentMaterializationResult`, `MaterializationSlot` and its four methods on `ThreadAuthority`,
`spawn_discovery_consumer`, `admit_discovered_thread`, and the `coordinator_snapshot` scans in
both. `ensure_subagent_thread_open` shrinks to its two remaining callers (`attach_subagent_thread`
and the admission's "existing file" path share the claim-and-bind helper).

## Every site that changes

| File | Lines | Change |
| --- | --- | --- |
| `registry/driver.rs` | `20-60` | `Link`, `Quiesce`, `Admission`, `InflightAdmission`, `Admitted`; `DriverHandle::link`, `DriverHandle::quiesce`; driver fields `weak_tx`, `discoveries`, `discoveries_closed`, `admission`, `quiesced` |
| `registry/driver.rs` | `101-119` | `spawn_project_event_driver` takes `harness.discoveries()` and `tx.downgrade()` |
| `registry/driver.rs` | `124-147` | The four-branch loop above |
| `registry/driver.rs` | `197-235` | Pass a `DriverHandle` (upgraded from `weak_tx`) to `ThreadEventForwarder::new` |
| `registry/driver.rs` | new | `begin_link`, `begin_discovery`, `finish_admission` |
| `registry/admission.rs` | new | `admit`, the orphan-file constructor moved from `admit_discovered_thread`, the classification block moved from `materialize_subagent_thread` |
| `registry.rs` | `501-660` | Delete `spawn_discovery_consumer` and `admit_discovered_thread`; `get_or_create_harness` no longer calls the consumer (`:895`) |
| `registry.rs` | `1318-1355` | `open_subagent_link` sends `Link` with a reply |
| `registry.rs` | `1649-1700` | `delete_project` sends `Quiesce` after `begin_delete` |
| `registry.rs` | `1768-1783` | Keep `SubagentActivityInfo` (now `pub(super)`), delete the job and result types |
| `registry.rs` | `1936-2347` | Delete materialization, enqueue, queue worker |
| `registry.rs` | `2349-2404` | `ensure_subagent_thread_open` uses the shared claim-and-bind helper |
| `registry.rs` | `285`, `420`, `525` | `discovery_records_processed` stays; incremented by the driver under `cfg(test)` |
| `registry/thread.rs` | `310-315`, `324`, `336`, `418-462` | Delete `MaterializationSlot` and its methods |
| `registry/event_forwarder.rs` | `761-830` | Field `driver: DriverHandle`; `new` takes it |
| `registry/event_forwarder.rs` | `1604-1636` | Send `Link` instead of enqueueing |
| `routes.rs` | `1863-1995` | `delete_thread`: retire all candidates, reload the graph, recompute the order, then delete |

Expected non-test delta: about 550 lines removed from `registry.rs`, 60 from `thread.rs`, and 350
added across `driver.rs` and `admission.rs`. Net negative.

## Tests

### Driver tests (`driver.rs`)

The driver test harness gains a `claims: Mutex<HashMap<String, ThreadId>>` route table so
`claim_native_thread` adopts or mints like Codex, a `discoveries` log, and a gate on
`claim_native_thread` so a test can hold an admission in flight.

1. `a_link_for_an_unknown_native_id_creates_a_subagent_and_its_owner`: parent primary file
   exists; send `Link`; a `Subagent` file appears with the parent and turn; a coordinator exists;
   the harness saw one claim.
2. `a_discovery_creates_a_hidden_orphan_and_its_owner`: announce a record; an `Orphan` file with
   the record's id appears; owner installed; no claim was made.
3. `a_link_after_discovery_classifies_the_same_thread`: discovery then link for the same native id;
   the file becomes `Subagent` under the parent; the coordinator is pointer-identical before and
   after (the M2 registry test at `:3104`, moved here).
4. `a_discovery_after_link_reuses_the_existing_thread` (from `:3162`).
5. `repeated_activity_on_a_classified_child_reads_no_graph`: link twice; assert
   `store.list_threads` was called once (add a `cfg(test)` counter on `PersistStore`, or a
   `TestStore` wrapper if one exists) or, more simply, that the second link completes with the
   claim gate held by a third unrelated link, proving it did not wait on anything else.
6. `an_invalid_link_still_records_the_claimed_identity_as_an_orphan`: parent file is a `Subagent`
   with a dangling parent chain; link; the child file exists as `Orphan`; the reply is `Ok(None)`.
7. `a_reverse_link_returns_none_and_creates_nothing`.
8. `admissions_are_sequential_and_ordered_with_detach`: gate the claim; send `Link` then
   `Detach` for the parent; detach's reply arrives only after the link's file exists.
9. `owners_keep_running_while_an_admission_is_in_flight`: gate the claim; append events to an
   attached thread's log; they persist while the gate is held.
10. `a_quiesced_driver_refuses_links_and_drops_discoveries`.
11. `a_link_from_a_parent_without_a_live_owner_is_refused`.
12. `the_driver_does_not_keep_the_harness_alive` (existing) must still pass with the discovery
    reader held: `DiscoveryStream` is a log reader, not a harness reference.

### Registry tests (`registry.rs`)

Delete `closed_registry_rejects_materialization_without_stranding_queue` (`:3717`),
`parent_materialization_queues_are_fifo_and_independent` (`:3788`),
`clearing_coordinator_does_not_replace_active_parent_worker` (`:3868`), and `materialization_job`.
Move `link_after_discovery_classifies_the_same_thread` and `discovery_after_link_reuses_the_existing_thread`
to the driver tests as above. Keep `discovered_native_thread_becomes_a_hidden_orphan_with_an_owner`,
`discovery_for_a_primary_is_ignored`, `discovery_consumer_survives_a_failed_record` and
`discovery_consumer_stops_on_registry_shutdown`; they exercise the registry through a harness and
must pass with the driver reading discoveries. Rename the last two to say "driver" only if the
rename is the whole diff of a separate commit.

### Deletion ordering tests

The harness cannot observe owner cancellation and must not: the cancel watch is server-internal
since M4. The race "a link admitted before the retire is durable, a link queued behind the retire
is refused" is therefore pinned where the ordering is controllable, next to the driver tests, using
only the driver handle and a gate inside the test double's own `claim_native_thread` (an existing
trait method, so no trait change):

1. A parent primary exists; the claim gate is closed.
2. Send `Link(L1)` for the parent. The admission starts and blocks on the gate.
3. Spawn `driver.detach(parent)`. It cannot be taken while the admission is in flight.
4. Send `Link(L2)` for the same parent. Sends from one task are ordered, so it is provably queued
   behind the detach.
5. Open the gate.
6. Assert: L1's child file exists as a sub-agent under the parent; the detach completed; L2 created
   nothing and its reply is the refusal; the parent has no coordinator.

The `delete_thread` route then needs only its structural property, covered by one plain e2e case:
a parent with a linked child is deleted over HTTP and no thread file remains under it. Its outcome
is deterministic under every interleaving because the driver guarantees the ordering; the route's
statement order (retire every candidate, reload the graph, recompute, delete) is reviewed in the
diff. Do not add a harness hook, a sleep, or a server-side notify for this; if a forced concurrent
e2e interleaving is ever wanted, the seam is a `cfg(test)` notify on `RegistryShared` when an
admission begins, in the spirit of `discovery_records_processed`, and it is not part of M6.

### End-to-end

Every sub-agent test in `e2e_smoke.rs` listed in the ground truth must pass unchanged.

## Documentation

- `docs/event-pipeline-milestones.md`: M6 status, the design as landed, exit line, plan pointer.
- `docs/subagents.md` "Link-open API" paragraph (`:179-186`): replace "share one per-project
  lifecycle lock, while linked evidence from one parent is processed through a FIFO" with: all
  native identity admissions (discovery, sub-agent link, explicit open) are processed one at a time
  by the project's event driver in arrival order; the lookup is the harness's idempotent claim;
  the thread graph is read only when a relationship is decided. Keep the 503 sentence: it still
  describes routes. In "Deletion and recovery" (`:204-206`) replace "Imports and deletion share the
  same project lifecycle lock" with: project deletion quiesces the driver before removing files,
  and subtree deletion retires every candidate before it computes its final order.
- `specs/giskard-specification.md`: bump to 1.83 with an amendment "native identity admission":
  the driver serializes discoveries and links; no lifecycle lock or per-parent queue on the path
  from a harness event to persistence; a claimed identity is always recorded, as a sub-agent when
  the link validates and as a hidden orphan otherwise. Rewrite the deletion bullet at `:515-524`
  ("share one project lifecycle lock", "per-parent FIFO") and the authority bullet at
  `:2590-2596` ("materialization FIFO remains present while its one per-parent worker is active")
  to match.
- `docs/event-pipeline-architecture-review.md` §6.6: tick the two acceptance bullets this
  milestone closes (`enqueue_subagent_materialization` and its queue; `lock_project_lifecycle` on
  the path from stdout to persistence).
- `AGENTS.md` next to the M4 and M5 rules: "Native identity admission (a discovery, a sub-agent
  link, an explicit link open) is a driver input processed one at a time per project. No code
  outside the driver creates or classifies a thread file for a native id, and admission never
  takes `lock_project_lifecycle`."

## Order of work

1. **Additive.** `admission.rs` with `admit` built from the moved code; `Link`, `Quiesce`, the
   driver fields and branches; `DriverHandle::link` and `quiesce`; the forwarder's `driver` field.
   Nothing sends links yet; the discovery consumer still runs; all tests pass.
2. **Switch.** The forwarder and `open_subagent_link` send `Link`; `spawn_project_event_driver`
   reads discoveries and `get_or_create_harness` stops spawning the consumer; `delete_project`
   quiesces; `delete_thread` retires first. Run the suite; the e2e sub-agent tests are the proof.
3. **Delete.** Materialization, enqueue, worker, `MaterializationSlot`, `admit_discovered_thread`,
   `spawn_discovery_consumer`, the scans, the three registry tests. Add the driver tests.
4. Docs.

Each step is a separate commit that compiles and passes on its own.

## Verification the implementer must perform and record

- `grep -rn 'enqueue_subagent_materialization\|MaterializationSlot\|materialization_job\|run_subagent_materialization_queue\|spawn_discovery_consumer\|admit_discovered_thread' crates/giskard-server/src` → empty.
- `grep -n 'lock_project_lifecycle' crates/giskard-server/src/registry.rs crates/giskard-server/src/registry/*.rs` → only the helper, its timeout wrapper, and tests. No call in `driver.rs`, `admission.rs` or `event_forwarder.rs`.
- `grep -n 'coordinator_snapshot' crates/giskard-server/src/registry.rs` → the definition and `delete_project` only.
- `grep -n 'load_thread_graph' crates/giskard-server/src/registry/admission.rs` → inside the
  relationship-decision branch only.
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace` with zero ignored tests.
- A manual run against real Codex with a prompt that spawns two sub-agents: both children appear
  in the Sub-agents monitor with titles; opening a transcript link navigates to the existing child;
  deleting the parent removes both children; deleting the project while a child is idle succeeds.
- Non-test line delta recorded in the PR description.

## Pitfalls

- The admission must be a future polled by the driver's `select!`, never an inline `await` in the
  loop. An inline await stops every forwarder in the project for its duration. The M5 in-flight
  pattern is the template; the branch expression must be total (`pending()` when `None`).
- Commands and discoveries must be gated on `admission.is_none()`. Without that gate, a detach can
  overtake a queued link and the deletion ordering argument fails.
- `finish_admission` calls the driver's own `attach`, not `DriverHandle::attach`: sending to your
  own channel from inside the loop deadlocks when the channel is full.
- Hold the harness strongly only inside the admission future. The driver keeps its `Weak`; the
  existing "driver does not keep the harness alive" test enforces this.
- Discovery admission does not claim; the record's id is already the route. Link admission always
  claims with a fresh id; the harness returns the existing id when it knows the native id.
- A fresh claim followed by a rejected link still creates the orphan file. Skipping that leaves a
  route with no owner and silently strands the child's events.
- The reverse-link case must load the graph. The comment at `registry.rs:1994-1998` and the e2e test
  at `:5526` exist because a direct-parent shape inside a malformed graph took the fast path once.
- `delete_thread` must retire before it computes the final deletion order, not after. Retiring is
  a driver detach and is ordered behind every link the parent sent before its cancel.
- Keep `discovery_records_processed` under `cfg(test)`; four discovery tests wait on it.
- Do not add a native-id map to the driver or to `RegistryShared`. The harness's route table is the
  index; a second one would be the kind of peer map `AGENTS.md` forbids and would drift.

## Stop rules

Stop and re-cut if:

- a per-thread or per-parent queue, worker task, or permit reappears;
- `lock_project_lifecycle` is taken anywhere in `driver.rs`, `admission.rs` or the forwarder;
- a keyed map from native id or thread id appears outside a function body;
- the admission needs the harness trait to change;
- the forwarder's `handle_event` needs more than replacing the two enqueue calls;
- non-test lines exceed the budget.
