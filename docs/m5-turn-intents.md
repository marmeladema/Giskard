# M5 — Intents replace prepared operations

Implementation plan for milestone M5 of [`event-pipeline-milestones.md`](event-pipeline-milestones.md).
Written against `main` at `eec099e` (M4 merged). Every file and line reference below was checked
against that tree; re-check them if the branch has moved.

## Goal

Delete the token machinery. Today a turn start is a three-party hand-off. The HTTP handler reserves
a runtime lease and records a `PreparedOperation` in the coordinator under its mutex, a spawned task
calls the harness and reports back into the coordinator, and the forwarder later consumes the
preparation when the first event of a new native turn arrives. Every step across those three parties
is guarded by `CoordinatorToken { generation, sequence }` equality checks in eight coordinator
methods, because any of them can observe the others' state at any time.

After M5 there is one party. A turn start or compaction is a `TurnIntent` message that the thread's
forwarder receives on the same `select!` loop that delivers its events. The forwarder reserves the
lease, calls the harness through a future it polls itself, and attaches the first new native turn to
the admitted intent. Because one sequential loop both admits and observes, no state can be stale
and there is nothing for a token to check. `CoordinatorToken`, generations, `PreparedOperation`,
`OwnedNativeTurn` and the eight methods are deleted. The coordinator keeps only what it holds for
other readers: the binding, the classification, and the owner phase.

This narrows the milestones document's exit line "`ThreadCoordinator` is plain data inside the
driver with no mutex". The *turn* state becomes plain data inside the forwarder. The binding,
classification and owner phase stay on the coordinator behind its mutex, because they are read by
the registry (`loaded_thread_binding`, `delete_project`, `reusable_handle`), written by
materialization (`classify_orphan_as_subagent`) and driven by the driver (`request_detach`,
`owner_exited`). Tokens never protected those fields; moving them is M6 territory.

## Non-goals

- No change to the forwarder's reduction: `complete_forwarded_turn`, item preparation, live-buffer
  and hub publication, gap recovery, materialization enqueueing.
- No change to `AgentHarness`, the adapter, the transport, or routes.
- No change to the driver's attach, detach, parking or owner-exit handling (M4), beyond creating one
  more channel per owner.
- No change to `OwnerLock` or `open_primary_thread`.
- No change to `interrupt`, `respond_approval`, `respond_server_request`, `terminate_command`. They
  reserve nothing and stay direct harness calls. The milestones document says "compaction and
  interrupt follow the same path"; compaction does, interrupt does not need to.
- No change to sub-agent materialization or the per-parent queue (M6).

## Ground truth

| Fact | Where |
| --- | --- |
| `CoordinatorToken { generation, sequence }`; state carries `generation`, `next_sequence`, `operation: Option<PreparedOperation>`, `native_turn: Option<OwnedNativeTurn>`, `native_activity`; `token()` and `token_is_current` | `registry/thread.rs:19-23`, `:102-112`, `:119-131` |
| `prepare_operation` refuses non-Primary with `ThreadReadOnly`, a `Failed` owner with `Protocol("thread {} event owner failed: {reason}")`, a non-`Live` owner with `Protocol("thread {} has no live event owner")`, and an existing operation or native turn with `ThreadBusy`; returns the lease on error | `thread.rs:179-226` |
| `abort_operation`, `take_unclaimed_operation`, `acknowledge_operation_turn`, `claim_native_turn` (adopts the prepared operation, else builds an external context from classification plus persisted defaults), `install_native_turn_gate`, `acknowledge_native_turn`, `take_native_turn_gate`, `finish_native_turn` (generation-checked), `owns_native_turn_for_test` | `thread.rs:228-250`, `:285-417`, `:476-484` |
| `owner_exited` bumps the generation on detach; `request_detach` clones the cancel sender out of `OwnerPhase::Live` | `thread.rs:427-474` |
| `NativeActivity` is written in four places and read nowhere; `changed: Notify` is notified in four places and awaited nowhere | grep `native_activity`, `changed` |
| `admit_operation`: `reserve_turn`, then `prepare_operation`, release on failure, publish overview. `abort_admitted_operation`: `abort_operation`, release, publish | `registry.rs:397-436` |
| `start_turn`: coordinator lookup, `binding()`, `active_harness`, build `TurnContext { kind: User }`, `admit_operation`, background permit, `tokio::spawn` calling `harness.start_turn`, `acknowledge_operation_turn` or `abort_admitted_operation`, abort again if the task fails | `registry.rs:1081-1187` |
| `compact_thread`: same shape with `UserInput::text("/compact")` and `TurnContextKind::ManualCompaction` | `registry.rs:1396-1489` |
| `interrupt` is a direct harness call through `loaded_thread_binding` | `registry.rs:1356-1394` |
| Forwarder fields: `coordinator`, `stream`, `cancel`, `turn: ForwardedTurnState` with `lease`, `owned_turn`, `owned_token`; `new()` loads persisted defaults, `run()` selects on cancel and stream | `registry/event_forwarder.rs:714-856` |
| `finish` pulls the lease out of the coordinator (`take_native_turn_gate` or `take_unclaimed_operation`), releases it, then `finish_native_turn` | `event_forwarder.rs:862-918` |
| `handle_stream_error` pulls the lease, synthesizes an `Interrupted` completion, then `finish_native_turn` | `event_forwarder.rs:920-1010` |
| First event of an unseen turn: `claim_native_turn`; external turns reserve a lease, acknowledge it, and hand it to the coordinator with `install_native_turn_gate`; exit reasons `DuplicateForwarder` and `RuntimeAuthorityReplaced` on failure | `event_forwarder.rs:1081-1143` |
| `TurnStarted` acknowledges the native turn through the coordinator; completion takes the gate, persists, then `finish_native_turn` | `event_forwarder.rs:1384-1394`, `:1583-1610` |
| Driver attach creates the cancel watch and `ThreadCoordinator::new_live(binding, classification, cancel_tx)`, then pushes the forwarder future | `registry/driver.rs:196-233` |
| `reserve_turn` rejects an active runtime with `ThreadBusy`; `ThreadTurnLease::acknowledge_turn` and `release` return overviews the caller must publish; `Drop` releases | `thread_runtime.rs:1118-1154`, `:1668-1725` |
| The runtime overview carries `RuntimeTurnState::Active { turn_id }` per thread | `giskard-proto/src/lib.rs:192-209` |
| Codex `turn/start`: the instance awaits the response inline with no inbox processing during the await, and the response handler registers the native turn id in the mapper before any notification of that turn is mapped. The `TurnId` returned by `start_turn` therefore equals the `TurnId` on that turn's events | `giskard-harness-codex/src/instance.rs:106-125`, `:273-296`; `lib.rs:1963-2005`; `mapping.rs:221-240` |
| Codex `thread/compact` responds on acceptance; completion is tracked separately in `pending_compactions`. The RPC is short | `instance.rs:671-707` |
| Routes: `start_turn` has no timeout wrapper; `compact_thread` is wrapped in `HARNESS_CONTROL_TIMEOUT` (2 s); `harness_api_error` maps `ThreadBusy` and `ThreadReadOnly` to 409 | `routes.rs:971-980`, `:5324-5345`, `:3779-3791`, `:52` |
| An e2e test calls `registry.start_turn` on a loaded sub-agent and expects `ThreadReadOnly` | `tests/e2e_smoke.rs:5766-5780` |
| Tests that use the deleted API: eight coordinator tests, `prepare_test_operation` and its callers, `spawn_forwarder_handle_with_runtime`, the gap test's `owns_native_turn_for_test` wait, one driver test | `thread.rs:789-1063`; `registry.rs:4310-4327`; `event_forwarder.rs:3120-3165`, `:4073`, `:4200-4268`; `driver.rs:530-562` |
| Edition 2024, `futures` is already a dependency of the server crate | `Cargo.toml:16`, `crates/giskard-server/Cargo.toml:20` |

## Design

### The intent

```rust
// registry/thread.rs
pub(super) enum TurnIntent {
    StartTurn {
        input: UserInput,
        overrides: TurnOverrides,
        context: TurnContext,
        reply: oneshot::Sender<Result<TurnId, HarnessError>>,
    },
    Compact {
        context: TurnContext,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
}

const TURN_INTENT_CAPACITY: usize = 4;

enum OwnerPhase {
    Live {
        cancel: watch::Sender<bool>,
        intents: mpsc::Sender<TurnIntent>,
    },
    Detaching {
        cancel: watch::Sender<bool>,
        waiters: Vec<oneshot::Sender<()>>,
    },
    Failed(String),
}
```

`ThreadCoordinator::intent_sender(&self) -> Result<mpsc::Sender<TurnIntent>, HarnessError>`
returns a clone for `Live`, and for `Detaching` and `Failed` the two `Protocol` errors
`prepare_operation` produces today, word for word. `request_detach` moves to `Detaching` exactly as
now; the intent sender is dropped with the `Live` variant, so the forwarder's receiver reports
closed and stops accepting intents before the cancel is observed.

The sender lives on the coordinator, not the driver. The driver serializes owner transitions; it
must not become a hop on every turn start. The registry finds the coordinator exactly as it does
today (`RegistryShared::coordinator`), asks for the sender, and sends. The forwarder is the only
holder of turn state, so it is the only possible recipient.

### The forwarder

New fields on `ThreadEventForwarder`:

```rust
harness: Weak<dyn AgentHarness>,           // upgraded per intent, never stored strong
intents: mpsc::Receiver<TurnIntent>,
intents_closed: bool,
admitted: Option<AdmittedIntent>,          // lease reserved, no native turn attached yet
inflight: Option<InflightRequest>,         // harness call in progress

struct AdmittedIntent {
    context: TurnContext,
    lease: ThreadTurnLease,
}

enum IntentReply {
    Turn(oneshot::Sender<Result<TurnId, HarnessError>>),
    Unit(oneshot::Sender<Result<(), HarnessError>>),
}

struct InflightRequest {
    request: BoxFuture<'static, Result<Option<TurnId>, HarnessError>>,
    reply: IntentReply,
    started: Instant,
}
```

`ForwardedTurnState` loses `owned_token`. Its `lease` field is now the only home of a native turn's
lease from the moment the turn is attached; nothing is parked in the coordinator.

`admitted` and `inflight` are forwarder fields, not `ForwardedTurnState` fields, because
`inflight` can outlive the turn it started (a very short turn can complete before the harness
reply is polled) and `ForwardedTurnState::reset` must not touch either.

The loop gains two branches:

```rust
loop {
    let step = tokio::select! {
        changed = self.cancel.changed() => Step::Cancelled(changed),
        intent = self.intents.recv(), if !self.intents_closed => Step::Intent(intent),
        outcome = async {
            match self.inflight.as_mut() {
                Some(request) => request.request.as_mut().await,
                None => std::future::pending().await,
            }
        }, if self.inflight.is_some() => Step::Answered(outcome),
        result = self.stream.recv() => Step::Event(result),
    };
    match step { /* below */ }
}
```

`tokio::select!` evaluates a disabled branch's expression before discarding it, so the inflight
branch must not `unwrap`; the `pending()` arm makes the expression total. Edition 2024 async blocks
capture disjoint fields, so `self.cancel`, `self.intents`, `self.inflight` and `self.stream` can be
borrowed mutably by four branches at once. If the borrow checker disagrees, move `inflight` and
`stream` into a small `Inputs` struct and select on its fields; do not introduce a lock.

`Step::Intent(None)` sets `intents_closed = true` and continues; it is not an exit. The cancel
watch remains the only exit signal, unchanged from M4.

### Admitting an intent

```text
admit(intent):
  classification = coordinator.classification().await        // new accessor, one short lock
  if classification != Primary                               -> reply ThreadReadOnly { thread }
  if inflight.is_some() || admitted.is_some() || turn.owned_turn.is_some()
                                                             -> reply ThreadBusy { thread }
  lease = runtime.reserve_turn(authority, turn_reservation(project_id, binding.handle, context))
                                                             -> on Err reply that error
  publish_runtime_overview(shared)                           // the reservation changed the overview
  harness = self.harness.upgrade()                           -> on None: lease.release() + publish,
                                                                reply Protocol("project harness is gone")
  admitted = Some(AdmittedIntent { context, lease })
  inflight = Some(InflightRequest {
      request: Box::pin(async move { harness.start_turn(&handle, input, overrides).await.map(Some) }),
      reply: IntentReply::Turn(reply), started })
```

`Compact` is identical with `harness.compact_thread(&handle).await.map(|()| None)` and
`IntentReply::Unit`. The "starting harness turn" / "starting context compaction" `info!` lines from
`registry.rs` move here with their fields.

The busy check is one line and is the whole of today's `prepare_operation` plus `reserve_turn`
guarding. `reserve_turn` still rejects an active runtime on its own; the forwarder's check comes
first so the runtime never sees an admission it has to refuse under normal operation.

### Attaching the first native turn

In `handle_event`, the `else if let Some(turn) = event_turn && !self.seen_turn_ids.contains(&turn)`
block (`event_forwarder.rs:1081-1143`) becomes:

```text
(context, lease) = match admitted.take() {
    Some(admitted) => (admitted.context, admitted.lease),
    None => {
        persisted = store.load_thread(project_id, thread_id)         // as today
        defaults = external_turn_defaults(&binding, persisted)
        classification = coordinator.classification().await
        context = TurnContext { user_input: external_turn_input_label(classification),
                                model: defaults.model, mode: defaults.mode,
                                kind: User | ExternalSubagent | ExternalOrphan by classification }
        lease = runtime.reserve_turn(authority, turn_reservation(project_id, binding.handle, &context))
            -> on Err: error!(...), Exit(RuntimeAuthorityReplaced)   // as today
        (context, lease)
    }
};
if let Some(overview) = lease.acknowledge_turn(turn) { hub.publish_runtime_overview(overview) }
turn.context = context; turn.lease = Some(lease); turn.owned_turn = Some(turn);
```

`external_turn_input_label` and the kind mapping move from `claim_native_turn` into this block
(`thread.rs:497-503` and `:330-343`); the function itself can stay in `thread.rs`.

The lease is acknowledged with the turn id at attach for both origins. Today the external path
already does this at attach, and the prepared path does it either when the harness replies or at
`TurnStarted`. Acknowledging at attach is the earliest correct point and removes the
`acknowledge_native_turn` call in the `TurnStarted` arm (`event_forwarder.rs:1388-1394`).

### Handling the harness answer

```text
answered(outcome):
  request = inflight.take()
  match outcome {
    Ok(turn_id) => {
      if let Some(admitted) = admitted.as_mut() && let Some(id) = turn_id
         && let Some(overview) = admitted.lease.acknowledge_turn(id) { publish(overview) }
      if let (Some(owned), Some(id)) = (turn.owned_turn, turn_id) && owned != id
         { warn!(... "harness named a different turn than the one already attached") }
      reply Ok
    }
    Err(error) => {
      if let Some(mut admitted) = admitted.take()
         && let Some(overview) = admitted.lease.release() { publish(overview) }
      reply Err(error)
    }
  }
```

`reply Ok` sends `Ok(id)` for `IntentReply::Turn` (the `Option` is always `Some` for a start) and
`Ok(())` for `IntentReply::Unit`. A dropped receiver (the HTTP client went away) is ignored; the
turn proceeds, as today's detached task does.

### Exit

`finish` replaces its two coordinator pulls (`event_forwarder.rs:865-871`) and its
`finish_native_turn` (`:915-917`) with:

```text
if let Some(mut admitted) = admitted.take() && let Some(overview) = admitted.lease.release() { publish }
if let Some(request) = inflight.take() { request.reply.send(Err(Protocol("event owner exited before the harness answered"))) }
// turn.lease release stays exactly as it is
```

`handle_stream_error` drops its `take_native_turn_gate` (`:928-932`) and `finish_native_turn`
(`:999-1003`) calls; the lease is already in `turn.lease`. The completion arm drops
`take_native_turn_gate` (`:1583-1588`) and `finish_native_turn` (`:1604-1606`).

### Why no token is needed: the interleavings

Let A be the intent admitted, R the harness reply, E the first native event of the turn, C its
completion. All four are processed by one loop in one order, so each case is a plain state check:

| Order | What happens |
| --- | --- |
| A R E C | R acknowledges the admitted lease with the id; E adopts `admitted`; C completes and resets |
| A E R C | E adopts `admitted` and acknowledges; R finds `admitted` empty and `owned_turn == id`, replies `Ok` |
| A E C R | The turn is already reset when R arrives; R replies `Ok`. A second intent in the window between C and R is refused with `ThreadBusy` because `inflight` is set. Today the same window admits it; the stricter rule is sound and the browser already retries `thread_turn_active` |
| A R(Err) | `admitted` is released, reply `Err`. Identical to today's `abort_operation` |
| A E R(Err) | The harness said it failed after a turn had already started. The turn keeps the admitted context and continues as observed; the reply is `Err`. Today `abort_operation` finds no operation and does the same |
| A then cancel or stream end before E | `finish` releases the admitted lease and answers the inflight request with an error. Today's `take_unclaimed_operation` |
| Detach while R is pending | The cancel branch wins the select; `finish` drops the request future. For Codex the dropped response receiver is harmless. The thread's native turn, if it started, is attached as external by the next owner |
| HTTP client dropped after sending | The reply send fails silently; the turn proceeds |

The old design needed tokens for two reasons that no longer exist: the handler's abort could race
the forwarder's claim (now both are the forwarder), and a stale owner generation could finish a
newer generation's turn (there is one owner per coordinator since M4, and the coordinator is
replaced, not reused, after detach).

### The coordinator after M5

```rust
struct ThreadCoordinatorState {
    binding: LoadedThreadBinding,
    classification: ClassificationPhase,
    owner: OwnerPhase,
}
pub(super) struct ThreadCoordinator { state: AsyncMutex<ThreadCoordinatorState> }
```

Kept: `new` (test), `new_live(binding, classification, cancel, intents)`, `binding`,
`classify_orphan_as_subagent`, `reusable_handle`, `is_detaching`, `is_failed`, `request_detach`,
`owner_exited` (minus the generation bump). New: `classification()` and `intent_sender()`.
Deleted: everything in the ground-truth table's first four rows, `NativeActivity`, `changed`,
`ClaimedNativeTurn`, `NativeTurnOrigin`, `owns_native_turn_for_test`. `ExternalTurnDefaults` stays
(the forwarder's `external_turn_defaults` builds it).

### The registry after M5

```rust
pub async fn start_turn(&self, thread_id, input, overrides, effective_model) -> Result<TurnId, HarnessError> {
    let coordinator = self.shared.coordinator(thread_id).await.ok_or(HarnessError::ThreadNotFound(thread_id))?;
    let intents = coordinator.intent_sender().await?;
    let context = TurnContext { user_input: input.clone(), model: TurnModel::Known(effective_model),
                                mode: TurnMode::Known(overrides.mode), kind: TurnContextKind::User };
    let (reply, response) = oneshot::channel();
    intents.send(TurnIntent::StartTurn { input, overrides, context, reply }).await
        .map_err(|_| HarnessError::Protocol(format!("thread {thread_id} has no live event owner")))?;
    response.await
        .map_err(|_| HarnessError::Protocol(format!("thread {thread_id} event owner exited before answering")))?
}
```

`compact_thread` is the same eleven lines with `UserInput::text("/compact")`,
`TurnContextKind::ManualCompaction` and `TurnIntent::Compact`. Both lose the `active_harness`
lookup, the background-task permit, the `tokio::spawn`, and the three abort paths. `admit_operation`
and `abort_admitted_operation` are deleted. The forwarder already runs under the driver's permit,
so no permit is needed for the harness call.

### The driver after M5

`attach` (`driver.rs:196-233`) creates `let (intent_tx, intent_rx) = mpsc::channel(TURN_INTENT_CAPACITY)`
next to the cancel watch, passes `intent_tx` to `new_live`, and passes `self.harness.clone()`
(the `Weak`) and `intent_rx` to `ThreadEventForwarder::new`. Nothing else changes.

## Every site that changes

| File | Lines | Change |
| --- | --- | --- |
| `registry/thread.rs` | `19-23`, `62-95`, `102-131` | Delete `CoordinatorToken`, `NativeActivity`, `PreparedOperation`, `NativeTurnOrigin`, `OwnedNativeTurn`, `ClaimedNativeTurn`, the state fields, `token`, `token_is_current` |
| `registry/thread.rs` | `41-48` | `OwnerPhase::Live` becomes `Live { cancel, intents }` |
| `registry/thread.rs` | new | `TurnIntent`, `TURN_INTENT_CAPACITY`, `classification()`, `intent_sender()` |
| `registry/thread.rs` | `135-158` | `new` builds a dummy intent channel; `new_live` takes `intents` |
| `registry/thread.rs` | `179-250`, `285-417`, `476-484` | Delete the eight turn methods and the test probe |
| `registry/thread.rs` | `252-283` | `reusable_handle` matches `Live { .. }` |
| `registry/thread.rs` | `427-474` | `request_detach` destructures `Live { cancel, .. }`; `owner_exited` loses the generation bump and `native_activity` writes |
| `registry/thread.rs` | `113-117` | Delete `changed: Notify` and its four `notify_waiters` calls |
| `registry.rs` | `66-67` | Drop `CoordinatorToken` from the import; add `TurnIntent` |
| `registry.rs` | `397-436` | Delete `abort_admitted_operation`, `admit_operation` |
| `registry.rs` | `1081-1187` | `start_turn` as above |
| `registry.rs` | `1396-1489` | `compact_thread` as above |
| `registry.rs` | `108-120` | `turn_reservation` stays; it is now called only from the forwarder |
| `registry/event_forwarder.rs` | `689-712` | Delete `DuplicateForwarder` and its label (its only producer was the claim failure) |
| `registry/event_forwarder.rs` | `714-758` | Drop `owned_token` from `ForwardedTurnState` and `reset` |
| `registry/event_forwarder.rs` | `761-830` | New fields; `new` takes `harness: Weak<dyn AgentHarness>` and `intents: mpsc::Receiver<TurnIntent>` |
| `registry/event_forwarder.rs` | `835-856` | The four-branch loop; `admit`, `answered` handlers |
| `registry/event_forwarder.rs` | `862-918` | `finish` as above |
| `registry/event_forwarder.rs` | `920-1010` | Remove the two coordinator calls |
| `registry/event_forwarder.rs` | `1081-1143` | Attach block as above; delete the `install_native_turn_gate` branch |
| `registry/event_forwarder.rs` | `1384-1394` | Delete the `acknowledge_native_turn` call |
| `registry/event_forwarder.rs` | `1583-1606` | Delete `take_native_turn_gate` and `finish_native_turn` calls |
| `registry/driver.rs` | `196-233` | Create the intent channel; pass `Weak` harness and receiver |
| `registry/driver.rs` | `1-18` | Import `mpsc` is already there; import `TurnIntent` |

Expected non-test delta: about 330 lines removed from `thread.rs`, 130 from `registry.rs`, and
140 added to `event_forwarder.rs`. Net negative, well under the budget.

## Tests

### Forwarder tests (`event_forwarder.rs`)

Add a `TestIntentHarness` to the test module: an `AgentHarness` whose `start_turn` returns a
configured `Result<TurnId, HarnessError>` after awaiting an optional `Notify` gate, and counts
calls. `compact_thread` likewise. Replace `spawn_forwarder_handle_with_runtime` with
`spawn_forwarder_with_intents` returning the join handle, runtime, coordinator, authority and the
`mpsc::Sender<TurnIntent>`; its callers stop pre-preparing an operation and instead send a
`StartTurn` intent when they need a user-labelled turn.

1. `an_intent_reserves_the_runtime_and_the_first_native_turn_adopts_it` (A R E C). Send the
   intent; the harness replies `T`; assert the overview shows `Active { turn_id: Some(T) }`; append
   `TurnStarted T`, one item, `TurnCompleted T`; assert the persisted turn carries the intent's
   input, model and mode, and the runtime is idle.
2. `native_events_before_the_harness_reply_still_adopt_the_intent` (A E R C). Gate the harness
   reply; append the turn's events first; release the gate; assert the reply is `Ok(T)` and the
   persisted turn carries the intent context.
3. `a_harness_rejection_releases_the_admitted_lease` (A R(Err)). Reply is the error; runtime idle;
   a second intent is admitted.
4. `a_second_intent_while_one_is_admitted_is_thread_busy`. Gate the first reply; the second intent's
   reply is `ThreadBusy`; the harness saw one call.
5. `a_subagent_owner_rejects_intents_as_read_only`. Classification `Subagent`; reply
   `ThreadReadOnly`; the harness saw no call; runtime idle.
6. `stream_end_before_the_native_turn_releases_the_admitted_intent` (rewrite of
   `stream_end_before_turn_started_releases_prepared_operation`, `:3120`). Gate the reply; close the
   log; the forwarder exits; runtime idle; the reply is an error.
7. `detach_while_the_harness_reply_is_pending_does_not_block` — in `driver.rs`, rewrite of
   `detach_cancels_the_owner_and_clears_the_slot` (`:530-562`): the driver test harness's
   `start_turn` awaits a never-signalled `Notify`; send an intent through
   `coordinator.intent_sender()`; `driver.detach` completes within the test timeout; runtime idle;
   the intent reply is an error.
8. `a_compaction_intent_labels_the_native_turn_as_manual_compaction`. Send `Compact`; the harness
   accepts; append the turn's events; assert the persisted turn's context kind or the existing
   compaction assertions the module already makes.
9. `forwarder_gap_recovers_but_truncates_the_interrupted_native_turn` (`:4073`): replace the
   `owns_native_turn_for_test` wait with a wait on
   `runtime.current_overview()` showing `Active { turn_id: Some(lagged_turn) }` for the thread.
10. `replacement_forwarder_persists_events_sent_while_no_forwarder_ran` (M0 test D): its helper
    now sends an intent whose harness reply is the test's own `turn` id. Everything else unchanged.

### Coordinator tests (`thread.rs`)

Delete the eight tests at `:789-1063`; each one asserts a property of tokens or of the coordinator
holding turn state, and the property either holds trivially now or is covered by the forwarder tests
above (`subagent_coordinator_rejects_prepared_operations` → test 5,
`cancelling_operation_admission_cannot_leave_runtime_reserved` → test 6, the four `stale_*` tests
→ the interleaving table, `external_claim_rederives_context_after_classification` →
`long_lived_forwarder_uses_current_external_context_for_each_turn` at `event_forwarder.rs:2669`,
`mismatched_native_start_preserves_the_active_turn` → the cross-turn drop already at `:1062-1079`,
`failed_owner_rejects_new_preparation_before_io` → the new test below).

Add `intent_sender_follows_the_owner_phase`: `Live` returns a sender; after `request_detach` it is
`Protocol("… has no live event owner")`; after `owner_exited(PersistenceBlocked)` on a fresh
coordinator it is `Protocol("… event owner failed: persistence_blocked")`.

### Registry tests (`registry.rs`)

Delete `prepare_test_operation` (`:4310-4327`). Keep `test_coordinator`, `install_test_coordinator`,
`test_authority`, `test_turn_context`. No registry test exercises `start_turn` directly; the 22
call sites in `e2e_smoke.rs` cover it end to end and must pass unchanged, including the
`ThreadReadOnly` expectation at `:5780`.

### Existing tests that must keep passing unchanged

Every driver test except the one rewritten above; every external-turn forwarder test
(`persist_external_turn_input`, `an_unclassified_native_turn_does_not_claim_to_be_a_sub_agent`,
`long_lived_forwarder_uses_current_external_context_for_each_turn`); the M0 scenario tests in the
adapter; the e2e suite.

## Documentation

- `docs/event-pipeline-milestones.md`: M5 status, the amended design (intents go to the thread's
  forwarder through the coordinator's owner phase; interrupt stays direct), the narrowed exit
  criterion, and a plan pointer.
- `docs/subagents.md:94-98`: "clears only the matching coordinator token" → the owner attaches the
  first event of a new native turn to the admitted intent if there is one, else labels it external;
  completion resets the owner's turn state.
- `specs/giskard-specification.md`: bump to 1.82 with an amendment "turn intents": starting a
  turn or compaction sends an intent to the thread's event owner; the owner reserves the runtime,
  calls the harness, and attaches the first native turn it sees; there is no coordinator token or
  generation. Rewrite the normative paragraph at `:3615-3621` ("The per-thread coordinator
  serializes prepared primary operations…") to describe the owner's admission. Leave the 1.76
  amendment text as history.
- `AGENTS.md` next to the M4 rule (`:127-129`): "Turn admission is a `TurnIntent` to the thread's
  event forwarder. No code outside the forwarder reserves a primary thread's turn lease or calls
  `start_turn` / `compact_thread` on a harness."

## Order of work

1. **Additive.** `TurnIntent`, `OwnerPhase::Live { cancel, intents }`, `intent_sender`,
   `classification()`, `new_live` signature, driver channel creation, forwarder fields and the two
   new select branches with `admit` and `answered`. `admit` uses `admitted`/`inflight`; the attach
   block still calls `claim_native_turn` when `admitted` is empty. Everything compiles, all tests
   pass, nothing sends intents yet.
2. **Switch.** `start_turn` and `compact_thread` send intents. The attach block takes `admitted`
   first and only falls back to the external path. `finish` and `handle_stream_error` release
   `admitted`. Run the whole suite; the e2e suite is the switch's proof.
3. **Delete.** The coordinator's turn API and types, `admit_operation`,
   `abort_admitted_operation`, the forwarder's coordinator calls, `owned_token`,
   `DuplicateForwarder`, `NativeActivity`, `changed`. Rewrite the tests listed above.
4. Docs.

Each step is a separate commit; each compiles and passes on its own.

## Verification the implementer must perform and record

- `grep -rn 'CoordinatorToken\|prepare_operation\|abort_operation\|claim_native_turn\|acknowledge_operation_turn\|take_unclaimed_operation\|install_native_turn_gate\|take_native_turn_gate\|finish_native_turn\|acknowledge_native_turn\|admit_operation\|owned_token\|generation' crates/giskard-server/src/registry crates/giskard-server/src/registry.rs` → empty.
- `grep -n 'tokio::spawn' crates/giskard-server/src/registry.rs` no longer lists the `start_turn`
  and `compact_thread` sites (`:1138`, `:1456` today).
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace` with zero ignored tests.
- A manual run against real Codex: send a message and see it complete; send a second message while
  the first runs and get the 409; run `/compact`; interrupt a running turn; unload and reopen the
  thread while a turn is starting.
- Non-test line delta recorded in the PR description.

## Pitfalls

- `tokio::select!` evaluates every branch expression, disabled or not. The inflight branch must be
  total (`pending()` when `None`), never `unwrap`.
- Do not hold the coordinator mutex across any await into the harness or the runtime.
  `classification()` is one short lock; the harness call is a boxed future polled by the loop.
- Every `acknowledge_turn` and `release` returns an overview that must be published; the
  `#[must_use]` on `acknowledge_turn` is there for a reason.
- Reply exactly once per intent. The reply sender is moved into `InflightRequest` or answered
  immediately in `admit`; `finish` answers whatever is still pending.
- Keep the `Weak` harness. Upgrade per intent; the boxed future holds the strong reference only
  for the duration of the call. The driver test `the_driver_does_not_keep_the_harness_alive` must
  keep passing.
- The forwarder's busy check must come before `reserve_turn`, so a refused intent never touches the
  runtime and never publishes an overview.
- The inflight future resolves when the harness accepts the request, never when the native turn
  completes. Nothing in the forwarder has a timeout, and no state transition depends on one. The
  route's pre-existing `HARNESS_CONTROL_TIMEOUT` only bounds how long the WebSocket client waits
  for the compaction reply; when it fires, the reply receiver is dropped and the admitted intent
  proceeds unchanged, exactly as today's detached task does. Do not add a timeout inside the
  forwarder to "match" it.
- Tests order events with gates (`Notify`) on the test harness's reply, never with sleeps. The
  only deadlines in tests are the existing `wait_for_*` bounds that fail a stuck test.
- Do not re-introduce acknowledgement at `TurnStarted`; attach already acknowledged.
- The `ThreadReadOnly` check lives in the forwarder because the classification lives on the
  coordinator; routes still call `ensure_thread_writable` first, so the forwarder's check is the
  registry-level guarantee the e2e test at `:5780` exercises, not the user-facing one.

## Stop rules

Stop and re-cut if:

- a token, sequence number or generation reappears anywhere;
- the driver becomes a hop for intents, or a keyed map of intent senders appears anywhere;
- the forwarder needs `tokio::spawn` to call the harness;
- a coordinator lock is held across an await into the harness, runtime or persistence;
- `complete_forwarded_turn` or the item reduction needs to change;
- the harness trait or the adapter needs a change;
- non-test lines exceed the budget.
