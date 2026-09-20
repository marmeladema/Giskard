# Server-request claims survive a harness-side resolution

Implementation plan for the `stale claim for request <id>` protocol error raised when a user
answers a Codex server request (a multiple-choice question, an elicitation, any
`ServerRequestReceived`). Written against `main` at `3c804dd` (spec 1.85). Every file and line
reference below was checked against that tree; re-check them if the branch has moved.

## Problem

Answering a server request runs two resolution paths against one runtime record, and nothing
orders them:

1. The WebSocket route calls `Registry::respond_server_request`
   (`crates/giskard-server/src/registry.rs:982-1040`). It claims the record, which moves it from
   `Pending` to `Responding(claim_id)` and publishes that transition, awaits
   `harness.respond_server_request`, then calls `RequestClaim::commit` (`:1016`).
2. The Codex adapter's `handle_respond_server_request`
   (`crates/giskard-harness-codex/src/lib.rs:2585-2620`) writes the JSON-RPC response to Codex,
   then appends `AgentEvent::ServerRequestResolved` to the retained event log, then returns. The
   control reply travels back to the registry through a oneshot (`lib.rs:1351-1367`).
3. The thread's event forwarder reads that event and applies it through `apply_event_locked`
   (`crates/giskard-server/src/thread_runtime.rs:1020-1022`), which calls
   `resolve_server_request_from_harness` (`:1784-1813`). That function skips only records that are
   already `Resolved`; a record in `Responding` is overwritten with a synthesized
   `Resolved(Server(Null))` and a new revision is published.
4. When the control reply reaches the registry, `commit` (`:1936-1943`) finds the status is no
   longer `Responding(claim_id)`, returns `HarnessError::Protocol("stale claim for request …")`,
   and the route surfaces it as `harness_protocol_error` with action `server_request_response`.
   `rollback_inner` (`:1972-1995`) does nothing because the record is no longer `Responding`.

The answer was delivered, so the turn continues; the user only sees the error. Which side wins is
scheduling. Since M3 the single Codex task appends the event before it replies, and since M7 the
forwarder's biased select processes retained events before intents, so step 3 now reliably
precedes step 4. The same code existed before the milestones (both functions date from `0cf6e60`);
the ordering that hid the race was accidental.

Approvals are not affected: nothing on the harness side resolves an approval record
(`registry.rs` `respond_approval`, same claim/commit shape, no `ApprovalResolved` event exists).

There is a second, rarer instance of the same race. Codex can resolve a server request on its own,
for example when the thread is interrupted (`lib.rs:2622-2700`
`reject_pending_requests_for_interrupted_thread`, and the `serverRequest/resolved` notification at
`mapping.rs:748-762`). If that lands while a claim is in flight and the claimant's harness call then
fails, today's rollback puts the record back to `Pending` even though Codex is no longer waiting,
and the request stays actionable forever. The fix below covers both instances.

## Goal

A harness-side resolution observed while a claim is in flight never preempts the claim:

- if the claimant commits, the record resolves with the claimant's real answer;
- if the claimant rolls back, the record resolves with the harness resolution instead of returning
  to `Pending`;
- no client-visible transition is published for the harness resolution while the claim is open,
  so the revision-gated `RequestState` sequence a tab sees is exactly
  `pending → responding → resolved`.

Every other rule of RT2 stays: a claim on a non-pending record is refused, a commit with a claim id
that is not the current one is stale, a failed or timed-out call without a harness resolution rolls
back to `Pending`.

## Non-goals

- No change to the adapter. It keeps emitting `ServerRequestResolved` after writing the response;
  spec SR6 relies on that event for reconnect and it is correct as is.
- No change to `AgentHarness`, the forwarder, the hub, the wire types, or the browser. The
  `WireRequestStatus` projection (`thread_runtime.rs:1881-1891`) is unchanged.
- No ordering between the control reply and the event stream. Both orders must produce the same
  end state; that is the whole point.
- No timers.

## Ground truth

| Fact | Where |
| --- | --- |
| `RequestStatus { Pending, Responding(u64), Resolved(RequestResolution) }`, `RequestRecord { turn_id, payload, status, revision }`, `RequestClaim { …, claim_id, settled }` | `thread_runtime.rs:260-290` |
| `RequestTransition { request_state, overview_if_changed }`, `RequestCommitError { error, rollback }` | `thread_runtime.rs:228-237` |
| `claim_request`: refuses a non-`Pending` record, sets `Responding(next_claim_id())`, bumps revision, returns the claim and a transition | `thread_runtime.rs:1215-1250` |
| `commit`: requires `status == Responding(self.claim_id)`, else "stale claim" and `rollback_inner`; checks payload/resolution kind; sets `Resolved`, bumps revision | `thread_runtime.rs:1909-1966` |
| `rollback` / `rollback_inner`: only a record still in `Responding(self.claim_id)` goes back to `Pending` (bumps revision); otherwise `None` | `thread_runtime.rs:1968-1995` |
| `Drop for RequestClaim`: rolls back and logs "rolled request back to pending"; the transition is not published (the route's timeout path republishes) | `thread_runtime.rs:1997-2010`; `routes.rs:5262-5275` |
| `resolve_server_request_from_harness`: no record → warn, false; `Resolved` → false; anything else → `Resolved(Server(result(Null)))`, revision bump, true | `thread_runtime.rs:1784-1813` |
| `apply_event_locked` publishes a `RequestState` only when the record changed (`request_changed`) and asks for an overview refresh on the same flag | `thread_runtime.rs:1020-1022, 1041-1062` |
| `register_request`: an existing record is never resurrected; identical redelivery is a no-op | `thread_runtime.rs:1750-1782` |
| `runtime_summary` lists `Pending` and `Responding` records as outstanding with a `responding` flag | `thread_runtime.rs:1836-1851` |
| Registry flow: claim → publish → harness call → on error rollback+publish → commit → on error rollback+publish → `resolve_live_server_request` → publish | `registry.rs:982-1040` |
| Route: 2 s `HARNESS_CONTROL_TIMEOUT`; on timeout the claim future is dropped and the state republished | `routes.rs:52, 5237-5290` |
| Adapter: write response, `mapper.resolve_server_request`, then `broadcast_event(ServerRequestResolved)`, then `Ok(())`; control command goes through the single Codex task | `giskard-harness-codex/src/lib.rs:2585-2620, 1351-1367` |
| Codex-originated resolution: interrupt rejection and the `serverRequest/resolved` notification | `lib.rs:2622-2700`; `mapping.rs:748-762` |
| Browser: `responding` disables the card's controls; `resolved` resolves the card; `RequestState` is revision-gated | `static/app.js:3095, 3240-3250` |
| Spec: RT2 `:218-221`; request resolution invariant and SR6 `:3709-3722`; version `:12`; amendment format `:14-18` | `specs/giskard-specification.md` |
| Existing runtime tests: `requests_are_claimed_independently_and_failed_claims_roll_back` `:2558`, `failed_commit_returns_the_authoritative_rollback_transition` `:2626`, `duplicate_request_event_does_not_resurrect_a_resolved_request` `:2666`, `claim_validates_the_thread_identity` `:2917`; helpers `test_authority` `:2032`, `approval` `:2036`, `apply_event_for_test` `:411` | `thread_runtime.rs` |
| Existing integration tests and fixture: `ServerRequestHarness` with `fail_next_response`, `hang_next_response`, `suppress_resolution`; its `respond_server_request` appends `ServerRequestResolved` then `TurnCompleted` before returning, exactly the production order, but nothing forces the forwarder to run first, so the race is not pinned | `crates/giskard-server/tests/server_requests.rs:36-60, 168-200, 343-380, 484-525, 701-730` |
| Forwarder test template driving `ServerRequestReceived` through a log and a hub subscriber | `event_forwarder.rs:4474-4575` |

## Design

### D1. The claim owns the record until it settles

```rust
enum RequestStatus {
    Pending,
    Responding { claim: u64, harness_resolved: bool },
    Resolved(RequestResolution),
}
```

`harness_resolved` records that the harness closed the request while a claim was open. It is
turn-local runtime state on the record that already exists; no new map, no new authority.

### D2. Transitions

| Current status | Event | New status | Published? |
| --- | --- | --- | --- |
| `Pending` | harness resolution | `Resolved(Server(Null))`, revision+1 | yes (unchanged) |
| `Responding { claim, .. }` | harness resolution | `Responding { claim, harness_resolved: true }`, revision unchanged | **no** |
| `Resolved` | harness resolution | unchanged | no (unchanged) |
| `Responding { claim == self.claim, .. }` | `commit(resolution)` | `Resolved(resolution)`, revision+1 | yes (unchanged) |
| `Responding { claim != self.claim, .. }` or not `Responding` | `commit` | unchanged; error "stale claim" | rollback result (unchanged) |
| `Responding { claim == self.claim, harness_resolved: false }` | rollback / drop | `Pending`, revision+1 | yes (unchanged) |
| `Responding { claim == self.claim, harness_resolved: true }` | rollback / drop | `Resolved(Server(Null))`, revision+1 | yes (**new**) |
| `Responding { claim != self.claim, .. }` | rollback / drop | unchanged | `None` (unchanged) |

Concretely, in `thread_runtime.rs`:

- `claim_request` (`:1231-1233`): `Responding { claim: claim_id, harness_resolved: false }`.
- `resolve_server_request_from_harness` (`:1784-1813`): add an arm before the synthesis:

  ```rust
  if let RequestStatus::Responding { harness_resolved, .. } = &mut record.status {
      if !*harness_resolved {
          debug!(%thread_id, request_id = %request_id.0,
                 "harness resolved a server request while a claim is in flight; deferring to the claimant");
      }
      *harness_resolved = true;
      return false;
  }
  ```

  Returning `false` keeps `request_changed` false, so `apply_event_locked` publishes no
  `RequestState` and requests no overview refresh (`:1041-1062`).
- `commit` (`:1936`): match `Responding { claim, .. } if claim == self.claim_id`; everything else
  is the existing stale-claim branch.
- `rollback_inner` (`:1983`): match `Responding { claim, harness_resolved } if claim ==
  self.claim_id`; set `Pending` when `!harness_resolved`, else
  `Resolved(RequestResolution::Server(ServerRequestResponse::result(Null)))`, the same value the
  harness path synthesizes today. Bump the revision and return the transition in both cases.
- `Drop` (`:1997-2010`): the warning text says "rolled request back" today; make it name the
  outcome (`pending` or `resolved`) from the returned state.
- `wire_request_state` (`:1881`) and `runtime_summary` (`:1838-1841`): pattern `Responding { .. }`.

Approvals get the same enum for free; `harness_resolved` is never set for them because no
harness event resolves an approval, and the plan does not add one.

### D3. What the registry and route see

Nothing changes in `registry.rs:982-1040` or `routes.rs:5237-5290`. With D2:

- Normal answer, forwarder first: claim → harness writes and emits → forwarder marks
  `harness_resolved` (no publish) → reply → commit succeeds with the real answer → publish
  `resolved` → `resolve_live_server_request`. Tabs see revisions 1, 2, 3.
- Normal answer, registry first: unchanged from today; the later harness event is a no-op on a
  `Resolved` record.
- Harness call fails after Codex resolved on its own: claim → Codex resolves → forwarder marks →
  harness returns `Err` → `claim.rollback()` returns a `Resolved` transition → published. A retry
  from the tab is refused with "request … is not pending", which is correct: nothing is waiting.
- Route timeout after Codex resolved on its own: the dropped claim resolves the record; the route's
  `republish_server_request_state` sends `resolved` to every tab instead of `pending`.

### D4. Why this is timing-free

The record has one owner at a time: the claimant, identified by `claim`. A harness resolution
during the claim is stored as a fact on the record and consumed by whichever settlement the
claimant reaches. Every interleaving of {harness event applied, control reply received} yields the
same final status and the same published sequence. No path waits for the other.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/thread_runtime.rs:260-264` | `Responding { claim, harness_resolved }` |
| `thread_runtime.rs:1231-1233` | claim sets the struct form |
| `thread_runtime.rs:1784-1813` | defer-to-claimant arm |
| `thread_runtime.rs:1838-1841, 1881` | pattern updates |
| `thread_runtime.rs:1936, 1983-1995, 1997-2010` | commit match, rollback outcome, drop log |
| `specs/giskard-specification.md` | see Documentation |
| `crates/giskard-server/tests/server_requests.rs` | fixture gate and new tests (below) |

No other crate compiles against `RequestStatus`; it is private to `thread_runtime.rs`.

## Tests

The existing suite never forces the forwarder to apply the harness resolution before the claimant
commits, which is why this shipped. Every test below either constructs the interleaving directly
(runtime unit tests) or fences it with an ordered event (forwarder and integration tests).

### Ordering fence for the async tests

The forwarder processes its log in order and the hub is a per-thread FIFO. A test that needs
"the forwarder has applied event E" appends a distinctive `AgentEvent::Notice { thread, turn,
message }` right after E and waits for that notice on a subscriber. Notices are deduplicated by
`(turn, message)` (`event_forwarder.rs:10-18`), so use a unique message per test. This is the same
fence pattern the milestone tests use; it is order-based, not time-based.

### Runtime unit tests (`thread_runtime.rs`, next to `:2558`)

Add a helper `server_request(id: &str) -> ServerRequest` and register through
`runtime.apply_event(&authority, &AgentEvent::ServerRequestReceived { … }, false)` so the record
takes the production path.

1. `harness_resolution_during_claim_does_not_preempt_commit`
   Register `srv`; claim → transition revision 2, `Responding`. Apply
   `ServerRequestResolved { request_id: srv }` → the returned `AppliedRuntimeEvent.request_state`
   is `None`, `request_state()` still reports `Responding` at revision 2. Commit with
   `Server(result({"answer": 1}))` → `Ok`, revision 3, `Resolved`. Then assert the stored
   resolution is the claimant's value, not `Null`: expose it through a `#[cfg(test)]`
   accessor on `ThreadRuntimeSupport` (`resolution_for_test(&authority, &id) ->
   Option<RequestResolution>`).
2. `harness_resolution_during_claim_resolves_on_rollback`
   Same setup; `claim.rollback()` → `Some(transition)` with `Resolved { resolution: Server }` at
   revision 3, not `Pending`. `claim_request` afterwards → `Err` containing "is not pending".
3. `harness_resolution_during_claim_resolves_on_drop`
   Same setup; `drop(claim)`; `request_state()` reports `Resolved` at revision 3.
4. `rollback_without_harness_resolution_still_returns_to_pending`
   Claim, rollback → `Pending` at revision 3; `claim_request` succeeds again. (Guards the
   existing behaviour through the new match.)
5. `harness_resolution_before_any_claim_still_resolves_and_publishes`
   Apply `ServerRequestResolved` on a `Pending` record → `request_state` is `Some`, `Resolved`,
   revision 2; `claim_request` → `Err`. (Existing behaviour, now pinned.)
6. `harness_resolution_after_commit_is_a_no_op`
   Claim, commit with a real value, apply `ServerRequestResolved` → `request_state` `None`,
   revision unchanged, stored resolution still the real value.
7. `repeated_harness_resolutions_during_claim_are_idempotent`
   Apply the resolved event twice during the claim → no publish either time, revision unchanged;
   commit still succeeds.
8. `only_the_current_claim_can_settle_the_record`
   Claim A, `A.rollback()` → `Pending`; claim B; apply a harness resolution; `B.commit` → `Ok`.
   Separately: claim A and hold it; a second `claim_request` → `Err("is not pending")`. Then
   apply a harness resolution and drop A → the record is `Resolved`; a fresh `claim_request` →
   `Err`. Together these show the stale-claim and single-owner rules survive the new enum shape.
9. `duplicate_request_delivery_during_a_claim_keeps_the_claim`
   Claim; apply an identical `ServerRequestReceived` → no change (`register_request` returns
   false); apply one with changed params → payload refreshed, revision+1, status still
   `Responding` with `harness_resolved` preserved (set it first, then redeliver, then commit →
   `Ok`).
10. `responding_record_with_harness_resolution_is_still_outstanding_in_the_overview`
    Claim, apply harness resolution, `current_overview()` lists the request with
    `responding: true`; after commit the request is gone from `outstanding_requests`.
11. `approval_claims_are_untouched_by_server_resolutions`
    Register approval `a` and server request `a` (different `RuntimeRequestId` kinds, same
    string). Claim the approval, apply `ServerRequestResolved { request_id: a }` → the server record
    resolves (it was `Pending`), the approval stays `Responding`; commit the approval → `Ok`.
12. `commit_with_mismatched_resolution_kind_after_harness_resolution_resolves_on_rollback`
    Claim a server request, apply harness resolution, commit with `Approval(Accept)` → `Err`
    (kind mismatch) and `failure.rollback` is `Some` with status `Resolved`, not `Pending`.

### Forwarder test (`event_forwarder.rs`, modelled on `:4474-4575`)

13. `forwarder_applied_harness_resolution_does_not_break_a_pending_claim`
    Spawn a forwarder with a hub subscriber. Append `TurnStarted`, then
    `ServerRequestReceived { turn: Some(turn), id: "q" }`; wait for the `RequestState` `Pending`
    (revision 1). Call `runtime.claim_request(&authority, Server("q"))` directly → revision 2,
    `Responding`. Append `ServerRequestResolved { "q" }` then `Notice { message: "fence-13" }`;
    wait for the notice on the subscriber. Assert no `RequestState` message arrived after the
    `Responding` one. Commit → `Ok`, revision 3. Drain the subscriber: the `RequestState`
    sequence is exactly `[pending 1, responding 2]` from the forwarder/claim and the commit
    transition is the caller's to publish (the test publishes nothing; it only asserts the hub
    saw no harness-originated `resolved`).
14. `forwarder_publishes_harness_resolution_for_an_unclaimed_request`
    Same setup without a claim: `ServerRequestResolved` → a `RequestState` `Resolved` at
    revision 2 is broadcast. (Pins the unchanged path.)

### Integration tests (`tests/server_requests.rs`)

Fixture change: add `resolve_before_reply: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>` to
`ServerRequestHarness` and a `resolve_before_reply()` method returning the `Sender`. In
`respond_server_request`, after recording the response and appending `ServerRequestResolved`
(keep the existing order), if the receiver is set: append
`Notice { thread, turn: Some(turn), message: "resolution-fence" }`, then `await` the receiver, then
append `TurnCompleted` and return. `fail_next_response` and `hang_next_response` must be checked
after that gate for tests 16 and 17, so restructure the method as: record → (optional) append
resolved + fence + await gate → (optional) fail → (optional) hang → append completion → `Ok`.
Note `suppress_resolution` keeps its meaning.

Add `wait_for_notice(ws, message)` next to `wait_for_server_request` (`:667`).

15. `server_request_answer_succeeds_when_the_harness_resolves_first`
    Subscribe, send input, `wait_for_server_request`. `let gate = harness.resolve_before_reply()`.
    Send `ServerRequestResponse`. `wait_for_notice(&mut ws, "resolution-fence")` (this proves the
    forwarder applied the resolution while the route is still awaiting the harness). `gate.send`.
    Then: `wait_for_request_state(&mut ws, "resolved")` has revision 3; no `ServerMessage::Error`
    arrives before the `TurnCompleted` event (drain until `turn_completed`, failing on any error);
    `harness.wait_for_response()` returns the answer. This test fails on `main` with
    `harness_protocol_error` / "stale claim for request srv_1".
16. `harness_failure_after_a_native_resolution_leaves_the_request_resolved`
    As 15 but also `fail_next_response(Protocol("late failure"))`. Expect the WS error with that
    detail, then `RequestState` `Resolved` at revision 3 (not `Pending`). A second
    `ServerRequestResponse` for `srv_1` → error whose detail contains "is not pending".
17. `timeout_after_a_native_resolution_republishes_resolved_to_peer_tabs`
    Two sockets as in `:484-525`. `hang_next_response` plus `resolve_before_reply`. Claimant sends;
    peer sees `responding` (2); wait for the fence on the peer; release the gate (the hang keeps
    the call open); claimant gets `harness_timeout`; peer's next `RequestState` is `resolved` at
    revision 3, and no `pending` at revision 3 is ever published.
18. `reconnect_after_a_native_resolution_during_a_claim_does_not_re_prompt`
    As 15, then reconnect a fresh socket and subscribe; the live snapshot's server-request rows
    (`server_request_rows`, `:731`) show `srv_1` answered, mirroring
    `answered_server_request_is_not_pending_after_reconnect` (`:576`).
19. Existing tests `:343, :382, :420, :484, :527, :576, :644` pass unchanged. In particular `:420`
    (retry after failure) still ends with the retry succeeding, because no native resolution was
    emitted before the failure.

### Adapter

No adapter test is needed for the fix itself. One regression guard is worthwhile:

20. `giskard-harness-codex/src/lib.rs` (next to the test at `:4500-4520`):
    `server_request_response_is_written_before_the_resolved_event_is_appended` — assert that the
    fake transport received the JSON-RPC response before the `ServerRequestResolved` event is
    observable on the stream. This pins the order the fix relies on for the "commit stores the
    real answer" guarantee: the answer is at Codex before Giskard ever calls the request resolved.

## Documentation

`specs/giskard-specification.md`:

- Bump `:12` to 1.86 and add an amendment blockquote after it, in the 1.85 form: a claim owns a
  request until it settles; a harness resolution that arrives during a claim is recorded on the
  request and consumed by the settlement, so a commit stores the user's answer and a rollback
  resolves instead of re-pending; no `RequestState` is published for it while the claim is open.
- RT2 (`:218-221`) is a historical entry; do not rewrite it. Add a `Changelog (1.85 → 1.86)` block
  at the top of the changelog region (before `:135`) with one item, `RT6`, stating the rule above.
- Normative text at `:3709-3722`: after "while a successful call commits resolved", add the
  sentence: "A harness-side resolution observed while a claim is open does not preempt the claim;
  it is recorded on the request, and the claim's settlement resolves the request either way." In
  the SR6 paragraph, keep "the server does not synthesize one" as is (it still does not; the
  rollback-to-resolved uses the same synthesized `Null` the harness path already used).

No README or `docs/api-endpoints.md` change; the wire contract is unchanged.

## Order of work

1. `RequestStatus` shape and the five match sites; `cargo check -p giskard-server`.
2. Runtime unit tests 1-12; they are synchronous and pin the state machine.
3. Forwarder tests 13-14.
4. Integration fixture gate, `wait_for_notice`, tests 15-19; run 15 against `main` first to see it
   fail with the stale-claim error, then against the fix.
5. Adapter test 20.
6. Spec.

Expected size: under 60 non-test lines.

## Verification the implementer must perform and record

- `cargo test -p giskard-server` (unit + integration), `cargo test -p giskard-harness-codex`,
  `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
- Test 15 fails on `main` and passes on the branch; paste both outcomes in the PR.
- Manual: answer a Codex multiple-choice question; the log must show no
  `stale claim for request` error and the browser must show no error toast; the request card
  resolves once.

## Pitfalls

- Do not "fix" this by making the adapter emit `ServerRequestResolved` after replying, or by
  delaying the reply. That only moves the race; the Codex-originated resolution (interrupt,
  `serverRequest/resolved`) can still land during a claim.
- Do not publish a `RequestState` when marking `harness_resolved`. A tab that sees `resolved`
  followed by the claimant's `resolved` at a higher revision would be harmless, but a tab that sees
  `resolved` followed by a rollback to `pending` would re-enable a dead request. Publishing nothing
  is the only sequence that is correct in every interleaving.
- Do not let `rollback_inner` return `None` when the record is `Responding` with
  `harness_resolved`; the route's timeout path relies on the state having moved so its republish
  carries `resolved`.
- `register_request` must keep `harness_resolved` on a payload refresh (test 9).
- The `Drop` warning must not claim "back to pending" when the outcome is `resolved`.
- Keep `HARNESS_CONTROL_TIMEOUT` at 2 s; test 17 already tolerates it as `:484` does.

## Stop rules

Stop and re-cut if the diff:

- touches the adapter, the forwarder, the hub, `giskard-proto`, or `app.js`;
- adds a wait, timer, or retry anywhere in the claim path;
- introduces a second status enum or a side map keyed by request or thread id;
- changes the published `RequestState` sequence for the cases that work today
  (`pending → responding → resolved`, `pending → responding → pending`).
