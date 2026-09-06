# S4b — One `FakeHarness` for the server integration tests, and event waits

Second half of finding C6 of [`design-straightening-review.md`](design-straightening-review.md),
following [`s4a-testenv.md`](s4a-testenv.md). Written against `main` at `d474b91` (S4a merged);
every file and line reference below was checked against that tree. Re-check them if the branch
has moved.

## Goal

Two things, both confined to `crates/giskard-testenv` and `crates/giskard-server/tests`:

1. Replace the fourteen hand-written `impl AgentHarness` fakes with one `FakeHarness<S>` in the
   testenv. The shared struct owns what every fake re-implements: per-thread event logs, the
   native-id table, call recording, the default handle shapes, and the trait plumbing. Each test
   file keeps only its scenario, as a small `Script` implementation whose methods have defaults,
   so a script overrides the two or three calls it cares about and nothing else.
2. Replace the two helpers that poll the store with a sleep to learn that an admission finished
   (`wait_for_native_thread`, `wait_for_subagent`, seven call sites) with the driver probe S3b
   and S4a made available, and replace every fake-side "spin until a counter moves" helper with
   a wait on the shared call record.

No test's assertions, sent messages, or config change. No production code changes.

## Non-goals

- No change under `crates/giskard-server/src`, `crates/giskard-harness`, or
  `crates/giskard-harness-replay`. In particular the classification decisions in
  `registry/admission.rs` get no event in this step, so the one test that captures tracing output
  (`reverse_subagent_activity_preserves_parent_and_uses_one_forwarder`) keeps its capture; see
  "Deferred" at the end.
- No change to the ~40 file-local WebSocket `wait_for_*` helpers; they may adopt `ws::recv_until`
  later, but they are not duplicated and are not this step's subject.
- No change to tests that use `ReplayHarness` through `factory::fixture` or `factory::from_fn`
  (`model_refresh.rs`, `project_models.rs`, and the fixture-based files).
- No new `allow` attribute, no `cfg`, no timer-based synchronisation: every wait the testenv adds
  is a bounded wait on a `Notify` or a channel.

## Ground truth

| Fact | Where |
| --- | --- |
| 14 fakes: `ApprovalHarness` (`approval_reconnect.rs:32-37`, impl `:56-174`), `UnsupportedCompactionHarness` (`e2e_smoke.rs:130-132`, `:566-652`), `SlowCompactionHarness` (`:135-140`, `:655-815`), `ActivityHarness` (`:150-163`, `:818-1347`), `SlowStartHarness` (`:142-147`, `:1350-1473`), `CountingOpenHarness` (`:166-178`, `:1476-1606`), `NoMcpHarness` (`:127`, `:1609-1682`), `InterruptHarness` (`interrupt.rs:37-45`, `:132-279`), `CapturingHarness` (`override_propagation.rs:28-34`, `:52-144`), `SwitchHarness` (`provider_switch.rs:41-45`, `:48-123`), `AttachFails` (`read_only_thread.rs:15-17`, `:20-79`), `ToolHarness` (`running_tasks.rs:26-29`, `:42-154`), `ServerRequestHarness` (`server_requests.rs:24-35`, `:84-235`), `RecordingHarness` (`worktree_threads.rs:228-242`, `:245-425`) | read |
| Twelve of the fourteen build the same handle in `open_thread`: `ThreadHandle { resumed_model: Some(opts.initial_model.clone()), ..ThreadHandle::opened(opts.thread, opts.resume.unwrap_or_else(<fallback>), opts.workspace_root.clone()) }`. Fallbacks: `"approval_harness"`, `"interrupt_harness"`, `"cap"`, `"fresh"`, `"tool_harness"`, `"server_request_harness"`, `format!("test_{thread}")` (four fakes), `format!("count_{thread}_{n}")`, `format!("native-{thread}")`. `NoMcpHarness` and `AttachFails` fail instead; `SwitchHarness` may fail or rewrite the provider | read |
| Event-log plumbing: six fakes use one shared `Arc<EventLog>` for all threads; five use `tokio::sync::Mutex<HashMap<ThreadId, Arc<EventLog>>>` with `try_lock` in the synchronous `subscribe`; `RecordingHarness` uses a `std::sync::Mutex` and documents why (`worktree_threads.rs:229-234`); `NoMcpHarness` returns `AgentEventStream::closed()`; `AttachFails` delegates to a `ReplayHarness` | read |
| Capabilities: five fakes share one literal (`live_approvals`, `plan_build_modes`, `per_turn_model`, `reasoning_effort`, `structured_diffs`, `resumable_threads`, `token_usage` true; the other six false): `approval_reconnect.rs:57-73`, `interrupt.rs:133-149`, `override_propagation.rs:53-69`, `running_tasks.rs:43-59`, `server_requests.rs:85-101`. `ActivityHarness` (`e2e_smoke.rs:819-835`) is that literal with `structured_diffs` and `token_usage` false. `SlowCompactionHarness` (`:656-672`) and `SlowStartHarness` (`:1351-1367`) have only `resumable_threads` and `context_compaction` true. `UnsupportedCompactionHarness` (`:567-583`) and `CountingOpenHarness` (`:1477-1493`) only `resumable_threads`; `RecordingHarness` the same via struct update (`worktree_threads.rs:246-251`). `NoMcpHarness` (`:1610-1626`) all false; `SwitchHarness` `HarnessCapabilities::default()` (`provider_switch.rs:49-51`), also all false. `AttachFails` delegates to `ReplayHarness::new()`, whose literal is `crates/giskard-harness-replay/src/lib.rs:137-150` (all true except `model_listing`, `provider_listing`, `mcp_oauth_login`; `with_providers` turns `provider_listing` on) | read |
| Overrides beyond the nine required methods: `claim_native_thread` in `ActivityHarness` (`:876-913`), `CountingOpenHarness` (`:1519-1535`), `RecordingHarness` (`:278-301`); `delete_thread` in the same three (`:1335-1342`, `:1596-1600`, `:414-420`); `compact_thread` in `SlowCompactionHarness` (`:765-810`) and `SlowStartHarness` (`:1466-1468`); `terminate_command` in `InterruptHarness` (`:253-274`); `list_providers` in `AttachFails` (`:29-33`). No fake overrides `discoveries`, `client_version`, the MCP methods, `set_thread_name`, or `set_thread_archived` | read |
| Thirteen polling helpers, all `5 s` deadlines with `sleep` or `yield_now`: `wait_for_compact_calls` `e2e_smoke.rs:188`, `wait_for_start_calls` `:214`, `wait_for_native_child_open` `:261`, `wait_for_approval_response` `:279`, `wait_for_server_response` `:292`, `wait_for_subscribers` `:321`, `wait_for_subscriber_count` `:336`, `wait_for_native_thread` `:2149`, `wait_until_active` `interrupt.rs:61`, `wait_until_terminated` `:86`, `wait_for_capture` `override_propagation.rs:319`, `wait_for_response` `server_requests.rs:69`, `wait_for_subagent` `worktree_threads.rs:699` | grep |
| Store-polling call sites: `wait_for_native_thread` at `e2e_smoke.rs:5577` (`route_rejects_native_child_with_a_different_parent`), `:5623` and `:5646` (`parent_deletion_cascades_to_all_descendants_leaf_first`, two different native ids), `:5747` (`parent_deletion_rejects_active_descendant_before_deleting_anything`); `wait_for_subagent` at `worktree_threads.rs:1085` (`materializing_a_subagent_opens_it_in_its_parents_worktree`), `:1120` and `:1168` (`reattaching_a_subagent_after_a_restart_uses_its_parents_worktree`; the second call, after a restart, asserts that the same child is found again) | read |
| Gates in the fakes: `ActivityHarness` holds opens and claims of `"native-child"`/`"native-terminal-child"` behind three `AtomicBool`s spun with `yield_now`; `SlowCompactionHarness` holds `compact_thread` the same way; `SlowStartHarness` holds only its first `start_turn` with a `sleep(10ms)` loop; `ApprovalHarness` and `ServerRequestHarness` hang a response with `std::future::pending()`; `ServerRequestHarness` also fences on a `oneshot` (`resolve_before_reply`); `InterruptHarness` sleeps `interrupt_delay` | read |
| Recorded state that tests read: `ActivityHarness` `resumed_native_ids`, `claims`, `deleted_harness_thread_ids`, approval and server responses (all through inherent methods); `CountingOpenHarness` `open_calls`, `claim_calls` (counted only when the thread's log entry was vacant), `start_calls` (counted before an injected error is returned), `delete_calls`, `shutdown_calls`, `opened_models`, `started_models`, `started_inputs`; `InterruptHarness` `interrupted_threads`, `terminated_processes`; `CapturingHarness` `captured: Vec<TurnOverrides>` and `requested_models` (both `Arc`s the test holds); `SwitchHarness` and `RecordingHarness` `opened_workspace_roots` (open and claim roots, in call order; cleared by the test at `worktree_threads.rs:1184`); `ServerRequestHarness` `responses` (recorded only after the fail/hang knobs pass; its one reader after a failed first attempt, `websocket_server_request_response_failure_can_be_retried` `server_requests.rs:394`, resends an identical payload, `:363-372` vs `:384-392`) | read |
| `concurrent_subagent_cold_opens_install_one_native_owner` (`e2e_smoke.rs:7581`) asserts `open_calls() == 0` and `claim_calls() == 1` after two concurrent claims of the same thread (`:7648-7649`) | read |
| Bootstrap seeding: `start_activity_server_on_available_port` (`e2e_smoke.rs:2004-2018`) and `recording_factory` (`worktree_threads.rs:427-436`) copy `bootstrap.known_threads` into the fake's `native_routes` inside the factory closure | read |
| Factory calls that hand a fake to the server: 14 `factory::shared(` and 16 `factory::from_fn(`, of which 6 `from_fn` build a `ReplayHarness` (`model_refresh.rs` 1, `project_models.rs` 5) and stay | grep |
| Event types the probe delivers: `DriverEvent::DiscoveryFinished { native_thread_id, attempts, outcome }` and `LinkFinished { native_thread_id, parent_thread_id, item_id, origin, attempts, outcome }` with `outcome: Result<Option<ThreadId>, HarnessError>`, `Ok(Some(id))` being the admitted thread; the testenv probe is `driver::probe() -> (Arc<dyn DriverEventSink>, DriverProbe)` with `expect(pred) -> (ProjectId, DriverEvent)` (5 s) and `drain()`; `TestServerBuilder::driver_events(sink)` installs it | `crates/giskard-server/src/registry/driver.rs:37-75`; `crates/giskard-testenv/src/driver.rs:20-58`; `src/server.rs:151` |
| `EventLog`: `new`, `append`, `close`, `reader_count`, `Arc<EventLog>::reader`; `AgentEventStream::new(reader)` / `closed()` | `crates/giskard-harness/src/event_log.rs:77-157`; `lib.rs:419-427` |
| `AgentHarness` signatures (`crates/giskard-harness/src/lib.rs`): `open_thread(&self, opts: OpenThreadOptions)` `:522`; `claim_native_thread(&self, thread: ThreadId, harness_thread_id: String, workspace_root: PathBuf)` `:530`; `start_turn(&self, thread: &ThreadHandle, input: UserInput, overrides: TurnOverrides) -> Result<TurnId, _>` `:543`; `subscribe(&self, thread: &ThreadHandle) -> AgentEventStream` `:551`; `respond_approval(&self, req: ApprovalId, decision: ApprovalDecision)` `:559`; `respond_server_request(&self, req: ServerRequestId, response: ServerRequestResponse)` `:566`; `interrupt(&self, thread: &ThreadHandle)` `:573`; `compact_thread(&self, thread: &ThreadHandle)` `:576`; `terminate_command(&self, _thread: &ThreadHandle, process_id: &str)` `:584`; `delete_thread(&self, thread: &ThreadHandle)` `:616`; `list_providers` `:485`; `list_models` `:480`; `shutdown` `:627`. Default error texts: compaction `"context compaction is not supported for thread {harness_thread_id}"`, termination `"command termination is not supported for process {process_id}"`, providers `"provider listing is not supported by this harness"` | read |
| `OpenThreadOptions { project, thread, workspace_root, resume: Option<String>, initial_model: ModelRef, updates }` derives `Clone`; `ThreadHandle { thread, harness_thread_id, workspace_root, warning, resumed_model, agent_name, parent_harness_thread_id }`; `TurnOverrides`, `UserInput`, `ApprovalDecision`, `ServerRequestResponse`, `ModelRef` all derive `Clone` | `lib.rs:291-303, 358-381`; `giskard-core/src/{turn.rs:126, user_input.rs:21, approval.rs:74, server_request.rs:21, model.rs:6}` |
| 226 integration tests; 14 `impl AgentHarness`; `install_registry_event_capture` defined once and called once | grep |

## Design

### D1. `fake::FakeCore`, the state every fake had

In `crates/giskard-testenv/src/fake.rs`:

```rust
pub struct FakeCore {
    threads: std::sync::Mutex<HashMap<ThreadId, Arc<EventLog>>>,   // std: `subscribe` is synchronous
    native_routes: std::sync::Mutex<HashMap<String, ThreadId>>,
    calls: std::sync::Mutex<Vec<Call>>,
    calls_changed: tokio::sync::Notify,
}

/// One trait call the server made, recorded on entry, before the script runs.
#[derive(Debug, Clone)]
pub enum Call {
    Open { thread: ThreadId, resume: Option<String>, model: ModelRef, workspace_root: PathBuf },
    Claim { thread: ThreadId, harness_thread_id: String, workspace_root: PathBuf },
    StartTurn { thread: ThreadId, turn: TurnId, input: UserInput, overrides: TurnOverrides },
    RespondApproval { request: ApprovalId, decision: ApprovalDecision },
    RespondServerRequest { request: ServerRequestId, response: ServerRequestResponse },
    Interrupt { thread: ThreadId },
    TerminateCommand { thread: ThreadId, process_id: String },
    CompactThread { thread: ThreadId },
    DeleteThread { thread: ThreadId, harness_thread_id: String },
    Shutdown,
}

impl FakeCore {
    // logs
    pub fn log(&self, thread: ThreadId) -> Arc<EventLog>;                    // creates on first use
    pub fn ensure_log(&self, thread: ThreadId) -> (Arc<EventLog>, bool);      // bool: newly created
    pub fn try_log(&self, thread: ThreadId) -> Option<Arc<EventLog>>;
    pub fn remove_log(&self, thread: ThreadId) -> Option<Arc<EventLog>>;
    pub fn append(&self, thread: ThreadId, event: AgentEvent);                // log(thread).append
    pub fn complete_turn(&self, thread: ThreadId, turn: TurnId);              // TurnCompleted { Completed, usage default }
    // routes
    pub fn route(&self, harness_thread_id: &str) -> Option<ThreadId>;
    pub fn bind_route(&self, harness_thread_id: &str, thread: ThreadId) -> ThreadId;   // entry().or_insert
    pub fn seed_routes(&self, bootstrap: &HarnessBootstrap);
    // default handle shapes
    pub fn opened(&self, opts: &OpenThreadOptions, fallback_native_id: String) -> ThreadHandle;
        // creates the log, binds the route, returns the twelve-of-fourteen handle shape
    pub fn claimed(&self, thread: ThreadId, harness_thread_id: &str, workspace_root: &Path) -> ThreadHandle;
        // bind_route + ensure_log, bare ThreadHandle::opened (no resumed_model)
    // calls
    pub fn calls(&self) -> Vec<Call>;
    pub fn count(&self, pred: impl Fn(&Call) -> bool) -> usize;
    pub fn clear_calls(&self);
    pub async fn wait_for_call<T>(&self, pick: impl Fn(&Call) -> Option<T>) -> T;
        // first recorded call `pick` accepts; waits on `calls_changed` up to 5 s, then panics listing the calls
    pub async fn wait_for_calls(&self, pred: impl Fn(&Call) -> bool, at_least: usize);
    // readers
    pub async fn wait_for_readers(&self, thread: ThreadId, at_least: usize);   // reader_count() >= n, 5 s, yield loop
    pub async fn wait_for_reader_count(&self, thread: ThreadId, exactly: usize);
}
```

Recording rule, stated once in the `Call` doc and honoured by `FakeHarness`: every trait call is
recorded **on entry**, before the script runs and regardless of what it returns. This matches
the fakes that count before failing (`CountingOpenHarness::start_turn`) or before gating
(`InterruptHarness::terminate_command`). The two fakes that recorded *after* their knobs are
covered: `ApprovalHarness::answered` is never read by a test, and
`ServerRequestHarness::responses`' only after-a-failure reader sees an identical payload on the
retry (ground truth). `CountingOpenHarness::claim_calls` counted only vacant-log claims; its
script reproduces that with `ensure_log`'s boolean, not with the call record.

The two reader-count waits are the only yield loops the testenv keeps: `EventLog` has no
notification for reader changes, and adding one is a harness change this step does not make.

### D2. `fake::Script`, what a test file still writes

```rust
#[async_trait]
pub trait Script: Send + Sync + 'static {
    fn capabilities(&self) -> HarnessCapabilities { caps::TURNS }
    /// Fallback native id for an open without `resume`.
    fn native_thread_id(&self, thread: ThreadId) -> String { format!("test_{thread}") }
    async fn open_thread(&self, core: &FakeCore, opts: &OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        Ok(core.opened(opts, self.native_thread_id(opts.thread)))
    }
    async fn claim_native_thread(&self, core: &FakeCore, thread: ThreadId, harness_thread_id: &str, workspace_root: &Path) -> Result<ThreadHandle, HarnessError> {
        Ok(core.claimed(thread, harness_thread_id, workspace_root))
    }
    /// The turn id is already chosen and recorded; the script appends whatever the scenario
    /// streams (including `TurnStarted`) and returns `Err` to refuse the turn.
    async fn start_turn(&self, core: &FakeCore, call: &TurnCall) -> Result<(), HarnessError> { Ok(()) }
    async fn respond_approval(&self, core: &FakeCore, request: ApprovalId, decision: ApprovalDecision) -> Result<(), HarnessError> { Ok(()) }
    async fn respond_server_request(&self, core: &FakeCore, request: ServerRequestId, response: ServerRequestResponse) -> Result<(), HarnessError> { Ok(()) }
    async fn interrupt(&self, core: &FakeCore, thread: &ThreadHandle) -> Result<(), HarnessError> { Ok(()) }
    async fn terminate_command(&self, core: &FakeCore, thread: &ThreadHandle, process_id: &str) -> Result<(), HarnessError> { Err(Unsupported(<trait default text>)) }
    async fn compact_thread(&self, core: &FakeCore, thread: &ThreadHandle) -> Result<(), HarnessError> { Err(Unsupported(<trait default text>)) }
    async fn delete_thread(&self, core: &FakeCore, thread: &ThreadHandle) -> Result<(), HarnessError> { core.remove_log(thread.thread); Ok(()) }
    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> { Ok(Vec::new()) }
    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> { Err(Unsupported(<trait default text>)) }
    async fn shutdown(&self, core: &FakeCore) -> Result<(), HarnessError> { Ok(()) }
}

pub struct TurnCall { pub thread: ThreadId, pub harness_thread_id: String, pub turn: TurnId, pub input: UserInput, pub overrides: TurnOverrides, pub log: Arc<EventLog> }
```

The defaults are the behaviours most fakes share: `Ok(())` for the response and interrupt
methods (nine fakes), the trait's own `Unsupported` texts for compaction, termination, and
providers (so a script that does not override them behaves exactly like a fake that did not
override the trait method), and log removal on delete (three fakes; harmless for the rest).

### D3. `fake::FakeHarness<S>`

```rust
pub struct FakeHarness<S: Script> { pub script: S, pub core: FakeCore }

impl<S: Script> FakeHarness<S> {
    pub fn new(script: S) -> Arc<Self>;
}

#[async_trait]
impl<S: Script> AgentHarness for FakeHarness<S> {
    fn capabilities(&self) -> HarnessCapabilities { self.script.capabilities() }
    async fn open_thread(&self, opts) { record Call::Open; self.script.open_thread(&self.core, &opts).await }
    async fn claim_native_thread(&self, thread, id, root) { record Call::Claim; self.script.claim_native_thread(..).await }
    async fn start_turn(&self, thread, input, overrides) {
        let turn = TurnId::new(); record Call::StartTurn; let call = TurnCall { .., log: self.core.log(thread.thread) };
        self.script.start_turn(&self.core, &call).await.map(|()| turn)
    }
    fn subscribe(&self, thread) { self.core.try_log(thread.thread).map(|log| AgentEventStream::new(log.reader())).unwrap_or_else(AgentEventStream::closed) }
    // respond_approval, respond_server_request, interrupt, terminate_command, compact_thread,
    // delete_thread, shutdown: record, then delegate
    async fn list_models(&self) { self.script.list_models().await }
    async fn list_providers(&self) { self.script.list_providers().await }
}
```

`subscribe` on a thread that was never opened returns a closed stream, which is what the
`try_lock` fakes did on a miss and what `NoMcpHarness` did unconditionally. The one fake with a
single shared log, `SwitchHarness`, only ever subscribes to threads it opened, so per-thread logs
are equivalent for it.

### D4. `fake::Gate`

```rust
pub struct Gate { held: AtomicBool, blocked: AtomicBool, released: tokio::sync::Notify }
impl Gate {
    pub fn open() -> Self; pub fn held() -> Self;
    pub fn hold(&self); pub fn release(&self);
    /// Returns at once when open; otherwise marks itself blocked and waits for `release`.
    pub async fn pass(&self);
    /// Waits until a caller is blocked in `pass` (5 s). Replaces `wait_for_native_child_open`.
    pub async fn wait_blocked(&self);
}
```

`pass` loops on `released.notified()` with the `held` flag re-checked after each wake, so a
release that lands before the waiter registers is not lost. It replaces the four spin loops
(three `AtomicBool` pairs, one `sleep(10ms)` loop). Hanging forever stays `std::future::pending()`
in the script; that is a scenario, not a gate.

### D5. `fake::caps`

Named literals for the presets the fakes use, verbatim from the ground truth:

| Const | Fields true |
| --- | --- |
| `caps::TURNS` | `live_approvals`, `plan_build_modes`, `per_turn_model`, `reasoning_effort`, `structured_diffs`, `resumable_threads`, `token_usage` |
| `caps::ACTIVITY` | `TURNS` minus `structured_diffs` and `token_usage` |
| `caps::RESUMABLE` | `resumable_threads` |
| `caps::RESUMABLE_COMPACTION` | `resumable_threads`, `context_compaction` |
| `caps::REPLAY` | the `ReplayHarness::new()` literal: all but `model_listing`, `provider_listing`, `mcp_oauth_login` |
| `HarnessCapabilities::default()` | none (`NoMcpHarness`, `SwitchHarness`) |

`Script::capabilities` defaults to `TURNS`; scripts that used another literal name it.

### D6. `fake::factory`

```rust
/// A factory that returns this harness for every project and copies `bootstrap.known_threads`
/// into its native-id table first, as the activity and recording factories do today.
pub fn factory<S: Script>(harness: Arc<FakeHarness<S>>) -> Arc<dyn HarnessFactory>;
```

Every fake is handed to the server through this; the two fresh-per-create closures
(`CapturingHarness`, `SwitchHarness`, `ToolHarness`, and the three `::default()` closures in
`e2e_smoke.rs`) become a single shared harness, which is equivalent because each of those tests
has one project. Seeding routes is harmless for scripts that never read them.

### D7. Event waits for admissions

The two store-polling helpers become one testenv helper over the probe:

```rust
// crates/giskard-testenv/src/driver.rs
impl DriverProbe {
    /// The next admission of `harness_thread_id` (discovery or link) that installed a thread;
    /// panics on a refused or failed admission for it.
    pub async fn expect_admitted(&mut self, harness_thread_id: &str) -> ThreadId;
    /// The next link admission under `parent` that installed a child.
    pub async fn expect_child_of(&mut self, parent: ThreadId) -> ThreadId;
}
```

Both match `DiscoveryFinished`/`LinkFinished` with `outcome: Ok(Some(id))` and return `id`; the
file the old helpers returned is one `store.load_thread(project, id)` away, and the event is
emitted after the file is written (S3's ordering rule), so no further wait is needed.

Because a probe consumes events, a test that waits twice for the same admission cannot use it
for the second wait. There is exactly one such site (`worktree_threads.rs:1168`), and that call
is an assertion ("the reattach must not have invented a second child"), not a wait; it becomes a
plain store read, `children_of(&server, parent)`, asserted equal to `vec![child]`.

## Migration

### The fourteen fakes

For each: the `Script` struct that replaces the fake (in the same file), what it overrides, and
what its old inherent helpers become. "core" is `harness.core`.

| Fake | Script | Overrides | Helpers → |
| --- | --- | --- | --- |
| `ApprovalHarness` | `ApprovalScript { active: Mutex<Option<(ThreadId, TurnId)>>, hang_next_approval: AtomicBool }`, native `"approval_harness"` | `start_turn` (records the turn, appends `TurnStarted`, `ApprovalRequested`, `ServerRequestReceived`), `respond_approval` (`pending()` if armed), `respond_server_request` (appends `ServerRequestResolved`) | `hang_next_approval()` stays on the script; `answered` dropped (unread) |
| `UnsupportedCompactionHarness` | `UnsupportedCompactionScript`, caps `RESUMABLE` | `start_turn`, `respond_approval`, `respond_server_request`, `interrupt` → `Err(Unsupported(<same text>))` | none |
| `SlowCompactionHarness` | `SlowCompactionScript { compaction: Gate }`, caps `RESUMABLE_COMPACTION` | `start_turn` (spawn the three events), `compact_thread` (`compaction.pass().await`, then the spawned events with the 5 s sleep) | `held()` → `Gate::held()`; `wait_for_compact_calls(n)` → `core.wait_for_calls(CompactThread, n)`; `release_compaction()` → `compaction.release()` |
| `ActivityHarness` | `ActivityScript { child_open: Gate, pending_approvals, pending_server_requests }`, caps `ACTIVITY` | `open_thread` (gate for the two native-child ids; `core.opened` then set `agent_name`/`parent_harness_thread_id`), `claim_native_thread` (gate; `core.claimed` then parent), `start_turn` (the twelve-branch dispatcher, verbatim), `respond_approval`/`respond_server_request` (record is automatic; pop pending or `Err(Protocol)`; `core.complete_turn`) | `resumed_native_ids()` → `core.calls()` `Open.resume`; `claims()` → `Claim`s; `deleted_harness_thread_ids()` → `DeleteThread`s; `wait_for_approval_response`/`wait_for_server_response` → `core.wait_for_call`; `hold/release_native_child_open` → gate; `wait_for_native_child_open` → `gate.wait_blocked()`; `wait_for_subscribers`/`wait_for_subscriber_count` → `core.wait_for_readers`/`wait_for_reader_count`; `complete_turn` → `core.complete_turn`; the five `emit_external_*` helpers become free functions taking `&FakeCore` |
| `SlowStartHarness` | `SlowStartScript { first_start: Gate, first_seen: AtomicBool }`, caps `RESUMABLE_COMPACTION` | `start_turn` (first call passes the gate, then the three events), `compact_thread` → `Ok(())` | `new()` → `first_start: Gate::held()`; `wait_for_start_calls(n)` → `core.wait_for_calls(StartTurn, n)`; `start_calls()` → `core.count(StartTurn)`; `release_first_start()` → gate |
| `CountingOpenHarness` | `CountingScript { new_claims: AtomicUsize, start_error: Mutex<Option<HarnessError>> }`, caps `RESUMABLE` | `open_thread` (native `format!("count_{thread}_{n}")` with `n = core.count(Open)`, which already includes this call), `claim_native_thread` (`core.claimed`; bump `new_claims` when `ensure_log` reports a new log), `start_turn` (return `start_error` if armed, else `TurnStarted` only) | `open_calls` → `core.count(Open)`; `claim_calls` → `new_claims`; `start_calls`/`delete_calls`/`shutdown_calls` → counts; `opened_models`/`started_models`/`started_inputs` → projections of `core.calls()`; `fail_start_with` stays |
| `NoMcpHarness` | `NoMcpScript`, caps `default()` | `open_thread`, `start_turn`, `respond_*`, `interrupt` → `Err(Unsupported(<same texts>))` | none |
| `InterruptHarness` | `InterruptScript { active, command, interrupt_delay, terminate_behavior }`, native `"interrupt_harness"` | `start_turn`, `interrupt` (optional sleep, `TurnCompleted { Interrupted }`), `terminate_command` (`TerminateBehavior` match, verbatim) | `wait_until_active()` → `core.wait_for_call(StartTurn)`; `interrupted_threads()` → `Interrupt` calls; `terminated_processes()`/`wait_until_terminated()` → `TerminateCommand` calls; `set_interrupt_delay`, `set_terminate_behavior`, `complete_command` stay |
| `CapturingHarness` | `CapturingScript`, native `"cap"` | `start_turn` (`TurnStarted` + `TurnCompleted`) | the two shared `Arc`s go: `requested_models` → `Open.model`s; `wait_for_capture(n)` → `core.wait_for_calls(StartTurn, n)` then the nth `overrides` |
| `SwitchHarness` | `SwitchScript { report_provider: Option<String> }`, caps `default()`, native `"fresh"` | `open_thread` (dead-provider error; provider rewrite; `core.opened` for the rest), `start_turn`/`respond_*`/`interrupt` → `Err(Unsupported(<same texts>))` | `opened_workspace_roots` → `Open.workspace_root`s from `core.calls()` (the `Fixture` field goes) |
| `AttachFails` | `AttachFailsScript { providers: Option<Vec<HarnessProvider>> }`, caps `REPLAY` with `provider_listing = providers.is_some()` | `open_thread` → `Err(Spawn("unknown provider: cloudflare-litellm"))`, `list_providers` → `Ok(providers.clone().unwrap_or_default())` (a `ReplayHarness` without providers answers `Ok(vec![])`, `replay/src/lib.rs:250-255`) | none; the inner `ReplayHarness` goes |
| `ToolHarness` | `ToolScript { active_turn }`, native `"tool_harness"` | `start_turn` (tool-call `ItemStarted`), `interrupt` (`TurnCompleted { Interrupted }`) | none |
| `ServerRequestHarness` | `ServerRequestScript { active, fail_next_response, hang_next_response, resolve_before_reply, suppress_resolution }`, native `"server_request_harness"` | `start_turn`, `respond_server_request` (the four knobs, verbatim minus the `responses` push) | `wait_for_response()` → `core.wait_for_call(RespondServerRequest)`; the four knob setters stay |
| `RecordingHarness` | `RecordingScript { spawns_subagent, agent_says }`, caps `RESUMABLE`, native `format!("native-{thread}")` | `start_turn` (verbatim) | `Harnessed::workspace_roots()` → `Open`/`Claim` roots from `core.calls()` in order; the `.clear()` at `:1184` → `core.clear_calls()`; `recording_factory` → `fake::factory` |

Every `Unsupported`, `Transport`, `Protocol`, and `Spawn` message a fake returns today is kept
byte-for-byte; the assertions on `error.code`/`error.message` depend on them.

### The probe sites

| Site | Today | After |
| --- | --- | --- |
| `e2e_smoke.rs:2004` `start_activity_server_on_available_port(harness)` | returns `TestServer` | unchanged for 13 callers; a sibling `start_activity_server_with_probe(harness) -> (TestServer, DriverProbe)` installs `driver::probe()` through `.driver_events(sink)` |
| `:5577`, `:5623`, `:5646`, `:5747` | `wait_for_native_thread(state, pid, id)` → `ThreadFile` | `let id = probe.expect_admitted("<native id>").await; state.store.load_thread(pid, id).await.unwrap().unwrap()` |
| `:2149-2175` | `wait_for_native_thread` | deleted |
| `worktree_threads.rs:446` `start(git_repo)` | `Harnessed { server, project, harness, project_id }` | gains `probe: DriverProbe`; `start` installs it |
| `:1611` `restart(&server)` | `(harness, base, cookie)` | `(harness, base, cookie, probe)` |
| `:1085`, `:1120` | `wait_for_subagent(&server, parent)` | `server.probe.expect_child_of(parent).await` (the restart site uses the restarted probe) |
| `:1168` | `assert_eq!(wait_for_subagent(&server, parent_id).await, child)` | `assert_eq!(children_of(&server, parent_id).await, vec![child])`, a store list with no polling |
| `:699-730` | `wait_for_subagent` | replaced by `children_of` (the list body without the loop) |

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/Cargo.toml:13` | `giskard-harness-replay` moves from `[dependencies]` to `[dev-dependencies]`: nothing under `src/` or `src/bin/` uses it, only six test files do, and production builds compile it for nothing |
| `crates/giskard-testenv/src/fake.rs` | new: `FakeCore`, `Call`, `Script`, `TurnCall`, `FakeHarness`, `Gate`, `caps`, `factory` |
| `crates/giskard-testenv/src/driver.rs` | `expect_admitted`, `expect_child_of` |
| `crates/giskard-testenv/src/lib.rs` | `pub mod fake;` and re-exports `fake::{FakeHarness, Script}` |
| `crates/giskard-testenv/Cargo.toml` | nothing new: `giskard-harness`, `async-trait`, `tokio` are already dependencies |
| the 8 test files with fakes | per the migration table; 14 `impl AgentHarness` → 14 `impl Script` |
| `e2e_smoke.rs`, `worktree_threads.rs` | per the probe table |
| `docs/design-straightening-review.md` | C6 and step 4: "landed in S4a/S4b" |

## Tests

The 226 integration tests are the specification and keep every assertion. The testenv gains
unit tests in `fake.rs`, none of which needs a server:

1. `calls_are_recorded_on_entry_even_when_the_script_fails`: a script whose `start_turn` returns
   `Err`; after the call, `core.count(StartTurn) == 1` and the returned error is the script's.
2. `wait_for_call_wakes_without_polling`: spawn a task that records a call after a short delay;
   `wait_for_call` returns it, and a second `wait_for_call` for a call that never comes panics
   with the recorded calls in its message.
3. `gate_release_before_pass_is_not_lost`: `Gate::held()`, `release()`, then `pass()` returns
   immediately; and `pass()` on a held gate returns after a concurrent `release()`.
4. `claimed_binds_the_route_once`: two `claimed` calls for the same native id return the first
   thread's id and `ensure_log` reports a new log only once.
5. `subscribe_is_closed_for_an_unknown_thread`: `FakeHarness::new(())` (the unit script, all
   defaults) returns a closed stream for a thread never opened and a live one after `open_thread`.

`()` implements `Script` with every default, so `FakeHarness::new(())` is the zero-config fake.

## Order of work

0. Move `giskard-harness-replay` to the server's `[dev-dependencies]`; `cargo build -p giskard-server`
   and `cargo test -p giskard-server --no-run` both still succeed.
1. `fake.rs` with D1–D6 and the unit tests; `cargo test -p giskard-testenv`.
2. D7 in `driver.rs`.
3. Migrate the small files first, each followed by `cargo test -p giskard-server --test <name>`:
   `running_tasks.rs`, `override_propagation.rs`, `approval_reconnect.rs`, `provider_switch.rs`,
   `read_only_thread.rs`, `server_requests.rs`, `interrupt.rs`.
4. `worktree_threads.rs` (fake, then the probe sites), then `e2e_smoke.rs` fake by fake in the
   order `NoMcp`, `UnsupportedCompaction`, `SlowStart`, `SlowCompaction`, `CountingOpen`,
   `Activity`, then its probe sites. Run the file after each fake.
5. `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo fmt --check`; run `e2e_smoke` and `worktree_threads` five times each.

Expected size: about 450 lines added in the testenv, about 1 300 deleted across the eight files.

## Exit checks

Validated on the base tree; baselines given.

```sh
T=crates/giskard-server/tests
# 14 → 0 and 0 → 14
grep -oE "impl (giskard_harness::)?AgentHarness for" $T/*.rs | wc -l
grep -o "impl Script for" $T/*.rs | wc -l
# 13 → 0: no polling helper left (the thirteen names from the ground truth)
grep -cE "^\s*(pub )?(async )?fn (wait_for_compact_calls|wait_for_native_child_open|wait_for_approval_response|wait_for_server_response|wait_for_subscribers|wait_for_subscriber_count|wait_for_start_calls|wait_until_active|wait_until_terminated|wait_for_response|wait_for_capture|wait_for_native_thread|wait_for_subagent)\(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 14 → 0: every fake goes through fake::factory
grep -o "factory::shared(" $T/*.rs | wc -l
# 16 → 6: only the ReplayHarness-building closures remain (model_refresh.rs 1, project_models.rs 5)
grep -o "factory::from_fn(" $T/*.rs | wc -l
# 0 → 6: four expect_admitted sites in e2e_smoke.rs, two expect_child_of sites in worktree_threads.rs
grep -oE "expect_admitted\(|expect_child_of\(" $T/*.rs | wc -l
# 2 → 2: the tracing capture is untouched (definition + one call)
grep -c "install_registry_event_capture" $T/e2e_smoke.rs
# 226 → 226
grep -cE "^\s*#\[(tokio::)?test" $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 0 → 0: no server change
git diff --stat origin/main -- crates/giskard-server/src crates/giskard-harness crates/giskard-harness-replay | wc -l
# 1 → 0 and 0 → 1: the replay crate is a dev-dependency of the server
sed -n '/^\[dependencies\]/,/^\[/p' crates/giskard-server/Cargo.toml | grep -c giskard-harness-replay
sed -n '/^\[dev-dependencies\]/,/^\[/p' crates/giskard-server/Cargo.toml | grep -c giskard-harness-replay
# 0 → 0
grep -rn "#\[allow" $T crates/giskard-testenv | wc -l
```

## Pitfalls

- Record on entry, then delegate. A script that hangs must already be visible in
  `core.calls()`, or `wait_for_call` deadlocks against it.
- `CountingScript::open_thread` reads `core.count(Open)` *after* the harness recorded this call,
  so the first open sees `1`; that is what `count_{thread}_1` needs.
- `ActivityScript` and `RecordingScript` rely on routes seeded from the bootstrap; `fake::factory`
  does that for every script, so do not add a second seeding path.
- The probe consumes events. Install it before the action (`TestServerBuilder::driver_events`
  is the only place), and never wait twice for the same admission; the one such site is an
  assertion and is migrated as one.
- `expect_admitted` must panic, not skip, on `Ok(None)` or `Err` for the awaited native id: a
  refused admission is a test failure, and skipping it would turn into a 5 s timeout with a
  misleading message.
- `Gate::pass` re-checks `held` after every wake; a plain `notified().await` loses a release that
  precedes it.
- Keep every error message byte-for-byte; `expect_error_for` matches on `code` and the tests
  assert on `message` text in several places.
- `Script` uses `async_trait`, like `AgentHarness` itself. Native `async fn` in the trait would
  compile, but its futures carry no `Send` bound, and `FakeHarness`'s `async_trait` methods must
  await them inside `Send` futures; `async_trait` supplies the bound without a second trait.

## Stop rules

Stop and re-cut if the diff:

- touches anything under `crates/giskard-server/src`, `crates/giskard-harness`, or
  `crates/giskard-harness-replay`;
- changes an assertion, a sent message, a config section, or an error message;
- adds a sleep-based or yield-based wait anywhere except the two reader-count waits in `FakeCore`;
- leaves an `impl AgentHarness` in a test file, or a fake-side `wait_for_*` polling helper;
- adds `allow`, `cfg`, or a second factory path for fakes.

## Deferred

The tracing capture in `reverse_subagent_activity_preserves_parent_and_uses_one_forwarder`
asserts on two log messages from `registry/admission.rs:238-252`, both of which return
`Ok(None)` from `admit`, so `LinkFinished`'s outcome cannot tell them apart. Replacing that
capture needs the admission disposition in the event (a server change, one enum and one field).
That is a candidate S3c, not part of S4b.
