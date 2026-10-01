# Milestone 6 implementation plan: supervisor state machine and idle reaping

Implements milestone 6 of [`../claude-code-harness-plan.md`](../claude-code-harness-plan.md)
(§5.2, §11, the design doc's open question). This plan is written for an implementing agent.
Every file, symbol and behaviour below was verified against `main` at `7251078` (MCP status per
thread merged) and against Claude Code **2.1.287**. Line numbers are for orientation; the symbol
quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this milestone: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; every long-lived keyed map carries an `ENTITY-AUTHORITY-EXCEPTION` comment and joins the
struct whose cleanup site matches its lifetime; a recovery path, timeout, idempotent close or
lifecycle cleanup gets focused tests for its failure path and logs that explain what happened; log
assertions use `#[traced_test]` with `logs_contain` / `logs_assert`, never a scoped subscriber;
spawned tasks that log are `.in_current_span()`; the README and `config.example.toml` change in
the same commit as a config key; the adapter README is kept in sync with lifecycle, process
control and restart semantics; Markdown prose is wrapped at 100 columns (table rows may run
longer).

## Outcome

After this milestone a `claude` process that has nothing to do goes away, and the thread it served
does not notice: the thread stays bound on the server, its event stream stays open, and the next
message the user sends respawns the process with `--resume` on the same session id, under the
same launch rules as the first open (bypass fallback, missing-transcript fallback with a notice).
Memory tracks threads *in use* rather than threads *opened*, which is the capacity problem §5.2
and §12 name (440–530 MB RSS per process at the spec's ~10-thread scale). The idle timeout is a
key on the `claude-code` declaration, `idle_shutdown_secs`, with a default and a way to switch it
off.

To make "nothing to do" a fact the supervisor can state, the supervisor first becomes an explicit
state machine: what it is waiting on (a turn's settings, a stop sequence's grace period, the idle
clock) is a field the main loop drives, not a nested read loop. Milestone 3's `await_control`
pumps frames in 50 ms slices because a handler that needs a control response cannot await a
waiter the same loop resolves; that goes, along with the deferred-command queue and the
stop-request side channel it needed. `Stop`, shutdown, a dropped command channel, a stage
timeout and the idle timer are all ordinary `select!` inputs of one loop.

Nothing in `giskard-server`'s runtime or in `static/app.js` changes. The server never learns a
child was reaped, and must not: `registry.open_thread` reuses an existing binding without calling
the harness (`registry.rs:904`, `reusable_handle`), the forwarder keeps reading the thread's log,
and a closed log would end the thread as a failed owner (`StreamEndedWithoutTurn`). Only the server
*binary* changes, for the new declaration key.

## Scope

One pull request in two commits, each green on its own:

1. **The supervisor as a state machine** (`session.rs`, no behaviour change a test can see except
   where noted): the per-turn settings as an in-flight `TurnSetup` the main loop advances,
   `stop_task` as a waiter, the stop sequence as a phase, commands refused at once while stopping,
   stage deadlines as `select!` arms. Removes `await_control`, `pump_until`,
   `stop_requested_while_waiting`, `AWAIT_POLL_SLICE`, `deferred`, `stop_request`, `Waiter::Raw`.
2. **Idle reaping and lazy respawn** (`session.rs`, `mapper.rs`, `harness.rs`, `process.rs`,
   `bin/giskard-server.rs`, config and docs): the idle definition and timer, the reap as a stop
   that keeps the thread, the façade's thread entries that outlive their child, respawn on the
   next `start_turn` / `compact_thread` / `open_thread`, the `idle_shutdown_secs` key.

The second commit depends on the first: a child is never reaped mid-handshake, mid-settings or
mid-answer, and "mid-settings" is a state only the first commit makes explicit.

## Non-goals

- No change to the `AgentHarness` trait, to `ThreadUpdate`, to the server's runtime, routes,
  WebSocket protocol or `app.js`, and no screenshot regeneration.
- No idle policy for Codex. The design doc's open question is answered for this adapter only
  (per process, inside the adapter); the Codex half ("terminates the app-server and resumes
  threads on next use") stays open and is said to.
- No reaping of the probe child (it already exits after its last answer) and no pre-warming of a
  spare process (`--await-claim` is SDK-internal and unverified).
- No change to how an *unexpected* child exit is handled: a crash still closes the thread's log,
  which is how the server learns the stream ended. Only a reap keeps the log open.
- No persistence of anything: the session id the respawn resumes is the one the thread file
  already stores, and the respawn is the same `--resume` the next server start would do.
- No change to sub-agent routes beyond what child exit already does to them (turned cold).
- `--replay-user-messages` acknowledgement is not part of this milestone.

## Verified facts this milestone rests on

| Fact | Consequence |
| --- | --- |
| The server calls `harness.open_thread` only when the thread has no coordinator (`registry.rs:904`: an existing coordinator answers through `reusable_handle` and returns its binding). A thread the user stops viewing keeps its coordinator; `forget_thread` / `retire_thread` (`registry.rs:1517`, `:1537`) run only on delete, on a failed new-thread startup, on a model-select unwind and on project delete | A reaped thread's next `start_turn` / `compact_thread` arrives on the façade **without** a new `open_thread`, so the façade must keep the thread's entry (session id, log, model) after its child is gone and respawn from it. `open_thread` on such an entry happens only after a server restart or a forget |
| The forwarder owning a bound thread reads the thread's log until it closes; a closed stream with no open turn is `StreamEndedWithoutTurn`, which the driver records as a failed owner outside teardown (`registry/driver.rs:479`) | The reap must **not** close the log. The same `Arc<EventLog>` outlives the child and the respawned supervisor appends to it, so the forwarder sees one continuous stream |
| The thread update forwarder spawned at open receives **one** `ThreadUpdate` and returns (`registry.rs:546`, a single `recv`); `ContextWindowRestored` is gated by a restoration permit | A lazy respawn cannot reuse the open's sink. It seeds the new mapper with `note_context_window` only, which is what makes the next `TurnUsageUpdated` carry the window; the persisted window was recorded at the first resume. `open_thread` on a reaped entry sends the update through the sink it was just given, as today |
| The forwarder drops a notice whose `(turn, message)` pair it has seen (`event_forwarder.rs:8`, `is_duplicate_notice`) | A respawn's "context was lost" notice must carry the turn it precedes (`turn: Some(turn)`), not `None`: a thread can lose its transcript more than once, and a turn-less fixed message would be dropped the second time |
| `AgentEvent::Notice { thread, turn: Option<TurnId>, message }` is a neutral event the mapper already emits (`mapper.rs:836`, the `apiKeySource` notice); `HarnessNotice { code, message, detail }` is only carried by `ThreadHandle.warning` at open | The lazy respawn's notice is an `AgentEvent::Notice` appended right after `TurnStarted`, with the same sentence `open_thread` puts in `claude_resume_failed`; the open path keeps `handle.warning` |
| Closing stdin makes an idle CLI exit 0 on its own (`ClaudeChild::close_stdin` doc, `process.rs:192`; every recording's `exit: 0`); the stop sequence already relies on it and kills after `STOP_EXIT_GRACE` | A reap is the stop sequence with no turn to interrupt: close stdin, read to EOF, kill on the grace timeout |
| `--resume <uuid>` restores the conversation and the adapter rebuilds the whole argv (`session_argv`, `process.rs:122`); a `--resume` whose transcript is gone exits with `No conversation found with session ID` and the adapter respawns `--session-id <same uuid>` with a notice (`harness.rs:1271`, `open_thread`; `resume-missing` fixture) | The lazy respawn is the open's resume path, factored out, not a new one: same argv, same bypass fallback, same missing-transcript fallback, same handshake (`get_context_usage` on resume) |
| `system/task_started` registers every task in `SessionState.tasks` (`mapper.rs:1054`), `local_bash` ones included, and only a terminal `task_updated` removes the entry (`mapper.rs:1126`); a `local_bash` task legitimately outlives its turn (`background-bash` fixture: `task_started` on line 8, the turn's `result` before the `completed` update on line 14; spec §5.2) | "Idle" must consult the task map, not only the active turn: a background shell can still produce frames and asks after the turn ended. The mapper gains `has_tasks()` |
| `ClaudeMapper::has_routes` is true while any sub-agent route is live (`mapper.rs:559`); routes are dropped by `finish_turn` once terminal, or by `child_exited` | A live route (a sub-agent still running, or its `Agent` call still open) keeps the child alive |
| Asks are recorded in the façade's `pending` map keyed by owner (`PendingRequests::remove_owner`, `session.rs:262`); an ask exists only while a turn or a task does | The idle check still counts them (one `std` lock per loop iteration, nothing awaited under it), so an ask a task raised after its turn ended is never reaped from under the user |
| The supervisor's `select!` is `biased` with the child's stdout first (`session.rs:714`, `main_loop`); arm bodies run after the race, so a write made from a body is never cancelled | Responses are dispatched from the line arm's body; the in-flight state advances there. The reason `await_control` checked the waiter *between* pumps (a dropped pump future losing a write) does not apply to a loop body |
| `commands.recv()` on a closed channel and `shutdown.wait_for` once the flag is set both resolve immediately on every poll; today each ends the loop at once (`session.rs:714`) | Once the loop keeps running through a stop sequence, those two arms must be disabled after they fired (`if` guards on the arms), or the loop spins |
| `ChildCommand::Stop` is intercepted by the main loop and replied on `run`'s exit (`session.rs:690`); `stop_handle` awaits that reply under `STOP_TIMEOUT` and aborts the task on overrun (`harness.rs:363`) | A stop that arrives while another is in progress joins it: the stop phase holds a `Vec` of replies, all sent at exit |
| `ClaudeDeclarationOptions {}` with `deny_unknown_fields` is type-checked at boot and again in `create` (`bin/giskard-server.rs:75`, `:78`, `:101`); Codex's `CodexDeclarationOptions { profile }` with `validate()` is the template (`giskard-harness-codex/src/lib.rs:485`); the `[harnesses.x]` test at `bin/giskard-server.rs:565` proves an extra key is refused | `idle_shutdown_secs` is one `Option<u64>` field on the Claude options; a misspelt key stays a startup error; a Codex declaration carrying it stays refused |
| `ClaudeLaunchOptions` derives `Default` and the façade tests build the harness from `ClaudeLaunchOptions::default()` (`harness.rs:1858`, `harness()`) | The launch options carry `idle_timeout: Option<Duration>` where `None` means never; the default binary value lives in the server binary's mapping (absent key → the adapter's `DEFAULT_IDLE_TIMEOUT`, `0` → `None`), so every existing test keeps a child that is never reaped |
| `tests/fake-claude.sh` treats every `--resume` as the missing-transcript failure (its header, `--resume <id>` line) | The real-process respawn test needs a successful resume: `FAKE_CLAUDE_RESUME_OK=1` makes `--resume` behave as a fresh child (answers `get_context_usage` too) |
| Claude Code's own background supervisor stops an idle unattached worker after about an hour (`claude agents` / `daemon status`, 2.1.287 help); the process is ~440–530 MB RSS and a `--resume` handshake is a few seconds | The default of **600 s** is a judgment, not a measurement: long enough that a user reading an answer and replying does not pay a respawn, short enough that a dozen threads do not hold gigabytes for an hour. It is one constant to change |

## Step 1: the supervisor as a state machine (`crates/giskard-harness-claude/src/session.rs`)

### What goes

- `AWAIT_POLL_SLICE` (`:36`), `await_control` (`:1458`), `stop_requested_while_waiting`
  (`:1548`), `pump_until` (`:1887`), the `deferred: VecDeque<ChildCommand>` and
  `stop_request` fields (`:609`, `Supervisor`), the `deferred` / `stop_request` drain at the top
  of `main_loop` (`:714`), `Waiter::Raw` (`:465`).
- `apply_turn_settings` (`:1263`) as one async function. Its four requests and every log line
  and error mapping in it survive, split by stage (below). Keep `CONTROL_TIMEOUT`,
  `TURN_SETTINGS_BUDGET`, `STOP_INTERRUPT_GRACE`, `STOP_EXIT_GRACE` and their meanings.
- `stop()` (`:1916`) as a function that pumps; it becomes the `Stopping` phase (below).
- The `ControlFailure` type (`:500`) stays: it is the outcome of one stage's response.

### The in-flight turn hand-off

```rust
/// The `StartTurn` hand-off between its first settings request and the user line: the main loop
/// advances it on each control response, fails it on its stage deadline, and runs its last step
/// (the write) when the read-back is in. At most one is in flight; a second `StartTurn` or a
/// `Compact` meanwhile is `ThreadBusy`.
struct TurnSetup {
    line: String,
    turn: TurnId,
    model: ModelRef,
    settings: TurnSettings,
    reply: oneshot::Sender<Result<(), HarnessError>>,
    /// A sentence to append as `AgentEvent::Notice` right after `TurnStarted` (a respawn that
    /// lost its transcript).
    notice: Option<String>,
    stage: SetupStage,
    /// The outstanding request's id; checked before `waiters` when a response arrives.
    request_id: String,
    /// `started + TURN_SETTINGS_BUDGET`.
    budget: Instant,
    /// The outstanding request's deadline: `min(now + CONTROL_TIMEOUT, budget)`.
    deadline: Instant,
    /// Decided when the mode is in: the `set_model` to send, and the effort to send.
    model_change: Option<String>,
    effort_change: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SetupStage {
    /// `set_permission_mode`, sent on every turn.
    Mode,
    /// `set_model`, when the requested model differs from the one the CLI holds.
    Model,
    /// `apply_flag_settings {effortLevel}`, when the effort differs or the model changed.
    Effort,
    /// `get_settings`, the read-back of `applied.model` and `applied.effort`.
    ReadBack,
}
```

`Supervisor` gains `turn_setup: Option<TurnSetup>` (a plain field with a doc comment: not keyed,
one at a time, cleared by the reply) and `ChildCommand::StartTurn` gains `notice: Option<String>`
(`None` from every caller until Step 3).

Functions, each a method on `Supervisor`:

- `begin_turn_setup(line, turn, model, settings, notice, reply)` replaces `start_turn` (`:1187`).
  The pre-checks are unchanged (caller gave up → `log_caller_gave_up` and drop; `busy` →
  `ThreadBusy`) plus one: `turn_setup.is_some()` → `ThreadBusy` with a `debug` line
  (`action = "start_turn"`, `"refusing a turn while another's settings are in flight"`). Then,
  as `apply_turn_settings` does first: `mapper.set_expected_mode(settings.mode)`, write
  `set_permission_mode`, and install the `TurnSetup` at `Mode` with its `request_id`, `budget`
  and `deadline`. A write failure replies the error and returns it (the child is broken, as
  today).
- `on_setup_response(payload)` is called by `dispatch` for a `ControlResponse` whose
  `request_id` is the setup's. It takes the setup out of the field, applies the stage's logic
  lifted verbatim from `apply_turn_settings`, and either writes the next request (putting the
  setup back with a fresh `request_id` and `deadline`), finishes, or fails:
  - `Mode`: success → `current_mode = settings.mode`, the `debug` line; compute `model_change`
    and `effort_change` exactly as today (`:1304`–`:1312`); none → `log_turn_settings(false)`
    and finish; else next is `Model` if `model_change` is `Some`, else `Effort`. Failure →
    restore `expected_mode` to `current_mode`, the `warn` line, fail with
    `failure.into_error()`.
  - `Model`: success → next is `Effort` if `effort_change` is `Some`, else `ReadBack`. Failure
    → the `warn` line and the `catalog_unknown` → `Unsupported` mapping, fail.
  - `Effort`: success → `ReadBack`. Failure → the `warn` line, fail.
  - `ReadBack`: the `applied.model` / `applied.effort` checks, `current_model` /
    `current_effort` updates and both mismatch errors, verbatim; success →
    `log_turn_settings(true)` and finish.
- `finish_turn_setup(setup)` is today's tail of `start_turn` after the settings: the second
  `reply.is_closed()` and `busy` checks ("frames read while the settings were applied may have
  opened a turn of the CLI's own"), `withdrawn.clear()`, `begin_turn`, `note_turn_model`, the
  `TurnStarted` append, then — new — `append(AgentEvent::Notice { thread, turn: Some(turn),
  message })` when `notice` is `Some`, then the `start_turn` info line and the write, replying
  `Ok` or the write error.
- `fail_turn_setup(setup, error, action)`: the `debug` line of today's `start_turn` ("the turn's
  settings were not applied; the turn does not start"), `reply.send(Err(error))`, and the field
  left `None`. On a **timeout** (below) the setup's `request_id` is pushed to `abandoned`, so the
  CLI's late answer logs at `debug` ("late answer to a handshake request that timed out" becomes
  "late answer to a request that timed out"), instead of today's `warn` for a request nobody
  waits on. The timeout error text stays `claude did not answer {subtype} within {:.1} s`.
- The stop sequence fails an in-flight setup with
  `HarnessError::Transport("claude child is stopping")`, today's message, before anything else.

The `TurnSetup` holds the `oneshot::Sender`, so a caller that gave up is still detected at the
write, as today; nothing new is written on behalf of a caller whose receiver is gone.

### Responses

`dispatch` (`:804`), `MapperOutput::ControlResponse` arm, in this order:

1. `turn_setup.as_ref().is_some_and(|setup| setup.request_id == request_id)` →
   `on_setup_response(payload).await?` (it may write the next request; a write failure is the
   `Err` that breaks the child, as every write in `dispatch`).
2. Else `waiters.remove(&request_id)`: a `Waiter::StopTask` (below) logs its outcome and
   replies; any other resolves as today.
3. Else `abandoned` → `debug`; else the `warn` for an unexpected response, unchanged.

### `stop_task` as a waiter

`stop_task` (`:1757`) keeps its three lookups (`route_task_id`: not started, ended, not a
route) and `note_stop_sent`, writes the request, and inserts
`Waiter::StopTask { thread, task_id, harness_thread_id, reply }` instead of awaiting. The
response handler logs today's `info` ("sub-agent stopped") or `warn` ("Claude Code did not stop
the sub-agent", `error`) with the same fields and replies. The supervisor-side `CONTROL_TIMEOUT`
goes: the façade's `interrupt_route` already bounds the call with `CONTROL_TIMEOUT`
(`harness.rs:481`), and a waiter left behind resolves `child_stopped` at exit like any other.

### The stop sequence as a phase

```rust
/// What the main loop is doing besides reading frames.
enum Phase {
    /// Reading frames and serving commands.
    Serving,
    /// The stop sequence. No command is served: each is refused at once with `child_stopped`.
    Stopping(Stopping),
}

struct Stopping {
    /// `stop`, `shutdown`, `harness_dropped` or `idle`.
    reason: &'static str,
    /// Every `Stop` that arrived; all are answered when the child has exited.
    replies: Vec<oneshot::Sender<()>>,
    stage: StopStage,
    /// The child is reaped for idleness: its thread keeps its entry and its log stays open.
    reaped: bool,
}

enum StopStage {
    /// `interrupt` was written for the live turn; waiting for its `result`.
    Interrupting { turn: TurnId, request_id: String, started: Instant, deadline: Instant },
    /// stdin is closed; waiting for EOF.
    Draining { deadline: Instant },
}
```

- `enter_stopping(reason, reply: Option<Sender>, reaped: bool)`: if already `Stopping`, push the
  reply and log at `debug` (`action = "stop"`, `"a second stop joined the stop sequence"`),
  return. Else fail an in-flight `TurnSetup` as above; then, as today's `stop()`: with an active
  turn, write `interrupt` (waiter `Waiter::Stop`, `interrupt_sent = true`,
  `note_interrupt_sent`) and enter `Interrupting { deadline: now + STOP_INTERRUPT_GRACE }`; a
  failed interrupt write logs today's `warn` ("could not interrupt the live turn before
  stopping") and falls through to draining. With no active turn, `close_stdin()` and enter
  `Draining { deadline: now + STOP_EXIT_GRACE }`.
- `after_line()` runs after every dispatched line: in `Interrupting`, when
  `mapper.active_turn()` is `None`, log today's `stop_interrupt` line (`turn_closed = true`,
  `elapsed_ms`) and move to `Draining` (close stdin, new deadline).
- The stage deadline is a `select!` arm, `() = sleep_until(deadline), if stopping`:
  `Interrupting` → the `stop_interrupt` line with `turn_closed = false`, then `Draining`;
  `Draining` → today's `stop_kill` `warn`, `start_kill()`, and the loop returns.
- EOF in `Draining` (the line arm's `Ok(None)`) returns normally. EOF while `Serving` is
  `Ending::Eof` as today. A read error anywhere is `broken` → `Ending::Broken`.
- `Ending::Stopped(Option<Sender>)` becomes
  `Ending::Stopped { replies: Vec<Sender>, reaped: bool }`; `run` passes `reaped` to `on_exit`
  and sends every reply after it.

### The main loop

```rust
loop {
    self.track_idle();                       // Step 2; a no-op in commit 1
    tokio::select! {
        biased;
        line = self.child.next_line() => { … dispatch, then self.after_line() … }
        command = self.commands.recv(), if !self.commands_closed => match command {
            Some(ChildCommand::Stop { reply }) => {
                self.enter_stopping("stop", Some(reply), false).await
            }
            Some(command) if self.stopping() => self.refuse(command),
            Some(command) => { … handle_command … }
            None => {
                self.commands_closed = true;
                self.enter_stopping("harness_dropped", None, false).await
            }
        },
        () = shutdown_signal(&mut self.shutdown), if !self.stopping() => {
            self.enter_stopping("shutdown", None, false).await
        }
        () = sleep_until(deadline), if let Some(deadline) = self.stage_deadline() => {
            self.on_deadline().await
        }
        () = sleep_until(idle_at), if let Some(idle_at) = self.idle_deadline() => {
            self.reap().await                // Step 2
        }
    }
    if let Some(ending) = self.ending.take() { return ending; }
}
```

(The `if let` guards are illustrative; compute the two `Option<Instant>`s before the `select!`
and guard with `is_some()`, using `tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now))`
or a `futures::future::pending()` fallback. `stage_deadline()` is the `TurnSetup`'s deadline
while `Serving` and the stop stage's while `Stopping`.) The two guards on `commands.recv()` and
`shutdown_signal` are the ones the verified-facts table calls out: without them a stop sequence
spins once the channel closed or the flag is set. The `Stop` command no longer needs its
"reaching here would be a routing bug" arm in `handle_command` (`:1150`); delete it.

- `refuse(command)`: every command carrying a reply gets `Err(child_stopped())` at once
  (`StartTurn`, `Interrupt`, `Control`, `RespondApproval`, `RespondServerRequest`, `StopTask`,
  `Compact`), logged once per command at `debug` (`action = "stop_refused"`, `command`,
  `reason`). Today such a command waits until the supervisor ends and the channel drops; a
  `RespondApproval` refused here puts nothing back in `pending` (its ask dies with the child
  anyway, `remove_owner` at exit).
- `on_deadline()`: a `TurnSetup` deadline → `fail_turn_setup` with the timeout error and the
  `warn` line `await_control` logs today ("Claude Code did not answer a control request in
  time", `timeout_ms`, `action = <subtype>`); a stop stage deadline → as above.

Everything else `handle_command` does (`Interrupt`, `Control`, `RespondApproval`,
`RespondServerRequest`, `Compact`) is untouched: none of them awaited a response. `Compact`
gains the `turn_setup.is_some()` → `ThreadBusy` check beside its `busy` check.

### Why this is safe to do first

The façade tests drive the supervisor through `ScriptedChild` (`session.rs:2146`), whose
`Respond`, `RespondError`, `RespondErrorCode`, `RespondOnNextWrite`, `RespondAfter`,
`BreakStdin` and `Exit` actions cover each stage outcome, and the existing tests
`every_turn_sets_its_permission_mode`, `a_refused_mode_fails_the_turn_start`,
`a_model_change_is_sent_and_read_back`, `an_unknown_model_fails_the_turn_start`,
`an_effort_change_is_sent_and_read_back`, `a_refused_effort_fails_the_turn_start`,
`a_read_back_mismatch_is_a_protocol_error`, `the_mode_status_after_a_set_is_not_drift`,
`a_start_turn_whose_caller_timed_out_is_never_written`, `stop_kills_a_child_that_ignores_eof`,
`a_stop_that_times_out_still_closes_the_stream`, `cancel_interrupts_and_the_turn_is_interrupted`
and the milestone-5 `stop_task` tests (`harness.rs:2018`–) must pass unchanged. One visible
difference: a request that times out now logs its late answer at `debug`; a test asserting the
`warn` ("control response for a request nobody is waiting on") for that case, if any, changes
to assert the `debug`.

## Step 2: idle reaping (`session.rs`, `mapper.rs`)

### What idle means

A child is idle when **all** of these hold, checked at the top of every loop iteration while
`Serving`:

| Condition | Where it is read | Why |
| --- | --- | --- |
| no in-flight `TurnSetup` | `turn_setup.is_none()` | the settings are half applied |
| no active turn | `mapper.active_turn().is_none()` | frames are coming |
| no live sub-agent route | `!mapper.has_routes()` | a sub-agent runs, or its `Agent` call is open |
| no open task | `!mapper.has_tasks()` (new: `!session.tasks.is_empty()`) | a `local_bash` outlives its turn and can still ask |
| no control request outstanding | `waiters.is_empty()` | an `interrupt`, `rename_session`, `stop_task` or a stop's interrupt awaits an answer |
| no ask pending for this owner | `lock(&self.pending).count_owner(self.thread) == 0` (new counter beside `remove_owner`) | the user has a card open |

The handshake is never "idle": the supervisor does not exist until the handshake is done, and the
mapped `early_lines` run before the first loop iteration.

A `task_started` whose terminal `task_updated` never comes keeps a child alive for good. That is
the right failure: reaping under a task the CLI believes is running would cut it off. It is
visible in the `idle` log line below (`reason = "tasks"`, `open_tasks`).

### The timer

- `SupervisorParts` and `Supervisor` gain `idle_timeout: Option<Duration>`; `Supervisor` gains
  `idle_since: Option<Instant>` (a plain field: "when the child last became idle; `None` while it
  has something to do").
- `track_idle()`: compute `not_idle: Option<&'static str>` (the first failing condition's name:
  `turn_setup`, `turn`, `routes`, `tasks`, `control_request`, `asks`). Transition `None → Some`
  (became idle): `idle_since = Some(now)`, `debug` (`action = "idle"`, `idle = true`,
  `timeout_ms`). Transition `Some → None`: `idle_since = None`, `debug` (`idle = false`,
  `reason`, `open_tasks`). No line when nothing changed. With `idle_timeout == None` the clock
  is still tracked (the lines are useful) but no arm is armed.
- `idle_deadline()`: `Serving` and `idle_timeout` and `idle_since` all present →
  `Some(idle_since + idle_timeout)`.

### The reap

`reap()`, when the idle arm fires:

1. Take the thread's child out of the façade's map **before** stdin is closed, under the
   `threads` lock (Step 3's `ThreadEntry`): if the entry's `child` has this supervisor's
   `generation`, set `entry.child = None` and `entry.model = self.current_model.clone()` (the
   model and effort the CLI held, so the respawn launches with them and `start_turn`'s fallback
   model matches). If the entry is gone (a concurrent delete) or holds another generation, log at
   `warn` (`action = "child_reaped"`, `"the thread's entry no longer holds this child"`) and run
   an ordinary stop instead (`reaped = false`): the log closes as for any exit.
2. `info` (`action = "child_reaped"`, `idle_ms`, `timeout_ms`, `pid`, `live_children` = entries
   with a child after this one left, `loaded_threads` = entries).
3. `enter_stopping("idle", None, true)`. There is no turn to interrupt, so this is
   `close_stdin()` and `Draining`; the CLI exits 0 at EOF, or is killed after `STOP_EXIT_GRACE`
   with today's `stop_kill` `warn`.

From step 1 on, the façade sees a thread with no child: a `start_turn` that was already past its
`live()` lookup and sends to this child's channel is refused with `child_stopped` (`refuse`) and
retried by the façade on a fresh child (Step 3, *The retry window*).

### Exit handling

`on_exit(exit, requested, reaped)` (`:1983`), on the reaped path:

- **does not** `remove_owner` the pending map: idle means none, and by the time this runs a
  respawned child may own asks under the same thread id;
- **does not** `log.close()`;
- **does not** touch the `threads` map (its child was taken at the reap);
- **does** `cool_routes()` (generation-guarded: the sub-agent threads of this child, if any logs
  are still published, become cold routes exactly as after any exit), drains `waiters` (none
  expected; the existing `warn` per unanswered request stays), and runs `mapper.child_exited`
  only if a turn or route is somehow open (it is not; the branch is kept as is);
- logs today's `child_exited` line with a new `reaped` field; `expected` is `requested && exit 0`
  as today, so a clean reap is `info`.

The non-reaped paths are unchanged, including the unexpected-exit path: a crash closes the log
and removes the whole thread entry (Step 3), so the server ends the stream and the thread goes
cold, as today.

## Step 3: the façade (`harness.rs`, `process.rs`, `bin/giskard-server.rs`, config)

### Thread entries

`children: Children` (`harness.rs:89`, `HashMap<ThreadId, ChildHandle>`) becomes
`threads: Threads`, `HashMap<ThreadId, ThreadEntry>`:

```rust
/// One primary thread this instance holds: its session, its retained log, and its child while
/// one runs. The entry outlives a reaped child; only delete, archive and shutdown remove it.
pub(crate) struct ThreadEntry {
    /// The session id: `--session-id` at the first spawn, `--resume` on every respawn.
    pub harness_thread_id: String,
    pub log: Arc<EventLog>,
    pub workspace_root: PathBuf,
    /// The model the CLI holds or held: the open model, then what the last supervisor confirmed.
    pub model: ModelRef,
    pub child: Option<ChildHandle>,
}

/// The façade's view of one live child.
pub(crate) struct ChildHandle {
    pub commands: mpsc::Sender<ChildCommand>,
    pub task: JoinHandle<()>,
    pub launch_mode: LaunchMode,
    pub generation: u64,
}
```

Rewrite the map's `ENTITY-AUTHORITY-EXCEPTION` comment: the source of truth is `open_thread`
and the respawn (insert), the supervisor's reap (clears `child`), the supervisor's other exits
(remove the entry); `delete_thread`, `set_thread_archived(true)` and `shutdown` remove entries
and close their logs. `live_children()` counts entries with a child; add `loaded_threads()`
(entries), reported on the `thread_opened`, `child_exited`, `child_reaped` and `shutdown` lines.
`LiveChild` (`:695`) gains `generation: u64`. `subscribe` (`:1569`) reads `entry.log`, so a
reaped thread's handle still subscribes to its open log.

### Respawn

Factor the spawn-with-fallbacks out of `open_thread` (`:1271`, from `spawn_child_for` to the
`HarnessNotice`) into

```rust
/// One session child with the open's fallbacks: bypass → standard, and on resume a missing
/// transcript → a fresh session with the same id, reported as `notice`.
async fn spawn_session(&self, target: &SpawnTarget, session: SessionFlag)
    -> Result<Spawned, HarnessError>;

struct SpawnTarget { thread: ThreadId, workspace_root: PathBuf, model: ModelRef }
struct Spawned {
    child: Box<dyn ClaudeChild>,
    handshake: Handshake,
    launch_mode: LaunchMode,
    notice: Option<HarnessNotice>,
}
```

(`spawn_child_for` and `spawn_and_handshake` take a `&SpawnTarget` instead of
`&OpenThreadOptions`; `OpenThreadOptions` supplies one.) Then

```rust
/// Register a handshaken child for `thread`: a new entry, or the entry's reaped slot.
fn register_child(
    &self, thread: ThreadId, spawned: Spawned, mapper: ClaudeMapper, log: Arc<EventLog>, …
) -> Result<LiveChild, (HarnessError, Box<dyn ClaudeChild>)>;
```

is today's `registered` block (`:1409`–`:1437`): under the lock, refuse after shutdown; on a
vacant entry insert a new `ThreadEntry`; on an entry with `child: None` fill the slot (this is
the respawn); on an entry with a live child, hand the fresh child back as the error "thread
{thread} was opened concurrently" — the caller reaps it, as today, and for a respawn then returns
the existing child instead of an error (two callers raced; both get the winner). The supervisor
is spawned with the entry's **existing** log on a respawn, a new one on a first open.

```rust
/// The thread's live child, respawned from its entry when it was reaped.
async fn ensure_child(&self, thread: ThreadId) -> Result<Respawned, HarnessError>;

struct Respawned {
    live: LiveChild,
    notice: Option<HarnessNotice>,
    context_window: Option<u32>,
    resumed_model: Option<ModelRef>,
}
```

`ensure_child`: `live(thread)` → `Some` → return it with no notice. Else take the entry's
`harness_thread_id`, `workspace_root` and `model` (no entry → `ThreadNotFound`), then
`spawn_session(target, SessionFlag::Resume(id))`, store the catalog from the handshake, compute
`resumed_model` (`:332`, which logs `model_not_applied`), build the mapper (`set_expected_mode
("default")`, `note_context_window` from the handshake), `register_child`, update `entry.model`
to the resumed model, and log `info` (`action = "respawn"`, `thread_id`, `harness_thread_id`,
`resume_fallback = notice.is_some()`, `launch_mode`, `live_children`, `loaded_threads`,
`elapsed_ms`). A failed respawn leaves the entry as it was (child `None`), so the next call tries
again, and returns the handshake's error (the same `Spawn` / `Unauthenticated` / `Timeout` text
`open_thread` would give), which the server shows as the turn's failure.

### The trait methods on a reaped thread

| Method | Today (`:1569`–`:1805`) | After |
| --- | --- | --- |
| `open_thread` | returns the live child's handle; else spawns | an entry with a live child: as today (log at `warn` if `opts.resume` names another session than the entry holds, keep returning the entry's); an entry without one: `ensure_child`, then `ThreadUpdate::ContextWindowRestored` through `opts.updates` when the handshake reported a window, `handle.warning = notice`, `handle.resumed_model`; no entry: as today, through `spawn_session` + `register_child` |
| `start_turn` | `live()` or `ThreadNotFound` | `ensure_child`; `notice.map(\|n\| n.message)` goes on `StartTurn { notice }`; the bypass check and the rest unchanged; retried once per *The retry window* |
| `compact_thread` | `live()` or `ThreadNotFound` | `ensure_child`; `Compact` gains `notice` too, appended after its `TurnStarted` (a compaction is a turn in the log); retried once |
| `interrupt` (primary) | `live()` or `ThreadNotFound` | entry with no child → `Ok(())` and a `debug` (`action = "interrupt"`, `"no live claude child; nothing to interrupt"`): the trait's contract is "interrupt the active turn", and a reaped thread has none. No entry → `ThreadNotFound` as today |
| `set_thread_name` | no live child → `Ok`, `debug` | unchanged (the entry without a child is "no live child") |
| `set_thread_archived(true)`, `delete_thread` | `stop_thread`: remove the child, stop it | `stop_thread` removes the **entry**; with a child, `stop_handle` as today (its exit closes the log); without one, close `entry.log` here and log `debug` (`"no live claude child; the thread's log is closed"`), drop its pending asks (none) |
| `shutdown` | drains the children, stops them, closes route logs | drains the entries: stops the live children through `stop_handle`, closes the log of every entry that had no child, then routes and pending as today; `children_stopped` and a new `threads_closed` on its line. A child mid-reap (taken from its entry, still draining) finishes on its own under the shutdown flag; `shutdown` does not wait for it |
| `list_mcp_servers` with a hint | `child_carrying` → live child or probe | unchanged: a reaped thread has no live child, so the probe answers, as the MCP plan specified for "a hint without a live child". A respawn is not spawned for an MCP status read |
| `respond_approval`, `respond_server_request` | `live(ask.owner)` | unchanged: an ask cannot exist for a reaped owner |
| `claim_native_thread`, `subscribe`, `interrupt` on `task:` | routes | unchanged; a reaped child's routes are cold, so `interrupt` reports "no longer running" as after any exit |

`stop_handle` (`:363`) takes the entry's `harness_thread_id` and `log` as arguments now that
`ChildHandle` no longer carries them; its two `log.close()` calls on the abort paths stay.

### The retry window

Between the supervisor taking the child out of the entry (reap step 1) and the façade's `live()`
lookup there is a window in which `start_turn` sends to a channel whose supervisor is draining.
The supervisor refuses it with `child_stopped()`; a channel already dropped gives the same error
from `call`. So `start_turn` and `compact_thread` do:

```rust
let mut attempt = self.ensure_child(thread).await?;
loop {
    match self.call(thread, attempt.live.commands.clone(), …).await {
        Err(error) if self.reaped_under(thread, attempt.live.generation) => {
            debug!(…, action = "start_turn", "reaped under the hand-off; respawning");
            attempt = self.ensure_child(thread).await?;   // once
        }
        outcome => break outcome,
    }
}
```

`reaped_under(thread, generation)` is true when the entry still exists and holds no child or a
child of another generation; it is false when the entry is gone (a crash or a delete: the
original error is returned, not a `ThreadNotFound` from the retry). One retry, then the error
stands. The `TurnId` is minted once and reused, so the server's admitted turn id is the one that
runs.

### Configuration

- `process.rs`: `ClaudeLaunchOptions` gains `idle_timeout: Option<Duration>` ("reap a child
  idle this long; `None` never reaps") and `pub const DEFAULT_IDLE_TIMEOUT: Duration =
  Duration::from_secs(600)`, re-exported from `lib.rs`. `session_argv` is untouched.
- `bin/giskard-server.rs`: `ClaudeDeclarationOptions { #[serde(default)] idle_shutdown_secs:
  Option<u64> }` (keep `deny_unknown_fields`); `claude_options` unchanged; `create` maps
  `None → Some(DEFAULT_IDLE_TIMEOUT)`, `Some(0) → None`, `Some(n) → Some(n s)`, logs the
  effective value on its "claude-code instance created" line (`idle_timeout_ms`). Rewrite the
  doc comment at `:70` ("has no kind-specific keys (plan §5.1)"): it has one.
- Codex declarations are untouched; `idle_shutdown_secs` on one is still refused by its own
  `deny_unknown_fields`.

## Step 4: tests

Façade tests in `harness.rs` (`mod tests`), through `ScriptedChild`, with
`#[tokio::test(start_paused = true)]` wherever time matters. Add a `harness_with(launch, children)`
beside `harness()` (`:1858`) so a test sets `idle_timeout: Some(Duration::from_secs(60))`; every
existing test keeps `ClaudeLaunchOptions::default()` (never reaps). A `reaped(record)` helper
waits for `ScriptRecord.stdin_closed` and `live_children() == 0`.

Commit 1 (state machine):

1. `a_turn_setup_that_times_out_names_its_stage`: the script answers `set_permission_mode` and
   never `set_model`; `start_turn` fails `Timeout` naming `set_model`, before
   `TURN_SETTINGS_BUDGET`; a `RespondOnNextWrite` answer later logs the `debug` late-answer
   line, not the `warn` (`#[traced_test]`, `logs_assert` with `no_line_with`).
2. `a_stop_during_a_turn_setup_fails_the_hand_off_at_once`: `set_permission_mode` unanswered;
   `delete_thread` concurrently; `start_turn` returns the Transport "claude child is stopping"
   and the delete completes well under `CONTROL_TIMEOUT` (paused time, `Instant` arithmetic).
3. `a_start_turn_during_a_turn_setup_is_thread_busy`, and the same for `compact_thread`.
4. `a_rename_answered_during_a_turn_setup_resolves`: `set_thread_name` sent while `set_model` is
   outstanding; its `rename_session` response resolves its own waiter.
5. `commands_during_the_stop_sequence_are_refused_at_once`: a child whose `result` after
   `interrupt` is delayed (`RespondAfter` on the interrupt, `Emit` the result later);
   `delete_thread` enters `Interrupting`; a `start_turn` sent meanwhile errors immediately with
   "claude child stopped", before the stop finishes; the `stop_refused` line is logged.
6. `two_stops_are_answered_together`: `set_thread_archived(true)` and `delete_thread` racing on
   one thread; both return, one child stopped, one `child_exited` line.
7. The milestone-5 `stop_task` tests and every settings test listed under *Why this is safe to
   do first* pass unchanged.

Commit 2 (reaping), all with `idle_timeout: Some(60 s)` unless stated:

8. `an_idle_child_is_reaped_after_the_timeout`: open, one text turn completes; advance 60 s; the
   child's stdin closed and it exited 0; `live_children() == 0`; the stream is **not** closed
   (a reader sees no `Closed`, `log.is_closed()` is false); `child_reaped` then `child_exited`
   with `reaped = true` at `info`.
9. `a_turn_resets_the_idle_clock`: advance 59 s, run a turn (it completes), advance 59 s: not
   reaped; advance 1 s: reaped.
10. `a_pending_ask_prevents_reaping`: the `tool-allowed` ask pending; advance 120 s: not reaped;
    answer it, the turn completes, advance 60 s: reaped. The `idle` line with `reason = "asks"`
    is logged.
11. `an_open_background_task_prevents_reaping`: `EmitFixturePrefix` of `background-bash` up to
    the turn's `result` (line 8's `task_started` included, line 14's `completed` excluded); advance
    120 s: not reaped (`reason = "tasks"`, `open_tasks = 1`); inject the terminal
    `task_updated`; advance 60 s: reaped.
12. `a_live_sub_agent_route_prevents_reaping`: `delegation` fixture cut before the sub-agent's
    terminal `task_updated`; not reaped; the rest of the fixture; reaped, and the route is cold
    (`interrupt` on the claimed sub-agent handle is "no longer running"; its log is open).
13. `a_reaped_thread_respawns_with_resume_on_the_next_turn`: two scripted children; after the
    reap, `start_turn` → the spawner's second argv carries `--resume <same id>`, `--model` the
    model the first child held, the handshake sent `get_context_usage`; the turn runs; the
    **same** reader that saw the first turn sees the second `TurnStarted` and a
    `TurnUsageUpdated` carrying the window; no `Notice`.
14. `a_reaped_thread_whose_transcript_is_gone_starts_fresh_and_notices`: second child =
    `missing_transcript_exit()`, third fresh; the third argv carries `--session-id <same id>`; the
    stream carries `Notice { turn: Some(turn), message }` right after `TurnStarted`, with the
    `claude_resume_failed` sentence; `claude_resume_failed` logged at `warn`.
15. `a_failed_respawn_fails_the_turn_and_keeps_the_thread`: second child exits unauthenticated;
    `start_turn` is `Unauthenticated`; `subscribe` is still live; a third child then succeeds.
16. `a_turn_that_lands_in_the_reap_window_runs_on_a_fresh_child`: first child `ignoring_eof()`
    (so `Draining` lasts `STOP_EXIT_GRACE`); advance to the reap, then `start_turn` while it
    drains: the turn runs on the second child, one `TurnStarted`, the first child killed
    (`stop_kill`), the `debug` "reaped under the hand-off" line logged.
17. `a_reaped_thread_is_interrupted_and_renamed_as_no_ops`: both `Ok`, nothing spawned.
18. `deleting_or_archiving_a_reaped_thread_closes_its_stream`: the reader gets `Closed`; the
    entry is gone (`open_thread` afterwards spawns anew).
19. `shutdown_closes_reaped_threads`: one live, one reaped; both streams close; the line reports
    `children_stopped = 1`, `threads_closed = 2`.
20. `open_thread_on_a_reaped_thread_respawns_eagerly`: `ThreadUpdate::ContextWindowRestored`
    arrives on the new sink, `resumed_model` is set, the handle is the same session.
21. `idle_timeout_none_never_reaps`: `ClaudeLaunchOptions::default()`, advance 24 h, still live.
22. `list_mcp_servers_hinting_a_reaped_thread_probes`: the probe child answers; no respawn (the
    spawner's argv list shows the probe's protocol-only argv, no `--resume`).
23. Mapper (`mapper.rs` tests): `has_tasks` true after `task_started`, false after the terminal
    update, for `local_bash` and `local_agent` alike.
24. Session (`session.rs` tests): `count_owner` counts approvals and server requests of one owner.
25. Real process (`fake-claude.sh`, `harness.rs:4985`–):
    `an_idle_fake_claude_is_reaped_and_resumed` with `FAKE_CLAUDE_RESUME_OK=1` and
    `idle_timeout: Some(1 s)` (real time, no pause): a turn, the process gone (`pid` no longer
    alive, or the `child_exited` line with `reaped = true`), a second turn on a `--resume`
    process, one continuous stream.
26. `bin/giskard-server.rs` tests, beside the `[harnesses.x]` cases at `:536`–:
    `idle_shutdown_secs = 0` and `= 30` accepted on `claude-code`, `= "soon"` refused naming the
    key, `= 30` on a `codex` declaration refused.

## Step 5: documentation

### `crates/giskard-harness-claude/README.md`

- Intro (`:27`): "Still to come" drops idle reaping and the supervisor state machine; keep
  milestones 7 and 8.
- *Runtime ownership* (`:31`–): the supervisor bullet replaces the `await_control` sentences
  with the phases: serving (with at most one turn hand-off in flight, advanced by its responses
  and failed by its stage deadline), stopping (interrupting, draining; commands refused at once),
  and the idle clock; the façade bullet renames `children` to `threads` and says an entry
  outlives a reaped child (session id, open log, last model) and what removes it.
- *Handshake, resume and respawn* (`:289`, "A second `open_thread` for a thread with a live child
  returns that child's handle"): add the reaped case (respawn with `--resume`, same fallbacks,
  the notice as an event on the next turn rather than on the handle).
- *Process control* (`:292`–): *Turns* keeps its "25 s budget … supervisor's own timeout"
  sentence (its meaning holds) and says the stage deadline is a loop input;
  *Stop* says commands arriving during the stop are refused at once; *Child exit* gains the reap
  (log stays open, entry keeps the thread, routes cold); a new **Idle reaping** bullet: the six
  conditions, `idle_shutdown_secs` (default 600 s, `0` never), the respawn on the next message
  and what the user sees (nothing, or the context-lost notice), and that an MCP status read never
  respawns.
- *Code and tests* (`:598`–): `session.rs`'s description names the phases and the turn hand-off
  state instead of `await_control`; `fake-claude.sh`'s gains `FAKE_CLAUDE_RESUME_OK`.

### `README.md`

- *Supported harnesses* (`:55`–`:57`): "Idle-process reaping and structured diffs are not there
  yet" → structured diffs only; one sentence that an idle process is reaped after
  `idle_shutdown_secs` and resumed on the next message.
- *Prerequisites* (`:74`–`:75`): "and idle ones are not reaped yet" → "and one idle for
  `idle_shutdown_secs` (default 10 minutes) is stopped and resumed on the next message".
- Configuration table (`:287`, the `profile` row): the Claude Code half becomes a row of its own,
  `idle_shutdown_secs` | `600` | **Claude Code only.** Seconds a thread's `claude` process may sit
  idle before it is stopped; `0` keeps every process until its thread is archived, deleted or
  the server stops. Any other kind-specific key is a startup error.
- The **Claude Code** paragraph (`:301`): "takes only the neutral keys above" → "takes the neutral
  keys above and `idle_shutdown_secs`"; add the key to the example.

### `config.example.toml`

- The rules comment (`:110`–`:113`): "a `claude-code` declaration accepts no kind-specific key"
  → names `idle_shutdown_secs`.
- The `[harnesses.claude]` example (`:130`–`:135`): `# idle_shutdown_secs = 600` with a one-line
  comment.

### `docs/multi-harness-design.md`

- The per-concern bullet (`:142`, "Idle policy"): Claude Code applies it per process, inside the
  adapter, through `idle_shutdown_secs` on the declaration.
- *Open questions* (`:520`): the Claude Code half is answered (a declaration key, per process);
  the Codex half stays open, reworded so.
- `:230`–`:231` ("idle shutdown as an instance policy remains an open question"): add "answered
  for Claude Code by its milestone 6".

### `specs/giskard-specification.md`

`:2446` ("**Idle shutdown:** not implemented"): Codex: not implemented; Claude Code: per
process, `idle_shutdown_secs` on the declaration, the thread stays open and resumes on its next
message.

### `specs/claude-code-harness-plan.md`

- §5.2's idle paragraph (`:591`–`:601`): keep the measurement and the gap; replace "Reaping is
  milestone 6" with how it is closed.
- §11, *Milestone 6* (`:1631`): append "Milestone 6 is implemented: …" naming the phases, the
  `TurnSetup`, the reap and the key, as milestones 4 and 5 did.
- §12's RSS row (`:1695`): "reaping in milestone 6" → reaped after `idle_shutdown_secs`.

## Logging

Stable fields: `project_id`, `harness`, `thread_id`, `harness_thread_id`, `turn_id`, `pid`,
`action`, `request_id`, `live_children`, `loaded_threads`. Actions introduced: `idle` (`debug`,
both transitions, with `reason` / `open_tasks` when not idle and `timeout_ms` when idle),
`child_reaped` (`info`, `idle_ms`, `timeout_ms`; `warn` when the entry no longer holds the
child), `respawn` (`info`, `resume_fallback`, `launch_mode`, `elapsed_ms`), `stop_refused`
(`debug`, `command`, `reason`), `stop` (`debug`, a second stop joining). Existing actions keep
their lines: `set_permission_mode`, `set_model`, `apply_flag_settings`, `get_settings`,
`turn_settings`, `start_turn`, `stop_task`, `stop_interrupt`, `stop_kill`, `child_exited` (new
field `reaped`), `thread_opened`, `shutdown` (new field `threads_closed`), `claude_resume_failed`
(now also from a respawn), `model_not_applied`. The timeout `warn` keeps its sentence ("Claude
Code did not answer a control request in time") with `action = <subtype>`; the late answer to a
timed-out request is `debug`.

## Verification

Before pushing each commit: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
--locked -- -D warnings`, `cargo test --workspace --locked`, `cargo deny check`.

Then, on a shell with a logged-in `claude`, a `claude-code` declaration with
`idle_shutdown_secs = 15` and a project:

1. Open a thread, send a message, wait for the answer. Within ~15 s of the answer
   `pgrep -f 'claude -p'` shows no process for it; the thread in the browser is unchanged (no
   warning, no "errored"). `RUST_LOG=giskard_harness_claude=debug` shows `idle` with
   `idle = true`, then `child_reaped`, then `child_exited` with `reaped = true` at `info`.
2. Send a second message: the answer refers to the first exchange (the transcript was resumed);
   the log shows `respawn` with `resume_fallback = false` and the `--resume` argv on the spawn
   line; no `Notice` in the transcript.
3. Delete that session's transcript under `~/.claude/projects/<cwd>/<uuid>.jsonl` while the
   thread is reaped, send a message: the turn runs, the transcript shows the "Agent context was
   lost" notice under the new message, the log shows `claude_resume_failed` and
   `resume_fallback = true`.
4. Run a command in the background (`sleep 120 &` through the agent) and let the turn end: the
   process is not reaped while the task runs (`idle = false`, `reason = "tasks"`); it is once the
   task completes.
5. Ask for a command needing approval under `ask_first` and leave the card unanswered past the
   timeout: not reaped; answer; reaped after the turn ends.
6. Set `idle_shutdown_secs = 0`, restart: the process stays past the timeout. Set it to `"x"`:
   startup refuses, naming `[harnesses.<name>]` and the key.
7. Open the MCP menu on a reaped thread: it answers (from the probe) and no `claude` process for
   the thread appears.
8. Archive a reaped thread: no process is spawned, the thread's stream ends cleanly (no `warn`).

## Acceptance

- `await_control`, `pump_until`, `stop_requested_while_waiting`, `AWAIT_POLL_SLICE`, `deferred`,
  `stop_request` and `Waiter::Raw` are gone; the per-turn settings are a `TurnSetup` the main loop
  advances; the stop sequence is a `Stopping` phase; `Stop`, shutdown, a dropped channel, a stage
  deadline and the idle timer are `select!` inputs of one loop, and the loop cannot spin after
  the channel closes or the shutdown flag is set.
- A command arriving during a stop sequence is answered at once.
- A child idle for `idle_shutdown_secs` is stopped (stdin closed, EOF, kill on the grace
  timeout) with its thread's log open and its entry kept; a child with a turn, a sub-agent
  route, an open task, an outstanding control request, an in-flight hand-off or a pending ask is
  not.
- The next `start_turn` / `compact_thread` on a reaped thread respawns with `--resume` on the
  same session id, with the open's bypass and missing-transcript fallbacks; the lost-transcript
  notice is an event after that turn's `TurnStarted`; a respawn that fails fails the turn and
  keeps the thread; a hand-off that lands in the reap window runs on the fresh child.
- `open_thread` on a reaped entry respawns eagerly and reports the window and the notice as a
  first open does; `interrupt` and `set_thread_name` on a reaped thread are `Ok` no-ops; delete,
  archive and shutdown close a reaped thread's log; an MCP status read never respawns.
- `idle_shutdown_secs` is typed on the `claude-code` declaration, `0` disables, absent is 600 s,
  a bad value or a Codex declaration carrying it refuses startup.
- The tests in Step 4 exist and pass; every existing test passes.
- The adapter README, the root README, `config.example.toml`, `docs/multi-harness-design.md`,
  `specs/giskard-specification.md` and this plan's parent (§5.2, §11, §12) say what the code
  does.
