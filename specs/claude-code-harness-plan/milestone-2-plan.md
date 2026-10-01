# Milestone 2 implementation plan: child supervisor and thread lifecycle

Implements milestone 2 of [`../claude-code-harness-plan.md`](../claude-code-harness-plan.md)
(§11). This plan is written for an implementing agent. Every file, symbol and behaviour below was
verified against `main` at `1ecc819` (milestone 1 merged), against Claude Code **2.1.286** driven
over a stdio pipe, and against the `claude-codes` **2.1.286** crate source. Line numbers are for
orientation; the symbol quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this milestone: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; nothing is dropped, coalesced or recovered from silently; every new async, process,
timeout, idempotent-close and lifecycle-cleanup path gets a focused test and a log line that
explains what happened; a long-lived keyed map gets an `ENTITY-AUTHORITY-EXCEPTION` comment; the
crate README is updated in the same change; Markdown prose is wrapped at 100 columns (table rows
may run longer).

## Outcome

After this milestone `giskard-harness-claude` exports `ClaudeHarness`, an `AgentHarness` that runs
**one `claude` process per primary thread**: `open_thread` spawns it (fresh or `--resume`, with the
same-id respawn when the transcript is gone), `subscribe` answers before the child has produced a
frame, `start_turn` writes the user message with inline attachments, `interrupt` sends the control
request, `set_thread_name` renames the session, `set_thread_archived(true)` and `delete_thread`
stop the child, `shutdown` stops every child after interrupting it, `list_models` answers from the
freshest handshake or from a probe child, and `list_providers` reports `anthropic`. Nothing is
user-reachable yet: no `HarnessKind` names the adapter until milestone 4, and approvals, server
requests, per-turn settings and `/compact` wait for milestone 3. The commit is one unit: the code,
its tests, the crate README, and the two plan amendments in Step 9.

## Scope

One commit, built in this order so that each step compiles on its own:

1. Manifest, capabilities and the new public surface.
2. The child I/O layer: argv, spawn, line reader, stderr tail, exit classification.
3. The supervisor task: one per child, owning the process, the mapper and the retained log.
4. The façade: `ClaudeHarness` and every `AgentHarness` method it implements.
5. User attachments as inline content blocks, with the encoded-size ceilings.
6. The model catalog and the provider report.
7. Two small mapper additions the supervisor needs.
8. Tests: an in-process scripted child for the protocol, a shell-script fake for the real process
   path.
9. Documentation: the crate README, and two amendments to the harness plan.

## Non-goals

No `HarnessKind`, no `config.example.toml` entry, no server change (milestone 4). No
`respond_approval` / `respond_server_request` bodies beyond recording what milestone 3 will answer,
no `set_permission_mode`, no `set_model` / effort read-back after open, no `/compact`
(milestone 3). No `claim_native_thread`, no `--forward-subagent-text` (milestone 5). No idle
reaping (milestone 6), no `mcp_status` (milestone 4, see Step 1), no version-drift warning
(milestone 8). No hook route. No change to the fixtures: every test input that is a CLI frame
comes from `tests/fixtures/`.

## Verified facts this milestone rests on

Each row was established in this session against Claude Code 2.1.286, with the fixtures' argv
(`tests/fixtures/README.md`), a scrubbed environment and a throwaway working directory. Rows marked
**[fixture]** are also in the committed fixtures.

| Fact | Consequence |
| --- | --- |
| Nothing is written to stdout at spawn. The first frame of a session is the `system/init` that answers the **first user message** **[fixture: `text-turn`]**; a child that only receives control requests emits only their responses **[fixture: `initialize`]** | `open_thread` cannot wait for `system/init`; the handshake is the `initialize` control response. `apiKeySource` and `claude_code_version` reach the adapter at the first turn, not at open |
| `initialize` answers in 0.5–1.7 s, with `models`, `current_permission_mode`, `account`, `pid`, `session_state: "idle"`, on a fresh **and** on a resumed session | The handshake is the same for both; the catalog is refreshed on every open |
| `--resume <unknown uuid>` with `initialize` written first: the child writes one `result` (`subtype: "error_during_execution"`, `is_error: true`, `num_turns: 0`, `errors: ["No conversation found with session ID: <uuid>"]`), prints the same sentence on stderr, and **exits 1 in about 1.5 s without answering `initialize`** **[fixture: `resume-missing`, after a user message]** | A missing transcript is detected at open, before any turn. The handshake races the control response against child exit |
| `--session-id <uuid of an existing transcript>` exits 1 in 0.2 s with stderr `Error: Session ID <uuid> is already in use.` and no stdout frame | The same-id respawn (plan §5.2) works **only because the transcript is gone**. A resume that fails for any other reason must not be retried with `--session-id`; it is an error |
| `--resume` from a different cwd than the recording works: `system/init.cwd` is the new cwd, and the conversation (a remembered word) is intact | cwd is always `OpenThreadOptions.workspace_root`; no cwd bookkeeping |
| `interrupt` while **idle** answers `{"still_queued": []}` with `subtype: "success"` and nothing else happens; mid-generation it answers the same, then a `user` frame and a `result` (`error_during_execution`, `terminal_reason: "aborted_streaming"`) arrive within 20 ms **[fixture: `cancel`, `delegation-interrupted`]** | `interrupt` with no active turn is `Ok` and cheap. The control response arrives **before** the result, so the waiter resolves first and the mapper closes the turn after |
| `rename_session {title, source: "host"}` answers `subtype: "success"` with no payload, then a `system/session_title_changed` frame | `set_thread_name` awaits the control response; the frame is already ignored by the mapper at `debug` |
| Closing stdin **mid-generation**: the CLI finishes the turn (a normal `result`) and exits 0. Closing it **while a Bash tool runs**: the tool completes, the turn completes, exit 0. Closing it while idle: exit 0 at once. After an `interrupt` ended the turn as an error, closing stdin exits **1** within 0.5 s | Stop = interrupt if a turn is live, then close stdin, then kill on a grace timeout. The exit code after a stop is not a health signal |
| `--session-id <fresh uuid>` and `--session-id` omitted both answer `initialize`; omitted, a throwaway `CLAUDE_CONFIG_DIR` gains machine state (`.claude.json`, `policy-limits.json`, `remote-settings.json`, a `sessions/` entry) and **no `projects/` directory** | The catalog probe passes no `--session-id` and leaves no transcript |
| `get_settings` answers `{applied: {model: "<resolved id>", effort: "<level>", …}, effective: {}, sources: []}`: with `--model sonnet --effort low` the applied model is `claude-sonnet-5-5` and effort `low` | The handshake can confirm the model the CLI applied, which `ThreadHandle.resumed_model` reports |
| `--effort high` with `--model haiku` (a model the catalog marks `supportsEffort: false`) is accepted silently; the turn runs | Passing `--effort` for a model that does not support it is harmless; the adapter passes what the `ModelRef` carries |
| `get_context_usage` answers at open, fresh and resumed, with `totalTokens`, `maxTokens` (200 000 for Haiku 4.5), `rawMaxTokens`, `autocompactSource`, `percentage` and a `categories` list | A resumed thread can report its context window through `ThreadUpdate::ContextWindowRestored` at open, the way Codex does |
| `--permission-mode manual` is reported as **`default`** everywhere: `initialize.current_permission_mode`, `system/init.permissionMode` and `system/status.permissionMode` **[fixture: every `manual` scenario]** | The adapter must never feed `set_expected_mode("manual")` to the mapper: the CLI's name for that mode is `default`. Milestone 3 owns the mode; this milestone does not call `set_expected_mode` |
| A second user message written while a turn is active is **queued** and run as a second turn after the first `result`, with its own `result` | `start_turn` on a thread whose mapper has an active turn is `ThreadBusy`; the adapter never queues |
| An invalid flag value (`--permission-mode bogus`) exits 1 in 0.1 s with the commander error on stderr and no stdout | Spawn-time failures surface as `HarnessError::Spawn` carrying the stderr tail |
| `--add-dir /nonexistent/dir` does not fail the spawn or the handshake | Not relied on; the adapter passes no `--add-dir` (the cwd is the workspace) |
| The unauthenticated shape could **not** be reproduced in this environment: a child with `PATH` only, a throwaway `HOME` and `CLAUDE_CONFIG_DIR` still completed a turn (the environment's proxy authenticates every request). Plan §7 says to fail the open with a message naming the fix | **[unverified]**: the adapter maps a handshake that fails with an exit whose stderr or `result.errors` mentions authentication to `HarnessError::Unauthenticated`, and everything else to `Spawn`. The match is a best-effort substring (`not logged in`, `Invalid API key`, `/login`, `authentication`); document it as such in the README |

## Step 1: manifest, capabilities and public surface

- `crates/giskard-harness-claude/Cargo.toml`: add `tokio = { workspace = true }`,
  `async-trait = { workspace = true }`, `futures = { workspace = true }`,
  `base64 = { workspace = true }` and `uuid = { version = "1", features = ["v4"] }`. `uuid 1.26.1`
  with `v4` is already in `Cargo.lock` through `claude-codes` (`Cargo.lock:2647`), so the lockfile
  gains no new crate; `base64 0.23` and `futures 0.3` are workspace dependencies already used by
  `giskard-server`. Dev-dependency: `tempfile = { workspace = true }` (`Cargo.toml:44`), for the
  shell-script fake's working directories. `cargo deny check` stays green: nothing new is added to
  the graph.
- `src/lib.rs`: new private modules `process`, `session`, `harness`, `attachments`, `catalog`;
  `pub use harness::{ClaudeHarness, ClaudeLaunchOptions}` and
  `pub use catalog::ANTHROPIC_PROVIDER_ID`. Keep `ClaudeMapper`, `MapperOutput`, `Frame`, `Route`,
  `TurnKind` exported as they are. The crate doc comment's "Milestone 1 of …" sentence becomes a
  milestone 2 sentence: the mapper plus a per-thread child supervisor; approvals, server requests
  and settings arrive in milestone 3.
- `capabilities()` (`src/lib.rs:22`): two flags change, each with a comment naming the milestone
  that flips it back: `context_compaction: false` (`compact_thread` is milestone 3, and the trait
  default returns `Unsupported`, so advertising it now would be a lie to the server) and
  `mcp_status: false` (`list_mcp_servers` is not wired; milestone 4 wires the `mcp_status` control
  request beside registration). Update the existing capability test accordingly. Everything else
  stays: `resumable_threads`, `model_listing`, `provider_listing`, `token_usage` true; the four
  milestone-3 flags false.
- `ClaudeHarness::capabilities` (the trait method) returns `capabilities()`.

## Step 2: the child I/O layer (`src/process.rs`)

This module owns everything about one operating-system process and nothing about the protocol.
Model it on `crates/giskard-harness-codex/src/transport.rs` (`StdioTransport::spawn` at line 104,
`drain_stderr` at 584, `shutdown_transport` at 263) but keep it smaller: Claude Code has no JSON-RPC
request layer, so there is no waiter table here, and no separate reader task is needed because the
supervisor (Step 3) reads the lines itself.

### Launch options and argv

```rust
/// How to launch this instance's children (the declaration's neutral keys plus logging context).
#[derive(Debug, Clone, Default)]
pub struct ClaudeLaunchOptions {
    /// Binary path or name. `None` is `claude` on `PATH`.
    pub command: Option<PathBuf>,
    /// Appended after the adapter's own arguments.
    pub args: Vec<String>,
    /// Applied on every child over the inherited environment.
    pub env: EnvOverlay,
    /// Only reported on log lines.
    pub project_id: Option<ProjectId>,
    /// The `[harnesses.<name>]` key this instance comes from. Only reported on log lines.
    pub declaration: Option<String>,
}
```

Mirror `CodexLaunchOptions` (`giskard-harness-codex/src/lib.rs:464`): milestone 4's `HarnessKind`
fills it from `HarnessDeclaration` (`giskard-persist/src/config.rs:34`) exactly as
`bin/giskard-server.rs:40-66` does for Codex. No kind-specific options exist yet; do not add a
`ClaudeDeclarationOptions` type until a key needs one.

`fn session_argv(options: &ClaudeLaunchOptions, session: &SessionArgs) -> Vec<String>` builds, in
this order, the plan §3.1 invocation as this milestone uses it:

```
-p --input-format stream-json --output-format stream-json --verbose
--permission-prompt-tool stdio
--setting-sources user
--disallowedTools EnterPlanMode ExitPlanMode
--include-partial-messages
--permission-mode manual
--model <model>
[--effort <level>]
(--session-id <uuid> | --resume <uuid>)
<declaration args…>
```

- `--setting-sources user` is the plan §8.3 decision. The fixtures were recorded with
  `--setting-sources ""`; that is the recorder's isolation, not the adapter's setting.
- `--include-partial-messages` is the choice milestone 1 deferred: with it the mapper emits
  `ItemDelta`s from `stream_event`s, so the UI streams text; without it an `AgentMessage` appears
  only when its `assistant` frame lands. Take it. The mapper handles both.
- `--permission-mode manual` is fixed in this milestone. Milestone 3 derives it from the turn's
  `TurnOverrides` and adds `set_permission_mode` per turn. **Do not pass `--permission-prompts
  none`**: it would turn every ask into a silent denial, and the point of recording pending asks
  now (Step 3) is that milestone 3 answers them.
- `--model` is `ModelRef.model` verbatim (an alias such as `sonnet` or a full id; both are catalog
  `value`s). `--effort` is `ModelRef.reasoning_effort` when present, verbatim; the catalog's
  `supportedEffortLevels` are what the UI offers, and the CLI tolerates a level the model ignores.
- `--forward-subagent-text` (milestone 5) and `--replay-user-messages` (never; the adapter already
  knows what it wrote) are absent. No `--add-dir`: the child's cwd is the workspace.
- The declaration's `args` come last so an operator can append, never override, the protocol flags.
- A probe child (Step 6) uses `probe_argv`: the same list without `--permission-mode`, `--model`,
  `--effort` and the session flag.

### Spawn

```rust
pub(crate) struct SpawnedChild { … }   // the real `ClaudeChild`

pub(crate) async fn spawn_child(
    options: &ClaudeLaunchOptions,
    argv: &[String],
    cwd: &Path,
    context: &ChildLogContext,   // project_id, harness, thread_id, harness_thread_id
) -> Result<SpawnedChild, HarnessError>
```

`tokio::process::Command::new(command.unwrap_or("claude"))`, `.args(argv)`,
`.envs(options.env.entries())` over the inherited environment (never `env_clear`: plan §7 names
the consequences and the mitigation is the `apiKeySource` notice milestone 1 already emits),
`.current_dir(cwd)`, stdin/stdout/stderr `piped()`, `.kill_on_drop(true)`. A spawn error is
`HarnessError::Spawn(format!("failed to start {command}: {error}"))`. Log at `info` with
`action = "spawn_claude"`, `project_id`, `harness`, `thread_id`, `harness_thread_id`, `command`,
`cwd`, `resume = <bool>`, `extra_args = <count>`, `env_names = ?options.env.names()` and, after
the spawn, `pid`. Names only, never values: the overlay and the extra args may carry credentials
(`giskard-harness-codex/src/lib.rs:573-588` is the model).

### The `ClaudeChild` trait

The supervisor and its tests talk to the process through one trait, the way `CodexTransport`
(`giskard-harness-codex/src/lib.rs:428`) lets `FakeCodexTransport` stand in for the app-server:

```rust
#[async_trait]
pub(crate) trait ClaudeChild: Send {
    /// Write one stdin line (the newline is appended here). Fails when stdin is closed.
    async fn write_line(&mut self, line: &str) -> Result<(), HarnessError>;
    /// The next stdout line without its newline; `None` at EOF.
    async fn next_line(&mut self) -> Result<Option<String>, HarnessError>;
    /// Close stdin so an idle CLI exits on its own. Idempotent.
    fn close_stdin(&mut self);
    /// Wait for exit after EOF and collect the stderr tail. Idempotent after the first `Ok`.
    async fn wait(&mut self) -> ChildExit;
    /// SIGKILL. Idempotent; a child that already exited is not an error.
    fn start_kill(&mut self);
    fn pid(&self) -> Option<u32>;
}

pub(crate) struct ChildExit {
    /// `None` when killed by a signal.
    pub code: Option<i32>,
    pub signal: Option<i32>,
    /// The last `STDERR_TAIL_LINES` lines, each cut at `STDERR_LINE_PREVIEW` chars.
    pub stderr_tail: Vec<String>,
}
```

`SpawnedChild` implements it over `BufWriter<ChildStdin>` (write, then flush, each call) and a
`BufReader<ChildStdout>` read with `read_until(b'\n')` into a reused buffer, capped at
`MAX_STDOUT_LINE_BYTES = 64 * 1024 * 1024`: a longer line is a `HarnessError::Protocol` naming the
byte count, and the supervisor treats it as fatal for that child (Step 3). A line that is not
UTF-8 is converted lossily and logged at `warn` with its byte count, never its content. `wait`
awaits `Child::wait` **and** the stderr drain task, so the tail is complete when the exit is
reported.

The stderr drain is a task spawned at `spawn_child`: it reads lines, strips ANSI (copy
`strip_ansi` from `giskard-harness-codex/src/transport.rs`), logs each at `debug` with
`target: "giskard_harness_claude::stderr"` and the log context, and keeps the last
`STDERR_TAIL_LINES = 8` lines (each cut to `STDERR_LINE_PREVIEW = 400` chars) in an
`Arc<Mutex<VecDeque<String>>>` the `ChildExit` is built from. stderr is the only place the CLI
explains a failed spawn or a failed resume (`No conversation found with session ID: …`,
`Error: Session ID … is already in use.`, the commander usage error), so the tail is what every
`Spawn` error and every unexpected-exit log line quotes.

### Exit classification

```rust
pub(crate) enum ExitKind {
    /// `No conversation found with session ID` on stderr or in `result.errors`.
    ResumeMissing,
    /// The best-effort authentication match (see the facts table).
    Unauthenticated,
    Other,
}
pub(crate) fn classify_exit(exit: &ChildExit, last_result_errors: &[String]) -> ExitKind
```

`last_result_errors` is the `errors` list of the `result` frame the child wrote before exiting, if
any. Only the handshake (Step 4) classifies an exit, and it reads that list off the raw line with
`Frame::parse`, because the mapper is not driven until the handshake has succeeded.

## Step 3: the supervisor (`src/session.rs`)

One `tokio` task per child, started by `open_thread` after the handshake succeeded. It is the
single owner of the `Box<dyn ClaudeChild>`, the `ClaudeMapper`, the pending control-request
waiters and the thread's `Arc<EventLog>`; nothing else touches them. This is the `CodexInstance`
rule from `AGENTS.md` applied per child: no `Arc<Mutex<_>>` around the mapper or the stdin handle,
and the façade reaches the task only through its command channel.

```rust
pub(crate) enum ChildCommand {
    StartTurn {
        line: String,
        turn: TurnId,
        model: ModelRef,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    Interrupt { reply: oneshot::Sender<Result<(), HarnessError>> },
    /// A control request whose success payload the caller wants: `rename_session` now,
    /// milestone 3's `set_permission_mode`, `set_model` and `get_settings` later.
    Control { request: Value, reply: oneshot::Sender<Result<Value, HarnessError>> },
    Stop { reply: oneshot::Sender<()> },
}

pub(crate) struct ChildHandle {
    pub harness_thread_id: String,
    pub log: Arc<EventLog>,
    pub commands: mpsc::Sender<ChildCommand>,
    pub task: JoinHandle<()>,
}
```

### The loop

`tokio::select!` over three sources, biased so that a frame already read is mapped before a new
command is accepted:

1. **A stdout line** (`child.next_line()`):
   - `Ok(Some(line))` → `mapper.map_line(&line)`, then dispatch each `MapperOutput`:
     - `Event(e)` → `log.append(e)`; a `false` return (log closed) is logged at `warn` once and
       the frame counted, never silently dropped.
     - `Reply(value)` → `child.write_line(&value.to_string())` (the mapper's own plan-mode
       denial). A write error is fatal for the child (below).
     - `ControlResponse { request_id, payload }` → resolve the waiter in
       `HashMap<String, oneshot::Sender<Result<Value, HarnessError>>>`. `payload` is the frame's
       `response` object: `subtype: "success"` resolves `Ok(payload["response"])` (may be absent,
       as for `rename_session`), `subtype: "error"` resolves `Err(HarnessError::Protocol(<the
       error string>))`. No waiter → `warn` with `request_id` and `action = "control_response"`.
     - `PendingApproval { id, request_id, tool_use_id }` and `PendingServerRequest { id,
       request_id }` → record in the façade's `pending` map (Step 4) keyed by the Giskard id,
       with the thread and the CLI `request_id`. Nothing answers them in this milestone; the
       README says so. The event the mapper emitted beside them is appended to the log as usual.
   - `Ok(None)` (EOF) → `child.wait().await`, then **child exit** handling below.
   - `Err(e)` (overlong line, read error) → log at `error` with `action = "read_stdout"`, then
     `child.start_kill()`, `child.wait().await`, and the same exit handling.
2. **A command**:
   - `StartTurn` → if `mapper.active_turn().is_some()` reply `ThreadBusy` (the façade checks too,
     but the task is the authority and the check is cheap); else `mapper.begin_turn(turn,
     TurnKind::User)` and `mapper.note_turn_model(model)`, append the outputs, then
     `write_line(line)`. A write failure fails the turn: `mapper.child_failed_turn(reason)`
     (Step 7) is appended, the reply carries the error, and the child is treated as broken (kill,
     wait, exit handling).
   - `Interrupt` → write the `interrupt` control request
     (`{"type":"control_request","request_id":<uuid>,"request":{"subtype":"interrupt"}}`),
     register the waiter, and only when `mapper.active_turn().is_some()` call
     `mapper.note_interrupt_sent()`. The reply resolves when the control response arrives or
     after `CONTROL_TIMEOUT = 10 s` (`HarnessError::Timeout`), whichever first; the timeout is
     armed by the façade around the oneshot so the loop never blocks on a waiter.
   - `Control { request }` → write the control request with a fresh `request_id`, register the
     waiter, reply when it resolves. `rename_session` uses it with
     `{"subtype":"rename_session","title":<name>,"source":"host"}`.
   - `Stop` → the stop sequence below, then reply and return from the task.
3. **Shutdown of the façade** (a `watch::Receiver<bool>` cloned into every task): same as `Stop`.

Waiters that are pending when the task returns are failed with
`HarnessError::Transport("claude child stopped")`.

### Child exit

Whether the CLI exited on its own, was killed after a read error, or finished after a `Stop`:

1. If `mapper.active_turn()` is `Some`, append `mapper.child_exited(&exit)` (Step 7): a
   `TurnCompleted` with `Failed` (`Interrupted` when an interrupt was sent) and a message naming
   the exit code or signal, so the server never has to synthesize the completion from a closed
   stream (it would, at `event_forwarder.rs:1209` `handle_stream_error`, but with less
   information).
2. `log.close()`. The server's forwarder sees `EventStreamError::Closed` and ends **that
   thread's** stream; siblings are untouched, which is the trait contract.
3. Remove the child from the façade's `children` map (the task holds a `Weak<ClaudeHarness>` or a
   cloned `Arc<Mutex<Children>>` for this one purpose; prefer the latter and keep the mutex
   `std::sync` since nothing is awaited under it). A later `open_thread` for the same thread
   respawns; a later `subscribe` gets `AgentEventStream::closed()`.
4. Log once, with `thread_id`, `harness_thread_id`, `pid`, `exit_code`, `signal`,
   `stderr_tail`, `requested = <bool>` (true after `Stop`/shutdown), `live_children = <n>`: at
   `info` when requested and the code is 0 or 1 after an interrupt, at `warn` otherwise. The
   plan's "log the live-child count" (§5.2) is this line and the spawn line.

### The stop sequence

`async fn stop(child, mapper, waiters, log)`:

1. If a turn is active: write `interrupt`, wait up to `STOP_INTERRUPT_GRACE = 5 s` for the mapper
   to close the turn (keep reading and dispatching frames meanwhile, so the `result` is mapped and
   the `TurnCompleted` reaches the log). Verified: the result follows the interrupt within tens
   of milliseconds. Log at `info` `action = "stop_interrupt"` with `elapsed_ms`.
2. `child.close_stdin()`, then keep reading until EOF or `STOP_EXIT_GRACE = 5 s`. Verified: an
   idle CLI exits within 0.5 s of EOF.
3. On the grace timeout, `child.start_kill()` and log at `warn` `action = "stop_kill"`.
4. `child.wait()`, then the child-exit handling above with `requested = true`.

SIGTERM is deliberately not used: plan §5.2 measured that it leaves the turn without a `result`.

## Step 4: the façade (`src/harness.rs`)

```rust
pub struct ClaudeHarness {
    workspace_root: PathBuf,
    launch: ClaudeLaunchOptions,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Reach each live child's supervisor task and retained log from the trait methods.
    // Source of truth: `open_thread` inserts an entry after the handshake; the supervisor removes
    //   it when the child exits.
    // Structural reason: The harness crate cannot depend on the server's thread authority.
    // Synchronization: A std mutex guards insert, lookup and removal; nothing awaits under it.
    // Invalidation/removal: Child exit, `delete_thread`, `set_thread_archived(true)` and
    //   `shutdown` remove entries; dropping the harness drops the map.
    children: Arc<Mutex<HashMap<ThreadId, ChildHandle>>>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Remember which thread and CLI request id a published approval or server request
    //   belongs to, for milestone 3's `respond_*`.
    // Source of truth: The supervisor records an entry when the mapper publishes the request.
    // Structural reason: The responses carry no thread (trait doc: ids are instance-unique).
    // Synchronization: A std mutex.
    // Invalidation/removal: Milestone 3 removes an entry when it is answered; the supervisor
    //   removes a thread's entries when its child exits; `shutdown` clears the map.
    pending: Arc<Mutex<PendingRequests>>,
    catalog: Arc<Mutex<Option<CatalogSnapshot>>>,
    /// Serializes probe children so concurrent `list_models` calls share one.
    probe: tokio::sync::Mutex<()>,
    shutdown_tx: watch::Sender<bool>,
    spawner: Arc<dyn ChildSpawner>,
}
```

`ChildSpawner` is `async fn spawn(&self, argv, cwd, context) -> Result<Box<dyn ClaudeChild>,
HarnessError>`; the production spawner wraps `spawn_child`, tests install a scripted one through
`ClaudeHarness::with_spawner` (`#[cfg(test)]`, or `#[doc(hidden)] pub` if an integration test
needs it). `ClaudeHarness::new(workspace_root, launch) -> Arc<Self>` is the constructor
milestone 4 will call; it spawns nothing: the first child is the first `open_thread` or the first
`list_models`.

`ApprovalId` and `ServerRequestId` are the CLI's `request_id`, a v4 UUID (`mapper.rs:1577-1640`),
so they are unique across children without a façade-side mint. Keep the pending map keyed by them
and note in the README that the uniqueness rests on the CLI minting UUIDs.

### `open_thread`

1. **Refuse `task:` ids.** `opts.resume` starting with `ids::TASK_ID_PREFIX` is
   `HarnessError::Unsupported("a Claude Code sub-agent thread has no session to resume")`: plan
   §5.3 says such an id must never reach `--resume`, and milestone 5 opens those through
   `claim_native_thread`. Any other `resume` must parse as a UUID (`uuid::Uuid::parse_str`), else
   `HarnessError::Protocol` naming the thread.
2. **Already open.** A live entry for `opts.thread` in `children` is returned as its existing
   handle (same `harness_thread_id`), logged at `debug`; do not spawn a second child for one
   thread. The registry only calls `open_thread` for a cold thread, so this is a guard, not a path.
3. **Session id.** Fresh: `Uuid::new_v4()` as `--session-id`. Resume: `--resume <id>`.
4. **Spawn and handshake** (`async fn handshake(child, expect_resume) -> Result<Handshake,
   HandshakeFailure>`), with `cwd = opts.workspace_root`:
   - write `{"type":"control_request","request_id":<uuid>,"request":{"subtype":"initialize"}}`.
     `claude_codes::ControlRequestPayload::Initialize(InitializeRequest { hooks: None })` inside
     `ClaudeInput::ControlRequest` serializes to exactly this; either form is fine, but the
     hand-built one keeps the handshake free of the crate's `UserMessage` sprawl.
   - read lines until the control response whose `request_id` matches (parse each line with
     `Frame::parse`; a `Frame::Result` seen here is remembered for its `errors`, every other
     frame is logged at `debug` and ignored: nothing else is expected before the response). EOF
     → `child.wait()` → `HandshakeFailure::Exited(ChildExit, result_errors)`. The whole step is
     under `INITIALIZE_TIMEOUT = 30 s`; on timeout kill, wait, and fail with
     `HarnessError::Timeout("claude did not answer initialize within 30 s")`.
   - the success payload is read with a local `#[derive(Deserialize)] struct InitializeReply {
     models: Vec<CatalogEntry>, current_permission_mode: Option<String>, pid: Option<u32> }`
     (unknown keys ignored; `claude-codes`' `InitializeResponse` in `messages.rs:166` is the
     SDK's own type with `session_id`/`version`/`capabilities` and does not match the CLI).
     Store `models` in the façade's `catalog` (Step 6).
   - write `{"subtype":"get_settings"}` the same way and read `applied.model` and
     `applied.effort`. Build `resumed_model`: `Some(opts.initial_model.clone())` when
     `applied.model` equals the requested `model` **or** the catalog's `resolvedModel` for it;
     otherwise `Some(ModelRef { provider: ANTHROPIC_PROVIDER_ID, model: applied.model,
     reasoning_effort: applied.effort })` and a `warn` with `action = "model_not_applied"`,
     `requested`, `applied`. The server compares `resumed_model` against its request
     (`ws.rs:1918`, `registry.rs:945`) and unwinds a provider switch the harness did not confirm,
     which is exactly the signal a mismatch should produce. A missing or unparsable
     `get_settings` reply is `None` plus a `warn`, never a failed open.
   - **resume only**: write `{"subtype":"get_context_usage"}`, read `maxTokens`; when it is a
     positive `u32`, `opts.updates.send(ThreadUpdate::ContextWindowRestored { model:
     resumed_model.unwrap_or(initial_model), context_window })`, ignoring `Full`/`Closed` at
     `debug`, and `mapper.note_context_window(window)` (the Step 7 addition, applied once the
     mapper is built in item 7 below) so the first `TurnUsageUpdated` carries a window. The registry's update forwarder
     (`registry.rs:540-575`) persists it as the resumed thread's window.
5. **Resume fallback.** A `HandshakeFailure::Exited` whose `classify_exit` is `ResumeMissing`
   **and** `expect_resume` is true: log at `warn` `action = "claude_resume_failed"` with
   `harness_thread_id` and the stderr tail, spawn again with `--session-id <the same uuid>`
   (`expect_resume = false`), and on success attach
   `warning = Some(HarnessNotice { code: "claude_resume_failed", message: "Agent context was
   lost; started a fresh Claude Code session. History is intact.", detail: Some(<the stderr
   sentence>) })` — the Codex wording at `giskard-harness-codex/src/instance.rs:313-320`, which
   the server shows as a non-fatal open warning (`ws.rs:1850`, `routes.rs:806`). The thread opens
   writable. If the respawn also fails, return its error: the second failure's stderr is the one
   to quote (`already in use` means the transcript exists and something else is wrong).
6. **Any other handshake failure** is `HarnessError::Unauthenticated` for `ExitKind::
   Unauthenticated`, else `HarnessError::Spawn(format!("claude exited with {status} before
   answering initialize: {stderr_tail joined by " | "}"))`. The `Display` of the error reaches the
   user as the WS error's `detail` (`ws.rs:133-170`), so the stderr sentence is the message.
7. **Register.** Create the `EventLog`, build the `ClaudeMapper::new(thread, session_id,
   workspace_root)`, spawn the supervisor task with the child, the mapper, the log and the
   command receiver, insert the `ChildHandle` into `children`, log `info` `action =
   "thread_opened"` with `live_children`. Return `ThreadHandle::opened(thread, session_id,
   workspace_root)` with `warning` and `resumed_model` set. `handle.thread` must equal
   `opts.thread` (the registry checks, `registry.rs:934`).

### `subscribe`

`children.lock()` → `AgentEventStream::new(entry.log.reader())`, else `AgentEventStream::closed()`
(the Codex shape, `giskard-harness-codex/src/lib.rs:1129`). Synchronous; the log exists from
`open_thread`'s return, so a reader created before the first frame sees everything.

### `start_turn`

1. Resolve the child by `thread.thread`; none → `HarnessError::ThreadNotFound`.
2. `overrides.model`: when `Some` and different from the model the thread was opened on (keep the
   open model in the `ChildHandle`), log at `warn` `action = "turn_model_override_ignored"` with
   both and proceed with the open model; `per_turn_model` is false until milestone 3, so the
   server should not send one. `overrides.mode` and `overrides.permission_preset` are logged at
   `debug` and otherwise ignored this milestone (milestone 3).
3. Build the stdin line with `attachments::user_message_line(text, &attachments)` (Step 5); its
   errors return unchanged.
4. `TurnId::new()`, send `StartTurn { line, turn, model }`, await the reply under
   `CONTROL_TIMEOUT`, return the id. The `TurnStarted` event is in the log before the line is
   written, so the server's forwarder sees the turn before its first frame.

### `interrupt`

Send `Interrupt`, await under `CONTROL_TIMEOUT`. A thread with no child is
`HarnessError::ThreadNotFound` (the registry only calls this for a loaded binding,
`registry.rs:1270-1285`). An idle child answers `Ok(())` after the verified `still_queued`
response.

### `set_thread_name`

A `task:` id or a thread with no live child → `Ok(())` and a `debug` line (plan §5.2: the
session title is cosmetic, Giskard keeps its own name; `registry.rs:1477` passes a detached handle
for a cold thread). Otherwise `Control { rename_session }` under `CONTROL_TIMEOUT`; the error
propagates.

### `set_thread_archived` and `delete_thread`

`archived == false` → `Ok(())`. `archived == true` and `delete_thread` → stop the child when
there is one (`Stop` on its channel, await the reply under `STOP_TIMEOUT = 15 s`, then await the
task), remove the thread's pending entries, `Ok(())`. No child (a cold thread: the registry calls
both with a detached handle, `registry.rs:1457`, `:1505`) → `Ok(())` at `debug`. `delete_thread`
does **not** touch `~/.claude` (plan §5.2). A `task:` id is `Ok(())` for both.

### `shutdown`

Idempotent: `shutdown_tx.send_replace(true)`; take every `ChildHandle` out of `children` under the
lock; `futures::future::join_all` over `Stop` + task await per child, each under `STOP_TIMEOUT`;
clear `pending`; log `info` `action = "shutdown"` with `children_stopped`. A second call finds no
children and returns `Ok(())`. After shutdown, `open_thread` and `list_models` fail with
`HarnessError::Transport("Claude Code harness is shut down")`. The registry wraps the call in
`HARNESS_SHUTDOWN_TIMEOUT = 15 s` (`registry.rs:148`), which is why `STOP_TIMEOUT` is 15 s and
the per-step graces inside the stop sequence sum to 10 s.

### Defaults kept

`claim_native_thread`, `steer_turn`, `compact_thread`, `terminate_command`, `list_mcp_servers`,
`reload_mcp_servers`, `start_mcp_oauth_login`, `discoveries` (closed) stay at the trait defaults.
`respond_approval` and `respond_server_request` (required methods) return
`HarnessError::Unsupported("Claude Code approvals are answered from milestone 3")`, with the
pending entry left in place so milestone 3's implementation finds it. `client_version` returns
`None` (milestone 8 reads `claude_code_version`).

## Step 5: user attachments (`src/attachments.rs`)

`pub(crate) fn user_message_line(text: &str, attachments: &[UserAttachment]) -> Result<String,
HarnessError>` builds the plan §3.6 message:

```json
{"type":"user","message":{"role":"user","content":[<attachment blocks…>,{"type":"text","text":"…"}]}}
```

Attachment blocks precede the text block (§3.6 point 1). Per attachment, by `mime_type`:

| `mime_type` | Block |
| --- | --- |
| `image/png`, `image/jpeg`, `image/gif`, `image/webp` | `{"type":"image","source":{"type":"base64","media_type":<mime>,"data":<base64>}}` |
| `application/pdf` | `{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":<base64>}}` |
| `text/*`, plus `application/json`, `application/xml`, `application/x-yaml`, `application/toml` and `application/javascript` (text under another label; the server has no such list of its own, `routes.rs:1189-1246` only validates the MIME syntax and, for `kind: Image`, that the bytes are one of the four image types above) | `{"type":"document","source":{"type":"text","media_type":"text/plain","data":<decoded UTF-8>}}` |
| anything else | `HarnessError::Unsupported(format!("attachment {name:?} ({mime}) is not supported by Claude Code; convert it to text or PDF"))` |

Rules:

- `data_base64` has every ASCII whitespace stripped before use (§3.6 point 2: the API rejects
  wrapped base64). Decode it with `base64::engine::general_purpose::STANDARD` to validate it
  (`Protocol` error naming the attachment on failure) and, for text, to get the string;
  non-UTF-8 text is `Unsupported` naming the attachment.
- The serialized line (`serde_json::to_string` of the whole object) is measured before it is
  written: `MAX_STDIN_LINE_BYTES = 10 * 1024 * 1024` (the CLI's own cap, §3.6). Over it →
  `HarnessError::Protocol(format!("message is {bytes} bytes encoded; Claude Code accepts at most
  10 MiB per message, attachments included"))`. Never truncate.
- `name` and `size` are not sent; log at `debug` per attachment `kind`, `mime_type`, `size`,
  never content.
- `text` is sent verbatim; an empty text with attachments is allowed (the API accepts a document
  with no text), and an empty text with no attachments is `Protocol("empty user message")`.

Add `attachment` as a stable log field name so milestone 4's docs can point at it.

## Step 6: the model catalog and the provider report (`src/catalog.rs`)

```rust
pub const ANTHROPIC_PROVIDER_ID: &str = "anthropic";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CatalogEntry {
    pub value: String,
    #[serde(rename = "resolvedModel")] pub resolved_model: Option<String>,
    #[serde(rename = "displayName")] pub display_name: Option<String>,
    #[serde(default, rename = "supportsEffort")] pub supports_effort: bool,
    #[serde(default, rename = "supportedEffortLevels")] pub supported_effort_levels: Vec<String>,
}

pub(crate) struct CatalogSnapshot {
    pub entries: Vec<CatalogEntry>,
    pub taken_at: Instant,
    /// `"handshake"` or `"probe"`, for the log line.
    pub source: &'static str,
}
```

Deserialize entries one by one (`Vec<Value>` then each `CatalogEntry`), skipping an entry that
does not parse with a `warn` naming its index, so one odd entry cannot empty the picker.

`fn descriptors(snapshot) -> Vec<ModelDescriptor>`: one per entry **except `value == "default"`**
(it is an alias of another entry, and a picker row named "default" would not survive a CLI
upgrade that re-points it). Fields: `provider = ANTHROPIC_PROVIDER_ID`, `model = value` (the
selector `--model` takes), `display_name`, `supports_reasoning_effort = supports_effort`,
`reasoning_efforts = supported_effort_levels`, `context_window =
ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW` (the catalog carries none, plan §6; the runtime
window arrives through `TurnUsageUpdated` and, on resume, `ContextWindowRestored`), `is_default`
on the **first** entry whose `resolved_model` equals the `default` entry's `resolved_model` (in
this environment `opus`; the plan's probe saw `sonnet` — it is the user's configuration, not a
constant). The server merges these at `models.rs:188` `apply_harness_metadata`; a non-empty
`provider` is what gets an entry appended to the picker.

`list_models`:

1. A snapshot exists → its descriptors. Every handshake replaces the snapshot, so a live child's
   catalog is never older than its open; the plan's "freshest `initialize.models` any live child
   reported".
2. None → take the `probe` mutex (a waiter that finds a snapshot afterwards uses it), spawn a
   probe child with `probe_argv` in `workspace_root`, write `initialize`, read the response under
   `PROBE_TIMEOUT = 30 s`, `close_stdin`, `wait` (verified exit 0 within 2 s), store the snapshot
   with `source = "probe"`, log `info` `action = "catalog_probe"` with `elapsed_ms` and
   `models = <count>`. Failure → the handshake's error (`Spawn`/`Timeout`); the server degrades
   to a warning (`routes.rs:4341`). The probe does not register in `children` and does not count
   as a live child.

`list_providers` → `vec![HarnessProvider { id: ANTHROPIC_PROVIDER_ID.into(), name: Some("Anthropic
(Claude Code)".into()), base_url: None, auth: None, http_headers: ProviderHttpHeaders::default(),
env: self.launch.env.clone() }]`. `base_url: None` keeps `/v1/models` discovery off (plan §7.1).

## Step 7: mapper additions (`src/mapper.rs`)

Two public methods, both tiny, both tested:

- `pub fn child_exited(&mut self, exit: &str) -> Vec<MapperOutput>`: if a turn is active,
  `finish_turn` with `Interrupted` when `interrupt_sent`, else `Failed`, message
  `format!("Claude Code exited ({exit}) before the turn completed")`, logged at `warn`
  `action = "child_exited"` with the turn id; no turn → empty, `debug`. The supervisor formats
  `exit` as `code 1` / `signal 9`.
- `pub fn note_context_window(&mut self, window: u32)`: seeds `session.model_usage_window` when
  it is `None` (the resumed thread's `get_context_usage.maxTokens`); `autocompact_state` and a
  later `result.modelUsage` still take precedence through `context_window()` (`mapper.rs:260`).

## Step 8: tests

Two layers, mirroring how the Codex crate tests its façade against `FakeCodexTransport` and its
transport against real `sh` children (`transport.rs:1153`).

### `ScriptedChild` (in `session.rs` tests, reused by `harness.rs` tests)

An in-process `ClaudeChild` driven by a script of steps:

```rust
enum Step {
    /// Wait for a stdin line matching the predicate (e.g. subtype == "initialize", a user text
    /// containing "pong"), then run the actions.
    OnStdin(Box<dyn Fn(&Value) -> bool + Send>, Vec<Action>),
}
enum Action {
    /// Emit these stdout lines (fixture frames, or a control response templated with the
    /// request's `request_id`).
    Emit(Vec<String>),
    EmitFixture { name: &'static str, skip_types: &'static [&'static str] },
    /// Answer the pending control request with this payload.
    Respond(Value),
    Exit { code: i32, stderr: Vec<String> },
}
```

`next_line` yields from an internal queue and parks until an action fills it; `close_stdin`
records the EOF (so a script can `Exit` on it); `write_line` records every line for assertions;
`wait` returns the scripted exit. A fixture emit replays `tests/fixtures/<name>.out.jsonl` through
the existing `fixture` helper (`mapper.rs:1920`), dropping `control_response` lines (those belong
to the scripted replies) so each test's frames are the CLI's own.

Tests, each named for its invariant:

1. `open_thread_handshakes_and_returns_a_subscribable_handle`: initialize answered from the
   `initialize` fixture's first line (its `models` populate the catalog), `get_settings` answered
   with the probed shape; `subscribe` before any frame yields a reader; the handle carries the
   minted UUID and `resumed_model == Some(initial_model)`.
2. `open_thread_reports_the_applied_model_when_it_differs`: `get_settings.applied.model` is
   another id → `resumed_model` is that id and the `model_not_applied` warning is logged.
3. `a_text_turn_streams_to_the_log_and_completes`: `start_turn` writes a line whose content is a
   single text block; replaying `text-turn` yields `TurnStarted`, deltas, `TurnUsageUpdated`,
   `TurnCompleted { Completed }` in the log, and `TurnUsageUpdated.model` is the open model.
4. `start_turn_while_a_turn_is_active_is_thread_busy` (the queued-message fact).
5. `interrupt_resolves_on_the_control_response_and_the_turn_ends_interrupted`: `cancel` fixture
   frames after the interrupt; the written line is an `interrupt` control request.
6. `interrupt_while_idle_is_ok`.
7. `a_resume_that_finds_no_transcript_respawns_with_the_same_id`: first spawn gets `--resume`,
   exits 1 with the `resume-missing` fixture's result line and stderr; second spawn has
   `--session-id <same uuid>` and handshakes; the handle has `warning.code ==
   "claude_resume_failed"` and the stderr sentence as `detail`.
8. `a_resume_failure_that_is_not_a_missing_transcript_is_an_error`: stderr `Error: Session ID …
   is already in use.` → `Spawn` whose message contains it, and no second spawn.
9. `a_second_failed_respawn_returns_the_second_error`.
10. `a_task_id_is_never_resumed`.
11. `a_resumed_thread_restores_its_context_window`: `get_context_usage` answered with
    `maxTokens: 200000` → the `ThreadUpdateStream` receives `ContextWindowRestored { model,
    200000 }` and the first `TurnUsageUpdated` of the next turn carries `context_window ==
    Some(200000)`.
12. `child_exit_mid_turn_fails_the_turn_and_closes_the_stream`: `Exit { code: 3 }` after half of
    `text-turn` → `TurnCompleted { Failed, "… (code 3) …" }` then `EventStreamError::Closed`, the
    child is gone from `children`, and the `warn` line names the exit code and stderr tail.
13. `a_closed_log_is_reported_not_ignored` (`append` returning false is logged once).
14. `set_thread_name_renames_a_live_session_and_is_a_no_op_without_one`.
15. `archiving_and_deleting_stop_the_child`: `Stop` sequence observed as `interrupt` (when a turn
    is scripted active), then EOF, then exit; `set_thread_archived(false)` writes nothing.
16. `stop_kills_a_child_that_ignores_eof`: the script never exits on EOF; the kill path runs
    within the grace and is logged.
17. `shutdown_stops_every_child_and_is_idempotent`; `open_thread` after shutdown fails.
18. `a_pending_ask_is_recorded_and_respond_approval_is_unsupported` (`tool-allowed` frames up to
    the ask).
19. `an_overlong_stdout_line_is_fatal_for_that_child_only`: two children; one yields a 65 MiB
    line; only its log closes.
20. `the_handshake_times_out`: no response → `Timeout`, the child is killed (`start_kill`
    observed).
21. `list_models_uses_the_freshest_handshake_then_probes`: before any open, the probe spawner is
    invoked with `probe_argv` (no `--session-id`, no `--model`) and the catalog comes from the
    `initialize` fixture; after an open with a different `models` list, that list wins; the
    `default` alias is absent and `is_default` lands on `opus`.
22. `list_models_shares_one_probe_between_concurrent_callers`.
23. `list_providers_reports_anthropic_with_the_overlay`.
24. `session_argv_is_the_plan_invocation` (exact vector, fresh and resume, with and without
    effort, declaration args last).
25. `attachments`: image → image block before the text block; PDF → base64 document; `text/plain`
    → text-source document with decoded content; wrapped base64 is unwrapped; an `.xlsx` is
    `Unsupported` naming it; a 10 MiB-plus line is `Protocol` naming the size and nothing is
    written; empty text with no attachment is rejected.
26. `child_exited_and_note_context_window` mapper unit tests (Step 7).

Capture logs with `tracing_subscriber` as the existing mapper tests do, and assert on the stable
fields named in *Logging* rather than on message text where the field exists.

### `tests/fake-claude.sh` and the real process path

A committed POSIX `sh` script (executable bit set in git) that the real `spawn_child` runs, so the
`tokio::process` path, the stderr tail, the exit codes and the kill are exercised on a real
child. It needs only `sh`, `sed` and `cat`. Behaviour by argv and stdin:

- `--resume` in `$*` → print `No conversation found with session ID: <the id>` to stderr, print
  the `resume-missing` fixture's result line, `exit 1`.
- `--permission-mode bogus` in `$*` → the commander error on stderr, `exit 1`.
- otherwise read stdin line by line: a line containing `"subtype":"initialize"` → print the
  `initialize` fixture's first line with its `request_id` replaced by the request's (extract with
  `sed -n 's/.*"request_id":"\([^"]*\)".*/\1/p'`; the adapter writes compact JSON so the pattern
  holds); `"subtype":"get_settings"` → a canned success with `applied.model` = the `--model`
  value; `"subtype":"interrupt"` → `{"still_queued":[]}`; a `"type":"user"` line → print the
  `text-turn` fixture's frames except `control_response` lines; `FAKE_CLAUDE_EXIT_MID_TURN=<n>`
  set → print only the first `n` of them and `exit 3`; `FAKE_CLAUDE_IGNORE_EOF=1` → on EOF
  `sleep 30` instead of exiting; otherwise exit 0 on EOF.

Tests (`#[tokio::test]`, in `process.rs` or a `tests/process.rs` integration test using the
`doc(hidden)` constructor), with the script path from `env!("CARGO_MANIFEST_DIR")`:

- `a_real_child_handshakes_and_completes_a_turn`;
- `a_missing_binary_is_a_spawn_error` (`command = "giskard-no-such-claude"`);
- `a_real_resume_failure_respawns_and_warns` (the script fails `--resume` and then serves the
  `--session-id` respawn);
- `a_real_child_that_exits_mid_turn_fails_the_turn_with_its_exit_code`;
- `stop_kills_a_real_child_that_ignores_eof` within the grace, asserting elapsed time;
- `stderr_is_drained_and_its_tail_is_bounded` (the script prints 50 stderr lines of 1000 chars;
  the tail has 8 lines of at most 400 chars).

If the CI runner lacked `sh` these tests would fail loudly rather than skip; it does not.

## Step 9: documentation

### Crate README (`crates/giskard-harness-claude/README.md`)

- The **Status** paragraph becomes milestone 2: one child per primary thread, what is reachable,
  what milestone 3 and 4 add; `capabilities()` now reports `context_compaction` and `mcp_status`
  false, with the milestones that flip them.
- **Runtime ownership** is written (it is a placeholder today): one supervisor task per child
  owning the process, the mapper, the waiters and the retained log; the façade's two maps and
  their lifetimes; the command channel; the probe child.
- New **Launch** section: the argv table from Step 2, the cwd rule, the environment overlay and
  the plan §7 warning about inherited `ANTHROPIC_*` variables, the declaration's `args` placement.
- New **Handshake, resume and respawn** section: the `initialize` + `get_settings` (+
  `get_context_usage` on resume) sequence, the two timeouts, the resume-missing classification,
  the `claude_resume_failed` notice, the `already in use` boundary, the `task:` refusal, what
  `resumed_model` means.
- **Process control, resume and approval responses** becomes **Process control**: the stop
  sequence with its graces, what `delete_thread` / `set_thread_archived` / `set_thread_name` /
  `shutdown` do, the child-exit handling, the live-child log line, and that pending asks are
  recorded but unanswered until milestone 3.
- New **User attachments** section with the Step 5 table and the ceiling.
- New **Model catalog (`initialize.models`)** and **Provider table** sections (Step 6), including
  the `default` alias rule and that the context window is conservative until runtime reports it.
- **Code and tests** lists the new modules and `tests/fake-claude.sh`.

Keep the identifier model and mapping sections untouched; add `ApprovalId` uniqueness resting on
the CLI's UUIDs to the identifier table's last row.

### Plan amendments (`specs/claude-code-harness-plan.md`)

The facts this milestone verified are already in the plan (the same commit that added this
document amended §3.3's `initialize` row, §5.2's resume-fallback bullet and §11's milestone 2 and
4 paragraphs). The implementing commit adds one sentence to the §11 milestone 2 paragraph, as
milestone 1 did: "Milestone 2 is implemented in `crates/giskard-harness-claude` (`ClaudeHarness`,
`ClaudeLaunchOptions`)." Nothing else in the plan changes.

`AGENTS.md`, the root `README.md`, `config.example.toml` and `docs/api-endpoints.md` do not
change: the crate list is already current and nothing is reachable from configuration or HTTP.

## Logging

Stable fields on every line the supervisor and façade write, where known: `project_id`, `harness`
(the declaration name), `thread_id`, `harness_thread_id`, `turn_id`, `pid`, `action`,
`request_id`, `elapsed_ms`, `exit_code`, `signal`, `live_children`. Actions introduced here:
`spawn_claude`, `handshake`, `claude_resume_failed`, `model_not_applied`, `thread_opened`,
`start_turn`, `turn_model_override_ignored`, `interrupt`, `control_request`, `control_response`,
`read_stdout`, `child_exited`, `stop_interrupt`, `stop_kill`, `thread_stopped`, `shutdown`,
`catalog_probe`, `attachment`. Levels: expected operator-visible outcomes (`spawn`, `thread_opened`,
`thread_stopped`, `catalog_probe`, `shutdown`) at `info`; a resume fallback, an unapplied model,
a kill, an unexpected exit, an unanswered waiter, a closed-log drop at `warn`; a read error at
`error`; stderr lines and ignored overrides at `debug`. No line carries a frame's content, a
prompt, an attachment, or an environment value; the stderr tail is bounded and is the one place a
CLI sentence is quoted.

## Verification

In order, before the commit:

1. `cargo fmt --all --check`, then `cargo clippy --workspace --all-targets --locked -- -D warnings`.
2. `cargo test -p giskard-harness-claude`, then `cargo test --workspace --locked`.
3. `cargo deny check advisories bans licenses sources`.
4. `grep -rn "unwrap()\|expect(\|panic!\|todo!\|unreachable!" crates/giskard-harness-claude/src`
   matches only inside `#[cfg(test)]` modules.
5. `git ls-files -s crates/giskard-harness-claude/tests/fake-claude.sh` shows mode `100755`.
6. With a real CLI on `PATH` (not in CI): a throwaway `main` that builds `ClaudeHarness::new`,
   opens a fresh thread, runs one text turn, interrupts a long one, renames, archives, and shuts
   down, watching the log for the `live_children` line reaching 0. Optional; the fake covers the
   protocol, this covers the binary.

## Acceptance

- One `claude` process per opened thread; a second `open_thread` for the same thread returns the
  same handle, and a child's exit closes only that thread's stream.
- `subscribe` returns a live reader for any handle `open_thread` issued, before the child has
  written a frame.
- A resume whose transcript is gone opens writable with `claude_resume_failed`; any other failed
  open is an error whose message quotes the CLI's stderr.
- `start_turn` on a busy thread is `ThreadBusy`; a user message is never queued behind another.
- Attachments go inline in the API's block shapes, a text attachment as decoded text, and a
  message over 10 MiB encoded is refused by size, never truncated.
- `shutdown` interrupts live turns before stopping their children, kills a child that ignores
  EOF within the grace, and is idempotent.
- `list_models` answers before any thread exists through a probe that leaves no transcript, and
  from the freshest handshake afterwards; `list_providers` names `anthropic`.
- `capabilities()` claims nothing the adapter cannot do in this milestone.
- Every timeout, exit path and fallback has a test and a log line naming what happened.
