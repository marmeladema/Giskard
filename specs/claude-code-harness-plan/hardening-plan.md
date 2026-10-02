# Hardening plan: issues found in real use after milestone 6

Fixes for the Claude Code harness found by using it, between milestone 6 and milestone 7 of
[`../claude-code-harness-plan.md`](../claude-code-harness-plan.md) (§11). This plan is written for
an implementing agent and grows one numbered section per issue; each section is one commit with its
own fixture or test. Every file, symbol and behaviour below was verified against `main` at
`eeefd7c` (milestone 6 merged) and against Claude Code **2.1.287**. Line numbers are for
orientation; the symbol quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this plan: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; a changed failure mode gets focused tests and logs that explain what happened; log
assertions use `#[traced_test]` with `logs_contain` / `logs_assert`, never a scoped subscriber;
the adapter README is kept in sync with identifier mappings, lifecycle and protocol routing;
Markdown prose is wrapped at 100 columns (table rows may run longer).

## 1. A turn's prompt is never acknowledged

### Symptom

The user's message stays greyed out for the whole turn. Switching to another thread and back
while the turn runs shows the agent's output with no prompt above it, as if the agent had started
on its own. Once the turn has ended, the prompt is back and no longer greyed.

### Cause, verified

Three pieces of existing behaviour line up:

| Where | What it does | Reference |
| --- | --- | --- |
| Browser | On send, the composer adds a `.user.pending` row. The only thing that removes `pending` is an item of kind `user_message` arriving in the live turn whose text equals what was sent (or an attachment row, matched by `preservesUserInputDisplay`). | `static/app.js:10036` (the row), `:7750` (the match) |
| Server | For a turn the user started, the live snapshot carries no `user_input`: `live_turn_user_input` returns it only for external turns, because the harness is expected to echo the prompt as an item. After completion the persisted record has `user_input`, and `renderPersistedTurn` draws it when the turn has no user item. | `registry.rs:135`, `app.js:4428` |
| Spec contract | "A successful provider `UserMessage` item is the ordered acceptance evidence." Codex meets it by echoing every prompt as a `userMessage` thread item. | `giskard-specification.md:3985` |
| Claude adapter | The CLI echoes nothing for a prompt unless `--replay-user-messages` is passed, and `session_argv` does not pass it (the README says so under *Launch*). The mapper emits a `UserMessage` only for a sub-agent's delegated prompt (`take_delegated_prompt`); a text block on the primary route is an `Activity`, which is right for the CLI's own interjections. | `process.rs:122` (`session_argv`), `mapper.rs:2341` (`on_user`) |

So the browser waits for an item the adapter never produces, and the live snapshot has nothing to
show in its place.

### Verified facts about `--replay-user-messages` (2.1.287)

The CLI documents the flag as "Re-emit user messages from stdin back on stdout for
acknowledgment", which is the case here. Recorded in the three `replay-*` fixtures that ship with
this plan (`crates/giskard-harness-claude/tests/fixtures/replay-{ack,image,compact}.*`, sanitized
like every other recording).

| Fact | Consequence |
| --- | --- |
| The replayed frame is `{"type":"user","message":{"role":"user","content":[…]},"isReplay":true,"uuid":…,"session_id":…,"parent_tool_use_id":null}`. `claude-codes` maps `isReplay` to `UserMessage.is_replay` (`#[serde(rename = "isReplay")]`, `message_types.rs:1087`) | No crate change. The mapper already reads `is_replay` |
| It arrives after the turn's `system/init` and in the same instant as the first `assistant` frame: `replay-ack` lines 2–3 and 8–9, both turns. Timing from the live probes: `init` 20 ms–1 s after the write, the replay ~0.8 s later | The acknowledgement lands as the answer starts. That is the CLI's definition of acknowledged; nothing else needs to be synthesized earlier |
| The content is echoed verbatim, image blocks included, in the order the adapter wrote them (attachments first, then the text block: `user_message_line`, `attachments.rs:27`): `replay-image` line 2 is `[image, text]` | The mapper takes the text block(s) and ignores the rest; the browser's attachment path matches the row. A replayed message is at most the adapter's own 10 MiB stdin line, under the 64 MiB stdout line cap (`MAX_STDOUT_LINE_BYTES`) |
| `tool_result` user frames, which the CLI writes itself, are not replayed: `replay-ack` line 11 has no `isReplay` | No double completion of tool calls. `on_user`'s tool-result path is untouched |
| A `/compact` line is not replayed as such. What comes back is what the CLI already emits **without** the flag: the compaction summary (`isSynthetic: true, isReplay: false`) and the local command's stdout `<local-command-stdout>Compacted </local-command-stdout>` with `isReplay: true` (`replay-compact` lines 11–12; the existing `compact` fixture lines 10–11 are identical in shape) | `is_replay` already appears in a compaction turn today, and the mapper skips it as bookkeeping. The new rule distinguishes by turn kind: a replay in a `User` turn is the prompt; in a `Compaction` turn it is the command's stdout |
| A `--resume` with the flag replays nothing at startup; only the new message comes back, after that turn's `init` (live probe on the recorded session) | Safe on every respawn; no flood of "user frame with no active turn" warnings |
| The replayed frame has no `isSynthetic`; the compaction summary has `isSynthetic: true` and `isReplay: false` | The two flags are independent and the mapper must test them separately, not as one `bookkeeping` bit |
| **The flag also echoes every `control_response` line the adapter writes** (its answer to a `can_use_tool` or other ask) back on stdout, verbatim, with no marker: `background-stop` line 9 and `background-complete` line 9 are the recorder's own `{"behavior":"allow"}` answers; the same probe without the flag echoes nothing. A stdin `control_request` is not echoed, and its reply still arrives (`background-stop` line 19 is the `stop_task` reply; `initialize`, `get_settings` and `set_permission_mode` were answered in a live probe with the flag on) | Without handling, every answered ask would log `warn` "control response for a request nobody is waiting on" (`session.rs`, the `ControlResponse` arm of `dispatch`). The supervisor must recognise the echo of its own answer (Step 2b) |

### Step 1: the flag (`crates/giskard-harness-claude/src/process.rs`)

`session_argv` (`:122`) adds `"--replay-user-messages"` after the launch mode. It is a session
flag, not a protocol flag: `protocol_argv` and the probe stay as they are, since a probe never
writes a `user` line. The real-process tests see the flag in the argv `fake-claude.sh` receives
(Step 3).

### Step 2: the mapper (`crates/giskard-harness-claude/src/mapper.rs`)

- `TurnState` (`:286`) gains `prompt_acknowledged: bool` (turn-lifetime: dropped with the turn),
  `false` from `TurnState::new`.
- `on_user` (`:2341`) replaces the single `bookkeeping` bit with two: `synthetic =
  message.is_synthetic == Some(true)` and `replay = message.is_replay == Some(true)`. Before the
  block loop, when `replay && !synthetic` and the route is `Primary` and no block is a
  `tool_result`:
  - turn kind `User`, `prompt_acknowledged == false`: emit the `UserMessage` item through the
    existing `user_message` helper (`:2420`) with `text` = the text blocks' text joined by `"\n"`
    (the adapter writes one; the join is defensive and an attachments-only message yields `""`),
    `harness_item_id` = `user:<uuid>:0` as the helper's callers already do, then set
    `prompt_acknowledged = true` and log at `debug` (`action = "prompt_acknowledged"`, `turn_id`,
    `frame_uuid`, `bytes`). Return: no block of this frame is mapped further (an image block would
    otherwise reach the "skipping a user block" `debug`).
  - turn kind `User`, already acknowledged: `warn` (`action = "prompt_acknowledged"`, "a second
    replayed user message in one turn; dropping it") and return. The adapter writes exactly one
    user line per turn, so this is a protocol surprise worth seeing.
  - turn kind `Compaction`: `debug` (`action = "compaction_replay"`) and return. This is the
    `<local-command-stdout>` frame; it was skipped before and stays skipped.
- Everything else is unchanged: no active turn keeps today's `warn`; a synthetic frame's text is
  skipped; a non-replay text block on the primary route is still an `Activity` (`cancel` fixture,
  "[Request interrupted by user for tool use]"); sub-agent routes are untouched, since a forwarded
  frame is never a replay of stdin and `take_delegated_prompt` keeps its path.

The façade does not change: `TurnStarted` is still appended before the line is written, and the
item follows from the stream like any other.

### Step 2b: the supervisor ignores the echo of its own answers (`session.rs`)

With the flag the CLI echoes each `control_response` the adapter writes (facts table). The
mapper turns such a line into `MapperOutput::ControlResponse { request_id, payload }` like any
other, and today the `dispatch` arm finds no waiter and warns. So:

- `Supervisor` gains `answered_asks: HashSet<String>` (an `ENTITY-AUTHORITY-EXCEPTION` beside
  `withdrawn`, same lifetime: cleared where `withdrawn` is, at the next turn's start). Every
  place the supervisor writes an answer records the ask's `request_id` first: `respond_approval`,
  `respond_server_request`, and the `MapperOutput::Reply` arm of `dispatch` (the mapper's own
  `ExitPlanMode` deny; read `response.request_id` from the value).
- In the `ControlResponse` arm, after the `TurnSetup` check and before `waiters`: a `request_id`
  found in `answered_asks` is removed and logged at `debug` (`action = "control_response"`,
  `echo = true`, "the CLI echoed the adapter's own answer; ignored"). The order matters: the
  adapter's own request ids are fresh UUIDs and never collide with the CLI's ask ids, but checking
  the echo set first keeps the `abandoned` and "nobody is waiting" paths meaning what they say.
- The echo arrives within milliseconds of the write (2 ms in the recordings), so clearing the set
  at the next turn start cannot drop a live entry.

### Step 3: tests

Mapper tests, with the existing helpers (`run_fixture`, `drive`, `completed_items`, `events`,
`on_thread`):

1. `a_replayed_prompt_is_the_turns_user_message`, on `replay-ack`: two turns; in each, exactly
   one `ItemCompleted` with `ItemPayload::UserMessage`, its text equal to the recorded prompt
   (`in.jsonl` line 1 and 2), its `harness_item_id` starting with `user:`, and its position after
   that turn's `TurnStarted` and before the turn's first `AgentMessage` or `ToolCall` item; the
   `tool_result` frame of the second turn (line 11) completes the `Bash` tool call and adds no
   `UserMessage`.
2. `an_attachment_replay_yields_the_text_only`, on `replay-image`: one `UserMessage` with text
   `Say pong.`; no item for the image block; no "skipping a user block" line (`#[traced_test]`,
   `logs_assert` with `no_line_with`).
3. `a_compaction_replay_is_not_a_user_message`, on `replay-compact` driven with
   `TurnKind::Compaction` for the last turn: the first turn has its `UserMessage`; the compaction
   turn's completed items are exactly the one `Context compacted` activity that
   `compaction_emits_an_activity_and_no_agent_message` already checks on `compact`, and the
   `compaction_replay` `debug` line is logged once.
4. `a_second_replay_in_a_turn_is_dropped`: synthetic lines (a `replay-ack` turn with its replay
   frame emitted twice): one `UserMessage`, the `warn` logged once.
5. The existing `cancel` and `compact` fixture tests pass unchanged, which proves that the
   interjection and the compaction frames keep their mapping.

6. `the_echo_of_an_answered_ask_is_ignored` (façade, scripted child): the `tool-allowed` ask
   answered through `respond_approval`; the script then `Emit`s the answer line back (as the CLI
   does; `EmitFixture` strips `control_response` lines on purpose, so this one is explicit); the
   turn completes and `logs_assert` finds the `echo = true` `debug` line and no "nobody is waiting"
   `warn`. The same for an `AskUserQuestion` answered through `respond_server_request`, and for
   the mapper's own `ExitPlanMode` deny (`plan-exit-denied`).

Façade test (`harness.rs`, scripted child): `a_turn_is_acknowledged_by_its_replay`: the script's
`OnStdin(user(), …)` emits the replayed user frame (the stdin line with `"isReplay": true` and a
`uuid` added) before the `text-turn` frames; the subscribed stream shows `TurnStarted`, then
`ItemStarted` and `ItemCompleted` of kind `UserMessage` with the sent text, then the turn's
events, then `TurnCompleted`.

`tests/fake-claude.sh`: when `--replay-user-messages` is in its argv (always, from Step 1), it
echoes each stdin `user` line back before replaying that turn's frames, with `"isReplay":true`
inserted after the opening brace (a `sed` on the line; no JSON parsing needed) and a fixed
`"uuid"`, and echoes each stdin `control_response` line back verbatim, as the CLI does.
`a_real_child_handshakes_and_completes_a_turn` then asserts the `UserMessage` item on the stream,
so the real process path (spawn, argv, stdin, stdout) covers the acknowledgement.

No server, browser or end-to-end test changes: the Playwright suite runs against the replay
harness, and the browser's un-grey path is the one Codex already exercises.

### Step 4: documentation

- `crates/giskard-harness-claude/README.md`: *Launch* lists the flag with the others and the
  sentence "No `--add-dir` or `--replay-user-messages`" keeps only `--add-dir`; *Process
  control* / *Turns* says what acknowledges a turn (the replayed prompt as the turn's
  `UserMessage` item, arriving with the first assistant frame) and that a second replay or one in
  a compaction turn is dropped; *Approvals* says the CLI echoes the adapter's answer and the
  supervisor ignores it; *Mapping keys* gains the `user` + `isReplay` row; *Code and tests* says
  `fake-claude.sh` echoes the prompt and the answers.
- `crates/giskard-harness-claude/tests/fixtures/README.md`: the three `replay-*` rows are added by
  this plan's commit; the implementer keeps them accurate if a re-recording changes a line number
  cited above.
- `specs/claude-code-harness-plan.md`: §3.1's invocation shows the flag as always on, with a
  pointer here; §11's hardening entry says this section is implemented.

### Logging

Stable fields: `thread_id`, `harness_thread_id`, `turn_id`, `frame_uuid`, `request_id`,
`action`. Actions introduced: `prompt_acknowledged` (`debug` on the item, `warn` on a second
replay), `compaction_replay` (`debug`); `control_response` gains `echo = true` (`debug`).
Unchanged: the `warn` for a user frame with no active turn, the `debug` for a skipped synthetic
block, the `warn` for a response nobody waits on (now only for a genuinely unknown id).

### Verification

Before pushing: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D
warnings`, `cargo test --workspace --locked`.

Then, on a shell with a logged-in `claude`, a `claude-code` declaration and a project:

1. Send a message. The bubble turns from grey to normal as the first words of the answer appear,
   not when the answer ends.
2. Send a message, switch to another thread while it runs, come back: the prompt is above the
   agent's output, not greyed.
3. Send a message with an image attached: same, and the attachment row is kept.
4. Compact the thread (`/compact` from the UI): the compaction turn shows the "Context compacted"
   row and no user bubble, as before.
5. `RUST_LOG=giskard_harness_claude=debug`: one `prompt_acknowledged` line per turn, no `warn`
   from `on_user`.

### Acceptance

- Every session child carries `--replay-user-messages`; the probe does not.
- Every user turn on a primary thread emits exactly one `UserMessage` item, from the CLI's replay,
  with the text that was sent, before the turn's first output item.
- A replay in a compaction turn, a second replay in one turn, a synthetic frame and an
  interjection are each handled as this section says, with the log line it names.
- The echo of an answered ask is recognised and logged at `debug`; answering an ask produces no
  `warn`.
- The three new fixtures are exercised by the mapper tests, the scripted-child and
  `fake-claude.sh` tests cover the stream, and every existing test passes.
- The adapter README, the fixtures README and the harness plan say what the code does.

## 2. Commands: no individual stop, and background commands shown as completed at once

### Symptoms

- A running command's **Stop** button is disabled; the only way to end a command is to stop the
  whole turn.
- A command run in the background (`run_in_background: true`) is shown as completed the moment the
  agent starts it, with no output, while it keeps running and the agent knows its state.

### Cause, verified

| Where | What it does | Reference |
| --- | --- | --- |
| Browser | A command row's Stop is enabled only when the running task carries a `process_id`; without one the title reads "No process id available". Stopping a tool task interrupts the turn instead; a command task never falls back to that, by spec rule R6. | `static/app.js:6081` (transcript row), `:7283` (Tasks menu), `:7319` (`stopTask`); `giskard-specification.md:4210` |
| Server | `RunningTaskState` registers a command from `ItemStarted` (status running) and keeps it across `ItemCompleted` while the status is running, taking `process_id` from either event; a terminal completion removes it; `TurnCompleted` marks surviving commands `after_turn`. A terminal `ItemCompleted` for an already-persisted turn is applied through the late path: it clears the running task, is broadcast to the browser as a transcript event with its output, and is **not** written to history ("deferred durable command-output update"). `TerminateCommand` goes to `AgentHarness::terminate_command(&handle, process_id)`; a `Transport` error matching Codex's wording counts as "unmanaged" and clears an `after_turn` task. | `thread_runtime/tasks.rs:37`, `event_forwarder.rs:1396` (`apply_late`), `ws.rs:1109`, `:202` |
| Claude adapter | A `Bash` `tool_use` starts a `CommandExecution` with `process_id: None`; its `tool_result` completes it with `status: completed` (or `failed` / `declined`), `process_id: None`, and the result text as output. For a background call that text is the CLI's note "Command running in background with ID: …", so the row reads completed with that sentence as output. `task_started` of type `local_bash` only registers the task id for the idle check; `task_updated` and `task_notification` for it do nothing. `terminate_command` is the trait default, `Unsupported`. | `mapper.rs:2255` (`start_tool`), `:2492` (`complete_tool`), `:1050` (`on_task_started`), `:801` (task frames "no event in this milestone") |
| Harness plan | Named exactly this: background command tasks "belong in Giskard's existing running-task projection, fed from the command item's own status, which is where a long-lived shell already surfaces to the UI and where `terminate_command` would hook in"; `terminate_command` is "unsupported (v1): scope, not absence". | `claude-code-harness-plan.md:715`, `:465` |

So a foreground command genuinely has nothing to stop it short of the turn, and a background
command is completed in Giskard's eyes at the moment the CLI hands it to a task.

### Verified facts (2.1.287)

Recorded in `background-stop` and `background-complete` (shipped with this plan, recorded with
`--replay-user-messages` on, as the adapter will run), and the milestone-1 `background-bash`.

| Fact | Consequence |
| --- | --- |
| A background `Bash` call produces, in order: the `tool_use` block, the `can_use_tool` ask (when not auto-approved), `system/background_tasks_changed`, `system/task_started {task_id, tool_use_id, description, is_backgrounded: true, task_type: "local_bash"}`, then the `tool_result` whose text is "Command running in background with ID: <task_id>. Output is being written to: <output_file> …" and whose `tool_use_result` carries `backgroundTaskId: <task_id>` (`background-complete` lines 6–12) | The task id is known **before** the tool result, from `task_started.tool_use_id`, and confirmed on the result itself. The item can complete as *running* with `process_id = task_id` |
| The turn's `result` follows at once; the task outlives it (`background-complete` line 15, then 33 s of silence) | The command must leave the turn as a running task, which the server already supports (`after_turn`) |
| On completion: `background_tasks_changed {tasks: []}`, `task_updated {patch: {status: "completed", end_time}}`, `task_notification {task_id, tool_use_id, status: "completed", output_file, summary: "Background command \"…\" completed (exit code 0)"}`, then the CLI starts a continuation turn on its own (`init`, assistant text, a second `result`) (`background-complete` lines 16–26) | The terminal frame to key on is `task_updated`; `task_notification` arrives ~70 ms later with the exit code (in `summary`) and the output file path. The continuation is an external turn the server already classifies |
| `stop_task {task_id}` on a `local_bash` task: `background_tasks_changed {tasks: []}`, `task_updated {status: "killed"}`, `task_notification {status: "stopped", output_file, summary: <the command>}`, then the control reply `{}` (`background-stop` lines 16–19). No continuation turn follows a kill | `terminate_command` is `stop_task`; the running task clears on `task_updated`; "stop requested" resolves to a terminal state within milliseconds |
| The output file holds stdout and stderr as written, then one marker line: `[exited with code N]` on completion, `[killed]` after a stop (live probes: `line 1 … finished\n\n[exited with code 0]\n` and `line 1\nline 2\n\n[killed]\n`). Its path is under the CLI's config directory, `…/<encoded cwd>/<session id>/tasks/<task_id>.output`, on the same machine as the server | The adapter can read the file at the terminal frame and report the output and the exit code. The file is the CLI's own and its marker format is unversioned, so the read is best-effort: a missing file or an unknown marker yields empty output and no exit code, never a failure |
| A foreground `Bash` call has no process id, and the control-request inventory (§3.3 of the harness plan) offers nothing known to stop one tool call. `interrupt` ends the whole turn. (Corrected after recording `background-taskstop`: a foreground call that runs a while does get a `local_bash` task with `is_backgrounded: false`, which ends with a `task_notification` carrying an empty `output_file` and no `task_updated`; whether `stop_task` stops it is unverified) | A foreground command cannot be stopped on its own. The spec forbids falling back to turn interruption for a command stop (R6), so the row stays disabled, with a title that says why |
| The notification follows the terminal update for a failure (`background-fail`) and for the model's own `TaskStop` tool (`background-taskstop`, recorded after this plan) as for a completion and a `stop_task` | The plan's ordering holds on every recorded path. Nothing promises it, so a terminal update still without its notification is settled at the next `result` or after a 2 s grace from the update and the output path the `tool_result` text names (implemented with this section) |
| `stop_task` on an unknown or already-ended task answers `{}` and emits a `killed` update for an ended one (milestone 5) | An unknown id must be refused by the adapter, not forwarded |
| The idle check already counts an open `local_bash` task as busy (milestone 6) | A thread with a background command is not reaped while it runs; a reaped thread has no running command |

### Step 1: the mapper (`crates/giskard-harness-claude/src/mapper.rs`)

- `TaskEntry` (`:206`, `SessionState.tasks`) gains `command: Option<BackgroundCommand>`:

  ```rust
  /// A `local_bash` task behind a background `Bash` call: the command item it completes later.
  struct BackgroundCommand {
      route: Route,
      turn: TurnId,
      item_id: ItemId,
      /// The `tool_use` id, the item's `harness_item_id`.
      tool_use_id: String,
      command: String,
      /// Set by the terminal `task_updated`; the item completes on the `task_notification` that
      /// follows, or on child exit.
      terminal: Option<TaskStatus>,
  }
  ```

  Session lifetime, like the entry it sits in: removed with the entry at the terminal frame or at
  `child_exited`.
- `on_task_started` (`:1050`): for `task_type: local_bash` with a `tool_use_id` that names an open
  `Command` tool of a route (`items.tools`), fill `command` from the open tool (`item_id`,
  `ToolKind::Command { command }`), the route and the route's turn. Log at `info`
  (`action = "task_started"`, already there) with `background_command = true`.
- `complete_tool` (`:2492`), `ToolKind::Command` arm: when the task map holds a `local_bash` entry
  for this `tool_use_id` (or `tool_use_result.backgroundTaskId` names one), the completion is
  `status: Some("in_progress")`, `process_id: Some(task_id)`, `output: String::new()`,
  `exit_code: None`: the command is running, not done, and the CLI's note is not its output. The
  open tool is still removed from `items.tools` (the turn may end); the task entry carries what the
  terminal completion needs. Log at `info` (`action = "background_command"`, `task_id`, the
  command).
- `on_task_updated` (`:1133`): a terminal status for an entry with a `command` sets
  `command.terminal` and waits for the notification; it does not remove the entry yet. Every other
  behaviour (routes, the agent-task gate) is unchanged.
- `task_notification` (`:801`, currently "no event in this milestone"): for a task whose entry has
  a `command` with `terminal` set, remove the entry and emit `ItemCompleted` on the **original
  route and turn** with the same `item_id` and `harness_item_id`:
  - `status`: `completed` for `Completed`, `failed` for `Failed`, `terminated` for `Killed` and
    `Stopped` (the browser renders `terminated` as the stopped glyph, `app.js:6113`);
  - `output` and `exit_code` from the output file (Step 1a); `duration_ms` from
    `task_updated.patch.end_time` minus the item's start when both are known;
  - `process_id: Some(task_id)` so the server's `terminating` bookkeeping matches.
  A notification for a task with no `terminal` yet (never seen) logs at `warn`
  (`action = "task_notification"`, "notification for a background command whose terminal update
  was not seen") and completes it the same way from the notification's `status`.
- `child_exited` (`:596`): every entry with a `command` completes as `terminated` with empty
  output and the exit text as `status` detail is not available on `CommandExecution`, so log it:
  `warn` (`action = "background_command_lost"`, `task_id`, the exit) per command.
- A new accessor `background_command(&self, task_id) -> Option<(ThreadId, &str)>` (the route's
  thread and the command line) for the supervisor's `terminate_command` check.

### Step 1a: reading the output file (`mapper.rs`, a small `background_output` helper)

At the terminal notification the mapper reads `output_file` with a bounded read: at most 64 KiB
from the head and 64 KiB from the tail (the server bounds persisted output again). The last
non-empty line is the marker: `[exited with code N]` gives `exit_code: Some(N)`, `[killed]` none;
the marker line and the blank line before it are stripped from `output`. A missing or unreadable
file logs at `warn` (`action = "background_output"`, `path`, the error) and yields empty output.
The mapper is pure today ("no I/O" is the core's rule, not the mapper's: `std::fs` here is fine,
but keep it in one helper with its own unit test on a temp file so the format assumption is in one
place). The read happens on the supervisor's task, which is blocking-free elsewhere: a file of a
few hundred KiB is read in microseconds; document the bound.

### Step 2: the supervisor and the façade (`session.rs`, `harness.rs`)

- `ChildCommand::StopBackgroundCommand { task_id: String, reply }`. The supervisor checks
  `mapper.background_command(&task_id)`: none → reply
  `HarnessError::Transport(format!("no background command with task id {task_id}"))` (the
  "unmanaged" wording below); some → write `stop_task {task_id}` and register a
  `Waiter::Unit(reply)` (the reply `{}` resolves it; the real signal is the `killed` update the
  mapper turns into the terminal completion, as with sub-agents). Log at `info`
  (`action = "terminate_command"`, `task_id`, `thread_id`, the command).
- `ClaudeHarness::terminate_command(&handle, process_id)`: `enqueue` on the primary thread
  (`process_id` is the task id). A sub-agent handle, or a thread with no live child (reaped: its
  background commands died with the child), answers the same `Transport("no background command
  …")`. The call is bounded by `CONTROL_TIMEOUT` through `answer`.
- `harness_error_means_command_unmanaged` (`ws.rs:202`) learns the new wording
  (`"no background command"`), so a stale `after_turn` task is cleared the way it is for Codex.
  This is the one server-side line of the adapter commit; the alternative, a `HarnessError`
  variant, is the better design and should be taken if a third adapter ever needs it.
- `capabilities()` is unchanged: there is no flag for "command termination" and the browser keys
  on `process_id` per task, which is right (a foreground command has none, a background one does).

### Step 3: the browser says why (`static/app.js`, copy only)

The disabled Stop at `:6082` (transcript row) and `:7284` (Tasks menu) gets the title
"Not supported by <harness>; stop the turn instead", where `<harness>` is the thread's declaration
name (`state.threadHarness`, falling back to "this harness" when unknown). It replaces "No process
id available", which named the mechanism rather than the remedy. True for both harnesses: a Codex
command without a process id is one Codex gave no handle for. The app already relies on a
`title` on a disabled control (the composer's `#stopBtn` carries "Stopping the current turn…"
while disabled, `:3128`), so this follows the convention; because some browsers skip tooltips on
disabled form controls, the same title also goes on the row's `.cmd-actions` container, so a hover
near the button shows it everywhere. The turn's own Stop stays the way to end the command. Copy
and attribute only: no layout change, no screenshot.

### Step 4: the server's late amendment (neutral layer, its own commit)

Without this, the adapter part still fixes the live experience: the row shows *running* with a
working Stop, the Tasks menu lists the command until it ends, the terminal completion reaches an
open browser with the output. But history keeps the row as it was when the turn was persisted
(`in_progress`, empty output), because the late path defers durable updates
(`event_forwarder.rs:1424`, "deferred durable command-output update for already-persisted
turn"). That is the spec's "durable late-item amendment milestone"
(`giskard-specification.md:4208`), and the payload format already absorbs it: items fold by
`ItemId` last-wins, a late record keeps the slot its first record established, and the index's
`item_count` is a display hint never validated against (`history.rs:397`, `:509`, `:82`).

The step: in `apply_late`, after `apply_to_runtime`, when the event is a terminal
`CommandExecution` completion for a persisted turn, append the item record to that turn's
payload file through a new `PersistStore::amend_turn_item(project, thread, turn, item)` (atomic
rewrite, since the store writes payloads whole; the per-thread lock orders it against a concurrent
turn commit), and grow the turn cache key with the payload file's metadata (`store.rs:793` says
exactly this). `renderPersistedTurn` then shows the final status and output after a reload. Tests:
a late completion amends the payload, the index record is untouched, a reload shows the terminal
item once, and the cache does not serve the stale turn. This step is independent of the adapter
and benefits Codex's after-turn commands equally.

### Step 5: tests

Mapper, on the two new fixtures and `background-bash`:

1. `a_background_command_is_a_running_task`: on `background-complete`, the `Bash` item's
   `ItemCompleted` at the tool result has `status: in_progress`, `process_id: Some(<task_id>)`,
   empty output; the turn completes with the task still open (`has_tasks()`); the `task_updated`
   + `task_notification` pair emits a second `ItemCompleted` with the same `item_id`, the original
   turn id, `status: completed`, `exit_code: Some(0)` and the output read from a temp file the test
   writes at the path the test substitutes for `output_file` (a test hook on the mapper's output
   reader, or a fixture copy with the path rewritten to the temp file); `has_tasks()` is then
   false; the continuation turn's frames map as an external turn.
2. `a_stopped_background_command_is_terminated`: on `background-stop`, the terminal completion has
   `status: terminated`, `exit_code: None`, the output the file held (`[killed]` stripped).
3. `a_background_command_dies_with_the_child`: `background-complete` cut after the turn's
   `result`, then `child_exited`: a `terminated` completion, the `warn` logged.
4. `a_notification_without_a_terminal_update_still_completes` (synthetic: drop line 17 of
   `background-complete`): completion from the notification's status, the `warn` logged.
5. `output_file_markers_are_parsed` (unit test of the helper): the two markers, a file without a
   marker, a missing file, a file over the bounds.
6. The existing `background-bash` test keeps passing: the turn completes on its first `result`
   with the command still open, and the second `result` is the continuation.

Façade (scripted child): `terminate_command_stops_a_background_command`: after the fixture's
turn, `terminate_command(&handle, "<task_id>")` writes `stop_task` with that id and resolves on
the scripted `{}`; `terminate_command` with an unknown id or on a sub-agent handle is the
`Transport("no background command …")` error without a write; on a reaped thread likewise.
`fake-claude.sh`: a message containing `background` replays `background-complete`'s turn, answers
`stop_task` with `{}` and emits the `killed` pair; `a_real_child_stops_a_background_command`
covers the real process path.

Server: `harness_error_means_command_unmanaged` accepts the new wording (unit test beside the
existing one). Step 4 has its own tests, listed there.

### Step 6: documentation

- Adapter README: *Mapping keys* (`tool_use` `Bash` row: the background case), a new
  **Background commands** subsection under *Process control* (the task id as `process_id`,
  `terminate_command` as `stop_task`, the terminal completion on the original turn, the output
  file read and its bounds, what a foreground command cannot do), *Capabilities* unchanged.
- Harness plan: §3.3's `stop_task` row and §4's `terminate_command` row ("unsupported (v1)")
  become "supported for `local_bash` tasks since the hardening pass"; §5.2's `RunningTaskState`
  sentence points here.
- Fixtures README: the two `background-*` rows (added by this plan's commit).
- `docs/api-endpoints.md`: unchanged (no route changes). Root README: the *Supported harnesses*
  entry may say background commands can be stopped; optional.
- Step 4 updates the spec's late-amendment sentence and the storage layout note in the root
  README if the payload file gains appended records.

### Logging

Actions introduced: `background_command` (`info`, at the running completion), `task_notification`
(`info` for the terminal completion with `status`, `exit_code`, `output_bytes`; `warn` without a
prior terminal update), `background_output` (`warn` on a read failure), `background_command_lost`
(`warn` at child exit), `terminate_command` (`info`; `warn` for an unknown id). Stable fields:
`thread_id`, `turn_id`, `task_id`, `tool_call_id` (the `tool_use` id), `item_id`, `path`.

### Verification

Before pushing: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D
warnings`, `cargo test --workspace --locked`.

Then, on a shell with a logged-in `claude`, a `claude-code` declaration and a project:

1. Ask for a long background command (`sleep 60 && echo done` in the background). The row shows
   running, the Tasks menu lists it after the turn ends, Stop is enabled. Press Stop: the row turns
   to terminated within a second, the Tasks menu empties, the log shows `terminate_command` and the
   `killed` update.
2. The same command left alone: after a minute the row turns to completed with `done` as output
   and exit code 0 (in an open browser); the CLI's continuation turn appears as its own turn.
3. Reload the page after step 2: with Step 4, the row shows the final state; without it, the
   row shows `in_progress` and the Tasks menu is empty, which is the documented gap.
4. A foreground command (`sleep 30`): the row's Stop is disabled and hovering it (or its action
   group) shows "Not supported by <harness>; stop the turn instead"; the turn's Stop still
   interrupts it.
5. `RUST_LOG=giskard_harness_claude=debug`: the `background_command`, `task_notification` and
   `terminate_command` lines with the task id.

### Acceptance

- A background `Bash` call completes as a running `CommandExecution` with the CLI task id as
  `process_id`, outlives its turn as a running task, and completes on the original turn with
  the terminal status, the exit code and the output when the task ends, is stopped, or the child
  exits.
- `terminate_command` stops a background command through `stop_task` and refuses anything else
  with the "unmanaged" wording the server recognises.
- A foreground command's Stop stays disabled and its tooltip says "Not supported by <harness>;
  stop the turn instead"; interrupting the turn still stops it.
- The two new fixtures are exercised by the mapper tests, the scripted-child and `fake-claude.sh`
  tests cover `terminate_command`, and every existing test passes.
- Step 4, when it lands, makes the terminal state durable; until then the gap is documented in
  the adapter README.

## 3. MCP servers: "Unknown" in green, 0 tools, 0 resources

### Symptom

A connected MCP server (the user's `claude.ai Claude Docs`, for one) shows a green chip reading
"Unknown" and two chips reading "0 tools" and "0 resources", although it is connected, has tools,
and the agent uses them.

### Cause, verified

| Where | What it does | Reference |
| --- | --- | --- |
| Browser | The coloured chip is the **auth** status: `mcpAuthLabel` renders `unknown` as "Unknown" and `mcpAuthTone` colours everything but `not_logged_in` green. The tool and resource chips are the lengths of `tools`, `resources` and `resource_templates`. Nothing shows whether the server is connected. | `static/app.js:9048` (`mcpAuthTone`), `:9050` (`mcpAuthLabel`), `:8999` (`renderMcpServerCard`), `:8864` (`mcpCounts`) |
| Neutral type | `McpServerStatus { name, auth_status, server_info, tools, resources, resource_templates }` has no connection state, and `Vec` fields cannot say "not reported". | `giskard-core/src/mcp.rs:66` |
| Claude adapter | Maps every status but `needs-auth` to `auth_status: Unknown`, puts the CLI's status string (or `<status>: <error>`) into `server_info.description`, and leaves `tools`, `resources` and `resource_templates` empty on the belief that `mcp_status` "carries no tool inventory". That belief came from recordings of **failed and pending** servers only (`mcp.rs` test payload `failed_and_pending`, recorded on 2.1.286). | `giskard-harness-claude/src/mcp.rs:1` (module doc), `:66` (`status`); README *MCP servers* |
| Codex adapter | Drops `McpServerStatus.runtimeStatus`, which Codex reports (`codex-codes` 0.155.1, `McpServerConnectionStatus`: `notStarted`, `starting`, `connected`, `authenticationRequired`, `failed`, `cancelled`, `disabled`); the spec lists it among the release's unused additions. | `giskard-harness-codex/src/lib.rs:2628` (`map_mcp_server_status`), `giskard-specification.md:349` |

So the chip the user reads as the server's state is an auth status no stdio server has, the tool
count is a parsing gap, and the resource count is a number the CLI cannot supply at all.

### Verified facts (2.1.287)

Recorded in the `mcp-status` fixture shipped with this plan: a probe child launched with
`--mcp-config` naming a minimal stdio server (`mini`: one tool, one resource, one resource
template, built for the recording) and a broken one, driven through `initialize`, `mcp_status`
and one text turn. The CLI's own strings were checked for the status vocabulary.

| Fact | Consequence |
| --- | --- |
| A **connected** server's `mcp_status` entry is `{name, status: "connected", serverInfo: {name, title, version}, config, scope, source, tools: [{name, annotations}]}` (`mcp-status` line 2). A failed one is `{name, status: "failed", error, config, scope, source}`, with no `serverInfo` and no `tools` | `mcp_status` **does** carry the tool inventory of a connected server, and its `serverInfo`. The adapter's module doc and README are wrong for the connected case |
| `tools[].name` is the bare tool name (`echo`), not the `mcp__<server>__<tool>` form; each entry carries `annotations` and nothing else: the description and input schema the server advertised are not relayed | `McpTool { name, title: None, description: None, input_schema: Null }`; the count is right, the detail list shows names |
| `system/init.mcp_servers` is `[{name, status, source}]` and `init.tools` lists `mcp__mini__echo` beside `ListMcpResourcesTool`, `ReadMcpResourceTool` and `ReadMcpResourceDirTool` (`mcp-status` line 5) | `init.tools` adds nothing `mcp_status` lacks; milestone 8's "tool inventory from `init.tools`" is unnecessary and is retired by this section |
| **Resources are not exposed to a stdio host.** No control request lists them: the CLI's `resources/list` strings are its own MCP-client calls, and the model reaches resources only through the `ListMcpResourcesTool` and `ReadMcpResourceTool` tools | The resource count cannot be reported. Showing "0 resources" is a false statement; the type must be able to say "not reported" |
| The status vocabulary in the 2.1.287 binary: `connected` (322 occurrences), `failed`, `pending`, `disabled`, `reconnecting`, and `needs-auth` with a hyphen (81 occurrences; the underscore spelling does not occur). Observed live: `connected`, `failed`; `pending` on 2.1.286 | A closed mapping with a logged fallback; the README's "both spellings matched" note resolves to the hyphen |
| `config.type` is `stdio` for a local server; remote servers carry `http` or `sse` (from the CLI's documented `--mcp-config` shapes; not recorded) | A stdio server has no authentication concept: its auth status is `Unsupported`, which the browser already labels "No auth". A remote server stays `Unknown` unless `needs-auth` |
| Codex's `runtimeStatus` already has the states this needs, and the Claude statuses map onto them one to one | One neutral `connection` field serves both adapters; nothing Claude-specific leaks into the type |

### Step 1: the neutral type (`crates/giskard-core/src/mcp.rs`, `giskard-proto` re-export)

```rust
/// How the harness sees its connection to the server; absent when the harness does not say.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpConnectionState {
    NotStarted,
    Starting,
    Connected,
    AuthenticationRequired,
    Failed,
    Cancelled,
    Disabled,
    /// A state the adapter did not recognise; `McpConnection.error` carries its raw name.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpConnection {
    pub state: McpConnectionState,
    /// The harness's reason for a `Failed` (or `Unknown`) state, verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
```

`McpServerStatus` gains `connection: Option<McpConnection>` (`#[serde(default,
skip_serializing_if = "Option::is_none")]`), and `resources` and `resource_templates` become
`Option<Vec<_>>` with the same attributes: `None` means the harness cannot report them,
`Some(vec![])` means it reported none. `tools` stays a `Vec`: both harnesses report tools. The
wire shape changes only by an optional key and by two keys that may now be absent, which
`docs/api-endpoints.md` records.

### Step 2: the Claude adapter (`crates/giskard-harness-claude/src/mcp.rs`)

`ServerEntry` grows `server_info: Option<ServerInfoEntry { name, title, version }>` (rename
`serverInfo`), `tools: Option<Vec<ToolEntry { name }>>` (unknown keys such as `annotations`
ignored, as `config` and `scope` are), and `config: Option<ConfigEntry { r#type }>`. `status(entry)`
then builds:

- `connection`: `connected` → `Connected`; `pending` and `reconnecting` → `Starting`;
  `failed` → `Failed` with `error`; `disabled` → `Disabled`; `needs-auth` →
  `AuthenticationRequired`; anything else → `Unknown` with the raw status as `error`, logged
  once per status at `warn`
  (`action = "mcp_status"`, `status`, "unrecognised MCP server status");
- `auth_status`: `NotLoggedIn` for `needs-auth`; `Unsupported` when `config.type` is `stdio`;
  `Unknown` otherwise (the `NEEDS_AUTH` pair collapses to the hyphen spelling, with a comment that
  the binary's strings settled it);
- `server_info`: the CLI's `serverInfo` (`name`, `title`, `version`; `description: None`), else
  `None`. The status text no longer travels in `description`: `connection` carries it;
- `tools`: one `McpTool` per entry, `name` as given, `title`/`description` `None`,
  `input_schema: Value::Null`;
- `resources: None`, `resource_templates: None`.

The module doc and the README's *MCP servers* section are rewritten to say what a connected entry
carries and that resources are not reachable from a stdio host.

### Step 3: the Codex adapter (`crates/giskard-harness-codex/src/lib.rs`)

`map_mcp_server_status` maps `runtime_status` onto `connection` (`notStarted` → `NotStarted`,
`starting` → `Starting`, `connected` → `Connected`, `authenticationRequired` →
`AuthenticationRequired`, `failed` → `Failed`, `cancelled` → `Cancelled`, `disabled` →
`Disabled`; `None` stays `None`) and wraps `resources` and `resource_templates` in `Some`. The
Codex README's MCP paragraph notes the field is now used; the spec sentence at
`giskard-specification.md:349` drops `runtimeStatus` from the unused list.

### Step 4: the browser (`static/app.js`, the MCP menu only)

- The card's dot and first chip come from `connection` when present: `connected` → green
  "Connected"; `starting` / `not_started` → yellow "Starting"; `authentication_required` →
  yellow "Needs auth"; `failed` → red "Failed" (the `error` shown in the expanded detail);
  `cancelled` / `disabled` → muted "Disabled"; `unknown` → muted "Unknown" with the raw name in
  the detail. Without `connection` (a harness that does not report it), today's auth-based dot
  and chip stay as they are.
- The auth chip is rendered only when `auth_status` is not `unknown`: "No auth", "Bearer token",
  "OAuth" or "Needs auth" are statements; "Unknown" was not. `needsAuth` in `mcpCounts` also
  counts `authentication_required`.
- The resources chip is rendered only when `resources` is an array; when it is absent the
  expanded detail shows "Resources: not reported by this harness" and the summary line's resource
  count sums only servers that report them. The "Authenticate" button keeps its condition.
- `mcpOverallState` treats a `failed` connection as it treats an error today.

The MCP menu is a header popover that the README screenshots do not open (`screenshots.sh` and
the e2e specs never toggle it), so no screenshot regeneration; the `layout-sticky` and
`running-tasks` specs that mention MCP do not read the card.

### Step 5: the replay harness (`crates/giskard-harness-replay/src/lib.rs:260`)

Its scripted `list_mcp_servers` fills the new fields with `connection: Some(Connected)` and
`resources: Some(..)`, so the seeded UI keeps its counts.

### Step 6: tests

- `mcp.rs`: the existing `failed_and_pending` payload keeps its test (now asserting `Failed` with
  the error, and `Starting`); a new `connected_and_failed` payload taken verbatim from
  `mcp-status` line 2 asserts `Connected`, `server_info {name: "mini", title: "Mini Server",
  version: "0.1.0"}`, one tool named `echo`, `auth_status: Unsupported` (stdio),
  `resources: None`; `needs-auth` → `AuthenticationRequired` + `NotLoggedIn`; an unknown status
  → `Unknown` with the raw name and the `warn` (`#[traced_test]`); a `tools` entry without a
  name is skipped with a `warn` and the server kept.
- A fixture-level façade test (`harness.rs`): `list_mcp_servers` over a scripted child answering
  `mcp_status` with `mcp-status` line 2's payload returns the two statuses above.
- `giskard-core` / `giskard-proto`: serde round trips with `connection` present and absent, and
  with `resources` `null`, absent and `[]`; the proto `McpServers` message test at
  `giskard-proto/src/lib.rs:1285` extended with a `connection`.
- Codex: `map_mcp_server_status` with and without `runtime_status`.
- The existing Claude façade MCP tests (`list_mcp_servers_maps_a_needs_auth_status` and the
  others listed in `harness.rs`) are updated for the hyphen spelling and the new fields.

### Step 7: documentation

- Adapter README *MCP servers*: the connected entry's shape, the tool list from `mcp_status`,
  "resources are not reachable from a stdio host", the status mapping, the `needs-auth` spelling;
  the intro sentence drops "an MCP tool inventory from `init.tools` (milestone 8)".
- Codex README: `runtimeStatus` mapped to `connection`.
- `docs/api-endpoints.md`: the `GET …/mcp` response gains `connection` and may omit `resources`
  and `resource_templates`.
- Harness plan: §3.3's `mcp_status` row and §4's `mcp_status` capability row say what a connected
  entry carries; §11's milestone 8 paragraph loses the tool-inventory item; §12 unchanged.
- Spec `giskard-specification.md:349`: `runtimeStatus` is used.

### Logging

`mcp_status` (`debug` per server, now with `connection` and `tools`; `warn` for an unrecognised
status, once per status string; `warn` for a tool entry without a name). Fields: `name`,
`status`, `source`, `tools`, `error`.

### Verification

1. With a connected server configured in the user's Claude Code settings, open the MCP menu on a
   Claude Code thread: the server shows a green "Connected" chip, "No auth" for a stdio server, the
   real tool count and the tool names when expanded, and no resources chip (the detail says they
   are not reported).
2. Configure a server whose command does not exist: a red "Failed" chip, the `ENOENT …` error in
   the detail.
3. Configure a remote server that needs login: "Needs auth" in yellow (and "Authenticate" only on
   a harness that offers OAuth login, which this one does not).
4. On a Codex thread the cards gain the "Connected" chip from `runtimeStatus` and keep their
   resource counts.

### Acceptance

- `McpServerStatus` carries a `connection` and can say that resources are not reported; both
  adapters fill it; the wire change is documented.
- A connected Claude Code server shows "Connected", its tools and no false resource count; a
  failed one shows "Failed" with the error; `needs-auth` shows "Needs auth".
- No chip reads "Unknown" for a connected server.
- The `mcp-status` fixture backs the adapter's tests; every existing test passes.

## Observed while recording, not yet an issue

- `system/thinking_tokens` frames appeared between assistant frames in the `replay-image`
  recording (two of them, lines 4–5 of the first, discarded take; the shipped take has none). The
  mapper logs an unknown `(type, subtype)` once at `warn` and skips it, so this costs one log line
  per session where it occurs. Worth a mapping (or an explicit skip) in a later section once its
  content is understood.
