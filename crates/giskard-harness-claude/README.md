# Claude Code harness adapter

`giskard-harness-claude` maps Claude Code's stream-json protocol (`claude -p --input-format
stream-json --output-format stream-json`) onto the harness-neutral types and lifecycle events
defined by `giskard-harness` and `giskard-core`.

The [Giskard specification](../../specs/giskard-specification.md) defines the owned identifier
semantics and invariants, and the
[Claude Code harness plan](../../specs/claude-code-harness-plan.md) records how the CLI actually
behaves and the milestones that build this adapter. This document describes what the adapter does
**today**, including the scope and lifetime of Claude Code-native identifiers.

**Status: milestone 3.** `ClaudeHarness` implements `AgentHarness` over **one `claude` process per
open primary thread**: `open_thread` spawns and handshakes it (fresh, `--resume`, or the same-id
respawn when the transcript is gone), `start_turn` applies the turn's permission mode, model and
effort and writes the user message with inline attachments, `respond_approval` and
`respond_server_request` answer the CLI's asks, `compact_thread` runs `/compact`, `interrupt` and
`set_thread_name` send control requests, `set_thread_archived(true)`, `delete_thread` and
`shutdown` stop children, `list_models` answers from the freshest handshake or a probe child, and
`list_providers` reports `anthropic`. `capabilities()` reports the plan §4 matrix, with
`live_approvals`, `plan_build_modes`, `per_turn_model`, `reasoning_effort` and
`context_compaction` true. Nothing is user-reachable yet: no `HarnessKind` names this adapter until
milestone 4, which also wires `list_mcp_servers` (`mcp_status` stays false until then). Sub-agent
threads, and routing a sub-agent's asks to them, are milestone 5's.

## Runtime ownership

- **One supervisor task per child** (`src/session.rs`) is the single owner of the child process,
  its `ClaudeMapper`, its pending control-request waiters and the thread's retained `EventLog`.
  Nothing else touches them, and none of them sits behind a lock: the façade reaches the task only
  through its bounded command channel (`StartTurn`, `Interrupt`, `Control`, `RespondApproval`,
  `RespondServerRequest`, `Compact`, `Stop`). The task selects over the child's stdout lines
  (first, so a frame already read is mapped before a new command is accepted), its commands, and
  the instance's shutdown signal. A command that needs a control *response* before it can go on
  (the per-turn settings) cannot await a waiter the same loop would resolve, so the supervisor
  writes the request and keeps reading and dispatching frames itself until the response or its
  deadline (`await_control`); nothing read meanwhile is lost. It reads in 50 ms slices so that a
  shutdown or a `Stop` ends the wait at once (the hand-off fails with "claude child is stopping"
  and the stop sequence runs), and any other command arriving meanwhile is kept and handled after.
  A reply the mapper must write while pumping (an `ExitPlanMode` deny) that cannot be written
  breaks the child, as in the main loop.
- **The façade** (`src/harness.rs`) holds two maps behind `std` mutexes that are never held across
  an await: `children` (thread → live child: its session id, retained log, command sender, task,
  open model, launch mode) and `pending` (approval / server-request id → thread, CLI
  `request_id`, and what the answer needs: the tool-use id, the tool name and the raw
  `permission_suggestions` of an approval, the subtype and `input` of a server request). The façade
  also remembers, once, the sentence in which the CLI refused a bypass launch (`bypass_refused`).
  `open_thread` inserts a child after its handshake; the supervisor removes its own entry when the
  child exits (a generation number keeps a stale supervisor from removing a reopened thread's
  entry); `delete_thread`, `set_thread_archived(true)` and `shutdown` take entries out before
  stopping them. A supervisor drops its thread's `pending` entries when its child exits; an answer
  or a `control_cancel_request` removes one entry.
- **The retained log is created at open**, so `subscribe` returns a live reader for any handle
  `open_thread` issued before the child has written a frame. Frames read during the handshake that
  were not its responses are mapped first, once the supervisor starts.
- **The probe child** that `list_models` spawns when no handshake has reported a catalog yet is
  owned by the call: it is not in `children` and does not count as a live child.

## Identifier model

| Giskard identity | Claude Code source |
| --- | --- |
| `harness_thread_id` of a primary thread | the session UUID Giskard mints and passes as `--session-id` |
| `harness_thread_id` of a sub-agent thread | `task:<tool_use_id>` of the parent's `Agent` call (`ids::TASK_ID_PREFIX`); routes are claimed in milestone 5 |
| `TurnId` | minted by Giskard at `start_turn` (`begin_turn`), or by the mapper for a continuation turn the CLI started on its own |
| `ItemId` | minted on first sight of a native key: a `tool_use` block's `id`, or `(message.id, block index)` for a text or thinking block; reused for the item's start, deltas and completion within the turn |
| `Item.harness_item_id` | the tool-use id, `<message_id>:<index>`, `compact_boundary:<uuid>`, or `user:<uuid>:<index>` for a user-frame activity |
| `ApprovalId`, `ServerRequestId` | the `control_request`'s `request_id`, a UUID the CLI mints and the reply must carry; the trait's instance-wide uniqueness across children rests on the CLI minting UUIDs |

`assistant` frames arrive **one content block per frame**, each repeating the whole message
envelope with the same `message.id`. The mapper numbers the blocks of one message across its frames,
so a block's index matches the `content_block_*.index` of the stream events for the same message.

## Mapping keys

| Source | `ItemStarted` | `ItemDelta` | `ItemCompleted` |
| --- | --- | --- | --- |
| `text` block | `content_block_start` (text) → `AgentMessage`; without stream events, the `assistant` frame starts it | `text_delta` → `Text` | the `assistant` frame with the block → `AgentMessage { text }`; empty text still completes |
| `thinking` block | only once a non-empty `thinking_delta` or a non-empty block arrives → `Reasoning` | `thinking_delta` → `Text` | `assistant` frame → `Reasoning { text }`; an empty thought emits nothing at all |
| `tool_use` `Bash` | `assistant` frame → `CommandExecution` with `command`, `cwd` = workspace root, `status: in_progress` | none (Claude streams no command output) | the `tool_result` → `CommandExecution` with `output` = `tool_use_result.stdout` then `stderr`, else the result text; `exit_code: None` |
| `tool_use` `Write`, `Edit`, `NotebookEdit` | `FileChange` | none | `FileChange { path: input.file_path, change }`, `Created` when `tool_use_result.type == "create"`, else `Modified`; no diff |
| `tool_use` `Agent` | `ToolCall { name: "Agent" }`, no `SubagentLink` until milestone 5 | none | `ToolCall { output: the result content }` |
| `tool_use` `mcp__<server>__<tool>` | `ToolCall { server: <server>, name: <tool> }` | none | `ToolCall` |
| any other `tool_use` | `ToolCall { name, input }` | `input_json_delta` is ignored; the `assistant` frame has the final input | `ToolCall`, with `error` = the result text when `is_error` |
| `user` frame with a text block and no `tool_result` | `Activity` started and completed together, `title` = the text (the `[Request interrupted by user for tool use]` marker) | | |
| `system/compact_boundary` | | | `Activity { title: "Context compacted", detail: "<pre> → <post> tokens (<trigger>)", metadata: compact_metadata }` |

A tool item completes with `status` `declined` when the result's `tool_result_meta` gives a
`non_execution_kind` or the ask was denied (the mapper's own plan-mode denial, or a
`system/permission_denied`), `failed` when `is_error`, else `completed`.

Asks and other control traffic:

| Frame | Output |
| --- | --- |
| `can_use_tool` for most tools | `ApprovalRequested` (`Bash` → `CommandExecution`, file tools → `FileChange`, MCP tools → `McpToolCall`, else `Permission`) with `Tool`, `Blocked path` and one `Suggestion` per `permission_suggestions` entry (type and destination only), plus `MapperOutput::PendingApproval` carrying the tool name and the raw suggestions |
| `can_use_tool` for `AskUserQuestion` | `ServerRequestReceived { method: "claude/ask_user_question", params: {questions} }`, each question the CLI's object plus `"id": "<index>"`, plus `PendingServerRequest { subtype: "can_use_tool", input }` |
| `can_use_tool` for `ExitPlanMode` / `EnterPlanMode` | no event; a `MapperOutput::Reply` denying it ("Giskard chooses the mode per turn"), logged at `warn` |
| any other `control_request` | `ServerRequestReceived { method: "claude/<subtype>", params: request }` plus `PendingServerRequest { subtype, input: null }` |
| `control_cancel_request` | `MapperOutput::CancelRequest { request_id }`, logged at `info` (`action = "control_cancel_request"`); the adapter looks the ask up |
| `control_response` | `MapperOutput::ControlResponse { request_id, payload }` for the adapter's own waiter |

Session-level frames: `system/init` stores the model and emits a `Notice` once per non-`none`
`apiKeySource` (usage is billed to that credential); a `system/init` or `system/status` whose
`permissionMode` differs from the one `set_expected_mode` recorded is a `Notice` and a `warn` with
`action = "permission_mode_drift"` (one check, `check_mode`, serves both: a re-emitted `init` after
a backgrounded task is the frame most likely to show a mode Giskard did not set); a
`rate_limit_event` is a `Notice` only when its status is not `allowed` or a window is at least 90%
used; `api_retry` and `permission_denied` are `Notice`s. `note_denied(tool_use_id)` marks a tool
use the adapter denied, so its `tool_result` completes the item as `declined`.

## Item lifecycle

Item state lives with the turn and is dropped when the turn completes. A text block is started by
its stream `content_block_start` (or first delta) and completed by its `assistant` frame. A thinking
block becomes an item only once it has text: the recordings ran with thinking display omitted, so a
recorded thought is `thinking: ""` with a signature and produces no item. A tool call is started by
its `assistant` frame and completed by the `tool_result` that names its id; a `tool_result` naming
no open call is logged at `warn` with its id and dropped. Frames whose `parent_tool_use_id` is set
belong to a sub-agent: no route is claimed for them in this milestone, so they are dropped with a
`debug` log naming `parent_tool_use_id` and the frame type, never attributed to the primary thread.

## Turn completion

- `begin_turn` opens a turn. Calling it while one is active is a caller bug: the previous turn is
  completed as `Failed` ("superseded by a new turn") and logged at `error`.
- **External turn.** After a backgrounded task finishes, the CLI re-emits `system/init` and runs a
  continuation with no user message. An `assistant`, `stream_event`, `result` or `can_use_tool`
  frame with no active turn opens one: the mapper mints a `TurnId`, emits `TurnStarted` and logs at
  `info` with `action = "external_turn"`. The server claims such a turn as an external turn.
- **Agent-task gate.** A `result` completes the turn unless a `local_agent` task the turn started is
  still open; then the result is held. A terminal `task_updated` removes its task; when the last one
  goes and a result is held: `completed` keeps holding (the CLI's continuation `result` completes
  the turn), while `killed`, `failed` or `stopped` completes it now from the held result, as
  `Interrupted` if the adapter sent an interrupt, else `Failed`, because no second `result` comes.
  A `local_bash` task never gates completion; a foreground delegation never holds, since its task
  completes before the single `result`.
- **Status.** `is_error: false` is `Completed`. An error result is `Interrupted` when the adapter
  called `note_interrupt_sent` or `terminal_reason` is `aborted_streaming` / `aborted_tools`, else
  `Failed` with the result text, else the joined `errors`, else the subtype.
- **Compaction turn.** A `TurnKind::Compaction` turn (the `/compact` `compact_thread` writes) emits
  the compact-boundary `Activity` and completes on the degenerate `result` as `Completed` with no
  agent message. The CLI's synthetic summary and replayed command output (`isSynthetic` /
  `isReplay` user frames) are bookkeeping and produce no item.

## Runtime context window

Usage reaches the server only through `TurnUsageUpdated`, emitted from every `message_delta` that
carries `usage` and once more just before `TurnCompleted`; consecutive identical
`(usage, window)` pairs are suppressed. Input tokens are `input_tokens +
cache_creation_input_tokens + cache_read_input_tokens`; output tokens are `output_tokens`.

`TurnUsageUpdated.usage` is always **one API request's** usage, because the context gauge reads its
input as what occupies the window now (spec §10.3): a `message_delta`'s, and for the closing event
the last entry of `result.usage.iterations` (the summed `result.usage` when there is none, as on the
degenerate `/compact` result). `TurnCompleted.usage` is the turn's billing figure: `result.usage`,
which sums every request, added across a held result and the CLI's continuation result, since both
belong to one Giskard turn.

`context_window` is the session's `autocompact_state.effective_window` when it reported one, else
`result.modelUsage[<session model>].contextWindow` once a `result` has been seen, else unknown.
`model` is set only when the adapter called `note_turn_model` for the turn.

`result.modelUsage` may name a second model. `AgentEvent` has no per-model usage channel, so the
turn's usage is the summed `result.usage` and each `modelUsage` entry is only logged at `debug`
under `model = <id>`.

## Frames the crate cannot type

`claude-codes` is pinned to the CLI release it models, but its `ClaudeOutput` has no fallback
variant: an unknown top-level `type` (`autocompact_state` and `active_goal` are real ones), an
untyped control-request subtype, or a missing required field would fail the whole line. So
`Frame::parse` peeks at `type` (and `subtype`) first and deserializes only the named struct. A
`type` it does not know is `Frame::Unknown`, logged at `warn` the first time each `(type, subtype)`
pair is seen and at `debug` after; a known kind that fails to convert is `FrameError::Untyped`,
logged at `warn`; a non-JSON line is logged with its byte length. No log line carries frame
content: serde errors have their quoted values redacted.

## Launch

Each primary thread's child runs, in this order:

| Arguments | Why |
| --- | --- |
| `-p --input-format stream-json --output-format stream-json --verbose` | the stdio protocol |
| `--permission-prompt-tool stdio` | asks arrive as `can_use_tool` control requests |
| `--setting-sources user` | plan §8.3: the user's settings, not a cloned repository's |
| `--disallowedTools EnterPlanMode ExitPlanMode` | Giskard chooses the mode per turn |
| `--include-partial-messages` | `stream_event`s, so text streams as `ItemDelta`s |
| `--permission-mode bypassPermissions`, or `manual` after a refused bypass launch | the launch mode is only the *ceiling* (see below); never `--permission-prompts none`, which would deny every ask silently |
| `--model <ModelRef.model>` | an alias or a full id, verbatim |
| `--effort <ModelRef.reasoning_effort>` | only when the model ref carries one; the CLI tolerates a level the model ignores |
| `--session-id <uuid>` or `--resume <uuid>` | a fresh session (or the same-id respawn), or a resume |
| the declaration's `args` | last, so an operator can append to, never override, the protocol flags |

**Launch mode.** `bypassPermissions` can only be *set* on a child *launched* with it (otherwise
`set_permission_mode` answers `bypass_not_launched`), and `open_thread` cannot know whether the
thread will ever run a `full_access` turn. So every session child is launched with
`--permission-mode bypassPermissions`, and the handshake sets `default` before anything else. **No
turn can run before that request has succeeded**: nothing reaches stdout before the first user
message, and every turn sets its own mode. A bypass-launched child that refuses or times out on
that request is killed and the open fails (`claude did not leave bypassPermissions: …`): it is
never used in bypass mode by accident. One that exits on it is reported like any other handshake
exit (exit status, stderr tail, authentication and bypass-refusal classification). When the CLI
refuses the bypass launch itself (as root: exit 1, `--dangerously-skip-permissions cannot be used
with root/sudo privileges for security reasons`; or, best effort and unverified, a stderr line
naming `bypassPermissions` and `disable` for settings that disable the mode), the adapter records
the sentence, logs `bypass_refused` at `warn` once per instance, and relaunches with the same
session flag and `--permission-mode manual`; later opens launch standard children directly. A
standard child still sends `set_permission_mode default` in its handshake (a failure there is
logged and ignored).

No `--add-dir`, `--forward-subagent-text` (milestone 5) or `--replay-user-messages`. The child's
working directory is `OpenThreadOptions.workspace_root`. The declaration's environment overlay is
applied **over** the inherited environment, never in place of it, so an `ANTHROPIC_API_KEY`,
`ANTHROPIC_BASE_URL` or other `ANTHROPIC_*` variable in Giskard's own environment reaches every
child (plan §7); the mapper's `apiKeySource` notice is the mitigation. The spawn log line names the
command, the working directory, the number of extra arguments and the overlay's variable **names**,
never values. stdout lines are capped at 64 MiB (a longer one is fatal for that child); stderr is
drained by its own task, logged at `debug` under the target `giskard_harness_claude::stderr`, and
its last 8 lines (400 characters each) are what every spawn error and exit log line quotes.

## Handshake, resume and respawn

Nothing reaches stdout at spawn: the CLI's first frame answers the first message. So the open
handshake is a sequence of control requests:

1. `initialize`, under a 30 s timeout. Its `models` replace the catalog snapshot. A child that exits
   instead, times out (killed), or refuses fails the open.
2. `set_permission_mode {"mode": "default"}`, under 10 s (see *Launch mode*). The CLI echoes
   `default`, and after a change emits `system/status`; the mapper expects `default` from the
   start, so that frame is not drift.
3. `get_settings`, under 10 s. `applied.model` equal to the requested model, or to the catalog's
   `resolvedModel` for it, makes `ThreadHandle.resumed_model` the requested `ModelRef`; another id
   makes it that id (with `applied.effort`) and logs `model_not_applied` at `warn`, so the server
   unwinds a provider switch the CLI did not confirm. No answer is `None` and a `warn`, never a
   failed open.
4. On resume only, `get_context_usage`, under 10 s. A positive `maxTokens` is sent as
   `ThreadUpdate::ContextWindowRestored` and seeds the mapper's window, so the first
   `TurnUsageUpdated` carries it.

The ids of `get_settings` or `get_context_usage` requests that timed out are handed to the
supervisor, so the CLI's late answer is logged at `debug` rather than as an unexpected response.

A `resume` id beginning with `task:` is refused as `Unsupported` (a sub-agent has no session; plan
§5.3), and any other id must be a UUID. With `--resume`, a child that exits before answering
`initialize` with `No conversation found with session ID` (on stderr or in the `result.errors` it
wrote) means the transcript is gone: the adapter logs `claude_resume_failed` at `warn`, respawns
with `--session-id <the same uuid>` and the same launch mode (a bypass refusal on that respawn
still falls back), and opens the thread writable with the notice
`claude_resume_failed` ("Agent context was lost; started a fresh Claude Code session. History is
intact.", detail: the CLI's sentence). That respawn works only because the transcript is gone:
any other resume failure (such as `Error: Session ID … is already in use.`) is an error and is
never retried, and a failed respawn returns its own error. A handshake failure whose stderr or
`result.errors` mentions `not logged in`, `Invalid API key`, `/login` or `authentication` is
`HarnessError::Unauthenticated`; this is a best-effort substring match, since the unauthenticated
shape could not be reproduced. Every other failure is `HarnessError::Spawn` quoting the exit status,
the handshake request left unanswered (`initialize`, `set_permission_mode`, `get_settings` or
`get_context_usage`) and the stderr tail, which is what the browser shows.

A second `open_thread` for a thread with a live child returns that child's handle.

## Process control

- **Turns.** `start_turn` refuses a turn while the mapper has one active (`ThreadBusy`): the CLI
  would queue the second message, and the adapter never queues. `TurnStarted` is in the log before
  the line is written. A `start_turn` whose caller timed out (its reply channel closed) before the
  supervisor reached it is dropped unwritten and logged at `warn`, so the user's message never runs
  under a turn the server did not admit. Before `TurnStarted`, the hand-off applies the turn's
  settings (below); any failure there fails `start_turn` with no turn started. The settings share
  one 25 s budget (each request at most 10 s of it), so the supervisor's own timeout, naming the
  request left unanswered, ends a slow hand-off before the façade's 30 s `start_turn` limit.
- **Interrupt** writes the `interrupt` control request and resolves on its response, within 10 s.
  With no active turn the CLI answers at once and nothing else happens.
- **Rename.** `set_thread_name` sends `rename_session` (`source: "host"`) to a live child; a cold
  or `task:` thread is a no-op, since Giskard keeps its own name.
- **Stop** (archive, delete, shutdown, or a dropped instance): if a turn is live, write `interrupt`
  and keep mapping frames for up to 5 s so the turn's `result` reaches the log; close stdin and
  read to EOF for up to 5 s; then SIGKILL (`stop_kill` at `warn`). SIGTERM is not used: it leaves
  the turn without a `result`. Each stop is bounded by 15 s, the registry's own shutdown budget;
  a supervisor that overruns it is aborted (which kills the process) and the façade closes the
  thread's event log itself, so the stream still ends.
  `set_thread_archived(false)` does nothing; `delete_thread` does not touch `~/.claude`.
- **Shutdown** is idempotent: it marks the instance shut down, stops every child concurrently,
  clears `pending`, and logs `children_stopped`. Afterwards `open_thread` and `list_models` fail.
- **Child exit.** However a child ends, an active turn completes from `ClaudeMapper::child_exited`
  (`Interrupted` after an interrupt, else `Failed`, naming the exit code or signal), the thread's
  log closes (only that thread's stream ends), the child leaves `children`, waiters fail, and one
  `child_exited` line logs the exit code or signal, the stderr tail, whether the stop was requested
  and `live_children`: at `info` for a requested stop that exited 0 (or 1 after an interrupt), at
  `warn` otherwise. The spawn line logs `live_children` too. A closed log's refusal of an event is
  logged once and counted on that line.
- **Asks.** `can_use_tool` and other inbound control requests are published as events, recorded
  in `pending`, and answered by `respond_approval` / `respond_server_request` (below).

## Permission presets and plan mode

Every turn sets its permission mode with `set_permission_mode` before its message, **on every
turn** (plan §8.2: a stale mode must be impossible to carry over), using the CLI's names:

| Turn | Mode sent |
| --- | --- |
| `Mode::Plan`, any preset | `plan` (Plan wins over the preset) |
| `ask_first` | `default` |
| `auto_approve` | `acceptEdits` |
| `full_access` | `bypassPermissions` |

`auto` and `dontAsk` are never sent (plan §8.1). The flag's `manual` is the control request's
`default` (the CLI echoes `default` for it), so the adapter only ever sends `default`. The mapper's
expected mode is set to the requested name *before* the request is written, so the `system/status`
the change emits is not drift; a refusal (`invalid_mode`, `bypass_not_launched`) fails the turn
start with the CLI's sentence and a `warn` with `action = "set_permission_mode"`, `mode` and
`error_code`, and puts the expected mode back to the one the CLI still holds, so a later frame
reporting it is not drift. A mode already set when a later step fails stays set; the next turn
sets its own.

`full_access` on a standard child (the CLI refused the bypass launch) is refused before anything is
written: `Unsupported("full_access is not available: Claude Code refused to start in
bypassPermissions mode (<the CLI's sentence>)")`.

`--disallowedTools EnterPlanMode ExitPlanMode` is what keeps a plan turn in plan mode: without it a
write request in plan mode becomes an `ExitPlanMode` ask that switches the mode back when allowed.
The mapper's own denial of such an ask is the backstop.

`ask_first` means **"ask before anything with an effect", not "ask before anything"** (plan §8.3,
§9.2.1): the CLI runs a built-in set of read-only commands inside the working directories without
an ask in every mode, and that set is not configurable. A mode Giskard did not set is reported on
both `system/init` and `system/status` (see *Mapping keys*).

## Per-turn model and effort

`overrides.model` (the thread's open model when absent) is compared with what the supervisor knows
the CLI holds (the open model, then the last read-back):

- a different model selector sends `set_model {model}`. An unknown model answers
  `catalog_unknown`, which fails the turn start as `Unsupported` with the CLI's sentence (`Model 'x'
  not found`): the picker is never pre-filtered (plan §6);
- a different effort level sends `apply_flag_settings {settings: {effortLevel}}`, which always
  answers success. A requested level is also sent, and checked, on every turn that switches
  model, since the switch can change the level the CLI holds (a model without effort reports
  none). A turn asking for no effort leaves the CLI's level alone (clearing it is unverified);
- if either was sent, `get_settings` reads back. `applied.model` must equal the requested selector
  or its catalog `resolvedModel`, else `Protocol("Claude Code applied model X instead of Y")`;
  `applied.effort` must equal the requested level, else `Unsupported("Claude Code did not accept
  effort LEVEL for MODEL")`. An invalid level leaves the previous one in `applied.effort`, and a
  model without effort reports `null`, so both read as refused. `effective.effortLevel` is never
  compared: a valid `max` clears it. Whatever the read-back says becomes what the supervisor
  believes the CLI holds, refused or not.

Every started turn logs one `turn_settings` line at `info`: the mode, model and effort the CLI
holds, and `model_or_effort_changed`.

Model changes are only sent while idle (`start_turn` refuses a busy thread). The turn's
`TurnUsageUpdated.model` is the requested `ModelRef`.

## Approvals

`respond_approval` removes the ask from `pending` (none: `Protocol("approval … is not pending")`
at `warn`: answered, withdrawn, or its child is gone) and has the supervisor write one
`control_response`:

| `ApprovalDecision` | `response` |
| --- | --- |
| `Accept` | `{"behavior":"allow"}` |
| `AcceptForSession` | `{"behavior":"allow","updatedPermissions":[…]}`: every `addRules` suggestion of the ask, cloned raw with `destination` set to `"session"` |
| `Decline` | `{"behavior":"deny","message":"Declined by the user in Giskard"}` |
| `Cancel` | `{"behavior":"deny","message":"Cancelled by the user in Giskard","interrupt":true}` |
| `AcceptWithExecPolicyAmendment` | not offered: `Unsupported`, and the ask stays pending |

`AcceptForSession` never sends a destination other than `session` and never rewrites a rule's
`ruleContent`: the suggestion objects are echoed as the CLI sent them (`claude-codes`' typed
suggestion drops `directories` and has no `localSettings` destination, so they are never retyped).
An ask with no `addRules` suggestion degrades to a plain allow and a `warn` with `action =
"accept_for_session_degraded"`. A session grant lives in the child and dies with it: archive,
delete, shutdown and a crash all end it. `Decline` and `Cancel` mark the tool use denied in the
mapper, so its item completes `declined`; `Cancel` also counts as an interrupt, so the
`aborted_tools` result persists the turn as `Interrupted` and the exit after it is expected. Every
answer logs `respond_approval` at `info` with the decision, the tool name and the number of rules
echoed, never the rule content. The CLI sends no acknowledgement: the browser's card is cleared by
the live snapshot and by the turn's end. A caller that gave up before the supervisor wrote the
answer leaves the ask pending.

**Cancellation.** On `interrupt` the CLI withdraws its own pending asks: it writes
`control_cancel_request` for each, then the interrupt's response and a result whose
`permission_denials` names the asked tools. So spec §9.2's "best-effort cancel the pending request"
is the CLI's, not the adapter's: the adapter only drops the entry (an approval's card vanishes with
the turn; a server request also gets `ServerRequestResolved`), logging `control_cancel_request`. A
later answer for that id is refused as `Protocol` (the CLI would ignore it anyway). The withdrawal
can also overtake an answer already on its way to the supervisor (the façade takes the ask from
`pending` before the supervisor writes): the supervisor remembers withdrawn ids it found nothing
for, until the next turn, and such an answer is not written, is logged at `info` as a late answer,
fails with `Protocol("… was withdrawn by Claude Code")`, and for a server request emits the
`ServerRequestResolved` the withdrawal could not.

Until milestone 5, a sub-agent's asks attach to the primary thread's turn.

## Server requests

- **`AskUserQuestion`** (`claude/ask_user_question`). The params' questions carry `id` = their
  index, which is what the browser's question card keys its answers by. The answer
  `{answers: {<id>: {answers: [<label>…]}}}` becomes `{"behavior":"allow","updatedInput":
  {"questions": <the ask's own questions>, "answers": {<question text>: <labels joined with
  ", ">}}}`. The map is keyed by **question text**, never by `header` (keyed by header, the CLI
  reports the questions unanswered); a question with no label is left out. A browser error is
  `{"behavior":"deny","message":<message>}`. An answer of another shape is a `Protocol` error and
  the request stays pending.
- **Any other control request** (`request_user_dialog`, `rename_session`, `hook_callback`,
  `mcp_message`, …): a result is `{"subtype":"success","request_id":…,"response":<value>}`, an
  error `{"subtype":"error","request_id":…,"error":<message>}` (the browser's numeric code has no
  counterpart; the error shape toward the CLI is unverified).

After the write the supervisor appends `ServerRequestResolved`, which clears the browser's card.
Answers are logged with `respond_server_request`, the subtype and whether it was a result or an
error, never the answer itself.

## Manual compaction

`compact_thread` mints a `TurnId` and has the supervisor open a `TurnKind::Compaction` turn and
write `{"type":"user","message":{"role":"user","content":[{"type":"text","text":"/compact"}]}}`. A
thread with an active turn is `ThreadBusy`; a thread with no live child is `ThreadNotFound`. No
per-turn settings are sent: compaction runs no tool, and the mode in force is the previous turn's.
The mapper does the rest: the `compact_boundary` is the turn's one `Activity`, the synthetic
summary frames are bookkeeping, and the degenerate `result` completes the turn as `Completed` with
no agent message (see *Turn completion*).

## User attachments

The user message is `{"type":"user","message":{"role":"user","content":[…]}}` with every
attachment block before the text block:

| `mime_type` | Block |
| --- | --- |
| `image/png`, `image/jpeg`, `image/gif`, `image/webp` | `image` with a `base64` source |
| `application/pdf` | `document` with a `base64` source |
| `text/*`, `application/json`, `application/xml`, `application/x-yaml`, `application/toml`, `application/javascript` | `document` with a `text` source holding the decoded UTF-8 |
| anything else | `Unsupported`, naming the attachment |

Whitespace is stripped from `data_base64` (the API rejects wrapped base64) and the data is decoded
to validate it. The serialized line must fit the CLI's 10 MiB cap; a larger message is refused by
size, never truncated. Empty text with attachments sends the attachments alone; an empty message is
refused. The `attachment` log action records each attachment's kind, MIME type and size, never its
content.

## Model catalog (`initialize.models`)

Every handshake replaces the catalog snapshot, so `list_models` answers from the freshest
`initialize` a child reported. Before any thread is open, `list_models` spawns one probe child
(serialized, so concurrent callers share it) with the protocol flags only (no mode, model, effort
or session flag, so it leaves no transcript), reads `initialize` under 30 s, closes stdin and logs
`catalog_probe`. Entries are parsed one at a time; an odd one is skipped with a `warn`. Each entry
except the `default` alias becomes a descriptor with `model` = the entry's `value`, its display name
and effort levels, and `is_default` on the first entry resolving to the same model as `default`
(the user's configuration decides which). The catalog carries no context window, so descriptors use
the conservative window until a turn's `TurnUsageUpdated` or a resume's `ContextWindowRestored`
reports the runtime one.

## Provider table

`list_providers` reports one provider, `anthropic` ("Anthropic (Claude Code)"), with no base URL (so
Giskard's own `/v1/models` discovery stays off), no auth source, and the instance's environment
overlay.

## Code and tests

- `src/lib.rs`: the public surface and `capabilities()`.
- `src/harness.rs`: `ClaudeHarness`, the handshake, the bypass and resume fallbacks, the probe,
  the per-turn mode, and the façade tests against a scripted child and against
  `tests/fake-claude.sh`.
- `src/session.rs`: the per-child supervisor, `await_control` and the per-turn settings, the
  approval and server-request answers, compaction, the stop sequence, the pending-ask map, and the
  in-process `ScriptedChild` the façade tests drive (it echoes every `set_permission_mode` the
  script does not handle itself).
- `src/process.rs`: `ClaudeLaunchOptions`, argv and the launch mode, spawning, the capped stdout
  reader, the stderr tail and exit classification.
- `src/attachments.rs`: the user message line and attachment blocks.
- `src/catalog.rs`: the `initialize.models` catalog, its descriptors and `ANTHROPIC_PROVIDER_ID`.
- `src/frame.rs`: one stdout line to a typed `Frame`, tolerant of everything the crate cannot type.
- `src/mapper.rs`: `ClaudeMapper`, the frame-to-event state machine, and its fixture-driven tests.
- `src/ids.rs`: `NativeItemKey` and the `task:` sub-agent id prefix.
- `src/log_fields.rs`: optional-field logging helper.
- `src/log_checks.rs` (tests only): the line checks the `#[traced_test]` log assertions pass to
  `logs_assert`.
- [`tests/fixtures/README.md`](tests/fixtures/README.md): the recorded scenarios, the recorder's
  argv and the sanitization.
- `tests/fake-claude.sh`: a POSIX `sh` stand-in for `claude` that replays the fixtures, so the real
  process path (spawn, stderr tail, exit codes, kill) is tested without the CLI. It answers
  `set_permission_mode` (`bypass_not_launched` for `bypassPermissions` on a child not launched
  with it), `set_model` (`catalog_unknown` outside the `initialize` catalog), `apply_flag_settings`
  and `get_settings` (echoing the model and effort it was told), replays the `tool-allowed` ask on
  a message containing `touch` and the rest once answered, and with `FAKE_CLAUDE_REFUSE_BYPASS=1`
  refuses a bypass launch with the root sentence.
