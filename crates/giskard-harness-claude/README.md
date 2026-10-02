# Claude Code harness adapter

`giskard-harness-claude` maps Claude Code's stream-json protocol (`claude -p --input-format
stream-json --output-format stream-json`) onto the harness-neutral types and lifecycle events
defined by `giskard-harness` and `giskard-core`.

The [Giskard specification](../../specs/giskard-specification.md) defines the owned identifier
semantics and invariants, and the
[Claude Code harness plan](../../specs/claude-code-harness-plan.md) records how the CLI actually
behaves and the milestones that build this adapter. This document describes what the adapter does
**today**, including the scope and lifetime of Claude Code-native identifiers.

**Status: sub-agent threads (milestone 5).** A `[harnesses.<name>]` declaration of kind
`claude-code` starts, and the server runs its projects and threads on `ClaudeHarness`, which
implements `AgentHarness` over **one `claude` process per open primary thread**: `open_thread`
spawns and handshakes it (fresh, `--resume`, or the same-id respawn when the transcript is gone),
`start_turn` applies the turn's permission mode, model and effort and writes the user message with
inline attachments, `respond_approval` and `respond_server_request` answer the CLI's asks,
`compact_thread` runs `/compact`, `interrupt` and `set_thread_name` send control requests,
`set_thread_archived(true)`, `delete_thread` and `shutdown` stop children, `list_models` answers
from the freshest handshake or a probe child, `list_mcp_servers` asks the hinted thread's child or
a probe for `mcp_status`, and `list_providers` reports `anthropic`. `capabilities()` reports the
plan §4 matrix, with `live_approvals`, `plan_build_modes`, `per_turn_model`, `reasoning_effort`,
`context_compaction` and `mcp_status` true. A delegation (an `Agent` tool call) is a **sub-agent
thread**: the mapper mints a route for it, `claim_native_thread` binds it, its forwarded frames are
its transcript, its asks are published on it, and `interrupt` on it is `stop_task` (see *Sub-agent
routes*). A child idle for `idle_shutdown_secs` is stopped and its thread respawned with
`--resume` on its next message (see *Process control*). Still to come: synthesized diffs
(milestone 7), and version-drift and headroom surfacing, including an MCP tool inventory from
`init.tools` (milestone 8).

## Runtime ownership

- **One supervisor task per child** (`src/session.rs`) is the single owner of the child process, its
  `ClaudeMapper`, its pending control-request waiters and the thread's retained `EventLog`. Nothing
  else touches them, and none of them sits behind a lock: the façade reaches the task only through
  its bounded command channel (`StartTurn`, `Interrupt`, `Control`, `RespondApproval`,
  `RespondServerRequest`, `StopTask`, `StopBackgroundCommand`, `Compact`, `Stop`). The façade never
  waits for room in it: a command is enqueued with `try_send` while the façade holds the `threads`
  lock (the `routes` lock for a sub-agent's `stop_task`), and a full queue (16 deep; the server
  serializes per thread, so only a wedged supervisor fills it) is a `Transport` error logged at
  `warn`. It also owns **one retained `EventLog` per sub-agent route** of its child: each event goes
  to the log of the thread it names (the primary's or a route's). The task is an explicit state
  machine driven by **one** `select!` loop over the child's stdout lines (first, so a frame already
  read is mapped before a new command is accepted), its commands, the instance's shutdown signal,
  and the deadline of whatever it is waiting on; every arm body runs to completion, so a write made
  from one is never cancelled. It is in one of two phases:
  - **serving**: frames are mapped and commands served. At most one **turn hand-off**
    (`TurnSetup`) is in flight: a `StartTurn`'s settings requests are written one at a time, each
    response (recognised by its `request_id`) advances the hand-off to its next stage (mode,
    model, effort, read-back) and the last one writes the user message; the outstanding request's
    deadline is a loop input that fails the hand-off. A second `StartTurn` or a `Compact`
    meanwhile is `ThreadBusy`; every other command (a rename, an interrupt, an answer) is served
    while the hand-off waits.
  - **stopping**: the stop sequence (*Process control*), as an *interrupting* stage (waiting for
    the interrupted turn's `result`) then a *draining* one (stdin closed, waiting for EOF), each
    with its deadline. A hand-off in flight fails at once with "claude child is stopping"; every
    command that arrives is refused at once with "claude child stopped" (`stop_refused` at
    `debug`); a second `Stop` joins the sequence and is answered with the first when the child
    has exited. The shutdown and command-channel arms are disabled once they fired, so the loop
    never spins on a closed channel or a set flag.

  While serving, the supervisor also runs the **idle clock**: at the top of every loop iteration
  it checks whether the child has anything to do (*Idle reaping*), starts or stops the clock
  (`idle` at `debug`, on each change of what keeps it busy), and arms the idle timer from the
  later of "idle since" and "last stdout line"; when it fires, the reap is a stop sequence that
  keeps the thread.

  A reply the mapper must write (an `ExitPlanMode` deny) that cannot be written breaks the child
  wherever it happens.
- **The façade** (`src/harness.rs`) holds three maps behind `std` mutexes that are never held across
  an await: `threads` (primary thread → its session id, retained log, workspace root and model, and
  its live child while it has one: command sender, task, launch mode, generation), `pending`
  (approval / server-request id → the thread the ask was published on, its **owner** (the primary
  thread whose child answers it), CLI `request_id`, and what the answer needs: the tool-use id, the
  tool name and the raw `permission_suggestions` of an approval, the subtype and `input` of a server
  request) and `routes` (sub-agent thread → its `task:` id, retained log, owning primary thread and
  command sender, parent native id, name and model; a **cold** route has a fresh, open, silent log
  and no owner). The façade also remembers, once, the sentence in which the CLI refused a bypass
  launch (`bypass_refused`). `open_thread` inserts an entry after its child's handshake. An entry
  **outlives a reaped child**: the reap clears its child (keeping the session id, the open log, the
  model the CLI holds, which a confirmed model change also updates, and the API-billing notice it
  already showed) and the next turn's respawn fills it again. Each entry carries a **respawn gate**,
  an async mutex, so one respawn runs per thread: a caller that waited on another's finds its child.
  The supervisor removes its own entry when its child exits any other way (a generation number keeps
  a stale supervisor from touching a reopened or respawned thread's entry); `delete_thread`,
  `set_thread_archived(true)` and `shutdown` take entries out before stopping their children, and
  close the log of an entry that has none. A supervisor drops every `pending` entry it owns (its
  routes' included) when its child exits, unless it was reaped (idle means none); an answer or a
  `control_cancel_request` removes one entry. A supervisor publishes a route into `routes` on the
  mapper's `RouteOpened`, and at a reap or at exit turns its own routes (guarded by its generation)
  **cold**: owner and command sender cleared, log left open. `claim_native_thread` inserts a cold
  route. Only the sub-agent thread's own `delete_thread` or `set_thread_archived(true)`, and
  `shutdown`, remove a route and close its log, like a Codex thread log that lives as long as its
  thread.
- **The retained log is created at open**, so `subscribe` returns a live reader for any handle
  `open_thread` issued before the child has written a frame. It lives as long as the thread's
  entry: a respawned child appends to the same log, so a reader sees one continuous stream. Frames
  read during the handshake that were not its responses are mapped first, once the supervisor
  starts.
- **The probe child** that `list_models` spawns when no handshake has reported a catalog yet, or
  `list_mcp_servers` when the call names no thread with a live child, is owned by the call: it is
  not in `threads` and does not count as a live child. It must never become a Claude Code session
  (`AGENTS.md`): it is launched with the protocol flags only (no `--session-id`, `--resume`,
  `--model`, `--permission-mode`), it is written only control requests through `control_line`,
  never a `user` line, and its stdin is closed after the last answer. Verified on 2.1.287:
  `initialize` alone, or `initialize` then `mcp_status`, on a probe leaves no `projects/<cwd>/`
  directory, no session `.jsonl`, no `sessions/` entry and no project entry in `.claude.json`;
  only cache files change (`cache/model-catalog/*`, growth-book features, `policy-limits.json`,
  `remote-settings.json`). The probe does start the user's configured MCP servers while it lives.

## Identifier model

| Giskard identity | Claude Code source |
| --- | --- |
| `harness_thread_id` of a primary thread | the session UUID Giskard mints and passes as `--session-id` |
| `harness_thread_id` of a sub-agent thread | `task:<tool_use_id>` of the parent's `Agent` call (`ids::TASK_ID_PREFIX`): a sub-agent runs inside its parent's session and has no session of its own |
| `ThreadId` of a sub-agent thread | minted by the mapper when the `Agent` block is mapped, and adopted by `claim_native_thread` whatever id the server proposed; a route whose session is gone is bound cold under the proposed id |
| `TurnId` | minted by Giskard at `start_turn` (`begin_turn`), or by the mapper for a continuation turn the CLI started on its own |
| `ItemId` | minted on first sight of a native key: a `tool_use` block's `id`, or `(message.id, block index)` for a text or thinking block; reused for the item's start, deltas and completion within the turn |
| `Item.harness_item_id` | the tool-use id, `<message_id>:<index>`, `compact_boundary:<uuid>`, `user:<uuid>:<index>` for a user-frame activity, a primary turn's replayed prompt or a sub-agent's delegated prompt, or `task_updated:<task_id>` for a backgrounded sub-agent's outcome |
| `ApprovalId`, `ServerRequestId` | the `control_request`'s `request_id`, a UUID the CLI mints and the reply must carry; the trait's instance-wide uniqueness across children rests on the CLI minting UUIDs |

`assistant` frames arrive **one content block per frame**, each repeating the whole message
envelope with the same `message.id`. The mapper numbers the blocks of one message across its frames,
so a block's index matches the `content_block_*.index` of the stream events for the same message.

## Mapping keys

| Source | `ItemStarted` | `ItemDelta` | `ItemCompleted` |
| --- | --- | --- | --- |
| `text` block | `content_block_start` (text) → `AgentMessage`; without stream events, the `assistant` frame starts it | `text_delta` → `Text` | the `assistant` frame with the block → `AgentMessage { text }`; empty text still completes |
| `thinking` block | only once a non-empty `thinking_delta` or a non-empty block arrives → `Reasoning` | `thinking_delta` → `Text` | `assistant` frame → `Reasoning { text }`; an empty thought emits nothing at all |
| `tool_use` `Bash` | `assistant` frame → `CommandExecution` with `command`, `cwd` = workspace root, `status: in_progress` | none (Claude streams no command output) | the `tool_result` → `CommandExecution` with `output` = `tool_use_result.stdout` then `stderr`, else the result text; `exit_code: None`. A **background** call (a `local_bash` task names it) completes `in_progress` with `process_id` = the task id and no output, then completes again on its original turn when the task ends (*Background commands*) |
| `tool_use` `Write`, `Edit`, `NotebookEdit` | `FileChange` | none | `FileChange { path: input.file_path, change }`, `Created` when `tool_use_result.type == "create"`, else `Modified`; no diff |
| `tool_use` `Agent` | `ToolCall { name: "Agent", subagent }` with the route's link (`task:<id>`, `initial_prompt` = `input.prompt`, `Spawned`, `Pending`), preceded by `MapperOutput::RouteOpened` | none | `ToolCall { output: the result content, subagent }` with the link as the route stands: `Completed` (or `Interrupted`) for an ended route, `Started` / `Running` for one still running (a backgrounded delegation) |
| `user` frame with `isReplay: true` (not `isSynthetic`, no `tool_result`) on the primary route, in a user turn | `UserMessage` started and completed together, `text` = the frame's text blocks joined by `\n` (image and document blocks ignored): the turn's acknowledgement. A second one in the turn is dropped (`warn`); in a compaction turn it is the `/compact` output, skipped (`debug`) | | |
| a sub-agent's first `user` text block equal to its delegated prompt | on the route: `UserMessage` started and completed together, `text` = the prompt | | |
| terminal `system/task_updated` of a route's `local_agent` task | | | on the route: open tool calls `interrupted`, then `TurnCompleted`; for a backgrounded delegation, on the spawning thread: `Activity { title: description, detail: completed / killed / failed, subagent: the link }` |
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
used; `api_retry` and `permission_denied` are `Notice`s. `note_denied(thread, tool_use_id)` marks a
tool use the adapter denied on the turn of `thread` (the primary's or a route's), so its
`tool_result` completes the item as `declined`.

## Item lifecycle

Item state lives with the turn and is dropped when the turn completes. A text block is started by
its stream `content_block_start` (or first delta) and completed by its `assistant` frame. A thinking
block becomes an item only once it has text: the recordings ran with thinking display omitted, so a
recorded thought is `thinking: ""` with a signature and produces no item. A tool call is started by
its `assistant` frame and completed by the `tool_result` that names its id; a `tool_result` naming
no open call is logged at `warn` with its id and dropped. Frames whose `parent_tool_use_id` is set
belong to a sub-agent: one naming a minted route is mapped onto that route's thread and turn by the
same item functions (see *Sub-agent routes*); one naming no route is dropped with a `debug` log
naming `parent_tool_use_id` and the frame type, never attributed to the primary thread.

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
  agent message. The CLI's synthetic summary (`isSynthetic`) and the replayed command output
  (`<local-command-stdout>`, `isReplay`) are bookkeeping and produce no item.

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
| `--forward-subagent-text` | a sub-agent's text and thinking reach stdout as `assistant` / `user` frames with `parent_tool_use_id`, its route's transcript |
| `--permission-mode bypassPermissions`, or `manual` after a refused bypass launch | the launch mode is only the *ceiling* (see below); never `--permission-prompts none`, which would deny every ask silently |
| `--replay-user-messages` | the CLI echoes each user line back (`isReplay`), which acknowledges the turn's prompt (*Process control*); a session flag only, since a probe never writes a user line. It also echoes every `control_response` the adapter writes (*Approvals*) |
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

No `--add-dir`. The child's working directory is
`OpenThreadOptions.workspace_root`. The declaration's environment overlay is applied **over** the
inherited environment, never in place of it, so an `ANTHROPIC_API_KEY`, `ANTHROPIC_BASE_URL` or
other `ANTHROPIC_*` variable in Giskard's own environment reaches every child (plan §7); the
mapper's `apiKeySource` notice is the mitigation. The spawn log line names the command, the working
directory, the number of extra arguments and the overlay's variable **names**, never values. stdout
lines are capped at 64 MiB (a longer one is fatal for that child); stderr is drained by its own
task, logged at `debug` under the target `giskard_harness_claude::stderr`, and its last 8 lines (400
characters each) are what every spawn error and exit log line quotes.

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
§5.3): it is an item id, never a session. Any other id must be a UUID. With `--resume`, a child that
exits before answering `initialize` with `No conversation found with session ID` (on stderr or in
the `result.errors` it wrote) means the transcript is gone: the adapter logs `claude_resume_failed`
at `warn`, respawns with `--session-id <the same uuid>` and the same launch mode (a bypass refusal
on that respawn still falls back), and opens the thread writable with the notice
`claude_resume_failed` ("Agent context was lost; started a fresh Claude Code session. History is
intact.", detail: the CLI's sentence). That respawn works only because the transcript is gone: any
other resume failure (such as `Error: Session ID … is already in use.`) is an error and is never
retried, and a failed respawn returns its own error. A handshake failure whose stderr or
`result.errors` mentions `not logged in`, `Invalid API key`, `/login` or `authentication` is
`HarnessError::Unauthenticated`; this is a best-effort substring match, since the unauthenticated
shape could not be reproduced. Every other failure is `HarnessError::Spawn` quoting the exit status,
the handshake request left unanswered (`initialize`, `set_permission_mode`, `get_settings` or
`get_context_usage`) and the stderr tail, which is what the browser shows.

A second `open_thread` for a thread with a live child returns that child's handle. For a thread
whose child was reaped (the server reopens one only after a restart or a forget; it never learns of
a reap), `open_thread` respawns at once with `--resume` on the thread's session id, under the same
fallbacks, and reports the restored window and a lost transcript as a first open does. The lazy
respawn on a turn (*Idle reaping*) is the same path: same argv, same bypass fallback, same
missing-transcript fallback, same handshake; there the lost-transcript notice is an
`AgentEvent::Notice` right after the turn's `TurnStarted` (its turn set, so a second loss is not
deduplicated away), since the server reads no handle.

## Process control

- **Turns.** `start_turn` refuses a turn while the mapper has one active (`ThreadBusy`): the CLI
  would queue the second message, and the adapter never queues. `TurnStarted` is in the log before
  the line is written. A `start_turn` whose caller timed out (its reply channel closed) before the
  supervisor reached it is dropped unwritten and logged at `warn`, so the user's message never runs
  under a turn the server did not admit. **Acknowledgement:** the CLI replays the prompt
  (`--replay-user-messages`) after the turn's `system/init` and before its first stream event or
  `assistant` frame, so it arrives as the answer starts; the mapper makes it the turn's
  `UserMessage` item (`prompt_acknowledged` at `debug`), which is what un-greys the browser's
  pending prompt. Attachments ride in the same frame and are not items of their own. A second replay
  in one turn is dropped with a `warn`, and a replay in a compaction turn is the `/compact` output,
  skipped at `debug` (`compaction_replay`). Before `TurnStarted`, the hand-off applies the turn's
  settings (below); any failure there fails `start_turn` with no turn started. The settings share
  one 25 s budget (each request at most 10 s of it), so the supervisor's own timeout, naming the
  request left unanswered, ends a slow hand-off before the façade's 30 s `start_turn` limit; that
  deadline is an input of the supervisor's loop, and the CLI's late answer to a request that timed
  out is logged at `debug` and ignored.
- **Interrupt** writes the `interrupt` control request and resolves on its response, within 10 s.
  With no active turn the CLI answers at once and nothing else happens. On a sub-agent thread it is
  `stop_task` instead (see *Sub-agent routes*).
- **Rename.** `set_thread_name` sends `rename_session` (`source: "host"`) to a live child; a cold,
  reaped or `task:` thread is a no-op, since Giskard keeps its own name. `interrupt` on a reaped
  thread is `Ok` too: it has no active turn.
- **Stop** (archive, delete, shutdown, or a dropped instance): if a turn is live, write `interrupt`
  and keep mapping frames for up to 5 s so the turn's `result` reaches the log; close stdin and
  read to EOF for up to 5 s; then SIGKILL (`stop_kill` at `warn`). SIGTERM is not used: it leaves
  the turn without a `result`. A command arriving during the stop is refused at once, and a second
  stop joins the first. Each stop is bounded by 15 s, the registry's own shutdown budget;
  a supervisor that overruns it is aborted (which kills the process) and the façade closes the
  thread's event log itself, so the stream still ends.
  `set_thread_archived(false)` does nothing; `delete_thread` does not touch `~/.claude`.
- **Shutdown** is idempotent: it marks the instance shut down, stops every child concurrently,
  closes the log of every reaped thread, clears `pending`, and logs `children_stopped` and
  `threads_closed`. A child still draining from its reap finishes on its own. Afterwards
  `open_thread` and `list_models` fail.
- **Child exit.** However a child ends, an active turn completes from `ClaudeMapper::child_exited`
  (`Interrupted` after an interrupt, else `Failed`, naming the exit code or signal), as does every
  open sub-agent route turn; the thread's log closes (only that stream ends), its routes turn cold
  with their logs left open, the thread leaves `threads`, waiters fail, and one
  `child_exited` line logs the exit code or signal, the stderr tail, whether the stop was
  requested, whether it was a reap, `live_children` and `loaded_threads`: at `info` for a
  requested stop that exited 0 (or 1 after an interrupt), at `warn` otherwise. The spawn line logs
  `live_children` too. A closed log's refusal of an event is logged once and counted on that line.
  A **reaped** child's exit is the exception: the thread's log stays open, its entry keeps the
  thread, and its pending map is left alone; its routes turned cold at the reap. Whatever a reaped
  child still writes while it drains is mapped and dropped (`reaped_frame` at `warn` once,
  `reaped_outputs` on the exit line): the log is the thread's, and a respawned child may already
  be writing to it.
- **Idle reaping.** A child is idle when it has no turn hand-off in flight, no ask awaiting the
  user, no live sub-agent route, no open task (a `local_bash` task outlives its turn and can still
  ask), no control request awaiting its answer and no active turn. Once it has been idle, and has
  written no stdout line, for `idle_shutdown_secs` (a key on the `claude-code` declaration; 600 s by
  default, `0` never; a value too large for the clock never reaps), the supervisor reaps it. The
  quiet-stdout condition makes the closed list fail safe: a frame the mapper does not model (a
  future background mechanism) postpones the reap, and a child that keeps talking while idle is
  never reaped. The reap turns the child's routes cold, then, in one critical section of the
  `threads` lock, takes the child out of the thread's entry and drains its command queue: since the
  façade only enqueues under that lock, a command that reached the supervisor before the take
  cancels the reap (`reap_cancelled` at `debug`; the child goes back into its entry and serves it,
  its routes left cold, which is benign since idle means every route already ended), and once the
  child is out no command can reach it. Otherwise it logs `child_reaped` at `info` (with `idle_ms`,
  `live_children` and `loaded_threads`) and runs the stop sequence with no turn to interrupt: close
  stdin, read to EOF, kill on the grace timeout. The thread stays bound on the server, which is
  never told, and its stream stays open. The next `start_turn`, `compact_thread` or `open_thread`
  respawns the child with `--resume` (`respawn` at `info`, with `resume_fallback` and `elapsed_ms`);
  the user sees nothing, or the "Agent context was lost" notice under the new message when the
  transcript is gone. Respawns are gated per thread, so two callers never start two `claude
  --resume` of one session. The first message after a reap pays the `--resume` handshake (a few
  seconds) before its `TurnStarted`; the server's forwarder has no timeout of its own on
  `start_turn`, so nothing fails, it just waits. A failed respawn fails that turn and keeps the
  thread, so the next message tries again. A task whose terminal update never comes keeps its child
  alive for good (`idle` with `reason = "tasks"`), the right failure: the CLI believes it runs. An
  MCP status read never respawns: a reaped thread's hint is answered by the probe.
- **Asks.** `can_use_tool` and other inbound control requests are published as events, recorded
  in `pending`, and answered by `respond_approval` / `respond_server_request` (below).

### Background commands

A `Bash` call with `run_in_background: true` hands its command to a CLI task: `system/task_started`
of type `local_bash` names the call's `tool_use_id` **before** the `tool_result`, whose text
("Command running in background with ID: …") and `tool_use_result.backgroundTaskId` name the same
task. The mapper attaches the open command item to the task entry, and the `tool_result` completes
the item `in_progress` with `process_id` = the task id and empty output (`background_command` at
`info`): the command is running, and the CLI's note is not its output. The turn ends on its
`result`; the command outlives it as a running task, which the server's running-task projection
keeps and whose Stop the browser enables because it has a process id.

- **End.** The terminal `task_updated` (`completed`, `failed`, `killed`, `stopped`) records the
  status and `end_time`; the entry stays until the `task_notification` that follows (~70 ms),
  which names the output file. The item then completes a second time, with the same `ItemId` and
  `harness_item_id`, on its **original thread and turn**: `completed`, `failed`, or `terminated`
  for `killed` / `stopped`; `output` and `exit_code` from the output file; `duration_ms` from
  `end_time` minus the item's start; `process_id` still the task id (`task_notification` at `info`
  with `status`, `exit_code` and `output_bytes`). A notification whose terminal update was not
  seen completes the item from its own status, with a `warn`. The CLI then runs its own
  continuation turn after a completion (an external turn), none after a kill.
- **Output file.** `…/<encoded cwd>/<session id>/tasks/<task_id>.output` under the CLI's config
  directory, on the server's machine: stdout and stderr as written, a blank line, then
  `[exited with code N]` or `[killed]`. The read is bounded to the first and last 64 KiB (the
  middle is replaced by an "omitted" line and the item marked truncated with the file's size); the
  marker and the blank line are stripped. The format is the CLI's own and unversioned, so the read
  is best-effort: an unknown last line stays output with no exit code, and a missing or unreadable
  file is empty output with a `warn` (`background_output`), never a failure.
- **Stop.** `terminate_command(handle, process_id)` sends `StopBackgroundCommand` to the primary
  thread's child, which writes `stop_task {task_id}` and resolves on the `{}` answer under the 10 s
  control timeout (`terminate_command` at `info`); the `killed` update and the notification that
  follow complete the item `terminated`. A task id that names no background command of the child,
  a sub-agent handle, or a thread with no live child (a reaped child's commands died with it) is
  `Transport("no background command with task id …")`, written nothing, which the server reads as
  "unmanaged" and clears a stale running task by.
- **Child exit.** Every background command still running completes `terminated` with empty output,
  and a `warn` (`background_command_lost`) names its task and the exit.
- **Foreground commands** have no task and no process id, and the CLI offers no control request
  that stops one tool call: only `interrupt`, which ends the turn. So their Stop stays disabled
  ("Not supported by <harness>; stop the turn instead"), and the turn's own Stop is the way.
- **History.** The terminal completion of a command whose turn is already persisted reaches an open
  browser live, and the server appends it to that turn's payload file as a late item record (spec
  LA1–LA5), so a reload or a reconnect shows the final state.

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

**Echoed answers.** With `--replay-user-messages` the CLI writes every `control_response` the
adapter sends back on stdout, verbatim and unmarked, where it reads as a response to a request of
the adapter's own. So the supervisor records the `request_id` of every answer it writes
(`respond_approval`, `respond_server_request`, and the mapper's own `ExitPlanMode` deny) until the
next turn starts; the echo of one is removed from that set and logged at `debug`
(`action = "control_response"`, `echo = true`), checked before the waiters, so the `warn` for a
response nobody is waiting on still means an unknown id.

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

**Sub-agent asks.** A sub-agent's `can_use_tool` carries `agent_id` (its task's id) and no
`parent_tool_use_id`, and it can arrive **before** the forwarded frame carrying its `tool_use`
block. It is routed by `agent_id` → task → route; else by its `tool_use_id` among the routes' open
tool calls (logged at `debug`); else, with an unknown `agent_id`, it stays on the primary thread
with a `warn` (`action = "ask_route_unknown"`) so it stays answerable. The ask is published on the
route's thread and turn and recorded with the route's thread and the primary as its owner;
`respond_approval` / `respond_server_request` reach the owner's child, and the answer is the same
`control_response`: the CLI's `request_id` is all it needs.

## Sub-agent routes

- **Mint.** An `Agent` `tool_use` block mints a route keyed by its tool-use id, on whichever route
  the block arrived (the primary, or an outer route for a nested delegation): a fresh `ThreadId`,
  `task:<id>`, the parent's native id (the session id, or `task:<outer>`), the name
  (`input.description`) and the prompt (`input.prompt`). `MapperOutput::RouteOpened` comes before
  the block's `ItemStarted`, so the supervisor has created and published the route's retained log
  before the server's forwarder sends the link, and every child frame is retained for the claim.
  Logged at `info` with `action = "route_opened"`.
- **Turn.** `system/task_started` of the `local_agent` task naming the route records its task id
  and opens the route's turn (`TurnStarted` on the route's thread). A routed frame arriving with no
  route turn opens one, at `info` with `action = "external_turn"`. The child's frames (forwarded
  with `--forward-subagent-text`, `assistant` and `user` only, no stream events) map exactly as the
  primary's, on the route's thread and turn; the delegated prompt is a `UserMessage`. Usage comes
  from each child API message's `message.usage`, counted once per `message.id` though every
  one-block frame repeats it.
- **End.** A terminal `task_updated` of the route's task completes its turn **at once**: open tool
  calls complete `interrupted` with no output, then `TurnCompleted`: `completed` → `Completed`;
  `killed` → `Interrupted` when the adapter sent `stop_task` for the route or the spawning turn was
  interrupted, else `Failed("agent task <id> was killed")`; `failed` / `stopped` → `Failed` with the
  patch's `error`. A terminal update for a task already ended or unknown is ignored at `debug`
  (`stop_task` on a completed task still emits `killed`). The primary turn's agent-task gate is
  unchanged.
- **Trailing frames.** A killed sub-agent's rejection `tool_result` and its `[Request interrupted by
  user for tool use]` marker **trail** the terminal update. They are dropped at `debug` with
  `action = "route_trailing_frame"`, the route and its thread: the `Interrupted` status says the
  same, and holding the turn open for them would leave the sub-agent looking busy.
- **Backgrounded.** A backgrounded delegation's `Agent` call completes at launch with its link still
  `Started`; when its task ends, an `Activity` on the spawning thread's turn carries the outcome and
  the link (`harness_item_id = task_updated:<task_id>`).
- **Close.** When the turn that spawned a route ends, the mapper drops the route (`RouteClosed`: the
  supervisor stops appending to its log and drops its asks; `info`, `action = "route_closed"`). The
  log stays **open and published**: the sub-agent thread's owner keeps reading it (a closed stream
  would end that owner as failed), and a claim that lands after the parent's turn ended still adopts
  the route and reads its retained events. A route still running there (an agent task the gate let
  through because the turn failed or was superseded) is first completed `Failed("parent turn
  ended")` at `warn` (`action = "route_still_open"`). Nested routes are dropped with their parent
  route. Child exit completes every open route turn (`Interrupted` after an interrupt or a
  `stop_task`, else `Failed` naming the exit) and drops every route; the supervisor then turns its
  published routes cold (logged as `routes_cooled` on the exit line). A route's mapper state never
  outlives its child; its log lives until the sub-agent thread's own delete or archive, or shutdown.
- **`stop_task`.** `interrupt` on a sub-agent thread sends `StopTask` to the owning child; the
  supervisor writes `{"subtype":"stop_task","task_id":…}`, resolves the call when its answer comes
  (the façade bounds it by 10 s), and logs `stop_task` at `info`. The stop is noted on the route
  **before** the write, since the CLI emits the task's `killed` update before it answers. A route
  whose task has not started yet is `Protocol("the sub-agent has not started yet")`; one that
  already ended is `Ok` at `debug`; a thread that is no route of the child is `Protocol`. The CLI
  withdraws the sub-agent's pending ask with a `control_cancel_request` (the existing path), and the
  parent's turn continues.
- **Claim.** `claim_native_thread` accepts only a `task:` id (anything else is `Protocol`). A live
  route is **adopted**: the handle's thread is the mapper's, with `agent_name`, `resumed_model`
  (the child's model) and `parent_harness_thread_id`. Otherwise the session that produced the
  route is gone (a persisted sub-agent reopened after a restart), and a **cold route** is bound
  under the proposed thread: an open, silent stream, no child. A route whose child exited is
  already cold, and the claim adopts it with its retained history.
  `interrupt` on it is `Unsupported("this Claude Code sub-agent is no longer running")`. The claim
  never spawns, resumes or writes anything, is idempotent for the same id, and refuses a proposed
  thread already bound to another native id. Logged at `info` with `action =
  "claim_native_thread"`, `adopted` and `cold`. `subscribe` reads a child's log, else a route's.
- **Cleanup.** `delete_thread` and `set_thread_archived(true)` on a `task:` thread remove its route,
  close its log (ending the thread's stream) and drop its asks; `shutdown` closes and clears every
  route. Archiving or deleting the *parent* stops its child, which only turns the routes cold.
  `open_thread` still refuses to resume a `task:` id, and `set_thread_name` is a no-op for one. An
  event for a route whose log is gone is counted and logged once per thread at `warn` (`action =
  "route_log_missing"`).

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
`catalog_probe`; `list_mcp_servers`' probe stores the catalog the same way. Entries are parsed one
at a time; an odd one is skipped with a `warn`. Each entry except the `default` alias becomes a
descriptor with `model` = the entry's `value`, its display name and effort levels, and
`is_default` on the first entry resolving to the same model as `default` (the user's configuration
decides which). The catalog carries no context window, so descriptors use the conservative window
until a turn's `TurnUsageUpdated` or a resume's `ContextWindowRestored` reports the runtime one.

## Provider table

`list_providers` reports one provider, `anthropic` ("Anthropic (Claude Code)"), with no base URL (so
Giskard's own `/v1/models` discovery stays off), no auth source, and the instance's environment
overlay.

## MCP servers

`list_mcp_servers` sends the `mcp_status` control request. The configured server set is the same in
every child (`--setting-sources user`), but `pending` and `failed` are per process, so the call
takes the thread the user is looking at as a hint. When the hinted thread has a live child, or the
hint is a sub-agent route whose owner's child is live (the `RouteHandle`'s `owner`), it asks that
child (under the 10 s control timeout); the line logs `mcp_status` with `thread_id` (the hinted
thread), `owner_thread_id` (the child's thread) and `hinted=true`. A live child's failure is
returned, never probed around: a timeout from a running child says the CLI is unresponsive, and a
probe would not describe that thread. With no hint, or a hint without a live child (a thread this
instance does not hold, a cold route, a child that just exited, logged at `debug` as "the hinted
thread has no live child; probing"), it never picks an arbitrary child: it spawns a probe child
exactly as the catalog probe does (protocol flags only, serialized with it), sends `initialize` and
then `mcp_status` under 30 s each, stores the catalog from `initialize` as the catalog probe would,
closes stdin and logs `mcp_probe` with `thread_id` (the hint, if any), `hinted`, `elapsed_ms` and
`servers`. A refused
`mcp_status` is `HarnessError::Protocol` with the CLI's message; a probe that exits before
answering is the handshake error (`claude exited with … before answering mcp_status: …`).

The answer is `{"mcpServers": [{name, status, error?, config, scope, source}]}` (`src/mcp.rs`).
Each entry becomes one `McpServerStatus`: `name` verbatim; `auth_status` `NotLoggedIn` when
`status` is `needs-auth` or `needs_auth` (the CLI's `/mcp` screen names an
authentication-required state whose wire spelling was not observed, so both are matched) and
`Unknown` otherwise; `server_info.description` the status, or `<status>: <error>` when the CLI
gave an error, so the panel shows `failed: ENOENT …` for a server that did not start and `pending`
for one still connecting (the panel's refresh asks again). An entry that does not parse is skipped
with a `warn` naming its index; each server is logged at `debug` with its name, status, source and
error. `mcp_status` carries **no tool inventory**: tools reach the model as
`mcp__<server>__<tool>` names in `system/init.tools`, so `tools`, `resources` and
`resource_templates` stay empty; listing them in the panel is milestone 8's `init.tools` work.
`mcp_reload` and `mcp_oauth_login` stay unadvertised.

## Code and tests

- `src/lib.rs`: the public surface and `capabilities()`.
- `src/harness.rs`: `ClaudeHarness`, the handshake, the bypass and resume fallbacks, the probe,
  the per-turn mode, and the façade tests against a scripted child and against
  `tests/fake-claude.sh`.
- `src/session.rs`: the per-child supervisor's state machine (its serving and stopping phases, the
  idle clock and the reap, and the in-flight turn hand-off that applies the per-turn settings), the
  approval and server-request answers, `stop_task`, the route logs, compaction, the stop sequence,
  the pending-ask map, and the in-process `ScriptedChild` the façade tests drive (it echoes every
  `set_permission_mode` the script does not handle itself).
- `src/process.rs`: `ClaudeLaunchOptions`, argv and the launch mode, spawning, the capped stdout
  reader, the stderr tail and exit classification.
- `src/attachments.rs`: the user message line and attachment blocks.
- `src/catalog.rs`: the `initialize.models` catalog, its descriptors and `ANTHROPIC_PROVIDER_ID`.
- `src/mcp.rs`: the `mcp_status` answer to `McpServerStatus`.
- `src/frame.rs`: one stdout line to a typed `Frame`, tolerant of everything the crate cannot type.
- `src/mapper.rs`: `ClaudeMapper`, the frame-to-event state machine, its sub-agent routes and
  background commands (with the output-file reader), and its fixture-driven tests (the `delegation`,
  `delegation-interrupted`, `subagent-stop` and `subagent-ask-withdrawn` recordings drive the route
  tests).
- `src/ids.rs`: `NativeItemKey` and the `task:` sub-agent id prefix.
- `src/log_fields.rs`: optional-field logging helper.
- `src/log_checks.rs` (tests only): the line checks the `#[traced_test]` log assertions pass to
  `logs_assert`.
- [`tests/fixtures/README.md`](tests/fixtures/README.md): the recorded scenarios, the recorder's
  argv and the sanitization.
- `tests/fake-claude.sh`: a POSIX `sh` stand-in for `claude` that replays the fixtures, so the real
  process path (spawn, stderr tail, exit codes, kill) is tested without the CLI. With
  `--replay-user-messages` it echoes each user line back with `isReplay` before the turn's frames
  and each answer it is written back verbatim, as the CLI does. It answers
  `set_permission_mode` (`bypass_not_launched` for `bypassPermissions` on a child not launched
  with it), `set_model` (`catalog_unknown` outside the `initialize` catalog), `apply_flag_settings`,
  `get_settings` (echoing the model and effort it was told), `stop_task` (`{}`) and `mcp_status`
  (no servers), treats `--resume` as the missing-transcript failure unless
  `FAKE_CLAUDE_RESUME_OK=1` makes it a successful resume, replays
  the `tool-allowed` ask on a message containing `touch` and the rest once answered, replays a
  background command's turn on a message containing `background` and its `killed` pair on
  `stop_task`, and with
  `FAKE_CLAUDE_REFUSE_BYPASS=1` refuses a bypass launch with the root sentence.
