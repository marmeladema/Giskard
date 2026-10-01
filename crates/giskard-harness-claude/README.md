# Claude Code harness adapter

`giskard-harness-claude` maps Claude Code's stream-json protocol (`claude -p --input-format
stream-json --output-format stream-json`) onto the harness-neutral types and lifecycle events
defined by `giskard-harness` and `giskard-core`.

The [Giskard specification](../../specs/giskard-specification.md) defines the owned identifier
semantics and invariants, and the
[Claude Code harness plan](../../specs/claude-code-harness-plan.md) records how the CLI actually
behaves and the milestones that build this adapter. This document describes what the adapter does
**today**, including the scope and lifetime of Claude Code-native identifiers.

**Status: milestone 2.** `ClaudeHarness` implements `AgentHarness` over **one `claude` process per
open primary thread**: `open_thread` spawns and handshakes it (fresh, `--resume`, or the same-id
respawn when the transcript is gone), `start_turn` writes the user message with inline attachments,
`interrupt` and `set_thread_name` send control requests, `set_thread_archived(true)`,
`delete_thread` and `shutdown` stop children, `list_models` answers from the freshest handshake or a
probe child, and `list_providers` reports `anthropic`. Nothing is user-reachable yet: no
`HarnessKind` names this adapter until milestone 4, which also wires `list_mcp_servers`. Milestone 3
answers approvals and server requests and adds per-turn mode, model and effort and `/compact`.
`capabilities()` reports the plan §4 matrix with `live_approvals`, `plan_build_modes`,
`per_turn_model`, `reasoning_effort` and `context_compaction` false until milestone 3, and
`mcp_status` false until milestone 4.

## Runtime ownership

- **One supervisor task per child** (`src/session.rs`) is the single owner of the child process,
  its `ClaudeMapper`, its pending control-request waiters and the thread's retained `EventLog`.
  Nothing else touches them, and none of them sits behind a lock: the façade reaches the task only
  through its bounded command channel (`StartTurn`, `Interrupt`, `Control`, `Stop`). The task
  selects over the child's stdout lines (first, so a frame already read is mapped before a new
  command is accepted), its commands, and the instance's shutdown signal.
- **The façade** (`src/harness.rs`) holds two maps behind `std` mutexes that are never held across
  an await: `children` (thread → live child: its session id, retained log, command sender, task,
  open model) and `pending` (approval / server-request id → thread and CLI `request_id`).
  `open_thread` inserts a child after its handshake; the supervisor removes its own entry when the
  child exits (a generation number keeps a stale supervisor from removing a reopened thread's
  entry); `delete_thread`, `set_thread_archived(true)` and `shutdown` take entries out before
  stopping them. A supervisor drops its thread's `pending` entries when its child exits.
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
| `can_use_tool` for most tools | `ApprovalRequested` (`Bash` → `CommandExecution`, file tools → `FileChange`, MCP tools → `McpToolCall`, else `Permission`) with `Tool`, `Blocked path` and one `Suggestion` per `permission_suggestions` entry (type and destination only), plus `MapperOutput::PendingApproval` |
| `can_use_tool` for `AskUserQuestion` | `ServerRequestReceived { method: "claude/ask_user_question" }` plus `PendingServerRequest` |
| `can_use_tool` for `ExitPlanMode` / `EnterPlanMode` | no event; a `MapperOutput::Reply` denying it ("Giskard chooses the mode per turn"), logged at `warn` |
| any other `control_request` | `ServerRequestReceived { method: "claude/<subtype>", params: request }` plus `PendingServerRequest` |
| `control_response` | `MapperOutput::ControlResponse { request_id, payload }` for the adapter's own waiter |

Session-level frames: `system/init` stores the model and permission mode and emits a `Notice` once
per non-`none` `apiKeySource` (usage is billed to that credential); `system/status` with a
`permissionMode` other than the one `set_expected_mode` recorded is a `Notice` and a `warn` with
`action = "permission_mode_drift"`; a `rate_limit_event` is a `Notice` only when its status is not
`allowed` or a window is at least 90% used; `api_retry` and `permission_denied` are `Notice`s.
The drift check covers `system/status` only. A re-emitted `system/init` (after a backgrounded
task) restores the session's mode rather than the per-turn one, so milestone 3, which sets the mode
per turn, must extend the check to `init`.

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
- **Compaction turn.** A `TurnKind::Compaction` turn (the `/compact` milestone 3 writes) emits the
  compact-boundary `Activity` and completes on the degenerate `result` as `Completed` with no agent
  message. The CLI's synthetic summary and replayed command output (`isSynthetic` / `isReplay` user
  frames) are bookkeeping and produce no item.

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
| `--permission-mode manual` | fixed until milestone 3 sets it per turn; never `--permission-prompts none`, which would deny every ask silently |
| `--model <ModelRef.model>` | an alias or a full id, verbatim |
| `--effort <ModelRef.reasoning_effort>` | only when the model ref carries one; the CLI tolerates a level the model ignores |
| `--session-id <uuid>` or `--resume <uuid>` | a fresh session (or the same-id respawn), or a resume |
| the declaration's `args` | last, so an operator can append to, never override, the protocol flags |

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
2. `get_settings`, under 10 s. `applied.model` equal to the requested model, or to the catalog's
   `resolvedModel` for it, makes `ThreadHandle.resumed_model` the requested `ModelRef`; another id
   makes it that id (with `applied.effort`) and logs `model_not_applied` at `warn`, so the server
   unwinds a provider switch the CLI did not confirm. No answer is `None` and a `warn`, never a
   failed open.
3. On resume only, `get_context_usage`, under 10 s. A positive `maxTokens` is sent as
   `ThreadUpdate::ContextWindowRestored` and seeds the mapper's window, so the first
   `TurnUsageUpdated` carries it.

The ids of `get_settings` or `get_context_usage` requests that timed out are handed to the
supervisor, so the CLI's late answer is logged at `debug` rather than as an unexpected response.

A `resume` id beginning with `task:` is refused as `Unsupported` (a sub-agent has no session; plan
§5.3), and any other id must be a UUID. With `--resume`, a child that exits before answering
`initialize` with `No conversation found with session ID` (on stderr or in the `result.errors` it
wrote) means the transcript is gone: the adapter logs `claude_resume_failed` at `warn`, respawns
with `--session-id <the same uuid>`, and opens the thread writable with the notice
`claude_resume_failed` ("Agent context was lost; started a fresh Claude Code session. History is
intact.", detail: the CLI's sentence). That respawn works only because the transcript is gone:
any other resume failure (such as `Error: Session ID … is already in use.`) is an error and is
never retried, and a failed respawn returns its own error. A handshake failure whose stderr or
`result.errors` mentions `not logged in`, `Invalid API key`, `/login` or `authentication` is
`HarnessError::Unauthenticated`; this is a best-effort substring match, since the unauthenticated
shape could not be reproduced. Every other failure is `HarnessError::Spawn` quoting the exit status,
the handshake request left unanswered (`initialize`, `get_settings` or `get_context_usage`) and the
stderr tail, which is what the browser shows.

A second `open_thread` for a thread with a live child returns that child's handle.

## Process control

- **Turns.** `start_turn` refuses a turn while the mapper has one active (`ThreadBusy`): the CLI
  would queue the second message, and the adapter never queues. `TurnStarted` is in the log before
  the line is written. A `start_turn` whose caller timed out (its reply channel closed) before the
  supervisor reached it is dropped unwritten and logged at `warn`, so the user's message never runs
  under a turn the server did not admit. A per-turn model whose provider or model differs from the
  open model is logged at `warn` (`turn_model_override_ignored`) and ignored, as are the per-turn
  effort, mode and preset, until milestone 3.
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
- **Asks.** `can_use_tool` and other inbound control requests are published as events and recorded
  in `pending`, but nothing answers them until milestone 3: `respond_approval` and
  `respond_server_request` return `Unsupported` and leave the entry in place.

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
- `src/harness.rs`: `ClaudeHarness`, the handshake, the resume fallback, the probe, and the façade
  tests against a scripted child and against `tests/fake-claude.sh`.
- `src/session.rs`: the per-child supervisor, the stop sequence, the pending-ask map, and the
  in-process `ScriptedChild` the façade tests drive.
- `src/process.rs`: `ClaudeLaunchOptions`, argv, spawning, the capped stdout reader, the stderr
  tail and exit classification.
- `src/attachments.rs`: the user message line and attachment blocks.
- `src/catalog.rs`: the `initialize.models` catalog, its descriptors and `ANTHROPIC_PROVIDER_ID`.
- `src/frame.rs`: one stdout line to a typed `Frame`, tolerant of everything the crate cannot type.
- `src/mapper.rs`: `ClaudeMapper`, the frame-to-event state machine, and its fixture-driven tests.
- `src/ids.rs`: `NativeItemKey` and the `task:` sub-agent id prefix.
- `src/log_fields.rs`: optional-field logging helper.
- [`tests/fixtures/README.md`](tests/fixtures/README.md): the recorded scenarios, the recorder's
  argv and the sanitization.
- `tests/fake-claude.sh`: a POSIX `sh` stand-in for `claude` that replays the fixtures, so the real
  process path (spawn, stderr tail, exit codes, kill) is tested without the CLI.
