# Claude Code harness adapter

`giskard-harness-claude` maps Claude Code's stream-json protocol (`claude -p --input-format
stream-json --output-format stream-json`) onto the harness-neutral types and lifecycle events
defined by `giskard-harness` and `giskard-core`.

The [Giskard specification](../../specs/giskard-specification.md) defines the owned identifier
semantics and invariants, and the
[Claude Code harness plan](../../specs/claude-code-harness-plan.md) records how the CLI actually
behaves and the milestones that build this adapter. This document describes what the adapter does
**today**, including the scope and lifetime of Claude Code-native identifiers.

**Status: milestone 1.** The crate holds the pure mapper from stream-json frames to `AgentEvent`s
and control replies, tested on recorded fixtures. Nothing spawns a process, nothing implements
`AgentHarness`, and no `[harnesses.<name>]` declaration can name this kind yet. `capabilities()`
already advertises the plan §4 matrix with `live_approvals`, `plan_build_modes`, `per_turn_model`
and `reasoning_effort` false until milestone 3.

## Runtime ownership

Milestone 2. Today `ClaudeMapper` is synchronous, owns all of its state, and does no I/O.

## Identifier model

| Giskard identity | Claude Code source |
| --- | --- |
| `harness_thread_id` of a primary thread | the session UUID Giskard mints and passes as `--session-id` |
| `harness_thread_id` of a sub-agent thread | `task:<tool_use_id>` of the parent's `Agent` call (`ids::TASK_ID_PREFIX`); routes are claimed in milestone 5 |
| `TurnId` | minted by Giskard at `start_turn` (`begin_turn`), or by the mapper for a continuation turn the CLI started on its own |
| `ItemId` | minted on first sight of a native key: a `tool_use` block's `id`, or `(message.id, block index)` for a text or thinking block; reused for the item's start, deltas and completion within the turn |
| `Item.harness_item_id` | the tool-use id, `<message_id>:<index>`, `compact_boundary:<uuid>`, or `user:<uuid>:<index>` for a user-frame activity |
| `ApprovalId`, `ServerRequestId` | the `control_request`'s `request_id`, a UUID the CLI mints and the reply must carry |

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

## Process control, resume and approval responses

Milestone 2 spawns and supervises the child and implements `AgentHarness`; milestone 3 answers
approvals and server requests, sets the permission mode, and sends `/compact`.

## Code and tests

- `src/lib.rs`: the public surface and `capabilities()`.
- `src/frame.rs`: one stdout line to a typed `Frame`, tolerant of everything the crate cannot type.
- `src/mapper.rs`: `ClaudeMapper`, the frame-to-event state machine, and its fixture-driven tests.
- `src/ids.rs`: `NativeItemKey` and the `task:` sub-agent id prefix.
- `src/log_fields.rs`: optional-field logging helper.
- [`tests/fixtures/README.md`](tests/fixtures/README.md): the recorded scenarios, the recorder's
  argv and the sanitization.
