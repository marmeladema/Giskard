# Codex harness adapter

`giskard-harness-codex` maps the Codex app-server JSON-RPC protocol onto the
harness-neutral types and lifecycle events defined by `giskard-harness` and
`giskard-core`.

The [Giskard specification](../../specs/giskard-specification.md) defines the
owned identifier semantics and invariants. This document describes how the
Codex adapter satisfies them, including the scope and lifetime of Codex-native
identifiers.

## Identifier model

Giskard-owned identifiers are durable application identities. Codex-native
identifiers are opaque protocol correlation values and process handles. Do not
substitute one category for the other.

| Identifier | Owner | Scope and lifetime | Persisted by Giskard | Purpose |
| --- | --- | --- | --- | --- |
| `ThreadId` | Giskard | Durable Giskard thread | Yes | Routes and stores a Giskard thread |
| Codex `threadId` | Codex | Codex thread store | Yes, as `harness_thread_id` | Opens or resumes the native thread and routes notifications |
| `TurnId` | Giskard | Durable Giskard turn | Yes | Identifies a turn in events and history |
| Codex `turnId` | Codex | Native Codex turn | Indirectly, through the mapped `TurnId` | Correlates native turn events |
| `ItemId` | Giskard | One logical transcript item | Yes | Correlates item start, deltas, completion, and UI state |
| Codex item `id` / tool `call_id` | Codex | One item lifecycle in its originating turn | Yes, as `harness_item_id` | Correlates native item events |
| Unified-exec `processId` | Codex | Loaded Codex thread session | Stored in command metadata, but live only in Codex memory | Controls an agent command process |
| `write_stdin.session_id` | Codex tool schema | Same lifetime as unified-exec `processId` | No additional value | Model-facing name for the unified-exec process ID |
| `command/exec.processId` | App-server client | Originating app-server connection | No | Controls a standalone `command/exec` process |
| JSON-RPC request ID | Codex and adapter | Pending request on the connection | No | Routes approval and server-request responses |
| `ApprovalId` / `ServerRequestId` | Giskard | Pending browser action | No | Routes a browser response back to the JSON-RPC request |
| Host OS PID | Operating system | Host process lifetime | No | Diagnostic only; not a supported Codex control handle |

## Mapping keys

The adapter currently maintains these identity mappings:

```text
Codex threadId
    -> Giskard ThreadId

(Giskard ThreadId, Codex turnId)
    -> Giskard TurnId

(Giskard ThreadId, Giskard TurnId, Codex itemId)
    -> Giskard ItemId

(Giskard ThreadId, Codex processId)
    -> originating Codex turnId while the command is known running
```

These registries belong to one adapter worker and are rebuilt when its Codex
app-server process is respawned. Durable Giskard IDs and completed transcript
items remain in Giskard persistence; native live-process state does not.

The turn key includes the Giskard thread because Codex does not expose a
protocol contract making turn IDs globally unique across threads. The item key
also includes the Giskard thread and turn because Codex does not expose a
protocol contract making item IDs unique across all turns and threads. These
scopes prevent copied or reused native IDs from aliasing Giskard entities.

An empty native item ID is not entered into the registry. The adapter mints a
new `ItemId` for that event because it has no native correlation key.

## Item lifecycle

Codex documents the item lifecycle as:

```text
item/started -> zero or more item-specific deltas -> item/completed
```

For one logical item, the adapter must emit the same Giskard `ItemId` for every
stage. `item/completed` is the authoritative final state and updates the item
started under the same identity.

Example:

```text
Codex item/started(thread_a, turn_1, call_7)
    -> Giskard ItemStarted(thread_A, turn_1, item_X)

Codex outputDelta(thread_a, turn_1, call_7)
    -> Giskard ItemDelta(thread_A, turn_1, item_X)

Codex item/completed(thread_a, turn_1, call_7)
    -> Giskard ItemCompleted(thread_A, turn_1, item_X)
```

Reusing `call_7` in another turn or thread produces another Giskard `ItemId`.

Some Codex notifications carry an item ID without producing a visible Giskard
item. The mapper may seed the scoped item registry from those notifications so
that later deltas and completion still resolve to the same `ItemId`.

## Sub-agent links

Codex collaboration items are mapped into harness-neutral `SubagentLink` values before they leave
the adapter. Both native spawning protocols are supported:

- legacy `multi_agent_v1` is exposed by the app server as a `collabAgentToolCall` whose tool is
  `spawnAgent`; its start event has no receiver, so the adapter links the child on completion and
  preserves the supplied prompt as `initial_prompt`. `agentsStates` is keyed by native thread id;
  the adapter reads only the linked receiver's state. Single-child `sendInput`, `wait`,
  `resumeAgent`, and `closeAgent` calls also carry lifecycle links, while a multi-child `wait`
  remains unlinked rather than attributing aggregate state to one child; and
- current collaboration v2 is exposed as a completed `subAgentActivity` with `kind = started`; the
  adapter preserves its child thread id and agent path. Its activity title uses the final non-empty
  path component as the task name and does not expose the native child id; the complete path and id
  remain in link metadata. This event does not contain the delegated prompt, so the server uses its
  explicit `Sub-agent turn` fallback rather than misidentifying an inherited parent turn as the
  task.

The server imports the child from either representation and passively monitors only lifecycle
evidence that can denote active work (`spawned`, `started`, `interacted`, `pending`, or `running`).
An explicitly active monitor has a 10-minute no-event pre-turn safety bound; any event restarts it,
and a started turn may run without that bound. Terminal evidence wakes an already-armed idle monitor
and never creates a new one; reopening a persisted child without lifecycle evidence does not monitor
it. The browser addresses links by Giskard parent-thread and item IDs; the server resolves native
routing and lifecycle metadata from its authoritative item, and native thread IDs are redacted from
browser-facing sub-agent payloads. Linked children use strict native resume: Codex can advertise a
newly spawned child milliseconds before its rollout is readable, so the adapter retries only the exact matching
`no rollout found` response for a short bounded window. It never applies the normal fresh-thread
fallback to a linked child, because that would replace the advertised routing identity and miss the
child's early commentary and command-start events. Primary threads retain the existing fresh-session
recovery when their stored native rollout is genuinely gone. Idle child threads accept direct user
follow-ups, while sends are rejected during delegated work. See
[Sub-agent threads](../../docs/subagents.md) for the complete lifecycle and ownership contract. When
opening or resuming a Codex thread, the adapter also maps
`thread.agent_nickname` to
`ThreadHandle.agent_name`; Giskard uses that harness-neutral name to title imported sub-agent
threads and their Sub-agents card entries. It maps `thread.parent_thread_id` to
`ThreadHandle.parent_harness_thread_id` as a validation signal: the server accepts a proposed
Giskard parent only when it agrees with this native parent when Codex supplies one. Reverse
child-to-parent activity therefore remains transcript navigation and cannot reparent the real
parent thread.

Codex thread deletion is idempotent only for the exact JSON-RPC `-32600` response `no rollout found
for thread id <requested-id>`. That response proves the requested native rollout is already absent,
so the adapter returns success and lets Giskard remove stale local metadata. A different native ID,
JSON-RPC code, timeout, authentication failure, or any other transport error remains an error.

## Command item ID versus process ID

A Codex command execution item can contain both:

```text
id        = logical item ID / tool call_id
processId = underlying process control ID
```

These identifiers are not interchangeable:

- The item ID updates the transcript item in its originating turn.
- The process ID sends input to or terminates the underlying process.
- A host OS PID is not accepted by the Codex process-control APIs.

Giskard retains both the Giskard `ItemId` and the Codex `processId` in running
command state. Selecting a task uses the item identity; stopping it uses the
process identity.

## Commands that outlive a turn

Each loaded Codex thread owns a unified-exec process manager shared across its
turns. Codex registers a live process in that manager before the initial command
wait yields, allowing the process to survive turn interruption or completion.

When the command remains live, Codex reports a model-facing session ID:

```text
Process running with session ID 12345
```

That value is the unified-exec `processId`. A later turn can interact with it
through `write_stdin`:

```json
{
  "session_id": 12345,
  "chars": ""
}
```

The later `write_stdin` invocation has its own tool call ID. It does not replace
the original command item identity. Output and final completion for the process
remain associated with the original command call ID and originating turn.

```text
Turn A: command item call_7 starts process 12345
Turn A: turn completes or is interrupted while process 12345 remains live
Turn B: write_stdin(session_id = 12345)
Later: item/completed(call_7, processId = 12345) updates the Turn A item
```

The adapter keeps draining Codex messages while it knows any command is running
so that this late completion can clear the running-task state.

## Background terminal discovery

`thread/backgroundTerminals/list` returns live unified-exec entries for a loaded
Codex thread. Each entry contains both:

```text
itemId    = original command item ID / call_id
processId = numeric unified-exec process ID
```

The process ID is the control handle. The item ID only links the process back to
its transcript item.

The list operation is the authoritative live inventory. A process ID retained
in old transcript history does not prove that a controllable process still
exists. Giskard currently relies on streamed command lifecycle events and does
not reconcile its running-command registry from this list operation.

## Process termination

Giskard sends `TerminateCommand { thread_id, process_id }` to the adapter. The
adapter must never implement command stop by interrupting the entire turn.

### Unified-exec commands

Use:

```text
thread/backgroundTerminals/terminate(threadId, processId)
```

The `processId` is numeric. Despite the API name, Codex registers the process
before the initial command wait completes, so this operation can terminate:

- a command still executing in the current active turn;
- a command that has yielded a session ID;
- a command that outlived its originating turn.

The operation terminates only the command process. It does not interrupt the
turn.

### Standalone app-server commands

Commands started directly with `command/exec` belong to a separate process
store and use a client-supplied process ID:

```text
command/exec/terminate(processId)
```

This operation cannot terminate an agent unified-exec command. Conversely,
`thread/backgroundTerminals/terminate` cannot terminate a standalone
`command/exec` process.

The current adapter uses a numeric process ID as the unified-exec discriminator.
For numeric IDs, it tries background-terminal termination first. If Codex
returns `terminated: false` or an error, the adapter currently tries
`command/exec/terminate`; nonnumeric IDs go directly to `command/exec/terminate`.
The fallback crosses two independent Codex process stores and therefore cannot
terminate the same unified-exec process. Tracking the process backend explicitly
or reconciling against `thread/backgroundTerminals/list` would remove this
heuristic.

## Model catalog (`model/list`)

The adapter advertises the `model_listing` capability and implements
`list_models` against the app-server `model/list` RPC. Like the MCP-status
listing, the request runs as a control command on the worker queue
(`handle_list_models`), paginating with the response cursor until exhausted.

Each returned model is mapped to a Giskard `ModelDescriptor` (`map_model`):

- **Display name** — Codex's friendly `display_name` is carried through, so the
  picker can show it instead of the raw slug.
- **Reasoning efforts** — the model's `supported_reasoning_efforts` are preserved
  verbatim (Codex `ReasoningEffort` is a bare string). Codex exposes the default
  separately from the selectable alternatives. If the alternatives list is empty
  and `default_reasoning_effort` is not `none`, the adapter inserts that default as
  the sole Giskard effort, matching the Codex TUI. An empty alternatives list with
  a `none` default maps to no reasoning-effort support.
- **Hidden models** are filtered out (only picker-visible entries are returned).
- **Empty provider** — the `model/list` catalog is provider-agnostic (a bare
  model slug, no provider), so descriptors leave `provider` empty; matching a
  catalog entry to a Giskard `(provider, model)` pair is by model id and is the
  caller's responsibility.
- **Conservative context window** — `model/list` omits the context window, so
  descriptors use the conservative default; the catalog is a source of names and
  reasoning-effort levels only, not gauge sizing.

The server overlays this metadata onto the configured/discovered model list by
model id (see `giskard-server` §8.3): config names win, and reasoning efforts
fill in for models the config did not explicitly declare.

## Runtime context window

Codex includes the effective context capacity in
`thread/tokenUsage/updated.tokenUsage.modelContextWindow`. This is the window Codex
actually applies after reserving any model-specific headroom, so it is authoritative
for the thread gauge even when it differs from a provider's raw advertised maximum.

The adapter emits `AgentEvent::ContextWindowUpdated` whenever the valid reported
value changes and suppresses consecutive unchanged repeats. Each event carries the
model selected for that turn, which the adapter records when `turn/start` is
acknowledged. Non-positive values and values outside Giskard's `u32` range are logged
and ignored without dropping the notification's token usage. The server persists
accepted values per `(provider, model)` so they survive reloads and model switches.

Existing threads initialize the gauge from Giskard's latest persisted runtime value
for the selected model. If none has been observed, they use provider/config metadata
or the conservative fallback. Codex may replay historical token usage after
`thread/resume`; that replay is not a new turn observation and is not folded into
Giskard's ledgers or context-window metadata.

## Restart and unload behavior

Unified-exec process entries are in memory and belong to the loaded Codex thread
session. Their process IDs:

- remain valid across later turns in that loaded session;
- are not persisted as resumable process handles;
- may be reused after process removal or restart;
- become stale when the Codex thread session or app-server exits.

Codex normally terminates stored unified-exec processes during thread/session
shutdown. If a host process survives an abnormal Codex exit, a new Codex process
cannot rediscover or terminate it through the background-terminal APIs.

Standalone `command/exec` processes are scoped to the app-server connection and
are terminated when their originating connection closes.

## Request and approval correlation

Codex server requests use their JSON-RPC request ID for protocol responses. The
adapter creates a Giskard `ApprovalId` or `ServerRequestId` for browser routing
and retains the original request ID in an in-memory pending-request registry.

The browser-facing ID is not a thread, turn, item, or process ID. Resolving a
request removes the pending registry entry so duplicate or stale responses fail
instead of being routed to another request.

## Code and tests

- [`src/mapping.rs`](src/mapping.rs) owns native-to-Giskard identity translation and command
  lifecycle tracking.
- [`src/lib.rs`](src/lib.rs) owns the Codex worker, JSON-RPC routing, timeouts, and process
  termination calls.
- Mapper tests assert same-lifecycle stability, cross-turn and cross-thread
  separation, and independent running commands when Codex reuses an item ID.
- Worker tests assert background-terminal and `command/exec` termination routing
  and verify that process termination never falls back to turn interruption.
