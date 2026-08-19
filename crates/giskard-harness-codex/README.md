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

## User attachments

Giskard receives browser attachments as transient `UserAttachment` values on
`UserInput::Text`. The adapter maps them before `turn/start`:

- image attachments are sent as Codex `UserInput::Image` values with
  `data:<mime>;base64,<bytes>` URLs;
- other files, including PDFs, are uploaded to the Codex app-server host with
  `fs/createDirectory` and `fs/writeFile`, then the harness-host path is appended
  to the text prompt.

Each turn uses a randomized upload directory under the harness host temp
directory, not the project workspace. The adapter removes it through
`fs/remove` when the turn ends and also cleans up partial uploads, a failed
`turn/start`, stream loss, command/control channel closure, and shutdown.
Cleanup failures are logged but do not replace the turn result. Giskard omits
raw attachment bytes from persisted history and the parsed in-memory history
cache.

## Permission presets

Giskard sends Codex `turn/start` overrides for every turn. Plan/Build mode maps
only to Codex collaboration mode (`plan` or `default`); it does not select the
sandbox. The thread permission preset selects Codex's built-in permission
profile and permission preset:

| Giskard preset | Codex `permissions` | Codex `approvalPolicy` |
| --- | --- | --- |
| `ask_first` | `:read-only` | `on-request` |
| `auto_approve` | `:workspace` | `on-request` |
| `full_access` | `:danger-full-access` | `never` |

`turn/start` must not include `sandboxPolicy` when it includes `permissions`;
Codex treats those fields as mutually exclusive.

After initialization, the adapter calls `config/read` with the project's
effective working directory and caches
`sandbox_workspace_write.writable_roots` for that app-server process. Auto
Approve turns send those paths as `runtimeWorkspaceRoots` alongside
`permissions: ":workspace"`, so configured Cargo, sccache, Docker, and similar
external workspace roots remain writable. The project working directory is
included explicitly because `runtimeWorkspaceRoots` replaces Codex's runtime
root set. A failed or unsupported config read is logged as a warning and omits
the override, leaving Codex's current thread roots unchanged. Ask First remains
read-only, and Full Access does not need additional roots.

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
- **Default model** — Codex's `is_default` is carried through, so the server can
  seed a new draft's starting model from the catalog rather than storing one per
  project.

The server overlays this metadata onto the configured/discovered model list by
model id (see `giskard-server` §8.3): config names win, and reasoning efforts
fill in for models the config did not explicitly declare.

## Provider table (`config/read`)

The adapter advertises the `provider_listing` capability and implements
`list_providers` from the same `config/read` RPC used for writable roots, run as
a control command on the worker queue (`handle_list_providers`). Codex owns
provider configuration, so Giskard reads it back instead of asking the user to
restate it in `config.toml`.

`config/read` returns the whole effective config, and the app-server `Config`
type forwards every key it does not model itself. `[model_providers]` therefore
arrives as an unmodeled key, which the adapter's own `CodexConfig` picks up — the
generated `codex-codes` types omit it, which is why it is deserialized locally.

- **Per-directory, but the table is not** — the request carries the project's
  workspace root, as every other `config/read` here does. It does not change the
  provider table: `model_provider` and `model_providers` are both on Codex's
  `PROJECT_LOCAL_CONFIG_DENYLIST`, so a project-local `.codex/config.toml` has
  them stripped (with a startup warning) before the layers are merged, trusted
  directory or not. Providers therefore always come from the user-level,
  packaged-default, or enterprise-managed layers.

  That is what makes running `auth.command` safe to do here: a checked-in config
  in a cloned repository cannot introduce a provider, so it cannot introduce a
  command for Giskard to run.
- **Built-ins are added** — `[model_providers]` holds only user-declared entries,
  so `CODEX_BUILT_IN_PROVIDER_IDS` (`openai`, `amazon-bedrock`,
  `amazon-bedrock-runtime`, `ollama`, `lmstudio`) is merged in. Without them a
  project pinned to `openai` would look like an unknown provider.
- **Key location, never the key** — a provider's `env_key` becomes
  `ProviderAuth::Env`, and `[model_providers.<id>.auth]` becomes
  `ProviderAuth::Command`, carrying the command, args, cwd and timeout so
  Giskard can run it when it needs a discovery token.
  `experimental_bearer_token` is not reported: an inline secret stays in Codex's
  config rather than being copied into another process.
- **Header locations too** — `env_http_headers` becomes `ProviderHeader` entries
  (header name + the environment variable holding its value), sorted by header
  name so two reads of an unchanged config compare equal, with entries naming an
  empty header or an empty variable dropped. They are what makes a gateway that
  admits on its own header instead of `Authorization` listable and not merely
  routable. The inline `http_headers` sibling is not reported, on the same rule
  as `experimental_bearer_token`.
- **`auth` wins over `env_key`** — Codex's own `ModelProviderInfo::validate`
  rejects a provider declaring both, so at most one is ever present. Preferring
  `auth` keeps a config Codex would refuse to load from authenticating discovery
  differently than it would authenticate a turn.
- **`refresh_interval_ms` is not read** — Giskard reruns the command whenever it
  needs a token instead of caching one, so there is no cached token for an
  interval to age out. `timeout_ms` *is* read, defaulting to Codex's own 5 s.
- **Empty is absence** — Codex defaults an omitted `name` to `""`; the adapter
  normalizes empty strings to `None`.

## Resume does not name a model

`thread/resume` treats `model`/`modelProvider` as *overrides*: Codex's
`merge_persisted_resume_metadata` returns early once either is present, so the
thread's own persisted model stops being applied. Supplying one therefore moves
an existing conversation onto a different model rather than expressing a
preference.

`OpenThreadOptions::initial_model` is consequently optional, and the adapter
omits both keys when it is `None`:

- **Importing** a native thread Giskard has no record of passes `None`, and the
  thread keeps the model Codex reports for it.
- **Reopening** a thread Giskard already tracks passes its persisted model — that
  override is also the mechanism for switching a thread's provider.
- **Starting** a fresh thread requires one (`fresh_model`); there is no existing
  thread whose model Codex could report. The resume-failed recovery path that
  starts a replacement therefore returns the resume error instead when no model
  was named.

`thread/resume` also reports `reasoningEffort`, and a reported effort wins over a
requested one — an imported thread must show the effort it is actually running,
not "Default". `thread/start` reports none, so there the request is the only
source. When Codex reports an empty model or provider the adapter logs and
returns no effective model, and the server refuses the import rather than
guessing.

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

The stdio transport is newline-delimited. If app-server stdout contains a
syntactically non-JSON line that does not begin like a JSON-RPC object, the
adapter logs the parse diagnostic, payload byte length, and an escaped preview
of up to 4 KiB. It then discards that single consumed line and continues
reading at the next frame boundary. This keeps obvious stdout contamination
from terminating every active turn and closing the project's worker while
retaining evidence for upstream diagnosis. Malformed object-like JSON,
parseable JSON with an invalid JSON-RPC envelope, and typed payload errors
remain fatal because discarding them could lose a real lifecycle event.

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

Current Codex file-change approval requests identify the associated item but do
not carry its changed paths. With current Codex app-server ordering, the adapter
retains the structured changes from the preceding `item/started` file-change
item and refreshes them from any subsequent `item/fileChange/patchUpdated`
notification. It then exposes those paths as approval metadata. If no item
changes were supplied, Giskard logs the degraded request and the browser states
that Codex did not provide the file list. An optional grant root remains
separately labeled permission-scope metadata and is never presented as a changed
target.

## Code and tests

- [`src/mapping.rs`](src/mapping.rs) owns native-to-Giskard identity translation and command
  lifecycle tracking.
- [`src/lib.rs`](src/lib.rs) owns the Codex worker, JSON-RPC routing, timeouts, and process
  termination calls.
- Mapper tests assert same-lifecycle stability, cross-turn and cross-thread
  separation, and independent running commands when Codex reuses an item ID.
- Worker tests assert background-terminal and `command/exec` termination routing
  and verify that process termination never falls back to turn interruption.
