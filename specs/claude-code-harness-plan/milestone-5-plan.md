# Milestone 5 implementation plan: sub-agent child threads

Implements milestone 5 of [`../claude-code-harness-plan.md`](../claude-code-harness-plan.md)
(§5.3, §11). This plan is written for an implementing agent. Every file, symbol and behaviour
below was verified against `main` at `b91ceb1` (milestone 4 merged) and against Claude Code
**2.1.287** driven over a stdio pipe; the crate pins `claude-codes = "=2.1.286"`, whose task
message types carry every field this milestone reads and reject no unknown one. Line numbers are
for orientation; the symbol quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this milestone: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; every long-lived keyed map carries an `ENTITY-AUTHORITY-EXCEPTION` comment and joins the
struct whose cleanup site matches its lifetime; log assertions use `#[traced_test]` with
`logs_contain` / `logs_assert`, never a scoped subscriber; spawned tasks that log are
`.in_current_span()`; the adapter README is kept in sync with identifier mappings, lifecycle and
process control; Markdown prose is wrapped at 100 columns (table rows may run longer).

## Outcome

After this milestone a delegation made by a Claude Code thread (an `Agent` tool call) **is a
sub-agent thread in Giskard**: it appears in the header's Sub-agents monitor, its transcript holds
the delegated prompt, the child's own tool calls and results and its closing message, an approval
the child raises is answered from the child's transcript, the child can be interrupted on its
own, and it stays readable forever while never being resumable. Nothing in `giskard-server` or
`static/app.js` changes: the server's admission, ownership, read-only and approval-routing
machinery already does all of this for any harness that implements `claim_native_thread` and
emits `SubagentLink`s, which is what the adapter gains here. The commit is one unit.

## Scope

One commit, built in this order so that each step compiles on its own:

1. `--forward-subagent-text` on every child.
2. The mapper: sub-agent **routes** (a thread per `Agent` call), `SubagentLink` on the `Agent`
   item, the child's turn and items, approvals routed by `agent_id`, `stop_task` bookkeeping.
3. The supervisor: one retained log per route, routes published to the façade, asks keyed by
   their owning child, `StopTask`.
4. The façade: `claim_native_thread`, `subscribe` and `interrupt` for `task:` ids, cold routes,
   cleanup.
5. Tests, on the two fixtures recorded for this milestone and the two delegation fixtures.
6. Documentation: `docs/subagents.md`, the adapter README, the fixtures README, the spec's §4.6a
   table, the README's harness entry, the plan's §11 note.

## Non-goals

No change to `giskard-server`, `giskard-harness`, `giskard-core` or `static/app.js` (Step 4
explains why none is needed; if one turns out to be, stop and say so rather than widening). No
`terminate_command` for background shell tasks (the capability stays false; `stop_task` is wired
only as a sub-agent thread's `interrupt`). No idle reaping or supervisor state machine (milestone
6): the `stop_task` request is awaited with the existing `await_control`. No synthesized diffs (7),
no drift or headroom surfacing (8). No screenshot regeneration: nothing visible changes in the UI.

## Verified facts this milestone rests on

Recorded this session unless a fixture is named. The two new fixtures,
`crates/giskard-harness-claude/tests/fixtures/subagent-stop.*` and `subagent-ask-withdrawn.*`,
are sanitized exactly like the milestone 1 recordings and are part of this plan's commit, so the
implementer needs no CLI login to test against them.

| Fact | Consequence |
| --- | --- |
| `--forward-subagent-text` is in `claude --help` (*"Forward subagent text and thinking blocks as assistant/user messages with parent_tool_use_id set (only works with --print and --output-format=stream-json)"*). Forwarded frames are **`assistant` and `user` only**: with `--include-partial-messages` a delegation produced 64 `stream_event` frames for the primary and none with a `parent_tool_use_id` | A child's text and thinking items are started and completed by their `assistant` frame, the path `complete_block` already takes when no stream event opened the block; `on_stream` keeps dropping nothing new |
| The `Agent` `tool_use` block's `input` is `{description, prompt, subagent_type, run_in_background?}`; `system/task_started` for it carries `task_id`, `tool_use_id` (the block's id), `description`, `subagent_type`, `is_backgrounded`, `spawn_depth` and the full `prompt` (`claude_codes::TaskStartedMessage`); the child's first forwarded frame is a `user` text frame with that prompt and `parent_tool_use_id` = the block's id (`delegation` fixture, every probe) | The route is minted at the `tool_use` block, so the link the server acts on exists before any child frame; the prompt is known from the block and confirmed by `task_started` |
| A sub-agent's `can_use_tool` has the keys `agent_id, blocked_path, description, display_name, input, permission_suggestions, subtype, tool_name, tool_use_id` and **no `parent_tool_use_id`**; `agent_id` equals the `task_id` of the sub-agent's `task_started`. **The ask can arrive before the forwarded `assistant` frame that carries its `tool_use` block**: it did in four of five recordings (`subagent-stop.out.jsonl` lines 11–12, `subagent-ask-withdrawn.out.jsonl` lines 12–13), the other had the block first | Routing goes by `agent_id` → task → route; the `tool_use_id` lookup among open tool calls is only a fallback. This corrects the harness plan's §9.3 assumption that the block is always seen first (amended in this commit) |
| `stop_task {task_id}` on a `local_agent` task answers `{"subtype":"success","response":{}}` (not `null` as §3.3 recorded on 2.1.285) and produces, in this order: `task_updated {status: killed}`, `task_notification {status: stopped}`, the control response, then on the child route a `tool_result` with `is_error` and the rejection sentence plus a `user` text frame `[Request interrupted by user for tool use]`, then on the primary the `Agent` call's `tool_result` with `is_error: true` and that same text, after which the primary turn continues to its own `result` (`subagent-stop` fixture). A pending sub-agent ask is withdrawn first with `control_cancel_request {request_id}` (`subagent-ask-withdrawn` fixture) | `interrupt` on a sub-agent thread is `stop_task`; the route's terminal frames **trail** its terminal `task_updated`; the primary turn is not ended by it |
| `stop_task` on an agent task that already reached `completed` still emits `task_updated {status: killed}` for it (and kills a background shell the agent started) | A terminal update for a task already terminal is ignored, never a second completion |
| A sub-agent's own `Bash` registers a `local_bash` task with `owned_by_subagent: true` (2.1.287; absent from the 2.1.286 recordings) | Nothing to do: `local_bash` tasks never gate a turn; the field is tolerated |
| The backgrounded shape is unchanged (`delegation-interrupted` fixture, re-verified): the `Agent` call's `tool_result` ("Async agent launched successfully") and the first `result` land at once; the child's frames, `task_updated`, `task_notification`, a re-emitted `init` and the second `result` follow | The `Agent` tool item completes while the child runs; the outcome needs its own row on the parent (Step 2) |
| A child's `assistant` frames carry `message.usage` per API message, repeated on each one-block frame of the same `message.id`; `task_progress` and `task_notification` carry `usage.total_tokens` only | The child's `TurnUsageUpdated` comes from its own messages, counted once per `message.id` |
| Nesting could not be reproduced live: a general-purpose Haiku sub-agent asked to delegate read the file itself. The CLI documents that nested agents' frames carry the id of the `Agent` call that started them and `spawn_depth: N+1` | Routes form a tree keyed by `parent_tool_use_id`, tested with synthetic frames (Step 5), and the handle reports `parent_harness_thread_id = task:<outer>` for a nested route |
| Server side, from code: the forwarder sends a `Link` for a `SubagentLink` on `ItemStarted` (`ToolCallStart.subagent`) and on `ItemCompleted` (`ToolCall` / `Activity` payloads) (`registry/event_forwarder.rs:1775`, `note_owned_event`); admission calls `claim_native_thread(ThreadId::new(), native, root)`, requires the handle to echo `native`, persists the thread under **`handle.thread`**, titles it `Sub-agent: <handle.agent_name>`, seeds `current_model` from `handle.resumed_model`, and refuses a parent whose `harness_thread_id` differs from `handle.parent_harness_thread_id` (`registry/admission.rs:100`, `admit`); a persisted sub-agent thread is reopened with the same claim (`registry.rs:2020`, `ensure_subagent_thread_open`), never `open_thread` | The adapter supplies identity and a stream; a claim must adopt the id the mapper minted and must succeed for a thread whose session is gone |
| A child owner whose stream is closed exits `StreamEndedWithoutTurn`, which the driver records as a failed owner outside teardown (`registry/driver.rs:479`); the child forwarder attaches on the first event of a new turn with the label `Sub-agent turn`, and `TurnStarted` may follow (`registry/event_forwarder.rs:1660`); a `UserMessage` item is rendered as the turn's user bubble (`app.js:4427`) | A claim for a gone session binds a **cold route** with an open, empty log, not a closed stream; the child's delegated prompt is a `UserMessage` item |

## Step 1: `--forward-subagent-text` (`crates/giskard-harness-claude/src/process.rs`)

Add `"--forward-subagent-text"` to `protocol_argv()` (`process.rs:97`), after
`--include-partial-messages`. It is harmless on a probe child and this keeps "every child" literal.
Extend the argv assertion in `open_thread_handshakes_and_returns_a_subscribable_handle`
(`harness.rs:1814`) and the `session_argv` unit tests to expect it. `fake-claude.sh` ignores
argv, so it needs no change for this step.

## Step 2: the mapper (`crates/giskard-harness-claude/src/mapper.rs`)

### Routes

Today `Route` (`mapper.rs:47`) is `Primary | Task` and `route()` (`mapper.rs:1122`) drops every
frame carrying a `parent_tool_use_id`. Make the route carry its key:

```rust
pub enum Route {
    Primary,
    /// A sub-agent route, keyed by its `Agent` call's tool-use id.
    Task(String),
}
```

(`Copy` goes; keep `Clone, PartialEq, Eq, Hash, Debug`.) A route is **minted when an `Agent`
`tool_use` block is mapped** (`start_tool`, `mapper.rs:1394`), on whichever route that block
arrived (the primary, or an outer task route for a nested delegation), and lives in session state:

```rust
// in SessionState (mapper.rs:125)
// ENTITY-AUTHORITY-EXCEPTION:
// Role: One sub-agent route per `Agent` call of this session, keyed by the call's tool-use id.
// Source of truth: The `assistant` frame carrying the `Agent` tool_use block mints the entry.
// Structural reason: Forwarded frames name the call (`parent_tool_use_id`), asks name the task
//   (`agent_id`), and the server names the thread; the entry joins the three.
// Synchronization: The single adapter task that owns the child process owns the mapper.
// Invalidation/removal: Dropped by `finish_turn` of the route that spawned it once its task is
//   terminal and its own turn has completed; `child_exited` completes and drops the rest.
routes: HashMap<String, RouteState>,
// ENTITY-AUTHORITY-EXCEPTION:
// Role: Resolve a task id (an ask's `agent_id`, a `task_updated`) to its route.
// Source of truth: `system/task_started` of a `local_agent` task with a `tool_use_id`.
// Structural reason: `can_use_tool` and `task_updated` carry the task id, not the call id.
// Synchronization: as above.
// Invalidation/removal: Removed with its route.
agent_tasks: HashMap<String, String>, // task_id → tool_use_id
```

Fold the existing `task_kinds` map into this picture rather than keeping three maps: one
`tasks: HashMap<String, TaskEntry { kind: TaskType, route: Option<String> }>` keyed by task id is
enough (`route` is `Some` for a `local_agent` task whose `tool_use_id` names a minted route).

```rust
struct RouteState {
    /// Minted here; the server adopts it through `claim_native_thread`.
    thread: ThreadId,
    /// `task:<tool_use_id>`.
    harness_thread_id: String,
    /// The route the `Agent` call was made on: `Primary`, or `Task(outer)` when nested.
    parent: Route,
    /// What the handle reports as `parent_harness_thread_id`: the session id, or `task:<outer>`.
    parent_harness_thread_id: String,
    /// `input.description` of the `Agent` call, the thread's name; `input.prompt`, its first item.
    description: Option<String>,
    prompt: Option<String>,
    subagent_type: Option<String>,
    /// From `task_started`; `None` until it arrives.
    task_id: Option<String>,
    is_backgrounded: bool,
    /// The adapter sent `stop_task` for this route (`note_stop_sent`).
    stop_sent: bool,
    /// The parent's `Agent` tool call is still open (its `tool_result` has not arrived).
    call_open: bool,
    /// The route's current status as the parent's link reports it.
    status: SubagentStatus,
    /// The child's turn: the same `TurnState` the primary uses, items included.
    turn: Option<TurnState>,
    /// Per-message usage already counted (`message.id`), so repeated block frames add once.
    counted_messages: HashSet<String>,
}
```

`route(parent_tool_use_id, frame_type)` becomes: `None` → `Primary`; `Some(id)` with a minted
route → `Task(id)`; `Some(id)` with none → `None`, dropped with the existing `debug` line (an
unknown call id still must not be attributed to the primary; keep the test
`a_frame_with_an_unknown_parent_is_dropped` or its equivalent).

### Item functions take a scope

`start_tool`, `complete_block`, `complete_tool`, `activity`, `resolve_item` and `ensure_turn` read
`self.turn` and emit on `self.thread`. Give them a **scope** instead: the `(ThreadId, &mut
TurnState)` of the route the frame belongs to, resolved once at the top of `on_assistant`,
`on_user` and `on_stream` (`on_stream` is always primary: no stream event carries a parent id).
The cleanest shape is a private `enum Scope<'a> { Primary(&'a mut TurnState), Task(&'a mut
RouteState) }` or a small `fn scope(&mut self, route: &Route) -> Option<(ThreadId, &mut
TurnState)>`; the choice is the implementer's, with two constraints: the primary's behaviour and
every existing test stay exactly as they are, and the log lines keep `thread_id` (now the route's
thread for a routed frame) and gain `route = "task:<id>"` on routed frames.

### The child's turn

- **Start.** `task_started` for a `local_agent` task whose `tool_use_id` names a minted route
  records `task_id`, `is_backgrounded`, and opens the route's turn: `TurnState::new(TurnId::new(),
  TurnKind::User)` and `TurnStarted { thread: route.thread, turn }`. A routed `assistant` / `user`
  frame arriving with no route turn (a `task_started` missed) opens one the same way, logged at
  `info` with `action = "external_turn"` like the primary's `ensure_turn`.
- **The prompt.** The first `user` text block on the route whose text equals the route's `prompt`
  (from the block's `input`, or `task_started.prompt`) becomes a **`UserMessage` item**
  (`ItemStarted` + `ItemCompleted`, `ItemPayload::UserMessage { text }`, `harness_item_id =
  "user:<uuid>:<index>"`). Any other `user` text block on a route is an `Activity` as today (the
  `[Request interrupted by user for tool use]` marker).
- **Items.** The child's `tool_use` / `tool_result` / text / thinking blocks map exactly as the
  primary's, on the route's thread and turn, through the scoped functions. A nested `Agent` block
  on a route mints a route whose `parent` is this route.
- **Usage.** Each child `assistant` frame carries `message.usage`; count it once per `message.id`
  (`counted_messages`): `window_usage` = that message's `input + cache_creation + cache_read` and
  `output` (through the existing `token_usage`), `result_usage` += the same, then `emit_usage` on
  the route (a scoped `emit_usage`), with the session's `context_window()`. `TurnCompleted.usage`
  is `completed_usage()` as for the primary.
- **End.** A terminal `task_updated` (`completed`, `failed`, `killed`, `stopped`) of the route's
  task completes the route's turn **immediately**: open tool calls are completed first with
  status `"interrupted"` and empty output (no `tool_result` will complete them on an ended turn),
  open text blocks are completed with what streamed, then `TurnCompleted { thread: route.thread
  }` with `completed` → `Completed`; `killed` → `Interrupted` when `route.stop_sent` or the
  spawning turn's `interrupt_sent`, else `Failed("agent task <id> was killed")`; `failed` /
  `stopped` → `Failed(patch.error or "agent task <id> ended <status>")`. The route's `status`
  becomes `Completed` / `Interrupted` / `Failed` accordingly. A terminal update for a task whose
  route turn already ended, or for an unknown task, is logged at `debug` and ignored.

  Why immediately: a killed sub-agent's rejection `tool_result` and its interruption marker
  **trail** the terminal update (both new fixtures, the `delegation-interrupted` fixture), so
  they arrive on a route whose turn has ended. They are dropped at `debug` with `action =
  "route_trailing_frame"`, `route`, `thread_id` and the frame type: their content (a rejection
  sentence and a marker) is redundant with the `Interrupted` status, and holding the child's turn
  open for them would leave the Sub-agents card showing a running agent until the next unrelated
  frame. Do not try to defer completion to a later frame.
- **The primary's gate is unchanged**: `open_agent_tasks` / `held_result` keep deciding when the
  primary turn ends (`on_task_updated`, `mapper.rs:825`); the route completion above is added
  beside that logic, not instead of it.
- **Child exit.** `child_exited` (`mapper.rs:389`) also completes every open route turn
  (`Interrupted` when the primary sent an interrupt or the route a `stop_task`, else `Failed`,
  with the exit in the message) and drops every route.
- **Route lifetime.** `finish_turn` (`mapper.rs:1029`) of a turn drops the routes it spawned whose
  task is terminal and whose turn has ended; a route still open there (an agent task the gate let
  through because the turn failed or was superseded) is completed `Failed("parent turn ended")`
  and dropped, at `warn`. The primary turn's gate normally guarantees every spawned agent task is
  terminal when the primary finishes, so the warn marks an anomaly.

### The parent's link

- The `Agent` `ToolCallStart` (`start_tool`, where `subagent: None` sits today at
  `mapper.rs:1450`) carries `SubagentLink { harness_thread_id: "task:<id>", path: None,
  initial_prompt: input.prompt, action: Spawned, status: Some(Pending), message: None }`. This is
  the `ItemStarted` the server's forwarder turns into a `Link`, so the claim (Step 4) is made while
  the child's first frames are already being retained on the route's log.
- The `Agent` `ToolCall` completion (`complete_tool`, `mapper.rs:1560`, `subagent: None` at
  `mapper.rs:1647`) carries the link with the route's current `status` and `action`
  (`Completed` for a terminal route, `Started` for one still running: the backgrounded case) and
  clears `call_open`. Its `output` / `error` / `status` come from the `tool_result` as for any
  tool call; a killed sub-agent therefore completes the call `failed` with the interruption text,
  and a backgrounded one `completed` with the "Async agent launched" metadata text.
- When a terminal `task_updated` arrives for a route whose `call_open` is false (a backgrounded
  delegation: the call completed at launch), the mapper adds one **`Activity`** on the spawning
  route's thread and turn: `title` = `description` or `"Sub-agent"`, `detail` = `"completed"` /
  `"killed"` / `"failed"` (`patch.error` when present), `harness_item_id =
  "task_updated:<task_id>"`, `subagent` = the link with `action: Completed` (or `Interrupted`
  when killed) and the final `status`. The server treats the second link idempotently (same
  native id, same parent) and the parent transcript shows the outcome with an *Open linked
  thread* button. No `Activity` is emitted for `task_started`: the `ItemStarted` already carries
  the link, and the title comes from the claim's `agent_name`.

### Asks

`on_can_use_tool` (`mapper.rs:1677`) resolves the ask's route **before** `ensure_turn`:

1. `agent_id` names a task in `tasks` with a route → that route;
2. else `tool_use_id` names an open tool call on some route → that route (the fallback for an ask
   whose `task_started` was missed; log it at `debug`);
3. else, with an `agent_id` present but unknown → the primary, logged at `warn` with `action =
   "ask_route_unknown"`, `agent_id` and `tool_use_id`: the ask must stay answerable, and the
   primary thread is where milestone 4 attached it;
4. no `agent_id` → the primary, as today.

The `ApprovalRequested` (or the `AskUserQuestion` `ServerRequestReceived`) is emitted on the
route's thread and turn (the route's turn opened by `ensure_turn` on the route if needed), and the
`PendingApproval` / `PendingServerRequest` outputs gain `thread: ThreadId` (the route's thread) so
the supervisor records the ask under the right thread (Step 3). The denial of a plan-mode tool and
`note_denied` work per route (the `denied_tool_use_ids` set lives in the route's `TurnState`).

### `stop_task` bookkeeping

Two new public methods: `route_task_id(&self, thread: ThreadId) -> Result<Option<String>,
RouteLookup>` (the task id of a live route, `None` before `task_started`, an error for a thread
that is not one of this mapper's routes or whose turn already ended) and `note_stop_sent(&mut
self, thread: ThreadId)`. Both are the supervisor's (Step 3).

### Mapper outputs

`MapperOutput` gains:

```rust
/// A sub-agent route was minted: the supervisor creates its retained log and publishes it.
RouteOpened {
    thread: ThreadId,
    harness_thread_id: String,
    parent_harness_thread_id: String,
    /// `input.description`, the thread's name.
    agent_name: Option<String>,
},
/// A route was dropped: its log is closed and its asks are gone.
RouteClosed { thread: ThreadId },
```

`PendingApproval` and `PendingServerRequest` gain `thread: ThreadId`. Every `Event` output names
its thread already (each `AgentEvent` variant has a `thread` field; `giskard-core` has no
accessor for it and gains none here, so the supervisor matches the variants in a private helper),
which is how the supervisor picks the log.

## Step 3: the supervisor (`crates/giskard-harness-claude/src/session.rs`)

- **Route logs.** The supervisor keeps `route_logs: HashMap<ThreadId, Arc<EventLog>>` (an
  `ENTITY-AUTHORITY-EXCEPTION`: created on `RouteOpened`, closed and removed on `RouteClosed` and
  at exit). `append` (`session.rs:856`) picks `self.log` when `event.thread() == self.thread`,
  else the route's log; an event for a thread with no log is counted in `dropped_events` and
  logged at `warn` once per thread (`action = "route_log_missing"`).
- **Publishing routes.** On `RouteOpened` the supervisor also inserts a `RouteHandle` into the
  façade's shared `routes` map (Step 4): `{ harness_thread_id, log, owner: Some(self.thread),
  commands: Some(self.commands_sender.clone()), parent_harness_thread_id, agent_name, model:
  Some(self.current_model.clone()), generation }`. `SupervisorParts` therefore gains the `routes`
  map and a clone of the command sender (the façade already holds it; pass it in). On
  `RouteClosed` and at exit the supervisor removes its own routes from the map, guarded by
  `generation` exactly like the `children` removal in `on_exit` (`session.rs:1690`), and closes
  their logs. **Amended in implementation:** closing a route's log ends the sub-agent thread's
  owner as a failed owner (`StreamEndedWithoutTurn` outside teardown), and a claim landing after
  the close would bind an empty cold route. So `RouteClosed` only stops appending and drops the
  route's asks; the log stays open and published, child exit turns the child's routes cold (owner
  and commands cleared, log open), and only the sub-agent thread's own delete or archive, or
  shutdown, removes a route and closes its log.
- **Asks.** `PendingAsk` (`session.rs:129`) gains `owner: ThreadId` (the primary thread whose child
  answers it; `thread` becomes the route's or the primary's thread, whatever the mapper said).
  `remove_by_request_id` searches by `owner`; `remove_thread(owner)` becomes `remove_owner`, so a
  child's exit drops the asks of all its routes; `on_cancel_request` (`session.rs:820`) emits its
  `ServerRequestResolved` on `ask.thread`. The approval and server-request responses the
  supervisor writes are unchanged: the CLI's `request_id` is all the answer needs.
- **`ChildCommand::StopTask { thread: ThreadId, reply }`.** Ask the mapper for
  `route_task_id(thread)`: an error → `Err(Protocol("thread … is not a running sub-agent of this
  child"))`; `None` → `Err(Protocol("the sub-agent has not started yet"))`; a route whose turn has
  ended → `Ok(())` at `debug` (nothing to stop; the CLI would still emit a `killed` update for a
  completed task, which the mapper ignores, but there is no reason to provoke it). Otherwise write
  `{"subtype": "stop_task", "task_id": …}` through `await_control` under `CONTROL_TIMEOUT`, and on
  success `note_stop_sent(thread)` so the `killed` that follows reads `Interrupted`. Log at `info`
  with `action = "stop_task"`, `thread_id` (the route's), `task_id`, `harness_thread_id`. The
  withdrawal of the route's pending ask arrives as a `control_cancel_request` and takes the
  existing path.
- **Exit.** `on_exit` runs the mapper's `child_exited` (which now also completes route turns),
  appends those events to their logs, closes every route log, removes the routes from the shared
  map, and drops the asks by owner. The exit line's `pending_dropped` keeps counting them all.

## Step 4: the façade (`crates/giskard-harness-claude/src/harness.rs`)

- **`routes: Routes`** beside `children` and `pending` (`harness.rs:91`),
  `Arc<Mutex<HashMap<ThreadId, RouteHandle>>>`, with an `ENTITY-AUTHORITY-EXCEPTION` comment:
  role, reach a sub-agent route's retained log and owning child from the trait methods; source
  of truth, a supervisor inserts on the mapper's `RouteOpened`, `claim_native_thread` inserts a
  cold route; removal, `RouteClosed`, child exit, `delete_thread`, `set_thread_archived(true)`,
  `shutdown`.

  ```rust
  pub(crate) struct RouteHandle {
      pub harness_thread_id: String,
      pub log: Arc<EventLog>,
      /// The primary thread whose child carries this route; `None` for a cold route.
      pub owner: Option<ThreadId>,
      pub commands: Option<mpsc::Sender<ChildCommand>>,
      pub parent_harness_thread_id: Option<String>,
      pub agent_name: Option<String>,
      pub model: Option<ModelRef>,
      pub generation: u64,
  }
  ```

- **`claim_native_thread(thread, harness_thread_id, workspace_root)`** (the trait default is at
  `giskard-harness/src/lib.rs:670`; the server's contract is in that doc comment and in
  `registry/admission.rs:100`):
  1. `ensure_running()`; an id without the `task:` prefix is `Err(Protocol("… is not a Claude Code
     sub-agent id"))`: nothing else is ever claimed on this harness.
  2. If `routes` holds an entry whose `harness_thread_id` equals the claimed id, **adopt it**: the
     handle's `thread` is that entry's key (the mapper-minted id), whatever `thread` was proposed.
     If the proposed `thread` is itself a key bound to a *different* native id, `Err(Protocol(…))`
     naming both, as the trait requires.
  3. Otherwise the session that produced this thread is gone (a persisted child reopened after a
     restart, or a child whose parent process already exited): bind a **cold route** under the
     proposed `thread` with a fresh, open `EventLog`, no owner, no commands. The stream stays open
     and silent, which is what the server expects of a cold native thread (a closed stream is
     recorded as a failed owner, see the facts table).
  4. Return `ThreadHandle::opened(thread, harness_thread_id, workspace_root)` with `resumed_model`
     = the route's `model`, `agent_name` = the route's `agent_name`, `parent_harness_thread_id` =
     the route's, and no warning. Log at `info` with `action = "claim_native_thread"`,
     `thread_id`, `harness_thread_id`, `adopted` (bool) and `cold` (bool).

  The claim never spawns, resumes or writes anything; it is idempotent for the same id.
- **`subscribe`** (`harness.rs:1371`): a thread in `children` → its log; else a thread in `routes`
  → the route's log; else closed, as today.
- **`interrupt`** (`harness.rs:1425`): a `task:` handle → the route's `owner` and `commands`;
  a cold route → `Err(Unsupported("this Claude Code sub-agent is no longer running"))`; a live
  route → `ChildCommand::StopTask { thread }` through `self.call` under `CONTROL_TIMEOUT`. The
  server's interrupt route for a sub-agent thread (`registry.rs:1266`) calls exactly this.
- **`open_thread`** keeps refusing a `task:` resume (`harness.rs:1157`); the server never calls it
  for a `ThreadKind::Subagent` file, so the refusal is a guard, not a path.
- **`stop_thread`** (`harness.rs:405`, the `delete_thread` / `set_thread_archived(true)` path):
  for a `task:` handle, remove its `routes` entry (cold or live; a live route's log stays owned by
  its supervisor, which just loses its reader) and drop its asks, logged at `debug`. `shutdown`
  (`harness.rs:1132`) clears `routes` after stopping the children. `set_thread_name` stays a no-op
  for `task:` ids.
- **`respond_approval` / `respond_server_request`** (`harness.rs:1518`, `harness.rs:1565`) look the
  live child up by `ask.owner`, not `ask.thread`.
- **`live_children()`** is unchanged; add `live_routes()` for tests and the periodic log line if one
  exists.

### Why the server needs no change

Walk these paths before writing code, so the claims below are checked rather than trusted:

- `registry/event_forwarder.rs:1775` sends the `Link` on `ItemStarted` with a `tool.subagent`;
  `admit` (`registry/admission.rs:100`) claims, persists the file under `handle.thread`, titles it
  from `handle.agent_name`, seeds its model from `handle.resumed_model`, inherits the parent's mode
  and preset, and publishes the created thread, which is what the Sub-agents card lists.
- The child's owner installs on the retained log (`install_event_owner`) and reads the events the
  route retained since the `Agent` block, so nothing is lost to the claim's latency.
- `ensure_subagent_thread_open` (`registry.rs:2020`) reopens a persisted child with the same claim
  and installs its owner on the cold route's silent stream.
- Approvals on the child thread are registered by its owner and answered through
  `harness.respond_approval(id)` (`docs/subagents.md`, *Approvals raised inside a child*); the
  interrupt button on a read-only child calls `registry.interrupt` → `harness.interrupt(handle)`.
- P8 (milestone 4) keeps a `task:` id of a Claude declaration from matching a thread of another
  declaration.

## Step 5: tests

Mapper tests (`mapper.rs`, fixture-driven through the existing `drive` / `run_fixture` /
`completed_items` / `turn_completions` helpers; add a `route_events(outputs, thread)` helper):

1. `a_foreground_delegation_materializes_a_child_route` (`delegation` fixture): one `RouteOpened`
   with `harness_thread_id = "task:toolu_01DSgcYLdZqTSvfAwdnE2njN"` and `agent_name = Some("Read
   and find magic number")`; on the route's thread: `TurnStarted`, a `UserMessage` item with the
   prompt, the `Read` `ToolCall` started and completed, an `AgentMessage`, `TurnUsageUpdated`, and
   `TurnCompleted` `Completed` on the `task_updated` line; the primary's `Agent` `ToolCallStart`
   carries the link (`Spawned` / `Pending`, `initial_prompt` = the prompt) and its completion the
   link with `Completed`; no `Read` item on the primary; the primary completes once (the existing
   assertions of `a_foreground_delegation_is_one_turn` stay, minus "child frame produced
   nothing").
2. `a_backgrounded_delegation_reports_its_outcome_on_the_parent` (`delegation-interrupted`
   fixture): the `Agent` call completes `completed` with the link still `Started`; the route turn
   completes `Interrupted` on the `killed` line (the primary sent `interrupt`); the two trailing
   child frames produce nothing and log `route_trailing_frame` at `debug`; an `Activity` with the
   link (`Interrupted`) lands on the primary; the primary completes `Interrupted` once (existing).
3. `a_sub_agent_ask_routes_to_its_thread_by_agent_id` (`subagent-stop` fixture): the
   `ApprovalRequested` on line 11 names the route's thread and turn **although its `tool_use`
   block arrives on line 12**; `PendingApproval.thread` is the route's; after `note_stop_sent` the
   `killed` update completes the route `Interrupted`, with the open `Bash` command completed
   `interrupted`; the primary's `Agent` call completes `failed` with the interruption text and the
   link `Interrupted`; the primary turn completes `Completed` on the `result` line.
4. `a_withdrawn_sub_agent_ask_is_a_cancel_for_the_route` (`subagent-ask-withdrawn` fixture): the
   `control_cancel_request` yields `CancelRequest` for the ask's `request_id`; the route completes
   `Interrupted` after `note_stop_sent`, `Failed` without it.
5. `an_ask_for_an_unknown_agent_attaches_to_the_primary` (synthetic): `agent_id` unknown →
   `ApprovalRequested.thread` is the primary's; `logs_contain("ask_route_unknown")`.
6. `a_nested_delegation_is_a_route_under_a_route` (synthetic lines built from the `delegation`
   fixture's frames with rewritten ids): an `Agent` block with `parent_tool_use_id = A` mints route
   `B` with `parent_harness_thread_id = "task:A"`; frames with parent `B` land on `B`'s thread.
7. `a_terminal_update_for_an_ended_route_is_ignored` (synthetic): a second `killed` for the same
   task yields nothing and logs at `debug`.
8. `child_exit_completes_open_route_turns`: drive the `delegation` fixture up to the child's
   `Read`, then `child_exited("code 1")` → `TurnCompleted Failed` on the route and on the primary,
   both logs named.
9. The existing `a_frame_with_an_unknown_parent_is_dropped`-style assertion (an id no route was
   minted for) stays.

Supervisor and façade tests (`harness.rs`, with `ScriptedChild`; a step that emits the
`delegation` or `subagent-stop` fixture on the user message, `Action::EmitFixture`):

10. `claim_native_thread_adopts_the_minted_route`: open a thread, start a turn that emits the
    `delegation` fixture, read the primary's `ItemStarted` with the link, claim its
    `harness_thread_id` with a fresh `ThreadId` → the handle's `thread` is the mapper's, the
    native id is echoed, `agent_name`, `resumed_model` (the session model) and
    `parent_harness_thread_id` (the session id) are set; `subscribe` on the handle yields the
    child's `TurnStarted` first; a second claim returns the same thread; claiming with a proposed
    id already bound to another native id is `Protocol`.
11. `claim_native_thread_binds_a_cold_route_for_a_gone_session`: an unknown `task:` id → the
    proposed thread, an open stream with nothing in it (`try_recv` is `None`, and the reader is not
    closed); `interrupt` on it is `Unsupported`; `delete_thread` removes it (`live_routes()` drops
    to 0); a non-`task:` id is `Protocol`.
12. `interrupt_on_a_sub_agent_writes_stop_task` (`subagent-stop` fixture, with the scripted child
    answering `stop_task` with `{}` and emitting the fixture's remainder): the stdin line is
    `{"subtype":"stop_task","task_id":"ada9b7fee5c0a73e9"}`, the call resolves on the control
    response, the route's `TurnCompleted` is `Interrupted`; before `task_started` it is `Protocol`.
13. `a_sub_agent_approval_is_answered_through_its_owner`: the ask of the `subagent-stop` fixture is
    pending under the route's thread with `owner` = the primary; `respond_approval(Accept)` writes
    the `control_response` to the child's stdin; after the child exits the route's asks are gone.
14. `child_exit_closes_route_logs`: after EOF the route's reader returns `Closed`, `live_routes()`
    is 0, the route's pending ask is dropped (`pending_dropped` counts it, `#[traced_test]`).
15. `shutdown_clears_routes`.
16. Extend the argv assertion (Step 1).

`fake-claude.sh`: answer `stop_task` with `respond "$request_id" '{}'` and document it in the
header comment; no scenario replay is needed, the scripted child covers the process path.

No server test changes. If a server test fails because of the new events, the change is in the
adapter's output, not the server.

## Step 6: documentation

### `docs/subagents.md`

Add a section **"Claude Code: sub-agent threads without native sessions"** after *Supported
Codex spawning events*, describing: the identity `task:<tool_use_id>` of the parent's `Agent`
call (a Claude Code sub-agent runs inside its parent's session and has no session of its own);
that the child's transcript is the forwarded stream (`--forward-subagent-text`): the delegated
prompt as the turn's user message, the child's tool calls and results, its closing message; that
the thread is **permanently** read-only and never resumable (reopening it after a restart shows
its persisted history and a silent live stream; there is no session to attach); approvals routed
by the ask's `agent_id` and answered from the child like any other; interrupt = `stop_task`,
which kills the sub-agent, withdraws its pending ask and lets the parent turn continue; the
backgrounded shape (the parent's `Agent` call completes at launch; the outcome arrives as an
activity row carrying the link); nesting (a sub-agent's own delegation is a child of the child);
what is not shown (the rejection sentence and interruption marker that trail a kill are dropped
in favour of the `Interrupted` status; `task_progress` counters). Keep the Codex sections as they
are; where a paragraph says "Codex" for something that is now both, say so.

### `crates/giskard-harness-claude/README.md`

Status paragraph: milestone 5 shipped; what remains (6, 7, 8). *Runtime ownership*: the per-route
retained logs owned by the supervisor, the `routes` map in the façade, cold routes. *Identifier
model*: the `task:` row no longer says "claimed in milestone 5"; add that the route's `ThreadId` is
minted by the mapper and adopted by the claim. *Mapping keys*: the `Agent` row gets its link; add
rows for the child's prompt (`UserMessage`), `task_updated` (route completion, the backgrounded
`Activity`), the trailing frames. *Item lifecycle*: replace "no route is claimed for them in this
milestone" with the route rules. *Asks*: replace "Until milestone 5, a sub-agent's asks attach to
the primary thread's turn" with the `agent_id` routing and the fallback. Add a **Sub-agent
routes** section (mint, turn, end, trailing frames, `stop_task`, claim, cold routes, cleanup) and
list `stop_task` under process control. *Code and tests*: the new fixtures.

### `crates/giskard-harness-claude/tests/fixtures/README.md`

Two rows for `subagent-stop` and `subagent-ask-withdrawn` are already added by this plan's commit,
with the note that they were recorded against 2.1.287; keep them in sync if the recordings are
regenerated.

### `specs/giskard-specification.md`

§4.6a's table: add `Agent` tool call + `system/task_started` / `task_updated` → a sub-agent
thread (`task:<tool_use_id>`, §7 sub-agent model) with the forwarded frames as its items, and
`stop_task` → `interrupt` on a sub-agent thread. Nothing else in the spec is harness-specific
here.

### `README.md`

The Claude Code entry under *Supported harnesses* (`README.md:49`) says "Sub-agent threads,
idle-process reaping and structured diffs are not there yet"; drop sub-agent threads from that
list and add one sentence that delegations appear as linked sub-agent threads, read-only and never
resumable, pointing at `docs/subagents.md`.

### `specs/claude-code-harness-plan.md`

§11's milestone 5 paragraph gets the "implemented" sentence, as the earlier milestones have.

## Logging

Stable fields on every new line: `thread_id` (the route's thread for route events), `route`
(`task:<id>`), `harness_thread_id` (the session), `turn_id`, `task_id`, `action`. Actions
introduced here: `route_opened`, `route_closed`, `route_trailing_frame`, `ask_route_unknown`,
`stop_task`, `claim_native_thread`, `route_log_missing`. Levels: minting and closing a route,
`claim_native_thread` and `stop_task` at `info`; a trailing frame at `debug`; an ask whose route is
unknown, a route still open when its spawning turn ends, and a missing route log at `warn`.

## Verification

Before pushing: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D
warnings`, `cargo test --workspace --locked`, `cargo deny check`.

Then, on a shell with a logged-in `claude`, a `claude-code` declaration and a project:

1. Ask the thread to delegate something to a sub-agent ("Use the Agent tool to …"). The Sub-agents
   button lights up; its card names the delegation's description; opening it shows the prompt as
   the user message, the child's tool calls and its answer, read-only.
2. Delegate a command that needs approval (`touch`) under `ask_first`: the approval card appears
   on the **child** (and the parent's row is hoisted); answering it from the child lets the
   sub-agent continue.
3. Interrupt the child while its command runs: the child's turn ends `Interrupted`, the parent's
   turn continues and finishes on its own.
4. Restart the server and open the child from the Sub-agents card or its parent's activity row: the
   history is intact, the thread is read-only, interrupt reports that the sub-agent is no longer
   running, no warning is logged for its owner.
5. `RUST_LOG=giskard_harness_claude=debug`: one `route_opened` per delegation, `claim_native_thread`
   with `adopted = true` the first time and `cold = true` after the restart.

## Acceptance

- `--forward-subagent-text` is on every child's argv.
- A frame with a `parent_tool_use_id` is mapped onto its route's thread; one no route was minted
  for is still dropped, never attributed to the primary.
- The parent's `Agent` item carries the link at start and completion; a backgrounded delegation's
  outcome is an activity row with the link.
- `claim_native_thread` adopts a live route and binds a cold one; it never spawns or resumes;
  `subscribe` on a claimed handle yields the route's events; `interrupt` on a live route is
  `stop_task`.
- A sub-agent's ask is published on its thread, by `agent_id` even when the ask precedes its
  `tool_use` frame, and answered through its owning child.
- Route turns end on the terminal `task_updated`, on child exit, or with their spawning turn; a
  route's log stays open until the sub-agent thread's own delete or archive, or shutdown (turned
  cold when its child exits).
- The two new fixtures are exercised by the mapper and façade tests; every existing test passes.
- `docs/subagents.md`, the adapter README, the fixtures README, spec §4.6a, the root README and
  the plan's §11 say what the code does.
