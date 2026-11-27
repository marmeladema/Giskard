# Command uuids and turn steering for the Claude Code harness

Plan for two changes to `giskard-harness-claude`, landed in order:

- **Part A, command uuids.** Every message the adapter writes to the CLI's stdin (a turn's prompt,
  `/compact`) carries a client-chosen `uuid`, and the adapter tracks the `command_lifecycle`
  frames the CLI then emits for it. Useful on its own, and the base Part B builds on. Step 0.
- **Part B, turn steering.** `AgentHarness::steer_turn`, and `turn_steering: true` for Claude
  Code threads. A steer is taken into the running turn at its next tool boundary; one that misses
  it runs as the next turn, as Claude Code does natively. Steps 1–6.

It is written for an implementing agent. Every file, symbol and behaviour below was verified
against `main` at `bf7e65e`, `claude-codes` **2.1.331** (pinned to **2.1.333** by step 0, with no
change to the types used here), and live probes of Claude Code **2.1.330**, repeated on **2.1.332**,
run on 2026-11-25 with the adapter's protocol flags (Haiku, with the cases that depend on the model
repeated on Sonnet). Line numbers are for orientation; the symbol beside each is the thing to
find.

Read `AGENTS.md` first. Its rules that bind this plan: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; a changed failure mode gets focused tests and logs that explain what happened; log
assertions use `#[traced_test]` with `logs_contain` / `logs_assert`, never a scoped subscriber;
the adapter README is kept in sync with identifier mappings, lifecycle and protocol routing;
Markdown prose is wrapped at 100 columns (table rows may run longer).

## 1. The steering contract

The server, protocol and browser already implement steering (spec amendment 1.91, TS1–TS3); only
Codex advertises it. The amendment was written with Codex's `turn/steer` in mind, which takes the
exact expected turn and rejects a stale one, and it says so in strict terms:

- `SteerInput { thread_id, turn_id, text }` appends text to the **exact** acknowledged active
  user turn. It is "not a second `SendInput`, a new turn, or a queued follow-up" (TS1, line 533).
- "Completion races fail instead of redirecting input into a successor" (line 190). Steering does
  not "fall back to a queued follow-up when it is rejected" (line 3199).

Claude Code cannot honour that without cost (§2, *The withdrawal window*), and nothing requires
it: it describes Codex's protocol, not a product decision. **This plan amends it** (step 6): a
steer is delivered into the running turn when the harness can; when the turn ends first, the
harness may run it as the next turn, which is what Claude Code does with a message typed during a
turn. The rest of the contract stands:

- "A successful provider `UserMessage` item is the ordered acceptance evidence" (line 4267). The
  browser matches its optimistic row against a `user_message` item **in the steered turn** with
  the same text (`static/app.js:8876`, `addItem`; both the browser, `sendInput`, and the server,
  `ws.rs:647`, trim the text), and fails the row on that turn's `turn_completed` if none arrived
  (`app.js:5411`). Step 4 extends the match to the next turn.
- "A steering failure is a direct error for the initiating browser" (line 4268): `ws.rs:637` runs
  the call in a spawned task, so a slow answer blocks neither the socket nor Stop, and sends a
  failure as an `error` with `action: "steer_input"`; the browser fails the row on it
  (`app.js:4069`). `Registry::steer_turn` (`registry.rs:1243`) holds no lock across the harness
  call.
- The expected turn id stays a precondition: a steer for a turn that is no longer the active one
  is refused, as Codex refuses it.

So `steer_turn(handle, expected_turn, text)` returns `Ok` once the CLI has taken the text in,
into that turn or as the next one, each marked by a `UserMessage` item in stream order on the
turn that received it. It returns `Err` when the text certainly will not reach the model, which
takes **positive evidence**: refused before it was written, a withdrawal the CLI confirmed
(`{"cancelled": true}`), a `discarded` or `refused` frame, or a `cancelled` frame followed by
the `result` of the turn it would have joined without listing it. The absence of a frame is
never such evidence. A steer whose fate the adapter cannot establish stays unsettled (§4.6). The
one exception is a child that exits or is stopped while a steer is still unsettled: `steer_turn`
must return, so it returns an `Err` whose message says the outcome is **unknown** (§4.4), never
one that claims the text was dropped.

### The decision this supersedes

`specs/claude-code-harness-plan.md` §4 (line 465) set `turn_steering: false`: on CLI 2.1.28x a
message written mid-turn was ignored by the running turn and joined the conversation only at the
next prompt. On 2.1.330 that no longer holds (§2), and the CLI reports each queued message's fate.

### A minimum Claude Code version

The harness plan's rule was to feature-detect from `system/init.capabilities` and never compare
version strings (§0, §11 of `../claude-code-harness-plan.md`). That stays the rule for features
that come and go, but compatibility with old CLIs is not a goal: step 0 adds a floor,
`MIN_CLAUDE_CODE_VERSION` = **2.1.330**, the oldest release every behaviour below was verified on.
The handshake reads `claude_code_version` from the `initialize` answer (2.1.286's had none) and
refuses an older or unversioned CLI before writing anything else. No capability can do this
earlier: `initialize` also answers a `capabilities` list, but it is "a subset" of `init`'s, and
on 2.1.330 and 2.1.332 it omits `msg_lifecycle_v1` and the `interrupt_*` members, which `init`
alone reports once the first prompt is taken. No control request reports them either. With the
floor, Part B needs no capability gate of its own.

## 2. Verified CLI behaviour (2.1.330, repeated on 2.1.332)

Probed with `-p --input-format stream-json --output-format stream-json --verbose
--permission-prompt-tool stdio --include-partial-messages`, `--model haiku`, prompts and steers
both carrying a `uuid`. Every row below was re-probed on 2.1.332 with the same result.
`claude-codes` 2.1.331 types the frame (`CommandLifecycleMessage`,
`io/claude_output.rs:242`); the CLI bundle carries the SDK schema text quoted below.

### `command_lifecycle`

A stdin `user` message with a top-level `"uuid"` gets
`{"type":"command_lifecycle","command_uuid":<that uuid>,"state":…,"uuid":…,"session_id":…}`
frames; one without gets none, which is why the adapter has never seen the frame. Schema: `queued`
when the message enters the command queue, `started` when it drains into a turn, then exactly one
terminal state: `completed` (the consuming turn ended cleanly), `cancelled` (withdrawn by
`cancel_async_message`, swept by an interrupt with `cancel_queued: true`, or taken into a turn
that was aborted), `discarded` (the session ended with it queued) or `refused` (receive-side
policy, never queued). "Not a strict pairing": a terminal state may come without `started`.
`CommandLifecycleState` (2.1.331, unchanged in 2.1.333) has no `Refused`: it reads as
`Unknown("refused")`.

**Where it is documented.** `msg_lifecycle_v1` appears only in the CLI's bundled schema, where the
frame is marked `@internal`. The protocol around it is public: the Agent SDK TypeScript
changelog announces `command_lifecycle` (0.3.206; `refused` in 0.3.238), `cancel_async_message`
("drop a queued user message by UUID before execution", 0.2.76), the interrupt's `still_queued`
(0.3.205) and `cancel_queued` (0.3.219), and `user_message_uuids` (0.3.259). The SDK reference
documents `SDKUserMessage.uuid` and `.priority`, `user_message_uuids` on the result, and the
`interrupt_cancel_queued_v1` capability.

**Ordering, from the schema and observed:** "a command that starts a fresh turn emits
`completed` AFTER that turn's result frame; a command folded into an already-in-flight turn emits
`completed` BEFORE that turn's result frame."

### A turn's own prompt

| Input | Observed |
| --- | --- |
| A prompt | `queued` and `started` on the write, ~0.1 s **before** the turn's `system/init`; `completed` right after the `result`; `cancelled` after an interrupted turn's `result` |
| `/compact` | `queued` and `started` on the write, **3.7 s before** its `init` (which comes only once compaction is done), then `compact_boundary`, the `result`, `completed` |

`result` and the first `assistant` frame of each request carry `user_message_uuids`: "client
uuids of every user message whose prompt this turn has consumed so far" (observed `['P']`, `['C']`
for `/compact`). `claude-codes` types it (`ResultMessage::user_message_uuids`, `result.rs:593`).

### Steers (a second message written while the turn runs)

A queued message is taken into the running turn only at a **tool boundary**: right after a tool
result, before the next request. The SDK reference says the same of the default priority:
"Claude reads the message in the same turn, as soon as the tool calls it is running finish. If
the turn ends first, the message starts the next turn."

| Case | Observed |
| --- | --- |
| Written while a `Bash` call runs | `queued` at once; `started` right after the call's `tool_result`, before the next request; the reply obeys it; steer `completed` before the single `result`; prompt `completed` after it; `result.user_message_uuids` = `[P, S]` |
| Written while the model streams text before a tool call | `started` after that tool's result; one `result` |
| Two, during one tool call | both `started` at the same boundary, one reply honouring both, one `result` |
| Two, during two tool calls of one turn | each `started` at its own boundary; one `result` listing `[P, S1, S2]` |
| Written while a `can_use_tool` ask waits | after the answer, `started` right after the `tool_result`; one `result` |
| Written while an `AskUserQuestion` waits | after the answer, `started` right after the `tool_result`; one `result` |
| Written while a foreground `Agent` sub-agent runs | `started` after the parent's `Agent` `tool_result` (never delivered to the sub-agent); one `result` |
| Written at or after a tool's `tool_result` | past that boundary: it waits for the next one |
| Written while the final answer streams, or in a turn with no tool | the turn ends without it; in the same millisecond as the `result`: prompt `completed`, steer `started`, a new `init`, and **the next turn** answering the steer |
| Two, during the final answer | both `started` after the `result`: **one** next turn answering both |
| Written during a turn that stops on `max_tokens` | the CLI retries within the same turn (four requests observed), with no tool boundary; the steer runs as the next turn |
| Queued, then a plain `interrupt` | answer `{"still_queued":[S]}`; the steer runs as **the next turn** |
| Queued, then `interrupt` with `cancel_queued: true` | steer `cancelled`; answer `{"still_queued":[],"cancelled":[S]}`; nothing runs after |
| Queued, then the ask answered deny with `interrupt: true` (Giskard's `Cancel`) | as a plain interrupt: **the next turn** |
| Queued, then `cancel_async_message {S}` and a plain `interrupt`, written back to back | steer `cancelled`, `{"cancelled":true,…}`, `{"still_queued":[]}`; nothing runs after |
| Queued during an ask, then `cancel_async_message {S}` and the deny with `interrupt: true` | steer `cancelled`; nothing runs after |
| Queued, then stdin closed | not dropped: the CLI finishes the turn, takes the steer in at the next boundary (`started`, `completed`) and the reply honours it; no `discarded` |
| Taken in (`started`), then a plain `interrupt` | steer `cancelled` (taken into an aborted turn) before the `result`; nothing runs after |
| Queued after a tool call, then `cancel_async_message {S}` and an `interrupt` sent at the final `message_delta` | the withdrawal loses (`cancelled: false`, the window below); the steered turn ends `completed`; the steer starts as the next turn and the `interrupt` aborts it at once (`aborted_streaming`, steer `cancelled`). The same with `cancel_queued: true` (3 of 3 each) |
| `priority: "next"` | as with no priority |
| `priority: "later"` | not taken in at the boundary; runs as the next turn |
| `priority: "now"` | at the tool boundary the running turn is aborted (`aborted_tools`, prompt `cancelled`) and the steer starts a new turn |

### Steers that start with `/`, `!` or `#`

| Steer, written while a `Bash` call runs | Observed |
| --- | --- |
| `/cost`, `/compact` | **never taken in**: they wait and run as the next turn, as the command (`/cost` prints its report; `/compact` compacts, with its `compact_boundary`) |
| `/no-such-command please` | the same; the next turn hands the text to the model, which says the command is not installed |
| `!echo bang` | plain text in `-p` mode: taken in at the boundary; the model chose to run `echo` |
| `# remember the word OTTER` | plain text: taken in; the model chose to write a memory file |

So a slash command written during a turn runs after it, as the CLI's own interactive mode runs
it; nothing runs mid-turn.

### A withdrawn message leaves no trace

A steer withdrawn with `cancel_async_message` before it was taken in never reaches the model: a
later prompt asking for unanswered messages and a code word the steer carried got `NONE` and
`NO-CODE`, and the session transcript on disk does not contain it.

### The withdrawal window

`cancel_async_message` withdraws a queued message only until the CLI commits to running it.
After a turn has made a tool call, that commitment comes **before** the final request's
`message_delta`: a withdrawal sent there lost **14 of 14** (text and `/` steers), sent at the
final text block's `content_block_stop` or `assistant` frame it won, and sent as soon as the
steer was `queued` it always won. In a turn with no tool call it still won at `message_delta`
(6 of 6). Sent on reading the `result`, it always loses. So the adapter cannot reliably stop a
queued message from running as the next turn once the model is writing its final answer, which
is why this plan lets it run (§1). Before a Stop the withdrawal wins in every phase but that
last one, and there the `interrupt` sent with it stops the steer's turn as it starts (row above).

### Why not `cancel_queued` on interrupt

Besides uuid-stamped messages, `cancel_queued: true` "also dequeues uuid-less commands (task
notifications, other than a passive notice that a task was stopped or paused…)" (schema text).
A Stop would then silently drop a finished background task's notification, which today drives the
CLI's continuation turn (adapter README, *External turn*). Withdrawing Giskard's own steers by
uuid before a plain `interrupt` does what steering needs and nothing else (rows above).

### A steer that meets a background task's notification

When a background task finishes while a steer is queued, both are taken in at the same boundary,
and the CLI renders them as **one** system block: "The user sent a new message while you were
working: …", followed by the notification, which ends "[SYSTEM NOTIFICATION - NOT USER INPUT] …
NOT a message from the user". Haiku ignored the steer 2 of 2 times ("it arrived inside the
command output rather than as a message from you"); Sonnet honoured it 2 of 2. The lifecycle
reports `started` and `completed` either way, so the adapter cannot tell. This is the CLI's
presentation, worth reporting upstream; Giskard documents it as a limitation.

### The old `--replay-user-messages` problems do not apply

The flag stays off. Lifecycle frames do not need it, are keyed by the uuid Giskard chose (a
CLI-merged input such as a queued `<task-notification>` cannot be mistaken for ours), and were
emitted for a prompt starting with `<teammate-message `, the one text the replay echo still skips.

## 3. Part A: command uuids

### 3.1 What changes

- **Every stdin `user` line gets a fresh v4 `uuid`**: `start_turn`'s prompt
  (`user_message_line`, `attachments.rs:27`, built in `harness.rs:2167`) and the `/compact` line
  (`session.rs:2401`). `UserLine` gains `command_uuid: String`. Nothing else in the line changes;
  no opt-in field (`requeue_if_interrupted_within_ms`, `priority`, …) is sent.
- **`Frame::CommandLifecycle(CommandLifecycleMessage)`** in `frame.rs` (`parse` and `kind`).
  Without it every lifecycle frame would be `Frame::Unknown`, a `warn`.
- **A command table in the mapper's `SessionState`**: `command_uuid → CommandEntry { turn, kind:
  Prompt | Compact | Steer, state, text (steers only) }`. An entry is added when the line is
  written (`note_prompt` and the compact path gain the uuid; Part B adds `note_steer`) and removed
  on its terminal state. It outlives the turn, because a prompt's `completed` arrives after the
  `result` that dropped the `TurnState`. `child_exited` clears it.
- **The turn's command uuid** is kept on `TurnState` too, for the cross-checks below.
- **`system/init` capabilities** are captured on every `init` (`InitMessage::capabilities`):
  whether it advertises `msg_lifecycle_v1` gates the cross-checks below. A CLI at the floor
  always does; if one ever stops, the table is emptied at that `init` and nothing is tracked.
- **The minimum version** (§1): `check_claude_code_version` on the `initialize` answer, in the
  handshake and in the model probe, refusing `HarnessError::Unsupported` with the version found
  and the one required, after reaping the child.
- **`claude-codes`** moves to 2.1.333, the release current with CLI 2.1.332.

### 3.2 What the lifecycle is used for in Part A

Observation and cross-checks only; no decision moves off its current signal yet.

| Frame | Action |
| --- | --- |
| prompt / compact `queued` | `debug` |
| prompt / compact `started` | `info` `command_started` with `turn_id` and `kind` (the mapper has no clock, so no latency field). A `started` for a turn that is no longer active: `warn` |
| prompt / compact `completed` | `debug`; remove the entry |
| prompt / compact `cancelled` | `info` (expected after an interrupt); remove |
| `discarded`, `refused`, any `Unknown` | `warn` with the raw state; remove only `discarded` and `refused` |
| a `command_uuid` the table does not hold | `debug` `lifecycle_untracked` |
| a user turn's `init` acknowledgement with no `started` yet for its prompt, while the latest `init` advertises `msg_lifecycle_v1` | `warn` `prompt_lifecycle_missing` |
| a `result` whose non-empty `user_message_uuids` lacks the turn's prompt uuid | `warn` `result_uuid_mismatch` (a turn the CLI started that consumed client messages: `debug` `result_foreign_uuids`). A CLI whose `init` lacks `msg_lifecycle_v1` sends no frames: the table is emptied at that `init` and nothing is tracked after it |

An unknown state is **not** terminal: a future intermediate state must not drop a tracked entry.

Promoting `started` to the prompt acknowledgement (it precedes `init`, and for `/compact` by
seconds) is a later, separate change, once these logs show the two signals agree in real use.

### 3.3 Interrupt answers

The interrupt answer (`interrupt_receipt_v1`) carries `still_queued` and, when relevant,
`cancelled`; both interrupt writes (`session.rs:1460`, the user's Stop; `:2665`, the stop
sequence) log their lengths on the existing `interrupt` lines. The request itself is unchanged
(no `cancel_queued`, §2).

## 4. Part B: steering

### 4.1 Shape

- **`ChildCommand::Steer { turn, text, reply }`**. The façade's `steer_turn` refuses a `task:`
  handle as `Unsupported`, enqueues with `enqueue` (never `enqueue_respawning`: a reaped child has
  no active turn) and awaits the reply **without** `answer`'s fixed timeout (`harness.rs:364`);
  the supervisor guarantees an answer (§4.6).
- **The steer line** is `user_message_line(text, &[])` with its own fresh `uuid`, as in Part A,
  written at once. No `priority` is sent: the default is the folding behaviour wanted, and
  `"now"` would abort the turn.
- **The mapper owns steer state** in the command table, because the `UserMessage` item must be
  emitted in stream order at `started`. The supervisor owns the reply channels, keyed by command
  uuid, and settles them from `MapperOutput::SteerSettled { command_uuid, outcome }`.
- **The text is sent as typed.** A steer starting with `/` runs as a command after the turn, as
  the CLI runs it interactively (§2); `!` and `#` are plain text in `-p` mode. Nothing is refused
  or escaped for its first character.

### 4.2 Accepting a steer (supervisor, serving phase)

Refuse with `HarnessError::Protocol` (reported as `turn_steer_failed`) unless, in this order:

1. the mapper's active turn is `expected_turn` (else "cannot steer turn X: the active turn is Y"
   or "…: no active turn", Codex's wording, `harness-codex/src/instance.rs:695`);
2. it is a `TurnKind::User` turn the adapter started (not external, not a compaction) and its
   prompt is acknowledged. A steer written before the prompt is taken in could be batched with it
   and its `UserMessage` would precede the prompt's, which the browser's ordered matching
   (`browser_reconciles_identical_delayed_initial_echo_before_steering_echo`) does not expect.

No capability check: the minimum version (§1) guarantees the lifecycle. The stopping phase
refuses as it refuses every command. Each refusal logs `debug` with `action = "steer_turn"` and
the reason.

Then write the line, `note_steer(command_uuid, turn, text)`, keep the reply, and log `info`
`steer_written` with `turn_id`, `command_uuid` and `bytes`, never the text.

### 4.3 Mapping a steer's lifecycle

| State | Its turn still active | Its turn already completed |
| --- | --- | --- |
| `queued` | record; `debug` | record; `debug` |
| `started` | **taken in**: `UserMessage` on that turn; settle `Ok`; `info` `steer_started` | **the next turn** (§4.5): `UserMessage` on it; settle `Ok`; `info` `steer_started` with `next_turn = true` |
| `completed` after `started` | `debug`; remove | `debug`; remove |
| `completed` with no `started` | **consumed**: the CLI reports no `started` but the turn that took it in ended ("not a strict pairing", §2). Settle `Ok`; emit its `UserMessage` on the active turn, with `warn` `steer_confirmed_late` (its position in the turn is unknown); then remove | settle `Ok`; emit its `UserMessage` on the turn whose `result` listed it in `user_message_uuids`, else on the turn just completed, with the same `warn`; then remove |
| `cancelled` after `started` | `info` (the model saw it; its turn was aborted); remove | same |
| `cancelled` with no `started` | if the adapter's own withdrawal of it was confirmed: **not taken in**, settle `Err` (§4.4), `info` `steer_dropped`, remove. Otherwise keep it unsettled until the next `result` (an aborted turn's `cancelled` frames precede its `result`, §2): listed in its `user_message_uuids`, it was taken into that aborted turn, handled as `completed` with no `started`; not listed, it was not taken in, settle `Err`. A child exit first gives the unknown outcome | same |
| `discarded`, `refused` | settle `Err` (§4.4); `info` `steer_dropped`; remove | same |
| `Unknown(other)` | `warn`, keep waiting | same |

An entry is removed only once its steer is settled and, if it was taken in, its `UserMessage`
emitted: a terminal state never drops an entry the adapter has not yet accounted for. The table
also remembers, per uuid, whether a `result` listed it and whether a withdrawal was confirmed, so
the rows above can decide.

The item's `harness_item_id` is `user:<turn_id>:steer:<command_uuid>`, its text the noted text. It
goes through `user_message` (beside `acknowledge_prompt` in `mapper.rs`), so it is an ordinary
user item in live state and history. `answers_the_prompt` and `acknowledge_prompt` are untouched.
At a `result`, a started steer missing from a non-empty `user_message_uuids` is `warn`
`steer_unconfirmed` (diagnostic only).

### 4.4 Error wording

| Cause | `Err` |
| --- | --- |
| withdrawn by Stop or `Cancel`, confirmed by the CLI (§4.5) | `Protocol("turn X was stopped before Claude Code took the steering input")` |
| withdrawn by the backstop, confirmed by the CLI (§4.6) | `Protocol("Claude Code did not report the steering input; it was withdrawn")` |
| `discarded` / `refused` | `Protocol("Claude Code dropped the steering input (<state>)")` |
| child exit / stop, the steer neither started nor settled by positive evidence (only `queued`, or no frame at all) | `Err("Claude Code exited before reporting the steering input; it may have reached the model")` (§4.6) |

### 4.5 The next turn, and Stop

**A steer that misses its turn runs as the next turn.** Its `started` arrives after the turn's
`result`, before the next turn's `init`; that turn's frames carry `user_message_uuids` naming
only steers. The mapper opens it through `ensure_turn` at that `started` (an external turn, `info`
`external_turn`, which the server claims as it claims a continuation), and emits each merged
steer's `UserMessage` on it. A `/` steer's turn is the command's own (`/cost`'s report, or a
compaction with its `compact_boundary` activity, mapped as on any turn the CLI starts). The turn
runs with the mode and model the previous turn set: no per-turn settings are applied to a turn
Giskard did not start.

**Stop withdraws what has not been taken in.** A plain `interrupt` leaves queued messages to run
as the next turn (§2), and so does a `Cancel` answer to an ask. So before writing `interrupt`
(the user's Stop, `session.rs:1460`; the stop sequence, `:2665`) and before a `Cancel` answer
(deny with `interrupt: true`, `session.rs:475`), the supervisor writes one `cancel_async_message
{message_uuid}` per unstarted steer of the thread, each with a waiter under `CONTROL_TIMEOUT`.
stdin is ordered, so they reach the CLI first; both orders were verified. Settlement comes from
the lifecycle frame (`cancelled` arrives before the answer); the answer is logged
(`steer_withdrawn`, `cancelled` true/false).

**A withdrawal can lose**, and then the steer is not gone. Two cases, both settled by the
lifecycle and not by the answer:

- The steer was already taken into the turn being stopped (`started` before its `result`): it
  is part of that turn and is `cancelled` with it. Already settled `Ok`.
- The CLI had committed to running it as the next turn but had not yet reported `started` (the
  final `message_delta` of a turn that made a tool call, §2). The steered turn completes, the
  steer **starts as the next turn, and the `interrupt` aborts that turn at once**. Nothing can
  prevent this: `cancel_queued: true` does not either. So the mapper treats it as what the user
  sees: when a steer `started` after an interrupt the adapter sent and no turn was active to
  receive it, the next turn opens with the steer's `UserMessage`, is marked as interrupted
  (`note_interrupt_sent` carried over), and completes `Interrupted` on its aborted `result`.
  The steer settles `Ok` (it reached the model, and its text stays in the conversation inside
  that aborted turn). `info` `steer_after_stop`.

### 4.6 Every steer is answered

The façade waits without a timeout, so the supervisor settles every reply:

- by its lifecycle frame (§4.3);
- at child exit or a stop (archive, delete, shutdown, reap), the unknown-outcome `Err` of §4.4 for
  every steer still unsettled: `queued` proves it was queued once, not that it still was. Closing
  stdin does not drop a queued steer (§2: the CLI finishes the turn and takes it in), so the stop
  sequence settles it first: it withdraws unstarted steers before its `interrupt` (§4.5), and a
  confirmed withdrawal settles each as dropped. Only a child that dies on its own, or a
  withdrawal that was not confirmed, leaves an unknown outcome;
- **backstop**: a steer that has seen no lifecycle frame `NOTIFICATION_GRACE` (2 s,
  `session.rs:35`) after it was written means the protocol changed (the CLI emits `queued` on the
  write). The supervisor logs `warn` `steer_lifecycle_missing` and withdraws it. Only a confirmed
  withdrawal (`{"cancelled": true}`) settles it `Err`. Otherwise its fate is unknown, so it
  stays tracked and unsettled: a later lifecycle frame settles it as usual, and so does a
  `result` whose `user_message_uuids` lists it (`Ok`, its `UserMessage` emitted on that turn
  with `warn` `steer_confirmed_late`, since its position in the turn is unknown). If the child
  exits or is stopped first, `steer_turn` returns the unknown-outcome `Err` of §4.4, never the
  dropped one, and logs `warn` `steer_outcome_unknown`. A steer that was `queued` waits for its
  `started` or terminal state however long its turn runs. The deadline is a loop input of the
  supervisor, as the notification grace is.

A caller that gave up (reply channel closed) is logged as `log_caller_gave_up` does
(`session.rs:1533`); the steer stays tracked so its item lands where the CLI put it.

### 4.7 Busy and idle

- `busy()` (`session.rs:1545`) also answers `ThreadBusy` while any steer is unsettled, so a
  `start_turn` landing between a `result` and the next turn's `started` is refused rather than
  merged by the CLI into the steer's turn.
- `busy_with()` (`session.rs:2425`) reports `"steers"`, so such a child is never reaped.

### 4.8 Capability

`capabilities().turn_steering = true`. The server samples the static flag at open; the minimum
version (§1) means no open thread runs on a CLI without the lifecycle.

## 5. What the browser sees

| Situation | Behaviour, after step 4 |
| --- | --- |
| Steer taken at the next tool boundary | row pending until the `UserMessage` in the steered turn, then confirmed in place |
| Steer that runs as the next turn | at the steered turn's `turn_completed` the row stays pending ("will run next"); the next turn's `UserMessage` with the same text confirms it as that turn's prompt |
| Stop or `Cancel` while a steer waits | the `steer_input` error fails the row; nothing runs afterwards |
| Stop just as the turn ends, after a tool call | the steer appears as the next turn's prompt, and that turn shows as interrupted |

Limitations: one pending steer per tab (TS3); a steer lands only at a tool boundary, so it can wait
for a long `Bash` or sub-agent, and in a turn that answers without another tool call it runs as
the next turn; the steer the next turn receives is not itself steerable, since it is a turn
Giskard did not start (the server steers only an acknowledged user turn); on Haiku, a steer taken
in together with a background task's notification may be ignored (§2).

## 6. Steps

Each step is one commit with its tests.

### Step 0: Part A, command uuids

§3 in full, landed and shipped before any steering work. It records its own fixtures first, with
the CLI version current at implementation time, the adapter's argv (`protocol_argv` +
`--permission-mode` + `--model haiku`), uuid-stamped lines, and the conventions of
`tests/fixtures/README.md` (a row per scenario there):

| Fixture | Scenario |
| --- | --- |
| `lifecycle-prompt` | a text turn, then `/compact`: prompt and compact lifecycles around `init` and `result` |
| `lifecycle-interrupted` | a prompt interrupted during a `sleep` call: prompt `cancelled` after the `result` |

Then: the uuid on prompt and compact lines, `Frame::CommandLifecycle`, the command table, init
capabilities, the Part A logs and cross-checks, the interrupt-answer logging, the minimum version
and the `claude-codes` bump. `fake-claude.sh` and the scripted `initialize` report
`claude_code_version` 2.1.332 (the 2.1.286 recording has none); `FAKE_CLAUDE_VERSION` overrides it,
`none` drops it. Teach `tests/fake-claude.sh` to emit `queued`/`started` on a uuid-stamped line and
`completed` after the `result`. Tests: frame parsing (each state, `refused` as `Unknown`, a missing
`command_uuid` as `FrameError::Untyped`); the table across `result` (a prompt's `completed` after
its turn is gone); `lifecycle-prompt` and `lifecycle-interrupted` mapper runs with no `warn`; each
cross-check `warn` from a doctored frame; the line JSON carries a uuid; no command tracked on a CLI
without the capability; version parsing (numbers, not strings: 2.1.1000 is newer than 2.1.330); an
older and an unversioned CLI refused at the handshake, in the probe, and through the real process.
README: *Identifier model* (the command uuid), *Frames the crate cannot type*, *Process control*
(the lifecycle and what it is used for), and the fixtures table.

### Step 1: steering fixtures

Recorded before the steering code, since steps 2–3 test against them, under the same conventions
as step 0's.

| Fixture | Scenario |
| --- | --- |
| `steer-folded` | `sleep 6` call, steer 1 s in: taken in, one `result` |
| `steer-folded-two` | two steers during one call |
| `steer-two-boundaries` | two steers at two tool calls of one turn |
| `steer-during-ask` | steer while a `touch` ask waits, then allow |
| `steer-subagent` | steer while a foreground `Agent` runs |
| `steer-next-turn` | steer during a no-tool answer: it runs as the next turn |
| `steer-next-turn-merged` | two steers during the final answer: one next turn |
| `steer-slash` | `/compact` written during a `Bash` call: it runs as the next turn, as the command (`/compact` rather than `/cost`, whose output is the recording account's usage) |
| `steer-withdrawn-interrupt` | steer queued; withdrawal, then a plain `interrupt`: nothing after |
| `steer-withdrawn-cancel` | steer queued during an ask; withdrawal, then deny with `interrupt: true`: nothing after |
| `steer-folded-interrupted` | steer taken in, then `interrupt` |
| `steer-stop-late` | steer queued after a tool call; withdrawal and `interrupt` at the final `message_delta`: the withdrawal loses, the steer's next turn is aborted |

### Step 2: steer mapping (mapper)

`note_steer`, §4.3, `MapperOutput::SteerSettled`, the next turn opened at a steer's `started`
(§4.5), `MapperOutput::WithdrawSteers` for the supervisor, the `user_message_uuids` check. Mapper
tests from the step-1 fixtures: each scenario's items land on the right turn in stream order; the
next turn opens at `started` with the steers' `UserMessage`s; a `/` steer's next turn maps as the
command; an unknown uuid; an `Unknown` state keeps the steer.

### Step 3: `steer_turn` end to end

`ChildCommand::Steer`, §4.2, the reply map, the withdrawals before every interrupt and `Cancel`,
§4.6, §4.7, the façade method, the capability. Extend `fake-claude.sh` (a trigger that folds a
pending steer before the `result`, one that starts it after; `cancel_async_message` answering and
emitting `cancelled`). Tests: `Ok` at `started`, taken in and as the next turn; each refusal;
`Err` on a confirmed withdrawal or `discarded`; a lost withdrawal at Stop (the
next turn opens with the steer and completes `Interrupted`); the backstop (withdrawn: `Err`; not
withdrawn: unsettled, then `Ok` from `user_message_uuids` or the unknown-outcome `Err` at child
exit); `completed` and `cancelled` with no `started`, the latter settled only at the next
`result` (listed or not) or by a confirmed withdrawal; child exit with a steer only `queued`
(unknown outcome); a stop with it queued (withdrawn and confirmed: dropped); withdrawals
written before `interrupt` and before a `Cancel` answer (ScriptedChild asserts line order);
`ThreadBusy` from `start_turn` while a steer is unsettled; no reap meanwhile. Log assertions for
`steer_written`, `steer_started`, `steer_dropped`, `steer_withdrawn`, `steer_after_stop`,
`steer_lifecycle_missing`, `steer_confirmed_late`, `steer_outcome_unknown`.

### Step 4: browser, a steer that runs as the next turn

Today `turn_completed` fails a pending steer (`app.js:5411`, `failPendingSteer`). Instead it keeps
it pending, marked as waiting for the next turn; the first `user_message` of the next turn with
the same text confirms the row as that turn's prompt (`addItem`, beside the steered-turn match at
`app.js:8876`); an error with `action: "steer_input"` still fails it, and so does the next turn's
`turn_completed` without a match. Codex is unaffected: a Codex steer that loses the completion race
fails with that error. `tests/ui.rs` source assertions (as
`browser_reconciles_pending_steering_without_clearing_the_active_turn`), and an e2e case through
`tests/e2e/run.sh` with the replay harness: a scripted steer confirmed in the next turn.

### Step 5: server-level test

A `giskard-server` test driving a Claude thread through `fake-claude.sh`: `SteerInput` on the
running turn yields a same-turn `user_message` in the live snapshot and the persisted turn; a
steer that runs as the next turn yields an external turn whose persisted items carry the steer's
`user_message`; a steer withdrawn by Stop yields a `turn_steer_failed` error and nothing after.

### Step 6: documentation and spec

- Adapter README *Process control* gains **Steering**; *Mapping keys* the `started` rows; the idle
  list gains steers.
- `specs/claude-code-harness-plan.md` §4: `turn_steering` **true**, citing this plan, keeping the
  2.1.28x observation as history.
- `specs/giskard-specification.md`: amendment 1.91 (line 186), TS1 (line 533) and the *Steer*
  rule (line 3196) say a steer is delivered into the running turn when the harness can, and
  otherwise may run as the next turn; the expected-turn precondition stays. TS2 gains the Claude
  mapping (uuid-stamped stdin message, acceptance at `command_lifecycle` `started`, withdrawal
  before an interrupt). TS3 gains the browser's next-turn confirmation.
- `lib.rs` `capabilities`: drop `turn_steering` from the "false per plan §4" comment (line 49)
  and assert it true.

## 7. Risks

- `command_lifecycle` and `cancel_async_message` are announced in the Agent SDK changelog but
  `@internal` in the CLI's schema, and `msg_lifecycle_v1` is documented nowhere public. Part A's
  cross-checks surface drift in real use before steering depends on it; the missing-lifecycle
  backstop bounds the damage; the fixtures pin the behaviour, and the minimum version moves only
  after the probes in §2 are repeated on the new floor.
- The Stop withdrawal relies on stdin order: a `cancel_async_message` written before the
  `interrupt` is handled first. Verified for both the user's Stop and a `Cancel` answer, in every
  phase but the end of a turn that made a tool call, where the steer's next turn is aborted
  instead (§4.5).
- Whether a steer lands in the running turn or the next depends on timing the user does not see.
  The browser shows both outcomes (§5); a steer is never lost silently.
- The CLI's rapid-follow-up preemption (`system/turn_preempted`) only runs for a consumer that
  declares `rapidFollowupPreempt` on `initialize`. The adapter sends a bare `initialize`
  (`harness.rs:1060`) and must keep it that way: the option would stop a turn for a steer instead
  of folding it.
