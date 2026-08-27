# Sub-agent threads

Giskard represents a delegated agent as a real linked thread, not as a copied transcript row. The
child keeps its native harness thread ID and its own persisted turns while remaining owned by the
thread that spawned it.

This document describes the supported Codex event shapes, passive monitoring, prompt persistence,
direct follow-ups, ownership, and deletion behavior.

## Finding and opening children

Linked children appear in the header **Sub-agents** monitor. A transcript activity or tool row may
also show **Open linked thread**. Managed children are omitted from the primary sidebar, but opening
one directly, following a link, or reloading it restores the normal transcript view. While viewing
a child, the header shows a **Parent** button that returns to its immediate owning thread. The
button follows `parent_thread_id`, so it also works after a reload and for nested child threads.

A reverse activity from a child to its parent navigates to the existing parent. It never creates a
second thread or changes ownership.

The harness-neutral activity link is intentionally direction-neutral: Codex identifies the related
native thread but does not reliably label the relationship from the source thread's perspective.
Giskard resolves the direction from its persisted ownership graph. Automatic activity aimed at the
direct parent is therefore treated as navigation-only rather than as a failed child import;
genuinely incompatible ownership remains a warning.

The browser opens a transcript link with the Giskard parent-thread and item IDs. The server reads
the authoritative live or persisted item, extracts the native routing ID and lifecycle evidence,
and idempotently returns the linked Giskard thread. Native harness thread IDs are not included in
thread summaries or sub-agent item payloads sent to the browser.

Codex may publish the child's activity link just before its rollout becomes readable through
`thread/resume`. Giskard briefly retries that exact transient missing-rollout response and requires
the resumed native ID to equal the link's ID. A linked import never starts a fresh replacement
thread: doing so would monitor the wrong identity and could hide early commentary or a running
command until completion. Reopening a primary thread keeps its separate lost-context recovery.

## Ownership model

Persisted child metadata contains:

- `kind = subagent`
- `parent_thread_id`
- `spawned_by_turn_id`
- the native harness thread ID

Ownership is immutable after import. Giskard rejects self-links, cycles, reparenting, a child linked
under the wrong parent, and a native child whose harness-reported parent disagrees with the proposed
parent. Malformed or dangling records remain visible in the main sidebar so they can be repaired or
deleted instead of disappearing with managed children.

## Supported Codex spawning events

The Codex adapter maps both known protocols into the same harness-neutral sub-agent link:

- Legacy `collabAgentToolCall` / `spawnAgent` starts do not yet contain a child ID. The completion
  exposes the child and retains the delegated prompt. State is selected by the linked native thread
  ID, never by map order. Single-child `sendInput`, `wait`, `resumeAgent`, and `closeAgent` calls
  also update lifecycle evidence; multi-child waits stay unlinked because one transcript item
  cannot represent several child links safely.
- Current `subAgentActivity` events report actions such as `started`, `interacted`, or
  `interrupted`. Activity rows use the last non-empty agent-path component as the readable task
  name (for example, `/root/nested_reload_parent` is shown as `nested_reload_parent`) and keep the
  native child ID out of the visible copy. The event currently does not expose the delegated
  prompt.

Giskard does not decrypt or inspect Codex rollout storage to recover a missing prompt. It uses only
the fields exposed through the adapter protocol.

## Events a child produces before Giskard imports it

A child is already working before Giskard can know it exists. Codex creates the child's thread and
submits its first turn *before* it completes the parent tool call that names the child, and the
import that follows still has to read the project, validate the ownership chain, create the child's
thread record, and start its monitor. Everything Codex emits for the child in that window belongs
to the child's first turn.

Those events are kept rather than dropped. The Codex adapter binds a native thread to a `ThreadId`
as soon as Codex announces it — which the protocol guarantees happens before that thread's own
notifications are forwarded — and holds the child's events until the monitor attaches, at which
point they are replayed. Replay preserves each item's own order, not the exact interleaving Codex
produced: coalescing an item's deltas moves later ones forward to the position of that item's first
delta, ahead of unrelated items streamed in between. The import adopts the identity the adapter
already bound, so the retained events belong to the thread the server persists. See
[Native thread announcement and event
retention](../crates/giskard-harness-codex/README.md#native-thread-announcement-and-event-retention)
for the mechanism.

Two consequences are visible:

- An approval the child raises in that window is delivered to the browser once the monitor attaches,
  instead of being refused on the child's behalf.
- Everything the child completed in that window survives the wait. What is retained shrinks as the
  child works rather than growing with everything it streams: an item's streamed deltas collapse
  into one entry, and are discarded outright once that item completes, since the completion carries
  its final content. A child that finished several items before its monitor attached replays their
  completions and no streaming at all.

Retention is not unconditional, and the cases where it ends without a subscriber are worth naming
rather than glossing:

- **Deadline expiry.** A native thread Codex announces that Giskard never imports — one belonging to
  another owner, or one whose import was rejected — is retired after `PROVISIONAL_THREAD_TTL`. Its
  retained events are discarded with a warning naming the thread and how many went with it, and any
  Codex request still outstanding on it is refused, because Codex blocks until one is answered.
- **Retention released after import.** A thread the server *did* take but never attached a forwarder
  to has its retention released on the same schedule, keeping the binding. Those events are gone:
  this is the one case where a server-side failure between opening a child and forwarding it loses
  that child's opening events.
- **Never announced.** Traffic for an unknown native thread that was never announced is still
  dropped, with a warning. This is the original failure this mechanism exists to close, and it
  remains the fallback if Codex ever stops emitting `thread/status/changed` before a thread's own
  notifications.

Beyond retention, a subscriber that falls far enough behind a *live* thread can outrun the harness
channel's ring buffer and skip events. That is a separate, still-open gap: see the `Lagged` arm of
the server's event forwarder.

## Passive monitoring lifecycle

Opening or materializing a child and monitoring it are separate decisions:

| Observed evidence | Monitor behavior |
| --- | --- |
| `spawned`, `started`, `interacted`, `pending`, or `running` | Start or retain a passive monitor until the native child turn or a terminal lifecycle event arrives. Before a turn, ten minutes with no stream event releases a monitor whose terminal event was missed. |
| `interrupted`, `completed`, `failed`, `shutdown`, or `not_found` | Never start a new monitor. Wake an existing idle monitor immediately and recover terminal output when necessary. |
| Existing child reopened with no lifecycle evidence | Do not start another monitor. |

One monitor task serves every lifecycle a child is observed to start, rather than one task per
lifecycle. Forwarders are per-turn and exit at their own turn's completion, while an observation
arriving before the task releases monitor ownership is merged into its metadata instead of starting
a monitor of its own — so the exiting task is the only thing that can pick that lifecycle up. It
checks for one and releases ownership in a single step under the registration lock: doing the two
separately would leave a gap where an observation merges into a monitor that is already going away,
and the child's next turn would sit in the harness's retention with nobody to claim it. Because the
"active lifecycle observed" flag latches, the check counts observations instead, so a monitor can
tell the one it was started for from one that arrived while it was finishing.

That check steps through pending lifecycles one at a time rather than jumping to the newest, since
a forwarder ends at its own turn's completion and can only serve one. An unserved lifecycle also
outranks a terminal observation: terminal evidence says the child has finished, not that its turns
were recorded, and its fallback transcript is skipped once the thread has history — so releasing on
it would strand a turn that did happen. The replacement cannot hang on a child that really is over,
because the pre-turn wait polls the stream ahead of the lifecycle signal: it serves whatever was
retained and only then stops on the terminal flag. Cancellation still suppresses the handoff, since
it says to stop rather than that there is nothing left.

The ten-minute bound is restarted by every event. After `TurnStarted`, the forwarder waits for
normal completion regardless of how long the turn runs.

Terminal notifications are coordinated with monitor setup and teardown. A result arriving while
an idle monitor is starting or shutting down is claimed exactly once rather than being attached to
an absent or exited task. Linked evidence is processed in parent-event order, so a later terminal
observation cannot overtake an earlier active observation and leave a new idle monitor behind.
Queued native child events take priority over terminal fallback output.

## Approvals raised inside a child

A child's approval routes like any other: the harness maps it to the child's Giskard thread, the
passive monitor registers it, and answering it from the child transcript reaches the right harness.
The child is resumable, so its pending approval also survives a browser reload through the live-turn
snapshot.

What needs care is telling the user, because a managed child has no sidebar row. Three surfaces
report it:

- **The nearest visible ancestor row.** A hidden child's activity is hoisted to the closest ancestor
  that is actually rendered. That row shows the most urgent state among itself and its hidden
  descendants — an approval outranks an error, which outranks an active turn — so a blocked child is
  never masked by a busy parent. The row's tooltip names the child, and a marker distinguishes a
  hoisted state from the row's own. Walks up and down the ownership chain are bounded, so corrupted
  or cyclic metadata terminates rather than spinning.
- **The header Sub-agents monitor.** Being asked for something also marks the turn active, so
  "waiting on the user" is tracked separately from "running": the button takes a distinct state and
  the child's card reads `Waiting on you`. That covers approvals *and* server requests — Codex
  splits those, and already blurs the split itself by delivering MCP tool approvals as
  `requestUserInput`, but to the person looking at the sidebar they are one state. A card covers its
  whole owned subtree, because nested grandchildren are not listed separately.
- **A browser notification**, naming the child and its owning thread rather than an id prefix, and
  saying a sub-agent is blocked. Clicking it opens the child with the approval focused.

Because the server can materialize a child on its own, the browser may see activity for a thread it
has never listed. It refreshes its cached thread lists before naming or navigating to such a thread,
so the first approval from a brand-new child is still attributable.

A browser that was not connected when a child raised an approval is told on connect. The server
replays the set of threads still waiting on the user — before the browser subscribes to anything —
so the ancestor badge and the sub-agents monitor are correct immediately rather than only after the
blocked thread happens to be opened. Answered approvals are excluded, so a resolved one is never
re-surfaced.

That replay repaints badges every time, but alerts at most once per page load: a reconnect (tab
resume, network blip) stays silent for an approval already alerted, while a genuine reload starts a
new page session and alerts again.

## Prompts and transcript persistence

When the delegated prompt is available, Giskard persists it as `Turn.user_input` and shows one
ordered prompt row before child output. Late prompt metadata can update the live passive context
without creating a duplicate prompt row.

When the current Codex activity protocol does not expose the prompt, Giskard uses the visible
`Sub-agent turn` fallback. It does not treat inherited parent messages found in the child rollout as
the delegation prompt. Fallback state is tracked explicitly, so a real delegated prompt whose text
is exactly `Sub-agent turn` is still preserved as genuine input.

If terminal lifecycle evidence carries an output message but no native child turn was observed,
Giskard persists that message as a fallback child turn. Existing child history prevents a duplicate
fallback from being appended.

## Direct user follow-ups

An imported child is a resumable native thread, so Giskard allows direct user messages after the
delegated turn becomes idle. A follow-up creates and persists a normal turn in the child thread.

While delegated work owns the passive monitor, a direct send is rejected with
`thread_turn_active`. This prevents a user turn and the externally started child turn from racing
for the same native thread.

A direct child follow-up does not automatically send its result to the parent. It also does not
detach, promote, or reparent the child; deletion still follows the original ownership tree.

## Link-open API

The browser uses:

`POST /api/projects/{project_id}/threads/{parent_thread_id}/subagent-links/{item_id}/open`

The server resolves the item from the parent's live buffer or persisted turns. It derives the
native child ID, delegated prompt, lifecycle action/status/message, and `spawned_by_turn_id` from
that trusted item instead of accepting those values from the client. A reverse child-to-parent item
returns the existing parent. Unknown items, non-link items, invalid ownership, and mismatched native
parents are rejected.

`POST /api/projects/{project_id}/threads` remains the normal open/resume endpoint and accepts only
`thread_id` or `resume`; it cannot fabricate sub-agent ownership. Harness-observed and explicit
link-open materialization share one per-project lifecycle lock, while linked evidence from one
parent is processed through a FIFO. Concurrent attempts therefore cannot persist two Giskard
threads for one native child or apply lifecycle evidence out of order. Browser HTTP operations
waiting on that lifecycle serialization return `503 Service Unavailable` after five seconds rather
than hanging indefinitely. First-time materialization runs outside the parent event-forwarding
path; repeated activity reuses the live binding without rescanning every thread file.

Turn-scoped child events may arrive before the harness emits `TurnStarted`. The server starts the
live reconnect buffer from the first such event and reuses it when `TurnStarted` arrives, preserving
the complete in-flight transcript across a browser reload regardless of notification order.
A genuine new `TurnStarted` also replaces a stale reconnect buffer left by an interrupted
forwarder; a conflicting non-start event remains live and persistable without being mixed into the
wrong buffer.

## Deletion and recovery

Deleting a parent deletes its complete ownership subtree in leaf-first order, including native
harness threads and local transcripts. Before deleting anything, Giskard rejects the operation if
the parent or any descendant has an active turn or running task. Idle pre-turn monitors are
cancelled and awaited across the entire subtree, followed by a second active-work preflight, so a
late child event cannot recreate storage after deletion. Imports and deletion share the same
project lifecycle lock.

Codex may report that a native rollout is already absent. Only the exact matching missing-rollout
response is treated as idempotent success, allowing stale local metadata to be removed. Other native
deletion failures remain fatal and preserve the corresponding local record.
