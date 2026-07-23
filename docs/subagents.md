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

## Passive monitoring lifecycle

Opening or materializing a child and monitoring it are separate decisions:

| Observed evidence | Monitor behavior |
| --- | --- |
| `spawned`, `started`, `interacted`, `pending`, or `running` | Start or retain a passive monitor until the native child turn or a terminal lifecycle event arrives. Before a turn, ten minutes with no stream event releases a monitor whose terminal event was missed. |
| `interrupted`, `completed`, `failed`, `shutdown`, or `not_found` | Never start a new monitor. Wake an existing idle monitor immediately and recover terminal output when necessary. |
| Existing child reopened with no lifecycle evidence | Do not start another monitor. |

The ten-minute bound is restarted by every event. After `TurnStarted`, the forwarder waits for
normal completion regardless of how long the turn runs.

Terminal notifications are coordinated with monitor setup and teardown. A result arriving while
an idle monitor is starting or shutting down is claimed exactly once rather than being attached to
an absent or exited task. Linked evidence is processed in parent-event order, so a later terminal
observation cannot overtake an earlier active observation and leave a new idle monitor behind.
Queued native child events take priority over terminal fallback output.

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
