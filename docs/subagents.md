# Sub-agent threads

Giskard represents a delegated agent as a real linked thread, not as a copied transcript row. The
child keeps its native harness thread ID and its own persisted turns while remaining owned by the
thread that spawned it.

This document describes the supported Codex event shapes, native event ownership, read-only child
behavior, prompt persistence, approvals, and deletion behavior.

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

Giskard claims the child's advertised native identity directly. Materializing or reattaching a
provider-owned child never sends `thread/resume` or starts replacement work. The claim is
idempotent for the same native/local pair, adopts the identity the adapter already minted for that
native id, and rejects a proposed Giskard id already bound to another native id.
Reopening a primary thread keeps its separate lost-context recovery.

If child frames arrive before the parent's relationship link, the adapter mints the child's final
Giskard identity from the first frame and announces it on a retained discovery stream. The server
persists a hidden orphan and installs its event owner before consuming the child's retained events.
When the parent link arrives, it classifies that same record as a sub-agent; it does not create or
rekey another thread.

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

A native identity observed before its relationship is authoritative is persisted under one final
ID as `kind = orphan`. It is hidden and read-only, so ordinary thread opens do not expose it, and it
retains explicit `unknown` model/mode values rather than parent-derived guesses. Its only
classification is an expected-revision `orphan -> subagent` update on that same ID once parent
evidence arrives.

## Supported Codex spawning events

The Codex adapter maps both known protocols into the same harness-neutral sub-agent link:

- Legacy `collabAgentToolCall` / `spawnAgent` starts do not yet contain a child ID. The completion
  exposes the child and retains the delegated prompt. State is selected by the linked native thread
  ID, never by map order. Single-child `sendInput`, `wait`, `resumeAgent`, and `closeAgent` calls
  may expose relationship links; multi-child waits stay unlinked because one transcript item
  cannot represent several child links safely.
- Current `subAgentActivity` events report actions such as `started`, `interacted`, `interrupted`,
  or `completed`. Activity rows use the last non-empty agent-path component as the readable task
  name (for example, `/root/nested_reload_parent` is shown as `nested_reload_parent`) and keep the
  native child ID out of the visible copy. The event currently does not expose the delegated
  prompt.

Giskard does not decrypt or inspect Codex rollout storage to recover a missing prompt. It uses only
the fields exposed through the adapter protocol.

## Long-lived native event ownership

Opening or materializing a child installs one coordinator and one long-lived subscriber for that
native thread. The same owner processes every native turn until the binding is explicitly retired;
parent activity does not start a second subscriber, hand ownership between tasks, or stop the owner
after a timeout.

One project event driver owns all of the harness's forwarders and serializes their ownership
transitions. Retirement is a `Detach` message to that driver. An attach that arrives while the same
thread is detaching is queued until the old forwarder exits, so a replacement subscriber cannot
overlap it and no task waits on another task's owner lifecycle.

The first event carrying a previously unseen native turn ID atomically claims that turn before any
live-buffer, browser, or persistence mutation. `TurnStarted` may arrive later. On completion the
owner commits the real native turn, clears only the matching coordinator token, discards that
turn's prompt context, and continues reading the same stream. Parent lifecycle labels establish or
refresh links only: they never synthesize a child turn or terminal result.

The harness retains a thread's events until its owner consumes them, so owner installation or
replacement cannot lose events. The retention cap (`EVENT_LOG_RETAIN_LIMIT`) is the only loss
boundary and is reported to the owner as a gap. The owner persists the received prefix as an
`Interrupted` turn with an explicit overflow message and then continues with retained events.

## Approvals raised inside a child

A child's approval routes like any other: the harness maps it to the child's Giskard thread, the
long-lived owner registers it, and answering it from the child transcript reaches the right harness.
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

Only a real native child turn creates a persisted turn. When its native event stream does not expose
the delegated prompt, Giskard uses `Sub-agent turn` as that real turn's input label. A native turn
observed on a thread that is still a hidden orphan uses `Unclassified native turn` instead, because
no parent relationship has been proven for it yet; turns already committed under that label keep it
after classification, the same way they keep their historical mode. Giskard does not derive a prompt
from inherited parent history, insert a synthetic prompt item, or persist terminal parent activity
as a fallback child turn.

## Read-only child behavior

Sub-agent threads are agent-owned and always read-only, including while idle. Direct messages,
compaction, model/mode/permission changes, rename, archive, individual deletion, and worktree
mutations such as `SavePlan` return `thread_read_only` before harness I/O. The browser disables the
corresponding composer and controls.

Read-only does not prevent resolving work the child is already waiting on. Matched approval and
server-request responses, interrupting an active child turn, terminating a command, transcript and
history reads, and navigation remain supported.

## Link-open API

The browser uses:

`POST /api/projects/{project_id}/threads/{parent_thread_id}/subagent-links/{item_id}/open`

The server resolves the item from the parent's live buffer or persisted turns. It derives the
native child ID, display metadata, and `spawned_by_turn_id` from
that trusted item instead of accepting those values from the client. A reverse child-to-parent item
returns the existing parent. Unknown items, non-link items, invalid ownership, and mismatched native
parents are rejected.

`POST /api/projects/{project_id}/threads` opens an existing persisted thread and requires its
`thread_id`; it cannot fabricate sub-agent ownership. Harness-observed and explicit link-open
materialization share one per-project lifecycle lock, while linked evidence from one
parent is processed through a FIFO. Concurrent attempts therefore cannot persist two Giskard
threads for one native child or install competing owners. Browser HTTP operations
waiting on that lifecycle serialization return `503 Service Unavailable` after five seconds rather
than hanging indefinitely. First-time materialization runs outside the parent event-forwarding
path; repeated activity reuses the live binding without rescanning every thread file.

Turn-scoped child events may arrive before the harness emits `TurnStarted`. The server starts the
live reconnect buffer from the first such event and reuses it when `TurnStarted` arrives, preserving
the complete in-flight transcript across a browser reload regardless of notification order.
A genuine new `TurnStarted` also replaces a stale reconnect buffer left by an interrupted owner; a
conflicting non-start event remains live and persistable without being mixed into the wrong buffer.

## Deletion and recovery

A graph-invalid persisted sub-agent is quarantined instead of becoming a broken ordinary sidebar
row. Its files remain on disk, the project shows a damaged-record count, and project deletion
removes it. Targeted offline inspection and removal through `giskard-admin` is deferred to a
follow-up milestone.

Deleting a primary thread deletes its complete ownership subtree in leaf-first order, including
native harness threads and local transcripts. An individual sub-agent cannot be the requested
deletion root. Before deleting anything, Giskard rejects the operation if the primary or any
descendant has an active turn or running task. Each long-lived owner is retired as its binding is
removed, so a late child event cannot recreate storage after deletion. Imports and deletion share
the same project lifecycle lock.

Codex may report that a native rollout is already absent. Only the exact matching missing-rollout
response is treated as idempotent success, allowing stale local metadata to be removed. Other native
deletion failures remain fatal and preserve the corresponding local record.
