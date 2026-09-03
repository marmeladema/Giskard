# HTTP / WebSocket API

The browser (and any client) drives everything through a small REST surface plus one multiplexed
WebSocket. Highlights: `POST /api/login`, `POST /api/logout`, `GET /api/ws-ticket`, `GET /api/ws`,
`GET/POST /api/projects`, `GET/DELETE /api/projects/{id}`, `GET/POST
/api/projects/{id}/threads`, `POST /api/projects/{id}/threads/start`, `DELETE
/api/projects/{id}/threads/{thread_id}`, `POST
/api/projects/{id}/threads/{parent_thread_id}/subagent-links/{item_id}/open`, `PATCH
/api/projects/{id}/threads/{thread_id}/title`,
`POST /api/projects/{id}/threads/{thread_id}/archive`,
`GET /api/projects/{id}/threads/{thread_id}/history`,
`GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/diffs/{diff_id}`,
`GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}/command-output`,
`GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}/command-output-links`,
`GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}/tool-output`,
`GET /api/projects/{id}/threads/{thread_id}/deletion-impact`,
`GET /api/projects/{id}/models`,
`GET /api/tokens`, `GET /api/projects/{id}/tokens`,
`GET /api/projects/{id}/threads/{thread_id}/highlight|raw|image`, `POST
/api/projects/{id}/threads/{thread_id}/linkify`, `POST
/api/projects/{id}/threads/{thread_id}/render`,
`GET /api/projects/{id}/git/status`, `GET /api/projects/{id}/git/diff`, `GET /api/browse`, `POST
/api/browse/mkdir`, `GET /api/projects/{id}/mcp`, `POST /api/projects/{id}/mcp/reload`, and `POST
/api/projects/{id}/mcp/oauth-login`. Wire types are defined once in `giskard-proto`. See
[§13.6](../specs/giskard-specification.md) for the message protocol.

`GET /api/ws-ticket` returns the short-lived `ticket` and `ui_version`, the content identity of the
server's embedded JavaScript. The browser compares `ui_version` with the identity embedded in its
loaded page before opening or reopening a WebSocket. A mismatch means the tab predates a server
upgrade, so it stops reconnecting and asks the user to reload before version-sensitive messages can
reach stale JavaScript.

`POST /api/projects` takes a name and a directory; there is no `default_model`. A project record
stores no model at all. The model a new thread starts on is derived from the project's catalog when
the draft opens (the harness's default when it marks one, else the first entry), so it tracks the
current provider and harness configuration rather than caching a choice that can go stale.
`GET /api/projects/{id}/models` is the only model-listing endpoint: the declared
`[[providers.<id>.models]]` entries, plus each listing-enabled provider's `/v1/models` discovery and the
harness's own catalog, with unknown provider ids and per-provider discovery failures reported in
`warnings`. There is no project-less equivalent. Both discovery and the harness catalog need a
provider's endpoint, which only a harness knows, and there is no harness until a project is open —
so a project-less list could only ever repeat `config.toml` back, which is why neither
`GET /api/models` nor `POST /api/models/refresh` exists. The thread picker's reload button re-runs
this endpoint for the active project.

`POST /api/projects/{id}/threads/start` takes `git_strategy`, which decides where the thread's
working tree comes from: `shared` (the project's own checkout — the default, and what an omitted
field means) or `worktree` (a linked Git worktree of its own, §7.1). It is an enum rather than a
flag so the set can grow, and an unrecognized value is rejected rather than treated as the default:
a client that asked for isolation and was silently given the shared checkout would have no way to
tell.

`POST /api/projects/{id}/threads/start` creates the durable thread from the first user message or
attachment set, persists a deterministic title generated from the prompt or first attachment name,
and returns the title with the new thread and turn identifiers. The request accepts optional
transient attachment payloads; Giskard validates them and does not persist raw attachment bytes.
Image MIME types must match PNG, JPEG, GIF, or WebP file signatures. Raw bytes are also redacted
before turns enter the parsed in-memory history cache. The Codex adapter transfers non-image files
into a randomized per-turn directory under the harness host's temporary directory. It removes the
directory after turn completion, upload/start failure, stream loss, channel closure, or shutdown;
it never writes uploads into the project workspace.

`POST /api/projects/{id}/threads` requires `thread_id` and opens that existing persisted local
thread. Linked transcript items use the dedicated parent/item endpoint above; the server resolves
native routing, ownership, provenance, prompt, and lifecycle evidence from its authoritative item
rather than accepting those fields from the browser. The parent thread must already be open with a
live event owner; otherwise the linked-item open request returns `409 Conflict`. Thread summaries
include the effective
`workspace_root` the thread uses for file reads, Git status and diffs — the project's workspace for
shared threads, the inherited worktree
workspace for isolated threads and their sub-agents. Thread summaries and browser-facing sub-agent
payloads omit native harness thread IDs. Every summary carries the thread's durable metadata
`revision`. The WebSocket's typed `ThreadState` carries the same revision with title, mode, selected
model, effective context window, permission preset, and thread token aggregates; it never exposes
the persisted native harness id or internal per-model/worktree caches. A committed catalog change
sends `ThreadCatalogChanged`, which coalesces into serialized refetches of the known project thread
catalogs. Metadata and catalog invalidations use a coalescing replacement lane, so a full socket
queue neither blocks the committing task nor permanently drops the latest value. Replacements go
directly from that lane to the socket writer and do not consume ordered event capacity. Mode,
model, and permission messages require a `request_id`; the initiating browser receives a correlated
`ThreadMetadataResult` after commit, including for no-op changes, or an `Error` carrying that id.
`TokenUpdate` no longer exists; thread totals use the revisioned metadata snapshot.

An internal `orphan` native thread is omitted from thread lists and returns not-found from detail,
history, and browser mutation routes until authoritative relationship evidence classifies the same
ID as a read-only sub-agent. It cannot be opened or imported as a primary. Model or mode fields may
be the reserved string `"unknown"` when the provider supplied no authoritative value; clients must
display that state and must not submit it as a model or mode selection.

Agent-produced diffs are advertised as bounded descriptors in live events and history. `GET
/api/projects/{id}/threads/{thread_id}/turns/{turn_id}/diffs/{diff_id}` returns the exact captured
unified or structured content on demand. It reads active runtime state or the immutable completed
turn payload; it never recomputes the current workspace diff. A superseded active identity returns
JSON `409 diff_superseded` with the current descriptor, while an unknown turn or identity returns
404. This endpoint is distinct from `/api/projects/{id}/git/diff`, which intentionally answers a
current-worktree question.

Completed command events and history carry a bounded 8 KiB tail preview and output statistics,
not the completed output body. `GET
/api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}/command-output` lazily returns
all durably retained output as `text/plain; charset=utf-8`. The
`X-Giskard-Output-Truncated`, `X-Giskard-Output-Original-Bytes`, and
`X-Giskard-Output-Original-Lines` headers describe the provider output before durable truncation.
Its strong `ETag` identifies the exact retained bytes. `GET` on the sibling `command-output-links`
path accepts that value in the required `If-Output-Match` header and returns only link spans,
without uploading or echoing the output. A missing precondition returns 428 and a stale version
returns 412; the browser keeps the raw output as plain text in either degraded case.
The lookup uses active runtime state first and immutable history second; unknown, wrong-kind,
unavailable, and still-running items all return 404. The browser requests it only when the command
overlay opens and releases the fetched body when that overlay closes.

Completed tool events, reconnect snapshots, `WireTurn`, and history retain all non-output fields
but replace a present terminal JSON output with `WireToolOutput { serialized_bytes, version }`.
`GET
/api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}/tool-output` lazily returns
the complete output value itself as compact JSON with exactly `application/json`. Its strong
`ETag` equals the descriptor version, and `serialized_bytes` equals the response-body length.
Explicit JSON `null` is a present four-byte body; missing output has no descriptor.

The lookup resolves completed active runtime output first, then targets the selected immutable turn
and item. Runtime authority remains available across the persistence race and while
`PersistenceBlocked`; active, persisted, and legacy reads return byte-identical JSON. Project,
thread, turn, and item containment is enforced before lookup. Unknown or cross-container items,
non-tool items, absent or unavailable output, and still-running tools all return the same 404.
The browser fetches only while the matching tool overlay is open and accepts the body only when its
`ETag` still matches the selected descriptor. Post-persistence late tool completion is not
advertised or retrievable until durable late-item amendments are implemented.

Process-local thread state is published separately: `ThreadRuntimeOverview { revision, threads }`
is a global replacement snapshot (including an empty `threads` list), `RequestState` carries a
per-request revision and the authoritative pending/responding/resolved status for each approval or
server request, and
`RunningTasks { thread_id, revision, tasks }` owns only the Tasks menu and its controls. Tasks carry
identity and lifecycle metadata but no command or tool output. Task-card navigation and the command
`Open` action resolve the matching transcript row, paging the existing authenticated history API
when an `after_turn` command's owning turn is not loaded. Output remains owned by ordered events,
history, and `LiveTurnSnapshot`. Approval decisions and
server-request responses both include `thread_id` so the runtime registry can validate and claim
the request atomically instead of consulting a global request-to-thread routing map.
See [Sub-agent threads](subagents.md) for the full contract.

`GET /api/projects/{id}/threads/{thread_id}/history` returns completed turns oldest-first as
`{ thread_id, turns, has_more }`. `before=<TurnId>` selects the page immediately before that turn;
without it the endpoint returns the newest page. `limit` is optional and is clamped to 1–100 turns;
the configured `[history] initial` default applies to the newest page and `page` to older pages.
History pagination is authenticated HTTP, not a WebSocket message, and a thread outside the named
project returns 404.

If you open a thread whose agent can no longer be started — most often because its
**provider was removed from config** (e.g. you swapped one proxy provider id for another) — the
thread still opens **read-only**: its history loads, a persistent banner above the composer names
the missing provider, and the composer is disabled. To rescue such a thread, pick a model from a
configured provider in the model picker (it
is unlocked for read-only threads): Giskard re-resumes the native thread under the new provider,
verifies the agent actually applied the switch before persisting it, and the thread becomes live
again with its history intact. The same verified switch works for any thread that hasn't been
opened since the server started; threads with a live agent session stay bound to their provider
(create a new thread to change providers there).

Agent-owned sub-agent threads are independently and permanently read-only. Their composer, compact,
model/mode/permission controls, rename, archive, direct delete, and workspace writes such as
`SavePlan` are disabled or rejected with `thread_read_only` before harness I/O. This restriction is
not recoverable through
a provider switch. Matched approval/server-request responses, interrupting active work, command
termination, transcript/history reads, and navigation remain supported.

`DELETE /api/projects/{id}/threads/{thread_id}` refuses with `409` when the thread — or any linked
child it cascades to — has a Git worktree holding work that exists nowhere else: uncommitted
changes, or commits on its branch that no other ref reaches. The message names what would be
destroyed. `?force=true` deletes anyway, which is what the browser sends once its confirmation
card has shown the same facts. Deleting a thread takes its worktree and the branch it started on;
branches the agent created during the thread are left alone, since they live in the shared
repository and are the user's now. Deleting a *project* sweeps its worktrees unconditionally —
that confirmation is project-scoped and one thread's unfinished work must not strand the rest
half-deleted.

The requested deletion root must be a primary thread. Requesting deletion of an individual
sub-agent returns `409 thread_read_only`; deleting a primary recursively removes its linked child
threads after the existing subtree preflight succeeds.

`GET /api/projects/{id}/threads/{thread_id}/deletion-impact` reports, per worktree in that
subtree, the branch, the uncommitted-change count, the count of commits reachable from no other
ref, and a `summary` sentence when either is non-zero. It exists so a confirmation can state the
cost before the user decides rather than after they try. When Git cannot answer for a worktree, the
endpoint returns `503` rather than reporting zeroes: "the cost could not be determined" and "nothing
would be lost" lead to opposite confirmation copy, so a client must not read the first as the
second. The matching `DELETE` refuses for the same reason.

Six endpoints resolve a path against a thread's workspace, and all six name that thread in the
path — there is no thread-less form. The thread is part of the request rather than an optional
scope, because a caller that could omit it would be answered from a workspace it never named. That
workspace is the thread's own Git worktree when it was started with one, its parent's when it is a
sub-agent, and the project's otherwise (§7.1) — so with isolation the answer genuinely differs
between threads of one project, and the same path names a different file in each. An unknown
thread, or one belonging to another project, is a `404` rather than a silent answer.

They use the workspace for two different things, which is worth keeping straight:

- **`highlight`, `raw` and `image` read the file.** The workspace decides which bytes come back, so
  the wrong one is answered successfully with the wrong content.
- **`linkify`, `command-output-links`, and `render` only test that a path exists.** None opens a file: `linkify` and `command-output-links` return
  spans for the candidates that resolve inside the workspace and are files, and `render` is
  workspace-independent apart from running that same pass to decide which text becomes a
  `.path-link` button. The wrong workspace there costs a link, not content — a path rendered
  clickable that then fails to load, or a real file left as plain text.

That second group is why they belong on the same scope as the first. The existence check is a
prediction about what the read endpoints will serve; resolve them against different workspaces and
the UI renders links that break when clicked. A workspace path that cannot be canonicalized simply
yields no links, so `render` still returns correct Markdown.

`GET /api/projects/{id}/git/status` returns best-effort, read-only Git metadata for a workspace,
parsed from `git status --porcelain=v2 -z`: the current branch (reported
even on an unborn one), `detached` with the short commit in `head`, ahead/behind counts when Git
reports an upstream, staged/unstaged/untracked and conflicted counts, and the changed file list.

Which workspace it reads is decided by the optional `?thread_id=...`: with it, the named thread's
workspace — its own Git worktree when the thread was started with one, or its parent's when the
thread is a sub-agent, which is where the harness ran it (§7.1) — and without it, the project's
effective workspace root, which is what a draft reads before any thread exists.
A
`thread_id` that does not resolve *within this project* is a 404 rather than a fall back to the
project's workspace: answering from a different tree under the name of the one that was asked for is
the confusion isolation exists to prevent, and it would also let one project's endpoints read
through another's workspace.

An untracked directory is reported as a single entry with a trailing slash rather than one entry
per file beneath it, matching what `git status` reports to a person.

Each file carries `staged_added`/`staged_deleted` and `unstaged_added`/`unstaged_deleted` line
counts from `git diff --numstat`, kept apart so a file that is both staged and modified reports each
side accurately, with `added_total`/`deleted_total` summing them on the response. The counts are
omitted for the side with no changes, for untracked and binary files, and for conflicted files —
git diffs an unmerged path against each merge stage, so no single count describes it. The numstat
calls are skipped entirely for a clean tree, and a numstat failure is non-fatal — the status is
returned without line counts. Non-Git workspaces return
`is_repository: false` with no `error`: git's own "not a git repository" is logged rather than
reported, so `error` means only that the status could not be determined (git could not be run, or
timed out).

`GET /api/projects/{id}/git/diff` takes the same `?thread_id=...` scope, on the same terms, so a
diff describes the tree the status row that opened it described. It returns the combined staged and
unstaged diff for the whole working tree; with `?path=...` it returns that diff for one
workspace-relative path, and the response echoes the path back. `?side=staged` or `?side=unstaged`
narrows it to the index or the
worktree, so a path that is both staged and modified again can be diffed one side at a time; any
other value is rejected. The path is lexical workspace-relative only: absolute paths and `..`
escapes are rejected, so deleted files can still be diffed without allowing access outside the
workspace.
