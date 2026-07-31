# HTTP / WebSocket API

The browser (and any client) drives everything through a small REST surface plus one multiplexed
WebSocket. Highlights: `POST /api/login`, `POST /api/logout`, `GET /api/ws-ticket`, `GET /api/ws`,
`GET/POST /api/projects`, `GET/DELETE /api/projects/{id}`, `GET/POST
/api/projects/{id}/threads`, `POST /api/projects/{id}/threads/start`, `DELETE
/api/projects/{id}/threads/{thread_id}`, `POST
/api/projects/{id}/threads/{parent_thread_id}/subagent-links/{item_id}/open`, `PATCH
/api/projects/{id}/threads/{thread_id}/title`,
`POST /api/projects/{id}/threads/{thread_id}/archive`, `GET /api/models`, `POST
/api/models/refresh`, `GET /api/projects/{id}/models`,
`GET /api/tokens`, `GET /api/projects/{id}/tokens`,
`GET /api/projects/{id}/highlight|raw|image`, `POST
/api/projects/{id}/linkify`, `POST /api/projects/{id}/render`,
`GET /api/projects/{id}/git/status`, `GET /api/projects/{id}/git/diff`, `GET /api/browse`, `POST
/api/browse/mkdir`, `GET /api/projects/{id}/mcp`, `POST /api/projects/{id}/mcp/reload`, and `POST
/api/projects/{id}/mcp/oauth-login`. Wire types are defined once in `giskard-proto`. See
[§13.6](../specs/giskard-specification.md) for the message protocol.

`POST /api/projects/{id}/threads/start` creates the durable thread from the first user message or
attachment set, persists a deterministic title generated from the prompt or first attachment name,
and returns the title with the new thread and turn identifiers. The request accepts optional
transient attachment payloads; Giskard validates them and does not persist raw attachment bytes.
Image MIME types must match PNG, JPEG, GIF, or WebP file signatures. Raw bytes are also redacted
before turns enter the parsed in-memory history cache. The Codex adapter transfers non-image files
into a randomized per-turn directory under the harness host's temporary directory. It removes the
directory after turn completion, upload/start failure, stream loss, channel closure, or shutdown;
it never writes uploads into the project workspace.

`POST /api/projects/{id}/threads` opens an existing local thread when `thread_id` is provided, or
imports/resumes a native harness thread when `resume` is provided. Linked transcript items use the
dedicated parent/item endpoint above; the server resolves native routing, ownership, provenance,
prompt, and lifecycle evidence from its authoritative item rather than accepting those fields from
the browser. Thread summaries and browser-facing sub-agent payloads omit native harness thread IDs.
See [Sub-agent threads](subagents.md) for the full contract.

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

`GET /api/projects/{id}/git/status` returns best-effort, read-only Git metadata for the project's
effective workspace root: current branch or detached head (resolved to a short commit when Git
reports no branch name), ahead/behind counts when Git reports them, staged/unstaged/untracked and
conflicted counts, and the changed file list. Each file carries `staged_added`/`staged_deleted` and
`unstaged_added`/`unstaged_deleted` line counts from `git diff --numstat`, kept apart so a file that
is both staged and modified reports each side accurately, with `added_total`/`deleted_total`
summing them on the response. The counts are omitted for the side with no changes and for untracked
and binary files, and the numstat calls are skipped entirely for a clean tree. A numstat failure is
non-fatal — the status is returned without line counts. Non-Git workspaces return
`is_repository: false` with no `error`: git's own "not a git repository" is logged rather than
reported, so `error` means only that the status could not be determined (git could not be run, or
timed out).

`GET /api/projects/{id}/git/diff` returns the combined staged and unstaged diff for the whole
working tree; with `?path=...` it returns that diff for one workspace-relative path, and the
response echoes the path back. `?side=staged` or `?side=unstaged` narrows it to the index or the
worktree, so a path that is both staged and modified again can be diffed one side at a time; any
other value is rejected. The path is lexical workspace-relative only: absolute paths and `..`
escapes are rejected, so deleted files can still be diffed without allowing access outside the
workspace.
