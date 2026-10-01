# Implementation plan: MCP status per thread

Implements the §11 entry *After milestone 5, as its own change — MCP status per thread* of
[`../claude-code-harness-plan.md`](../claude-code-harness-plan.md). This plan is written for an
implementing agent. Every file, symbol and behaviour below was verified against `main` at `65704b1`
(milestone 5 merged). Line numbers are for orientation; the symbol quoted beside each is the thing
to find.

Read `AGENTS.md` first. Its rules that bind this change: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; `docs/api-endpoints.md` changes in the same commit as the route it documents; the adapter
README is kept in sync with process control; log assertions use `#[traced_test]` with
`logs_contain` / `logs_assert`; Markdown prose is wrapped at 100 columns (table rows may run
longer). No screenshot regeneration: nothing visible changes.

## Outcome

The MCP menu shows the servers of the process behind the thread the user is looking at. Today
`AgentHarness::list_mcp_servers(&self)` names no thread and the route `GET
/api/projects/{id}/harnesses/{name}/mcp` names a declaration, which fits Codex (one `app-server` per
project) but not Claude Code (one `claude` per thread): the adapter asks whichever live child its
`HashMap` yields first (`crates/giskard-harness-claude/src/harness.rs:1139`). The configured server
set is the same in every child (`--setting-sources user`), but the state the panel shows, `pending`
and `failed`, is per process. After this change the browser sends the open thread's id, the server
resolves it to that thread's handle on the named declaration, the Claude adapter asks that thread's
child (a sub-agent's owning child) and otherwise its thread-less probe, never an arbitrary child,
and Codex ignores the hint. The commit is one unit across `giskard-harness`, the three adapters,
`giskard-server`, `app.js` and the docs.

## Scope

1. The trait: `list_mcp_servers(&self, thread: Option<&ThreadHandle>)`.
2. The adapters: Codex and replay ignore the hint; Claude honours it.
3. The route: an optional `thread` query parameter, resolved and validated.
4. The browser: `loadMcpServers` sends the open thread's id.
5. Tests at each layer.
6. Documentation: `docs/api-endpoints.md`, the spec (§4.3 signature and the multi-harness note),
   the adapter README, the harness plan's §11 entry.

## Non-goals

`reload_mcp_servers` and `start_mcp_oauth_login` keep their instance-scoped signatures and routes:
Claude Code advertises neither, Codex has one process, and nothing would read a hint. No change to
`giskard-core::mcp` types, to the wire types in `giskard-proto`, or to the MCP menu's rendering.
No probe fallback for a **live** child whose `mcp_status` request fails (a timeout from a running
child says the CLI is unresponsive; a probe would not describe that thread). No Playwright change:
`giskard-server-replay` reports `mcp_status: false`, so the e2e UI never opens the menu.

## Verified facts this change rests on

| Fact | Consequence |
| --- | --- |
| `AgentHarness::list_mcp_servers(&self)` (`giskard-harness/src/lib.rs:629`) is an instance-scoped method with an `Unsupported` default; the trait doc lists it under "instance" (`lib.rs:580`). Implementers: `giskard-harness-codex/src/lib.rs:1043` (a `ControlCommand::ListMcpServers` to the instance task), `giskard-harness-replay/src/lib.rs:260` (`Ok(vec![])`), `giskard-harness-claude/src/harness.rs:1136`. `giskard-testenv`'s `FakeHarness` (`fake.rs:411`) does not override it | Four signatures change; the fake keeps the default unless a test needs a hook (Step 5) |
| The Claude adapter asks `lock(&self.children).iter().next()` when any child is live, else a probe under `self.probe`; a live child's failure is returned, never probed around (`harness.rs:1139-1169`) | The hint replaces the arbitrary pick; without a hint the probe answers, so no path picks a child the caller did not name |
| A probe launched with the adapter's exact protocol flags (`protocol_argv`, `process.rs:97`: no `--session-id`, `--resume`, `--model`, `--permission-mode`), sent `initialize` alone or `initialize` then `mcp_status` and then closed, leaves **no session behind** on 2.1.287: no `projects/<cwd>/` directory, no session `.jsonl`, no `sessions/` entry, no project entry in `.claude.json`; only caches change (`cache/model-catalog/*`, growth-book features, `policy-limits.json`, `remote-settings.json`). It does start the user's configured MCP servers while it lives. `probe_request` (`harness.rs:607`) writes through `control_line` only, so a probe cannot be sent a `user` line | The probe is a safe fallback for every call without a usable hint; the rule is in `AGENTS.md` and Step 5 pins its shape |
| A sub-agent thread's handle is a `task:` id whose `RouteHandle` names its `owner` (the primary thread whose child carries it) and `commands`; a cold route has neither (`session.rs:131`, `RouteHandle`) | A sub-agent hint resolves to its owner's child; a cold route is "no live child" |
| The route handler resolves the instance with `declared_project_harness` (`routes.rs:4377`): an unknown project or an undeclared name is `404`. `registry.loaded_thread_binding(thread_id)` (`registry.rs:590`) returns the `LoadedThreadBinding` of an **open** thread, with `project_id()`, `harness()` (the declaration name) and `handle()` (`registry.rs:225`); `None` for a thread that is not open | The hint is validated against the path's project and declaration from the binding, with no store read |
| `ThreadId` is a newtype over `ulid::Ulid` deriving `Deserialize` (`giskard-core/src/ids.rs:11`), and routes take query parameters through `axum::extract::Query` on a `#[derive(Deserialize)]` struct (`routes.rs:1946`, `DeleteThreadQuery`) | `Option<ThreadId>` in a query struct; a malformed id is Axum's `400` |
| `loadMcpServers` (`app.js:8891`) derives the harness from `activeModelScope()`, which is `null` on a draft (`state.threadHarness` otherwise), and calls `/api/projects/${projectId}/harnesses/${encodeURIComponent(harness)}/mcp`; `state.threadId` is the open thread's id, `null` on a draft (`isDraftThread`, `app.js:2527`). `tests/ui.rs:3773` asserts that URL literal is in the source | One query parameter in the browser, one assertion to update |
| `docs/api-endpoints.md:29-33` and the spec's multi-harness note (`giskard-specification.md:23-27`) describe the MCP routes as addressing one instance; the spec's §4.3 trait listing (`giskard-specification.md:1902`) shows the signature | Three documentation passages name the new parameter |
| `thread_lifecycle.rs:131` (`start_two_declaration_server`) builds a server with `stable` and `nightly` declarations, both replay harnesses, and `start_thread(server, pid, Some("nightly"))` opens a thread on the second | The "thread on another declaration" case has a ready-made setup |

## Step 1: the trait (`crates/giskard-harness/src/lib.rs`)

```rust
/// List configured MCP servers and their visible tools/resources.
///
/// `thread` is a hint, not a scope: the thread the user is looking at, when it is open on this
/// instance. An adapter that runs one process per thread answers from that thread's process
/// (for a sub-agent, the process that carries it); an adapter with one process per instance
/// ignores it. With no hint, or a hint for a thread this instance does not hold, the answer is
/// the instance's own view. The method stays instance-scoped: it never opens or resumes a thread.
async fn list_mcp_servers(
    &self,
    thread: Option<&ThreadHandle>,
) -> Result<Vec<McpServerStatus>, HarnessError> {
    let _ = thread;
    Err(HarnessError::Unsupported(
        "MCP server status is not supported by this harness".into(),
    ))
}
```

In the trait's grouping doc comment (`lib.rs:580`) keep `list_mcp_servers` under "instance" and
add "(with an optional thread hint)".

## Step 2: the adapters

- **Codex** (`giskard-harness-codex/src/lib.rs:1043`): `_thread: Option<&ThreadHandle>`, body
  unchanged. One process serves every thread.
- **Replay** (`giskard-harness-replay/src/lib.rs:260`): same.
- **Claude** (`giskard-harness-claude/src/harness.rs:1136`):

  ```rust
  async fn list_mcp_servers(&self, thread: Option<&ThreadHandle>) -> Result<…> {
      self.ensure_running()?;
      let request = json!({"subtype": "mcp_status"});
      let hinted = thread.map(|handle| handle.thread);
      let child = match hinted {
          Some(thread) => self.child_carrying(thread),
          None => lock(&self.children).iter().next().map(|(t, h)| (*t, h.commands.clone())),
      };
      …
  }
  ```

  `child_carrying(thread) -> Option<(ThreadId, mpsc::Sender<ChildCommand>)>`: the entry of
  `children` for `thread`; else, for a `routes` entry with `owner: Some(owner)`, the entry of
  `children` for `owner`; else `None`. Take the two locks one after the other, never nested
  (nothing in the façade holds two of its mutexes at once; keep it so).

  Then: a child found → the existing `Control` call under `CONTROL_TIMEOUT`; its error is returned
  as today. No child found → the **probe** (the existing path under `self.probe`): with a hint,
  after `debug!(thread_id, action = "mcp_status", "the hinted thread has no live child; probing")`.
  The `lock(&self.children).iter().next()` pick goes away: a caller that names no thread gets the
  instance's thread-less view, never some other thread's process. The `mcp_status` log line gains
  `thread_id` = the hinted thread and `owner_thread_id` = the child's thread; `mcp_probe` gains
  `thread_id` as `display_opt(hinted)` and `hinted` (bool).

  **The probe rule** (`AGENTS.md`, verified for `mcp_status` on 2.1.287, facts table): the probe
  never becomes a Claude Code session. `probe_argv` carries the protocol flags only and
  `probe_request` writes only control requests, so the rule is structural; Step 5 pins both.

  Add a `RouteHandle` doc line: the owner is also what `list_mcp_servers` asks for a sub-agent.

## Step 3: the route (`crates/giskard-server/src/routes.rs:4399`)

```rust
#[derive(Deserialize)]
struct McpStatusQuery {
    /// The open thread whose process the status should describe, when the harness runs one
    /// process per thread. Optional: the instance's own view otherwise.
    thread: Option<ThreadId>,
}

async fn list_mcp_servers(
    State(state): State<AppState>,
    AxumPath((project_id, harness_name)): AxumPath<(ProjectId, String)>,
    Query(q): Query<McpStatusQuery>,
) -> Result<Json<ListMcpServersResponse>, ApiError> {
```

Resolution, after `declared_project_harness`:

1. No `thread` → `None`.
2. `registry.loaded_thread_binding(thread)` is `None` → the thread is not open (closed between
   the browser's render and this request, or never opened): `debug!(%project_id, %thread_id,
   harness, action = "mcp_status", "hinted thread is not open; answering for the instance")` and
   `None`. Not an error: the menu must still open.
3. `binding.project_id() != project_id` → `ApiError::NotFound` (a thread of another project is
   not addressable through this project's path, like every other thread route).
4. `binding.harness() != harness_name` → `ApiError::BadRequest(format!("thread {thread} runs on
   harness {}, not {harness_name}", binding.harness()))`.
5. Otherwise `Some(binding.handle())`.

Call `harness.list_mcp_servers(hint)`. The existing `info!` line gains `thread_id =
display_opt(hint.map(|h| h.thread))` and `hinted = hint.is_some()`.

## Step 4: the browser (`crates/giskard-server/static/app.js:8891`)

In `loadMcpServers`, after `harness` is known:

```js
const thread = state.threadId;
const query = thread ? `?thread=${encodeURIComponent(thread)}` : "";
const res = await api("GET",
  `/api/projects/${projectId}/harnesses/${encodeURIComponent(harness)}/mcp${query}`);
```

Update the comment above the function: the status is the open thread's process's, when the
harness has one per thread. `reloadMcpServers` and `startMcpOauthLogin` are unchanged (Non-goals).
No CSS, no HTML, no screenshot.

## Step 5: tests

Adapter (`giskard-harness-claude/src/harness.rs`, the existing MCP tests at `harness.rs:4650-4780`
become `list_mcp_servers(None)`; add):

1. `list_mcp_servers_asks_the_hinted_thread_s_child`: open two threads on two scripted children,
   each answering `mcp_status` with a different payload (one empty, one
   `crate::mcp::tests::failed_and_pending()`); hint the second handle → its two servers, and the
   `mcp_status` line is written to the **second** child's stdin only (`written(&record)`); hint
   the first → empty, written to the first only. No probe spawned (`spawner.spawns().len() == 2`).
   Then `list_mcp_servers(None)` with both children live → a **probe** is spawned (a third
   scripted child answering `mcp_status`) and neither child's stdin gets a second `mcp_status`.
2. `a_sub_agent_hint_asks_its_owner`: a delegation through the `delegation` fixture (the
   `delegating` helper from the milestone 5 tests), claim the route, hint the route's handle →
   the owner child's stdin gets `mcp_status`; `#[traced_test]`, `logs_assert` a line with
   `owner_thread_id=` the primary and `hinted=true`.
3. `a_hint_for_a_thread_without_a_child_probes`: no child open, hint a fresh
   `ThreadHandle::detached(ThreadId::new(), "x".into())` → the probe answers;
   `logs_contain("the hinted thread has no live child; probing")`. Then a cold route
   (`claim_native_thread` of an unknown `task:` id) hinted → the probe again.
4. `a_hinted_child_s_failure_is_returned_not_probed`: the hinted child answers `mcp_status` with
   an error → `HarnessError::Protocol`, `spawner.spawns().len()` unchanged.
5. `a_probe_never_becomes_a_session` (the rule's shape, in `process.rs` and `harness.rs`):
   `probe_argv` contains none of `--session-id`, `--resume`, `--model`, `--permission-mode`, and
   after `list_mcp_servers(None)` and `list_models()` against scripted probes every line in
   `written(&record)` has `"type": "control_request"` (`initialize`, then `mcp_status`), with
   `stdin_closed` set. The real-CLI check behind the rule is in the facts table and is repeated in
   Verification.

Server (`giskard-server/tests/e2e_smoke.rs`, extend
`mcp_status_routes_surface_empty_replay_status_and_reload` and add one test):

6. `?thread=<the open thread>` → `200`, same body. `?thread=<ThreadId::new()>` (not open) →
   `200`. `?thread=nope` → `400`.
7. `mcp_status_hint_is_checked_against_the_path`: with `start_two_declaration_server` moved to
   `giskard-testenv` or duplicated locally (it is private to `thread_lifecycle.rs`), open a
   thread on `nightly`, then `GET …/harnesses/stable/mcp?thread=<it>` → `400` naming
   `nightly`; a second project's thread through the first project's path → `404`.
8. To prove the hint reaches the harness rather than being dropped: add to `giskard-testenv`'s
   `Script` trait (`fake.rs:300`) `async fn list_mcp_servers(&self, _core: &FakeCore, thread:
   Option<&ThreadHandle>) -> Result<Vec<McpServerStatus>, HarnessError>` with the trait default's
   `Unsupported` as its default, forward it from `FakeHarness` (`fake.rs:411`), and write an
   `McpHintScript` in `e2e_smoke.rs` whose capabilities set `mcp_status: true` and which answers
   one server named after the hint's `harness_thread_id`, or `"none"`. Assert the name with and
   without `?thread=`.

UI (`giskard-server/tests/ui.rs:3773`): the assertion becomes
`source.contains("/harnesses/${encodeURIComponent(harness)}/mcp${query}`")` plus one for
`?thread=${encodeURIComponent(thread)}`, so the hint cannot silently disappear from the page.

## Step 6: documentation

- `docs/api-endpoints.md:29-33`: `GET …/harnesses/{name}/mcp` takes an optional
  `thread=<thread_id>`; what it means (the open thread's process, for a harness with one per
  thread; ignored by Codex), that a thread that is not open is answered for the instance (the
  Claude adapter's thread-less probe), a thread of another project is `404`, a thread on another
  declaration is `400`. The browser sends the open thread's id.
- `specs/giskard-specification.md`: the §4.3 listing at line 1902 gets the new signature and a
  one-line doc; the multi-harness note (line 23-27) gains "and the MCP route takes the open thread
  as a hint for harnesses with one process per thread".
- `crates/giskard-harness-claude/README.md`, *MCP servers* (line 559): replace "When a child is
  live it asks that child" with the hint rule: the hinted thread's child (a sub-agent's owner),
  else the probe, never an arbitrary child; a live child's failure is returned. *The probe child*
  bullet (line 71) states the probe rule from `AGENTS.md` and the 2.1.287 evidence: `initialize`
  and `mcp_status` on a probe leave no session, transcript or `.claude.json` project entry, only
  cache files, and the probe starts the user's MCP servers while it lives.
- `specs/claude-code-harness-plan.md`, the §11 entry (line 1634): point at this plan and add the
  "implemented" sentence the milestone paragraphs carry.

## Logging

New fields on existing lines only: `thread_id`, `owner_thread_id`, `hinted` on the adapter's
`mcp_status` line; `thread_id` on `mcp_probe`; `thread_id` and `hinted` on the route's "MCP server
status loaded". New lines: the adapter's `debug` when a hinted thread has no live child, the
route's `debug` when a hinted thread is not open. A rejected hint (another project, another
declaration) is a client error and logs at `debug` through the normal `ApiError` path.

## Verification

Before pushing: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D
warnings`, `cargo test --workspace --locked`, `cargo deny check`.

Then, on a shell with a logged-in `claude` and a `claude-code` declaration. First the probe rule:
create a marker file, `GET …/mcp` with no `thread` on a project with no open thread, then
`find "${CLAUDE_CONFIG_DIR:-$HOME/.claude}" -newer <marker> -type f` and confirm no
`projects/<cwd>/` directory, session file or `.claude.json` project entry appeared. Then open two
threads, give
one an MCP server that fails to start (a `--mcp-config` naming a missing command in the
declaration's `args`, or a user-scope server that is down) and open the MCP menu on each: the
failing server's `failed: …` shows on that thread; `RUST_LOG=giskard_harness_claude=debug` shows
`hinted=true` with the thread's id and its child as `owner_thread_id`; open a sub-agent thread and
the menu names its parent's child as the owner.

## Acceptance

- `list_mcp_servers` takes `Option<&ThreadHandle>`; Codex and replay ignore it; the Claude adapter
  asks the hinted thread's child, a sub-agent's owner, else the probe, never an arbitrary child; a
  live child's failure is returned; the probe's argv and stdin keep the shape the rule requires.
- `GET …/harnesses/{name}/mcp?thread=` resolves the hint from the open thread's binding, answers
  for the instance when the thread is not open, and rejects a thread of another project (`404`) or
  declaration (`400`).
- `loadMcpServers` sends the open thread's id.
- The tests of Step 5 pass; every existing test passes.
- `docs/api-endpoints.md`, the spec, the adapter README and the §11 entry say what the code does.
