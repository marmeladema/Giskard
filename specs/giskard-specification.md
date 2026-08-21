# Giskard — Technical Specification

> A local-first, single-user web application that provides a modern browser UI on top of
> agentic coding CLIs. The first supported agent harness is OpenAI's **Codex CLI** (via its
> `app-server` JSON-RPC protocol), but the application is designed so the harness is a
> replaceable component. Built entirely in Rust (Axum backend + a hand-authored vanilla
> HTML/CSS/JS UI embedded in the server binary), with **no npm, Node, or JavaScript toolchain**
> anywhere in the build.

**Document status:** Implementation-ready specification.
**Audience:** An AI coding agent (and its human reviewer) implementing the system.
**Version:** 1.69

> **Amendment — frontend approach (supersedes the Dioxus/WASM design below).**
> This document was written targeting a **Dioxus fullstack / WebAssembly** frontend (`giskard-ui`),
> and many sections (notably §3.5, §13.1, and the crate map in §3.2) still describe it that way.
> That approach was **not adopted and is no longer a goal.** The shipped and supported UI is a
> single hand-authored **vanilla HTML/CSS/JavaScript** page, served as same-origin static assets
> that are `include_str!`-embedded into the `giskard-server` binary (`crates/giskard-server/static/`:
> `index.html`, `app.css`, `app.js`, `sw.js`, `favicon.svg`). It requires no npm/Node and no WASM
> build. The `giskard-ui` crate has been **removed** from the workspace. This vanilla static UI is
> the intended frontend for the foreseeable future; treat every Dioxus/WASM/`giskard-ui` reference
> below as historical design context, not a current requirement. The wire contract (`giskard-proto`)
> and all backend design remain authoritative.

**Changelog (1.68 → 1.69), runtime and bootstrap reconciliation:**
- **RB1:** One `ThreadRuntimeRegistry` now owns active-turn leases, live reconstruction, tasks,
  requests, the bounded per-thread event journal, and the replacement runtime overview. Each
  client-visible agent event is applied once and receives one process-local sequence.
- **RB2:** Subscribe now produces one staged `ThreadBootstrap` transaction containing metadata,
  history, live reconstruction, ordered suffix, final runtime, and notices. The browser stages
  start/chunk frames and changes authoritative state only at a matching-generation commit.
- **RB3:** Delivery is bounded and class-aware. Ordered-event loss invalidates only the affected
  subscription and requests a same-socket resync; revisioned replacements coalesce independently,
  and slow clients cannot block event forwarding or turn persistence.
- **RB4:** Request receipt, claim, rollback, and resolution use the ordered event lane plus the
  final-runtime replacement. Running-task snapshots are menu-only, and one revisioned runtime
  overview replaces cross-thread additive activity.
- **RB5:** Turn completion remains owned by runtime until authoritative history persistence
  succeeds. Bounded retry can enter an actionable `PersistenceBlocked` state with explicit retry
  and confirmed-discard recovery.
- **RB6:** The earlier wire mechanisms named in historical changelog entries—including
  `ThreadState`, `ThreadActivity`, `HistoryDelta`, `LiveTurnSnapshot`, `RunningTasks`,
  `ApprovalResolved`, and the top-level `Event` message—are superseded and are not current
  protocol alternatives. The paired browser and server implement only the §13.6 protocol.
- **RB7:** Turn-less context-window restoration is deliberately deferred. When added, it must use
  the existing metadata authority and ordinary `ThreadMetadata` publication. Persisted late-command
  reconciliation and broader thread/project lifecycle sagas remain separate adjacent work.
- **RB8:** Late command completion is persisted by appending one newline-terminated replacement
  item record to the already-committed turn payload before ordered publication. Completed append is
  the process-level commit; `sync_data` is best-effort crash hardening and failure is warned.
  Per-thread amendments use a process epoch and sequence in the subscription cursor; they do not
  rewrite the history index, refold usage, or change recency.
- **RB9:** Failed late amendments use an ordered runtime-owned recovery queue independent of active
  turn ownership. Completed command output is normalized once to a bounded head/tail retention
  with an exact omission marker before the journal, live projection, turn payload, or amendment
  consumes it. Other turn fields remain unbounded, so bootstrap byte chunking remains mandatory.

**Changelog (1.67 → 1.68), advisory data-directory lock:**
- **DL1:** One `flock` per data directory (`<data_dir>/.giskard.lock`) supplies the cross-process
  exclusion the in-process per-file mutexes never could. `giskard-server` holds it for its process
  lifetime and refuses to start when another process has it; every mutating `giskard-admin` command
  holds it for the command and exits non-zero instead of proceeding (§5.4, §5.5).
- **DL2:** `--dry-run` and read-only inspection take no lock and warn that their output may be
  stale. A preview that refused while a server ran would make "stop the server first" advice
  circular — the preview is what an operator uses to decide (§5.4).
- **DL3:** The orphan sweep's 24-hour mtime threshold is removed. It was a guess about another
  process's progress standing in for exclusion; with the directory locked, unreferenced means
  unreferenced. The magnitude and missing-index guards stay: they defend against a *damaged index*,
  which is a data condition a lock does nothing about (§5.5).
- **DL4:** MSRV rises to 1.89 for `std::fs::File::try_lock`. CI uses `@stable` with no
  `rust-toolchain.toml`, so nothing enforces the manifest's claim — the bump is deliberate.

**Changelog (1.66 → 1.67), per-turn payload files with a versioned header:**
- **Motivation:** a turn's *count* is human-scaled (someone types each prompt) while a turn's
  *contents* are agent-driven and unbounded (command output, diffs, tool JSON). Writing both to one
  append-only file put them on the same durability mechanism, and the large one broke it: a whole
  turn written with one `write_all` can be torn by a crash or a full disk, the next append
  concatenates onto the partial line, and the merged garbage line is no longer *last* — so the
  torn-final-line tolerance stops covering it and the thread's entire history becomes unreadable.
  Probability scaled with turn size, so it was likeliest on the threads with the largest output.
- **L1:** A thread is a directory. `<thread_id>/history.jsonl` is a bounded **index** (a header
  line, then one strictly bounded record per turn); `<thread_id>/turns/<turn_id>.jsonl` is that
  turn's **payload** — full `UserInput`, items, diffs — written with temp file + `fsync` + rename,
  so it is complete or absent (§5.2, §5.4).
- **L2:** A turn commits payload first, index last. A crash between them leaves a payload no turn
  record references, invisible to every read path because reads start from the index (§5.4).
- **L3:** Three independent version markers: `thread.json` → `version` (metadata schema),
  `history.jsonl` header → `format` (layout + index schema, written once — at thread creation, or
  by the first append for a thread the store never saw created — and never rewritten), each
  payload header → `format` (that turn's payload schema). Unknown `kind` within a known format is
  skipped with a warning; a newer payload format fails **that turn only**; a newer history format
  fails the thread (§5.4).
- **L4:** `recompute_aggregates` is index-only — `usage`, `model`, `status` and the turn timestamps
  are all turn-record fields, so repair opens no payload file. No API change (§5.4).
- **L5:** `delete_thread` renames `<thread_id>/` to `<thread_id>.deleting/` before removing it, so
  the thread leaves enumeration atomically and the recursive removal can fail and be retried. This
  preserves — deliberately, rather than by ordering luck — the property that a partial delete leaves
  the thread visible instead of orphaning history (§5.4).
- **L6:** Format 1 threads migrate per thread, on open, idempotently, under the per-thread lock: a
  staged rebuild, verified against the source before a single commit rename, after which the
  originals are *relocated* to `<id>/legacy/` and never deleted. `giskard-admin migrate-storage`,
  `prune-legacy` and `sweep-orphan-payloads` do the bulk and the cleanup explicitly (§5.4, §5.5).
- **L7:** `prompt_preview`/`prompt_truncated`, `item_count`, and a turn record's `status.message`
  are **display hints**: derived, capped, never authoritative, and never used for search,
  comparison, or validation. A mismatch is logged and the payload file wins. `status.kind` is the
  exception and stays authoritative in the index, because the ledger folds it and repair must not
  have to open a payload file (§5.4).
- **L8:** A committed turn payload may later receive newline-terminated replacement `item`
  records for background commands which finish after turn commit. The append is serialized by the
  per-thread lock. Readers accept only terminated records, warn and skip malformed committed lines,
  and preserve an unterminated tail. The next amendment adds a separator newline, recovering a
  complete JSON value missing only that delimiter while leaving genuinely partial JSON malformed,
  then writes its own complete record rather than truncating old bytes.
  A completed append advances the process-local amendment sequence even if best-effort `sync_data`
  fails. Item identity folds last-wins while retaining the original display position. The index
  remains append-only and unchanged. Completed command output is normalized to a bounded head and
  tail with an exact omitted-byte marker before either initial commit or amendment; other payload
  fields remain agent-sized and unbounded.

**Changelog (1.65 → 1.66), explicit client-state authorities:**
- **ST1:** Every client-visible state projection names one authority, one clock, and one delivery
  class. Persisted thread metadata is a typed snapshot ordered by a per-thread durable revision;
  runtime/transcript/task/request state keeps its own process-local clocks. A clock never orders a
  different authority. New wire state must extend the authority table in §13.6 rather than adding
  a field-local ordering rule.
- **ST2:** Thread metadata mutations compare domain state under the per-thread store lock. A no-op
  neither writes nor advances the revision. Recency is explicit: user-visible mutations touch it,
  turn completion records activity, ordinary background/cache mutations preserve it, and crash
  repair may restore recency from the latest persisted turn without using the repair time.
- **ST3:** Browser thread state is an audited `ThreadMetadata` projection rather than serialized
  `ThreadFile`. Project thread rows carry the same revision, and committed catalog changes publish
  one coalescible catalog invalidation for authoritative HTTP refetches.
- **ST4:** Authoritative metadata and catalog invalidations use replacement entries in the
  bounded, class-aware per-connection outbox. They coalesce independently of ordered events:
  metadata by subscribed thread and catalog changes as one global dirty signal. Metadata
  producers therefore never wait for a slow browser, and overload enters explicit recovery rather
  than silently dropping the latest committed value.
- **ST5:** Metadata actions carry a browser request id and receive an authoritative direct result
  even when the mutation is a no-op. The browser reconciles WebSocket detail, HTTP summaries, and
  action results through one revision authority, and retries raced or failed catalog refetches.

**Changelog (1.64 → 1.65), refresh Codex protocol SDK:**
- **CP1:** The Codex harness now builds against `codex-codes` 0.146.4. The update brings the
  schema snapshot back in line with newer Codex app-server releases and exposes newer typed surfaces
  such as account/auth helpers, local auth status reads, thread sections, app/plugin protocol
  methods, and generated defaults. Giskard currently consumes only the compatibility-relevant
  changes: `InitializeCapabilities.extensions` is explicitly unset, and Codex's new `unknown` MCP
  auth status is preserved as a neutral harness state instead of being collapsed into another
  status.

**Changelog (1.63 → 1.64), Send offers only what it can do:**
- **LT10:** Send is unavailable when there is nothing to send — no non-whitespace composer text and
  no attachment; a composer holding only whitespace counts as empty.
  It was previously offered in that state and `sendInput` returned early with no notice, so the
  click did nothing and said nothing. That is fine when the user simply clicked an empty composer,
  but it also silently absorbed a composer that was empty unexpectedly (LT6), which then read as a
  dead button. The condition belongs on the control, alongside the other reasons a send cannot
  happen: read-only, a turn already running, attachments still loading, an unresolved draft model.
  The keyboard path keeps a matching guard, since it does not go through the control, and stays
  silent there: an empty composer is not an error worth reporting. Because the button now depends
  on the composer's contents, every path that changes them refreshes it: the input event, the
  attachment render, and the per-thread draft restore.

**Changelog (1.62 → 1.63), the draft opens before the project is fetched:**
- **LT6:** The project `+` action must switch to the draft thread *synchronously*. It previously
  awaited `GET /api/projects/{id}` for the project's default model and only then opened the draft,
  which left the previously selected thread on screen — composer visible and editable — for the
  length of that round-trip. Anything typed in that window was destroyed when the draft finally
  opened and reset the composer, and the send that followed found an empty box and returned with no
  message and no error, so the click read as "nothing happened". Nothing about drawing a draft needs
  the project record; only the model default does.
- **LT7:** A draft's model must never be a stand-in. Mode and permission preset default locally and
  the composer is editable at once, but the model is server-derived, so until the project's default
  arrives the draft holds *no* model: `state.currentModel` is null and the draft carries explicit
  `modelLoading` / `modelError` state, which the picker shows. The first send is unavailable for
  that window — the Send control is disabled, and the keyboard path refuses with the same reason,
  since it does not go through the control. `threads/start` may therefore never carry a fallback
  `model_ref`. This is a correctness rule, not a cosmetic one: a thread's provider is fixed once its
  first turn starts (LT5), and switching a started thread across providers is rejected, so a turn
  begun on a fallback binds the thread to the wrong provider permanently. When resolution fails, or
  the project has no valid default, the draft stays uncommittable until the user picks a model
  rather than falling back to one. An explicit model or reasoning effort chosen while resolution is
  in flight pins the draft: it resolves the draft, and a later-arriving default must not replace it.
  The default applies only to the draft it was requested for, never one the user has since sent,
  switched away from, or replaced.
- **LT8:** A draft send is identified by the draft it was issued for, not by its composer key. Two
  successive drafts in the same project share the key `draft:<project_id>`, so a slow
  `threads/start` for the first could otherwise return after the user opened a second one and clear
  its composer, open over it, or mark its rows failed — losing text typed in the meantime, the same
  class of loss as LT6. The continuation touches nothing unless the draft it was started for is
  still the one on screen.
- **LT9 (superseded by §8.3 "A new thread's starting model is derived, never stored"):** The
  project's model *catalog* (`GET /api/projects/{id}/models`) is a separate fetch from
  its default model, and failing it does not block the draft: the default is what `threads/start`
  carries, and the catalog only governs what else could be picked. The catalog is now the draft's
  only model source — there is no stored default to fall back to — so failing it does leave the
  draft with no model, and it says so rather than starting a turn on a guess. The failure is surfaced anyway —
  it leaves the picker with no options at all, so without a message it reads as a project with no
  models rather than a list that could not be loaded. This applies to any active project, not just a
  draft; per-source discovery *warnings* stay gated on an explicit reload, where they are not noise.

**Changelog (1.61 → 1.62), drop `pending_server_requests`:**
- **SR11a:** `LiveTurnSnapshot` no longer carries `pending_server_requests: Vec<ServerRequest>`.
  It was a precomputed outstanding set that duplicated a derivation already available on both ends:
  the outstanding server requests are the rows in `accumulated` whose latest event is a
  `server_request_received` and which are neither in `answered_server_requests` (the user answered)
  nor closed by a `server_request_resolved` later in `accumulated` (the harness closed it). They
  are reported in arrival order: a `server_request_received` appends the id (or updates its payload
  in place), a `server_request_resolved` drops it, and a re-sent id after a resolution re-appends
  at the end — so a reopen moves to the back, it does not restore its first-seen position. The
  client derives this with `outstandingServerRequests`, exactly as it already derives
  `outstandingApprovals`; the server's `pending_attention` derives it for the SB5 connect bootstrap.
  `answered_server_requests` is kept: it is the only record of a user's answer before/without the
  harness's own resolved event, so dropping it would replay an answered request as actionable and
  re-answering routes a stale id to the harness (SR6).
- **SR11b:** The client re-asserts the outstanding server requests *after* replaying `accumulated`,
  mirroring the approval path (SR10b): a later `error` overwrites the thread's activity and clears
  the active turn, so re-asserting last gives the waiting state the last word and keeps a turn
  blocked on a server request that then errored reading as waiting on the user rather than
  "errored, idle".

**Changelog (1.60 → 1.61), drop the single `pending_approval`:**
- **SR10a:** `LiveTurnSnapshot` no longer carries `pending_approval: Option<WireApprovalRequest>`.
  It was derived with `.iter().rev().find_map(...)`, so it named only the most recently raised
  approval and silently dropped the rest when a turn was blocked on several at once (three commands
  proposed together, say). The outstanding approvals are now derived on both sides from
  `accumulated` plus `answered_approvals`: every `ApprovalRequested` rides along in `accumulated`,
  answered ones included, and the client renders answered ones resolved and treats the rest as
  actionable. `PendingAttention.approval: Option` becomes `approvals: Vec`, so the SB5 connect
  bootstrap reports every approval that is still blocking the thread, not just one.
- **SR10b:** The client re-asserts the outstanding approvals *after* replaying `accumulated`, not
  before. That order is load-bearing: later events speak for the thread too, and an `error` in
  particular declares the turn inactive, so a turn blocked on an approval that then errored would
  come back reading "errored, idle" — no sidebar glyph, no waiting rank, no sub-agent hoist — while
  the approval sat in the transcript with nothing pointing at it. Re-asserting last gives the
  waiting state the last word, so it outranks the error. (The server-request side of this rule and
  the further unification of approvals and server requests into one request model are follow-ups.)

**Changelog (1.59 → 1.60), one "waiting on the user" state:**
- **SR8:** An approval and a server request both block a turn until the user answers, and the
  browser must present them as one state. Previously only approvals were ranked and rendered as
  blocked; a thread waiting on a server request fell through to the generic active-turn branch and
  was indistinguishable from one that was merely busy, and fired no notification. Codex itself
  already blurs the split — MCP tool approvals arrive as `requestUserInput` and are promoted to
  approval cards — so the distinction is a protocol artifact, not something to surface. Activity
  ranking, the sidebar glyph, the sub-agent monitor, and notifications now key off "waits on the
  user" rather than "is an approval". Card rendering stays per-kind: a decision has fixed choices, a
  server request has a per-method form.
- **SR9:** The waiting state must clear as soon as the user acts, not when the harness confirms.
  Answering an approval broadcasts `ApprovalResolved`, but a server request's resolved event comes
  from the harness on its own schedule and may never come, so the browser clears its own waiting
  state on send.

**Changelog (1.58 → 1.59), answered server requests survive a reload:**
- **SR6:** Answering a server request recorded nothing server-side. A request leaves the pending set
  only when the harness emits its resolved event, which arrives on the harness's own schedule and
  may never arrive at all. Until then the replayed `ServerRequestReceived` still reads as
  outstanding, so a reload rendered the request actionable again and answering it a second time
  routed a stale id to the harness, which errors — the defect AR1 already fixed for approvals. The
  answer is now recorded against the in-flight turn the moment it is routed.
- **SR7:** `LiveTurnSnapshot` carries `answered_server_requests`, and answered requests are excluded
  from both `pending_server_requests` and the SB5 connect bootstrap. The answered set is required
  in addition to the exclusion: the request still rides along in `accumulated`, and replaying that
  renders an actionable card unless the client is told it was answered — exactly as
  `answered_approvals` works.

**Changelog (1.57 → 1.58), replaying missed cross-thread activity:**
- **SB5:** `ThreadActivity` is broadcast live and was never replayed, so a browser that was closed or
  disconnected when a thread became blocked learned nothing about it: no sidebar badge and no
  notification, until that thread happened to be opened. For a managed sub-agent, which has no
  sidebar row, that meant knowing to look. On connect the server now sends the connecting client —
  and only that client, before it subscribes to anything — the set of threads currently waiting on
  the user, derived from the in-flight live buffers. Approvals the user already answered are
  excluded, on the same reasoning that excludes them from the live-turn snapshot.
- **SB6:** A connect replay is not a new event. Clients must repaint badges from it every time, but
  must alert at most once per page session for a given approval: a reconnect (tab resume, network
  blip) stays silent for one already alerted, while a genuine reload starts a new session and
  alerts again. A time-windowed dedup cannot express this, since it cannot answer whether an alert
  was ever shown.

**Changelog (1.56 → 1.57), surfacing a blocked sub-agent:**
- **SB1:** Approval requests already route correctly to a managed sub-agent thread, but that thread
  is deliberately absent from the sidebar, so every browser affordance keyed to a thread id used to
  no-op for it. The browser must therefore resolve thread identity from the cached per-project
  thread summaries — which include managed sub-agents — rather than from the rendered sidebar row.
- **SB2:** A managed sub-agent's `ThreadActivity` must be hoisted onto the nearest ancestor that
  does have a sidebar row, and that row displays the most urgent state among itself and its hidden
  descendants: `approval_requested` outranks `error`, which outranks an active turn. The row's
  tooltip names the originating descendant, and a distinct marker separates a hoisted state from
  the row's own. Ancestor walks are bounded so corrupted or cyclic ownership terminates.
- **SB3:** An approval notification for a sub-agent must name the child and its owning thread and
  say that a sub-agent is blocked. Because the server materializes a child thread on its own, the
  browser may receive activity for a thread it has never listed; it must refresh its cached thread
  lists before naming or navigating to such a thread rather than falling back to an id prefix or
  refusing to navigate.
- **SB4:** An approval also marks a turn active, so a sub-agent waiting on the user must be visually
  distinct from one that is merely running, both on the header sub-agent monitor and on its card.
  A card represents its whole owned subtree, since nested descendants are not listed separately.

**Changelog (1.55 → 1.56), image and file attachments:**
- **A1:** User input may carry transient attachments. Browser requests include attachment metadata
  and base64 bytes, but persisted and in-memory cached `UserInput` values omit raw bytes. Image
  MIME types must match PNG, JPEG, GIF, or WebP file signatures before image attachments map to
  Codex `UserInput::Image` data URLs. Non-image files, including PDFs, are transferred to the Codex
  harness host with `fs/writeFile`; the resulting harness-host path is appended to the prompt.
  Each turn uses a randomized harness-host temp directory that is removed on completion and
  lifecycle failures. Giskard does not write upload bytes into the project workspace.

**Changelog (1.54 → 1.55), harness-neutral sub-agent support:**
- **SA1:** Giskard models harness-neutral sub-agent links and linked child threads. Thread metadata
  carries `parent_thread_id`, `spawned_by_turn_id`, and `kind = subagent`; native imports reuse an
  existing `harness_thread_id`; and transcript links can idempotently resolve, import, and open a
  child. Rendering history alone never imports a thread.

  The Codex adapter maps both legacy `multi_agent_v1` (`collabAgentToolCall/spawnAgent`) and current
  collaboration v2 (`subAgentActivity` with `kind = started`) into that model. A real legacy spawn
  start has no receiver ID, so its completion exposes the child and retains the spawn prompt.
  Legacy `agentsStates` is keyed by native thread ID and must be read for the linked receiver;
  single-child `sendInput`, `wait`, `resumeAgent`, and `closeAgent` calls also provide lifecycle
  evidence, while multi-child waits remain unlinked. Current activity links contain no prompt, so
  Giskard uses `Sub-agent turn` rather than deriving input from inherited parent history. Their
  visible activity label uses the
  final non-empty agent-path component as the task name and omits the native child id; both full
  values remain in server-side link metadata. Browser wire links redact native thread IDs. Passive
  `TurnStarted` events and live snapshots carry the best known input; when a real prompt is
  available, a stable synthetic `user_message` is inserted or upgraded in place before output.
  Live and persisted transcripts therefore contain exactly one ordered prompt row, including
  server-resolved imports and prompts whose metadata arrives late. Synthetic fallback state is
  explicit rather than inferred from the literal text, so a real prompt equal to `Sub-agent turn`
  remains genuine input.

  Linked-thread discovery never changes established ownership: primary threads are not reclassified,
  children are not reparented, and self-links, cycles, invalid parent chains, and native-parent
  mismatches are rejected. Invalid persisted graphs are logged and remain visible in the primary
  sidebar for recovery. Reverse child-to-parent activity remains a navigation link and never creates
  a duplicate thread.

  Materializing or reopening a child is separate from monitoring it. Giskard starts a passive
  forwarder only for explicit non-terminal lifecycle evidence (`spawned`, `started`, `interacted`,
  `pending`, or `running`). Explicit active evidence has a ten-minute no-event pre-turn safety bound
  that restarts on activity and no longer applies after a turn starts. Terminal observations (`interrupted`,
  `completed`, `failed`, `shutdown`, or `not_found`) wake an existing idle monitor but never start a
  new one. Reopening a persisted child without lifecycle evidence does not arm a monitor, and an
  unchanged generated title does not rewrite its metadata.

  Linked children require an identity-preserving native resume. Codex may emit the activity link
  immediately before the new rollout becomes readable, so the adapter retries only the exact
  matching missing-rollout response for a short bounded window. It must return the advertised
  native ID and must never use the primary-thread fresh-session fallback for a child; otherwise the
  passive monitor can attach to a replacement thread and miss early commentary and command-start
  events from the real child. Primary thread recovery remains unchanged.

  Passive and interactive forwarders share the per-thread turn gate, so only one subscriber can
  persist a native turn. Direct child turns fail with `thread_turn_active` while delegated work owns
  the child; once idle, follow-ups are normal persisted turns that neither change ownership nor
  automatically report results to the parent. If terminal output arrives without native child
  events, its fallback is persisted immediately unless history already exists; monitor teardown
  and setup atomically claim a racing fallback so terminal results are neither lost nor duplicated.
  Idle monitors are cancellable and awaited by deletion, which performs another active-work
  preflight before removing any subtree records.

  A turn-scoped event can precede `TurnStarted`; the first such event creates the exact live-turn
  reconnect buffer, and the later start notification reuses it without discarding accumulated
  items. A genuine new start replaces a stale conflicting buffer without dropping the new turn;
  conflicting non-start events remain live and persistable without mixing buffers. Browser links
  carry only Giskard parent-thread and item IDs. The server derives native routing, prompt,
  lifecycle evidence, and `spawned_by_turn_id` from the authoritative live or persisted item;
  clients cannot assert ownership or lifecycle metadata.

  The browser omits valid managed children from the primary sidebar and exposes them through the
  **Sub-agents** header monitor with running state and harness-reported agent names. Child selection
  is restored after reload, and a child's **Parent** control opens its immediate owning thread,
  including for nested ownership.

  Deleting any thread recursively deletes its direct and transitive children from both the native
  harness and local persistence in leaf-first order. The server preflights the entire subtree, and
  an active turn or running task anywhere returns `409 Conflict` before deletion begins. The browser
  confirms descendant count, native scope, and irreversibility, then clears any deleted active view.
  HTTP imports, asynchronously observed imports, and deletion share one project lifecycle lock;
  HTTP contention is bounded to five seconds and returns `503 Service Unavailable`. Linked
  lifecycle evidence is processed through a per-parent FIFO so terminal evidence cannot overtake
  earlier active evidence. Materialization runs outside the parent event-forwarding path and
  repeated live-child activity avoids full project scans. Deletion cancels idle monitors and
  repeats its preflight before the first native or local record is removed.
  Codex treats only the exact JSON-RPC `-32600` response
  `no rollout found for thread id <requested-id>` as idempotent deletion success; a different ID or
  any other native failure still aborts before local deletion.

**Changelog (1.53 → 1.54), authoritative context-window metadata:**
- **C4:** Giskard has no model-name defaults table. Initial context capacity comes from exact
  config, provider-advertised `context_window` / `max_input_tokens`, or the conservative fallback.
- **C8:** harnesses may emit `ContextWindowUpdated` for a turn. The server persists the effective
  value for the event's exact `(provider, model)`, updates the active gauge only while that model is
  selected, and restores it after reloads and model switches without replacing it during turn
  completion.
- **C9:** the Codex adapter maps
  `thread/tokenUsage/updated.tokenUsage.modelContextWindow`, rejects invalid values with a warning,
  and suppresses consecutive unchanged reports within a turn. Resume-time historical usage replay
  is not treated as a new runtime observation.

**Changelog (1.52 → 1.53), project model-catalog consistency:**
- **M8:** the server caches the composed model descriptors per project and uses that same catalog
  for picker responses, new-thread creation, and model selection. Catalog-derived reasoning support
  and exact effort values therefore survive persistence and reach the harness.
- **M9:** config effort precedence is scoped to the exact `(provider, model)` entry. For models not
  declared under that provider, the harness catalog's reasoning-support flag and effort list are
  authoritative. The Codex adapter normalizes a non-`none` default with no selectable alternatives
  into a one-entry effort list, so an empty Codex alternatives list does not by itself disable
  reasoning effort.
- **M10:** global models used by project creation remain separate from the active project's harness
  catalog. Project changes invalidate stale picker options until the matching catalog loads.
- **M11:** provider and harness model-listing failures are returned as structured warnings with a
  source label and surfaced by the browser while the usable portion of the catalog remains active.

**Changelog (1.51 → 1.52), reliable Markdown finalization:**
- **M4:** completed agent, reasoning, and user messages use the Markdown renderer whether their
  rows are live, optimistic, or being built in a detached history/resync container. Detachment
  alone must not discard a valid render result.
- **M5:** asynchronous Markdown results are scoped to the active project/thread and the latest
  render request for that body, so stale responses cannot overwrite newer item content.
- **E6:** identified streamed items retain separate turn-scoped rows even when their deltas are
  interleaved; the legacy unscoped stream fallback is only for deltas without an item identity.

**Changelog (1.50 → 1.51), item identity and upsert invariants:**
- **B2/E6:** `ItemId` is the authoritative item lifecycle identity. Native item IDs are secondary
  adapter correlation keys and must never re-key or merge distinct Giskard items.
- **B2/P1:** repeated finalized values for one `(TurnId, ItemId)` replace the prior buffered value,
  including items with no native ID.
- **E6:** visually merged file-change rows remain turn-scoped and retain independent per-item
  contributions when one item is updated.

**Changelog (1.49 → 1.50), ImageView activity previews:**
- **IV1:** Codex `ImageView` activity rows render a bounded inline raster image preview in the
  transcript instead of exposing protocol metadata such as call ids. The preview is sourced through
  a workspace-confined `GET /api/projects/{id}/threads/{thread_id}/image?path=…` endpoint. SVG is
  intentionally excluded
  from inline preview v1 and remains a normal file/path link.

**Changelog (1.48 → 1.49), composer drafts and approval button contrast:**
- **UI1:** Unsent composer text is browser-local and scoped to the selected persisted thread, or to
  the per-project new-thread draft. Switching threads must save and restore the matching draft
  instead of sharing one textarea value globally.
- **UI2:** Approval action buttons must make `accept_for_session` visually distinct from the
  default/cancel button treatment while keeping Cancel in the neutral/default style.

**Changelog (1.47 → 1.48), cross-tab approval resolution:**
- **AR1:** When one browser client answers an approval request, the server broadcasts
  `ApprovalResolved { thread_id, request_id, decision }` to every client subscribed to that thread
  after the harness accepts the decision. Other tabs must resolve the matching approval card and
  remove its actions so a stale duplicate response cannot be submitted. Browser clients that created
  native notifications for that approval close only notifications keyed to the resolved
  `(thread_id, request_id)`.

**Changelog (1.46 → 1.47), cross-thread activity and browser diagnostics:**
- **TA1:** Inactive threads emit lightweight `ThreadActivity` WebSocket messages to all connected
  browser clients. These messages carry the thread id, activity kind, active-turn flag, optional
  kind-specific approval/server-request ids, and a short summary. They are intentionally separate
  from full transcript events so the sidebar can show progress without subscribing the browser to
  every thread's live stream.
- **TA2:** Approval requests from inactive threads, or from the focused thread while the page is
  hidden or the browser window is not focused, may create browser notifications when permission is
  granted. Notification clicks focus the target thread and scroll to the matching approval row when
  it is still pending. Duplicate lightweight/full approval paths are deduplicated client-side.
  Notifications are shown through a service worker (`/sw.js`, via
  `ServiceWorkerRegistration.showNotification()`), which is the only path that works on Chrome for
  Android where the `Notification` constructor is illegal; the worker delivers clicks back to the
  page by `postMessage`, and the page closes a resolved approval's notification by its stable
  `(thread_id, request_id)` tag. Contexts without an active worker (e.g. plain-http LAN access) fall
  back to the `Notification` constructor.
- **BD1:** The browser exposes a tucked-away Settings → Browser diagnostics panel with a bounded
  client-side event buffer, copy/clear actions, and a test-notification action. Diagnostics include
  notification lifecycle decisions, visibility/focus state, approval routing decisions, and
  WebSocket status changes.

**Changelog (1.45 → 1.46), per-process parsed history cache:**
- **HC1:** `PersistStore` maintains a per-process parsed JSONL history cache keyed by
  `(ProjectId, ThreadId)`. The authoritative source remains the on-disk history; cache entries are
  only reused when the index file's metadata still matches, appended to only after the disk append succeeds, and
  invalidated on thread/project deletion or unexpected metadata changes. This keeps thread switching
  and repeated history paging from reparsing unchanged histories while preserving the history-first
  crash-consistency contract.

**Changelog (1.44 → 1.45), transcript sticky-scroll after expanded cards:**
- **SC1:** Live transcript auto-scroll captures whether the user was already near the bottom before
  appending a new row. If so, the browser keeps the row anchored after synchronous card population
  and after asynchronous Markdown rendering or path linkification. Approval cards, generic server
  request cards, and completed transcript rows must remain visible when they arrive at the bottom;
  history prepends still preserve scroll position, and user-scrolled-up transcript views are not
  pulled to the bottom by unrelated live updates.

**Changelog (1.43 → 1.44), same-project turn concurrency:**
- **CT1:** A project-scoped Codex harness worker is an event pump, not a single-turn drain loop.
  After `turn/start` is acknowledged, the worker records that active turn and keeps accepting
  `thread/start`, `thread/resume`, and `turn/start` requests for other threads in the same project
  while continuing to drain Codex notifications. This preserves §6.5's same-project concurrent
  thread guarantee and prevents opening or sending in one thread from waiting behind a long turn in
  another thread.
- **CT2:** Codex server requests and approvals are non-blocking from the harness worker's
  perspective. The worker broadcasts the browser-visible request, records its owning thread, and
  continues processing Codex messages and normal harness commands; browser responses are routed by
  the stored request id later. Interrupting a thread best-effort cancels/rejects all pending
  approval/server requests owned by that thread so Codex is not left waiting on a request from an
  interrupted turn.
- **CT3:** Harness operations that are invalid during an active turn (`CompactThread`, archive, and
  delete) are rejected only for the target thread. Other threads in the same project can still be
  opened, renamed, compacted when idle, or started while a different thread is running.

**Changelog (1.42 → 1.43), turnless MCP/server-request hardening:**
- **M3:** MCP tool-call approval promotion no longer invents a `TurnId` when Codex omits
  `turnId`. Promotion uses the explicit native turn when present, otherwise the currently active
  turn for the thread; if neither exists, the request remains visible as a generic
  `ServerRequestReceived` card instead of becoming a fake-turn approval. Older Codex approval
  methods with no active turn are rejected through the unroutable-request path and logged.
- **SR5:** Turnless `ServerRequestReceived` events are registered for response routing and
  broadcast even if they arrive before the forwarder has seen `TurnStarted`. This preserves the
  browser prompt for MCP elicitations whose `turnId` is nullable while keeping them out of turn
  persistence until a real turn is owned.
- **INT3:** Interrupting a live turn while the Codex adapter is blocked on an approval/server-request
  response must not leave the adapter waiting for that original response. After a successful
  `turn/interrupt`, the adapter best-effort cancels/rejects the pending JSON-RPC request, logs any
  cleanup failure, and resumes draining Codex messages so `turn/completed` can release the active
  turn.

**Changelog (1.41 → 1.42), orphaned-thread recovery — read-only open + verified provider switch:**
- **RO2:** RO1's read-only degrade now also applies to `POST /api/projects/{id}/threads` for an
  existing `thread_id`. The browser opens a thread over HTTP *before* subscribing on the
  WebSocket, so a harness-attach failure there returned a 500 and the UI aborted before RO1's
  subscribe-side degrade could ever run. The endpoint now answers 200 with the persisted
  `harness_thread_id` and a `thread_read_only` warning, letting the client proceed to the
  (already degrading) subscribe. New-thread creation and explicit resume imports still fail hard
  (§13.6).
- **PS1:** Selecting a model from a different provider is now allowed on a **cold** thread (one
  not loaded in any harness process this server run — e.g. after a restart, or a read-only thread
  whose provider was removed from config). The switch performs a native `thread/resume` with the
  requested `model`/`modelProvider` and **verifies** the response reports them as effective
  before anything is persisted; Codex answers JSON-RPC success even when it ignores resume
  overrides for loaded threads, so success alone is never trusted (see
  `specs/model-provider-switching-analysis.md`). On an unconfirmed switch the fresh binding is
  dropped, a structured `thread_provider_switch_ignored` error is surfaced, and persisted state
  is untouched. Warm (loaded) threads keep the PB2 `thread_provider_locked` rejection.
- **PS2:** `ThreadHandle` carries `resumed_model` — the model/provider the harness reports as
  effective on open — and the registry binds the native model from it (truth over request). The
  Codex adapter populates it from the `thread/start`/`thread/resume` responses; the replay
  harness echoes the request. (Codex protocol SDK `codex-codes` bumped 0.143.0 → 0.143.2; the
  `model`/`modelProvider`/`thread.id` response fields the verification relies on are unchanged.)
- **PS3:** Selection stays **explicit**: no fuzzy matching picks a replacement provider
  automatically. The existing exact-id normalization (§8.4/E5) still auto-heals a stale provider
  when the model id exists under exactly one configured provider; otherwise the thread loads
  read-only (RO1/RO2) with the model picker **unlocked**, and the user chooses. The
  `thread_read_only` client state clears once a thread-state snapshot arrives under a different
  provider (the switch confirmed).
- **RO3:** The read-only state is now impossible to miss and tells the user what to do. The
  `thread_read_only` warning is **action-first** and **names the thread's provider** — claiming
  "provider \"X\" is no longer configured" only when config verifiably lacks it (attach failures
  can also be auth/spawn problems, which get neutral wording). The client renders it as a
  **persistent banner** above the composer instead of only an 8-second toast, disables the
  composer input and Send button with an explanatory placeholder/tooltip, and clears everything
  (banner, disabled state, picker lock rules) once a verified provider switch lands. The
  transient toast is suppressed for this code — the banner replaces it (§13.6).

**Changelog (1.40 → 1.41), MCP tool-call approval promotion:**
- **M1:** Codex surfaces MCP tool-call approvals as generic `ToolRequestUserInput` or
  `McpServerElicitationRequest` server requests rather than first-class approval requests.
  When the `codex_approval_kind: "mcp_tool_call"` marker is present (elicitation) or the
  question header is `"Approve app tool call?"` (requestUserInput), Giskard now promotes the
  request to a first-class `ApprovalRequested` event with `ApprovalKind::McpToolCall { server,
  tool_name }`. The user gets the same Accept / Accept-for-session / Decline / Cancel card as
  command and file approvals, and `AcceptForSession` is only offered when Codex advertised it.
  Non-MCP-tool requests fall through to the generic server-request path unchanged.
- **M2:** The approval response is built in the wire shape Codex expects for each transport:
  `requestUserInput` gets an answer map keyed by question id with the chosen option label
  (`"Allow"` / `"Allow for this session"` / `"Cancel"`); `elicitation` gets
  `{action: "accept", _meta: {persist: "session"}}` for session-scoped approval. Codex reads
  these to apply the session-remembered grant for the tool.

**Changelog (1.39 → 1.40), orphaned-thread read-only viewing:**
- **RO1:** Subscribing to a thread whose harness cannot attach — most commonly because its
  provider was removed from config (e.g. swapping a proxy provider id) — now **degrades to a
  read-only view** instead of failing the whole subscribe. The persisted history is still served
  and the attach failure is surfaced as a non-fatal `thread_read_only` warning; only a genuinely
  missing thread stays a hard error. Reading history is a persistence-only operation and no longer
  depends on a successful harness attach (extends the degraded-open philosophy of E2/E3 to the
  subscribe/history paths). New turns still cannot run on such a thread (§13.6).

**Changelog (1.38 → 1.39), public-exposure hardening pass:**
- **SEC1:** Login brute-force throttling: a global lockout with exponential backoff engages after
  repeated consecutive password failures and is checked **before** Argon2 verification, so failed
  floods can neither guess the password nor burn server CPU/RAM. Throttled attempts receive
  `429` + `Retry-After`; failed and throttled logins are logged with a stable message and the
  `X-Forwarded-For` value for external log watchers (§12.1).
- **SEC2:** Signed-token domain separation: session cookies and WebSocket tickets share the
  signing key but MAC distinct purpose domains, so a ticket (which travels in a URL query string
  and can reach proxy access logs) can never authenticate as a session cookie, nor vice versa.
  Existing sessions are invalidated by the token-format change (§12.1).
- **SEC3:** Sliding sessions made concrete: a cookie-authenticated request in the second half of
  the session lifetime re-issues the cookie for a full `session_days` window, and the cookie's
  `Max-Age` follows `session_days` instead of a hardcoded constant (§12.1).
- **SEC4:** Session revocation: `giskard-admin revoke-sessions` rotates `session.key`,
  invalidating every outstanding session and ticket after a server restart. Stateless HMAC
  sessions cannot be revoked individually and logout only clears the browser cookie (§12.1, §5.5).
- **SEC5:** Hardening response headers on every route: a strict `Content-Security-Policy`
  (`script-src 'self'`, `frame-ancestors 'none'`), `X-Content-Type-Options: nosniff`,
  `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, COOP/CORP `same-origin`, and a
  minimal `Permissions-Policy`. The single-page UI's script and stylesheet are served as
  separate same-origin assets (`/favicon.svg`, plus the script/stylesheet) so no inline script
  executes (§12.1, §13.1).
- **SEC7 (build identity & cache-busting):** `build.rs` stamps the binary with a git short hash
  (`-dirty` when the working tree has uncommitted changes; `unknown` without git) and SHA-256
  content hashes of the two assets. The script/stylesheet are served under content-hashed URLs
  (`/app.<hash>.js`, `/app.<hash>.css`) with `Cache-Control: immutable`, while `index.html` is
  served `no-cache` and points at the current URLs — so an upgraded binary can never be shadowed by
  a stale browser cache. The version is exposed to the browser via a CSP-safe
  `<meta name="giskard-version">` tag and shown (click-to-copy) in the Settings panel, so it is easy
  to confirm which build — and which assets — are live.
- **SEC6:** `browse.roots`, when configured, also confines `POST /api/projects`: a project's
  `dir`/`workspace_root` must canonicalize into an allowed root, closing the API bypass of the
  previously picker-only confinement (§6.2, Appendix C).

**Changelog (1.37 → 1.38), provider-bound model picker clarity:**
- **PB1:** Model picker labels include the provider, rendered as `Model name [provider]`, so models
  with the same or similar ids across providers are distinguishable before selection.
- **PB2:** Draft threads keep all providers selectable because no native Codex thread exists yet.
  Existing native-backed Codex threads disable model options from different providers and keep the
  selector on the current provider if a stale/forced `select_model` request is rejected.

**Changelog (1.36 → 1.37), lazy first-turn thread creation:**
- **LT1:** The browser's project `+` action opens an unpersisted draft thread. No local
  `ThreadFile` and no native Codex thread are created until the user sends the first message.
- **LT2:** New thread creation uses `POST /api/projects/{id}/threads/start` with initial text,
  model/provider, reasoning effort, mode, and permission preset. The server creates the native Codex
  thread with `thread/start`, persists the Giskard thread, and starts the first turn as one
  server-owned operation.
- **LT3:** Blank `POST /api/projects/{id}/threads` creation is rejected. That endpoint opens
  existing persisted threads and supports explicit native-id resume/import flows only.
- **LT4:** If native thread creation fails, no local thread is persisted. If persistence or
  synchronous `turn/start` fails after native creation, Giskard best-effort deletes the native
  thread, removes any local partial thread, logs cleanup failures, and surfaces the original
  browser-visible error.
- **LT5:** Provider selection for a new thread is fixed at first send. Because no native Codex
  thread exists during draft editing, the selected provider/model is sent directly to Codex
  `thread/start`; Giskard no longer creates an empty native thread and later replaces it.

**Changelog (1.35 → 1.36), observability gap closure:**
- **O1:** Turn startup and forwarding emit structured operator logs at the decision points needed to
  diagnose provider/proxy failures: harness `turn/start` acceptance or rejection, turnless harness
  errors before turn ownership, stream failures before completion, and forwarder exit while the
  active-turn gate is still held. Logs include project/thread ids, harness thread id when known,
  model/provider, mode, elapsed time, buffered state, and the underlying harness/Codex error.
- **O2:** HTTP/API errors are logged at the response boundary. Internal failures are `error`,
  conflicts are `warn`, and expected client errors remain `debug` so browser-visible failures are
  not silently returned without any server-side diagnostic trail.
- **O3:** WebSocket subscribe must not silently omit history when loading persisted history fails.
  The client receives a structured error, and the server logs the failure through the WebSocket
  action-error path with the affected thread/action.
- **O4:** Server-side event drops and recovery paths are observable. Slow/closed browser outbound
  queues, same-thread/different-turn harness events, duplicate notice/item suppression, failed
  persisted-history scans used for deduplication, history page-size config fallbacks, corrupt-file
  quarantine failures, and token-ledger load fallbacks emit structured logs with stable ids where
  available.
- **O5:** If the Codex stream closes or fails before any `turn/started` event arrives, Giskard emits
  a browser-visible harness error rather than only logging server-side. If a turn did start, Giskard
  still synthesizes a terminal failed turn using the `TurnId` bound from the `turn/start` response,
  so history records the failed attempt and the active-turn gate releases correctly. Giskard must
  not mint a replacement `TurnId` for fatal-error recovery if the `turn/start` response failed to
  bind one; that case is logged as a protocol/stream inconsistency instead. Codex error
  notifications carrying `turnId` remain turn-scoped, and fatal-error recovery clears the adapter's
  active native-turn binding for that thread.
- **O6:** Browser-only enhancement failures for Markdown rendering and path linkification degrade to
  plain text, but emit console warnings so UI diagnostics can distinguish expected fallback from a
  missing feature.
- **O7:** Codex-provided ids are preserved or consumed at the correct boundary. `threadId`,
  `turnId`, `itemId`, and `serverRequest/resolved.requestId` drive routing/correlation; approval
  `approvalId`/`itemId`/`callId` values remain routing/protocol details and must not be surfaced as
  user-facing card metadata; unknown future server requests that carry raw `threadId`/`turnId` are
  scoped through the same native-id registry instead of being relabeled onto the fallback thread.
  Because Codex can buffer a late
  `turn/completed` for the previous native turn while acknowledging a new `turn/start`, the Codex
  harness stream for the new turn only terminates on the acknowledged current `TurnId`; stale
  same-thread completions/errors are routed normally but must not complete or fail the new turn.
- **O8:** Codex binds `modelProvider` at native `thread/start`/`thread/resume`; `turn/start` can
  override `model` but not provider. Giskard records the model/provider used to open the native
  thread. Provider changes on an already-native-backed thread are rejected with a structured browser
  error explaining that the Codex thread is provider-bound; `send_input` applies the same rejection
  for any already-persisted provider mismatch. Same-provider model changes remain per-turn
  overrides. For new threads, LT1-LT5 avoid the old empty-thread rebinding case by delaying native
  creation until the first message carries the selected provider/model.

**Changelog (1.34 → 1.35), authoritative reconnect resync:**
- **RX4:** A browser `Subscribe` response without a resync cursor is an authoritative active-thread
  resync, not an append-only delta. The client clears transient browser-rendered transcript state
  before replaying the returned recent history and any live snapshot so failed-turn fallback bubbles,
  optimistic user rows, and stale active-turn flags cannot duplicate or survive a reconnect. When the
  client can supply a `since` cursor (its newest rendered turn), it instead requests an incremental
  `HistoryDelta` (H8) and keeps the immutable completed-turn DOM, repainting only the in-flight turn
  — falling back to the full authoritative resync when the cursor is unresolvable.
- **RX5:** WebSocket `error` events update persistent connection status but do not directly create
  warning notices. Warning/error notices are reserved for actionable foreground failures such as
  authorization failures, offline state, or abnormal foreground closes; sockets recently
  foregrounded after tab/mobile lifecycle suspension reconnect without toast spam. Once a socket
  successfully opens or receives a message while foregrounded, later failures are treated as normal
  foreground failures again.

**Changelog (1.33 → 1.34), mobile-friendly WebSocket reconnect UX:**
- **RX1:** Browser WebSocket disconnects are treated as a recoverable lifecycle state. The client
  reconnects with bounded exponential backoff, resubscribes to the active thread, and uses the
  existing thread state/history/live-turn/task snapshots to resync.
- **RX2:** Expected mobile/tab-suspension disconnects must not produce repeated error toasts. The
  thread header shows persistent connection state instead, while foreground auth/network failures
  remain visible through throttled warnings/errors.
- **RX3:** User messages are blocked while the WebSocket is reconnecting rather than queued. The
  composer stays editable so users can keep drafting, but no optimistic pending bubble is created
  until the socket is open.

**Changelog (1.32 → 1.33), thread-scoped WebSocket and Codex routing isolation:**
- **WS1:** Browser clients must reject stale messages from a replaced WebSocket connection and must
  ignore any thread-scoped server message whose `thread_id` does not match the currently selected
  thread. This guard applies before rendering or mutating transcript state for `ThreadState`,
  `HistoryPage`, `LiveTurnSnapshot`, `RunningTasks`, `Event`, and thread-scoped `Error`.
- **WS2 (superseded by ST1–ST3):** Thread token totals now travel only in revisioned
  `ThreadMetadata`; the standalone `TokenUpdate` message and `TokenScope` were removed.
- **WS3:** Event forwarders must verify that each incoming `AgentEvent.thread` matches the
  forwarder's owning `ThreadId` before attaching to a turn, updating live buffers, broadcasting, or
  persisting. Harness stream leakage across native subscriptions must therefore be ignored rather
  than written into the wrong Giskard JSONL history, and logged as an operator-visible error with
  the owner thread, event thread, event kind, and turn id when available.
- **WS4:** The Codex harness must broadcast mapped notifications and server requests to the
  `ThreadId` carried by the mapped `AgentEvent`, not merely to the thread whose stream is currently
  being drained. A `TurnCompleted` only ends the currently drained stream when the completed event
  belongs to that same thread; foreign-thread lifecycle events must not terminate or release another
  thread's forwarder.
- **WS5:** After at least one native Codex thread id is registered, non-empty unknown native
  `threadId` values are unroutable. The harness must reject/drop them with an operator-visible warning
  instead of falling back to the caller's scoped thread. Omitted `threadId` values may still use the
  scoped fallback for global/threadless notifications and requests.
- **WS6:** Reopening an already-open Giskard thread must preserve the existing per-thread harness
  sender/subscriptions. Metadata normalization while opening a thread may update persisted thread
  state, but it must not force a second native open or replace the live sender.

**Changelog (1.31 → 1.32), thread reasoning-effort selector:**
- **RE1:** The thread header shows an `Effort` selector immediately after the model picker when the
  selected model descriptor advertises `supports_reasoning_effort`. The selector offers `Default`
  plus the descriptor's exact `reasoning_efforts`; descriptors without an exact list use the common
  Codex-compatible fallback set. It sends the selected value through
  `SelectModel.model_ref.reasoning_effort` and hides for models without reasoning effort.
- **RE2:** Selecting `Default` for the current model clears the thread's explicit
  `reasoning_effort`, so the next turn omits the effort parameter. Switching away from and back to
  a reasoning model still restores that model's last explicit effort via the existing per-thread
  `model_efforts` map.

**Changelog (1.30 → 1.31), manual compaction completion hardening:**
- **CC4:** After `thread/compact/start` succeeds, the Codex harness keeps draining app-server
  notifications even when no command is running, so context-compaction notifications/items reach
  the browser and registry. A `Context compacted` marker is terminal only for marker-only
  compactions; once Codex emits `TurnStarted`, Giskard keeps draining until the matching
  `TurnCompleted` event.
- **CC5:** Manual compaction completion is robust to Codex versions that emit only a
  context-compaction item/notification and no normal `turn/completed`. If no `TurnStarted` was
  observed for the manual compaction, the first `Context compacted` activity item is treated as a
  terminal successful compaction turn, persisted as `/compact`, broadcast to the browser, and used
  to release the per-thread turn gate.

**Changelog (1.29 → 1.30), collapsible project sidebar groups:**
- **PC1:** Project groups in the left sidebar / mobile Projects drawer are collapsible so a user can
  hide a project's thread list without leaving the project behind. The collapsed/expanded state is
  a durable browser UI preference stored in local storage, not server state, and the New Thread
  action remains available from the project row.

**Changelog (1.28 → 1.29), per-thread turn gate:**
- **TG1:** Giskard enforces a server-side, per-thread turn gate around normal user turns and manual
  context compaction. The gate is reserved before calling the harness, so it covers the race before
  `TurnStarted` reaches the live buffer. Overlapping `SendInput` or `CompactContext` on the same
  thread is rejected with a structured `thread_turn_active` error; other threads and projects
  remain usable. The gate is released when the owned turn completes, or earlier startup paths fail
  or are cancelled. The browser must mirror this boundary optimistically once a `SendInput` frame is
  written to the WebSocket, then reconcile from any `send_input` error response; it must not wait for
  Codex's later `TurnStarted` notification before disabling additional sends and exposing Stop.

**Changelog (1.27 → 1.28), manual context compaction:**
- **CC1:** The context usage menu exposes a `Compact context` action that asks the active harness to
  compact the native thread context. For Codex this maps to app-server `thread/compact/start`, not
  to sending a literal `/compact` user message.
- **CC2:** While a manual compaction request is in flight, the header control is disabled and shows
  `Compacting...`. The control is also disabled while another turn is running. Giskard still relies
  on Codex for automatic near-limit compaction; no threshold warning is required.
- **CC3:** Manual compaction starts the same event-forwarding path as a normal turn so compaction
  activity is visible live and persisted in Giskard history.

**Changelog (1.26 → 1.27), appearance-aware transcript scrollbar:**
- **SB1:** The thread transcript owns a scoped thin scrollbar whose track, thumb, and hover colors
  are part of the active Appearance theme. Other browser scroll containers keep their native
  rendering unless they get their own explicit styling later.

**Changelog (1.25 → 1.26), settings menu and two-column shell:**
- **S1:** The desktop shell no longer reserves a right column for appearance-only content. The
  application layout is a left project/thread sidebar plus the main thread workspace.
- **S2:** Durable client UI preferences, starting with Appearance, live in a `Settings` popover
  opened from an icon button pinned to the bottom of the left sidebar. On mobile the same control is
  reached inside the Projects drawer.

**Changelog (1.24 → 1.25), transcript task grouping:**
- **TG1:** Command execution and tool/MCP call transcript items render inside top-level `Tasks`
  transcript rows. Every task item participates, including singletons. Consecutive task items in
  the same turn merge into the same group; any non-task transcript item or turn boundary closes the
  active group.
- **TG2:** A task group shows a compact chronological task list when expanded. Selecting a compact
  task expands that task's existing command/tool detail row inline inside the selected task card
  itself, and selecting the same task again collapses its detail. Transcript-row task selection
  updates in place without scrolling the thread; header Tasks-menu selection may still scroll to
  the task entry. The task preserves the original item id, lifecycle state, output/input collapse
  state, Stop action, and menu select/scroll behavior.
- **TG2a:** The task-group header is an aggregate control: activating it expands all task details
  in the group, or collapses all details when every task detail is already expanded. It does not
  perform an invisible no-op.
- **TG3:** Task groups remain expanded while tasks are running unless the user manually toggles the
  group. Once every task in a group reaches a terminal state, the group collapses automatically
  unless the user explicitly expanded or collapsed it.

**Changelog (1.23 → 1.24), tasks menu:**
- **TM1:** The thread header includes a `Tasks N` control for commands and tool/MCP calls that are
  still known running. The count is the current running-task snapshot size, and the control changes
  visual state between idle, running, and stop-requested tasks.
- **TM2:** Running-task cards move from the permanent right context panel into the `Tasks` popover.
  Selecting a task still scrolls to/selects the transcript row, and the same Stop action remains
  available from the menu.

**Changelog (1.22 → 1.23), context usage menu:**
- **CU1:** The thread-header context gauge is an interactive `Context` control. Activating it opens a
  popover that shows the current context footprint and the thread's cumulative input/output/total
  token usage. Cumulative tokens remain separate from the gauge source: they are informational totals
  and must not drive the context-occupancy numerator.
- **CU2:** Thread token totals no longer occupy a permanent right-column section. The right context
  panel kept running tasks until v1.24 moved them to the header `Tasks` menu; thread-level token
  details are reached from the header context control.

**Changelog (1.21 → 1.22), tool calls as running tasks:**
- **TK1:** The running-command surface is generalized to **running tasks**. `RunningCommand` →
  `RunningTask` with a `kind` (`command` | `tool`) and a `server` field; the `RunningCommands`
  server message → `RunningTasks { thread_id, tasks }`. The server registry tracks tool/MCP calls
  the same way it tracks commands (name + server, live output, elapsed time), so they appear in the
  same running-task summary. Tool calls carry no `process_id` and do not outlive their turn: a tool
  still running when its turn completes (an interrupted turn) is dropped; commands are kept as
  `after_turn`. Stopping a tool sends `Interrupt { thread_id }` (Codex has no per-call cancel);
  commands still `TerminateCommand` by process id. Tool progress arrives as `Text` item deltas.
- **TK2:** Tool-call transcript rows render input/output like command output: running rows stay
  expanded while small and may auto-collapse once large; completed tool-call input/output is
  collapsed by default regardless of size. The transcript row itself owns the toggle handler, so
  clicking the row (or pressing Enter/Space while focused) expands or collapses tool input/output.
  Tool-call lifecycle status uses the same symbol/wording and row placement as command lifecycle
  status, including best-effort elapsed/terminal duration when the start timestamp is available.

**Changelog (1.20 → 1.21), MCP status surface:**
- **MCP1:** The thread header includes an `MCP` control with a status dot and server count.
  Activating it opens an MCP menu that lists the active project's MCP servers, auth state, tool
  count, resource count, and expandable tool/resource detail. Servers that require OAuth expose an
  authenticate action when the harness supports it. Codex `unsupported` auth state means the
  server does not expose Codex-managed auth, not that the MCP server itself is unusable, so the UI
  presents it as a usable unauthenticated server state. MCP elicitation cards with an empty
  requested schema do not show an empty JSON content editor; accepting them sends empty content.
- **MCP2:** Giskard exposes project-scoped MCP REST endpoints backed by the harness:
  `GET /api/projects/{id}/mcp`, `POST /api/projects/{id}/mcp/reload`, and
  `POST /api/projects/{id}/mcp/oauth-login`. Codex maps these to `mcpServerStatus/list`,
  `config/mcpServer/reload`, and `mcpServer/oauth/login`. Server status is visible first;
  enable/disable is not implemented until the exact Codex config contract is intentionally modeled.

**Changelog (1.19 → 1.20), thread rename lifecycle:**
- **TN1:** The thread list actions menu includes `Rename`. Activating it edits the row title next
  to the `...` menu, not the read-only thread header. Enter saves; Escape/blur cancels. A
  successful rename updates the sidebar row and the header/mobile title when that thread is open.
- **TN2:** Rename calls the harness lifecycle operation first (Codex `thread/name/set`) and updates
  local `ThreadFile.title` only after success. Empty titles are rejected, whitespace is normalized
  to a single line, and native rename failure preserves the old local title.

**Changelog (1.18 → 1.19), thread archive/delete lifecycle:**
- **TD1:** Historical note: v1.19 kept native Codex thread creation/resume eager when a thread was
  opened. This is superseded by LT1-LT5 for new thread creation; opening existing persisted threads
  still resumes eagerly.
- **TD2:** The thread list exposes a per-thread `...` actions menu. Active threads offer `Archive`
  and `Delete`; archived threads offer `Unarchive` and `Delete`.
- **TD3:** Archive/unarchive calls the harness first (`thread/archive` / `thread/unarchive` for
  Codex) and only then updates the local thread metadata. Delete calls the harness first
  (`thread/delete` for Codex) and only then removes local metadata/history. Giskard rejects
  archive/delete while a turn or command is active.

**Changelog (1.17 → 1.18), Codex collaboration mode alignment:**
- **CM1:** Giskard Plan/Build mode maps to Codex `collaborationMode` on `turn/start`: Plan sends
  `collaborationMode.mode = "plan"` and Build sends `collaborationMode.mode = "default"`. This
  keeps Codex-only tool availability, including `request_user_input` /
  `item/tool/requestUserInput`, aligned with the visible Giskard mode and resets the app-server
  after a plan turn. Codex permissions are selected by the thread's permission preset (§9).
- **CM2:** The Codex harness initializes app-server with `capabilities.experimentalApi = true`,
  matching the current app-server contract needed for experimental interaction APIs such as
  collaboration modes and `request_user_input`.

**Changelog (1.16 → 1.17), thread-scoped permission preset:**
- **AP1:** The permission preset is now a concrete thread setting stored in
  `<thread_id>.json`. Project creation no longer asks for it, and `project.json` no longer owns
  an effective preset. New threads start with `ask_first`.
- **AP2:** `SetPermissionPreset` is thread-scoped: `SetPermissionPreset { thread_id, preset }` persists
  the selected thread's preset and broadcasts `ThreadState`. Threads in the same project can
  therefore run with different permission presets.

**Changelog (1.15 → 1.16), approval metadata:**
- **A3:** `ApprovalRequest` carries structured, card-facing `metadata` entries in addition to the
  backward-compatible `ApprovalKind` summary. Codex command approvals surface managed-network
  hosts, proposed network/exec policy amendments, and parsed command action paths; file approvals
  surface grant roots and changed paths; permissions approvals surface requested filesystem paths,
  glob/special entries, and network enablement. Path metadata is rendered as plain text unless the
  harness marks it as a validated workspace source file, in which case the browser uses the normal
  source-overlay link controls instead of burying it in an opaque detail string.

**Changelog (1.14 → 1.15), pending Codex server requests:**
- **SR1:** Codex `ServerRequest`s are no longer rejected as the normal unsupported path.
  Command, file, permissions, `execCommandApproval`, and `applyPatchApproval` requests are mapped
  to first-class approvals; MCP tool-call approvals (surfaced as `ToolRequestUserInput` or
  `McpServerElicitationRequest` carrying the `codex_approval_kind: "mcp_tool_call"` marker) are
  likewise promoted to first-class `McpToolCall` approval cards (M1). All other request methods
  are surfaced as pending transcript cards and wait for an explicit browser response.
- **SR2:** The neutral harness contract includes `respond_server_request`, and the browser sends
  `ServerRequestResponse { result | error }` for non-approval server requests. The Codex adapter
  preserves the original JSON-RPC request id (integer or string) when sending the response.
- **SR3:** The browser has dedicated pending-card handling for `item/tool/call`,
  `item/tool/requestUserInput`, and `mcpServer/elicitation/request`, plus an unknown-method
  fallback that can intentionally return `{}` or a JSON-RPC error.
- **SR4:** Live-turn snapshots include unresolved server requests as well as approvals, so reloads
  and reconnects do not lose browser-side work while Codex is waiting.

**Changelog (1.13 → 1.14), tool calls, approvals, and Codex parity:**
- **T1:** `ItemStart` carries optional `ToolCallStart` metadata for MCP/dynamic tool calls when
  the harness can provide it (`name`, `input`, optional `server`, `status`, `started_at_ms`).
- **T2:** The browser renders started tool calls as visible pending transcript rows immediately,
  including their server/tool name, status, and input. Progress deltas append to that row, and the
  later `ItemCompleted` finalizes it in place. A stuck MCP call therefore remains visible instead
  of looking like an idle active turn.
- **Q1:** Plan-mode Codex turns send `readOnly { networkAccess: true }`; Build-mode Codex turns
  send `workspaceWrite { networkAccess: true }`. Network reads are available in Plan mode, while
  agents remain responsible for avoiding mutating network actions during planning.
- **Q2 (superseded by SR1/SR2 in v1.15):** Every Codex `ServerRequest` must receive a JSON-RPC
  response. The temporary v1.14 unsupported-request rejection path has been replaced by pending
  server-request cards and explicit browser responses.
- **A1:** Browser clients render live approval requests as actionable transcript cards, not
  transient notices. The card sends `ApprovalDecision` messages for command, file, and permissions
  approvals and is de-duplicated across live-turn snapshots.
- **A2:** Codex `item/permissions/requestApproval` is a supported approval request. Accept replies
  with `{ permissions, scope: "turn" }`, accept-for-session replies with
  `{ permissions, scope: "session" }`, and decline/cancel use JSON-RPC errors, matching CodexUI's
  app-server contract.

**Changelog (1.12 → 1.13), rendered agent Markdown:**
- **M1:** Agent and reasoning messages are GitHub-flavored Markdown. The server renders them to
  sanitized HTML via `POST /api/projects/{id}/threads/{thread_id}/render`; the browser injects the
  returned HTML.
  Rendering happens when the `ItemCompleted` message is finalized (not per delta); the raw text is
  shown until the render resolves, so streaming stays readable and a failed request degrades to
  plain text.
- **M2:** Rendering is a superset of the `/linkify` pass: detected workspace paths become the same
  `.path-link` controls, wrapped inline during rendering. Paths inside code spans/fenced code
  blocks are left literal. `/linkify` remains for command output (which is not Markdown).
- **M3:** The renderer is the trust boundary. It escapes all text, never passes through raw HTML in
  the source (it is escaped to inert text), and only emits `href`s with an `http`/`https`/`mailto`
  scheme; images are not fetched (alt text is shown). Output is safe to inject as trusted HTML.
- **M4/M5:** Finalized message rows invoke the same renderer for live, optimistic, history, and
  resync paths. A row may be rendered while detached before insertion into the transcript. Each
  asynchronous result applies only if its project/thread scope and per-body request identity still
  match, preventing both dropped detached renders and stale-response overwrites.

**Changelog (1.11 → 1.12), live-turn interruption and running commands:**
- **I1:** The browser exposes a Stop control while a turn is live and sends
  `Interrupt { thread_id }`; the control is disabled while the interrupt request is in flight.
- **I2:** Harness adapters must be able to process interrupt/control commands while a turn is
  streaming, not only while waiting for an approval request. Normal queued user messages remain a
  separate policy decision.
- **R1:** Command execution items are surfaced as transcript rows with live output, elapsed time,
  lifecycle status, and a Stop control when the harness supplies a process id.
- **R2:** The UI includes a running-command summary. Selecting a summary row scrolls to and selects
  the matching transcript command row.
- **R3:** The server maintains a running-command registry from command start/output/completion
  events, separate from the live-turn buffer, and broadcasts running-task snapshots on subscribe
  and after registry changes. The current wire message is `RunningTasks` (generalized in TK1).
- **R4:** Harness adapters that can observe command lifecycle notifications after `TurnCompleted`
  continue draining them while commands are known running. Late terminal command completions update
  the running-command registry and may be broadcast to connected clients without mutating persisted
  completed turn history.
- **R5:** Command rows and summaries distinguish running, succeeded, failed, and
  terminated/declined/interrupted states with both a fixed symbol and subtle state color.
- **R6:** `TerminateCommand { thread_id, process_id }` is a request to the active harness. Giskard
  must not terminate local processes directly. The adapter uses the process-specific control
  operation appropriate to its harness and must not substitute turn interruption for command stop;
  users can interrupt the whole turn separately when that broader cancellation is what they want.
- **R7:** A command marked `terminating` means "stop requested through the harness", not "process
  terminated". The browser labels this state as "stop requested" and preserves the later terminal
  command status reported by the harness. If the harness reports normal successful completion after
  a stop request, the command row shows the successful completion annotated with "stop requested"
  and the server logs a structured warning.
- **R8:** Stop-request failures are surfaced through the normal structured `Error` path and the
  command remains visible with `terminating: false`. An adapter may classify a harness-specific
  not-found response as stale-state cleanup only for commands already marked `after_turn`; that
  classification must be documented by the adapter.

**Changelog (1.10 → 1.11), source target positioning:**
- **L7:** Opening a source link with a target line centers that line in the code overlay viewport
  when possible, instead of pinning it near the top, so surrounding context remains visible.

**Changelog (1.9 → 1.10), colon source line targets:**
- **L6:** Path linkification accepts `path:<line>` and compiler-style `path:<line>:<column>` in
  addition to `path#<line>`. The column is kept in the clickable span but the overlay targets the
  line.

**Changelog (1.8 → 1.9), source overlay line targets:**
- **L4:** Code overlay previews render a left-side line-number gutter for text files.
- **L5:** Path linkification recognizes line-target references such as `path#<line>`. The server
  validates `path` exactly like a normal link, returns the normalized path plus an optional target
  line, and the UI opens the overlay scrolled to that line.

**Changelog (1.7 → 1.8), code overlay implementation slice:**
- **L1:** Wired the existing Phase 4 highlight/linkify/raw-file backend into the served browser UI:
  completed agent/reasoning text and command output are linkified through the server endpoint, and
  clicked paths open a code overlay with server-side `syntect` HTML plus a download action (§11.2).
- **L2:** Hardened path detection for absolute workspace paths, `./`-prefixed relative paths, and
  sentence/markdown punctuation. The linkifier still validates every candidate by canonicalizing it
  under the workspace root before surfacing a link (§11.2).
- **L3:** Full large-file virtualization remains Phase 4 follow-up work: oversized or binary files
  currently render metadata and a download-only fallback, while the endpoint already accepts line
  ranges for future paginated viewing (§11.3).

**Changelog (1.0 → 1.1), from review:**
- Resolved thread token schema vs §10.2: thread `tokens` now carries `total` + `by_model` (§5.3).
- Config models are now **typed** `[[providers.<id>.models]]` entries with a documented metadata
  precedence + conservative fallback (§8.3, App. C), fixing the missing `context_window` /
  `supports_reasoning_effort` source.
- `AgentHarness::shutdown` is now `&self` (object-safe for `Arc<dyn AgentHarness>`); added an
  explicit object-safety note (§4.3).
- Added §4.5 **normative type sketches** for all previously-undefined referenced types (`Item`,
  `FileDiff`, `ApprovalRequest`, `HarnessError`, `OpenThreadOptions`, `ThreadHandle`,
  `TurnStatus`, `UserInput`, etc.).
- Removed `attachments` from `SendInput`; attachments were out of v1 scope at that point
  (superseded by the 1.56 attachment contract above).
- Defined "session" for `accept_for_session` = harness-process lifetime, fail-closed on respawn (§9.2.1).
- Defined reconnect/live-turn resync via a per-turn in-memory live buffer + snapshot (§13.6).
- Clarified Plan mode × permission preset: collaboration mode and permissions are orthogonal; the
  preset value is preserved (§9.1).
- Made `tokens-global.json` a single-writer **ledger actor** (cross-project hot file) (§5.4).
- Pinned **Dioxus 0.7** and forbade the auto-Tailwind path (no-npm) (§13.1).
- Named candidate Codex context-usage fields + selection order (§10.3).
- Corrected Codex client crate references to real crates: `codex-app-server-sdk` (recommended),
  `codex-codes`, `codex-app-server-protocol` (§3.3, App. A, App. D).

**Changelog (1.1 → 1.2), from D2 investigation against installed Codex CLI 0.142.5:**
- **D2 resolved: `codex-codes` v0.143.0 is the chosen client crate** (async-client feature).
  Verified on crates.io: its `AsyncClient` API maps 1:1 to `AgentHarness`, it tracks Codex CLI
  0.143.0 (≈ installed 0.142.5), includes a schema-drift scorecard for CI (§14.4), and ships real
  JSONL test captures. Reordered §3.3 + App. A to put `codex-codes` first; updated App. D item 2
  from "open" to "resolved". Fallback (`codex-app-server-sdk` v0.5.1 or hand-rolled) only if a
  future CLI version diverges.

**Changelog (1.2 → 1.3), from review (integration pass over v1.2):**
- **B1:** Added the normative `Turn` type sketch (§4.5); `Thread.turns` persists `Vec<Turn>` (§5.3).
- **B2:** Split the Giskard-owned item id from the harness-native id: `ItemId(Ulid)` + a separate
  `harness_item_id: String` field on `Item`/`ItemStart` (§4.5). Applies the thread-id pattern to items,
  so persistence, the diff viewer, and the code overlay no longer depend on Codex item-id stability
  across resume.
- **B3:** Documented that the single `TokenUsage { input, output, total }` struct is reused for both
  per-turn usage and cumulative ledger/`by_model` sums — no parallel `TokenTotals` type (§4.5, §10.2).
- **B4:** Required `CodexHarness` to maintain an explicit `harness_thread_id ↔ ThreadId` map,
  populated at `open_thread` and used to translate inbound notifications, including the resume case
  where the native id is re-established (§4.7).
- **B5:** Renamed the `ItemStarted` **struct** to `ItemStart` to remove the collision with the
  `AgentEvent::ItemStarted` **variant** (§4.4, §4.5).
- **C1 (most important):** Resolved the core-vs-proto ownership contradiction at the WASM boundary.
  `giskard-core` stays native/authoritative; `giskard-proto` owns the wire vocabulary and defines
  `Wire*` mirror types for every payload that carries a `PathBuf` (paths become `String` via a
  server-side lossy conversion); path-free domain types are re-exported through `giskard-proto`. The
  server maps `core → wire` at the outbound boundary. `giskard-ui` depends only on `giskard-proto`
  (§3.2, §3.5, §13.6).
- **C2:** `ApprovalDecision` is path-free (its `AcceptWithExecPolicyAmendment { amendment: Vec<String> }`
  round-trips as JSON), so it is re-exported through `giskard-proto` rather than mirrored — consistent
  with the C1 decision (§3.5, §9.2).
- **C3:** The per-model token breakdown is stored as a **nested object** (`by_model[provider][model]`),
  not an interpolated `"provider/model"` string key, because provider/model ids can contain slashes
  (e.g. `@cf/z-ai/glm-4.7`) and would be ambiguous to re-split (§5.3, §10.2).
- **C4:** Thread `context_window` is a **cache**, not a source of truth. This original descriptor-only
  rule is superseded by 1.54/C8: harness-reported runtime values are retained per model and take
  precedence over initial descriptor metadata (§5.3, §8.4, §10.3).
- **C5:** Defined the resume-failure policy: if resume-by-id fails (Codex thread store purged/rotated),
  start a fresh native thread, keep the Giskard-side history, and warn the user that agent context was
  lost (§4.7, §7.1).
- **S1:** Corrected the §4.6 mapping table — the `initialize`/`initialized` handshake happens once per
  process (per project), not per thread; `thread/start` maps to `open_thread`.
- **S2:** Removed `TurnStatusKind::Declined` (no producer; the pinned Codex `TurnStatus` is
  `Completed | Interrupted | Failed | InProgress`) (§4.5).
- **S3:** Renamed `HarnessError::Timed` → `HarnessError::Timeout` (§4.5).
- **S4:** Aligned `Effort` to the pinned Codex `ModelReasoningEffort`
  (`minimal | low | medium | high | xhigh`) instead of hardcoding three values (§4.5, §8.5).
  Since generalized: `Effort` is now a transparent string newtype (Codex's `ReasoningEffort` is
  itself a bare string), so model-defined efforts round-trip without a fixed set (§8.5).
- **S5:** Documented that project ordering defaults to ULID creation order; the `projects.json`
  `order` field is reserved for a future manual/drag reorder and is not yet surfaced by the UI (§5.3).

**Changelog (1.3 → 1.4), from review (Phase 3 contract hardening):**
- **P1:** Removed the effort double-home: `TurnOverrides.reasoning_effort` is dropped — effort
  lives only in `ModelRef.reasoning_effort` (§8.1). `TurnOverrides` is now a **resolved snapshot**
  (not a delta): the server builds it at `start_turn` from the thread's current mode, current model
  (which carries effort), and effective permission preset. `TurnOverrides.model = None` means "reuse
  the thread's current model." `TurnOverrides.permission_preset` remains in the struct but is now the
  preset snapshot (read from durable state or coerced), not a per-turn override — see P3/AP1
  (§7.5).
- **P2:** `SwitchMode` and `SelectModel` now **persist immediately** and echo state: the new
  mode/current_model is written to `<thread_id>.json` before the server returns, then a
  `ThreadState` is broadcast to all connected tabs so they stay in sync. The sandbox/model effect
  still takes hold at the next turn; only the stored intent is now durable (§7.4, §13.6).
- **P3 (superseded by v1.17 AP1/AP2):** Downgraded the "overridable per turn"
  permission-preset claim. `TurnOverrides.permission_preset` is no longer a per-turn override — it is
  the preset snapshot the server reads from durable state and includes so the harness can pass it to
  `turn/start` (§9.1, §13.6).
- **P4:** The plan-dump write path (§7.4.1) now explicitly cross-references §6.2's path-confinement:
  the resolved path is canonicalized and anything escaping the workspace root is rejected before
  writing.
- **C6:** Confirmed "current plan" = strictly the single most recent Plan-mode turn; no
  concatenation of earlier plan turns, even when they held content the user might expect (§7.4.1).
- **C7:** Per-model effort retention: switching away from a reasoning model preserves its effort
  value; switching back restores it. The effort param is never sent when the active model doesn't
  support it (§8.5 already handles the send-side) (§8.4).
- **C8:** Policy coercion for degraded harnesses: on harness attach, if the harness lacks
  `live_approvals` and the stored preset needs live approval support, the effective preset is
  coerced for that session without overwriting the stored value, and a notice is surfaced (§9.4).
- **S6:** Approval diff preview in Phase 3 uses the **raw diff string** from the harness; structured
  `FileDiff` parsing is deferred to Phase 4 (§9.2, §15).
- **S7:** When `plan_build_modes = false`, `Mode` resolves to the Build-equivalent single mode, so
  `TurnOverrides` is well-defined for every harness (§7.5, §13.5).

**Changelog (1.4 → 1.5), from usability/debugging pass:**
- **E1:** Added structured, flattened server errors with stable `code`, `severity`, `message`,
  optional `detail`, `thread_id`, and `action`, and required WebSocket parse/handler failures to be
  sent to the browser and logged without panicking (§13.6).
- **E2:** Added degraded-open warnings: `ThreadHandle.warning` / `OpenThreadResponse.warning`
  surface non-fatal resume/attach failures while keeping the persisted Giskard thread usable (§4.5,
  §13.6).
- **E3:** Defined persisted-thread reopen semantics: opening or subscribing to an existing thread
  reattaches the harness using the stored native thread id and preserves the durable Giskard
  `ThreadId`; if native resume fails, Giskard starts a fresh native session and warns (§4.5, §7.1).
- **E4:** Added a short-lived signed WebSocket ticket endpoint for browser clients that cannot rely
  on the session cookie during upgrade; `/api/ws` accepts either the session cookie or the ticket
  (§12.1, §13.6).
- **E5:** Required model refs loaded from projects/threads to be normalized against configured
  providers when a stale provider id names a model that exists under exactly one configured provider;
  unsupported reasoning effort is cleared during normalization (§8.3, §8.4).
- **E6:** Required live UI rendering to de-duplicate completed items by Giskard `ItemId` and
  harness-native `harness_item_id`, so streamed deltas finalize in place instead of duplicating the
  completed agent response (§13.6).

**Changelog (1.6 → 1.7), split thread persistence into metadata + JSONL history:**
- **Motivation:** history previously lived inside `<thread_id>.json` as a `turns[]` array, rewritten
  in full on every turn — so listing/restoring parsed whole histories and per-turn write cost was
  O(history). The `.jsonl` (formerly "disposable") is now the **authoritative** history and the
  `.json` a small metadata/aggregates file.
- **H1:** Two files. `<thread_id>.json` = metadata only (version, id, project_id, revision, title,
  harness_thread_id, mode, current_model, context_window cache, token aggregates, timestamps — no
  `turns[]`). `<thread_id>.jsonl` = authoritative history, **one `Turn` per line**, append-only
  (§5.2, §5.3, §5.4).
- **H2:** Append path is a single `write()` of `JSON + "\n"` to an `O_APPEND` file — atomic against
  concurrent writers and process-kill on local POSIX (no app lock for append ordering); the loader
  tolerates a torn final line (skips it) for the power-loss case. NFS/network storage is out of
  scope (§1.2 local-first).
- **H3:** Append history first, then update metadata aggregates. Aggregates are a recomputable
  cache (like `context_window`, C4); `recompute_aggregates(thread)` folds the JSONL to repair after a
  crash between the two writes.
- **H4/H6:** Restore/list read only `.json` (no history parse). Opening a thread loads the last N
  turns in its staged bootstrap; older pages load on demand through authenticated
  `GET /api/projects/{project_id}/threads/{thread_id}/history?before={turn_id}&limit={count}`.
  The response is `{ before, turns: [WireTurn], has_more }`. Page sizes are config
  (`[history] initial`/`page`, §16.3), and the HTTP endpoint caps a page at 100 turns. `TurnId`
  (ULID) is the pagination cursor — no separate pagination index.
- **H5:** The loader composes `[last N turns from JSONL] + [live turn from the live buffer]`; the
  in-flight turn is not in the JSONL until `TurnCompleted`.
- **H8:** Incremental reconnect: a `Subscribe { since: TurnId }` cursor requests only the turns after
  it, served history-first as `HistoryDelta { thread_id, turns: [WireTurn] }` (via
  `load_turns_after`). Because persisted turns are immutable, the browser keeps its completed-turn
  DOM and repaints only the in-flight turn. An unresolvable cursor (stale/unknown turn) falls back to
  a full-page bootstrap sent history-first so the client rebuilds cleanly before the live snapshot.
- **H7:** `giskard-admin`: `compact_thread`/`dump_thread` operate on the `.jsonl`, plus
  `recompute_aggregates`; `validate` parses the JSONL line-by-line and reports the first bad line
  rather than quarantining whole histories (§5.5).

**Changelog (1.5 → 1.6), from typed transcript rendering pass:**
- **E7:** File-change and tool-call items are visible transcript items, not hidden/empty agent
  bubbles. `FileChange` keeps a backward-compatible summary `path`/`change` plus optional
  per-file `changes` and `status`; `ToolCall` preserves server, status, and error metadata
  (§4.5, §13.6).
- **E8:** Added a generic `Activity` item kind/payload for Codex app-server items that are not chat
  text but must still be surfaced, such as web searches, image events, sub-agent activity, context
  compaction, and model reroutes (§4.5).
- **E9:** The browser must replay `LiveTurnSnapshot` accumulated events on subscribe/reconnect and
  track `ItemStarted.kind` so streamed deltas are styled as command, file-change, tool-call,
  reasoning, or activity rows before finalization (§13.6).

---

## Table of Contents

1. [Overview & Goals](#1-overview--goals)
2. [Glossary & Concepts](#2-glossary--concepts)
3. [System Architecture](#3-system-architecture)
4. [The `AgentHarness` Abstraction](#4-the-agentharness-abstraction)
5. [Data Model & Persistence](#5-data-model--persistence)
6. [Project Management](#6-project-management)
7. [Threads & Turns](#7-threads--turns)
8. [Model Selection & Providers](#8-model-selection--providers)
9. [Approvals & Permissions](#9-approvals--permissions)
10. [Token Tracking](#10-token-tracking)
11. [Visualization: Diffs & Code Overlay](#11-visualization-diffs--code-overlay)
12. [Authentication](#12-authentication)
13. [UI / UX](#13-ui--ux)
14. [Testing Strategy](#14-testing-strategy)
15. [Implementation Phases](#15-implementation-phases)
16. [Appendices](#16-appendices)

---

## 1. Overview & Goals

### 1.1 Purpose

Giskard is a web application that lets a single user drive one or more agentic coding CLIs
from a browser, on desktop and mobile, instead of a terminal. It manages multiple projects,
each containing multiple concurrent conversation threads, streams the agent's work in real
time, visualizes file changes and referenced source files, and tracks token usage.

### 1.2 Hard Constraints

These are non-negotiable and shape every downstream decision:

- **Rust everywhere.** Backend and frontend are both Rust. The frontend is compiled to
  WebAssembly via Dioxus. There must be **zero** dependency on npm, Node.js, Yarn, a
  JavaScript bundler, or any JS package manager in the build pipeline. The only acceptable
  JS is small hand-written glue if strictly unavoidable (see §13.7), checked into the repo,
  not fetched from a registry.
- **Local-first.** The application and the agent harness processes run on the same machine.
  Remote execution is explicitly out of scope for v1 but the abstractions must not preclude
  it.
- **Single-user.** One shared password protects the whole app. No user accounts, no roles,
  no multi-tenancy. (The word "permissions" in this document refers to *agent action
  approvals*, never to user roles — see §9.)
- **Harness-agnostic.** Codex is the only harness implemented in v1, but all
  agent-facing logic goes through a trait (`AgentHarness`). Adding another harness (e.g.
  Claude Code) later must not require touching the persistence layer, the UI, or the core
  domain model.
- **Everything is tested.** Unit tests for pure logic, integration tests driven by
  **deterministic recorded replays** of agent sessions (no live LLM calls in CI), and a
  small headless-browser end-to-end suite to guard against UI regressions. Testability
  (a mockable harness transport) is a design input from day one, not an afterthought.

### 1.3 Non-Goals (v1)

- Remote / multi-machine harness execution.
- Multiple end users, role-based access control, or per-user data isolation.
- Git integration for the diff viewer (staging, committing, branch ops). The diff view is
  read-only visualization of agent-produced changes.
- Accepting/rejecting individual diffs (visualization only).
- A second harness implementation (the *abstraction* is in scope; a working Claude Code
  adapter is not).

### 1.4 Target Scale

Mono-user, roughly **up to ~10 concurrently active threads**. The design should be simple
and correct at this scale rather than optimized for high concurrency.

### 1.5 Naming

The project is **Giskard** (after R. Giskard Reventlov, the orchestrating robot of Asimov's
Robot series). The Cargo workspace uses `giskard-*` crate names throughout (see §3.2).

---

## 2. Glossary & Concepts

| Term | Definition |
|------|------------|
| **Harness** | An underlying agentic coding CLI that does the actual model interaction and tool execution. v1: Codex CLI. Abstracted behind the `AgentHarness` trait. |
| **Project** | A working context bound to exactly one filesystem **directory**. Holds metadata, configuration, and a set of threads. Backed by one harness process instance. |
| **Workspace root** | The directory the agent is allowed to read/write within (the harness sandbox boundary). Defaults to the project directory; overridable per project. |
| **Thread** | A durable conversation within a project (maps to a Codex *Thread*). Contains an ordered sequence of turns. Resumable across restarts. |
| **Turn** | One unit of agent work initiated by a single user input (maps to a Codex *Turn*). Produces a sequence of items and ends with a completion carrying token usage. |
| **Item** | The atomic unit of agent input/output within a turn: a user message, an agent message, a reasoning note, a command execution, a file change, an approval request, a diff. Has a lifecycle: `started` → optional `delta`s → `completed`. |
| **Mode** | A thread-level collaboration state: **Plan** (the agent analyzes and proposes) or **Build** (the agent implements). Switchable within a thread (§7.4). |
| **Approval** | A server-initiated request from the harness asking the user to allow or deny a command execution or file change. Handled per the thread's permission preset (§9). |
| **AgentEvent** | Giskard's internal, harness-neutral representation of everything streamed from a harness. Codex protocol messages are mapped into `AgentEvent`s. |
| **Replay** | A recorded sequence of harness transport messages, played back through a mock harness for deterministic testing (§14). |

### 2.1 Conceptual hierarchy

```
Config (global)
└── Project (1 directory, 1 harness process)
    ├── ProjectConfig (workspace root, harness kind, …)
    └── Thread (durable conversation)
        ├── ThreadMetadata (mode, current model, permission preset, token totals, context window)
        └── Turn (one user input → agent work)
            └── Item (message / reasoning / command / file-change / diff / approval)
```

---

## 3. System Architecture

### 3.1 High-level component diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                          Browser (WASM)                                │
│  Dioxus frontend  ── single multiplexed WebSocket ──┐                  │
│  (desktop + mobile responsive UI)                    │                 │
└──────────────────────────────────────────────────────┼────────────────┘
                                                        │  WS frames
                                                        │  (client↔server
                                                        │   protocol, §13.6)
┌───────────────────────────────────────────────────────▼────────────────┐
│                       giskard-server (Axum)                             │
│                                                                         │
│  ┌───────────┐   ┌──────────────┐   ┌───────────────┐  ┌─────────────┐  │
│  │ Auth /    │   │ WS hub        │   │ Domain / app  │  │ Persistence │  │
│  │ session   │   │ (fan-out)     │◄─►│ services      │◄►│ (flat files)│  │
│  └───────────┘   └──────┬───────┘   └───────┬───────┘  └─────────────┘  │
│                         │                    │                          │
│                  ┌──────▼────────────────────▼──────┐                   │
│                  │        AgentHarness trait         │                  │
│                  │  (harness-neutral event stream)   │                  │
│                  └──────┬───────────────────┬────────┘                  │
│                         │                    │                          │
│              ┌──────────▼───────┐   ┌────────▼──────────┐               │
│              │ CodexHarness     │   │ ReplayHarness     │  (tests only) │
│              │ (JSON-RPC client)│   │ (recorded fixture)│               │
│              └──────────┬───────┘   └───────────────────┘               │
└─────────────────────────┼───────────────────────────────────────────────┘
                          │  JSON-RPC 2.0 over stdio (newline-delimited)
                          │  (one app-server process per project)
                ┌─────────▼──────────┐
                │  codex app-server  │  ── OpenAI / provider API ──►  LLM
                │  (child process)   │  ── sandboxed FS / shell   ──►  project dir
                └────────────────────┘
```

### 3.2 Cargo workspace layout

A single Cargo workspace with focused crates. Names are prefixed `giskard-`.

| Crate | Responsibility |
|-------|----------------|
| `giskard-core` | Harness-neutral domain types: `Project`, `Thread`, `Turn`, `Item`, `AgentEvent`, `UserInput`, `Mode`, `ModelRef`, `TokenUsage`, IDs, error types. No I/O. Pure, fully unit-testable. |
| `giskard-harness` | The `AgentHarness` trait + `HarnessCapabilities`. Defines the neutral contract only. |
| `giskard-harness-codex` | `CodexHarness`: spawns/manages `codex app-server`, speaks JSON-RPC, maps Codex protocol ⇄ `giskard-core` types. |
| `giskard-harness-replay` | `ReplayHarness`: reads a recorded transcript, emits the same `AgentEvent` stream deterministically. Used by integration tests and for a "demo mode". |
| `giskard-persist` | Flat-file persistence: load/save projects, threads, token ledgers; atomic writes; a small maintenance/debug API (list/inspect/delete). |
| `giskard-server` | Axum app: routes, auth/session, WebSocket hub, application services orchestrating harness + persistence, syntax highlighting, filesystem browser. |
| `giskard-ui` | Dioxus frontend (compiled to WASM). Components, client-side state, WS client. |
| `giskard-proto` | Shared client↔server **wire vocabulary** (serde), used by both `giskard-server` and `giskard-ui` so the wire protocol is defined once. Owns `Wire*` mirror types for any payload that carries a `PathBuf` (§3.5) and re-exports the path-free `giskard-core` domain types. This is the **only** crate `giskard-ui` depends on. |

> Dioxus "fullstack" can colocate server and client in one crate, but splitting `giskard-ui`
> (client) from `giskard-server` (backend) with a shared `giskard-proto` crate keeps the
> harness/persistence layers free of any WASM-target constraints and makes the backend
> independently testable. The implementer may merge `giskard-ui` into a fullstack crate if
> Dioxus tooling makes the split awkward, provided `giskard-proto`, `giskard-core`,
> `giskard-harness*`, and `giskard-persist` remain separate crates.
>
> **`giskard-core` is authoritative and native-facing** (it holds `PathBuf` and `serde_json::Value`
> internally). The browser never consumes `giskard-core` directly; it consumes `giskard-proto`.
> `giskard-proto` re-exports the pure, path-free `giskard-core` types (ids, `ModelRef`, `TokenUsage`,
> `Mode`, `PermissionPreset`, `ApprovalDecision`, `Effort`, `TurnStatus`, `DiffHunk`/`DiffLine`,
> `HarnessError`) — these are trivial serde structs that compile to `wasm32` cleanly — and defines
> its own `Wire*` mirrors for the path-bearing streamed tree (§3.5). This keeps `giskard-core` clean
> and its persisted/internal path representation lossless, while the wire representation is UTF-8
> `String` and cross-platform-safe.

### 3.3 Runtime & key dependencies

- **Async runtime:** Tokio.
- **HTTP/WS server:** Axum (Dioxus fullstack integrates with Axum).
- **Frontend:** Dioxus (WASM target), built with the `dx` CLI. No JS toolchain.
- **Serialization:** `serde` + `serde_json`.
- **Syntax highlighting:** `syntect` (server-side; returns highlighted HTML). See §11.
- **Persistence:** flat JSON files (see §5); `tempfile`-style atomic rename for writes.
- **Password hashing:** `argon2` (session password verification, §12).
- **Session cookies:** signed cookies (e.g. `tower-cookies` + an HMAC key), or a signed
  bearer token; see §12.
- **Codex client:** prefer an existing crate over hand-rolling. Verified options on crates.io
  (versions checked against the pinned Codex CLI 0.142.5 at implementation time):
  - **`codex-codes`** (v0.146.4) — **recommended first choice.** Typed Rust SDK for the Codex
    CLI app-server JSON-RPC protocol, tested against Codex CLI 0.146.x.
    Provides `AsyncClient` (Tokio) with `start()` (process spawn), `thread_start`, `turn_start`
    (accepting `model`, `reasoning_effort`, `sandbox_policy` — mapping onto `TurnOverrides`
    + `ModelRef.reasoning_effort`, P1),
    `next_message()` (streaming `ServerMessage::Notification/Request`), `respond()` (approval
    decisions), and `shutdown()`. Feature flags: `async-client` (Tokio), `types` (WASM-compatible
    serde models only). Includes a **schema coverage scorecard** that validates typed structs
    against `codex app-server generate-json-schema` output — directly usable for the CI
    protocol-drift check (§14.4). Ships real JSONL test captures (useful as `ReplayHarness`
    fixtures, §14.2). Raw `JsonRpcMessage`/`ServerMessage` access preserved for unknown/drifted
    messages. Apache-2.0. Sibling `claude-codes` crate exists (bonus for a future second harness).
    Repository: github.com/meawoppl/rust-code-agent-sdks.
  - **`codex-app-server-sdk`** (v0.5.1) — Tokio SDK for the app-server JSON-RPC over
    stdio/JSONL, with typed v2 request methods, raw-JSON fallback, `spawn_stdio` process
    management, and `resume_thread`. Smaller version offset from the CLI and less recent; evaluate
    as a fallback if `codex-codes` proves insufficient. Repository: github.com/thehumanworks/codex-sdk-rs.
  - **`codex-app-server-protocol`** (v0.63.0) — protocol types only (no client), stale relative to
    Codex 0.142.x. Not recommended unless only types are needed and `codex-codes`' `types` feature
    is somehow unsuitable.

  **Decision: use `codex-codes` with the `async-client` feature.** Its API maps directly onto the
  `AgentHarness` trait; Giskard wraps `next_message()` into a `broadcast::Sender<AgentEvent>` for
  multi-subscriber support and maps `codex-codes` types to `giskard-core` types at the boundary.
  If a future Codex CLI version diverges beyond what `codex-codes` tracks, fall back to a minimal
  hand-rolled JSON-RPC client inside `giskard-harness-codex`. **Either way, all Codex/app-server
  types must be confined to `giskard-harness-codex`** (nothing Codex-specific leaks upward) and the
  raw-JSON/unknown-message fallback preserved so protocol drift degrades gracefully rather than
  panicking.

### 3.4 Data-flow summary

1. Browser authenticates (§12), opens one WebSocket to the server.
2. User selects/creates a project → server ensures a `codex app-server` process exists for
   that project (spawned lazily on first use, see §6.4).
3. User opens a thread and sends input → server issues `turn/start` to the harness.
4. Harness streams JSON-RPC notifications → `CodexHarness` maps them to `AgentEvent`s →
   application service updates in-memory + persisted thread state → WS hub fans the events
   out to the subscribed browser(s) for that thread.
5. Server-initiated approval requests flow the same way in reverse: harness → `AgentEvent`
   (approval requested) → WS → UI prompt → user decision → WS → harness response (§9).
6. On `turn/completed`, token usage is recorded in the ledger (§10) and persisted.

### 3.5 Core-vs-proto ownership at the WASM boundary (decision — resolves C1/C2)

The frontend (WASM) and the backend (native) both need to speak about `AgentEvent`s, `Item`s, diffs,
and approval requests. Two of the core types are hostile to a naïve shared-crate approach:

- `PathBuf` serializes losslessly on the native side but a non-UTF-8 path (legal on Linux) round-trips
  **lossily** through JSON and back, so a shared `PathBuf` on the wire is a latent cross-platform bug.
- `serde_json::Value` is fine in `wasm32` but is an untyped escape hatch.

**Decision.** `giskard-proto` is the single wire vocabulary and the **only** crate `giskard-ui` links:

1. **Path-free domain types stay in `giskard-core` and are re-exported by `giskard-proto`.** IDs,
   `ModelRef`/`Effort`, `TokenUsage`, `Mode`, `PermissionPreset`, `ApprovalDecision`, `TurnStatus`,
   `DiffHunk`/`DiffLine`, `FileChangeKind`, `HarnessError`. They contain no `PathBuf`, so there is no
   lossiness and no reason to duplicate them.
2. **Path-bearing streamed types are mirrored in `giskard-proto` as `Wire*` types with `String`
   paths.** Concretely: `WireAgentEvent`, `WireItem`, `WireItemPayload`, `WireFileDiff`,
   `WireApprovalRequest`, `WireApprovalKind`. `serde_json::Value` payloads (`ToolCall`) stay `Value`
   (wasm-safe).
3. **The server maps `core → wire` at the outbound edge** (the ordered `ThreadEvent` lane and
   bootstrap live/suffix projections), performing the lossy `PathBuf → String` conversion
   **once, server-side**, with
   `Path::to_string_lossy()`. Inbound client messages are already path-free (`SendInput` is text;
   `SavePlan` carries a `String` path validated server-side against the workspace root).

**C2 corollary.** `ApprovalDecision` — including `AcceptWithExecPolicyAmendment { amendment: Vec<String> }`
— is path-free and round-trips as JSON, so it is re-exported (case 1), not mirrored. It travels
client→server in `ClientMessage::ApprovalDecision` and server→client inside `WireApprovalRequest`.


---

## 4. The `AgentHarness` Abstraction

This is the keystone of the "harness-agnostic" requirement. Everything above this layer
(domain services, persistence, UI) speaks only in `giskard-core` types.

### 4.1 Design principles

- **Capabilities are negotiated, not assumed.** Different harnesses support different
  features. A harness advertises what it can do via `HarnessCapabilities`; the UI adapts
  (e.g. hides the live-approval prompt if the active harness cannot push approval requests).
- **The internal event model is a superset shaped by, but not identical to, Codex.** Codex's
  Thread/Turn/Item model is well designed and maps cleanly onto Giskard's model. A weaker
  harness (e.g. Claude Code's `stream-json`) maps onto a subset and reports reduced
  capabilities.
- **The transport is mockable.** `AgentHarness` is a trait; `ReplayHarness` implements it
  from a recorded transcript. No integration test ever spawns a real LLM call.

### 4.2 Capabilities

```rust
pub struct HarnessCapabilities {
    /// Server-initiated, per-action approval requests (accept/decline while a turn is live).
    pub live_approvals: bool,
    /// Distinct read-only (plan) vs read-write (build) sandbox modes switchable per turn.
    pub plan_build_modes: bool,
    /// Per-turn model override (change model between turns of one thread).
    pub per_turn_model: bool,
    /// Reasoning-effort control (medium/high/xhigh, model-dependent).
    pub reasoning_effort: bool,
    /// Structured, per-file diff stream (for the side-by-side viewer).
    pub structured_diffs: bool,
    /// Durable thread resume across process/app restarts.
    pub resumable_threads: bool,
    /// The harness can list its own model catalog (e.g. Codex's app-server `model/list`), used
    /// both to overlay metadata onto the configured list and, where the harness attributes its
    /// models to a provider, as a source of picker entries in its own right (§8.3).
    pub model_listing: bool,
    /// The harness can report the providers it is configured to route to (§8.2), supplying the
    /// endpoint and key location for discovery and the set of ids a config may name.
    pub provider_listing: bool,
    /// Token usage reported on turn completion.
    pub token_usage: bool,
    /// MCP server status can be listed through the harness.
    pub mcp_status: bool,
    /// MCP server config can be reloaded through the harness.
    pub mcp_reload: bool,
    /// MCP OAuth login can be started through the harness.
    pub mcp_oauth_login: bool,
    /// Manual context compaction can be requested for a thread.
    pub context_compaction: bool,
}
```

Codex advertises all Codex-backed capabilities as `true`, including `model_listing`: the adapter
maps the app-server `model/list` RPC and Giskard overlays that metadata — friendly display names and
each model's advertised reasoning efforts — onto its config/provider model list, by model id (§8.3).
Context window still comes from Giskard's config/provider metadata (`model/list` omits it), unless
the provider serves the harness catalog shape on discovery, which does carry one (§8.3). The
adapter attributes `model/list` entries to the provider Codex routes to, read from the same
`config/read` that supplies the provider table — without a provider the entries could only ever
enrich someone else's, leaving a stock Codex with an empty picker. An **absent** `model_provider`
means the `openai` built-in, Codex's own default; a **failed** `config/read` fails the listing
instead, since guessing would attribute every model to `openai` for a user routing elsewhere.
`provider_listing` is likewise `true`: the adapter reads Codex's `[model_providers]` table out of
`config/read` (§8.2). `client_version` comes from the `user_agent` in the initialize handshake — the
only place Codex states its own version — and identifies Giskard to a provider's `/models` endpoint
as the harness.
A future
Claude Code adapter would likely set `live_approvals`,
`structured_diffs`, `mcp_status`, and possibly `plan_build_modes` to `false` or a degraded form,
and the UI reacts accordingly (§13.5).

### 4.3 The trait

```rust
#[async_trait]
pub trait AgentHarness: Send + Sync {
    fn capabilities(&self) -> HarnessCapabilities;

    /// List models available through this harness/provider, if supported.
    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError>;

    /// List the providers this harness routes to, if it can introspect its own config (§8.2).
    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError>;

    /// List configured MCP servers and their visible tools/resources.
    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError>;

    /// Reload MCP server configuration.
    async fn reload_mcp_servers(&self) -> Result<(), HarnessError>;

    /// Start an OAuth login flow for one MCP server.
    async fn start_mcp_oauth_login(&self, name: &str) -> Result<McpOauthStart, HarnessError>;

    /// Open (or resume) a thread. `resume` carries a harness-native thread id if resuming.
    async fn open_thread(
        &self,
        opts: OpenThreadOptions,
    ) -> Result<ThreadHandle, HarnessError>;

    /// Start a turn: send user input, applying per-turn overrides (model, mode).
    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError>;

    /// Subscribe to the stream of neutral events for a thread.
    /// Implemented as a broadcast/mpsc receiver of `AgentEvent`.
    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream;

    /// Respond to a pending approval request (no-op error if unsupported).
    async fn respond_approval(
        &self,
        req: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError>;

    /// Respond to a pending non-approval server request.
    async fn respond_server_request(
        &self,
        req: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError>;

    /// Interrupt the active turn of a thread.
    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

    /// Ask the harness to compact the thread context.
    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

    /// Rename a durable thread in the underlying harness.
    async fn set_thread_name(
        &self,
        thread: &ThreadHandle,
        name: &str,
    ) -> Result<(), HarnessError>;

    /// Archive or unarchive a durable thread in the underlying harness.
    async fn set_thread_archived(
        &self,
        thread: &ThreadHandle,
        archived: bool,
    ) -> Result<(), HarnessError>;

    /// Delete a durable thread in the underlying harness.
    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

    /// Cleanly shut down the harness (terminate child process, flush).
    /// Takes `&self` (not `self: Arc<Self>`) so the trait stays object-safe and is
    /// callable through `Arc<dyn AgentHarness>`. Idempotent: implementations perform the
    /// actual teardown once (e.g. behind a `OnceCell`/atomic flag) and treat further calls
    /// as no-ops. The child process is also terminated on `Drop` as a safety net.
    async fn shutdown(&self) -> Result<(), HarnessError>;
}
```

> **Object-safety note.** Every method above is dyn-compatible: `&self` receivers, no
> generic method params, no `Self`-by-value. The whole application holds harnesses as
> `Arc<dyn AgentHarness>`, so this is a hard requirement, not a stylistic one. `#[async_trait]`
> is used to keep `async fn` in the trait object-safe.

`AgentEventStream` is an `impl Stream<Item = AgentEvent>` (or a typed wrapper around a
`tokio::sync::broadcast::Receiver`). Multiple subscribers per thread are supported (e.g. two
browser tabs).

### 4.4 The neutral event model (`AgentEvent`)

```rust
pub enum AgentEvent {
    ThreadOpened { thread: ThreadId, harness_thread_id: String },
    TurnStarted  { thread: ThreadId, turn: TurnId },
    ContextWindowUpdated {
        thread: ThreadId,
        turn: TurnId,
        model: ModelRef,
        context_window: u32,
    },

    ItemStarted   { thread: ThreadId, turn: TurnId, item: ItemStart },
    ItemDelta     { thread: ThreadId, turn: TurnId, item_id: ItemId, delta: ItemDelta },
    ItemCompleted { thread: ThreadId, turn: TurnId, item: Item },

    /// A structured file diff update (for the diff viewer).
    DiffUpdated { thread: ThreadId, turn: TurnId, diff: FileDiff },

    /// Server-initiated approval request.
    ApprovalRequested { thread: ThreadId, turn: TurnId, request: ApprovalRequest },

    /// Server-initiated non-approval request that needs a browser response.
    ServerRequestReceived { thread: ThreadId, turn: Option<TurnId>, request: ServerRequest },

    /// A pending server request was answered or otherwise resolved.
    ServerRequestResolved { thread: ThreadId, turn: Option<TurnId>, request_id: ServerRequestId },

    TurnCompleted { thread: ThreadId, turn: TurnId, usage: TokenUsage, status: TurnStatus },

    Error { thread: ThreadId, turn: Option<TurnId>, error: HarnessError },
}
```

`ItemStart`/`Item` cover: user message, agent message (with streaming text deltas), reasoning
note, command execution (with output deltas), file change, and MCP/tool calls. `ItemDelta` carries
incremental text or command output, keyed by the Giskard-owned `ItemId`.

Every adapter translates its harness-native identifiers into Giskard-owned ids. For one logical
item lifecycle, `ItemStarted`, every `ItemDelta`, and `ItemCompleted` MUST carry the same `ItemId`.
Distinct logical items MUST NOT alias even when the harness reuses a native identifier across turns,
threads, sessions, or resumes. Native identifiers are opaque protocol details: each adapter defines
and documents the native key scope needed to satisfy these invariants.

Within one turn, the server treats `(TurnId, ItemId)` as the authoritative finalized-item key.
Receiving another `ItemCompleted` for that key replaces the previously buffered value rather than
appending a duplicate, including when `harness_item_id` is empty. A non-empty native item ID is a
secondary consistency key: it may detect an adapter identity violation, but it MUST NOT re-key one
Giskard item onto another.

> **Note (B5):** the `ItemStarted` above is an `AgentEvent` **variant**; the payload struct it carries
> is named `ItemStart` (§4.5), not `ItemStarted`, to avoid the name collision.

### 4.5 Supporting types (normative sketches)

These types are referenced by the trait (§4.3) and event model (§4.4). The shapes below are
**normative sketches**: field names and variants are the contract; the implementer may add
fields but must not rename or drop the ones shown, so that persistence (§5), the wire protocol
(`giskard-proto`), and the UI agree. All live in `giskard-core`.

`ProjectId`, `ThreadId`, `TurnId`, and `ItemId` are Giskard-owned identities. Each is minted once
for one logical entity, remains stable across persistence and replay, and must not alias another
entity even when a harness reuses or changes its native identifier. Harness-native identifiers are
stored separately and are never substituted for these owned IDs. `ApprovalId` and
`ServerRequestId` are short-lived routing identities for pending browser actions rather than
durable transcript identities.

```rust
// ---- IDs (ULID-backed newtypes) ----
pub struct ProjectId(pub Ulid);
pub struct ThreadId(pub Ulid);
pub struct TurnId(pub Ulid);
pub struct ItemId(pub Ulid);         // Giskard-owned item id (B2); the harness-native id
                                     // lives in `harness_item_id` on Item/ItemStart
pub struct ApprovalId(pub String);   // harness-native request id (opaque; short-lived, not persisted)

// ---- Handles / options ----
pub struct ThreadHandle {
    pub thread: ThreadId,
    pub harness_thread_id: String,    // native id used for resume
    pub warning: Option<HarnessNotice>, // non-fatal attach/open warning to surface to the user
    pub resumed_model: Option<ModelRef>, // effective model reported by the native open/resume
}

pub struct HarnessNotice {
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
}

pub struct OpenThreadOptions {
    pub project: ProjectId,
    pub thread: Option<ThreadId>,     // Some(existing id) ⇒ resume/attach to persisted thread
    pub workspace_root: PathBuf,      // effective sandbox root (§6.3)
    pub resume: Option<String>,       // Some(native id) ⇒ resume; None ⇒ fresh thread
    pub initial_model: ModelRef,
}

pub struct TurnStatus {              // outcome of a completed turn
    pub kind: TurnStatusKind,        // Completed | Interrupted | Failed
    pub message: Option<String>,
}
// S2: no `Declined` — the pinned Codex `TurnStatus` is Completed | Interrupted | Failed | InProgress
// (InProgress is not a terminal outcome and maps to no completed-turn kind). Re-add a variant here
// only when a real producer exists (and wire it in §7/§9).
pub enum TurnStatusKind { Completed, Interrupted, Failed }

// ---- Turn (B1) ----
/// One unit of agent work initiated by a single user input. Persisted inside the thread file
/// (§5.3) as an element of `Thread.turns`, and the unit the diff viewer / token gauge read from.
pub struct Turn {
    pub id: TurnId,
    pub user_input: UserInput,
    pub items: Vec<Item>,             // completed items, in order
    pub model: ModelRef,              // model used for this turn (may differ across turns, §8.4)
    pub mode: Mode,                   // plan | build applied to this turn (§7.4)
    pub status: TurnStatus,
    pub usage: TokenUsage,            // per-turn usage (same struct reused in ledgers, B3)
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,   // None while the turn is still live
}

// ---- Items ----
pub struct ItemStart {                // B5: renamed from `ItemStarted` (collides with the event variant)
    pub id: ItemId,                   // Giskard-owned (B2)
    pub harness_item_id: String,      // secondary native correlation id; never authoritative
    pub kind: ItemKind,               // discriminant; payload fills in on completion
    pub command: Option<CommandExecutionStart>, // present for command items when known
    pub tool: Option<ToolCallStart>,   // present for tool-call items when known
}

pub struct CommandExecutionStart {
    pub command: String,
    pub cwd: String,                  // wire-safe display path
    pub status: Option<String>,       // e.g. in_progress
    pub process_id: Option<String>,   // enables terminate when present
    pub started_at_ms: Option<i64>,   // Unix epoch ms when supplied by the harness
}

pub struct ToolCallStart {
    pub name: String,
    pub input: serde_json::Value,
    pub server: Option<String>,
    pub status: Option<String>,       // e.g. in_progress
    pub started_at_ms: Option<i64>,   // Unix epoch ms when supplied by the harness
}

pub enum TaskKind { Command, Tool }   // TK1: a running task is a shell command or a tool/MCP call

pub struct RunningTask {              // TK1: formerly `RunningCommand`; generalized over commands + tools
    pub kind: TaskKind,
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub item_id: ItemId,              // transcript row to select/scroll to
    pub harness_item_id: String,
    pub command: String,              // command line (command) or tool name (tool)
    pub cwd: String,                  // wire-safe display path (empty for tools)
    pub server: Option<String>,       // MCP/tool server name when this is a tool call
    pub status: String,               // in_progress / running-like while present
    pub process_id: Option<String>,   // present for commands; None for tools (stop → turn interrupt)
    pub started_at_ms: i64,           // server-observed fallback when harness omits it
    pub output: String,               // bounded output tail for the task menu
    pub after_turn: bool,             // true when the turn ended but the command is still known
    pub terminating: bool,             // true while waiting for a terminal event after terminate
}

pub enum ItemKind {
    UserMessage,
    AgentMessage,
    Reasoning,
    CommandExecution,
    FileChange,
    ToolCall,                          // MCP/other tool invocations
    Activity,                          // non-chat Codex activity surfaced in the transcript
}

/// The finalized item persisted in thread history and sent on `ItemCompleted`.
pub struct Item {
    pub id: ItemId,                   // Giskard-owned (B2): stable across resume, addressable by
                                      // the diff viewer and linked by the code overlay
    pub harness_item_id: String,      // native id (opaque; not relied on for stability)
    pub payload: ItemPayload,
    pub created_at: DateTime<Utc>,
}

pub enum ItemPayload {
    UserMessage    { text: String },
    AgentMessage   { text: String },
    Reasoning      { text: String },
    CommandExecution {
        command: String,
        cwd: PathBuf,
        output: String,               // accumulated stdout+stderr
        exit_code: Option<i32>,
        status: Option<String>,       // completed / failed / in_progress / declined
        process_id: Option<String>,   // retained for UI correlation / terminate affordance
        duration_ms: Option<i64>,     // completed command elapsed time when supplied
    },
    FileChange {
        path: PathBuf,                  // summary/back-compat path
        change: FileChangeKind,         // summary/back-compat change
        changes: Vec<FileChangeEntry>,  // optional per-file details
        status: Option<String>,
    },
    ToolCall {
        name: String,
        input: serde_json::Value,
        output: Option<serde_json::Value>,
        server: Option<String>,
        status: Option<String>,
        error: Option<String>,
    },
    Activity {
        title: String,
        detail: Option<String>,
        metadata: Option<serde_json::Value>,
    },
}
pub struct FileChangeEntry { path: PathBuf, change: FileChangeKind, diff: Option<String> }
pub enum FileChangeKind { Created, Modified, Deleted }

pub enum ItemDelta {
    Text { text: String },            // agent-message / reasoning increment
    CommandOutput { chunk: String },  // command stdout/stderr increment
}

// ---- Diffs (for the side-by-side viewer, §11.1) ----
pub struct FileDiff {
    pub path: PathBuf,
    pub change: FileChangeKind,
    pub old_text: Option<String>,     // None for created files
    pub new_text: Option<String>,     // None for deleted files
    pub hunks: Vec<DiffHunk>,         // precomputed for rendering; may be empty if full-text
    pub binary: bool,
}
pub struct DiffHunk {
    pub old_start: u32, pub old_lines: u32,
    pub new_start: u32, pub new_lines: u32,
    pub lines: Vec<DiffLine>,
}
pub enum DiffLine { Context(String), Added(String), Removed(String) }

// ---- Approvals (§9) ----
pub struct ApprovalRequest {
    pub id: ApprovalId,
    pub kind: ApprovalKind,
    pub reason: Option<String>,
    pub metadata: Vec<ApprovalMetadata>,      // structured host/path/detail rows for the card
    pub available: Vec<ApprovalDecision>,   // decisions the harness will accept
}
pub enum ApprovalKind {
    CommandExecution { command: String, cwd: PathBuf },
    FileChange       { path: PathBuf, change: FileChangeKind },
    Permission       { detail: String },    // network / extra-fs escalation
}
pub enum ApprovalMetadata {
    Text { label: String, value: String },
    Path { label: String, path: PathBuf, source_link: bool },
    Host {
        label: String,
        host: String,
        protocol: Option<String>,
        port: Option<i64>,
        target: Option<String>,
    },
}
pub enum ApprovalDecision {
    Accept,
    AcceptForSession,                        // see §9.2.1 for "session" definition
    Decline,
    Cancel,
    AcceptWithExecPolicyAmendment { amendment: Vec<String> }, // command exec only
}

// ---- Non-approval server requests (§9.2) ----
pub struct ServerRequest {
    pub id: ServerRequestId,
    pub method: String,
    pub params: serde_json::Value,            // original harness method params
    pub received_at: DateTime<Utc>,
}
pub enum ServerRequestResponse {
    Result { value: serde_json::Value },
    Error { code: i64, message: String },
}

// ---- Models & usage ----
pub struct ModelRef {
    pub provider: String,
    pub model: String,
    pub reasoning_effort: Option<Effort>,
}
// Reasoning efforts are model-defined (Codex's `ReasoningEffort` is a bare string), so this is a
// transparent string newtype, not a closed set: Giskard passes the value through to the harness and
// never branches on it. Common values are minimal | low | medium | high | xhigh; the effort selector
// is only shown when the chosen model advertises `supports_reasoning_effort` (§8.5).
pub struct Effort(pub String);

pub struct ModelDescriptor {
    pub provider: String,
    pub model: String,
    pub context_window: u32,                 // drives the context gauge (§10.3)
    pub supports_reasoning_effort: bool,     // drives effort-selector visibility (§8.5)
    pub reasoning_efforts: Vec<String>,      // exact effort levels the model advertises (§8.3, §8.5)
    pub display_name: Option<String>,
}

// B3: this ONE struct is reused everywhere usage is expressed — the per-turn usage on `Turn`/
// `TurnCompleted`, and the cumulative sums in the thread/project/global ledgers and their
// `by_model` breakdowns (§10.2). Do not introduce a parallel `TokenTotals` type.
pub struct TokenUsage { pub input: u64, pub output: u64, pub total: u64 }

// ---- User input ----
pub enum AttachmentKind { Image, File }

pub struct UserAttachment {
    pub name: String,
    pub mime_type: String,
    pub size: u64,
    pub kind: AttachmentKind,
    pub data_base64: String, // transient request payload; skipped when persisted
}

pub enum UserInput {
    Text {
        text: String,
        attachments: Vec<UserAttachment>,
    },
}

// ---- Errors ----
pub enum HarnessError {
    Spawn(String),            // failed to start/locate the harness binary
    NotInitialized,           // used before handshake completed
    Unauthenticated,          // harness reports missing/invalid credentials
    Transport(String),        // I/O / framing / connection error
    Protocol(String),         // unexpected/unparseable protocol message
    Overloaded,               // JSON-RPC -32001 after retries exhausted
    Unsupported(String),      // capability not offered by this harness
    ThreadNotFound(ThreadId),
    ThreadBusy { thread: ThreadId },
    Timeout(String),          // operation timed out (S3: renamed from `Timed`)
}
```

> `AgentEventStream` is `impl Stream<Item = AgentEvent> + Send` (concretely a wrapper over a
> `tokio::sync::broadcast::Receiver<AgentEvent>`), supporting multiple subscribers per thread.

### 4.6 Codex mapping (informative)

The `CodexHarness` maps the Codex app-server JSON-RPC protocol onto the above. Key mappings
(protocol details in [Appendix A](#appendix-a-codex-app-server-mapping-reference)):

| Codex app-server | Giskard |
|------------------|---------|
| `initialize` + `initialized` handshake | **once per process** (per project), during process spawn — not per thread (S1) |
| `thread/start`, `thread/resume` | `open_thread` (S1: this is the per-thread call, distinct from the handshake) |
| `turn/start` (with model/effort/permissions per turn) | `start_turn` + `TurnOverrides` (P1: effort lives in `ModelRef`, not `TurnOverrides`) |
| `item/started`, `item/*/delta`, `item/completed` | `ItemStarted` / `ItemDelta` / `ItemCompleted` |
| `turn/diff/updated` | `DiffUpdated` |
| `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval` | `ApprovalRequested` |
| `serverRequest/resolved` | `ServerRequestResolved` |
| `turn/completed` (token usage) | `TurnCompleted` |
| `turn/interrupt` | `interrupt` |
| JSON-RPC error `-32001` "overloaded" | retry with exponential backoff + jitter, surfaced as transient `Error` only if retries exhausted |

Plan vs build maps to Codex collaboration mode only: **Plan → `plan`**, **Build → `default`**.
The thread permission preset maps to Codex's built-in `permissions` profile and approval
configuration (§9).

### 4.7 Process lifecycle (Codex)

- **One `codex app-server` process per project.** The process hosts all of that project's
  threads (Codex threads are durable containers within a connection). This isolates projects
  from each other, matches Codex's model, and generalizes to future harnesses ("one working
  context = one harness instance"). See also §4.5 for the object-safety constraint this places
  on the trait.
- Transport: **stdio** (newline-delimited JSON-RPC), the stable/production transport. The
  WebSocket transport is not used in v1 (it is for remote, which is out of scope).
- **Lazy spawn:** the process starts on first interaction with the project, not at app boot.
- **Idle shutdown (optional, configurable):** a project's process may be terminated after a
  configurable idle timeout to reclaim memory; threads are resumed on next use via
  `thread/resume`. Default: keep alive while the app runs (given the ~10-thread scale).
- **Crash handling:** if the child exits unexpectedly, the server marks the project's active
  threads as "disconnected", surfaces an `Error` event to the UI, and offers a "reconnect"
  action that respawns and resumes.
- **Native identifier mapping.** The adapter translates native thread, turn, item, and request
  identifiers as required by the Giskard-owned identity and lifecycle rules in §4.4–§4.5. Native
  process handles remain separate control identifiers. The Codex-specific key scopes and routing
  behavior are documented in `crates/giskard-harness-codex/README.md`.
- **Resume-failure fallback (C5).** `thread/resume {threadId}` can fail even though Giskard has the
  stored native id — Codex's own thread store may have been purged or rotated. On a resume-by-id
  failure the harness MUST **not** hard-error the thread: it starts a **fresh** native thread
  (`thread/start`), re-binds the new native id to the existing `ThreadId` in the B4 map, preserves
  the Giskard-side display history (already on disk), and surfaces a non-fatal `Error`/warning event
  so the UI can tell the user "agent context was lost — continuing with a new session; your history
  is intact." This is Phase-1 behavior.
- **Version check:** on spawn, record the Codex CLI version. If it differs from the version
  the protocol mapping was written/tested against, log a warning surfaced in the UI (the
  app-server protocol is versioned and can drift). The implementer should generate and vendor
  the schema via `codex app-server generate-json-schema` for the pinned version and add a CI
  check.


---

## 5. Data Model & Persistence

### 5.1 Requirements recap

- After a backend restart, the app comes back in the **same state**: the list of projects,
  their threads, thread history, mode, selected model, and token ledgers.
- Storage is **flat files** (human-readable, hand-editable, debuggable with `cat`/`jq`),
  unless a technical constraint makes it untenable — in which case SQLite is the documented
  fallback (§5.6), but SQLite is *not* v1's default.
- State corruption must be avoidable: **atomic writes** (write to temp file, `fsync`, rename)
  and a single-writer discipline per file.

### 5.2 On-disk layout

Root directory (XDG): `${XDG_DATA_HOME:-~/.local/share}/giskard/`. Overridable via
`GISKARD_DATA_DIR`.

```
giskard/
├── config.toml                 # global app config (§16.3) — human-authored + app-updated
├── .giskard.lock               # advisory data-directory lock: one Giskard process at a time (§5.4)
├── projects.json               # index of projects (id, name, dir, created_at, order)
├── projects/
│   └── <project_id>/
│       ├── project.json        # ProjectConfig: workspace root, harness kind
│       ├── threads/
│       │   └── <thread_id>/            # a thread is a directory (§5.4)
│       │       ├── thread.json         # thread metadata, permission preset, token cache — no history
│       │       ├── history.jsonl       # bounded turn index: header line, then one record per turn
│       │       ├── turns/
│       │       │   └── <turn_id>.jsonl # atomic initial payload; appended amendments (§5.4)
│       │       └── legacy/             # pre-migration originals, retained (§5.4)
│       ├── worktrees/          # only for threads started isolated (§7.1)
│       │   └── <thread_id>/            # the thread's linked Git worktree — a checkout, not Giskard state
│       └── tokens.json         # per-project token ledger (aggregates + daily buckets)
└── tokens-global.json          # global token ledger (daily/weekly/monthly/total)
```

- **IDs** are ULIDs (sortable, timestamp-prefixed) rendered as strings. Filenames use the ID.
- **`projects.json`** is the small, frequently-read index. Individual project/thread files
  hold the bulk, so no single giant file must be parsed to render the project list.
- **`order` field (S5):** project ordering defaults to **ULID creation order** (ULIDs already sort
  by creation time). The `order` field is **reserved** for a future explicit/drag reorder and is not
  yet surfaced by the UI; until then it is written as the creation index and the list is sorted by
  id. Keep the field (cheap to persist now) rather than migrating the schema later.

### 5.3 Core persisted types (serde JSON)

All defined in `giskard-core`, serialized by `giskard-persist`. Illustrative shapes:

```jsonc
// projects.json
{
  "version": 1,
  "projects": [
    { "id": "01J…", "name": "ostinato-radio", "dir": "/home/user/dev/ostinato-radio",
      "created_at": "2026-07-06T10:00:00Z", "order": 0 }
  ]
}
```

```jsonc
// projects/<id>/project.json
{
  "version": 1,
  "id": "01J…",
  "name": "ostinato-radio",
  "dir": "/home/user/dev/ostinato-radio",
  "harness": "codex",
  "workspace_root": null,               // null ⇒ defaults to `dir`
  // no default model: a new thread's starting model is derived from the project's catalog (§8.3).
  // A file written before that change still carries `default_model`; it is ignored on load and
  // dropped the next time the file is written, because `deny_unknown_fields` would otherwise
  // make every pre-existing project unloadable.
  "created_at": "…", "updated_at": "…"
}
```

```jsonc
// projects/<id>/threads/<thread_id>/thread.json
{
  "version": 1,
  "id": "01J…",
  "project_id": "01J…",
  "revision": 42,                       // durable per-thread metadata ordering clock
  "title": "Fix Qobuz OAuth refresh",
  "harness_thread_id": "th_abc123",     // native id used for resume
  "mode": "build",                       // "plan" | "build"
  "current_model": { "provider": "openai", "model": "gpt-5.5", "reasoning_effort": "high" },
  "context_window": 258400,              // CACHE ONLY (C4): effective window for current_model;
                                         //   starts from descriptor metadata and is replaced by a
                                         //   harness-reported runtime value when available.
  "model_context_windows": {             // C8: harness-reported effective windows retained by
    "openai": { "gpt-5.5": 258400 }      //   exact provider/model for reloads and model switches.
  },
  "permission_preset": "ask_first",        // permission preset (§9)
  "archived": false,                     // hidden from the active thread group when true
  "model_efforts": {                     // C7: per-model effort retention. Maps "provider/model"
    "openai/gpt-5.5": "high"             //   → stored Effort, so switching back to a reasoning model
  },                                     //   restores the user's last effort choice. Entries are
                                         //   created/updated on SelectModel when the outgoing model
                                         //   supports reasoning_effort.
  "tokens": {
    "total": { "input": 12000, "output": 3400, "total": 15400 },
    "by_model": {                        // nested object (C3): provider → model → usage.
      "openai": {                        //   NOT an interpolated "provider/model" string key, which
        "gpt-5.5": { "input": 12000, "output": 3400, "total": 15400 }   // is ambiguous when the
      }                                  //   model id contains slashes (e.g. "@cf/z-ai/glm-4.7").
    }
  },
  "git_workspace": {                     // absent unless the thread was started isolated (§7.1).
    "strategy": "worktree",              // which `git_strategy` produced it; the tag is what lets a
                                         //   later strategy be a new variant rather than a migration
    "path": "/home/user/.giskard/projects/01J…/worktrees/01K…",   // the checkout Git manages
    "workspace": "…/worktrees/01K…/packages/api",  // omitted unless the project is a repository
                                         //   subdirectory; then this is where the thread works
    "branch": "giskard/worktree-01k9x2m4qpz8v",   // the branch Giskard created — only the starting
    "base_commit": "e17b742…",           //   point; where the thread went afterwards is not tracked
    "repo_root": "/home/user/dev/ostinato-radio",     // the project checkout it was branched from
    "common_dir": "/home/user/dev/ostinato-radio/.git",              // resolved, never constructed:
    "git_dir": "/home/user/dev/ostinato-radio/.git/worktrees/01K…"   //   the project may itself be
  },                                     //   a linked worktree, whose `.git` is a pointer file
  "created_at": "…", "updated_at": "…"
  // NB: no `turns[]` — history is the authoritative `<thread_id>/history.jsonl` index plus one
  //     `turns/<turn_id>.jsonl` payload file per turn (H1, §5.4).
}
```

> The thread `tokens` object carries both the aggregate (`total`) **and** the per-model
> breakdown (`by_model`), matching §10.2. A thread accumulates a distinct `by_model[provider][model]`
> entry whenever its model changes mid-thread (§8.4). `context_window` is a cache (C4): catalog or
> config metadata supplies its initial value, while `model_context_windows` retains authoritative
> effective values reported by the harness for exact provider/model pairs.

```jsonc
// projects/<id>/tokens.json  and  tokens-global.json
{
  "version": 1,
  "total": { "input": 0, "output": 0, "total": 0 },
  "by_day":   { "2026-07-06": { "input": …, "output": …, "total": … } },
  "by_model": { "openai": { "gpt-5.5": { "input": …, "output": …, "total": … } } }  // nested (C3)
}
```
Weekly/monthly aggregates are **derived on read** from `by_day` (no separate storage), so
there is one source of truth to correct if needed.

### 5.4 Write strategy & durability

- **In-memory authoritative state** per running server; disk is the durable mirror.
- Every mutating operation updates memory, then persists the affected file(s) via
  **atomic replace**: write to `<file>.tmp-<rand>`, `fsync`, `rename` over the target.
- **Per-file async mutex** (or an actor owning each file) guarantees single-writer; the
  ~10-thread scale makes contention negligible. These are **in-process** locks and order nothing
  between binaries.
- **Advisory data-directory lock (`<data_dir>/.giskard.lock`).** `giskard-admin` is a separate
  binary from `giskard-server`, so the per-file mutexes above provide no exclusion between them: a
  `prune-legacy` or `migrate-storage` run against a live server's data directory, or a second
  server started on the same directory, had no protection at all. One `flock` per data directory
  supplies it. `giskard-server` takes it exclusively at startup, holds it for the process lifetime,
  and refuses to start if another process holds it. Every `giskard-admin` command that rewrites or
  deletes takes it for the command and exits non-zero rather than proceeding. `--dry-run` and
  read-only inspection take no lock and warn instead, so they remain usable against a live server —
  which is what makes "stop the server, then run this" advice actionable rather than circular.
  `try_lock`, never `lock`: an operator who ran a command by mistake wants an error, not a process
  that appears to hang. The file is created if absent and never deleted, including on clean
  shutdown — unlinking races the next holder and buys nothing — and the kernel releases the lock
  when the holding process dies, so a crash leaves nothing stale and no pidfile liveness check is
  needed. The lock is **advisory**: it constrains Giskard's own binaries, not other tools; it is
  **not reliable over NFS** (acceptable under §1.2 local-first, but not a guarantee); and its scope
  is the data directory, so two instances on two directories never contend.
- **`tokens-global.json` is a cross-project hot file** (every `TurnCompleted` in any project
  updates it). It is owned by a **single dedicated ledger actor** (one Tokio task holding the
  in-memory global ledger); all projects send `TokenUsage` deltas to it over an `mpsc`
  channel, and it serializes the atomic writes. This avoids multi-writer races on the one
  shared file without a global lock, and it batches rapid updates (coalesce N deltas arriving
  close together into one atomic write). The same actor owns per-project `tokens.json` writes,
  or those may be delegated to per-project sub-tasks — either is acceptable, but the global
  file must have exactly one writer.
- **Turn history (authoritative, H1/H2/H3):** a thread's history is the **source of truth**, split
  across two files by how the two halves grow. `<thread_id>/history.jsonl` is the **index**: a
  header line, then one strictly bounded record per turn (turn id, model, mode, status kind, usage,
  timestamps, item count, a capped prompt preview, a capped status message, attachment
  descriptors). It is never agent-driven — a turn's *status kind* is a bounded enum, but its
  *status message* is composed from provider error text with no ceiling, so only a capped rendering
  of it reaches the index and the payload file holds what the harness reported. `<thread_id>/turns/<turn_id>.jsonl` is the **payload**: the full `UserInput`, the
  items and the diffs — everything whose size is a function of what the agent did.
  On `TurnCompleted` the server writes the initial payload file **first**, with temp file + `fsync` +
  rename, and appends the bounded index record **last**. A crash between the two leaves a payload
  file no turn record references; it is invisible to every read path, because reads start from the
  index, so the worst case is a wasted file. (This is what closes the corruption path a whole-turn
  append had: a torn line that a later append concatenates onto stops being the *final* line, and
  the entire thread's history becomes unreadable — likeliest on the threads with the largest command
  output.) The index record is appended via a single `write()` (JSON + `\n`) to an `O_APPEND` file:
  on a local POSIX filesystem this is atomic against concurrent writers without an application lock,
  and a process kill leaves the line all-or-nothing. It does **not** survive power loss (page
  cache), so the loader tolerates a single unparseable **final** line (torn append) — skipping it —
  while a bad interior line is real corruption; at turn-record size that tolerance is adequate. This
  atomicity holds on local storage only; NFS/network `GISKARD_DATA_DIR` is out of scope (§1.2).
  After appending, the server updates `thread.json` (token aggregates, `updated_at`) — history-first,
  so a crash between the two leaves the turn recoverable and the aggregates rebuildable from the
  index alone (`recompute_aggregates` reads no payload file, treating aggregates as a cache like
  `context_window`, C4). A per-thread persistence lock spans the append and metadata fold and also
  spans repair's history read and metadata replacement. The history write remains first, but repair
  cannot race a live completion and overwrite or double-count its usage. Repair also restores the
  latest durable turn timestamp as recency; it does not use the repair time and make an old thread
  look newly active. The metadata `thread.json` never holds `turns[]`. The server may cache the
  parsed history in memory for the process lifetime, but the cache is never authoritative: reads
  validate the index file's metadata before reuse, disk append succeeds before in-memory append, and
  delete/repair paths invalidate stale entries. It currently caches whole reassembled turns, so
  resident memory per open thread stays unbounded as before: the bounded index makes a
  bounded-residency cache *possible*, but claiming it means caching only the parsed index, which
  waits on the bounded read APIs. Transient attachment bytes are redacted before a
  turn is added to this cache.
- **Late payload amendments.** A background command which finishes after its turn committed appends
  one ordinary `item` record under the same per-thread persistence lock. Completed `write_all` is
  the process-level commit and advances the amendment sequence before publication. `sync_data` is
  attempted afterward as best-effort crash hardening; failure is warned but does not fail the
  amendment.
  Only newline-terminated records are committed. Readers warn and skip malformed JSON, malformed
  known record shapes, and non-UTF-8 records independently; the required `user_input` record must
  still survive or that turn fails.
  An unterminated byte tail, including one cut inside a UTF-8 character, is warned about, ignored,
  and preserved. The next append adds a newline separator before its complete item record. A valid
  JSON value missing only its delimiter is thereby recovered; a genuinely partial or malformed
  value remains invalid and is skipped. No old bytes are truncated or rewritten. Item records fold
  last-wins by `ItemId` at their original display position. The bounded index, usage aggregates,
  and recency do not change.
- **Format identification.** Three independent version numbers, each describing exactly what it
  governs: `thread.json` → `version` (the metadata record schema), the `history.jsonl` header →
  `format` (the directory layout and index schema, written once — at thread creation, or by the
  first append for a thread the store never saw created — and never rewritten), and each
  `turns/<id>.jsonl` header → `format` (that turn's payload schema, written once when the turn
  commits). The layout version lives in the history header rather than in `thread.json` because
  `thread.json` is rewritten on every metadata mutation, and because a file that carries its own
  format claim has no cross-file consistency for a crash to break. Payload headers are per-file so a
  directory holding a mix of old and new payload files is legal, which is what makes lazy per-turn
  migration possible. Unknown behavior is fixed: an unknown `kind` within a known format is **skipped
  with a warning** (a future downgrade is non-destructive); a payload `format` newer than understood
  **fails that turn only**; a history `format` newer than understood **fails the thread**, since
  without the index there is nothing to partially recover.
- **Deletion.** A thread directory is renamed to `<thread_id>.deleting/` before it is removed. The
  rename is atomic and immediately drops the thread out of enumeration, after which the recursive
  removal can fail and be retried harmlessly — preserving the property that a partial delete leaves
  the thread visible rather than deleting the catalog record while history survives as an invisible
  orphan. Enumeration matches a `<ulid>/` directory or a `<ulid>.json` file, deduped by id;
  `<ulid>.migrating/`, `<ulid>.deleting/` and `*.corrupt-*` never parse as a bare ULID and so are
  never enumerated.
- **Migration (format 1 → format 2).** Threads written as a flat `<thread_id>.json` beside a
  `<thread_id>.jsonl` are migrated per thread, on open, idempotently, under the per-thread lock —
  self-hosted users do not run a migration before starting the binary. The rebuild is staged in
  `<id>.migrating/`, verified against the source (turn ids, order, count) before committing, and
  committed with one rename; the originals are then *relocated* to `<id>/legacy/`, never deleted.
  A crash before the rename leaves a staging directory the next run discards and rebuilds; a crash
  between the rename and the relocation leaves both shapes on disk, and readers prefer the
  directory while a later pass finishes the move. A format 1 history that cannot be parsed aborts
  its migration and leaves that thread on format 1 rather than rebuilding it minus the lost turns.
- **Crash consistency:** replacement JSON writes and initial turn-payload commits are atomic
  renames, so a crash leaves either the old or the new complete file. Append-only JSONL authorities
  tolerate an unterminated tail as specified above. On startup the server validates each JSON file;
  corrupt atomic JSON documents are moved aside to `<file>.corrupt-<ts>` and logged. Turn-payload
  record damage instead stays in place and is warned/skipped at record granularity; a missing
  required record fails only that turn. The app continues with the rest in either case.

### 5.5 Debug / maintenance surface

Because the store is plain files, the primary debugging tool is the filesystem itself
(inspect with `jq`, delete a thread by removing its file). In addition, `giskard-persist`
exposes a small maintenance API used by a `giskard-admin` binary (or hidden UI panel):

- `list_projects`, `list_threads(project)` (including active/archived status),
  `dump_thread(id)` (pretty JSON to stdout),
- `delete_thread(id)`, `delete_project(id)` (with confirmation),
- `validate_all` (parse every file, report corruption — per turn for an unreadable payload),
- `compact_thread(id)` (rewrite/prune the jsonl log),
- `migrate-storage` (bulk format 1 → format 2, with `--dry-run`),
- `prune-legacy` (delete the retained pre-migration originals — the one command here that can
  destroy transcript history, hence separate and explicit),
- `sweep-orphan-payloads` (delete payload files no turn record references). Deliberately not
  automatic, and gated on the data-directory lock: with the directory locked there is no in-flight
  commit an unreferenced payload could belong to, which is what lets this be actual exclusion rather
  than the wall-clock guess about another process's progress it replaced. It also refuses any thread
  whose index is missing, references no turns,
  or would have more than a handful of payloads swept at once: the index is the less durable file,
  and losing a *tail* of its page-cached appends leaves exactly that many fsynced payloads
  unreferenced, so a large sweep set is evidence of a truncated index rather than of orphaned
  commits. A refusal is a judgment about the thread, not a failure of the sweep, so it is reported
  alongside the files it names rather than raised in place of them: the refused thread is skipped
  and never a reason to abandon the run, and a dry run still reports the refusal *and* lists what it
  is about — which is what makes "inspect them first" advice a thing an operator can act on.

`migrate-storage --dry-run` previews through the migration's own classifier rather than a
reimplementation of it, so the plan names every case the run acts on (including a thread caught
between the commit rename and the legacy relocation). The one thing a plan cannot know is whether
the work will *succeed*: a format 1 history that will not parse plans as a migration and reports as
an error only when attempted.

This satisfies the "complete tool to debug and potentially correct the database" requirement
without a SQL console.

### 5.6 SQLite fallback (documented evolution, not v1)

If flat files prove painful (e.g. thread history JSON grows large enough that per-turn
rewrites cause latency, or concurrent aggregation becomes error-prone), migrate to SQLite via
`sqlx` or `rusqlite`. The domain types in `giskard-core` are storage-agnostic and
`giskard-persist` is the only crate that would change. Provide a one-shot importer that reads
the flat-file tree into the SQLite schema, and ship a debug view (e.g. a bundled
`giskard-admin db …` subcommand) so the "inspect and correct the DB" requirement is preserved.
This section exists so the migration path is pre-approved; do **not** implement it in v1.

---

## 6. Project Management

### 6.1 Project creation

Flow: user clicks "New project" → names it → picks a directory via the file browser (§6.2)
→ optionally sets workspace root → confirm. No model is chosen here: the project has no harness
yet, so there is no catalog to choose from (§8.3).

- The chosen directory may be **empty or existing, git or non-git** — all valid. No git
  requirement, no scaffolding.
- On confirm: create `projects/<id>/`, write `project.json`, add to `projects.json`. The
  harness process is **not** spawned yet (lazy, §6.4).

### 6.2 Filesystem browser / picker

- Backend-driven: the picker browses **the server machine's filesystem** (not the browser
  host). Endpoint returns directory entries (name, is_dir, size, mtime) for a given path.
- **Access scope** is governed by config key `browse.roots` (§16.3):
  - **unset / empty ⇒ full filesystem** is browsable (default).
  - if set to a list of absolute paths ⇒ navigation is **confined** to those subtrees.
- **Security hardening (mandatory even though single-user):** the server canonicalizes every
  requested path (resolve `.`/`..`/symlinks) and, when `browse.roots` is set, rejects any
  path escaping the allowed roots. Never trust a client-supplied path verbatim. Hidden files
  are listed but visually de-emphasized; the picker can filter to directories only when
  choosing a project dir.

### 6.3 Workspace root

- `workspace_root` in `project.json`: **`null` ⇒ equals the project `dir`** (the common
  case). May be set to a subdirectory (narrow the agent's write scope) or a different/wider
  path. This value becomes the harness sandbox boundary passed to Codex.
- The UI shows the effective workspace root and warns if it differs from the project dir.

### 6.4 Harness process management (per project)

- One `codex app-server` per project, spawned lazily (§4.6), reused across the project's
  threads, resumed after idle shutdown or crash.
- The server keeps a registry: `project_id → HarnessInstance` (holding the `Arc<dyn AgentHarness>`,
  child process handle, and per-thread subscriber bookkeeping).
- Deleting a project: shut down its harness, then remove `projects/<id>/` and its
  `projects.json` entry (with a confirm dialog; irreversible).

### 6.5 Multiple projects & threads in parallel

- Projects are independent; their harness processes run concurrently.
- Within a project, multiple threads can be active concurrently (the app-server supports
  concurrent turns across threads). The UI lets the user switch among open threads without
  interrupting their in-flight work; background threads keep streaming and their state keeps
  updating server-side (and is pushed over the shared WebSocket, §13.6).


---

## 7. Threads & Turns

### 7.1 Thread lifecycle

- **Draft new thread:** user starts a new thread in a project; the browser opens an unpersisted
  draft immediately, with mode and permission preset defaulted synchronously and the composer
  editable. The model is resolved asynchronously (LT6/LT7): until it lands the draft has no model
  and the first send is unavailable, so the turn can never start on a stand-in. There is no local
  `<thread_id>.json` and no native Codex thread yet.
- **Create + first send:** user submits the first message; the browser calls
  `POST /api/projects/{id}/threads/start` with text, model/provider, mode, and permission preset.
  The server calls `open_thread` (Codex `thread/start`) with that provider/model, stores the
  returned `harness_thread_id`, writes `<thread_id>.json`, and immediately calls `start_turn`.
  If native creation fails, nothing is persisted. If persistence or synchronous `turn/start` fails
  after native creation, cleanup is best-effort and failures are logged.
  This first turn begins before the browser subscribes to the new thread, so the composer opens
  locked on a turn it has observed nothing of. It must not stay locked on the strength of that
  assumption alone: the committed bootstrap's `final_runtime.turn_state` (§13.6) is what settles
  whether the turn is still running, and a turn that finished during the gap releases the composer
  with no re-open.
- **Isolation in a Git worktree:** the start request carries `git_strategy`, settable only from a
  draft because a thread's workspace is fixed once it exists. It is an enum — `shared` (the
  project's own checkout, the default and the only possibility for a non-Git workspace) and
  `worktree` — rather than a flag, because where a thread's working tree comes from is an open
  question and a boolean could not carry a third answer. An unknown value is **rejected**: a client
  asking for a strategy the server does not implement must not be quietly started in the shared
  checkout, which looks like it worked. On `worktree`, the server creates a linked
  worktree at `projects/<project_id>/worktrees/<thread_id>` on branch
  `giskard/worktree-<first 13 chars of the lowercased thread ULID>` *before* opening the harness —
  the worktree is the cwd the harness is given — and records it as `ThreadFile.git_workspace`,
  tagged with the strategy that produced it. Creation
  failure fails the start rather than falling back to the project's checkout, and every later failure
  in that handler rolls the worktree and its branch back. The thread's workspace is that worktree
  everywhere it matters: harness cwd, the file endpoints and the Git status endpoints (§13.5).
  `ThreadWorktree.path` is the checkout Git manages and `workspace` is where the thread works; they
  differ only when the project directory is a *subdirectory* of its repository, since Git can check
  out only a whole repository — the worktree is then the repository root and the workspace is the
  same subdirectory beneath it, so a path names the same file with isolation as without. A project
  directory absent from the repository's committed content has no counterpart in a fresh checkout,
  and isolation fails rather than inventing one.
  Isolation decides *where* a thread works and nothing else — the permission preset still decides
  what it may do there, and an isolated thread's sandbox is exactly an ordinary thread's, so Git
  commands that write the repository escalate for approval under Auto Approve just as they do
  without a worktree. Sub-agent threads never get a worktree record of their own: a child spawned
  during an isolated thread's turn inherits its parent's cwd from the harness, and Giskard resolves
  the same worktree for it by reading the parent chain — for the harness cwd on open, resume and
  reattach, and for the Git status endpoints. The chain is read, never copied down it, so the
  worktree stays owned by the thread that created it and deleting a sub-agent cannot remove it.
  Full behaviour, including the shared-repository boundary, is documented in
  `docs/git-worktrees.md`.
- **Open existing:** selecting a persisted thread calls the same open endpoint with
  `thread_id = Some(existing_id)`. The server reattaches the harness using the stored native
  `harness_thread_id` but preserves the durable Giskard `ThreadId`; opening a thread is
  idempotent if it is already attached and its model/provider state is still current.
- **Send input:** user submits a message; server builds a `TurnOverrides` snapshot from the
  thread's persisted state (mode, current model — which carries effort) and calls `start_turn`.
  A turn begins.
- **Stream:** `AgentEvent`s flow to the UI (§13.6) and update persisted state.
- **Complete:** on `TurnCompleted`, token usage is folded into the ledgers (§10) and the
  thread file is rewritten atomically.
- **Resume (after restart):** on startup or first access, `open_thread` with the stored
  `harness_thread_id` (Codex `thread/resume`) rehydrates the native session; Giskard already
  holds the display history from disk. If resume-by-id fails (Codex store purged/rotated), the
  harness falls back to a fresh native thread and warns that agent context was lost, keeping the
  Giskard history intact (C5, §4.7). The initial gauge uses the latest persisted runtime window for
  the selected model, then provider/config metadata, then the conservative fallback. A later turn
  replaces that value when the harness reports its effective window.
- **Interrupt:** user can interrupt an in-flight turn (`turn/interrupt`). The UI exposes this as a
  live-turn Stop control; sending another user message while a turn is still live is a separate
  queueing policy and is not implied by interrupt support.
- **Archive / unarchive:** the thread list exposes an actions menu (`...`) per thread. Archive calls
  the harness lifecycle operation first (Codex `thread/archive`) and marks the local thread
  metadata `archived = true` only after success. Unarchive is the reverse operation (Codex
  `thread/unarchive`, then `archived = false`). Archived threads are listed separately and do not
  restore as the active thread after reload. Archiving leaves a thread's worktree on disk; reclaiming
  it is deferred to a later phase.
- **Delete:** delete calls the harness lifecycle operation first (Codex `thread/delete`) and removes
  the local `<thread_id>/` directory only after success (§5.4: renamed out of enumeration first,
  then removed). Delete also drops the in-memory
  Giskard harness handle. Archive/delete are rejected while the thread has an active turn or running
  command; the browser surfaces the failure as an error notice. Delete also takes the thread's
  worktree and the branch Giskard created with it — in that order, since Git refuses to delete a
  checked-out branch — leaving branches the agent made during the thread alone. It is refused with
  `409` when the worktree of the thread or of any sub-agent beneath it holds uncommitted changes or
  commits reachable from no other ref, unless `?force=true`; `GET …/deletion-impact` reports the same
  facts so the confirmation can name them first. Deleting a *project* sweeps its worktrees
  unconditionally, because a single thread must not be able to leave a project half-deleted.
- **Rename:** the thread list actions menu exposes `Rename`. It edits the row title next to the
  `...` menu. Saving calls the harness first (Codex `thread/name/set`) and then persists
  `ThreadFile.title`; the browser updates both the row title and the open-thread header/mobile
  breadcrumb after success.

### 7.2 Titles

Auto-generate an initial title from the first user message (truncated); user-editable.
(Optional enhancement: ask the harness to summarize; not required for v1.)

### 7.3 Streaming semantics

- Agent message text arrives as `ItemDelta`s; the UI appends incrementally.
- Command executions stream stdout/stderr as `ItemDelta`s under a command item.
- Command output bodies are collapsible transcript sections. Running command output starts
  expanded while small and may auto-collapse once output is large; completed command output is
  collapsed by default regardless of size. Expanding a command renders the output inline.
- Tool-call input/output bodies follow the same collapse model as command output: running rows
  start expanded while small and may auto-collapse once large; completed tool-call input/output is
  collapsed by default, and expanding the row renders input/output inline. Tool-call status is
  rendered in the same meta position and with the same lifecycle wording as command status.
- Reasoning notes (if the model/effort emits them) render in a collapsible "thinking" block.
- Each item ends with `ItemCompleted` carrying its final, canonical form (this is what gets
  persisted; deltas are transient).

### 7.4 Plan / Build modes

- **Mode is thread state**, persisted, and **switchable at any time within the thread**
  (requires `capabilities.plan_build_modes`).
- **Plan mode** ⇒ harness runs in planning collaboration mode; the agent analyzes and proposes an
  implementation plan. File and command permissions still come from the thread's permission preset
  (§9).
- **Build mode** ⇒ harness runs in default collaboration mode; the agent implements, subject to the
  thread's permission preset (§9).
- For the Codex harness, this same thread mode also drives Codex's app-server
  `collaborationMode`: Plan sends `plan`, Build sends `default`. This is distinct from sandboxing
  but must stay synchronized because Codex gates some interaction tools, such as
  `request_user_input` / `item/tool/requestUserInput`, on collaboration mode.
- The mode applied to a turn is the thread's mode **at the moment `start_turn` is called**.
  Switching mode takes effect on the next turn; the UI makes this explicit.
- **Durable switch (P2).** `SwitchMode` and `SelectModel` **persist immediately**: the new
  `mode` / `current_model` is written to `<thread_id>.json` before the server acknowledges, then
  a revisioned `ThreadMetadata` is published to subscribed tabs so they stay in sync. The initiating
  request also receives a correlated `ThreadMetadataResult`, including for a no-op. This satisfies
  the §5 "same state after restart" requirement — a switch is not lost if the app restarts before
  the user sends the next message. The sandbox/model *effect* still takes hold at the next turn
  (Codex accepts these per `turn/start`); only the stored *intent* is durable now.
- **Switching back and forth** is fully supported (Plan → Build → Plan …).

#### 7.4.1 Plan dump to markdown

- A **"Save plan to project"** button is available while in (or after) Plan mode.
- It writes the current plan as a markdown file **into the project directory**. Default path:
  `docs/plan-<thread-title-slug>-<YYYYMMDD-HHmm>.md` (configurable default in `config.toml`;
  the user may edit the path in a small dialog before saving). If `docs/` doesn't exist it is
  created.
- **What constitutes "the current plan":** the concatenation of the agent-message items of
  the **most recent Plan-mode turn** in the thread (i.e. the latest plan the agent produced),
  rendered to markdown. This is **strictly the single most recent Plan-mode turn** — no
  concatenation of earlier plan turns, even when earlier plan turns held content the user might
  expect (C6). If multiple plan turns exist, the latest wins; the dialog shows a preview so the
  user can confirm. (Rationale: simplest unambiguous rule; avoids trying to detect "the plan"
  heuristically across the whole thread.)
- Writing the plan file is a normal file write within the workspace root; it is **not** gated
  by the agent approval flow (it's a user action, not an agent action), but it respects the
  workspace-root boundary. The root is the one resolved *for the saving thread* — the same
  resolution the thread-scoped file endpoints use (§11.2) — so a plan is written where that
  thread works and the link offered afterwards can read it back. **Path confinement (P4):** the
  resolved path is canonicalized and anything escaping the workspace root (via `..` or symlink) is
  rejected before writing, using the same hardening specified for the browse endpoint in §6.2. A
  user-edited path like
  `../../etc/foo.md` must hit this check on write, not just on browse.
- After saving, the UI links the new file (openable in the code overlay, §11.2).

### 7.5 `TurnOverrides`

```rust
pub struct TurnOverrides {
    pub model: Option<ModelRef>,          // None ⇒ reuse the thread's current model
    pub mode: Mode,                       // plan | build → Codex collaboration mode
    pub permission_preset: PermissionPreset,  // thread permission preset snapshot
}
```

`TurnOverrides` is a **resolved snapshot**, not a delta. The server constructs it at
`start_turn` by reading the thread's persisted state:

- **`mode`** — from `thread.mode` (the thread's current mode, switchable via `SwitchMode`, §7.4).
- **`model`** — `None` means "reuse the thread's `current_model`." The server always resolves it
  to the effective `ModelRef` (which carries `reasoning_effort` in itself, §8.1) before passing it
  to the harness, so there is exactly one home for effort. A non-`None` value would override the
  thread's model for this turn only (not persisted); in practice the UI persists model changes via
  `SelectModel` (P2) and sends `None` here.
- **`permission_preset`** — read from `thread.permission_preset`. This is **not** a per-turn override
  (P3/AP1): the user changes the thread's durable setting, not a single message. It appears in the
  snapshot because the harness needs it to pass permissions to `turn/start`. It is set persistently via
  `SetPermissionPreset` (§13.6).

**Effort lives only in `ModelRef.reasoning_effort`** (P1). There is no standalone
`TurnOverrides.reasoning_effort` field — it was removed to eliminate the double-home. The
effective effort is read from `current_model.reasoning_effort` and is sent to the harness only when
the active model advertises `supports_reasoning_effort` (§8.5).

**When `plan_build_modes = false`** (S7): `Mode` resolves to `Build` (the default collaboration
mode), so `TurnOverrides` is well-defined for every harness regardless of capability.
The Plan/Build toggle is hidden in the UI (§13.5) and `Mode::Build` is always used.

---

## 8. Model Selection & Providers

### 8.1 Model identity

A model is identified by the **pair `(provider, model_id)`** plus optional reasoning effort:

```rust
pub struct ModelRef {
    pub provider: String,          // e.g. "openai", "cloudflare-litellm"
    pub model: String,             // e.g. "gpt-5.5", "@cf/z-ai/glm-4.7"
    pub reasoning_effort: Option<Effort>,
}
```

The **same model name on two providers is two distinct entries** (explicit requirement). The
UI always shows provider + model together.

### 8.2 Provider configuration

Providers are declared in `config.toml` (§16.3) as a table keyed by routing id —
`[providers.<id>]`, the same shape Codex uses for `[model_providers.<id>]`. A declaration carries
only what the harness cannot supply: whether to run model-list discovery, and the models to offer.
Example providers relevant here: OpenAI direct (Codex's built-in), and a LiteLLM gateway fronting
Cloudflare Workers AI.

Keying by id rather than listing entries makes a repeated id a TOML parse error instead of a silent
first-wins duplicate, and declaration order is preserved as the picker's order (§8.3). It also makes
the id a TOML key rather than a string value, so an id that is not a bare key must be quoted —
`[providers."openrouter.ai"]`. Unrecognised keys within a declaration are rejected, which is what
turns the unquoted form (a provider `openrouter` with a stray sub-table) into an error naming the
offending segment rather than a provider silently offering no models.

**The harness owns provider configuration.** A provider's display name, endpoint, and key
location already exist in the harness's own configuration, and Giskard reads them back through
`AgentHarness::list_providers` (behind the `provider_listing` capability) rather than asking the
user to restate them. Restating them is not merely redundant: two copies of an endpoint drift,
and the copy Giskard holds is not the one that routes turns. A harness that cannot introspect its
own configuration reports nothing, and Giskard falls back to the declared list alone.

Only the **location** of a key is read, never an inline secret. A harness reports either the name
of an environment variable holding it, or a command whose stdout is the token (Codex's
`[model_providers.<id>.auth]`), and the two are mutually exclusive — Codex rejects a provider
declaring both. An inline secret (Codex's `experimental_bearer_token`) is not read at all: copying
it into Giskard would spread a credential into another process's memory and logs to no end.

Giskard runs that command itself, because `/v1/models` discovery is its own HTTP request rather
than one the harness makes. A harness may only report a command it read from configuration the
*user* controls, never from the project directory: opening a project points the harness at a
checkout, and a command named by a file inside that checkout would run on nothing more than
composing a model list. (Codex holds this line itself — `model_providers` is on its project-local
denylist, so a repository's own `.codex/config.toml` cannot declare a provider at all.)

The token is recomputed whenever discovery needs it rather than cached: discovery runs when a
project's model list is composed, which is rare enough that a cache would mostly hold a stale
token. A command that fails, times out, or prints nothing is reported as itself and the request is
not attempted — sending it unauthenticated would bury the cause under a 401 blaming the endpoint.
The same vetting applies to a key read from an environment variable: either source is trimmed and
must be usable as a header value, and the failure names whichever one it came from.

**Id validation.** A `[providers.<id>]` key is the routing id sent to the harness, so an id the
harness does not know cannot route. Giskard checks the configured ids against the harness's
provider table whenever it composes a project's model list, and reports each unknown id as a
warning (§8.3) naming the provider. The models stay in the picker — the harness may be
misconfigured rather than the id being wrong — but the mismatch is surfaced at picker time
instead of arriving as a provider-side `model_not_found` in the middle of a turn. An unanswered
table (no capability, or a failed query) validates nothing: silence is not evidence that the ids
are wrong.

> Note: Codex itself reads its own `~/.codex/config.toml` for provider/auth (Codex is
> "already configured", §12.2). Giskard's provider config governs (a) what the UI offers in
> the model picker and (b) optional `/v1/models` discovery. Codex's provider table reaches
> Giskard through `config/read`, which returns the whole effective config including the
> `[model_providers]` entries; its five built-in ids (`openai`, `amazon-bedrock`,
> `amazon-bedrock-runtime`, `ollama`, `lmstudio`) never appear there and the adapter adds them.
> The `ModelRef` Giskard sends as a
> per-turn override must correspond to a model/provider Codex is configured to reach. For the
> Codex harness specifically, provider is native-thread-scoped: `thread/start`/`thread/resume`
> receive `modelProvider`, while `turn/start` only supports a model override. New thread drafts
> avoid provider rebinding by delaying native creation until the first send. Once a native Codex
> thread exists, it stays on its native provider; switching providers requires a new Giskard thread.

### 8.3 Model list: static + dynamic

- A **static list** in config is always available (works offline, deterministic for tests).
  Each static entry is a **typed model definition**, not a bare string, so it can supply the
  `ModelDescriptor` fields the UI needs (see the `[[providers.<id>.models]]` tables in Appendix C):

  ```toml
  [[providers.openai.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
  ```
- **Discovery is on for every provider the harness reports**, refreshing the list from
  `GET {base_url}/models` and merging the results over the static list. The `base_url` and the key
  come from the harness's provider table (§8.2), so discovery is a per-project operation: there is
  no endpoint to query until a harness can name one. A manual "refresh models" action triggers
  this; results are cached in memory per project.

  Which providers end up *offered* is narrower than which are queried: one with no endpoint yields
  nothing, so it appears only if the harness catalog covers it (below) or config declares its
  models. A harness reporting built-in ids it neither has an endpoint for nor routes to — Codex
  lists `ollama` and `lmstudio` whether or not they are used — contributes nothing for them, which
  is the intended outcome rather than a gap.

  `[providers.<id>]` is therefore **optional**, and a config naming no providers at all is the
  expected case. A provider the harness reports is one the user already declared *to the harness*;
  requiring them to declare it again to Giskard bought nothing, and in practice a hand-written
  `[[providers.<id>.models]]` list is rare — discovery is what makes a new model appear under the
  right slug at all. `model_listing = false` turns it off for one provider, which is what an
  endpoint that serves turns but has no `/models` route needs.

  The setting is **tri-state**, and the third state is load-bearing: unset means on, `true` means
  on *and* asked for, `false` means off. Only a provider explicitly asked for is worth a warning
  when it cannot be discovered — a harness reports providers with no `base_url` at all (Codex's
  five built-in ids among them), and complaining about each on every refresh would bury the
  warnings that matter under ones nobody asked for.

  Providers are queried **concurrently**. Serially, one slow endpoint delayed every provider behind
  it; that was tolerable while listing was opt-in per provider and is not once it is on by default.
  Results are collected in picker order, so concurrency changes the timing and nothing else.
- **Picker order** is config's where config names a provider, then everything else by id, applied
  once to the composed list. The three sources contribute at different points — declared models,
  discovery, then the harness catalog — so ordering any one of them alone would let a provider
  supplied only by the catalog land behind one the user never declared. Within a provider the
  arrival order stands: a declared entry precedes that provider's discovered ones, and a stated
  `priority` still orders its catalog. The
  harness table carries no order to inherit — Codex parses `[model_providers]` into a `HashMap`
  before its config reaches the wire, so declaration order is lost upstream and what arrives is a
  hash order that differs between app-server restarts. Declaring a provider is therefore how a user
  pins it first; sorting the remainder keeps the picker, and the first-model fallback a draft starts
  on (§8.5), from reshuffling underneath them.
- **Two response shapes, combined.** The same endpoint answers differently depending on who is
  asking, so Giskard reads both:
  - the OpenAI-compatible `{"data": [{"id": …}]}`, which usually names ids and little else;
  - the **harness catalog** `{"models": [{"slug", "display_name", "context_window",
    "supported_reasoning_levels", "visibility", …}]}`, which a provider serves to a caller that
    identified itself.

  Asking for the catalog does not guarantee one — a provider may answer the OpenAI shape regardless,
  and since `data` and `models` are different keys it may answer both at once. Both lists are read
  and combined per model id, the catalog winning **field by field**: where it says nothing, the
  OpenAI entry's value stands rather than being discarded. A model the catalog marks hidden is
  dropped even if the OpenAI list named it. A body carrying neither key is an error, not an empty
  listing.

  The second is worth asking for because it answers what nothing else can: `context_window` exists
  in the harness's own catalog but is dropped before its protocol, so a harness cannot report it
  even in principle (§4.3). A provider serving this shape needs **no** `[[providers.<id>.models]]`
  entries at all — the endpoint supplies the windows, names, and effort levels config would
  otherwise have to state. Models marked `visibility: "hide"` or `"none"` are not offered; an
  absent `visibility` is treated as listed rather than silently emptying the picker. Where the
  catalog states a `priority`, it is stating an order and the models are offered in it — within
  that provider, so the picker's top-level order stays the `[providers.<id>]` declaration order.

  A stated effort list is an answer even when it is empty, and is kept apart from having said
  nothing: a provider that reports a model has no reasoning levels must not have levels handed back
  to it by the harness catalog. A non-`none` `default_reasoning_level` with no alternatives is that
  default, matching how the harness's own catalog is read (§4.3). The window is resolved the way the
  harness resolves it — `context_window`, else `max_context_window` — so an entry carrying only the
  maximum is not left on the conservative fallback.
- **Identifying the harness.** Discovery sends `client_version={harness version}` when the harness
  reports one (`AgentHarness::client_version`, §4.3), because that is how a provider is asked for
  the harness catalog. It is sent to every `model_listing` provider, not only those known to serve
  that shape: an OpenAI-compatible endpoint ignores a query parameter it does not recognise, and
  gating on how a provider authenticates would deny the richer catalog to one that serves it with a
  plain `env_key`. A harness that cannot state its version has none invented for it — the
  parameter is simply omitted, as it is for a version that could not go in a query string as-is.
- Each model entry resolves to a `ModelDescriptor { provider, model, context_window,
  supports_reasoning_effort, display_name }`. `context_window` drives the thread context gauge
  (§10.3); `supports_reasoning_effort` drives whether the effort selector is shown (§8.5).
- **Metadata source precedence** (first hit wins) for `context_window` and
  `supports_reasoning_effort`:
  1. the typed `[[providers.<id>.models]]` entry in config;
  2. the **provider's** `/v1/models` response, **if** it includes the field — both of its lists
     combined as above: the harness catalog shape carries `context_window` and
     `supported_reasoning_levels` outright, and many OpenAI-compatible endpoints, including
     LiteLLM, return `context_window`/`max_input_tokens` and capability hints;
  3. the **harness's own catalog** (Codex's `model/list`), overlaid per project as described
     below — for `supports_reasoning_effort` and the effort list only, since it carries no context
     window;
  4. a **conservative fallback** (`context_window = 128000`, `supports_reasoning_effort =
     false`), so an unknown model is still usable until provider or runtime metadata is available.

  Steps 2 and 3 are distinct sources and their order matters. The provider's response describes a
  model *under that provider*; the harness catalog is keyed by model id alone and knows nothing
  about providers, so it can only describe what some model of that name supports. It therefore
  fills what discovery left unsaid and never replaces it — otherwise a level a provider advertised
  for its own model would be overwritten by a same-named model's.

  Giskard must not maintain a model-name defaults table. Model metadata changes independently of
  Giskard releases and must come from configuration, provider discovery, or the active harness.

  `display_name` follows the same four steps, with the harness catalog filling a name the config
  and the provider both left unset.
- **The harness catalog is a source, not only an overlay.** A harness that names the provider its
  models route to has supplied everything a picker entry needs, so its catalog entries are offered
  in their own right when no other source produced them — appended after config and discovery, and
  skipped in two cases: when the harness cannot say which provider a model belongs to, since an
  unattributed entry is not routable, and when that provider's config sets `model_listing = false`,
  since the opt-out is from listing whatever the source and such a provider offers only its
  declared models. This is what a stock harness depends on: Codex's built-in providers carry
  no `base_url`, so discovery finds nothing for them and `model/list` is the only source there is.
  Context window still comes from elsewhere (§4.3) — the catalog has none to give.
- **An empty picker explains itself.** When nothing supplies a model *and* nothing else has already
  warned, the refresh says so rather than serving a blank selector. A provider that reported its own
  failure has already named the cause and is not followed by a vaguer one.
- **Per-project model list + harness metadata.** When a project is open, the picker list is served
  **per project** by `GET /api/projects/{id}/models`: the configured models, merged with each
  `model_listing` provider's `/v1/models` discovery, with the project harness's metadata overlaid on
  top. A harness may advertise its own model catalog — the Codex adapter maps the app-server
  `model/list` RPC (behind the `model_listing` capability). Two things are overlaid, keyed by
  **model id and independent of provider** (the Codex catalog carries no provider; the same model id
  denotes the same model even if two providers route it differently — routing stays keyed by
  `(provider, model)` per §8.1):
  - **`display_name`** — applied when the config left one unset, so an explicit config name wins.
  - **`reasoning_efforts`** — the exact effort levels the model advertises, used to populate the
    effort selector (§8.5). Applied only when the exact `(provider, model)` pair is not explicitly
    declared; a `[[providers.<id>.models]]` entry under another provider does not suppress the overlay.
    A catalog entry's `supports_reasoning_effort` flag and effort list are authoritative for an
    undeclared pair. An empty effort list can therefore describe either an unsupported model or a
    reasoning model without model-specific selectable levels; §8.5 defines the browser fallback for
    the latter. Efforts are model-defined strings (see `Effort` in §8.5), so whatever the catalog
    lists is offered verbatim.

  Codex exposes `default_reasoning_effort` separately from `supported_reasoning_efforts`. When the
  alternatives list is empty but the default is not `none`, the Codex TUI treats that default as the
  sole valid effort. The adapter mirrors that behavior by normalizing the default into a one-entry
  Giskard `reasoning_efforts` list. Only an empty alternatives list paired with a `none` default maps
  to no reasoning-effort support.

  Codex's `model/list` does not supply `context_window`; Codex reports its effective runtime window
  separately through `thread/tokenUsage/updated` (§10.3). Per-provider provider or harness listing
  failures come back as structured `warnings` whose `source` identifies
  `provider:<id>` or `harness:<kind>`. The overlay is best-effort, so failures preserve the usable
  config + discovery list while remaining visible to the user. The composed list is cached per
  project and is the descriptor source for both picker display and model mutations; the picker and
  turn-start/select paths must not resolve the same model against different metadata. "Reload
  models" replaces that project's cache. This is the **only** model list: there is no project-less
  route. Discovery and the harness catalog both need a provider's endpoint, which only a harness
  knows (§8.2), and no harness exists before a project does — so a project-less list could only
  repeat `config.toml` back, which is not a model list worth serving.
- **Importing a native thread takes that thread's model, not a chosen one.** Importing a harness
  thread Giskard has no record of must not name a model: on Codex, `model`/`modelProvider` on
  `thread/resume` are overrides that suppress the thread's own persisted model
  (`merge_persisted_resume_metadata` returns early once either is present), so naming one silently
  moves an existing conversation onto a different model. The imported thread takes the model **and
  reasoning effort** the harness reports — `thread/resume` answers with both, and the picker has to
  land on what the thread is actually running rather than showing "Default" for a thread mid-flight
  at another effort. If the harness reports no model, the thread cannot be imported. Reopening a
  thread Giskard already tracks is the opposite case and does name its persisted model — that
  override is also how a thread's provider is switched (§8.2).
- **A new thread's starting model is derived, never stored.** A project record carries no default
  model. The model a draft starts on is taken from the project's catalog at the moment the draft
  opens — the model the harness marks as its default (Codex's `model/list` `isDefault`), else the
  first entry. Storing it would only cache a decision made against an earlier provider and harness
  configuration: it can name a model the config no longer declares, or miss the one that has since
  become the default, and nothing would say so. Deriving it leaves nothing to go stale and no
  second place to keep in sync. A project is also created before its harness exists, so there is
  nothing to choose from at creation time in any case; the new-project dialog asks for a folder and
  a name only. If the catalog is empty the draft says so rather than substituting a model — a wrong
  provider is not recoverable once a thread has started (LT7).
- **Stale-provider normalization (E5).** When a persisted thread's `ModelRef` names a provider
  that is no longer configured, but its `model` id appears under exactly one configured provider,
  the server rewrites the provider to that configured provider before opening/resuming or starting
  a turn. If the matched model does not support reasoning effort, `reasoning_effort` is cleared.
  If zero or multiple configured providers match, the model ref is left unchanged and normal
  error/reporting paths apply.

### 8.4 Changing model within a thread

- Supported and expected. Selecting a different model updates the thread's `current_model`;
  it takes effect on the **next turn** (Codex accepts model per `turn/start`). This satisfies
  "change model during a thread".
- When the model changes, the thread's cached `context_window` (C4) uses a retained harness-reported
  value for the exact `(provider, model)` when available, otherwise the new model descriptor. The
  context gauge (§10.3) recomputes immediately. Turn completion must not replace an authoritative
  runtime value with descriptor or fallback metadata.
- On project/thread load, open/resume, `SendInput`, and `SelectModel`, the server applies
  stale-provider normalization (E5) before computing the context window or passing the model to the
  harness. This allows a project saved with an old provider id to recover when the configured
  provider set now contains the model under one unambiguous provider.
- **Per-model effort retention (C7).** When switching from a reasoning model (e.g.
  `effort = high`) to one with `supports_reasoning_effort = false`, the old model's effort value is
  **retained** in a per-thread `model_efforts` map keyed by `(provider, model)`. Switching back to
  the reasoning model restores the stored effort automatically. The effort parameter is never sent
  to the harness when the active model doesn't support it (§8.5 already handles the send-side). This
  means a user can toggle between a reasoning and a non-reasoning model without losing their effort
  preference on the reasoning model.

### 8.5 Reasoning effort

- Reasoning efforts are **model-defined**, not a fixed set. The `Effort` type is a transparent
  string newtype (Codex's own `ReasoningEffort` is likewise a bare string); Giskard never branches on
  the value, it just carries the user's selection to the harness. Common values are
  `minimal | low | medium | high | xhigh`, but a model may advertise any string (e.g. `max`), and it
  round-trips unchanged.
- Effort is selectable **only when the chosen model supports it** (`supports_reasoning_effort`);
  otherwise the selector is hidden and no effort param is sent (avoids sending unsupported
  parameters). When a model descriptor supplies a concrete effort list (`reasoning_efforts` — e.g.
  from Codex's `model/list`, §8.3), the browser offers exactly those; otherwise the common set above
  is offered for reasoning models.

---

## 9. Approvals & Permissions

> "Permissions" here = **agent action approvals**, not user roles. There is exactly one user.

### 9.1 Permission preset per thread

`PermissionPreset` is stored in each thread's `<thread_id>.json`, but its values are permission
presets:

- **`ask_first`** — starts from Codex's built-in `:read-only` permissions profile and `on-request`
  permission preset. Reads can proceed; writes, commands, network, and other escalations require
  approval.
- **`auto_approve`** — uses Codex's built-in `:workspace` permissions profile and `on-request`
  permission preset. Workspace work can proceed automatically; outside-workspace or other escalations
  still require approval.
- **`full_access`** — uses Codex's built-in `:danger-full-access` permissions profile and `never`
  permission preset. The UI labels it with a warning marker.

The preset is a **thread-level** setting, **not** a per-project or per-turn override (P3/AP1). Project
creation does not ask for it. New thread drafts default to `ask_first`, and the selected draft preset
is persisted when the first message creates the thread. On existing threads, the preset is settable via
the `SetPermissionPreset` client message (§13.6), which persists immediately, publishes a
revisioned `ThreadMetadata` to subscribed tabs, and returns a correlated metadata result to the
initiator — the same durable-switch pattern as `SwitchMode`/`SelectModel` (P2).

**Interaction with Plan mode.** Mode (Plan/Build) and permission preset are **orthogonal
settings**. Plan mode changes Codex collaboration behavior (`plan` vs `default`) but does not force
read-only sandboxing. The selected permission preset controls what the agent may do without asking.

### 9.2 Live approval flow (requires `capabilities.live_approvals`)

1. Harness pushes an approval request (command exec / file change / permission escalation);
   `CodexHarness` maps it to `AgentEvent::ApprovalRequested` with the details (command, cwd,
   reason, target path, and the set of available decisions). Codex approval/item/call ids are
   retained for routing/protocol responses, not shown as card metadata.
2. UI shows a non-blocking prompt scoped to the thread (with the command/diff preview).
   **Phase 3 (S6):** the preview uses the **raw diff string** from the harness (the text carried
   in the `ApprovalRequest`'s reason/detail). Structured `FileDiff` parsing and the side-by-side
   diff viewer are Phase 4 (§11, §15); the dependency is stated here so it is not discovered as a
   gap later.
3. User chooses a decision; server calls `respond_approval`.

Codex also has server-initiated requests that are not approval decisions:
`item/tool/call`, `item/tool/requestUserInput`, `mcpServer/elicitation/request`, auth refresh,
attestation, and future method names. These use `AgentEvent::ServerRequestReceived` rather than
`ApprovalRequested`, are rendered as transcript cards, and must remain pending until the browser
sends `respond_server_request`. Giskard may provide first-class UI for known methods, but unknown
methods must still be visible and answerable; silent best-effort rejection is not a valid normal
path.

Some Codex server requests can legitimately omit `turnId` (notably MCP elicitations, where the
JSON-RPC request id is the protocol identity and turn correlation is best-effort). Giskard must not
mint a synthetic `TurnId` to force these into an approval card. When a first-class approval card
requires a turn, the adapter may use the thread's active native turn if one is already registered;
otherwise it must surface the request as a generic pending server request or reject it with a logged
unroutable-request error rather than creating fake turn ownership.

If the user interrupts a turn while the harness adapter is waiting for an approval or generic server
request response, the adapter must keep the turn stoppable: send the harness interrupt, best-effort
cancel/reject the pending request, and resume draining harness messages. Waiting forever for the
original approval response after an accepted interrupt is a correctness bug because it leaves the UI
in an active turn with no remaining user action that can complete it.

**Decision granularity** (mirrors Codex):
- `accept` (this once), `accept_for_session` (don't ask again this session for this kind),
  `decline`, `cancel`. For command exec, an optional "accept with amended exec policy" may be
  offered if the harness provides it. For MCP tool calls, `accept_for_session` is only offered
  when the harness advertises it (Codex gates it on the tool's approval mode).

#### 9.2.1 Definition of "session" for `accept_for_session`

"Session" = **the lifetime of the current harness process for that project** (i.e. the
`codex app-server` child spawned for the project, §4.7). Rationale: the approval memory is a
property of the running agent process, which is what actually enforces it, so the boundary
must match that process.

Concretely:
- A `accept_for_session` grant persists until the project's harness process is shut down or
  restarted (idle shutdown, crash + reconnect, or app restart). After a respawn, previously
  session-granted approvals are **not** remembered and the agent will ask again. This is the
  safe default (fail-closed) and is simple to reason about.
- It is **not** tied to the browser tab/session, and **not** persisted to disk. Giskard
  mirrors the harness's own session-grant behavior rather than maintaining an independent
  grant store. The UI communicates this ("Approved for this session — resets if the agent
  restarts").
- Scope of a grant follows what the harness scopes it to (e.g. by command kind / destination,
  §9.3); Giskard does not broaden it.

### 9.3 Grouping & concurrency

- Concurrent approval prompts are grouped where the harness groups them (e.g. Codex groups
  network-access prompts by destination). The UI shows a queue if several are pending, scoped
  per thread so prompts from background threads don't hijack the foreground.

### 9.4 Degraded harness

If the active harness lacks `live_approvals`, the UI hides live prompts and should disable presets
that require user approval to become useful. This keeps the experience coherent across harnesses.


---

## 10. Token Tracking

### 10.1 Sources

Token usage comes from `TurnCompleted` (Codex reports usage on `turn/completed`). Each turn
contributes `{ input, output, total }` tagged with the `(provider, model)` used for that turn.

### 10.2 Aggregation levels

Recorded and viewable at:

- **Thread** — running totals in `<thread_id>.json` (`tokens`), plus per-model breakdown. The
  browser shows these totals in the thread-header context usage popover, not as a permanent
  right-panel section.
- **Project** — `projects/<id>/tokens.json`: `total`, `by_day`, `by_model`.
- **Global** — `tokens-global.json`: `total`, `by_day`, `by_model`.

Every `total` / `by_day[…]` / `by_model[provider][model]` value is the same `TokenUsage`
struct (B3). `by_model` is a **nested** `provider → model → TokenUsage` object (C3), never a
`"provider/model"` string key, so slash-bearing model ids stay unambiguous.

**Time windows** for the global (and project) views: **day / week / month / total**. Weekly
and monthly figures are derived on read by summing `by_day` buckets (single source of truth,
§5.3). A dashboard renders these as tables and simple charts.

### 10.3 Context-window gauge (per thread)

Within a thread, show the thread's current context footprint **relative to the active
model's context window** (e.g. 15.4k / 258.4k, or / 1M). The denominator starts from
`ModelDescriptor.context_window` and is replaced by a valid harness-reported effective window for
the current `(provider, model)`. It **recomputes when the model changes** (§8.4). This is a
usage-vs-capacity indicator to warn before hitting context limits.
The gauge is rendered as a header button; activating it opens a compact card with the same current
context footprint plus cumulative thread token totals from §10.2 and a manual `Compact context`
action routed through `HarnessCapabilities.context_compaction`. Unsupported harnesses return a
structured browser-visible error. Manual compaction is a thread-level operation and is disabled
while that thread has an active turn; other threads and projects remain usable while compaction is
running. Giskard does not need to warn near the limit because Codex may compact automatically.

> **Codex source field.** "Tokens used in the thread" for the gauge should reflect the current
> conversation's *context occupancy*, which is not the same as cumulative billed tokens
> (cumulative usage keeps growing across turns; context occupancy reflects what's currently in
> the window after any compaction). Codex reports usage through `thread/tokenUsage/updated`; the
> payload distinguishes cumulative totals from the last turn's usage and exposes an effective
> `modelContextWindow` denominator. The Codex adapter emits `ContextWindowUpdated` whenever the
> value changes within a turn, tagged with that turn's exact model, and the server persists it by
> `(provider, model)`. For the numerator,
> use an input-tokens / context-used figure per turn. **Candidate fields to use (in order):**
> (1) an explicit context/window-used field on the turn's usage object if present in the
> pinned schema; (2) otherwise the **last turn's input tokens** (the input side reflects the
> context sent to the model, which is the best available proxy for occupancy); (3) fall back
> to cumulative `total` only if neither is available. The implementer must inspect the pinned
> `codex app-server generate-json-schema` output for the exact fields on
> `thread/tokenUsage/updated.tokenUsage`, pick per the order above, and record the choice in a code
> comment + the README.

### 10.4 Optional cost estimation

Optional (config-gated): per-model € rates in `config.toml` produce an estimated spend
alongside raw token counts. Off by default; raw token counts are the primary metric.

---

## 11. Visualization: Diffs & Code Overlay

### 11.1 Diff viewer (side-by-side)

- **Visualization only** in v1 (no accept/reject of individual hunks, no mutating git ops).
- Fed by `AgentEvent::DiffUpdated` (Codex `turn/diff/updated`), per file.
- Rendered **side-by-side, in a panel next to the thread** on desktop. On mobile it becomes a
  full-screen tab/drawer (unified inline diff if side-by-side doesn't fit; §13.4).
- Shows the set of files changed in the current turn, selectable; each file shows old vs new
  with additions/deletions highlighted. Large diffs are virtualized (§11.3).
- The active project surface may expose a compact read-only Git status line above the composer:
  current branch or detached head, ahead/behind, conflicted and changed counts, and the working
  tree's diffstat. It expands in place into the changed files, grouped by conflicted/staged/
  unstaged/untracked, each opening its own diff, plus the combined working-tree diff. An untracked
  directory is listed as one entry rather than expanded file by file. The line refreshes as the
  working tree changes — when a turn completes, and as file changes stream during one — so it
  describes the tree now rather than when the thread was opened. Staging, committing, branch
  creation/switching, and hunk mutation stay out of scope.

### 11.2 Code overlay for referenced paths

- When an agent message (or command output) mentions a **filesystem path** within the
  workspace, Giskard makes it a clickable link. Clicking opens an **overlay/panel** showing
  that file.
- **Server-side syntax highlighting** with `syntect`: the backend reads the file from the
  project filesystem, highlights based on extension/first line, and returns highlighted HTML
  (plus raw text for download). The frontend renders the trusted server HTML; no JS
  highlighter, no npm.
- The overlay provides a **"Download file"** action (streams the raw file) and shows the
  file's path, size, language, and line numbers for text previews.
- **Path detection:** a server-side (or shared) linkifier scans agent text for path-like
  tokens and resolves them against the workspace root. Only paths that (a) resolve inside the
  allowed scope and (b) exist are linkified. Ambiguous/relative paths are resolved relative to
  the workspace root. This detection runs when an `ItemCompleted` agent message is finalized
  (not on every delta) for efficiency.
- **Line targets (L5/L6):** a path may include a `#<line>` or `:<line>` suffix, for example
  `src/main.rs#42` or `src/main.rs:42`. Compiler-style `:<line>:<column>` is also accepted, with
  the column ignored for navigation. The suffix is included in the clickable span but removed before
  filesystem validation; the response carries `path = "src/main.rs"` and `line = 42`, and the
  overlay scrolls to that line after loading. When possible, the target line is centered in the
  overlay viewport so before/after context is visible.
- **Initial UI slice (L1):** the served self-contained UI calls
  `POST /api/projects/{id}/threads/{thread_id}/linkify`
  for completed text items, renders path spans as inline controls, opens
  `GET /api/projects/{id}/threads/{thread_id}/highlight?path=…` in the code overlay, and downloads
  through
  `GET /api/projects/{id}/threads/{thread_id}/raw?path=…`. This is intentionally whole-file
  oriented until the
  virtualized line-range viewer in §11.3 is implemented.
- **Image previews (IV1):** completed Codex `ImageView` activity rows render a thumbnail through
  `GET /api/projects/{id}/threads/{thread_id}/image?path=…`. The endpoint uses the same workspace
  confinement as
  `highlight`/`raw`, serves only common raster image types with image MIME types, and rejects SVG
  for inline preview.
- **Markdown rendering (M1–M3):** agent/reasoning text is Markdown, so finalized messages are sent
  to `POST /api/projects/{id}/threads/{thread_id}/render` instead of `/linkify`. The server parses
  the Markdown
  (`pulldown-cmark`) and emits **sanitized** HTML with a custom serializer: all text is escaped,
  raw HTML in the source is escaped to inert text (never passed through), link URLs are restricted
  to `http`/`https`/`mailto`, and images are not fetched. Path detection runs in the same pass over
  prose text runs (not inside code), emitting the same `.path-link` controls the overlay wires up.
  Fenced code blocks are syntax-highlighted server-side with `syntect` when their fence language is
  recognized, and every code block is rendered with a compact header showing the resolved language
  label (for example `Rust` or `JSON`; unknown fence labels are shown as provided after
  sanitization). Inline code spans are escaped/styled but not syntax-highlighted because Markdown
  does not carry a reliable language for them. The browser injects the returned HTML as trusted
  markup and attaches the path-link handlers. `/linkify` is retained for command output, which is
  plain text rather than Markdown.

### 11.3 Large files & performance

- Files above a configurable size threshold are highlighted/served in **paginated chunks**
  (line ranges) and the viewer virtualizes rendering (only visible lines in the DOM).
- Highlighting is cached per (path, mtime) in memory to avoid recomputing on repeat opens.
- Binary/non-text files are detected and shown as "binary — download only".

---

## 12. Authentication

### 12.1 App auth (single shared password)

- **One shared password** gates the whole app. No accounts, no roles, no 2FA (v1). The threat
  model to keep in mind: an authenticated client can direct the agent (code execution) and read
  files within project workspaces, so the password guards host-level access, not just a UI.
- The password is stored as an **Argon2 hash** in `config.toml` (or an env var
  `GISKARD_PASSWORD_HASH`), never in plaintext. A `giskard-admin set-password` command
  generates the hash.
- **Brute-force throttling (SEC1):** login failures feed a global (not per-IP) lockout with
  exponential backoff: a handful of consecutive failures are tolerated, after which the login
  endpoint answers `429` with `Retry-After` until the window elapses. The lockout check runs
  **before** Argon2 verification so a flood of wrong passwords cannot be used as a memory-hard
  CPU/RAM DoS. Failed and throttled attempts are logged with a stable message plus the
  `X-Forwarded-For` value (meaningful behind a trusted reverse proxy) so external watchers
  (e.g. fail2ban) can key on them. The counter is in-memory; a restart clears it.
- **Login:** a POST verifies the password against the hash and issues a **signed session
  cookie** (HMAC-signed, `HttpOnly`, `SameSite=Strict`, `Secure`). Session lifetime is
  configurable (default 30 days) and **sliding**: a cookie-authenticated request in the second
  half of the lifetime re-issues the cookie for a full window, and the cookie `Max-Age` follows
  `session_days` (SEC3). A signing key is generated on first run and stored in the data dir
  (`session.key`, 0600).
- **Revocation (SEC4):** sessions are stateless HMAC tokens — logout clears the browser cookie
  but cannot invalidate the token server-side. `giskard-admin revoke-sessions` rotates
  `session.key`; after a server restart every outstanding session and ticket is invalid.
- **All routes except the login page and static assets require a valid session.** The
  WebSocket upgrade validates either the session cookie or a short-lived signed ticket from
  authenticated `GET /api/ws-ticket`. Tickets and session cookies are **domain-separated**
  (SEC2): they share the signing key but MAC distinct purpose strings, so the ticket — which
  travels in the `/api/ws` query string and can land in proxy access logs — is only good for a
  WebSocket upgrade within its 60-second lifetime, and a session cookie is not accepted in the
  ticket position.
- **Hardening headers (SEC5):** every response carries a strict `Content-Security-Policy`
  (`script-src 'self'`; the UI's script/stylesheet are the separate same-origin assets
  `/app.js` / `/app.css`, with app chrome at `/favicon.svg`, so no inline script executes),
  `X-Content-Type-Options: nosniff`,
  `X-Frame-Options: DENY` + `frame-ancestors 'none'`, `Referrer-Policy: no-referrer`,
  COOP/CORP `same-origin`, and a minimal `Permissions-Policy`.
- **Workspace confinement (SEC6):** when `browse.roots` is configured it bounds not only the
  filesystem picker but also `POST /api/projects` — the supplied `dir`/`workspace_root` must
  canonicalize into an allowed root. With no roots configured the whole filesystem remains
  reachable to an authenticated client (single-user default).
- TLS is terminated upstream (Nginx). Giskard assumes HTTPS in production; the `Secure`
  cookie flag is on by default and can be disabled via config for local HTTP dev.

### 12.2 Harness (Codex) auth

Codex is **already configured** on the machine (its own `~/.codex` credentials — ChatGPT
login or API key / custom provider). Giskard does **not** manage Codex's auth; it inherits the
environment when spawning the child process. Document the assumption clearly and fail with a
helpful message if the spawned app-server reports it is unauthenticated.


---

## 13. UI / UX

### 13.1 Stack

- **Dioxus fullstack, pinned to the 0.7 line** (`dioxus = "0.7"`, latest patch; 0.7 is the
  Axum-based Server Functions rebuild with single-line fullstack WebSocket support, which
  Giskard depends on). **Pin the exact minor in `Cargo.toml` and the `dx` CLI version in
  `rust-toolchain`/CI**, because the fullstack API differs between 0.6 and 0.7. Do not build
  against `main`/git.
- WASM frontend + Axum backend, built with the `dx` CLI. **No npm / Node / JS bundler.**
- **Styling: hand-authored CSS** (a single scoped stylesheet or Dioxus scoped-CSS), **not
  Tailwind.** Note: Dioxus 0.7 ships *automatic Tailwind* that spawns a Tailwind watcher when a
  `tailwind.css` is present — **do not use it**, since that path can pull in a JS toolchain and
  violates the no-npm constraint. Simply omit the `tailwind.css` trigger file. The Radix-based
  Dioxus primitives (unstyled, accessible) may be used for behavior (focus, ARIA, keyboard) as
  long as styling stays hand-authored CSS.
- Shared wire types live in `giskard-proto` so client and server never disagree on the
  protocol.

### 13.2 Design direction

A **minimal, intuitive, calm control surface** — this is a tool the user lives in for long
sessions, so clarity and low visual noise beat flourish. Explicitly avoid the generic
"AI app" defaults (cream + terracotta serif, or black + acid-green). Concrete direction:

- **Restraint first.** One accent color used sparingly for the primary action and active
  state; everything else neutral. Dark theme by default (long coding sessions), with a light
  theme available.
- **Typography:** a clean, slightly technical UI face for chrome; a real **monospace** for
  code, command output, diffs, and paths (these are the substance of the app). Paths and
  model names are always monospace so they read as "things you can click / act on".
- **Structure encodes state,** not decoration: mode (Plan/Build), model, and permission preset
  are always visible and legible at a glance in the thread header; a running turn has a clear
  live indicator. Running commands are shown both inline in the transcript and in the header
  `Tasks` menu; selecting a summary entry scrolls to the transcript command row.
  Command lifecycle state is shown with a non-color cue plus subtle color: `●` amber for running,
  `✓` green for succeeded, `✕` red for failed, and `■` muted gray/orange for terminated or
  declined.
- **Signature element:** the **thread transcript** treated as a first-class typed document —
  agent text, collapsible reasoning, command blocks with collapsible streamed output, and inline
  linkified paths — paired with the **context-window gauge** as a persistent, honest read on
  "how full is this conversation." That gauge + linkified transcript is what makes Giskard
  feel purpose-built rather than a generic chat wrapper.
- Copy is plain and action-named (§ frontend writing guidance): buttons say exactly what
  happens ("Save plan to project", "Switch to Build", "Interrupt"). Empty states invite
  action ("No projects yet — create one to start."). Thread setting controls use visible labels
  and action-oriented option text; for example, the permission preset selector is labeled
  "Permissions" and shows "Ask first", "Auto approve", and "⚠ Full Access" rather than raw enum names.

> The design plan above is a starting brief for the implementer, not a locked visual spec.
> The implementer should produce a small token system (4–6 named colors, the 2–3 typefaces,
> spacing scale) and iterate, keeping the restraint principle and the two non-negotiables:
> monospace for code/paths, and the always-visible mode/model/gauge state.

### 13.3 Primary layout (desktop / laptop)

```
┌───────────┬───────────────────────────────────────────────┐
│ Projects  │  Thread header: mode · model · permissions ·   │
│ + threads │  tasks · MCP · context usage                   │
│ (sidebar) ├───────────────────────────────────────────────┤
│           │                                               │
│  proj A   │  Transcript (streamed items, linkified paths,  │
│   ├ th 1  │  collapsible reasoning, tasks, command output, │
│   └ th 2  │  file changes)                                │
│  proj B   │                                               │
│   └ th 3  │                                               │
│           ├───────────────────────────────────────────────┤
│  Settings │  Composer: input · send/interrupt              │
└───────────┴───────────────────────────────────────────────┘
```

- **Left sidebar:** projects with their thread lists, a project-row disclosure control to collapse
  or expand each project's threads, "new project" / per-project "new thread" actions, and a
  bottom-pinned **Settings** menu for durable client UI preferences such as Appearance. Project
  collapse state is browser-local and persists across reloads.
- **Center:** thread header (mode, model, permission preset, tasks menu, MCP menu, context usage menu
  with manual compact action, plan-dump & interrupt actions) + transcript + composer.
- **Composer drafts:** unsent text is browser-local and scoped to the active persisted thread id.
  A new-thread draft uses a per-project draft key until the first message creates the thread.
  Switching threads saves the previous draft and restores the target draft; sending successfully
  clears only that draft. A view switch must not discard text typed while it is in progress (LT6):
  the composer belongs to the thread on screen, so a switch that leaves the old composer editable
  while it waits on the network can silently eat a message.
- **Approval buttons:** `accept_for_session` uses a distinct secondary treatment so it does not
  read like the neutral/default Cancel action.
- Source/code previews and downloads open as overlays from linkified transcript paths rather than
  occupying a permanent right column.

### 13.4 Responsive (smartphone)

- The two columns collapse into a **single-column drawer navigation**:
  - The top bar opens the **Projects** drawer, which also contains the **Settings** menu.
  - The transcript remains the primary view.
  - Side-by-side diffs fall back to **unified inline** diffs when width is insufficient.
- Composer stays pinned to the bottom on the Transcript view. Approval prompts appear as a
  bottom sheet.
- Touch targets ≥ 44px; the app is usable one-handed for the common loop (read → approve →
  send).

### 13.5 Capability-driven UI

The UI reads `HarnessCapabilities` for the active harness and adapts:

- No `live_approvals` ⇒ hide approval prompts; presets that depend on live approval routing should
  be disabled or coerced.
- No `plan_build_modes` ⇒ hide the Plan/Build toggle (thread is single-mode). `Mode` resolves to
  `Build` (default collaboration mode) so `TurnOverrides` is always well-defined (S7).
- No `per_turn_model` ⇒ model is fixed at thread creation (picker disabled mid-thread).
- No `reasoning_effort` or model doesn't support it ⇒ hide the effort selector.
- No `structured_diffs` ⇒ hide the Diffs tab (or show a plain textual change summary).
- No `mcp_status` ⇒ hide or disable the MCP menu. No `mcp_reload` ⇒ the menu can refresh the
  cached status but not ask the harness to reload MCP config. No `mcp_oauth_login` ⇒ servers in
  `not_logged_in` state show the auth state without an authenticate button.

This guarantees a coherent experience when a future, less-capable harness is plugged in.

### 13.6 Client ↔ server protocol (single multiplexed WebSocket)

#### 13.6.1 State authorities, clocks, and delivery classes

Client-visible state must belong to exactly one authority in this table. A clock orders only the
projection in its own row; comparing clocks from different rows has no meaning. Before adding a
wire message, its specification must name its authority, clock, and delivery class here.

These are the implemented authorities for persisted metadata, process-local runtime state, and
connection reconciliation. No client-visible field may be maintained by a parallel legacy store.

| State | Authority | Clock | Delivery class |
| --- | --- | --- | --- |
| Persisted thread metadata | thread metadata service | durable thread revision | revisioned replacement |
| Project thread catalog | persisted thread files | each row's thread revision | invalidation + HTTP replacement |
| Completed transcript | history JSONL | ordered `TurnId` | bootstrap/HTTP page |
| Active transcript | thread runtime registry | process-local event sequence | ordered journal |
| Active-turn ownership | thread runtime registry | runtime transition order | bootstrap/runtime replacement |
| Running tasks | thread runtime registry | process-local task revision | revisioned replacement |
| Requests | thread runtime registry | request state transition | ordered + runtime replacement |
| Cross-thread runtime overview | thread runtime registry | process-local overview revision | revisioned replacement |
| Direct action result | action handler | domain identity | direct control response |
| Background notice | notice authority | notice identity/revision | revisioned replacement |

Persisted thread revisions survive a server restart. Event, task, and overview counters do not;
their bootstrap establishes a new per-connection baseline. A metadata revision does not order
active-turn ownership, transcript events, tasks, requests, or notices.

`ThreadMetadata` is the only browser projection of persisted thread detail. It contains the thread
id, revision, title, mode, current model, effective context window, permission preset, and token
aggregates. Native harness ids, per-model caches for unselected models, ownership internals, and Git
workspace records remain server-side. Every project thread-summary row carries that thread's same
revision so WebSocket detail and HTTP catalog results can be compared without treating their
different field sets as interchangeable.

The metadata store owns revision allocation and no-op detection under the existing per-thread
write lock. A mutation which changes no durable domain value performs no write and advances no
revision. Because the paired browser compares JSON numbers, allocation stops at JavaScript's
maximum safe integer; revision exhaustion is an error, never a wrap or saturation. Recency is an
explicit mutation intent: successful user-visible setting changes touch `updated_at`; successful
turn completion records activity; normalization, imports, native-id repair, model cache updates,
and context-window restoration preserve it. Crash aggregate repair may advance recency only to the
latest persisted turn timestamp; it never uses the repair time.

- **One WebSocket per browser client**, multiplexing all projects/threads (chosen for lowest
  CPU/memory: one connection, one server-side fan-out task, no per-thread sockets).
- Messages are tagged with `project_id` / `thread_id`. Defined once in `giskard-proto`.
- **Thread open/create is REST-backed:** `POST /api/projects/{project_id}/threads` accepts
  `{ thread_id?: ThreadId, resume?: String }` and opens/reattaches persisted threads or explicit
  native resume/import flows. Unknown fields are rejected, so this endpoint cannot fabricate
  linked ownership or lifecycle evidence. Blank creation is rejected. New first-message creation uses
  `POST /api/projects/{project_id}/threads/start` with
  `{ text, attachments?, model_ref, mode, permission_preset }` and returns
  `{ thread_id, harness_thread_id, turn_id, warning? }`.

  A transcript link is opened through
  `POST /api/projects/{project_id}/threads/{parent_thread_id}/subagent-links/{item_id}/open`.
  The browser supplies only Giskard-owned coordinates. The server resolves the raw item from the
  parent's trusted runtime item view or persisted turns, derives its native child ID, prompt,
  lifecycle evidence,
  and containing `TurnId`, then idempotently materializes or returns the linked thread. Unknown and
  non-link items return `409 Conflict`. A reverse child-to-parent item returns the existing parent
  without changing ownership.

Linked-thread ownership is immutable once persisted. Existing primary threads are not reclassified,
and existing children are not reparented. New child imports reject invalid/cyclic parent chains and
reject a harness-reported native parent that differs from the owning parent's native ID. Transcript
rendering is read-only: only an explicit **Open linked thread** action may issue the item-based open
request. `DELETE /api/projects/{project_id}/threads/{thread_id}` computes the complete
linked descendant subtree, rejects the request with `409 Conflict` before mutation if any member has
an active turn or running task, then deletes native and local threads leaf-first so the parent is
removed last. A Codex native rollout that is verifiably already absent is an idempotent native
deletion; all other native deletion errors stop the cascade before deleting that local record.

**Client → server** (examples): `Subscribe { thread_id, since? }` (`since` is the incremental-resync
cursor, H8), `Unsubscribe { thread_id }`,
`SendInput { thread_id, text, attachments? }`,
`SwitchMode { thread_id, request_id, mode }`,
`SelectModel { thread_id, request_id, model_ref }`,
`SetPermissionPreset { thread_id, request_id, preset }`,
`Interrupt { thread_id }`, `CompactContext { thread_id }`,
`TerminateCommand { thread_id, process_id }`,
`ApprovalDecision { thread_id, request_id, decision }`,
`ServerRequestResponse { thread_id, request_id, response }`,
`RetryTurnPersistence { thread_id, turn_id }`,
`DiscardUnpersistedTurn { thread_id, turn_id }`, and `SavePlan { thread_id, path }`.

`SendInput` and `CompactContext` are serialized per thread by the server before they enter the
harness. If another normal turn or manual context compaction is already starting or running on the
same thread, the server rejects the later request with `Error { code: "thread_turn_active", ... }`
instead of starting a second forwarder. This is a correctness boundary, not only a browser disabled
state: multiple tabs or reconnect races must not be able to start overlapping native turns for one
thread. The browser marks a turn active immediately after successfully sending `SendInput`, before
any harness `TurnStarted` event, and clears that optimistic state if the server rejects the send for
anything other than `thread_turn_active`.

Passive sub-agent monitoring uses that same per-thread gate. It is armed only by explicit
non-terminal lifecycle evidence; reopening an existing child without such evidence and terminal
lifecycle events do not start a monitor. Its first turn-bearing
event reserves and acknowledges the native turn before any runtime-registry, publication, or
persistence mutation. If a normal forwarder already owns the thread, the passive subscriber exits
without processing it. A terminal observation wakes an idle passive subscriber immediately so
queued child events win, then fallback recovery or clean exit occurs without waiting for the idle
timeout.
Explicit active evidence has a renewable ten-minute no-event pre-turn safety bound; any stream or
lifecycle activity restarts it, and it no longer applies once a native turn starts. Direct user
input is rejected while that passive ownership is registered, then allowed as a normal child turn
after delegated work becomes idle; it never changes parentage or implicitly forwards the result to
the parent. Passive and interactive subscribers do not both rebroadcast turnless events: when the
interactive forwarder owns the turn gate, the passive subscriber yields before broadcasting.

> **Durable settings switches (P2/P3).** `SwitchMode`, `SelectModel`, and `SetPermissionPreset`
> persist immediately to `<thread_id>.json` before the server acknowledges, then broadcast a
> revisioned `ThreadMetadata` to subscribed tabs. The initiating browser receives
> `ThreadMetadataResult { request_id, ...ThreadMetadata }` after commit even when the requested
> value was already current; failures carry the same request id in `Error`. This guarantees the §5
> "same state after restart" requirement without leaving an optimistic control pending on a no-op.
> The permissions/model/mode *effect* still takes hold at the next turn; only the stored *intent* is
> durable now. Draft-thread setting changes are local until the first message creates the thread;
> they become durable as part of `POST /threads/start`.

> **Attachment payloads are transient.** `SendInput` and first-message thread creation accept up to
> eight attachments and 25 MiB of decoded attachment data in total. Each attachment carries name,
> MIME type, byte size, kind, and base64 bytes in the browser request. The server validates base64,
> decoded size, bounded metadata, the aggregate limit, and supported image signatures before
> starting a turn; an image's declared MIME type must match its PNG, JPEG, GIF, or WebP signature.
> Raw bytes are omitted from Giskard history and its parsed in-memory history cache. For the Codex
> harness,
> image attachments become `UserInput::Image { url: "data:<mime>;base64,<bytes>" }`; other files
> are transferred with Codex `fs/writeFile` to a randomized, per-turn harness-host temp directory,
> and that path is appended to the text prompt. The Codex adapter removes the directory with
> `fs/remove` after the turn, an upload/start failure, stream loss, command/control channel closure,
> or shutdown. It never writes upload bytes into the project workspace.

**Server → client** has one current, paired-client protocol:

- `ThreadEvent { thread_id, subscription_generation, seq, event }` is the only ordered live lane.
  `event` is either `Agent { agent_event: WireAgentEvent }` or
  `Request { request: RequestState }`. Request receipt, claim, rollback, and resolution therefore
  use the same sequence and journal as transcript events; one harness event never produces a
  second field-specific live message.
- `ThreadMetadata`, `ThreadTasks`, `ThreadNotices`, and `ThreadRuntimeOverview` are complete,
  revisioned replacements for their authorities. `ThreadRuntimeOverview` is sent on every socket
  connection, including when empty. `ThreadMetadataResult` is the direct correlated result for a
  browser metadata action, including a no-op. `ThreadCatalogChanged` is a coalescible invalidation
  for authoritative HTTP catalog replacement.
- `ThreadBootstrap { thread_id, subscription_generation, frame }` is one staged transaction.
  Every bootstrap emits `Start`, one or more `Chunk` frames for each of the six sections, then
  `Commit`, even when each section fits one chunk. The independently encoded sections are metadata,
  history, live turn, ordered suffix, final runtime, and notices. Start/chunk frames never mutate
  authoritative browser state; only a matching-generation commit validates and applies the whole
  payload.
- Older completed transcript pages use authenticated HTTP, not the WebSocket:
  `GET /api/projects/{project_id}/threads/{thread_id}/history?before={turn_id}&limit={count}`.
  The required cursor is echoed in `{ before, turns, has_more }`; the optional count is capped at
  100. Browser requests are cancelled or ignored when navigation changes the active thread.
- `ResyncRequired { thread_id, subscription_generation, reason, retry_after_ms? }` invalidates one
  subscription, not the socket. The browser re-subscribes that thread on the same connection.
- `Error { code, severity, message, detail?, thread_id?, action?, request_id?, process_id? }` is a
  direct control result; `Pong` is the heartbeat response.

The bootstrap final-runtime section contains the committed `through_seq`, active-turn or
`PersistenceBlocked` state, an optional independent historical-amendment recovery, the task
baseline, and every current request state. The live-turn
section contains a compact reconstruction plus `represented_through`. The ordered suffix contains
only events after that representation watermark which are not explicitly covered by a history turn
included in the same transaction. An appended late-command amendment carries its sequence as
coverage; it is filtered only when the coherent history snapshot already includes that sequence.

`OpenThreadResponse` may also carry `warning: ErrorInfo?` with the same `code` / `severity` /
`message` shape when the requested thread was opened but degraded (for example, Codex resume
failed and Giskard started a fresh native session while keeping persisted history).

**Request resolution invariant (AR1/SR6):** request identity is `(thread_id, request_id)`. Approval
and server requests share one authoritative state machine: `Pending → Responding → Resolved`, with
a harness-delivery failure returning `Responding → Pending`. Only one concurrent claim succeeds.
Every transition is an ordered `ThreadEventPayload::Request`, and the final-runtime bootstrap
replacement settles actionability after replaying older chronology. A harness may emit its own
server-request resolution late or not at all; the successful response commit remains authoritative.
Duplicate, stale, or wrong-thread responses are protocol errors. Browsers close only native
notifications keyed to the resolved `(thread_id, request_id)`.

**Client rendering invariant (E6):** `ItemDelta { item_id }` and the later `ItemCompleted`
for the same `Item.id` are one lifecycle. The UI must finalize or replace the streamed body in
place when the completed item arrives. Scoped Giskard `(TurnId, ItemId)` is authoritative for
rendered-item identity. Deltas for distinct identified items must retain distinct rows even when
they are interleaved. Scoped `harness_item_id` is a secondary consistency and replay-correlation
key; it must never re-key or merge rows with different Giskard identities. Consecutive file-change
items may share one visual row only within the same turn. The row retains one contribution per
scoped item identity, so updating one item replaces only its contribution and preserves the other
merged file changes.

**Client thread isolation invariant (WS1/WS2):** before rendering or mutating local thread state,
the browser must verify that every thread-scoped server message belongs to the active thread.
Messages for a previously selected thread, including frames delivered by a replaced WebSocket
connection, are ignored. Thread-scoped messages without a usable `thread_id` fail closed, except
for global errors that intentionally omit `thread_id`.

**Server thread isolation invariant (WS3):** a per-thread event forwarder only owns
`AgentEvent`s whose `thread` field equals the forwarder's `ThreadId`. Events for another thread
are ignored before turn ownership, runtime-registry application, request/task updates, journal
publication, or JSONL persistence. Each ignored foreign-thread event is
logged at error level with structured fields sufficient to diagnose the harness routing bug without
dumping the full event payload.

**Harness routing invariant (WS4/WS5/WS6):** harness adapters must route every mapped native event by
the mapped `AgentEvent.thread` before it reaches the server forwarder. If a native message carries a
non-empty unknown native thread id after native-thread registration has begun, the adapter treats it
as unroutable and logs/rejects it rather than relabeling it as the current fallback thread. Reopening
an already-open thread reuses the existing per-thread sender so live subscribers and forwarders are
not orphaned by metadata refreshes or duplicate open requests.

**Transcript visibility invariant (E7/E8/E9):** every finalized item payload with user-observable
meaning is rendered as a transcript row. `FileChange`, `ToolCall`, and `Activity` are visible rows;
they must not fall through to empty agent bubbles or be silently hidden. Started tool calls with
`ToolCallStart` metadata are also visible before completion, so long-running or stuck tool calls
do not appear as silent active turns. The client records `ItemStarted.kind` and uses it to style
streamed deltas before completion. Bootstrap live projection and ordered suffix events pass through
the same event handler used for live `ThreadEvent`s.

> **Wire types (C1/§3.5).** Everything the server emits that could carry a filesystem path
> (ordered events and bootstrap live/suffix contents) is mapped `core → Wire*` at the
> fan-out boundary, so paths are UTF-8 `String`s on the wire. Client→server messages are path-free
> (`SendInput` is text; `SavePlan.path` is a `String` re-validated server-side).
> Started and completed sub-agent items use `WireSubagentLink`, which retains display/prompt/
> lifecycle fields but omits the native harness thread ID. `ThreadSummary` likewise exposes only
> Giskard ownership IDs. Native routing identities remain in core/persistence and are resolved by
> the item-based open endpoint.

- **Fan-out:** the server keeps `thread_id → set<client_conn>` for ordered transcript traffic and a
  global connected-client registry for the replacement runtime overview. Background threads keep
  producing journal events, but only subscribers receive their transcript stream. Every connection
  receives the complete runtime overview, so inactive-thread turn/request badges and notifications
  do not depend on additive signals or a full transcript subscription.
- **Runtime overview and hidden threads:** the overview contains exact membership for threads with
  an active or persistence-blocked turn, a failed historical amendment, or a pending/responding
  request. An empty replacement clears
  stale state. The browser resolves hidden managed sub-agents through cached catalog ownership and
  hoists the most urgent descendant state to the nearest visible ancestor. Bounded graph walks
  prevent corrupt ownership cycles from spinning. Newly observed request identities drive browser
  notifications; replacing overview state never replays a notification already deduplicated in the
  page session.
- **Backpressure:** each connection has one bounded, class-aware outbox with data and reserved
  control capacity. Revisioned replacements coalesce by authority key in their original queue
  position. Ordered fragments are never coalesced or silently dropped: their admission pressure or
  a sequence gap moves only that thread subscription to `NeedsResync` and enqueues one same-socket
  `ResyncRequired`. Admission pressure for a thread replacement does the same rather than silently
  losing authoritative state. A slow tab cannot await or block event forwarding, history
  persistence, turn cleanup, or another tab. Bootstrap frame admission is bounded; failure releases
  its journal pin. A connection that cannot admit a global catalog or runtime-overview replacement
  is closed because no thread-scoped repair can restore it. Invalid client messages log a
  diagnostic, enqueue a final structured error, receive a bounded writer drain, and close the
  connection. Each physical WebSocket write also has a bounded deadline; expiry closes the stalled
  connection so its subscriptions, journal baselines, and idle runtime state are reclaimed.
- **Aggregate bootstrap and reconnect.** A subscription is registered in `Bootstrapping` before
  any read. The runtime captures and pins the live cut first, persisted history is read second, and
  the final journal cut atomically installs the Hub's `Committing` barrier under the
  runtime→subscription lock order. Events through `represented_through` are reconstructed by the
  compact live projection/final runtime; later events form a sequenced suffix. An event covered by
  a history turn in the same bootstrap is filtered from the suffix, while uncovered late command
  events remain. Metadata, tasks, and notices racing the transaction wait behind its barrier.

  The browser stages all six sections by subscription generation and applies nothing until commit.
  Commit clears the prior transient authority once, renders history, replays the compact live
  projection and ordered suffix through the normal event path, and applies final runtime last.
  Full, incremental, cursor-reset, one-chunk, and multi-chunk bootstraps use this one path. A
  superseded generation cannot commit. Pagination never enters it.

  The runtime registry owns one bounded per-thread journal shared by subscribers. A bootstrap pin
  reserves a bounded suffix budget from the live cut; exhaustion returns a retryable thread-scoped
  resync with a retry delay rather than growing memory or closing every thread on the socket.
  Runtime state and its process-local counters are retired once no lease, live projection, task,
  actionable request, journal pin, or subscriber remains.
- **Turn persistence handoff.** `TurnCompleted` is applied to the runtime but is not published and
  its lease is not released until the history append succeeds. The server makes three named,
  bounded automatic attempts and checks the authoritative history by `TurnId` before retrying an
  ambiguous append. Exhaustion transfers the complete `Turn` and lease to `PersistenceBlocked`,
  visible in the runtime overview and bootstrap. `RetryTurnPersistence` and
  `DiscardUnpersistedTurn` both require the expected `TurnId`, so a stale recovery action cannot
  settle a later turn. Discard requires browser confirmation, logs a structured lost-turn
  diagnostic, and is the only path which intentionally releases ownership without durable history.
- **Late history amendments.** Terminal command items arriving after their turn commit enter one
  runtime-owned FIFO per thread. The queue serializes payload mutation, amendment-sequence
  allocation, runtime application, and ordered publication, so two completions cannot expose their
  clocks in reverse order. Persistence appends one newline-terminated ordinary replacement item
  record to `turns/<turn_id>.jsonl`; the last committed item record for an
  `ItemId` wins without moving its display position. A malformed record or unterminated tail is
  preserved and skipped as defined in §5.4. The bounded index, aggregates, usage ledger, and
  recency do not change. Three failed
  attempts expose the queue head as `history_recovery` without changing `turn_state`, so a newer
  turn may continue. Retry and confirmed discard claim only that head. Discard resolves the live
  task without durable coverage and retains a warning that the result will not survive restart.
- **Running-task replacement (TK1).** Commands can outlive an interrupted turn after its transcript
  becomes durable. The thread runtime registry owns task state keyed by
  `(thread_id, turn_id, item_id)`, updated atomically with command **and tool-call** item
  start/output/completion events. Tool calls are tracked the same way (name + server, elapsed time,
  output tail) and shown
  in the same `Tasks` menu, but they carry no `process_id` and do not outlive their turn: a
  tool still running when its turn completes (i.e. an interrupted turn) is dropped, while commands
  are kept as `after_turn`. Stopping a tool has no per-call cancel in Codex, so the browser sends
  `Interrupt { thread_id }` (turn-level) rather than `TerminateCommand`. Subscribe carries the
  authoritative task baseline in `ThreadBootstrap.final_runtime`; after each later registry
  change, the server sends revisioned `ThreadTasks`. The browser renders these in the header
  `Tasks` menu and maps `(turn_id, item_id)` back to the transcript row for select/scroll (tool
  transcript rows are owned by the item stream, not re-rendered from the snapshot).
  `TerminateCommand` requests are forwarded to the active harness. Giskard must not terminate
  local processes directly; the adapter uses the harness's process-specific control operation. It
  must not fall back to turn interruption for command stop; the browser exposes turn interruption
  as a separate, broader action. When the harness accepts a terminate request, the matching command
  remains in the registry with `terminating: true` until a terminal command event arrives, but the
  browser labels this state as "stop requested" rather than "terminated" or "terminating". A
  successful terminate request is not itself proof that the process has stopped. If the harness
  later reports a normal successful completion, the browser preserves the successful status,
  annotates it with "stop requested", and logs that the harness did not terminate the process. A
  harness-specific not-found response may be treated as stale-state cleanup only for commands
  already marked `after_turn`; for live commands it is surfaced through the normal structured
  `Error` path and the command remains visible with `terminating: false`.
  Harness adapters that can observe post-turn command lifecycle messages must keep draining them
  while any command is known running. When a late terminal command completion arrives for an
  already-persisted turn, the server updates `ThreadTasks` and may publish the terminal ordered
  `ItemCompleted` event to connected clients, but it does not mutate the already-appended JSONL
  turn record.
  Running-task snapshots include `started_at_ms`; clients use it to render elapsed time and
  refresh that display about once per second. Completed command payloads include `duration_ms`
  when the harness supplies it; clients render terminal outcome text from the status plus duration.
- **Auth:** the WS upgrade validates the session cookie (§12).

### 13.7 JavaScript glue policy

Default: **none.** If a browser capability is unreachable from Dioxus/WASM without a tiny JS
shim (e.g. a specific clipboard or file-download nicety), the shim is **hand-written, minimal,
and committed to the repo** — never pulled from npm. Document any such shim in the README with
its justification. Prefer WASM/Rust or server-side solutions first (e.g. downloads are served
by the backend, §11.2, avoiding JS entirely).

---

## 14. Testing Strategy

Everything is tested. Three layers:

### 14.1 Unit tests

- `giskard-core`: pure domain logic (mode transitions, token aggregation math, path
  linkification, model-ref equality treating provider as significant, context-gauge
  computation, week/month derivation from daily buckets). No I/O, fast, exhaustive.
- `giskard-persist`: atomic-write behavior, atomic-JSON quarantine, payload-record recovery,
  concurrent-write safety (property tests where useful).
- `giskard-server`: auth (password hash verify, cookie signing/expiry), the filesystem
  browser's path-confinement logic (must reject `..`/symlink escapes when roots are set),
  syntax-highlight caching, protocol (de)serialization.

### 14.2 Integration tests with deterministic replay (no LLM)

This is the core requirement. Mechanism:

- **`ReplayHarness`** implements `AgentHarness` by reading a **recorded transcript** — an
  ordered list of harness transport messages (the raw JSON-RPC frames exchanged with a real
  `codex app-server`, captured once) — and emitting the corresponding `AgentEvent` stream
  with deterministic timing (no real model, no network).
- **Recording:** a `giskard-admin record` mode (or a test harness wrapper) runs a real Codex
  session once and writes the transcript fixture (`tests/fixtures/<name>.jsonl`). Fixtures are
  committed. A scrubbing step removes any credentials.
- **Replaying:** integration tests wire the application services to `ReplayHarness` and assert
  end-to-end behavior: sending input produces the expected persisted thread state; token
  ledgers update correctly; approval and server requests surface and responses are routed; plan
  dump writes the expected markdown; diffs are parsed and exposed; mode/model switches take effect
  on the right turn.
- **Determinism:** replay advances on demand (test drives the clock/step), so assertions are
  stable. The same fixtures double as a "demo mode" for the app without a real harness.

### 14.3 End-to-end (headless browser)

- A small **headless-browser** suite (e.g. via a WebDriver/Chromium-headless runner invoked
  from Rust) guards critical flows against regression: login, create project (with the file
  picker), open thread, send a message (backed by `ReplayHarness`), see streamed transcript,
  receive and answer approval and server-request prompts, view a diff, open a code overlay, switch
  mode/model, observe token/context updates. Runs in CI headless.
- Kept intentionally small (smoke-level for the main loop); business logic is covered more
  cheaply by §14.2.

### 14.4 CI gates

- All layers run in CI. A CI job regenerates the Codex app-server JSON schema for the pinned
  Codex version and diffs it against the vendored schema to catch protocol drift.
- Formatting (`cargo fmt`), lints (`cargo clippy -D warnings`), and the WASM build must pass.

---

## 15. Implementation Phases

All features are in scope for v1. Phases order the work so each builds on a working base; they
are **not** a scope reduction.

**Phase 0 — Foundations.** Workspace + crates skeleton; `giskard-core` domain types;
`giskard-proto`; flat-file persistence with atomic writes + validation + `giskard-admin`;
config loading; unit tests for core + persist.

**Phase 1 — Harness spine.** `AgentHarness` trait + capabilities; `CodexHarness` (spawn
app-server, JSON-RPC client, handshake, thread/turn lifecycle, event mapping);
`ReplayHarness` + fixture format + recording tool. Integration test: open thread, one turn,
assert persisted state (replay-driven).

**Phase 2 — Server & minimal UI loop.** Axum app; auth (password + session cookie); single
multiplexed WebSocket + fan-out; Dioxus shell; project list + create + filesystem picker;
open thread; send input; streamed transcript. E2E smoke: login → project → thread → message.

**Phase 3 — Modes, models, approvals.** Plan/Build toggle + Codex collaboration-mode mapping; plan
dump to markdown; model picker (static list) + per-turn model change + reasoning effort;
permission presets + live approval prompts + decision routing. Approval diff preview uses the raw
diff string (S6); structured `FileDiff` parsing is deferred to Phase 4. Tests for each via replay.

**Phase 4 — Visualization.** Side-by-side diff viewer from `DiffUpdated`; path linkification;
code overlay with `syntect` highlighting + download (initial whole-file UI slice complete in L1);
large-file virtualization/pagination.

**Phase 5 — Tokens & polish.** Thread/project/global ledgers; day/week/month/total dashboard;
context-window gauge; dynamic `/v1/models` refresh; responsive/mobile passes; optional cost
estimation; accessibility (focus, reduced motion), reconnect/backpressure hardening.

**Phase 6 — Hardening & docs.** Full E2E suite; protocol-drift CI check; README (setup, Codex
prerequisite, config reference, admin tooling); corruption/crash-recovery tests.

> The multi-harness abstraction is built in Phase 1 and exercised by `ReplayHarness`
> throughout; a second real harness (Claude Code) is **not** implemented in v1 but the trait,
> capabilities, and capability-driven UI ensure it can be added without touching persistence,
> core, or most of the UI.


---

## 16. Appendices

### Appendix A — Codex app-server mapping reference

The Codex **app-server** is a bidirectional **JSON-RPC 2.0** interface (the `"jsonrpc":"2.0"`
header is omitted on the wire). v1 uses the **stdio** transport (newline-delimited JSON), the
stable/production transport. The protocol is organized around three nested primitives —
**Thread → Turn → Item** — which map directly onto Giskard's model.

**Handshake (once per connection):** send `initialize`, then the `initialized` notification,
before any other call. The server returns its user-agent, `codexHome`, and platform info.

**Lifecycle:**

```
initialize → initialized
thread/start (or thread/resume {threadId})            → { threadId }
turn/start { threadId, input:[…], model?, effort?, sandbox? }
    ⇢ item/started, item/*/delta, item/completed  (stream)
    ⇢ turn/diff/updated                            (stream)
    ⇢ item/commandExecution/requestApproval  |  item/fileChange/requestApproval
                                              |  item/permissions/requestApproval   (server→client request)
    ⇠ (client responds with a decision)
turn/completed { usage, … }
turn/interrupt { threadId }                            (to cancel)
```

**Approval decisions** (client → server response): command execution — `accept`,
`acceptForSession`, `decline`, `cancel`, or an exec-policy-amendment variant; file change —
`accept`, `acceptForSession`, `decline`, `cancel`. Requests include `threadId`/`turnId` to
scope UI state.

**Overload:** JSON-RPC error `-32001` ("Server overloaded; retry later") ⇒ retry with
exponential backoff + jitter.

**Schema generation (vendored + CI-checked):**
```
codex app-server generate-json-schema --out schemas/
codex app-server generate-ts          --out schemas/   # reference only; not used at build
```
Artifacts are version-pinned to the Codex binary that produced them; regenerate on upgrade.

> Permission preset mapping: **Ask first ⇒ `permissions: ":read-only"`, `approvalPolicy:
> "on-request"`**, **Auto approve ⇒ `permissions: ":workspace"`, `approvalPolicy:
> "on-request"`**, **Full Access ⇒ `permissions: ":danger-full-access"`, `approvalPolicy:
> "never"`**. Codex collaboration-mode mapping is sent on every turn too: **Plan ⇒ `plan`**,
> **Build ⇒ `default`**.
> After app-server initialization, the Codex adapter resolves the effective
> `sandbox_workspace_write.writable_roots` with `config/read` for the project working directory.
> Auto Approve sends the project working directory plus those cached absolute paths as
> `runtimeWorkspaceRoots` on `turn/start`, so replacing Codex's runtime root set retains both the
> project and configured external build caches. A failed config read omits the override with a
> warning, leaving Codex's current thread roots unchanged.
> The Build/default send is intentional because Codex app-server collaboration mode is sticky after
> a plan turn. Plan/Build does not select the Codex sandbox; the permission preset does.
> `TurnOverrides.model` maps
> to the per-turn `turn/start` model field, but Codex has no per-turn `modelProvider` override;
> provider changes are handled at the native-thread boundary (§8.2). Reasoning effort is carried
> inside `ModelRef.reasoning_effort` (P1: no standalone effort field on `TurnOverrides`).
> `TurnOverrides.permission_preset` is the thread preset snapshot (P3/AP1: not a per-turn override).

**Client library:** use `codex-codes` (v0.146.4, tested against Codex CLI 0.146.x) with the
`async-client` feature — its `AsyncClient` API (`spawn`, `initialize`, `thread_start`, generic
`request`, `next_message`, `respond`, `shutdown`) maps onto the `AgentHarness` trait. The Codex
`turn/start` call uses the generic `request` path while `codex-codes`' typed `TurnStartParams`
lags newer fields such as `collaborationMode`. Its built-in schema coverage scorecard validates
typed structs against `codex app-server generate-json-schema` output and can be wired into the CI
drift check (§14.4). Fall back to `codex-app-server-sdk` (v0.5.1) or a hand-rolled client only if a
future Codex CLI version diverges beyond what `codex-codes` tracks. Whichever is chosen, confine all
Codex types to `giskard-harness-codex` and preserve the raw-JSON fallback for unknown/drifted
messages.

The harness initializes Codex app-server with `capabilities.experimentalApi = true` before starting
or resuming threads. This is required for the experimental app-server fields/requests Giskard
supports, including `collaborationMode` and `item/tool/requestUserInput`.

### Appendix B — Example client↔server WebSocket messages

```jsonc
// client → server
{ "type": "send_input", "thread_id": "01J…", "text": "Refactor the auth module" }
{ "type": "switch_mode", "thread_id": "01J…", "request_id": "meta_1", "mode": "build" }
{ "type": "select_model", "thread_id": "01J…", "request_id": "meta_2",
  "model_ref": { "provider": "cloudflare-litellm", "model": "@cf/z-ai/glm-4.7",
                 "reasoning_effort": null } }
{ "type": "set_permission_preset", "thread_id": "01J…", "request_id": "meta_3",
  "preset": "auto_approve" }
{ "type": "approval_decision", "thread_id": "01J…", "request_id": "ap_7",
  "decision": "accept_for_session" }
{ "type": "server_request_response", "thread_id": "01J…", "request_id": "srv_4",
  "response": { "kind": "result", "value": { "answers": ["Yes"] } } }
{ "type": "save_plan", "thread_id": "01J…",
  "path": "docs/plan-auth-20260706-1030.md" }

// server → client: one ordered lane for transcript and request transitions
{ "type": "thread_event", "thread_id": "01J…", "subscription_generation": 7, "seq": 42,
  "event": { "kind": "agent", "agent_event": {
    "kind": "item_delta", "thread": "01J…", "turn": "01K…", "item_id": "it_3",
    "delta": { "type": "text", "text": "I'll start by reading auth.rs…" }
  } } }
{ "type": "thread_event", "thread_id": "01J…", "subscription_generation": 7, "seq": 43,
  "event": { "kind": "request", "request": {
    "thread_id": "01J…", "request_id": "ap_7",
    "payload": { "kind": "approval", "request": {
      "id": "ap_7",
      "kind": { "kind": "command_execution", "command": "cargo test",
                "cwd": "/home/user/dev/x" },
      "available": ["accept", "accept_for_session", "decline", "cancel"]
    } },
    "status": { "status": "pending" }
  } } }

// server → client: every subscription uses the same staged transaction shape
{ "type": "thread_bootstrap", "thread_id": "01J…", "subscription_generation": 8,
  "frame": { "phase": "start", "sections": [
    { "section": "metadata", "encoded_bytes": 512, "chunk_count": 1 },
    { "section": "history", "encoded_bytes": 16384, "chunk_count": 2 },
    { "section": "live_turn", "encoded_bytes": 2048, "chunk_count": 1 },
    { "section": "ordered_suffix", "encoded_bytes": 1024, "chunk_count": 1 },
    { "section": "final_runtime", "encoded_bytes": 768, "chunk_count": 1 },
    { "section": "notices", "encoded_bytes": 128, "chunk_count": 1 }
  ] } }
// One chunk frame is sent for each declared chunk; one is shown here.
{ "type": "thread_bootstrap", "thread_id": "01J…", "subscription_generation": 8,
  "frame": { "phase": "chunk", "section": "metadata", "index": 0,
             "payload_base64": "eyJ0aHJlYWRfaWQiOiIuLi4ifQ==" } }
{ "type": "thread_bootstrap", "thread_id": "01J…", "subscription_generation": 8,
  "frame": { "phase": "commit" } }
```

### Appendix C — Configuration reference (`config.toml`)

```toml
# ${XDG_DATA_HOME:-~/.local/share}/giskard/config.toml   (path overridable via GISKARD_DATA_DIR)
# The file must exist and parse for giskard-server startup. Individual sections/keys may be omitted
# and then fall back to the defaults shown here.

[server]
bind = "127.0.0.1:8787"
secure_cookies = true          # set false only for local plain-HTTP dev

[auth]
# generate with: giskard-admin set-password
password_hash = "$argon2id$v=19$m=…"    # or via env GISKARD_PASSWORD_HASH
session_days = 30

[browse]
# empty/unset ⇒ entire filesystem browsable.
# set to confine the file picker to these subtrees:
roots = []                     # e.g. ["/home/user/dev"]

[plan]
default_dir = "docs"           # where "Save plan to project" writes
filename_template = "plan-{slug}-{ts}.md"

[tokens]
cost_estimation = false
# [tokens.rates."openai/gpt-5.5"]  input_per_mtok_eur = …  output_per_mtok_eur = …

# Optional in full: discovery runs for every provider the harness reports, and the harness's own
# catalog covers the provider it routes to (§8.3). A provider with neither contributes nothing.
# Keyed
# by routing id, mirroring Codex's own `[model_providers.<id>]`. The id must name a provider the
# harness knows (§8.2); name, endpoint, and key location are read back from the harness rather than
# restated here. Declared providers lead the picker in this order; the rest follow by id.
[providers.openai]
model_listing = false           # opt out — e.g. an endpoint with no /models route
  # typed model entries supply what no harness reports (§8.3):
  [[providers.openai.models]]
  id = "gpt-5.5"
  context_window = 262144
  [[providers.openai.models]]
  id = "gpt-5.4"
  context_window = 262144

# ⇒ [model_providers.cloudflare-litellm] in ~/.codex/config.toml. Discovery is on without saying
# so; this entry only adds metadata and pins the provider's place in the picker.
[providers.cloudflare-litellm]
  [[providers.cloudflare-litellm.models]]   # refines what discovery reports
  id = "@cf/z-ai/glm-4.7"
  context_window = 131072
  # display_name / supports_reasoning_effort may be set to override the harness catalog (§8.3)

[harness]
kind = "codex"
idle_shutdown_secs = 0          # 0 ⇒ keep alive while app runs
```

### Appendix D — Open items to confirm during implementation

These are deliberately left for the implementer to resolve and document, with a recommended
default already stated in-line:

1. **Context-gauge source field** — §10.3 now names the candidate fields and a selection order
   (explicit context-used field → last turn's input tokens → cumulative total). Remaining task:
   confirm the exact field name in the pinned Codex JSON schema and record the pick in code +
   README.
2. **Codex client crate choice** — **resolved: `codex-codes` v0.143.0** with the `async-client`
   feature (§3.3, App. A). Verified on crates.io against installed Codex CLI 0.142.5; its
   `AsyncClient` API maps 1:1 to `AgentHarness`, it includes a schema-drift scorecard for CI, and
   it ships real JSONL test captures. Fallback (`codex-app-server-sdk` v0.5.1 or hand-rolled) only
   if a future CLI version diverges.
3. **Dioxus fullstack single-crate vs split `giskard-ui`/`giskard-server`** — keep split
   unless tooling friction dictates otherwise; non-WASM crates stay separate regardless (§3.2).
   **Resolved (C1/C2):** `giskard-proto` is the sole crate `giskard-ui` links; it owns `Wire*`
   mirrors for path-bearing streamed types and re-exports the path-free `giskard-core` types; the
   server maps `core → wire` at the fan-out edge (§3.5).
4. **Plan-content extraction rule** — spec defaults to "latest Plan-mode turn's agent
   messages" with a preview before saving (§7.4.1); confirmed (C6): strictly the single most
   recent Plan-mode turn, no concatenation of earlier plan turns.
5. **Headless-browser runner choice** — pick a Rust-drivable headless option for §14.3 that
   introduces no npm dependency.

---

*End of specification.*
