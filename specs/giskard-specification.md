# Giskard — Technical Specification

> A local-first, single-user web application that provides a modern browser UI on top of
> agentic coding CLIs. The first supported agent harness is OpenAI's **Codex CLI** (via its
> `app-server` JSON-RPC protocol), but the application is designed so the harness is a
> replaceable component. Built entirely in Rust (Dioxus fullstack + Axum), with **no npm,
> Node, or JavaScript toolchain** anywhere in the build.

**Document status:** Implementation-ready specification.
**Audience:** An AI coding agent (and its human reviewer) implementing the system.
**Version:** 1.2

**Changelog (1.0 → 1.1), from review:**
- Resolved thread token schema vs §10.2: thread `tokens` now carries `total` + `by_model` (§5.3).
- Config models are now **typed** `[[providers.models]]` entries with a documented metadata
  precedence + conservative fallback (§8.3, App. C), fixing the missing `context_window` /
  `supports_reasoning_effort` source.
- `AgentHarness::shutdown` is now `&self` (object-safe for `Arc<dyn AgentHarness>`); added an
  explicit object-safety note (§4.3).
- Added §4.5 **normative type sketches** for all previously-undefined referenced types (`Item`,
  `FileDiff`, `ApprovalRequest`, `HarnessError`, `OpenThreadOptions`, `ThreadHandle`,
  `TurnStatus`, `UserInput`, etc.).
- Removed `attachments` from `SendInput`; attachments are explicitly out of v1 scope (§13.6, §4.5).
- Defined "session" for `accept_for_session` = harness-process lifetime, fail-closed on respawn (§9.2.1).
- Defined reconnect/live-turn resync via a per-turn in-memory live buffer + snapshot (§13.6).
- Clarified Plan mode × approval policy: orthogonal but read-only makes policy moot; policy value
  preserved (§9.1).
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
| **Mode** | A thread-level state: **Plan** (read-only; the agent analyzes and proposes) or **Build** (read-write; the agent implements). Switchable within a thread (§7.4). |
| **Approval** | A server-initiated request from the harness asking the user to allow or deny a command execution or file change. Handled per the project's approval policy (§9). |
| **AgentEvent** | Giskard's internal, harness-neutral representation of everything streamed from a harness. Codex protocol messages are mapped into `AgentEvent`s. |
| **Replay** | A recorded sequence of harness transport messages, played back through a mock harness for deterministic testing (§14). |

### 2.1 Conceptual hierarchy

```
Config (global)
└── Project (1 directory, 1 harness process)
    ├── ProjectConfig (workspace root, default model, approval policy, …)
    └── Thread (durable conversation)
        ├── ThreadState (mode, current model, token totals, context window)
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
| `giskard-proto` | Shared client↔server message types (serde), used by both `giskard-server` and `giskard-ui` so the wire protocol is defined once. |

> Dioxus "fullstack" can colocate server and client in one crate, but splitting `giskard-ui`
> (client) from `giskard-server` (backend) with a shared `giskard-proto` crate keeps the
> harness/persistence layers free of any WASM-target constraints and makes the backend
> independently testable. The implementer may merge `giskard-ui` into a fullstack crate if
> Dioxus tooling makes the split awkward, provided `giskard-proto`, `giskard-core`,
> `giskard-harness*`, and `giskard-persist` remain separate, native-only crates.

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
  - **`codex-codes`** (v0.143.0) — **recommended first choice.** Typed Rust SDK for the Codex
    CLI app-server JSON-RPC protocol, tested against Codex CLI 0.143.0 (≈ installed 0.142.5).
    Provides `AsyncClient` (Tokio) with `start()` (process spawn), `thread_start`, `turn_start`
    (accepting `model`, `reasoning_effort`, `sandbox_policy` — mapping 1:1 to `TurnOverrides`),
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
    /// A queryable model list (e.g. GET /v1/models via the provider).
    pub model_listing: bool,
    /// Token usage reported on turn completion.
    pub token_usage: bool,
}
```

Codex advertises all of these as `true`. A future Claude Code adapter would likely set
`live_approvals`, `structured_diffs`, and possibly `plan_build_modes` to `false` or a
degraded form, and the UI reacts accordingly (§13.5).

### 4.3 The trait

```rust
#[async_trait]
pub trait AgentHarness: Send + Sync {
    fn capabilities(&self) -> HarnessCapabilities;

    /// List models available through this harness/provider, if supported.
    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError>;

    /// Open (or resume) a thread. `resume` carries a harness-native thread id if resuming.
    async fn open_thread(
        &self,
        opts: OpenThreadOptions,
    ) -> Result<ThreadHandle, HarnessError>;

    /// Start a turn: send user input, applying per-turn overrides (model, mode, effort).
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

    /// Interrupt the active turn of a thread.
    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

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

    ItemStarted   { thread: ThreadId, turn: TurnId, item: ItemStarted },
    ItemDelta     { thread: ThreadId, turn: TurnId, item_id: ItemId, delta: ItemDelta },
    ItemCompleted { thread: ThreadId, turn: TurnId, item: Item },

    /// A structured file diff update (for the diff viewer).
    DiffUpdated { thread: ThreadId, turn: TurnId, diff: FileDiff },

    /// Server-initiated approval request.
    ApprovalRequested { thread: ThreadId, turn: TurnId, request: ApprovalRequest },

    TurnCompleted { thread: ThreadId, turn: TurnId, usage: TokenUsage, status: TurnStatus },

    Error { thread: ThreadId, turn: Option<TurnId>, error: HarnessError },
}
```

`ItemStarted`/`Item` variants cover: user message, agent message (with streaming text
deltas), reasoning note, command execution (with output deltas), file change, and MCP/tool
calls. `ItemDelta` carries incremental text or command output.

### 4.5 Supporting types (normative sketches)

These types are referenced by the trait (§4.3) and event model (§4.4). The shapes below are
**normative sketches**: field names and variants are the contract; the implementer may add
fields but must not rename or drop the ones shown, so that persistence (§5), the wire protocol
(`giskard-proto`), and the UI agree. All live in `giskard-core`.

```rust
// ---- IDs (ULID-backed newtypes) ----
pub struct ProjectId(pub Ulid);
pub struct ThreadId(pub Ulid);
pub struct TurnId(pub Ulid);
pub struct ItemId(pub String);       // harness-native item id (opaque)
pub struct ApprovalId(pub String);   // harness-native request id (opaque)

// ---- Handles / options ----
pub struct ThreadHandle {
    pub thread: ThreadId,
    pub harness_thread_id: String,    // native id used for resume
}

pub struct OpenThreadOptions {
    pub project: ProjectId,
    pub workspace_root: PathBuf,      // effective sandbox root (§6.3)
    pub resume: Option<String>,       // Some(native id) ⇒ resume; None ⇒ fresh thread
    pub initial_model: ModelRef,
}

pub struct TurnStatus {              // outcome of a completed turn
    pub kind: TurnStatusKind,        // Completed | Interrupted | Failed | Declined
    pub message: Option<String>,
}
pub enum TurnStatusKind { Completed, Interrupted, Failed, Declined }

// ---- Items ----
pub struct ItemStarted {
    pub id: ItemId,
    pub kind: ItemKind,               // discriminant only; payload fills in on completion
}

pub enum ItemKind {
    UserMessage,
    AgentMessage,
    Reasoning,
    CommandExecution,
    FileChange,
    ToolCall,                          // MCP/other tool invocations
}

/// The finalized item persisted in thread history and sent on `ItemCompleted`.
pub struct Item {
    pub id: ItemId,
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
    },
    FileChange     { path: PathBuf, change: FileChangeKind },
    ToolCall       { name: String, input: serde_json::Value, output: Option<serde_json::Value> },
}
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
    pub available: Vec<ApprovalDecision>,   // decisions the harness will accept
}
pub enum ApprovalKind {
    CommandExecution { command: String, cwd: PathBuf },
    FileChange       { path: PathBuf, change: FileChangeKind },
    Permission       { detail: String },    // network / extra-fs escalation
}
pub enum ApprovalDecision {
    Accept,
    AcceptForSession,                        // see §9.2.1 for "session" definition
    Decline,
    Cancel,
    AcceptWithExecPolicyAmendment { amendment: Vec<String> }, // command exec only
}

// ---- Models & usage ----
pub struct ModelRef {
    pub provider: String,
    pub model: String,
    pub reasoning_effort: Option<Effort>,
}
pub enum Effort { Medium, High, XHigh }

pub struct ModelDescriptor {
    pub provider: String,
    pub model: String,
    pub context_window: u32,                 // drives the context gauge (§10.3)
    pub supports_reasoning_effort: bool,     // drives effort-selector visibility (§8.5)
    pub display_name: Option<String>,
}

pub struct TokenUsage { pub input: u64, pub output: u64, pub total: u64 }

// ---- User input ----
pub enum UserInput {
    Text { text: String },
    // NOTE: image/file attachments are NOT in v1 scope. If added later, extend here.
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
    Timed(String),            // operation timed out
}
```

> `AgentEventStream` is `impl Stream<Item = AgentEvent> + Send` (concretely a wrapper over a
> `tokio::sync::broadcast::Receiver<AgentEvent>`), supporting multiple subscribers per thread.

### 4.6 Codex mapping (informative)

The `CodexHarness` maps the Codex app-server JSON-RPC protocol onto the above. Key mappings
(protocol details in [Appendix A](#appendix-a-codex-app-server-mapping-reference)):

| Codex app-server | Giskard |
|------------------|---------|
| `initialize` + `initialized` handshake | done inside `open`/process spawn |
| `thread/start`, `thread/resume` | `open_thread` |
| `turn/start` (with model/effort/sandbox per turn) | `start_turn` + `TurnOverrides` |
| `item/started`, `item/*/delta`, `item/completed` | `ItemStarted` / `ItemDelta` / `ItemCompleted` |
| `turn/diff/updated` | `DiffUpdated` |
| `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval` | `ApprovalRequested` |
| `turn/completed` (token usage) | `TurnCompleted` |
| `turn/interrupt` | `interrupt` |
| JSON-RPC error `-32001` "overloaded" | retry with exponential backoff + jitter, surfaced as transient `Error` only if retries exhausted |

Plan vs build maps to the Codex per-turn sandbox policy: **Plan → `read-only`**,
**Build → `workspace-write`**. Approval policy maps to Codex's approval configuration (§9).

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
├── projects.json               # index of projects (id, name, dir, created_at, order)
├── projects/
│   └── <project_id>/
│       ├── project.json        # ProjectConfig: workspace root, default model,
│       │                       #   approval policy, provider defaults, harness kind
│       ├── threads/
│       │   ├── <thread_id>.json        # ThreadState + ordered turns/items (history)
│       │   └── <thread_id>.jsonl       # optional append-only event log (see §5.4)
│       └── tokens.json         # per-project token ledger (aggregates + daily buckets)
└── tokens-global.json          # global token ledger (daily/weekly/monthly/total)
```

- **IDs** are ULIDs (sortable, timestamp-prefixed) rendered as strings. Filenames use the ID.
- **`projects.json`** is the small, frequently-read index. Individual project/thread files
  hold the bulk, so no single giant file must be parsed to render the project list.

### 5.3 Core persisted types (serde JSON)

All defined in `giskard-core`, serialized by `giskard-persist`. Illustrative shapes:

```jsonc
// projects.json
{
  "version": 1,
  "projects": [
    { "id": "01J…", "name": "ostinato-radio", "dir": "/home/elie/dev/ostinato-radio",
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
  "dir": "/home/elie/dev/ostinato-radio",
  "harness": "codex",
  "workspace_root": null,               // null ⇒ defaults to `dir`
  "default_model": { "provider": "openai", "model": "gpt-5.5", "reasoning_effort": "high" },
  "approval_policy": "ask",             // "ask" | "auto" | "read_only" (§9)
  "created_at": "…", "updated_at": "…"
}
```

```jsonc
// projects/<id>/threads/<thread_id>.json
{
  "version": 1,
  "id": "01J…",
  "project_id": "01J…",
  "title": "Fix Qobuz OAuth refresh",
  "harness_thread_id": "th_abc123",     // native id used for resume
  "mode": "build",                       // "plan" | "build"
  "current_model": { "provider": "openai", "model": "gpt-5.5", "reasoning_effort": "high" },
  "context_window": 262144,              // tokens; from active model descriptor
  "tokens": {
    "total": { "input": 12000, "output": 3400, "total": 15400 },
    "by_model": {                        // per-(provider/model) breakdown (§10.2)
      "openai/gpt-5.5": { "input": 12000, "output": 3400, "total": 15400 }
    }
  },
  "created_at": "…", "updated_at": "…",
  "turns": [ /* ordered Turn objects, each holding its completed Items */ ]
}
```

> The thread `tokens` object carries both the aggregate (`total`) **and** the per-model
> breakdown (`by_model`), matching §10.2. A thread accumulates a distinct `by_model` entry
> whenever its model changes mid-thread (§8.4).

```jsonc
// projects/<id>/tokens.json  and  tokens-global.json
{
  "version": 1,
  "total": { "input": 0, "output": 0, "total": 0 },
  "by_day":   { "2026-07-06": { "input": …, "output": …, "total": … } },
  "by_model": { "openai/gpt-5.5": { "input": …, "output": …, "total": … } }
}
```
Weekly/monthly aggregates are **derived on read** from `by_day` (no separate storage), so
there is one source of truth to correct if needed.

### 5.4 Write strategy & durability

- **In-memory authoritative state** per running server; disk is the durable mirror.
- Every mutating operation updates memory, then persists the affected file(s) via
  **atomic replace**: write to `<file>.tmp-<rand>`, `fsync`, `rename` over the target.
- **Per-file async mutex** (or an actor owning each file) guarantees single-writer; the
  ~10-thread scale makes contention negligible.
- **`tokens-global.json` is a cross-project hot file** (every `TurnCompleted` in any project
  updates it). It is owned by a **single dedicated ledger actor** (one Tokio task holding the
  in-memory global ledger); all projects send `TokenUsage` deltas to it over an `mpsc`
  channel, and it serializes the atomic writes. This avoids multi-writer races on the one
  shared file without a global lock, and it batches rapid updates (coalesce N deltas arriving
  close together into one atomic write). The same actor owns per-project `tokens.json` writes,
  or those may be delegated to per-project sub-tasks — either is acceptable, but the global
  file must have exactly one writer.
- **Turn history growth:** a thread's history lives in its `<thread_id>.json`. To bound
  rewrite cost, completed turns are appended and the file is rewritten atomically on each
  turn completion (acceptable at this scale). The optional `<thread_id>.jsonl` append-only
  event log (raw `AgentEvent`s) is written for debugging/replay-recording and is **not** the
  authoritative store; it can be truncated/deleted safely.
- **Crash consistency:** because writes are atomic renames, a crash leaves either the old or
  the new complete file, never a partial one. On startup the server validates each JSON file;
  a corrupt file is moved aside to `<file>.corrupt-<ts>` and logged, and the app continues
  with the rest (a single bad thread never blocks the whole app).

### 5.5 Debug / maintenance surface

Because the store is plain files, the primary debugging tool is the filesystem itself
(inspect with `jq`, delete a thread by removing its file). In addition, `giskard-persist`
exposes a small maintenance API used by a `giskard-admin` binary (or hidden UI panel):

- `list_projects`, `list_threads(project)`, `dump_thread(id)` (pretty JSON to stdout),
- `delete_thread(id)`, `delete_project(id)` (with confirmation),
- `validate_all` (parse every file, report corruption),
- `compact_thread(id)` (rewrite/prune the jsonl log).

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
→ optionally sets workspace root, default model, and approval policy → confirm.

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

- **Create:** user starts a new thread in a project; server calls `open_thread` (Codex
  `thread/start`), stores the returned `harness_thread_id`, writes `<thread_id>.json`.
- **Send input:** user submits a message; server calls `start_turn` with the thread's current
  mode/model/effort as `TurnOverrides`. A turn begins.
- **Stream:** `AgentEvent`s flow to the UI (§13.6) and update persisted state.
- **Complete:** on `TurnCompleted`, token usage is folded into the ledgers (§10) and the
  thread file is rewritten atomically.
- **Resume (after restart):** on startup or first access, `open_thread` with the stored
  `harness_thread_id` (Codex `thread/resume`) rehydrates the native session; Giskard already
  holds the display history from disk.
- **Interrupt:** user can interrupt an in-flight turn (`turn/interrupt`).
- **Rename / delete:** thread title editable; delete removes the thread file (§5.5).

### 7.2 Titles

Auto-generate an initial title from the first user message (truncated); user-editable.
(Optional enhancement: ask the harness to summarize; not required for v1.)

### 7.3 Streaming semantics

- Agent message text arrives as `ItemDelta`s; the UI appends incrementally.
- Command executions stream stdout/stderr as `ItemDelta`s under a command item.
- Reasoning notes (if the model/effort emits them) render in a collapsible "thinking" block.
- Each item ends with `ItemCompleted` carrying its final, canonical form (this is what gets
  persisted; deltas are transient).

### 7.4 Plan / Build modes

- **Mode is thread state**, persisted, and **switchable at any time within the thread**
  (requires `capabilities.plan_build_modes`).
- **Plan mode** ⇒ harness runs **read-only**; the agent analyzes and proposes an
  implementation plan without modifying files.
- **Build mode** ⇒ harness runs **workspace-write**; the agent implements, subject to the
  approval policy (§9).
- The mode applied to a turn is the thread's mode **at the moment `start_turn` is called**
  (Codex takes sandbox per turn). Switching mode takes effect on the next turn; the UI makes
  this explicit ("Plan mode — next message will be read-only").
- **Switching back and forth** is fully supported (Plan → Build → Plan …).

#### 7.4.1 Plan dump to markdown

- A **"Save plan to project"** button is available while in (or after) Plan mode.
- It writes the current plan as a markdown file **into the project directory**. Default path:
  `docs/plan-<thread-title-slug>-<YYYYMMDD-HHmm>.md` (configurable default in `config.toml`;
  the user may edit the path in a small dialog before saving). If `docs/` doesn't exist it is
  created.
- **What constitutes "the current plan":** the concatenation of the agent-message items of
  the **most recent Plan-mode turn** in the thread (i.e. the latest plan the agent produced),
  rendered to markdown. If multiple plan turns exist, the latest wins; the dialog shows a
  preview so the user can confirm. (Rationale: simplest unambiguous rule; avoids trying to
  detect "the plan" heuristically across the whole thread.)
- Writing the plan file is a normal file write within the workspace root; it is **not** gated
  by the agent approval flow (it's a user action, not an agent action), but it respects the
  workspace-root boundary.
- After saving, the UI links the new file (openable in the code overlay, §11.2).

### 7.5 `TurnOverrides`

```rust
pub struct TurnOverrides {
    pub model: Option<ModelRef>,          // per-turn model (change between turns)
    pub reasoning_effort: Option<Effort>, // medium | high | xhigh (model-dependent)
    pub mode: Mode,                       // plan | build → sandbox policy
    pub approval_policy: ApprovalPolicy,  // from project config, overridable per turn
}
```

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

Providers are declared in `config.toml` (§16.3). Each declares: `id`, display `name`,
`base_url`, auth reference, `wire_api` (`responses`/`chat`), and whether it exposes a model
list endpoint. Example providers relevant here: OpenAI direct (Codex's built-in), and a
LiteLLM gateway fronting Cloudflare Workers AI.

> Note: Codex itself reads its own `~/.codex/config.toml` for provider/auth (Codex is
> "already configured", §12.2). Giskard's provider config governs (a) what the UI offers in
> the model picker and (b) optional `/v1/models` discovery. The `ModelRef` Giskard sends as a
> per-turn override must correspond to a model/provider Codex is configured to reach.

### 8.3 Model list: static + dynamic

- A **static list** in config is always available (works offline, deterministic for tests).
  Each static entry is a **typed model definition**, not a bare string, so it can supply the
  `ModelDescriptor` fields the UI needs (see the `[[providers.models]]` tables in Appendix C):

  ```toml
  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
  ```
- If a provider advertises `model_listing` and exposes `GET /v1/models`, Giskard can **refresh
  the list dynamically** and merge results with the static list. A manual "refresh models"
  action triggers this; results are cached in memory (and optionally written back into the
  provider's config section) with a timestamp.
- Each model entry resolves to a `ModelDescriptor { provider, model, context_window,
  supports_reasoning_effort, display_name }`. `context_window` drives the thread context gauge
  (§10.3); `supports_reasoning_effort` drives whether the effort selector is shown (§8.5).
- **Metadata source precedence** (first hit wins):
  1. the typed `[[providers.models]]` entry in config;
  2. the `/v1/models` response, **if** it includes the field (many OpenAI-compatible
     endpoints, including LiteLLM, return `context_window`/`max_input_tokens` and capability
     hints — use them when present);
  3. a built-in **defaults table** in `giskard-core` keyed by well-known model ids;
  4. a **conservative fallback** (`context_window = 128000`, `supports_reasoning_effort =
     false`) with a UI warning badge ("context size unknown — using default"), so an unknown
     model is still usable and the gauge still renders.

### 8.4 Changing model within a thread

- Supported and expected. Selecting a different model updates the thread's `current_model`;
  it takes effect on the **next turn** (Codex accepts model per `turn/start`). This satisfies
  "change model during a thread".
- When the model changes, the thread's `context_window` is updated from the new model's
  descriptor and the context gauge (§10.3) recomputes.

### 8.5 Reasoning effort

- Effort (medium/high/xhigh) is selectable **only when the chosen model supports it**
  (`supports_reasoning_effort`); otherwise the selector is hidden and no effort param is sent
  (avoids sending unsupported parameters).

---

## 9. Approvals & Permissions

> "Permissions" here = **agent action approvals**, not user roles. There is exactly one user.

### 9.1 Policy per project (overridable per turn)

`ApprovalPolicy` (stored in `project.json`, overridable in `TurnOverrides`):

- **`read_only`** — strictly no writes/exec (natural companion to Plan mode).
- **`ask`** — the agent must request approval for each command execution and file change;
  the UI prompts the user.
- **`auto`** — approvals are granted automatically (full-auto within the workspace sandbox).

**Interaction with Plan mode.** Mode (Plan/Build) and approval policy are **orthogonal
settings**, but Plan mode makes the policy largely moot: in Plan mode the harness sandbox is
`read-only`, so the agent cannot perform the write/exec actions that trigger approvals in the
first place. Therefore in Plan mode an `ask` policy will, in practice, rarely (if ever) raise a
prompt, and `auto` grants nothing meaningful. The UI reflects this: while a thread is in Plan
mode it shows the policy as "not applicable (read-only)" without changing the stored value, so
switching back to Build restores the previously chosen policy. Plan mode does **not** overwrite
the project's `approval_policy`.

### 9.2 Live approval flow (requires `capabilities.live_approvals`)

1. Harness pushes an approval request (command exec / file change / permission escalation);
   `CodexHarness` maps it to `AgentEvent::ApprovalRequested` with the details (command, cwd,
   reason, target path, and the set of available decisions).
2. UI shows a non-blocking prompt scoped to the thread (with the command/diff preview).
3. User chooses a decision; server calls `respond_approval`.

**Decision granularity** (mirrors Codex):
- `accept` (this once), `accept_for_session` (don't ask again this session for this kind),
  `decline`, `cancel`. For command exec, an optional "accept with amended exec policy" may be
  offered if the harness provides it.

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

If the active harness lacks `live_approvals`, the UI hides live prompts and the project must
run in `auto` or `read_only` (the picker disables `ask`). This keeps the experience coherent
across harnesses.


---

## 10. Token Tracking

### 10.1 Sources

Token usage comes from `TurnCompleted` (Codex reports usage on `turn/completed`). Each turn
contributes `{ input, output, total }` tagged with the `(provider, model)` used for that turn.

### 10.2 Aggregation levels

Recorded and viewable at:

- **Thread** — running totals in `<thread_id>.json` (`tokens`), plus per-model breakdown.
- **Project** — `projects/<id>/tokens.json`: `total`, `by_day`, `by_model`.
- **Global** — `tokens-global.json`: `total`, `by_day`, `by_model`.

**Time windows** for the global (and project) views: **day / week / month / total**. Weekly
and monthly figures are derived on read by summing `by_day` buckets (single source of truth,
§5.3). A dashboard renders these as tables and simple charts.

### 10.3 Context-window gauge (per thread)

Within a thread, show the thread's cumulative token footprint **relative to the active
model's context window** (e.g. 15.4k / 262k, or / 1M). The denominator is
`ModelDescriptor.context_window` for the current model and **recomputes when the model
changes** (§8.4). This is a usage-vs-capacity indicator to warn before hitting context limits.

> **Codex source field.** "Tokens used in the thread" for the gauge should reflect the current
> conversation's *context occupancy*, which is not the same as cumulative billed tokens
> (cumulative usage keeps growing across turns; context occupancy reflects what's currently in
> the window after any compaction). Codex reports usage on `turn/completed`; the token-usage
> payload distinguishes cumulative totals from the last turn's usage and typically exposes an
> input-tokens / context-used figure per turn. **Candidate fields to use (in order):**
> (1) an explicit context/window-used field on the turn's usage object if present in the
> pinned schema; (2) otherwise the **last turn's input tokens** (the input side reflects the
> context sent to the model, which is the best available proxy for occupancy); (3) fall back
> to cumulative `total` only if neither is available. The implementer must inspect the pinned
> `codex app-server generate-json-schema` output for the exact field name on the
> `turn/completed` usage object, pick per the order above, and record the choice in a code
> comment + the README. Whichever is chosen, the gauge denominator is the active model's
> `context_window` (§8.3).

### 10.4 Optional cost estimation

Optional (config-gated): per-model € rates in `config.toml` produce an estimated spend
alongside raw token counts. Off by default; raw token counts are the primary metric.

---

## 11. Visualization: Diffs & Code Overlay

### 11.1 Diff viewer (side-by-side)

- **Visualization only** in v1 (no accept/reject of individual hunks, no git ops).
- Fed by `AgentEvent::DiffUpdated` (Codex `turn/diff/updated`), per file.
- Rendered **side-by-side, in a panel next to the thread** on desktop. On mobile it becomes a
  full-screen tab/drawer (unified inline diff if side-by-side doesn't fit; §13.4).
- Shows the set of files changed in the current turn, selectable; each file shows old vs new
  with additions/deletions highlighted. Large diffs are virtualized (§11.3).

### 11.2 Code overlay for referenced paths

- When an agent message (or command output) mentions a **filesystem path** within the
  workspace, Giskard makes it a clickable link. Clicking opens an **overlay/panel** showing
  that file.
- **Server-side syntax highlighting** with `syntect`: the backend reads the file from the
  project filesystem, highlights based on extension/first line, and returns highlighted HTML
  (plus raw text for download). The WASM client renders the HTML; no JS highlighter, no npm.
- The overlay provides a **"Download file"** action (streams the raw file) and shows the
  file's path, size, and language.
- **Path detection:** a server-side (or shared) linkifier scans agent text for path-like
  tokens and resolves them against the workspace root. Only paths that (a) resolve inside the
  allowed scope and (b) exist are linkified. Ambiguous/relative paths are resolved relative to
  the workspace root. This detection runs when an `ItemCompleted` agent message is finalized
  (not on every delta) for efficiency.

### 11.3 Large files & performance

- Files above a configurable size threshold are highlighted/served in **paginated chunks**
  (line ranges) and the viewer virtualizes rendering (only visible lines in the DOM).
- Highlighting is cached per (path, mtime) in memory to avoid recomputing on repeat opens.
- Binary/non-text files are detected and shown as "binary — download only".

---

## 12. Authentication

### 12.1 App auth (single shared password)

- **One shared password** gates the whole app. No accounts, no roles, no 2FA (v1).
- The password is stored as an **Argon2 hash** in `config.toml` (or an env var
  `GISKARD_PASSWORD_HASH`), never in plaintext. A `giskard-admin set-password` command
  generates the hash.
- **Login:** a POST verifies the password against the hash and issues a **signed session
  cookie** (HMAC-signed, `HttpOnly`, `SameSite=Strict`, `Secure`). Session lifetime is
  configurable (default 30 days, sliding). A signing key is generated on first run and stored
  in the data dir (`session.key`, 0600).
- **All routes except the login page and static assets require a valid session.** The
  WebSocket upgrade also validates the session cookie before accepting.
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
- **Structure encodes state,** not decoration: mode (Plan/Build), model, and approval policy
  are always visible and legible at a glance in the thread header; a running turn has a clear
  live indicator.
- **Signature element:** the **thread transcript** treated as a first-class typed document —
  agent text, collapsible reasoning, command blocks with streamed output, and inline
  linkified paths — paired with the **context-window gauge** as a persistent, honest read on
  "how full is this conversation." That gauge + linkified transcript is what makes Giskard
  feel purpose-built rather than a generic chat wrapper.
- Copy is plain and action-named (§ frontend writing guidance): buttons say exactly what
  happens ("Save plan to project", "Switch to Build", "Interrupt"). Empty states invite
  action ("No projects yet — create one to start.").

> The design plan above is a starting brief for the implementer, not a locked visual spec.
> The implementer should produce a small token system (4–6 named colors, the 2–3 typefaces,
> spacing scale) and iterate, keeping the restraint principle and the two non-negotiables:
> monospace for code/paths, and the always-visible mode/model/gauge state.

### 13.3 Primary layout (desktop / laptop)

```
┌───────────┬────────────────────────────────────┬────────────────────┐
│ Projects  │  Thread header: mode · model ·      │  Context panel     │
│ + threads │  approval · context gauge · actions │  (tabs):           │
│ (sidebar) ├────────────────────────────────────┤  • Diffs (side-by- │
│           │                                     │    side)           │
│  proj A   │  Transcript (streamed items,        │  • Code overlay    │
│   ├ th 1  │  linkified paths, collapsible        │    (syntect HTML)  │
│   └ th 2  │  reasoning, command output, diffs)  │  • Tokens          │
│  proj B   │                                     │                    │
│   └ th 3  │                                     │                    │
│           ├────────────────────────────────────┤                    │
│  [+ proj] │  Composer: input · mode toggle ·    │                    │
│           │  model picker · send/interrupt      │                    │
└───────────┴────────────────────────────────────┴────────────────────┘
```

- **Left sidebar:** projects with their threads (collapsible), token summary entry point,
  "new project" action.
- **Center:** thread header (mode, model, approval policy, context gauge, plan-dump &
  interrupt actions) + transcript + composer.
- **Right context panel:** tabbed — **Diffs** (side-by-side), **Code overlay**, **Tokens**.

### 13.4 Responsive (smartphone)

- The three columns collapse into a **single-column, tab/drawer navigation**:
  - Bottom (or top) nav switches between **Threads list**, **Transcript**, **Context panel**.
  - The right-panel tabs (Diffs / Code / Tokens) become a secondary switcher within the
    Context view.
  - Side-by-side diffs fall back to **unified inline** diffs when width is insufficient.
- Composer stays pinned to the bottom on the Transcript view. Approval prompts appear as a
  bottom sheet.
- Touch targets ≥ 44px; the app is usable one-handed for the common loop (read → approve →
  send).

### 13.5 Capability-driven UI

The UI reads `HarnessCapabilities` for the active harness and adapts:

- No `live_approvals` ⇒ hide approval prompts; approval-policy picker offers only
  `auto`/`read_only`.
- No `plan_build_modes` ⇒ hide the Plan/Build toggle (thread is single-mode).
- No `per_turn_model` ⇒ model is fixed at thread creation (picker disabled mid-thread).
- No `reasoning_effort` or model doesn't support it ⇒ hide the effort selector.
- No `structured_diffs` ⇒ hide the Diffs tab (or show a plain textual change summary).

This guarantees a coherent experience when a future, less-capable harness is plugged in.

### 13.6 Client ↔ server protocol (single multiplexed WebSocket)

- **One WebSocket per browser client**, multiplexing all projects/threads (chosen for lowest
  CPU/memory: one connection, one server-side fan-out task, no per-thread sockets).
- Messages are tagged with `project_id` / `thread_id`. Defined once in `giskard-proto`.

**Client → server** (examples): `Subscribe { thread_id }`, `Unsubscribe { thread_id }`,
`SendInput { thread_id, text }`, `SwitchMode { thread_id, mode }`,
`SelectModel { thread_id, model_ref }`, `Interrupt { thread_id }`,
`ApprovalDecision { request_id, decision }`, `SavePlan { thread_id, path }`.

> **`SendInput` carries text only in v1.** Image/file attachments are out of scope (matching
> `UserInput` in §4.5). If added later, extend both `UserInput` and this message together.

**Server → client** (examples): `Event { thread_id, agent_event }` (a serialized
`AgentEvent`), `ThreadState { thread_id, state }` (persisted snapshot on subscribe/resync),
`LiveTurnSnapshot { thread_id, turn_id, accumulated, pending_approval? }` (in-flight turn
reconstruction on reconnect, per the resync policy above), `TokenUpdate { scope, ledger }`,
`ApprovalRequest { thread_id, request }`, `Error { … }`, `Pong`.

- **Fan-out:** the server keeps `thread_id → set<client_conn>`. An `AgentEvent` is serialized
  once and sent only to subscribed clients. Background threads keep producing events; a client
  that isn't subscribed to a thread still receives lightweight `ThreadState`/badge updates so
  the sidebar shows activity, but not the full delta stream (bandwidth control).
- **Backpressure:** per-connection bounded queue; if a client falls behind, coalesce deltas
  (keep latest) rather than unbounded buffering. Heartbeat ping/pong; auto-reconnect on the
  client with resubscribe + state resync.
- **Reconnect & live-turn resync.** Persisted thread state only advances on `TurnCompleted`
  (§5.4), so a client that reconnects **during** an in-flight turn cannot reconstruct the live
  turn from disk alone. To close this gap the server keeps, **per active turn**, an in-memory
  **live buffer**: the ordered `AgentEvent`s accumulated since `TurnStarted` (agent-text so
  far, command output so far, pending approval requests, current diffs). This buffer is
  discarded on `TurnCompleted` (the finalized items are then on disk). On `Subscribe`, the
  server sends a **snapshot** first:
  1. the persisted thread state (all completed turns) from disk, then
  2. if a turn is currently live, a `LiveTurnSnapshot` reconstructed from the live buffer
     (accumulated text/output + any pending approval), then
  3. subsequent deltas as normal.

  This means a reconnected client sees the full in-progress turn, including a still-pending
  approval prompt (so approvals are never lost across a reconnect). The live buffer is bounded
  (coalesced/truncated for very long command output, keeping head+tail) to cap memory; the
  authoritative full output still lands on disk at `TurnCompleted`.
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
- `giskard-persist`: atomic-write behavior, corruption quarantine, load/round-trip fidelity,
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
  ledgers update correctly; approval requests surface and decisions are routed; plan dump
  writes the expected markdown; diffs are parsed and exposed; mode/model switches take effect
  on the right turn.
- **Determinism:** replay advances on demand (test drives the clock/step), so assertions are
  stable. The same fixtures double as a "demo mode" for the app without a real harness.

### 14.3 End-to-end (headless browser)

- A small **headless-browser** suite (e.g. via a WebDriver/Chromium-headless runner invoked
  from Rust) guards critical flows against regression: login, create project (with the file
  picker), open thread, send a message (backed by `ReplayHarness`), see streamed transcript,
  receive and answer an approval prompt, view a diff, open a code overlay, switch mode/model,
  observe token/context updates. Runs in CI headless.
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

**Phase 3 — Modes, models, approvals.** Plan/Build toggle + per-turn sandbox mapping; plan
dump to markdown; model picker (static list) + per-turn model change + reasoning effort;
approval policy + live approval prompts + decision routing. Tests for each via replay.

**Phase 4 — Visualization.** Side-by-side diff viewer from `DiffUpdated`; path linkification;
code overlay with `syntect` highlighting + download; large-file virtualization/pagination.

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

> Sandbox/mode mapping: **Plan ⇒ `read-only`**, **Build ⇒ `workspace-write`**. Approval policy
> maps to Codex's approval configuration. `TurnOverrides.model` / `reasoning_effort` map to the
> per-turn `turn/start` model & effort fields.

**Client library:** use `codex-codes` (v0.143.0, tested against Codex CLI 0.143.0) with the
`async-client` feature — its `AsyncClient` API (`start`, `thread_start`, `turn_start`,
`next_message`, `respond`, `shutdown`) maps directly onto the `AgentHarness` trait. Its built-in
schema coverage scorecard validates typed structs against `codex app-server generate-json-schema`
output and can be wired into the CI drift check (§14.4). Fall back to `codex-app-server-sdk`
(v0.5.1) or a hand-rolled client only if a future Codex CLI version diverges beyond what
`codex-codes` tracks. Whichever is chosen, confine all Codex types to `giskard-harness-codex` and
preserve the raw-JSON fallback for unknown/drifted messages.

### Appendix B — Example client↔server WebSocket messages

```jsonc
// client → server
{ "type": "SendInput", "thread_id": "01J…", "text": "Refactor the auth module" }
{ "type": "SwitchMode", "thread_id": "01J…", "mode": "build" }
{ "type": "SelectModel", "thread_id": "01J…",
  "model_ref": { "provider": "cloudflare-litellm", "model": "@cf/z-ai/glm-4.7",
                 "reasoning_effort": null } }
{ "type": "ApprovalDecision", "request_id": "ap_7", "decision": "accept_for_session" }
{ "type": "SavePlan", "thread_id": "01J…", "path": "docs/plan-auth-20260706-1030.md" }

// server → client
{ "type": "Event", "thread_id": "01J…",
  "agent_event": { "kind": "ItemDelta", "item_id": "it_3",
                   "delta": { "text": "I'll start by reading auth.rs…" } } }
{ "type": "ApprovalRequest", "thread_id": "01J…",
  "request": { "id": "ap_7", "kind": "command_execution",
               "command": "cargo test", "cwd": "/home/elie/dev/x",
               "decisions": ["accept","accept_for_session","decline","cancel"] } }
{ "type": "Event", "thread_id": "01J…",
  "agent_event": { "kind": "TurnCompleted",
                   "usage": { "input": 1200, "output": 340, "total": 1540 },
                   "status": "completed" } }
```

### Appendix C — Configuration reference (`config.toml`)

```toml
# ${XDG_DATA_HOME:-~/.local/share}/giskard/config.toml   (path overridable via GISKARD_DATA_DIR)

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
roots = []                     # e.g. ["/home/elie/dev"]

[plan]
default_dir = "docs"           # where "Save plan to project" writes
filename_template = "plan-{slug}-{ts}.md"

[tokens]
cost_estimation = false
# [tokens.rates."openai/gpt-5.5"]  input_per_mtok_eur = …  output_per_mtok_eur = …

[[providers]]
id = "openai"
name = "OpenAI (Codex built-in)"
wire_api = "responses"
model_listing = false
  # typed model entries carry the metadata the UI needs (§8.3):
  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
  [[providers.models]]
  id = "gpt-5.4"
  display_name = "GPT-5.4"
  context_window = 262144
  supports_reasoning_effort = true

[[providers]]
id = "cloudflare-litellm"
name = "Cloudflare Workers AI (via LiteLLM)"
base_url = "http://127.0.0.1:4000/v1"
wire_api = "responses"          # LiteLLM bridges /responses → /chat/completions
model_listing = true            # GET /v1/models available; merged over static entries
  [[providers.models]]          # static fallback; dynamic listing may add/refine
  id = "@cf/z-ai/glm-4.7"
  display_name = "GLM-4.7 (Workers AI)"
  context_window = 131072
  supports_reasoning_effort = false

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
4. **Plan-content extraction rule** — spec defaults to "latest Plan-mode turn's agent
   messages" with a preview before saving (§7.4.1); confirm this matches intent in practice.
5. **Headless-browser runner choice** — pick a Rust-drivable headless option for §14.3 that
   introduces no npm dependency.

---

*End of specification.*
