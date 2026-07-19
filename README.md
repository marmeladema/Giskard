# Giskard

[![CI](https://github.com/marmeladema/Giskard/actions/workflows/ci.yml/badge.svg)](https://github.com/marmeladema/Giskard/actions/workflows/ci.yml)

A **local-first, single-user web UI** on top of agentic coding CLIs. Giskard runs on your machine,
manages projects and durable conversation threads, streams the agent's work to a browser in real
time, visualizes file diffs and referenced source, and tracks token usage. The first (and current)
agent harness is OpenAI's **Codex CLI**, spoken over its `app-server` JSON-RPC protocol.

Built entirely in **Rust** (Axum backend + a self-contained web frontend). No npm/Node/JS toolchain
in the build.

> The authoritative design document is [`specs/giskard-specification.md`](specs/giskard-specification.md).
> This README is the practical setup/usage guide and must be kept in sync with the code (see
> [AGENTS.md](AGENTS.md)).

---

## Prerequisites

- **Rust** — edition 2024, MSRV **1.85+** (`rustup` recommended).
- **Codex CLI**, already installed and authenticated on the machine. Giskard does **not** manage
  Codex's credentials — it inherits `~/.codex` (ChatGPT login or an API key / custom provider) when
  it spawns the app-server. If Codex isn't configured, turns will fail with an "unauthenticated"
  message. See [§12.2 of the spec](specs/giskard-specification.md).

Giskard spawns one `codex app-server` process per project; each project is bound to a filesystem
directory that becomes the agent's sandbox/workspace.

---

## Installation

The repository root is a virtual Cargo workspace, so `cargo install --path .` is not valid. Install
the two binaries from their package manifests instead:

```bash
cargo install --locked --path crates/giskard-server --bin giskard-server
cargo install --locked --path crates/giskard-persist --bin giskard-admin
```

Cargo installs binaries into `~/.cargo/bin` by default; make sure that directory is on `PATH`.
`giskard-server` serves the browser app, while `giskard-admin` manages passwords and stored data.

If you prefer not to install, run the same binaries from the checkout with `cargo run` as shown in
the quick start below.

---

## Quick start

With the installed binaries:

```bash
# 1. Choose a data directory (holds config, projects, threads, token ledgers).
#    Defaults to ~/.local/share/giskard; override with GISKARD_DATA_DIR.
export GISKARD_DATA_DIR="$HOME/.local/share/giskard"
mkdir -p "$GISKARD_DATA_DIR"

# 2. Set the app password (prints an Argon2 hash to paste into config.toml).
giskard-admin set-password

# 3. Create the config from the annotated example and edit it (paste the hash;
#    keep secure_cookies = false for plain-HTTP localhost).
cp config.example.toml "$GISKARD_DATA_DIR/config.toml"
$EDITOR "$GISKARD_DATA_DIR/config.toml"

# 4. Run the server.
giskard-server
```

From the checkout without installing, use the package binaries explicitly:

```bash
cargo run -p giskard-persist --bin giskard-admin -- set-password
cargo run --release -p giskard-server --bin giskard-server
```

Then open **http://127.0.0.1:8787**, log in, and:

1. **+** next to *Projects* → name it and give an **absolute directory path** that exists on the
   server machine (the agent's workspace).
2. **+** on the project → draft a new thread. No Codex thread is created until the first message is
   sent, so choose the **Plan/Build** mode, **approval policy**, and **model** first if needed.
3. Type in the composer (Enter to send). The first send creates the Codex thread with the selected
   provider/model and starts the turn. Existing threads show the **Tasks** menu for running
   commands/tools, **MCP** status menu, and **Context** usage button; scrolling the transcript to
   the top lazy-loads older history.

The header context value is a context-window indicator, not a billing total. Codex currently exposes
the latest turn's input tokens rather than a dedicated context-occupancy field, so Giskard uses that
as the best available proxy for "how full is the active conversation?" Clicking **Context** opens a
card with both the current context footprint and cumulative input/output/total tokens. Those
cumulative totals can legitimately exceed the model's context window over a long thread.

> **Common gotcha:** with `secure_cookies = true` over plain HTTP, the browser drops the session
> cookie — login appears to succeed but nothing loads. Use `false` for local HTTP; set `true` only
> behind HTTPS/TLS (e.g. an Nginx terminator).

---

## Logging

`giskard-server` logs to the server process output using Rust's standard `RUST_LOG` filter syntax.
When `RUST_LOG` is unset, the server defaults to:

```bash
giskard=info,tower_http=info
```

For normal debugging, start the server with Giskard logs at `debug`:

```bash
RUST_LOG=giskard=debug,tower_http=info giskard-server
```

From a checkout:

```bash
RUST_LOG=giskard=debug,tower_http=info \
  cargo run --release -p giskard-server --bin giskard-server
```

For verbose turn-lifecycle, Codex harness, and HTTP request diagnostics, use `trace` selectively:

```bash
RUST_LOG=giskard=trace,giskard_harness_codex=trace,tower_http=debug giskard-server
```

If the output is too noisy, scope logging to the area being diagnosed. For example, this focuses on
thread turn ownership and Codex harness events while keeping the rest of Giskard at `info`:

```bash
RUST_LOG=giskard_server::registry=trace,giskard_harness_codex=trace,giskard=info,tower_http=info \
  giskard-server
```

Use `debug` first for most issues. `trace` can be very verbose, but it is useful when diagnosing
stuck turns, harness protocol failures, WebSocket forwarding, or command/tool lifecycle bugs.

For browser-side issues, open Settings → **Browser diagnostics** in the Giskard UI. The panel keeps
a bounded local buffer of recent WebSocket status changes, notification lifecycle events, approval
routing decisions, and visibility/focus state. Use **Copy** from that panel when reporting a
browser-only problem; **Test notification** verifies the browser/OS notification path without
waiting for an approval request.

---

## Configuration

Config lives at `${GISKARD_DATA_DIR:-~/.local/share/giskard}/config.toml`. A fully annotated,
copy-pasteable template is in [`config.example.toml`](config.example.toml). Every section is
optional and falls back to the defaults below, but the `config.toml` file itself must exist:
`giskard-server` refuses to start when it is missing, unreadable, or invalid so a mis-pointed
service does not silently run with an empty provider list.

| Section | Key | Default | Purpose |
|---------|-----|---------|---------|
| `[server]` | `bind` | `127.0.0.1:8787` | HTTP/WS listen address. |
| | `secure_cookies` | `true` | `Secure` flag on the session cookie. **Set `false` for plain-HTTP local dev.** |
| `[auth]` | `password_hash` | — | Argon2 hash of the shared password (or env `GISKARD_PASSWORD_HASH`). Generate with `giskard-admin set-password`. |
| | `session_days` | `30` | Session lifetime, sliding: requests in the second half of the window re-issue the cookie for a full window. |
| `[browse]` | `roots` | `[]` (whole FS) | Confine the filesystem picker **and project creation** to these absolute subtrees (see [Security](#security)). |
| `[plan]` | `default_dir` / `filename_template` | `docs` / `plan-{slug}-{ts}.md` | Where "Save plan to project" writes. |
| `[tokens]` | `cost_estimation` | `false` | Show an estimated € cost from `[tokens.rates."provider/model"]`. |
| `[viz]` | `max_highlight_size` | `10485760` (10 MiB) | Files larger than this aren't syntax-highlighted. |
| `[history]` | `initial` / `page` | `5` / `50` | Turns fetched on open (topped up client-side to ~2 screens) / per scroll-up page. |
| `[harness]` | `kind` | `codex` | Agent harness (v1: `codex`). |
| | `idle_shutdown_secs` | `0` (keep alive) | Terminate an idle project's harness after N seconds. |
| `[[providers]]` | `id`, `name`, `wire_api`, `base_url?`, `model_listing`, `api_key?` / `api_key_env?`, `[[providers.models]]` | — | What the model picker offers. With `model_listing = true` + `base_url` the picker is refreshed from `GET {base_url}/models` (on load, and via the ↻ button), so `[[providers.models]]` becomes optional. Set `api_key` (inline) or `api_key_env` (env-var name) for endpoints that require auth — sent as `Authorization: Bearer …`. |

Provider config governs the **picker** and optional `/v1/models` discovery only — Codex itself
reads `~/.codex/config.toml` for real provider/auth, so any model you select must be one Codex can
actually reach.

Models with `supports_reasoning_effort = true` expose a thread-header **Effort** selector next to
the model picker. Choose `Model default` to omit the effort parameter, or select a concrete Codex
effort (`Minimal` through `Extra High`) for subsequent turns in that thread.

---

## Security

Read this section before exposing an instance beyond `localhost`. Full details in
[§12 of the spec](specs/giskard-specification.md).

**Threat model in one sentence:** an authenticated client can drive a coding agent (i.e. execute
code) and read/write files inside project workspaces with the server user's privileges — so the
shared password is guarding host access, not just a dashboard. Prefer keeping Giskard on a
private network (VPN/WireGuard/Tailscale). If you do expose it publicly, always front it with a
TLS-terminating reverse proxy, keep `bind` on `127.0.0.1`, set `secure_cookies = true`, and use a
long random password.

What the server enforces itself:

- **Password storage & verification.** The password is only ever stored as an Argon2id hash
  (config or `GISKARD_PASSWORD_HASH`); verification is constant-time.
- **Login throttling.** After a handful of consecutive failures, `/api/login` locks out with
  exponentially increasing windows (up to 15 minutes) and answers `429` + `Retry-After`. The
  check runs *before* the (memory-hard) Argon2 verification, so a password-guessing flood can't
  be used to burn CPU/RAM either. Failed attempts are logged as `login failed: invalid password`
  with the client's `X-Forwarded-For` — a stable line you can point fail2ban at when running
  behind a trusted proxy. The counter is in-memory; restarting the server clears it.
- **Sessions.** The session cookie is an HMAC-signed, stateless token (`HttpOnly`,
  `SameSite=Strict`, `Secure` when `secure_cookies = true`) with a sliding `session_days`
  lifetime. Because it's stateless, **logout only clears the browser cookie** — to actually
  invalidate outstanding sessions (lost laptop, leaked token), run `giskard-admin
  revoke-sessions` and restart the server. Changing the password does *not* invalidate existing
  sessions; rotating the key does.
- **WebSocket tickets.** `GET /api/ws-ticket` mints a 60-second token for the WS upgrade.
  Tickets are cryptographically domain-separated from session cookies: a ticket that leaks via a
  URL (e.g. proxy access logs record `/api/ws?ticket=...`) cannot be replayed as a session
  cookie, and vice versa.
- **Response hardening.** Every response carries a strict `Content-Security-Policy`
  (`script-src 'self'` — the UI has no inline script, so even an HTML-injection bug cannot
  escalate to script execution), `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY` /
  `frame-ancestors 'none'`, `Referrer-Policy: no-referrer`, and same-origin COOP/CORP.
- **Workspace confinement.** File reads (`highlight`/`raw`/`image`), plan writes, and the browse picker
  are confined to each project's workspace root with symlink-resolving canonicalization. When
  `[browse] roots` is set, it also constrains **project creation** — without it, an
  authenticated client can create a project rooted anywhere the server user can read. Set
  `roots` to your development directories on any exposed instance.
- **CSRF.** `SameSite=Strict` cookies plus a same-origin-only API surface (no CORS layer) block
  cross-site request forgery and cross-site WebSocket hijacking in current browsers.

Upgrade note: the session-token format changed when ticket/cookie domain separation was
introduced — everyone is logged out once after upgrading across that change.

---

## Storage layout

Flat files under the data directory (human-readable; inspect with `cat`/`jq`):

```
$GISKARD_DATA_DIR/
├── config.toml                  # this config
├── session.key                  # 32-byte local key for signed browser sessions
├── projects.json                # project index (id, name, dir, created_at, order)
├── projects/<project_id>/
│   ├── project.json             # workspace root, default model, harness kind
│   ├── threads/
│   │   ├── <thread_id>.json      # thread metadata, approval policy, token cache
│   │   └── <thread_id>.jsonl     # authoritative turn history — one Turn per line, append-only
│   └── tokens.json               # per-project token ledger (total, by_day, by_model)
└── tokens-global.json            # cross-project token ledger
```

Thread **history** is the append-only `.jsonl` (source of truth); the `.json` is small metadata +
token aggregates that can be rebuilt from the history. Writes are crash-safe: metadata/ledgers use
atomic temp-file+rename, history appends are single `O_APPEND` writes (a torn final line is skipped
on load).

---

## Admin CLI (`giskard-admin`)

```bash
giskard-admin <command>
```

From the checkout without installing, use
`cargo run -p giskard-persist --bin giskard-admin -- <command>`.

| Command | Description |
|---------|-------------|
| `set-password` | Prompt for a password and print its Argon2 hash. |
| `revoke-sessions` | Rotate the session signing key (`session.key`), invalidating **all** logged-in sessions. Restart `giskard-server` afterwards. |
| `list-projects` | List projects in the data dir. |
| `list-threads <project_id>` | List a project's threads. |
| `dump-thread <project_id> <thread_id>` | Pretty-print a thread's metadata JSON. |
| `delete-thread <project_id> <thread_id>` | Delete a thread (metadata + history). |
| `delete-project <project_id>` | Delete a project and its threads. |
| `validate` | Parse every stored file and report corruption (history is checked line-by-line). |

---

## HTTP / WebSocket API

The browser (and any client) drives everything through a small REST surface plus one multiplexed
WebSocket. Highlights: `POST /api/login`, `POST /api/logout`, `GET /api/ws-ticket`, `GET /api/ws`,
`GET/POST /api/projects`, `GET/DELETE /api/projects/{id}`, `GET/POST
/api/projects/{id}/threads`, `POST /api/projects/{id}/threads/start`, `DELETE
/api/projects/{id}/threads/{thread_id}`, `PATCH /api/projects/{id}/threads/{thread_id}/title`,
`POST /api/projects/{id}/threads/{thread_id}/archive`, `GET /api/models`, `POST
/api/models/refresh`,
`GET /api/tokens`, `GET /api/projects/{id}/tokens`,
`GET /api/projects/{id}/highlight|raw|image`, `POST
/api/projects/{id}/linkify`, `POST /api/projects/{id}/render`, `GET /api/browse`, `POST
/api/browse/mkdir`, `GET /api/projects/{id}/mcp`, `POST /api/projects/{id}/mcp/reload`, and `POST
/api/projects/{id}/mcp/oauth-login`. Wire types are defined once in `giskard-proto`. See
[§13.6](specs/giskard-specification.md) for the message protocol.

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

---

## Architecture

Cargo workspace under `crates/`:

| Crate | Responsibility |
|-------|----------------|
| `giskard-core` | Harness-neutral domain types (no I/O). |
| `giskard-harness` | The `AgentHarness` trait + capabilities. |
| `giskard-harness-codex` | Codex CLI adapter (spawns/speaks to `codex app-server`). |
| `giskard-harness-replay` | Deterministic replay harness for tests. |
| `giskard-persist` | Flat-file storage + the `giskard-admin` binary. |
| `giskard-proto` | Shared client↔server wire types (path-mirrored `Wire*` types). |
| `giskard-server` | Axum backend: auth, WS hub, services, syntax highlighting, and the web UI. |
| `giskard-ui` | Frontend crate (see note below). |

**Frontend note:** the desktop UI is currently a single self-contained page (HTML/CSS/vanilla JS,
no npm) served by `giskard-server` at `/`, with its stylesheet and script as separate same-origin
assets (`/app.css`, `/app.js`) so the Content-Security-Policy can forbid inline script. The
favicon is served as a same-origin SVG at `/favicon.svg`. The spec targets a Dioxus/WASM frontend
(`giskard-ui`); because the wire contract (`giskard-proto`) is stable, that port can happen
without server changes.

---

## Development

```bash
cargo build --workspace
cargo test --workspace
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

Tests never call a real LLM: integration/e2e tests drive the application through the
`ReplayHarness`. See [AGENTS.md](AGENTS.md) for contributor conventions (error surfacing, panic
policy, failure-path test expectations) and the spec for the full design.

[GitHub Actions CI](.github/workflows/ci.yml) runs the same three gates on every push to `main` and
every pull request: `rustfmt` (`cargo fmt --all --check`), `clippy` (`cargo clippy --workspace
--all-targets --locked -- -D warnings`), and the full locked workspace suite (`cargo build
--workspace --locked` + `cargo test --workspace --locked`).

A separate [security-audit workflow](.github/workflows/audit.yml) runs
[`cargo-deny`](https://embarkstudios.github.io/cargo-deny/) (advisories, bans, licenses, sources,
configured in [`deny.toml`](deny.toml)) on dependency-manifest changes, on pull requests, and on a
weekly schedule so newly disclosed advisories are caught even without a code change. Run it locally
with `cargo deny check`.

---

## License

MIT — see [LICENSE](LICENSE).
