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

## Quick start

```bash
# 1. Build
cargo build --release

# 2. Choose a data directory (holds config, projects, threads, token ledgers).
#    Defaults to ~/.local/share/giskard; override with GISKARD_DATA_DIR.
export GISKARD_DATA_DIR="$HOME/.local/share/giskard"
mkdir -p "$GISKARD_DATA_DIR"

# 3. Set the app password (prints an Argon2 hash to paste into config.toml).
cargo run -p giskard-persist --bin giskard-admin -- set-password

# 4. Create the config from the annotated example and edit it (paste the hash;
#    keep secure_cookies = false for plain-HTTP localhost).
cp config.example.toml "$GISKARD_DATA_DIR/config.toml"
$EDITOR "$GISKARD_DATA_DIR/config.toml"

# 5. Run the server.
cargo run --release -p giskard-server --bin giskard-server
```

Then open **http://127.0.0.1:8787**, log in, and:

1. **+** next to *Projects* → name it and give an **absolute directory path** that exists on the
   server machine (the agent's workspace).
2. **+** on the project → *New thread*.
3. Type in the composer (Enter to send). The header has the **Plan/Build** toggle, the **model**
   picker, and a **token/context** gauge; scrolling the transcript to the top lazy-loads older
   history.

The header gauge is a context-window indicator, not a billing total. Codex currently exposes the
latest turn's input tokens rather than a dedicated context-occupancy field, so Giskard uses that as
the best available proxy for "how full is the active conversation?" The usage tiles in the right
panel remain cumulative input/output/total tokens and can legitimately exceed the model's context
window over a long thread.

> **Common gotcha:** with `secure_cookies = true` over plain HTTP, the browser drops the session
> cookie — login appears to succeed but nothing loads. Use `false` for local HTTP; set `true` only
> behind HTTPS/TLS (e.g. an Nginx terminator).

---

## Configuration

Config lives at `${GISKARD_DATA_DIR:-~/.local/share/giskard}/config.toml`. A fully annotated,
copy-pasteable template is in [`config.example.toml`](config.example.toml). Every section is
optional and falls back to the defaults below.

| Section | Key | Default | Purpose |
|---------|-----|---------|---------|
| `[server]` | `bind` | `127.0.0.1:8787` | HTTP/WS listen address. |
| | `secure_cookies` | `true` | `Secure` flag on the session cookie. **Set `false` for plain-HTTP local dev.** |
| `[auth]` | `password_hash` | — | Argon2 hash of the shared password (or env `GISKARD_PASSWORD_HASH`). Generate with `giskard-admin set-password`. |
| | `session_days` | `30` | Session lifetime (sliding). |
| `[browse]` | `roots` | `[]` (whole FS) | Confine the filesystem picker to these absolute subtrees. |
| `[plan]` | `default_dir` / `filename_template` | `docs` / `plan-{slug}-{ts}.md` | Where "Save plan to project" writes. |
| `[tokens]` | `cost_estimation` | `false` | Show an estimated € cost from `[tokens.rates."provider/model"]`. |
| `[viz]` | `max_highlight_size` | `10485760` (10 MiB) | Files larger than this aren't syntax-highlighted. |
| `[history]` | `initial` / `page` | `50` / `50` | Turns loaded on open / per scroll-up page. |
| `[harness]` | `kind` | `codex` | Agent harness (v1: `codex`). |
| | `idle_shutdown_secs` | `0` (keep alive) | Terminate an idle project's harness after N seconds. |
| `[[providers]]` | `id`, `name`, `wire_api`, `base_url?`, `model_listing`, `api_key?` / `api_key_env?`, `[[providers.models]]` | — | What the model picker offers. With `model_listing = true` + `base_url` the picker is refreshed from `GET {base_url}/models` (on load, and via the ↻ button), so `[[providers.models]]` becomes optional. Set `api_key` (inline) or `api_key_env` (env-var name) for endpoints that require auth — sent as `Authorization: Bearer …`. |

Provider config governs the **picker** and optional `/v1/models` discovery only — Codex itself
reads `~/.codex/config.toml` for real provider/auth, so any model you select must be one Codex can
actually reach.

---

## Storage layout

Flat files under the data directory (human-readable; inspect with `cat`/`jq`):

```
$GISKARD_DATA_DIR/
├── config.toml                  # this config
├── projects.json                # project index (id, name, dir, created_at, order)
├── projects/<project_id>/
│   ├── project.json             # workspace root, default model, approval policy, harness kind
│   ├── threads/
│   │   ├── <thread_id>.json      # thread metadata + token aggregates (a recomputable cache)
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
cargo run -p giskard-persist --bin giskard-admin -- <command>
```

| Command | Description |
|---------|-------------|
| `set-password` | Prompt for a password and print its Argon2 hash. |
| `list-projects` | List projects in the data dir. |
| `list-threads <project_id>` | List a project's threads. |
| `dump-thread <project_id> <thread_id>` | Pretty-print a thread's metadata + history. |
| `delete-thread <project_id> <thread_id>` | Delete a thread (metadata + history). |
| `delete-project <project_id>` | Delete a project and its threads. |
| `validate` | Parse every stored file and report corruption (history is checked line-by-line). |

---

## HTTP / WebSocket API

The browser (and any client) drives everything through a small REST surface plus one multiplexed
WebSocket. Highlights: `POST /api/login`, `GET/POST /api/projects`, `GET/POST
/api/projects/{id}/threads`, `GET /api/models` (+ `POST /api/models/refresh`), `GET /api/tokens` and
`GET /api/projects/{id}/tokens` (dashboards), `GET /api/projects/{id}/highlight|raw`, `POST
/api/projects/{id}/linkify`, `POST /api/projects/{id}/render` (agent Markdown → sanitized HTML),
`GET /api/browse`, and `GET /api/ws`. Wire types are defined once in
`giskard-proto`. See [§13.6](specs/giskard-specification.md) for the message protocol.

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
no npm) served by `giskard-server` at `/`. The spec targets a Dioxus/WASM frontend (`giskard-ui`);
because the wire contract (`giskard-proto`) is stable, that port can happen without server changes.

---

## Development

```bash
cargo build            # build native crates
cargo test             # run the full test suite (unit + replay-driven integration + e2e)
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

Tests never call a real LLM: integration/e2e tests drive the application through the
`ReplayHarness`. See [AGENTS.md](AGENTS.md) for contributor conventions (error surfacing, panic
policy, failure-path test expectations) and the spec for the full design.

[GitHub Actions CI](.github/workflows/ci.yml) runs the same three gates on every push to `main` and
every pull request: `rustfmt` (`--check`), `clippy` (`--workspace --all-targets -- -D warnings`),
and the full `--workspace` test suite (build + test).

A separate [security-audit workflow](.github/workflows/audit.yml) runs
[`cargo-deny`](https://embarkstudios.github.io/cargo-deny/) (advisories, bans, licenses, sources,
configured in [`deny.toml`](deny.toml)) on dependency-manifest changes, on pull requests, and on a
weekly schedule so newly disclosed advisories are caught even without a code change. Run it locally
with `cargo deny check`.

---

## License

MIT — see [LICENSE](LICENSE).
