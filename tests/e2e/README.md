# End-to-end (Playwright) tests

Browser tests that drive Giskard's real web UI — login, projects/threads, live message streaming,
linked sub-agent navigation/reload/prompt ordering/cascade deletion, how a sub-agent blocked on an
approval is surfaced, server requests surviving a reload, the composer growing with what is typed
(bounded, scrollable, and re-bounded by the on-screen keyboard), and settings — through a headless
Chromium.

The browser side runs **inside Docker**, so you don't need Node, npm, or the Playwright browsers on
your host. You need Docker and a Rust toolchain: the replay server is built on the host and mounted
into the container.

## What's under test

The tests run against **`giskard-server-replay`**, a deterministic, Codex-free build of the server
(`crates/giskard-server/src/bin/giskard-server-replay.rs`). It serves the exact same UI and
REST/WebSocket API as the real `giskard-server`, but:

- uses an in-process **scripted harness** that streams a fixed agent reply on every turn (no LLM, no
  network), plus a deterministic parent/child/reverse-link scenario, so transcript and sub-agent
  lifecycle assertions are fully deterministic;
- boots with a **known password** (`giskard` by default) and one pre-seeded **"Demo"** project, so
  the tests can log in and drive a thread with zero host-side setup.

The sub-agent, approval, and server-request trigger constants in `giskard-server-replay` are mirrored by
`tests/helpers.ts`; update both locations together when changing that scenario. That includes the
approval-blocked sub-agent scenario (`SCRIPTED_SUBAGENT_APPROVAL_*`), whose child deliberately waits
before raising its approval: thread activity is broadcast live and never replayed on connect, so
firing immediately would race the browser's WebSocket and reach nobody. It also includes the
server-request scenario (`SCRIPTED_SERVER_REQUEST_*`), whose harness deliberately never emits a
resolved event when the answer is routed — modelling a harness whose resolved event is late or
absent, which is the window a reload has to survive.

This keeps the suite hermetic: the production server needs a real, authenticated Codex CLI, which
can't run unattended in CI.

## Run it

```bash
# From anywhere in the repo. Builds the replay server and the image, runs the whole suite.
tests/e2e/run.sh

# Run a single spec, or pass any `playwright test` flags:
tests/e2e/run.sh tests/login.spec.ts
tests/e2e/run.sh --reporter=line
```

`run.sh` builds `giskard-server-replay` on the host with `cargo build --release` — release, because
the suite takes far longer against a debug build — and mounts it into the container. To use another
build instead, point `GISKARD_E2E_PREBUILT_BIN` at it:

```bash
cargo build -p giskard-server --bin giskard-server-replay
GISKARD_E2E_PREBUILT_BIN=target/debug/giskard-server-replay tests/e2e/run.sh
```

The binary runs in the image, not on the host, so it must be a Linux executable for the image's CPU
architecture whose glibc and libraries the image's Ubuntu release (Resolute, 26.04) provides. A host
with a newer glibc can produce a binary that needs symbols the image lacks. Before any test, the
script briefly starts the server inside the image, and stops with an error if it cannot run there.

The HTML report lands in `tests/e2e/playwright-report/` on your host.
The test container runs with the invoking user's numeric UID and GID, so the report remains
removable and writable without elevated privileges; transient Playwright test results stay in a
container-only temporary filesystem. If an older run left root-owned report files, repair them
once before rerunning:

```bash
sudo chown -R "$(id -u):$(id -g)" tests/e2e/playwright-report
```

The suite is not stored in the image: `run.sh` and `screenshots.sh` mount `tests/e2e/` into the
container, so editing a spec, a helper or the Playwright config takes effect on the next run without
a new image. The image holds only the Playwright base, the CA certificates and the npm dependencies.
Both scripts still run `docker build` every time, and Docker's layer cache rebuilds it exactly when
one of those inputs changes — the base image tag, a certificate, or `package.json` /
`package-lock.json` — and otherwise reuses it as is.

If your local network intercepts TLS, put any additional host CA certificates as `.crt` files in
`tests/e2e/.host-ca-certificates/` before running the Docker-based tests. The directory is optional:
CI and ordinary local environments build with only the committed `.gitkeep`, while any local
certificates in that hidden directory are copied into the e2e image trust store and ignored by git.

## Run it without Docker (optional, for UI development)

If you already have Node and the matching Playwright browsers, you can iterate faster by pointing
Playwright at a locally built server binary:

```bash
cargo build -p giskard-server --bin giskard-server-replay

cd tests/e2e
npm ci
npx playwright install chromium   # only if you don't already have the pinned browser
GISKARD_SERVER_BIN=../../target/debug/giskard-server-replay npx playwright test
```

Playwright starts and stops the server itself (see `webServer` in `playwright.config.ts`).

## Screenshots

The README's UI screenshots are generated by the same replay server and browser tooling, so they
stay honest as the UI evolves. Regenerate them (no host Node/npm) with:

```bash
tests/e2e/screenshots.sh
```

It builds the replay server the same way `run.sh` does, and honours `GISKARD_E2E_PREBUILT_BIN` too.

This writes `docs/screenshots/ide-desktop.png` and `docs/screenshots/ide-mobile.png` — the default
IDE theme at desktop (1440×900 @2×) and mobile (390×844 @3×) viewports, each with a project open and
a thread showing a message and the scripted reply. The generator lives in `screenshots/` and uses
`screenshots.config.ts` (separate from the test suite so `run.sh` never regenerates images and this
never runs the assertions). As with the report runner, generated files retain the invoking user's
numeric UID and GID.

Without Docker: `GISKARD_SERVER_BIN=../../target/debug/giskard-server-replay npx playwright test
--config=screenshots.config.ts` after building the binary (see below). Override the output location
with `SCREENSHOT_DIR`.

## CI

`.github/workflows/e2e.yml` runs `run.sh` — the same command as a local run, building the replay
server on the runner with the same cargo cache as the other CI jobs — on every push to `main` and
every pull request, uploading the HTML report as a build artifact.

## Configuration

All knobs are environment variables with sensible defaults (see `playwright.config.ts`):

| Variable                   | Default                 | Purpose                                           |
| -------------------------- | ----------------------- | ------------------------------------------------- |
| `GISKARD_SERVER_BIN`       | `giskard-server-replay` | Path/command for the replay server binary.        |
| `GISKARD_E2E_PREBUILT_BIN` | unset                   | Replay binary to mount instead of building one.   |
| `GISKARD_PLAYWRIGHT_OUTPUT_DIR` | `test-results`     | Transient Playwright test output directory.       |
| `GISKARD_HOST`             | `127.0.0.1`             | Host the server binds to and tests connect to.    |
| `GISKARD_PORT`             | `8787`                  | Port for the same.                                |
| `GISKARD_BASE_URL`         | `http://<host>:<port>`  | Full base URL (overrides host/port if set).       |
| `GISKARD_REPLAY_PASSWORD`  | `giskard`               | App password the server accepts and tests submit. |

## Keeping versions in sync

The Playwright npm package and its browsers must match the Docker base image. When bumping
Playwright, change **both**:

- `@playwright/test` in `tests/e2e/package.json`, and
- the `mcr.microsoft.com/playwright:vX.Y.Z-resolute` tag in `tests/e2e/Dockerfile`.

The CI runner (`runs-on` in `.github/workflows/e2e.yml`) must be the same Ubuntu release as that
image (`-resolute` is Ubuntu 26.04): the replay server is built on the runner and run in the image, so
it must not need a newer glibc than the image has. Moving the image to a new Ubuntu release means
moving the runner with it.

The scripted replies and special prompts asserted by the browser specs are defined in the replay
binary and mirrored in `tests/helpers.ts`; keep the two in step. The turn-steering fixture starts a
turn with `SCRIPTED_STEERING_TRIGGER`, holds it active until text is steered into that exact turn,
then emits a same-turn user message, `SCRIPTED_STEERING_REPLY`, and one completion.
