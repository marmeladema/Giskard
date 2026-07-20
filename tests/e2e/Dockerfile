# End-to-end test image for Giskard's web UI.
#
# Stage 1 builds the deterministic, Codex-free `giskard-server-replay` binary from the workspace.
# Stage 2 is the official Playwright image (Node + browsers preinstalled), which runs the Playwright
# suite against that binary. The result: `docker build` + `docker run` exercises the real browser UI
# with no Node/npm, no Rust, and no Codex on the host.
#
# Build context is the repository root:
#   docker build -f tests/e2e/Dockerfile -t giskard-e2e .

# ---- Stage 1: build the replay server ----
FROM rust:1-slim-bookworm AS builder
WORKDIR /src

# ring (via rustls) needs a C toolchain and perl to build.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config perl \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release -p giskard-server --bin giskard-server-replay

# ---- Stage 2: run Playwright ----
# The tag MUST match the @playwright/test version in tests/e2e/package.json so the bundled browsers
# line up. Bump both together.
FROM mcr.microsoft.com/playwright:v1.56.0-jammy
WORKDIR /e2e

COPY --from=builder /src/target/release/giskard-server-replay /usr/local/bin/giskard-server-replay

# Install JS deps first (better layer caching), then copy the tests.
COPY tests/e2e/package.json tests/e2e/package-lock.json ./
RUN npm ci
COPY tests/e2e/ ./

# Playwright's webServer launches `giskard-server-replay` (resolved on PATH). CI mode disables
# reuse-existing-server and enables one retry.
ENV CI=1
ENTRYPOINT ["npx", "playwright", "test"]
CMD []
