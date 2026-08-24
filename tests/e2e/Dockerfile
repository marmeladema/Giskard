# syntax=docker/dockerfile:1.7

# End-to-end test image for Giskard's web UI.
#
# The optional builder produces the deterministic, Codex-free `giskard-server-replay`. The runtime
# is the official Playwright image (Node + browsers preinstalled). Local scripts bind-mount a host
# binary or export the builder's result; the default target remains self-contained for CI.
#
# Build context is the repository root:
#   docker build -f tests/e2e/Dockerfile -t giskard-e2e .

# ---- Stage 1: build the replay server ----
FROM rust:1-slim-bookworm AS builder
WORKDIR /src

# ring (via rustls) needs a C toolchain and perl to build.
COPY tests/e2e/.host-ca-certificates/ /usr/local/share/ca-certificates/giskard-host/
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates pkg-config perl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . .
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt
RUN --mount=type=cache,id=giskard-e2e-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=giskard-e2e-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=giskard-e2e-target,target=/src/target,sharing=locked \
    cargo build --release -p giskard-server --bin giskard-server-replay \
    && install -Dm755 target/release/giskard-server-replay /out/giskard-server-replay

# ---- Stage 2: run Playwright ----
# The tag MUST match the @playwright/test version in tests/e2e/package.json so the bundled browsers
# line up. Bump both together.
FROM mcr.microsoft.com/playwright:v1.62.1-noble AS runtime
WORKDIR /e2e

COPY tests/e2e/.host-ca-certificates/ /usr/local/share/ca-certificates/giskard-host/
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt

# Install JS deps first (better layer caching), then copy the tests.
COPY tests/e2e/package.json tests/e2e/package-lock.json ./
RUN npm ci
COPY tests/e2e/ ./

# Playwright's webServer launches `giskard-server-replay` (resolved on PATH). CI mode disables
# reuse-existing-server and enables one retry.
ENV CI=1
ENTRYPOINT ["npx", "playwright", "test"]
CMD []

# Local runs use this image and bind-mount the replay binary, keeping it out of image layers.
FROM runtime AS e2e-runtime

# The local fallback exports only this artifact to a temporary host directory. Cargo's mutable
# registry, Git, and target data remain BuildKit cache mounts rather than image layers.
FROM scratch AS replay-binary
COPY --from=builder /out/giskard-server-replay /giskard-server-replay

# Preserve a self-contained default image for CI and direct Dockerfile callers.
FROM runtime AS e2e
COPY --from=builder /out/giskard-server-replay /usr/local/bin/giskard-server-replay
