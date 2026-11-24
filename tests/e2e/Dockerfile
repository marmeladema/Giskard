# syntax=docker/dockerfile:1.7

# End-to-end test image for Giskard's web UI: the official Playwright image (Node + browsers
# preinstalled) plus the suite's npm dependencies, and nothing else.
#
# `run.sh` and `screenshots.sh` — locally and in CI — build the deterministic, Codex-free
# `giskard-server-replay` on the host and bind-mount it into the container together with the suite
# (tests/e2e), so neither a code change nor a test edit produces a new image. Docker's layer cache
# rebuilds it only when the base image, the CA certificates or package.json/package-lock.json change.
#
# Build context is this directory:
#   docker build -t giskard-e2e tests/e2e

# The tag MUST match the @playwright/test version in package.json so the bundled browsers line up,
# and its Ubuntu release (`resolute`) must be the one CI builds the replay server on. See README.md.
FROM mcr.microsoft.com/playwright:v1.65.1-resolute
WORKDIR /e2e

COPY .host-ca-certificates/ /usr/local/share/ca-certificates/giskard-host/
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt

# Only the dependencies live in the image, in /e2e/node_modules. The suite itself sits below, in
# /e2e/suite, where the scripts mount it, and Node resolves `@playwright/test` by walking up to them.
COPY package.json package-lock.json ./
RUN npm ci
ENV PATH=/e2e/node_modules/.bin:$PATH
WORKDIR /e2e/suite

# Playwright's webServer launches `giskard-server-replay` (resolved on PATH, where the scripts mount
# it). CI mode disables reuse-existing-server and enables one retry.
ENV CI=1
ENTRYPOINT ["playwright", "test"]
CMD []
