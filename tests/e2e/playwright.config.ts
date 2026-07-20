import { defineConfig, devices } from "@playwright/test";

// The replay server (crates/giskard-server, bin `giskard-server-replay`) is a deterministic,
// Codex-free build of the real server: same UI, same REST + WebSocket API, a known password, and a
// pre-seeded "Demo" project. Tests drive the actual browser app against it.
//
// Everything is overridable by env so the same config works in the Docker image (binary on PATH),
// in CI, and on a dev box that ran `cargo build -p giskard-server --bin giskard-server-replay`.
const PORT = process.env.GISKARD_PORT ?? "8787";
const HOST = process.env.GISKARD_HOST ?? "127.0.0.1";
const BASE_URL = process.env.GISKARD_BASE_URL ?? `http://${HOST}:${PORT}`;
const PASSWORD = process.env.GISKARD_REPLAY_PASSWORD ?? "giskard";
const SERVER_BIN = process.env.GISKARD_SERVER_BIN ?? "giskard-server-replay";

export default defineConfig({
  testDir: "./tests",
  // The suite shares one stateful server (a single seeded project, a global login throttle), so it
  // runs serially for determinism rather than for speed.
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: [["list"], ["html", { open: "never" }]],
  timeout: 30_000,
  expect: { timeout: 10_000 },

  use: {
    baseURL: BASE_URL,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
  },

  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],

  // Playwright starts (and, in local dev, reuses) the replay server itself. `GISKARD_SERVER_BIN`
  // may be an absolute path (dev box) or a bare command resolved on PATH (Docker image).
  webServer: {
    command: SERVER_BIN,
    url: BASE_URL,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
    stdout: "pipe",
    stderr: "pipe",
    env: {
      GISKARD_BIND: `${HOST}:${PORT}`,
      GISKARD_REPLAY_PASSWORD: PASSWORD,
    },
  },
});
