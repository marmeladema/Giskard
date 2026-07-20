import { defineConfig, devices } from "@playwright/test";

// Config for generating README screenshots (not part of the test suite — see playwright.config.ts
// for that). Same deterministic replay server; two viewport "projects" produce a desktop and a
// mobile capture of the IDE theme. Run via `tests/e2e/screenshots.sh`.
const PORT = process.env.GISKARD_PORT ?? "8788";
const HOST = process.env.GISKARD_HOST ?? "127.0.0.1";
const BASE_URL = process.env.GISKARD_BASE_URL ?? `http://${HOST}:${PORT}`;
const PASSWORD = process.env.GISKARD_REPLAY_PASSWORD ?? "giskard";
const SERVER_BIN = process.env.GISKARD_SERVER_BIN ?? "giskard-server-replay";

export default defineConfig({
  testDir: "./screenshots",
  // The generator files aren't named *.spec.ts (they don't assert product behaviour), so match all
  // TS under screenshots/ explicitly.
  testMatch: "**/*.ts",
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  timeout: 60_000,
  expect: { timeout: 15_000 },

  use: {
    baseURL: BASE_URL,
  },

  projects: [
    {
      name: "desktop",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 1440, height: 900 },
        // Retina-density capture so the image stays sharp when scaled in the README.
        deviceScaleFactor: 2,
      },
    },
    {
      name: "mobile",
      // A mobile-sized Chromium (not the WebKit iPhone profile) so only the bundled chromium browser
      // is required. Width < 820px triggers the app's mobile layout.
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 390, height: 844 },
        deviceScaleFactor: 3,
        isMobile: true,
        hasTouch: true,
      },
    },
  ],

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
