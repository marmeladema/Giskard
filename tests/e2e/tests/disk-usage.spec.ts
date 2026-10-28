import { test, expect, type Page } from "@playwright/test";
import { login } from "./helpers";

declare const state: { ws: WebSocket | null };

const GiB = 1024 ** 3;
function filesystem(path = "/data", available = 8) {
  return { path, available_bytes: available * GiB, total_bytes: 100 * GiB,
    min_available_bytes: 10 * GiB, low: available < 10, error: null };
}

/**
 * Serve a scripted `GET /api/status` under the page's real UI version, so the version gate passes
 * and only the disk usage snapshot is under test. Returns a function that reads how many polls hit.
 */
async function serveStatus(page: Page, paths: unknown[], enabled = true) {
  const uiVersion = await page.evaluate(() =>
    document.querySelector('meta[name="giskard-ui-version"]')?.getAttribute("content"));
  let reads = 0;
  await page.route("**/api/status", async route => {
    reads++;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ui_version: uiVersion, disk_usage: {
        enabled, check_interval_seconds: 60, checked_at: new Date().toISOString(), paths } }),
    });
  });
  return () => reads;
}

/** Trigger the poll the page would otherwise run on the server's interval. */
async function poll(page: Page) {
  await page.evaluate(async () => {
    await (window as unknown as { refreshAppStatus: () => Promise<unknown> }).refreshAppStatus();
  });
}

async function connected(page: Page) {
  await page.waitForFunction(() => state.ws?.readyState === WebSocket.OPEN);
}

test("disk usage warning shows without a thread, survives opening one, and clears on recovery or disable", async ({ page }) => {
  await login(page);
  const banner = page.locator("#diskUsageBanner");
  await expect(banner).toBeHidden();

  await serveStatus(page, [filesystem()]);
  await poll(page);
  await expect(banner).toContainText("8.0 GiB available. Warning threshold: 10.0 GiB");
  await expect(page.locator("#wsStatusBadge")).toBeHidden();

  // Opening a draft closes any socket and opens none; the banner is independent of both.
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await expect(banner).toBeVisible();
  await expect(page.locator("#wsStatusBadge")).toHaveText("Draft");

  await page.unroute("**/api/status");
  await serveStatus(page, [filesystem("/data", 20)]);
  await poll(page);
  await expect(banner).toBeHidden();

  await page.unroute("**/api/status");
  await serveStatus(page, [filesystem()], false);
  await poll(page);
  await expect(banner).toBeHidden();
});

test("sampling failures stay visible and a socket open refreshes the reading", async ({ page }) => {
  await login(page);
  const banner = page.locator("#diskUsageBanner");
  await serveStatus(page, [{ ...filesystem(), error: "Filesystem lookup failed" }]);
  await poll(page);
  await expect(banner).toContainText("Some locations could not be checked");
  await banner.locator("summary").click();
  await expect(banner).toContainText("Last successful reading");
  await expect(banner).toContainText("Filesystem lookup failed");

  await page.unroute("**/api/status");
  const reads = await serveStatus(page, [filesystem("/after-restart", 3)]);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Create a thread so the socket opens.");
  await page.locator("#sendBtn").click();
  await connected(page);
  await expect(banner).toContainText("3.0 GiB available");
  expect(reads()).toBeGreaterThanOrEqual(1);
});

for (const appearance of ["ide", "terminal", "bubbles"]) {
  test.describe(`disk usage warning fits a phone (${appearance})`, () => {
    test.use({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    test("multiple monitored paths and long paths wrap without horizontal overflow", async ({ page }) => {
      await page.addInitScript(value => localStorage.setItem("giskard.appearance", value), appearance);
      await login(page);
      await serveStatus(page, [filesystem("/mnt/" + "long_directory/".repeat(40)), filesystem("/other", 2)]);
      await poll(page);
      const banner = page.locator("#diskUsageBanner");
      await expect(banner).toContainText("Low disk space at 2 locations");
      await banner.locator("summary").click();
      expect(await banner.evaluate(el => el.scrollWidth - el.clientWidth)).toBeLessThanOrEqual(1);
      expect(await banner.evaluate(el => el.getBoundingClientRect().right)).toBeLessThanOrEqual(390);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
    });
  });
}
