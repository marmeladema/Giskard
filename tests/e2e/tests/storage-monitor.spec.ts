import { test, expect, type Page } from "@playwright/test";
import { login } from "./helpers";

declare const state: { ws: WebSocket | null };

const GiB = 1024 ** 3;
function filesystem(path = "/data", available = 8) {
  return { id: path, paths: [path], available_bytes: available * GiB,
    total_bytes: 100 * GiB, minimum_available_bytes: 10 * GiB, low: available < 10, error: null };
}
async function snapshot(page: Page, revision: number, filesystems: unknown[], issues: unknown[] = [], enabled = true) {
  await page.evaluate(status => {
    (window as unknown as { handleServer: (message: unknown) => void }).handleServer(status);
  }, { type: "storage_status", revision, checked_at: new Date().toISOString(), enabled, filesystems, issues });
}
async function connected(page: Page) {
  await page.waitForFunction(() => state.ws?.readyState === WebSocket.OPEN);
}

test("global storage warning survives navigation and clears on recovery or disable", async ({ page }) => {
  await login(page);
  await connected(page);
  const banner = page.locator("#storageBanner");
  await snapshot(page, 10000, [filesystem()]);
  await expect(banner).toContainText("8.0 GiB available. Warning threshold: 10.0 GiB");
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await connected(page);
  await snapshot(page, 10001, [filesystem()]);
  await expect(banner).toBeVisible();
  await snapshot(page, 10000, []);
  await expect(banner).toBeVisible();
  await snapshot(page, 10002, [filesystem("/data", 20)]);
  await expect(banner).toBeHidden();
  await snapshot(page, 10003, [filesystem()]);
  await snapshot(page, 10004, [filesystem()], [], false);
  await expect(banner).toBeHidden();
});

test("sampling failures stay visible and a reconnect accepts a restarted revision", async ({ page }) => {
  await login(page);
  await connected(page);
  const banner = page.locator("#storageBanner");
  await snapshot(page, 10000, [{ ...filesystem(), error: "Filesystem lookup failed" }],
    [{ path: "/restricted", message: "Permission denied" }]);
  await expect(banner).toContainText("Some locations could not be checked");
  await banner.locator("summary").click();
  await expect(banner).toContainText("Last successful reading");
  await expect(banner).toContainText("/restricted: Permission denied");
  await page.evaluate(async () => {
    await (window as unknown as { connectWs: (options: unknown) => Promise<void> })
      .connectWs({ reconnect: true, reason: "test storage revision reset" });
  });
  await connected(page);
  await snapshot(page, 1, [filesystem("/after-restart", 3)]);
  await expect(banner).toContainText("3.0 GiB available");
});

for (const appearance of ["ide", "terminal", "bubbles"]) {
  test.describe(`storage warning fits a phone (${appearance})`, () => {
    test.use({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    test("multiple monitored paths and long paths wrap without horizontal overflow", async ({ page }) => {
      await page.addInitScript(value => localStorage.setItem("giskard.appearance", value), appearance);
      await login(page);
      await connected(page);
      await snapshot(page, 10000, [filesystem("/mnt/" + "long_directory/".repeat(40)), filesystem("/other", 2)]);
      const banner = page.locator("#storageBanner");
      await expect(banner).toContainText("Low disk space at 2 locations");
      await banner.locator("summary").click();
      expect(await banner.evaluate(el => el.scrollWidth - el.clientWidth)).toBeLessThanOrEqual(1);
      expect(await banner.evaluate(el => el.getBoundingClientRect().right)).toBeLessThanOrEqual(390);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
    });
  });
}
