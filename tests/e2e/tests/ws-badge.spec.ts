import { test, expect, type Page } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

// The connection badge in the thread header. On a phone it shrinks to its coloured dot to save
// header space; there is no hover there for a tooltip, so a tap shows the state for a few seconds in
// a bubble below it, without resizing the badge. On a desktop it always shows the word.

async function openThread(page: Page) {
  await login(page);
  if (await page.locator("#btnMenu").isVisible()) await page.locator("#btnMenu").click();
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Open a thread for the connection badge");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  await expect(page.locator("#wsStatusBadge")).toHaveClass(/\bstate-open\b/);
}

test.describe("connection badge", () => {
  test("shows its word on a desktop", async ({ page }) => {
    await openThread(page);
    await expect(page.locator("#wsStatusBadge .ws-label")).toBeVisible();
    await expect(page.locator("#wsStatusBadge")).toHaveText("Connected");
    await expect(page.locator("#subagentsBtn")).toHaveText(/^\s*Agents\s*\d/);
  });

  test.describe("on a phone", () => {
    test.use({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 3, isMobile: true, hasTouch: true });

    test("is just its dot, and a tap shows the state without resizing it", async ({ page }) => {
      await openThread(page);
      const badge = page.locator("#wsStatusBadge");
      const bubble = () => badge.evaluate((el) => {
        const after = getComputedStyle(el, "::after");
        return after.content === "none" ? null : after.content.replace(/^"|"$/g, "");
      });

      // The word is hidden visually only: still there, still read by screen readers.
      await expect(badge.locator(".ws-label")).toHaveText("Connected");
      expect((await badge.locator(".ws-label").boundingBox())!.width).toBeLessThanOrEqual(1);
      const before = (await badge.boundingBox())!;
      expect(before.width).toBeLessThan(30);
      // As tall as the badges beside it, and on the same line.
      const tasks = (await page.locator("#tasksBtn").boundingBox())!;
      expect(Math.abs(before.height - tasks.height)).toBeLessThan(0.5);
      expect(Math.abs(before.y - tasks.y)).toBeLessThan(0.5);
      expect(await bubble()).toBeNull();

      await badge.tap();
      await expect.poll(bubble).toBe("Connected");
      // The word floats below the badge; the badge itself doesn't change size or move.
      const after = (await badge.boundingBox())!;
      expect(after).toEqual(before);
      expect((await badge.locator(".ws-label").boundingBox())!.width).toBeLessThanOrEqual(1);
      // …for a few seconds, then it goes.
      await expect.poll(bubble, { timeout: 6_000 }).toBeNull();
    });

    test("is a dot in every state, and the bubble follows a state change", async ({ page }) => {
      await openThread(page);
      const badge = page.locator("#wsStatusBadge");
      const bubble = () => badge.evaluate((el) => getComputedStyle(el, "::after").content);
      await page.evaluate(() => {
        state.wsStatus = "reconnecting";
        renderWsStatus();
      });
      await expect(badge.locator(".ws-label")).toHaveText("Reconnecting...");
      expect((await badge.locator(".ws-label").boundingBox())!.width).toBeLessThanOrEqual(1);

      await badge.tap();
      await expect.poll(bubble).toBe('"Reconnecting..."');
      // A state change while the bubble shows updates it, rather than hiding it early.
      await page.evaluate(() => {
        state.wsStatus = "open";
        renderWsStatus();
      });
      await expect.poll(bubble).toBe('"Connected"');
    });
  });
});

declare const state: { wsStatus: string };
declare function renderWsStatus(): void;
