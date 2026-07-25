import { test, expect, type Page } from "@playwright/test";
import { login } from "./helpers";

/**
 * Behaviour of the message composer's Enter key on desktop vs. mobile keyboards.
 *
 * On desktop, Enter sends and Shift+Enter inserts a newline — the conventional ChatGPT-style
 * composer. On a touch/mobile keyboard there is no Shift modifier reachable while typing, so the
 * newline key fires a plain `Enter`. The composer must therefore insert a newline on Enter for
 * touch devices and require an explicit tap of the Send button to send, instead of swallowing the
 * newline and sending the half-typed message.
 */

async function openDraftComposer(page: Page) {
  // On mobile the projects live in a drawer; reveal it before opening a draft. On desktop the
  // hamburger is hidden, so this is a no-op.
  const menuButton = page.locator("#btnMenu");
  if (await menuButton.isVisible()) {
    await menuButton.click();
  }
  const project = page.locator(".proj", { hasText: "Demo" });
  await project.locator(".project-add").click();
  const input = page.locator("#input");
  await expect(input).toBeVisible();
  return input;
}

test.describe("composer Enter key (desktop)", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("Enter sends, Shift+Enter inserts a newline", async ({ page }) => {
    const input = await openDraftComposer(page);
    const transcript = page.locator("#transcript");

    await input.fill("hello");
    // Shift+Enter inserts a newline, does not send.
    await input.press("Shift+Enter");
    await expect(input).toHaveValue("hello\n");
    await expect(transcript.locator(".msg.user")).toHaveCount(0);

    // Plain Enter sends the message (trailing newline is trimmed before sending).
    await input.press("Enter");
    await expect(transcript.locator(".msg.user", { hasText: "hello" })).toBeVisible();
    // Composer clears on send.
    await expect(input).toHaveValue("");
  });

});

// A mobile/touch Chromium so `isMobile` + `hasTouch` match a real phone and the composer's touch
// detection sees a coarse pointer. The fixture's own page is reused so the suite's single login runs
// once and isn't hit by the global login throttle.
test.describe("composer Enter key (mobile)", () => {
  test.use({
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 3,
    isMobile: true,
    hasTouch: true,
  });
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("plain Enter inserts a newline and only the Send button sends", async ({ page }) => {
    const input = await openDraftComposer(page);
    const transcript = page.locator("#transcript");

    await input.fill("hello");
    // The mobile newline key fires a plain Enter (no Shift). It must insert a newline, not send.
    await input.press("Enter");
    await expect(input).toHaveValue("hello\n");
    // The composer's contents were not sent: there is no user message in the transcript yet.
    await expect(transcript.locator(".msg.user")).toHaveCount(0);

    // The user finishes typing and taps the Send button to send.
    await page.locator("#sendBtn").click();
    await expect(transcript.locator(".msg.user", { hasText: "hello" })).toBeVisible();
    await expect(input).toHaveValue("");
  });

  test("Ctrl/Cmd+Enter still sends on touch (external keyboard on a tablet)", async ({ page }) => {
    const input = await openDraftComposer(page);
    const transcript = page.locator("#transcript");

    await input.fill("tablet message");
    // An external keyboard on a tablet can still use Ctrl/Cmd+Enter to send directly.
    const mod = process.platform === "darwin" ? "Meta" : "Control";
    await input.press(`${mod}+Enter`);
    await expect(transcript.locator(".msg.user", { hasText: "tablet message" })).toBeVisible();
    await expect(input).toHaveValue("");
  });
});
