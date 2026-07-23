import { test, expect } from "@playwright/test";
import path from "node:path";
import { SCRIPTED_REPLY, login } from "../tests/helpers";

// Where the PNGs land. In Docker this is set to a bind-mounted dir; locally it defaults to the
// repo's docs/screenshots/ (playwright runs with tests/e2e as its cwd).
const OUT_DIR =
  process.env.SCREENSHOT_DIR ?? path.resolve(process.cwd(), "..", "..", "docs", "screenshots");

// This is a screenshot generator, not an assertion suite — but running it through the test runner
// gives us the managed replay server (webServer), viewport projects, and retries for free. One
// "test" per project (desktop / mobile, see screenshots.config.ts) captures the IDE theme with the
// UI populated: a project open, a thread with a user message and the scripted agent reply.
test("IDE theme", async ({ page }, testInfo) => {
  // Force the IDE appearance before the app's boot script reads it, so the shot is deterministic
  // regardless of any persisted preference.
  await page.addInitScript(() => {
    try {
      localStorage.setItem("giskard.appearance", "ide");
    } catch {
      /* first-party storage always available; ignore */
    }
  });

  await login(page);
  await expect(page.locator("html")).toHaveAttribute("data-appearance", "ide");

  // On mobile the projects live in a drawer; reveal it before starting a thread. On desktop the
  // hamburger is hidden, so this is a no-op.
  const menuButton = page.locator("#btnMenu");
  if (await menuButton.isVisible()) {
    await menuButton.click();
  }

  // Start a thread from the seeded "Demo" project. openDraftThread() closes the drawer, so on
  // mobile this reveals the transcript/composer.
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

  const input = page.locator("#input");
  await expect(input).toBeVisible();
  await input.fill("What does this project do?");
  await page.locator("#sendBtn").click();

  // Wait for the streamed reply so the transcript has real content.
  const transcript = page.locator("#transcript");
  await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

  // Reload so the thread re-opens from persisted history: this drops the just-finished turn's
  // "Agent is running…" composer state and gives a clean, idle UI to capture. The last thread is
  // restored automatically (client-side), and on mobile that leaves the transcript (not the drawer)
  // in view.
  await page.reload();
  await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  await expect(page.locator("#stopBtn")).toBeHidden();

  // Drop focus so no text caret blinks into the capture.
  await page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur());

  const file = path.join(OUT_DIR, `ide-${testInfo.project.name}.png`);
  await page.screenshot({ path: file, animations: "disabled" });
  // Surface the path in the runner output and attach it to the report.
  console.log(`wrote ${file}`);
  await testInfo.attach(`ide-${testInfo.project.name}`, {
    path: file,
    contentType: "image/png",
  });
});
