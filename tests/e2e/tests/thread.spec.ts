import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

test.describe("projects and threads", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("lists the seeded demo project", async ({ page }) => {
    await expect(page.locator(".proj .project-name", { hasText: "Demo" })).toBeVisible();
  });

  test("starts a thread and streams the agent reply", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });

    // The per-project "+" (title "New thread") opens a draft thread and the composer.
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    await expect(input).toBeVisible();

    const message = "Please summarize the project.";
    await input.fill(message);
    await page.locator("#sendBtn").click();

    const transcript = page.locator("#transcript");

    // The user's message echoes into the transcript immediately.
    await expect(transcript.locator(".msg.user", { hasText: message })).toBeVisible();

    // The scripted harness streams a canned reply over the WebSocket; it lands in the transcript.
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

    // Starting the turn persisted a real thread, so a thread row now exists in the sidebar.
    await expect(page.locator(".thread").first()).toBeVisible();
  });

  test("clears the composer and keeps the transcript after a turn", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    const message = "First message";
    await input.fill(message);
    await page.locator("#sendBtn").click();
    const transcript = page.locator("#transcript");
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

    // The composer is emptied once the message is sent, and the composer itself stays available.
    await expect(input).toHaveValue("");
    await expect(input).toBeVisible();

    // Both sides of the exchange remain in the transcript.
    await expect(transcript.locator(".msg.user", { hasText: message })).toBeVisible();
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  });
});
