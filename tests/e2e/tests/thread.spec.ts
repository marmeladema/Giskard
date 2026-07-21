import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

declare const state: {
  threadId: string;
  currentModel: { provider: string; model: string; reasoning_effort: null };
  contextUsed: number;
  contextWindow: number;
  awaitingThreadMetadataState: boolean;
  pendingContextWindowUpdate: unknown;
};
declare function handleServer(message: unknown): void;

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

    // The user's message echoes into the transcript immediately.
    await expect(page.getByText(message)).toBeVisible();

    // The scripted harness streams a canned reply over the WebSocket; it lands in the transcript.
    await expect(page.getByText(SCRIPTED_REPLY)).toBeVisible();

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
    await expect(page.getByText(SCRIPTED_REPLY)).toBeVisible();

    // The composer is emptied once the message is sent, and the composer itself stays available.
    await expect(input).toHaveValue("");
    await expect(input).toBeVisible();

    // Both sides of the exchange remain in the transcript.
    await expect(page.getByText(message)).toBeVisible();
    await expect(page.getByText(SCRIPTED_REPLY)).toBeVisible();
  });

  test("keeps a context update that arrives before a resync snapshot", async ({ page }) => {
    const result = await page.evaluate(() => {
      const threadId = "01J00000000000000000000000";
      const model = { provider:"openai", model:"gpt-5.5", reasoning_effort:null };
      state.threadId = threadId;
      state.currentModel = model;
      state.contextUsed = 64_000;
      state.contextWindow = 128_000;
      state.awaitingThreadMetadataState = true;
      state.pendingContextWindowUpdate = null;

      handleServer({
        type:"thread_context_window_updated",
        thread_id:threadId,
        context_window:258_400
      });
      handleServer({
        type:"thread_state",
        thread_id:threadId,
        state:{
          id:threadId,
          mode:"build",
          approval_policy:"ask",
          current_model:model,
          context_window:128_000
        }
      });

      return {
        contextWindow:state.contextWindow,
        pending:state.pendingContextWindowUpdate
      };
    });

    expect(result.contextWindow).toBe(258_400);
    expect(result.pending).toBeNull();
  });
});
