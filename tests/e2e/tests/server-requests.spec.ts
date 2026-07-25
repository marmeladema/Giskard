import { test, expect } from "@playwright/test";
import {
  SCRIPTED_SERVER_REQUEST_QUESTION,
  SCRIPTED_SERVER_REQUEST_TRIGGER,
  login,
} from "./helpers";

// Server requests (`requestUserInput` and friends) had no browser coverage at all, despite having
// real UI: a card, per-question controls, Continue/Cancel, and a resolved state.
//
// The gap that matters is the one already fixed for approvals. A request is cleared from the live
// buffer by the harness's own resolved event, which arrives on the harness's schedule and may never
// arrive. Answering used to record nothing server-side, so a reload in that window replayed the
// request as actionable — and answering a second time routes a stale id to the harness, which
// errors. The scripted harness deliberately never resolves, so this is exactly that window.
test.describe("server requests", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("an answered user-input request stays resolved after a browser reload", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    await expect(input).toBeVisible();
    await input.fill(SCRIPTED_SERVER_REQUEST_TRIGGER);
    await page.locator("#sendBtn").click();

    const transcript = page.locator("#transcript");
    const request = transcript.locator(".msg.server-request");
    await expect(request).toBeVisible();
    await expect(request).toContainText("Agent needs your answer");
    await expect(request).toContainText(SCRIPTED_SERVER_REQUEST_QUESTION);

    // Actionable before it is answered: a question control and both actions.
    const answer = request.locator("select.server-request-answer");
    await expect(answer).toBeVisible();
    const continueBtn = request.getByRole("button", { name: "Continue", exact: true });
    await expect(continueBtn).toBeEnabled();

    await answer.selectOption("develop");
    await continueBtn.click();

    // Answering disables the controls and records what was sent. The harness never emits a resolved
    // event, so this is as far as the live UI goes.
    await expect(request.locator(".server-request-sent")).toHaveText("Sent: Continue");
    await expect(continueBtn).toBeDisabled();

    // Reload: in-memory state is wiped, so the resolved state has to be reconstructed entirely from
    // the server's live-turn snapshot.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    const after = page.locator("#transcript .msg.server-request");
    await expect(after).toBeVisible();
    await expect(after).toHaveClass(/\bresolved\b/);
    // Never actionable again — re-answering would route a stale id to the harness.
    await expect(after.getByRole("button", { name: "Continue", exact: true })).toBeDisabled();
    await expect(after.getByRole("button", { name: "Cancel", exact: true })).toBeDisabled();
    await expect(page.locator("#transcript .msg.error")).toHaveCount(0);
    // The turn is still in flight, so the thread legitimately still shows activity — but it must no
    // longer claim to be waiting on the user for input they already gave.
    await expect(page.locator(".thread.active .thread-status")).not.toHaveAttribute(
      "title",
      /Waiting for input/,
    );
  });

  test("an unanswered request survives a reload as actionable", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await page.locator("#input").fill(SCRIPTED_SERVER_REQUEST_TRIGGER);
    await page.locator("#sendBtn").click();

    const request = page.locator("#transcript .msg.server-request");
    await expect(request).toBeVisible();

    // The mirror image of the test above: nothing was answered, so the reload must bring the
    // request back still actionable rather than swallowing it.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    const after = page.locator("#transcript .msg.server-request");
    await expect(after).toBeVisible();
    await expect(after).not.toHaveClass(/\bresolved\b/);
    await expect(after.getByRole("button", { name: "Continue", exact: true })).toBeEnabled();
    await expect(after.locator("select.server-request-answer")).toBeEnabled();
  });
});
