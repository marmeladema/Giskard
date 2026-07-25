import { test, expect } from "@playwright/test";
import {
  SCRIPTED_SERVER_REQUEST_ID,
  SCRIPTED_SERVER_REQUEST_QUESTION,
  SCRIPTED_SERVER_REQUEST_TRIGGER,
  login,
  recordedNotifications,
  stubNotifications,
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
    await stubNotifications(page);
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
      /Waiting for your input/,
    );
  });

  // A server request blocks the turn on the user exactly as an approval does, so it must read the
  // same way in the sidebar. Before this it fell through to the generic "active turn" branch and
  // rendered as `o` — indistinguishable from a thread that was merely busy.
  test("a thread waiting for input reads as waiting, not merely running", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await page.locator("#input").fill(SCRIPTED_SERVER_REQUEST_TRIGGER);
    await page.locator("#sendBtn").click();

    const request = page.locator("#transcript .msg.server-request");
    await expect(request).toBeVisible();

    const row = page.locator(".thread.active");
    await expect(row).toHaveClass(/\bactivity-waiting\b/);
    await expect(row).not.toHaveClass(/\bactivity-running\b/);
    const status = row.locator(".thread-status");
    await expect(status).toHaveText("!");
    await expect(status).toHaveAttribute("title", /Waiting for your input/);

    // Answering hands the turn back to the agent, so the row drops to plain running.
    await request.locator("select.server-request-answer").selectOption("main");
    await request.getByRole("button", { name: "Continue", exact: true }).click();
    await expect(request.locator(".server-request-sent")).toBeVisible();
    await expect(row).not.toHaveClass(/\bactivity-waiting\b/);
    await expect(status).not.toHaveText("!");
  });

  // The two kinds share the waiting state but not the copy: telling someone an approval is needed
  // when the agent asked them a question sends them looking for a decision that does not exist.
  test("notifies about input rather than approval", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await page.locator("#input").fill(SCRIPTED_SERVER_REQUEST_TRIGGER);
    await page.locator("#sendBtn").click();
    await expect(page.locator("#transcript .msg.server-request")).toBeVisible();

    // The current thread is visible and focused, so the live event suppresses its own notification;
    // drive the shared entry point directly to inspect the headline it would produce.
    const title = await page.evaluate(async () => {
      const app = window as unknown as {
        maybeNotifyWaitingRequest: (tid: string, activity: Record<string, unknown>) => Promise<void>;
      };
      await app.maybeNotifyWaitingRequest("some-other-thread", {
        kind: "server_request_received",
        active_turn: true,
        approval_id: null,
        server_request_id: "sr-headline-probe",
        summary: "Waiting for your input",
        source: "test",
        unread: true,
      });
      const calls = (window as Window).__giskardNotifications ?? [];
      return calls[calls.length - 1]?.title ?? null;
    });
    expect(title).toBe("Giskard: input needed");
  });

  // The reconnect snapshot replays every ServerRequestReceived, answered ones included. An answered
  // one must not re-alert: the user already dealt with it. The approval path has always guarded
  // this; without the same guard here a backgrounded tab gets pinged for work already done.
  test("an answered request does not re-notify on reconnect", async ({ page }) => {
    // Notifications are suppressed while the page is visible AND focused AND on that thread, which
    // would mask the bug. Unfocus so the reconnect path is actually allowed to alert.
    await page.addInitScript(() => { document.hasFocus = () => false; });

    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await page.locator("#input").fill(SCRIPTED_SERVER_REQUEST_TRIGGER);
    await page.locator("#sendBtn").click();
    const request = page.locator("#transcript .msg.server-request");
    await expect(request).toBeVisible();
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    expect(tid).toBeTruthy();

    await request.locator("select.server-request-answer").selectOption("main");
    await request.getByRole("button", { name: "Continue", exact: true }).click();
    await expect(request.locator(".server-request-sent")).toBeVisible();

    // The scripted harness never resolves, so the answer is known only from the server's record.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);
    await expect(page.locator("#transcript .msg.server-request")).toHaveClass(/\bresolved\b/);

    await page.waitForTimeout(1000);
    // Scope to this test's thread. The replay server is shared and earlier tests leave outstanding
    // requests behind, all reusing the same scripted request id — the connect bootstrap replays
    // those too, so filtering by id alone would count another test's leftovers.
    const notifications = await recordedNotifications(page);
    expect(
      notifications.filter(
        (n) => n.data?.requestId === SCRIPTED_SERVER_REQUEST_ID && n.data?.threadId === tid,
      ),
    ).toEqual([]);
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
