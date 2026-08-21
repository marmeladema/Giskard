import { test, expect } from "@playwright/test";
import {
  SCRIPTED_SERVER_REQUEST_ID,
  SCRIPTED_SERVER_REQUEST_QUESTION,
  SCRIPTED_SERVER_REQUEST_TRIGGER,
  SCRIPTED_SERVER_REQUEST_THEN_ERROR_MESSAGE,
  SCRIPTED_SERVER_REQUEST_THEN_ERROR_TRIGGER,
  login,
  recordedNotifications,
  stubNotifications,
} from "./helpers";

// Server requests (`requestUserInput` and friends) had no browser coverage at all, despite having
// real UI: a card, per-question controls, Continue/Cancel, and a resolved state.
//
// The gap that matters is the one already fixed for approvals. Answering used to record nothing
// server-side, so a reload could reconstruct the request as actionable — and answering a second
// time routed a stale id to the harness, which errored. The ordered request chronology and final
// runtime state must instead agree that the request was answered.
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

    // The transient sent marker can be replaced immediately by the authoritative `resolved` state.
    // Wait for that state before reloading so this test does not race the response round trip.
    await expect(request).toHaveClass(/\bresolved\b/);
    await expect(request.locator(".server-request-sent")).toHaveText("Resolved");
    await expect(continueBtn).toHaveCount(0);

    // Reload: in-memory state is wiped, so the resolved state has to be reconstructed entirely from
    // ordered request chronology and the authoritative runtime state.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    const after = page.locator("#transcript .msg.server-request");
    await expect(after).toBeVisible();
    await expect(after).toHaveClass(/\bresolved\b/);
    // Never actionable again — re-answering would route a stale id to the harness.
    await expect(after.getByRole("button", { name: "Continue", exact: true })).toHaveCount(0);
    await expect(after.getByRole("button", { name: "Cancel", exact: true })).toHaveCount(0);
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

  // Reconciliation includes the whole request chronology, answered requests included. An answered
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

    // Reload only after the authoritative server state has resolved the request.
    await expect(request).toHaveClass(/\bresolved\b/);
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

// The replacement runtime overview is authoritative for what the turn is still waiting on. A later
// transcript `error` must not strand an outstanding request in a thread that reads as idle: the
// runtime request state still gives the waiting activity precedence.
test.describe("a still-blocked turn survives a reload that replays a later error", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("a thread blocked on a server request still reads as waiting after an error", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    await expect(input).toBeVisible();
    await input.fill(SCRIPTED_SERVER_REQUEST_THEN_ERROR_TRIGGER);
    await page.locator("#sendBtn").click();

    // Both arrive in the same still-open turn, the error last.
    await expect(page.locator("#transcript .msg.server-request")).toBeVisible();
    await expect(page.locator("#transcript .msg.error")).toContainText(
      SCRIPTED_SERVER_REQUEST_THEN_ERROR_MESSAGE,
    );

    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    // The error is replayed and still shown — it happened, and the transcript is the record.
    await expect(page.locator("#transcript .msg.error")).toContainText(
      SCRIPTED_SERVER_REQUEST_THEN_ERROR_MESSAGE,
    );
    // But the turn is still blocked on the user, and that outranks the error in the sidebar.
    const row = page.locator(".thread.active");
    await expect(row).toHaveClass(/\bactivity-waiting\b/);
    await expect(row.locator(".thread-status")).toHaveText("!");
    // And the request is still answerable, not stranded in a thread that reads as finished.
    await expect(
      page.locator("#transcript .msg.server-request").getByRole("button", { name: "Continue", exact: true }),
    ).toBeEnabled();
  });
});
