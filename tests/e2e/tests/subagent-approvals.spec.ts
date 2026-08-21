import { test, expect, type Page } from "@playwright/test";
import {
  SCRIPTED_APPROVAL_SUBAGENT_NAME,
  SCRIPTED_SUBAGENT_APPROVAL_COMMAND,
  SCRIPTED_SUBAGENT_APPROVAL_ID,
  SCRIPTED_SUBAGENT_APPROVAL_TRIGGER,
  login,
  recordedNotifications,
  selectedThread,
  stubNotifications,
} from "./helpers";

/** Ask the server for the sub-agent child spawned under `parentTid`. */
function childThreadId(page: Page, pid: string, parentTid: string): Promise<string | null> {
  return page.evaluate(
    async ({ pid, parentTid }) => {
      const res = await fetch(`/api/projects/${pid}/threads`);
      const body = await res.json();
      const child = body.threads.find(
        (t: { kind?: string; parent_thread_id?: string }) =>
          t.kind === "subagent" && String(t.parent_thread_id || "") === parentTid,
      );
      return child ? String(child.id) : null;
    },
    { pid, parentTid },
  );
}

/**
 * Notifications recorded for one specific child thread's approval.
 *
 * Scoping to the thread matters: the replay server is shared across the suite and every
 * approval-blocked child holds its turn open forever, so earlier tests leave blocked threads behind
 * — and they all reuse the same fixed approval id. A connecting client is now told about every
 * outstanding approval, so counting by approval id alone would tally other tests' leftovers.
 */
async function approvalNotificationsFor(page: Page, childThread: string): Promise<number> {
  const notifications = await recordedNotifications(page);
  return notifications.filter(
    (n) => n.data?.requestId === SCRIPTED_SUBAGENT_APPROVAL_ID && n.data?.threadId === childThread,
  ).length;
}

/** Start a thread on the seeded Demo project with `text` and return its `{ pid, tid }`. */
async function startThreadWith(page: Page, text: string): Promise<{ pid: string; tid: string }> {
  const project = page.locator(".proj", { hasText: "Demo" });
  await project.locator(".project-add").click();
  await page.locator("#input").fill(text);
  await page.locator("#sendBtn").click();
  await expect.poll(async () => (await selectedThread(page))?.tid ?? null).not.toBeNull();
  const selection = await selectedThread(page);
  return { pid: selection!.pid, tid: selection!.tid };
}

// A managed sub-agent has no sidebar row of its own, so everything keyed off a thread id used to
// no-op for it: no status symbol anywhere, an anonymous notification, and a click that dead-ended on
// "Approval thread is not in the current project list." The child simply looked hung. These tests
// pin the three surfaces that now report it.
test.describe("sub-agent blocked on an approval", () => {
  test.beforeEach(async ({ page }) => {
    await stubNotifications(page);
    await login(page);
  });

  test("reports on the parent row, the sub-agents button, and a named notification", async ({ page }) => {
    // The parent's own turn completes; the child then blocks on an approval while the user is still
    // sitting on the parent thread.
    const parent = await startThreadWith(page, SCRIPTED_SUBAGENT_APPROVAL_TRIGGER);
    const parentRow = page.locator(`.thread[data-tid="${parent.tid}"]`);
    await expect(parentRow).toBeVisible();

    // 1. The parent row carries the child's state: `!`, the approval colour, and the marker saying
    //    the state was hoisted from a descendant rather than being the row's own.
    await expect(parentRow).toHaveClass(/\bactivity-waiting\b/);
    await expect(parentRow).toHaveClass(/\bactivity-subagent\b/);
    const status = parentRow.locator(".thread-status");
    await expect(status).toHaveText("!");
    // The tooltip names the blocked sub-agent, so the row is not an unattributable badge.
    await expect(status).toHaveAttribute(
      "title",
      new RegExp(`${SCRIPTED_APPROVAL_SUBAGENT_NAME}: Approval requested`),
    );

    // The child is deliberately absent from the sidebar; only the ancestor reports it.
    const child = await childThreadId(page, parent.pid, parent.tid);
    expect(child).toBeTruthy();
    await expect(page.locator(`.thread[data-tid="${child}"]`)).toHaveCount(0);

    // 2. The header monitor separates "waiting on the user" from "busy".
    const subagentsBtn = page.locator("#subagentsBtn");
    await expect(subagentsBtn).toHaveClass(/\bstate-waiting\b/);
    await expect(subagentsBtn).toHaveAttribute("title", /waiting on you/);
    await subagentsBtn.click();
    const card = page.locator("#subagentsMenu .subagent-card");
    await expect(card).toHaveCount(1);
    await expect(card).toHaveClass(/\bwaiting\b/);
    await expect(card.locator(".subagent-card-state")).toHaveText("Waiting on you");
    await expect(page.locator("#subagentsMenu .subagents-summary")).toHaveText(
      /1 waiting on you/,
    );
    await page.locator("#subagentsClose").click();

    // 3. The notification names the child and its parent instead of a bare id prefix.
    await expect.poll(async () => (await recordedNotifications(page)).length).toBeGreaterThan(0);
    const approval = (await recordedNotifications(page)).find(
      (n) => n.data?.requestId === SCRIPTED_SUBAGENT_APPROVAL_ID && n.data?.threadId === child,
    );
    expect(approval).toBeTruthy();
    expect(approval!.title).toBe("Giskard: sub-agent approval needed");
    expect(approval!.body).toContain(`${SCRIPTED_APPROVAL_SUBAGENT_NAME} (in `);
    expect(approval!.body).toContain("Approval requested");
    expect(approval!.data?.threadId).toBe(child);
  });

  test("clicking the notification opens the child and the approval is answerable", async ({ page }) => {
    const parent = await startThreadWith(page, SCRIPTED_SUBAGENT_APPROVAL_TRIGGER);
    const parentRow = page.locator(`.thread[data-tid="${parent.tid}"]`);
    await expect(parentRow).toHaveClass(/\bactivity-waiting\b/);
    const child = await childThreadId(page, parent.pid, parent.tid);
    expect(child).toBeTruthy();

    // Both delivery paths (service-worker postMessage and desktop Notification.onclick) funnel into
    // this one handler, so driving it directly exercises the navigation fix faithfully.
    await page.evaluate(
      ({ child, approvalId }) =>
        (window as unknown as {
          handleNotificationClick: (data: { threadId: string; requestId: string }) => void;
        }).handleNotificationClick({ threadId: child, requestId: approvalId }),
      { child: child!, approvalId: SCRIPTED_SUBAGENT_APPROVAL_ID },
    );

    // It navigates to the sub-agent instead of warning that the thread is not in the project list.
    await expect.poll(async () => (await selectedThread(page))?.tid).toBe(child);
    await expect(page.locator("#notices .warn")).toHaveCount(0);
    await expect(page.getByRole("button", { name: /Back to parent thread:/ })).toBeVisible();

    // The approval is live and answerable on the child thread.
    const transcript = page.locator("#transcript");
    const approvalCard = transcript.locator(".msg.approval");
    await expect(approvalCard).toBeVisible();
    await expect(approvalCard).toContainText(SCRIPTED_SUBAGENT_APPROVAL_COMMAND);
    await approvalCard.getByRole("button", { name: "Accept", exact: true }).click();
    await expect(approvalCard).toHaveClass(/\bresolved\b/);
    await expect(
      transcript.locator(".msg.agent", { hasText: "Approval recorded: accept" }),
    ).toBeVisible();

    // Opening the child clears the escalation from its ancestor: the row may still report the child
    // as running, but it must stop demanding attention.
    await expect(parentRow).not.toHaveClass(/\bactivity-waiting\b/);
    await expect(page.locator("#subagentsBtn")).not.toHaveClass(/\bstate-waiting\b/);
  });

  // The hoisting rules decide which of several states one row shows. Drive them through the app's
  // own entry points so the priority order and the cycle guard are pinned without depending on
  // harness timing.
  test("hoists the most urgent descendant state and survives corrupted ownership", async ({ page }) => {
    const result = await page.evaluate(() => {
      const app = window as unknown as {
        rememberProjectThreads: (pid: string, threads: unknown[]) => void;
        handleThreadActivity: (msg: Record<string, unknown>) => void;
        clearApprovalThreadActivity: (tid: string, approvalId: string) => void;
        activityHostThreadId: (tid: string) => string | null;
        threadDescendsFrom: (tid: string, ancestorId: string) => boolean;
        effectiveThreadActivity: (tid: string) => {
          activity: { kind: string } | null;
          origin: string | null;
        };
        renderThreadActivityIndicator: (tid: string) => void;
      };

      // A synthetic project: root (the only thread with a sidebar row) → child → grandchild, plus a
      // self-referential record standing in for corrupted ownership metadata.
      app.rememberProjectThreads("synthetic-project", [
        {
          id: "root", revision: 1, kind: "primary", mode: "build",
          parent_thread_id: null, title: "Root",
        },
        {
          id: "child", revision: 1, kind: "subagent", mode: "build",
          parent_thread_id: "root", title: "Sub-agent: Child",
        },
        {
          id: "grandchild", revision: 1, kind: "subagent", mode: "build",
          parent_thread_id: "child", title: "Sub-agent: Grandchild",
        },
        {
          id: "loop", revision: 1, kind: "subagent", mode: "build",
          parent_thread_id: "loop", title: "Sub-agent: Loop",
        },
      ]);
      const row = document.createElement("div");
      row.className = "thread";
      row.dataset.tid = "root";
      row.dataset.pid = "synthetic-project";
      const status = document.createElement("span");
      status.className = "thread-status";
      row.append(status);
      document.body.append(row);

      try {
        // A merely-running root must not mask a grandchild blocked on an approval.
        app.handleThreadActivity({
          thread_id: "root", kind: "progress", active_turn: true, summary: "Turn running",
        });
        app.handleThreadActivity({
          thread_id: "grandchild", kind: "approval_requested", active_turn: true,
          approval_id: "a1", summary: "Approval requested",
        });
        const hoisted = {
          host: app.activityHostThreadId("grandchild"),
          winner: app.effectiveThreadActivity("root").activity?.kind ?? null,
          origin: app.effectiveThreadActivity("root").origin,
          statusText: status.textContent,
          rowClass: row.className,
          title: status.title,
          descends: app.threadDescendsFrom("grandchild", "root"),
          unrelated: app.threadDescendsFrom("root", "grandchild"),
        };

        // Once answered, the row falls back to root's own running state and drops the escalation.
        app.clearApprovalThreadActivity("grandchild", "a1");
        const afterAnswer = { statusText: status.textContent, rowClass: row.className };

        // Corrupted ownership (a thread that is its own parent) must terminate, not spin.
        const cycle = {
          host: app.activityHostThreadId("loop"),
          descends: app.threadDescendsFrom("loop", "root"),
        };
        return { hoisted, afterAnswer, cycle };
      } finally {
        row.remove();
      }
    });

    expect(result.hoisted.host).toBe("root");
    expect(result.hoisted.winner).toBe("approval_requested");
    expect(result.hoisted.origin).toBe("grandchild");
    expect(result.hoisted.statusText).toBe("!");
    expect(result.hoisted.rowClass).toContain("activity-waiting");
    expect(result.hoisted.rowClass).toContain("activity-subagent");
    // Named after the blocked descendant, not the row it is displayed on.
    expect(result.hoisted.title).toBe("Grandchild: Approval requested");
    expect(result.hoisted.descends).toBe(true);
    expect(result.hoisted.unrelated).toBe(false);

    expect(result.afterAnswer.statusText).toBe("o");
    expect(result.afterAnswer.rowClass).not.toContain("activity-waiting");
    expect(result.afterAnswer.rowClass).not.toContain("activity-subagent");

    expect(result.cycle.host).toBeNull();
    expect(result.cycle.descends).toBe(false);
  });

  // Thread activity is broadcast live and never replayed, so a browser that was not running when a
  // sub-agent blocked used to come back to a completely silent sidebar — the child has no row of
  // its own, and the approval only reaches the transcript once that thread is opened. A reload is
  // the cleanest way to be "a browser that missed it": the page starts with empty activity state.
  test("a reload learns about an approval it was not connected for", async ({ page }) => {
    const parent = await startThreadWith(page, SCRIPTED_SUBAGENT_APPROVAL_TRIGGER);
    const parentRow = page.locator(`.thread[data-tid="${parent.tid}"]`);
    await expect(parentRow).toHaveClass(/\bactivity-waiting\b/);
    const child = await childThreadId(page, parent.pid, parent.tid);
    expect(child).toBeTruthy();

    // The scripted child holds its turn open forever, so the approval is still outstanding here.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    // Rebuilt from nothing, the sidebar still reports the blocked descendant.
    const reloadedRow = page.locator(`.thread[data-tid="${parent.tid}"]`);
    await expect(reloadedRow).toHaveClass(/\bactivity-waiting\b/);
    await expect(reloadedRow).toHaveClass(/\bactivity-subagent\b/);
    await expect(reloadedRow.locator(".thread-status")).toHaveText("!");
    // A reload is a new page session, so the alert fires again rather than being suppressed.
    await expect.poll(() => approvalNotificationsFor(page, child!)).toBe(1);
  });

  // An id that no refresh can resolve — trailing activity from a deleted thread, say — must not
  // re-fetch every project list on every event for the rest of the turn.
  test("spends a bounded number of refreshes on an unresolvable thread", async ({ page }) => {
    let threadListFetches = 0;
    page.on("request", (request) => {
      if (request.method() === "GET" && /\/api\/projects\/[^/]+\/threads$/.test(request.url())) {
        threadListFetches += 1;
      }
    });
    // Settle any refresh the login/bootstrap path already scheduled.
    await page.waitForTimeout(1000);
    const before = threadListFetches;

    await page.evaluate(async () => {
      const app = window as unknown as {
        handleThreadActivity: (msg: Record<string, unknown>) => void;
      };
      // Twelve events for a thread the server will never list. Each is spaced past the burst
      // debounce so an ungated implementation would schedule a refresh for every one.
      for (let i = 0; i < 12; i += 1) {
        app.handleThreadActivity({
          thread_id: "thread-that-does-not-exist",
          kind: "progress",
          active_turn: true,
          summary: `tick ${i}`,
        });
        await new Promise((resolve) => setTimeout(resolve, 120));
      }
    });
    await page.waitForTimeout(1500);

    const projects = await page.evaluate(() => document.querySelectorAll(".proj").length);
    // At most STALE_THREAD_LIST_REFRESH_MAX_ATTEMPTS (3) refreshes, each reloading every project.
    expect(threadListFetches - before).toBeLessThanOrEqual(3 * projects);
    expect(threadListFetches - before).toBeGreaterThan(0);
  });

  // Two events can describe one approval (the live activity broadcast and the live-turn snapshot
  // replay). Only one notification may reach the user, even though the handler now awaits a
  // thread-list refresh between the dedup check and the record.
  test("notifies once when two events describe the same approval", async ({ page }) => {
    const parent = await startThreadWith(page, SCRIPTED_SUBAGENT_APPROVAL_TRIGGER);
    await expect(page.locator(`.thread[data-tid="${parent.tid}"]`)).toHaveClass(
      /\bactivity-waiting\b/,
    );
    const child = await childThreadId(page, parent.pid, parent.tid);
    // Without this the events below would fire with a null thread id, be ignored, and leave the
    // final count at 1 — green without ever reaching the dedup gate.
    expect(child).toBeTruthy();
    expect(await approvalNotificationsFor(page, child!)).toBe(1);

    // Fire two more events for the same approval in the same tick, so both reach the dedup gate
    // before either records. Neither may notify again.
    await page.evaluate(
      ({ child, approvalId }) => {
        const app = window as unknown as {
          handleThreadActivity: (msg: Record<string, unknown>) => void;
        };
        const event = {
          thread_id: child,
          kind: "approval_requested",
          active_turn: true,
          approval_id: approvalId,
          summary: "Approval requested",
        };
        app.handleThreadActivity({ ...event });
        app.handleThreadActivity({ ...event });
      },
      { child: child!, approvalId: SCRIPTED_SUBAGENT_APPROVAL_ID },
    );
    await page.waitForTimeout(1000);

    expect(await approvalNotificationsFor(page, child!)).toBe(1);
  });
});
