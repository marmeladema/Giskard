import { test, expect } from "@playwright/test";
import { login, recordedNotifications, stubNotifications } from "./helpers";

test("an older request replacement cannot resurrect a resolved request", async ({ page }) => {
  await login(page);

  const remainsResolved = await page.evaluate(() => {
    const app = window as unknown as {
      handleRequestState: (state: Record<string, unknown>) => void;
      renderServerRequest: (request: Record<string, unknown>) => void;
    };
    app.renderServerRequest({ id: "request-a", method: "test/request", params: {} });
    app.handleRequestState({
      thread_id: "thread-a",
      request_id: "request-a",
      revision: 3,
      payload: { kind: "server", request: {} },
      status: { status: "resolved", resolution: { kind: "server" } },
    });
    app.handleRequestState({
      thread_id: "thread-a",
      request_id: "request-a",
      revision: 2,
      payload: { kind: "server", request: {} },
      status: { status: "pending" },
    });

    return document.querySelector('[data-server-request-id="request-a"]')?.classList.contains("resolved");
  });

  expect(remainsResolved).toBe(true);
});

test("an approval card becomes actionable again after a response error", async ({ page }) => {
  await login(page);

  const actionable = await page.evaluate(() => {
    const app = window as unknown as {
      renderApprovalRequest: (request: Record<string, unknown>) => void;
      handleServer: (message: Record<string, unknown>, socket: Record<string, unknown>) => void;
    };
    app.renderApprovalRequest({
      id: "retry-approval",
      kind: { kind: "permission", detail: "test" },
      available: ["accept", "decline"],
      metadata: [],
    });
    const row = document.querySelector<HTMLElement>('[data-approval-id="retry-approval"]')!;
    row.dataset.resolving = "true";
    row.querySelectorAll<HTMLButtonElement>("button").forEach((button) => { button.disabled = true; });
    app.handleServer({
      type: "error",
      action: "approval_decision",
      code: "harness_error",
      message: "temporary failure",
    }, {});
    return row.dataset.resolving === "false"
      && Array.from(row.querySelectorAll<HTMLButtonElement>("button")).every((button) => !button.disabled);
  });

  expect(actionable).toBe(true);
});

test("runtime overviews notify a pending request only once per page session", async ({ page }) => {
  await stubNotifications(page);
  await login(page);

  const threadId = "overview-thread";
  await page.evaluate((tid) => {
    const testWindow = window as Window & { __giskardTestNow?: number };
    testWindow.__giskardTestNow = 1_000;
    Date.now = () => testWindow.__giskardTestNow!;
    const app = window as unknown as {
      handleThreadRuntimeOverview: (overview: Record<string, unknown>) => void;
    };
    app.handleThreadRuntimeOverview({
      revision: 100,
      threads: [{
        thread_id: tid,
        turn_state: { state: "active" },
        outstanding_requests: [{ request_id: "overview-request", kind: "approval", responding: false }],
      }],
    });
  }, threadId);
  await expect.poll(async () => (await recordedNotifications(page)).length).toBe(1);

  await page.evaluate((tid) => {
    const testWindow = window as Window & { __giskardTestNow?: number };
    testWindow.__giskardTestNow = 17_001;
    const app = window as unknown as {
      handleThreadRuntimeOverview: (overview: Record<string, unknown>) => void;
    };
    app.handleThreadRuntimeOverview({
      revision: 101,
      threads: [{
        thread_id: tid,
        turn_state: { state: "active" },
        outstanding_requests: [{ request_id: "overview-request", kind: "approval", responding: false }],
      }],
    });
  }, threadId);

  await page.waitForTimeout(250);
  expect(await recordedNotifications(page)).toHaveLength(1);
});

test("a runtime overview does not alert for a request the user already answered", async ({ page }) => {
  await stubNotifications(page);
  await login(page);

  await page.evaluate((tid) => {
    const app = window as unknown as {
      handleThreadRuntimeOverview: (overview: Record<string, unknown>) => void;
    };
    // The server keeps a responding request in outstanding_requests until the harness settles it.
    // Nothing here is waiting on the user, so nothing should alert.
    app.handleThreadRuntimeOverview({
      revision: 200,
      threads: [{
        thread_id: tid,
        turn_state: { state: "active" },
        outstanding_requests: [{ request_id: "answered-request", kind: "approval", responding: true }],
      }],
    });
  }, "responding-only-thread");

  await page.waitForTimeout(250);
  expect(await recordedNotifications(page)).toHaveLength(0);
});

test("a completed request ID can be reused by a later turn", async ({ page }) => {
  await login(page);

  const reusedRequestIsPending = await page.evaluate(() => {
    const app = window as unknown as {
      handleRequestState: (state: Record<string, unknown>) => void;
      clearCompletedRequestState: () => void;
      renderServerRequest: (request: Record<string, unknown>) => void;
    };
    app.handleRequestState({
      thread_id: "thread-a",
      request_id: "provider-reused-id",
      revision: 3,
      payload: { kind: "server", request: {} },
      status: { status: "resolved", resolution: { kind: "server" } },
    });
    app.clearCompletedRequestState();
    app.renderServerRequest({ id: "provider-reused-id", method: "test/request", params: {} });
    app.handleRequestState({
      thread_id: "thread-a",
      request_id: "provider-reused-id",
      revision: 1,
      payload: { kind: "server", request: {} },
      status: { status: "pending" },
    });

    return !document.querySelector('[data-server-request-id="provider-reused-id"]')?.classList.contains("resolved");
  });

  expect(reusedRequestIsPending).toBe(true);
});
