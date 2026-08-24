import { test, expect, type Page, type Route } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

async function createThread(page: Page, message: string): Promise<string> {
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill(message);
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  const tid = await page.locator(".thread.active").getAttribute("data-tid");
  expect(tid).toBeTruthy();
  return tid as string;
}

function nextRoute(): {
  promise: Promise<Route>;
  resolve: (route: Route) => void;
} {
  let resolve!: (route: Route) => void;
  return { promise:new Promise<Route>(done => { resolve = done; }), resolve };
}

test.describe("thread metadata reconciliation", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("rejects older detail and uses an equal snapshot to repair rendered controls", async ({ page }) => {
    const tid = await createThread(page, "Exercise revisioned metadata reconciliation.");
    const result = await page.evaluate((threadId) => {
      const app = window as any;
      const current = app.composedThreadDetail(threadId);
      const newer = { ...current, revision:current.revision + 1, title:"Newest authority" };
      app.applyThreadMetadata(newer);
      app.applyThreadMetadata({ ...current, title:"Stale authority" });

      const mode = document.querySelector<HTMLSelectElement>("#modeSel")!;
      mode.value = newer.mode === "plan" ? "build" : "plan";
      document.querySelector(`.thread[data-tid="${threadId}"] .thread-title`)!.textContent =
        "Corrupted sidebar";
      app.applyThreadMetadata(newer);
      const conflict = app.reconcileThreadProjection("detail", {
        ...newer,
        context_window:newer.context_window + 1,
      });
      return {
        title:app.composedThreadDetail(threadId).title,
        renderedMode:mode.value,
        authoritativeMode:newer.mode,
        sidebarTitle:document.querySelector(
          `.thread[data-tid="${threadId}"] .thread-title`,
        )!.textContent,
        conflictAccepted:conflict.accepted,
        contextWindow:app.composedThreadDetail(threadId).context_window,
        expectedContextWindow:newer.context_window,
      };
    }, tid);

    expect(result.title).toBe("Newest authority");
    expect(result.renderedMode).toBe(result.authoritativeMode);
    expect(result.sidebarTitle).toBe("Newest authority");
    expect(result.conflictAccepted).toBe(false);
    expect(result.contextWindow).toBe(result.expectedContextWindow);
  });

  test("repairs an equal-revision detail conflict on the same socket", async ({ page }) => {
    const tid = await createThread(page, "Exercise detail conflict recovery.");
    let replacementSockets = 0;
    page.on("websocket", () => { replacementSockets++; });
    const canonical = await page.evaluate((threadId) => {
      const app = window as any;
      const current = app.composedThreadDetail(threadId);
      app.applyThreadMetadata({ ...current, title:"Impossible same-revision title" });
      return current.title;
    }, tid);

    await expect(page.locator("#wsStatusBadge")).toHaveClass(/state-open/);
    await expect.poll(() => page.evaluate(
      (threadId) => (window as any).composedThreadDetail(threadId)?.title,
      tid,
    )).toBe(canonical);
    await expect(page.locator(`.thread[data-tid="${tid}"] .thread-title`)).toHaveText(canonical);
    expect(replacementSockets).toBe(0);
  });

  test("uses one catalog refetch to replace a conflicting cached summary", async ({ page }) => {
    const seed = await page.evaluate(() => {
      const app = window as any;
      const summary = {
        id:"synthetic-thread", revision:7, title:"Cached title", workspace_root:"/tmp",
        parent_thread_id:null, spawned_by_turn_id:null, mode:"build", archived:false,
        created_at:"2026-08-21T00:00:00Z", updated_at:"2026-08-21T00:00:00Z",
      };
      app.rememberProjectThreads("synthetic-project", [summary]);
      return { ...summary, title:"Authoritative title" };
    });
    let requests = 0;
    await page.route("**/api/projects/synthetic-project/threads", async (route) => {
      requests++;
      await route.fulfill({ json:{ threads:[seed] } });
    });

    await page.evaluate(() => (window as any).loadThreads("synthetic-project"));

    expect(requests).toBe(2);
    expect(await page.evaluate(() =>
      (window as any).composedThreadSummary("synthetic-thread")?.title,
    )).toBe("Authoritative title");
  });

  test("discards catalog membership fetched before a newer invalidation", async ({ page }) => {
    const tid = await createThread(page, "Exercise catalog invalidation ordering.");
    const projectId = await page.locator(".thread.active").getAttribute("data-pid");
    expect(projectId).toBeTruthy();
    const latest = await page.evaluate((threadId) => {
      const app = window as any;
      const current = app.composedThreadSummary(threadId);
      return { ...current, revision:current.revision + 1, title:"Catalog catch-up" };
    }, tid);

    const first = nextRoute();
    const second = nextRoute();
    let requests = 0;
    await page.route(`**/api/projects/${projectId}/threads`, async (route) => {
      requests++;
      (requests === 1 ? first : second).resolve(route);
    });

    await page.evaluate((pid) => { void (window as any).loadThreads(pid); }, projectId);
    const firstRoute = await first.promise;
    await page.evaluate((pid) => { void (window as any).loadThreads(pid); }, projectId);
    await firstRoute.fulfill({ json:{ threads:[] } });
    const secondRoute = await second.promise;

    // The first response's empty membership must never commit while the catch-up request is held.
    await expect(page.locator(`.thread[data-tid="${tid}"]`)).toBeVisible();
    await secondRoute.fulfill({ json:{ threads:[latest] } });
    await expect(page.locator(`.thread[data-tid="${tid}"] .thread-title`)).toHaveText(
      "Catalog catch-up",
    );
    expect(requests).toBe(2);
  });

  test("retries a failed catalog invalidation without another notification", async ({ page }) => {
    const tid = await createThread(page, "Exercise catalog retry.");
    const projectId = await page.locator(".thread.active").getAttribute("data-pid");
    expect(projectId).toBeTruthy();
    const latest = await page.evaluate((threadId) => {
      const app = window as any;
      const current = app.composedThreadSummary(threadId);
      return { ...current, revision:current.revision + 1, title:"Recovered catalog" };
    }, tid);

    let requests = 0;
    await page.route(`**/api/projects/${projectId}/threads`, async (route) => {
      requests++;
      if (requests === 1) {
        await route.fulfill({ status:503, json:{ error:"temporary" } });
      } else {
        await route.fulfill({ json:{ threads:[latest] } });
      }
    });

    await page.evaluate((pid) => { void (window as any).loadThreads(pid); }, projectId);
    await expect(page.locator(`.thread[data-tid="${tid}"] .thread-title`)).toHaveText(
      "Recovered catalog",
    );
    expect(requests).toBe(2);
  });

  test("rejects a malformed catalog row before changing membership and retries", async ({ page }) => {
    const tid = await createThread(page, "Exercise malformed catalog recovery.");
    const projectId = await page.locator(".thread.active").getAttribute("data-pid");
    expect(projectId).toBeTruthy();
    const current = await page.evaluate((threadId) =>
      (window as any).composedThreadSummary(threadId), tid,
    );
    const retry = nextRoute();
    let requests = 0;
    await page.route(`**/api/projects/${projectId}/threads`, async (route) => {
      requests++;
      if (requests === 1) {
        await route.fulfill({ json:{ threads:[{ ...current, revision:"invalid" }] } });
      } else {
        retry.resolve(route);
      }
    });

    const refreshPromise = page.evaluate((pid) => (window as any).loadThreads(pid), projectId);
    const retryRoute = await retry.promise;
    await expect(page.locator(`.thread[data-tid="${tid}"]`)).toBeVisible();
    expect(await page.evaluate((threadId) =>
      (window as any).composedThreadSummary(threadId)?.revision, tid,
    )).toBe(current.revision);
    await retryRoute.fulfill({ json:{ threads:[current] } });
    await refreshPromise;
    await expect.poll(() => requests).toBe(2);
  });

  test("retires an open thread removed by an authoritative catalog response", async ({ page }) => {
    const tid = await createThread(page, "Exercise catalog membership removal.");
    const projectId = await page.locator(".thread.active").getAttribute("data-pid");
    expect(projectId).toBeTruthy();

    await page.route(`**/api/projects/${projectId}/threads`, async (route) => {
      await route.fulfill({ json:{ threads:[] } });
    });
    await page.evaluate((pid) => (window as any).loadThreads(pid), projectId);

    await expect(page.locator(`.thread[data-tid="${tid}"]`)).toHaveCount(0);
    await expect(page.locator("#composer")).toBeVisible();
    await expect(page.locator("#mbTitle")).toContainText("New thread");
    await expect(page.locator("#transcript .draft-empty")).toBeVisible();
    await expect(page.locator("#notices .notice.warn")).toHaveCount(1);
    expect(await page.evaluate((threadId) =>
      (window as any).composedThreadDetail(threadId), tid,
    )).toBeNull();
  });

  test("settles a control overlay from its correlated metadata result", async ({ page }) => {
    const tid = await createThread(page, "Exercise correlated metadata results.");
    const mode = page.locator("#modeSel");
    const nextMode = await mode.inputValue() === "plan" ? "build" : "plan";

    await page.locator("#turnPickerBtn").click();
    await mode.selectOption(nextMode);
    await expect(mode).toHaveValue(nextMode);
    await expect(mode).toBeEnabled();
    await expect.poll(() => page.evaluate(
      (threadId) => (window as any).pendingMetadataGroup(threadId, "mode"),
      tid,
    )).toBe(false);
    expect(await page.evaluate(
      (threadId) => (window as any).composedThreadDetail(threadId).mode,
      tid,
    )).toBe(nextMode);
  });
});
