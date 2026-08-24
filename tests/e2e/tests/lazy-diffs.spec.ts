import { test, expect, type Page } from "@playwright/test";
import {
  SCRIPTED_DIFF_PATH,
  SCRIPTED_DIFF_TRIGGER,
  login,
  recordedNotices,
  recordNotices,
} from "./helpers";

async function flushBrowserTasks(page: Page) {
  await page.evaluate(() => new Promise<void>((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
  }));
}

test.describe("lazy agent diffs", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
    await recordNotices(page);
    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  });

  async function startDiffTurn(page: Page) {
    await page.locator("#input").fill(SCRIPTED_DIFF_TRIGGER);
    await page.locator("#sendBtn").click();
    const group = page.locator("#transcript .msg.file", {
      hasText: SCRIPTED_DIFF_PATH,
    });
    await expect(group).toBeVisible();
    return group;
  }

  test("loads a live diff body only when View diff is clicked", async ({ page }) => {
    let requests = 0;
    page.on("request", (request) => {
      if (request.url().includes("/diffs/")) requests += 1;
    });

    const group = await startDiffTurn(page);
    expect(requests).toBe(0);
    await group.locator(".file-change-entry").first().locator(".diff-open").click();

    await expect(page.locator("#codeOverlay")).toHaveClass(/\bopen\b/);
    await expect(page.locator("#codePath")).toHaveText(`Diff: ${SCRIPTED_DIFF_PATH}`);
    await expect(page.locator("#codeView")).toContainText("first version");
    expect(requests).toBe(1);
  });

  test("loads the persisted replacement after a reload", async ({ page }) => {
    const group = await startDiffTurn(page);
    await expect(group.locator(".file-change-status", { hasText: "completed" })).toBeVisible();
    await page.reload();

    const restored = page.locator("#transcript .msg.file", {
      hasText: SCRIPTED_DIFF_PATH,
    });
    await expect(restored).toBeVisible();
    await restored.locator(".diff-open").last().click();
    await expect(page.locator("#codeView")).toContainText("second version");
    await expect(page.locator("#codeView")).not.toContainText("first version");
  });

  test("deduplicates repeated immutable diffs in a collapsed file-change row", async ({ page }) => {
    const rendered = await page.evaluate(() => {
      const row = document.createElement("div");
      row.className = "msg file";
      row.dataset.turn = "turn-1";
      const body = document.createElement("div");
      body.className = "body";
      row.append(body);
      document.querySelector("#transcript")?.append(row);

      renderFileChangeContribution(body, {
        kind: "file_change",
        changes: [{
          path: "same.rs", change: "modified", diff: { id: "same-diff" }, status: "in_progress",
        }],
      }, { id: "item-1" }, "turn-1");
      renderFileChangeContribution(body, {
        kind: "file_change",
        changes: [
          {
            path: "same.rs", change: "modified", diff: { id: "same-diff" }, status: "completed",
          },
          {
            path: "same.rs", change: "modified", diff: { id: "new-diff" }, status: "completed",
          },
        ],
      }, { id: "item-2" }, "turn-1");

      return {
        titles: Array.from(body.querySelectorAll(":scope > div")).map(node => node.textContent),
        entries: Array.from(body.querySelectorAll(".file-change-entry")).map(node => node.textContent),
      };
    });

    expect(rendered.titles).toEqual(["File changes"]);
    expect(rendered.entries).toHaveLength(2);
    expect(rendered.entries[0]).toContain("completed");
  });

  test("renders a full-text-only structured diff from the lazy endpoint", async ({ page }) => {
    const group = await startDiffTurn(page);
    await expect(group.locator(".file-change-status", { hasText: "completed" })).toBeVisible();
    await expect(page.locator("#stopBtn")).toBeHidden();

    const opened = await page.evaluate(async () => {
      const history = await api(
        "GET",
        `/api/projects/${state.projectId}/threads/${state.threadId}/history`,
      );
      const turn = history.turns.find((candidate: { diffs?: Array<{ path?: string }> }) =>
        (candidate.diffs || []).some((diff) => diff.path === "src/full-text-only.rs"),
      );
      const descriptor = turn?.diffs?.find(
        (diff: { path?: string }) => diff.path === "src/full-text-only.rs",
      );
      if (!turn || !descriptor) return false;
      await openCapturedDiff(descriptor, turn.id);
      return true;
    });

    expect(opened).toBe(true);
    await expect(page.locator("#codeView .diff-del")).toContainText("fn old() {}");
    await expect(page.locator("#codeView .diff-add")).toContainText("fn new() {}");

    const fallbacks = await page.evaluate(() => ({
      created: structuredCapturedDiffText({
        path: "created.rs", old_text: null, new_text: "new\n", hunks: [],
      }),
      deleted: structuredCapturedDiffText({
        path: "deleted.rs", old_text: "old\n", new_text: null, hunks: [],
      }),
      frontMatter: structuredCapturedDiffText({
        path: "config.md", old_text: null, new_text: "--- title: Example\nbody\n", hunks: [],
      }),
    }));
    expect(fallbacks.created).toContain("@@ -0,0 +1,1 @@\n+new");
    expect(fallbacks.deleted).toContain("@@ -1,1 +0,0 @@\n-old");
    expect(fallbacks.frontMatter).toContain("+++ b/config.md");
    expect(fallbacks.frontMatter).toContain("+--- title: Example");
  });

  test("ignores a conflict after its rendered diff is replaced", async ({ page }) => {
    let delayed = false;
    await page.route("**/diffs/*", async (route) => {
      if (!delayed) {
        delayed = true;
        await new Promise((resolve) => setTimeout(resolve, 1600));
      }
      await route.continue();
    });

    const responsePromise = page.waitForResponse("**/diffs/*");
    const group = await startDiffTurn(page);
    await group.locator(".file-change-entry").first().locator(".diff-open").click();
    await expect(group.locator(".file-change-status", { hasText: "completed" })).toBeVisible();
    const response = await responsePromise;
    await response.finished();
    await flushBrowserTasks(page);
    expect((await recordedNotices(page)).map((notice) => notice.text)).not.toContain(
      "That diff was replaced while it was loading. Open the current diff to retry.",
    );
    await expect(page.locator("#codeOverlay")).not.toHaveClass(/\bopen\b/);
  });

  test("reports a conflict while the requested descriptor is still current", async ({ page }) => {
    await page.route("**/diffs/*", async (route) => {
      await route.fulfill({
        status: 409,
        contentType: "application/json",
        body: JSON.stringify({ code: "diff_superseded", current: {} }),
      });
    });

    const group = await startDiffTurn(page);
    await group.locator(".file-change-entry").first().locator(".diff-open").click();
    await expect
      .poll(async () => (await recordedNotices(page)).map((notice) => notice.text))
      .toContain("That diff was replaced while it was loading. Open the current diff to retry.");
    await expect(page.locator("#codeOverlay")).not.toHaveClass(/\bopen\b/);
  });

  test("surfaces an ordinary lazy-diff fetch failure", async ({ page }) => {
    await page.route("**/diffs/*", (route) => route.abort("failed"));
    const group = await startDiffTurn(page);
    await group.locator(".file-change-entry").first().locator(".diff-open").click();

    await expect
      .poll(async () => (await recordedNotices(page)).map((notice) => notice.text).join("\n"))
      .toContain("Could not load captured diff:");
    await expect(page.locator("#codeOverlay")).not.toHaveClass(/\bopen\b/);
  });

  test("ignores an old response after the current diff is selected", async ({ page }) => {
    let requestNumber = 0;
    await page.route("**/diffs/*", async (route) => {
      requestNumber += 1;
      if (requestNumber === 1) {
        await new Promise((resolve) => setTimeout(resolve, 1800));
      }
      await route.continue();
    });

    const group = await startDiffTurn(page);
    const firstRequestPromise = page.waitForRequest("**/diffs/*");
    await group.locator(".file-change-entry").first().locator(".diff-open").click();
    const firstRequest = await firstRequestPromise;
    const firstResponsePromise = page.waitForResponse(
      (response) => response.url() === firstRequest.url(),
    );
    await expect(group.locator(".file-change-status", { hasText: "completed" })).toBeVisible();
    await group.locator(".file-change-entry").last().locator(".diff-open").click();
    await expect(page.locator("#codeView")).toContainText("second version");

    await expect.poll(() => requestNumber).toBe(2);
    const firstResponse = await firstResponsePromise;
    await firstResponse.finished();
    await flushBrowserTasks(page);
    await expect(page.locator("#codeView")).toContainText("second version");
    expect((await recordedNotices(page)).map((notice) => notice.text)).not.toContain(
      "That diff was replaced while it was loading. Open the current diff to retry.",
    );
  });

  test("ignores a successful response after its rendered diff is replaced", async ({ page }) => {
    let releaseResponse: (() => void) | undefined;
    const responseReleased = new Promise<void>((resolve) => {
      releaseResponse = resolve;
    });
    await page.route("**/diffs/*", async (route) => {
      const diffId = new URL(route.request().url()).pathname.split("/").pop();
      await responseReleased;
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          diff_id: diffId,
          content: { kind: "unified", text: "@@ -1 +1 @@\n-before\n+stale success" },
        }),
      });
    });

    const responsePromise = page.waitForResponse("**/diffs/*");
    const group = await startDiffTurn(page);
    await group.locator(".file-change-entry").first().locator(".diff-open").click();
    await expect(group.locator(".file-change-status", { hasText: "completed" })).toBeVisible();
    await group.evaluate((row: HTMLElement & { _fileChangePayload?: any }) => {
      row._fileChangePayload = {
        changes: [{ diff: { id: "replacement-diff-id" } }],
      };
    });
    releaseResponse?.();

    const response = await responsePromise;
    await response.finished();
    await flushBrowserTasks(page);
    await expect(page.locator("#codeOverlay")).not.toHaveClass(/\bopen\b/);
    expect((await recordedNotices(page)).map((notice) => notice.text)).not.toContain(
      "Could not load captured diff:",
    );
  });

  test("ignores a failed response after its rendered diff is replaced", async ({ page }) => {
    let releaseResponse: (() => void) | undefined;
    const responseReleased = new Promise<void>((resolve) => {
      releaseResponse = resolve;
    });
    await page.route("**/diffs/*", async (route) => {
      await responseReleased;
      await route.abort("failed");
    });

    const failedRequestPromise = page.waitForEvent(
      "requestfailed",
      (request) => request.url().includes("/diffs/"),
    );
    const group = await startDiffTurn(page);
    await group.locator(".file-change-entry").first().locator(".diff-open").click();
    await expect(group.locator(".file-change-status", { hasText: "completed" })).toBeVisible();
    await group.evaluate((row: HTMLElement & { _fileChangePayload?: any }) => {
      row._fileChangePayload = {
        changes: [{ diff: { id: "replacement-diff-id" } }],
      };
    });
    releaseResponse?.();

    await failedRequestPromise;
    await flushBrowserTasks(page);
    await expect(page.locator("#codeOverlay")).not.toHaveClass(/\bopen\b/);
    expect((await recordedNotices(page)).map((notice) => notice.text).join("\n")).not.toContain(
      "Could not load captured diff:",
    );
  });

  test("opening a source file invalidates a pending captured diff", async ({ page }) => {
    let releaseResponse: (() => void) | undefined;
    const responseReleased = new Promise<void>((resolve) => {
      releaseResponse = resolve;
    });
    await page.route("**/diffs/*", async (route) => {
      const diffId = new URL(route.request().url()).pathname.split("/").pop();
      await responseReleased;
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          diff_id: diffId,
          content: { kind: "unified", text: "@@ -1 +1 @@\n-before\n+stale diff" },
        }),
      });
    });

    const responsePromise = page.waitForResponse("**/diffs/*");
    const group = await startDiffTurn(page);
    await group.locator(".file-change-entry").first().locator(".diff-open").click();
    await page.evaluate(() => openCodeOverlay("src/main.rs"));
    await expect(page.locator("#codePath")).toHaveText("src/main.rs");
    releaseResponse?.();

    const response = await responsePromise;
    await response.finished();
    await flushBrowserTasks(page);
    await expect(page.locator("#codePath")).toHaveText("src/main.rs");
    await expect(page.locator("#codeView")).not.toContainText("stale diff");
  });
});

declare const state: { projectId: string; threadId: string };
declare function api(method: string, path: string): Promise<any>;
declare function openCapturedDiff(descriptor: unknown, turnId: string): Promise<void>;
declare function openCodeOverlay(path: string, line?: number): Promise<void>;
declare function structuredCapturedDiffText(diff: unknown): string;
declare function renderFileChangeContribution(
  body: HTMLElement, payload: unknown, item: unknown, turnId: string,
): void;
