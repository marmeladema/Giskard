import { test, expect, type Page } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

// Creating a project from the new-project modal (the sidebar "+" under "Projects") must land on
// that project's draft view, not leave the previously selected thread on screen. This drives the
// real modal — folder picker, name field, Create button — rather than creating the project
// server-side, so it guards the `newThread(id)` call in `$("pmCreate").onclick`.
//
// The replay server seeds one "Demo" project and configures no `browse.roots`, so the picker can
// reach the whole filesystem and `/tmp` is a valid project directory (the server-side create
// helpers in other specs use `dir: "/tmp"` for the same reason).
test.describe("new-project modal", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // Remove a project created during a test so it does not leak into the shared server state for
  // the rest of the serial suite. Best-effort: the project is gone from the assertions' perspective
  // once the draft landed, so a failed cleanup should not fail the test.
  async function cleanupProject(page: Page, id: string): Promise<void> {
    // Swallow failures: this runs in a `finally`, so an unhandled rejection here would mask the
    // real assertion error. A delete against a valid id does not normally fail, but the test's
    // own result is what matters.
    try {
      await page.evaluate(async (projectId) => {
        await fetch(`/api/projects/${projectId}`, { method: "DELETE" });
      }, id);
    } catch {
      // Best-effort cleanup; nothing to act on here.
    }
  }

  test("creating a project opens its draft view", async ({ page }) => {
    const projectCountBefore = await page.locator(".proj").count();

    // Open the new-project modal from the sidebar header.
    await page.locator("#newProj").click();
    await expect(page.locator("#projectModal")).toHaveClass(/open/);

    // The modal asks for a folder and a name, and nothing else: a project has no harness until it
    // exists, so there is no model catalog to offer here.
    await expect(page.locator("#pmModel")).toHaveCount(0);
    const createRequest = page.waitForRequest(
      (r) => r.method() === "POST" && new URL(r.url()).pathname === "/api/projects",
    );

    // Browse the picker to /tmp. The modal reopens where the browser last browsed (persisted in
    // localStorage), so the start is not fixed; `browseTo` walks up to "/" then down into the
    // target, which works from any starting directory.
    await browseTo(page, "/tmp");
    await expect(page.locator("#pmPath")).toHaveText("/tmp");

    // Give the project a unique name so we can locate its row after the sidebar reloads.
    const projectName = `Modal project ${Date.now()}`;
    await page.locator("#pmName").fill(projectName);

    // The POST creates the project and returns its id; `newThread(id)` then opens the draft.
    const created = page.waitForResponse(
      (r) => r.request().method() === "POST" && new URL(r.url()).pathname === "/api/projects",
    );
    await page.locator("#pmCreate").click();
    // Absent from the DOM is not the same as absent from the request: the payload must name no
    // model either, since a project record no longer stores one.
    expect((await createRequest).postDataJSON()).not.toHaveProperty("default_model");
    const createdJson = (await (await created).json()) as { id: string };
    const projectId = createdJson.id;

    try {
      // The modal closes and the sidebar reloads with the new project.
      await expect(page.locator("#projectModal")).not.toHaveClass(/open/);
      await expect(page.locator(".proj")).toHaveCount(projectCountBefore + 1);

      // The new project's row is present and is the active selection (a draft in that project is
      // open, so the project name is highlighted, not any thread row).
      const projectRow = page.locator(".proj", { hasText: projectName });
      await expect(projectRow).toBeVisible();
      const projectNameBtn = projectRow.locator(".project-name");
      await expect(projectNameBtn).toHaveClass(/\bactive\b/);
      await expect(projectNameBtn).toHaveAttribute("aria-current", "true");
      await expect(page.locator(".thread.active")).toHaveCount(0);

      // The view is the new project's draft: the transcript shows the draft explainer and the
      // composer is visible, focused, and ready. The title bar names the new project.
      await expect(page.locator("#composer")).toBeVisible();
      await expect(page.locator("#input")).toBeVisible();
      await expect(page.locator("#input")).toBeFocused();
      await expect(page.locator("#transcript .draft-empty")).toBeVisible();
      await expect(page.locator("#transcript")).toContainText("Start a new thread");
      await expect(page.locator("#mbTitle")).toContainText(projectName);

      // The draft's starting model is derived from the new project's catalog — a project record
      // stores none — so the picker shows the configured model and Send becomes available, the
      // same end state as the per-project "+".
      // Type first: an empty composer disables Send for its own reason, which would mask whether
      // the model resolved.
      await expect(page.locator("#modelPickerBtn")).toContainText("Replay Model");
      const message = "First message in the modal-created project";
      await page.locator("#input").fill(message);
      await expect(page.locator("#sendBtn")).toBeEnabled();

      // A message can actually be sent from this draft, proving the composer is live and the
      // project is usable, not just drawn.
      await page.locator("#sendBtn").click();
      await expect(
        page.locator("#transcript .msg.user", { hasText: message }),
      ).toBeVisible();
      await expect(
        page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
      ).toBeVisible();
      // Sending created a real thread, so a thread row now exists and takes the active highlight.
      await expect(page.locator(".thread.active")).toHaveCount(1);
      await expect(projectNameBtn).not.toHaveClass(/\bactive\b/);
    } finally {
      await cleanupProject(page, projectId);
    }
  });
});

/**
 * Navigate the new-project folder picker to an absolute path by walking up to "/" then down into
 * the target. The picker remembers the last browsed dir in localStorage and reopens there, so the
 * starting point is not fixed; walking via ".." to the root and then into the target works from any
 * start. Directories are selected by clicking their ".direntry" row.
 */
async function browseTo(page: Page, target: string): Promise<void> {
  // Walk up to the filesystem root. Each ".." row carries data-nav="up". The modal reopens where
  // the browser last browsed (persisted in localStorage), but each Playwright test gets a fresh
  // browser context with empty localStorage, so the start is "/" anyway; the walk-up handles any
  // in-test navigation that left the picker elsewhere and makes the descent deterministic.
  while (await page.locator("#pmList .direntry[data-nav='up']").count()) {
    await page.locator("#pmList .direntry[data-nav='up']").click();
  }
  await expect(page.locator("#pmPath")).toHaveText("/");

  // Descend into each component of the target path.
  const parts = target.split("/").filter(Boolean);
  for (const part of parts) {
    await page
      .locator("#pmList .direntry", { hasText: part })
      .first()
      .click();
  }
  await expect(page.locator("#pmPath")).toHaveText(target);
}
