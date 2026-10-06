import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

// A project's archived threads fold away under a counted "Archived (N)" header. The section
// starts collapsed, remembers an explicit expansion across reloads, and unfolds on its own while
// the open thread is one of them so the selection is never hidden.
//
// The replay server is shared across the suite, so earlier specs may already have archived
// threads in the Demo project; the count is read rather than assumed.
test.describe("archived thread section", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  async function createThread(page, message: string): Promise<string> {
    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    const input = page.locator("#input");
    await expect(input).toBeVisible();
    await input.fill(message);
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    expect(tid).toBeTruthy();
    return tid as string;
  }

  test("starts collapsed, shows its count, and unfolds for the open thread", async ({ page }) => {
    const archivedTid = await createThread(page, "Charlie thread to be archived");
    const openTid = await createThread(page, "Delta thread that stays open");
    const demo = page.locator(".proj", { hasText: "Demo" });

    const row = demo.locator(`.thread[data-tid="${archivedTid}"]`).locator("..");
    await row.locator(".thread-menu-btn").click();
    await row.locator(".thread-menu button", { hasText: "Archive" }).click();

    const toggle = demo.locator(".archived-toggle");
    await expect(toggle).toHaveText(/Archived \(\d+\)/);
    const count = Number((await toggle.textContent())!.match(/\((\d+)\)/)![1]);
    expect(count).toBeGreaterThanOrEqual(1);
    await expect(demo.locator(".archived-threads .thread")).toHaveCount(count);
    await expect(toggle).toHaveAttribute("aria-expanded", "false");
    const archivedRow = page.locator(`.thread[data-tid="${archivedTid}"]`);
    await expect(archivedRow).toBeHidden();

    await toggle.click();
    await expect(toggle).toHaveAttribute("aria-expanded", "true");
    await expect(archivedRow).toBeVisible();

    // The expansion is remembered across a reload.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);
    await expect(page.locator(`.thread[data-tid="${openTid}"]`)).toHaveClass(/\bactive\b/);
    await expect(archivedRow).toBeVisible();

    await demo.locator(".archived-toggle").click();
    await expect(archivedRow).toBeHidden();

    // Restoring an archived thread unfolds the section around it without changing the saved
    // preference: moving to another thread folds it away again.
    const pid = await demo.getAttribute("data-pid");
    await page.evaluate(
      ([pid, tid]) => localStorage.setItem("giskard.lastThread", JSON.stringify({ pid, tid })),
      [pid, archivedTid],
    );
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);
    await expect(archivedRow).toHaveClass(/\bactive\b/);
    await expect(archivedRow).toBeVisible();
    await expect(demo.locator(".archived-toggle")).toHaveAttribute("aria-expanded", "true");

    // Collapsing a section held open by the open thread saves "collapsed" from what is shown; the
    // open row stays visible until the selection moves away.
    await demo.locator(".archived-toggle").click();
    await expect(archivedRow).toBeVisible();
    await expect(demo.locator(".archived-toggle")).toHaveAttribute("aria-expanded", "true");

    await page.locator(`.thread[data-tid="${openTid}"]`).click();
    await expect(page.locator(`.thread[data-tid="${openTid}"]`)).toHaveClass(/\bactive\b/);
    await expect(archivedRow).toBeHidden();
  });

  test("archiving the open thread folds it into the collapsed section", async ({ page }) => {
    const tid = await createThread(page, "Echo thread archived while open");
    const demo = page.locator(".proj", { hasText: "Demo" });
    const row = demo.locator(`.thread[data-tid="${tid}"]`).locator("..");
    await row.locator(".thread-menu-btn").click();
    await row.locator(".thread-menu button", { hasText: "Archive" }).click();

    // Closing the view drops the selection, so nothing holds the section open around the row.
    const archivedRow = page.locator(`.thread[data-tid="${tid}"]`);
    await expect(page.locator(".thread.active")).toHaveCount(0);
    await expect(demo.locator(".archived-toggle")).toHaveAttribute("aria-expanded", "false");
    await expect(archivedRow).toBeHidden();

    await demo.locator(".archived-toggle").click();
    await expect(archivedRow).toBeVisible();
    await demo.locator(".archived-toggle").click();
    await expect(archivedRow).toBeHidden();
  });
});
