import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

// The selected thread must be unmistakable in the sidebar, and switching threads must move the
// highlight — a previously selected row that stays highlighted (e.g. because the thread list was
// rebuilt and the imperative highlight went stale) is the regression guarded here.
//
// The replay server is shared and stateful across the suite, so earlier specs may have left threads
// in the Demo project. The assertions therefore never assume a clean slate: they rely on the
// invariant that exactly one row is active and identify the threads under test by their own ids.
test.describe("sidebar thread selection", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // Create a thread from a fresh draft and return the id of the row that becomes selected.
  async function createThread(page, message: string): Promise<string> {
    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    const input = page.locator("#input");
    await expect(input).toBeVisible();
    await input.fill(message);
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
    // The just-opened thread is the only active one.
    await expect(page.locator(".thread.active")).toHaveCount(1);
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    expect(tid).toBeTruthy();
    return tid as string;
  }

  test("highlights exactly one thread and moves it when switching", async ({ page }) => {
    const tid1 = await createThread(page, "Alpha thread about the login flow");
    const tid2 = await createThread(page, "Bravo thread about the cache layer");
    expect(tid2).not.toBe(tid1);

    // Switch back to the first thread.
    await page.locator(`.thread[data-tid="${tid1}"]`).click();
    await expect(page.locator(`.thread[data-tid="${tid1}"]`)).toHaveClass(/\bactive\b/);
    // The previously selected thread must lose the highlight — exactly one active row remains.
    await expect(page.locator(".thread.active")).toHaveCount(1);
    await expect(page.locator(`.thread[data-tid="${tid2}"]`)).not.toHaveClass(/\bactive\b/);

    // The selection is visually reinforced with an accent bar (inset box-shadow), not just a
    // background tint, so it reads clearly even in a long list.
    const shadow = await page
      .locator(`.thread[data-tid="${tid1}"]`)
      .evaluate((el) => getComputedStyle(el).boxShadow);
    expect(shadow).not.toBe("none");
    // aria-current mirrors the visual selection for assistive tech.
    await expect(page.locator(`.thread[data-tid="${tid1}"]`)).toHaveAttribute(
      "aria-current",
      "true",
    );

    // A reload rebuilds the whole thread list from scratch. The selection is derived from the
    // restored thread rather than an imperative highlight, so exactly one row — the restored one —
    // must come back active, with no stale highlight left on the other.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);
    await expect(page.locator(`.thread[data-tid="${tid1}"]`)).toHaveClass(/\bactive\b/);
    await expect(page.locator(".thread.active")).toHaveCount(1);
    await expect(page.locator(`.thread[data-tid="${tid2}"]`)).not.toHaveClass(/\bactive\b/);
  });
});
