import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

/**
 * Isolating a thread in its own Git worktree is chosen once, on the draft, because a thread's
 * workspace is fixed the moment it exists. These cover the contract that choice has to keep: it is
 * offered only where it can be acted on, it says what it will and will not carry across, and it is
 * visible without opening the popover that set it.
 */
test.describe("worktree draft toggle", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  const newDraft = (page: import("@playwright/test").Page) =>
    page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

  const openPicker = (page: import("@playwright/test").Page) =>
    page.locator("#turnPickerBtn").click();

  test("is offered on a draft and gone once the thread exists", async ({ page }) => {
    await newDraft(page);
    await openPicker(page);
    await expect(page.locator("#gitStrategyControl")).toBeVisible();
    await expect(page.locator("#gitStrategySel")).toBeEnabled();

    // Send, so the draft becomes a thread whose workspace is now settled.
    await page.locator("#turnPickerBtn").click();
    await page.locator("#input").fill("Thread that settles the workspace");
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();

    await openPicker(page);
    await expect(page.locator("#gitStrategyControl")).toBeHidden();
  });

  /**
   * The row above the composer reports the project's changed files at the moment of the decision,
   * so the hint has to say those changes are not coming along — otherwise the first thing the agent
   * reports is that the work in progress is missing.
   */
  test("says what stays behind when the project has uncommitted work", async ({ page }) => {
    await newDraft(page);
    await expect(page.locator("#gitCount")).toHaveText("1");

    await openPicker(page);
    // Nothing to explain until the choice is made.
    await expect(page.locator("#gitStrategyHint")).toBeHidden();

    await page.locator("#gitStrategySel").selectOption("worktree");
    const hint = page.locator("#gitStrategyHint");
    await expect(hint).toContainText("Starts from the last commit");
    await expect(hint).toContainText("1 uncommitted change");
    await expect(hint).toContainText("stay in the project's checkout");
  });

  test("shows the choice on the closed chip", async ({ page }) => {
    await newDraft(page);
    await expect(page.locator("#turnPickerBtn .mp-label")).toHaveText("Build · Ask first");

    await openPicker(page);
    await page.locator("#gitStrategySel").selectOption("worktree");
    await page.locator("#turnPickerBtn").click();   // close the popover

    await expect(page.locator("#turnPickerBtn .mp-label")).toHaveText(
      "Build · Ask first · Worktree",
    );
  });

  test("does not carry the choice into the next draft", async ({ page }) => {
    await newDraft(page);
    await openPicker(page);
    await page.locator("#gitStrategySel").selectOption("worktree");
    await page.locator("#turnPickerBtn").click();
    await expect(page.locator("#turnPickerBtn .mp-label")).toContainText("Worktree");

    await newDraft(page);

    await expect(page.locator("#turnPickerBtn .mp-label")).toHaveText("Build · Ask first");
    await openPicker(page);
    await expect(page.locator("#gitStrategySel")).toHaveValue("shared");
  });

  test("starts a thread that runs in the worktree", async ({ page }) => {
    await newDraft(page);
    await openPicker(page);
    await page.locator("#gitStrategySel").selectOption("worktree");
    await page.locator("#turnPickerBtn").click();

    await page.locator("#input").fill("Work in an isolated checkout");
    const started = page.waitForResponse(
      (r) => r.request().method() === "POST" && r.url().endsWith("/threads/start"),
    );
    await page.locator("#sendBtn").click();

    // The flag has to reach the server: everything downstream of it is server-side.
    const request = (await started).request();
    expect(JSON.parse(request.postData() ?? "{}")).toMatchObject({ git_strategy: "worktree" });
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
  });
});
