import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

/**
 * Isolating a thread in its own Git worktree is chosen once, on the draft, because a thread's
 * workspace is fixed the moment it exists. The choice sits on the Git status row — the row that
 * describes the very tree it changes. These cover the contract it has to keep: it is offered only
 * where it can be acted on, its current value is readable without opening anything, and it says what
 * the chosen option will and will not carry across.
 */
test.describe("git checkout choice on a draft", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  const newDraft = (page: import("@playwright/test").Page) =>
    page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

  test("is offered on a draft and gone once the thread exists", async ({ page }) => {
    await newDraft(page);
    await expect(page.locator("#gitStrategySel")).toBeVisible();
    await expect(page.locator("#gitStrategySel")).toBeEnabled();

    // Send, so the draft becomes a thread whose workspace is now settled.
    await page.locator("#input").fill("Thread that settles the workspace");
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();

    // The Git row itself stays — it describes the thread's tree — but the choice it carried is
    // settled, so the control goes rather than standing there dead.
    await expect(page.locator("#gitLine")).toBeVisible();
    await expect(page.locator("#gitStrategySel")).toBeHidden();
  });

  /**
   * The row the control sits on reports the project's changed files at the moment of the decision,
   * so the choice has to say those changes are not coming along — otherwise the first thing the
   * agent reports is that the work in progress is missing. What the option *is* rides on the
   * control's tooltip; what it would *cost* is printed, because a tooltip cannot be read on a phone.
   */
  test("says what stays behind when the project has uncommitted work", async ({ page }) => {
    await newDraft(page);
    await expect(page.locator("#gitCount")).toHaveText("1");

    const select = page.locator("#gitStrategySel");
    const warning = page.locator("#gitStrategyWarning");
    // The default is described too, so hovering never comes up empty — and it costs nothing, so
    // there is no line under the row.
    await expect(select).toHaveAttribute("title", /Shares the project's working tree/);
    await expect(warning).toBeHidden();

    await select.selectOption("worktree");
    await expect(select).toHaveAttribute("title", /Starts from the last commit/);
    // Visible text, not a tooltip: this is the fact a phone has to be able to read.
    // Singular count, singular verb — the sentence is asking to be trusted about what it drops.
    await expect(warning).toBeVisible();
    await expect(warning).toHaveText("Your 1 uncommitted change stays in the project's checkout.");

    // Choosing back is a full retraction: emptied, not merely hidden, or a screen reader meets it
    // again the next time the row comes back.
    await select.selectOption("shared");
    await expect(warning).toBeHidden();
    await expect(warning).toHaveText("");
  });

  /**
   * The control reads its own value with nothing opened, and the mode/permission chip beside it
   * stays out of it: one fact, one place.
   */
  test("reads its current value on the row", async ({ page }) => {
    await newDraft(page);
    await expect(page.locator("#gitStrategySel")).toHaveValue("shared");

    await page.locator("#gitStrategySel").selectOption("worktree");

    await expect(page.locator("#gitStrategySel")).toHaveValue("worktree");
    await expect(page.locator("#turnPickerBtn .mp-label")).toHaveText("Build · Ask first");
  });

  test("does not carry the choice into the next draft", async ({ page }) => {
    await newDraft(page);
    await page.locator("#gitStrategySel").selectOption("worktree");
    await expect(page.locator("#gitStrategySel")).toHaveValue("worktree");

    await newDraft(page);

    await expect(page.locator("#gitStrategySel")).toHaveValue("shared");
  });

  /**
   * Deleting a thread destroys its worktree, and a worktree can hold the only copy of work. The
   * card names it while the question is still open — after the fact there is nothing to decide.
   */
  test("names what deleting an isolated thread would destroy", async ({ page }) => {
    await newDraft(page);
    await page.locator("#gitStrategySel").selectOption("worktree");
    await page.locator("#input").fill("Work that is not committed");
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();

    // The scripted harness leaves the worktree clean, so nothing is at risk yet and the card asks
    // its ordinary question.
    const rowContainer = page.locator(".thread.active").locator("xpath=..");
    await rowContainer.locator(".thread-menu-btn").click();
    const impact = page.waitForResponse((r) => r.url().includes("/deletion-impact"));
    await rowContainer.locator(".thread-menu .danger").click();
    await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);

    // The card asked, and the answer was "nothing at stake" — for the thread's own worktree, which
    // it reports either way.
    const body = await (await impact).json();
    expect(body.worktrees).toHaveLength(1);
    expect(body.worktrees[0].summary).toBeUndefined();
    await expect(page.locator("#removeThreadWorktree")).toBeHidden();

    await page.locator("#removeThreadCancel").click();
    await expect(page.locator("#removeThreadModal")).not.toHaveClass(/open/);
  });

  test("starts a thread that runs in the worktree", async ({ page }) => {
    await newDraft(page);
    await page.locator("#gitStrategySel").selectOption("worktree");

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
