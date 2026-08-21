import { test, expect } from "@playwright/test";
import {
  SCRIPTED_APPROVAL_THEN_ERROR_MESSAGE,
  SCRIPTED_APPROVAL_THEN_ERROR_TRIGGER,
  SCRIPTED_APPROVAL_TRIGGER,
  SCRIPTED_EMPTY_FILE_APPROVAL_TRIGGER,
  login,
} from "./helpers";

test("a path-free file approval keeps grant root separate from changed files", async ({ page }) => {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

  await page.locator("#input").fill(SCRIPTED_EMPTY_FILE_APPROVAL_TRIGGER);
  await page.locator("#sendBtn").click();

  const approval = page.locator("#transcript .msg.approval");
  await expect(approval.locator(".approval-title")).toHaveText("Apply file changes?");
  await expect(approval.locator(".approval-detail")).toHaveText(
    "File list was not provided by Codex.",
  );
  await expect(approval.locator(".approval-detail")).not.toContainText("modified");
  const metadata = approval.locator(".approval-meta-row");
  await expect(metadata.locator(".approval-meta-label")).toHaveText("Grant root");
  await expect(metadata.locator(".approval-meta-value")).toHaveText("/tmp/project");
});

// Regression: an approval answered during an in-flight turn must stay resolved after a browser
// reload. Approval resolution used to live only in browser memory, so a reload re-surfaced the
// answered card as actionable — and answering it again routed a stale id to the harness, which
// errored. This has regressed multiple times, so it is pinned end-to-end.
test.describe("approval persistence across reload", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("an answered approval stays resolved after a browser reload", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    await expect(input).toBeVisible();
    await input.fill(SCRIPTED_APPROVAL_TRIGGER);
    await page.locator("#sendBtn").click();

    const transcript = page.locator("#transcript");
    const approval = transcript.locator(".msg.approval");
    await expect(approval).toBeVisible();

    // Hold on to this thread's row: `activity-waiting` now covers server requests too, so a
    // page-wide assertion would fail on any unrelated thread that happens to be waiting.
    const row = page.locator(".thread.active");
    await expect(row).toHaveCount(1);
    const tid = await row.getAttribute("data-tid");
    expect(tid).toBeTruthy();

    // The card is actionable before it is answered.
    const acceptBtn = approval.getByRole("button", { name: "Accept", exact: true });
    await expect(acceptBtn).toBeVisible();
    await acceptBtn.click();

    // The card resolves in place, and the scripted harness acknowledges on the still-open turn —
    // which only happens once the server has routed the decision and recorded the resolution.
    await expect(approval).toHaveClass(/\bresolved\b/);
    await expect(
      transcript.locator(".msg.agent", { hasText: "Approval recorded: accept" }),
    ).toBeVisible();
    await expect(approval.locator(".approval-result")).toHaveText(/Decision: Accept/);
    await expect(acceptBtn).toHaveCount(0);

    // Reload: the in-memory answered state is wiped, so ordered request chronology and the
    // authoritative runtime state must reconstruct the resolved state.
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    // The last thread auto-reopens and the approval comes back resolved — never actionable — and
    // no error surfaces (re-answering a stale approval used to raise one).
    const approvalAfter = page.locator("#transcript .msg.approval");
    await expect(approvalAfter).toBeVisible();
    await expect(approvalAfter).toHaveClass(/\bresolved\b/);
    await expect(approvalAfter.locator(".approval-result")).toHaveText(/Decision: Accept/);
    await expect(
      approvalAfter.getByRole("button", { name: "Accept", exact: true }),
    ).toHaveCount(0);
    await expect(page.locator("#transcript .msg.error")).toHaveCount(0);

    // The sidebar must not nag that this thread still needs an approval: an answered approval
    // replayed from the snapshot must not re-arm its waiting indicator.
    await expect(page.locator(`.thread[data-tid="${tid}"]`)).not.toHaveClass(/\bactivity-waiting\b/);
  });
});

// The replacement runtime overview is authoritative for what the turn is still waiting on. A later
// transcript `error` must not strand an outstanding approval in a thread that reads as idle: the
// runtime request state still gives the waiting activity precedence.
test.describe("a still-blocked turn survives a reload that replays a later error", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("a thread blocked on an approval still reads as waiting after an error", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    await expect(input).toBeVisible();
    await input.fill(SCRIPTED_APPROVAL_THEN_ERROR_TRIGGER);
    await page.locator("#sendBtn").click();

    // Both arrive in the same still-open turn, the error last.
    await expect(page.locator("#transcript .msg.approval")).toBeVisible();
    await expect(page.locator("#transcript .msg.error")).toContainText(
      SCRIPTED_APPROVAL_THEN_ERROR_MESSAGE,
    );

    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    // The error is replayed and still shown — it happened, and the transcript is the record.
    await expect(page.locator("#transcript .msg.error")).toContainText(
      SCRIPTED_APPROVAL_THEN_ERROR_MESSAGE,
    );
    // But the turn is still blocked on the user, and that outranks the error in the sidebar.
    const row = page.locator(".thread.active");
    await expect(row).toHaveClass(/\bactivity-waiting\b/);
    await expect(row.locator(".thread-status")).toHaveText("!");
    // And the approval is still answerable, not stranded in a thread that reads as finished.
    await expect(
      page.locator("#transcript .msg.approval").getByRole("button", { name: "Accept", exact: true }),
    ).toBeEnabled();
  });
});
