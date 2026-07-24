import { test, expect } from "@playwright/test";
import { SCRIPTED_APPROVAL_TRIGGER, login } from "./helpers";

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

    // Reload: the in-memory answered state is wiped, so the resolved state must be reconstructed
    // entirely from the server's live-turn snapshot.
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

    // The sidebar must not nag that the thread still needs an approval: an answered approval
    // replayed from the snapshot must not re-arm the "approval needed" thread indicator.
    await expect(page.locator(".thread.activity-approval")).toHaveCount(0);
  });
});
