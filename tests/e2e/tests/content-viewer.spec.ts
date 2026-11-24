import { test, expect, type Page } from "@playwright/test";
import { SCRIPTED_REPLY, SCRIPTED_SUBAGENT_REPLY, SCRIPTED_SUBAGENT_TRIGGER, login } from "./helpers";

// The viewer for files, diffs and command/tool output opens in the thread column, in place of the
// transcript, rather than as a modal over the whole app. The sidebar, the thread header, the Git
// line and the composer all stay in reach; "← Thread" (or Escape) goes back.
//
// The transcript is covered, not removed: it stays laid out underneath, so its scroll position and
// its stick-to-bottom following carry on while the viewer is open.

const viewer = (page: Page) => page.locator("#codeOverlay");

async function openThreadWithReply(page: Page) {
  if (await page.locator("#btnMenu").isVisible()) await page.locator("#btnMenu").click();
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Start a thread for the viewer");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  await expect(page.locator("#stopBtn")).toBeHidden();
}

async function openWorkingTreeDiff(page: Page) {
  await page.locator('#gitLineToggle').click();
  await page.locator('.git-file[data-git-diff="src/main.rs"]').click();
  await expect(viewer(page)).toHaveClass(/\bopen\b/);
  await expect(page.locator("#codePath")).toHaveText("src/main.rs");
}

/** A transcript tall enough to scroll, so a scroll position means something. */
async function fillTallTranscript(page: Page) {
  await page.evaluate(() => {
    const filler = document.createElement("div");
    filler.className = "viewer-test-filler";
    filler.style.height = "4000px";
    document.getElementById("transcript")!.append(filler);
  });
}

test.describe("content viewer", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("opens in the thread column, leaving the sidebar, header and composer in reach", async ({ page }) => {
    await openThreadWithReply(page);
    await openWorkingTreeDiff(page);

    const box = (sel: string) => page.locator(sel).evaluate((el) => {
      const r = el.getBoundingClientRect();
      return { left: r.left, right: r.right, top: r.top, bottom: r.bottom };
    });
    const column = await box(".col.center");
    const view = await box("#codeOverlay");
    const header = await box("#thrHeader");
    const composer = await box("#composer");
    // Inside the thread column, between the thread header and what sits above the composer.
    expect(view.left).toBeGreaterThanOrEqual(column.left);
    expect(view.right).toBeLessThanOrEqual(column.right + 0.5);
    expect(view.top).toBeGreaterThanOrEqual(header.bottom - 0.5);
    expect(view.bottom).toBeLessThanOrEqual(composer.top + 0.5);

    // Nothing covers the rest of the app: the sidebar's thread row is still what a click there hits.
    const row = page.locator(".thread.active").first();
    await expect(row).toBeVisible();
    const hit = await row.evaluate((el) => {
      const r = el.getBoundingClientRect();
      const top = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2);
      return !!top && el.contains(top);
    });
    expect(hit, "the sidebar is not under a backdrop").toBe(true);
    await expect(page.locator("#input")).toBeEditable();

    // The transcript is hidden, not removed.
    await expect(page.locator("#transcript")).toBeHidden();
    expect(await page.locator("#transcript").evaluate((el) => getComputedStyle(el).display)).not.toBe("none");

    await page.locator("#codeClose").click();
    await expect(viewer(page)).toBeHidden();
    await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  });

  // Opened from something in the transcript, the viewer hides what had focus; focus moves into it.
  test("takes keyboard focus from the transcript it covers", async ({ page }) => {
    await openThreadWithReply(page);
    await page.evaluate(() => {
      const link = document.createElement("button");
      link.id = "viewer-test-opener";
      link.textContent = "open";
      document.getElementById("transcript")!.append(link);
      link.focus();
    });
    await page.evaluate(() =>
      (window as unknown as { openDiffOverlay: (p: string, d: string) => void })
        .openDiffOverlay("focus.rs", "@@ -1 +1 @@\n-old\n+new"));
    await expect(page.locator("#codeView")).toBeFocused();

    // Opened from the composer's side of the column, focus stays where the user is typing.
    await page.locator("#codeClose").click();
    await page.locator("#input").focus();
    await page.evaluate(() =>
      (window as unknown as { openDiffOverlay: (p: string, d: string) => void })
        .openDiffOverlay("focus.rs", "@@ -1 +1 @@\n-old\n+new"));
    await expect(page.locator("#input")).toBeFocused();
  });

  test("leaves the transcript where it was, and following the turn underneath", async ({ page }) => {
    await openThreadWithReply(page);
    await fillTallTranscript(page);
    const transcript = page.locator("#transcript");

    // Mid-transcript: the position survives a visit to the viewer, both while covered and after.
    await transcript.evaluate((el) => { el.scrollTop = 1500; });
    await openWorkingTreeDiff(page);
    expect(await transcript.evaluate((el) => el.scrollTop)).toBe(1500);
    await page.locator("#codeClose").click();
    expect(await transcript.evaluate((el) => el.scrollTop)).toBe(1500);

    // At the bottom: a row that arrives while the viewer is open is followed, as it would be on screen.
    await transcript.evaluate((el) => { el.scrollTop = el.scrollHeight; });
    await page.locator('.git-file[data-git-diff="src/main.rs"]').click();
    await expect(viewer(page)).toHaveClass(/\bopen\b/);
    await page.evaluate(() => {
      const body = (window as unknown as { bubble: (cls: string, role: string) => HTMLElement })
        .bubble("agent", "agent");
      body.textContent = "Arrived while the viewer was open";
      body.style.minHeight = "600px";
    });
    await page.locator("#codeClose").click();
    const gap = await transcript.evaluate((el) => el.scrollHeight - el.scrollTop - el.clientHeight);
    expect(gap).toBeLessThanOrEqual(2);
    await expect(page.locator("#transcript .msg.agent", { hasText: "Arrived while the viewer was open" }))
      .toBeInViewport();
  });

  test("Escape closes a menu over the viewer first, then goes back to the thread", async ({ page }) => {
    await openThreadWithReply(page);
    await openWorkingTreeDiff(page);

    await page.locator("#turnPickerBtn").click();
    await expect(page.locator("#turnPickerMenu")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator("#turnPickerMenu")).toBeHidden();
    await expect(viewer(page)).toHaveClass(/\bopen\b/);

    // The header's popovers open over it too.
    await page.locator("#usageBtn").click();
    await expect(page.locator("#usageMenu")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator("#usageMenu")).toBeHidden();
    await expect(viewer(page)).toHaveClass(/\bopen\b/);

    // So do the sidebar's thread and project action menus.
    const row = page.locator(".thread.active").locator("..");
    await row.locator(".thread-menu-btn").click();
    await expect(row.locator(".thread-menu")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(row.locator(".thread-menu")).toBeHidden();
    await expect(viewer(page)).toHaveClass(/\bopen\b/);

    // A dialog that doesn't close on Escape still keeps the viewer under it.
    await page.locator("#newProj").click();
    await expect(page.locator("#projectModal")).toHaveClass(/\bopen\b/);
    await page.keyboard.press("Escape");
    await expect(viewer(page)).toHaveClass(/\bopen\b/);
    await page.evaluate(() => (window as unknown as { closeProjectModal: () => void }).closeProjectModal());

    // The phone drawer's class can outlive a resize to desktop width, where no drawer is shown;
    // it mustn't take the Escape meant for the viewer.
    await page.evaluate(() => document.getElementById("app")!.classList.add("drawer-left"));

    await page.keyboard.press("Escape");
    await expect(viewer(page)).toBeHidden();
  });

  // The sidebar stays usable while the viewer is open, so the thread behind it can go away.
  test("closes when its thread is archived from the sidebar", async ({ page }) => {
    await openThreadWithReply(page);
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    await openWorkingTreeDiff(page);

    const row = page.locator(`.thread[data-tid="${tid}"]`).locator("..");
    await row.locator(".thread-menu-btn").click();
    await row.locator(".thread-menu button", { hasText: "Archive" }).click();
    await expect(viewer(page)).toBeHidden();
  });

  test("sending a message goes back to the thread to show the reply", async ({ page }) => {
    await openThreadWithReply(page);
    await openWorkingTreeDiff(page);

    // The composer stays usable while the viewer is open.
    await page.locator("#input").fill("A question about that diff");
    await page.locator("#sendBtn").click();
    await expect(viewer(page)).toBeHidden();
    await expect(page.locator("#transcript .msg.user", { hasText: "A question about that diff" })).toBeVisible();
  });

  // The way back sits where a sub-agent thread's "← Parent" does: one back button, one level at a time.
  test("takes the parent button's place in a sub-agent thread", async ({ page }) => {
    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await page.locator("#input").fill(SCRIPTED_SUBAGENT_TRIGGER);
    await page.locator("#sendBtn").click();
    await page.locator("#transcript").getByRole("button", { name: "Open linked thread" }).click();
    await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_SUBAGENT_REPLY })).toBeVisible();

    const parent = page.locator("#parentThreadBtn");
    const back = page.locator("#codeClose");
    await expect(parent).toBeVisible();
    await expect(back).toBeHidden();
    const parentBox = await parent.boundingBox();

    await page.evaluate(() =>
      (window as unknown as { openDiffOverlay: (p: string, d: string) => void })
        .openDiffOverlay("child.rs", "@@ -1 +1 @@\n-old\n+new"));
    await expect(viewer(page)).toHaveClass(/\bopen\b/);
    await expect(parent).toBeHidden();
    await expect(back).toBeVisible();
    await expect(back).toHaveText("←Thread");
    const backBox = await back.boundingBox();
    expect(Math.abs(backBox!.x - parentBox!.x)).toBeLessThan(1);
    expect(Math.abs(backBox!.y - parentBox!.y)).toBeLessThan(1);

    // Back to the child thread, where "← Parent" leads on up.
    await back.click();
    await expect(viewer(page)).toBeHidden();
    await expect(back).toBeHidden();
    await expect(parent).toBeVisible();
  });

  test.describe("on a phone", () => {
    test.use({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 3, isMobile: true, hasTouch: true });

    test("keeps the header compact: a one-line title, its details, and icon buttons beside them", async ({ page }) => {
      await openThreadWithReply(page);
      const path = "crates/giskard-server/src/registry/event_forwarder.rs";
      await page.evaluate((p) =>
        (window as unknown as { openDiffOverlay: (p: string, d: string) => void })
          .openDiffOverlay(p, "@@ -1 +1 @@\n-old\n+new"), path);
      await expect(viewer(page)).toHaveClass(/\bopen\b/);
      await expect(page.locator("#codePath")).toHaveText(path);

      const layout = await page.evaluate(() => {
        const rect = (el: Element) => el.getBoundingClientRect();
        const title = document.getElementById("codePath")!;
        // Where the path's first and last characters land: a long path is cut at the start.
        const text = title.firstChild as Text;
        const range = document.createRange();
        range.setStart(text, 0); range.setEnd(text, 1);
        const first = range.getBoundingClientRect();
        range.setStart(text, text.length - 1); range.setEnd(text, text.length);
        const last = range.getBoundingClientRect();
        const head = document.querySelector("#codeOverlay .code-head")!;
        return {
          head: rect(head), title: rect(title), meta: rect(document.getElementById("codeMeta")!),
          copy: rect(document.getElementById("codeCopyDiff")!),
          download: rect(document.getElementById("codeDownload")!),
          first, last, lineHeight: parseFloat(getComputedStyle(title).lineHeight) || 20,
          headOverflow: head.scrollWidth - head.clientWidth,
        };
      });
      // Two short lines, not three rows: about half the height the labelled buttons needed.
      expect(layout.head.height).toBeLessThan(70);
      expect(layout.title.height).toBeLessThan(layout.lineHeight * 1.5);
      // The file name at the end of the path stays in view; the start is what gets cut.
      expect(layout.last.right).toBeLessThanOrEqual(layout.title.right + 0.5);
      expect(layout.first.left).toBeLessThan(layout.title.left);
      // The actions sit beside the title and details, as icon buttons.
      expect(layout.download.left).toBeGreaterThanOrEqual(layout.title.right);
      expect(layout.download.top).toBeLessThan(layout.meta.bottom);
      expect(layout.download.width).toBeLessThan(40);
      expect(layout.copy.width).toBeLessThan(40);
      expect(layout.headOverflow).toBeLessThanOrEqual(0);
      // Their words stay as their names.
      await expect(page.getByRole("button", { name: "Copy", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "Download" })).toBeVisible();

      // There's no hover for the tooltip on a phone: a tap shows the cut title in full.
      const title = page.locator("#codePath");
      await expect(title).toHaveAttribute("title", path);
      await title.click();
      expect((await title.boundingBox())!.height).toBeGreaterThan(layout.lineHeight * 1.5);
      expect(await title.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);
      await title.click();
      expect((await title.boundingBox())!.height).toBeLessThan(layout.lineHeight * 1.5);
      await expect(title).toHaveAttribute("aria-expanded", "false");

      // The keyboard works it too.
      await title.focus();
      await page.keyboard.press("Enter");
      await expect(title).toHaveAttribute("aria-expanded", "true");
      expect((await title.boundingBox())!.height).toBeGreaterThan(layout.lineHeight * 1.5);
      // It stays expanded while the view repaints its title (a running command's output does every
      // second)…
      await page.evaluate((p) =>
        (window as unknown as { setViewerTitle: (t: string, isPath: boolean) => void }).setViewerTitle(p, true), path);
      await expect(title).toHaveAttribute("aria-expanded", "true");
      await page.keyboard.press(" ");
      await expect(title).toHaveAttribute("aria-expanded", "false");
      // …and a newly opened view starts with its title collapsed.
      await title.click();
      await expect(title).toHaveAttribute("aria-expanded", "true");
      await page.evaluate((p) =>
        (window as unknown as { openDiffOverlay: (p: string, d: string) => void })
          .openDiffOverlay(p, "@@ -1 +1 @@\n-old\n+new"), path);
      await expect(title).toHaveAttribute("aria-expanded", "false");

      // Details too long for one line wrap rather than being cut: what ends them can matter.
      await page.evaluate(() => {
        document.getElementById("codeMeta")!.textContent =
          "⟳ Running · 12,345 lines · 1.2 MB · retained output truncated";
      });
      const meta = page.locator("#codeMeta");
      expect(await meta.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);

      // With the word hidden, a copy's result shows as the icon, then goes back.
      await page.locator("#codeCopyDiff").click();
      await expect(page.locator("#codeCopyDiff")).toHaveAttribute("data-result", /^(ok|err)$/);
      await expect(page.locator("#codeCopyDiff")).not.toHaveAttribute("data-result", /./, { timeout: 4_000 });
    });

    test("shows the view the Source button switches to as its icon", async ({ page }) => {
      await openThreadWithReply(page);
      await page.locator("#gitLineToggle").click();
      await page.locator('.git-file[data-git-file="NOTES.md"]').click();
      await expect(page.locator("#codeView .code-markdown")).toHaveCount(1);
      const toggle = page.locator("#codeSourceToggle");
      await expect(toggle).toHaveAccessibleName("Source");
      await expect(toggle).toHaveAttribute("data-icon", "source");
      await toggle.click();
      await expect(toggle).toHaveAccessibleName("Rendered");
      await expect(toggle).toHaveAttribute("data-icon", "rendered");
    });
  });
});
