import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

// The Git status line above the composer. The replay server seeds its demo workspace as a real
// repository on `main` with one modified tracked file (`src/main.rs`), so the line has a branch, a
// change count and one row to list — see `seed_git_workspace` in `giskard-server-replay.rs`.
//
// The suite shares one stateful server, so these assertions never assume a clean slate beyond that
// seeded workspace: they identify the row under test by path and read the line's own fields.
test.describe("git status line", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await expect(page.locator("#gitLine")).toBeVisible();
  });

  test("summarises the working tree and lists its changes", async ({ page }) => {
    // Collapsed, the line answers the common questions without being opened.
    await expect(page.locator("#gitBranch")).toHaveText("main");
    await expect(page.locator("#gitLine")).toHaveClass(/\bstate-dirty\b/);
    await expect(page.locator("#gitCount")).toHaveText("1");
    await expect(page.locator("#gitLineBody")).toBeHidden();

    // It expands in place rather than into a popover.
    await page.locator("#gitLineToggle").click();
    await expect(page.locator("#gitLineToggle")).toHaveAttribute("aria-expanded", "true");
    await expect(page.locator("#gitLineBody")).toBeVisible();

    const row = page.locator('.git-file[data-git-diff="src/main.rs"]');
    await expect(row).toHaveCount(1);
    // A tracked worktree edit belongs to the unstaged side, and the row carries that side so its
    // diff matches the counts printed beside it.
    await expect(row).toHaveAttribute("data-git-side", "unstaged");
    await expect(row.locator(".git-file-name")).toHaveText("main.rs");
    await expect(row.locator(".git-file-status")).toHaveText("M");

    // The whole row is the button: clicking it opens the existing diff overlay.
    await row.click();
    await expect(page.locator("#codeOverlay")).toHaveClass(/\bopen\b/);
    await expect(page.locator("#codePath")).toHaveText("Diff: src/main.rs");
  });

  test("re-rendering an unchanged list keeps its rows", async ({ page }) => {
    await page.locator("#gitLineToggle").click();
    await expect(page.locator("#gitLineBody")).toBeVisible();

    // The list is rebuilt from a string, so an unchanged refresh must leave the existing nodes in
    // place — rebuilding one would discard the reader's scroll position in it.
    await page.locator(".git-file").first().evaluate((node) => {
      (node as HTMLElement & { _marker?: string })._marker = "kept";
    });
    await page.locator("#gitRefresh").click();
    await expect
      .poll(async () =>
        page.locator(".git-file").first().evaluate(
          (node) => (node as HTMLElement & { _marker?: string })._marker ?? null,
        ),
      )
      .toBe("kept");
  });

  // The line has one flexible item and a branch name nobody controls, so the shortening rule is
  // what keeps the two from fighting. Exercised directly: it is a pure function, and driving it
  // through real viewports would only test the width tiers around it.
  test("shortens branch names by shedding the prefix before the tail", async ({ page }) => {
    const shorten = (name: string, budget: number) =>
      page.evaluate(
        ([n, b]) => {
          const parts = (window as never as { gitBranchParts: typeof gitBranchParts })
            .gitBranchParts(n as string, b as number);
          return parts.prefix + parts.tail;
        },
        [name, budget] as const,
      );

    // Fits: the prefix is kept whole, only dimmed.
    expect(await shorten("feature/git-project-status-ui", 60)).toBe("feature/git-project-status-ui");
    // Too long, but the identifying tail still fits: the prefix goes first.
    expect(await shorten("dependabot/cargo/tokio-1.42.0-security-backport", 34)).toBe(
      "…/tokio-1.42.0-security-backport",
    );
    // Tail alone is too long: it is cut from the head, never the end.
    expect(await shorten("claude/git-info-ui-design-54ikod", 22)).toBe("…info-ui-design-54ikod");
    // A name clipped to a few characters is worse than none, so the tail stops shrinking at a
    // ten-character floor however small the budget gets.
    expect(await shorten("feature/a-very-long-single-branch-name", 4)).toBe("…ranch-name");
    expect(await shorten("feature/a-very-long-single-branch-name", 1)).toBe("…ranch-name");
    // No prefix to shed.
    expect(await shorten("main", 60)).toBe("main");
  });

  // The working tree changes while the agent works, so a line that only reflects the moment the
  // thread was opened is decoration. A finished turn has to refresh it — and once, however many
  // files the turn touched.
  test("refreshes itself when a turn finishes", async ({ page }) => {
    // Two things make the first send the wrong turn to measure: opening a thread loads status by
    // itself, and creating a thread starts its turn over HTTP before the socket is attached, so
    // the completion never reaches the client. Create the thread, re-open it from the sidebar so
    // the socket is live, and measure the turn after that.
    await page.locator("#input").fill("First turn, which creates the thread");
    await page.locator("#sendBtn").click();
    await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

    await page.locator(".thread").first().click();
    await expect(page.locator("#gitLine")).toBeVisible();
    await expect(page.locator("#sendBtn")).toBeVisible();
    await page.waitForTimeout(2_000); // let the thread-open load and its debounce settle

    let statusRequests = 0;
    page.on("request", (request) => {
      if (request.url().includes("/git/status")) statusRequests += 1;
    });

    await page.locator("#input").fill("Second turn, in a thread that is already open");
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toHaveCount(2);

    await expect.poll(() => statusRequests, { timeout: 10_000 }).toBeGreaterThan(0);
    // `expect.poll` returns on the first request, so a second one arriving a moment later would go
    // unseen — wait past a debounce window before pinning the count.
    await page.waitForTimeout(2_000);
    expect(statusRequests).toBe(1);
  });

  // The turn above triggers a single refresh, so it cannot show that several triggers coalesce.
  // A turn that edits twenty files fires the scheduler twenty times, and that is the case the
  // debounce exists for, so it is driven directly.
  test("coalesces a burst of refreshes into one request", async ({ page }) => {
    let statusRequests = 0;
    page.on("request", (request) => {
      if (request.url().includes("/git/status")) statusRequests += 1;
    });

    await page.evaluate(() => {
      const schedule = (window as never as { scheduleGitRefresh: () => void }).scheduleGitRefresh;
      for (let i = 0; i < 20; i += 1) schedule();
    });
    await expect.poll(() => statusRequests, { timeout: 10_000 }).toBeGreaterThan(0);
    await page.waitForTimeout(2_000);
    expect(statusRequests).toBe(1);
  });

  // The line sits between the transcript and the composer, and has to read as its own band rather
  // than as part of either. That means a rule on each boundary — including below the file list when
  // it is open, which is the edge that is easy to lose.
  test("keeps a separator on every edge, collapsed and expanded", async ({ page }) => {
    const borderTop = (selector: string) =>
      page.locator(selector).evaluate((node) => getComputedStyle(node).borderTopWidth);

    expect(await borderTop("#gitLine")).toBe("1px");
    expect(await borderTop("#composer")).toBe("1px");

    await page.locator("#gitLineToggle").click();
    await expect(page.locator("#gitLineBody")).toBeVisible();
    // Head from list, and list from composer.
    expect(await borderTop("#gitLineBody")).toBe("1px");
    expect(await borderTop("#composer")).toBe("1px");
  });

  // The branch glyph carries the appearance theme's accent, so it belongs to the theme rather than
  // being another grey mark in the row.
  test("tints the branch glyph with the theme accent", async ({ page }) => {
    const iconColor = async (theme: string) => {
      await page.evaluate((t) => document.documentElement.setAttribute("data-appearance", t), theme);
      return page.locator("#gitIcon").evaluate((node) => getComputedStyle(node).color);
    };
    const accent = async () =>
      page.evaluate(() =>
        getComputedStyle(document.documentElement).getPropertyValue("--accent").trim(),
      );

    for (const theme of ["ide", "bubbles", "terminal"]) {
      const [color, themeAccent] = [await iconColor(theme), await accent()];
      expect(themeAccent).not.toBe("");
      // Compare through a canvas-free parse: the computed colour is rgb(), the token is a hex.
      const expected = await page.evaluate((hex) => {
        const probe = document.createElement("span");
        probe.style.color = hex;
        document.body.append(probe);
        const value = getComputedStyle(probe).color;
        probe.remove();
        return value;
      }, themeAccent);
      expect(color, `branch glyph should match the ${theme} accent`).toBe(expected);
    }
  });

  // An untracked directory is reported collapsed, with a trailing slash. Its own name is what the
  // reader is looking for, so it has to end up in the bold segment rather than leaving the row
  // dimmed with an empty filename.
  test("renders a collapsed untracked directory as its own name", async ({ page }) => {
    const parts = (path: string) =>
      page.evaluate(
        (p) => {
          const html = (window as never as { renderGitPath: typeof renderGitPath }).renderGitPath(p);
          const host = document.createElement("div");
          host.innerHTML = html;
          return {
            dir: host.querySelector(".git-file-dir")?.textContent ?? "",
            name: host.querySelector(".git-file-name")?.textContent ?? "",
          };
        },
        path,
      );

    expect(await parts("node_modules/")).toEqual({ dir: "", name: "node_modules/" });
    expect(await parts("crates/giskard-server/target/")).toEqual({
      dir: "crates/giskard-server/",
      name: "target/",
    });
    // Ordinary files are unaffected: directory dimmed, basename bold.
    expect(await parts("src/main.rs")).toEqual({ dir: "src/", name: "main.rs" });
    expect(await parts("README.md")).toEqual({ dir: "", name: "README.md" });
  });
});

declare function gitBranchParts(name: string, budget: number): { prefix: string; tail: string };
declare function renderGitPath(path: string): string;
