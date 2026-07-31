import { test, expect } from "@playwright/test";
import { login } from "./helpers";

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
