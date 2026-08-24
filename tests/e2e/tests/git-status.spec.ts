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

  // The diff is laid out like a source file — two gutters and one row per line — rather than as a
  // markdown code block nested inside the overlay.
  test("renders a diff as source, with a line number on each side", async ({ page }) => {
    await page.locator("#gitLineToggle").click();
    await page.locator('.git-file[data-git-diff="src/main.rs"]').click();
    await expect(page.locator("#codeOverlay")).toHaveClass(/\bopen\b/);

    // No markdown code block, and no nested box for it to sit in.
    await expect(page.locator("#codeView .code-block")).toHaveCount(0);
    await expect(page.locator("#codeView .diff-table")).toHaveCount(1);
    await expect(page.locator("#codeView .diff-line-nos")).toHaveCount(2);

    const rows = await page.evaluate(() => {
      const gutters = document.querySelectorAll("#codeView .diff-line-nos");
      const old = [...gutters[0].children].map((cell) => cell.textContent ?? "");
      const fresh = [...gutters[1].children].map((cell) => cell.textContent ?? "");
      return [...document.querySelectorAll("#codeView .diff-line")].map((line, i) => ({
        kind: (line.className.match(/diff-(add|del|hunk|meta|context)/) ?? [])[1] ?? "",
        text: line.textContent ?? "",
        old: old[i] ?? "",
        new: fresh[i] ?? "",
      }));
    });

    // Every line has a row, and the three columns stay in step.
    expect(rows.length).toBeGreaterThan(4);
    // The file header carries no line number on either side.
    expect(rows[0]).toMatchObject({ kind: "meta", old: "", new: "" });
    expect(rows.some((r) => r.kind === "hunk" && r.old === "" && r.new === "")).toBe(true);

    // The seed adds one line to a three-line file, so the two sides diverge after it:
    //
    //   1  1   fn main() {
    //   2  2       println!("hello from demo");
    //      3   +   println!("edited for status");
    //   3  4   }
    //
    // The line after the addition is the assertion that matters — old 3 against new 4 is what
    // shows the gutters counting independently rather than one number printed twice.
    const addedAt = rows.findIndex((r) => r.kind === "add");
    expect(addedAt, "the seeded workspace has an added line").toBeGreaterThan(0);
    expect(rows[addedAt].text.startsWith("+")).toBe(true);
    expect(rows[addedAt]).toMatchObject({ old: "", new: "3" });

    const after = rows[addedAt + 1];
    expect(after).toMatchObject({ kind: "context", old: "3", new: "4" });

    // And before it, the sides agree.
    expect(rows[addedAt - 1]).toMatchObject({ kind: "context", old: "2", new: "2" });
  });

  // The same overlay renders the diffs on transcript file-change rows, and those come from the
  // agent rather than from `git`: the Codex adapter passes through a bare hunk with no `diff --git`
  // header at all. The replay harness emits no file-change items, so the renderer is driven
  // directly with the shapes that path produces.
  test("renders an agent's headerless diff", async ({ page }) => {
    const render = async (diff: string) => {
      await page.evaluate((d) => openDiffOverlay("agent.rs", d), diff);
      return page.evaluate(() => {
        const gutters = document.querySelectorAll("#codeView .diff-line-nos");
        const old = [...gutters[0].children].map((c) => c.textContent ?? "");
        const fresh = [...gutters[1].children].map((c) => c.textContent ?? "");
        return [...document.querySelectorAll("#codeView .diff-line")].map((line, i) => ({
          kind: (line.className.match(/diff-(add|del|hunk|meta|context)/) ?? [])[1] ?? "",
          old: old[i] ?? "",
          new: fresh[i] ?? "",
        }));
      });
    };

    // What the Codex adapter actually sends: a hunk header and nothing above it.
    expect(await render("@@ -1 +1 @@\n-old\n+new")).toEqual([
      { kind: "hunk", old: "", new: "" },
      { kind: "del", old: "1", new: "" },
      { kind: "add", old: "", new: "1" },
    ]);

    // Several hunks, each restarting both counters from its own header.
    const multi = await render("@@ -1,2 +1,2 @@\n ctx\n-gone\n+added\n@@ -20,2 +21,2 @@\n ctx2\n-x\n+y");
    expect(multi[5]).toEqual({ kind: "context", old: "20", new: "21" });
    expect(multi[7]).toEqual({ kind: "add", old: "", new: "22" });

    // With no header there is nothing to count from, but the markers still say what changed —
    // greying the whole thing out would be worse than the code block this replaced.
    expect(await render("-just a removal\n+just an addition")).toEqual([
      { kind: "del", old: "", new: "" },
      { kind: "add", old: "", new: "" },
    ]);

    // `---` and `+++` in a file header stay filenames rather than becoming changes.
    const headed = await render("diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -1 +1 @@\n+x");
    expect(headed.slice(0, 3).map((r) => r.kind)).toEqual(["meta", "meta", "meta"]);
  });

  // `git diff` on a conflicted path returns a combined diff: `@@@` with one range per parent, and
  // one marker column per parent. Read as a plain diff, a line brought in by one side (` +MAIN`)
  // looks like context while a conflict marker (`++=======`) looks like an addition.
  test("reads a combined diff from a conflicted path", async ({ page }) => {
    const rows = await page.evaluate(() => {
      openDiffOverlay(
        "f.txt",
        "diff --cc f.txt\n@@@ -1,3 -1,3 +1,7 @@@\n  a\n++<<<<<<< HEAD\n +MAIN\n++=======\n+ SIDE\n++>>>>>>> side\n  c\n",
      );
      const gutters = document.querySelectorAll("#codeView .diff-line-nos");
      const old = [...gutters[0].children].map((c) => c.textContent ?? "");
      const fresh = [...gutters[1].children].map((c) => c.textContent ?? "");
      return [...document.querySelectorAll("#codeView .diff-line")].map((line, i) => ({
        kind: (line.className.match(/diff-(add|del|hunk|meta|context)/) ?? [])[1] ?? "",
        old: old[i] ?? "",
        new: fresh[i] ?? "",
      }));
    });

    expect(rows[1].kind, "the `@@@` header is a hunk header").toBe("hunk");
    // The line one side contributed is an addition, not context.
    expect(rows[4]).toMatchObject({ kind: "add", new: "3" });
    // Numbering follows the result range (`+1,7`); with two parents there is no single old side.
    expect(rows.map((r) => r.new).filter(Boolean)).toEqual(["1", "2", "3", "4", "5", "6", "7"]);
    expect(rows.every((r) => r.old === "")).toBe(true);
  });

  // A regenerated lockfile can run to six figures, and three DOM nodes per line would block the
  // tab. The rows are capped and the shortfall stated; the copy still carries the whole patch.
  test("caps a very large diff without losing it", async ({ page }) => {
    const total = 60_000;
    const result = await page.evaluate((n) => {
      const body = Array.from({ length: n }, (_, i) => (i % 2 ? "+" : "-") + "line " + i);
      const diff = ["diff --git a/big b/big", `@@ -1,${n} +1,${n} @@`, ...body].join("\n");
      openDiffOverlay("big", diff);
      const lines = [...document.querySelectorAll("#codeView .diff-line")];
      return {
        rendered: lines.length,
        last: lines[lines.length - 1].textContent ?? "",
        copiedLines: (state.diffOverlayText ?? "").split("\n").length,
      };
    }, total);

    expect(result.rendered).toBeLessThan(total);
    expect(result.last).toContain("more lines not shown");
    // Nothing is lost — only what is drawn is bounded.
    expect(result.copiedLines).toBe(total + 2);
  });

  test("offers the raw diff for copying", async ({ page }) => {
    await expect(page.locator("#codeCopyDiff")).toBeHidden();
    await page.locator("#gitLineToggle").click();
    await page.locator('.git-file[data-git-diff="src/main.rs"]').click();
    await expect(page.locator("#codeCopyDiff")).toBeVisible();

    // The rendered rows are separate cells, so the button hands back what git produced rather than
    // whatever a manual selection would drag in.
    // `state` is a top-level `let`, so it lives in script scope rather than on `window`.
    const copied = await page.evaluate(() => state.diffOverlayText);
    expect(copied).toContain("diff --git");
    expect(copied).toContain("+");

    // It belongs to the diff view only.
    await page.locator("#codeClose").click();
    await expect(page.locator("#codeCopyDiff")).toBeHidden();
  });

  // The overlay is shared, so a view that takes it over has to hand back what the previous one
  // put there — including when the takeover happens without closing first.
  test("drops the diff state when a source file takes over the overlay", async ({ page }) => {
    // A source view is read as a thread sees the file, so this takeover needs a real thread — the
    // diff beneath it does not, which is exactly why the two can meet in one overlay.
    await page.locator("#input").fill("Open a source file over a diff");
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();

    await page.locator("#gitLineToggle").click();
    await page.locator('.git-file[data-git-diff="src/main.rs"]').click();
    await expect(page.locator("#codeCopyDiff")).toBeVisible();

    // Straight from the diff into a source file, with the overlay still open.
    let releaseHighlight: (() => void) | undefined;
    const highlightMayContinue = new Promise<void>(resolve => { releaseHighlight = resolve; });
    await page.route("**/highlight?path=src%2Fmain.rs", async route => {
      await highlightMayContinue;
      await route.continue();
    });
    await page.evaluate(() => { void openCodeOverlay("src/main.rs"); });
    await expect(page.locator("#codeCopyDiff")).toBeHidden();
    await expect.poll(() => page.evaluate(() => state.diffOverlayText)).toBeNull();
    await expect(page.locator("#codeView")).toHaveText("Loading source…");
    await expect(page.locator("#codeDownload")).toBeEnabled();
    releaseHighlight?.();
    // And the view really is the source one now, not the diff left behind.
    await expect(page.locator("#codeView .diff-table")).toHaveCount(0);
    await expect(page.locator("#codeView")).toContainText("fn main");
    await expect(page.locator("#codeDownload")).toBeEnabled();
  });

  test("says so when the clipboard refuses the diff", async ({ page }) => {
    await page.locator("#gitLineToggle").click();
    await page.locator('.git-file[data-git-diff="src/main.rs"]').click();
    await expect(page.locator("#codeCopyDiff")).toBeVisible();

    // `copyToClipboard` is a function declaration, so it is replaceable on `window`; the fallback
    // path it wraps can fail for real when the app is served over plain HTTP.
    await page.evaluate(() => {
      (window as never as { copyToClipboard: () => Promise<boolean> }).copyToClipboard =
        async () => false;
    });
    await page.locator("#codeCopyDiff").click();
    await expect(page.locator("#codeCopyDiff")).toHaveText("Copy failed");

    // The label goes back so the button is usable again rather than stuck on the error.
    await expect(page.locator("#codeCopyDiff")).toHaveText("Copy diff", { timeout: 5_000 });
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
declare const state: { diffOverlayText: string | null };
declare function openDiffOverlay(path: string, diff: string): void;
declare function openCodeOverlay(path: string, line?: number): Promise<void>;
