import { test, expect, type Page } from "@playwright/test";
import { login } from "./helpers";

/**
 * The mobile top bar (#mobileBar) and the thread status header (header.thr) must stay pinned at
 * the top of the screen on mobile — both while the transcript scrolls and when the on-screen
 * keyboard opens.
 *
 * The layout makes the two bars the first, non-scrolling flex children of `.center` (a flex
 * column), with only #transcript (flex:1, overflow-y:auto) scrolling internally. So the bars are
 * inherently above the scroll and can't be scrolled away by the transcript.
 *
 * The on-screen keyboard is the harder case and differs by browser:
 *  - Android / Firefox: the `interactive-widget=resizes-content` viewport meta resizes the layout
 *    viewport, so 100dvh shrinks and the column reflows above the keyboard.
 *  - iOS Safari: ignores interactive-widget, overlays the keyboard without resizing the layout,
 *    then shifts the layout viewport to reveal the focused composer — which would push the top
 *    bars off-screen. app.js syncAppHeight counters this by sizing the shell to
 *    window.visualViewport.height (the visible area above the keyboard) via --app-height, so the
 *    column shrinks to the visible region and there's nothing for Safari to scroll away.
 *
 * An earlier attempt used position:sticky on the bars, but that was a dead no-op: sticky sticks
 * within a scroll container, and .center never scrolls. These tests pin the real invariants so a
 * future regression (bars scrolling with the transcript, or the keyboard scrolling the bars away)
 * fails loudly.
 */

async function openDraft(page: Page) {
  // On mobile the projects live in a drawer; reveal it before opening a draft. On desktop the
  // hamburger is hidden, so this is a no-op.
  const menuButton = page.locator("#btnMenu");
  if (await menuButton.isVisible()) await menuButton.click();
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await expect(page.locator("#input")).toBeVisible();
}

/** Inject a tall block into the transcript so it overflows and scrolls. */
async function fillTallTranscript(page: Page) {
  await page.evaluate(() => {
    const t = document.getElementById("transcript")!;
    const div = document.createElement("div");
    div.style.height = "4000px";
    div.textContent = "tall filler";
    t.appendChild(div);
  });
}

/** Viewport-relative tops of the two bars and the transcript's scrollTop. */
function barTops(page: Page) {
  return page.evaluate(() => {
    const mb = document.getElementById("mobileBar")!.getBoundingClientRect();
    const th = document.getElementById("thrHeader")!.getBoundingClientRect();
    const t = document.getElementById("transcript")!;
    return {
      mbTop: Math.round(mb.top),
      mbHeight: Math.round(mb.height),
      thTop: Math.round(th.top),
      transcriptScrolled: t.scrollTop > 0,
    };
  });
}

/** Distance between the transcript's current viewport and its latest content. */
function transcriptBottomGap(page: Page) {
  return page.evaluate(() => {
    const t = document.getElementById("transcript")!;
    return Math.round(t.scrollHeight - t.scrollTop - t.clientHeight);
  });
}

/** Simulate the iOS Safari keyboard: shrink the visual viewport without resizing the layout, and
 *  dispatch the visualViewport resize/scroll events that syncAppHeight listens for. The layout
 *  viewport (window.innerHeight) stays at the full 844px, modelling Safari overlaying the keyboard
 *  rather than resizing the page. Returns the final visible height used. */
async function simulateIOSKeyboard(
  page: Page,
  visibleHeight = 400,
  offsetTop = 0,
): Promise<void> {
  // Model the iOS Safari keyboard: the visual viewport shrinks to the area above the keyboard
  // (height = visibleHeight) and may pan down from the layout-viewport top (offsetTop > 0) when
  // Safari shifts the layout viewport to reveal the focused composer. window.innerHeight stays
  // at the full 844 (the LAYOUT viewport is NOT resized). Playwright's headless Chromium has a
  // real visualViewport; shadow its properties with getters and dispatch the resize + scroll
  // events syncAppHeight listens to.
  await page.evaluate(([vh, ot]) => {
    const vv = window.visualViewport!;
    Object.defineProperty(vv, "height", { configurable: true, get: () => vh });
    Object.defineProperty(vv, "offsetTop", { configurable: true, get: () => ot });
    vv.dispatchEvent(new Event("resize"));
    vv.dispatchEvent(new Event("scroll"));
  }, [visibleHeight, offsetTop]);
}

test.describe("mobile title + status bars stay pinned at the top", () => {
  test.use({
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 3,
    isMobile: true,
    hasTouch: true,
  });

  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("bars stay at the top while the transcript scrolls", async ({ page }) => {
    await openDraft(page);
    await fillTallTranscript(page);

    // Sanity: the transcript is actually scrollable, otherwise the assertion is vacuous.
    const scrollable = await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      return el.scrollHeight > el.clientHeight + 1;
    });
    expect(scrollable).toBe(true);

    // Before scrolling the bars sit at the top of the column.
    expect((await barTops(page)).mbTop).toBe(0);

    // Scroll the transcript to the bottom and well past it, then poll until it has actually
    // scrolled (the scroll can settle a frame later), before reading the bars' positions.
    await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      el.scrollTop = el.scrollHeight;
    });
    await expect.poll(async () => (await barTops(page)).transcriptScrolled).toBe(true);
    const after = await barTops(page);
    expect(after.mbTop).toBe(0);
    // The thread header stacks directly below the mobile bar, separated by its own height.
    expect(after.thTop).toBe(after.mbTop + after.mbHeight);
  });

  test("bars stay at the top when the keyboard shrinks the LAYOUT viewport (Android)", async ({ page }) => {
    await openDraft(page);
    await fillTallTranscript(page);

    // A conversation normally follows its newest row. The keyboard resize must preserve that
    // bottom anchor, otherwise the shortened transcript hides its latest content below the fold.
    await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      el.scrollTop = el.scrollHeight;
    });
    expect(await transcriptBottomGap(page)).toBeLessThanOrEqual(1);

    // Content-resize regime: the keyboard shrinks the layout viewport (Android + Firefox with
    // interactive-widget=resizes-content). 100dvh / --app-height track it, so the column reflows
    // above the keyboard and the bars stay at top:0. Focus the composer (as opening the keyboard
    // would) and shrink the viewport to mimic the keyboard taking ~half the screen.
    await page.locator("#input").focus();
    await page.setViewportSize({ width: 390, height: 400 });
    // Wait for the shell to reflow to the new viewport height before measuring.
    await expect.poll(async () => (await barTops(page)).mbTop).toBe(0);
    const before = await barTops(page);
    expect(before.mbTop).toBe(0);
    await expect.poll(async () => transcriptBottomGap(page)).toBeLessThanOrEqual(1);

    // Scroll the now-shorter transcript and re-check.
    await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      el.scrollTop = el.scrollHeight;
    });
    await expect.poll(async () => (await barTops(page)).transcriptScrolled).toBe(true);
    const after = await barTops(page);
    expect(after.mbTop).toBe(0);
  });

  test("bars stay at the top when the keyboard OVERLAYS the viewport (iOS Safari)", async ({ page }) => {
    await openDraft(page);
    await fillTallTranscript(page);

    await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      el.scrollTop = el.scrollHeight;
    });
    expect(await transcriptBottomGap(page)).toBeLessThanOrEqual(1);

    // iOS Safari regime: the keyboard overlays WITHOUT resizing the layout (window.innerHeight
    // stays 844). Without the fix, Safari shifts the layout viewport to reveal the focused
    // composer and the bars scroll off the top. syncAppHeight sizes the shell to
    // visualViewport.height so the column shrinks to the visible region and the bars stay put.
    await page.locator("#input").focus();
    await simulateIOSKeyboard(page, 400);

    // The shell height must have shrunk to the visible area (400px), proving --app-height took
    // effect. window.innerHeight is unchanged (844) — the layout viewport was NOT resized. Poll
    // until the shell has actually reflowed before asserting the bars, since the visualViewport
    // listener runs asynchronously.
    await expect.poll(async () => await page.evaluate(() => {
      const c = document.querySelector(".center") as HTMLElement;
      return Math.round(c.getBoundingClientRect().height);
    })).toBeLessThan(500);
    const shell = await page.evaluate(() => {
      const c = document.querySelector(".center") as HTMLElement;
      return {
        centerHeight: Math.round(c.getBoundingClientRect().height),
        appHeightVar: getComputedStyle(document.documentElement).getPropertyValue("--app-height").trim(),
        innerHeight: window.innerHeight,
      };
    });
    expect(shell.innerHeight).toBe(844); // layout viewport NOT resized (iOS overlay regime)
    expect(shell.centerHeight).toBeLessThan(500); // shell shrank to the visible area
    expect(shell.appHeightVar).toBe("400px");
    // The latest transcript rows move up with the shortened scrollport instead of remaining below
    // it where the overlaid keyboard would hide them.
    await expect.poll(async () => transcriptBottomGap(page)).toBeLessThanOrEqual(1);

    // The bars stay pinned at the top of the now-shorter visible region.
    const after = await barTops(page);
    expect(after.mbTop).toBe(0);
    expect(after.thTop).toBe(after.mbHeight);

    // And scrolling the transcript still doesn't move them.
    await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      el.scrollTop = el.scrollHeight;
    });
    await expect.poll(async () => (await barTops(page)).transcriptScrolled).toBe(true);
    const afterScroll = await barTops(page);
    expect(afterScroll.mbTop).toBe(0);
  });

  test("shell tracks the visual-viewport pan (offsetTop) on iOS Safari", async ({ page }) => {
    await openDraft(page);
    await fillTallTranscript(page);

    // When iOS Safari shifts the layout viewport to reveal the focused composer, the visual
    // viewport pans DOWN relative to the layout viewport (visualViewport.offsetTop > 0). The
    // shell must translate down by offsetTop so it stays aligned with the visible region's top
    // edge; without that the bars sit above the visible area (top < 0) and get clipped.
    await page.locator("#input").focus();
    await simulateIOSKeyboard(page, 400, 120); // 120px pan

    // --app-height shrinks to 400 and --app-top becomes 120px; the shell is translated to sit
    // inside the visible region.
    await expect.poll(async () => await page.evaluate(() => {
      const c = document.querySelector(".center") as HTMLElement;
      return Math.round(c.getBoundingClientRect().top);
    })).toBe(120);
    const vars = await page.evaluate(() => ({
      appHeight: getComputedStyle(document.documentElement).getPropertyValue("--app-height").trim(),
      appTop: getComputedStyle(document.documentElement).getPropertyValue("--app-top").trim(),
    }));
    expect(vars.appHeight).toBe("400px");
    expect(vars.appTop).toBe("120px");

    // The bars stay at the top of the (translated) shell, i.e. at the visible region's top.
    const tops = await barTops(page);
    expect(tops.mbTop).toBe(120);
    expect(tops.thTop).toBe(120 + tops.mbHeight);
  });

  test(".center never becomes a scroll container (sticky has nothing to stick to)", async ({ page }) => {
    await openDraft(page);
    await fillTallTranscript(page);

    // .center must not scroll: if it did, the bars would need a real sticky (or to be moved inside
    // the scrolling element) to stay pinned. Asserting it never scrolls is the invariant that
    // makes the no-sticky layout correct.
    const centerScrollable = await page.evaluate(() => {
      const c = document.querySelector(".center") as HTMLElement;
      return c.scrollHeight > c.clientHeight + 1;
    });
    expect(centerScrollable).toBe(false);

    // And even after the keyboard shrinks the shell (visual-viewport regime), .center still must
    // not scroll — the shell shrinks instead, keeping the bars in the visible area.
    await page.locator("#input").focus();
    await simulateIOSKeyboard(page, 400);
    // Wait for the shell to reflow to the shrunk height before re-checking scrollability.
    await expect.poll(async () => await page.evaluate(() => {
      const c = document.querySelector(".center") as HTMLElement;
      return Math.round(c.getBoundingClientRect().height);
    })).toBeLessThan(500);
    const centerScrollableShrunk = await page.evaluate(() => {
      const c = document.querySelector(".center") as HTMLElement;
      return c.scrollHeight > c.clientHeight + 1;
    });
    expect(centerScrollableShrunk).toBe(false);
  });
});

// Desktop mirror: on desktop the mobile bar is hidden, but the thread header must still stay
// pinned at the top of the (non-scrolling) center column while the transcript scrolls.
test.describe("desktop thread status header stays pinned at the top", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("header stays at the top while the transcript scrolls", async ({ page }) => {
    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();
    await fillTallTranscript(page);

    const scrollable = await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      return el.scrollHeight > el.clientHeight + 1;
    });
    expect(scrollable).toBe(true);

    const headerTopBefore = await page.evaluate(() =>
      Math.round(document.getElementById("thrHeader")!.getBoundingClientRect().top),
    );
    expect(headerTopBefore).toBe(0);

    await page.evaluate(() => {
      const el = document.getElementById("transcript")!;
      el.scrollTop = el.scrollHeight;
    });
    // Poll until the scroll has settled before reading the header's position.
    await expect.poll(async () => await page.evaluate(() =>
      document.getElementById("transcript")!.scrollTop > 0,
    )).toBe(true);
    const headerTopAfter = await page.evaluate(() =>
      Math.round(document.getElementById("thrHeader")!.getBoundingClientRect().top),
    );
    expect(headerTopAfter).toBe(0);
  });
});

// The header popovers (Tasks/MCP/Context) are position:absolute descendants of their wrappers
// inside header.thr. The header carries no stacking context (no position:sticky + z-index), so
// the popovers paint at the .center level (z-index:35). This locks in that they still overlay
// the transcript and stay within the viewport.
test.describe("header popovers overlay the transcript", () => {
  test.use({
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 3,
    isMobile: true,
    hasTouch: true,
  });

  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("context popover opens above the transcript without clipping", async ({ page }) => {
    await openDraft(page);
    // Start a real turn so a real thread exists (the usage/context button is disabled on a
    // draft). Wait for the scripted reply so the thread is fully open.
    const input = page.locator("#input");
    await input.fill("populate the thread");
    await page.locator("#sendBtn").click();
    await expect(page.locator("#transcript .msg.agent").first()).toBeVisible();
    await fillTallTranscript(page);

    // The Context/usage button is enabled once a real thread is open, unlike the Tasks button
    // (disabled when idle) and MCP (disabled when no servers).
    await expect(page.locator("#usageBtn")).toBeEnabled();
    await page.locator("#usageBtn").click();
    const popover = page.locator("#usageMenu");
    await expect(popover).toBeVisible();

    // The popover must paint above the transcript: its rendered rect must overlap the transcript's
    // top area and not be hidden behind it (check via elementFromPoint at the popover's centre).
    const overlap = await page.evaluate(() => {
      const pop = document.getElementById("usageMenu")!;
      const tr = document.getElementById("transcript")!;
      const pr = pop.getBoundingClientRect();
      const trr = tr.getBoundingClientRect();
      const cx = Math.round(pr.left + pr.width / 2);
      const cy = Math.round(pr.top + pr.height / 2);
      const top = document.elementFromPoint(cx, cy) as HTMLElement | null;
      const inPopover = top ? (top.closest("#usageMenu") ? true : false) : false;
      return {
        popoverTop: Math.round(pr.top),
        transcriptTop: Math.round(trr.top),
        popoverOverlapsTranscript: pr.bottom > trr.top,
        elementAtCentreInPopover: inPopover,
      };
    });
    expect(overlap.popoverOverlapsTranscript).toBe(true);
    expect(overlap.elementAtCentreInPopover).toBe(true);
  });
});
