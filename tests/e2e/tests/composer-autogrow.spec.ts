import { test, expect, type Page } from "@playwright/test";
import { login, simulateIOSKeyboard } from "./helpers";

/**
 * The composer grows with what is typed instead of holding a single line.
 *
 * Long prompts are the common case for an agent, and the old fixed-height box showed only the last
 * line of one. The textarea now sizes itself to its content (autosizeComposer in app.js) up to a cap
 * expressed in CSS as a fraction of the *visible* viewport (--composer-max-height, currently 30% of
 * --app-height); past the cap it stops growing and scrolls instead.
 *
 * These tests assert against the *computed* max-height rather than a literal fraction, so tuning the
 * limit — or splitting it per breakpoint, or exposing it as a setting — does not rewrite them. What
 * they pin is the behaviour: it grows, it is bounded, the bound is a real fraction of the viewport
 * rather than the whole screen, it scrolls once bounded, and it collapses again when emptied.
 */

const ONE_LINE = "one line";
/** Comfortably taller than 30% of any viewport under test, so the cap is definitely reached. */
const MANY_LINES = Array.from({ length: 60 }, (_, i) => `line ${i + 1}`).join("\n");

async function openDraftComposer(page: Page) {
  // On mobile the projects live in a drawer; reveal it before opening a draft. On desktop the
  // hamburger is hidden, so this is a no-op.
  const menuButton = page.locator("#btnMenu");
  if (await menuButton.isVisible()) await menuButton.click();
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  const input = page.locator("#input");
  await expect(input).toBeVisible();
  return input;
}

/** What the composer's box actually is right now, and what CSS allows it to become. */
function composerBox(page: Page) {
  return page.evaluate(() => {
    const ta = document.getElementById("input") as HTMLTextAreaElement;
    const style = getComputedStyle(ta);
    return {
      height: Math.round(ta.getBoundingClientRect().height),
      maxHeight: Math.round(parseFloat(style.maxHeight)),
      minHeight: Math.round(parseFloat(style.minHeight)),
      overflowY: style.overflowY,
      // Whether the content is actually taller than the box, i.e. there is something to scroll.
      scrolls: ta.scrollHeight > ta.clientHeight + 1,
      appHeight: Math.round(
        parseFloat(getComputedStyle(document.documentElement).getPropertyValue("--app-height")),
      ),
    };
  });
}

/** Distance between the transcript's current viewport and its latest content. */
function transcriptBottomGap(page: Page) {
  return page.evaluate(() => {
    const t = document.getElementById("transcript")!;
    return t.scrollHeight - t.scrollTop - t.clientHeight;
  });
}

/** Height of the transcript, to prove the composer never swallows the conversation. */
function transcriptHeight(page: Page) {
  return page.evaluate(() =>
    Math.round(document.getElementById("transcript")!.getBoundingClientRect().height));
}

/**
 * The shared assertions, run at whatever viewport the calling describe block sets. `fill()` is used
 * rather than typing because the app resizes on the `input` event, which `fill` dispatches once —
 * the same signal a paste produces, and the case a one-line box handled worst.
 */
async function assertGrowsAndClamps(page: Page) {
  const input = await openDraftComposer(page);

  const atRest = await composerBox(page);
  // The empty composer is unchanged from the single-line box it has always been.
  expect(atRest.height).toBe(atRest.minHeight);
  // The cap is a genuine fraction of the visible viewport: bigger than one line, but leaving most
  // of the screen to the transcript.
  expect(atRest.maxHeight).toBeGreaterThan(atRest.minHeight);
  expect(atRest.maxHeight).toBeLessThan(atRest.appHeight / 2);

  // One line still occupies exactly the at-rest box: no jump on the first keystroke.
  await input.fill(ONE_LINE);
  expect((await composerBox(page)).height).toBe(atRest.height);

  // A handful of lines grows the box without reaching the cap, and nothing scrolls yet.
  await input.fill("a\nb\nc\nd");
  const grown = await composerBox(page);
  expect(grown.height).toBeGreaterThan(atRest.height);
  expect(grown.height).toBeLessThan(grown.maxHeight);
  expect(grown.overflowY).toBe("hidden");
  expect(grown.scrolls).toBe(false);

  // Far more text than fits: the box stops at the cap and becomes scrollable instead of growing
  // over the transcript.
  await input.fill(MANY_LINES);
  const clamped = await composerBox(page);
  expect(clamped.height).toBe(clamped.maxHeight);
  expect(clamped.overflowY).toBe("auto");
  expect(clamped.scrolls).toBe(true);

  // The rest of the thread view survives: the transcript still has real height and the send control
  // is still on screen rather than pushed below the fold.
  expect(await transcriptHeight(page)).toBeGreaterThan(0);
  const sendBox = await page.locator("#sendBtn").boundingBox();
  const viewport = page.viewportSize()!;
  expect(sendBox).not.toBeNull();
  expect(sendBox!.y + sendBox!.height).toBeLessThanOrEqual(viewport.height);

  // Deleting the text collapses the box back to one line — it shrinks as well as grows.
  await input.fill(ONE_LINE);
  expect((await composerBox(page)).height).toBe(atRest.height);

  return { input, atRest };
}

test.describe("composer auto-grow (desktop)", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("grows with its content, clamps at the cap, and scrolls beyond it", async ({ page }) => {
    await assertGrowsAndClamps(page);
  });

  test("collapses back to one line after sending", async ({ page }) => {
    const input = await openDraftComposer(page);
    const atRest = await composerBox(page);

    await input.fill(MANY_LINES);
    expect((await composerBox(page)).height).toBeGreaterThan(atRest.height);

    // Plain Enter sends on desktop.
    await input.press("Enter");
    await expect(input).toHaveValue("");
    // The next prompt starts from a single line, not from the previous one's box.
    await expect.poll(async () => (await composerBox(page)).height).toBe(atRest.height);
    expect((await composerBox(page)).overflowY).toBe("hidden");
  });

  test("a transcript following its newest row keeps following it as the box grows", async ({ page }) => {
    const input = await openDraftComposer(page);
    // A transcript tall enough to scroll, parked at its newest row — the state a conversation is
    // normally in while its next prompt is being typed.
    await page.evaluate(() => {
      const t = document.getElementById("transcript")!;
      const filler = document.createElement("div");
      filler.style.height = "4000px";
      t.appendChild(filler);
      t.scrollTop = t.scrollHeight;
    });
    expect(await transcriptBottomGap(page)).toBeLessThanOrEqual(1);

    // Growing the composer takes height from the transcript. A scroll container keeps its scrollTop
    // across a resize, not its distance from the bottom, so without restoring the anchor the newest
    // row would slide out of view behind the growing box.
    await input.fill(MANY_LINES);
    await expect.poll(async () => transcriptBottomGap(page)).toBeLessThanOrEqual(1);
  });

  test("restores the height of a draft when switching back to it", async ({ page }) => {
    // A thread to switch away to: send once, which turns the draft into a persisted thread. The
    // suite shares one server, so this thread is identified by the id the app just selected rather
    // than by its position in a list that also holds every other spec's threads.
    const input = await openDraftComposer(page);
    const atRest = await composerBox(page);
    await input.fill("first message");
    await input.press("Enter");
    await expect(page.locator("#transcript .msg.user", { hasText: "first message" })).toBeVisible();
    await expect(page.locator(".thread.active")).toHaveCount(1);
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    expect(tid).toBeTruthy();
    await expect.poll(async () => (await composerBox(page)).height).toBe(atRest.height);

    // Type a tall draft in a *new* thread, leaving the persisted one holding nothing.
    await openDraftComposer(page);
    await input.fill(MANY_LINES);
    const tall = await composerBox(page);
    expect(tall.height).toBe(tall.maxHeight);

    // Switch to the persisted thread: its own (empty) draft sizes the box back to one line.
    await page.locator(`.thread[data-tid="${tid}"]`).click();
    await expect(page.locator(".thread.active")).toHaveAttribute("data-tid", tid!);
    await expect(input).toHaveValue("");
    await expect.poll(async () => (await composerBox(page)).height).toBe(atRest.height);

    // Switch back to the draft. The restored text is tall, so the box must be tall again —
    // restoring the value without re-measuring would leave a one-line box holding 60 lines.
    await openDraftComposer(page);
    await expect(input).toHaveValue(MANY_LINES);
    await expect.poll(async () => (await composerBox(page)).height).toBe(tall.maxHeight);
    expect((await composerBox(page)).overflowY).toBe("auto");
  });
});

test.describe("composer auto-grow (mobile)", () => {
  test.use({
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 3,
    isMobile: true,
    hasTouch: true,
  });

  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("grows with its content, clamps at the cap, and scrolls beyond it", async ({ page }) => {
    await assertGrowsAndClamps(page);
  });

  test("re-clamps when the on-screen keyboard shrinks the visible viewport", async ({ page }) => {
    const input = await openDraftComposer(page);
    await input.fill(MANY_LINES);

    const beforeKeyboard = await composerBox(page);
    expect(beforeKeyboard.height).toBe(beforeKeyboard.maxHeight);

    // iOS Safari overlays the keyboard: the layout viewport keeps its full height and only the
    // visual viewport shrinks. --app-height follows the visible area, so the cap — a fraction of it
    // — shrinks too, and a box already taller than the new cap has to come back down. Without that
    // the composer would cover the transcript it was sized against.
    await input.focus();
    await simulateIOSKeyboard(page, 400);

    await expect.poll(async () => (await composerBox(page)).appHeight).toBe(400);
    const afterKeyboard = await composerBox(page);
    expect(afterKeyboard.maxHeight).toBeLessThan(beforeKeyboard.maxHeight);
    expect(afterKeyboard.height).toBe(afterKeyboard.maxHeight);
    // Still scrollable, so none of the typed text became unreachable.
    expect(afterKeyboard.overflowY).toBe("auto");
    expect(afterKeyboard.scrolls).toBe(true);
    // And the transcript still exists above it rather than being squeezed to nothing.
    expect(await transcriptHeight(page)).toBeGreaterThan(0);
  });
});
