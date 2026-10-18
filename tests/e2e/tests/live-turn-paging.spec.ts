import { test, expect, type Page } from "@playwright/test";
import {
  REPLAY_TURN_ITEMS,
  SCRIPTED_LONG_TURN_REPLIES,
  SCRIPTED_LONG_TURN_TRIGGER,
  SCRIPTED_REASONING_DETAIL,
  login,
} from "./helpers";

// A reload in a long live turn sends only its newest items (spec §13.6, LP1): the turn's items are
// its prompt echo, twenty replies and the open note. The snapshot carries the newest `turn_items`
// of them (the note and the last replies) and the echo, which always rides along. The replies left
// out page in under a marker row, one page of `turn_items` at a time, before any older turn.
const TAIL_REPLIES = REPLAY_TURN_ITEMS - 1;   // the open note is the tail's newest item
const OMITTED = SCRIPTED_LONG_TURN_REPLIES - TAIL_REPLIES;
const FIRST_TAIL_REPLY = OMITTED + 1;

const reply = (k: number) => `Scripted reply ${k} of ${SCRIPTED_LONG_TURN_REPLIES}`;
const replies = (from: number, to: number) =>
  Array.from({ length: to - from + 1 }, (_, i) => reply(from + i));

/** The long turn's prompt and agent rows, in transcript order. */
async function longTurnRows(page: Page): Promise<string[]> {
  return page.evaluate(trigger => {
    const rows = [...document.querySelectorAll<HTMLElement>("#transcript .msg.user, #transcript .msg.agent")];
    const start = rows.findIndex(row => row.classList.contains("user") && row.textContent!.includes(trigger));
    return rows.slice(start).map(row => row.querySelector(".body")!.textContent!.trim());
  }, SCRIPTED_LONG_TURN_TRIGGER);
}

async function startLongTurnAndReload(page: Page): Promise<void> {
  await page.locator("#input").fill(SCRIPTED_LONG_TURN_TRIGGER);
  await page.locator("#sendBtn").click();
  const transcript = page.locator("#transcript");
  await expect(transcript.locator(".msg.agent", { hasText: reply(SCRIPTED_LONG_TURN_REPLIES) })).toHaveCount(1);
  await expect(transcript.locator(".msg.reasoning .body")).toContainText(SCRIPTED_REASONING_DETAIL);
  await expect(page.locator("#stopBtn")).toBeVisible();
  await page.reload();
  await expect(page.locator("#app")).toHaveClass(/open/);
}

async function expectTailOnly(page: Page): Promise<void> {
  const transcript = page.locator("#transcript");
  const gap = transcript.locator(".msg.turn-gap");
  await expect(gap).toHaveCount(1);
  await expect(gap).toContainText(`${OMITTED} earlier items`);
  await expect.poll(() => longTurnRows(page)).toEqual([
    SCRIPTED_LONG_TURN_TRIGGER,
    ...replies(FIRST_TAIL_REPLY, SCRIPTED_LONG_TURN_REPLIES),
  ]);
  // The marker sits directly under the prompt.
  expect(await gap.evaluate(row => {
    const prev = row.previousElementSibling as HTMLElement | null;
    return !!prev && prev.classList.contains("user") && prev.dataset.turn === row.dataset.turn;
  })).toBe(true);
  const note = transcript.locator(".msg.reasoning");
  await expect(note).toHaveCount(1);
  await expect(note).toHaveClass(/expanded/);
  await expect(note.locator(".body")).toContainText(SCRIPTED_REASONING_DETAIL);
}

async function expectWholeTurn(page: Page): Promise<void> {
  const transcript = page.locator("#transcript");
  await expect(transcript.locator(".msg.turn-gap")).toHaveCount(0);
  await expect.poll(() => longTurnRows(page)).toEqual([
    SCRIPTED_LONG_TURN_TRIGGER,
    ...replies(1, SCRIPTED_LONG_TURN_REPLIES),
  ]);
  await expect(transcript.locator(".msg.user", { hasText: SCRIPTED_LONG_TURN_TRIGGER })).toHaveCount(1);
  await expect(transcript.locator(".msg.reasoning")).toHaveClass(/expanded/);
}

async function stopTurn(page: Page): Promise<void> {
  await page.locator("#stopBtn").click();
  await expect(page.locator("#stopBtn")).toBeHidden();
  await expect(page.locator("#sendBtn")).toBeVisible();
}

async function newThread(page: Page): Promise<void> {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
}

// The tail alone is far less than two screens here, so once the snapshot has drawn the marker the
// open-time autofill pages the turn's earlier items in without the reader doing anything.
test.describe("on a tall screen", () => {
  test.use({ viewport: { width: 1280, height: 1600 } });

  test("the open-time autofill pages a long live turn's earlier items in", async ({ page }) => {
    await newThread(page);
    await startLongTurnAndReload(page);
    await expectWholeTurn(page);
    await stopTurn(page);
  });
});

test.describe("on a short screen", () => {
  test.use({ viewport: { width: 1280, height: 420 } });

  // Here the tail fills two screens, so the autofill leaves the earlier items to the reader.
  test("pressing the marker pages a long live turn's earlier items in under it", async ({ page }) => {
    await newThread(page);
    await startLongTurnAndReload(page);
    await expectTailOnly(page);

    const gap = page.locator("#transcript .msg.turn-gap");
    await gap.getByRole("button", { name: "Load earlier items" }).click();
    // The first page lands directly under the marker, above the tail.
    const firstPaged = OMITTED - REPLAY_TURN_ITEMS + 1;
    await expect(gap).toContainText(`${firstPaged - 1} earlier items`);
    await expect.poll(() => longTurnRows(page)).toEqual([
      SCRIPTED_LONG_TURN_TRIGGER,
      ...replies(firstPaged, SCRIPTED_LONG_TURN_REPLIES),
    ]);
    expect(await gap.evaluate(row => row.nextElementSibling!.textContent)).toContain(reply(firstPaged));
    await gap.getByRole("button", { name: "Load earlier items" }).click();
    await expectWholeTurn(page);
    await stopTurn(page);
  });

  test("scrolling to the top pages a long live turn's earlier items in", async ({ page }) => {
    await newThread(page);
    await startLongTurnAndReload(page);
    await expectTailOnly(page);

    const gap = page.locator("#transcript .msg.turn-gap");
    await expect(async () => {
      await page.locator("#transcript").evaluate(el => { el.scrollTop = 0; });
      await expect(gap).toHaveCount(0, { timeout: 1_000 });
    }).toPass();
    await expectWholeTurn(page);
    await stopTurn(page);
  });

  test("older turns page in only once the live turn is whole", async ({ page }) => {
    const OLDER_TURNS = 6;   // one more than `[history] initial`, so the oldest needs paging
    await newThread(page);
    for (let n = 1; n <= OLDER_TURNS; n++) {
      // Opening the new thread after the first turn can reset the composer under a fill.
      await expect(async () => {
        await page.locator("#input").fill(`Older turn ${n}`);
        await page.locator("#sendBtn").click({ timeout: 1_000 });
      }).toPass();
      await expect(page.locator("#transcript .msg.user", { hasText: `Older turn ${n}` })).toHaveCount(1);
      await expect(page.locator("#transcript .msg.agent")).toHaveCount(n);
      await expect(page.locator("#sendBtn")).toBeVisible();
    }

    // Until the marker is up, refuse history pages, so the oldest turn is still unloaded when the
    // live turn's items start paging. After that, record whether a marker was on screen at every
    // history page: none may be requested while the live turn still has items to page.
    let markerUp = false;
    const markersAtHistoryPage: number[] = [];
    await page.route(/\/history\?/, async route => {
      if (!markerUp) return route.abort();
      markersAtHistoryPage.push(await page.locator("#transcript .msg.turn-gap").count());
      return route.continue();
    });
    await startLongTurnAndReload(page);
    await expectTailOnly(page);
    await expect(page.locator("#transcript .msg.user", { hasText: "Older turn 1" })).toHaveCount(0);
    markerUp = true;

    const gap = page.locator("#transcript .msg.turn-gap");
    const transcript = page.locator("#transcript");
    // At the top the reader is in older turns and the marker is below the view, so the page that
    // lands under it must not move what they are looking at.
    await transcript.evaluate(el => { el.scrollTop = 0; });
    expect(await gap.evaluate(row =>
      row.getBoundingClientRect().top >= document.getElementById("transcript")!.getBoundingClientRect().bottom
    )).toBe(true);
    await transcript.evaluate(el => el.dispatchEvent(new Event("scroll")));
    await expect(gap).toContainText(`${OMITTED - REPLAY_TURN_ITEMS} earlier items`);
    expect(await transcript.evaluate(el => el.scrollTop)).toBe(0);
    await expect(async () => {
      await transcript.evaluate(el => { el.scrollTop = 0; el.dispatchEvent(new Event("scroll")); });
      await expect(gap).toHaveCount(0, { timeout: 1_000 });
    }).toPass();
    await expectWholeTurn(page);
    await expect(async () => {
      await page.locator("#transcript").evaluate(el => { el.scrollTop = 0; });
      await expect(page.locator("#transcript .msg.user", { hasText: "Older turn 1" })).toHaveCount(1, { timeout: 1_000 });
    }).toPass();
    expect(markersAtHistoryPage.length).toBeGreaterThan(0);
    expect(markersAtHistoryPage.every(count => count === 0)).toBe(true);
    await stopTurn(page);
  });
});
