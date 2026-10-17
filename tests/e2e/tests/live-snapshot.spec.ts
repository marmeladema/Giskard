import { test, expect, type Page } from "@playwright/test";
import {
  SCRIPTED_REASONING_DETAIL,
  SCRIPTED_REPLY,
  SCRIPTED_STREAMING_HOLD_TRIGGER,
  login,
} from "./helpers";

/** Wait for two animation frames, so scroll work queued with requestAnimationFrame has run. */
async function nextFrames(page: Page): Promise<void> {
  await page.evaluate(() => new Promise<void>(resolve => {
    requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
  }));
}

// A reload in the middle of a turn rebuilds the transcript from the server's live-turn snapshot.
// The live buffer keeps one entry per item: a completed message replays as its start and
// completion, without the chunks it streamed, and the still-open note as its start and one
// coalesced delta. The rebuilt transcript must match what the live session drew.
test("a reload mid-stream rebuilds each item once from the coalesced live snapshot", async ({ page }) => {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill(SCRIPTED_STREAMING_HOLD_TRIGGER);
  await page.locator("#sendBtn").click();

  const transcript = page.locator("#transcript");
  const reply = transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY });
  const note = transcript.locator(".msg.reasoning");
  await expect(reply).toHaveCount(1);
  await expect(note).toHaveCount(1);
  await expect(note.locator(".body")).toContainText(SCRIPTED_REASONING_DETAIL);
  await expect(page.locator("#stopBtn")).toBeVisible();

  await page.reload();
  await expect(page.locator("#app")).toHaveClass(/open/);

  const replyAfter = page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY });
  const noteAfter = page.locator("#transcript .msg.reasoning");
  await expect(replyAfter).toHaveCount(1);
  await expect(noteAfter).toHaveCount(1);
  // The note is still the newest row, so it stays open, with everything streamed so far.
  await expect(noteAfter).toHaveClass(/expanded/);
  await expect(noteAfter.locator(".body")).toBeVisible();
  await expect(noteAfter.locator(".body")).toContainText(SCRIPTED_REASONING_DETAIL);
  // The turn is still running: the composer offers Stop, not Send.
  await expect(page.locator("#stopBtn")).toBeVisible();
  await expect(page.locator("#sendBtn")).toBeHidden();
  await expect(page.locator("#transcript .msg.error")).toHaveCount(0);

  // Stop the held turn so no turn is left running on the shared replay server.
  await page.locator("#stopBtn").click();
  await expect(page.locator("#stopBtn")).toBeHidden();
  await expect(page.locator("#sendBtn")).toBeVisible();
});

// The replay measures once whether the reader follows the bottom. A reader scrolled up into the
// history when the socket reconnects must keep their place: the incremental resync keeps the
// completed turns, and the live snapshot that redraws the open turn must not pull the view down.
test("an incremental reconnect leaves a scrolled-up reader where they were", async ({ page }) => {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Create a thread with one completed turn.");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toHaveCount(1);
  await expect(page.locator("#sendBtn")).toBeVisible();

  await page.locator("#input").fill(SCRIPTED_STREAMING_HOLD_TRIGGER);
  await page.locator("#sendBtn").click();
  const note = page.locator("#transcript .msg.reasoning");
  await expect(note.locator(".body")).toContainText(SCRIPTED_REASONING_DETAIL);

  // Let the frames the stream queued to follow the bottom run first, or one of them would undo
  // the reader's scroll below.
  await nextFrames(page);
  // Make the history tall enough to scroll, mark the live note so its redraw is observable, and
  // scroll the reader to the top.
  await page.evaluate(() => {
    const transcript = document.getElementById("transcript")!;
    const filler = document.createElement("div");
    filler.style.height = "4000px";
    filler.textContent = "older history";
    transcript.prepend(filler);
    (document.querySelector("#transcript .msg.reasoning") as HTMLElement).dataset.beforeReconnect = "1";
    transcript.scrollTop = 0;
  });
  await nextFrames(page);
  expect(await page.locator("#transcript").evaluate(el => el.scrollTop)).toBe(0);

  await page.evaluate(() => {
    (window as unknown as { connectWs: (options: unknown) => void })
      .connectWs({ reconnect: true, reason: "test reconnect" });
  });
  // The live snapshot replaced the open turn's rows.
  await expect(note).toHaveCount(1);
  await expect(note).not.toHaveAttribute("data-before-reconnect", "1");
  await expect(note.locator(".body")).toContainText(SCRIPTED_REASONING_DETAIL);
  await nextFrames(page);
  expect(await page.locator("#transcript").evaluate(el => el.scrollTop)).toBe(0);

  await page.locator("#stopBtn").click();
  await expect(page.locator("#stopBtn")).toBeHidden();
});
