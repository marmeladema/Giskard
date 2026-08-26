import { test, expect, type Page } from "@playwright/test";
import {
  SCRIPTED_REASONING_DETAIL,
  SCRIPTED_REASONING_SUMMARY,
  SCRIPTED_REASONING_TRIGGER,
  SCRIPTED_REPLY,
  login,
} from "./helpers";

/** Open a real thread so the transcript, WebSocket and thread state are live. */
async function openThread(page: Page): Promise<void> {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Create a reasoning test thread.");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
}

const LIVE_TURN = "turn-reasoning";

/** Feed one client event through the same entry point the WebSocket stream uses. */
async function dispatch(page: Page, event: Record<string, unknown>): Promise<void> {
  await page.evaluate(ev => {
    (window as unknown as { handleEvent: (ev: unknown) => void }).handleEvent(ev);
  }, event);
}

/**
 * Stream a reasoning note into the open transcript exactly as the live event path does (item
 * start, text deltas, then the completed item), driven one event at a time so the streaming and
 * completion states can each be asserted instead of racing the harness.
 */
async function startReasoning(page: Page, itemId: string): Promise<void> {
  await dispatch(page, {
    kind: "item_started",
    turn: LIVE_TURN,
    item: { id: itemId, harness_item_id: `native-${itemId}`, kind: "reasoning" },
  });
}

async function streamReasoning(page: Page, itemId: string, text: string): Promise<void> {
  await dispatch(page, {
    kind: "item_delta",
    turn: LIVE_TURN,
    item_id: itemId,
    delta: { type: "text", text },
  });
}

/** Complete an ordinary agent message, which appends the row that supersedes an open note. */
async function completeAgentMessage(page: Page, itemId: string, text: string): Promise<void> {
  await dispatch(page, {
    kind: "item_completed",
    turn: LIVE_TURN,
    item: {
      id: itemId,
      harness_item_id: `native-${itemId}`,
      payload: { kind: "agent_message", text },
      created_at: new Date().toISOString(),
    },
  });
}

async function completeReasoning(page: Page, itemId: string, text: string): Promise<void> {
  await dispatch(page, {
    kind: "item_completed",
    turn: LIVE_TURN,
    item: {
      id: itemId,
      harness_item_id: `native-${itemId}`,
      payload: { kind: "reasoning", text },
      created_at: new Date().toISOString(),
    },
  });
}

test.describe("collapsible reasoning rows", () => {
  test("folds a streamed reasoning note once the reply lands, and toggles on click", async ({ page }) => {
    await openThread(page);

    await page.locator("#input").fill(SCRIPTED_REASONING_TRIGGER);
    await page.locator("#sendBtn").click();

    const row = page.locator("#transcript .msg.reasoning");
    await expect(row).toHaveCount(1);
    // The agent message lands under the note, so the settled row is the collapsed summary: the
    // first line of the note with its Markdown emphasis stripped, and no body on screen.
    await expect(row).toHaveClass(/collapsed/);
    await expect(row.locator(".reasoning-summary")).toHaveText(SCRIPTED_REASONING_SUMMARY);
    await expect(row.locator(".body")).toBeHidden();

    const toggle = row.locator(".reasoning-toggle");
    await expect(toggle).toHaveAttribute("aria-expanded", "false");
    await toggle.click();
    await expect(row).toHaveClass(/expanded/);
    await expect(toggle).toHaveAttribute("aria-expanded", "true");
    await expect(row.locator(".body")).toContainText(SCRIPTED_REASONING_DETAIL);

    await toggle.click();
    await expect(row).toHaveClass(/collapsed/);
    await expect(row.locator(".body")).toBeHidden();
  });

  test("keeps a persisted reasoning note collapsed after a reload", async ({ page }) => {
    await openThread(page);
    await page.locator("#input").fill(SCRIPTED_REASONING_TRIGGER);
    await page.locator("#sendBtn").click();
    await expect(page.locator("#transcript .msg.reasoning")).toHaveCount(1);
    await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toHaveCount(2);

    await page.reload();
    const row = page.locator("#transcript .msg.reasoning");
    await expect(row).toHaveCount(1);
    await expect(row).toHaveClass(/collapsed/);
    await expect(row.locator(".reasoning-summary")).toHaveText(SCRIPTED_REASONING_SUMMARY);
    await expect(row.locator(".body")).toBeHidden();
    await row.locator(".reasoning-toggle").click();
    await expect(row.locator(".body")).toContainText(SCRIPTED_REASONING_DETAIL);
  });

  test("keeps the newest note open and folds it when the next row is appended", async ({ page }) => {
    await openThread(page);

    await startReasoning(page, "reasoning-live-1");
    await streamReasoning(page, "reasoning-live-1", "Reading the config");
    const row = page.locator("#transcript .msg.reasoning");
    await expect(row).toHaveCount(1);
    await expect(row).toHaveClass(/expanded/);
    await expect(row.locator(".body")).toBeVisible();
    await expect(row.locator(".reasoning-summary")).toHaveText("Reading the config");

    // The summary keeps up with the text arriving under it.
    await streamReasoning(page, "reasoning-live-1", " before deciding");
    await expect(row.locator(".reasoning-summary")).toHaveText("Reading the config before deciding");

    // Completing the item is not what folds the note: it is still the newest row, so it stays
    // readable.
    await completeReasoning(page, "reasoning-live-1", "Reading the config before deciding\n\nDetail line.");
    await expect(row).toHaveClass(/expanded/);
    await expect(row.locator(".body")).toBeVisible();

    // The next row appended below it does.
    await completeAgentMessage(page, "agent-live-1", "Here is the answer.");
    await expect(row).toHaveClass(/collapsed/);
    await expect(row.locator(".body")).toBeHidden();
    await expect(row.locator(".reasoning-summary")).toHaveText("Reading the config before deciding");
  });

  test("folds an earlier note when a later one starts streaming", async ({ page }) => {
    await openThread(page);

    await startReasoning(page, "reasoning-first");
    await streamReasoning(page, "reasoning-first", "First thought");
    await completeReasoning(page, "reasoning-first", "First thought");
    const rows = page.locator("#transcript .msg.reasoning");
    await expect(rows).toHaveClass([/expanded/]);

    await startReasoning(page, "reasoning-second");
    await streamReasoning(page, "reasoning-second", "Second thought");
    await expect(rows).toHaveCount(2);
    // The older note folds to its summary; the live one stays open.
    await expect(rows).toHaveClass([/collapsed/, /expanded/]);
  });

  test("summarizes past Markdown marks without breaking identifiers", async ({ page }) => {
    await openThread(page);

    await startReasoning(page, "reasoning-marks");
    await streamReasoning(page, "reasoning-marks", "**Checking `snake_case_name` in src/app_shell.js**");
    const row = page.locator("#transcript .msg.reasoning");
    await expect(row.locator(".reasoning-summary"))
      .toHaveText("Checking snake_case_name in src/app_shell.js");
  });

  test("restores the reader's choice on a row rebuilt by a resync", async ({ page }) => {
    await openThread(page);

    await startReasoning(page, "reasoning-resync");
    await streamReasoning(page, "reasoning-resync", "Thinking about the resync");
    const row = page.locator("#transcript .msg.reasoning");
    await expect(row).toHaveClass(/expanded/);
    // Fold it by hand while it is still the newest row, so the stored choice differs from the
    // default a rebuilt row would otherwise pick up.
    await row.locator(".reasoning-toggle").click();
    await expect(row).toHaveClass(/collapsed/);
    await completeReasoning(page, "reasoning-resync", "Thinking about the resync");

    // A resync drops the in-flight turn's rows and re-renders the same items from the snapshot.
    await page.evaluate(() => {
      document.querySelector("#transcript .msg.reasoning")?.remove();
      (window as unknown as { rebuildRenderTrackingFromDom: () => void }).rebuildRenderTrackingFromDom();
    });
    await expect(row).toHaveCount(0);
    await completeReasoning(page, "reasoning-resync", "Thinking about the resync");
    await expect(row).toHaveCount(1);
    await expect(row).toHaveClass(/collapsed/);
    await expect(row.locator(".body")).toBeHidden();
  });

  test("keeps the reader's choice across an authoritative transcript rebuild", async ({ page }) => {
    await openThread(page);

    await startReasoning(page, "reasoning-authoritative");
    await streamReasoning(page, "reasoning-authoritative", "Thinking before the resubscribe");
    const row = page.locator("#transcript .msg.reasoning");
    await expect(row).toHaveClass(/expanded/);
    // Fold it by hand while it is the newest row, so the stored choice differs from the default a
    // rebuilt row would otherwise take.
    await row.locator(".reasoning-toggle").click();
    await expect(row).toHaveClass(/collapsed/);

    // A resubscribe snapshot empties the transcript and replays the thread's items into it. That
    // is the same thread, so the reader's choices still apply to the rebuilt rows.
    await page.evaluate(() => {
      (window as unknown as { resetTranscriptForAuthoritativeSnapshot: () => void })
        .resetTranscriptForAuthoritativeSnapshot();
    });
    await expect(row).toHaveCount(0);
    await completeReasoning(page, "reasoning-authoritative", "Thinking before the resubscribe");
    await expect(row).toHaveCount(1);
    await expect(row).toHaveClass(/collapsed/);
    await expect(row.locator(".body")).toBeHidden();
  });

  test("a note the user opened stays open once later rows arrive", async ({ page }) => {
    await openThread(page);

    await startReasoning(page, "reasoning-live-2");
    await streamReasoning(page, "reasoning-live-2", "Sketching the plan");
    const row = page.locator("#transcript .msg.reasoning");
    await expect(row).toHaveClass(/expanded/);
    // Collapse it mid-stream: the choice must survive the next delta.
    await row.locator(".reasoning-toggle").click();
    await expect(row).toHaveClass(/collapsed/);
    await streamReasoning(page, "reasoning-live-2", " and the fallback");
    await expect(row).toHaveClass(/collapsed/);

    // Re-open it, then let the item complete and a later row arrive: a note the reader opened
    // stays open, superseded or not.
    await row.locator(".reasoning-toggle").click();
    await expect(row).toHaveClass(/expanded/);
    await completeReasoning(page, "reasoning-live-2", "Sketching the plan and the fallback\n\nDetail line.");
    await completeAgentMessage(page, "agent-live-2", "Here is the answer.");
    await expect(row).toHaveClass(/expanded/);
    await expect(row.locator(".body")).toContainText("Detail line.");
  });
});
