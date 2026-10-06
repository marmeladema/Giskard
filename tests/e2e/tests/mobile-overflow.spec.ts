import { test, expect, type Page } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

/**
 * Nothing in the transcript may run off the right edge of a phone screen, in any appearance and
 * whatever the row holds. The check is on the rendered layout rather than on any one CSS rule, so
 * it catches a row widened by a grid track, a `nowrap` flex item or a `max-content` child alike.
 * Every row kind is fed text that cannot wrap on its own (long identifiers, paths, URLs, code).
 */

const APPEARANCES = ["ide", "terminal", "bubbles"] as const;
const TURN = "turn-mobile-overflow";

const LONG_TOKEN = "src/" + "deeply_nested_module_name/".repeat(8) + "a_file_with_a_very_long_name.rs";
const LONG_LINE =
  `Checking whether ${LONG_TOKEN} still compiles once the configuration loader is moved behind ` +
  "the new feature flag, and whether the fallback path keeps working on every platform we ship";
const LONG_CODE = "let value = " + "some_function_call(argument_one, argument_two) + ".repeat(6) + "0;";
const REASONING_TEXT =
  `**${LONG_LINE}**\n\nThe body goes on about ${LONG_TOKEN} and then shows the code:\n\n` +
  "```rust\n" + LONG_CODE + "\n```\n";
const AGENT_TEXT =
  `See https://example.com/${"segment/".repeat(20)}end and \`${LONG_TOKEN}\`.\n\n` +
  "```rust\n" + LONG_CODE + "\n```\n\n" +
  "| column_one | column_two | column_three | column_four | column_five | column_six |\n" +
  "|---|---|---|---|---|---|\n" +
  `| ${LONG_TOKEN} | b | c | d | e | f |\n`;

async function dispatch(page: Page, event: Record<string, unknown>): Promise<void> {
  await page.evaluate(ev => {
    (window as unknown as { handleEvent: (ev: unknown) => void }).handleEvent(ev);
  }, event);
}

async function complete(page: Page, id: string, payload: Record<string, unknown>): Promise<void> {
  await dispatch(page, {
    kind: "item_completed",
    turn: TURN,
    item: { id, harness_item_id: `native-${id}`, payload, created_at: new Date().toISOString() },
  });
}

async function openThread(page: Page): Promise<void> {
  await login(page);
  // On phones the projects live in a drawer.
  const menuButton = page.locator("#btnMenu");
  if (await menuButton.isVisible()) await menuButton.click();
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill(`Overflow check for ${LONG_TOKEN}`);
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
}

/** Every transcript row whose right edge passes the transcript's, plus the transcript's own overflow. */
async function horizontalOverflow(page: Page) {
  return page.evaluate(() => {
    const transcript = document.getElementById("transcript")!;
    const edge = transcript.getBoundingClientRect().right + 0.5;
    const rows = [...transcript.querySelectorAll<HTMLElement>(".msg, .reasoning-toggle")]
      .filter(el => el.offsetParent !== null && el.getBoundingClientRect().right > edge)
      .map(el => `${el.className} (right ${Math.round(el.getBoundingClientRect().right)} > ${Math.round(edge)})`);
    return { scrollOverflow: transcript.scrollWidth - transcript.clientWidth, rows };
  });
}

async function expectNoOverflow(page: Page, when: string): Promise<void> {
  const overflow = await horizontalOverflow(page);
  expect(overflow.rows, `rows past the right edge (${when})`).toEqual([]);
  expect(overflow.scrollOverflow, `transcript scrolls sideways (${when})`).toBeLessThanOrEqual(0);
}

for (const appearance of APPEARANCES) {
  test.describe(`transcript fits a phone screen (${appearance})`, () => {
    test.use({
      viewport: { width: 390, height: 844 },
      deviceScaleFactor: 3,
      isMobile: true,
      hasTouch: true,
    });
    test.beforeEach(async ({ page }) => {
      await page.addInitScript(a => localStorage.setItem("giskard.appearance", a), appearance);
    });

    test("no row runs off the right edge, reasoning expanded or collapsed", async ({ page }) => {
      await openThread(page);
      await expect(page.locator("html")).toHaveAttribute("data-appearance", appearance);

      // The reasoning note is the newest row, so it renders expanded.
      await complete(page, "reasoning-overflow", { kind: "reasoning", text: REASONING_TEXT });
      const reasoning = page.locator("#transcript .msg.reasoning");
      await expect(reasoning).toHaveClass(/expanded/);
      await expect(reasoning.locator(".body pre")).toBeVisible();
      await expectNoOverflow(page, "reasoning expanded");
      // The one-line summary is cut with an ellipsis rather than widening the row.
      expect(await reasoning.locator(".reasoning-summary").evaluate(el => el.scrollWidth > el.clientWidth))
        .toBe(true);

      await complete(page, "agent-overflow", { kind: "agent_message", text: AGENT_TEXT });
      await complete(page, "command-overflow", {
        kind: "command_execution", command: `cat ${LONG_TOKEN} | grep ${LONG_TOKEN}`, cwd: `/home/${LONG_TOKEN}`,
        output: `${LONG_CODE}\n${LONG_TOKEN}\n`, exit_code: 0, status: "completed", process_id: null, duration_ms: 12,
      });
      await complete(page, "tool-overflow", {
        kind: "tool_call", server: "a_server_with_a_long_name", name: `read_${LONG_TOKEN}`,
        status: "failed", error: `could not read ${LONG_TOKEN}`,
      });
      await complete(page, "file-overflow", {
        kind: "file_change", changes: [{ path: LONG_TOKEN, change: "modified", status: "completed" }],
      });
      await complete(page, "activity-overflow", {
        kind: "activity", title: `Indexed ${LONG_TOKEN}`, detail: LONG_LINE,
      });
      await expect(page.locator("#transcript .msg.agent pre")).toBeVisible();

      // Rows appended below the note folded it to its summary line.
      await expect(reasoning).toHaveClass(/collapsed/);
      await expectNoOverflow(page, "reasoning collapsed");

      await reasoning.locator(".reasoning-toggle").click();
      await expect(reasoning).toHaveClass(/expanded/);
      await expectNoOverflow(page, "reasoning re-expanded");
    });
  });
}
