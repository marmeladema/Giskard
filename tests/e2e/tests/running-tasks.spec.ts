import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

async function openCommandOutputTestThread(page: import("@playwright/test").Page) {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Create a command-output test thread.");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
}

async function addCompletedCommand(
  page: import("@playwright/test").Page,
  itemId: string,
  turnId: string,
  preview = "output preview",
) {
  await page.evaluate(({ itemId, turnId, preview }) => {
    const app = window as unknown as {
      addItem: (item: unknown, turnId: string, fromHistory: boolean) => void;
    };
    app.addItem({
      id: itemId,
      harness_item_id: `native-${itemId}`,
      payload: {
        kind: "command_execution",
        command: `printf ${itemId}`,
        cwd: "/tmp/project",
        output: {
          preview,
          preview_truncated: true,
          durable_truncated: false,
          original_bytes: preview.length,
          original_lines: 1,
          durable_bytes: preview.length,
          durable_lines: 1,
          preview_bytes: preview.length,
          preview_lines: 1,
          output_available: true,
        },
        exit_code: 0,
        status: "completed",
        process_id: null,
        duration_ms: 10,
      },
    }, turnId, false);
  }, { itemId, turnId, preview });
}

test("late completion replaces a processless running command in place", async ({ page }) => {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Create a thread for running-task reconciliation.");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

  const result = await page.evaluate(() => {
    const app = window as unknown as {
      resetTranscriptForAuthoritativeSnapshot: () => void;
      renderRunningCommandSnapshot: (tasks: unknown[]) => void;
      addItem: (item: unknown, turnId: string, fromHistory: boolean) => void;
    };
    const turnId = "turn-late-command";
    const itemId = "command-late-1";
    const rowSelector = `[data-command-item-id="${turnId}:${itemId}"]`;

    app.resetTranscriptForAuthoritativeSnapshot();
    app.renderRunningCommandSnapshot([{
      kind: "command",
      thread_id: "browser-thread",
      turn_id: turnId,
      item_id: itemId,
      harness_item_id: "native-command-late-1",
      command: "<command included NUL byte>",
      cwd: "/tmp/project",
      status: "in_progress",
      process_id: null,
      started_at_ms: Date.now(),
      output: "starting",
      after_turn: true,
      terminating: false,
    }]);

    const runningRow = document.querySelector(rowSelector) as HTMLElement | null;
    const initial = {
      taskCount: document.querySelector("#tasksCount")?.textContent,
      rowCount: document.querySelectorAll(rowSelector).length,
      running: runningRow?.classList.contains("state-running"),
      stopDisabled: (runningRow?.querySelector("button.danger") as HTMLButtonElement | null)?.disabled,
    };

    app.renderRunningCommandSnapshot([]);
    const untrackedRow = document.querySelector(rowSelector) as HTMLElement | null;
    const afterEmptySnapshot = {
      taskCount: document.querySelector("#tasksCount")?.textContent,
      terminated: untrackedRow?.classList.contains("state-terminated"),
      text: untrackedRow?.textContent || "",
    };

    app.addItem({
      id: itemId,
      harness_item_id: "native-command-late-1",
      payload: {
        kind: "command_execution",
        command: "<command included NUL byte>",
        cwd: "/tmp/project",
        output: {
          preview: "failed before spawn",
          preview_truncated: false,
          durable_truncated: false,
          original_bytes: 19,
          original_lines: 1,
          durable_bytes: 19,
          durable_lines: 1,
          preview_bytes: 19,
          preview_lines: 1,
          output_available: false,
        },
        exit_code: 1,
        status: "failed",
        process_id: null,
        duration_ms: 10,
      },
    }, turnId, false);

    const completedRow = document.querySelector(rowSelector) as HTMLElement | null;
    return {
      initial,
      afterEmptySnapshot,
      completed: {
        rowCount: document.querySelectorAll(rowSelector).length,
        failed: completedRow?.classList.contains("state-failed"),
        text: completedRow?.textContent || "",
      },
    };
  });

  expect(result.initial.taskCount).toBe("1");
  expect(result.initial.rowCount).toBe(0);
  expect(result.afterEmptySnapshot.taskCount).toBe("0");
  expect(result.afterEmptySnapshot.terminated).toBeUndefined();
  expect(result.afterEmptySnapshot.text).not.toContain("No longer tracked");
  expect(result.completed.rowCount).toBe(1);
  expect(result.completed.failed).toBe(true);
  expect(result.completed.text).toContain("Failed");
  expect(result.completed.text).toContain("failed before spawn");

  await page.evaluate(() => {
    const browser = window as unknown as {
      copyToClipboard: (text: string) => Promise<boolean>;
      copiedLateOutput?: string;
      renderRunningCommandSnapshot: (tasks: unknown[]) => void;
      addItem: (item: unknown, turnId: string, fromHistory: boolean) => void;
    };
    browser.copyToClipboard = async (text: string) => {
      browser.copiedLateOutput = text;
      return true;
    };
    browser.renderRunningCommandSnapshot([{
      kind: "command", thread_id: "browser-thread", turn_id: "turn-copy-late",
      item_id: "command-copy-late", harness_item_id: "native-command-copy-late",
      command: "printf preserved", cwd: "/tmp/project", status: "in_progress",
      process_id: null, started_at_ms: Date.now(), output: "preserved locally",
      after_turn: true, terminating: false,
    }]);
    browser.addItem({
      id: "command-copy-late", harness_item_id: "native-command-copy-late",
      payload: {
        kind: "command_execution", command: "printf preserved", cwd: "/tmp/project",
        output: {
          preview: "", preview_truncated: false, durable_truncated: false,
          original_bytes: 0, original_lines: 0, durable_bytes: 0, durable_lines: 0,
          preview_bytes: 0, preview_lines: 0, output_available: false,
        },
        exit_code: 0, status: "completed", process_id: null, duration_ms: 10,
      },
    }, "turn-copy-late", false);
  });
  const completedRow = page.locator('[data-command-item-id="turn-copy-late:command-copy-late"]');
  await completedRow.locator(".output-overlay-btn")
    .evaluate((button: HTMLButtonElement) => button.click());
  await expect(page.locator("#codeView")).toContainText("preserved locally");
  await page.locator("#codeCopyDiff").click();
  await expect.poll(() => page.evaluate(() =>
    (window as unknown as { copiedLateOutput?: string }).copiedLateOutput,
  )).toBe("preserved locally");
});

test("completed command output is fetched only when its overlay opens", async ({ page }) => {
  const outputEtag = '"sha256_1111111111111111111111111111111111111111111111111111111111111111"';
  let reads = 0;
  let linkReads = 0;
  await page.route("**/items/command-lazy-1/command-output-links", async route => {
    linkReads++;
    expect(route.request().method()).toBe("GET");
    expect(route.request().postData()).toBeNull();
    expect(route.request().headers()["if-output-match"]).toBe(outputEtag);
    await route.fulfill({ status:200, contentType:"application/json", body:'{"links":[]}' });
  });
  await page.route("**/items/command-lazy-1/command-output", async route => {
    reads++;
    await route.fulfill({
      status: 200,
      contentType: "text/plain; charset=utf-8",
      headers: {
        "ETag": outputEtag,
        "X-Giskard-Output-Truncated": "false",
        "X-Giskard-Output-Original-Bytes": "36",
        "X-Giskard-Output-Original-Lines": "2",
      },
      body: "full retained output\nsecond line",
    });
  });
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Create a command-output test thread.");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

  await page.evaluate(() => {
    const app = window as unknown as {
      addItem: (item: unknown, turnId: string, fromHistory: boolean) => void;
    };
    app.addItem({
      id: "command-lazy-1",
      harness_item_id: "native-command-lazy-1",
      payload: {
        kind: "command_execution",
        command: "printf output",
        cwd: "/tmp/project",
        output: {
          preview: "second line",
          preview_truncated: true,
          durable_truncated: false,
          original_bytes: 36,
          original_lines: 2,
          durable_bytes: 36,
          durable_lines: 2,
          preview_bytes: 11,
          preview_lines: 1,
          output_available: true,
        },
        exit_code: 0,
        status: "completed",
        process_id: null,
        duration_ms: 10,
      },
    }, "turn-lazy-command", false);
  });

  const row = page.locator('[data-command-item-id="turn-lazy-command:command-lazy-1"]');
  await expect(row).toContainText("second line");
  expect(reads).toBe(0);
  await row.locator(".output-overlay-btn").evaluate((button: HTMLButtonElement) => button.click());
  await expect(page.locator("#codeView")).toContainText("full retained output");
  expect(reads).toBe(1);
  await expect.poll(() => linkReads).toBe(1);

  await page.locator("#codeClose").click();
  await expect(page.locator("#codeView")).toBeEmpty();
  await expect(page.locator("#codeDownload")).toBeDisabled();
  await expect(page.locator("#codeCopyDiff")).toBeHidden();

  await row.locator(".output-overlay-btn").evaluate((button: HTMLButtonElement) => button.click());
  await expect(page.locator("#codeView")).toContainText("full retained output");
  expect(reads).toBe(2);
  await expect.poll(() => linkReads).toBe(2);
});

test("failed command output fetch can be retried", async ({ page }) => {
  let reads = 0;
  await page.route("**/items/command-raw-retry/command-output", async route => {
    reads++;
    if (reads === 1) {
      await route.fulfill({ status: 500, contentType: "text/plain", body: "temporary read failure" });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "text/plain; charset=utf-8",
      body: "output loaded after retry",
    });
  });

  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-raw-retry", "turn-raw-retry");
  await page.locator('[data-command-item-id="turn-raw-retry:command-raw-retry"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());

  await expect(page.locator("#codeView")).toContainText("temporary read failure");
  await page.locator("#codeView .output-overlay-btn", { hasText: "Retry" }).click();
  await expect(page.locator("#codeView pre.out")).toHaveText("output loaded after retry");
  expect(reads).toBe(2);
});

test("closing command output isolates a late raw response", async ({ page }) => {
  let releaseRaw: (() => void) | undefined;
  const mayRespond = new Promise<void>(resolve => { releaseRaw = resolve; });
  let reads = 0;
  await page.route("**/items/command-close-raw/command-output", async route => {
    reads++;
    await mayRespond;
    await route.fulfill({ status: 200, body: "late closed output" }).catch(() => {});
  });

  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-close-raw", "turn-close-raw");
  await page.locator('[data-command-item-id="turn-close-raw:command-close-raw"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());
  await expect.poll(() => reads).toBe(1);
  await page.locator("#codeClose").click();
  releaseRaw?.();
  await page.waitForTimeout(100);

  await expect(page.locator("#codeOverlay")).not.toHaveClass(/open/);
  await expect(page.locator("#codeView")).toBeEmpty();
});

test("switching command overlays isolates a late raw response", async ({ page }) => {
  let releaseFirst: (() => void) | undefined;
  const firstMayRespond = new Promise<void>(resolve => { releaseFirst = resolve; });
  let firstReads = 0;
  await page.route("**/items/command-first-raw/command-output", async route => {
    firstReads++;
    await firstMayRespond;
    await route.fulfill({ status: 200, body: "late first output" }).catch(() => {});
  });
  await page.route("**/items/command-second-raw/command-output", async route => {
    await route.fulfill({ status: 200, body: "current second output" });
  });

  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-first-raw", "turn-first-raw");
  await addCompletedCommand(page, "command-second-raw", "turn-second-raw");
  await page.locator('[data-command-item-id="turn-first-raw:command-first-raw"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());
  await expect.poll(() => firstReads).toBe(1);
  await page.locator('[data-command-item-id="turn-second-raw:command-second-raw"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());
  await expect(page.locator("#codeView pre.out")).toHaveText("current second output");

  releaseFirst?.();
  await page.waitForTimeout(100);
  await expect(page.locator("#codeView pre.out")).toHaveText("current second output");
  await expect(page.locator("#codeView")).not.toContainText("late first output");
});

test("late clipboard completion does not relabel a different overlay", async ({ page }) => {
  await page.route("**/items/command-copy-race/command-output", async route => {
    await route.fulfill({ status: 200, body: "copied command output" });
  });
  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-copy-race", "turn-copy-race");
  await page.locator('[data-command-item-id="turn-copy-race:command-copy-race"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());
  await expect(page.locator("#codeView pre.out")).toHaveText("copied command output");

  await page.evaluate(() => {
    const app = window as unknown as {
      copyToClipboard: () => Promise<boolean>;
      finishCopy?: (ok: boolean) => void;
    };
    app.copyToClipboard = () => new Promise<boolean>(resolve => { app.finishCopy = resolve; });
  });
  await page.locator("#codeCopyDiff").click();
  await page.evaluate(() => {
    const app = window as unknown as {
      openDiffOverlay: (path: string, diff: string) => void;
      finishCopy?: (ok: boolean) => void;
    };
    app.openDiffOverlay("changed.rs", "--- a/changed.rs\n+++ b/changed.rs\n@@ -1 +1 @@\n-old\n+new\n");
    app.finishCopy?.(true);
  });

  await expect(page.locator("#codeCopyDiff")).toHaveText("Copy diff");
});

test("stale command output linkification falls back to plain text without retrying", async ({ page }) => {
  const staleEtag = '"sha256_2222222222222222222222222222222222222222222222222222222222222222"';
  let linkReads = 0;
  await page.route("**/items/command-stale-links/command-output-links", async route => {
    linkReads++;
    expect(route.request().headers()["if-output-match"]).toBe(staleEtag);
    await route.fulfill({ status: 412, contentType: "text/plain", body: "command output changed" });
  });
  await page.route("**/items/command-stale-links/command-output", async route => {
    await route.fulfill({
      status: 200,
      contentType: "text/plain; charset=utf-8",
      headers: { ETag: staleEtag },
      body: "src/stale.rs:12 remains readable",
    });
  });

  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-stale-links", "turn-stale-links");
  await page.locator('[data-command-item-id="turn-stale-links:command-stale-links"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());

  const output = page.locator("#codeView pre.out");
  await expect(output).toHaveText("src/stale.rs:12 remains readable");
  await expect(output.locator(".path-link")).toHaveCount(0);
  await expect.poll(() => linkReads).toBe(1);
  await page.waitForTimeout(100);
  expect(linkReads).toBe(1);
});

test("command output without an ETag skips the links request", async ({ page }) => {
  let linkReads = 0;
  await page.route("**/items/command-no-etag/command-output-links", async route => {
    linkReads++;
    await route.fulfill({ status: 500, body: "must not be requested" });
  });
  await page.route("**/items/command-no-etag/command-output", async route => {
    await route.fulfill({
      status: 200,
      contentType: "text/plain; charset=utf-8",
      body: "src/plain.rs:7 stays plain",
    });
  });

  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-no-etag", "turn-no-etag");
  await page.locator('[data-command-item-id="turn-no-etag:command-no-etag"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());

  const output = page.locator("#codeView pre.out");
  await expect(output).toHaveText("src/plain.rs:7 stays plain");
  await expect(output.locator(".path-link")).toHaveCount(0);
  await page.waitForTimeout(100);
  expect(linkReads).toBe(0);
});

test("closing command output cancels and isolates a late link response", async ({ page }) => {
  let releaseLinks: (() => void) | undefined;
  const mayRespond = new Promise<void>(resolve => { releaseLinks = resolve; });
  let linkReads = 0;
  await page.route("**/items/command-close-links/command-output-links", async route => {
    linkReads++;
    await mayRespond;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ links: [{ start: 0, end: 12, path: "src/close.rs", line: 3 }] }),
    }).catch(() => {});
  });
  await page.route("**/items/command-close-links/command-output", async route => {
    await route.fulfill({
      status: 200,
      contentType: "text/plain; charset=utf-8",
      headers: { ETag: '"sha256_3333333333333333333333333333333333333333333333333333333333333333"' },
      body: "src/close.rs:3",
    });
  });

  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-close-links", "turn-close-links");
  await page.locator('[data-command-item-id="turn-close-links:command-close-links"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());
  await expect.poll(() => linkReads).toBe(1);
  await page.locator("#codeClose").click();
  await expect(page.locator("#codeView")).toBeEmpty();

  releaseLinks?.();
  await page.waitForTimeout(100);
  await expect(page.locator("#codeView")).toBeEmpty();
  await expect(page.locator("#codeOverlay")).not.toHaveClass(/open/);
});

test("switching command overlays cancels and isolates late link responses", async ({ page }) => {
  let releaseFirst: (() => void) | undefined;
  const firstMayRespond = new Promise<void>(resolve => { releaseFirst = resolve; });
  let firstLinkReads = 0;
  let secondLinkReads = 0;

  await page.route("**/items/command-first-links/command-output-links", async route => {
    firstLinkReads++;
    await firstMayRespond;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ links: [{ start: 0, end: 12, path: "src/first.rs", line: 1 }] }),
    }).catch(() => {});
  });
  await page.route("**/items/command-first-links/command-output", async route => {
    await route.fulfill({
      status: 200,
      contentType: "text/plain; charset=utf-8",
      headers: { ETag: '"sha256_4444444444444444444444444444444444444444444444444444444444444444"' },
      body: "src/first.rs:1",
    });
  });
  await page.route("**/items/command-second-links/command-output-links", async route => {
    secondLinkReads++;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ links: [{ start: 0, end: 13, path: "src/second.rs", line: 2 }] }),
    });
  });
  await page.route("**/items/command-second-links/command-output", async route => {
    await route.fulfill({
      status: 200,
      contentType: "text/plain; charset=utf-8",
      headers: { ETag: '"sha256_5555555555555555555555555555555555555555555555555555555555555555"' },
      body: "src/second.rs:2",
    });
  });

  await openCommandOutputTestThread(page);
  await addCompletedCommand(page, "command-first-links", "turn-first-links");
  await addCompletedCommand(page, "command-second-links", "turn-second-links");
  await page.locator('[data-command-item-id="turn-first-links:command-first-links"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());
  await expect.poll(() => firstLinkReads).toBe(1);

  await page.locator('[data-command-item-id="turn-second-links:command-second-links"] .output-overlay-btn')
    .evaluate((button: HTMLButtonElement) => button.click());
  const output = page.locator("#codeView pre.out");
  await expect(output).toContainText("src/second.rs:2");
  await expect(output.locator(".path-link")).toHaveText("src/second.rs");
  await expect.poll(() => secondLinkReads).toBe(1);

  releaseFirst?.();
  await page.waitForTimeout(100);
  await expect(output).toContainText("src/second.rs:2");
  await expect(output.locator(".path-link")).toHaveText("src/second.rs");
  await expect(output.locator("text=src/first.rs")).toHaveCount(0);
});
