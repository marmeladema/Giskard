import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

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
        output: "failed before spawn",
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
});
