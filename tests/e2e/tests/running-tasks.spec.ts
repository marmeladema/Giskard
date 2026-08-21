import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

test("task snapshots stay menu-only while ordered events own one transcript row", async ({ page }) => {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
  await page.locator("#input").fill("Create a thread for running-task reconciliation.");
  await page.locator("#sendBtn").click();
  await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  const threadId = await page.locator(".thread.active").getAttribute("data-tid");
  expect(threadId).toBeTruthy();

  const result = await page.evaluate((activeThreadId) => {
    const app = window as unknown as {
      resetTranscriptForAuthoritativeSnapshot: () => void;
      applyThreadTasks: (snapshot: unknown, resetBaseline: boolean) => void;
      toggleTasksMenu: () => void;
      handleEvent: (event: unknown) => void;
    };
    const turnId = "turn-running-command";
    const itemId = "command-running-1";
    const rowSelector = `[data-command-item-id="${turnId}:${itemId}"]`;
    const task = {
      kind: "command",
      thread_id: activeThreadId,
      turn_id: turnId,
      item_id: itemId,
      harness_item_id: "native-command-running-1",
      command: "printf starting",
      cwd: "/tmp/project",
      status: "in_progress",
      process_id: "proc-running-1",
      started_at_ms: Date.now(),
      output: "starting",
      after_turn: false,
      terminating: false,
    };

    app.resetTranscriptForAuthoritativeSnapshot();
    app.applyThreadTasks({
      thread_id: activeThreadId,
      revision: 1,
      tasks: [task],
    }, true);
    app.toggleTasksMenu();
    const afterSnapshot = {
      taskCount: document.querySelector("#tasksCount")?.textContent,
      menuHidden: (document.querySelector("#tasksMenu") as HTMLElement | null)?.hidden,
      menuCards: document.querySelectorAll("#tasksMenu .cmd-card").length,
      menuText: document.querySelector("#tasksMenu")?.textContent || "",
      transcriptRows: document.querySelectorAll(rowSelector).length,
    };

    app.handleEvent({
      kind: "item_started",
      thread: activeThreadId,
      turn: turnId,
      item: {
        id: itemId,
        harness_item_id: "native-command-running-1",
        kind: "command_execution",
        command: {
          command: "printf starting",
          cwd: "/tmp/project",
          status: "in_progress",
          process_id: "proc-running-1",
          started_at_ms: task.started_at_ms,
        },
        tool: null,
      },
    });
    app.handleEvent({
      kind: "item_delta",
      thread: activeThreadId,
      turn: turnId,
      item_id: itemId,
      delta: { type: "command_output", chunk: "starting" },
    });

    // The same output is now present in both authorities. Refreshing the task projection must not
    // append it to the transcript projection, whose chronology comes only from ordered events.
    app.applyThreadTasks({
      thread_id: activeThreadId,
      revision: 2,
      tasks: [task],
    }, false);
    const runningRow = document.querySelector(rowSelector) as HTMLElement | null;
    const runningOutput = runningRow?.querySelector(".out")?.textContent || "";
    const afterOverlap = {
      rowCount: document.querySelectorAll(rowSelector).length,
      running: runningRow?.classList.contains("state-running"),
      output: runningOutput,
      startingOccurrences: runningOutput.split("starting").length - 1,
    };

    app.handleEvent({
      kind: "item_completed",
      thread: activeThreadId,
      turn: turnId,
      item: {
        id: itemId,
        harness_item_id: "native-command-running-1",
        payload: {
          kind: "command_execution",
          command: "printf starting",
          cwd: "/tmp/project",
          output: "starting\nfinished",
          exit_code: 0,
          status: "completed",
          process_id: "proc-running-1",
          duration_ms: 10,
        },
      },
    });
    app.applyThreadTasks({
      thread_id: activeThreadId,
      revision: 3,
      tasks: [],
    }, false);
    const completedRow = document.querySelector(rowSelector) as HTMLElement | null;
    return {
      afterSnapshot,
      afterOverlap,
      completed: {
        taskCount: document.querySelector("#tasksCount")?.textContent,
        rowCount: document.querySelectorAll(rowSelector).length,
        succeeded: completedRow?.classList.contains("state-succeeded"),
        output: completedRow?.querySelector(".out")?.textContent || "",
      },
    };
  }, threadId as string);

  expect(result.afterSnapshot.taskCount).toBe("1");
  expect(result.afterSnapshot.menuHidden).toBe(false);
  expect(result.afterSnapshot.menuCards).toBe(1);
  expect(result.afterSnapshot.menuText).toContain("$ printf starting");
  expect(result.afterSnapshot.transcriptRows).toBe(0);

  expect(result.afterOverlap).toEqual({
    rowCount: 1,
    running: true,
    output: "starting",
    startingOccurrences: 1,
  });

  expect(result.completed.taskCount).toBe("0");
  expect(result.completed.rowCount).toBe(1);
  expect(result.completed.succeeded).toBe(true);
  expect(result.completed.output).toBe("starting\nfinished");
});
