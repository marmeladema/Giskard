import { test, expect } from "@playwright/test";
import {
  SCRIPTED_NESTED_SUBAGENT_TRIGGER,
  SCRIPTED_SUBAGENT_PROMPT,
  SCRIPTED_SUBAGENT_REPLY,
  SCRIPTED_SUBAGENT_TRIGGER,
  login,
} from "./helpers";

test.describe("linked sub-agent threads", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("opens, restores, and reverse-navigates without losing the parent", async ({ page }) => {
    let linkedOpenRequests = 0;
    page.on("request", (request) => {
      if (
        request.method() === "POST" &&
        request.url().includes("/subagent-links/") &&
        request.url().endsWith("/open")
      ) linkedOpenRequests += 1;
    });
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await page.locator("#input").fill(SCRIPTED_SUBAGENT_TRIGGER);
    await page.locator("#sendBtn").click();

    const transcript = page.locator("#transcript");
    const parentLink = transcript.getByRole("button", { name: "Open linked thread" });
    await expect(parentLink).toBeVisible();

    const parentSelection = await page.evaluate(() =>
      JSON.parse(localStorage.getItem("giskard.lastThread") || "null"),
    );
    expect(parentSelection?.pid).toBeTruthy();
    expect(parentSelection?.tid).toBeTruthy();
    const parentRow = page.locator(`.thread[data-tid="${parentSelection.tid}"]`);
    await expect(parentRow).toBeVisible();

    await parentLink.click();
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_SUBAGENT_REPLY })).toBeVisible();
    await expect(transcript.locator(".msg.user", { hasText: SCRIPTED_SUBAGENT_PROMPT })).toHaveCount(1);

    const promptRow = transcript.locator(".msg.user", { hasText: SCRIPTED_SUBAGENT_PROMPT });
    const replyRow = transcript.locator(".msg.agent", { hasText: SCRIPTED_SUBAGENT_REPLY });
    const promptBeforeReply = await promptRow.evaluate(
      (prompt, reply) => !!(prompt.compareDocumentPosition(reply as Node) & Node.DOCUMENT_POSITION_FOLLOWING),
      await replyRow.elementHandle(),
    );
    expect(promptBeforeReply).toBe(true);

    const childSelection = await page.evaluate(() =>
      JSON.parse(localStorage.getItem("giskard.lastThread") || "null"),
    );
    expect(childSelection?.tid).not.toBe(parentSelection.tid);
    await expect(page.locator("#turnPickerBtn")).toBeDisabled();
    await expect(page.locator("#modeSel")).toBeDisabled();
    await expect(page.locator("#approvalSel")).toBeDisabled();
    await expect(page.locator("#modelPickerBtn")).toBeEnabled();
    await expect(page.locator("#modelSel")).toBeEnabled();
    await expect(page.locator("#effortSel")).toBeEnabled();
    const parentButton = page.getByRole("button", { name: /Back to parent thread:/ });
    await expect(parentButton).toBeVisible();

    await page.reload();
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_SUBAGENT_REPLY })).toBeVisible();
    const restored = await page.evaluate(() =>
      JSON.parse(localStorage.getItem("giskard.lastThread") || "null"),
    );
    expect(restored?.tid).toBe(childSelection.tid);
    await expect(parentRow).toBeVisible();
    await expect(parentButton).toBeVisible();
    await expect(page.locator("#turnPickerBtn")).toBeDisabled();
    await expect(page.locator("#modeSel")).toBeDisabled();
    await expect(page.locator("#approvalSel")).toBeDisabled();

    await parentButton.click();
    await expect.poll(async () => {
      const selected = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("giskard.lastThread") || "null")?.tid,
      );
      return selected;
    }).toBe(parentSelection.tid);
    await expect(parentRow).toBeVisible();
    await expect(parentButton).toBeHidden();
    await expect(page.locator("#turnPickerBtn")).toBeEnabled();
    await expect(page.locator("#modeSel")).toBeEnabled();
    await expect(page.locator("#approvalSel")).toBeEnabled();

    const opensBeforeKnownOpen = linkedOpenRequests;
    await transcript.getByRole("button", { name: "Open linked thread" }).click();
    await expect.poll(async () => {
      const selected = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("giskard.lastThread") || "null")?.tid,
      );
      return selected;
    }).toBe(childSelection.tid);
    expect(linkedOpenRequests).toBe(opensBeforeKnownOpen + 1);
    await parentButton.click();
    await expect.poll(async () => {
      const selected = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("giskard.lastThread") || "null")?.tid,
      );
      return selected;
    }).toBe(parentSelection.tid);

    await page.locator("#subagentsBtn").click();
    await expect(page.locator("#subagentsMenu .subagent-card")).toHaveCount(1);
    await page.locator("#subagentsClose").click();

    const parentRowContainer = parentRow.locator("xpath=..");
    await parentRowContainer.locator(".thread-menu-btn").click();
    const dialogPromise = page.waitForEvent("dialog");
    const deleteClick = parentRowContainer.locator(".thread-menu .danger").click();
    const dialog = await dialogPromise;
    expect(dialog.message()).toContain("1 linked sub-agent thread");
    expect(dialog.message()).toContain("all corresponding Codex threads");
    expect(dialog.message()).toContain("cannot be undone");
    await dialog.accept();
    await deleteClick;
    await expect(parentRow).toHaveCount(0);

    const remainingIds = await page.evaluate(async (pid) => {
      const response = await fetch(`/api/projects/${pid}/threads`);
      const body = await response.json();
      return body.threads.map((thread: { id: string }) => thread.id);
    }, parentSelection.pid);
    expect(remainingIds).not.toContain(parentSelection.tid);
    expect(remainingIds).not.toContain(childSelection.tid);
  });

  test("keeps one prompt row before output for early and late metadata", async ({ page }) => {
    const result = await page.evaluate(() => {
      const app = window as unknown as {
        resetTranscriptForAuthoritativeSnapshot: () => void;
        renderLiveTurnUserInput: (turn: string, input: { type: string; text: string }) => void;
        addItem: (item: unknown, turn: string, fromHistory: boolean) => void;
        isManagedSubagentThread: (thread: unknown, threads: unknown[]) => boolean;
      };
      const transcript = document.querySelector("#transcript") as HTMLElement;
      const run = (turn: string, prompt: string, provisionalFirst: boolean) => {
        app.resetTranscriptForAuthoritativeSnapshot();
        if (provisionalFirst) {
          app.renderLiveTurnUserInput(turn, { type: "text", text: prompt });
        }
        app.addItem({
          id: `${turn}-output`,
          harness_item_id: `${turn}-output`,
          payload: { kind: "agent_message", text: `${turn} output` },
        }, turn, false);
        app.addItem({
          id: `${turn}-prompt`,
          harness_item_id: `subagent_prompt:${turn}`,
          payload: { kind: "user_message", text: prompt },
        }, turn, false);
        const rows = Array.from(transcript.querySelectorAll(`.msg[data-turn="${turn}"]`));
        return {
          userRows: rows.filter(row => row.classList.contains("user")).length,
          promptFirst: rows[0]?.classList.contains("user") === true,
          texts: rows.map(row => row.textContent || ""),
        };
      };

      return {
        early: run("browser-early", "early child prompt", true),
        late: run("browser-late", "late child prompt", false),
        validChain: app.isManagedSubagentThread(
          { id: "child", kind: "subagent", parent_thread_id: "root" },
          [
            { id: "root", kind: "primary", parent_thread_id: null },
            { id: "child", kind: "subagent", parent_thread_id: "root" },
          ],
        ),
        malformedIntermediate: app.isManagedSubagentThread(
          { id: "grandchild", kind: "subagent", parent_thread_id: "broken" },
          [
            { id: "root", kind: "primary", parent_thread_id: null },
            { id: "broken", kind: "primary", parent_thread_id: "root" },
            { id: "grandchild", kind: "subagent", parent_thread_id: "broken" },
          ],
        ),
      };
    });

    expect(result.early.userRows).toBe(1);
    expect(result.early.promptFirst).toBe(true);
    expect(result.early.texts[0]).toContain("early child prompt");
    expect(result.late.userRows).toBe(1);
    expect(result.late.promptFirst).toBe(true);
    expect(result.late.texts[0]).toContain("late child prompt");
    expect(result.validChain).toBe(true);
    expect(result.malformedIntermediate).toBe(false);
  });

  test("restores a running nested sub-agent activity after reload", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await page.locator("#input").fill(SCRIPTED_NESTED_SUBAGENT_TRIGGER);
    await page.locator("#sendBtn").click();

    const transcript = page.locator("#transcript");
    await transcript.getByRole("button", { name: "Open linked thread" }).click();
    const parentButton = page.getByRole("button", { name: /Back to parent thread:/ });
    await expect(parentButton).toBeVisible();
    const firstChild = await page.evaluate(() =>
      JSON.parse(localStorage.getItem("giskard.lastThread") || "null"),
    );
    const runningActivity = transcript.locator(".msg.activity", { hasText: "Sub-agent running" });
    await expect(runningActivity).toBeVisible();

    const nestedOpenRequest = page.waitForRequest((request) =>
      request.method() === "POST" &&
      request.url().includes(`/threads/${firstChild.tid}/subagent-links/`) &&
      request.url().endsWith("/open"),
    );
    await runningActivity.getByRole("button", { name: "Open linked thread" }).click();
    const nestedOpenUrl = new URL((await nestedOpenRequest).url());
    const nestedItemId = nestedOpenUrl.pathname.split("/subagent-links/")[1]?.split("/")[0];
    expect(nestedItemId).toMatch(/^[0-7][0-9A-HJKMNP-TV-Z]{25}$/i);

    await expect.poll(async () => {
      const selected = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("giskard.lastThread") || "null")?.tid,
      );
      return selected;
    }).not.toBe(firstChild.tid);
    await expect(parentButton).toBeVisible();
    await parentButton.click();
    await expect.poll(async () => {
      const selected = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("giskard.lastThread") || "null")?.tid,
      );
      return selected;
    }).toBe(firstChild.tid);
    await expect(runningActivity).toBeVisible();

    await page.reload();
    await expect(runningActivity).toBeVisible();
    await expect(runningActivity.getByRole("button", { name: "Open linked thread" })).toBeVisible();
  });
});

test.describe("cross-project thread deletion", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // Deleting a thread cascades to descendants the server discovered itself, so the browser clears
  // the active transcript when that thread no longer exists in the refreshed list. That decision
  // must be scoped to the deleted thread's project: if the user has navigated to a thread in a
  // different project, deleting elsewhere must not wipe the unrelated active view. A source-string
  // assertion cannot observe this race, so exercise it end to end.
  test("deleting a thread in another project keeps the active view", async ({ page }) => {
    // Use two freshly created projects instead of the shared "Demo" project, so this test is
    // isolated from threads other tests leave behind on the reused replay server. The replay
    // server leaves browse roots unrestricted, so projects can be seeded against an existing
    // directory instead of driving the folder picker.
    const createProject = (name: string): Promise<string> =>
      page.evaluate(async (projectName) => {
        const res = await fetch("/api/projects", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name: projectName,
            dir: "/tmp",
            default_model: { provider: "replay", model: "replay-model" },
          }),
        });
        return (await res.json()).id as string;
      }, name);
    const deleteProject = (id: string): Promise<void> =>
      page.evaluate(async (projectId) => {
        await fetch(`/api/projects/${projectId}`, { method: "DELETE" });
      }, id);
    // Create a persisted thread server-side, avoiding the shared composer and its WebSocket-open
    // timing (an unrelated concern that only makes this UI test flaky).
    const startThread = (pid: string, text: string): Promise<string> =>
      page.evaluate(
        async ({ pid, text }) => {
          const res = await fetch(`/api/projects/${pid}/threads/start`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              text,
              model_ref: { provider: "replay", model: "replay-model" },
              mode: "build",
              approval_policy: "ask",
            }),
          });
          return (await res.json()).thread_id as string;
        },
        { pid, text },
      );

    const otherProjectName = "Other project A";
    const activeProjectName = "Active project B";
    const otherProjectId = await createProject(otherProjectName);
    const activeProjectId = await createProject(activeProjectName);

    try {
      const otherThreadId = await startThread(otherProjectId, "thread in project A");
      const activeThreadId = await startThread(activeProjectId, "thread in project B");
      expect(activeThreadId).not.toBe(otherThreadId);

      // Render both projects and their threads in the sidebar.
      await page.evaluate(() =>
        (window as unknown as { loadProjects: () => Promise<void> }).loadProjects(),
      );
      const lastThread = () =>
        page.evaluate(() => JSON.parse(localStorage.getItem("giskard.lastThread") || "null"));
      const otherRow = page.locator(`.thread[data-tid="${otherThreadId}"]`);
      const activeRow = page.locator(`.thread[data-tid="${activeThreadId}"]`);
      await expect(otherRow).toBeVisible();
      await expect(activeRow).toBeVisible();

      // Open project B's thread so it is unambiguously the active view.
      await activeRow.click();
      await expect
        .poll(async () => {
          const selection = await lastThread();
          return `${selection?.pid}:${selection?.tid}`;
        })
        .toBe(`${activeProjectId}:${activeThreadId}`);

      // Delete project A's thread via its row menu while project B's thread is the active view.
      const otherRowContainer = otherRow.locator("xpath=..");
      await otherRowContainer.locator(".thread-menu-btn").click();
      const dialogPromise = page.waitForEvent("dialog");
      const deleteClick = otherRowContainer.locator(".thread-menu .danger").click();
      const dialog = await dialogPromise;
      await dialog.accept();
      await deleteClick;
      await expect(otherRow).toHaveCount(0);

      // The active view is unaffected: its selection persists (a non-scoped clear would null it
      // out), because cascade-delete clearing is scoped to the deleted thread's project.
      const afterSelection = await lastThread();
      expect(afterSelection?.pid).toBe(activeProjectId);
      expect(afterSelection?.tid).toBe(activeThreadId);
    } finally {
      await deleteProject(otherProjectId);
      await deleteProject(activeProjectId);
    }
  });
});
