import { test, expect } from "@playwright/test";
import {
  SCRIPTED_NESTED_SUBAGENT_TRIGGER,
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
    await expect(transcript.locator(".msg.user", { hasText: "Sub-agent turn" })).toHaveCount(1);

    const promptRow = transcript.locator(".msg.user", { hasText: "Sub-agent turn" });
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
    const parentButton = page.getByRole("button", { name: /Back to parent thread:/ });
    await expect(parentButton).toBeVisible();
    await expect(page.locator("#readOnlyBanner")).toHaveText("This agent-owned thread is read-only.");
    await expect(page.locator("#input")).toBeDisabled();
    await expect(page.locator("#sendBtn")).toBeDisabled();
    await expect(page.locator("#modelPickerBtn")).toBeDisabled();
    await page.getByRole("button", { name: "Context usage" }).click();
    await expect(page.locator("#compactBtn")).toBeDisabled();
    await expect(page.locator("#modeSel")).toBeDisabled();
    await expect(page.locator("#permissionPresetSel")).toBeDisabled();
    await page.locator("#usageClose").click();

    await page.reload();
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_SUBAGENT_REPLY })).toBeVisible();
    const restored = await page.evaluate(() =>
      JSON.parse(localStorage.getItem("giskard.lastThread") || "null"),
    );
    expect(restored?.tid).toBe(childSelection.tid);
    await expect(parentRow).toBeVisible();
    await expect(parentButton).toBeVisible();

    await parentButton.click();
    await expect.poll(async () => {
      const selected = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("giskard.lastThread") || "null")?.tid,
      );
      return selected;
    }).toBe(parentSelection.tid);
    await expect(parentRow).toBeVisible();
    await expect(parentButton).toBeHidden();

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
    await parentRowContainer.locator(".thread-menu .danger").click();
    await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);
    await expect(page.locator("#removeThreadCascade")).toContainText("1 linked sub-agent thread");
    await expect(page.locator("#removeThreadCascade")).toContainText("all corresponding Codex threads");
    await expect(page.locator("#removeThreadModal")).toContainText("cannot be undone");
    await page.locator("#removeThreadConfirm").click();
    await expect(parentRow).toHaveCount(0);

    const remainingIds = await page.evaluate(async (pid) => {
      const response = await fetch(`/api/projects/${pid}/threads`);
      const body = await response.json();
      return body.threads.map((thread: { id: string }) => thread.id);
    }, parentSelection.pid);
    expect(remainingIds).not.toContain(parentSelection.tid);
    expect(remainingIds).not.toContain(childSelection.tid);
  });

  test("recognizes only valid managed sub-agent ownership chains", async ({ page }) => {
    const result = await page.evaluate(() => {
      const app = window as unknown as {
        isManagedSubagentThread: (thread: unknown, threads: unknown[]) => boolean;
      };

      return {
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

    expect(result.validChain).toBe(true);
    expect(result.malformedIntermediate).toBe(false);
  });

  test("quarantines invalid sub-agent chains from ordinary sidebar rows", async ({ page }) => {
    const result = await page.evaluate(() => {
      const projectId = "quarantine-test";
      const box = document.createElement("div");
      box.id = `threads-${projectId}`;
      document.body.append(box);
      const appState = (window as any).eval("state");
      appState.projectThreads.set(projectId, [
        {
          id: "damaged-child",
          kind: "subagent",
          parent_thread_id: "missing-parent",
          archived: false,
        },
      ]);
      (window as any).renderProjectThreads(projectId);
      const rendered = {
        rows: box.querySelectorAll(".thread-row").length,
        warning: box.textContent || "",
      };
      appState.projectThreads.delete(projectId);
      box.remove();
      return rendered;
    });

    expect(result.rows).toBe(0);
    expect(result.warning).toContain("1 damaged agent-owned thread record is hidden");
    expect(result.warning).toContain("targeted cleanup is planned");
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
              permission_preset: "ask_first",
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
      await otherRowContainer.locator(".thread-menu .danger").click();
      await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);
      await page.locator("#removeThreadConfirm").click();
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

// Deleting the thread that is currently open used to leave the view titled with the deleted
// thread's name and an empty transcript ("No thread selected"). It now drops into a fresh draft
// in the same project so the composer is ready for the next conversation, and clears the persisted
// last-thread so a reload no longer resurrects the deleted thread.
test.describe("active-thread deletion lands on a draft", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // Shared seeding helpers: create an isolated project + persisted thread server-side so the
  // test below drives only the deletion without depending on the shared composer/WebSocket
  // timing. Mirrors the helpers in the "delete-thread confirmation card" block below.
  const createProject = (page: import("@playwright/test").Page, name: string): Promise<string> =>
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
      if (!res.ok) {
        throw new Error(`create project failed: ${res.status} ${await res.text()}`);
      }
      return (await res.json()).id as string;
    }, name);
  const startThread = (page: import("@playwright/test").Page, pid: string, text: string): Promise<string> =>
    page.evaluate(
      async ({ pid, text }) => {
        const res = await fetch(`/api/projects/${pid}/threads/start`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            text,
            model_ref: { provider: "replay", model: "replay-model" },
            mode: "build",
            permission_preset: "ask_first",
          }),
        });
        if (!res.ok) {
          throw new Error(`start thread failed: ${res.status} ${await res.text()}`);
        }
        return (await res.json()).thread_id as string;
      },
      { pid, text },
    );
  const deleteProject = (page: import("@playwright/test").Page, id: string): Promise<void> =>
    page.evaluate(async (projectId) => {
      await fetch(`/api/projects/${projectId}`, { method: "DELETE" });
    }, id);

  test("deleting the open thread opens a draft in the same project", async ({ page }) => {
    const projectId = await createProject(page, "Draft-after-delete project");
    try {
      const tid = await startThread(page, projectId, "delete me while open");
      await page.evaluate(() =>
        (window as unknown as { loadProjects: () => Promise<void> }).loadProjects(),
      );
      const row = page.locator(`.thread[data-tid="${tid}"]`);
      await expect(row).toBeVisible();

      // Open the thread so it is unambiguously the active view.
      await row.click();
      await expect.poll(async () =>
        page.evaluate(() =>
          JSON.parse(localStorage.getItem("giskard.lastThread") || "null"),
        ),
      ).toEqual({ pid: projectId, tid });

      // Delete the active thread via its row menu.
      const rowContainer = row.locator("xpath=..");
      await rowContainer.locator(".thread-menu-btn").click();
      await rowContainer.locator(".thread-menu .danger").click();
      await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);
      await page.locator("#removeThreadConfirm").click();
      await expect(row).toHaveCount(0);

      // The view is now a draft in the same project: the composer is visible and ready, the
      // title bar shows "New thread" (not the deleted thread's name), and the transcript shows
      // the draft explainer rather than the stale thread or an empty "No thread selected" view.
      await expect(page.locator("#composer")).toBeVisible();
      await expect(page.locator("#input")).toBeVisible();
      // The composer keeps focus after the deletion: the modal skips focus restoration so it
      // does not yank focus back to the deleted thread's row button.
      await expect(page.locator("#input")).toBeFocused();
      await expect(page.locator("#mbTitle")).toContainText("New thread");
      await expect(page.locator("#transcript .draft-empty")).toBeVisible();
      await expect(page.locator("#transcript")).not.toContainText("No thread selected");

      // The deleted thread is no longer the persisted last-thread, so a reload does not
      // resurrect it; the draft view is what reload would land on (no lastThread entry).
      const lastAfter = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("giskard.lastThread") || "null"),
      );
      expect(lastAfter).toBeNull();
    } finally {
      await deleteProject(page, projectId);
    }
  });
});

test.describe("delete-thread confirmation card", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // Shared seeding helpers: create an isolated project + persisted thread server-side so the
  // tests below drive only the modal without depending on the shared composer/WebSocket timing.
  const createProject = (page: import("@playwright/test").Page, name: string): Promise<string> =>
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
      if (!res.ok) {
        throw new Error(`create project failed: ${res.status} ${await res.text()}`);
      }
      return (await res.json()).id as string;
    }, name);
  const startThread = (page: import("@playwright/test").Page, pid: string, text: string): Promise<string> =>
    page.evaluate(
      async ({ pid, text }) => {
        const res = await fetch(`/api/projects/${pid}/threads/start`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            text,
            model_ref: { provider: "replay", model: "replay-model" },
            mode: "build",
            permission_preset: "ask_first",
          }),
        });
        if (!res.ok) {
          throw new Error(`start thread failed: ${res.status} ${await res.text()}`);
        }
        return (await res.json()).thread_id as string;
      },
      { pid, text },
    );
  const deleteProject = (page: import("@playwright/test").Page, id: string): Promise<void> =>
    page.evaluate(async (projectId) => {
      await fetch(`/api/projects/${projectId}`, { method: "DELETE" });
    }, id);
  // Open the delete-thread card for `tid` via its sidebar row menu.
  async function openDeleteCard(page: import("@playwright/test").Page, tid: string) {
    const row = page.locator(`.thread[data-tid="${tid}"]`);
    const rowContainer = row.locator("xpath=..");
    const menuButton = rowContainer.locator(".thread-menu-btn");
    await menuButton.click();
    await rowContainer.locator(".thread-menu .danger").click();
    await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);
    await expect(page.locator("#removeThreadConfirm")).toBeFocused();
    return menuButton;
  }

  test("Cancel dismisses the card without deleting", async ({ page }) => {
    const projectId = await createProject(page, "Card cancel project");
    try {
      const tid = await startThread(page, projectId, "survives cancel");
      await page.evaluate(() =>
        (window as unknown as { loadProjects: () => Promise<void> }).loadProjects(),
      );
      const row = page.locator(`.thread[data-tid="${tid}"]`);
      await expect(row).toBeVisible();

      const menuButton = await openDeleteCard(page, tid);
      await page.locator("#removeThreadCancel").click();
      await expect(page.locator("#removeThreadModal")).not.toHaveClass(/open/);
      await expect(menuButton).toBeFocused();
      // The thread is still listed: no DELETE was issued.
      await expect(row).toBeVisible();
      const stillThere = await page.evaluate(async (pid) => {
        const res = await fetch(`/api/projects/${pid}/threads`);
        const body = await res.json();
        return body.threads.map((t: { id: string }) => t.id);
      }, projectId);
      expect(stillThere).toContain(tid);
    } finally {
      await deleteProject(page, projectId);
    }
  });

  test("Escape and outside-click dismiss without deleting", async ({ page }) => {
    const projectId = await createProject(page, "Card escape project");
    try {
      const tid = await startThread(page, projectId, "survives escape");
      await page.evaluate(() =>
        (window as unknown as { loadProjects: () => Promise<void> }).loadProjects(),
      );
      const row = page.locator(`.thread[data-tid="${tid}"]`);
      await expect(row).toBeVisible();

      // Escape closes the card.
      const escapeMenuButton = await openDeleteCard(page, tid);
      await page.keyboard.press("Escape");
      await expect(page.locator("#removeThreadModal")).not.toHaveClass(/open/);
      await expect(escapeMenuButton).toBeFocused();
      await expect(row).toBeVisible();

      // Reopen and dismiss by clicking the overlay backdrop (outside the dialog). Click a corner
      // of the full-screen overlay rather than its center, which would hit the dialog card.
      const backdropMenuButton = await openDeleteCard(page, tid);
      await page.locator("#removeThreadModal").click({ position: { x: 4, y: 4 } });
      await expect(page.locator("#removeThreadModal")).not.toHaveClass(/open/);
      await expect(backdropMenuButton).toBeFocused();
      await expect(row).toBeVisible();
    } finally {
      await deleteProject(page, projectId);
    }
  });

  test("a failed DELETE surfaces an inline error and keeps the card open", async ({ page }) => {
    const projectId = await createProject(page, "Card error project");
    try {
      const tid = await startThread(page, projectId, "delete failure");
      await page.evaluate(() =>
        (window as unknown as { loadProjects: () => Promise<void> }).loadProjects(),
      );
      const row = page.locator(`.thread[data-tid="${tid}"]`);
      await expect(row).toBeVisible();

      let resolveDeleteStarted: (() => void) | null = null;
      let releaseDelete: (() => void) | null = null;
      const deleteStarted = new Promise<void>((resolve) => {
        resolveDeleteStarted = resolve;
      });
      const deleteReleased = new Promise<void>((resolve) => {
        releaseDelete = resolve;
      });

      // Make the single DELETE for this thread fail with a 500 so the handler surfaces the inline
      // error instead of closing the card. `times: 1` lets the route auto-unregister after it
      // fires, so a later retry (or teardown) reaches the real endpoint.
      await page.route(`**/api/projects/${projectId}/threads/${tid}`, async (route) => {
        if (route.request().method() !== "DELETE") {
          await route.continue();
          return;
        }
        resolveDeleteStarted?.();
        await deleteReleased;
        await route.fulfill({ status: 500, body: "scripted delete failure" });
      }, { times: 1 });

      await openDeleteCard(page, tid);
      const confirm = page.locator("#removeThreadConfirm");
      const cancel = page.locator("#removeThreadCancel");
      await confirm.click();
      await deleteStarted;
      await expect(confirm).toBeDisabled();
      await expect(cancel).toBeDisabled();
      await page.keyboard.press("Escape");
      await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);
      await page.locator("#removeThreadModal").click({ position: { x: 4, y: 4 } });
      await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);
      releaseDelete?.();
      // The card stays open with an inline error message, not a closed-and-toasted failure.
      await expect(page.locator("#removeThreadErr")).toContainText("Delete thread failed");
      await expect(page.locator("#removeThreadModal")).toHaveClass(/open/);
      // Confirm is re-enabled after the failure so the user can dismiss or retry.
      await expect(confirm).not.toBeDisabled();
      // The DELETE was intercepted before reaching the backend, so the thread is still listed.
      await expect(row).toBeVisible();
    } finally {
      await deleteProject(page, projectId);
    }
  });
});
