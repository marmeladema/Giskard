import { test, expect, type Page } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

/**
 * A draft is one per project, and it keeps everything picked for it — text, model and effort, mode,
 * permissions, Git checkout — until it is sent: across switching away and back, and across a reload.
 * Sending is what ends it, so the next draft starts from the defaults again.
 */
test.describe("draft settings", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  const demo = (page: Page) => page.locator(".proj", { hasText: "Demo" });
  const newDraft = (page: Page) => demo(page).locator(".project-add").click();
  const modelButton = (page: Page) => page.locator("#modelPickerBtn");
  const turnLabel = (page: Page) => page.locator("#turnPickerBtn .mp-label");

  /** Change every setting a draft offers away from its default. */
  async function pickEverything(page: Page) {
    await expect(modelButton(page)).toContainText("Replay Model");
    await modelButton(page).click();
    await page.locator("#effortSel").selectOption("high");
    await expect(modelButton(page)).toContainText("High");
    await page.keyboard.press("Escape");

    await page.locator("#turnPickerBtn").click();
    await page.locator("#modeSel").selectOption("plan");
    await page.locator("#permissionPresetSel").selectOption("auto_approve");
    await page.keyboard.press("Escape");
    await expect(turnLabel(page)).toHaveText("Plan · Auto approve");

    await page.locator("#gitStrategySel").selectOption("worktree");
  }

  /** Send a draft in Demo and wait for its thread, which is then the open view. */
  async function startThread(page: Page, text: string) {
    await newDraft(page);
    await page.locator("#input").fill(text);
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
    await expect(page.locator(".thread.active")).toHaveCount(1);
  }

  async function seedDemoDraft(page: Page, record: object) {
    const pid = await demo(page).getAttribute("data-pid");
    expect(pid).toBeTruthy();
    await page.evaluate(([pid, record]) => {
      localStorage.setItem(`giskard.composerDraft.draft:${pid}`,
        JSON.stringify({ ...(record as object), savedAt: Date.now() }));
    }, [pid, record] as const);
  }

  async function expectEverythingKept(page: Page, text: string) {
    await expect(page.locator("#transcript")).toContainText("Start a new thread");
    await expect(page.locator("#input")).toHaveValue(text);
    await expect(modelButton(page)).toContainText("High");
    await expect(turnLabel(page)).toHaveText("Plan · Auto approve");
    await expect(page.locator("#gitStrategySel")).toHaveValue("worktree");
  }

  test("keeps them when the draft is opened again", async ({ page }) => {
    await newDraft(page);
    await page.locator("#input").fill("Kept with its settings");
    await pickEverything(page);

    await newDraft(page);

    await expectEverythingKept(page, "Kept with its settings");
  });

  test("comes back to the draft, settings and all, after a reload", async ({ page }) => {
    await newDraft(page);
    await page.locator("#input").fill("Survives a reload");
    await pickEverything(page);

    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    await expectEverythingKept(page, "Survives a reload");

    // What is shown is what is sent.
    const started = page.waitForRequest(
      (r) => r.method() === "POST" && r.url().endsWith("/threads/start"),
    );
    await page.locator("#sendBtn").click();
    const body = (await started).postDataJSON();
    expect(body).toMatchObject({
      text: "Survives a reload",
      mode: "plan",
      permission_preset: "auto_approve",
      git_strategy: "worktree",
      model_ref: { provider: "replay", model: "replay-model", reasoning_effort: "high" },
    });
  });

  test("keeps settings picked before any text is typed", async ({ page }) => {
    await newDraft(page);
    await pickEverything(page);

    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    await expectEverythingKept(page, "");
  });

  test("starts the next draft from the defaults once this one is sent", async ({ page }) => {
    await newDraft(page);
    await page.locator("#input").fill("Sent, so its settings go with it");
    await pickEverything(page);
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();

    await newDraft(page);

    await expect(page.locator("#input")).toHaveValue("");
    await expect(modelButton(page)).toContainText("Replay Model");
    await expect(modelButton(page)).not.toContainText("High");
    await expect(turnLabel(page)).toHaveText("Build · Ask first");
    await expect(page.locator("#gitStrategySel")).toHaveValue("shared");
  });

  // The configuration can change between visits. A model it no longer offers cannot be started on,
  // so the draft falls back to the project's default — and says so, since it is not what was picked.
  test("falls back to the default when the saved model is gone", async ({ page }) => {
    const pid = await demo(page).getAttribute("data-pid");
    expect(pid).toBeTruthy();
    await page.evaluate((pid) => {
      localStorage.setItem(`giskard.composerDraft.draft:${pid}`, JSON.stringify({
        text: "Saved on a model since removed",
        settings: { model: { harness: null, provider: "gone", model: "gone-model" }, mode: "plan" },
        savedAt: Date.now(),
      }));
      localStorage.setItem("giskard.lastThread", JSON.stringify({ pid, draft: true }));
    }, pid);

    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);

    await expect(page.locator("#input")).toHaveValue("Saved on a model since removed");
    await expect(modelButton(page)).toContainText("Replay Model");
    await expect(page.locator("#notices")).toContainText("gone-model");
    // Only the model was dropped.
    await expect(turnLabel(page)).toHaveText("Plan · Ask first");
    await expect(page.locator("#sendBtn")).toBeEnabled();
  });

  // The fallback saves the draft, and must save the draft's own text — not what the composer held
  // for the view just left. Here the catalog arrives after the draft has opened; `openDraftThread`
  // settles last for the case where it is already loaded, which a thread with a harness never leaves.
  test("keeps the draft's text when its saved model is gone", async ({ page }) => {
    await startThread(page, "A thread to come back from");
    await page.locator("#input").fill("Typed into the thread");
    await seedDemoDraft(page, {
      text: "The draft's own text",
      settings: { model: { harness: null, provider: "gone", model: "gone-model" } },
    });

    await newDraft(page);

    await expect(page.locator("#notices")).toContainText("gone-model");
    await expect(page.locator("#input")).toHaveValue("The draft's own text");
    await page.locator(".thread.active, .thread").first().click();
    await newDraft(page);
    await expect(page.locator("#input")).toHaveValue("The draft's own text");
  });

  // Each tab saves straight to storage; a tab's memory is only for what storage refused. So a draft
  // another tab has sent stays sent here, rather than coming back from this tab's memory.
  test("does not bring back a draft another tab has sent", async ({ page, context }) => {
    await startThread(page, "Somewhere to leave the draft for");
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    const thread = page.locator(`.thread[data-tid="${tid}"]`);
    await newDraft(page);
    await page.locator("#input").fill("Sent from the other tab");
    await thread.click();
    await expect(page.locator("#input")).toHaveValue("");

    const other = await context.newPage();
    await other.goto("/");
    await expect(other.locator("#app")).toHaveClass(/open/);
    await other.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await expect(other.locator("#input")).toHaveValue("Sent from the other tab");
    await expect(other.locator("#sendBtn")).toBeEnabled();
    await other.locator("#sendBtn").click();
    await expect(
      other.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
    await other.close();

    await newDraft(page);
    await expect(page.locator("#input")).toHaveValue("");
  });

  // Leaving a draft while its first send is in flight saves it again. Once the thread exists that
  // copy is a message already sent, and must not be offered for sending a second time.
  test("forgets a draft sent just before leaving it", async ({ page }) => {
    await startThread(page, "Somewhere to go while the send is in flight");
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    const thread = page.locator(`.thread[data-tid="${tid}"]`);
    await newDraft(page);
    await page.locator("#input").fill("Sent, then left at once");
    await page.locator("#turnPickerBtn").click();
    await page.locator("#modeSel").selectOption("plan");
    await page.keyboard.press("Escape");

    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    let arrived!: () => void;
    const reached = new Promise<void>((resolve) => { arrived = resolve; });
    await page.route(/\/threads\/start$/, async (route) => {
      arrived();
      await held;
      return route.fallback();
    });
    const started = page.waitForResponse((r) => r.url().endsWith("/threads/start"));
    const threadsBefore = await page.locator(".thread").count();
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#sendBtn").click();
    // The send checks the UI version first, and leaving before that answers cancels it outright;
    // the window under test opens only once the start request is on its way.
    await reached;
    await thread.click();
    await expect(page.locator(".thread.active")).toHaveCount(1);
    release();
    expect((await started).ok()).toBe(true);
    // The stale continuation reloads the list once the thread exists; its row is the sign it ran.
    await expect(page.locator(".thread")).toHaveCount(threadsBefore + 1);

    await newDraft(page);
    await expect(page.locator("#input")).toHaveValue("");
    await expect(turnLabel(page)).toHaveText("Build · Ask first");
  });

  // A tab still showing the draft another tab sent holds an unchanged copy. Leaving saves the
  // composer it leaves, and that save must not write the sent message back; an edit made after the
  // send is new work, and is kept. Storage writes for other composers — here, forgetting a deleted
  // thread's text — must not disturb what this tab knows about its own.
  test("does not write back a draft another tab sent while it was open", async ({ page, context }) => {
    await startThread(page, "Somewhere to go after the other tab sends");
    const tid = await page.locator(".thread.active").getAttribute("data-tid");
    const thread = page.locator(`.thread[data-tid="${tid}"]`);
    await startThread(page, "Deleted while the draft is open");
    const doomedTid = await page.locator(".thread.active").getAttribute("data-tid");
    const doomed = page.locator(`.thread[data-tid="${doomedTid}"]`);
    await newDraft(page);
    await page.locator("#input").fill("Open here, sent there");

    const doomedRow = doomed.locator("xpath=..");
    await doomedRow.locator(".thread-menu-btn").click();
    await doomedRow.locator(".thread-menu .danger").click();
    await page.locator("#removeThreadConfirm").click();
    await expect(doomed).toHaveCount(0);
    await expect(page.locator("#input")).toHaveValue("Open here, sent there");

    const other = await context.newPage();
    const sendFromOtherTab = async () => {
      await other.goto("/");
      await expect(other.locator("#app")).toHaveClass(/open/);
      await other.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
      await expect(other.locator("#sendBtn")).toBeEnabled();
      await other.locator("#sendBtn").click();
      await expect(
        other.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
      ).toBeVisible();
    };
    await sendFromOtherTab();

    await thread.click();
    await newDraft(page);
    await expect(page.locator("#input")).toHaveValue("");

    // Again, but with an edit after the other tab's send: that text is kept.
    await page.locator("#input").fill("Open here, sent there, again");
    await sendFromOtherTab();
    await other.close();
    await page.locator("#input").fill("Edited after the send");
    await thread.click();
    await newDraft(page);
    await expect(page.locator("#input")).toHaveValue("Edited after the send");
  });

  // Until the draft's model settles, the capabilities in force are the project default harness's.
  // Here that harness has no Plan mode while the saved model's harness has one, so a saved Plan mode
  // must survive being shown as Build in the meantime. The catalog is mocked to add that second
  // harness; the start request is only inspected, since the server declares one harness.
  test("restores Plan mode on a harness that offers it when the default does not", async ({ page }) => {
    let catalogFetches = 0;
    await page.route(/\/api\/projects\/[^/?]+\/models$/, async (route) => {
      if (route.request().method() !== "GET") return route.fallback();
      catalogFetches += 1;
      const response = await route.fetch();
      const body = await response.json();
      const base = body.harnesses.find((h: { name: string }) => h.name === body.project_harness);
      body.harnesses = [
        { ...base, capabilities: { ...base.capabilities, plan_build_modes: false } },
        { ...base, name: "planner", capabilities: { ...base.capabilities, plan_build_modes: true } },
      ];
      body.models = [
        ...body.models,
        ...body.models.map((m: object) => ({ ...m, harness: "planner", is_default: false })),
      ];
      await route.fulfill({ response, json: body });
    });
    const pid = await demo(page).getAttribute("data-pid");
    await seedDemoDraft(page, {
      text: "Planned on the second harness",
      settings: { model: { harness: "planner", provider: "replay", model: "replay-model" }, mode: "plan" },
    });
    await page.evaluate((pid) =>
      localStorage.setItem("giskard.lastThread", JSON.stringify({ pid, draft: true })), pid);

    // A reload settles the model once the catalog arrives…
    await page.reload();
    await expect(page.locator("#app")).toHaveClass(/open/);
    await expect(page.locator("#input")).toHaveValue("Planned on the second harness");
    // The picker is enabled only once the catalog is in, so the label below is the settled one.
    await expect(modelButton(page)).toBeEnabled();
    await expect(turnLabel(page)).toHaveText("Plan · Ask first");

    // …and reopening settles it at once, from the catalog already loaded: no second fetch.
    const fetchesBefore = catalogFetches;
    await newDraft(page);
    await expect(modelButton(page)).toBeEnabled();
    await expect(turnLabel(page)).toHaveText("Plan · Ask first");
    expect(catalogFetches).toBe(fetchesBefore);

    let startBody: Record<string, unknown> | null = null;
    await page.route(/\/threads\/start$/, async (route) => {
      startBody = route.request().postDataJSON();
      await route.fulfill({ status: 503, json: { error: "inspected only" } });
    });
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#sendBtn").click();
    await expect.poll(() => startBody).not.toBeNull();
    expect(startBody).toMatchObject({ mode: "plan", harness: "planner" });
  });
});
