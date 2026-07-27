import { test, expect, type Page } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

// Clicking "+" used to fetch the project before switching to the draft. For the length of that
// round-trip the *previous* thread stayed on screen with its composer visible and editable, so
// anything typed landed in the old composer and was destroyed when the draft finally opened and
// reset it. The Send that followed found an empty box and returned silently: the click read as
// "nothing happened", and the message was gone with no error.
//
// This surfaced as a rare flake in the suite — a test would type into a composer that was about to
// be wiped — but it is a real way to lose a message, and it gets more likely the slower the server.
//
// The draft now opens immediately and the project's default model is applied when it arrives. That
// makes the draft interactive *while* the fetch is in flight, so these tests hold the fetch open to
// make the window wide and deterministic, and each waits for the response to be delivered before
// asserting — otherwise they would pass on state captured before the deferred callback ever ran.
test.describe("draft composer", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  /** Hold the single-project GET (the one `newThread` used to await) until the caller releases it. */
  async function holdProjectFetch(page: Page): Promise<() => void> {
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    await page.route(/\/api\/projects\/[^/?]+$/, async (route) => {
      if (route.request().method() !== "GET") return route.fallback();
      await held;
      return route.fallback();
    });
    return () => release();
  }

  function projectFetched(page: Page) {
    return page.waitForResponse(
      (r) =>
        r.request().method() === "GET" &&
        /\/api\/projects\/[^/?]+$/.test(new URL(r.url()).pathname),
    );
  }

  const modelButton = (page: Page) => page.locator("#modelPickerBtn");

  test("keeps a message typed while the new thread is still opening", async ({ page }) => {
    const message = "Typed before the draft finished opening";
    const release = await holdProjectFetch(page);
    const fetched = projectFetched(page);

    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

    // The draft must be usable straight away, without waiting for the project fetch.
    const input = page.locator("#input");
    await expect(input).toBeVisible();
    await expect(page.locator("#transcript")).toContainText("Start a new thread");
    await input.fill(message);

    // Let the fetch land. Under the old ordering this is the moment the composer was reset, so the
    // assertions below have to come after the response has actually been delivered.
    release();
    await fetched;
    // The project's default model arriving is what proves the deferred callback ran at all.
    await expect(modelButton(page)).toContainText("Replay Model");
    await expect(input).toHaveValue(message);

    // And the message actually sends, rather than the click being a silent no-op.
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
  });

  // A draft opens before its model is known, so `state.currentModel` is null rather than a
  // stand-in until the project's default arrives. Send stays unavailable for that window: starting
  // the first turn on a fallback would bind the thread to a provider the project never chose, and a
  // started thread cannot be switched across providers, so it would not be recoverable.
  test("holds the first send until the project's model has resolved", async ({ page }) => {
    const release = await holdProjectFetch(page);
    const fetched = projectFetched(page);
    const started = page.waitForRequest(
      (r) => r.method() === "POST" && r.url().endsWith("/threads/start"),
    );

    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();
    await page.locator("#input").fill("Sent once the project model resolved");

    // Composing is fine; committing is not, and the UI says which state it is in.
    await expect(modelButton(page)).toContainText("Loading model");
    await expect(page.locator("#sendBtn")).toBeDisabled();

    release();
    await fetched;
    await expect(modelButton(page)).toContainText("Replay Model");
    await expect(page.locator("#sendBtn")).toBeEnabled();

    await page.locator("#sendBtn").click();
    const body = (await started).postDataJSON();
    expect(body.model_ref).toMatchObject({ provider: "replay", model: "replay-model" });
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
  });

  // A choice made while the default is still in flight is explicit, and the later-arriving default
  // must not quietly replace it — the turn would then run on settings the user did not pick. The
  // project's stored default carries no reasoning effort, so an unguarded overwrite resets a chosen
  // one to "Default"; picking an effort also resolves the draft, so it becomes sendable at once.
  test("keeps a reasoning effort chosen while the project's model was still loading", async ({ page }) => {
    const release = await holdProjectFetch(page);
    const fetched = projectFetched(page);
    const started = page.waitForRequest(
      (r) => r.method() === "POST" && r.url().endsWith("/threads/start"),
    );

    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();
    await expect(page.locator("#sendBtn")).toBeDisabled();

    // The catalog is a separate fetch and has already landed, so the controls are usable here.
    await modelButton(page).click();
    await expect(page.locator("#effortSel")).toBeVisible();
    await page.locator("#effortSel").selectOption("high");
    await expect(modelButton(page)).toContainText("High");
    // An explicit choice resolves the draft, so it is sendable without waiting for the default.
    await expect(page.locator("#sendBtn")).toBeEnabled();

    release();
    await fetched;
    // The default has had its chance to land. The explicit choice must still be in effect.
    await expect(modelButton(page)).toContainText("High");

    await page.keyboard.press("Escape");   // the picker popover overlays the composer
    await page.locator("#input").fill("Sent on an explicitly chosen effort");
    await page.locator("#sendBtn").click();
    const body = (await started).postDataJSON();
    expect(body.model_ref).toMatchObject({
      provider: "replay",
      model: "replay-model",
      reasoning_effort: "high",
    });
  });

  // If the model never resolves there is nothing authoritative to start on, so the draft stays
  // uncommittable rather than falling back. The keyboard path is checked too: Enter reaches
  // `sendInput` directly and cannot be gated by the button's disabled state.
  test("keeps the first send unavailable when the project's model never resolved", async ({ page }) => {
    await page.route(/\/api\/projects\/[^/?]+$/, async (route) => {
      if (route.request().method() !== "GET") return route.fallback();
      return route.abort();
    });
    let started = false;
    page.on("request", (r) => {
      if (r.method() === "POST" && r.url().endsWith("/threads/start")) started = true;
    });

    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();
    await page.locator("#input").fill("Typed while the project fetch was failing");

    await expect(modelButton(page)).toContainText("Model unavailable");
    await expect(page.locator("#sendBtn")).toBeDisabled();

    // Enter bypasses the button, so it has to be refused on its own.
    await page.locator("#input").press("Enter");
    await expect(page.locator("#notices")).toContainText("Cannot start a thread here");

    expect(started, "no turn may be started without a project model").toBe(false);
    // And the text is still there to send once a model is picked.
    await expect(page.locator("#input")).toHaveValue("Typed while the project fetch was failing");
  });

  // The model catalog is a separate fetch from the project's default model. If it fails the draft is
  // still committable — the default is what `threads/start` carries — but the picker is left with no
  // options, so the failure has to be visible rather than presenting as an empty list.
  test("reports a failed model catalog and still sends on the project's default", async ({ page }) => {
    await page.route(/\/api\/projects\/[^/?]+\/models$/, (route) =>
      route.request().method() === "GET" ? route.abort() : route.fallback(),
    );
    const started = page.waitForRequest(
      (r) => r.method() === "POST" && r.url().endsWith("/threads/start"),
    );

    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();
    await expect(page.locator("#notices")).toContainText("Could not load this project's models");

    // The default model still resolved, so the draft is sendable and goes out on that model. With
    // no catalog there is no descriptor to supply a display name, so the picker shows the raw id.
    await expect(modelButton(page)).toContainText("replay-model");
    await page.locator("#input").fill("Sent with an unavailable model catalog");
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#sendBtn").click();

    const body = (await started).postDataJSON();
    expect(body.model_ref).toMatchObject({ provider: "replay", model: "replay-model" });
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
  });

  // Two successive drafts in the same project share the composer draft key `draft:<pid>`, so the key
  // alone cannot tell them apart. A slow `threads/start` for the first draft must not come back and
  // act on the second: clearing its composer, opening over it, or failing its rows would lose text
  // the user typed after the send. The send is identified by the draft object it was issued for.
  test("a slow first send does not clobber a newer draft in the same project", async ({ page }) => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    await page.route(/\/threads\/start$/, async (route) => {
      if (route.request().method() !== "POST") return route.fallback();
      await held;
      return route.fallback();
    });

    const newDraft = () => page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

    // Draft A: send it, and leave the POST hanging.
    await newDraft();
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#input").fill("Draft A, sent first");
    await page.locator("#sendBtn").click();
    await expect(page.locator("#transcript .msg.user")).toBeVisible();

    // Draft B: opened and typed into while A's request is still in flight.
    await newDraft();
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#input").fill("Draft B, typed while A was in flight");

    const startedResponse = page.waitForResponse(
      (r) => r.request().method() === "POST" && r.url().endsWith("/threads/start"),
    );
    const threadsBefore = await page.locator(".thread").count();
    release();
    await startedResponse;
    // A's thread is created and its row appears — that is how we know the continuation actually
    // ran, rather than the assertions below passing because nothing had happened yet.
    await expect(page.locator(".thread")).toHaveCount(threadsBefore + 1);

    // B is untouched: still a draft, still holding its text, and not opened over by A's thread.
    await expect(page.locator("#input")).toHaveValue("Draft B, typed while A was in flight");
    await expect(page.locator("#transcript")).toContainText("Start a new thread");
    await expect(page.locator(".thread.active")).toHaveCount(0);
  });
});
