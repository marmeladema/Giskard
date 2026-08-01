import { test, expect, type Page } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

/**
 * Hold matching requests until the returned `release` is called, so a window that is normally a few
 * milliseconds becomes wide and deterministic instead of a race the test has to win.
 */
async function holdRoute(page: Page, url: RegExp, method: string): Promise<() => void> {
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  await page.route(url, async (route) => {
    if (route.request().method() !== method) return route.fallback();
    await held;
    return route.fallback();
  });
  return () => release();
}

/** `GET /api/projects/{id}` — the project record, not its `/models` catalog. */
const PROJECT_GET = /\/api\/projects\/[^/?]+$/;

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

  /** The single-project GET that `newThread` used to await before opening the draft. */
  const holdProjectFetch = (page: Page) => holdRoute(page, PROJECT_GET, "GET");

  function projectFetched(page: Page) {
    return page.waitForResponse(
      (r) => r.request().method() === "GET" && PROJECT_GET.test(new URL(r.url()).pathname),
    );
  }

  const modelButton = (page: Page) => page.locator("#modelPickerBtn");

  test("highlights the project that owns the current draft", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    const projectName = project.locator(".project-name");

    await project.locator(".project-add").click();

    await expect(page.locator("#transcript")).toContainText("Start a new thread");
    await expect(projectName).toHaveClass(/\bactive\b/);
    await expect(projectName).toHaveAttribute("aria-current", "true");
    await expect(page.locator(".thread.active")).toHaveCount(0);

    const shadow = await projectName.evaluate((el) => getComputedStyle(el).boxShadow);
    expect(shadow).not.toBe("none");

    await page.locator("#input").fill("Create the highlighted draft's thread");
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();

    await expect(projectName).not.toHaveClass(/\bactive\b/);
    await expect(page.locator(".thread.active")).toHaveCount(1);
  });

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
    // Fill first, so the send-availability assertions below turn only on the model resolving and
    // not on whether there is anything to send.
    await page.locator("#input").fill("Sent on an explicitly chosen effort");
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
    await page.route(PROJECT_GET, async (route) =>
      route.request().method() === "GET" ? route.abort() : route.fallback(),
    );
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
    const release = await holdRoute(page, /\/threads\/start$/, "POST");

    const newDraft = () => page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

    // Draft A: send it, and leave the POST hanging.
    await newDraft();
    await page.locator("#input").fill("Draft A, sent first");
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#sendBtn").click();
    await expect(page.locator("#transcript .msg.user")).toBeVisible();

    // Draft B: opened and typed into while A's request is still in flight.
    await newDraft();
    await page.locator("#input").fill("Draft B, typed while A was in flight");
    await expect(page.locator("#sendBtn")).toBeEnabled();

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

  // A draft's first turn is started over HTTP, before the thread has a socket, so the composer
  // locks with nothing on the wire yet to confirm the turn. Against a fast harness that turn is
  // usually over before the socket attaches: its `turn_completed` was addressed to nobody, and a
  // finished turn leaves no live snapshot to follow, so nothing ever contradicted the optimistic
  // lock. The composer stayed disabled — Stop showing, Send gone — with the agent's reply sitting
  // right there on screen, until the thread was re-opened from the sidebar.
  test("unlocks the composer when the first turn ends before the socket attaches", async ({ page }) => {
    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    await page.locator("#input").fill("First turn of a brand new thread");
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#sendBtn").click();

    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();

    // No re-open, no reload: this same view has to come back to life on its own.
    await expect(page.locator("#stopBtn")).toBeHidden();
    await expect(page.locator("#sendBtn")).toBeVisible();

    // And the thread is genuinely usable, not merely repainted: a second turn sends over the
    // socket that is now attached, and answers.
    await page.locator("#input").fill("Second turn, same thread");
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await page.locator("#sendBtn").click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toHaveCount(2);
  });
});

// `openThread` has the same save/await/restore shape as the draft opening that lost a message, and
// reads like the same defect. It is not: `saveComposerDraft` is bound to the composer's `input`
// event, so every keystroke is filed under the key derived from `state.threadId`, which still names
// the thread being left until the switch completes. Text typed mid-switch stays with that thread
// and comes back on returning to it — it is not carried into the arriving thread, and not lost.
//
// Pinned because the shape invites a "fix" that would carry it across, which is not where it was
// written.
test.describe("composer across a thread switch", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("files text typed mid-switch against the thread it was typed in", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });

    // Two threads to switch between.
    const create = async (message: string) => {
      await project.locator(".project-add").click();
      await page.locator("#input").fill(message);
      await expect(page.locator("#sendBtn")).toBeEnabled();
      await page.locator("#sendBtn").click();
      await expect(
        page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
      ).toBeVisible();
      const tid = await page.locator(".thread.active").getAttribute("data-tid");
      expect(tid).toBeTruthy();
      return tid as string;
    };
    const first = await create("First thread for the switch test");
    const second = await create("Second thread for the switch test");

    // Hold the open request. `/threads` opens an existing thread; `/threads/start` creates one, and
    // the anchored pattern keeps the two apart.
    const release = await holdRoute(page, /\/api\/projects\/[^/?]+\/threads$/, "POST");

    const typed = "Typed while the other thread was opening";
    await page.locator(`.thread[data-tid="${first}"]`).click();
    await page.locator("#input").fill(typed);
    release();
    await expect(page.locator(`.thread[data-tid="${first}"]`)).toHaveClass(/\bactive\b/);

    // The composer now belongs to the thread that was opened, and the text was not carried across.
    // `openThread` sets the highlight before restoring the composer, so the assertion above does not
    // by itself mean the restore has run — this one retries until it has, and would time out if the
    // text had been carried over. It was filed against the thread it was typed in, and comes back on
    // returning there: not lost, just where it was written.
    await expect(page.locator("#input")).toHaveValue("");
    await page.locator(`.thread[data-tid="${second}"]`).click();
    await expect(page.locator(`.thread[data-tid="${second}"]`)).toHaveClass(/\bactive\b/);
    await expect(page.locator("#input")).toHaveValue(typed);
    // Restored text is text to send: coming back must not leave the button reading empty.
    await expect(page.locator("#sendBtn")).toBeEnabled();
  });
});

// Send used to be enabled with nothing to send, and `sendInput` returned early without a word. A
// composer that was unexpectedly empty — as one used to be when a draft opened mid-typing — then
// read as a dead button: the click did nothing and nothing said why. The emptiness is now on the
// button itself.
test.describe("composer send availability", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("offers Send only when there is something to send", async ({ page }) => {
    let started = false;
    page.on("request", (r) => {
      if (r.method() === "POST" && r.url().endsWith("/threads/start")) started = true;
    });

    await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();
    const input = page.locator("#input");
    const send = page.locator("#sendBtn");
    await expect(input).toBeVisible();

    // Nothing typed: unavailable, and the button says why rather than swallowing the click.
    await expect(send).toBeDisabled();
    await expect(send).toHaveAttribute("title", /Type a message/);

    // The button cannot be clicked while disabled, so Enter is the only way to reach `sendInput`
    // with an empty composer — the path that keeps its own silent guard. Without this the
    // no-request assertion at the end would hold vacuously.
    await input.press("Enter");
    await expect(input).toHaveValue("");
    await expect(send).toBeDisabled();

    await input.fill("Something to send");
    await expect(send).toBeEnabled();
    await expect(send).toHaveAttribute("title", "Send");

    // Emptying it again takes the offer away — this is the state that used to look like a dead
    // button, and the one a future regression would land in.
    await input.fill("");
    await expect(send).toBeDisabled();

    // Whitespace is not a message.
    await input.fill("   ");
    await expect(send).toBeDisabled();

    // An attachment alone is enough, with no text at all.
    await input.fill("");
    await page.locator("#attachmentInput").setInputFiles({
      name: "note.txt",
      mimeType: "text/plain",
      buffer: Buffer.from("attached without a message"),
    });
    await expect(page.locator(".attachment-chip")).toHaveCount(1);
    await expect(send).toBeEnabled();

    // Nothing was routed to the server while the composer was empty.
    expect(started, "an empty composer must not start a turn").toBe(false);
  });

  // Now that Send tracks emptiness, every path that puts text back in the composer has to refresh
  // it. Restoring a draft is the one that does not go through the input event.
  test("offers Send again when a draft's text comes back", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    const input = page.locator("#input");
    const send = page.locator("#sendBtn");

    // An existing thread to leave the draft for.
    await project.locator(".project-add").click();
    await input.fill("A thread to switch to");
    await send.click();
    await expect(
      page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY }),
    ).toBeVisible();
    const tid = await page.locator(".thread.active").getAttribute("data-tid");

    const text = "Draft text left behind";
    await project.locator(".project-add").click();
    await input.fill(text);
    await expect(send).toBeEnabled();

    await page.locator(`.thread[data-tid="${tid}"]`).click();
    await expect(page.locator(`.thread[data-tid="${tid}"]`)).toHaveClass(/\bactive\b/);
    await expect(input).toHaveValue("");

    await project.locator(".project-add").click();
    await expect(input).toHaveValue(text);
    await expect(send).toBeEnabled();
    await expect(send).toHaveAttribute("title", "Send");
  });
});
