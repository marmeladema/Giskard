import { type Page, expect } from "@playwright/test";

/** The app password the replay server is configured with (see playwright.config.ts). */
export const PASSWORD = process.env.GISKARD_REPLAY_PASSWORD ?? "giskard";

/**
 * The exact reply the scripted replay harness streams on every turn. Kept in sync with
 * `SCRIPTED_REPLY` in `crates/giskard-server/src/bin/giskard-server-replay.rs`.
 */
export const SCRIPTED_REPLY = "Hello from the scripted replay harness!";
export const SCRIPTED_SUBAGENT_TRIGGER = "Spawn the scripted linked sub-agent.";
export const SCRIPTED_NESTED_SUBAGENT_TRIGGER = "Spawn a scripted nested sub-agent.";
export const SCRIPTED_SUBAGENT_PROMPT = "Review the linked child task.";
export const SCRIPTED_SUBAGENT_REPLY = "Child replay output";

/**
 * Prompt that makes the scripted replay harness raise an approval request and then hold the turn
 * open (no turn-completed). Kept in sync with `SCRIPTED_APPROVAL_TRIGGER` in
 * `crates/giskard-server/src/bin/giskard-server-replay.rs`.
 */
export const SCRIPTED_APPROVAL_TRIGGER = "Trigger a scripted approval request.";

/** Log in through the real login form and wait for the app shell to become visible. */
export async function login(page: Page, password: string = PASSWORD): Promise<void> {
  await page.goto("/");
  await expect(page.locator("#login")).toBeVisible();
  await page.locator("#pw").fill(password);
  await page.getByRole("button", { name: "Log in" }).click();
  // The shell only gets the `open` class after `/api/login` succeeds and startApp() runs.
  await expect(page.locator("#app")).toHaveClass(/open/);
}
