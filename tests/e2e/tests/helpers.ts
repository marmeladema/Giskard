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

/**
 * Prompt that spawns a linked child which raises an approval and then holds its turn open, while the
 * parent turn completes normally. Kept in sync with `SCRIPTED_SUBAGENT_APPROVAL_TRIGGER` and friends
 * in `crates/giskard-server/src/bin/giskard-server-replay.rs`.
 */
export const SCRIPTED_SUBAGENT_APPROVAL_TRIGGER = "Spawn a sub-agent that needs approval.";
export const SCRIPTED_SUBAGENT_APPROVAL_ID = "scripted-subagent-approval-1";
export const SCRIPTED_SUBAGENT_APPROVAL_COMMAND = "rm -rf ./child-build";
export const SCRIPTED_APPROVAL_SUBAGENT_NAME = "Approval child";

/** One entry recorded by the notification stub installed by {@link stubNotifications}. */
export type RecordedNotification = {
  title: string;
  body: string;
  tag: string;
  data: { threadId?: string; approvalId?: string } | null;
};

declare global {
  interface Window {
    __giskardNotifications?: RecordedNotification[];
  }
}

/**
 * Replace both notification delivery paths (the `Notification` constructor and the service-worker
 * registration's `showNotification`) with recorders, and pre-grant permission. Headless Chromium
 * gives no way to read a real notification back, and the app picks its path at runtime, so both are
 * stubbed into one array. Must be called before the page navigates.
 */
export async function stubNotifications(page: Page): Promise<void> {
  await page.addInitScript(() => {
    const calls: RecordedNotification[] = [];
    (window as Window).__giskardNotifications = calls;
    const record = (title: string, options?: NotificationOptions) => {
      calls.push({
        title,
        body: (options && options.body) || "",
        tag: (options && options.tag) || "",
        data: (options && (options.data as RecordedNotification["data"])) || null,
      });
    };
    class StubNotification {
      static permission = "granted";
      static requestPermission() { return Promise.resolve("granted"); }
      onclick: (() => void) | null = null;
      onshow: (() => void) | null = null;
      onerror: (() => void) | null = null;
      onclose: (() => void) | null = null;
      constructor(title: string, options?: NotificationOptions) { record(title, options); }
      close() {}
    }
    Object.defineProperty(window, "Notification", {
      configurable: true, writable: true, value: StubNotification,
    });
    const registration = {
      active: true,
      scope: "/",
      showNotification: (title: string, options?: NotificationOptions) => {
        record(title, options);
        return Promise.resolve();
      },
      getNotifications: () => Promise.resolve([]),
    };
    Object.defineProperty(navigator, "serviceWorker", {
      configurable: true,
      value: {
        ready: Promise.resolve(registration),
        register: () => Promise.resolve(registration),
        addEventListener: () => {},
        controller: null,
      },
    });
  });
}

/** The notifications recorded so far, in order. */
export function recordedNotifications(page: Page): Promise<RecordedNotification[]> {
  return page.evaluate(() => (window as Window).__giskardNotifications ?? []);
}

/** The browser's currently selected `{ pid, tid }`, as persisted for reload restore. */
export function selectedThread(page: Page): Promise<{ pid: string; tid: string } | null> {
  return page.evaluate(() => JSON.parse(localStorage.getItem("giskard.lastThread") || "null"));
}

/** Log in through the real login form and wait for the app shell to become visible. */
export async function login(page: Page, password: string = PASSWORD): Promise<void> {
  await page.goto("/");
  await expect(page.locator("#login")).toBeVisible();
  await page.locator("#pw").fill(password);
  await page.getByRole("button", { name: "Log in" }).click();
  // The shell only gets the `open` class after `/api/login` succeeds and startApp() runs.
  await expect(page.locator("#app")).toHaveClass(/open/);
}
