import { test, expect } from "@playwright/test";
import {
  SCRIPTED_STEERING_REPLY,
  SCRIPTED_STEERING_TRIGGER,
  login,
} from "./helpers";

test("steers an acknowledged active turn with text", async ({ page }) => {
  await login(page);
  await page.locator(".proj", { hasText: "Demo" }).locator(".project-add").click();

  const input = page.locator("#input");
  const send = page.locator("#sendBtn");
  const stop = page.locator("#stopBtn");
  const attach = page.locator("#attachBtn");
  const transcript = page.locator("#transcript");

  await input.fill(SCRIPTED_STEERING_TRIGGER);
  await send.click();
  await expect(transcript.locator(".msg.user", { hasText: SCRIPTED_STEERING_TRIGGER }))
    .toBeVisible();

  // The scripted turn has acknowledged its start but deliberately emits no reply or completion
  // until it receives steering. Empty active composers show only Stop, and attachments stay off.
  await expect(stop).toBeVisible();
  await expect(send).toBeHidden();
  await expect(attach).toBeDisabled();

  const steeringText = "Focus the answer on the browser-visible behavior.";
  await input.fill(steeringText);
  await expect(send).toBeVisible();
  await expect(send).toBeEnabled();
  await expect(stop).toBeVisible();
  await send.click();

  // The harness echoes the accepted steer as a real UserMessage on the original turn, responds,
  // and completes that turn exactly once. The UI reconciles the optimistic bubble with the echo.
  await expect(transcript.locator(".msg.user", { hasText: steeringText })).toHaveCount(1);
  await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_STEERING_REPLY }))
    .toBeVisible();
  await expect(stop).toBeHidden();
  await expect(send).toBeVisible();
  await expect(input).toHaveValue("");
});
