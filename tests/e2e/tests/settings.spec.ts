import { test, expect } from "@playwright/test";
import { login } from "./helpers";

test.describe("settings", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("opens the settings menu and closes it", async ({ page }) => {
    await expect(page.locator("#settingsMenu")).toBeHidden();
    await page.locator("#settingsBtn").click();
    await expect(page.locator("#settingsMenu")).toBeVisible();
    await page.locator("#settingsClose").click();
    await expect(page.locator("#settingsMenu")).toBeHidden();
  });

  test("switching appearance updates the document theme attribute", async ({ page }) => {
    await page.locator("#settingsBtn").click();
    const appearance = page.locator("#appearanceSel");

    await appearance.selectOption("terminal");
    await expect(page.locator("html")).toHaveAttribute("data-appearance", "terminal");

    await appearance.selectOption("bubbles");
    await expect(page.locator("html")).toHaveAttribute("data-appearance", "bubbles");

    await appearance.selectOption("ide");
    await expect(page.locator("html")).toHaveAttribute("data-appearance", "ide");
  });
});
