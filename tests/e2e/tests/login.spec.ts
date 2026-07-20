import { test, expect } from "@playwright/test";
import { PASSWORD, login } from "./helpers";

test.describe("authentication", () => {
  test("shows the login form before authenticating", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveTitle("Giskard");
    await expect(page.locator("#login")).toBeVisible();
    await expect(page.locator("#pw")).toBeVisible();
    // The app shell stays hidden until login succeeds.
    await expect(page.locator("#app")).not.toHaveClass(/open/);
  });

  test("rejects a wrong password with a visible error", async ({ page }) => {
    await page.goto("/");
    await page.locator("#pw").fill("definitely-not-the-password");
    await page.getByRole("button", { name: "Log in" }).click();
    await expect(page.locator("#loginErr")).toHaveText("Wrong password.");
    await expect(page.locator("#app")).not.toHaveClass(/open/);
  });

  test("logs in with the correct password and reveals the app", async ({ page }) => {
    await login(page, PASSWORD);
    await expect(page.locator("#login")).toBeHidden();
    await expect(page.locator(".sidebar-title")).toHaveText("Projects");
  });
});
