import { test, expect } from "@playwright/test";
import { SCRIPTED_REPLY, login } from "./helpers";

test.describe("projects and threads", () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test("lists the seeded demo project", async ({ page }) => {
    await expect(page.locator(".proj .project-name", { hasText: "Demo" })).toBeVisible();
  });

  test("starts a thread and streams the agent reply", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });

    // The per-project "+" (title "New thread") opens a draft thread and the composer.
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    await expect(input).toBeVisible();

    const message = "Please summarize the project.";
    await input.fill(message);
    await page.locator("#sendBtn").click();

    const transcript = page.locator("#transcript");

    // The user's message echoes into the transcript immediately.
    await expect(transcript.locator(".msg.user", { hasText: message })).toBeVisible();

    // The scripted harness streams a canned reply over the WebSocket; it lands in the transcript.
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

    // Starting the turn persisted a real thread, so a thread row now exists in the sidebar.
    await expect(page.locator(".thread").first()).toBeVisible();
  });

  test("starts an attachment-only thread with image bytes", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();
    const pngBase64 =
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";
    const png = Buffer.from(pngBase64, "base64");

    const chooserPromise = page.waitForEvent("filechooser");
    await page.locator("#attachBtn").click();
    const chooser = await chooserPromise;
    await chooser.setFiles({
      name: "diagram.png",
      mimeType: "application/octet-stream",
      buffer: png,
    });
    const attachmentOutcome = await page.waitForFunction(() => {
      if (document.querySelector(".attachment-chip")) return "attached";
      const error = document.querySelector("#notices .err");
      return error ? `error: ${error.textContent}` : false;
    });
    expect(await attachmentOutcome.jsonValue()).toBe("attached");
    await expect(page.locator(".attachment-chip", { hasText: "diagram.png" })).toBeVisible();

    const startRequest = page.waitForRequest((request) =>
      request.method() === "POST" && request.url().endsWith("/threads/start"));
    await page.locator("#sendBtn").click();
    const body = (await startRequest).postDataJSON();
    expect(body.text).toBe("");
    expect(body.attachments).toEqual([expect.objectContaining({
      name: "diagram.png",
      mime_type: "image/png",
      size: png.length,
      kind: "image",
      data_base64: pngBase64,
    })]);

    const transcript = page.locator("#transcript");
    await expect(transcript.locator(".msg.user", { hasText: "Attached: diagram.png" })).toBeVisible();
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

    await page.reload();
    await expect(transcript.locator(".msg.user", { hasText: "Attached: diagram.png" })).toBeVisible();
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  });

  test("treats declared images without an image signature as files", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();

    await page.locator("#attachmentInput").setInputFiles({
      name: "not-an-image.png",
      mimeType: "image/png",
      buffer: Buffer.from("%PDF-1.7"),
    });
    await expect(page.locator(".attachment-chip", { hasText: "not-an-image.png" })).toBeVisible();

    const startRequest = page.waitForRequest((request) =>
      request.method() === "POST" && request.url().endsWith("/threads/start"));
    await page.locator("#sendBtn").click();
    const body = (await startRequest).postDataJSON();
    expect(body.attachments).toEqual([expect.objectContaining({
      name: "not-an-image.png",
      mime_type: "application/octet-stream",
      kind: "file",
    })]);
  });

  test("rejects invalid names, empty files, and oversized files", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();

    const result = await page.evaluate(async () => {
      const overlongName = "x".repeat(256);
      await (window as any).attachFiles([
        new File(["x"], "bad\nname.txt", { type: "text/plain" }),
        new File(["x"], overlongName, { type: "text/plain" }),
        new File([], "empty.txt", { type: "text/plain" }),
        {
          name: "large.bin",
          type: "application/octet-stream",
          size: 25 * 1024 * 1024 + 1,
        },
      ]);
      return {
        overlongName,
        notices: Array.from(document.querySelectorAll("#notices .notice.err"))
          .map((el) => el.textContent || ""),
      };
    });

    expect(result.notices).toContain("bad\nname.txt has an invalid or overlong file name.");
    expect(result.notices).toContain(
      `${result.overlongName} has an invalid or overlong file name.`,
    );
    expect(result.notices).toContain("empty.txt is empty.");
    expect(result.notices).toContain("large.bin exceeds the 25 MB limit.");
    await expect(page.locator(".attachment-chip")).toHaveCount(0);
  });

  test("rejects attachment count overflow", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();

    await page.evaluate(async () => {
      const files = Array.from({ length: 9 }, (_, index) =>
        new File(["x"], `file-${index}.txt`, { type: "text/plain" }));
      await (window as any).attachFiles(files);
    });

    await expect(page.locator("#notices .notice.err", {
      hasText: "Attach at most 8 files per message.",
    })).toBeVisible();
    await expect(page.locator(".attachment-chip")).toHaveCount(0);
  });

  test("rejects aggregate attachment size overflow", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();

    await page.evaluate(async () => {
      const files = ["first.bin", "second.bin"].map((name) => ({
        name,
        type: "application/octet-stream",
        size: 13 * 1024 * 1024,
      }));
      await (window as any).attachFiles(files);
    });

    await expect(page.locator("#notices .notice.err", {
      hasText: "Attachments exceed the 25 MB total limit.",
    })).toBeVisible();
    await expect(page.locator(".attachment-chip")).toHaveCount(0);
  });

  test("reports base64 and header read failures without adding files", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();

    await page.evaluate(async () => {
      const originalReadAsDataURL = FileReader.prototype.readAsDataURL;
      FileReader.prototype.readAsDataURL = function() {
        queueMicrotask(() => this.dispatchEvent(new ProgressEvent("error")));
      };
      try {
        await (window as any).attachFiles([
          new File(["broken"], "base64.txt", { type: "text/plain" }),
        ]);
      } finally {
        FileReader.prototype.readAsDataURL = originalReadAsDataURL;
      }

      const headerFile = new File(["header"], "header.bin", {
        type: "application/octet-stream",
      });
      Object.defineProperty(headerFile, "slice", {
        value: () => ({
          arrayBuffer: () => Promise.reject(new Error("header read failed")),
        }),
      });
      await (window as any).attachFiles([headerFile]);
    });

    const notices = await page.locator("#notices .notice.err").allTextContents();
    expect(notices).toContain("Could not attach base64.txt: file read failed");
    expect(notices).toContain("Could not attach header.bin: header read failed");
    await expect(page.locator(".attachment-chip")).toHaveCount(0);
  });

  test("accepts dropped files and removes them from the composer", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    await expect(page.locator("#input")).toBeVisible();

    await page.locator("#composer").evaluate((composer) => {
      const transfer = new DataTransfer();
      transfer.items.add(new File(["pdf"], "notes.pdf", { type: "application/pdf" }));
      composer.dispatchEvent(new DragEvent("dragenter", {
        bubbles: true,
        dataTransfer: transfer,
      }));
      composer.dispatchEvent(new DragEvent("drop", {
        bubbles: true,
        dataTransfer: transfer,
      }));
    });

    await expect(page.locator(".attachment-chip", { hasText: "notes.pdf" })).toBeVisible();
    await page.getByRole("button", { name: "Remove notes.pdf" }).click();
    await expect(page.locator("#attachmentTray")).toBeHidden();
  });

  test("discards a file read after switching threads", async ({ page }) => {
    await page.evaluate(() => {
      const readAsDataURL = FileReader.prototype.readAsDataURL;
      FileReader.prototype.readAsDataURL = function(file: Blob) {
        setTimeout(() => readAsDataURL.call(this, file), 200);
      };
    });
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();
    const input = page.locator("#input");
    await expect(input).toBeVisible();
    const baselineMessage = "Baseline for stale attachment test";
    await input.fill(baselineMessage);
    await page.locator("#sendBtn").click();
    await expect(page.locator("#transcript .msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
    const baselineThreadId = await page.locator(".thread.active").getAttribute("data-tid");
    expect(baselineThreadId).toBeTruthy();

    await project.locator(".project-add").click();
    await expect(input).toBeVisible();

    await page.locator("#attachmentInput").setInputFiles({
      name: "slow.txt",
      mimeType: "text/plain",
      buffer: Buffer.from("slow upload"),
    });
    await expect(page.locator("#sendBtn")).toBeDisabled();
    await page.locator(`.thread[data-tid="${baselineThreadId}"]`).click();
    await page.waitForTimeout(300);

    await expect(page.locator("#attachmentTray")).toBeHidden();
    await expect(page.locator(".attachment-chip")).toHaveCount(0);
    await expect(page.locator("#notices .notice.err", {
      hasText: "Could not attach slow.txt",
    })).toHaveCount(0);
    await expect(page.locator("#sendBtn")).toBeEnabled();
    await expect(page.locator("#transcript .msg.user", { hasText: baselineMessage })).toBeVisible();
  });

  test("clears the composer and keeps the transcript after a turn", async ({ page }) => {
    const project = page.locator(".proj", { hasText: "Demo" });
    await project.locator(".project-add").click();

    const input = page.locator("#input");
    const message = "First message";
    await input.fill(message);
    await page.locator("#sendBtn").click();
    const transcript = page.locator("#transcript");
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();

    // The composer is emptied once the message is sent, and the composer itself stays available.
    await expect(input).toHaveValue("");
    await expect(input).toBeVisible();

    // Both sides of the exchange remain in the transcript.
    await expect(transcript.locator(".msg.user", { hasText: message })).toBeVisible();
    await expect(transcript.locator(".msg.agent", { hasText: SCRIPTED_REPLY })).toBeVisible();
  });
});
