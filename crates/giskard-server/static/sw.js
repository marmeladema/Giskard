// Giskard service worker.
//
// Its only job is notifications. On Chrome for Android `new Notification(...)` is an illegal
// constructor — notifications must be shown through `ServiceWorkerRegistration.showNotification()`,
// and their clicks are delivered here rather than to an `onclick` handler on the page. This worker
// activates immediately, then on a notification click focuses an existing Giskard tab (opening one
// if none is around) and forwards the notification's `data` so the page can jump to the relevant
// approval.

self.addEventListener("install", () => self.skipWaiting());

self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("notificationclick", (event) => {
  const notification = event.notification;
  notification.close();
  const data = notification.data || {};
  event.waitUntil(
    (async () => {
      const clients = await self.clients.matchAll({
        type: "window",
        includeUncontrolled: true,
      });
      let client = clients.find((c) => "focus" in c) || null;
      if (client) {
        try {
          await client.focus();
        } catch {}
      } else if (self.clients.openWindow) {
        client = await self.clients.openWindow("/");
      }
      if (client && "postMessage" in client) {
        client.postMessage({ type: "giskard-notification-click", notification: data });
      }
    })(),
  );
});
