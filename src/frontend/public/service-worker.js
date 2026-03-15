self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

const normalizePayload = (data = {}) => {
  const title = String(data?.title || "Nueva notificación").trim() || "Nueva notificación";
  const body = String(data?.body || "Tienes una nueva notificación en Moose+.").trim();
  const url = String(data?.url || "/notificaciones.html").trim() || "/notificaciones.html";
  const tag = String(data?.tag || "mooseplus-notification").trim() || "mooseplus-notification";
  const icon =
    String(data?.icon || "/assets/favicon/logo-corto-blanco-icono.png").trim() ||
    "/assets/favicon/logo-corto-blanco-icono.png";
  const badge =
    String(data?.badge || "/assets/favicon/logo-corto-blanco-icono.png").trim() ||
    "/assets/favicon/logo-corto-blanco-icono.png";
  return {
    title,
    options: {
      body,
      icon,
      badge,
      tag,
      renotify: false,
      data: {
        url,
      },
    },
  };
};

self.addEventListener("push", (event) => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (_err) {
    payload = {
      title: "Nueva notificación",
      body: event.data ? event.data.text() : "Tienes una nueva notificación en Moose+.",
    };
  }

  const { title, options } = normalizePayload(payload);
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const rawUrl = String(event.notification?.data?.url || "/notificaciones.html").trim();
  const targetUrl = new URL(rawUrl, self.location.origin).toString();

  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientsList) => {
      for (const client of clientsList) {
        if ("focus" in client) {
          const clientUrl = new URL(client.url, self.location.origin).toString();
          if (clientUrl === targetUrl) {
            return client.focus();
          }
        }
      }

      for (const client of clientsList) {
        if ("navigate" in client && "focus" in client) {
          return client.navigate(targetUrl).then(() => client.focus());
        }
      }

      if (self.clients.openWindow) {
        return self.clients.openWindow(targetUrl);
      }

      return undefined;
    }),
  );
});
