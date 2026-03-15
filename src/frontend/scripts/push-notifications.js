import { API_BASE, ensureServerSession } from "./api.js";
import { requireSession } from "./session.js";

const WEB_PUSH_SUPPORT =
  typeof window !== "undefined" &&
  "serviceWorker" in navigator &&
  "PushManager" in window &&
  "Notification" in window;

let webPushPublicKeyPromise = null;
let serviceWorkerRegistrationPromise = null;

const getDeviceLabel = () => {
  const platform =
    String(navigator.userAgentData?.platform || navigator.platform || "").trim() || "Navegador";
  const family = /mobile/i.test(navigator.userAgent || "") ? "movil" : "escritorio";
  return `${platform} (${family})`;
};

const urlBase64ToUint8Array = (base64String) => {
  const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
  const normalized = `${base64String}${padding}`.replace(/-/g, "+").replace(/_/g, "/");
  const rawData = window.atob(normalized);
  const outputArray = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; i += 1) {
    outputArray[i] = rawData.charCodeAt(i);
  }
  return outputArray;
};

const parseJsonResponse = async (res) => {
  const data = await res.json().catch(() => ({}));
  if (res.ok) return data;
  throw new Error(data?.error || "No se pudo completar la operación de web push.");
};

const ensureLoggedSession = async () => {
  if (!requireSession()) {
    throw new Error("Necesitas iniciar sesión para configurar notificaciones.");
  }
  await ensureServerSession();
};

const getServiceWorkerRegistration = async () => {
  if (!WEB_PUSH_SUPPORT) {
    throw new Error("Este navegador no soporta notificaciones push.");
  }
  if (!serviceWorkerRegistrationPromise) {
    const url = new URL("/service-worker.js", window.location.origin).toString();
    serviceWorkerRegistrationPromise = navigator.serviceWorker.register(url, { scope: "/" });
  }
  return serviceWorkerRegistrationPromise;
};

const loadWebPushPublicKey = async () => {
  if (!webPushPublicKeyPromise) {
    webPushPublicKeyPromise = (async () => {
      await ensureLoggedSession();
      const res = await fetch(`${API_BASE}/api/web-push/public-key`, {
        credentials: "include",
      });
      return parseJsonResponse(res);
    })().catch((err) => {
      webPushPublicKeyPromise = null;
      throw err;
    });
  }
  return webPushPublicKeyPromise;
};

const saveSubscriptionOnServer = async (subscription) => {
  await ensureLoggedSession();
  const payload =
    typeof subscription?.toJSON === "function" ? subscription.toJSON() : subscription || null;
  const res = await fetch(`${API_BASE}/api/web-push/subscribe`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
    body: JSON.stringify({
      subscription: payload,
      device_label: getDeviceLabel(),
      user_agent: navigator.userAgent || "",
    }),
  });
  return parseJsonResponse(res);
};

const removeSubscriptionOnServer = async (endpoint = "") => {
  await ensureLoggedSession();
  const res = await fetch(`${API_BASE}/api/web-push/subscribe`, {
    method: "DELETE",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
    body: JSON.stringify({ endpoint }),
  });
  return parseJsonResponse(res);
};

const getCurrentPushSubscription = async () => {
  if (!WEB_PUSH_SUPPORT) return null;
  const registration = await getServiceWorkerRegistration();
  return registration.pushManager.getSubscription();
};

export const isWebPushSupported = () => WEB_PUSH_SUPPORT;

export const syncWebPushSubscription = async () => {
  if (!WEB_PUSH_SUPPORT || !requireSession() || Notification.permission !== "granted") {
    return { ok: false, reason: "not-applicable" };
  }
  const subscription = await getCurrentPushSubscription();
  if (!subscription) {
    return { ok: false, reason: "no-subscription" };
  }
  const data = await saveSubscriptionOnServer(subscription);
  return { ok: true, subscription, ...data };
};

export const getWebPushState = async () => {
  const state = {
    supported: WEB_PUSH_SUPPORT,
    permission: WEB_PUSH_SUPPORT ? Notification.permission : "unsupported",
    currentDeviceSubscribed: false,
    currentEndpoint: "",
    backendEnabled: false,
    backendConfigured: false,
    deviceCount: 0,
    tableMissing: false,
  };

  if (WEB_PUSH_SUPPORT) {
    try {
      const subscription = await getCurrentPushSubscription();
      state.currentDeviceSubscribed = Boolean(subscription?.endpoint);
      state.currentEndpoint = subscription?.endpoint || "";
    } catch (_err) {
      // noop
    }
  }

  if (!requireSession()) return state;

  try {
    await ensureLoggedSession();
    const res = await fetch(`${API_BASE}/api/web-push/status`, {
      credentials: "include",
    });
    const data = await parseJsonResponse(res);
    state.backendEnabled = data?.enabled === true;
    state.backendConfigured = data?.configured === true;
    state.deviceCount = Number(data?.deviceCount || 0);
    state.tableMissing = data?.tableMissing === true;
  } catch (_err) {
    // noop
  }

  return state;
};

export const enablePushOnCurrentDevice = async () => {
  if (!WEB_PUSH_SUPPORT) {
    throw new Error("Este navegador no soporta notificaciones push.");
  }

  await ensureLoggedSession();

  let permission = Notification.permission;
  if (permission !== "granted") {
    permission = await Notification.requestPermission();
  }
  if (permission !== "granted") {
    throw new Error("No se concedió el permiso para mostrar notificaciones.");
  }

  const keyData = await loadWebPushPublicKey();
  if (!keyData?.enabled || !keyData?.publicKey) {
    throw new Error("El backend aún no tiene configurado web push.");
  }

  const registration = await getServiceWorkerRegistration();
  let subscription = await registration.pushManager.getSubscription();
  if (!subscription) {
    subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(keyData.publicKey),
    });
  }

  const data = await saveSubscriptionOnServer(subscription);
  return { ok: true, subscription, ...data };
};

export const disablePushOnCurrentDevice = async () => {
  if (!WEB_PUSH_SUPPORT) {
    return { ok: true, unsupported: true };
  }

  const subscription = await getCurrentPushSubscription();
  if (subscription?.endpoint && requireSession()) {
    try {
      await removeSubscriptionOnServer(subscription.endpoint);
    } catch (_err) {
      // noop: igual desuscribimos localmente
    }
  }

  if (subscription) {
    await subscription.unsubscribe().catch(() => {});
  }

  return { ok: true };
};

export const sendTestWebPush = async () => {
  await ensureLoggedSession();
  const res = await fetch(`${API_BASE}/api/web-push/test`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
    body: JSON.stringify({}),
  });
  return parseJsonResponse(res);
};

const autoSyncGrantedSubscription = async () => {
  if (!WEB_PUSH_SUPPORT || !requireSession() || Notification.permission !== "granted") return;
  try {
    await syncWebPushSubscription();
  } catch (_err) {
    // noop
  }
};

if (typeof window !== "undefined") {
  window.addEventListener("load", () => {
    autoSyncGrantedSubscription();
  });
}
