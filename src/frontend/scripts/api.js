import { createClient } from "./vendor/supabase-global.js";
import { clearSession, requireSession } from "./session.js";

const supabase = createClient(
  "https://ojigtjcwhcrnawdbtqkl.supabase.co",
  "sb_publishable_pUhdf8wgEJyUtUg6TZqcTA_qF9gwEjJ"
);

const normalizeApiBase = (value) => {
  if (!value) return "";
  const raw = String(value).trim().replace(/\/+$/, "");
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  return "";
};

const canUseApiBaseOverride = () => {
  if (typeof window === "undefined") return false;
  const host = String(window.location.hostname || "").trim().toLowerCase();
  return host === "localhost" || host === "127.0.0.1";
};

const readApiBaseOverride = () => {
  if (!canUseApiBaseOverride()) return "";
  if (typeof window === "undefined") return "";
  try {
    const query = new URLSearchParams(window.location.search || "").get("api_base");
    const fromQuery = normalizeApiBase(query);
    if (fromQuery) {
      window.localStorage.setItem("api_base", fromQuery);
      return fromQuery;
    }
  } catch (_err) {
    // noop
  }
  try {
    return normalizeApiBase(window.localStorage.getItem("api_base"));
  } catch (_err) {
    return "";
  }
};

// En local (ej. :5500) usa backend en :3000 con el mismo hostname.
const API_BASE = (() => {
  if (typeof window === "undefined") return "http://localhost:3000";
  const override = readApiBaseOverride();
  if (override) return override;
  const { protocol, hostname, host, port } = window.location;
  if (!host) return "http://localhost:3000";
  const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost";
  if (isLocalHost && port !== "3000") return `http://${hostname}:3000`;
  return `${protocol}//${host}`;
})();

let authRecoveryInProgress = false;
let clientErrorReporterInit = false;
let traficoWebTrackerInit = false;
let ensureServerSessionInFlight = null;

const CLIENT_ERROR_MAX_REPORTS_PER_PAGE = 40;
const CLIENT_ERROR_DEDUP_WINDOW_MS = 15000;
let clientErrorReportsSent = 0;
const clientErrorLastSentByKey = new Map();
const TRAFICO_WEB_SESSION_STORAGE_KEY = "trafico_web_sesion_v1";
const TRAFICO_WEB_SESSION_IDLE_MS = 30 * 60 * 1000;
const API_DEBUG_PREFIX = "[api-debug]";
const AUTH_ACCESS_TOKEN_BRIDGE_KEY = "auth_access_token_bridge_v1";
const AUTH_ACCESS_TOKEN_BRIDGE_TTL_MS = 5 * 60 * 1000;
const AUTH_SESSION_FAILURE_COOLDOWN_MS = 12000;
let cachedAuthAccessToken = "";
let cachedAuthAccessTokenStoredAt = 0;
let authStateBridgeInit = false;
let authSessionFailureCooldownUntil = 0;
let authSessionFailureMessage = "";
const orderProcessInFlightById = new Map();

const trimText = (value, max = 2000) => {
  const txt = String(value ?? "");
  return txt.length > max ? `${txt.slice(0, max)}…` : txt;
};

const getDebugNow = () =>
  typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();

const getElapsedMs = (startedAt) => Math.round(getDebugNow() - startedAt);

const summarizeError = (err) => ({
  name: err?.name || null,
  message: err?.message || String(err || ""),
  code: err?.code || null,
  status: Number.isFinite(Number(err?.status)) ? Number(err.status) : null,
});

const logApiDebug = (_step, _details = {}) => {};

const warnApiDebug = (_step, _details = {}) => {};

const readResponseBodySafe = async (res) => {
  if (!res) return { text: "", data: null };
  let text = "";
  try {
    text = await res.text();
  } catch (_err) {
    text = "";
  }
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (_err) {
      data = null;
    }
  }
  return { text, data };
};

const normalizeAccessToken = (value = "") => {
  const token = String(value || "").trim();
  return token || "";
};

const writeAccessTokenBridge = (token = "", source = "") => {
  const normalizedToken = normalizeAccessToken(token);
  if (typeof window === "undefined") return;
  try {
    if (!normalizedToken) {
      window.sessionStorage.removeItem(AUTH_ACCESS_TOKEN_BRIDGE_KEY);
      return;
    }
    window.sessionStorage.setItem(
      AUTH_ACCESS_TOKEN_BRIDGE_KEY,
      JSON.stringify({
        token: normalizedToken,
        source: trimText(source, 80),
        stored_at: Date.now(),
      }),
    );
  } catch (_err) {
    // noop
  }
};

const readAccessTokenBridge = () => {
  if (typeof window === "undefined") return "";
  try {
    const raw = window.sessionStorage.getItem(AUTH_ACCESS_TOKEN_BRIDGE_KEY);
    if (!raw) return "";
    const parsed = JSON.parse(raw);
    const token = normalizeAccessToken(parsed?.token);
    const storedAt = Number(parsed?.stored_at || 0);
    if (!token || !storedAt || Date.now() - storedAt > AUTH_ACCESS_TOKEN_BRIDGE_TTL_MS) {
      window.sessionStorage.removeItem(AUTH_ACCESS_TOKEN_BRIDGE_KEY);
      return "";
    }
    return token;
  } catch (_err) {
    return "";
  }
};

const rememberAccessToken = (token = "", source = "") => {
  const normalizedToken = normalizeAccessToken(token);
  if (!normalizedToken) return "";
  cachedAuthAccessToken = normalizedToken;
  cachedAuthAccessTokenStoredAt = Date.now();
  writeAccessTokenBridge(normalizedToken, source);
  logApiDebug("accessToken.remember", {
    source: trimText(source, 80),
    tokenLength: normalizedToken.length,
  });
  return normalizedToken;
};

const clearRememberedAccessToken = (source = "") => {
  cachedAuthAccessToken = "";
  cachedAuthAccessTokenStoredAt = 0;
  writeAccessTokenBridge("", source);
  logApiDebug("accessToken.clear", {
    source: trimText(source, 80),
  });
};

const getRememberedAccessToken = () => {
  const cached = normalizeAccessToken(cachedAuthAccessToken);
  const cachedAgeMs = Date.now() - Number(cachedAuthAccessTokenStoredAt || 0);
  if (cached && cachedAgeMs >= 0 && cachedAgeMs <= AUTH_ACCESS_TOKEN_BRIDGE_TTL_MS) {
    return cached;
  }
  if (cached) {
    cachedAuthAccessToken = "";
    cachedAuthAccessTokenStoredAt = 0;
  }
  const bridged = readAccessTokenBridge();
  if (bridged) {
    cachedAuthAccessToken = bridged;
    cachedAuthAccessTokenStoredAt = Date.now();
    logApiDebug("accessToken.restoreFromBridge", {
      tokenLength: bridged.length,
    });
    return bridged;
  }
  return "";
};

const initAuthStateBridge = () => {
  if (authStateBridgeInit) return;
  authStateBridgeInit = true;
  try {
    supabase.auth.onAuthStateChange((event, session) => {
      const accessToken = normalizeAccessToken(session?.access_token);
      if (accessToken) {
        rememberAccessToken(accessToken, `authState:${event}`);
      } else {
        clearRememberedAccessToken(`authState:${event}`);
      }
      logApiDebug("authStateChange", {
        event,
        hasSession: Boolean(session),
        hasAccessToken: Boolean(accessToken),
        authUserId: session?.user?.id || "",
      });
    });
  } catch (err) {
    console.error(`${API_DEBUG_PREFIX} authStateBridge:error`, summarizeError(err));
  }
};

const safeSerialize = (value, maxLen = 4000) => {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return trimText(value, maxLen);
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (value instanceof Error) {
    const stack = trimText(value.stack || "", maxLen);
    if (stack) return stack;
    return trimText(value.message || value.name || "Error", maxLen);
  }
  try {
    return trimText(JSON.stringify(value), maxLen);
  } catch (_err) {
    try {
      return trimText(String(value), maxLen);
    } catch (_err2) {
      return "";
    }
  }
};

const shouldSendClientError = (dedupKey = "") => {
  if (clientErrorReportsSent >= CLIENT_ERROR_MAX_REPORTS_PER_PAGE) return false;
  const key = String(dedupKey || "").trim();
  if (!key) return true;
  const now = Date.now();
  const last = Number(clientErrorLastSentByKey.get(key) || 0);
  if (last && now - last < CLIENT_ERROR_DEDUP_WINDOW_MS) return false;
  clientErrorLastSentByKey.set(key, now);
  return true;
};

const postClientError = async (payload = {}) => {
  try {
    await fetch(`${API_BASE}/api/client-errors`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      keepalive: true,
      body: JSON.stringify(payload),
    });
  } catch (_err) {
    // noop: nunca bloquear UX por fallas de logging
  }
};

const buildUuidFallback = () =>
  "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (char) => {
    const rand = Math.floor(Math.random() * 16);
    const value = char === "x" ? rand : (rand & 0x3) | 0x8;
    return value.toString(16);
  });

const generateTrafficSessionId = () => {
  try {
    if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  } catch (_err) {
    // noop
  }
  return buildUuidFallback();
};

const readTrafficSessionState = () => {
  try {
    if (typeof window === "undefined") return null;
    const raw = window.localStorage.getItem(TRAFICO_WEB_SESSION_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch (_err) {
    return null;
  }
};

const writeTrafficSessionState = (state) => {
  try {
    if (typeof window === "undefined" || !state || typeof state !== "object") return;
    window.localStorage.setItem(TRAFICO_WEB_SESSION_STORAGE_KEY, JSON.stringify(state));
  } catch (_err) {
    // noop
  }
};

const getTrafficNavigationType = () => {
  try {
    const nav = performance?.getEntriesByType?.("navigation")?.[0];
    return trimText(nav?.type || "", 40);
  } catch (_err) {
    return "";
  }
};

const canTrackProductionTraffic = () => {
  if (typeof window === "undefined") return false;
  const host = String(window.location.hostname || "").trim().toLowerCase();
  return host === "mooseplus.com" || host === "www.mooseplus.com";
};

const getOrCreateTrafficSession = () => {
  const now = Date.now();
  const current = readTrafficSessionState();
  const currentId = trimText(current?.id_sesion || "", 120);
  const lastSeenAt = Number(current?.ultima_actividad || 0);
  const isExpired = !currentId || !lastSeenAt || now - lastSeenAt > TRAFICO_WEB_SESSION_IDLE_MS;
  const nextState = {
    id_sesion: isExpired ? generateTrafficSessionId() : currentId,
    creada_en: isExpired ? now : Number(current?.creada_en || now),
    ultima_actividad: now,
  };
  writeTrafficSessionState(nextState);
  return {
    idSesion: nextState.id_sesion,
    esNuevaSesion: isExpired,
  };
};

const touchTrafficSession = () => {
  const current = readTrafficSessionState();
  const idSesion = trimText(current?.id_sesion || "", 120);
  if (!idSesion) return;
  writeTrafficSessionState({
    id_sesion: idSesion,
    creada_en: Number(current?.creada_en || Date.now()),
    ultima_actividad: Date.now(),
  });
};

const postTrafficWebEvent = async (payload = {}) => {
  try {
    await fetch(`${API_BASE}/api/eventos-trafico-web`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      keepalive: true,
      body: JSON.stringify(payload),
    });
  } catch (_err) {
    // noop: nunca bloquear UX por fallas de analítica
  }
};

const reportTrafficWebEvent = (tipoEvento, extraMetadata = null) => {
  try {
    if (typeof window === "undefined") return;
    if (!canTrackProductionTraffic()) return;
    const sessionUserId = Number(requireSession());
    if (!Number.isFinite(sessionUserId) || sessionUserId <= 0) return;
    const { idSesion } = getOrCreateTrafficSession();
    if (!idSesion) return;
    const metadataBase = {
      titulo: trimText(document?.title || "", 200),
      hash: trimText(window.location.hash || "", 200),
      busqueda: trimText(window.location.search || "", 500),
      idioma: trimText(navigator.language || "", 40),
      ancho_ventana: Number.isFinite(Number(window.innerWidth)) ? Number(window.innerWidth) : null,
      alto_ventana: Number.isFinite(Number(window.innerHeight)) ? Number(window.innerHeight) : null,
      tipo_navegacion: getTrafficNavigationType(),
    };
    const metadata =
      extraMetadata && typeof extraMetadata === "object"
        ? { ...metadataBase, ...extraMetadata }
        : metadataBase;
    postTrafficWebEvent({
      tipo_evento: trimText(tipoEvento || "vista_pagina", 40),
      id_sesion: trimText(idSesion, 120),
      ruta: trimText(window.location.pathname || "/", 500),
      url_completa: trimText(window.location.href || "", 2000),
      referidor: trimText(document.referrer || "", 2000),
      agente_usuario: trimText(navigator.userAgent || "", 1000),
      metadatos: metadata,
      fecha_hora: new Date().toISOString(),
    });
  } catch (_err) {
    // noop
  }
};

const reportClientError = (input = {}) => {
  try {
    if (typeof window === "undefined") return;
    const payload = {
      level: trimText(input.level || "error", 20),
      kind: trimText(input.kind || "runtime", 40),
      message: trimText(input.message || "Frontend error", 4000),
      stack: trimText(input.stack || "", 12000),
      source: trimText(input.source || "", 1000),
      line: Number.isFinite(Number(input.line)) ? Number(input.line) : null,
      column: Number.isFinite(Number(input.column)) ? Number(input.column) : null,
      page_url: trimText(window.location.href || "", 2000),
      page_path: trimText(window.location.pathname || "", 500),
      user_agent: trimText(navigator.userAgent || "", 1000),
      metadata: input.metadata && typeof input.metadata === "object" ? input.metadata : null,
      occurred_at: new Date().toISOString(),
    };
    const dedupKey = [
      payload.kind,
      payload.message,
      payload.source,
      payload.line ?? "",
      payload.column ?? "",
    ].join("|");
    if (!shouldSendClientError(dedupKey)) return;
    clientErrorReportsSent += 1;
    postClientError(payload);
  } catch (_err) {
    // noop
  }
};

const initTrafficWebTracker = () => {
  if (traficoWebTrackerInit || typeof window === "undefined") return;
  traficoWebTrackerInit = true;

  const { esNuevaSesion } = getOrCreateTrafficSession();
  if (esNuevaSesion) {
    reportTrafficWebEvent("inicio_sesion_web");
  }
  reportTrafficWebEvent("vista_pagina");

  window.addEventListener(
    "focus",
    () => {
      touchTrafficSession();
    },
    { passive: true },
  );

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      touchTrafficSession();
    }
  });
};

const initClientErrorReporter = () => {
  if (clientErrorReporterInit || typeof window === "undefined") return;
  clientErrorReporterInit = true;

  window.addEventListener("error", (event) => {
    const err = event?.error;
    reportClientError({
      level: "error",
      kind: "window_error",
      message:
        trimText(event?.message || err?.message || "window error", 4000) || "window error",
      stack: safeSerialize(err?.stack || err || "", 12000),
      source: trimText(event?.filename || "", 1000),
      line: event?.lineno,
      column: event?.colno,
    });
  });

  window.addEventListener("unhandledrejection", (event) => {
    const reason = event?.reason;
    const message =
      reason instanceof Error
        ? reason.message
        : typeof reason === "string"
          ? reason
          : "Unhandled promise rejection";
    reportClientError({
      level: "error",
      kind: "unhandled_rejection",
      message: trimText(message || "Unhandled promise rejection", 4000),
      stack: safeSerialize(reason instanceof Error ? reason.stack || reason : reason, 12000),
    });
  });

  const originalConsoleError = console.error?.bind(console);
  if (typeof originalConsoleError === "function") {
    console.error = (...args) => {
      try {
        const first = args[0];
        const maybeError = first instanceof Error ? first : null;
        const message =
          maybeError?.message ||
          args.map((part) => safeSerialize(part, 600)).filter(Boolean).join(" | ") ||
          "console.error";
        const stack = maybeError?.stack
          ? safeSerialize(maybeError.stack, 12000)
          : safeSerialize(args, 12000);
        reportClientError({
          level: "error",
          kind: "console_error",
          message: trimText(message, 4000),
          stack,
        });
      } catch (_err) {
        // noop
      }
      originalConsoleError(...args);
    };
  }
};

initClientErrorReporter();
initTrafficWebTracker();
initAuthStateBridge();

const isAuthFatalErrorMessage = (msg = "") => {
  const text = String(msg || "").toLowerCase();
  if (!text) return false;
  return (
    text.includes("usuario no autenticado") ||
    text.includes("access denied") ||
    text.includes("auth required") ||
    text.includes("invalid jwt") ||
    text.includes("jwt")
  );
};

const isRecoverableAuthBridgeError = (msg = "") => {
  const text = String(msg || "").toLowerCase();
  if (!text) return false;
  return (
    text.includes("sesión de auth no disponible") ||
    text.includes("no se pudo leer la sesión de auth")
  );
};

const clearSupabaseLocalArtifacts = () => {
  try {
    const keysToDelete = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = String(window.localStorage.key(i) || "");
      if (!key) continue;
      if (key.startsWith("sb-") || key.includes("supabase")) keysToDelete.push(key);
    }
    keysToDelete.forEach((key) => window.localStorage.removeItem(key));
  } catch (_err) {
    // noop
  }
};

const getLoginHref = () => {
  const idx = window.location.pathname.indexOf("/pages/");
  if (idx >= 0) {
    const base = window.location.pathname.slice(0, idx + "/pages/".length);
    return `${window.location.origin}${base}login.html`;
  }
  return `${window.location.origin}/src/frontend/pages/login.html`;
};

const isAnonymousAllowedPath = () => {
  const path = String(window.location.pathname || "").toLowerCase();
  return (
    path === "/" ||
    path.endsWith("/index.html") ||
    path.endsWith("/src/frontend/pages/index.html")
  );
};

const bufferToBase64 = (buffer) => {
  let binary = "";
  const bytes = new Uint8Array(buffer);
  const chunkSize = 8192;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
};

const readBlobAsDataUrl = (blob) =>
  new Promise((resolve, reject) => {
    if (!(blob instanceof Blob)) {
      reject(new Error("Archivo inválido para lectura."));
      return;
    }
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("No se pudo leer el archivo."));
    reader.readAsDataURL(blob);
  });

const extractBase64Content = (value = "") => {
  const raw = String(value || "");
  const commaIndex = raw.indexOf(",");
  return commaIndex >= 0 ? raw.slice(commaIndex + 1) : raw;
};

const buildUploadPayloadFile = async (file = {}, { normalizeType = false } = {}) => {
  const name = String(file?.name || "file").trim() || "file";
  const baseType = String(file?.type || "").trim();
  const type = normalizeType ? normalizeImageUploadType(baseType, name) : baseType;
  const inlineContent = String(file?.content || "").trim();
  if (inlineContent) {
    return { name, type, content: inlineContent };
  }

  let lastError = null;
  if (typeof file?.arrayBuffer === "function") {
    try {
      return {
        name,
        type,
        content: bufferToBase64(await file.arrayBuffer()),
      };
    } catch (err) {
      lastError = err;
    }
  }

  if (file instanceof Blob) {
    try {
      const dataUrl = await readBlobAsDataUrl(file);
      const content = extractBase64Content(dataUrl);
      if (content) return { name, type, content };
    } catch (err) {
      lastError = err;
    }
  }

  throw lastError || new Error("No se pudo leer el archivo para subir.");
};

const IMAGE_MIME_BY_EXT = {
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  webp: "image/webp",
  gif: "image/gif",
  bmp: "image/bmp",
  avif: "image/avif",
  svg: "image/svg+xml",
};

const getImageMimeFromName = (name = "") => {
  const match = String(name || "")
    .trim()
    .toLowerCase()
    .match(/\.([a-z0-9]+)$/);
  const ext = match?.[1] || "";
  return IMAGE_MIME_BY_EXT[ext] || "";
};

const normalizeImageUploadType = (type = "", fileName = "") => {
  const rawType = String(type || "").trim().toLowerCase();
  const mimeFromName = getImageMimeFromName(fileName);
  if (rawType === "image/webp" || mimeFromName === "image/webp") return "image/webp";
  if (rawType.startsWith("image/")) return rawType;
  if (mimeFromName) return mimeFromName;
  return "application/octet-stream";
};

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const TRANSIENT_NETWORK_ERROR_FRAGMENTS = [
  "failed to fetch",
  "fetch failed",
  "network request failed",
  "networkerror",
  "load failed",
  "timeout",
  "timed out",
  "econnreset",
  "enotfound",
  "dns",
  "hostname could not be found",
  "server with the specified hostname could not be found",
  "name or service not known",
  "name not resolved",
  "err_network",
];

const hasTransientNetworkMessage = (value = "") => {
  const msg = String(value || "").toLowerCase();
  if (!msg) return false;
  return TRANSIENT_NETWORK_ERROR_FRAGMENTS.some((fragment) => msg.includes(fragment));
};

const isTransientNetworkError = (err) => {
  if (!err) return false;
  if (typeof err === "string") return hasTransientNetworkMessage(err);
  return hasTransientNetworkMessage(err?.message || err?.details || err);
};

const getFriendlyApiErrorMessage = (err, fallbackMessage = "") => {
  if (isTransientNetworkError(err)) {
    return String(fallbackMessage || "Problema de conexión. Intenta de nuevo.");
  }
  const message = String(err?.message || err || "").trim();
  return message || String(fallbackMessage || "Ocurrió un error inesperado.");
};

const getAuthSessionCooldownRemainingMs = () =>
  Math.max(0, Number(authSessionFailureCooldownUntil || 0) - Date.now());

const clearAuthSessionFailureCooldown = () => {
  authSessionFailureCooldownUntil = 0;
  authSessionFailureMessage = "";
};

const isAuthGetSessionTransientError = (err) => {
  if (!err) return false;
  const code = String(err?.code || "").trim().toUpperCase();
  if (code === "AUTH_GET_SESSION_TIMEOUT") return true;
  if (isTransientNetworkError(err)) return true;
  const msg = String(err?.message || err || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("auth.getsession timeout") ||
    msg.includes("getsession timeout") ||
    msg.includes("tiempo de espera excedido")
  );
};

const rememberAuthSessionTransientFailure = (err, fallbackMessage = "") => {
  if (!isAuthGetSessionTransientError(err)) return;
  authSessionFailureCooldownUntil = Date.now() + AUTH_SESSION_FAILURE_COOLDOWN_MS;
  authSessionFailureMessage = getFriendlyApiErrorMessage(err, fallbackMessage);
};

const resolveAccessTokenFromSupabaseSession = async ({
  timeoutMs = 6000,
  startedAt = getDebugNow(),
  fallbackSessionErrorMessage = "",
  source = "auth.getSession",
  forceRefresh = false,
} = {}) => {
  let timeoutId = null;
  let sessionResult = null;
  const authMethod =
    forceRefresh && typeof supabase.auth.refreshSession === "function"
      ? () => supabase.auth.refreshSession()
      : () => supabase.auth.getSession();

  try {
    sessionResult = await Promise.race([
      authMethod(),
      new Promise((_, reject) => {
        timeoutId = window.setTimeout(() => {
          const timeoutErr = new Error("Supabase auth.getSession timeout");
          timeoutErr.code = "AUTH_GET_SESSION_TIMEOUT";
          reject(timeoutErr);
        }, timeoutMs);
      }),
    ]).finally(() => {
      if (timeoutId) window.clearTimeout(timeoutId);
    });
  } catch (sessionRuntimeErr) {
    rememberAuthSessionTransientFailure(sessionRuntimeErr, fallbackSessionErrorMessage);
    if (isAuthGetSessionTransientError(sessionRuntimeErr)) {
      warnApiDebug(`${source}:transientFailure`, {
        ms: getElapsedMs(startedAt),
        timeoutMs,
        ...summarizeError(sessionRuntimeErr),
      });
      return {
        accessToken: "",
        error: getFriendlyApiErrorMessage(sessionRuntimeErr, fallbackSessionErrorMessage),
      };
    }
    throw sessionRuntimeErr;
  }

  const { data: sessionData, error: sessionErr } = sessionResult || {};
  if (sessionErr) {
    rememberAuthSessionTransientFailure(sessionErr, fallbackSessionErrorMessage);
    if (isAuthGetSessionTransientError(sessionErr)) {
      warnApiDebug(`${source}:transientResponse`, {
        ms: getElapsedMs(startedAt),
        ...summarizeError(sessionErr),
      });
      return {
        accessToken: "",
        error: getFriendlyApiErrorMessage(sessionErr, fallbackSessionErrorMessage),
      };
    }
    console.error(`${source} error`, sessionErr);
    console.error(`${API_DEBUG_PREFIX} ${source}:error`, {
      ms: getElapsedMs(startedAt),
      ...summarizeError(sessionErr),
    });
    return { accessToken: "", error: "No se pudo leer la sesión de auth" };
  }

  const accessToken = rememberAccessToken(
    sessionData?.session?.access_token || "",
    source,
  );
  if (accessToken) {
    clearAuthSessionFailureCooldown();
  }
  logApiDebug(`${source}:done`, {
    ms: getElapsedMs(startedAt),
    hasSession: Boolean(sessionData?.session),
    hasAccessToken: Boolean(accessToken),
    authUserId: sessionData?.session?.user?.id || "",
    authEmail: trimText(sessionData?.session?.user?.email || "", 120),
  });

  return {
    accessToken,
    error: accessToken ? "" : "Sesión de auth no disponible",
  };
};

const postServerSessionWithAccessToken = async (accessToken = "") => {
  const requestStartedAt = getDebugNow();
  const res = await fetchWithRetry(
    `${API_BASE}/api/session`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
      credentials: "include",
      body: "{}",
    },
    { attempts: 2, label: "startSession" },
  );
  const { text, data } = await readResponseBodySafe(res);
  return { res, text, data, requestStartedAt };
};

const fetchWithRetry = async (url, options = {}, retryOptions = {}) => {
  const attempts = Math.max(1, Number(retryOptions?.attempts) || 1);
  const delayMs = Math.max(0, Number(retryOptions?.delayMs) || 250);
  const label = String(retryOptions?.label || "fetch");
  let lastErr = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fetch(url, options);
    } catch (err) {
      lastErr = err;
      const shouldRetry = attempt < attempts && isTransientNetworkError(err);
      if (!shouldRetry) throw err;
      warnApiDebug(`${label}:transientNetworkRetry`, {
        attempt,
        message: String(err?.message || err || "").slice(0, 220),
      });
      await wait(delayMs * attempt);
    }
  }

  throw lastErr || new Error("Fetch failed");
};

const runSupabaseQueryWithRetry = async (queryFactory, retryOptions = {}) => {
  const attempts = Math.max(1, Number(retryOptions?.attempts) || 1);
  const delayMs = Math.max(0, Number(retryOptions?.delayMs) || 250);
  const label = String(retryOptions?.label || "supabase");
  let lastResult = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const result = await queryFactory();
    lastResult = result;
    const shouldRetry =
      !!result?.error && attempt < attempts && isTransientNetworkError(result.error);
    if (!shouldRetry) return result;
    warnApiDebug(`${label}:transientSupabaseRetry`, {
      attempt,
      message: String(result?.error?.message || result?.error || "").slice(0, 220),
    });
    await wait(delayMs * attempt);
  }

  return lastResult;
};

const isTransientCartBackendError = (status, bodyText = "") => {
  if (Number(status) < 500) return false;
  return hasTransientNetworkMessage(bodyText);
};

const fetchMySpecialPrices = async () => {
  try {
    const res = await fetch(`${API_BASE}/api/precios-especiales/me`, {
      credentials: "include",
    });
    if (!res.ok) return [];
    const data = await res.json().catch(() => ({}));
    return Array.isArray(data?.items) ? data.items : [];
  } catch (_err) {
    return [];
  }
};

export async function loadCatalog() {
  const startedAt = getDebugNow();
  logApiDebug("loadCatalog:start", {});
  const normalizePlataformaRow = (row = {}) => {
    const idDescMesBase = row.id_descuento_mes ?? 1;
    const idDescCantidadBase = row.id_descuento_cantidad ?? 2;
    const isExplicitFalse = (value) =>
      value === false || value === 0 || value === "0" || value === "false" || value === "f";
    return {
      ...row,
      id_descuento_mes: idDescMesBase,
      id_descuento_cantidad: idDescCantidadBase,
      id_descuento_mes_detal: row.id_descuento_mes_detal ?? idDescMesBase,
      id_descuento_mes_mayor: row.id_descuento_mes_mayor ?? idDescMesBase,
      id_descuento_cantidad_detal: row.id_descuento_cantidad_detal ?? idDescCantidadBase,
      id_descuento_cantidad_mayor: row.id_descuento_cantidad_mayor ?? idDescCantidadBase,
      aplica_descuento_mes_detal: isExplicitFalse(row.aplica_descuento_mes_detal) ? false : true,
      aplica_descuento_mes_mayor: isExplicitFalse(row.aplica_descuento_mes_mayor) ? false : true,
      aplica_descuento_cantidad_detal: isExplicitFalse(row.aplica_descuento_cantidad_detal)
        ? false
        : true,
      aplica_descuento_cantidad_mayor: isExplicitFalse(row.aplica_descuento_cantidad_mayor)
        ? false
        : true,
    };
  };

  const fetchPlataformasCatalog = async () => {
    // `select("*")` evita 400 por columnas opcionales no presentes en algunos entornos.
    const base = await supabase.from("plataformas").select("*").order("nombre");
    if (base.error) return base;
    return {
      ...base,
      data: (base.data || []).map(normalizePlataformaRow),
    };
  };

  const [
    { data: categorias, error: errCat },
    { data: plataformas, error: errPlat },
    { data: precios, error: errPre },
    { data: descuentos, error: errDesc },
    preciosEspeciales,
  ] = await Promise.all([
    supabase.from("categorias").select("id_categoria, nombre").order("id_categoria"),
    fetchPlataformasCatalog(),
    supabase
      .from("precios")
      .select(
        "id_precio, id_plataforma, cantidad, precio_usd_detal, precio_usd_mayor, duracion, completa, plan, region, valor_tarjeta_de_regalo, moneda, sub_cuenta, descripcion_plan"
      )
      .order("id_precio"),
    supabase.from("descuentos").select("*").order("meses", { ascending: true }),
    fetchMySpecialPrices(),
  ]);

  if (errCat || errPlat || errPre || errDesc) {
    console.error(`${API_DEBUG_PREFIX} loadCatalog:error`, {
      ms: getElapsedMs(startedAt),
      categorias: errCat?.message || null,
      plataformas: errPlat?.message || null,
      precios: errPre?.message || null,
      descuentos: errDesc?.message || null,
    });
    throw new Error(errCat?.message || errPlat?.message || errPre?.message || errDesc?.message);
  }

  const specialByPrecio = (preciosEspeciales || []).reduce((acc, row) => {
    const precioId = Number(row?.id_precio);
    const specialId = Number(row?.id);
    const monto = Number(row?.monto);
    if (!Number.isFinite(precioId) || precioId <= 0) return acc;
    if (!Number.isFinite(monto)) return acc;
    acc[precioId] = {
      id: Number.isFinite(specialId) && specialId > 0 ? Math.trunc(specialId) : null,
      monto,
    };
    return acc;
  }, {});
  const preciosResolved = (precios || []).map((price) => {
    const precioId = Number(price?.id_precio);
    const special = Number.isFinite(precioId) ? specialByPrecio[precioId] : null;
    if (!special || !Number.isFinite(Number(special?.monto))) {
      return {
        ...price,
        id_precio_especial: null,
      };
    }
    const montoEspecial = Number(special.monto);
    return {
      ...price,
      id_precio_especial: special.id,
      precio_usd_detal: montoEspecial,
      precio_usd_mayor: montoEspecial,
    };
  });

  logApiDebug("loadCatalog:done", {
    ms: getElapsedMs(startedAt),
    categorias: Array.isArray(categorias) ? categorias.length : 0,
    plataformas: Array.isArray(plataformas) ? plataformas.length : 0,
    precios: Array.isArray(preciosResolved) ? preciosResolved.length : 0,
    descuentos: Array.isArray(descuentos) ? descuentos.length : 0,
  });
  return { categorias, plataformas, precios: preciosResolved, descuentos };
}

export async function searchSpecialPriceClients(query = "", limit = 30) {
  try {
    await ensureServerSession();
    const url = new URL(`${API_BASE}/api/admin/precios-especiales/clientes`);
    const q = String(query || "").trim();
    if (q) url.searchParams.set("q", q);
    if (Number.isFinite(Number(limit))) {
      url.searchParams.set("limit", String(Math.max(1, Math.trunc(Number(limit)))));
    }
    const res = await fetch(url.toString(), {
      credentials: "include",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo buscar clientes.", items: [] };
    }
    return {
      items: Array.isArray(data?.items) ? data.items : [],
    };
  } catch (err) {
    console.error("searchSpecialPriceClients error", err);
    return { error: err.message, items: [] };
  }
}

export async function fetchSpecialPricesForClient(idUsuario) {
  try {
    await ensureServerSession();
    const idUsuarioNum = Number(idUsuario);
    if (!Number.isFinite(idUsuarioNum) || idUsuarioNum <= 0) {
      return { error: "id_usuario inválido." };
    }
    const url = new URL(`${API_BASE}/api/admin/precios-especiales`);
    url.searchParams.set("id_usuario", String(Math.trunc(idUsuarioNum)));
    const res = await fetch(url.toString(), {
      credentials: "include",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudieron cargar los precios especiales." };
    }
    return data;
  } catch (err) {
    console.error("fetchSpecialPricesForClient error", err);
    return { error: err.message };
  }
}

export async function saveSpecialPriceForClient({ id_usuario, id_precio, monto }) {
  try {
    await ensureServerSession();
    const res = await fetch(`${API_BASE}/api/admin/precios-especiales`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        id_usuario,
        id_precio,
        monto,
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo guardar el precio especial." };
    }
    return data;
  } catch (err) {
    console.error("saveSpecialPriceForClient error", err);
    return { error: err.message };
  }
}

export async function fetchHomeBanners(options = {}) {
  const startedAt = getDebugNow();
  logApiDebug("fetchHomeBanners:start", {
    includeInactive: options?.includeInactive === true,
  });
  try {
    const url = new URL(`${API_BASE}/api/home-banners`);
    if (options?.includeInactive === true) {
      url.searchParams.set("include_inactive", "true");
    }
    const res = await fetch(url.toString(), {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      warnApiDebug("fetchHomeBanners:response", {
        ms: getElapsedMs(startedAt),
        status: res.status,
        body: trimText(text, 300),
      });
      return { error: text || "No se pudieron cargar banners", items: [] };
    }
    const data = await res.json().catch(() => ({}));
    logApiDebug("fetchHomeBanners:done", {
      ms: getElapsedMs(startedAt),
      items: Array.isArray(data?.items) ? data.items.length : 0,
      tableMissing: data?.tableMissing === true,
    });
    return {
      items: Array.isArray(data?.items) ? data.items : [],
      tableMissing: data?.tableMissing === true,
    };
  } catch (err) {
    console.error("No se pudieron cargar los banners del home:", err);
    console.error(`${API_DEBUG_PREFIX} fetchHomeBanners:error`, {
      ms: getElapsedMs(startedAt),
      ...summarizeError(err),
    });
    return { error: err.message, items: [] };
  }
}

export async function createHomeBanner(payload = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const res = await fetch(`${API_BASE}/api/home-banners`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        ...payload,
        id_usuario,
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo crear el banner." };
    }
    return data;
  } catch (err) {
    console.error("No se pudo crear banner:", err);
    return { error: err.message };
  }
}

export async function updateHomeBanner(idBanner, payload = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const res = await fetch(`${API_BASE}/api/home-banners/${encodeURIComponent(idBanner)}`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        ...payload,
        id_usuario,
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo actualizar el banner." };
    }
    return data;
  } catch (err) {
    console.error("No se pudo actualizar banner:", err);
    return { error: err.message };
  }
}

export async function createUsuarioSignupLink(idUsuarioTarget) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const target = Number(idUsuarioTarget);
    if (!Number.isFinite(target) || target <= 0) {
      return { error: "id_usuario inválido" };
    }

    const res = await fetch(
      `${API_BASE}/api/usuarios/${encodeURIComponent(target)}/signup-link`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ id_usuario }),
      },
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo generar el link de registro." };
    }
    return data;
  } catch (err) {
    console.error("createUsuarioSignupLink error", err);
    return { error: err.message };
  }
}

export async function createPublicSignupLink(accesoCliente = true) {
  await ensureServerSession();
  try {
    console.log("[api][signup-link/public] request", {
      acceso_cliente: accesoCliente === true,
      api_base: API_BASE,
    });
    const res = await fetch(`${API_BASE}/api/signup-link/public`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ acceso_cliente: accesoCliente === true }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      console.warn("[api][signup-link/public] response not ok", {
        status: res.status,
        error: data?.error || "",
      });
      return { error: data?.error || "No se pudo generar el link de registro." };
    }
    console.log("[api][signup-link/public] response ok", {
      hasUrl: !!String(data?.url || "").trim(),
      expires_at: data?.expires_at || null,
      acceso_cliente: data?.acceso_cliente,
    });
    return data;
  } catch (err) {
    console.error("createPublicSignupLink error", err);
    return { error: err.message };
  }
}

export async function updateUsuarioCorreoWithAuth(idUsuarioTarget, correo) {
  await ensureServerSession();
  try {
    const target = Number(idUsuarioTarget);
    if (!Number.isFinite(target) || target <= 0) {
      return { error: "id_usuario inválido" };
    }
    const correoNormalizado = String(correo || "")
      .trim()
      .toLowerCase();
    if (!correoNormalizado) {
      return { error: "Correo requerido." };
    }
    const res = await fetch(
      `${API_BASE}/api/admin/usuarios/${encodeURIComponent(target)}/correo`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ correo: correoNormalizado }),
      },
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo actualizar el correo del usuario." };
    }
    return data;
  } catch (err) {
    console.error("updateUsuarioCorreoWithAuth error", err);
    return { error: err.message };
  }
}

export async function fetchAuthVerifiedUsersStats() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/auth/stats/verified-users`, {
      credentials: "include",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo cargar la estadística de Auth." };
    }
    return data;
  } catch (err) {
    console.error("fetchAuthVerifiedUsersStats error", err);
    return { error: err.message };
  }
}

export async function triggerWhatsappReminderForUser(idUsuarioTarget) {
  await ensureServerSession();
  try {
    const target = Number(idUsuarioTarget);
    if (!Number.isFinite(target) || target <= 0) {
      return { error: "id_usuario inválido" };
    }

    const res = await fetch(`${API_BASE}/api/whatsapp/recordatorios/trigger-user`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ id_usuario: target }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo disparar el recordatorio." };
    }
    return data;
  } catch (err) {
    console.error("triggerWhatsappReminderForUser error", err);
    return { error: err.message };
  }
}

export async function validateSignupRegistrationToken(token) {
  try {
    const value = String(token || "").trim();
    if (!value) return { error: "Token requerido." };
    const res = await fetch(
      `${API_BASE}/api/signup-link/validate?token=${encodeURIComponent(value)}`,
      {
        credentials: "include",
      },
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo validar el token." };
    }
    return data;
  } catch (err) {
    console.error("validateSignupRegistrationToken error", err);
    return { error: err.message };
  }
}

export async function completeSignupWithRegistrationToken(payload = {}) {
  try {
    const res = await fetch(`${API_BASE}/api/signup-link/complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo completar el registro con token." };
    }
    return data;
  } catch (err) {
    console.error("completeSignupWithRegistrationToken error", err);
    return { error: err.message };
  }
}

export async function applyRenewalReminderToken(token) {
  try {
    await ensureServerSession();
    const value = String(token || "").trim();
    if (!value) return { error: "Token requerido." };
    const res = await fetch(`${API_BASE}/api/cart/renewal-link/apply`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ token: value }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudieron agregar las renovaciones al carrito." };
    }
    return data;
  } catch (err) {
    console.error("applyRenewalReminderToken error", err);
    return { error: err.message };
  }
}

// Enviar delta de cantidad (+ agrega / - resta). Si la cantidad resultante es <= 0 se borra el item, y si no quedan items se elimina el carrito.
export async function sendCartDelta(idPrecio, delta, meses, extra = {}) {
  await ensureServerSession();
  const id_usuario = requireSession();
  const res = await fetch(`${API_BASE}/api/cart/item`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
    body: JSON.stringify({ id_precio: idPrecio, delta, id_usuario, meses, ...extra }),
  });
  if (!res.ok) {
    const text = await res.text();
    console.error("cart/item response", res.status, text);
    throw new Error(text || "No se pudo actualizar el carrito");
  }
}

export async function createCart() {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const res = await fetch(`${API_BASE}/api/cart`, {
      method: "POST",
      credentials: "include",
      body: JSON.stringify({ id_usuario }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart create response", res.status, text);
      return null;
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo crear el carrito:", err);
    return null;
  }
}

export async function fetchCart() {
  const startedAt = getDebugNow();
  logApiDebug("fetchCart:start", {
    sessionUserId: requireSession(),
  });
  await ensureServerSession();
  try {
    const id = requireSession();
    const url = `${API_BASE}/api/cart?id_usuario=${encodeURIComponent(id)}`;
    const startedAt = Date.now();
    let res = null;
    let data = null;
    let lastErrText = "";
    const maxAttempts = 2;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      res = await fetch(url, {
        credentials: "include",
      });
      const elapsedMs = Date.now() - startedAt;

      if (res.ok) {
        data = await res.json();
        break;
      }

      lastErrText = await res.text();
      const shouldRetry =
        attempt < maxAttempts &&
        isTransientCartBackendError(res.status, lastErrText);
      if (!shouldRetry) break;

      warnApiDebug("fetchCart:transientBackendRetry", {
        attempt,
        status: res.status,
        body: String(lastErrText || "").slice(0, 220),
      });
      await wait(250 * attempt);
    }

    if (!res || !res.ok || !data) {
      console.error("cart get response", res?.status, String(lastErrText || "").slice(0, 800));
      warnApiDebug("fetchCart:response", {
        ms: getElapsedMs(startedAt),
        status: res?.status || 0,
        body: trimText(lastErrText, 300),
      });
      return { items: [] };
    }

    logApiDebug("fetchCart:done", {
      ms: getElapsedMs(startedAt),
      items: Array.isArray(data?.items) ? data.items.length : 0,
    });
    return data;
  } catch (err) {
    console.error("No se pudo obtener el carrito:", err, {
      apiBase: API_BASE,
    });
    console.error(`${API_DEBUG_PREFIX} fetchCart:error`, {
      ms: getElapsedMs(startedAt),
      ...summarizeError(err),
    });
    return { items: [] };
  }
}

export async function submitCheckout(payload) {
  try {
    await ensureServerSession();
    const id_usuario = requireSession();
    const res = await fetchWithRetry(
      `${API_BASE}/api/checkout`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ ...payload, id_usuario, comprobante: payload.comprobantes }),
      },
      { attempts: 2, label: "submitCheckout" },
    );
    if (!res.ok) {
      const text = await res.text();
      console.error("checkout response", res.status, text);
      return { error: text || "Error en checkout" };
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo completar el checkout:", err);
    return {
      error: getFriendlyApiErrorMessage(
        err,
        "Problema de conexión al enviar el pago. Intenta de nuevo.",
      ),
    };
  }
}

export async function fetchCheckoutDraft() {
  try {
    await ensureServerSession();
    const res = await fetchWithRetry(
      `${API_BASE}/api/checkout/draft`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({}),
      },
      { attempts: 2, label: "fetchCheckoutDraft" },
    );
    if (!res.ok) {
      const text = await res.text();
      console.error("checkout/draft response", res.status, text);
      return { error: text || "No se pudo obtener la orden de checkout" };
    }
    return res.json();
  } catch (err) {
    console.error("fetchCheckoutDraft error", err);
    return {
      error: getFriendlyApiErrorMessage(
        err,
        "No se pudo preparar la orden de checkout por un problema de conexión.",
      ),
    };
  }
}

export async function fetchCheckoutSummary(payload = {}) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/checkout/summary`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload || {}),
    });
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (_err) {
        message = (await res.text()) || "";
      }
      return { error: message || "No se pudo cargar el resumen del checkout", status: res.status };
    }
    return res.json();
  } catch (err) {
    console.error("fetchCheckoutSummary error", err);
    return { error: err.message };
  }
}

export async function procesarOrden(id_orden, options = {}) {
  const orderId = Number(id_orden);
  if (!Number.isFinite(orderId) || orderId <= 0) {
    return { error: "id_orden inválido" };
  }
  if (orderProcessInFlightById.has(orderId)) {
    return orderProcessInFlightById.get(orderId);
  }
  const requestPromise = (async () => {
    await ensureServerSession();
    try {
      const saldoRaw = options?.saldo_a_favor;
      const saldoNum = Number(String(saldoRaw ?? "").replace(",", "."));
      const payload = { id_orden: orderId };
      if (Number.isFinite(saldoNum) && saldoNum > 0) {
        payload.saldo_a_favor = saldoNum;
      }
      if (options?.force_whatsapp_pago_verificado === true) {
        payload.force_whatsapp_pago_verificado = true;
      }
      const res = await fetch(`${API_BASE}/api/ordenes/procesar`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const text = await res.text();
        console.error("ordenes/procesar response", res.status, text);
        return { error: text || "No se pudo procesar la orden" };
      }
      return res.json();
    } catch (err) {
      console.error("procesarOrden error", err);
      return { error: err.message };
    } finally {
      orderProcessInFlightById.delete(orderId);
    }
  })();
  orderProcessInFlightById.set(orderId, requestPromise);
  return requestPromise;
}

export async function notificarVerificacionManualOrden(id_orden, options = {}) {
  await ensureServerSession();
  const orderId = Number(id_orden);
  if (!Number.isFinite(orderId) || orderId <= 0) {
    return { error: "id_orden inválido" };
  }
  try {
    const payload = { id_orden: orderId };
    const source = String(options?.source || "").trim();
    if (source) payload.source = source;
    const res = await fetch(`${API_BASE}/api/ordenes/notificar-verificacion-manual`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("ordenes/notificar-verificacion-manual response", res.status, text);
      return { error: text || "No se pudo notificar verificación manual", status: res.status };
    }
    return res.json();
  } catch (err) {
    console.error("notificarVerificacionManualOrden error", err);
    return {
      error: getFriendlyApiErrorMessage(
        err,
        "No se pudo notificar al admin por un problema de conexión.",
      ),
    };
  }
}

export async function updateCartMontos(monto_usd) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/cart/montos`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ monto_usd }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart/montos response", res.status, text);
      return { error: text || "No se pudo actualizar montos del carrito" };
    }
    return res.json();
  } catch (err) {
    console.error("updateCartMontos error", err);
    return { error: err.message };
  }
}

export async function updateCartFlags({ usa_saldo } = {}) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/cart/flags`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ usa_saldo }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart/flags response", res.status, text);
      return { error: text || "No se pudo actualizar el carrito" };
    }
    return res.json();
  } catch (err) {
    console.error("updateCartFlags error", err);
    return { error: err.message };
  }
}

export async function uploadComprobantes(files = []) {
  try {
    await ensureServerSession();
    const id_usuario = requireSession();
    const payloadFiles = await Promise.all(files.map((file) => buildUploadPayloadFile(file)));

    const res = await fetchWithRetry(
      `${API_BASE}/api/checkout/upload`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        credentials: "include",
        body: JSON.stringify({ files: payloadFiles, id_usuario }),
      },
      { attempts: 2, label: "uploadComprobantes" },
    );

    if (!res.ok) {
      const text = await res.text();
      console.error("upload comprobantes response", res.status, text);
      return { error: text || "No se pudieron subir los comprobantes" };
    }

    return res.json();
  } catch (err) {
    console.error("No se pudieron subir los comprobantes:", err);
    return {
      error: getFriendlyApiErrorMessage(
        err,
        "Problema de conexión al subir los comprobantes. Intenta de nuevo.",
      ),
    };
  }
}

export async function uploadPlatformLogos(files = [], options = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const folder = String(options?.folder || "").trim();
    const overwriteByName = !!options?.overwriteByName;
    const payloadFiles = await Promise.all(
      files.map((file) => buildUploadPayloadFile(file, { normalizeType: true }))
    );

    const res = await fetch(`${API_BASE}/api/logos/upload`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        files: payloadFiles,
        id_usuario,
        ...(folder ? { folder } : {}),
        ...(overwriteByName ? { overwrite_by_name: true } : {}),
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      console.error("upload logos response", res.status, text);
      return { error: text || "No se pudieron subir los logos" };
    }

    return res.json();
  } catch (err) {
    console.error("No se pudieron subir los logos:", err);
    return { error: err.message };
  }
}

export async function deletePublicAssets(payload = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const paths = Array.isArray(payload?.paths)
      ? payload.paths.map((row) => String(row || "").trim()).filter(Boolean)
      : [];
    const public_urls = Array.isArray(payload?.public_urls)
      ? payload.public_urls.map((row) => String(row || "").trim()).filter(Boolean)
      : [];

    const res = await fetch(`${API_BASE}/api/logos/delete`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        id_usuario,
        paths,
        public_urls,
      }),
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudieron eliminar archivos." };
    }
    return data;
  } catch (err) {
    console.error("No se pudieron eliminar assets públicos:", err);
    return { error: err.message };
  }
}

export async function fetchInventario() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/inventario`, {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("inventario response", res.status, text);
      return { error: text || "No se pudo cargar el inventario" };
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo cargar el inventario:", err);
    return { error: err.message };
  }
}

export async function fetchVentasOrden(idOrden) {
  await ensureServerSession();
  try {
    const res = await fetch(
      `${API_BASE}/api/ventas/orden?id_orden=${encodeURIComponent(idOrden)}`,
      {
        credentials: "include",
      }
    );
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (err) {
        message = (await res.text()) || "";
      }
      if (!message && res.status === 403) {
        message = "Orden no pertenece al usuario.";
      }
      if (!message && res.status === 401) {
        message = "Usuario no autenticado.";
      }
      console.error("ventas/orden response", res.status, message);
      return { error: message || "No se pudo cargar ventas por orden", status: res.status };
    }
    const json = await res.json();
    return json;
  } catch (err) {
    console.error("fetchVentasOrden error", err);
    return { error: err.message };
  }
}

export async function fetchOrdenByVenta(idVenta) {
  await ensureServerSession();
  try {
    const ventaIdNum = Number(idVenta);
    if (!Number.isFinite(ventaIdNum) || ventaIdNum <= 0) {
      return { error: "id_venta inválido" };
    }
    const res = await fetch(
      `${API_BASE}/api/ventas/orden-por-venta?id_venta=${encodeURIComponent(ventaIdNum)}`,
      {
        credentials: "include",
      }
    );
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (_err) {
        message = (await res.text()) || "";
      }
      if (!message && res.status === 403) {
        message = "La venta no pertenece al usuario.";
      }
      if (!message && res.status === 404) {
        message = "Venta no encontrada.";
      }
      if (!message && res.status === 401) {
        message = "Usuario no autenticado.";
      }
      console.error("ventas/orden-por-venta response", res.status, message);
      return { error: message || "No se pudo resolver la orden de la venta", status: res.status };
    }
    return await res.json();
  } catch (err) {
    console.error("fetchOrdenByVenta error", err);
    return { error: err.message };
  }
}

export async function notificarSpotifyProveedorPendientes({
  idOrden = null,
  ventaIds = [],
  source = "",
} = {}) {
  await ensureServerSession();
  try {
    const payload = {};
    const ordenNum = Number(idOrden);
    if (Number.isFinite(ordenNum) && ordenNum > 0) payload.id_orden = Math.trunc(ordenNum);
    if (Array.isArray(ventaIds) && ventaIds.length) {
      payload.id_ventas = ventaIds
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value > 0)
        .map((value) => Math.trunc(value));
    }
    const sourceTxt = String(source || "").trim();
    if (sourceTxt) payload.source = sourceTxt;
    if (!payload.id_orden && !(payload.id_ventas || []).length) {
      return { error: "Debes enviar id_orden o id_ventas." };
    }

    const res = await fetch(`${API_BASE}/api/ventas/proveedor/spotify/notificar-pendientes`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (_err) {
        message = (await res.text()) || "";
      }
      if (!message && res.status === 401) message = "Usuario no autenticado.";
      if (!message && res.status === 403) message = "Solo admin/superadmin.";
      console.error("notificarSpotifyProveedorPendientes response", res.status, message);
      return { error: message || "No se pudo notificar al proveedor.", status: res.status };
    }
    return await res.json();
  } catch (err) {
    console.error("notificarSpotifyProveedorPendientes error", err);
    return { error: err.message };
  }
}

export async function entregarGiftCardPendiente(idVenta) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/admin/ventas/entregar-giftcard`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ id_venta: idVenta }),
    });
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (_err) {
        message = (await res.text()) || "";
      }
      return { error: message || "No se pudo entregar la gift card", status: res.status };
    }
    return res.json();
  } catch (err) {
    console.error("entregarGiftCardPendiente error", err);
    return { error: err.message };
  }
}

export async function fetchOrdenDetalle(idOrden) {
  await ensureServerSession();
  try {
    const res = await fetch(
      `${API_BASE}/api/ordenes/detalle?id_orden=${encodeURIComponent(idOrden)}`,
      { credentials: "include" }
    );
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (err) {
        message = (await res.text()) || "";
      }
      if (!message && res.status === 403) {
        message = "Orden no pertenece al usuario.";
      }
      if (!message && res.status === 401) {
        message = "Usuario no autenticado.";
      }
      console.error("ordenes/detalle response", res.status, message);
      return { error: message || "No se pudo cargar el detalle de la orden", status: res.status };
    }
    return res.json();
  } catch (err) {
    console.error("ordenes/detalle error", err);
    return { error: err.message };
  }
}

export async function fetchEntregadas() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/ventas/entregadas`, {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("entregadas response", res.status, text);
      return { error: text || "No se pudo cargar entregas" };
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo cargar entregas:", err);
    return { error: err.message };
  }
}

export async function fetchPendingReminderNoPhoneClients() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/whatsapp/recordatorios/pending-no-phone`, {
      credentials: "include",
    });
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (_err) {
        message = (await res.text()) || "";
      }
      return {
        error: message || "No se pudo cargar clientes pendientes sin teléfono",
        status: res.status,
      };
    }
    return res.json();
  } catch (err) {
    console.error("fetchPendingReminderNoPhoneClients error", err);
    return { error: err.message };
  }
}

export async function fetchWhatsappQrStatus({ autoStart = false } = {}) {
  await ensureServerSession();
  try {
    const query = new URLSearchParams({
      start: autoStart ? "true" : "false",
    });
    const res = await fetch(`${API_BASE}/api/whatsapp/qr?${query.toString()}`, {
      credentials: "include",
    });
    if (!res.ok) {
      const { text, data } = await readResponseBodySafe(res);
      const message = String(data?.error || text || "").trim();
      return {
        error: message || "No se pudo consultar el estado de WhatsApp",
        status: res.status,
      };
    }
    const { data } = await readResponseBodySafe(res);
    return data || {};
  } catch (err) {
    console.error("fetchWhatsappQrStatus error", err);
    return { error: err.message };
  }
}

export async function fetchWhatsappPersistentWorkerStatus() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/whatsapp/persistent-worker`, {
      credentials: "include",
    });
    if (!res.ok) {
      const { text, data } = await readResponseBodySafe(res);
      const message = String(data?.error || text || "").trim();
      if (res.status === 404) {
        return {
          unsupported: true,
          error: "",
          status: res.status,
        };
      }
      return {
        error: message || "No se pudo consultar el modo persistente de WhatsApp",
        status: res.status,
      };
    }
    const { data } = await readResponseBodySafe(res);
    return data || {};
  } catch (err) {
    console.error("fetchWhatsappPersistentWorkerStatus error", err);
    return { error: err.message };
  }
}

export async function updateWhatsappPersistentWorkerStatus(enabled) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/whatsapp/persistent-worker`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ enabled: enabled === true }),
    });
    if (!res.ok) {
      const { text, data } = await readResponseBodySafe(res);
      const message = String(data?.error || text || "").trim();
      if (res.status === 404) {
        return {
          unsupported: true,
          error: "",
          status: res.status,
        };
      }
      return {
        error: message || "No se pudo actualizar el modo persistente de WhatsApp",
        status: res.status,
      };
    }
    const { data } = await readResponseBodySafe(res);
    return data || {};
  } catch (err) {
    console.error("updateWhatsappPersistentWorkerStatus error", err);
    return { error: err.message };
  }
}

export async function notifyReporteCreatedWhatsapp(idReporte) {
  await ensureServerSession();
  const reportId = Number(idReporte);
  if (!Number.isFinite(reportId) || reportId <= 0) {
    return { error: "id_reporte invalido" };
  }
  try {
    const headers = { "Content-Type": "application/json" };
    const accessToken = getRememberedAccessToken();
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const res = await fetch(`${API_BASE}/api/whatsapp/reportes/notificar`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({ id_reporte: reportId }),
    });
    const { text, data } = await readResponseBodySafe(res);
    if (!res.ok) {
      const message = String(data?.error || text || "").trim();
      return {
        error: message || "No se pudo notificar el reporte por WhatsApp",
        status: res.status,
      };
    }
    return data || { ok: true };
  } catch (err) {
    console.error("notifyReporteCreatedWhatsapp error", err);
    return { error: err.message };
  }
}

export async function notifyReporteSolvedWhatsapp(idReporte) {
  await ensureServerSession();
  const reportId = Number(idReporte);
  if (!Number.isFinite(reportId) || reportId <= 0) {
    return { error: "id_reporte invalido" };
  }
  try {
    const headers = { "Content-Type": "application/json" };
    const accessToken = getRememberedAccessToken();
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const res = await fetch(`${API_BASE}/api/whatsapp/reportes/notificar-solucion`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({ id_reporte: reportId }),
    });
    const { text, data } = await readResponseBodySafe(res);
    if (!res.ok) {
      const message = String(data?.error || text || "").trim();
      return {
        error: message || "No se pudo notificar la solución del reporte por WhatsApp",
        status: res.status,
      };
    }
    return data || { ok: true };
  } catch (err) {
    console.error("notifyReporteSolvedWhatsapp error", err);
    return { error: err.message };
  }
}

export async function notifyReporteIncorrectDataWhatsapp(
  idReporte,
  { correo = false, clave = false, imagen = "" } = {},
) {
  await ensureServerSession();
  const reportId = Number(idReporte);
  if (!Number.isFinite(reportId) || reportId <= 0) {
    return { error: "id_reporte invalido" };
  }
  const payload = {
    id_reporte: reportId,
    correo: correo === true,
    clave: clave === true,
    imagen: String(imagen || "").trim(),
  };
  if (!payload.correo && !payload.clave) {
    return { error: "Debe seleccionar al menos Correo o Contraseña." };
  }
  try {
    const headers = { "Content-Type": "application/json" };
    const accessToken = getRememberedAccessToken();
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const res = await fetch(`${API_BASE}/api/whatsapp/reportes/notificar-datos-incorrectos`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify(payload),
    });
    const { text, data } = await readResponseBodySafe(res);
    if (!res.ok) {
      const message = String(data?.error || text || "").trim();
      return {
        error: message || "No se pudo notificar datos incorrectos por WhatsApp",
        status: res.status,
      };
    }
    return data || { ok: true };
  } catch (err) {
    console.error("notifyReporteIncorrectDataWhatsapp error", err);
    return { error: err.message };
  }
}

export async function notifyVentaAdminReportWhatsapp(
  idVenta,
  { correo = false, clave = false, imagen = "", modo = "datos_incorrectos" } = {},
) {
  await ensureServerSession();
  const ventaId = Number(idVenta);
  if (!Number.isFinite(ventaId) || ventaId <= 0) {
    return { error: "id_venta invalido" };
  }
  const mode = String(modo || "datos_incorrectos").trim().toLowerCase();
  const modeValue = mode === "problemas_correo" ? "problemas_correo" : "datos_incorrectos";
  const payload = {
    id_venta: ventaId,
    correo: correo === true,
    clave: clave === true,
    imagen: String(imagen || "").trim(),
    modo: modeValue,
  };
  if (modeValue === "datos_incorrectos" && !payload.correo && !payload.clave) {
    return { error: "Debe seleccionar al menos Correo o Contraseña." };
  }
  try {
    const headers = { "Content-Type": "application/json" };
    const accessToken = getRememberedAccessToken();
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const res = await fetch(`${API_BASE}/api/whatsapp/ventas/notificar-reporte-admin`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify(payload),
    });
    const { text, data } = await readResponseBodySafe(res);
    if (!res.ok) {
      const message = String(data?.error || text || "").trim();
      return {
        error: message || "No se pudo enviar el reporte de admin por WhatsApp",
        status: res.status,
      };
    }
    return data || { ok: true };
  } catch (err) {
    console.error("notifyVentaAdminReportWhatsapp error", err);
    return { error: err.message };
  }
}

export async function notifyVentaDeliveredWhatsapp(idVenta) {
  await ensureServerSession();
  const ventaId = Number(idVenta);
  if (!Number.isFinite(ventaId) || ventaId <= 0) {
    return { error: "id_venta invalido" };
  }
  try {
    const headers = { "Content-Type": "application/json" };
    const accessToken = getRememberedAccessToken();
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const res = await fetch(`${API_BASE}/api/whatsapp/ventas/notificar-entregada`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({ id_venta: ventaId }),
    });
    const { text, data } = await readResponseBodySafe(res);
    if (!res.ok) {
      const message = String(data?.error || text || "").trim();
      return {
        error: message || "No se pudo notificar la orden entregada por WhatsApp",
        status: res.status,
      };
    }
    return data || { ok: true };
  } catch (err) {
    console.error("notifyVentaDeliveredWhatsapp error", err);
    return { error: err.message };
  }
}

export async function fetchP2PRate() {
  const startedAt = getDebugNow();
  logApiDebug("fetchP2PRate:start", {});
  try {
    const res = await fetch(`${API_BASE}/api/p2p/rate`, {
      credentials: "include",
      cache: "no-store",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("p2p rate response", res.status, text);
      warnApiDebug("fetchP2PRate:response", {
        ms: getElapsedMs(startedAt),
        status: res.status,
        body: trimText(text, 300),
      });
      return null;
    }
    const data = await res.json();
    const rateValue = Number.isFinite(Number(data?.tasa_actual))
      ? Number(data.tasa_actual)
      : Number.isFinite(Number(data?.rate))
        ? Number(data.rate)
        : null;
    logApiDebug("fetchP2PRate:done", {
      ms: getElapsedMs(startedAt),
      rate: rateValue,
    });
    return rateValue;
  } catch (err) {
    console.error("No se pudo obtener la tasa P2P:", err);
    console.error(`${API_DEBUG_PREFIX} fetchP2PRate:error`, {
      ms: getElapsedMs(startedAt),
      ...summarizeError(err),
    });
    return null;
  }
}

export async function fetchP2PMarkup() {
  try {
    const res = await fetch(`${API_BASE}/api/p2p/markup`, {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("p2p markup response", res.status, text);
      return null;
    }
    const data = await res.json();
    return Number.isFinite(data?.markup) ? data.markup : null;
  } catch (err) {
    console.error("No se pudo obtener el markup de tasa:", err);
    return null;
  }
}

export async function updateP2PMarkup(markup) {
  try {
    const res = await fetch(`${API_BASE}/api/p2p/markup`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({ markup }),
    });
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (_err) {
        message = (await res.text()) || "";
      }
      return { error: message || "No se pudo actualizar el markup de tasa", status: res.status };
    }
    const data = await res.json();
    return { markup: Number.isFinite(data?.markup) ? data.markup : null };
  } catch (err) {
    console.error("No se pudo actualizar el markup de tasa:", err);
    return { error: err.message };
  }
}

export async function fetchTestingFlag() {
  try {
    const { data, error } = await supabase
      .from("testing")
      .select("testing")
      .eq("id_testing", 1)
      .maybeSingle();
    if (error) throw error;
    return data?.testing ?? false;
  } catch (err) {
    console.error("fetchTestingFlag error", err);
    return false;
  }
}

export async function updateTestingFlag(value) {
  try {
    const { data, error } = await supabase
      .from("testing")
      .update({ testing: value })
      .eq("id_testing", 1)
      .select("testing")
      .maybeSingle();
    if (error) throw error;
    return data?.testing ?? value;
  } catch (err) {
    console.error("updateTestingFlag error", err);
    return value;
  }
}

export async function startSession(_idUsuario) {
  const options =
    _idUsuario && typeof _idUsuario === "object" && !Array.isArray(_idUsuario) ? _idUsuario : {};
  const startedAt = getDebugNow();
  const fallbackSessionErrorMessage = "No se pudo validar la sesión por un problema de conexión.";
  const timeoutMs = Math.max(1000, Number(options?.timeoutMs) || 6000);
  logApiDebug("startSession:start", {
    apiBase: API_BASE,
    path: typeof window !== "undefined" ? window.location.pathname : "",
  });
  try {
    let accessToken = rememberAccessToken(
      options?.accessToken || options?.access_token || "",
      options?.source || "startSession:provided",
    );

    if (!accessToken) {
      accessToken = getRememberedAccessToken();
      if (accessToken) {
        logApiDebug("startSession:usingRememberedAccessToken", {
          ms: getElapsedMs(startedAt),
          tokenLength: accessToken.length,
        });
      }
    }

    if (!accessToken) {
      const cooldownRemainingMs = getAuthSessionCooldownRemainingMs();
      if (cooldownRemainingMs > 0) {
        warnApiDebug("startSession:authSessionCooldown", {
          ms: getElapsedMs(startedAt),
          cooldownRemainingMs,
        });
        return {
          error: authSessionFailureMessage || fallbackSessionErrorMessage,
        };
      }
      const sessionResolution = await resolveAccessTokenFromSupabaseSession({
        timeoutMs,
        startedAt,
        fallbackSessionErrorMessage,
        source: "startSession:getSession",
      });
      if (sessionResolution?.error && !sessionResolution?.accessToken) {
        return { error: sessionResolution.error };
      }
      accessToken = sessionResolution?.accessToken || "";
    }

    if (!accessToken) {
      warnApiDebug("startSession:missingAccessToken", {
        ms: getElapsedMs(startedAt),
      });
      return { error: "Sesión de auth no disponible" };
    }

    let sessionResponse = await postServerSessionWithAccessToken(accessToken);
    if (
      !sessionResponse.res.ok &&
      isAuthFatalErrorMessage(sessionResponse.data?.error || sessionResponse.text || "")
    ) {
      clearRememberedAccessToken("startSession:staleAccessToken");
      const refreshedSession = await resolveAccessTokenFromSupabaseSession({
        timeoutMs,
        startedAt,
        fallbackSessionErrorMessage,
        source: "startSession:refreshSession",
        forceRefresh: true,
      });
      const refreshedToken = normalizeAccessToken(refreshedSession?.accessToken);
      if (refreshedToken && refreshedToken !== accessToken) {
        sessionResponse = await postServerSessionWithAccessToken(refreshedToken);
        accessToken = refreshedToken;
      }
    }

    if (!sessionResponse.res.ok) {
      console.error(
        "startSession response",
        sessionResponse.res.status,
        sessionResponse.text,
      );
      warnApiDebug("startSession:response", {
        ms: getElapsedMs(sessionResponse.requestStartedAt),
        totalMs: getElapsedMs(startedAt),
        status: sessionResponse.res.status,
        body: trimText(sessionResponse.text, 300),
      });
      return {
        error:
          sessionResponse.data?.error ||
          sessionResponse.text ||
          "No se pudo establecer la sesión",
      };
    }
    const payload =
      sessionResponse.data && typeof sessionResponse.data === "object" ? sessionResponse.data : {};
    clearAuthSessionFailureCooldown();
    logApiDebug("startSession:done", {
      ms: getElapsedMs(startedAt),
      requestMs: getElapsedMs(sessionResponse.requestStartedAt),
      id_usuario: Number(payload?.id_usuario) || null,
    });
    return payload;
  } catch (err) {
    rememberAuthSessionTransientFailure(err, fallbackSessionErrorMessage);
    console.error("startSession error", err);
    console.error(`${API_DEBUG_PREFIX} startSession:error`, {
      ms: getElapsedMs(startedAt),
      ...summarizeError(err),
    });
    return {
      error: getFriendlyApiErrorMessage(err, fallbackSessionErrorMessage),
    };
  }
}

export async function clearServerSession() {
  try {
    clearRememberedAccessToken("clearServerSession");
    await fetch(`${API_BASE}/api/session`, {
      method: "DELETE",
      credentials: "include",
    });
    // Fallback: limpia cookie en caso de que el delete falle silenciosamente
    document.cookie = "session_user_id=; Max-Age=0; path=/";
  } catch (err) {
    console.error("clearServerSession error", err);
  }
}

async function handleFatalAuthSessionError(reason = "") {
  if (authRecoveryInProgress) return;
  authRecoveryInProgress = true;
  try {
    console.warn("[auth] sesión inválida, cerrando sesión automáticamente", {
      reason: String(reason || "").trim(),
    });
    try {
      await supabase.auth.signOut();
    } catch (_err) {
      // noop
    }
    clearSession();
    clearSupabaseLocalArtifacts();
    await clearServerSession();
  } finally {
    if (isAnonymousAllowedPath()) {
      return;
    }
    const loginUrl = new URL(getLoginHref());
    const currentParams = new URLSearchParams(window.location.search || "");
    const renewalToken = String(currentParams.get("rr") || "").trim();
    if (renewalToken) {
      loginUrl.searchParams.set("rr", renewalToken);
    }
    const loginHref = loginUrl.toString();
    const current = `${window.location.origin}${window.location.pathname}${window.location.search}`;
    if (current !== `${loginUrl.origin}${loginUrl.pathname}${loginUrl.search}`) {
      window.location.replace(loginHref);
    }
  }
}

export async function ensureServerSession() {
  if (ensureServerSessionInFlight) return ensureServerSessionInFlight;

  const currentRun = (async () => {
    const startedAt = getDebugNow();
    const id = requireSession();
    logApiDebug("ensureServerSession:start", {
      sessionUserId: id,
    });
    if (!id) {
      if (!isAnonymousAllowedPath()) {
        await handleFatalAuthSessionError("session_user_id ausente");
      }
      throw new Error("Sesión no disponible");
    }
    const result = await startSession(id);
    if (result?.error) {
      warnApiDebug("ensureServerSession:failed", {
        ms: getElapsedMs(startedAt),
        sessionUserId: id,
        error: result.error,
      });
      if (isRecoverableAuthBridgeError(result.error)) {
        const fallbackUser = await fetchCurrentUserServer({
          allowFallback: true,
        });
        if (fallbackUser?.id_usuario) {
          logApiDebug("ensureServerSession:recoverWithServerCookie", {
            ms: getElapsedMs(startedAt),
            sessionUserId: id,
            id_usuario: Number(fallbackUser.id_usuario) || null,
          });
          return {
            ok: true,
            id_usuario: Number(fallbackUser.id_usuario) || null,
            user: fallbackUser,
            recovered: true,
          };
        }
      }
      if (isAuthFatalErrorMessage(result.error)) {
        await handleFatalAuthSessionError(result.error);
      }
      throw new Error(result.error);
    }
    logApiDebug("ensureServerSession:done", {
      ms: getElapsedMs(startedAt),
      sessionUserId: id,
      id_usuario: Number(result?.id_usuario) || null,
    });
    return result;
  })();

  ensureServerSessionInFlight = currentRun;
  try {
    return await currentRun;
  } finally {
    if (ensureServerSessionInFlight === currentRun) {
      ensureServerSessionInFlight = null;
    }
  }
}

export async function fetchCurrentUserServer(options = {}) {
  const startedAt = getDebugNow();
  logApiDebug("fetchCurrentUserServer:start", {
    expectedId: Number(options?.expectedId) || null,
  });
  try {
    const res = await fetchWithRetry(
      `${API_BASE}/api/session/user`,
      {
        credentials: "include",
      },
      { attempts: 2, delayMs: 300, label: "fetchCurrentUserServer" },
    );
    if (!res.ok) {
      const text = await res.text();
      warnApiDebug("fetchCurrentUserServer:response", {
        ms: getElapsedMs(startedAt),
        status: res.status,
        body: trimText(text, 300),
      });
      return null;
    }
    const payload = await res.json().catch(() => ({}));
    const user = payload?.user && typeof payload.user === "object" ? payload.user : null;
    logApiDebug("fetchCurrentUserServer:done", {
      ms: getElapsedMs(startedAt),
      found: Boolean(user),
      id_usuario: Number(user?.id_usuario) || null,
    });
    return user;
  } catch (err) {
    console.error(`${API_DEBUG_PREFIX} fetchCurrentUserServer:error`, {
      ms: getElapsedMs(startedAt),
      ...summarizeError(err),
    });
    return null;
  }
}

export async function loadCurrentUser() {
  const startedAt = getDebugNow();
  const idUsuario = requireSession();
  logApiDebug("loadCurrentUser:start", {
    idUsuario,
  });
  if (!idUsuario) {
    warnApiDebug("loadCurrentUser:missingSession", {});
    return null;
  }
  const data = await fetchCurrentUserServer({ expectedId: idUsuario });
  if (!data) return null;
  logApiDebug("loadCurrentUser:done", {
    ms: getElapsedMs(startedAt),
    idUsuario,
    found: Boolean(data),
  });
  return data;
}

export { supabase, API_BASE };
