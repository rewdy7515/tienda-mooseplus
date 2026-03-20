const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcode = require("qrcode-terminal");

let clientInstance = null;
let hasInitialized = false;
let initializePromise = null;
let latestQrRaw = "";
let latestQrAscii = "";
let latestQrUpdatedAt = null;
const readyListeners = new Set();
const disconnectedListeners = new Set();
const PROJECT_ROOT = path.resolve(__dirname, "..");
const DEFAULT_PROJECT_AUTH_PATH = path.join(PROJECT_ROOT, ".wwebjs_auth");
const DEFAULT_PROJECT_CACHE_PATH = path.join(PROJECT_ROOT, ".wwebjs_cache");
const DEFAULT_PROJECT_RUNTIME_ROOT = path.join(PROJECT_ROOT, ".mooseplus-runtime", "whatsapp");

const normalizeAbsolutePath = (value = "") => {
  const raw = String(value || "").trim();
  return raw ? path.resolve(raw) : "";
};

const uniqPaths = (values = []) =>
  Array.from(
    new Set(
      (values || [])
        .map((value) => normalizeAbsolutePath(value))
        .filter(Boolean),
    ),
  );

const buildWhatsappStorageCandidates = () => {
  const customRuntimeRoot = normalizeAbsolutePath(process.env.WHATSAPP_RUNTIME_DIR);
  const customAuthPath = normalizeAbsolutePath(process.env.WHATSAPP_AUTH_PATH);
  const customCachePath = normalizeAbsolutePath(process.env.WHATSAPP_CACHE_PATH);
  const tmpRoot = path.join(os.tmpdir(), "mooseplus-runtime", "whatsapp");

  return {
    authPaths: uniqPaths([
      customAuthPath,
      customRuntimeRoot ? path.join(customRuntimeRoot, "auth") : "",
      DEFAULT_PROJECT_AUTH_PATH,
      path.join(DEFAULT_PROJECT_RUNTIME_ROOT, "auth"),
      path.join(tmpRoot, "auth"),
    ]),
    cachePaths: uniqPaths([
      customCachePath,
      customRuntimeRoot ? path.join(customRuntimeRoot, "cache") : "",
      DEFAULT_PROJECT_CACHE_PATH,
      path.join(DEFAULT_PROJECT_RUNTIME_ROOT, "cache"),
      path.join(tmpRoot, "cache"),
    ]),
  };
};

const WHATSAPP_STORAGE_CANDIDATES = buildWhatsappStorageCandidates();
let resolvedWhatsappPaths = {
  authPath: WHATSAPP_STORAGE_CANDIDATES.authPaths[0] || DEFAULT_PROJECT_AUTH_PATH,
  cachePath: WHATSAPP_STORAGE_CANDIDATES.cachePaths[0] || DEFAULT_PROJECT_CACHE_PATH,
};
let resolvedWhatsappPathsReady = false;

const ensureWritableDirFromCandidates = async (candidates = [], label = "dir") => {
  let lastErr = null;
  for (const candidate of candidates) {
    try {
      await fs.mkdir(candidate, { recursive: true });
      return candidate;
    } catch (err) {
      lastErr = err;
      console.warn(
        `[WhatsApp] No se pudo preparar ${label} en ${candidate}: ${err?.message || err}`,
      );
    }
  }
  throw lastErr || new Error(`No se pudo preparar un directorio para ${label}`);
};

const ensureWhatsappRuntimeDirs = async () => {
  if (resolvedWhatsappPathsReady) return resolvedWhatsappPaths;

  const authPath = await ensureWritableDirFromCandidates(
    WHATSAPP_STORAGE_CANDIDATES.authPaths,
    "auth",
  );
  const cachePath = await ensureWritableDirFromCandidates(
    WHATSAPP_STORAGE_CANDIDATES.cachePaths,
    "cache",
  );

  resolvedWhatsappPaths = { authPath, cachePath };
  resolvedWhatsappPathsReady = true;
  console.log(
    `[WhatsApp] Runtime paths auth=${resolvedWhatsappPaths.authPath} cache=${resolvedWhatsappPaths.cachePath}`,
  );
  return resolvedWhatsappPaths;
};

const resetWhatsappClientState = async ({
  destroyClient = false,
  reason = "reset",
} = {}) => {
  const client = clientInstance;
  hasInitialized = false;
  initializePromise = null;
  clientInstance = null;
  clearWhatsappQrState();
  if (!destroyClient || !client) return;
  try {
    await client.destroy();
  } catch (err) {
    console.error(`[WhatsApp] destroy error (${reason}):`, err);
  }
};

const clearWhatsappQrState = () => {
  latestQrRaw = "";
  latestQrAscii = "";
  latestQrUpdatedAt = null;
};

const setWhatsappQrState = (rawQr = "", asciiQr = "") => {
  latestQrRaw = String(rawQr || "");
  latestQrAscii = String(asciiQr || "");
  latestQrUpdatedAt = new Date().toISOString();
};

const getWhatsappQrState = () => ({
  raw: latestQrRaw || null,
  ascii: latestQrAscii || null,
  updatedAt: latestQrUpdatedAt || null,
});

const notifyReadyListeners = () => {
  readyListeners.forEach((listener) => {
    try {
      listener();
    } catch (err) {
      console.error("[WhatsApp] ready listener error:", err);
    }
  });
};

const notifyDisconnectedListeners = (reason) => {
  disconnectedListeners.forEach((listener) => {
    try {
      listener(reason);
    } catch (err) {
      console.error("[WhatsApp] disconnected listener error:", err);
    }
  });
};

const getWhatsappClient = () => {
  if (clientInstance) return clientInstance;
  const { authPath, cachePath } = resolvedWhatsappPaths;

  clientInstance = new Client({
    authStrategy: new LocalAuth({
      clientId: "mooseplus-admin",
      dataPath: authPath,
    }),
    webVersionCache: {
      type: "local",
      path: cachePath,
    },
    puppeteer: {
      args:
        process.platform === "linux" && typeof process.getuid === "function" && process.getuid() === 0
          ? ["--no-sandbox", "--disable-setuid-sandbox"]
          : [],
    },
  });

  clientInstance.on("qr", (qr) => {
    qrcode.generate(qr, { small: true }, (asciiQr) => {
      setWhatsappQrState(qr, asciiQr);
    });
    if (!latestQrRaw) {
      setWhatsappQrState(qr, "");
    }
    console.log("[WhatsApp] Escanea el QR para iniciar sesion.");
  });

  clientInstance.on("authenticated", () => {
    console.log("[WhatsApp] Sesion autenticada.");
    clearWhatsappQrState();
  });

  clientInstance.on("ready", () => {
    console.log("[WhatsApp] Cliente listo.");
    clearWhatsappQrState();
    notifyReadyListeners();
  });

  clientInstance.on("auth_failure", (msg) => {
    console.error("[WhatsApp] Error de autenticacion:", msg);
  });

  clientInstance.on("disconnected", async (reason) => {
    console.warn("[WhatsApp] Cliente desconectado:", reason);
    await resetWhatsappClientState({
      destroyClient: false,
      reason: "disconnected",
    });
    notifyDisconnectedListeners(reason);
  });

  return clientInstance;
};

const startWhatsappClient = async () => {
  if (initializePromise) return initializePromise;

  await ensureWhatsappRuntimeDirs();
  const client = getWhatsappClient();
  if (hasInitialized) return client;

  hasInitialized = true;
  initializePromise = client
    .initialize()
    .then(() => client)
    .catch(async (err) => {
      await resetWhatsappClientState({
        destroyClient: true,
        reason: "initialize_error",
      });
      throw err;
    })
    .finally(() => {
      initializePromise = null;
    });
  return initializePromise;
};

const isWhatsappReady = () => {
  return Boolean(clientInstance?.info?.wid?._serialized);
};

const isWhatsappClientActive = () => {
  return Boolean(isWhatsappReady() || hasInitialized || initializePromise);
};

const stopWhatsappClient = async () => {
  await resetWhatsappClientState({
    destroyClient: true,
    reason: "stop",
  });
};

const onWhatsappReady = (listener) => {
  if (typeof listener !== "function") return () => {};
  readyListeners.add(listener);
  return () => readyListeners.delete(listener);
};

const onWhatsappDisconnected = (listener) => {
  if (typeof listener !== "function") return () => {};
  disconnectedListeners.add(listener);
  return () => disconnectedListeners.delete(listener);
};

module.exports = {
  getWhatsappClient,
  startWhatsappClient,
  stopWhatsappClient,
  isWhatsappReady,
  isWhatsappClientActive,
  getWhatsappQrState,
  onWhatsappReady,
  onWhatsappDisconnected,
};
