const os = require("os");
const path = require("path");
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
const DEFAULT_RUNTIME_ROOT = path.join(os.homedir(), ".mooseplus-runtime", "whatsapp");

const resolveWhatsappRuntimeRoot = () => {
  const customRoot = String(process.env.WHATSAPP_RUNTIME_DIR || "").trim();
  return customRoot ? path.resolve(customRoot) : DEFAULT_RUNTIME_ROOT;
};

const WHATSAPP_RUNTIME_ROOT = resolveWhatsappRuntimeRoot();
const WHATSAPP_AUTH_PATH = path.join(WHATSAPP_RUNTIME_ROOT, "auth");
const WHATSAPP_CACHE_PATH = path.join(WHATSAPP_RUNTIME_ROOT, "cache");

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

  clientInstance = new Client({
    authStrategy: new LocalAuth({
      clientId: "mooseplus-admin",
      dataPath: WHATSAPP_AUTH_PATH,
    }),
    webVersionCache: {
      type: "local",
      path: WHATSAPP_CACHE_PATH,
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

  clientInstance.on("disconnected", (reason) => {
    console.warn("[WhatsApp] Cliente desconectado:", reason);
    hasInitialized = false;
    clearWhatsappQrState();
    notifyDisconnectedListeners(reason);
  });

  return clientInstance;
};

const startWhatsappClient = async () => {
  if (initializePromise) return initializePromise;

  const client = getWhatsappClient();
  if (hasInitialized) return client;

  hasInitialized = true;
  initializePromise = client
    .initialize()
    .then(() => client)
    .catch((err) => {
      hasInitialized = false;
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

const stopWhatsappClient = async () => {
  const client = clientInstance;
  hasInitialized = false;
  initializePromise = null;
  clientInstance = null;
  clearWhatsappQrState();
  if (!client) return;
  try {
    await client.destroy();
  } catch (err) {
    console.error("[WhatsApp] destroy error:", err);
  }
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
  getWhatsappQrState,
  onWhatsappReady,
  onWhatsappDisconnected,
};
