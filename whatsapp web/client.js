const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcode = require("qrcode-terminal");

let clientInstance = null;
let hasInitialized = false;
const readyListeners = new Set();
const disconnectedListeners = new Set();

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
      dataPath: ".wwebjs_auth",
    }),
    puppeteer: {
      args:
        process.platform === "linux" && typeof process.getuid === "function" && process.getuid() === 0
          ? ["--no-sandbox", "--disable-setuid-sandbox"]
          : [],
    },
  });

  clientInstance.on("qr", (qr) => {
    qrcode.generate(qr, { small: true });
    console.log("[WhatsApp] Escanea el QR para iniciar sesion.");
  });

  clientInstance.on("authenticated", () => {
    console.log("[WhatsApp] Sesion autenticada.");
  });

  clientInstance.on("ready", () => {
    console.log("[WhatsApp] Cliente listo.");
    notifyReadyListeners();
  });

  clientInstance.on("auth_failure", (msg) => {
    console.error("[WhatsApp] Error de autenticacion:", msg);
  });

  clientInstance.on("disconnected", (reason) => {
    console.warn("[WhatsApp] Cliente desconectado:", reason);
    hasInitialized = false;
    notifyDisconnectedListeners(reason);
  });

  return clientInstance;
};

const startWhatsappClient = async () => {
  const client = getWhatsappClient();
  if (hasInitialized) return client;

  hasInitialized = true;
  await client.initialize();
  return client;
};

const isWhatsappReady = () => {
  return Boolean(clientInstance?.info?.wid?._serialized);
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
  isWhatsappReady,
  onWhatsappReady,
  onWhatsappDisconnected,
};
