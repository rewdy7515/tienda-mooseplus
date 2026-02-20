const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcode = require("qrcode-terminal");

let clientInstance = null;
let hasInitialized = false;

const getWhatsappClient = () => {
  if (clientInstance) return clientInstance;

  clientInstance = new Client({
    authStrategy: new LocalAuth({
      clientId: "mooseplus-admin",
      dataPath: ".wwebjs_auth",
    }),
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
  });

  clientInstance.on("auth_failure", (msg) => {
    console.error("[WhatsApp] Error de autenticacion:", msg);
  });

  clientInstance.on("disconnected", (reason) => {
    console.warn("[WhatsApp] Cliente desconectado:", reason);
    hasInitialized = false;
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

module.exports = {
  getWhatsappClient,
  startWhatsappClient,
  isWhatsappReady,
};
