const { getWhatsappClient } = require("./client");

const toWhatsappChatId = (phone) => {
  if (!phone) return null;
  const digits = String(phone).replace(/\D+/g, "");
  if (!digits) return null;
  return `${digits}@c.us`;
};

const sendWhatsappText = async ({ phone, message }) => {
  if (!message || !String(message).trim()) {
    throw new Error("El mensaje es obligatorio.");
  }

  const chatId = toWhatsappChatId(phone);
  if (!chatId) {
    throw new Error("Numero de telefono invalido.");
  }

  const client = getWhatsappClient();
  const sent = await client.sendMessage(chatId, String(message));

  return {
    ok: true,
    chatId,
    messageId: sent?.id?._serialized || null,
  };
};

module.exports = {
  toWhatsappChatId,
  sendWhatsappText,
};
