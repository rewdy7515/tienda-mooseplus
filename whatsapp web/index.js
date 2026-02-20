const {
  getWhatsappClient,
  startWhatsappClient,
  isWhatsappReady,
} = require("./client");
const { sendWhatsappText, toWhatsappChatId } = require("./send-message");

module.exports = {
  getWhatsappClient,
  startWhatsappClient,
  isWhatsappReady,
  sendWhatsappText,
  toWhatsappChatId,
};
