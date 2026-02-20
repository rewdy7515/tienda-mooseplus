// Catálogo centralizado de plantillas de notificaciones.
// Esta capa mantiene el API ESM para frontend, usando un core compartido
// que también puede ser consumido por backend.
import "./notification-templates-core.js";

const core = globalThis.NotificationTemplatesCore;
if (!core) {
  throw new Error("NotificationTemplatesCore no disponible");
}

export const notificationTemplates = core.notificationTemplates;
export const buildNotification = core.buildNotification;
export const buildNotificationPayload = core.buildNotificationPayload;
export const pickNotificationUserIds = core.pickNotificationUserIds;
