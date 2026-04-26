const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs/promises");
const webpush = require("web-push");
const { supabaseAdmin } = require("../database/db");
const {
  port,
  webPushVapidPublicKey,
  webPushVapidPrivateKey,
  webPushSubject,
} = require("../../config/config");
const {
  startWhatsappClient,
  stopWhatsappClient,
  getWhatsappClient,
  isWhatsappReady,
  isWhatsappClientActive,
  getWhatsappQrState,
  onWhatsappReady,
  onWhatsappDisconnected,
} = require("../../whatsapp web/client");
const {
  buildNotificationPayload,
} = require("../frontend/scripts/notification-templates-core");

const app = express();
app.disable("x-powered-by");
app.use((_, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  next();
});
const ALLOWED_CORS_ORIGINS = (() => {
  const defaults = [
    "https://mooseplus.com",
    "https://www.mooseplus.com",
    "http://localhost:3000",
    "http://localhost:5500",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5500",
  ];
  const fromEnv = String(process.env.CORS_ORIGINS || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  return Array.from(new Set([...defaults, ...fromEnv]));
})();

const isAllowedCorsOrigin = (origin = "") => {
  const value = String(origin || "").trim();
  if (!value) return false;
  if (ALLOWED_CORS_ORIGINS.includes(value)) return true;
  return /^https:\/\/([a-z0-9-]+\.)*mooseplus\.com$/i.test(value);
};

const isAllowedPublicRedirectUrl = (rawUrl = "") => {
  const value = String(rawUrl || "").trim();
  if (!value) return false;
  try {
    const parsed = new URL(value);
    if (!/^https?:$/i.test(parsed.protocol)) return false;
    const host = String(parsed.hostname || "").trim().toLowerCase();
    return /^([a-z0-9-]+\.)*mooseplus\.com$/i.test(host);
  } catch (_err) {
    return false;
  }
};

const PUBLIC_SITE_URL = (() => {
  const raw = String(process.env.PUBLIC_SITE_URL || process.env.SITE_URL || "https://mooseplus.com")
    .trim()
    .replace(/\/+$/g, "");
  if (/^https?:\/\/[^/]+$/i.test(raw)) {
    try {
      const parsed = new URL(raw);
      const host = String(parsed.hostname || "").trim().toLowerCase();
      if (host === "www.mooseplus.com") {
        parsed.hostname = "mooseplus.com";
      }
      return parsed.toString().replace(/\/+$/g, "");
    } catch (_err) {
      return raw;
    }
  }
  return "https://mooseplus.com";
})();

const buildPublicSiteUrl = () => PUBLIC_SITE_URL;

app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || isAllowedCorsOrigin(origin)) {
        return callback(null, true);
      }
      return callback(null, false);
    },
    credentials: true,
  })
);
const jsonParser = express.json({ limit: "25mb" });

const shouldStartWhatsapp =
  process.env.ENABLE_WHATSAPP === "true" && process.env.VERCEL !== "1";
const INTERNAL_WORKER_TRIGGER_TOKEN = String(process.env.INTERNAL_WORKER_TRIGGER_TOKEN || "")
  .trim();
const BINANCE_GMAIL_PAYMENTS_TABLE = "binance_gmail_pagos";
const BINANCE_GMAIL_APP_NAME = "GMAIL_BINANCE";
const BINANCE_GMAIL_SYNC_ENABLED =
  process.env.BINANCE_GMAIL_SYNC_ENABLED === "true" && process.env.VERCEL !== "1";
const BINANCE_GMAIL_SYNC_INTERVAL_MS = Math.max(
  15000,
  Number(process.env.BINANCE_GMAIL_SYNC_INTERVAL_MS) || 45000,
);
const BINANCE_GMAIL_MAX_RESULTS = Math.max(
  1,
  Math.min(100, Number(process.env.BINANCE_GMAIL_MAX_RESULTS) || 20),
);
const BINANCE_GMAIL_LOOKBACK_MINUTES = Math.max(
  10,
  Number(process.env.BINANCE_GMAIL_LOOKBACK_MINUTES) || 240,
);
const BINANCE_GMAIL_ORDER_WINDOW_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.BINANCE_GMAIL_ORDER_WINDOW_MS) || 3 * 60 * 60 * 1000,
);
const BINANCE_GMAIL_AMOUNT_TOLERANCE = Math.max(
  0,
  Number(process.env.BINANCE_GMAIL_AMOUNT_TOLERANCE) || 0.01,
);
const BINANCE_GMAIL_LABEL_IDS = String(process.env.BINANCE_GMAIL_LABEL_IDS || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const BINANCE_GMAIL_QUERY = String(
  process.env.BINANCE_GMAIL_QUERY ||
    'from:(binance.com) ("received" OR "recibido" OR "payment" OR "pago" OR "paid")',
).trim();
const GMAIL_OAUTH_CLIENT_ID = String(process.env.GMAIL_OAUTH_CLIENT_ID || "").trim();
const GMAIL_OAUTH_CLIENT_SECRET = String(process.env.GMAIL_OAUTH_CLIENT_SECRET || "").trim();
const GMAIL_OAUTH_REDIRECT_URI = String(process.env.GMAIL_OAUTH_REDIRECT_URI || "").trim();
const GMAIL_OAUTH_SCOPES = String(
  process.env.GMAIL_OAUTH_SCOPES || "https://www.googleapis.com/auth/gmail.readonly",
)
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const BINANCE_GMAIL_OAUTH_STATE_TTL_MS = Math.max(
  60 * 1000,
  Number(process.env.BINANCE_GMAIL_OAUTH_STATE_TTL_MS) || 10 * 60 * 1000,
);
const BINANCE_GMAIL_METODO_CACHE_MS = Math.max(
  10 * 1000,
  Number(process.env.BINANCE_GMAIL_METODO_CACHE_MS) || 5 * 60 * 1000,
);
const BINANCE_UNIQUE_AMOUNT_ENABLED = process.env.BINANCE_UNIQUE_AMOUNT_ENABLED !== "false";
const BINANCE_UNIQUE_AMOUNT_MAX_DECIMAL_STEP = Math.max(
  1,
  Math.min(9, Number(process.env.BINANCE_UNIQUE_AMOUNT_MAX_DECIMAL_STEP) || 9),
);
const BINANCE_UNIQUE_AMOUNT_MIN_PENDING = Math.max(
  1,
  Number(process.env.BINANCE_UNIQUE_AMOUNT_MIN_PENDING) || 1,
);

const hasValidInternalWorkerTriggerToken = (req = {}) => {
  if (!INTERNAL_WORKER_TRIGGER_TOKEN) return false;
  const token =
    String(
      req.headers?.["x-worker-token"] ||
      req.body?.worker_token ||
      req.query?.worker_token ||
      "",
    ).trim();
  return token.length > 0 && token === INTERNAL_WORKER_TRIGGER_TOKEN;
};

const clearCorsHeaders = (res) => {
  res.removeHeader("Access-Control-Allow-Origin");
  res.removeHeader("Access-Control-Allow-Credentials");
  res.removeHeader("Access-Control-Allow-Headers");
  res.removeHeader("Access-Control-Allow-Methods");
};

const WHATSAPP_AUTO_RECORDATORIOS_ENABLED =
  process.env.WHATSAPP_AUTO_RECORDATORIOS !== "false";
const WHATSAPP_AUTO_RECORDATORIOS_WEEKDAY_HOUR = 10;
const WHATSAPP_AUTO_RECORDATORIOS_WEEKEND_HOUR = 11;
const WHATSAPP_CUTOFF_PLATFORM_12_ID = 12;
const WHATSAPP_CUTOFF_PLATFORM_12_MESSAGE = `*Su cuenta de Canva Pro ha vencido* 🎨

En caso de no renovar, tendrá 24 horas para copiar sus diseños a su equipo principal. Transcurrido el tiempo se excluirá como miembro del equipo y se eliminaran los diseños *permanentemente*

*Para copiar un diseño o carpeta a tu equipo principal debes:*

1. Seleccionar el diseño o carpeta deseada y pulsar los 3 puntos.
2. Seleccionar “Copiar a otro equipo”.
3. Eligir tu cuenta (por ejemplo: “Equipo de Andrés Villarreal”).
4. Pulsar “Copiar”.

*‼️Por favor tomar previsiones*‼️`;
const WHATSAPP_SEND_DELAY_MIN_MS = 8000;
const WHATSAPP_SEND_DELAY_MAX_MS = 15000;
const WHATSAPP_SEND_TIMEOUT_MS = Math.max(
  15000,
  Number(process.env.WHATSAPP_SEND_TIMEOUT_MS) || 45000,
);
const WHATSAPP_READY_TIMEOUT_MS = Math.max(
  15000,
  Number(process.env.WHATSAPP_READY_TIMEOUT_MS) || 120000,
);
const WHATSAPP_QR_WAIT_TIMEOUT_MS = Math.max(
  1200,
  Number(process.env.WHATSAPP_QR_WAIT_TIMEOUT_MS) || 3000,
);
const WHATSAPP_QR_WAIT_POLL_MS = Math.max(
  120,
  Number(process.env.WHATSAPP_QR_WAIT_POLL_MS) || 250,
);
const WHATSAPP_RECORDATORIOS_LOCK_STALE_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.WHATSAPP_RECORDATORIOS_LOCK_STALE_MS) || 20 * 60 * 1000,
);
const WHATSAPP_PERSISTENT_SYNC_INTERVAL_MS = Math.max(
  15000,
  Number(process.env.WHATSAPP_PERSISTENT_SYNC_INTERVAL_MS) || 60000,
);
const WHATSAPP_RESET_HOUR = 0;
const WHATSAPP_HETZNER_PERSISTENT_CFG_KEY = "whatsapp_hetzner_persistent_worker";
let lastAutoRecordatoriosRunDate = null;
let autoRecordatoriosRetryPending = false;
let autoRecordatoriosRunInProgress = false;
let recordatoriosSendInProgress = false;
let recordatoriosSendLockMeta = {
  source: null,
  startedAt: null,
  heartbeatAt: null,
  mode: null,
  total: 0,
  processed: 0,
  sent: 0,
  failed: 0,
  skippedNoPhone: 0,
  skippedInvalidPhone: 0,
  lastCliente: null,
};
let recordatoriosEnviados = false;
let recordatoriosEnviadosDate = null;
let autoRecordatoriosSchedulerStarted = false;
let autoRecordatoriosIntervalId = null;
let whatsappBootInProgress = false;
let whatsappHetznerPersistentWorkerEnabled = false;
let whatsappHetznerPersistentWorkerLoaded = false;
let whatsappPersistentWorkerSyncInProgress = false;
let whatsappPersistentWorkerSyncIntervalId = null;
let lastAutoRecordatoriosState = {
  date: null,
  status: "idle",
  attemptedAt: null,
  completedAt: null,
  total: 0,
  sent: 0,
  failed: 0,
  skippedNoPhone: 0,
  skippedInvalidPhone: 0,
  updatedVentas: 0,
  sameDayCutoffTotal: 0,
  sameDayCutoffSent: 0,
  sameDayCutoffFailed: 0,
  sameDayPlatform12Total: 0,
  sameDayPlatform12Sent: 0,
  sameDayPlatform12Failed: 0,
  updatedVentasCorteHoy: 0,
  recordatoriosEnviados: false,
  error: null,
};
const NUEVO_SERVICIO_NOTIF_QUEUE_ENABLED =
  process.env.NUEVO_SERVICIO_NOTIF_QUEUE !== "false" && process.env.VERCEL !== "1";
const NUEVO_SERVICIO_NOTIF_QUEUE_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.NUEVO_SERVICIO_NOTIF_QUEUE_INTERVAL_MS) || 15000,
);
const NUEVO_SERVICIO_NOTIF_QUEUE_BATCH = Math.max(
  1,
  Math.min(50, Number(process.env.NUEVO_SERVICIO_NOTIF_QUEUE_BATCH) || 20),
);
const NUEVO_SERVICIO_NOTIF_QUEUE_TABLE = "eventos_notificacion_nuevo_servicio";
const WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID = 23;
const WHATSAPP_MANUAL_VERIFICATION_ALERT_TITLE = "Verificación manual de pago";
const WHATSAPP_MANUAL_VERIFICATION_WATCHER_ENABLED =
  process.env.WHATSAPP_MANUAL_VERIFICATION_WATCHER !== "false" && process.env.VERCEL !== "1";
const WHATSAPP_MANUAL_VERIFICATION_WATCHER_INTERVAL_MS = Math.max(
  10000,
  Number(process.env.WHATSAPP_MANUAL_VERIFICATION_WATCHER_INTERVAL_MS) || 30000,
);
const WHATSAPP_MANUAL_VERIFICATION_WINDOW_MS = Math.max(
  60000,
  Number(process.env.WHATSAPP_MANUAL_VERIFICATION_WINDOW_MS) || 3 * 60 * 1000,
);
const WHATSAPP_MANUAL_VERIFICATION_WATCHER_BATCH = Math.max(
  1,
  Math.min(300, Number(process.env.WHATSAPP_MANUAL_VERIFICATION_WATCHER_BATCH) || 120),
);
const WHATSAPP_PEDIDO_PENDIENTE_WATCHER_ENABLED =
  process.env.WHATSAPP_PEDIDO_PENDIENTE_WATCHER !== "false" && process.env.VERCEL !== "1";
const WHATSAPP_PEDIDO_PENDIENTE_WATCHER_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.WHATSAPP_PEDIDO_PENDIENTE_WATCHER_INTERVAL_MS) || 15000,
);
const WHATSAPP_PEDIDO_PENDIENTE_WATCHER_BATCH = Math.max(
  1,
  Math.min(200, Number(process.env.WHATSAPP_PEDIDO_PENDIENTE_WATCHER_BATCH) || 80),
);
const WHATSAPP_PEDIDOS_PENDIENTES_GROUP_NAME = String(
  process.env.WHATSAPP_PEDIDOS_PENDIENTES_GROUP_NAME || "Pedidos Pendientes Moose+",
).trim();
const WHATSAPP_PEDIDOS_PENDIENTES_GROUP_CHAT_ID = String(
  process.env.WHATSAPP_PEDIDOS_PENDIENTES_GROUP_CHAT_ID || "",
).trim();
const WHATSAPP_REPORTES_GROUP_NAME = String(
  process.env.WHATSAPP_REPORTES_GROUP_NAME || "Reportes moose+",
).trim();
const WHATSAPP_REPORTES_GROUP_CHAT_ID = String(
  process.env.WHATSAPP_REPORTES_GROUP_CHAT_ID || "",
).trim();
const WEB_PUSH_SUBSCRIPTIONS_TABLE = "web_push_subscriptions";
const WEB_PUSH_DELIVERY_QUEUE_TABLE = "web_push_delivery_queue";
const SANDBOX_GIFTCARD_ORDERS_TABLE = "sandbox_giftcard_orders";
const SANDBOX_GIFTCARD_ORDER_ITEMS_TABLE = "sandbox_giftcard_order_items";
const SANDBOX_GIFTCARD_HISTORY_TABLE = "sandbox_giftcard_historial_ventas";
const WEB_PUSH_ENABLED = process.env.WEB_PUSH_NOTIFICATIONS !== "false";
const WEB_PUSH_QUEUE_WORKER_ENABLED = WEB_PUSH_ENABLED && process.env.VERCEL !== "1";
const WEB_PUSH_QUEUE_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.WEB_PUSH_QUEUE_INTERVAL_MS) || 15000,
);
const WEB_PUSH_QUEUE_BATCH = Math.max(
  1,
  Math.min(100, Number(process.env.WEB_PUSH_QUEUE_BATCH) || 30),
);
const WEB_PUSH_QUEUE_MAX_RETRIES = Math.max(
  1,
  Math.min(10, Number(process.env.WEB_PUSH_QUEUE_MAX_RETRIES) || 3),
);
const AUTO_GIFTCARD_PENDING_DELIVERY_ENABLED =
  process.env.AUTO_GIFTCARD_PENDING_DELIVERY !== "false" && process.env.VERCEL !== "1";
const AUTO_GIFTCARD_PENDING_DELIVERY_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.AUTO_GIFTCARD_PENDING_DELIVERY_INTERVAL_MS) || 15000,
);
const AUTO_GIFTCARD_PENDING_DELIVERY_BATCH = Math.max(
  1,
  Math.min(500, Number(process.env.AUTO_GIFTCARD_PENDING_DELIVERY_BATCH) || 120),
);
const AUTO_PENDING_STOCK_DELIVERY_ENABLED =
  process.env.AUTO_PENDING_STOCK_DELIVERY !== "false" && process.env.VERCEL !== "1";
const AUTO_PENDING_STOCK_DELIVERY_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.AUTO_PENDING_STOCK_DELIVERY_INTERVAL_MS) || 15000,
);
const AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_ENABLED =
  process.env.AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS !== "false" && process.env.VERCEL !== "1";
const AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_INTERVAL_MS) || 5000,
);
const AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_BATCH = Math.max(
  1,
  Math.min(300, Number(process.env.AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_BATCH) || 120),
);
const WHATSAPP_REPORTES_WATCHER_ENABLED =
  process.env.WHATSAPP_REPORTES_WATCHER !== "false" && process.env.VERCEL !== "1";
const WHATSAPP_REPORTES_WATCHER_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.WHATSAPP_REPORTES_WATCHER_INTERVAL_MS) || 15000,
);
const WHATSAPP_REPORTES_WATCHER_BATCH = Math.max(
  1,
  Math.min(300, Number(process.env.WHATSAPP_REPORTES_WATCHER_BATCH) || 80),
);
const HOME_BANNERS_TABLE = "banners";
const WEB_TRAFFIC_EVENTS_TABLE = "eventos_trafico_web";
const WEB_TRAFFIC_DAILY_TABLE = "metricas_trafico_web_diario";
const WEB_TRAFFIC_HOURLY_TABLE = "metricas_trafico_web_horario";
const MONTHLY_METRICS_TABLE = "metricas_mensuales";
const MONTHLY_ACTIVE_CLIENTS_TABLE = "metricas_clientes_activos_mes";
const MONTHLY_METRICS_CLOSURE_ENABLED = process.env.MONTHLY_METRICS_CLOSURE !== "false";
const MONTHLY_METRICS_CLOSURE_INTERVAL_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.MONTHLY_METRICS_CLOSURE_INTERVAL_MS) || 60 * 60 * 1000,
);
let nuevoServicioNotifQueueInProgress = false;
let nuevoServicioNotifQueueTableMissing = false;
let nuevoServicioNotifQueueLastRunAt = null;
let nuevoServicioNotifQueueLastResult = {
  fetched: 0,
  sent: 0,
  duplicates: 0,
  failed: 0,
  invalidUser: 0,
};
let nuevoServicioNotifQueueLastError = null;
let pendingSpotifyAlertsInProgress = false;
let pendingSpotifyAlertsAvisoAdminMissing = false;
let deliveredOrderWhatsappAvisoPagoMissing = false;
let pendingSpotifyAlertsLastRunAt = null;
let pendingSpotifyAlertsLastResult = {
  fetched: 0,
  sent: 0,
  failed: 0,
  skipped: 0,
};
let pendingSpotifyAlertsLastError = null;
let manualVerificationWatcherInProgress = false;
let manualVerificationWatcherLastRunAt = null;
let manualVerificationWatcherLastResult = {
  fetched: 0,
  sentWhatsapp: 0,
  notifCreated: 0,
  alreadyNotified: 0,
  skippedRecent: 0,
  skippedNoDate: 0,
  failed: 0,
  autoFetched: 0,
  autoSentWhatsapp: 0,
  autoAlreadyNotified: 0,
  autoSkippedRecent: 0,
  autoSkippedNoDate: 0,
  autoSkippedNoSale: 0,
  autoFailed: 0,
};
let manualVerificationWatcherLastError = null;
let reportesWhatsappWatcherInProgress = false;
let reportesWhatsappWatcherColumnMissing = false;
let reportesWhatsappWatcherLastRunAt = null;
let reportesWhatsappWatcherLastResult = {
  scanned: 0,
  sent: 0,
  skipped: 0,
  failed: 0,
  markedSent: 0,
};
let reportesWhatsappWatcherLastError = null;
const WEB_PUSH_IS_CONFIGURED =
  Boolean(String(webPushVapidPublicKey || "").trim()) &&
  Boolean(String(webPushVapidPrivateKey || "").trim());
let webPushQueueInProgress = false;
let webPushQueueTableMissing = false;
let webPushQueueLastRunAt = null;
let webPushQueueLastResult = {
  fetched: 0,
  sent: 0,
  skipped: 0,
  failed: 0,
  removedSubscriptions: 0,
};
let webPushQueueLastError = null;
let autoGiftCardPendingDeliveryInProgress = false;
let autoGiftCardPendingDeliveryInitialized = false;
let autoGiftCardPendingDeliveryCursor = 0;
let autoPendingStockDeliveryInProgress = false;
let autoCreditedPagoMovilOrderProcessInProgress = false;
let binanceGmailSyncInProgress = false;
let binanceGmailSyncLastRunAt = null;
let binanceGmailSyncLastError = null;
let binanceGmailSyncLastResult = {
  fetched: 0,
  parsed: 0,
  stored: 0,
  duplicates: 0,
  matched: 0,
  processed: 0,
  skipped: 0,
  failed: 0,
};
let cachedGmailRefreshToken = String(process.env.GMAIL_OAUTH_REFRESH_TOKEN || "").trim();
let cachedBinanceMetodoPagoIds = { ids: [], ts: 0 };
const binanceGmailOauthStateMap = new Map();
let monthlyMetricsClosureInProgress = false;
const ORDER_PROCESS_LOCK_STALE_MS = Math.max(
  2 * 60 * 1000,
  Number(process.env.ORDER_PROCESS_LOCK_STALE_MS) || 10 * 60 * 1000,
);
const orderProcessLocks = new Map();
const ORDER_PROCESS_LOCK_OWNER = [
  "backend",
  process.pid,
  Math.random().toString(36).slice(2, 10),
].join("-");
const acquireOrderProcessLock = (idOrden, meta = {}) => {
  const orderId = toPositiveInt(idOrden);
  if (!orderId) return { acquired: false, reason: "invalid_order" };
  const now = Date.now();
  const current = orderProcessLocks.get(orderId);
  if (current) {
    const ageMs = now - Number(current.startedAt || 0);
    if (Number.isFinite(ageMs) && ageMs >= 0 && ageMs > ORDER_PROCESS_LOCK_STALE_MS) {
      console.warn("[ordenes/procesar] lock stale liberado", {
        id_orden: orderId,
        age_ms: ageMs,
      });
      orderProcessLocks.delete(orderId);
    } else {
      return {
        acquired: false,
        reason: "already_processing",
        lock: {
          id_orden: orderId,
          startedAt: current.startedAt || null,
          source: current.source || null,
        },
      };
    }
  }
  const lockInfo = {
    startedAt: now,
    source: String(meta?.source || "api_ordenes_procesar"),
  };
  orderProcessLocks.set(orderId, lockInfo);
  return { acquired: true, id_orden: orderId, lock: lockInfo };
};
const releaseOrderProcessLock = (idOrden) => {
  const orderId = toPositiveInt(idOrden);
  if (!orderId) return;
  orderProcessLocks.delete(orderId);
};
const isMissingOrderProcessLockRpc = (err) => {
  const msg = String(err?.message || "").toLowerCase();
  const code = String(err?.code || "").toLowerCase();
  return code === "pgrst202" || msg.includes("acquire_order_process_lock");
};
const acquireSharedOrderProcessLock = async (idOrden, meta = {}) => {
  const localLock = acquireOrderProcessLock(idOrden, meta);
  if (!localLock?.acquired) return localLock;
  const orderId = toPositiveInt(idOrden);
  const staleSeconds = Math.max(120, Math.floor(ORDER_PROCESS_LOCK_STALE_MS / 1000));
  try {
    const { data, error } = await supabaseAdmin.rpc("acquire_order_process_lock", {
      p_order_id: orderId,
      p_owner: ORDER_PROCESS_LOCK_OWNER,
      p_stale_seconds: staleSeconds,
    });
    if (error) {
      if (isMissingOrderProcessLockRpc(error)) {
        console.warn("[ordenes/procesar] rpc lock no disponible; usando lock local", {
          id_orden: orderId,
          source: localLock?.lock?.source || null,
          error: error?.message || String(error),
        });
        return {
          ...localLock,
          lock: {
            ...(localLock.lock || {}),
            owner: ORDER_PROCESS_LOCK_OWNER,
            shared: false,
            rpc_missing: true,
          },
        };
      }
      releaseOrderProcessLock(orderId);
      return {
        acquired: false,
        reason: "db_lock_error",
        error: error?.message || String(error),
      };
    }
    if (data !== true) {
      releaseOrderProcessLock(orderId);
      return {
        acquired: false,
        reason: "already_processing_shared",
      };
    }
    return {
      ...localLock,
      lock: {
        ...(localLock.lock || {}),
        owner: ORDER_PROCESS_LOCK_OWNER,
        shared: true,
      },
    };
  } catch (err) {
    releaseOrderProcessLock(orderId);
    return {
      acquired: false,
      reason: "db_lock_exception",
      error: err?.message || String(err),
    };
  }
};
const releaseSharedOrderProcessLock = async (idOrden) => {
  const orderId = toPositiveInt(idOrden);
  if (!orderId) return;
  releaseOrderProcessLock(orderId);
  try {
    const { error } = await supabaseAdmin.rpc("release_order_process_lock", {
      p_order_id: orderId,
      p_owner: ORDER_PROCESS_LOCK_OWNER,
    });
    if (error && !isMissingOrderProcessLockRpc(error)) {
      console.warn("[ordenes/procesar] release shared lock error", {
        id_orden: orderId,
        error: error?.message || String(error),
      });
    }
  } catch (err) {
    console.warn("[ordenes/procesar] release shared lock exception", {
      id_orden: orderId,
      error: err?.message || String(err),
    });
  }
};
let monthlyMetricsClosureSchedulerStarted = false;
let monthlyMetricsClosureIntervalId = null;
let monthlyMetricsClosureSchemaMissing = false;
let monthlyMetricsClosureLastRunAt = null;
let monthlyMetricsClosureLastError = null;
let monthlyMetricsClosureLastMonthKey = "";

if (WEB_PUSH_IS_CONFIGURED) {
  try {
    webpush.setVapidDetails(
      String(webPushSubject || "mailto:soporte@mooseplus.com").trim(),
      String(webPushVapidPublicKey || "").trim(),
      String(webPushVapidPrivateKey || "").trim(),
    );
  } catch (err) {
    console.error("[WebPush] No se pudieron configurar las claves VAPID", err);
  }
}

const uniqPositiveIds = (values = []) =>
  Array.from(
    new Set(
      (values || [])
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value > 0),
    ),
  );

const isMissingHistorialGiftCardColumnError = (err) => {
  const message = String(err?.message || err?.details || "").toLowerCase();
  if (!message) return false;
  return (
    message.includes("id_tarjeta_de_regalo") &&
    message.includes("historial_ventas") &&
    message.includes("schema cache")
  );
};

const isMissingHistorialGiftCardRelationError = (err) => {
  const message = String(err?.message || err?.details || "").toLowerCase();
  if (!message) return false;
  return (
    message.includes("historial_ventas") &&
    message.includes("tarjetas_de_regalo") &&
    message.includes("relationship") &&
    message.includes("schema cache")
  );
};

const isMissingHistorialGiftCardSchemaError = (err) =>
  isMissingHistorialGiftCardColumnError(err) || isMissingHistorialGiftCardRelationError(err);

const stripHistorialGiftCardColumn = (rows = []) =>
  (rows || []).map((row) => {
    if (!row || typeof row !== "object") return row;
    const nextRow = { ...row };
    delete nextRow.id_tarjeta_de_regalo;
    return nextRow;
  });

const normalizeGiftCardFaceValue = (value) => {
  const raw = String(value ?? "")
    .trim()
    .replace(/\s+/g, "")
    .replace(",", ".");
  if (!raw) return null;
  const amount = Number(raw);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return Math.round((amount + Number.EPSILON) * 100) / 100;
};

const fetchAvailableGiftCardStock = async ({
  idPlataforma,
  valorTarjetaDeRegalo,
  limit = 1,
} = {}) => {
  const platId = toPositiveInt(idPlataforma);
  const monto = normalizeGiftCardFaceValue(valorTarjetaDeRegalo);
  const queryLimit = Math.max(1, Math.min(500, Number(limit) || 1));
  if (!platId || !Number.isFinite(monto) || monto <= 0) {
    return [];
  }

  const { data, error } = await supabaseAdmin
    .from("tarjetas_de_regalo")
    .select("id_tarjeta_de_regalo, id_plataforma, pin, para_venta, usado, monto")
    .eq("id_plataforma", platId)
    .eq("para_venta", true)
    .eq("usado", false)
    .eq("monto", monto)
    .order("id_tarjeta_de_regalo", { ascending: true })
    .limit(queryLimit);
  if (error) throw error;
  return data || [];
};

const buildGiftCardSaleCopyText = ({
  plataformaNombre = "",
  idVenta = null,
  region = "",
  valorTarjeta = "",
  moneda = "",
  pin = "",
} = {}) => {
  const lines = [];
  const plataformaTxt = String(plataformaNombre || "").trim().toUpperCase();
  const idVentaTxt = toPositiveInt(idVenta);
  const regionTxt = String(region || "").trim() || "-";
  const valueParts = [String(valorTarjeta || "").trim(), String(moneda || "").trim()].filter(Boolean);
  const valueTxt = valueParts.join(" ") || "-";
  const pinTxt = String(pin || "").trim() || "Pendiente";

  if (plataformaTxt || idVentaTxt) {
    lines.push(
      `*${plataformaTxt || "GIFT CARD"}*${idVentaTxt ? ` 🫎 \`ID Venta: #${idVentaTxt}\`` : ""}`.trim(),
    );
  }
  lines.push(`(Región: ${regionTxt})`);
  lines.push("_Pagina Web: https://mooseplus.com_");
  lines.push("");
  lines.push(`\`${valueTxt}\``);
  lines.push(`PIN: ${pinTxt}`);
  return lines.join("\n");
};

const giftCardDeliveryError = (code, message) => {
  const err = new Error(message || code);
  err.code = code;
  return err;
};

const isMissingVentasIdOrdenColumnError = (err) => {
  const message = String(err?.message || err?.details || "").toLowerCase();
  if (!message) return false;
  return (
    message.includes("ventas") &&
    message.includes("id_orden") &&
    (message.includes("does not exist") || message.includes("schema cache"))
  );
};

const fetchVentasByOrder = async ({
  idOrden = null,
  select = "id_venta",
  onlyPending = false,
  limit = null,
  ascending = true,
} = {}) => {
  const orderId = toPositiveInt(idOrden);
  if (!orderId) return [];

  const selectCols = String(select || "id_venta").trim() || "id_venta";
  const limitNum = toPositiveInt(limit);
  const orderAscending = ascending !== false;

  const { data: snapshotRows, error: snapshotErr } = await supabaseAdmin
    .from("ordenes_items")
    .select("id_venta")
    .eq("id_orden", orderId)
    .not("id_venta", "is", null);
  if (snapshotErr) throw snapshotErr;

  const { data: historialRows, error: historialErr } = await supabaseAdmin
    .from("historial_ventas")
    .select("id_venta")
    .eq("id_orden", orderId)
    .not("id_venta", "is", null);
  if (historialErr) throw historialErr;

  const ventaIds = uniqPositiveIds([
    ...(snapshotRows || []).map((row) => row?.id_venta),
    ...(historialRows || []).map((row) => row?.id_venta),
  ]);
  if (ventaIds.length) {
    let query = supabaseAdmin
      .from("ventas")
      .select(selectCols)
      .in("id_venta", ventaIds)
      .order("id_venta", { ascending: orderAscending });
    if (onlyPending) query = query.eq("pendiente", true);
    if (limitNum > 0) query = query.limit(limitNum);
    const { data, error } = await query;
    if (error) throw error;
    return data || [];
  }

  // Fallback legacy para órdenes históricas sin snapshot en ordenes_items.
  let fallbackQuery = supabaseAdmin
    .from("ventas")
    .select(selectCols)
    .eq("id_orden", orderId)
    .order("id_venta", { ascending: orderAscending });
  if (onlyPending) fallbackQuery = fallbackQuery.eq("pendiente", true);
  if (limitNum > 0) fallbackQuery = fallbackQuery.limit(limitNum);
  const { data: fallbackData, error: fallbackErr } = await fallbackQuery;
  if (fallbackErr) {
    if (isMissingVentasIdOrdenColumnError(fallbackErr)) return [];
    throw fallbackErr;
  }
  return fallbackData || [];
};

const hasHistorialVentasForOrder = async (idOrden) => {
  const orderId = toPositiveInt(idOrden);
  if (!orderId) return false;
  const { data, error } = await supabaseAdmin
    .from("historial_ventas")
    .select("id_historial_ventas")
    .eq("id_orden", orderId)
    .limit(1);
  if (error) throw error;
  return Array.isArray(data) && data.length > 0;
};

const resolveOrderIdByVentaId = async ({ idVenta = null, fallbackOrderId = null } = {}) => {
  const saleId = toPositiveInt(idVenta);
  const orderIdHint = toPositiveInt(fallbackOrderId);
  if (orderIdHint) return orderIdHint;
  if (!saleId) return null;

  const { data: itemRow, error: itemErr } = await supabaseAdmin
    .from("ordenes_items")
    .select("id_orden, id_item_orden")
    .eq("id_venta", saleId)
    .not("id_orden", "is", null)
    .order("id_item_orden", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (itemErr) throw itemErr;
  const orderIdFromItem = toPositiveInt(itemRow?.id_orden);
  if (orderIdFromItem) return orderIdFromItem;

  const { data: histRow, error: histErr } = await supabaseAdmin
    .from("historial_ventas")
    .select("id_orden, id_historial_ventas")
    .eq("id_venta", saleId)
    .not("id_orden", "is", null)
    .order("id_historial_ventas", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (histErr) throw histErr;
  const orderIdFromHist = toPositiveInt(histRow?.id_orden);
  if (orderIdFromHist) return orderIdFromHist;

  const { data: ventaRow, error: ventaErr } = await supabaseAdmin
    .from("ventas")
    .select("id_orden")
    .eq("id_venta", saleId)
    .maybeSingle();
  if (ventaErr) {
    if (isMissingVentasIdOrdenColumnError(ventaErr)) return null;
    throw ventaErr;
  }

  return toPositiveInt(ventaRow?.id_orden) || null;
};

const syncOrderPendingStateById = async (idOrden) => {
  const ordenId = toPositiveInt(idOrden);
  if (!ordenId) return;
  const pendingRows = await fetchVentasByOrder({
    idOrden: ordenId,
    select: "id_venta",
    onlyPending: true,
  });

  const { error: updOrdenErr } = await supabaseAdmin
    .from("ordenes")
    .update({ en_espera: (pendingRows || []).length > 0 })
    .eq("id_orden", ordenId);
  if (updOrdenErr) throw updOrdenErr;
};

const fetchGiftCardPriceIdsByPlatformAndAmount = async ({
  idPlataforma,
  monto,
} = {}) => {
  const platId = toPositiveInt(idPlataforma);
  const montoNorm = normalizeGiftCardFaceValue(monto);
  if (!platId || !Number.isFinite(montoNorm) || montoNorm <= 0) return [];

  const { data: priceRows, error: priceErr } = await supabaseAdmin
    .from("precios")
    .select("id_precio, valor_tarjeta_de_regalo")
    .eq("id_plataforma", platId)
    .not("valor_tarjeta_de_regalo", "is", null);
  if (priceErr) throw priceErr;

  return uniqPositiveIds(
    (priceRows || [])
      .filter((row) => normalizeGiftCardFaceValue(row?.valor_tarjeta_de_regalo) === montoNorm)
      .map((row) => row?.id_precio),
  );
};

const findNextPendingGiftCardVentaIdByPriceIds = async (priceIds = []) => {
  const ids = uniqPositiveIds(priceIds);
  if (!ids.length) return null;

  const { data, error } = await supabaseAdmin
    .from("ventas")
    .select("id_venta")
    .eq("pendiente", true)
    .is("id_tarjeta_de_regalo", null)
    .in("id_precio", ids)
    .order("id_venta", { ascending: true })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return toPositiveInt(data?.id_venta);
};

const deliverPendingGiftCardVenta = async ({
  idVenta,
  idTarjetaDeRegalo = null,
  adminInvolucradoId = null,
  source = "manual",
} = {}) => {
  const ventaId = toPositiveInt(idVenta);
  if (!ventaId) {
    throw giftCardDeliveryError("INVALID_ID_VENTA", "id_venta inválido");
  }

  const { data: venta, error: ventaErr } = await supabaseAdmin
    .from("ventas")
    .select(
      `
      id_venta,
      id_usuario,
      id_precio,
      id_tarjeta_de_regalo,
      pendiente,
      precios:precios(id_precio, id_plataforma, plan, valor_tarjeta_de_regalo, region, moneda),
      tarjetas_de_regalo:tarjetas_de_regalo!ventas_id_tarjeta_de_regalo_fkey(
        id_tarjeta_de_regalo,
        pin,
        vendido_a,
        monto,
        usado,
        para_venta
      )
    `,
    )
    .eq("id_venta", ventaId)
    .maybeSingle();
  if (ventaErr) throw ventaErr;
  if (!venta) {
    throw giftCardDeliveryError("GIFT_CARD_SALE_NOT_FOUND", "Venta no encontrada");
  }
  if (!isTrue(venta?.pendiente)) {
    throw giftCardDeliveryError("GIFT_CARD_SALE_NOT_PENDING", "La venta ya no está pendiente.");
  }

  const idUsuarioVenta = toPositiveInt(venta?.id_usuario);
  const idOrden = await resolveOrderIdByVentaId({ idVenta: ventaId });
  const idPlataforma = toPositiveInt(venta?.precios?.id_plataforma);
  if (!idUsuarioVenta || !idPlataforma) {
    throw giftCardDeliveryError(
      "GIFT_CARD_SALE_INVALID_CONTEXT",
      "La venta no tiene plataforma o usuario válidos.",
    );
  }

  const { data: plataformaRow, error: plataformaErr } = await supabaseAdmin
    .from("plataformas")
    .select("id_plataforma, nombre, tarjeta_de_regalo")
    .eq("id_plataforma", idPlataforma)
    .maybeSingle();
  if (plataformaErr) throw plataformaErr;
  if (!isTrue(plataformaRow?.tarjeta_de_regalo)) {
    throw giftCardDeliveryError(
      "GIFT_CARD_SALE_NOT_GIFTCARD",
      "La venta indicada no es una gift card.",
    );
  }

  const expectedMonto = normalizeGiftCardFaceValue(venta?.precios?.valor_tarjeta_de_regalo);
  let giftCardCandidate = null;

  const requestedCardId = toPositiveInt(idTarjetaDeRegalo);
  if (requestedCardId) {
    const { data: row, error: rowErr } = await supabaseAdmin
      .from("tarjetas_de_regalo")
      .select("id_tarjeta_de_regalo, id_plataforma, pin, monto, para_venta, usado")
      .eq("id_tarjeta_de_regalo", requestedCardId)
      .maybeSingle();
    if (rowErr) throw rowErr;
    if (!row) {
      throw giftCardDeliveryError("GIFT_CARD_NOT_FOUND", "La gift card seleccionada no existe.");
    }
    if (toPositiveInt(row?.id_plataforma) !== idPlataforma) {
      throw giftCardDeliveryError(
        "GIFT_CARD_PLATFORM_MISMATCH",
        "La gift card no coincide con la plataforma de la venta.",
      );
    }
    if (!isTrue(row?.para_venta)) {
      throw giftCardDeliveryError(
        "GIFT_CARD_NOT_FOR_SALE",
        "La gift card indicada no está disponible para venta.",
      );
    }
    if (isTrue(row?.usado)) {
      throw giftCardDeliveryError("GIFT_CARD_ALREADY_USED", "La gift card indicada ya fue usada.");
    }
    const rowMonto = normalizeGiftCardFaceValue(row?.monto);
    if (
      Number.isFinite(expectedMonto) &&
      expectedMonto > 0 &&
      (!Number.isFinite(rowMonto) || rowMonto !== expectedMonto)
    ) {
      throw giftCardDeliveryError(
        "GIFT_CARD_AMOUNT_MISMATCH",
        "La gift card no coincide con el monto de la venta.",
      );
    }
    giftCardCandidate = row;
  } else {
    const [giftCardRow] = await fetchAvailableGiftCardStock({
      idPlataforma,
      valorTarjetaDeRegalo: expectedMonto,
      limit: 1,
    });
    if (!giftCardRow?.id_tarjeta_de_regalo) {
      throw giftCardDeliveryError("GIFT_CARD_STOCK_EMPTY", "No hay stock disponible para esta gift card.");
    }
    giftCardCandidate = giftCardRow;
  }

  const selectedCardId = toPositiveInt(giftCardCandidate?.id_tarjeta_de_regalo);
  if (!selectedCardId) {
    throw giftCardDeliveryError("GIFT_CARD_NOT_FOUND", "No se pudo resolver la gift card para la entrega.");
  }

  const fechaUso = todayInVenezuela();
  const { data: updatedGiftRow, error: updGiftErr } = await supabaseAdmin
    .from("tarjetas_de_regalo")
    .update({
      usado: true,
      vendido_a: idUsuarioVenta,
      fecha_uso: fechaUso,
      admin_involucrado: toPositiveInt(adminInvolucradoId) || null,
      id_orden: idOrden || null,
    })
    .eq("id_tarjeta_de_regalo", selectedCardId)
    .eq("id_plataforma", idPlataforma)
    .eq("para_venta", true)
    .eq("usado", false)
    .select("id_tarjeta_de_regalo, pin")
    .maybeSingle();
  if (updGiftErr) throw updGiftErr;
  if (!updatedGiftRow?.id_tarjeta_de_regalo) {
    throw giftCardDeliveryError(
      "GIFT_CARD_STOCK_UPDATE_CONFLICT",
      "La gift card ya no está disponible para entrega.",
    );
  }

  const pinGiftCard = String(updatedGiftRow?.pin || giftCardCandidate?.pin || "").trim();

  const { error: histUpdErr } = await supabaseAdmin
    .from("historial_ventas")
    .update({ id_tarjeta_de_regalo: selectedCardId })
    .eq("id_venta", ventaId);
  if (histUpdErr && !isMissingHistorialGiftCardColumnError(histUpdErr)) throw histUpdErr;

  const plataformaNombre = String(plataformaRow?.nombre || "").trim() || `Plataforma ${idPlataforma}`;
  const giftRegion = String(venta?.precios?.region || "").trim();
  const giftValue = String(venta?.precios?.valor_tarjeta_de_regalo || "").trim();
  const giftCurrency = String(venta?.precios?.moneda || "").trim();

  const notifPayload = buildNotificationPayload(
    "nuevo_servicio",
    {
      items: [
        {
          plataforma: plataformaNombre,
          idVenta: ventaId,
          region: giftRegion,
          valorTarjeta: giftValue,
          moneda: giftCurrency,
          pin: pinGiftCard || "Pendiente",
        },
      ],
    },
    { idCuenta: null },
  );
  const { error: notifErr } = await supabaseAdmin.from("notificaciones").insert({
    ...notifPayload,
    id_usuario: idUsuarioVenta,
    id_orden: idOrden || null,
  });
  if (notifErr) throw notifErr;

  const { error: deleteVentaErr } = await supabaseAdmin
    .from("ventas")
    .delete()
    .eq("id_venta", ventaId);
  if (deleteVentaErr) throw deleteVentaErr;

  await syncOrderPendingStateById(idOrden);

  const copyText = buildGiftCardSaleCopyText({
    plataformaNombre,
    idVenta: ventaId,
    region: giftRegion,
    valorTarjeta: giftValue,
    moneda: giftCurrency,
    pin: pinGiftCard || "Pendiente",
  });

  return {
    ok: true,
    id_venta: ventaId,
    id_orden: idOrden || null,
    id_tarjeta_de_regalo: selectedCardId,
    plataforma: plataformaNombre,
    pin: pinGiftCard || "Pendiente",
    copy_text: copyText,
    source,
  };
};

const initAutoGiftCardPendingDeliveryCursor = async () => {
  if (autoGiftCardPendingDeliveryInitialized) return;
  const { data: lastCardRow, error: lastCardErr } = await supabaseAdmin
    .from("tarjetas_de_regalo")
    .select("id_tarjeta_de_regalo")
    .order("id_tarjeta_de_regalo", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (lastCardErr) throw lastCardErr;
  autoGiftCardPendingDeliveryCursor = toPositiveInt(lastCardRow?.id_tarjeta_de_regalo) || 0;
  autoGiftCardPendingDeliveryInitialized = true;
  console.log(
    `[GiftCards] Auto-entrega inicializada. Cursor de inserciones en #${autoGiftCardPendingDeliveryCursor}.`,
  );
};

const processAutoGiftCardPendingDeliveries = async () => {
  if (!AUTO_GIFTCARD_PENDING_DELIVERY_ENABLED) return null;
  if (autoGiftCardPendingDeliveryInProgress) return { skipped: true, reason: "in_progress" };
  autoGiftCardPendingDeliveryInProgress = true;

  try {
    await initAutoGiftCardPendingDeliveryCursor();
    const cursorBase = toPositiveInt(autoGiftCardPendingDeliveryCursor) || 0;
    const { data: insertedRows, error: insertedErr } = await supabaseAdmin
      .from("tarjetas_de_regalo")
      .select("id_tarjeta_de_regalo, id_plataforma, monto, para_venta, usado")
      .gt("id_tarjeta_de_regalo", cursorBase)
      .order("id_tarjeta_de_regalo", { ascending: true })
      .limit(AUTO_GIFTCARD_PENDING_DELIVERY_BATCH);
    if (insertedErr) throw insertedErr;

    const rows = Array.isArray(insertedRows) ? insertedRows : [];
    if (!rows.length) {
      return {
        scanned: 0,
        delivered: 0,
        matched: 0,
        skippedNoMatch: 0,
        failed: 0,
        cursor: cursorBase,
      };
    }

    const priceIdsByKey = new Map();
    let maxSeenId = cursorBase;
    const summary = {
      scanned: rows.length,
      delivered: 0,
      matched: 0,
      skippedNoMatch: 0,
      failed: 0,
    };

    for (const stockRow of rows) {
      const stockId = toPositiveInt(stockRow?.id_tarjeta_de_regalo);
      if (stockId && stockId > maxSeenId) maxSeenId = stockId;

      const platId = toPositiveInt(stockRow?.id_plataforma);
      const stockMonto = normalizeGiftCardFaceValue(stockRow?.monto);
      if (
        !stockId ||
        !platId ||
        !Number.isFinite(stockMonto) ||
        stockMonto <= 0 ||
        !isTrue(stockRow?.para_venta) ||
        isTrue(stockRow?.usado)
      ) {
        summary.skippedNoMatch += 1;
        continue;
      }

      const key = `${platId}::${stockMonto.toFixed(2)}`;
      let matchingPriceIds = priceIdsByKey.get(key);
      if (!matchingPriceIds) {
        matchingPriceIds = await fetchGiftCardPriceIdsByPlatformAndAmount({
          idPlataforma: platId,
          monto: stockMonto,
        });
        priceIdsByKey.set(key, matchingPriceIds);
      }
      if (!matchingPriceIds.length) {
        summary.skippedNoMatch += 1;
        continue;
      }

      const pendingVentaId = await findNextPendingGiftCardVentaIdByPriceIds(matchingPriceIds);
      if (!pendingVentaId) {
        summary.skippedNoMatch += 1;
        continue;
      }
      summary.matched += 1;

      try {
        await deliverPendingGiftCardVenta({
          idVenta: pendingVentaId,
          idTarjetaDeRegalo: stockId,
          adminInvolucradoId: null,
          source: "auto_stock_insert",
        });
        summary.delivered += 1;
      } catch (itemErr) {
        summary.failed += 1;
        console.error("[GiftCards] Auto-entrega item error", {
          stockId,
          pendingVentaId,
          message: itemErr?.message || String(itemErr || ""),
          code: itemErr?.code || "",
        });
      }
    }

    autoGiftCardPendingDeliveryCursor = Math.max(cursorBase, maxSeenId);
    if (summary.delivered > 0 || summary.failed > 0 || summary.matched > 0) {
      console.log(
        `[GiftCards] Auto-entrega por inserciones: scanned=${summary.scanned}, matched=${summary.matched}, delivered=${summary.delivered}, failed=${summary.failed}, skipped=${summary.skippedNoMatch}, cursor=${autoGiftCardPendingDeliveryCursor}`,
      );
    }
    return {
      ...summary,
      cursor: autoGiftCardPendingDeliveryCursor,
    };
  } finally {
    autoGiftCardPendingDeliveryInProgress = false;
  }
};

const attachGiftCardPriceInfo = async (rows = []) => {
  const baseRows = Array.isArray(rows) ? rows : [];
  if (!baseRows.length) return [];

  const platformIds = uniqPositiveIds(baseRows.map((row) => row?.id_plataforma));
  if (!platformIds.length) {
    return baseRows.map((row) => ({ ...row, precio_tarjeta_de_regalo: null }));
  }

  const { data: priceRows, error: priceErr } = await supabaseAdmin
    .from("precios")
    .select("id_precio, id_plataforma, plan, region, valor_tarjeta_de_regalo, moneda")
    .in("id_plataforma", platformIds)
    .not("valor_tarjeta_de_regalo", "is", null);
  if (priceErr) throw priceErr;

  const priceMap = new Map();
  (priceRows || []).forEach((row) => {
    const platId = toPositiveInt(row?.id_plataforma);
    const amount = normalizeGiftCardFaceValue(row?.valor_tarjeta_de_regalo);
    if (!platId || !Number.isFinite(amount) || amount <= 0) return;
    const key = `${platId}::${amount.toFixed(2)}`;
    if (!priceMap.has(key)) {
      priceMap.set(key, row);
    }
  });

  return baseRows.map((row) => {
    const platId = toPositiveInt(row?.id_plataforma);
    const amount = normalizeGiftCardFaceValue(row?.tarjetas_de_regalo?.monto);
    const key = platId && Number.isFinite(amount) && amount > 0 ? `${platId}::${amount.toFixed(2)}` : "";
    return {
      ...row,
      precio_tarjeta_de_regalo: key ? priceMap.get(key) || null : null,
    };
  });
};

const fetchHistorialGiftCardRowsByOrder = async (idOrden, idUsuarioCliente) => {
  const joinedSelect = `
    id_historial_ventas,
    id_venta,
    id_orden,
    id_usuario_cliente,
    renovacion,
    monto,
    id_plataforma,
    id_tarjeta_de_regalo,
    tarjetas_de_regalo:tarjetas_de_regalo!historial_ventas_id_tarjeta_de_regalo_fkey(
      id_tarjeta_de_regalo,
      pin,
      vendido_a,
      monto
    ),
    plataformas:plataformas!historial_ventas_id_plataforma_fkey(
      id_plataforma,
      nombre
    )
  `;
  const fallbackSelect = `
    id_historial_ventas,
    id_venta,
    id_orden,
    id_usuario_cliente,
    renovacion,
    monto,
    id_plataforma,
    id_tarjeta_de_regalo,
    plataformas:plataformas!historial_ventas_id_plataforma_fkey(
      id_plataforma,
      nombre
    )
  `;

  const runJoinedQuery = () =>
    supabaseAdmin
      .from("historial_ventas")
      .select(joinedSelect)
      .eq("id_orden", idOrden)
      .eq("id_usuario_cliente", idUsuarioCliente)
      .not("id_tarjeta_de_regalo", "is", null)
      .order("id_historial_ventas", { ascending: false });

  const runFallbackQuery = () =>
    supabaseAdmin
      .from("historial_ventas")
      .select(fallbackSelect)
      .eq("id_orden", idOrden)
      .eq("id_usuario_cliente", idUsuarioCliente)
      .not("id_tarjeta_de_regalo", "is", null)
      .order("id_historial_ventas", { ascending: false });

  const { data, error } = await runJoinedQuery();
  if (!error) return attachGiftCardPriceInfo(data || []);
  if (isMissingHistorialGiftCardColumnError(error)) return [];
  if (!isMissingHistorialGiftCardRelationError(error)) throw error;

  const { data: fallbackRows, error: fallbackErr } = await runFallbackQuery();
  if (fallbackErr) {
    if (isMissingHistorialGiftCardColumnError(fallbackErr)) return [];
    throw fallbackErr;
  }

  const giftCardIds = uniqPositiveIds((fallbackRows || []).map((row) => row?.id_tarjeta_de_regalo));
  if (!giftCardIds.length) {
    return (fallbackRows || []).map((row) => ({ ...row, tarjetas_de_regalo: null }));
  }

  const { data: giftCardRows, error: giftCardErr } = await supabaseAdmin
    .from("tarjetas_de_regalo")
    .select("id_tarjeta_de_regalo, pin, vendido_a, monto")
    .in("id_tarjeta_de_regalo", giftCardIds);
  if (giftCardErr) throw giftCardErr;

  const giftCardMap = (giftCardRows || []).reduce((acc, row) => {
    const id = toPositiveInt(row?.id_tarjeta_de_regalo);
    if (id) acc[id] = row;
    return acc;
  }, {});

  return attachGiftCardPriceInfo((fallbackRows || []).map((row) => ({
    ...row,
    tarjetas_de_regalo: giftCardMap[toPositiveInt(row?.id_tarjeta_de_regalo)] || null,
  })));
};

const getCaracasDateStr = (offsetDays = 0) => {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const year = Number(parts.find((part) => part.type === "year")?.value || 0);
  const month = Number(parts.find((part) => part.type === "month")?.value || 0);
  const day = Number(parts.find((part) => part.type === "day")?.value || 0);
  const base = new Date(Date.UTC(year, month - 1, day));
  base.setUTCDate(base.getUTCDate() + offsetDays);
  return base.toISOString().slice(0, 10);
};

const getCaracasClock = () => {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    hourCycle: "h23",
  }).formatToParts(now);
  const year = parts.find((part) => part.type === "year")?.value || "0000";
  const month = parts.find((part) => part.type === "month")?.value || "00";
  const day = parts.find((part) => part.type === "day")?.value || "00";
  // Algunos runtimes devuelven medianoche como 24:00; la normalizamos a 00:00
  // para no habilitar envíos antes de la hora programada.
  const rawHour = Number(parts.find((part) => part.type === "hour")?.value || 0);
  const hour = rawHour === 24 ? 0 : rawHour;
  const minute = Number(parts.find((part) => part.type === "minute")?.value || 0);
  const weekday = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Caracas",
    weekday: "short",
  }).format(now);
  return { dateStr: `${year}-${month}-${day}`, hour, minute, weekday };
};

const getWhatsappAutoRecordatoriosHour = ({ weekday } = {}) => {
  const weekdayValue = String(weekday || "").trim().toLowerCase();
  return weekdayValue === "sat" || weekdayValue === "sun"
    ? WHATSAPP_AUTO_RECORDATORIOS_WEEKEND_HOUR
    : WHATSAPP_AUTO_RECORDATORIOS_WEEKDAY_HOUR;
};

const normalizeWhatsappRecordatorioMode = (mode = "pending") => {
  const rawMode = String(mode || "").trim().toLowerCase();
  if (rawMode === "cutoff_today") return "cutoff_today";
  if (rawMode === "cutoff_today_platform_12") return "cutoff_today_platform_12";
  return "pending";
};

const normalizeRecordatorioDiasAntes = (value, fallback = 1) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0 || !Number.isInteger(parsed)) {
    return fallback;
  }
  return parsed;
};

const formatDDMMYYYY = (value) => {
  const str = String(value || "").trim().slice(0, 10);
  const [year, month, day] = str.split("-");
  if (!year || !month || !day) return str || "-";
  return `${day}/${month}/${year}`;
};

const normalizeWhatsappPhone = (rawPhone) => {
  const digits = String(rawPhone || "").replace(/\D/g, "");
  if (!digits) return null;
  if (digits.startsWith("58") && digits.length >= 11) return digits;
  if (digits.length === 11 && digits.startsWith("0")) return `58${digits.slice(1)}`;
  if (digits.length === 10 && digits.startsWith("4")) return `58${digits}`;
  return digits.length >= 10 ? digits : null;
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const withTimeout = (promise, timeoutMs, message = "Operacion expirada") =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      const timeoutErr = new Error(message);
      timeoutErr.code = "TIMEOUT";
      reject(timeoutErr);
    }, timeoutMs);
    Promise.resolve(promise)
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
  });

const trimWebPushText = (value, max = 4000) => {
  const txt = String(value ?? "").trim();
  if (!txt) return "";
  return txt.length > max ? `${txt.slice(0, max)}…` : txt;
};

const decodeHtmlEntities = (value = "") =>
  String(value || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");

const htmlToPlainText = (html = "") => {
  const normalized = String(html || "")
    .replace(/<\s*br\s*\/?>/gi, "\n")
    .replace(/<\s*\/p\s*>/gi, "\n\n")
    .replace(/<[^>]+>/g, " ");
  return trimWebPushText(
    decodeHtmlEntities(normalized)
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .replace(/[ \t]{2,}/g, " ")
      .trim(),
    3000,
  );
};

const buildWebPushTargetUrl = (pathName = "/notificaciones.html") => {
  try {
    return new URL(pathName, buildPublicSiteUrl()).toString();
  } catch (_err) {
    return buildPublicSiteUrl();
  }
};

const buildWebPushPayload = (notification = {}, extra = {}) => {
  const idNotificacion = Number(notification?.id_notificacion);
  const titulo = trimWebPushText(notification?.titulo || extra?.titulo || "Nueva notificación", 120);
  const body = htmlToPlainText(notification?.mensaje || extra?.mensaje || "Tienes una nueva novedad.");
  const url = trimWebPushText(
    extra?.url || buildWebPushTargetUrl("/notificaciones.html"),
    1000,
  );
  return {
    title: titulo || "Nueva notificación",
    body: body || "Tienes una nueva notificación en Moose+.",
    url,
    tag: Number.isFinite(idNotificacion) && idNotificacion > 0 ? `notif-${idNotificacion}` : "notif",
    notificationId: Number.isFinite(idNotificacion) && idNotificacion > 0 ? idNotificacion : null,
    icon: "/assets/favicon/logo-corto-blanco-icono.png",
    badge: "/assets/favicon/logo-corto-blanco-icono.png",
  };
};

const parseWebPushSubscriptionInput = (body = {}) => {
  const payload = body?.subscription && typeof body.subscription === "object"
    ? body.subscription
    : body;
  const endpoint = trimWebPushText(payload?.endpoint || "", 3000);
  const p256dh = trimWebPushText(payload?.keys?.p256dh || body?.p256dh || "", 512);
  const auth = trimWebPushText(payload?.keys?.auth || body?.auth || "", 512);
  const expirationRaw = payload?.expirationTime;
  const expirationTime = Number.isFinite(Number(expirationRaw)) ? Number(expirationRaw) : null;
  const deviceLabel = trimWebPushText(body?.device_label || "", 160) || null;
  const userAgent = trimWebPushText(body?.user_agent || "", 1000) || null;
  if (!endpoint || !p256dh || !auth) {
    const err = new Error("Suscripción push incompleta.");
    err.code = "INVALID_PUSH_SUBSCRIPTION";
    throw err;
  }
  return {
    endpoint,
    p256dh,
    auth,
    expirationTime,
    deviceLabel,
    userAgent,
  };
};

const serializeWebPushError = (err) => {
  const parts = [
    err?.message,
    err?.body,
    err?.details,
    err?.statusCode ? `status ${err.statusCode}` : "",
  ]
    .map((value) => trimWebPushText(value, 500))
    .filter(Boolean);
  return trimWebPushText(parts.join(" | "), 1000) || "Error desconocido";
};

const isObsoleteWebPushError = (err) => {
  const status = Number(err?.statusCode || err?.status || 0);
  return status === 404 || status === 410;
};

const markWebPushQueueItem = async (idQueue, patch = {}) => {
  const queueId = Number(idQueue);
  if (!Number.isFinite(queueId) || queueId <= 0) return;
  const { error } = await supabaseAdmin
    .from(WEB_PUSH_DELIVERY_QUEUE_TABLE)
    .update(patch)
    .eq("id_queue", queueId);
  if (error) throw error;
};

const deleteWebPushSubscription = async (idSubscription) => {
  const subscriptionId = Number(idSubscription);
  if (!Number.isFinite(subscriptionId) || subscriptionId <= 0) return;
  const { error } = await supabaseAdmin
    .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
    .delete()
    .eq("id_subscription", subscriptionId);
  if (error) throw error;
};

const sendWebPushToSubscription = async (subscriptionRow = {}, payload = {}) => {
  if (!WEB_PUSH_ENABLED) {
    const err = new Error("Web push deshabilitado.");
    err.code = "WEB_PUSH_DISABLED";
    throw err;
  }
  if (!WEB_PUSH_IS_CONFIGURED) {
    const err = new Error("Faltan claves VAPID para web push.");
    err.code = "WEB_PUSH_NOT_CONFIGURED";
    throw err;
  }

  const endpoint = trimWebPushText(subscriptionRow?.endpoint || "", 3000);
  const p256dh = trimWebPushText(subscriptionRow?.p256dh || "", 512);
  const auth = trimWebPushText(subscriptionRow?.auth || "", 512);
  if (!endpoint || !p256dh || !auth) {
    const err = new Error("Suscripción push inválida.");
    err.code = "INVALID_PUSH_SUBSCRIPTION";
    throw err;
  }

  await webpush.sendNotification(
    {
      endpoint,
      expirationTime: subscriptionRow?.expiration_time ?? null,
      keys: { p256dh, auth },
    },
    JSON.stringify(payload),
    {
      TTL: 60,
      urgency: "normal",
      topic: trimWebPushText(payload?.tag || "", 32) || undefined,
    },
  );
};

const sendWebPushPayloadToUserSubscriptions = async (idUsuario, payload = {}) => {
  const userId = Number(idUsuario);
  if (!Number.isFinite(userId) || userId <= 0) {
    const err = new Error("id_usuario inválido.");
    err.code = "INVALID_USER_ID";
    throw err;
  }

  const { data: subscriptions, error } = await supabaseAdmin
    .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
    .select("id_subscription, endpoint, p256dh, auth, expiration_time, disabled_at")
    .eq("id_usuario", userId)
    .is("disabled_at", null);
  if (error) throw error;

  const rows = Array.isArray(subscriptions) ? subscriptions : [];
  const result = { total: rows.length, sent: 0, failed: 0, removedSubscriptions: 0 };

  for (const subscriptionRow of rows) {
    try {
      await sendWebPushToSubscription(subscriptionRow, payload);
      result.sent += 1;
    } catch (err) {
      result.failed += 1;
      if (isObsoleteWebPushError(err)) {
        try {
          await deleteWebPushSubscription(subscriptionRow.id_subscription);
          result.removedSubscriptions += 1;
        } catch (deleteErr) {
          console.error("[WebPush] No se pudo borrar suscripción obsoleta", deleteErr);
        }
      } else {
        console.error("[WebPush] Error enviando test push", {
          id_usuario: userId,
          id_subscription: subscriptionRow.id_subscription,
          error: serializeWebPushError(err),
        });
      }
    }
  }

  return result;
};

const processWebPushDeliveryQueue = async () => {
  if (!WEB_PUSH_QUEUE_WORKER_ENABLED || !WEB_PUSH_IS_CONFIGURED) {
    return {
      fetched: 0,
      sent: 0,
      skipped: 0,
      failed: 0,
      removedSubscriptions: 0,
    };
  }
  if (webPushQueueInProgress) {
    return webPushQueueLastResult;
  }

  webPushQueueInProgress = true;
  webPushQueueLastRunAt = new Date().toISOString();
  webPushQueueLastError = null;

  try {
    const { data: queueRows, error: queueErr } = await supabaseAdmin
      .from(WEB_PUSH_DELIVERY_QUEUE_TABLE)
      .select("id_queue, id_notificacion, id_subscription, id_usuario, intentos")
      .eq("estado", "pending")
      .order("id_queue", { ascending: true })
      .limit(WEB_PUSH_QUEUE_BATCH);
    if (queueErr) {
      if (String(queueErr?.code || "") === "42P01") {
        webPushQueueTableMissing = true;
        return webPushQueueLastResult;
      }
      throw queueErr;
    }

    webPushQueueTableMissing = false;
    const rows = Array.isArray(queueRows) ? queueRows : [];
    const notificationIds = uniqPositiveIds(rows.map((row) => row.id_notificacion));
    const subscriptionIds = uniqPositiveIds(rows.map((row) => row.id_subscription));
    const result = {
      fetched: rows.length,
      sent: 0,
      skipped: 0,
      failed: 0,
      removedSubscriptions: 0,
    };

    if (!rows.length) {
      webPushQueueLastResult = result;
      return result;
    }

    const [
      { data: notifications, error: notificationsErr },
      { data: subscriptions, error: subscriptionsErr },
    ] = await Promise.all([
      supabaseAdmin
        .from("notificaciones")
        .select("id_notificacion, titulo, mensaje, fecha, id_usuario")
        .in("id_notificacion", notificationIds),
      supabaseAdmin
        .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
        .select("id_subscription, endpoint, p256dh, auth, expiration_time, disabled_at")
        .in("id_subscription", subscriptionIds),
    ]);
    if (notificationsErr) throw notificationsErr;
    if (subscriptionsErr) throw subscriptionsErr;

    const notificationById = new Map(
      (notifications || []).map((row) => [Number(row.id_notificacion), row]),
    );
    const subscriptionById = new Map(
      (subscriptions || []).map((row) => [Number(row.id_subscription), row]),
    );

    for (const row of rows) {
      const queueId = Number(row.id_queue);
      const attempts = Number(row.intentos || 0) + 1;
      const notification = notificationById.get(Number(row.id_notificacion)) || null;
      const subscription = subscriptionById.get(Number(row.id_subscription)) || null;

      if (!notification || !subscription || subscription?.disabled_at) {
        await markWebPushQueueItem(queueId, {
          estado: "skipped",
          intentos: attempts,
          ultimo_error: "Notificación o suscripción no disponible.",
          procesado_en: new Date().toISOString(),
        });
        result.skipped += 1;
        continue;
      }

      try {
        await sendWebPushToSubscription(subscription, buildWebPushPayload(notification));
        await markWebPushQueueItem(queueId, {
          estado: "sent",
          intentos: attempts,
          ultimo_error: null,
          procesado_en: new Date().toISOString(),
        });
        result.sent += 1;
      } catch (err) {
        if (isObsoleteWebPushError(err)) {
          try {
            await deleteWebPushSubscription(subscription.id_subscription);
            result.removedSubscriptions += 1;
            result.skipped += 1;
          } catch (deleteErr) {
            console.error("[WebPush] No se pudo eliminar la suscripción obsoleta", deleteErr);
            await markWebPushQueueItem(queueId, {
              estado: attempts >= WEB_PUSH_QUEUE_MAX_RETRIES ? "failed" : "pending",
              intentos: attempts,
              ultimo_error: serializeWebPushError(deleteErr),
              procesado_en:
                attempts >= WEB_PUSH_QUEUE_MAX_RETRIES ? new Date().toISOString() : null,
            });
            result.failed += 1;
          }
          continue;
        }

        const finalState = attempts >= WEB_PUSH_QUEUE_MAX_RETRIES ? "failed" : "pending";
        await markWebPushQueueItem(queueId, {
          estado: finalState,
          intentos: attempts,
          ultimo_error: serializeWebPushError(err),
          procesado_en: finalState === "failed" ? new Date().toISOString() : null,
        });
        result.failed += 1;
      }
    }

    webPushQueueLastResult = result;
    return result;
  } catch (err) {
    webPushQueueLastError = serializeWebPushError(err);
    throw err;
  } finally {
    webPushQueueInProgress = false;
  }
};

const randomWhatsappDelayMs = () => {
  return (
    WHATSAPP_SEND_DELAY_MIN_MS +
    Math.floor(
      Math.random() * (WHATSAPP_SEND_DELAY_MAX_MS - WHATSAPP_SEND_DELAY_MIN_MS + 1),
    )
  );
};

const ensureWhatsappClientStarted = async ({
  reason = "unspecified",
  allowWhenDisabled = false,
  waitForInitialize = true,
} = {}) => {
  if (!shouldStartWhatsapp && !allowWhenDisabled) {
    const err = new Error("WhatsApp deshabilitado");
    err.code = "WHATSAPP_DISABLED";
    throw err;
  }
  if (isWhatsappReady()) return true;
  if (whatsappBootInProgress) return false;

  const bootTask = async () => {
    console.log(`[WhatsApp] Iniciando cliente (${reason}).`);
    await startWhatsappClient();
    return isWhatsappReady();
  };

  whatsappBootInProgress = true;
  if (waitForInitialize === false) {
    void bootTask()
      .catch((err) => {
        console.error(`[WhatsApp] init error (${reason}):`, err);
      })
      .finally(() => {
        whatsappBootInProgress = false;
      });
    return false;
  }

  try {
    return await bootTask();
  } catch (err) {
    console.error(`[WhatsApp] init error (${reason}):`, err);
    throw err;
  } finally {
    whatsappBootInProgress = false;
  }
};

const shutdownWhatsappClient = async ({
  reason = "unspecified",
  allowWhenDisabled = false,
  force = false,
} = {}) => {
  const reasonText = String(reason || "").toLowerCase();
  if (!force && whatsappHetznerPersistentWorkerEnabled) {
    return;
  }
  const isPendingSpotifyShutdown =
    reasonText.includes("pending_spotify_orders_worker") ||
    reasonText.includes("pending_spotify_order");
  const isManualVerificationShutdown = reasonText.includes("manual_verification");
  const isNuevoServicioShutdown =
    reasonText.includes("nuevo_servicio") || reasonText.includes("new_service");
  const isRecordatoriosShutdown =
    reasonText.includes("recordatorio") ||
    reasonText.includes("recordatorios") ||
    reasonText.includes("auto_schedule") ||
    reasonText.includes("auto_run");

  const hasRecordatoriosWork = recordatoriosSendInProgress || autoRecordatoriosRunInProgress;
  const pendingConflicts =
    manualVerificationWatcherInProgress || nuevoServicioNotifQueueInProgress || hasRecordatoriosWork;
  const manualConflicts =
    pendingSpotifyAlertsInProgress || nuevoServicioNotifQueueInProgress || hasRecordatoriosWork;
  const nuevoServicioConflicts =
    pendingSpotifyAlertsInProgress || manualVerificationWatcherInProgress || hasRecordatoriosWork;
  const recordatoriosConflicts =
    pendingSpotifyAlertsInProgress || manualVerificationWatcherInProgress || nuevoServicioNotifQueueInProgress;

  if (
    !force &&
    ((isPendingSpotifyShutdown && pendingConflicts) ||
      (isManualVerificationShutdown && manualConflicts) ||
      (isNuevoServicioShutdown && nuevoServicioConflicts) ||
      (isRecordatoriosShutdown && recordatoriosConflicts))
  ) {
    console.log(`[WhatsApp] Omitiendo apagado (${reason}) por otro worker activo.`);
    return;
  }

  if (!shouldStartWhatsapp && !allowWhenDisabled) return;
  if (!force && (recordatoriosSendInProgress || autoRecordatoriosRunInProgress)) return;
  if (!isWhatsappReady() && !whatsappBootInProgress) return;
  console.log(`[WhatsApp] Apagando cliente (${reason}).`);
  await stopWhatsappClient();
};

const waitForWhatsappReady = async ({
  timeoutMs = WHATSAPP_READY_TIMEOUT_MS,
  reason = "unspecified",
} = {}) => {
  if (isWhatsappReady()) return true;

  return new Promise((resolve, reject) => {
    let settled = false;
    let timer = null;
    let unsubscribeReady = () => {};
    let unsubscribeDisconnected = () => {};

    const cleanup = () => {
      if (timer) clearTimeout(timer);
      unsubscribeReady();
      unsubscribeDisconnected();
    };

    const finishResolve = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(true);
    };

    const finishReject = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };

    unsubscribeReady = onWhatsappReady(() => {
      finishResolve();
    });

    unsubscribeDisconnected = onWhatsappDisconnected((disconnectReason) => {
      const err = new Error(
        disconnectReason
          ? `WhatsApp se desconectó mientras iniciaba: ${disconnectReason}`
          : "WhatsApp se desconectó mientras iniciaba",
      );
      err.code = "WHATSAPP_DISCONNECTED";
      finishReject(err);
    });

    timer = setTimeout(() => {
      const err = new Error(
        `WhatsApp no quedó listo tras esperar ${Math.round(timeoutMs / 1000)}s`,
      );
      err.code = "WHATSAPP_NOT_READY";
      finishReject(err);
    }, timeoutMs);

    if (isWhatsappReady()) finishResolve();
    console.log(
      `[WhatsApp] Esperando cliente listo (${reason}) hasta ${Math.round(timeoutMs / 1000)}s.`,
    );
  });
};

const ensureWhatsappClientReady = async ({
  reason = "unspecified",
  timeoutMs = WHATSAPP_READY_TIMEOUT_MS,
  allowWhenDisabled = false,
} = {}) => {
  if (!shouldStartWhatsapp && !allowWhenDisabled) {
    const err = new Error("WhatsApp deshabilitado");
    err.code = "WHATSAPP_DISABLED";
    throw err;
  }
  if (isWhatsappReady()) return true;

  await ensureWhatsappClientStarted({ reason, allowWhenDisabled });
  if (isWhatsappReady()) return true;

  return waitForWhatsappReady({ timeoutMs, reason });
};

const sleepMs = (ms = 0) =>
  new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Number(ms) || 0));
  });

const waitForWhatsappQrOrReady = async ({
  timeoutMs = WHATSAPP_QR_WAIT_TIMEOUT_MS,
  pollMs = WHATSAPP_QR_WAIT_POLL_MS,
} = {}) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const ready = isWhatsappReady();
    const qrState = getWhatsappQrState();
    if (ready || String(qrState?.raw || "").trim()) {
      return { ready, qrState };
    }
    if (!isWhatsappClientActive() && !whatsappBootInProgress) {
      break;
    }
    await sleepMs(pollMs);
  }
  return {
    ready: isWhatsappReady(),
    qrState: getWhatsappQrState(),
  };
};

const loadWhatsappHetznerPersistentWorkerEnabled = async ({ refresh = false } = {}) => {
  if (whatsappHetznerPersistentWorkerLoaded && !refresh) {
    return whatsappHetznerPersistentWorkerEnabled;
  }
  try {
    const { data, error } = await supabaseAdmin
      .from("configuracion_sistema")
      .select("valor_bool")
      .eq("clave", WHATSAPP_HETZNER_PERSISTENT_CFG_KEY)
      .maybeSingle();
    if (error) throw error;
    whatsappHetznerPersistentWorkerEnabled = data?.valor_bool === true;
    whatsappHetznerPersistentWorkerLoaded = true;
  } catch (err) {
    const code = String(err?.code || "").trim();
    const schemaMissing = code === "42P01" || code === "42703";
    if (!schemaMissing) {
      console.error("[whatsapp:persistent] load config error", err);
      // Error transitorio (por ejemplo Supabase pausado). No marcamos "loaded"
      // para permitir nuevos intentos automáticos cuando vuelva la conexión.
      whatsappHetznerPersistentWorkerLoaded = false;
      return whatsappHetznerPersistentWorkerEnabled;
    }
    // Si faltan tabla/columna, dejamos fallback estable en false.
    whatsappHetznerPersistentWorkerEnabled = false;
    whatsappHetznerPersistentWorkerLoaded = true;
  }
  return whatsappHetznerPersistentWorkerEnabled;
};

const syncWhatsappPersistentWorkerState = async (reason = "sync") => {
  if (!shouldStartWhatsapp) return;
  if (whatsappPersistentWorkerSyncInProgress) return;
  whatsappPersistentWorkerSyncInProgress = true;
  try {
    const enabled = await loadWhatsappHetznerPersistentWorkerEnabled({ refresh: true });
    if (!enabled) return;
    if (!isWhatsappReady() && !whatsappBootInProgress) {
      await ensureWhatsappClientStarted({
        reason: `persistent_worker_${reason}`,
        allowWhenDisabled: true,
        waitForInitialize: false,
      });
    }
  } catch (err) {
    console.error("[whatsapp:persistent] sync error", err);
  } finally {
    whatsappPersistentWorkerSyncInProgress = false;
  }
};

const saveWhatsappHetznerPersistentWorkerEnabled = async (enabled) => {
  const next = enabled === true;
  try {
    const { error } = await supabaseAdmin.from("configuracion_sistema").upsert(
      [{
        clave: WHATSAPP_HETZNER_PERSISTENT_CFG_KEY,
        valor_bool: next,
        actualizado_en: new Date().toISOString(),
      }],
      {
        onConflict: "clave",
      },
    );
    if (error) throw error;
    whatsappHetznerPersistentWorkerEnabled = next;
    whatsappHetznerPersistentWorkerLoaded = true;
  } catch (err) {
    console.error("[whatsapp:persistent] save config error", err);
    throw err;
  }
  return whatsappHetznerPersistentWorkerEnabled;
};

const buildWhatsappErrorLog = (err) => {
  const cause =
    err?.cause instanceof Error
      ? {
          name: err.cause.name || null,
          message: err.cause.message || null,
          stack: err.cause.stack || null,
        }
      : err?.cause ?? null;
  return {
    name: err?.name || null,
    message: err?.message || null,
    code: err?.code || null,
    stack: err?.stack || null,
    cause,
  };
};

const didSendAllRecordatorios = (result = {}) => {
  const total = Number(result.total || 0);
  if (total <= 0) return false;
  return (
    Number(result.sent || 0) === total &&
    Number(result.failed || 0) === 0 &&
    Number(result.skippedNoPhone || 0) === 0 &&
    Number(result.skippedInvalidPhone || 0) === 0
  );
};

const isMissingTableError = (err, tableName) => {
  const code = String(err?.code || "").trim();
  const message = String(err?.message || "").toLowerCase();
  if (code === "42P01") return true;
  if (!tableName) return false;
  return message.includes("does not exist") && message.includes(String(tableName).toLowerCase());
};

const isMissingColumnError = (err, columnName = "") => {
  const code = String(err?.code || "").trim();
  const message = String(err?.message || "").toLowerCase();
  if (code === "42703") return true;
  if (!columnName) return false;
  return message.includes("does not exist") && message.includes(String(columnName).toLowerCase());
};

const isUniqueViolationError = (err, hint = "") => {
  const code = String(err?.code || "").trim();
  if (code !== "23505") return false;
  if (!hint) return true;
  const meta = `${err?.message || ""} ${err?.details || ""} ${err?.hint || ""}`.toLowerCase();
  return meta.includes(String(hint).toLowerCase());
};

const SUPABASE_TRANSIENT_RETRIES = 2;
const SUPABASE_TRANSIENT_RETRY_BASE_MS = 150;
const SUPABASE_SELECT_PAGE_SIZE = 1000;

const isTransientSupabaseFetchError = (err) => {
  const msg = String(err?.message || err || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("fetch failed") ||
    msg.includes("network request failed") ||
    msg.includes("socket hang up") ||
    msg.includes("econnreset") ||
    msg.includes("etimedout") ||
    msg.includes("timeout")
  );
};

const runSupabaseQueryWithRetry = async (queryFactory, label = "supabase") => {
  let attempt = 0;
  while (attempt <= SUPABASE_TRANSIENT_RETRIES) {
    const result = await queryFactory();
    const queryErr = result?.error;
    if (!queryErr) return result;
    if (
      !isTransientSupabaseFetchError(queryErr) ||
      attempt >= SUPABASE_TRANSIENT_RETRIES
    ) {
      return result;
    }
    const waitMs = SUPABASE_TRANSIENT_RETRY_BASE_MS * (attempt + 1);
    console.warn(
      `[${label}] error transitorio (${queryErr?.message || queryErr}). Reintento ${attempt + 1}/${
        SUPABASE_TRANSIENT_RETRIES
      } en ${waitMs}ms.`,
    );
    await sleep(waitMs);
    attempt += 1;
  }
  return { data: null, error: new Error("Query retry exhausted") };
};

const fetchAllSupabaseRows = async (
  queryFactory,
  { label = "supabase:fetchAll", pageSize = SUPABASE_SELECT_PAGE_SIZE } = {},
) => {
  const rows = [];
  let from = 0;

  while (true) {
    const to = from + pageSize - 1;
    const { data, error } = await runSupabaseQueryWithRetry(
      () => queryFactory(from, to),
      label,
    );
    if (error) throw error;
    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return rows;
};

const normalizePerfilText = (perfilRaw) => {
  const perfil = String(perfilRaw || "").trim();
  if (!perfil) return "";
  if (/^m\d+$/i.test(perfil)) return `M${perfil.replace(/^m/i, "")}`;
  if (/^\d+$/.test(perfil)) return `M${perfil}`;
  return perfil;
};

const normalizeWhatsappGroupChatId = (rawChatId = "") => {
  const raw = String(rawChatId || "").trim();
  if (!raw) return "";
  if (/@g\.us$/i.test(raw)) return raw;
  if (raw.includes("@")) return "";
  const compact = raw.replace(/\s+/g, "");
  if (!/^[0-9-]+$/.test(compact)) return "";
  return `${compact}@g.us`;
};

const formatWhatsappReporteText = (value = "", fallback = "-") => {
  const text = String(value || "").trim();
  return text || fallback;
};

const resolveWhatsappReportesGroupChatId = async ({ client = null } = {}) => {
  const configuredChatId = normalizeWhatsappGroupChatId(WHATSAPP_REPORTES_GROUP_CHAT_ID);
  if (configuredChatId) return configuredChatId;

  const groupName = String(WHATSAPP_REPORTES_GROUP_NAME || "").trim();
  if (!groupName) return null;

  const waClient = client || getWhatsappClient();
  if (!waClient || typeof waClient.getChats !== "function") return null;

  const targetName = groupName.toLowerCase();
  const chats = await waClient.getChats();
  const match = (chats || []).find((chat) => {
    if (!isTrue(chat?.isGroup)) return false;
    const name = String(chat?.name || "").trim().toLowerCase();
    return name === targetName;
  });
  if (!match) return null;

  const serializedId = String(match?.id?._serialized || "").trim();
  if (serializedId) return serializedId;

  const stringId = String(match?.id || "").trim();
  if (/@g\.us$/i.test(stringId)) return stringId;
  return normalizeWhatsappGroupChatId(stringId);
};

const resolveWhatsappPedidosPendientesGroupChatId = async ({ client = null } = {}) => {
  const configuredChatId = normalizeWhatsappGroupChatId(WHATSAPP_PEDIDOS_PENDIENTES_GROUP_CHAT_ID);
  if (configuredChatId) return configuredChatId;

  const groupName = String(WHATSAPP_PEDIDOS_PENDIENTES_GROUP_NAME || "").trim();
  if (!groupName) return null;

  const waClient = client || getWhatsappClient();
  if (!waClient || typeof waClient.getChats !== "function") return null;

  const targetName = groupName.toLowerCase();
  const chats = await waClient.getChats();
  const match = (chats || []).find((chat) => {
    if (!isTrue(chat?.isGroup)) return false;
    const name = String(chat?.name || "").trim().toLowerCase();
    return name === targetName;
  });
  if (!match) return null;

  const serializedId = String(match?.id?._serialized || "").trim();
  if (serializedId) return serializedId;

  const stringId = String(match?.id || "").trim();
  if (/@g\.us$/i.test(stringId)) return stringId;
  return normalizeWhatsappGroupChatId(stringId);
};

const buildWhatsappReporteCreadoMessage = ({
  idReporte = null,
  plataforma = "",
  correo = "",
  clave = "",
  cliente = "",
  motivo = "",
} = {}) => {
  const reportId = toPositiveInt(idReporte) || idReporte || "-";
  const plataformaText = formatWhatsappReporteText(plataforma, "Sin plataforma");
  const correoText = formatWhatsappReporteText(correo, "-");
  const claveText = formatWhatsappReporteText(clave, "-");
  const clienteText = formatWhatsappReporteText(cliente, "-");
  const motivoText = formatWhatsappReporteText(motivo, "-");

  return `\`Reporte #${reportId} 🚨\`
*${plataformaText}*
Correo: ${correoText}
Clave: ${claveText}

Cliente: ${clienteText}
Motivo: ${motivoText}`;
};

const buildWhatsappReporteSolucionadoMessage = ({
  idReporte = null,
  plataforma = "",
  correo = "",
  nota = "",
} = {}) => {
  const reportId = toPositiveInt(idReporte) || idReporte || "-";
  const plataformaText = formatWhatsappReporteText(plataforma, "Sin plataforma");
  const correoText = formatWhatsappReporteText(correo, "-");
  const notaText = formatWhatsappReporteText(nota, "-");
  return `\`Reporte #${reportId}\`
*${plataformaText}*
Correo: ${correoText}
Estado: solucionado ✅
Nota: ${notaText}`;
};

const buildInventarioUrlByCorreo = (correo = "") => {
  const correoTxt = String(correo || "").trim();
  try {
    const url = new URL("/inventario.html", buildPublicSiteUrl());
    if (correoTxt) url.searchParams.set("correo", correoTxt);
    return url.toString();
  } catch (_err) {
    const base = String(buildPublicSiteUrl() || "").replace(/\/+$/, "");
    if (!correoTxt) return `${base}/inventario.html`;
    return `${base}/inventario.html?correo=${encodeURIComponent(correoTxt)}`;
  }
};

const normalizeReplacementText = (value = "") =>
  String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();

const formatWhatsappReporteSolvedNote = ({ nota = "", correo = "" } = {}) => {
  const notaRaw = String(nota || "").trim();
  const normalized = normalizeReplacementText(notaRaw);
  const isReplacement =
    normalized.includes("reemplazo") || normalized.includes("reemplazado");
  if (!isReplacement) return notaRaw || "-";
  const inventarioUrl = buildInventarioUrlByCorreo(correo);
  return `Su servicio fue reemplazado a una cuenta funcional. Verifique sus nuevos datos en: ${inventarioUrl}`;
};

const buildWhatsappReporteDatosIncorrectosMessage = ({
  idReporte = null,
  idVenta = null,
  incluirCorreo = false,
  incluirClave = false,
  imagen = "",
  updateDataUrl = "",
} = {}) => {
  const reportId = toPositiveInt(idReporte) || idReporte || "-";
  const imagenUrl = String(imagen || "").trim();
  const updateUrl = String(updateDataUrl || "").trim();
  const bullets = [];
  if (incluirCorreo === true) bullets.push("* Correo");
  if (incluirClave === true) bullets.push("* Contraseña");
  const bodyBullets = bullets.length ? bullets.join("\n") : "* -";
  const resetClaveMsg =
    incluirClave === true
      ? "\n\nPuedes restablecer tu contraseña de Spotify a través de este link:\nhttps://accounts.spotify.com/es/password-reset"
      : "";
  const imageSection = imagenUrl ? `${imagenUrl}\n` : "";
  const ventaSuffix = toPositiveInt(idVenta) ? ` · Venta #${toPositiveInt(idVenta)}` : "";
  const updateSection = updateUrl
    ? `\n\nDespues de restablecer los datos actualizalos por la pagina:\n${updateUrl}`
    : "";
  return `${imageSection}\`Reporte #${reportId}${ventaSuffix}\`
Respuesta:
Datos erroneos:
${bodyBullets}${resetClaveMsg}${updateSection}`;
};

const buildWhatsappVentaReporteAdminMessage = ({
  idVenta = null,
  incluirCorreo = false,
  incluirClave = false,
  imagen = "",
  incluirResetClaveSpotify = false,
  updateDataUrl = "",
} = {}) => {
  const ventaId = toPositiveInt(idVenta) || idVenta || "-";
  const imagenUrl = String(imagen || "").trim();
  const updateUrl = String(updateDataUrl || "").trim();
  const bullets = [];
  if (incluirCorreo === true) bullets.push("* Correo");
  if (incluirClave === true) bullets.push("* Contraseña");
  const bodyBullets = bullets.length ? bullets.join("\n") : "* -";
  const resetClaveMsg =
    incluirClave === true && incluirResetClaveSpotify === true
      ? "\n\nPuedes restablecer tu contraseña de Spotify a través de este link:\nhttps://accounts.spotify.com/es/password-reset"
      : "";
  const imageSection = imagenUrl ? `${imagenUrl}\n` : "";
  const updateSection = updateUrl
    ? `\n\nDespues de restablecer los datos actualizalos por la pagina:\n${updateUrl}`
    : "";
  return `${imageSection}Reporte de Admin.
\`Venta #${ventaId}\`
Respuesta:
Datos erroneos:
${bodyBullets}${resetClaveMsg}${updateSection}`;
};

const buildWhatsappVentaProblemasCorreoMessage = ({ idVenta = null, updateDataUrl = "" } = {}) => {
  const updateUrl = String(updateDataUrl || "").trim() || buildWhatsappUpdateDataUrl({ idVenta });
  const fallbackUrl =
    "https://www.mooseplus.com/src/frontend/pages/entregar_servicios.html?id_venta=5074&modificar_datos=1";
  return `El correo enviado no está permitiendo agregar la suscripción de Spotify.
Por favor envienos un nuevo correo a traves de:
${updateUrl || fallbackUrl}

Su nueva cuenta de Spotify tendrá todos sus Playlists y Me gusta de la cuenta anterior.`;
};

const buildWhatsappOrderDetailUrl = (idOrden = null) => {
  const ordenId = toPositiveInt(idOrden);
  if (!ordenId) return "";
  try {
    const url = new URL("/detalle_ordenes.html", buildPublicSiteUrl());
    url.searchParams.set("id_orden", String(ordenId));
    return url.toString();
  } catch (_err) {
    return `${buildPublicSiteUrl()}/detalle_ordenes.html?id_orden=${encodeURIComponent(String(ordenId))}`;
  }
};

const buildWhatsappServiceDeliveryUrl = ({ idVenta = null, idOrden = null } = {}) => {
  const ventaId = toPositiveInt(idVenta);
  const ordenId = toPositiveInt(idOrden);
  try {
    const url = new URL("/entregar_servicios.html", buildPublicSiteUrl());
    if (ventaId) url.searchParams.set("id_venta", String(ventaId));
    if (ordenId) url.searchParams.set("id_orden", String(ordenId));
    return url.toString();
  } catch (_err) {
    const params = new URLSearchParams();
    if (ventaId) params.set("id_venta", String(ventaId));
    if (ordenId) params.set("id_orden", String(ordenId));
    const qs = params.toString();
    return `${buildPublicSiteUrl()}/entregar_servicios.html${qs ? `?${qs}` : ""}`;
  }
};

const buildWhatsappUpdateDataUrl = ({ idVenta = null, idOrden = null } = {}) => {
  const baseUrl = buildWhatsappServiceDeliveryUrl({ idVenta, idOrden });
  const trimmed = String(baseUrl || "").trim();
  if (!trimmed) return "";
  try {
    const url = new URL(trimmed);
    url.searchParams.set("modificar_datos", "1");
    return url.toString();
  } catch (_err) {
    const glue = trimmed.includes("?") ? "&" : "?";
    return `${trimmed}${glue}modificar_datos=1`;
  }
};

const buildWhatsappOrdenEntregadaMessage = ({ idOrden = null, detalleOrdenUrl = "" } = {}) => {
  const ordenId = toPositiveInt(idOrden) || idOrden || "-";
  const detalleUrl = String(detalleOrdenUrl || "").trim() || buildPublicSiteUrl();
  return `\`Orden #${ordenId}\`
estado: pago verificado ✅

Puedes verificar la orden por:
${detalleUrl}`;
};

const buildWhatsappPagoVerificadoManualMessage = ({
  metodoPagoNombre = "",
  referencia = "",
} = {}) => {
  const metodoText = formatWhatsappReporteText(metodoPagoNombre, "-");
  const refText = formatWhatsappReporteText(referencia, "-");
  return `\`Pago verificado ✅\`
Metodo de pago: ${metodoText}
Ref: ${refText}`;
};

const buildWhatsappSpotifyRenewalsMessage = (items = []) => {
  const normalized = (Array.isArray(items) ? items : [])
    .map((item) => ({
      id_venta: toPositiveInt(item?.id_venta) || null,
      correo: String(item?.correo || "").trim(),
      clave: String(item?.clave || "").trim(),
    }))
    .filter((item) => item.correo && item.clave);
  if (!normalized.length) return "";

  const lines = ["`Renovación Spotify`"];
  normalized.forEach((item, idx) => {
    lines.push("");
    lines.push(`${idx + 1}.`);
    lines.push(`correo: ${item.correo || "-"}`);
    lines.push(`clave: ${item.clave || "-"}`);
  });
  return lines.join("\n");
};

const sendSpotifyRenewalsToProviderWhatsapp = async ({
  renewals = [],
  providerId = 11,
  manageWhatsappLifecycle = true,
  source = "checkout_renewal",
  idOrden = null,
} = {}) => {
  const items = (Array.isArray(renewals) ? renewals : [])
    .map((item) => ({
      id_venta: toPositiveInt(item?.id_venta) || null,
      correo: String(item?.correo || "").trim(),
      clave: String(item?.clave || "").trim(),
    }))
    .filter((item) => item.correo && item.clave);
  if (!items.length) {
    return {
      sent: false,
      skipped: true,
      reason: "no_spotify_renewals",
      id_orden: toPositiveInt(idOrden) || null,
    };
  }

  const resolvedProviderId = toPositiveInt(providerId);
  if (!resolvedProviderId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_provider_id",
      id_orden: toPositiveInt(idOrden) || null,
    };
  }

  const { data: providerRow, error: providerErr } = await supabaseAdmin
    .from("proveedores")
    .select("id_proveedor, whatsapp")
    .eq("id_proveedor", resolvedProviderId)
    .maybeSingle();
  if (providerErr) {
    return {
      sent: false,
      skipped: false,
      reason: isMissingColumnError(providerErr, "whatsapp")
        ? "provider_whatsapp_column_missing"
        : "provider_lookup_error",
      error: providerErr?.message || String(providerErr),
      id_proveedor: resolvedProviderId,
      id_orden: toPositiveInt(idOrden) || null,
    };
  }

  const targetPhone = normalizeWhatsappPhone(providerRow?.whatsapp);
  if (!targetPhone) {
    return {
      sent: false,
      skipped: true,
      reason: "provider_phone_missing",
      id_proveedor: resolvedProviderId,
      id_orden: toPositiveInt(idOrden) || null,
    };
  }

  const message = buildWhatsappSpotifyRenewalsMessage(items);
  if (!message) {
    return {
      sent: false,
      skipped: true,
      reason: "empty_message_payload",
      id_proveedor: resolvedProviderId,
      id_orden: toPositiveInt(idOrden) || null,
    };
  }

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    try {
      await ensureWhatsappClientReady({
        reason: shouldManageWhatsappLifecycle
          ? `spotify_renewal_provider_alert:${source}`
          : "spotify_renewal_provider_alert_batch",
        allowWhenDisabled: true,
      });
    } catch (readyErr) {
      return {
        sent: false,
        skipped: false,
        reason: "whatsapp_not_ready",
        error: readyErr?.message || String(readyErr),
        id_proveedor: resolvedProviderId,
        id_orden: toPositiveInt(idOrden) || null,
      };
    }
  }

  let sendErr = null;
  try {
    const client = getWhatsappClient();
    await withTimeout(
      client.sendMessage(`${targetPhone}@c.us`, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando renovaciones Spotify al proveedor",
    );
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: `spotify_renewal_provider_alert_completed:${source}`,
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[spotify-renovacion] whatsapp shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_proveedor: resolvedProviderId,
      id_orden: toPositiveInt(idOrden) || null,
      phone: targetPhone,
    };
  }

  return {
    sent: true,
    skipped: false,
    reason: null,
    id_proveedor: resolvedProviderId,
    id_orden: toPositiveInt(idOrden) || null,
    phone: targetPhone,
    total: items.length,
  };
};

const fetchSpotifyProviderPendingSales = async ({ idOrden = null, saleIds = [] } = {}) => {
  const resolvedOrderId = toPositiveInt(idOrden);
  const resolvedSaleIds = uniqPositiveIds(saleIds);
  if (!resolvedOrderId && !resolvedSaleIds.length) {
    return { rows: [], missingMensajeProveedorColumn: false };
  }

  let candidateSaleIds = resolvedSaleIds;
  if (!candidateSaleIds.length && resolvedOrderId) {
    const orderRows = await fetchVentasByOrder({
      idOrden: resolvedOrderId,
      select: "id_venta",
    });
    candidateSaleIds = uniqPositiveIds((orderRows || []).map((row) => row?.id_venta));
  }
  if (!candidateSaleIds.length) {
    return { rows: [], missingMensajeProveedorColumn: false };
  }

  const { data, error } = await supabaseAdmin
    .from("ventas")
    .select(
      "id_venta, completa, mensaje_proveedor, correo_miembro, clave_miembro, precios:precios!ventas_id_precio_fkey(id_plataforma)",
    )
    .eq("completa", true)
    .eq("mensaje_proveedor", false)
    .in("id_venta", candidateSaleIds)
    .order("id_venta", { ascending: true });
  if (error) {
    if (isMissingColumnError(error, "mensaje_proveedor")) {
      return { rows: [], missingMensajeProveedorColumn: true };
    }
    throw error;
  }

  const rows = (data || [])
    .map((row) => ({
      id_venta: toPositiveInt(row?.id_venta) || null,
      id_orden: resolvedOrderId || null,
      id_plataforma: toPositiveInt(row?.precios?.id_plataforma) || null,
      mensaje_proveedor: row?.mensaje_proveedor === true,
      correo: String(row?.correo_miembro || "").trim(),
      clave: String(row?.clave_miembro || "").trim(),
    }))
    .filter(
      (row) =>
        row.id_venta &&
        row.id_plataforma === 9 &&
        row.mensaje_proveedor === false &&
        row.correo &&
        row.clave,
    );

  return { rows, missingMensajeProveedorColumn: false };
};

const notifySpotifyProviderPendingSales = async ({
  idOrden = null,
  saleIds = [],
  providerId = 11,
  manageWhatsappLifecycle = true,
  source = "spotify_provider_pending_manual",
} = {}) => {
  const resolvedOrderId = toPositiveInt(idOrden);
  const resolvedSaleIds = uniqPositiveIds(saleIds);
  const { rows, missingMensajeProveedorColumn } = await fetchSpotifyProviderPendingSales({
    idOrden: resolvedOrderId,
    saleIds: resolvedSaleIds,
  });

  if (missingMensajeProveedorColumn) {
    return {
      sent: false,
      skipped: true,
      reason: "mensaje_proveedor_column_missing",
      id_orden: resolvedOrderId || null,
    };
  }

  if (!rows.length) {
    return {
      sent: false,
      skipped: true,
      reason: "no_pending_sales_with_credentials",
      id_orden: resolvedOrderId || null,
      venta_ids: [],
    };
  }

  const ventaIds = uniqPositiveIds(rows.map((row) => row.id_venta));
  const resolvedOrderIdFromRows =
    resolvedOrderId ||
    (await resolveOrderIdByVentaId({ idVenta: ventaIds?.[0] || null })) ||
    null;
  const waResult = await sendSpotifyRenewalsToProviderWhatsapp({
    renewals: rows,
    providerId,
    manageWhatsappLifecycle,
    source,
    idOrden: resolvedOrderIdFromRows,
  });

  if (!waResult?.sent) {
    return {
      ...waResult,
      id_orden: resolvedOrderIdFromRows,
      venta_ids: ventaIds,
      total_pendientes: rows.length,
    };
  }

  let markedCount = 0;
  let markError = null;
  if (ventaIds.length) {
    const markQuery = supabaseAdmin
      .from("ventas")
      .update({ mensaje_proveedor: true })
      .in("id_venta", ventaIds)
      .eq("mensaje_proveedor", false);
    const { data: markedRows, error: markErr } = await markQuery.select("id_venta");
    if (markErr) {
      markError = markErr;
    } else {
      markedCount = Array.isArray(markedRows) ? markedRows.length : 0;
    }
  }

  return {
    ...waResult,
    id_orden: resolvedOrderIdFromRows,
    venta_ids: ventaIds,
    total_pendientes: rows.length,
    mensaje_proveedor_actualizadas: markedCount,
    marked_sent: !markError,
    mark_error: markError ? markError?.message || String(markError) : null,
  };
};

const maybeAutoNotifySpotifyProviderForOrder = async ({
  idOrden = null,
  providerId = 11,
  manageWhatsappLifecycle = true,
  source = "spotify_provider_pending_auto_single",
} = {}) => {
  const resolvedOrderId = toPositiveInt(idOrden);
  if (!resolvedOrderId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_order_id",
      id_orden: null,
    };
  }

  const { rows, missingMensajeProveedorColumn } = await fetchSpotifyProviderPendingSales({
    idOrden: resolvedOrderId,
  });
  if (missingMensajeProveedorColumn) {
    return {
      sent: false,
      skipped: true,
      reason: "mensaje_proveedor_column_missing",
      id_orden: resolvedOrderId,
    };
  }
  if (!rows.length) {
    return {
      sent: false,
      skipped: true,
      reason: "no_pending_sales_with_credentials",
      id_orden: resolvedOrderId,
    };
  }
  if (rows.length > 1) {
    return {
      sent: false,
      skipped: true,
      reason: "multiple_pending_sales_waiting_grouped_send",
      id_orden: resolvedOrderId,
      venta_ids: uniqPositiveIds(rows.map((row) => row?.id_venta)),
      total_pendientes: rows.length,
    };
  }

  return notifySpotifyProviderPendingSales({
    idOrden: resolvedOrderId,
    providerId,
    manageWhatsappLifecycle,
    source,
  });
};

const buildWhatsappVentaEntregadaMessage = ({
  idVenta = null,
  idOrden = null,
  detalleEntregaUrl = "",
  clienteRegistrado = true,
  signupRegistroUrl = "",
} = {}) => {
  const ventaId = toPositiveInt(idVenta) || idVenta || "-";
  const ordenId = toPositiveInt(idOrden);
  const detalleUrl =
    String(detalleEntregaUrl || "").trim() ||
    buildWhatsappServiceDeliveryUrl({ idVenta: ventaId, idOrden: ordenId });
  const isRegisteredClient = clienteRegistrado !== false;
  const signupUrl = String(signupRegistroUrl || "").trim();
  const targetUrl = isRegisteredClient ? detalleUrl : signupUrl || detalleUrl;
  const actionText = isRegisteredClient
    ? "Puedes revisar este servicio por:"
    : "Registrate y revisa este servicio por:";
  return `\`Venta #${ventaId}\`
*estado:* servicio entregado ✅

${actionText}
${targetUrl}`;
};

const fetchReporteWhatsappContextById = async (idReporte) => {
  const reportId = toPositiveInt(idReporte);
  if (!reportId) return null;

  const { data, error } = await supabaseAdmin
    .from("reportes")
    .select(
      "id_reporte, id_usuario, id_venta, id_cuenta, id_perfil, id_plataforma, descripcion, descripcion_solucion, en_revision, solucionado, plataformas:plataformas!reportes_id_plataforma_fkey(nombre), cuentas:cuentas!reportes_id_cuenta_fkey1(correo, clave), usuarios:usuarios!reportes_id_usuario_fkey(nombre, apellido), reporte_tipos:reporte_tipos!reportes_id_tipo_reporte_fkey(titulo)",
    )
    .eq("id_reporte", reportId)
    .maybeSingle();
  if (error) throw error;
  return data || null;
};

const findVentaAsociadaFromReporteForWhatsapp = async (reporte = null) => {
  const ventaSelectForWhatsapp =
    "id_venta, pendiente, correo_miembro, clave_miembro, cuentas:cuentas!ventas_id_cuenta_fkey(correo, clave), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(correo, clave)";
  const ventaIdDirecta = toPositiveInt(reporte?.id_venta);
  if (ventaIdDirecta) {
    const { data: ventaDirecta, error: ventaDirectaErr } = await supabaseAdmin
      .from("ventas")
      .select(ventaSelectForWhatsapp)
      .eq("id_venta", ventaIdDirecta)
      .maybeSingle();
    if (ventaDirectaErr) throw ventaDirectaErr;
    if (ventaDirecta?.id_venta) return ventaDirecta;
  }

  const cuentaId = toPositiveInt(reporte?.id_cuenta);
  if (!cuentaId) return null;
  const perfilId = toPositiveInt(reporte?.id_perfil);
  const withPerfil = !!perfilId;
  const userId = toPositiveInt(reporte?.id_usuario);

  const findWithFilter = async (withUserFilter = true, strictPerfil = true) => {
    let query = supabaseAdmin
      .from("ventas")
      .select(ventaSelectForWhatsapp)
      .or(`id_cuenta.eq.${cuentaId},id_cuenta_miembro.eq.${cuentaId}`)
      .order("id_venta", { ascending: false })
      .limit(1);
    if (withUserFilter && userId) query = query.eq("id_usuario", userId);
    if (withPerfil) {
      if (strictPerfil) query = query.eq("id_perfil", perfilId);
    } else {
      query = query.is("id_perfil", null);
    }
    const { data, error } = await query;
    if (error) throw error;
    return data?.[0] || null;
  };

  let venta = await findWithFilter(true, true);
  if (venta) return venta;
  venta = await findWithFilter(false, true);
  if (venta) return venta;
  if (!withPerfil) return null;
  venta = await findWithFilter(true, false);
  if (venta) return venta;
  venta = await findWithFilter(false, false);
  return venta || null;
};

const getVentaCorreoMiembroForWhatsapp = (venta = null) => {
  const correoVenta = String(venta?.correo_miembro || "").trim();
  if (correoVenta) return correoVenta;
  const cuentaMiembro = Array.isArray(venta?.cuentas_miembro)
    ? venta.cuentas_miembro[0] || null
    : venta?.cuentas_miembro || null;
  const correoCuentaMiembro = String(cuentaMiembro?.correo || "").trim();
  if (correoCuentaMiembro) return correoCuentaMiembro;
  return "";
};

const getVentaClaveMiembroForWhatsapp = (venta = null) => {
  const claveVenta = String(venta?.clave_miembro || "").trim();
  if (claveVenta) return claveVenta;
  const cuentaMiembro = Array.isArray(venta?.cuentas_miembro)
    ? venta.cuentas_miembro[0] || null
    : venta?.cuentas_miembro || null;
  const claveCuentaMiembro = String(cuentaMiembro?.clave || "").trim();
  if (claveCuentaMiembro) return claveCuentaMiembro;
  return "";
};

const getVentaCorreoCuentaMiembroForWhatsapp = (venta = null) => {
  const cuentaMiembro = Array.isArray(venta?.cuentas_miembro)
    ? venta.cuentas_miembro[0] || null
    : venta?.cuentas_miembro || null;
  const correoCuentaMiembro = String(cuentaMiembro?.correo || "").trim();
  if (correoCuentaMiembro) return correoCuentaMiembro;
  const correoVenta = String(venta?.correo_miembro || "").trim();
  if (correoVenta) return correoVenta;
  return "";
};

const getVentaClaveCuentaMiembroForWhatsapp = (venta = null) => {
  const cuentaMiembro = Array.isArray(venta?.cuentas_miembro)
    ? venta.cuentas_miembro[0] || null
    : venta?.cuentas_miembro || null;
  const claveCuentaMiembro = String(cuentaMiembro?.clave || "").trim();
  if (claveCuentaMiembro) return claveCuentaMiembro;
  const claveVenta = String(venta?.clave_miembro || "").trim();
  if (claveVenta) return claveVenta;
  return "";
};

const getVentaCorreoCuentaPrincipalForWhatsapp = (venta = null) => {
  const cuentaPrincipal = Array.isArray(venta?.cuentas) ? venta.cuentas[0] || null : venta?.cuentas || null;
  const correoCuenta = String(cuentaPrincipal?.correo || "").trim();
  if (correoCuenta) return correoCuenta;
  return "";
};

const getVentaClaveCuentaPrincipalForWhatsapp = (venta = null) => {
  const cuentaPrincipal = Array.isArray(venta?.cuentas) ? venta.cuentas[0] || null : venta?.cuentas || null;
  const claveCuenta = String(cuentaPrincipal?.clave || "").trim();
  if (claveCuenta) return claveCuenta;
  return "";
};

const isWhatsappNoLidForUserError = (err) => {
  const text = String(err?.message || err || "")
    .trim()
    .toLowerCase();
  return text.includes("no lid for user");
};

const sendReporteCreatedToWhatsappGroup = async ({
  reporte = null,
  manageWhatsappLifecycle = true,
} = {}) => {
  const reportId = toPositiveInt(reporte?.id_reporte);
  if (!reportId) {
    return { sent: false, skipped: true, reason: "invalid_report" };
  }
  if (isTrue(reporte?.solucionado) || reporte?.en_revision === false) {
    return {
      sent: false,
      skipped: true,
      reason: "report_not_pending_review",
      id_reporte: reportId,
      solucionado: isTrue(reporte?.solucionado),
      en_revision: reporte?.en_revision === false ? false : true,
    };
  }

  const nombre = String(reporte?.usuarios?.nombre || "").trim();
  const apellido = String(reporte?.usuarios?.apellido || "").trim();
  const cliente = [nombre, apellido].filter(Boolean).join(" ").trim();
  const motivo =
    String(reporte?.reporte_tipos?.titulo || "").trim() || String(reporte?.descripcion || "").trim();
  const ventaAsociada = await findVentaAsociadaFromReporteForWhatsapp(reporte);
  const plataformaId = toPositiveInt(reporte?.id_plataforma);
  const isPlatform9 = plataformaId === 9;
  const correoReporte = isPlatform9
    ? getVentaCorreoCuentaMiembroForWhatsapp(ventaAsociada)
    : getVentaCorreoCuentaPrincipalForWhatsapp(ventaAsociada);
  const claveReporte = isPlatform9
    ? getVentaClaveCuentaMiembroForWhatsapp(ventaAsociada)
    : getVentaClaveCuentaPrincipalForWhatsapp(ventaAsociada);

  const message = buildWhatsappReporteCreadoMessage({
    idReporte: reportId,
    plataforma: reporte?.plataformas?.nombre,
    correo: correoReporte || "-",
    clave: claveReporte || "-",
    cliente: cliente || `Usuario ${toPositiveInt(reporte?.id_usuario) || "-"}`,
    motivo,
  });

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle
        ? "report_created_group_alert"
        : "report_created_group_alert_batch",
      allowWhenDisabled: true,
    });
  }

  let groupChatId = null;
  let sendErr = null;
  try {
    const client = getWhatsappClient();
    groupChatId = await resolveWhatsappReportesGroupChatId({ client });
    if (!groupChatId) {
      return {
        sent: false,
        skipped: true,
        reason: "target_destinations_not_found",
        id_reporte: reportId,
        groupName: WHATSAPP_REPORTES_GROUP_NAME || null,
        groupChatId: null,
      };
    }

    console.log(
      `[reportes:whatsapp] Enviando reporte #${reportId} al grupo ${groupChatId}`,
    );
    await withTimeout(
      client.sendMessage(groupChatId, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando alerta de reporte al grupo",
    );
    console.log(
      `[reportes:whatsapp] Enviado reporte #${reportId} destino=group:${groupChatId}`,
    );

    return {
      sent: true,
      skipped: false,
      reason: null,
      id_reporte: reportId,
      groupName: WHATSAPP_REPORTES_GROUP_NAME || null,
      groupChatId: groupChatId || null,
    };
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: "report_created_group_alert_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[reportes:whatsapp] shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    if (isWhatsappNoLidForUserError(sendErr)) {
      return {
        sent: false,
        skipped: true,
        reason: "target_user_chat_not_found",
        error: sendErr?.message || String(sendErr),
        id_reporte: reportId,
        id_usuario_destino: targetUserId,
        phone: targetPhone,
      };
    }
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_reporte: reportId,
      groupChatId: groupChatId || null,
      groupName: WHATSAPP_REPORTES_GROUP_NAME || null,
    };
  }
};

const sendReporteSolvedToWhatsappOwner = async ({
  reporte = null,
  manageWhatsappLifecycle = true,
} = {}) => {
  const reportId = toPositiveInt(reporte?.id_reporte);
  const targetUserId = toPositiveInt(reporte?.id_usuario);
  if (!reportId) {
    return { sent: false, skipped: true, reason: "invalid_report" };
  }
  if (!targetUserId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_target_user",
      id_reporte: reportId,
    };
  }

  const ventaAsociada = await findVentaAsociadaFromReporteForWhatsapp(reporte);
  const ventaAsociadaId = toPositiveInt(ventaAsociada?.id_venta);
  if (!ventaAsociadaId) {
    return {
      sent: false,
      skipped: true,
      reason: "associated_sale_not_found",
      id_reporte: reportId,
      id_usuario_destino: targetUserId,
    };
  }

  const targetPhone = await resolveWhatsappPhoneForUser(targetUserId);
  if (!targetPhone) {
    return {
      sent: false,
      skipped: true,
      reason: "target_user_phone_missing",
      id_reporte: reportId,
      id_usuario_destino: targetUserId,
    };
  }

  const plataformaId = toPositiveInt(reporte?.id_plataforma);
  const isPlatform9 = plataformaId === 9;
  const correoReporte = isPlatform9
    ? getVentaCorreoCuentaMiembroForWhatsapp(ventaAsociada)
    : getVentaCorreoCuentaPrincipalForWhatsapp(ventaAsociada) ||
      getVentaCorreoMiembroForWhatsapp(ventaAsociada);
  if (!correoReporte) {
    return {
      sent: false,
      skipped: true,
      reason: "associated_sale_email_missing",
      id_reporte: reportId,
      id_venta: ventaAsociadaId,
      id_usuario_destino: targetUserId,
    };
  }

  const notaWhatsapp = formatWhatsappReporteSolvedNote({
    nota: reporte?.descripcion_solucion || reporte?.descripcion,
    correo: correoReporte,
  });
  const message = buildWhatsappReporteSolucionadoMessage({
    idReporte: reportId,
    plataforma: reporte?.plataformas?.nombre,
    correo: correoReporte,
    nota: notaWhatsapp,
  });

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle
        ? "report_solved_user_alert"
        : "report_solved_user_alert_batch",
      allowWhenDisabled: true,
    });
  }

  let sendErr = null;
  try {
    const client = getWhatsappClient();
    await withTimeout(
      client.sendMessage(`${targetPhone}@c.us`, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando alerta de reporte solucionado",
    );
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: "report_solved_user_alert_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[reportes:whatsapp] solucionado shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    if (isWhatsappNoLidForUserError(sendErr)) {
      return {
        sent: false,
        skipped: true,
        reason: "target_user_chat_not_found",
        error: sendErr?.message || String(sendErr),
        id_reporte: reportId,
        id_usuario_destino: targetUserId,
        phone: targetPhone,
      };
    }
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_reporte: reportId,
      id_usuario_destino: targetUserId,
      phone: targetPhone,
    };
  }

  return {
    sent: true,
    skipped: false,
    reason: null,
    id_reporte: reportId,
    id_usuario_destino: targetUserId,
    phone: targetPhone,
  };
};

const sendReporteIncorrectDataToWhatsappOwner = async ({
  reporte = null,
  incluirCorreo = false,
  incluirClave = false,
  imagen = "",
  manageWhatsappLifecycle = true,
} = {}) => {
  const reportId = toPositiveInt(reporte?.id_reporte);
  const targetUserId = toPositiveInt(reporte?.id_usuario);
  if (!reportId) {
    return { sent: false, skipped: true, reason: "invalid_report" };
  }
  if (!targetUserId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_target_user",
      id_reporte: reportId,
    };
  }
  const plataformaId = toPositiveInt(reporte?.id_plataforma);
  if (plataformaId !== 9) {
    return {
      sent: false,
      skipped: true,
      reason: "platform_not_supported",
      id_reporte: reportId,
      id_usuario_destino: targetUserId,
      id_plataforma: plataformaId || null,
    };
  }
  if (incluirCorreo !== true && incluirClave !== true) {
    return {
      sent: false,
      skipped: true,
      reason: "missing_incorrect_data_flags",
      id_reporte: reportId,
      id_usuario_destino: targetUserId,
    };
  }

  const targetPhone = await resolveWhatsappPhoneForUser(targetUserId);
  if (!targetPhone) {
    return {
      sent: false,
      skipped: true,
      reason: "target_user_phone_missing",
      id_reporte: reportId,
      id_usuario_destino: targetUserId,
    };
  }

  const ventaAsociada = await findVentaAsociadaFromReporteForWhatsapp(reporte);
  const ventaId = toPositiveInt(reporte?.id_venta) || toPositiveInt(ventaAsociada?.id_venta) || null;
  const updateDataUrl = buildWhatsappUpdateDataUrl({ idVenta: ventaId });

  const message = buildWhatsappReporteDatosIncorrectosMessage({
    idReporte: reportId,
    idVenta: ventaId,
    incluirCorreo,
    incluirClave,
    imagen,
    updateDataUrl,
  });

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle
        ? "report_incorrect_data_user_alert"
        : "report_incorrect_data_user_alert_batch",
      allowWhenDisabled: true,
    });
  }

  let sendErr = null;
  try {
    const client = getWhatsappClient();
    await withTimeout(
      client.sendMessage(`${targetPhone}@c.us`, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando alerta de datos incorrectos",
    );
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: "report_incorrect_data_user_alert_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[reportes:whatsapp] datos incorrectos shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_reporte: reportId,
      id_usuario_destino: targetUserId,
      phone: targetPhone,
    };
  }

  return {
    sent: true,
    skipped: false,
    reason: null,
    id_reporte: reportId,
    id_usuario_destino: targetUserId,
    phone: targetPhone,
  };
};

const sendVentaAdminReportToWhatsappOwner = async ({
  idVenta = null,
  incluirCorreo = false,
  incluirClave = false,
  imagen = "",
  modo = "datos_incorrectos",
  manageWhatsappLifecycle = true,
} = {}) => {
  const ventaId = toPositiveInt(idVenta);
  if (!ventaId) {
    return { sent: false, skipped: true, reason: "invalid_sale" };
  }
  const reportMode =
    String(modo || "datos_incorrectos").trim().toLowerCase() === "problemas_correo"
      ? "problemas_correo"
      : "datos_incorrectos";
  if (reportMode === "datos_incorrectos" && incluirCorreo !== true && incluirClave !== true) {
    return {
      sent: false,
      skipped: true,
      reason: "missing_incorrect_data_flags",
      id_venta: ventaId,
    };
  }

  const { data: ventaRow, error: ventaErr } = await supabaseAdmin
    .from("ventas")
    .select(
      "id_venta, id_usuario, id_precio, precios:precios!ventas_id_precio_fkey(id_plataforma), cuenta:cuentas!ventas_id_cuenta_fkey(id_plataforma), cuenta_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_plataforma)",
    )
    .eq("id_venta", ventaId)
    .maybeSingle();
  if (ventaErr) throw ventaErr;

  if (!ventaRow?.id_venta) {
    return { sent: false, skipped: true, reason: "sale_not_found", id_venta: ventaId };
  }

  const targetUserId = toPositiveInt(ventaRow?.id_usuario);
  if (!targetUserId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_target_user",
      id_venta: ventaId,
    };
  }

  const targetPhone = await resolveWhatsappPhoneForUser(targetUserId);
  if (!targetPhone) {
    return {
      sent: false,
      skipped: true,
      reason: "target_user_phone_missing",
      id_venta: ventaId,
      id_usuario_destino: targetUserId,
    };
  }

  const platformId =
    toPositiveInt(ventaRow?.precios?.id_plataforma) ||
    toPositiveInt(ventaRow?.cuenta?.id_plataforma) ||
    toPositiveInt(ventaRow?.cuenta_miembro?.id_plataforma) ||
    null;
  const isSpotify = platformId === 9;
  const updateDataUrl = buildWhatsappUpdateDataUrl({ idVenta: ventaId });

  const message =
    reportMode === "problemas_correo"
      ? buildWhatsappVentaProblemasCorreoMessage({
          idVenta: ventaId,
          updateDataUrl,
        })
      : buildWhatsappVentaReporteAdminMessage({
          idVenta: ventaId,
          incluirCorreo,
          incluirClave,
          imagen,
          incluirResetClaveSpotify: isSpotify,
          updateDataUrl,
        });

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle ? "sale_admin_report_user_alert" : "sale_admin_report_user_alert_batch",
      allowWhenDisabled: true,
    });
  }

  let sendErr = null;
  try {
    const client = getWhatsappClient();
    await withTimeout(
      client.sendMessage(`${targetPhone}@c.us`, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando reporte de admin",
    );
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: "sale_admin_report_user_alert_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[ventas:whatsapp] reporte admin shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    if (isWhatsappNoLidForUserError(sendErr)) {
      return {
        sent: false,
        skipped: true,
        reason: "target_user_chat_not_found",
        error: sendErr?.message || String(sendErr),
        id_venta: ventaId,
        id_usuario_destino: targetUserId,
        phone: targetPhone,
      };
    }
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_venta: ventaId,
      id_usuario_destino: targetUserId,
      phone: targetPhone,
    };
  }

  return {
    sent: true,
    skipped: false,
    reason: null,
    modo: reportMode,
    id_venta: ventaId,
    id_usuario_destino: targetUserId,
    phone: targetPhone,
  };
};

const buildIncorrectDataFieldsText = ({ incluirCorreo = false, incluirClave = false } = {}) => {
  const fields = [];
  if (incluirCorreo === true) fields.push("Correo");
  if (incluirClave === true) fields.push("Contraseña");
  return fields.length ? fields.join(" y ") : "datos de acceso";
};

const createWebNotificationForIncorrectData = async ({
  idUsuario = null,
  idCuenta = null,
  titulo = "Datos incorrectos",
  incluirCorreo = false,
  incluirClave = false,
  idVenta = null,
} = {}) => {
  const userId = toPositiveInt(idUsuario);
  if (!userId) return { ok: false, skipped: true, reason: "invalid_target_user" };

  const fieldsTxt = buildIncorrectDataFieldsText({ incluirCorreo, incluirClave });
  const ventaId = toPositiveInt(idVenta);
  const updateUrl = buildWhatsappUpdateDataUrl({ idVenta: ventaId });
  const updateLine = updateUrl
    ? `<br><a href="${updateUrl}" class="link-inline">Actualizar datos</a>`
    : "";
  const mensaje =
    `Te enviamos un mensaje por WhatsApp para actualizar tus datos de acceso.<br>` +
    `Datos reportados: ${fieldsTxt}.${updateLine}`;

  const { error } = await supabaseAdmin.from("notificaciones").insert({
    id_usuario: userId,
    id_cuenta: toPositiveInt(idCuenta) || null,
    titulo: String(titulo || "Datos incorrectos").trim() || "Datos incorrectos",
    mensaje,
    fecha: getCaracasDateStr(0),
    leido: false,
  });
  if (error) throw error;
  return { ok: true };
};

const markOrderPaymentVerificationWhatsappNotified = async (idOrden, value = true) => {
  const ordenId = toPositiveInt(idOrden);
  if (!ordenId) return false;
  try {
    const { error: markErr } = await supabaseAdmin
      .from("ordenes")
      .update({ aviso_verificacion_pago: value === true })
      .eq("id_orden", ordenId);
    if (markErr) {
      if (isMissingColumnError(markErr, "aviso_verificacion_pago")) {
        if (!deliveredOrderWhatsappAvisoPagoMissing) {
          console.warn(
            "[WhatsApp] Falta la columna ordenes.aviso_verificacion_pago. El aviso de pago verificado al cliente se omite hasta crearla.",
          );
        }
        deliveredOrderWhatsappAvisoPagoMissing = true;
        return false;
      }
      console.error("[ventas:whatsapp] no se pudo marcar aviso_verificacion_pago", {
        id_orden: ordenId,
        error: markErr?.message || markErr,
      });
      return false;
    }
    if (deliveredOrderWhatsappAvisoPagoMissing) {
      console.log("[WhatsApp] Columna ordenes.aviso_verificacion_pago detectada nuevamente.");
      deliveredOrderWhatsappAvisoPagoMissing = false;
    }
    return true;
  } catch (markCatchErr) {
    if (isMissingColumnError(markCatchErr, "aviso_verificacion_pago")) {
      if (!deliveredOrderWhatsappAvisoPagoMissing) {
        console.warn(
          "[WhatsApp] Falta la columna ordenes.aviso_verificacion_pago. El aviso de pago verificado al cliente se omite hasta crearla.",
        );
      }
      deliveredOrderWhatsappAvisoPagoMissing = true;
      return false;
    }
    console.error("[ventas:whatsapp] error marcando aviso_verificacion_pago", {
      id_orden: ordenId,
      error: markCatchErr?.message || markCatchErr,
    });
    return false;
  }
};

const sendVentaEntregadaToWhatsappOwner = async ({
  idVenta = null,
  venta = null,
  manageWhatsappLifecycle = true,
  verifiedByAdmin = false,
  forceResend = false,
} = {}) => {
  const ventaId = toPositiveInt(idVenta || venta?.id_venta);
  if (!ventaId) {
    return { sent: false, skipped: true, reason: "invalid_sale" };
  }

  let ventaRow = null;
  const snapshotVentaId = toPositiveInt(venta?.id_venta);
  if (snapshotVentaId && snapshotVentaId === ventaId) {
    ventaRow = venta;
  } else {
    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select("id_venta, id_usuario, pendiente, reportado")
      .eq("id_venta", ventaId)
      .maybeSingle();
    if (error) throw error;
    ventaRow = data || null;
  }
  if (!ventaRow?.id_venta) {
    return { sent: false, skipped: true, reason: "sale_not_found", id_venta: ventaId };
  }
  if (isTrue(ventaRow?.reportado)) {
    return { sent: false, skipped: true, reason: "sale_reported", id_venta: ventaId };
  }

  const targetUserId = toPositiveInt(ventaRow?.id_usuario);
  const ordenId = await resolveOrderIdByVentaId({
    idVenta: ventaId,
    fallbackOrderId: ventaRow?.id_orden,
  });
  if (!targetUserId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_target_user",
      id_venta: ventaId,
    };
  }
  if (!ordenId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_order",
      id_venta: ventaId,
      id_usuario_destino: targetUserId,
    };
  }

  const { data: ordenRow, error: ordenErr } = await supabaseAdmin
    .from("ordenes")
    .select(
      "id_orden, fecha, hora_orden, pago_verificado, id_metodo_de_pago, aviso_verificacion_pago",
    )
    .eq("id_orden", ordenId)
    .maybeSingle();
  if (ordenErr && isMissingColumnError(ordenErr, "aviso_verificacion_pago")) {
    if (!deliveredOrderWhatsappAvisoPagoMissing) {
      console.warn(
        "[WhatsApp] Falta la columna ordenes.aviso_verificacion_pago. El aviso de pago verificado al cliente se omite hasta crearla.",
      );
    }
    deliveredOrderWhatsappAvisoPagoMissing = true;
    return {
      sent: false,
      skipped: true,
      reason: "missing_aviso_verificacion_pago_column",
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
    };
  }
  if (ordenErr) throw ordenErr;
  if (deliveredOrderWhatsappAvisoPagoMissing) {
    console.log("[WhatsApp] Columna ordenes.aviso_verificacion_pago detectada nuevamente.");
    deliveredOrderWhatsappAvisoPagoMissing = false;
  }
  if (!ordenRow?.id_orden) {
    return {
      sent: false,
      skipped: true,
      reason: "order_not_found",
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
    };
  }

  const verificationWindowElapsed = hasManualVerificationWindowElapsed(ordenRow);
  if (!isTrue(ordenRow?.pago_verificado)) {
    return {
      sent: false,
      skipped: true,
      reason: "order_payment_not_verified",
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
    };
  }
  if (isTrue(ordenRow?.aviso_verificacion_pago) && forceResend !== true) {
    return {
      sent: false,
      skipped: true,
      reason: "order_payment_already_notified",
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
    };
  }

  let metodoVerificacionAutomatica = null;
  const metodoPagoId = toPositiveInt(ordenRow?.id_metodo_de_pago);
  if (metodoPagoId) {
    const { data: metodoPagoRow, error: metodoPagoErr } = await supabaseAdmin
      .from("metodos_de_pago")
      .select("verificacion_automatica")
      .eq("id_metodo_de_pago", metodoPagoId)
      .maybeSingle();
    if (metodoPagoErr) throw metodoPagoErr;
    metodoVerificacionAutomatica = metodoPagoRow?.verificacion_automatica;
  }

  const manualVerifiedByAdmin = verifiedByAdmin === true;
  const isMetodoAutoVerification = metodoVerificacionAutomatica === true;
  if (!manualVerifiedByAdmin && isMetodoAutoVerification && verificationWindowElapsed !== true) {
    return {
      sent: false,
      skipped: true,
      reason:
        verificationWindowElapsed === false
          ? "auto_verified_before_window"
          : "order_verification_window_unknown",
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
    };
  }

  const targetPhone = await resolveWhatsappPhoneForUser(targetUserId);
  if (!targetPhone) {
    return {
      sent: false,
      skipped: true,
      reason: "target_user_phone_missing",
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
    };
  }

  const message = buildWhatsappOrdenEntregadaMessage({
    idOrden: ordenId,
    detalleOrdenUrl: buildWhatsappOrderDetailUrl(ordenId),
  });

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle ? "sale_delivered_user_alert" : "sale_delivered_user_alert_batch",
      allowWhenDisabled: true,
    });
  }

  let sendErr = null;
  try {
    const client = getWhatsappClient();
    await withTimeout(
      client.sendMessage(`${targetPhone}@c.us`, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando alerta de pago verificado",
    );
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: "sale_delivered_user_alert_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[ventas:whatsapp] entregada shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
      phone: targetPhone,
    };
  }

  await markOrderPaymentVerificationWhatsappNotified(ordenId, true);

  return {
    sent: true,
    skipped: false,
    reason: null,
    id_venta: ventaId,
    id_orden: ordenId,
    id_usuario_destino: targetUserId,
    phone: targetPhone,
  };
};

const sendVentaServicioEntregadaToWhatsappOwner = async ({
  idVenta = null,
  venta = null,
  manageWhatsappLifecycle = true,
} = {}) => {
  const ventaId = toPositiveInt(idVenta || venta?.id_venta);
  if (!ventaId) {
    return { sent: false, skipped: true, reason: "invalid_sale" };
  }

  let ventaRow = null;
  const snapshotVentaId = toPositiveInt(venta?.id_venta);
  if (snapshotVentaId && snapshotVentaId === ventaId) {
    ventaRow = venta;
  } else {
    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select("id_venta, id_usuario, pendiente, reportado")
      .eq("id_venta", ventaId)
      .maybeSingle();
    if (error) throw error;
    ventaRow = data || null;
  }
  if (!ventaRow?.id_venta) {
    return { sent: false, skipped: true, reason: "sale_not_found", id_venta: ventaId };
  }
  if (isTrue(ventaRow?.reportado)) {
    return { sent: false, skipped: true, reason: "sale_reported", id_venta: ventaId };
  }
  if (ventaRow?.pendiente !== false) {
    return { sent: false, skipped: true, reason: "sale_still_pending", id_venta: ventaId };
  }

  const targetUserId = toPositiveInt(ventaRow?.id_usuario);
  const ordenId = await resolveOrderIdByVentaId({
    idVenta: ventaId,
    fallbackOrderId: ventaRow?.id_orden,
  });
  if (!targetUserId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_target_user",
      id_venta: ventaId,
    };
  }
  if (!ordenId) {
    return {
      sent: false,
      skipped: true,
      reason: "invalid_order",
      id_venta: ventaId,
      id_usuario_destino: targetUserId,
    };
  }

  const { data: targetUserRow, error: targetUserErr } = await supabaseAdmin
    .from("usuarios")
    .select("id_usuario, telefono, fecha_registro, id_auth")
    .eq("id_usuario", targetUserId)
    .maybeSingle();
  if (targetUserErr) throw targetUserErr;

  const targetPhone = normalizeWhatsappPhone(targetUserRow?.telefono);
  if (!targetPhone) {
    return {
      sent: false,
      skipped: true,
      reason: "target_user_phone_missing",
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
    };
  }

  const detalleEntregaUrl = buildWhatsappServiceDeliveryUrl({
    idVenta: ventaId,
    idOrden: ordenId,
  });
  const clienteRegistrado = isUsuarioWebRegistrado(targetUserRow);
  let signupRegistroUrl = "";
  if (!clienteRegistrado) {
    try {
      signupRegistroUrl = buildSignupRegistrationUrl(targetUserId, {
        redirectUrl: detalleEntregaUrl,
      });
    } catch (signupErr) {
      console.error("[ventas:whatsapp] signup url build error", {
        id_venta: ventaId,
        id_orden: ordenId,
        id_usuario: targetUserId,
        error: signupErr?.message || signupErr,
      });
    }
  }

  const message = buildWhatsappVentaEntregadaMessage({
    idVenta: ventaId,
    idOrden: ordenId,
    detalleEntregaUrl,
    clienteRegistrado,
    signupRegistroUrl,
  });

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle
        ? "sale_service_delivered_user_alert"
        : "sale_service_delivered_user_alert_batch",
      allowWhenDisabled: true,
    });
  }

  let sendErr = null;
  try {
    const client = getWhatsappClient();
    await withTimeout(
      client.sendMessage(`${targetPhone}@c.us`, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando alerta de servicio entregado",
    );
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: "sale_service_delivered_user_alert_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[ventas:whatsapp] servicio entregado shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
      phone: targetPhone,
      cliente_registrado: clienteRegistrado,
    };
  }

  const { error: markAvisoErr } = await supabaseAdmin
    .from("ventas")
    .update({ entrega_aviso: true })
    .eq("id_venta", ventaId);
  if (markAvisoErr) {
    return {
      sent: false,
      skipped: false,
      reason: "sale_delivery_notice_mark_error",
      error: markAvisoErr?.message || String(markAvisoErr),
      id_venta: ventaId,
      id_orden: ordenId,
      id_usuario_destino: targetUserId,
      phone: targetPhone,
      cliente_registrado: clienteRegistrado,
    };
  }

  return {
    sent: true,
    skipped: false,
    reason: null,
    id_venta: ventaId,
    id_orden: ordenId,
    id_usuario_destino: targetUserId,
    phone: targetPhone,
    cliente_registrado: clienteRegistrado,
  };
};

const notifyVentaEntregadaAfterOrderVerified = async ({
  idOrden = null,
  manageWhatsappLifecycle = true,
  verifiedByAdmin = false,
  forceResend = false,
} = {}) => {
  const ordenId = toPositiveInt(idOrden);
  if (!ordenId) {
    return { sent: false, skipped: true, reason: "invalid_order" };
  }

  const ventasRows = await fetchVentasByOrder({
    idOrden: ordenId,
    select: "id_venta, pendiente, reportado",
    limit: 120,
  });

  const ventasNoReportadas = (ventasRows || []).filter((row) => !isTrue(row?.reportado));
  const ventaTarget =
    ventasNoReportadas.find((row) => row?.pendiente === false) || ventasNoReportadas[0] || null;
  const ventaId = toPositiveInt(ventaTarget?.id_venta);
  if (!ventaId) {
    return {
      sent: false,
      skipped: true,
      reason: "no_delivered_non_reported_sale",
      id_orden: ordenId,
    };
  }

  return sendVentaEntregadaToWhatsappOwner({
    idVenta: ventaId,
    manageWhatsappLifecycle,
    verifiedByAdmin,
    forceResend,
  });
};

const markReporteWhatsappEnviado = async (idReporte, value = true) => {
  const reportId = toPositiveInt(idReporte);
  if (!reportId) return false;
  const { error: updateErr } = await supabaseAdmin
    .from("reportes")
    .update({ enviado_whatsapp: value === true })
    .eq("id_reporte", reportId);
  if (updateErr) throw updateErr;
  return true;
};

const processPendingReportesWhatsappAlerts = async () => {
  if (!WHATSAPP_REPORTES_WATCHER_ENABLED || !shouldStartWhatsapp) {
    return {
      skipped: true,
      reason: "disabled",
      ...reportesWhatsappWatcherLastResult,
    };
  }
  if (reportesWhatsappWatcherInProgress) {
    return {
      skipped: true,
      reason: "in_progress",
      ...reportesWhatsappWatcherLastResult,
    };
  }

  reportesWhatsappWatcherInProgress = true;
  const result = {
    scanned: 0,
    sent: 0,
    skipped: 0,
    failed: 0,
    markedSent: 0,
  };
  let managedWhatsappForRun = false;

  try {
    const { data: reportRows, error: reportErr } = await supabaseAdmin
      .from("reportes")
      .select("id_reporte")
      .eq("solucionado", false)
      .eq("en_revision", true)
      .not("enviado_whatsapp", "is", true)
      .order("id_reporte", { ascending: false })
      .limit(WHATSAPP_REPORTES_WATCHER_BATCH);
    if (reportErr && isMissingColumnError(reportErr, "enviado_whatsapp")) {
      if (!reportesWhatsappWatcherColumnMissing) {
        console.warn(
          "[reportes:whatsapp] Worker desactivado: falta columna reportes.enviado_whatsapp.",
        );
      }
      reportesWhatsappWatcherColumnMissing = true;
      reportesWhatsappWatcherLastError = null;
      reportesWhatsappWatcherLastResult = result;
      return {
        skipped: true,
        reason: "missing_enviado_whatsapp_column",
        ...result,
      };
    }
    if (reportErr) throw reportErr;
    if (reportesWhatsappWatcherColumnMissing) {
      console.log("[reportes:whatsapp] Columna reportes.enviado_whatsapp detectada nuevamente.");
      reportesWhatsappWatcherColumnMissing = false;
    }

    const rows = Array.isArray(reportRows) ? reportRows : [];
    if (!rows.length) {
      reportesWhatsappWatcherLastError = null;
      reportesWhatsappWatcherLastResult = result;
      return { ...result };
    }

    for (const row of rows) {
      const reportId = toPositiveInt(row?.id_reporte);
      if (!reportId) continue;
      result.scanned += 1;

      try {
        if (!managedWhatsappForRun) {
          await ensureWhatsappClientReady({
            reason: "reportes_watcher_worker",
            allowWhenDisabled: true,
          });
          managedWhatsappForRun = true;
        }

        const reporte = await fetchReporteWhatsappContextById(reportId);
        if (!reporte?.id_reporte) {
          result.skipped += 1;
          continue;
        }

        const sendRes = await sendReporteCreatedToWhatsappGroup({
          reporte,
          manageWhatsappLifecycle: false,
        });
        if (sendRes?.sent) {
          result.sent += 1;
          await markReporteWhatsappEnviado(reportId, true);
          result.markedSent += 1;
        } else {
          result.skipped += 1;
          console.warn("[reportes:whatsapp] Worker skip", {
            id_reporte: reportId,
            reason: sendRes?.reason || "unknown",
            error: sendRes?.error || null,
            groupChatId: sendRes?.groupChatId || null,
            fallbackPhone: sendRes?.fallbackPhone || null,
          });
        }
      } catch (itemErr) {
        result.failed += 1;
        console.error("[reportes:whatsapp] Worker item error", {
          id_reporte: reportId,
          error: itemErr?.message || itemErr,
        });
      }
    }

    reportesWhatsappWatcherLastError = null;
    reportesWhatsappWatcherLastResult = result;
    if (result.sent > 0 || result.failed > 0 || result.skipped > 0) {
      console.log(
        `[reportes:whatsapp] Worker resumen: scanned=${result.scanned}, sent=${result.sent}, markedSent=${result.markedSent}, skipped=${result.skipped}, failed=${result.failed}`,
      );
    }
    return { ...result };
  } catch (err) {
    reportesWhatsappWatcherLastError = err?.message || "Error desconocido";
    throw err;
  } finally {
    if (managedWhatsappForRun) {
      try {
        await shutdownWhatsappClient({
          reason: "reportes_watcher_worker_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[reportes:whatsapp] Worker shutdown error", shutdownErr);
      }
    }
    reportesWhatsappWatcherLastRunAt = new Date().toISOString();
    reportesWhatsappWatcherInProgress = false;
  }
};

const buildWhatsappPendingOrderGroupMessage = ({
  idVenta = null,
  plataforma = "",
  cliente = "",
} = {}) => {
  const ventaId = toPositiveInt(idVenta) || idVenta || "-";
  const plataformaText = formatWhatsappReporteText(plataforma, "Sin plataforma");
  const clienteText = formatWhatsappReporteText(cliente, "-");
  return `\`Pedido pendiente #${ventaId}\`
*${plataformaText}*
Cliente: ${clienteText}`;
};

const resolveWhatsappPhoneForUser = async (idUsuario) => {
  const userId = toPositiveInt(idUsuario);
  if (!userId) return null;
  const { data: userRow, error: userErr } = await supabaseAdmin
    .from("usuarios")
    .select("telefono")
    .eq("id_usuario", userId)
    .maybeSingle();
  if (userErr) throw userErr;
  const phone = normalizeWhatsappPhone(userRow?.telefono);
  return phone || null;
};

const maybeSendPendingSpotifyOrderToWhatsapp = async ({
  idVenta = null,
  idPlataformaHint = null,
  manageWhatsappLifecycle = true,
  ventaSnapshot = null,
} = {}) => {
  const ventaId = toPositiveInt(idVenta);
  if (!ventaId) {
    return { sent: false, skipped: true, reason: "invalid_sale" };
  }
  const plataformaHint = toPositiveInt(idPlataformaHint);
  const snapshotVentaId = toPositiveInt(ventaSnapshot?.id_venta);
  const snapshotPrecio = Array.isArray(ventaSnapshot?.precios)
    ? ventaSnapshot.precios[0] || null
    : ventaSnapshot?.precios || null;
  const snapshotPlataforma = Array.isArray(snapshotPrecio?.plataformas)
    ? snapshotPrecio.plataformas[0] || null
    : snapshotPrecio?.plataformas || null;
  const snapshotUsuario = Array.isArray(ventaSnapshot?.usuarios)
    ? ventaSnapshot.usuarios[0] || null
    : ventaSnapshot?.usuarios || null;
  const snapshotClienteNombre = [snapshotUsuario?.nombre, snapshotUsuario?.apellido]
    .filter(Boolean)
    .join(" ")
    .trim();
  const snapshotPlataformaNombre = String(snapshotPlataforma?.nombre || "").trim();
  const hasSnapshotDetails =
    snapshotVentaId === ventaId &&
    Object.prototype.hasOwnProperty.call(ventaSnapshot || {}, "pendiente") &&
    Object.prototype.hasOwnProperty.call(ventaSnapshot || {}, "reportado") &&
    Object.prototype.hasOwnProperty.call(ventaSnapshot || {}, "aviso_admin") &&
    Object.prototype.hasOwnProperty.call(ventaSnapshot || {}, "id_usuario") &&
    !!snapshotClienteNombre &&
    (!!snapshotPlataformaNombre || plataformaHint > 0);

  let ventaRow = hasSnapshotDetails ? ventaSnapshot : null;
  if (!ventaRow) {
    const { data, error: ventaErr } = await supabaseAdmin
      .from("ventas")
      .select(
        "id_venta, id_usuario, pendiente, aviso_admin, reportado, precios:precios(id_plataforma, plataformas:plataformas(nombre)), usuarios:usuarios!ventas_id_usuario_fkey(nombre, apellido)",
      )
      .eq("id_venta", ventaId)
      .maybeSingle();
    if (ventaErr && isMissingColumnError(ventaErr, "aviso_admin")) {
      if (!pendingSpotifyAlertsAvisoAdminMissing) {
        console.warn(
          "[WhatsApp] Falta la columna ventas.aviso_admin. El aviso de pedidos pendientes se omite hasta crearla.",
        );
      }
      pendingSpotifyAlertsAvisoAdminMissing = true;
      return { sent: false, skipped: true, reason: "missing_aviso_admin_column" };
    }
    if (ventaErr) throw ventaErr;
    if (pendingSpotifyAlertsAvisoAdminMissing) {
      console.log("[WhatsApp] Columna ventas.aviso_admin detectada nuevamente.");
      pendingSpotifyAlertsAvisoAdminMissing = false;
    }
    ventaRow = data || null;
  }
  if (!ventaRow?.id_venta) {
    return { sent: false, skipped: true, reason: "sale_not_found" };
  }
  if (!isTrue(ventaRow?.pendiente)) {
    return { sent: false, skipped: true, reason: "sale_not_pending" };
  }
  if (isTrue(ventaRow?.reportado)) {
    return { sent: false, skipped: true, reason: "sale_reported" };
  }
  if (isTrue(ventaRow?.aviso_admin)) {
    return { sent: false, skipped: true, reason: "already_notified_pending_group" };
  }

  const nestedPrecio = Array.isArray(ventaRow?.precios)
    ? ventaRow.precios[0] || null
    : ventaRow?.precios || null;
  const nestedPlataforma = Array.isArray(nestedPrecio?.plataformas)
    ? nestedPrecio.plataformas[0] || null
    : nestedPrecio?.plataformas || null;
  const usuarioRow = Array.isArray(ventaRow?.usuarios)
    ? ventaRow.usuarios[0] || null
    : ventaRow?.usuarios || null;

  const plataformaId = toPositiveInt(nestedPrecio?.id_plataforma) || plataformaHint;
  const plataformaNombre =
    String(nestedPlataforma?.nombre || "").trim() ||
    (plataformaId ? `Plataforma ${plataformaId}` : "Plataforma");
  const clienteNombre =
    [usuarioRow?.nombre, usuarioRow?.apellido].filter(Boolean).join(" ").trim() ||
    `Usuario ${toPositiveInt(ventaRow?.id_usuario) || "-"}`;

  const message = buildWhatsappPendingOrderGroupMessage({
    idVenta: ventaId,
    plataforma: plataformaNombre,
    cliente: clienteNombre,
  });
  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle
        ? "pending_order_group_alert"
        : "pending_order_group_alert_batch",
      allowWhenDisabled: true,
    });
  }

  try {
    const client = getWhatsappClient();
    const groupChatId = await resolveWhatsappPedidosPendientesGroupChatId({ client });
    if (!groupChatId) {
      return { sent: false, skipped: true, reason: "pending_orders_group_not_found" };
    }

    await withTimeout(
      client.sendMessage(groupChatId, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando alerta de pedido pendiente",
    );

    const { error: avisoErr } = await supabaseAdmin
      .from("ventas")
      .update({ aviso_admin: true })
      .eq("id_venta", ventaId);
    if (avisoErr) throw avisoErr;

    return {
      sent: true,
      skipped: false,
      reason: null,
      id_venta: ventaId,
      groupChatId,
      groupName: WHATSAPP_PEDIDOS_PENDIENTES_GROUP_NAME || null,
    };
  } finally {
    if (shouldManageWhatsappLifecycle) {
      await shutdownWhatsappClient({
        reason: "pending_order_group_alert_completed",
        allowWhenDisabled: true,
      });
    }
  }
};

const buildWhatsappManualVerificationMessage = ({
  metodoPagoNombre = "",
  referencia = "",
} = {}) => {
  const metodo = String(metodoPagoNombre || "").trim() || "No especificado";
  const ref = String(referencia || "").trim() || "-";
  return `*VERIFICACIÓN MANUAL*
Metodo de pago: ${metodo}
Ref: ${ref}`;
};

const buildManualVerificationAdminNotificationMessage = ({
  idOrden = null,
  metodoPagoNombre = "",
  referencia = "",
  source = "",
  triggerReason = "",
} = {}) => {
  const ordenId = toPositiveInt(idOrden);
  const metodo = String(metodoPagoNombre || "").trim() || "No especificado";
  const ref = String(referencia || "").trim() || "-";
  const origen = String(source || "").trim() || "manual";
  const reason = String(triggerReason || "").trim();
  const resumenMotivo =
    reason === "automatic_payment_not_confirmed_after_window"
      ? "Pago no confirmado automáticamente en la ventana."
      : "Pago enviado sin verificación automática.";
  return [
    resumenMotivo,
    `ID Orden: #${ordenId || "-"}`,
    `Método: ${metodo}`,
    `Referencia: ${ref}`,
    `Origen: ${origen}`,
  ].join("<br>");
};

const ensureManualVerificationAdminInboxNotification = async ({
  idOrden = null,
  metodoPagoNombre = "",
  referencia = "",
  source = "",
  triggerReason = "",
} = {}) => {
  const ordenId = toPositiveInt(idOrden);
  if (!ordenId) {
    return { created: false, skipped: true, reason: "invalid_order", id_notificacion: null };
  }

  const { data: existingNotif, error: existingNotifErr } = await supabaseAdmin
    .from("notificaciones")
    .select("id_notificacion")
    .eq("id_usuario", WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID)
    .eq("id_orden", ordenId)
    .eq("titulo", WHATSAPP_MANUAL_VERIFICATION_ALERT_TITLE)
    .order("id_notificacion", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (existingNotifErr) throw existingNotifErr;
  if (existingNotif?.id_notificacion) {
    return {
      created: false,
      skipped: true,
      reason: "already_notified",
      id_notificacion: Number(existingNotif.id_notificacion) || null,
    };
  }

  const payload = {
    titulo: WHATSAPP_MANUAL_VERIFICATION_ALERT_TITLE,
    mensaje: buildManualVerificationAdminNotificationMessage({
      idOrden: ordenId,
      metodoPagoNombre,
      referencia,
      source,
      triggerReason,
    }),
    fecha: getCaracasDateStr(0),
    leido: false,
    id_usuario: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
    id_orden: ordenId,
  };
  const { data: insertedNotif, error: insertErr } = await supabaseAdmin
    .from("notificaciones")
    .insert(payload)
    .select("id_notificacion")
    .maybeSingle();
  if (insertErr) throw insertErr;

  return {
    created: true,
    skipped: false,
    reason: null,
    id_notificacion: Number(insertedNotif?.id_notificacion) || null,
  };
};

const notifyManualVerificationToWhatsappAdmin = async ({
  idOrden = null,
  source = "manual_verification",
  manageWhatsappLifecycle = true,
  allowAutomaticAfterWindow = true,
} = {}) => {
  const ordenId = toPositiveInt(idOrden);
  if (!ordenId) {
    return { sent: false, skipped: true, reason: "invalid_order" };
  }

  const { data: ordenRow, error: ordenErr } = await supabaseAdmin
    .from("ordenes")
    .select(
      "id_orden, fecha, hora_orden, referencia, pago_verificado, orden_cancelada, id_metodo_de_pago, aviso_verificacion_manual",
    )
    .eq("id_orden", ordenId)
    .maybeSingle();
  if (ordenErr) throw ordenErr;
  if (!ordenRow?.id_orden) {
    return { sent: false, skipped: true, reason: "order_not_found" };
  }
  if (isTrue(ordenRow?.pago_verificado)) {
    return { sent: false, skipped: true, reason: "order_already_verified" };
  }
  if (isTrue(ordenRow?.orden_cancelada)) {
    return { sent: false, skipped: true, reason: "order_cancelled" };
  }
  if (isTrue(ordenRow?.aviso_verificacion_manual)) {
    return { sent: false, skipped: true, reason: "already_flagged_manual_verification" };
  }

  const metodoPagoId = toPositiveInt(ordenRow?.id_metodo_de_pago);
  let metodoPagoNombre = "";
  let metodoPagoVerificacionAutomatica = null;
  let manualVerificationTriggerReason = "manual_payment_method";
  if (metodoPagoId) {
    const { data: metodoPagoRow, error: metodoPagoErr } = await supabaseAdmin
      .from("metodos_de_pago")
      .select("nombre, verificacion_automatica")
      .eq("id_metodo_de_pago", metodoPagoId)
      .maybeSingle();
    if (metodoPagoErr) throw metodoPagoErr;
    metodoPagoNombre = String(metodoPagoRow?.nombre || "").trim();
    metodoPagoVerificacionAutomatica = metodoPagoRow?.verificacion_automatica;
  }

  if (metodoPagoVerificacionAutomatica === false) {
    manualVerificationTriggerReason = "manual_payment_method";
  } else if (metodoPagoVerificacionAutomatica === true) {
    if (allowAutomaticAfterWindow !== true) {
      return {
        sent: false,
        skipped: true,
        reason: "payment_method_verification_is_automatic",
        id_orden: ordenId,
        id_metodo_de_pago: metodoPagoId || null,
        id_usuario_destino: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
      };
    }
    const verificationWindowElapsed = hasManualVerificationWindowElapsed(ordenRow);
    if (verificationWindowElapsed !== true) {
      return {
        sent: false,
        skipped: true,
        reason:
          verificationWindowElapsed === false
            ? "payment_method_verification_window_not_elapsed"
            : "order_verification_window_unknown",
        id_orden: ordenId,
        id_metodo_de_pago: metodoPagoId || null,
        id_usuario_destino: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
      };
    }
    manualVerificationTriggerReason = "automatic_payment_not_confirmed_after_window";
  } else {
    return {
      sent: false,
      skipped: true,
      reason: "payment_method_verification_mode_unknown",
      id_orden: ordenId,
      id_metodo_de_pago: metodoPagoId || null,
      id_usuario_destino: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
    };
  }

  const referenciaText = String(ordenRow?.referencia || "").trim();
  let inboxNotifResult = {
    created: false,
    skipped: false,
    reason: null,
    id_notificacion: null,
  };
  try {
    inboxNotifResult = await ensureManualVerificationAdminInboxNotification({
      idOrden: ordenId,
      metodoPagoNombre: metodoPagoNombre || `ID ${metodoPagoId || "-"}`,
      referencia: referenciaText || "-",
      source,
      triggerReason: manualVerificationTriggerReason,
    });
  } catch (notifErr) {
    console.error("[manual_verification] notificacion interna error", {
      id_orden: ordenId,
      error: notifErr?.message || notifErr,
    });
    inboxNotifResult = {
      created: false,
      skipped: true,
      reason: "notification_insert_error",
      id_notificacion: null,
      error: notifErr?.message || String(notifErr),
    };
  }
  if (process.env.VERCEL === "1") {
    return {
      sent: false,
      skipped: true,
      reason: "whatsapp_deferred_to_persistent_worker",
      id_orden: ordenId,
      id_usuario_destino: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
      inbox_notification: inboxNotifResult,
    };
  }

  const targetPhone = await resolveWhatsappPhoneForUser(WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID);
  if (!targetPhone) {
    return {
      sent: false,
      skipped: true,
      reason: "target_admin_phone_missing",
      id_orden: ordenId,
      id_usuario_destino: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
      inbox_notification: inboxNotifResult,
    };
  }

  const message = buildWhatsappManualVerificationMessage({
    metodoPagoNombre: metodoPagoNombre || `ID ${metodoPagoId || "-"}`,
    referencia: referenciaText || "-",
  });

  const shouldManageWhatsappLifecycle = manageWhatsappLifecycle !== false;
  if (shouldManageWhatsappLifecycle || !isWhatsappReady()) {
    await ensureWhatsappClientReady({
      reason: shouldManageWhatsappLifecycle
        ? `manual_verification_alert:${source}`
        : "manual_verification_alert_batch",
      allowWhenDisabled: true,
    });
  }

  let sendErr = null;
  try {
    const client = getWhatsappClient();
    await withTimeout(
      client.sendMessage(`${targetPhone}@c.us`, message, {
        linkPreview: false,
        waitUntilMsgSent: true,
      }),
      WHATSAPP_SEND_TIMEOUT_MS,
      "Timeout enviando alerta de verificación manual",
    );
  } catch (err) {
    sendErr = err;
  } finally {
    if (shouldManageWhatsappLifecycle) {
      try {
        await shutdownWhatsappClient({
          reason: `manual_verification_alert_completed:${source}`,
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[manual_verification] whatsapp shutdown error", shutdownErr);
      }
    }
  }

  if (sendErr) {
    return {
      sent: false,
      skipped: false,
      reason: "whatsapp_send_error",
      error: sendErr?.message || String(sendErr),
      id_orden: ordenId,
      id_usuario_destino: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
      phone: targetPhone,
      inbox_notification: inboxNotifResult,
    };
  }

  try {
    const { error: markErr } = await supabaseAdmin
      .from("ordenes")
      .update({ aviso_verificacion_manual: true })
      .eq("id_orden", ordenId);
    if (markErr) {
      if (!isMissingColumnError(markErr, "aviso_verificacion_manual")) {
        console.error("[manual_verification] no se pudo marcar aviso_verificacion_manual", {
          id_orden: ordenId,
          error: markErr?.message || markErr,
        });
      }
    }
  } catch (markCatchErr) {
    console.error("[manual_verification] error marcando aviso_verificacion_manual", {
      id_orden: ordenId,
      error: markCatchErr?.message || markCatchErr,
    });
  }

  return {
    sent: true,
    skipped: false,
    reason: null,
    id_orden: ordenId,
    id_usuario_destino: WHATSAPP_MANUAL_VERIFICATION_NOTIFY_USER_ID,
    phone: targetPhone,
    inbox_notification: inboxNotifResult,
  };
};

const markNuevoServicioQueueEventProcessed = async (idEvento, { error = null } = {}) => {
  if (!idEvento) return;
  const payload = {
    procesado: true,
    procesado_en: new Date().toISOString(),
    ultimo_error: error ? String(error).slice(0, 500) : null,
  };
  const { error: updErr } = await supabaseAdmin
    .from(NUEVO_SERVICIO_NOTIF_QUEUE_TABLE)
    .update(payload)
    .eq("id_evento", idEvento);
  if (updErr) throw updErr;
};

const markNuevoServicioQueueEventError = async (idEvento, errorMessage) => {
  if (!idEvento) return;
  const payload = {
    ultimo_error: String(errorMessage || "Error desconocido").slice(0, 500),
  };
  const { error: updErr } = await supabaseAdmin
    .from(NUEVO_SERVICIO_NOTIF_QUEUE_TABLE)
    .update(payload)
    .eq("id_evento", idEvento);
  if (updErr) throw updErr;
};

const processNuevoServicioNotificationQueue = async () => {
  if (!NUEVO_SERVICIO_NOTIF_QUEUE_ENABLED) {
    return {
      skipped: true,
      reason: "disabled",
      ...nuevoServicioNotifQueueLastResult,
    };
  }
  if (nuevoServicioNotifQueueInProgress) {
    return {
      skipped: true,
      reason: "in_progress",
      ...nuevoServicioNotifQueueLastResult,
    };
  }

  nuevoServicioNotifQueueInProgress = true;
  const result = {
    fetched: 0,
    sent: 0,
    duplicates: 0,
    failed: 0,
    invalidUser: 0,
    whatsappSent: 0,
    whatsappFailed: 0,
    whatsappSkipped: 0,
  };

  try {
    const { data: events, error: qErr } = await supabaseAdmin
      .from(NUEVO_SERVICIO_NOTIF_QUEUE_TABLE)
      .select(
        "id_evento, id_venta, id_usuario, id_cuenta, id_plataforma, plataforma, correo_cuenta, perfil, fecha_corte, procesado",
      )
      .eq("procesado", false)
      .order("id_evento", { ascending: true })
      .limit(NUEVO_SERVICIO_NOTIF_QUEUE_BATCH);

    if (qErr) {
      if (isMissingTableError(qErr, NUEVO_SERVICIO_NOTIF_QUEUE_TABLE)) {
        if (!nuevoServicioNotifQueueTableMissing) {
          console.warn(
            `[Notificaciones] Tabla ${NUEVO_SERVICIO_NOTIF_QUEUE_TABLE} no existe. Se reintentará automáticamente.`,
          );
        }
        nuevoServicioNotifQueueTableMissing = true;
        return {
          skipped: true,
          reason: "table_missing",
          ...result,
        };
      }
      throw qErr;
    }

    const queueItems = Array.isArray(events) ? events : [];
    const entregaInmediataByPlataforma = new Map();
    if (nuevoServicioNotifQueueTableMissing) {
      console.log(
        `[Notificaciones] Tabla ${NUEVO_SERVICIO_NOTIF_QUEUE_TABLE} detectada. Worker retomado.`,
      );
    }
    nuevoServicioNotifQueueTableMissing = false;
    result.fetched = queueItems.length;

    for (const eventRow of queueItems) {
      const idEvento = Number(eventRow?.id_evento);
      const idUsuario = Number(eventRow?.id_usuario);
      const idVenta = Number(eventRow?.id_venta);

      if (!Number.isFinite(idUsuario) || idUsuario <= 0) {
        result.invalidUser += 1;
        try {
          await markNuevoServicioQueueEventProcessed(idEvento, {
            error: "id_usuario inválido",
          });
        } catch (markErr) {
          console.error("[Notificaciones] No se pudo cerrar evento con id_usuario inválido", markErr);
          result.failed += 1;
        }
        continue;
      }

      try {
        const plataformaEventoId = Number(eventRow?.id_plataforma);
        let esServicioEnProceso = false;
        if (Number.isFinite(plataformaEventoId) && plataformaEventoId > 0) {
          if (!entregaInmediataByPlataforma.has(plataformaEventoId)) {
            const { data: plataformaRow, error: platErr } = await supabaseAdmin
              .from("plataformas")
              .select("entrega_inmediata")
              .eq("id_plataforma", plataformaEventoId)
              .maybeSingle();
            if (platErr) throw platErr;
            entregaInmediataByPlataforma.set(
              plataformaEventoId,
              isTrue(plataformaRow?.entrega_inmediata),
            );
          }
          esServicioEnProceso = !entregaInmediataByPlataforma.get(plataformaEventoId);
        }
        if (Number.isFinite(idVenta) && idVenta > 0) {
          const duplicatePattern = `%ID Venta: #${idVenta}%`;
          const { data: existingRows, error: existingErr } = await supabaseAdmin
            .from("notificaciones")
            .select("id_notificacion")
            .eq("id_usuario", idUsuario)
            .eq("titulo", esServicioEnProceso ? "Servicio en proceso" : "Nuevo servicio")
            .ilike("mensaje", duplicatePattern)
            .limit(1);
          if (existingErr) throw existingErr;
          if ((existingRows || []).length) {
            result.duplicates += 1;
            try {
              const waRes = await maybeSendPendingSpotifyOrderToWhatsapp({
                idVenta,
                idPlataformaHint: plataformaEventoId,
              });
              if (waRes?.sent) result.whatsappSent += 1;
              else if (waRes?.skipped) result.whatsappSkipped += 1;
            } catch (waErr) {
              result.whatsappFailed += 1;
              console.error("[Notificaciones] WhatsApp pedido pendiente (duplicado) error", {
                id_evento: idEvento,
                id_venta: idVenta,
                error: waErr?.message || waErr,
              });
            }
            await markNuevoServicioQueueEventProcessed(idEvento);
            continue;
          }
        }

        const perfilTxt = normalizePerfilText(eventRow?.perfil);
        const plataformaTxt =
          String(eventRow?.plataforma || "").trim() ||
          (eventRow?.id_plataforma ? `Plataforma ${eventRow.id_plataforma}` : "Plataforma");
        const payload = esServicioEnProceso
          ? buildNotificationPayload(
              "servicio_en_proceso",
              {
                plataforma: plataformaTxt,
                fechaCorte: eventRow?.fecha_corte || "",
                idVenta: Number.isFinite(idVenta) && idVenta > 0 ? idVenta : null,
              },
              { idCuenta: toPositiveInt(eventRow?.id_cuenta) || null },
            )
          : buildNotificationPayload(
              "nuevo_servicio",
              {
                plataforma: plataformaTxt,
                correoCuenta: String(eventRow?.correo_cuenta || "").trim(),
                perfil: perfilTxt,
                fechaCorte: eventRow?.fecha_corte || "",
                idVenta: Number.isFinite(idVenta) && idVenta > 0 ? idVenta : null,
              },
              { idCuenta: toPositiveInt(eventRow?.id_cuenta) || null },
            );

        const { error: insErr } = await supabaseAdmin.from("notificaciones").insert({
          ...payload,
          id_usuario: idUsuario,
        });
        if (insErr) throw insErr;

        try {
          const waRes = await maybeSendPendingSpotifyOrderToWhatsapp({
            idVenta,
            idPlataformaHint: plataformaEventoId,
          });
          if (waRes?.sent) result.whatsappSent += 1;
          else if (waRes?.skipped) result.whatsappSkipped += 1;
        } catch (waErr) {
          result.whatsappFailed += 1;
          console.error("[Notificaciones] WhatsApp pedido pendiente error", {
            id_evento: idEvento,
            id_venta: idVenta,
            error: waErr?.message || waErr,
          });
        }

        await markNuevoServicioQueueEventProcessed(idEvento);
        result.sent += 1;
      } catch (itemErr) {
        result.failed += 1;
        try {
          await markNuevoServicioQueueEventError(idEvento, itemErr?.message || "Error al crear notificación");
        } catch (markErr) {
          console.error("[Notificaciones] No se pudo registrar error en cola", markErr);
        }
      }
    }

    nuevoServicioNotifQueueLastError = null;
    nuevoServicioNotifQueueLastResult = result;
    return { ...result };
  } catch (err) {
    nuevoServicioNotifQueueLastError = err?.message || "Error desconocido";
    throw err;
  } finally {
    nuevoServicioNotifQueueLastRunAt = new Date().toISOString();
    nuevoServicioNotifQueueInProgress = false;
  }
};

const processPendingSpotifyAdminAlerts = async () => {
  if (!WHATSAPP_PEDIDO_PENDIENTE_WATCHER_ENABLED || !shouldStartWhatsapp) {
    return {
      skipped: true,
      reason: "disabled",
      ...pendingSpotifyAlertsLastResult,
    };
  }
  if (pendingSpotifyAlertsInProgress) {
    return {
      skipped: true,
      reason: "in_progress",
      ...pendingSpotifyAlertsLastResult,
    };
  }

  pendingSpotifyAlertsInProgress = true;
  const result = {
    fetched: 0,
    sent: 0,
    failed: 0,
    skipped: 0,
  };
  let managedWhatsappForRun = false;

  try {
    let lastVentaId = 0;
    while (true) {
      let query = supabaseAdmin
        .from("ventas")
        .select(
          "id_venta, id_usuario, pendiente, aviso_admin, reportado, precios:precios!inner(id_plataforma, plataformas:plataformas(nombre)), usuarios:usuarios!ventas_id_usuario_fkey(nombre, apellido)",
        )
        .eq("pendiente", true)
        .not("reportado", "is", true)
        .or("aviso_admin.eq.false,aviso_admin.is.null")
        .order("id_venta", { ascending: true })
        .limit(WHATSAPP_PEDIDO_PENDIENTE_WATCHER_BATCH);
      if (lastVentaId > 0) {
        query = query.gt("id_venta", lastVentaId);
      }

      const { data: ventasRows, error: ventasErr } = await query;

      if (ventasErr && isMissingColumnError(ventasErr, "aviso_admin")) {
        if (!pendingSpotifyAlertsAvisoAdminMissing) {
          console.warn(
            "[WhatsApp] Worker de pedidos Spotify pendientes desactivado: falta columna ventas.aviso_admin.",
          );
        }
        pendingSpotifyAlertsAvisoAdminMissing = true;
        pendingSpotifyAlertsLastError = null;
        pendingSpotifyAlertsLastResult = result;
        return {
          skipped: true,
          reason: "missing_aviso_admin_column",
          ...result,
        };
      }
      if (ventasErr) throw ventasErr;

      if (pendingSpotifyAlertsAvisoAdminMissing) {
        console.log("[WhatsApp] Worker de pedidos Spotify pendientes reactivado.");
        pendingSpotifyAlertsAvisoAdminMissing = false;
      }

      const ventas = Array.isArray(ventasRows) ? ventasRows : [];
      if (!ventas.length) break;
      result.fetched += ventas.length;

      const batchMaxVentaId = ventas.reduce((maxId, row) => {
        const rowVentaId = toPositiveInt(row?.id_venta);
        if (!rowVentaId) return maxId;
        return rowVentaId > maxId ? rowVentaId : maxId;
      }, 0);

      for (const venta of ventas) {
        const idVenta = toPositiveInt(venta?.id_venta);
        if (!idVenta) {
          result.skipped += 1;
          continue;
        }

        try {
          if (!managedWhatsappForRun) {
            await ensureWhatsappClientReady({
              reason: "pending_spotify_orders_worker",
              allowWhenDisabled: true,
            });
            managedWhatsappForRun = true;
          }

          const sendRes = await maybeSendPendingSpotifyOrderToWhatsapp({
            idVenta,
            idPlataformaHint: toPositiveInt(
              Array.isArray(venta?.precios)
                ? venta?.precios?.[0]?.id_plataforma
                : venta?.precios?.id_plataforma,
            ),
            manageWhatsappLifecycle: false,
            ventaSnapshot: venta,
          });
          if (sendRes?.sent) result.sent += 1;
          else result.skipped += 1;
        } catch (err) {
          result.failed += 1;
          console.error("[WhatsApp] Worker pedidos Spotify pendientes error", {
            id_venta: idVenta,
            error: err?.message || err,
          });
        }
      }

      if (batchMaxVentaId <= 0 || ventas.length < WHATSAPP_PEDIDO_PENDIENTE_WATCHER_BATCH) {
        break;
      }
      lastVentaId = batchMaxVentaId;
    }

    if (!managedWhatsappForRun && isWhatsappClientActive() && !whatsappBootInProgress) {
      await shutdownWhatsappClient({
        reason: "pending_spotify_orders_worker_idle",
        allowWhenDisabled: true,
      });
    }

    pendingSpotifyAlertsLastError = null;
    pendingSpotifyAlertsLastResult = result;
    return { ...result };
  } catch (err) {
    pendingSpotifyAlertsLastError = err?.message || "Error desconocido";
    throw err;
  } finally {
    if (managedWhatsappForRun) {
      try {
        await shutdownWhatsappClient({
          reason: "pending_spotify_orders_worker_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[WhatsApp] No se pudo apagar el cliente tras pedidos pendientes", shutdownErr);
      }
    }
    pendingSpotifyAlertsLastRunAt = new Date().toISOString();
    pendingSpotifyAlertsInProgress = false;
  }
};

const parseCaracasOrderDateTime = (fechaRaw, horaRaw) => {
  const fechaMatch = String(fechaRaw || "").match(/\d{4}-\d{2}-\d{2}/);
  const horaMatch = String(horaRaw || "").match(/\d{2}:\d{2}:\d{2}/);
  if (!fechaMatch || !horaMatch) return null;
  const parsed = new Date(`${fechaMatch[0]}T${horaMatch[0]}-04:00`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
};

const hasManualVerificationWindowElapsed = (ordenRow = {}) => {
  const orderDate = parseCaracasOrderDateTime(ordenRow?.fecha, ordenRow?.hora_orden);
  if (!orderDate) return null;
  const elapsedMs = Date.now() - orderDate.getTime();
  return elapsedMs >= WHATSAPP_MANUAL_VERIFICATION_WINDOW_MS;
};

const processPendingManualVerificationAlerts = async () => {
  if (!WHATSAPP_MANUAL_VERIFICATION_WATCHER_ENABLED) {
    return {
      skipped: true,
      reason: "disabled",
      ...manualVerificationWatcherLastResult,
    };
  }
  if (manualVerificationWatcherInProgress) {
    return {
      skipped: true,
      reason: "in_progress",
      ...manualVerificationWatcherLastResult,
    };
  }

  manualVerificationWatcherInProgress = true;
  const result = {
    fetched: 0,
    sentWhatsapp: 0,
    notifCreated: 0,
    alreadyNotified: 0,
    skippedRecent: 0,
    skippedNoDate: 0,
    failed: 0,
    autoFetched: 0,
    autoSentWhatsapp: 0,
    autoAlreadyNotified: 0,
    autoSkippedRecent: 0,
    autoSkippedNoDate: 0,
    autoSkippedNoSale: 0,
    autoFailed: 0,
  };

  let managedWhatsappForRun = false;
  try {
    const { data: manualMetodoRows, error: manualMetodoErr } = await supabaseAdmin
      .from("metodos_de_pago")
      .select("id_metodo_de_pago")
      .eq("verificacion_automatica", false);
    if (manualMetodoErr) throw manualMetodoErr;

    const manualMetodoIds = (manualMetodoRows || [])
      .map((row) => toPositiveInt(row?.id_metodo_de_pago))
      .filter((id) => !!id);

    if (manualMetodoIds.length) {
      const { data: pendingOrders, error: pendingErr } = await supabaseAdmin
        .from("ordenes")
        .select(
          "id_orden, marcado_pago, checkout_finalizado, en_espera, pago_verificado, orden_cancelada, aviso_verificacion_manual",
        )
        .eq("marcado_pago", true)
        .eq("checkout_finalizado", true)
        .eq("pago_verificado", false)
        .in("id_metodo_de_pago", manualMetodoIds)
        .or("orden_cancelada.eq.false,orden_cancelada.is.null")
        .order("id_orden", { ascending: false })
        .limit(WHATSAPP_MANUAL_VERIFICATION_WATCHER_BATCH);
      if (pendingErr) throw pendingErr;

      const orders = Array.isArray(pendingOrders) ? pendingOrders : [];
      result.fetched = orders.length;

      for (const ordenRow of orders) {
        const ordenId = toPositiveInt(ordenRow?.id_orden);
        if (!ordenId) {
          result.failed += 1;
          continue;
        }
        if (isTrue(ordenRow?.aviso_verificacion_manual)) {
          result.alreadyNotified += 1;
          continue;
        }

        try {
          const notifyRes = await notifyManualVerificationToWhatsappAdmin({
            idOrden: ordenId,
            source: "manual_verification_watcher",
            manageWhatsappLifecycle: false,
          });
          if (!managedWhatsappForRun && isWhatsappClientActive()) {
            managedWhatsappForRun = true;
          }
          const notifCreated = notifyRes?.inbox_notification?.created === true;
          if (notifCreated) result.notifCreated += 1;
          if (notifyRes?.sent) {
            result.sentWhatsapp += 1;
            continue;
          }
          console.warn("[manual_verification_watcher] not sent", {
            id_orden: ordenId,
            reason: notifyRes?.reason || "unknown",
            skipped: !!notifyRes?.skipped,
            notif_created: notifCreated,
            target_user: notifyRes?.id_usuario_destino || null,
            phone: notifyRes?.phone || null,
            error: notifyRes?.error || null,
          });
          if (notifCreated) continue;
          result.failed += 1;
        } catch (notifyErr) {
          result.failed += 1;
          console.error("[manual_verification_watcher] notify error", {
            id_orden: ordenId,
            error: notifyErr?.message || notifyErr,
          });
        }
      }
    }

    const { data: autoMetodoRows, error: autoMetodoErr } = await supabaseAdmin
      .from("metodos_de_pago")
      .select("id_metodo_de_pago")
      .eq("verificacion_automatica", true);
    if (autoMetodoErr) throw autoMetodoErr;

    const autoMetodoIds = (autoMetodoRows || [])
      .map((row) => toPositiveInt(row?.id_metodo_de_pago))
      .filter((id) => !!id);
    if (autoMetodoIds.length) {
      const { data: autoPendingOrders, error: autoPendingErr } = await supabaseAdmin
        .from("ordenes")
        .select(
          "id_orden, fecha, hora_orden, marcado_pago, checkout_finalizado, pago_verificado, orden_cancelada, aviso_verificacion_manual",
        )
        .eq("marcado_pago", true)
        .eq("checkout_finalizado", true)
        .eq("pago_verificado", false)
        .in("id_metodo_de_pago", autoMetodoIds)
        .or("orden_cancelada.eq.false,orden_cancelada.is.null")
        .order("id_orden", { ascending: false })
        .limit(WHATSAPP_MANUAL_VERIFICATION_WATCHER_BATCH);
      if (autoPendingErr) throw autoPendingErr;

      const autoOrders = Array.isArray(autoPendingOrders) ? autoPendingOrders : [];
      result.autoFetched = autoOrders.length;

      for (const ordenRow of autoOrders) {
        const ordenId = toPositiveInt(ordenRow?.id_orden);
        if (!ordenId) {
          result.autoFailed += 1;
          continue;
        }
        if (isTrue(ordenRow?.aviso_verificacion_manual)) {
          result.autoAlreadyNotified += 1;
          continue;
        }

        const isExpired = hasManualVerificationWindowElapsed(ordenRow);
        if (isExpired === null) {
          result.autoSkippedNoDate += 1;
          continue;
        }
        if (isExpired !== true) {
          result.autoSkippedRecent += 1;
          continue;
        }

        try {
          const notifyRes = await notifyManualVerificationToWhatsappAdmin({
            idOrden: ordenId,
            source: "auto_verification_timeout_watcher",
            manageWhatsappLifecycle: false,
            allowAutomaticAfterWindow: true,
          });
          if (!managedWhatsappForRun && isWhatsappClientActive()) {
            managedWhatsappForRun = true;
          }
          const notifCreated = notifyRes?.inbox_notification?.created === true;
          if (notifCreated) result.notifCreated += 1;
          if (notifyRes?.sent) {
            result.autoSentWhatsapp += 1;
            continue;
          }
          if (
            notifyRes?.reason === "already_flagged_manual_verification" ||
            notifyRes?.reason === "payment_method_verification_window_not_elapsed"
          ) {
            result.autoAlreadyNotified += 1;
            continue;
          }
          console.warn("[auto_verification_timeout_watcher] not sent", {
            id_orden: ordenId,
            reason: notifyRes?.reason || "unknown",
            skipped: !!notifyRes?.skipped,
            notif_created: notifCreated,
            target_user: notifyRes?.id_usuario_destino || null,
            phone: notifyRes?.phone || null,
            error: notifyRes?.error || null,
          });
          if (notifyRes?.skipped || notifCreated) {
            result.autoSkippedNoSale += 1;
            continue;
          }
          result.autoFailed += 1;
        } catch (notifyErr) {
          result.autoFailed += 1;
          console.error("[auto_verification_timeout_watcher] notify error", {
            id_orden: ordenId,
            error: notifyErr?.message || notifyErr,
          });
        }
      }
    }

    // Reintenta avisos al cliente para órdenes automáticas ya verificadas que quedaron sin notificar.
    if (shouldStartWhatsapp && autoMetodoIds.length) {
      const { data: autoVerifiedOrders, error: autoVerifiedErr } = await supabaseAdmin
        .from("ordenes")
        .select(
          "id_orden, fecha, hora_orden, marcado_pago, checkout_finalizado, pago_verificado, orden_cancelada, aviso_verificacion_pago",
        )
        .eq("marcado_pago", true)
        .eq("checkout_finalizado", true)
        .eq("pago_verificado", true)
        .in("id_metodo_de_pago", autoMetodoIds)
        .or("orden_cancelada.eq.false,orden_cancelada.is.null")
        .order("id_orden", { ascending: false })
        .limit(WHATSAPP_MANUAL_VERIFICATION_WATCHER_BATCH);
      if (autoVerifiedErr) throw autoVerifiedErr;

      const autoOrdersVerified = Array.isArray(autoVerifiedOrders) ? autoVerifiedOrders : [];
      result.autoFetched += autoOrdersVerified.length;

      for (const ordenRow of autoOrdersVerified) {
        const ordenId = toPositiveInt(ordenRow?.id_orden);
        if (!ordenId) {
          result.autoFailed += 1;
          continue;
        }
        if (isTrue(ordenRow?.aviso_verificacion_pago)) {
          result.autoAlreadyNotified += 1;
          continue;
        }

        const isExpired = hasManualVerificationWindowElapsed(ordenRow);
        if (isExpired === null) {
          result.autoSkippedNoDate += 1;
          continue;
        }
        if (isExpired !== true) {
          result.autoSkippedRecent += 1;
          continue;
        }

        try {
          const notifyRes = await notifyVentaEntregadaAfterOrderVerified({
            idOrden: ordenId,
            manageWhatsappLifecycle: false,
          });
          if (!managedWhatsappForRun && isWhatsappClientActive()) {
            managedWhatsappForRun = true;
          }
          if (notifyRes?.sent) {
            result.autoSentWhatsapp += 1;
            continue;
          }
          if (notifyRes?.reason === "order_payment_already_notified") {
            result.autoAlreadyNotified += 1;
            continue;
          }
          if (notifyRes?.reason === "no_delivered_non_reported_sale") {
            result.autoSkippedNoSale += 1;
            continue;
          }
          console.warn("[auto_verified_payment_watcher] not sent", {
            id_orden: ordenId,
            reason: notifyRes?.reason || "unknown",
            skipped: !!notifyRes?.skipped,
            target_user: notifyRes?.id_usuario_destino || null,
            phone: notifyRes?.phone || null,
            error: notifyRes?.error || null,
          });
          if (notifyRes?.skipped) {
            result.autoSkippedNoSale += 1;
            continue;
          }
          result.autoFailed += 1;
        } catch (notifyErr) {
          result.autoFailed += 1;
          console.error("[auto_verified_payment_watcher] notify error", {
            id_orden: ordenId,
            error: notifyErr?.message || notifyErr,
          });
        }
      }
    }

    manualVerificationWatcherLastError = null;
    manualVerificationWatcherLastResult = result;
    return { ...result };
  } catch (err) {
    manualVerificationWatcherLastError = err?.message || "Error desconocido";
    throw err;
  } finally {
    if (managedWhatsappForRun) {
      try {
        await shutdownWhatsappClient({
          reason: "manual_verification_watcher_completed",
          allowWhenDisabled: true,
        });
      } catch (shutdownErr) {
        console.error("[manual_verification_watcher] whatsapp shutdown error", shutdownErr);
      }
    }
    manualVerificationWatcherLastRunAt = new Date().toISOString();
    manualVerificationWatcherInProgress = false;
  }
};

const ensureDailyRecordatoriosState = (dateStr) => {
  if (!dateStr) return;
  if (recordatoriosEnviadosDate === dateStr) return;
  recordatoriosEnviadosDate = dateStr;
  recordatoriosEnviados = false;
  lastAutoRecordatoriosRunDate = null;
  autoRecordatoriosRetryPending = false;
  lastAutoRecordatoriosState = {
    date: dateStr,
    status: "idle",
    attemptedAt: null,
    completedAt: null,
    total: 0,
    sent: 0,
    failed: 0,
    skippedNoPhone: 0,
    skippedInvalidPhone: 0,
    updatedVentas: 0,
    sameDayCutoffTotal: 0,
    sameDayCutoffSent: 0,
    sameDayCutoffFailed: 0,
    sameDayPlatform12Total: 0,
    sameDayPlatform12Sent: 0,
    sameDayPlatform12Failed: 0,
    updatedVentasCorteHoy: 0,
    recordatoriosEnviados: false,
    error: null,
  };
  console.log(
    `[WhatsApp] Reset diario ${dateStr} ${String(WHATSAPP_RESET_HOUR).padStart(2, "0")}:00 America/Caracas: recordatorios_enviados=false`,
  );
};

const loadWhatsappRecordatorioUsers = async ({ targetUserIds = null } = {}) => {
  const userIdsFilter = uniqPositiveIds(targetUserIds || []);
  if (targetUserIds && !userIdsFilter.length) return [];
  return fetchAllSupabaseRows(
    (from, to) => {
      let query = supabaseAdmin
        .from("usuarios")
        .select("id_usuario, nombre, apellido, telefono, fecha_registro, id_auth, recordatorio_dias_antes")
        .order("id_usuario", { ascending: true })
        .range(from, to);
      if (userIdsFilter.length) {
        query = query.in("id_usuario", userIdsFilter);
      }
      return query;
    },
    { label: "recordatorios:usuarios" },
  );
};

const loadWhatsappRecordatorioVentas = async ({
  usersList = [],
  mode = "pending",
} = {}) => {
  const effectiveMode = normalizeWhatsappRecordatorioMode(mode);
  if (!Array.isArray(usersList) || !usersList.length) return [];

  const userIds = uniqPositiveIds(usersList.map((user) => user.id_usuario));
  if (!userIds.length) return [];

  const todayCaracas = getCaracasDateStr(0);
  const userById = usersList.reduce((acc, user) => {
    const userId = Number(user?.id_usuario);
    if (!Number.isFinite(userId) || userId <= 0) return acc;
    acc[userId] = user;
    return acc;
  }, {});

  if (effectiveMode === "cutoff_today") {
    const ventas = await fetchAllSupabaseRows(
      (from, to) =>
        supabaseAdmin
          .from("ventas")
          .select(
            "id_usuario, id_cuenta, id_precio, id_venta, id_perfil, fecha_corte, correo_miembro, recordatorio_corte_enviado",
          )
          .in("id_usuario", userIds)
          .eq("fecha_corte", todayCaracas)
          .or("recordatorio_corte_enviado.eq.false,recordatorio_corte_enviado.is.null")
          .order("id_venta", { ascending: true })
          .range(from, to),
      { label: "recordatorios:ventas:cutoff_today" },
    );

    return ventas.filter((venta) => {
      const fechaCorte = String(venta?.fecha_corte || "").trim().slice(0, 10);
      if (!fechaCorte || fechaCorte !== todayCaracas) return false;
      return !isTrue(venta?.recordatorio_corte_enviado);
    });
  }

  const maxRecordatorioDiasAntes = usersList.reduce((max, user) => {
    return Math.max(max, normalizeRecordatorioDiasAntes(user?.recordatorio_dias_antes, 1));
  }, 1);
  const fechaMaxRecordatorio = getCaracasDateStr(maxRecordatorioDiasAntes);

  const ventas = await fetchAllSupabaseRows(
    (from, to) =>
      supabaseAdmin
        .from("ventas")
        .select(
          "id_usuario, id_cuenta, id_precio, id_venta, id_perfil, fecha_corte, correo_miembro, recordatorio_enviado",
        )
        .in("id_usuario", userIds)
        .lte("fecha_corte", fechaMaxRecordatorio)
        .or("recordatorio_enviado.eq.false,recordatorio_enviado.is.null")
        .order("id_venta", { ascending: true })
        .range(from, to),
    { label: "recordatorios:ventas:pending" },
  );

  return ventas.filter((venta) => {
    const user = userById[Number(venta?.id_usuario)] || null;
    const diasAntes = normalizeRecordatorioDiasAntes(user?.recordatorio_dias_antes, 1);
    const fechaLimite = getCaracasDateStr(diasAntes);
    const fechaCorte = String(venta?.fecha_corte || "").trim().slice(0, 10);
    if (!fechaCorte) return false;
    if (fechaCorte < todayCaracas) return true;
    return fechaCorte <= fechaLimite;
  });
};

const loadWhatsappRecordatorioContextMaps = async (ventasList = []) => {
  const cuentasIds = uniqPositiveIds(ventasList.map((venta) => venta.id_cuenta));
  const precioIds = uniqPositiveIds(ventasList.map((venta) => venta.id_precio));
  const perfilIds = uniqPositiveIds(ventasList.map((venta) => venta.id_perfil));

  const [cuentas, precios, perfiles] = await Promise.all([
    cuentasIds.length
      ? fetchAllSupabaseRows(
          (from, to) =>
            supabaseAdmin
              .from("cuentas")
              .select("id_cuenta, correo, id_plataforma")
              .in("id_cuenta", cuentasIds)
              .order("id_cuenta", { ascending: true })
              .range(from, to),
          { label: "recordatorios:cuentas" },
        )
      : Promise.resolve([]),
    precioIds.length
      ? fetchAllSupabaseRows(
          (from, to) =>
            supabaseAdmin
              .from("precios")
              .select("id_precio, id_plataforma")
              .in("id_precio", precioIds)
              .order("id_precio", { ascending: true })
              .range(from, to),
          { label: "recordatorios:precios" },
        )
      : Promise.resolve([]),
    perfilIds.length
      ? fetchAllSupabaseRows(
          (from, to) =>
            supabaseAdmin
              .from("perfiles")
              .select("id_perfil, n_perfil, perfil_hogar")
              .in("id_perfil", perfilIds)
              .order("id_perfil", { ascending: true })
              .range(from, to),
          { label: "recordatorios:perfiles" },
        )
      : Promise.resolve([]),
  ]);

  const platIds = uniqPositiveIds([
    ...cuentas.map((cuenta) => cuenta.id_plataforma),
    ...precios.map((precio) => precio.id_plataforma),
  ]);
  const plats = platIds.length
    ? await fetchAllSupabaseRows(
        (from, to) =>
          supabaseAdmin
            .from("plataformas")
            .select("id_plataforma, nombre, correo_cliente, por_pantalla, por_acceso")
            .in("id_plataforma", platIds)
            .order("id_plataforma", { ascending: true })
            .range(from, to),
        { label: "recordatorios:plataformas" },
      )
    : [];

  return {
    mapCuenta: cuentas.reduce((acc, cuenta) => {
      acc[cuenta.id_cuenta] = cuenta;
      return acc;
    }, {}),
    mapPrecio: precios.reduce((acc, precio) => {
      acc[precio.id_precio] = precio;
      return acc;
    }, {}),
    mapPlat: plats.reduce((acc, plat) => {
      acc[plat.id_plataforma] = plat;
      return acc;
    }, {}),
    mapPerf: perfiles.reduce((acc, perf) => {
      acc[perf.id_perfil] = { n: perf.n_perfil, hogar: perf.perfil_hogar === true };
      return acc;
    }, {}),
  };
};

const buildWhatsappRecordatorioItems = async ({
  targetUserIds = null,
  mode = "pending",
} = {}) => {
  const effectiveMode = normalizeWhatsappRecordatorioMode(mode);
  if (effectiveMode === "cutoff_today_platform_12") {
    return [];
  }
  const usersList = await loadWhatsappRecordatorioUsers({ targetUserIds });
  if (!usersList.length) return [];

  const ventasList = await loadWhatsappRecordatorioVentas({
    usersList,
    mode: effectiveMode,
  });
  if (!ventasList.length) return [];

  const { mapCuenta, mapPrecio, mapPlat, mapPerf } = await loadWhatsappRecordatorioContextMaps(
    ventasList,
  );

  const mapUser = usersList.reduce((acc, user) => {
    const userId = Number(user.id_usuario);
    if (!Number.isFinite(userId) || userId <= 0) return acc;
    const isRegistered = isUsuarioWebRegistrado(user);
    let signupUrl = "";
    if (!isRegistered) {
      try {
        signupUrl = buildSignupRegistrationUrl(userId);
      } catch (err) {
        console.error("[recordatorios] signup url build error", { userId, err });
      }
    }
    acc[userId] = {
      cliente: [user.nombre, user.apellido].filter(Boolean).join(" ").trim() || `Usuario ${userId}`,
      telefono: String(user.telefono || "").trim(),
      registrado: isRegistered,
      signupUrl,
      recordatorioDiasAntes: normalizeRecordatorioDiasAntes(user?.recordatorio_dias_antes, 1),
    };
    return acc;
  }, {});

  const grouped = ventasList.reduce((acc, venta) => {
    const userId = Number(venta.id_usuario);
    if (!Number.isFinite(userId) || userId <= 0) return acc;

    const userInfo = mapUser[userId] || {};
    const cuenta = mapCuenta[venta.id_cuenta] || {};
    const precio = mapPrecio[venta.id_precio] || {};
    const platId = cuenta.id_plataforma || precio.id_plataforma || null;
    const platInfo = platId ? mapPlat[platId] || {} : {};
    const platNombre = platId ? platInfo.nombre || `Plataforma ${platId}` : "-";
    const platNombreUpper = String(platNombre || "-").toLocaleUpperCase("es-VE");
    const useCorreoCliente =
      platInfo.correo_cliente === true ||
      platInfo.correo_cliente === "true" ||
      platInfo.correo_cliente === "1";
    const allowPerfilByScreen =
      platInfo.por_pantalla === true ||
      platInfo.por_pantalla === "true" ||
      platInfo.por_pantalla === "1" ||
      platInfo.por_pantalla === 1 ||
      platInfo.por_pantalla === "t";
    const allowPerfilByAccess =
      platInfo.por_acceso === true ||
      platInfo.por_acceso === "true" ||
      platInfo.por_acceso === "1" ||
      platInfo.por_acceso === 1 ||
      platInfo.por_acceso === "t";
    const correo = useCorreoCliente ? venta.correo_miembro || "-" : cuenta.correo || "-";
    const perfInfo = venta.id_perfil ? mapPerf[venta.id_perfil] : null;
    const perfilTxt =
      !allowPerfilByScreen && allowPerfilByAccess
        ? "Acceso: 1 dispositivo"
        : allowPerfilByScreen && perfInfo?.n
          ? `Perfil: M${perfInfo.n}`
          : "";
    const isNetflix = Number(platId) === 1;
    const isPlan2Precio = Number(venta.id_precio) === 4 || Number(venta.id_precio) === 5;
    const isNetflixPlan2 = isNetflix && (isPlan2Precio || perfInfo?.hogar);
    const hogarTxt = isNetflixPlan2 ? " (HOGAR ACTUALIZADO)" : "";

    if (!acc[userId]) {
      const isRegistered = userInfo?.registrado === true;
      let signupUrl = "";
      if (!isRegistered) {
        signupUrl = String(userInfo.signupUrl || "").trim();
        if (!signupUrl) {
          try {
            signupUrl = buildSignupRegistrationUrl(userId);
          } catch (err) {
            console.error("[recordatorios] signup url fallback build error", { userId, err });
          }
        }
      }
      acc[userId] = {
        idUsuario: userId,
        cliente: userInfo.cliente || `Usuario ${userId}`,
        telefono: userInfo.telefono || "",
        registrado: isRegistered,
        signupUrl,
        plataformas: {},
        ventaIds: [],
        platformIds: [],
        ventaIdsByPlatform: {},
        fechasPago: [],
      };
    }

    const platDisplayName = `${platNombreUpper}${hogarTxt}`;
    const platformKey = `${String(platId || platNombre || "-")}::${hogarTxt ? "hogar" : "normal"}`;
    if (!acc[userId].plataformas[platformKey]) {
      acc[userId].plataformas[platformKey] = {
        nombre: platDisplayName,
        detalles: [],
      };
    }

    if (venta.id_venta) acc[userId].ventaIds.push(venta.id_venta);
    const normalizedPlatId = toPositiveInt(platId);
    if (normalizedPlatId) {
      acc[userId].platformIds.push(normalizedPlatId);
      if (!Array.isArray(acc[userId].ventaIdsByPlatform[normalizedPlatId])) {
        acc[userId].ventaIdsByPlatform[normalizedPlatId] = [];
      }
      if (venta.id_venta) {
        acc[userId].ventaIdsByPlatform[normalizedPlatId].push(venta.id_venta);
      }
    }
    if (venta.fecha_corte) acc[userId].fechasPago.push(venta.fecha_corte);

    const detalle = [
      `\`ID VENTA: #${venta.id_venta}\``,
      `Correo: ${correo}`,
      perfilTxt || null,
    ]
      .filter(Boolean)
      .join("\n");
    acc[userId].plataformas[platformKey].detalles.push(detalle);
    return acc;
  }, {});

  return Object.values(grouped).map((group) => {
    const bloques = Object.values(group.plataformas || {})
      .map((plat) => {
        const detalles = (plat.detalles || []).join("\n\n");
        return `*${plat.nombre || "-"}*\n${detalles}`;
      })
      .join("\n\n");
    const servicios = Object.values(group.plataformas || {})
      .map((plat) => String(plat?.nombre || "").trim())
      .filter(Boolean);

    let plain = "";
    const ventaIds = uniqPositiveIds(group.ventaIds || []).sort((a, b) => a - b);
    let renewalCartToken = "";
    let renewalCartUrl = "";
    let signupRenewalUrl = String(group.signupUrl || "").trim();
    if (ventaIds.length) {
      try {
        renewalCartUrl = buildRenewalCartUrl({
          idUsuario: group.idUsuario,
          ventaIds,
        });
        const renewalUrlObj = new URL(renewalCartUrl);
        renewalCartToken = String(renewalUrlObj.searchParams.get("rr") || "").trim();
        if (!renewalCartToken) {
          const shortMatch = renewalUrlObj.pathname.match(/\/r\/([^/?#]+)/i);
          if (shortMatch?.[1]) {
            renewalCartToken = decodeURIComponent(shortMatch[1]).trim();
          }
        }

        if (!group.registrado && renewalCartToken) {
          signupRenewalUrl = buildSignupRegistrationUrl(group.idUsuario, {
            renewalToken: renewalCartToken,
          });
        }
      } catch (err) {
        console.error("[recordatorios] renewal cart url build error", {
          userId: group.idUsuario,
          err,
        });
      }
    }
    const renewalTargetUrl = String(
      group.registrado ? renewalCartUrl : signupRenewalUrl || renewalCartUrl,
    ).trim();
    const fechasPagoUnicas = Array.from(
      new Set(
        (Array.isArray(group.fechasPago) ? group.fechasPago : [])
          .map((fecha) => (fecha ? formatDDMMYYYY(fecha) : ""))
          .filter(Boolean),
      ),
    );
    const fechaPagoMensaje =
      fechasPagoUnicas.length === 1
        ? fechasPagoUnicas[0]
        : fechasPagoUnicas.length > 1
          ? fechasPagoUnicas.join(" / ")
          : "-";
    const fechaPagoHeader = `\`Fecha de pago: ${fechaPagoMensaje}\``;

    if (effectiveMode === "cutoff_today") {
      const cutoffRenewalUrl = renewalTargetUrl || buildPublicSiteUrl();
      plain = `🚨 *HOY* vencen tus membresías\nAñade tus renovaciones al carrito automaticamente:\n${cutoffRenewalUrl}\n\n${fechaPagoHeader}\n\n${bloques}\n\nRenueva ahora para seguir disfrutando de nuestros servicios sin interrupciones 🔁✨`;
    } else if (renewalTargetUrl) {
      const saludo = `*¡Hola ${group.cliente}! ❤️🫎*`;
      const intro = `Añade tus renovaciones al carrito automaticamente:\n${renewalTargetUrl}`;
      plain = `${saludo}\n${intro}\n\n${fechaPagoHeader}\n\n${bloques}\n\nRenueva ahora para seguir disfrutando de nuestros servicios sin interrupciones 🔁✨`;
    } else {
      const saludo = `*¡Hola ${group.cliente}! ❤️🫎*`;
      const renewUrl = signupRenewalUrl || buildPublicSiteUrl();
      const intro = `Renueva tus membresías por nuestra nueva pagina web:\n${renewUrl}`;
      plain = `${saludo}\n${intro}\n\n${fechaPagoHeader}\n\n${bloques}\n\nRenueva ahora para seguir disfrutando de nuestros servicios sin interrupciones 🔁✨`;
    }

    return {
      idUsuario: group.idUsuario,
      cliente: group.cliente,
      telefonoRaw: group.telefono,
      phone: normalizeWhatsappPhone(group.telefono),
      plain,
      servicios,
      ventaIds: uniqPositiveIds(group.ventaIds),
      platformIds: uniqPositiveIds(group.platformIds),
      ventaIdsByPlatform: Object.entries(group.ventaIdsByPlatform || {}).reduce(
        (acc, [platId, ids]) => {
          const normalizedPlatId = toPositiveInt(platId);
          if (!normalizedPlatId) return acc;
          acc[normalizedPlatId] = uniqPositiveIds(ids || []);
          return acc;
        },
        {},
      ),
      mode: effectiveMode,
    };
  });
};

const buildWhatsappCutoffPlatform12Items = (cutoffTodayItems = []) => {
  const items = Array.isArray(cutoffTodayItems) ? cutoffTodayItems : [];
  return items
    .filter((item) => {
      const platformIds = uniqPositiveIds(item?.platformIds || []);
      return platformIds.includes(WHATSAPP_CUTOFF_PLATFORM_12_ID);
    })
    .map((item) => {
      const ventaIdsPlat12 = uniqPositiveIds(
        item?.ventaIdsByPlatform?.[WHATSAPP_CUTOFF_PLATFORM_12_ID] || [],
      );
      return {
        ...item,
        plain: WHATSAPP_CUTOFF_PLATFORM_12_MESSAGE,
        ventaIds: ventaIdsPlat12,
        mode: "cutoff_today_platform_12",
      };
    });
};

const buildWhatsappRecordatorioPreviewItems = (
  items = [],
  { pendingReason = "Pendiente de envío manual" } = {},
) => {
  return (Array.isArray(items) ? items : []).map((item) => {
    const hasRawPhone = Boolean(String(item?.telefonoRaw || "").trim());
    if (!hasRawPhone) {
      return {
        ...item,
        status: "skipped_no_phone",
        error: "Cliente sin teléfono registrado",
      };
    }
    if (!item?.phone) {
      return {
        ...item,
        status: "skipped_invalid_phone",
        error: `Teléfono inválido: ${item?.telefonoRaw || "sin valor"}`,
      };
    }
    return {
      ...item,
      status: "pending",
      error: String(pendingReason || "Pendiente de envío manual"),
    };
  });
};

const buildWhatsappPendingNoPhoneClients = async ({ targetUserIds = null } = {}) => {
  const items = await buildWhatsappRecordatorioItems({ targetUserIds });
  const grouped = {};

  (Array.isArray(items) ? items : []).forEach((item) => {
    const rawPhone = String(item?.telefonoRaw || "").trim();
    const hasRawPhone = Boolean(rawPhone);
    const hasValidPhone = Boolean(item?.phone);
    if (hasRawPhone && hasValidPhone) return;

    const idUsuario = Number(item?.idUsuario);
    const key = Number.isFinite(idUsuario) && idUsuario > 0 ? String(idUsuario) : String(item?.cliente || "cliente");
    if (!grouped[key]) {
      grouped[key] = {
        idUsuario: Number.isFinite(idUsuario) && idUsuario > 0 ? idUsuario : null,
        cliente: String(item?.cliente || "Cliente"),
        telefono: rawPhone,
        reason: hasRawPhone ? "invalid_phone" : "missing_phone",
        ventaIds: [],
      };
    } else {
      // Si hay múltiples filas para el mismo cliente, prioriza reason missing_phone.
      if (grouped[key].reason !== "missing_phone" && !hasRawPhone) {
        grouped[key].reason = "missing_phone";
      }
      if (!grouped[key].telefono && rawPhone) {
        grouped[key].telefono = rawPhone;
      }
    }

    const ventas = uniqPositiveIds(item?.ventaIds || []);
    grouped[key].ventaIds = uniqPositiveIds([...(grouped[key].ventaIds || []), ...ventas]);
  });

  return Object.values(grouped).sort((a, b) => {
    const aName = String(a?.cliente || "").toLowerCase();
    const bName = String(b?.cliente || "").toLowerCase();
    return aName.localeCompare(bName);
  });
};

const isWhatsappRecordatorioSendable = (item = {}) => {
  const hasRawPhone = Boolean(String(item?.telefonoRaw || "").trim());
  return hasRawPhone && Boolean(item?.phone);
};

const countWhatsappSendableRecordatorios = (items = []) => {
  return (Array.isArray(items) ? items : []).filter((item) => isWhatsappRecordatorioSendable(item))
    .length;
};

const resetRecordatoriosSendLockMeta = () => {
  recordatoriosSendLockMeta = {
    source: null,
    startedAt: null,
    heartbeatAt: null,
    mode: null,
    total: 0,
    processed: 0,
    sent: 0,
    failed: 0,
    skippedNoPhone: 0,
    skippedInvalidPhone: 0,
    lastCliente: null,
  };
};

const touchRecordatoriosSendLock = (partial = {}) => {
  const nowIso = new Date().toISOString();
  recordatoriosSendLockMeta = {
    ...recordatoriosSendLockMeta,
    ...partial,
    heartbeatAt: nowIso,
  };
};

const getRecordatoriosSendLockState = () => {
  const nowMs = Date.now();
  const startedMs = Date.parse(recordatoriosSendLockMeta?.startedAt || "");
  const heartbeatMs = Date.parse(
    recordatoriosSendLockMeta?.heartbeatAt || recordatoriosSendLockMeta?.startedAt || "",
  );
  const ageMs = Number.isFinite(startedMs) ? Math.max(0, nowMs - startedMs) : null;
  const idleMs = Number.isFinite(heartbeatMs) ? Math.max(0, nowMs - heartbeatMs) : null;
  return {
    ...recordatoriosSendLockMeta,
    active: recordatoriosSendInProgress,
    staleAfterMs: WHATSAPP_RECORDATORIOS_LOCK_STALE_MS,
    ageMs,
    idleMs,
  };
};

const isRecordatoriosSendLockStale = () => {
  if (!recordatoriosSendInProgress) return false;
  const lockState = getRecordatoriosSendLockState();
  if (!Number.isFinite(lockState.idleMs)) return true;
  return lockState.idleMs > WHATSAPP_RECORDATORIOS_LOCK_STALE_MS;
};

const buildEmptyWhatsappRecordatorioSendResult = (source = "", mode = "pending") => ({
  source,
  mode: normalizeWhatsappRecordatorioMode(mode),
  total: 0,
  sent: 0,
  failed: 0,
  skippedNoPhone: 0,
  skippedInvalidPhone: 0,
  updatedVentas: 0,
  items: [],
});

const sendWhatsappRecordatorioBatch = async ({
  source = "manual",
  items = [],
  mode = "pending",
  onProgress = null,
} = {}) => {
  const effectiveMode = normalizeWhatsappRecordatorioMode(mode);
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) {
    return buildEmptyWhatsappRecordatorioSendResult(source, effectiveMode);
  }

  if (!isWhatsappReady()) {
    const notReadyErr = new Error("WhatsApp no listo");
    notReadyErr.code = "WHATSAPP_NOT_READY";
    throw notReadyErr;
  }

  const client = getWhatsappClient();
  const updatedVentaIds = new Set();
  const processedItems = [];
  const sendableItemsCount = countWhatsappSendableRecordatorios(rows);
  console.log(
    `[WhatsApp] Recordatorios ${source}:${effectiveMode}: inicio procesamiento total=${rows.length}, enviables=${sendableItemsCount}`,
  );
  let sendableProcessed = 0;
  let sent = 0;
  let failed = 0;
  let skippedNoPhone = 0;
  let skippedInvalidPhone = 0;
  const emitProgress = (extra = {}) => {
    if (typeof onProgress !== "function") return;
    const processed = sent + failed + skippedNoPhone + skippedInvalidPhone;
    try {
      onProgress({
        source,
        mode: effectiveMode,
        total: rows.length,
        sendable: sendableItemsCount,
        processed,
        sent,
        failed,
        skippedNoPhone,
        skippedInvalidPhone,
        ...extra,
      });
    } catch (progressErr) {
      console.error("[WhatsApp] recordatorios progress callback error", progressErr);
    }
  };
  emitProgress({ stage: "start" });

  for (const item of rows) {
    const hasRawPhone = Boolean(String(item.telefonoRaw || "").trim());
    if (!hasRawPhone) {
      skippedNoPhone += 1;
      processedItems.push({
        ...item,
        status: "skipped_no_phone",
        error: "Cliente sin teléfono registrado",
      });
      console.log(
        `[WhatsApp] Recordatorios ${source}:${effectiveMode}: omitido_sin_telefono cliente="${item.cliente || "Cliente"}"`,
      );
      emitProgress({ stage: "skip_no_phone", item });
      continue;
    }
    if (!item.phone) {
      skippedInvalidPhone += 1;
      processedItems.push({
        ...item,
        status: "skipped_invalid_phone",
        error: `Teléfono inválido: ${item.telefonoRaw || "sin valor"}`,
      });
      console.log(
        `[WhatsApp] Recordatorios ${source}:${effectiveMode}: omitido_telefono_invalido cliente="${item.cliente || "Cliente"}" telefono="${item.telefonoRaw || ""}"`,
      );
      emitProgress({ stage: "skip_invalid_phone", item });
      continue;
    }

    const progressIndex = sendableProcessed + 1;
    const progressTag = `${progressIndex}/${sendableItemsCount}`;
    const chatId = `${item.phone}@c.us`;
    try {
      console.log(
        `[WhatsApp] Recordatorios ${source}:${effectiveMode}: ${progressTag} enviando cliente="${item.cliente || "Cliente"}" phone="${item.phone}"`,
      );
      await withTimeout(
        client.sendMessage(chatId, item.plain, {
          linkPreview: false,
          waitUntilMsgSent: true,
        }),
        WHATSAPP_SEND_TIMEOUT_MS,
        `Timeout enviando WhatsApp a ${item.phone}`,
      );

      const ventaIdsItem = uniqPositiveIds(item.ventaIds || []);
      if (ventaIdsItem.length && effectiveMode !== "cutoff_today_platform_12") {
        const fechaCaracas = getCaracasDateStr(0);
        const updates =
          effectiveMode === "cutoff_today"
            ? { recordatorio_corte_enviado: true }
            : {
                recordatorio_enviado: true,
                fecha_recordatorio_enviado: fechaCaracas,
              };
        const { error: updateErr } = await runSupabaseQueryWithRetry(
          () =>
            supabaseAdmin
              .from("ventas")
              .update(updates)
              .in("id_venta", ventaIdsItem),
          `recordatorios:update:${effectiveMode}`,
        );
        if (updateErr) throw updateErr;
        ventaIdsItem.forEach((id) => updatedVentaIds.add(id));
      }

      sent += 1;
      processedItems.push({ ...item, status: "sent" });
      console.log(
        `[WhatsApp] Recordatorios ${source}:${effectiveMode}: ${progressTag} enviado cliente="${item.cliente || "Cliente"}" phone="${item.phone}" ventas=${ventaIdsItem.length}`,
      );
      emitProgress({ stage: "sent", item });
    } catch (err) {
      failed += 1;
      const errLog = buildWhatsappErrorLog(err);
      processedItems.push({
        ...item,
        status: "failed",
        error: err?.message || "No se pudo enviar",
      });
      console.error(
        `[WhatsApp] Recordatorios ${source}:${effectiveMode}: ${progressTag} fallido cliente="${item.cliente || "Cliente"}" phone="${item.phone}" raw_phone="${item.telefonoRaw || ""}" chat_id="${chatId}"`,
        errLog,
      );
      emitProgress({ stage: "failed", item, error: err?.message || "No se pudo enviar" });
    }

    sendableProcessed += 1;
    if (sendableProcessed < sendableItemsCount) {
      const delayMs = randomWhatsappDelayMs();
      console.log(
        `[WhatsApp] Recordatorios ${source}:${effectiveMode}: espera ${Math.round(delayMs / 1000)}s antes del siguiente envío`,
      );
      await sleep(delayMs);
    }
  }
  emitProgress({ stage: "done" });

  return {
    source,
    mode: effectiveMode,
    total: rows.length,
    sent,
    failed,
    skippedNoPhone,
    skippedInvalidPhone,
    updatedVentas: updatedVentaIds.size,
    items: processedItems,
  };
};

const attemptWhatsappRecordatoriosForUserOnPhoneUpdate = async (
  idUsuario,
  { source = "phone_update" } = {},
) => {
  const userId = Number(idUsuario);
  if (!Number.isFinite(userId) || userId <= 0) {
    return { ok: false, skipped: true, reason: "invalid_user" };
  }
  if (!shouldStartWhatsapp || !WHATSAPP_AUTO_RECORDATORIOS_ENABLED) {
    return { ok: false, skipped: true, reason: "disabled" };
  }
  if (!isWhatsappReady()) {
    return { ok: false, skipped: true, reason: "not_ready" };
  }

  const { dateStr, hour, weekday } = getCaracasClock();
  const targetHour = getWhatsappAutoRecordatoriosHour({ weekday });
  ensureDailyRecordatoriosState(dateStr);
  if (hour < targetHour) {
    return { ok: false, skipped: true, reason: "before_schedule", targetHour, weekday };
  }
  if (!isWhatsappReady()) {
    await ensureWhatsappClientStarted({ reason: source });
    if (!isWhatsappReady()) {
      return { ok: false, skipped: true, reason: "not_ready" };
    }
  }

  const userItems = await buildWhatsappRecordatorioItems({ targetUserIds: [userId] });
  let userCutoffTodayItems = [];
  try {
    userCutoffTodayItems = await buildWhatsappRecordatorioItems({
      targetUserIds: [userId],
      mode: "cutoff_today",
    });
  } catch (err) {
    if (!isMissingColumnError(err, "recordatorio_corte_enviado")) throw err;
  }
  const candidateItems = [...userItems, ...userCutoffTodayItems];
  if (!candidateItems.length) {
    return { ok: false, skipped: true, reason: "no_pending_for_user" };
  }
  if (countWhatsappSendableRecordatorios(candidateItems) === 0) {
    return { ok: false, skipped: true, reason: "no_sendable_pending_for_user" };
  }

  try {
    const result = await sendWhatsappRecordatorios({
      source,
      itemsOverride: userItems,
      targetUserIds: [userId],
    });

    console.log(
      `[WhatsApp] Recordatorios ${source}: usuario=${userId} total=${result.total} sent=${result.sent} failed=${result.failed}`,
    );

    return {
      ok: true,
      skipped: false,
      userId,
      ...result,
    };
  } finally {
    await shutdownWhatsappClient({ reason: source });
  }
};

const attemptWhatsappRecordatoriosForUsersOnPhoneUpdate = async (
  userIds,
  { source = "phone_update_batch" } = {},
) => {
  const targetUserIds = uniqPositiveIds(userIds || []);
  if (!targetUserIds.length) {
    return { ok: false, skipped: true, reason: "invalid_users" };
  }
  if (!shouldStartWhatsapp || !WHATSAPP_AUTO_RECORDATORIOS_ENABLED) {
    return { ok: false, skipped: true, reason: "disabled" };
  }
  if (!isWhatsappReady()) {
    return { ok: false, skipped: true, reason: "not_ready" };
  }

  const { dateStr, hour, weekday } = getCaracasClock();
  const targetHour = getWhatsappAutoRecordatoriosHour({ weekday });
  ensureDailyRecordatoriosState(dateStr);
  if (hour < targetHour) {
    return { ok: false, skipped: true, reason: "before_schedule", targetHour, weekday };
  }
  if (!isWhatsappReady()) {
    await ensureWhatsappClientStarted({ reason: source });
    if (!isWhatsappReady()) {
      return { ok: false, skipped: true, reason: "not_ready" };
    }
  }

  const items = await buildWhatsappRecordatorioItems({ targetUserIds });
  let cutoffTodayItems = [];
  try {
    cutoffTodayItems = await buildWhatsappRecordatorioItems({
      targetUserIds,
      mode: "cutoff_today",
    });
  } catch (err) {
    if (!isMissingColumnError(err, "recordatorio_corte_enviado")) throw err;
  }
  const candidateItems = [...items, ...cutoffTodayItems];
  if (!candidateItems.length) {
    return { ok: false, skipped: true, reason: "no_pending_for_users" };
  }
  if (countWhatsappSendableRecordatorios(candidateItems) === 0) {
    return { ok: false, skipped: true, reason: "no_sendable_pending_for_users" };
  }

  try {
    const result = await sendWhatsappRecordatorios({
      source,
      itemsOverride: items,
      targetUserIds,
    });

    console.log(
      `[WhatsApp] Recordatorios ${source}: usuarios=${targetUserIds.length} total=${result.total} sent=${result.sent} failed=${result.failed}`,
    );

    return {
      ok: true,
      skipped: false,
      users: targetUserIds,
      ...result,
    };
  } finally {
    await shutdownWhatsappClient({ reason: source });
  }
};

const sendWhatsappRecordatorios = async ({
  source = "manual",
  itemsOverride = null,
  targetUserIds = null,
  includeCutoffTodayFollowUp = true,
} = {}) => {
  if (recordatoriosSendInProgress) {
    if (isRecordatoriosSendLockStale()) {
      const staleInfo = getRecordatoriosSendLockState();
      console.warn("[WhatsApp] Liberando lock stale de recordatorios", staleInfo);
      recordatoriosSendInProgress = false;
      resetRecordatoriosSendLockMeta();
    }
  }
  if (recordatoriosSendInProgress) {
    const lockInfo = getRecordatoriosSendLockState();
    const lockErr = new Error("Ya hay un envío de recordatorios en progreso");
    lockErr.code = "RECORDATORIOS_SEND_IN_PROGRESS";
    lockErr.lock = lockInfo;
    throw lockErr;
  }

  recordatoriosSendInProgress = true;
  const startedAtIso = new Date().toISOString();
  recordatoriosSendLockMeta = {
    source: String(source || "manual"),
    startedAt: startedAtIso,
    heartbeatAt: startedAtIso,
    mode: "pending",
    total: 0,
    processed: 0,
    sent: 0,
    failed: 0,
    skippedNoPhone: 0,
    skippedInvalidPhone: 0,
    lastCliente: null,
  };
  const syncLockProgress = (progress = {}) => {
    const sourceValue = String(progress?.source || source || "manual");
    const modeValue = normalizeWhatsappRecordatorioMode(progress?.mode || "pending");
    const totalValue = Number.isFinite(Number(progress?.total)) ? Number(progress.total) : 0;
    const processedValue = Number.isFinite(Number(progress?.processed)) ? Number(progress.processed) : 0;
    const sentValue = Number.isFinite(Number(progress?.sent)) ? Number(progress.sent) : 0;
    const failedValue = Number.isFinite(Number(progress?.failed)) ? Number(progress.failed) : 0;
    const skippedNoPhoneValue = Number.isFinite(Number(progress?.skippedNoPhone))
      ? Number(progress.skippedNoPhone)
      : 0;
    const skippedInvalidPhoneValue = Number.isFinite(Number(progress?.skippedInvalidPhone))
      ? Number(progress.skippedInvalidPhone)
      : 0;
    const itemCliente = String(progress?.item?.cliente || "").trim();
    touchRecordatoriosSendLock({
      source: sourceValue,
      mode: modeValue,
      total: totalValue,
      processed: processedValue,
      sent: sentValue,
      failed: failedValue,
      skippedNoPhone: skippedNoPhoneValue,
      skippedInvalidPhone: skippedInvalidPhoneValue,
      lastCliente: itemCliente || recordatoriosSendLockMeta.lastCliente || null,
    });
  };
  touchRecordatoriosSendLock({
    source: String(source || "manual"),
    mode: "pending",
    total: 0,
    processed: 0,
    sent: 0,
    failed: 0,
    skippedNoPhone: 0,
    skippedInvalidPhone: 0,
    lastCliente: null,
  });
  try {
    const primaryItems = Array.isArray(itemsOverride)
      ? itemsOverride
      : await buildWhatsappRecordatorioItems({ targetUserIds });
    touchRecordatoriosSendLock({
      mode: "pending",
      total: Array.isArray(primaryItems) ? primaryItems.length : 0,
      processed: 0,
      sent: 0,
      failed: 0,
      skippedNoPhone: 0,
      skippedInvalidPhone: 0,
    });
    const primaryResult = await sendWhatsappRecordatorioBatch({
      source,
      items: primaryItems,
      mode: "pending",
      onProgress: syncLockProgress,
    });

    let cutoffTodayItems = [];
    let cutoffTodayResult = buildEmptyWhatsappRecordatorioSendResult(source, "cutoff_today");
    if (includeCutoffTodayFollowUp) {
      try {
        cutoffTodayItems = await buildWhatsappRecordatorioItems({
          targetUserIds,
          mode: "cutoff_today",
        });
        touchRecordatoriosSendLock({
          mode: "cutoff_today",
          total: Array.isArray(cutoffTodayItems) ? cutoffTodayItems.length : 0,
          processed: 0,
          sent: 0,
          failed: 0,
          skippedNoPhone: 0,
          skippedInvalidPhone: 0,
        });
        cutoffTodayResult = await sendWhatsappRecordatorioBatch({
          source,
          items: cutoffTodayItems,
          mode: "cutoff_today",
          onProgress: syncLockProgress,
        });
      } catch (err) {
        if (!isMissingColumnError(err, "recordatorio_corte_enviado")) {
          throw err;
        }
        cutoffTodayResult = {
          ...buildEmptyWhatsappRecordatorioSendResult(source, "cutoff_today"),
          error:
            "Falta la columna ventas.recordatorio_corte_enviado para habilitar el segundo recordatorio del día de corte.",
        };
        console.warn(
          "[WhatsApp] Segundo recordatorio del día de corte deshabilitado: falta columna ventas.recordatorio_corte_enviado.",
        );
      }
    }

    const cutoffTodayPlatform12Items = includeCutoffTodayFollowUp
      ? buildWhatsappCutoffPlatform12Items(cutoffTodayItems)
      : [];
    touchRecordatoriosSendLock({
      mode: "cutoff_today_platform_12",
      total: Array.isArray(cutoffTodayPlatform12Items) ? cutoffTodayPlatform12Items.length : 0,
      processed: 0,
      sent: 0,
      failed: 0,
      skippedNoPhone: 0,
      skippedInvalidPhone: 0,
    });
    const cutoffTodayPlatform12Result = await sendWhatsappRecordatorioBatch({
      source,
      items: cutoffTodayPlatform12Items,
      mode: "cutoff_today_platform_12",
      onProgress: syncLockProgress,
    });

    return {
      source,
      total:
        Number(primaryResult.total || 0) +
        Number(cutoffTodayResult.total || 0) +
        Number(cutoffTodayPlatform12Result.total || 0),
      sent:
        Number(primaryResult.sent || 0) +
        Number(cutoffTodayResult.sent || 0) +
        Number(cutoffTodayPlatform12Result.sent || 0),
      failed:
        Number(primaryResult.failed || 0) +
        Number(cutoffTodayResult.failed || 0) +
        Number(cutoffTodayPlatform12Result.failed || 0),
      skippedNoPhone:
        Number(primaryResult.skippedNoPhone || 0) +
        Number(cutoffTodayResult.skippedNoPhone || 0) +
        Number(cutoffTodayPlatform12Result.skippedNoPhone || 0),
      skippedInvalidPhone:
        Number(primaryResult.skippedInvalidPhone || 0) +
        Number(cutoffTodayResult.skippedInvalidPhone || 0) +
        Number(cutoffTodayPlatform12Result.skippedInvalidPhone || 0),
      updatedVentas: Number(primaryResult.updatedVentas || 0),
      updatedVentasCorteHoy: Number(cutoffTodayResult.updatedVentas || 0),
      items: [
        ...(primaryResult.items || []),
        ...(cutoffTodayResult.items || []),
        ...(cutoffTodayPlatform12Result.items || []),
      ],
      primary: primaryResult,
      cutoffToday: cutoffTodayResult,
      cutoffTodayPlatform12: cutoffTodayPlatform12Result,
    };
  } finally {
    recordatoriosSendInProgress = false;
    resetRecordatoriosSendLockMeta();
  }
};

const runAutoWhatsappRecordatoriosIfNeeded = async () => {
  if (!shouldStartWhatsapp || !WHATSAPP_AUTO_RECORDATORIOS_ENABLED) return;
  if (autoRecordatoriosRunInProgress) return;
  const { dateStr, hour, weekday } = getCaracasClock();
  const targetHour = getWhatsappAutoRecordatoriosHour({ weekday });
  ensureDailyRecordatoriosState(dateStr);
  if (hour < targetHour) return;
  if (lastAutoRecordatoriosRunDate === dateStr) return;
  const attemptedAt = new Date().toISOString();
  autoRecordatoriosRunInProgress = true;
  autoRecordatoriosRetryPending = false;
  lastAutoRecordatoriosState = {
    date: dateStr,
    status: "running",
    attemptedAt,
    completedAt: null,
    total: 0,
    sent: 0,
    failed: 0,
    skippedNoPhone: 0,
    skippedInvalidPhone: 0,
    updatedVentas: 0,
    sameDayCutoffTotal: 0,
    sameDayCutoffSent: 0,
    sameDayCutoffFailed: 0,
    sameDayPlatform12Total: 0,
    sameDayPlatform12Sent: 0,
    sameDayPlatform12Failed: 0,
    updatedVentasCorteHoy: 0,
    recordatoriosEnviados,
    error: null,
  };

  try {
    if (!isWhatsappReady()) {
      await ensureWhatsappClientReady({ reason: "auto_schedule" });
    }

    const pendingItems = await buildWhatsappRecordatorioItems();
    lastAutoRecordatoriosState = {
      ...lastAutoRecordatoriosState,
      total: pendingItems.length,
    };

    const result = await sendWhatsappRecordatorios({
      source: "auto",
      itemsOverride: pendingItems,
      targetUserIds: null,
    });
    const sentAll = didSendAllRecordatorios(result.primary || result);
    recordatoriosEnviados = sentAll;
    lastAutoRecordatoriosRunDate = dateStr;
    autoRecordatoriosRetryPending = false;
    lastAutoRecordatoriosState = {
      date: dateStr,
      status: result.total > 0 ? "completed" : "no_pending",
      attemptedAt,
      completedAt: new Date().toISOString(),
      total: Number(result.total || 0),
      sent: Number(result.sent || 0),
      failed: Number(result.failed || 0),
      skippedNoPhone: Number(result.skippedNoPhone || 0),
      skippedInvalidPhone: Number(result.skippedInvalidPhone || 0),
      updatedVentas: Number(result.updatedVentas || 0),
      sameDayCutoffTotal: Number(result?.cutoffToday?.total || 0),
      sameDayCutoffSent: Number(result?.cutoffToday?.sent || 0),
      sameDayCutoffFailed: Number(result?.cutoffToday?.failed || 0),
      sameDayPlatform12Total: Number(result?.cutoffTodayPlatform12?.total || 0),
      sameDayPlatform12Sent: Number(result?.cutoffTodayPlatform12?.sent || 0),
      sameDayPlatform12Failed: Number(result?.cutoffTodayPlatform12?.failed || 0),
      updatedVentasCorteHoy: Number(result.updatedVentasCorteHoy || 0),
      recordatoriosEnviados,
      error: null,
    };
    console.log(
      `[WhatsApp] Recordatorios auto ${dateStr} ${weekday}: total=${result.total}, sent=${result.sent}, failed=${result.failed}, skipped_no_phone=${result.skippedNoPhone}, skipped_invalid_phone=${result.skippedInvalidPhone}, corte_hoy_sent=${result?.cutoffToday?.sent || 0}, corte_hoy_plat12_sent=${result?.cutoffTodayPlatform12?.sent || 0}, recordatorios_enviados=${recordatoriosEnviados}`,
    );
  } catch (err) {
    if (err?.code === "RECORDATORIOS_SEND_IN_PROGRESS") {
      console.warn(
        `[WhatsApp] Recordatorios auto ${dateStr}: ya hay un envío en progreso, se espera próximo ciclo.`,
      );
      return;
    }
    if (err?.code === "WHATSAPP_NOT_READY" || err?.code === "WHATSAPP_DISCONNECTED") {
      autoRecordatoriosRetryPending = true;
      lastAutoRecordatoriosState = {
        date: dateStr,
        status: "not_ready",
        attemptedAt,
        completedAt: new Date().toISOString(),
        total: 0,
        sent: 0,
        failed: 0,
        skippedNoPhone: 0,
        skippedInvalidPhone: 0,
        updatedVentas: 0,
        sameDayCutoffTotal: 0,
        sameDayCutoffSent: 0,
        sameDayCutoffFailed: 0,
        sameDayPlatform12Total: 0,
        sameDayPlatform12Sent: 0,
        sameDayPlatform12Failed: 0,
        updatedVentasCorteHoy: 0,
        recordatoriosEnviados,
        error: err?.message || "WhatsApp no listo",
      };
      console.warn(
        `[WhatsApp] Recordatorios auto ${dateStr}: cliente no listo o desconectado, se reintentará en la próxima verificación hasta conectar sesión.`,
      );
      return;
    }
    autoRecordatoriosRetryPending = false;
    lastAutoRecordatoriosRunDate = dateStr;
    lastAutoRecordatoriosState = {
      date: dateStr,
      status: "error",
      attemptedAt,
      completedAt: new Date().toISOString(),
      total: 0,
      sent: 0,
      failed: 0,
      skippedNoPhone: 0,
      skippedInvalidPhone: 0,
      updatedVentas: 0,
      sameDayCutoffTotal: 0,
      sameDayCutoffSent: 0,
      sameDayCutoffFailed: 0,
      sameDayPlatform12Total: 0,
      sameDayPlatform12Sent: 0,
      sameDayPlatform12Failed: 0,
      updatedVentasCorteHoy: 0,
      recordatoriosEnviados,
      error: err?.message || "Error desconocido",
    };
    console.error("[WhatsApp] Recordatorios auto error", err);
  } finally {
    autoRecordatoriosRunInProgress = false;
    await shutdownWhatsappClient({ reason: "auto_run_completed" });
  }
};

const triggerAutoWhatsappRecordatoriosCheck = (label = "run") => {
  runAutoWhatsappRecordatoriosIfNeeded().catch((err) => {
    console.error(`[WhatsApp] Scheduler de recordatorios ${label} error`, err);
  });
};

const startAutoWhatsappRecordatoriosScheduler = () => {
  if (!shouldStartWhatsapp || !WHATSAPP_AUTO_RECORDATORIOS_ENABLED) return;
  if (!autoRecordatoriosSchedulerStarted) {
    autoRecordatoriosSchedulerStarted = true;
    console.log(
      "[WhatsApp] Recordatorios automáticos activos; el cliente iniciará a demanda lunes a viernes 10:00 y sábado/domingo 11:00 America/Caracas.",
    );
    autoRecordatoriosIntervalId = setInterval(() => {
      triggerAutoWhatsappRecordatoriosCheck("tick");
    }, 60 * 1000);
  }
  triggerAutoWhatsappRecordatoriosCheck("init");
};

if (shouldStartWhatsapp) {
  syncWhatsappPersistentWorkerState("bootstrap");
  if (!whatsappPersistentWorkerSyncIntervalId) {
    whatsappPersistentWorkerSyncIntervalId = setInterval(() => {
      syncWhatsappPersistentWorkerState("interval");
    }, WHATSAPP_PERSISTENT_SYNC_INTERVAL_MS);
  }
}

if (shouldStartWhatsapp && WHATSAPP_AUTO_RECORDATORIOS_ENABLED) {
  startAutoWhatsappRecordatoriosScheduler();
  onWhatsappReady(() => {
    triggerAutoWhatsappRecordatoriosCheck("ready");
  });
  onWhatsappDisconnected(() => {
    console.warn(
      "[WhatsApp] Scheduler de recordatorios en espera hasta que se restablezca la sesión.",
    );
  });
}

if (NUEVO_SERVICIO_NOTIF_QUEUE_ENABLED) {
  console.log(
    `[Notificaciones] Worker nuevo_servicio activo (cada ${Math.round(
      NUEVO_SERVICIO_NOTIF_QUEUE_INTERVAL_MS / 1000,
    )}s).`,
  );
  setInterval(() => {
    processNuevoServicioNotificationQueue().catch((err) => {
      console.error("[Notificaciones] Worker nuevo_servicio error", err);
    });
  }, NUEVO_SERVICIO_NOTIF_QUEUE_INTERVAL_MS);
  processNuevoServicioNotificationQueue().catch((err) => {
    console.error("[Notificaciones] Worker nuevo_servicio init error", err);
  });
}

if (WHATSAPP_PEDIDO_PENDIENTE_WATCHER_ENABLED && shouldStartWhatsapp) {
  console.log(
    `[WhatsApp] Worker pedidos Spotify pendientes activo (cada ${Math.round(
      WHATSAPP_PEDIDO_PENDIENTE_WATCHER_INTERVAL_MS / 1000,
    )}s).`,
  );
  onWhatsappReady(() => {
    processPendingSpotifyAdminAlerts().catch((err) => {
      console.error("[WhatsApp] Worker pedidos Spotify pendientes ready error", err);
    });
  });
  setInterval(() => {
    processPendingSpotifyAdminAlerts().catch((err) => {
      console.error("[WhatsApp] Worker pedidos Spotify pendientes error", err);
    });
  }, WHATSAPP_PEDIDO_PENDIENTE_WATCHER_INTERVAL_MS);
  processPendingSpotifyAdminAlerts().catch((err) => {
    console.error("[WhatsApp] Worker pedidos Spotify pendientes init error", err);
  });
}

if (WHATSAPP_MANUAL_VERIFICATION_WATCHER_ENABLED) {
  console.log(
    `[Notificaciones] Worker verificación manual/auto activo (cada ${Math.round(
      WHATSAPP_MANUAL_VERIFICATION_WATCHER_INTERVAL_MS / 1000,
    )}s, ventana ${Math.round(WHATSAPP_MANUAL_VERIFICATION_WINDOW_MS / 1000)}s).`,
  );
  setInterval(() => {
    processPendingManualVerificationAlerts().catch((err) => {
      console.error("[manual_verification_watcher] error", err);
    });
  }, WHATSAPP_MANUAL_VERIFICATION_WATCHER_INTERVAL_MS);
  processPendingManualVerificationAlerts().catch((err) => {
    console.error("[manual_verification_watcher] init error", err);
  });
}

if (WHATSAPP_REPORTES_WATCHER_ENABLED && shouldStartWhatsapp) {
  console.log(
    `[reportes:whatsapp] Worker de pendientes (enviado_whatsapp=false) activo (cada ${Math.round(
      WHATSAPP_REPORTES_WATCHER_INTERVAL_MS / 1000,
    )}s, batch ${WHATSAPP_REPORTES_WATCHER_BATCH}).`,
  );
  setInterval(() => {
    processPendingReportesWhatsappAlerts().catch((err) => {
      console.error("[reportes:whatsapp] Worker error", err);
    });
  }, WHATSAPP_REPORTES_WATCHER_INTERVAL_MS);
  processPendingReportesWhatsappAlerts().catch((err) => {
    console.error("[reportes:whatsapp] Worker init error", err);
  });
} else {
  console.warn(
    `[reportes:whatsapp] Worker desactivado al iniciar. watcher=${WHATSAPP_REPORTES_WATCHER_ENABLED} shouldStartWhatsapp=${shouldStartWhatsapp} ENABLE_WHATSAPP=${String(process.env.ENABLE_WHATSAPP || "").trim()}`,
  );
}

if (WEB_PUSH_QUEUE_WORKER_ENABLED) {
  if (WEB_PUSH_IS_CONFIGURED) {
    console.log(
      `[WebPush] Worker activo (cada ${Math.round(WEB_PUSH_QUEUE_INTERVAL_MS / 1000)}s).`,
    );
    setInterval(() => {
      processWebPushDeliveryQueue().catch((err) => {
        console.error("[WebPush] Worker error", err);
      });
    }, WEB_PUSH_QUEUE_INTERVAL_MS);
    processWebPushDeliveryQueue().catch((err) => {
      console.error("[WebPush] Worker init error", err);
    });
  } else {
    console.warn("[WebPush] Worker desactivado: faltan WEB_PUSH_VAPID_PUBLIC_KEY/PRIVATE_KEY.");
  }
}

if (AUTO_GIFTCARD_PENDING_DELIVERY_ENABLED) {
  console.log(
    `[GiftCards] Auto-entrega de ventas pendientes por inserciones activa (cada ${Math.round(
      AUTO_GIFTCARD_PENDING_DELIVERY_INTERVAL_MS / 1000,
    )}s).`,
  );
  setInterval(() => {
    processAutoGiftCardPendingDeliveries().catch((err) => {
      console.error("[GiftCards] Worker auto-entrega error", err);
    });
  }, AUTO_GIFTCARD_PENDING_DELIVERY_INTERVAL_MS);
  processAutoGiftCardPendingDeliveries().catch((err) => {
    console.error("[GiftCards] Worker auto-entrega init error", err);
  });
}

if (AUTO_PENDING_STOCK_DELIVERY_ENABLED) {
  console.log(
    `[Ventas] Auto-entrega por stock disponible activa (cada ${Math.round(
      AUTO_PENDING_STOCK_DELIVERY_INTERVAL_MS / 1000,
    )}s).`,
  );
  setInterval(() => {
    processAutoPendingStockDeliveries().catch((err) => {
      console.error("[ventas:auto-pending-stock] worker error", err);
    });
  }, AUTO_PENDING_STOCK_DELIVERY_INTERVAL_MS);
}

const normalizeMontoBs = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const raw = String(value).trim();
  if (!raw) return null;
  let clean = raw;
  if (raw.includes(".") && raw.includes(",")) {
    clean = raw.replace(/\./g, "").replace(",", ".");
  } else if (raw.includes(",")) {
    clean = raw.replace(",", ".");
  }
  const num = Number(clean);
  return Number.isFinite(num) ? Math.round(num * 100) / 100 : null;
};

const normalizeReferenceDigits = (value) => String(value || "").replace(/\D/g, "");
const getReferenceMatchScore = (targetRef, candidateRef) => {
  const target = normalizeReferenceDigits(targetRef);
  const candidate = normalizeReferenceDigits(candidateRef);
  if (target.length < 4 || candidate.length < 4) return 0;
  if (candidate === target) return 5;
  if (target.length > 4 || candidate.length > 4) {
    if (candidate.endsWith(target) || target.endsWith(candidate)) return 4;
  }
  if (candidate.slice(-4) === target.slice(-4)) return 1;
  return 0;
};
const resolveBestReferenceCandidate = (targetRef, candidateRefs = []) => {
  const refs = Array.from(
    new Set(
      (Array.isArray(candidateRefs) ? candidateRefs : [])
        .map((value) => normalizeReferenceDigits(value))
        .filter((value) => value.length >= 4),
    ),
  );
  const target = normalizeReferenceDigits(targetRef);
  if (target.length < 4 || !refs.length) {
    return { score: 0, ref: null };
  }
  let bestScore = 0;
  let bestRef = null;
  refs.forEach((ref) => {
    const score = getReferenceMatchScore(target, ref);
    if (score > bestScore) {
      bestScore = score;
      bestRef = ref;
      return;
    }
    if (score > 0 && score === bestScore && bestRef && ref.length > bestRef.length) {
      bestRef = ref;
    }
  });
  return { score: bestScore, ref: bestRef };
};
const stripQuoteChars = (value) =>
  String(value || "").replace(/["'`“”‘’«»]/g, " ");
const normalizeNotificationTextForParsing = (value) =>
  stripQuoteChars(value).replace(/\s+/g, " ").trim();

const getRefExtractionSources = (text) => {
  const base = String(text || "").trim();
  if (!base) return [];
  const sources = [];
  const pushSource = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return;
    if (!sources.includes(raw)) sources.push(raw);
    const normalized = normalizeNotificationTextForParsing(raw);
    if (normalized && !sources.includes(normalized)) sources.push(normalized);
  };

  try {
    const parsed = JSON.parse(base);
    if (parsed && typeof parsed === "object") {
      // Si el webhook llega como JSON, extraemos solo campos útiles del mensaje para
      // evitar tomar timestamps u otros números técnicos como referencia.
      const nested = [
        parsed.texto,
        parsed.text,
        parsed.mensaje,
        parsed.message,
        parsed.referencia,
        parsed.ref,
        parsed.reference,
        parsed.payload?.texto,
        parsed.payload?.text,
        parsed.payload?.mensaje,
        parsed.payload?.message,
        parsed.payload?.referencia,
        parsed.payload?.ref,
        parsed.payload?.reference,
      ];
      nested.forEach((value) => pushSource(value));
      if (sources.length) return sources;
    }
  } catch (_err) {
    // Ignorar si no es JSON válido; usamos el texto crudo.
  }

  pushSource(base);
  return sources;
};

const extractRefFieldCandidates = (text) => {
  const base = String(text || "").trim();
  if (!base) return [];

  try {
    const parsed = JSON.parse(base);
    if (!parsed || typeof parsed !== "object") return [];
    const fieldCandidates = [
      parsed.referencia,
      parsed.ref,
      parsed.reference,
      parsed.nro_referencia,
      parsed.numero_referencia,
      parsed.payload?.referencia,
      parsed.payload?.ref,
      parsed.payload?.reference,
      parsed.payload?.nro_referencia,
      parsed.payload?.numero_referencia,
    ];
    return Array.from(
      new Set(
        fieldCandidates
          .map((value) => normalizeReferenceDigits(value))
          .filter((digits) => digits.length >= 6),
      ),
    );
  } catch (_err) {
    return [];
  }
};

const extractRefKeywordCandidates = (text) => {
  const sources = getRefExtractionSources(text);
  const patterns = [
    /\bref(?:erencia)?\.?\s*[:#-]?\s*["'`“”‘’«»]?\s*(\d{4,20})/gi,
    /(?:n(?:u|ú)mero|num(?:ero)?|n[º°#])\s*(?:de\s*)?(?:operaci(?:o|ó)n|referencia)\s*[:#-]?\s*["'`“”‘’«»]?\s*(\d{6,20})/gi,
    /(?:operaci(?:o|ó)n|referencia)\s*(?:n(?:u|ú)mero|num(?:ero)?|n[º°#])?\s*[:#-]?\s*["'`“”‘’«»]?\s*(\d{6,20})/gi,
    /(?:trx|transacci(?:o|ó)n|operaci(?:o|ó)n)\s*[:#-]?\s*["'`“”‘’«»]?\s*(\d{6,20})/gi,
  ];
  const results = [];
  sources.forEach((source) => {
    patterns.forEach((pattern) => {
      pattern.lastIndex = 0;
      let match;
      while ((match = pattern.exec(source)) !== null) {
        const digits = normalizeReferenceDigits(match?.[1] || "");
        if (digits.length >= 6) results.push(digits);
      }
    });
  });
  return Array.from(new Set(results));
};

const extractRefCandidates = (text) => {
  const sources = getRefExtractionSources(text);
  const fieldCandidates = extractRefFieldCandidates(text);
  const keywordCandidates = extractRefKeywordCandidates(text);
  const genericCandidates = [];
  sources.forEach((source) => {
    const matches = source.match(/\d{4,}/g) || [];
    matches.forEach((match) => {
      const digits = normalizeReferenceDigits(match);
      if (digits.length >= 4) genericCandidates.push(digits);
    });
  });
  return Array.from(
    new Set([...fieldCandidates, ...keywordCandidates, ...genericCandidates]),
  );
};

const pickPrimaryReferenceCandidate = (text) => {
  const keywordCandidates = extractRefKeywordCandidates(text);
  if (keywordCandidates.length) return keywordCandidates[0];
  const fieldCandidates = extractRefFieldCandidates(text);
  if (fieldCandidates.length) return fieldCandidates[0];
  return null;
};

const computeOrderMontoBs = async (orden = {}) => {
  const direct = normalizeMontoBs(orden?.monto_bs);
  if (Number.isFinite(direct)) return direct;
  const total = Number(orden?.total);
  if (!Number.isFinite(total)) return null;
  const tasa = await getStrictStoredTasaActual();
  return Math.round(total * tasa * 100) / 100;
};

const matchPagoMovilToOrder = async (
  orden = {},
  { includeCreditedForUserId = null, requireCredited = false } = {},
) => {
  const metodoId = Number(orden?.id_metodo_de_pago || 0);
  if (metodoId !== 1) {
    return { matched: false, reason: "metodo_no_pagomovil" };
  }

  const refDigits = normalizeReferenceDigits(orden?.referencia);
  if (refDigits.length < 4) {
    return { matched: false, reason: "referencia_invalida" };
  }
  const montoOrdenBs = await computeOrderMontoBs(orden);
  if (!Number.isFinite(montoOrdenBs)) {
    return { matched: false, reason: "monto_orden_invalido" };
  }

  const creditedUserId = toPositiveInt(includeCreditedForUserId);
  let pagosQuery = supabaseAdmin
    .from("pagomoviles")
    .select("id, referencia, texto, monto_bs, saldo_acreditado, saldo_acreditado_a")
    .order("id", { ascending: false })
    .limit(400);
  if (requireCredited === true) {
    pagosQuery = pagosQuery.eq("saldo_acreditado", true);
    if (creditedUserId) {
      pagosQuery = pagosQuery.eq("saldo_acreditado_a", creditedUserId);
    }
  } else if (creditedUserId) {
    pagosQuery = pagosQuery.or(
      `saldo_acreditado.is.null,saldo_acreditado.eq.false,and(saldo_acreditado.eq.true,saldo_acreditado_a.eq.${creditedUserId})`,
    );
  } else {
    pagosQuery = pagosQuery.or("saldo_acreditado.is.null,saldo_acreditado.eq.false");
  }

  const { data: pagosRows, error: pagosErr } = await pagosQuery;
  if (pagosErr) throw pagosErr;

  const pagos = Array.isArray(pagosRows) ? pagosRows : [];
  const matchedCandidates = pagos
    .map((pago) => {
      const refCandidates = [
        ...extractRefCandidates(pago?.texto || ""),
        ...extractRefCandidates(pago?.referencia || ""),
      ];
      const refResolution = resolveBestReferenceCandidate(refDigits, refCandidates);
      if (refResolution.score <= 0) return null;
      const pagoMonto = normalizeMontoBs(pago?.monto_bs);
      if (!Number.isFinite(pagoMonto)) return null;
      if (Math.abs(pagoMonto - montoOrdenBs) > 0.01) return null;
      const pagoAcreditado = isTrue(pago?.saldo_acreditado);
      const pagoAcreditadoA = toPositiveInt(pago?.saldo_acreditado_a);
      if (requireCredited === true && !pagoAcreditado) return null;
      if (requireCredited === true && creditedUserId && pagoAcreditadoA !== creditedUserId) {
        return null;
      }
      return {
        pago,
        refResolution,
        pagoId: Number(pago?.id) || 0,
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (b.refResolution.score !== a.refResolution.score) {
        return b.refResolution.score - a.refResolution.score;
      }
      return b.pagoId - a.pagoId;
    });
  const bestCandidate = matchedCandidates[0] || null;
  const matchedPago = bestCandidate?.pago || null;

  if (!matchedPago?.id) {
    return {
      matched: false,
      reason: "no_pago_match",
      monto_bs_orden: montoOrdenBs,
    };
  }

  const referenciaMatch = bestCandidate?.refResolution?.ref || null;

  return {
    matched: true,
    pago_id: Number(matchedPago.id),
    referencia_match: referenciaMatch,
    monto_bs_orden: montoOrdenBs,
    monto_bs_pago: normalizeMontoBs(matchedPago?.monto_bs),
    saldo_acreditado: matchedPago?.saldo_acreditado === true ||
      matchedPago?.saldo_acreditado === "true" ||
      matchedPago?.saldo_acreditado === "1" ||
      matchedPago?.saldo_acreditado === 1 ||
      matchedPago?.saldo_acreditado === "t",
    saldo_acreditado_a: Number(matchedPago?.saldo_acreditado_a || 0) || null,
  };
};

const markPagoMovilCreditedForOrder = async ({ orden = {}, idUsuario = null } = {}) => {
  const targetUserId = Number(idUsuario || orden?.id_usuario || 0);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
    return { matched: false, reason: "id_usuario_invalido" };
  }

  const match = await matchPagoMovilToOrder(orden, {
    includeCreditedForUserId: targetUserId,
  });
  if (!match?.matched) return match;

  const updates = {
    saldo_acreditado_a: targetUserId,
    saldo_acreditado: true,
  };
  if (match.referencia_match) {
    updates.referencia = match.referencia_match;
  }

  const { error: updErr } = await supabaseAdmin
    .from("pagomoviles")
    .update(updates)
    .eq("id", match.pago_id);
  if (updErr) throw updErr;

  return {
    ...match,
    saldo_acreditado: true,
    saldo_acreditado_a: targetUserId,
  };
};

const markPagoMovilRowAsCredited = async ({
  pagoId = null,
  idUsuario = null,
  referenciaMatch = null,
} = {}) => {
  const pagoIdNum = toPositiveInt(pagoId);
  const userId = toPositiveInt(idUsuario);
  if (!pagoIdNum || !userId) {
    return { updated: false, reason: "invalid_pago_or_user" };
  }

  const updates = {
    saldo_acreditado_a: userId,
    saldo_acreditado: true,
  };
  const refDigits = normalizeReferenceDigits(referenciaMatch);
  if (refDigits.length >= 4) {
    updates.referencia = refDigits;
  }

  const { error: updErr } = await supabaseAdmin
    .from("pagomoviles")
    .update(updates)
    .eq("id", pagoIdNum);
  if (updErr) throw updErr;

  return { updated: true, pago_id: pagoIdNum, saldo_acreditado_a: userId };
};

const creditSaldoUsdToUser = async ({ idUsuario = null, montoUsd = 0 } = {}) => {
  const targetUserId = toPositiveInt(idUsuario);
  const amount = Math.round((Number(montoUsd) || 0) * 100) / 100;
  if (!targetUserId || !(amount > 0)) {
    return { acreditado: false, monto: 0, saldoNuevo: null };
  }

  const { data: userRow, error: userErr } = await supabaseAdmin
    .from("usuarios")
    .select("saldo")
    .eq("id_usuario", targetUserId)
    .maybeSingle();
  if (userErr) throw userErr;

  const saldoActual = Number(userRow?.saldo);
  const saldoBase = Number.isFinite(saldoActual) ? saldoActual : 0;
  const saldoNuevo = Math.round((saldoBase + amount) * 100) / 100;
  const { error: updSaldoErr } = await supabaseAdmin
    .from("usuarios")
    .update({ saldo: saldoNuevo })
    .eq("id_usuario", targetUserId);
  if (updSaldoErr) throw updSaldoErr;

  return { acreditado: true, monto: amount, saldoNuevo };
};

const autoProcessMatchedOrder = async (
  match = {},
  { allowedMetodoPagoIds = [1], syncPagoMovil = true } = {},
) => {
  const idOrden = toPositiveInt(match?.id_orden);
  if (!idOrden) {
    return { processed: false, reason: "id_orden_invalido" };
  }
  const allowedMetodoIds = Array.from(
    new Set(
      (Array.isArray(allowedMetodoPagoIds) ? allowedMetodoPagoIds : [])
        .map((value) => toPositiveInt(value))
        .filter(Boolean),
    ),
  );

  const orderProcessLock = await acquireSharedOrderProcessLock(idOrden, {
    source: "auto_process_matched_order",
  });
  if (!orderProcessLock?.acquired) {
    return {
      processed: false,
      reason: "orden_en_proceso",
      id_orden: idOrden,
      lock: orderProcessLock?.lock || null,
    };
  }

  try {
    const { data: orden, error: ordErr } = await supabaseAdmin
      .from("ordenes")
      .select(
        "id_orden, id_usuario, id_carrito, referencia, comprobante, id_metodo_de_pago, total, total_bruto_usd, usa_saldo, saldo_aplicado_usd, tasa_bs, monto_bs, pago_verificado, en_espera, orden_cancelada",
      )
      .eq("id_orden", idOrden)
      .maybeSingle();
    if (ordErr) throw ordErr;
    if (!orden?.id_orden) {
      return { processed: false, reason: "orden_no_encontrada", id_orden: idOrden };
    }
    if (isTrue(orden?.orden_cancelada)) {
      return { processed: false, reason: "orden_cancelada", id_orden: idOrden };
    }
    const metodoPagoId = toPositiveInt(orden?.id_metodo_de_pago);
    if (allowedMetodoIds.length && !allowedMetodoIds.includes(metodoPagoId || 0)) {
      return {
        processed: false,
        reason: "metodo_no_permitido",
        id_orden: idOrden,
        id_metodo_de_pago: metodoPagoId,
      };
    }

    const idUsuarioVentas = toPositiveInt(match?.id_usuario) || toPositiveInt(orden?.id_usuario);
    if (!idUsuarioVentas) {
      return { processed: false, reason: "id_usuario_invalido", id_orden: idOrden };
    }

    const syncPagoMovilCredito = async () => {
      if (syncPagoMovil !== true) {
        return { ok: true, skipped: true, error: null };
      }
      try {
        await markPagoMovilRowAsCredited({
          pagoId: match?.pago_id,
          idUsuario: idUsuarioVentas,
          referenciaMatch: match?.referencia_match,
        });
        return { ok: true, error: null };
      } catch (err) {
        const errorMsg = err?.message || String(err);
        console.error("[autoProcessMatchedOrder] pagomoviles sync error", {
          id_orden: idOrden,
          pago_id: toPositiveInt(match?.pago_id) || null,
          id_usuario: idUsuarioVentas,
          error: errorMsg,
        });
        return { ok: false, error: errorMsg };
      }
    };

    const alreadyProcessed = await hasHistorialVentasForOrder(idOrden);

    if (alreadyProcessed) {
      const ventasExist = await fetchVentasByOrder({
        idOrden,
        select: "id_venta, pendiente",
      });
      const pendientesCount = (ventasExist || []).filter((row) => isTrue(row?.pendiente)).length;
      let saldoReconciliado = { debitado: false, monto: 0, saldoNuevo: null };
      let saldoReconciliacionError = null;
      const saldoEsperadoOrden = await resolveSaldoAplicadoForOrderVerification({ orden });
      const ordenUpdate = {
        pago_verificado: true,
        en_espera: pendientesCount > 0,
      };
      if (!(toFiniteMoney(orden?.saldo_aplicado_usd) > 0) && saldoEsperadoOrden > 0) {
        try {
          const saldoDebitRes = await debitSaldoUsdFromUser({
            idUsuario: idUsuarioVentas,
            montoUsd: saldoEsperadoOrden,
            source: "auto_process_order:already_processed",
          });
          saldoReconciliado = {
            debitado: !!saldoDebitRes?.debitado,
            monto: Number(saldoDebitRes?.monto) || 0,
            saldoNuevo: saldoDebitRes?.saldoNuevo ?? null,
          };
          ordenUpdate.saldo_aplicado_usd = saldoEsperadoOrden;
        } catch (saldoErr) {
          saldoReconciliacionError = saldoErr?.message || "saldo_reconciliation_failed";
          console.error("[autoProcessMatchedOrder] saldo reconciliation error", {
            id_orden: idOrden,
            id_usuario: idUsuarioVentas,
            expected_saldo_usd: saldoEsperadoOrden,
            error: saldoReconciliacionError,
          });
        }
      }
      const { error: updOrdErr } = await supabaseAdmin
        .from("ordenes")
        .update(ordenUpdate)
        .eq("id_orden", idOrden);
      if (updOrdErr) throw updOrdErr;
      try {
        const waDeliveredRes = await notifyVentaEntregadaAfterOrderVerified({
          idOrden,
          manageWhatsappLifecycle: true,
        });
        if (!waDeliveredRes?.sent) {
          console.log("[autoProcessMatchedOrder] whatsapp orden entregada post-verificacion skipped", {
            id_orden: idOrden,
            reason: waDeliveredRes?.reason || "unknown",
            error: waDeliveredRes?.error || null,
          });
        }
      } catch (waDeliveredErr) {
        console.error("[autoProcessMatchedOrder] whatsapp orden entregada post-verificacion error", {
          id_orden: idOrden,
          error: waDeliveredErr?.message || waDeliveredErr,
        });
      }
      const pagoSync = await syncPagoMovilCredito();
      try {
        await cleanupClosedOrderCarrito({
          idOrden: idOrden,
          idCarrito: orden?.id_carrito,
          source: "auto_process_order:already_processed",
        });
      } catch (cleanupErr) {
        console.error("[autoProcessMatchedOrder] cleanup carrito already_processed error", {
          id_orden: toPositiveInt(idOrden) || null,
          id_carrito: toPositiveInt(orden?.id_carrito) || null,
          error: cleanupErr?.message || cleanupErr,
        });
      }
      return {
        processed: true,
        reason: "orden_ya_procesada",
        id_orden: idOrden,
        ventas: ventasExist.length,
        pendientes: pendientesCount,
        pago_sync_ok: pagoSync.ok,
        pago_sync_error: pagoSync.error,
        saldo_debitado: saldoReconciliado.debitado,
        saldo_debitado_usd: saldoReconciliado.monto,
        saldo_usuario_nuevo: saldoReconciliado.saldoNuevo,
        saldo_debitado_error: saldoReconciliacionError,
      };
    }

    if (isTrue(orden?.pago_verificado)) {
      const pagoSync = await syncPagoMovilCredito();
      try {
        await cleanupClosedOrderCarrito({
          idOrden: idOrden,
          idCarrito: orden?.id_carrito,
          source: "auto_process_order:already_verified",
        });
      } catch (cleanupErr) {
        console.error("[autoProcessMatchedOrder] cleanup carrito already_verified error", {
          id_orden: toPositiveInt(idOrden) || null,
          id_carrito: toPositiveInt(orden?.id_carrito) || null,
          error: cleanupErr?.message || cleanupErr,
        });
      }
      return {
        processed: false,
        reason: "orden_ya_verificada",
        id_orden: idOrden,
        pago_sync_ok: pagoSync.ok,
        pago_sync_error: pagoSync.error,
      };
    }

    const completeNoItemsOrder = async ({ montoAuto = 0, motivo = "sin_items" } = {}) => {
      const saldoInfo = await creditSaldoUsdToUser({
        idUsuario: idUsuarioVentas,
        montoUsd: montoAuto,
      });
      const { error: updOrdErr } = await supabaseAdmin
        .from("ordenes")
        .update({
          pago_verificado: true,
          en_espera: false,
        })
        .eq("id_orden", idOrden);
      if (updOrdErr) throw updOrdErr;
      try {
        await cleanupClosedOrderCarrito({
          idOrden: idOrden,
          idCarrito: orden?.id_carrito,
          source: "auto_process_order:no_items",
        });
      } catch (cleanupErr) {
        console.error("[autoProcessMatchedOrder] cleanup carrito no_items error", {
          id_orden: toPositiveInt(idOrden) || null,
          id_carrito: toPositiveInt(orden?.id_carrito) || null,
          error: cleanupErr?.message || cleanupErr,
        });
      }
      const pagoSync = await syncPagoMovilCredito();

      return {
        processed: true,
        reason: motivo,
        id_orden: idOrden,
        ventas: 0,
        pendientes: 0,
        saldo_acreditado: saldoInfo.acreditado,
        excedente_acreditado: saldoInfo.monto,
        saldo_nuevo: saldoInfo.saldoNuevo,
        pago_sync_ok: pagoSync.ok,
        pago_sync_error: pagoSync.error,
      };
    };

    if (!toPositiveInt(orden?.id_carrito)) {
      return completeNoItemsOrder({
        montoAuto: Number(orden?.total) || 0,
        motivo: "orden_sin_carrito",
      });
    }

    const context = await buildCheckoutContext({
      idUsuarioVentas,
      carritoId: orden.id_carrito,
      totalCliente: orden.total,
    });
    const orderTotalProcesado = parseOptionalCheckoutNumber(orden?.total);
    const montoBaseCobrado = Number.isFinite(orderTotalProcesado)
      ? roundCheckoutMoney(orderTotalProcesado)
      : await resolveMontoBaseCarrito({
          carritoId: orden.id_carrito,
          fallbackTotal: context.total,
        });

    if (!context.items?.length) {
      return completeNoItemsOrder({
        montoAuto: Number(montoBaseCobrado) || Number(context.total) || 0,
        motivo: "carrito_sin_items",
      });
    }

    const archivos = normalizeFilesArray(orden?.comprobante);
    const result = await processOrderFromItems({
      ordenId: idOrden,
      idUsuarioSesion: idUsuarioVentas,
      idUsuarioVentas,
      items: context.items,
      priceMap: context.priceMap,
      platInfoById: context.platInfoById,
      platNameById: context.platNameById,
      pickPrecio: context.pickPrecio,
      descuentos: context.descuentos,
      discountColumns: context.discountColumns,
      discountColumnById: context.discountColumnById,
      isCliente: context.isCliente,
      referencia: orden?.referencia,
      archivos,
      id_metodo_de_pago: orden?.id_metodo_de_pago,
      carritoId: orden.id_carrito,
      montoHistorialTotalOverride: montoBaseCobrado,
      snapshotTotalUsd: orden?.total ?? context?.total ?? null,
      snapshotMontoBsTotal: orden?.monto_bs ?? null,
      snapshotTasaBs: orden?.tasa_bs ?? context?.tasaBs ?? null,
      saldoAplicadoExpectedUsd: await resolveSaldoAplicadoForOrderVerification({ orden }),
      allowSaldoInsuficiente: true,
    });

    const { error: updOrdErr } = await supabaseAdmin
      .from("ordenes")
      .update({
        pago_verificado: true,
        en_espera: result.pendientesCount > 0,
      })
      .eq("id_orden", idOrden);
    if (updOrdErr) throw updOrdErr;
    try {
      await cleanupClosedOrderCarrito({
        idOrden: idOrden,
        idCarrito: orden?.id_carrito,
        source: "auto_process_order:verified",
      });
    } catch (cleanupErr) {
      console.error("[autoProcessMatchedOrder] cleanup carrito verified error", {
        id_orden: toPositiveInt(idOrden) || null,
        id_carrito: toPositiveInt(orden?.id_carrito) || null,
        error: cleanupErr?.message || cleanupErr,
      });
    }
    try {
      const waDeliveredRes = await notifyVentaEntregadaAfterOrderVerified({
        idOrden,
        manageWhatsappLifecycle: true,
      });
      if (!waDeliveredRes?.sent) {
        console.log("[autoProcessMatchedOrder] whatsapp orden entregada post-procesado skipped", {
          id_orden: idOrden,
          reason: waDeliveredRes?.reason || "unknown",
          error: waDeliveredRes?.error || null,
        });
      }
    } catch (waDeliveredErr) {
      console.error("[autoProcessMatchedOrder] whatsapp orden entregada post-procesado error", {
        id_orden: idOrden,
        error: waDeliveredErr?.message || waDeliveredErr,
      });
    }
    const pagoSync = await syncPagoMovilCredito();

    return {
      processed: true,
      reason: "orden_procesada",
      id_orden: idOrden,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
      pago_sync_ok: pagoSync.ok,
      pago_sync_error: pagoSync.error,
    };
  } finally {
    await releaseSharedOrderProcessLock(idOrden);
  }
};

const autoMatchPagoMovilAgainstOrders = async (
  pagoMovilRow = {},
  { targetUserId = null } = {},
) => {
  const pagoId = Number(pagoMovilRow?.id || 0);
  if (!pagoId) return { matched: false, reason: "invalid_pago_id" };

  const pagoMonto = normalizeMontoBs(pagoMovilRow?.monto_bs);
  const refCandidates = Array.from(
    new Set(
      [
        ...extractRefCandidates(pagoMovilRow?.texto || ""),
        ...extractRefCandidates(pagoMovilRow?.referencia || ""),
      ]
        .map((r) => normalizeReferenceDigits(r))
        .filter((r) => r.length >= 4),
    ),
  );
  if (!refCandidates.length || !Number.isFinite(pagoMonto)) {
    return { matched: false, reason: "missing_ref_or_amount" };
  }

  const userFilterId = toPositiveInt(targetUserId);
  let pendingOrdersQuery = supabaseAdmin
    .from("ordenes")
    .select(
      "id_orden, id_usuario, referencia, monto_bs, total, tasa_bs, id_metodo_de_pago, marcado_pago, pago_verificado, en_espera, orden_cancelada, fecha, hora_orden",
    )
    .eq("id_metodo_de_pago", 1)
    .eq("marcado_pago", true)
    .eq("pago_verificado", false)
    .eq("orden_cancelada", false)
    .order("id_orden", { ascending: false })
    .limit(400);
  if (userFilterId) {
    pendingOrdersQuery = pendingOrdersQuery.eq("id_usuario", userFilterId);
  }
  const { data: pendingOrders, error: ordErr } = await pendingOrdersQuery;
  if (ordErr) throw ordErr;

  const matchedOrders = [];
  for (const orden of pendingOrders || []) {
    const refDigits = normalizeReferenceDigits(orden?.referencia);
    if (refDigits.length < 4) continue;
    const refResolution = resolveBestReferenceCandidate(refDigits, refCandidates);
    if (refResolution.score <= 0) continue;
    const montoOrdenBs = await computeOrderMontoBs(orden);
    if (!Number.isFinite(montoOrdenBs)) continue;
    if (Math.abs(montoOrdenBs - pagoMonto) > 0.01) continue;
    matchedOrders.push({
      orden,
      refResolution,
      orderId: Number(orden?.id_orden) || 0,
    });
  }

  if (!matchedOrders.length) {
    return { matched: false, reason: "no_order_match" };
  }
  matchedOrders.sort((a, b) => {
    if (b.refResolution.score !== a.refResolution.score) {
      return b.refResolution.score - a.refResolution.score;
    }
    return b.orderId - a.orderId;
  });
  const selected = matchedOrders[0];
  const matchedOrder = selected?.orden || null;
  const refMatch = selected?.refResolution?.ref || null;

  return {
    matched: true,
    id_orden: matchedOrder.id_orden,
    id_usuario: matchedOrder.id_usuario || null,
    pago_id: pagoId,
    referencia_match: refMatch,
    monto_bs_pago: pagoMonto,
  };
};

const processCreditedPagoMovilOrders = async () => {
  if (!AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_ENABLED) {
    return { skipped: true, reason: "disabled" };
  }
  if (autoCreditedPagoMovilOrderProcessInProgress) {
    return { skipped: true, reason: "in_progress" };
  }
  autoCreditedPagoMovilOrderProcessInProgress = true;
  const summary = {
    fetched: 0,
    matched: 0,
    processed: 0,
    skipped: 0,
    failed: 0,
  };
  try {
    const { data: pendingOrders, error: pendingErr } = await supabaseAdmin
      .from("ordenes")
      .select(
        "id_orden, id_usuario, referencia, monto_bs, total, tasa_bs, id_metodo_de_pago, marcado_pago, pago_verificado, en_espera, orden_cancelada, fecha, hora_orden, checkout_finalizado",
      )
      .eq("id_metodo_de_pago", 1)
      .eq("marcado_pago", true)
      .eq("checkout_finalizado", true)
      .eq("pago_verificado", false)
      .eq("orden_cancelada", false)
      .order("id_orden", { ascending: false })
      .limit(AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_BATCH);
    if (pendingErr) throw pendingErr;

    const orders = Array.isArray(pendingOrders) ? pendingOrders : [];
    summary.fetched = orders.length;

    for (const orden of orders) {
      const orderId = toPositiveInt(orden?.id_orden);
      const orderUserId = toPositiveInt(orden?.id_usuario);
      if (!orderId || !orderUserId) {
        summary.skipped += 1;
        continue;
      }
      try {
        const match = await matchPagoMovilToOrder(orden, {
          includeCreditedForUserId: orderUserId,
          requireCredited: true,
        });
        if (!match?.matched || !toPositiveInt(match?.pago_id)) {
          summary.skipped += 1;
          continue;
        }
        summary.matched += 1;
        const processResult = await autoProcessMatchedOrder({
          id_orden: orderId,
          id_usuario: orderUserId,
          pago_id: match.pago_id,
          referencia_match: match.referencia_match || null,
          monto_bs_pago: match.monto_bs_pago ?? null,
        });
        if (processResult?.processed === true) {
          summary.processed += 1;
        } else {
          summary.skipped += 1;
        }
      } catch (itemErr) {
        summary.failed += 1;
        console.error("[autoCreditedPagoMovilOrders] order error", {
          id_orden: orderId,
          error: itemErr?.message || itemErr,
        });
      }
    }
    return summary;
  } finally {
    autoCreditedPagoMovilOrderProcessInProgress = false;
  }
};

const parseFlexibleMoneyNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const raw = String(value).trim();
  if (!raw) return null;
  let clean = raw.replace(/[^\d.,-]/g, "");
  if (!clean) return null;
  if (clean.includes(".") && clean.includes(",")) {
    clean = clean.replace(/\./g, "").replace(",", ".");
  } else if (clean.includes(",")) {
    clean = clean.replace(",", ".");
  }
  const num = Number(clean);
  return Number.isFinite(num) ? num : null;
};

const getBinanceMetodoPagoIdsFromEnv = () =>
  Array.from(
    new Set(
      String(process.env.BINANCE_METODO_PAGO_IDS || "")
        .split(",")
        .map((value) => toPositiveInt(value))
        .filter(Boolean),
    ),
  );

const resolveBinanceMetodoPagoIds = async ({ forceRefresh = false } = {}) => {
  const now = Date.now();
  if (
    !forceRefresh &&
    Array.isArray(cachedBinanceMetodoPagoIds.ids) &&
    cachedBinanceMetodoPagoIds.ids.length &&
    now - Number(cachedBinanceMetodoPagoIds.ts || 0) < BINANCE_GMAIL_METODO_CACHE_MS
  ) {
    return [...cachedBinanceMetodoPagoIds.ids];
  }

  const ids = new Set(getBinanceMetodoPagoIdsFromEnv());
  try {
    const { data, error } = await supabaseAdmin
      .from("metodos_de_pago")
      .select("id_metodo_de_pago, nombre")
      .ilike("nombre", "%binance%");
    if (error) throw error;
    (Array.isArray(data) ? data : [])
      .map((row) => toPositiveInt(row?.id_metodo_de_pago))
      .filter(Boolean)
      .forEach((id) => ids.add(id));
  } catch (err) {
    console.error("[binance:metodos] no se pudo consultar metodos_de_pago", {
      error: err?.message || err,
    });
  }

  const resolvedIds = Array.from(ids).sort((a, b) => a - b);
  cachedBinanceMetodoPagoIds = { ids: resolvedIds, ts: now };
  return resolvedIds;
};

const isBinanceMetodoPago = async (metodoPagoRow = {}) => {
  const metodoId = toPositiveInt(metodoPagoRow?.id_metodo_de_pago);
  const nombre = String(metodoPagoRow?.nombre || "")
    .trim()
    .toLowerCase();
  if (nombre.includes("binance")) return true;
  if (!metodoId) return false;
  const knownIds = await resolveBinanceMetodoPagoIds();
  return knownIds.includes(metodoId);
};

const resolveOrderUsdAmountForBinance = (orden = {}) => {
  const montoObjetivo = parseFlexibleMoneyNumber(orden?.monto_usdt_objetivo);
  if (Number.isFinite(montoObjetivo) && montoObjetivo > 0) {
    return Math.round(montoObjetivo * 1000) / 1000;
  }
  const montoTransferido = parseFlexibleMoneyNumber(orden?.monto_transferido);
  if (Number.isFinite(montoTransferido) && montoTransferido > 0) {
    return Math.round(montoTransferido * 1000) / 1000;
  }
  const total = parseFlexibleMoneyNumber(orden?.total);
  if (Number.isFinite(total) && total > 0) {
    return Math.round(total * 1000) / 1000;
  }
  return null;
};

const isOrderInsideBinanceActiveWindow = (orden = {}, nowMs = Date.now()) => {
  const expiraEn = String(orden?.monto_objetivo_expira_en || "").trim();
  if (expiraEn) {
    const expiraDate = new Date(expiraEn);
    if (!Number.isNaN(expiraDate.getTime())) {
      return expiraDate.getTime() >= nowMs;
    }
  }
  const orderDate = parseCaracasOrderDateTime(orden?.fecha, orden?.hora_orden);
  if (!orderDate) return false;
  const elapsedMs = nowMs - orderDate.getTime();
  if (!Number.isFinite(elapsedMs)) return false;
  return elapsedMs >= -5 * 60 * 1000 && elapsedMs <= BINANCE_GMAIL_ORDER_WINDOW_MS;
};

const extractUniqueThirdDecimalStep = (amount, baseAmount) => {
  const parsedAmount = parseFlexibleMoneyNumber(amount);
  const parsedBase = parseFlexibleMoneyNumber(baseAmount);
  if (!Number.isFinite(parsedAmount) || !Number.isFinite(parsedBase)) return null;
  const diffMillis = Math.round((parsedAmount - parsedBase) * 1000);
  if (!Number.isFinite(diffMillis)) return null;
  if (diffMillis < 1 || diffMillis > BINANCE_UNIQUE_AMOUNT_MAX_DECIMAL_STEP) return null;
  return diffMillis;
};

const resolveUniqueBinanceCheckoutTotal = async ({
  baseTotalUsd = 0,
  metodoPagoRow = {},
  reuseOrderId = null,
} = {}) => {
  const base = parseFlexibleMoneyNumber(baseTotalUsd);
  if (!Number.isFinite(base) || base <= 0) {
    return { applied: false, total: baseTotalUsd, reason: "invalid_base_total" };
  }
  if (!BINANCE_UNIQUE_AMOUNT_ENABLED) {
    return { applied: false, total: Math.round(base * 100) / 100, reason: "disabled" };
  }
  const isBinanceMethod = await isBinanceMetodoPago(metodoPagoRow);
  if (!isBinanceMethod) {
    return { applied: false, total: Math.round(base * 100) / 100, reason: "metodo_no_binance" };
  }

  const metodoPagoId = toPositiveInt(metodoPagoRow?.id_metodo_de_pago);
  if (!metodoPagoId) {
    return { applied: false, total: Math.round(base * 100) / 100, reason: "invalid_metodo_pago" };
  }

  const baseRounded = Math.round(base * 100) / 100;
  const reuseId = toPositiveInt(reuseOrderId);
  if (reuseId) {
    const { data: existingOrder, error: existingOrderErr } = await supabaseAdmin
      .from("ordenes")
      .select(
        "id_orden, id_metodo_de_pago, total, monto_transferido, monto_usdt_objetivo, binance_decimal_slot, monto_objetivo_expira_en, marcado_pago, checkout_finalizado, pago_verificado, orden_cancelada, fecha, hora_orden",
      )
      .eq("id_orden", reuseId)
      .maybeSingle();
    if (existingOrderErr) throw existingOrderErr;
    const existingTotal = resolveOrderUsdAmountForBinance(existingOrder);
    const sameMethod = toPositiveInt(existingOrder?.id_metodo_de_pago) === metodoPagoId;
    const insideBucket =
      Number.isFinite(existingTotal) &&
      existingTotal >= baseRounded &&
      existingTotal < baseRounded + 0.01;
    const reusableState =
      isTrue(existingOrder?.marcado_pago) &&
      isTrue(existingOrder?.checkout_finalizado) &&
      !isTrue(existingOrder?.pago_verificado) &&
      !isTrue(existingOrder?.orden_cancelada);
    if (sameMethod && insideBucket && reusableState) {
      const reusedStep = extractUniqueThirdDecimalStep(existingTotal, baseRounded);
      if (reusedStep) {
        return {
          applied: true,
          total: Math.round(existingTotal * 1000) / 1000,
          step: reusedStep,
          reused: true,
          activeConflicts: null,
        };
      }
    }
  }

  let pendingOrdersQuery = supabaseAdmin
    .from("ordenes")
    .select(
      "id_orden, total, monto_transferido, monto_usdt_objetivo, binance_decimal_slot, monto_objetivo_expira_en, fecha, hora_orden, marcado_pago, checkout_finalizado, pago_verificado, orden_cancelada",
    )
    .eq("id_metodo_de_pago", metodoPagoId)
    .eq("marcado_pago", true)
    .eq("checkout_finalizado", true)
    .eq("pago_verificado", false)
    .eq("orden_cancelada", false)
    .gte("total", baseRounded)
    .lt("total", baseRounded + 0.01)
    .order("id_orden", { ascending: true })
    .limit(1000);
  if (reuseId) {
    pendingOrdersQuery = pendingOrdersQuery.neq("id_orden", reuseId);
  }
  const { data: pendingRows, error: pendingErr } = await pendingOrdersQuery;
  if (pendingErr) throw pendingErr;
  const pendingOrders = Array.isArray(pendingRows) ? pendingRows : [];
  const activeOrders = pendingOrders.filter((row) => isOrderInsideBinanceActiveWindow(row));
  const usedSteps = new Set(
    activeOrders
      .map((row) => extractUniqueThirdDecimalStep(resolveOrderUsdAmountForBinance(row), baseRounded))
      .filter((step) => Number.isFinite(step) && step >= 1),
  );

  const totalActiveIncludingCurrent = activeOrders.length + 1;
  if (totalActiveIncludingCurrent < BINANCE_UNIQUE_AMOUNT_MIN_PENDING) {
    return { applied: false, total: baseRounded, reason: "below_min_pending" };
  }

  let step = 1;
  while (usedSteps.has(step) && step <= BINANCE_UNIQUE_AMOUNT_MAX_DECIMAL_STEP) {
    step += 1;
  }
  if (step > BINANCE_UNIQUE_AMOUNT_MAX_DECIMAL_STEP) {
    const err = new Error(
      `No hay más slots de monto único para base ${baseRounded.toFixed(2)} (tope ${BINANCE_UNIQUE_AMOUNT_MAX_DECIMAL_STEP}).`,
    );
    err.code = "BINANCE_UNIQUE_AMOUNT_CAP_REACHED";
    throw err;
  }

  const uniqueTotal = Math.round((baseRounded + step / 1000) * 1000) / 1000;
  return {
    applied: true,
    total: uniqueTotal,
    step,
    reused: false,
    activeConflicts: activeOrders.length,
  };
};

const cleanupBinanceGmailOauthStates = () => {
  const now = Date.now();
  for (const [state, meta] of binanceGmailOauthStateMap.entries()) {
    if (now - Number(meta?.createdAt || 0) > BINANCE_GMAIL_OAUTH_STATE_TTL_MS) {
      binanceGmailOauthStateMap.delete(state);
    }
  }
};

const requireBinanceGmailOauthConfig = () => {
  if (!GMAIL_OAUTH_CLIENT_ID || !GMAIL_OAUTH_CLIENT_SECRET || !GMAIL_OAUTH_REDIRECT_URI) {
    const err = new Error(
      "Faltan GMAIL_OAUTH_CLIENT_ID, GMAIL_OAUTH_CLIENT_SECRET o GMAIL_OAUTH_REDIRECT_URI.",
    );
    err.code = "BINANCE_GMAIL_OAUTH_CONFIG_MISSING";
    throw err;
  }
};

const buildBinanceGmailOauthUrl = ({ state }) => {
  requireBinanceGmailOauthConfig();
  const url = new URL("https://accounts.google.com/o/oauth2/v2/auth");
  url.searchParams.set("client_id", GMAIL_OAUTH_CLIENT_ID);
  url.searchParams.set("redirect_uri", GMAIL_OAUTH_REDIRECT_URI);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("scope", GMAIL_OAUTH_SCOPES.join(" "));
  url.searchParams.set("access_type", "offline");
  url.searchParams.set("prompt", "consent");
  url.searchParams.set("include_granted_scopes", "true");
  if (state) {
    url.searchParams.set("state", state);
  }
  return url.toString();
};

const exchangeGoogleOauthCodeForTokens = async ({ code }) => {
  requireBinanceGmailOauthConfig();
  const payload = new URLSearchParams({
    code: String(code || "").trim(),
    client_id: GMAIL_OAUTH_CLIENT_ID,
    client_secret: GMAIL_OAUTH_CLIENT_SECRET,
    redirect_uri: GMAIL_OAUTH_REDIRECT_URI,
    grant_type: "authorization_code",
  });
  const resp = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: payload.toString(),
  });
  const raw = await resp.text();
  let parsed = {};
  try {
    parsed = raw ? JSON.parse(raw) : {};
  } catch (_err) {
    parsed = {};
  }
  if (!resp.ok) {
    const err = new Error(parsed?.error_description || parsed?.error || `OAuth error ${resp.status}`);
    err.code = "BINANCE_GMAIL_OAUTH_EXCHANGE_FAILED";
    err.status = resp.status;
    err.details = parsed;
    throw err;
  }
  return parsed || {};
};

const getConfiguredGmailRefreshToken = () => {
  const fromCache = String(cachedGmailRefreshToken || "").trim();
  if (fromCache) return fromCache;
  const fromEnv = String(process.env.GMAIL_OAUTH_REFRESH_TOKEN || "").trim();
  if (fromEnv) {
    cachedGmailRefreshToken = fromEnv;
    return fromEnv;
  }
  return "";
};

const fetchGoogleAccessTokenWithRefreshToken = async () => {
  requireBinanceGmailOauthConfig();
  const refreshToken = getConfiguredGmailRefreshToken();
  if (!refreshToken) {
    const err = new Error(
      "No hay refresh token configurado. Completa OAuth y guarda GMAIL_OAUTH_REFRESH_TOKEN.",
    );
    err.code = "BINANCE_GMAIL_REFRESH_TOKEN_MISSING";
    throw err;
  }
  const payload = new URLSearchParams({
    client_id: GMAIL_OAUTH_CLIENT_ID,
    client_secret: GMAIL_OAUTH_CLIENT_SECRET,
    refresh_token: refreshToken,
    grant_type: "refresh_token",
  });
  const resp = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: payload.toString(),
  });
  const raw = await resp.text();
  let parsed = {};
  try {
    parsed = raw ? JSON.parse(raw) : {};
  } catch (_err) {
    parsed = {};
  }
  if (!resp.ok || !String(parsed?.access_token || "").trim()) {
    const err = new Error(
      parsed?.error_description || parsed?.error || `Token refresh error ${resp.status}`,
    );
    err.code = "BINANCE_GMAIL_ACCESS_TOKEN_FAILED";
    err.status = resp.status;
    err.details = parsed;
    throw err;
  }
  return String(parsed.access_token).trim();
};

const buildGmailApiUrl = (path, query = {}) => {
  const url = new URL(`https://gmail.googleapis.com${String(path || "")}`);
  Object.entries(query || {}).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") return;
    if (Array.isArray(value)) {
      value
        .map((item) => String(item || "").trim())
        .filter(Boolean)
        .forEach((item) => url.searchParams.append(key, item));
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url.toString();
};

const gmailApiRequest = async ({ accessToken, path, query = {} } = {}) => {
  const token = String(accessToken || "").trim();
  if (!token) {
    const err = new Error("access token inválido para Gmail API");
    err.code = "BINANCE_GMAIL_ACCESS_TOKEN_INVALID";
    throw err;
  }
  const url = buildGmailApiUrl(path, query);
  const resp = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
    },
  });
  const raw = await resp.text();
  let parsed = {};
  try {
    parsed = raw ? JSON.parse(raw) : {};
  } catch (_err) {
    parsed = {};
  }
  if (!resp.ok) {
    const err = new Error(parsed?.error?.message || `Gmail API error ${resp.status}`);
    err.code = "BINANCE_GMAIL_API_ERROR";
    err.status = resp.status;
    err.details = parsed;
    throw err;
  }
  return parsed || {};
};

const decodeGmailBodyData = (rawData) => {
  const raw = String(rawData || "").trim();
  if (!raw) return "";
  try {
    return decodeBase64Url(raw).toString("utf8");
  } catch (_err) {
    return "";
  }
};

const stripHtmlTags = (value) =>
  String(value || "")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/\s+/g, " ")
    .trim();

const collectGmailPayloadTexts = (payload = {}, collected = []) => {
  if (!payload || typeof payload !== "object") return collected;
  const mime = String(payload.mimeType || "").toLowerCase();
  const bodyData = decodeGmailBodyData(payload?.body?.data);
  if (bodyData) {
    if (mime.includes("text/html")) {
      const stripped = stripHtmlTags(bodyData);
      if (stripped) collected.push(stripped);
    } else {
      collected.push(String(bodyData).replace(/\s+/g, " ").trim());
    }
  }
  const parts = Array.isArray(payload.parts) ? payload.parts : [];
  parts.forEach((part) => collectGmailPayloadTexts(part, collected));
  return collected;
};

const getGmailHeaderValue = (payload = {}, targetHeader = "") => {
  const target = String(targetHeader || "").trim().toLowerCase();
  if (!target) return "";
  const headers = Array.isArray(payload?.headers) ? payload.headers : [];
  const match = headers.find((header) => {
    const name = String(header?.name || "").trim().toLowerCase();
    return name === target;
  });
  return String(match?.value || "").trim();
};

const extractEmailFromHeader = (fromHeader = "") => {
  const raw = String(fromHeader || "").trim();
  if (!raw) return "";
  const explicit = raw.match(/<([^>]+)>/);
  const email = (explicit?.[1] || raw).trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return raw;
  return email;
};

const normalizeBinanceAmount = (value) => {
  const parsed = parseFlexibleMoneyNumber(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.round(parsed * 1000) / 1000;
};

const extractBinanceAmountFromText = (value = "") => {
  const text = String(value || "");
  if (!text) return null;
  const patterns = [
    /\b([0-9]{1,6}(?:[.,][0-9]{1,8})?)\s*(USDT|BUSD|USD)\b/gi,
    /\b(USDT|BUSD|USD)\s*([0-9]{1,6}(?:[.,][0-9]{1,8})?)\b/gi,
    /(?:monto|amount)\s*(?:recibido|received|paid|pagado)?\s*[:：]?\s*([0-9]{1,6}(?:[.,][0-9]{1,8})?)/gi,
  ];
  for (const pattern of patterns) {
    pattern.lastIndex = 0;
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const candidate = normalizeBinanceAmount(match?.[1] || match?.[2]);
      if (Number.isFinite(candidate) && candidate > 0) {
        return candidate;
      }
    }
  }
  return null;
};

const extractBinanceReferenceFromText = (value = "") => {
  const text = String(value || "");
  if (!text) return "";
  const patterns = [
    /(?:order|orden)\s*(?:id|n(?:o|ro)?|#)?\s*[:#-]?\s*([A-Z0-9-]{6,64})/i,
    /(?:transaction|transacci(?:o|ó)n)\s*(?:id|hash|#)?\s*[:#-]?\s*([A-Z0-9-]{6,64})/i,
    /(?:referencia|reference)\s*[:#-]?\s*([A-Z0-9-]{6,64})/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match?.[1]) continue;
    return String(match[1]).trim();
  }
  return "";
};

const extractGmailMessageDateIso = (message = {}) => {
  const internalDate = Number(message?.internalDate);
  if (Number.isFinite(internalDate) && internalDate > 0) {
    const fromInternal = new Date(internalDate);
    if (!Number.isNaN(fromInternal.getTime())) return fromInternal.toISOString();
  }
  const headerDate = getGmailHeaderValue(message?.payload, "date");
  if (headerDate) {
    const parsed = new Date(headerDate);
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
  }
  return new Date().toISOString();
};

const toCaracasTimeString = (isoDate) => {
  const date = new Date(isoDate);
  if (Number.isNaN(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "America/Caracas",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(date);
  const map = parts.reduce((acc, part) => {
    if (part.type !== "literal") acc[part.type] = part.value;
    return acc;
  }, {});
  if (!map.hour || !map.minute || !map.second) return null;
  return `${map.hour}:${map.minute}:${map.second}`;
};

const buildBinanceGmailPaymentCandidate = (message = {}) => {
  const messageId = String(message?.id || "").trim();
  if (!messageId) return null;
  const threadId = String(message?.threadId || "").trim();
  const payload = message?.payload || {};
  const subject = getGmailHeaderValue(payload, "subject") || "Binance";
  const fromHeader = getGmailHeaderValue(payload, "from");
  const fromEmail = extractEmailFromHeader(fromHeader);
  const snippet = String(message?.snippet || "").trim();
  const payloadTexts = collectGmailPayloadTexts(payload, []);
  const textParts = [subject, snippet, ...payloadTexts].filter(Boolean);
  const fullText = textParts.join(" ").replace(/\s+/g, " ").trim();
  const amount = extractBinanceAmountFromText(fullText);
  const monedaMatch = fullText.match(/\b(USDT|BUSD|USD)\b/i);
  const moneda = String(monedaMatch?.[1] || "USDT").toUpperCase();
  const referencia = extractBinanceReferenceFromText(fullText);
  const fechaIso = extractGmailMessageDateIso(message);
  const horaRecibido = toCaracasTimeString(fechaIso);
  const hash = crypto
    .createHash("sha256")
    .update(
      [
        BINANCE_GMAIL_APP_NAME,
        messageId,
        threadId,
        subject,
        fromEmail,
        fechaIso,
        amount ?? "",
        referencia,
      ].join("|"),
    )
    .digest("hex");

  return {
    gmail_message_id: messageId,
    gmail_thread_id: threadId || null,
    titulo: subject,
    texto: fullText || snippet || null,
    fecha: fechaIso,
    fecha_correo: fechaIso,
    hora_recibido: horaRecibido,
    referencia: referencia || null,
    monto_usdt: Number.isFinite(amount) ? amount : null,
    moneda,
    remitente: fromEmail || null,
    hash,
    raw_payload: {
      snippet,
      from: fromHeader || null,
    },
  };
};

const findBinanceGmailPaymentByMessageId = async (messageId = "") => {
  const id = String(messageId || "").trim();
  if (!id) return null;
  const { data, error } = await supabaseAdmin
    .from(BINANCE_GMAIL_PAYMENTS_TABLE)
    .select(
      "id, gmail_message_id, monto_usdt, referencia, pago_verificado, id_orden, id_usuario, estado, detalle_estado",
    )
    .eq("gmail_message_id", id)
    .maybeSingle();
  if (error) {
    if (isMissingTableError(error, BINANCE_GMAIL_PAYMENTS_TABLE)) {
      const err = new Error(
        `Falta la tabla ${BINANCE_GMAIL_PAYMENTS_TABLE}. Ejecuta el SQL de creación primero.`,
      );
      err.code = "BINANCE_GMAIL_TABLE_MISSING";
      throw err;
    }
    throw error;
  }
  return data || null;
};

const insertBinanceGmailPayment = async (candidate = {}) => {
  const messageId = String(candidate?.gmail_message_id || "").trim();
  if (!messageId) return { stored: false, duplicate: false, row: null };

  const existing = await findBinanceGmailPaymentByMessageId(messageId);
  if (existing?.id) {
    return { stored: false, duplicate: true, row: existing };
  }

  const payload = {
    titulo: String(candidate?.titulo || "Binance").trim() || "Binance",
    texto: String(candidate?.texto || "").trim() || null,
    fecha: String(candidate?.fecha || "").trim() || new Date().toISOString(),
    dispositivo: "gmail_api",
    app: BINANCE_GMAIL_APP_NAME,
    hash: String(candidate?.hash || "").trim() || null,
    referencia: String(candidate?.referencia || "").trim() || null,
    monto_usdt: Number.isFinite(Number(candidate?.monto_usdt))
      ? Number(candidate.monto_usdt)
      : null,
    moneda: String(candidate?.moneda || "USDT").trim().toUpperCase(),
    remitente: String(candidate?.remitente || "").trim() || null,
    gmail_message_id: messageId,
    gmail_thread_id: String(candidate?.gmail_thread_id || "").trim() || null,
    fecha_correo: String(candidate?.fecha_correo || "").trim() || null,
    pago_verificado: false,
    hora_recibido: String(candidate?.hora_recibido || "").trim() || null,
    raw_payload:
      candidate?.raw_payload && typeof candidate.raw_payload === "object"
        ? candidate.raw_payload
        : null,
    estado: Number.isFinite(Number(candidate?.monto_usdt)) ? "pendiente" : "sin_monto",
  };

  const { data, error } = await supabaseAdmin
    .from(BINANCE_GMAIL_PAYMENTS_TABLE)
    .insert(payload)
    .select(
      "id, gmail_message_id, monto_usdt, referencia, pago_verificado, id_orden, id_usuario, estado, detalle_estado",
    )
    .maybeSingle();
  if (error) {
    if (
      isMissingTableError(error, BINANCE_GMAIL_PAYMENTS_TABLE) ||
      isMissingColumnError(error, "gmail_message_id")
    ) {
      const err = new Error(
        `Falta la tabla/columnas de ${BINANCE_GMAIL_PAYMENTS_TABLE}. Ejecuta el SQL de creación primero.`,
      );
      err.code = "BINANCE_GMAIL_TABLE_MISSING";
      throw err;
    }
    if (isUniqueViolationError(error, "gmail_message_id") || isUniqueViolationError(error, "hash")) {
      const duplicateRow = await findBinanceGmailPaymentByMessageId(messageId);
      return { stored: false, duplicate: true, row: duplicateRow };
    }
    throw error;
  }

  return { stored: true, duplicate: false, row: data || null };
};

const updateBinanceGmailPaymentById = async (rowId, patch = {}) => {
  const id = toPositiveInt(rowId);
  if (!id || !patch || typeof patch !== "object" || !Object.keys(patch).length) return;
  const payload = { ...patch };
  const { error } = await supabaseAdmin
    .from(BINANCE_GMAIL_PAYMENTS_TABLE)
    .update(payload)
    .eq("id", id);
  if (error) {
    if (
      isMissingTableError(error, BINANCE_GMAIL_PAYMENTS_TABLE) ||
      isMissingColumnError(error, "gmail_message_id")
    ) {
      const err = new Error(
        `Falta la tabla/columnas de ${BINANCE_GMAIL_PAYMENTS_TABLE}. Ejecuta el SQL de creación primero.`,
      );
      err.code = "BINANCE_GMAIL_TABLE_MISSING";
      throw err;
    }
    throw error;
  }
};

const autoMatchBinanceGmailPaymentAgainstOrders = async (paymentRow = {}) => {
  const monto = parseFlexibleMoneyNumber(paymentRow?.monto_usdt);
  if (!Number.isFinite(monto) || monto <= 0) {
    return { matched: false, reason: "monto_invalido" };
  }

  const metodoPagoIds = await resolveBinanceMetodoPagoIds();
  if (!metodoPagoIds.length) {
    return { matched: false, reason: "metodo_binance_no_configurado" };
  }

  const { data: pendingRows, error: pendingErr } = await supabaseAdmin
    .from("ordenes")
    .select(
      "id_orden, id_usuario, id_metodo_de_pago, total, monto_transferido, monto_usdt_objetivo, binance_decimal_slot, monto_objetivo_expira_en, fecha, hora_orden, marcado_pago, checkout_finalizado, pago_verificado, orden_cancelada",
    )
    .in("id_metodo_de_pago", metodoPagoIds)
    .eq("marcado_pago", true)
    .eq("checkout_finalizado", true)
    .eq("pago_verificado", false)
    .eq("orden_cancelada", false)
    .order("id_orden", { ascending: false })
    .limit(800);
  if (pendingErr) throw pendingErr;

  const orders = Array.isArray(pendingRows) ? pendingRows : [];
  const activeOrders = orders.filter((row) => isOrderInsideBinanceActiveWindow(row));
  const matchedOrders = activeOrders.filter((row) => {
    const targetAmount = resolveOrderUsdAmountForBinance(row);
    if (!Number.isFinite(targetAmount) || targetAmount <= 0) return false;
    return Math.abs(targetAmount - monto) <= BINANCE_GMAIL_AMOUNT_TOLERANCE;
  });

  if (!matchedOrders.length) {
    return { matched: false, reason: "no_order_match", metodo_pago_ids: metodoPagoIds };
  }
  if (matchedOrders.length > 1) {
    return {
      matched: false,
      reason: "ambiguous_amount",
      metodo_pago_ids: metodoPagoIds,
      candidate_orders: matchedOrders.map((row) => toPositiveInt(row?.id_orden)).filter(Boolean),
    };
  }

  const selected = matchedOrders[0];
  return {
    matched: true,
    id_orden: selected.id_orden,
    id_usuario: selected.id_usuario || null,
    monto_usdt_pago: Math.round(monto * 1000) / 1000,
    metodo_pago_ids: metodoPagoIds,
  };
};

const processBinanceGmailPaymentRow = async (row = {}) => {
  const rowId = toPositiveInt(row?.id);
  if (!rowId) return { processed: false, matched: false, reason: "invalid_row_id" };

  if (isTrue(row?.pago_verificado) && toPositiveInt(row?.id_orden)) {
    return {
      processed: false,
      matched: true,
      reason: "already_verified",
      id_orden: toPositiveInt(row?.id_orden),
    };
  }

  const match = await autoMatchBinanceGmailPaymentAgainstOrders(row);
  if (!match?.matched) {
    await updateBinanceGmailPaymentById(rowId, {
      estado: String(match?.reason || "sin_match").slice(0, 80),
      detalle_estado:
        match?.candidate_orders && Array.isArray(match.candidate_orders)
          ? `candidate_orders=${match.candidate_orders.join(",")}`
          : null,
      procesado_en: new Date().toISOString(),
    });
    return { processed: false, matched: false, reason: match?.reason || "sin_match" };
  }

  const processResult = await autoProcessMatchedOrder(
    {
      id_orden: match.id_orden,
      id_usuario: match.id_usuario,
    },
    {
      allowedMetodoPagoIds: match?.metodo_pago_ids || [],
      syncPagoMovil: false,
    },
  );
  const markedAsVerified =
    processResult?.processed === true ||
    processResult?.reason === "orden_ya_verificada" ||
    processResult?.reason === "orden_ya_procesada";
  await updateBinanceGmailPaymentById(rowId, {
    id_orden: toPositiveInt(match.id_orden),
    id_usuario: toPositiveInt(match.id_usuario),
    pago_verificado: markedAsVerified,
    estado: String(processResult?.reason || "orden_match").slice(0, 80),
    detalle_estado: processResult?.error
      ? String(processResult.error).slice(0, 280)
      : processResult?.processed
        ? "orden_procesada"
        : null,
    procesado_en: new Date().toISOString(),
  });
  return {
    processed: processResult?.processed === true,
    matched: true,
    reason: processResult?.reason || null,
    id_orden: toPositiveInt(match.id_orden),
  };
};

const syncBinanceGmailPayments = async ({
  maxResults = BINANCE_GMAIL_MAX_RESULTS,
  lookbackMinutes = BINANCE_GMAIL_LOOKBACK_MINUTES,
  source = "manual",
} = {}) => {
  if (binanceGmailSyncInProgress) {
    return { skipped: true, reason: "in_progress", ...binanceGmailSyncLastResult };
  }
  binanceGmailSyncInProgress = true;
  const summary = {
    fetched: 0,
    parsed: 0,
    stored: 0,
    duplicates: 0,
    matched: 0,
    processed: 0,
    skipped: 0,
    failed: 0,
    source,
  };

  try {
    const accessToken = await fetchGoogleAccessTokenWithRefreshToken();
    const max = Math.max(1, Math.min(100, Number(maxResults) || BINANCE_GMAIL_MAX_RESULTS));
    const lookback = Math.max(10, Number(lookbackMinutes) || BINANCE_GMAIL_LOOKBACK_MINUTES);
    const afterEpoch = Math.floor((Date.now() - lookback * 60 * 1000) / 1000);
    const queryParts = [BINANCE_GMAIL_QUERY, `after:${afterEpoch}`].filter(Boolean);
    const gmailQuery = queryParts.join(" ").trim();

    const listResp = await gmailApiRequest({
      accessToken,
      path: "/gmail/v1/users/me/messages",
      query: {
        maxResults: max,
        q: gmailQuery,
        labelIds: BINANCE_GMAIL_LABEL_IDS,
      },
    });
    const messages = Array.isArray(listResp?.messages) ? listResp.messages : [];
    summary.fetched = messages.length;

    for (const msg of messages) {
      const messageId = String(msg?.id || "").trim();
      if (!messageId) {
        summary.skipped += 1;
        continue;
      }
      try {
        const fullMessage = await gmailApiRequest({
          accessToken,
          path: `/gmail/v1/users/me/messages/${encodeURIComponent(messageId)}`,
          query: { format: "full" },
        });
        const candidate = buildBinanceGmailPaymentCandidate(fullMessage);
        if (!candidate) {
          summary.skipped += 1;
          continue;
        }
        summary.parsed += 1;
        const inserted = await insertBinanceGmailPayment(candidate);
        const row = inserted?.row || null;
        if (inserted?.duplicate) {
          summary.duplicates += 1;
        } else if (inserted?.stored) {
          summary.stored += 1;
        }

        if (!row?.id) {
          summary.skipped += 1;
          continue;
        }
        if (!Number.isFinite(parseFlexibleMoneyNumber(row?.monto_usdt))) {
          await updateBinanceGmailPaymentById(row.id, {
            estado: "sin_monto",
            detalle_estado: "No se pudo extraer monto del correo.",
            procesado_en: new Date().toISOString(),
          });
          summary.skipped += 1;
          continue;
        }

        const processResult = await processBinanceGmailPaymentRow(row);
        if (processResult?.matched) summary.matched += 1;
        if (processResult?.processed) summary.processed += 1;
        if (!processResult?.processed) summary.skipped += 1;
      } catch (itemErr) {
        summary.failed += 1;
        console.error("[binance:gmail] item error", {
          message_id: messageId,
          error: itemErr?.message || itemErr,
        });
      }
    }

    binanceGmailSyncLastError = null;
    binanceGmailSyncLastResult = { ...summary };
    return summary;
  } catch (err) {
    const code = String(err?.code || "");
    if (
      code === "BINANCE_GMAIL_OAUTH_CONFIG_MISSING" ||
      code === "BINANCE_GMAIL_REFRESH_TOKEN_MISSING" ||
      code === "BINANCE_GMAIL_ACCESS_TOKEN_FAILED"
    ) {
      const skippedSummary = {
        ...summary,
        skipped: summary.skipped + 1,
        reason: code,
      };
      binanceGmailSyncLastResult = skippedSummary;
      binanceGmailSyncLastError = err?.message || code;
      return skippedSummary;
    }
    binanceGmailSyncLastError = err?.message || "sync_error";
    throw err;
  } finally {
    binanceGmailSyncLastRunAt = new Date().toISOString();
    binanceGmailSyncInProgress = false;
  }
};

if (AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_ENABLED) {
  console.log(
    `[ordenes:auto-credit-sync] conciliación por pagos acreditados activa (cada ${Math.round(
      AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_INTERVAL_MS / 1000,
    )}s).`,
  );
  setInterval(() => {
    processCreditedPagoMovilOrders().catch((err) => {
      console.error("[ordenes:auto-credit-sync] worker error", err);
    });
  }, AUTO_CREDITED_PAGOMOVIL_ORDER_PROCESS_INTERVAL_MS);
  processCreditedPagoMovilOrders().catch((err) => {
    console.error("[ordenes:auto-credit-sync] worker init error", err);
  });
}

if (BINANCE_GMAIL_SYNC_ENABLED) {
  console.log(
    `[binance:gmail] worker activo (cada ${Math.round(
      BINANCE_GMAIL_SYNC_INTERVAL_MS / 1000,
    )}s, max ${BINANCE_GMAIL_MAX_RESULTS} correos).`,
  );
  setInterval(() => {
    syncBinanceGmailPayments({ source: "worker" }).catch((err) => {
      console.error("[binance:gmail] worker error", err);
    });
  }, BINANCE_GMAIL_SYNC_INTERVAL_MS);
  syncBinanceGmailPayments({ source: "worker_init" }).catch((err) => {
    console.error("[binance:gmail] worker init error", err);
  });
}

app.get("/api/admin/binance-gmail/oauth/url", async (req, res) => {
  try {
    const sessionUserId = await requireAdminSession(req);
    cleanupBinanceGmailOauthStates();
    const state = crypto.randomBytes(24).toString("hex");
    const redirectRaw = String(req.query?.redirect_url || "").trim();
    const redirectUrl = isAllowedPublicRedirectUrl(redirectRaw) ? redirectRaw : null;
    binanceGmailOauthStateMap.set(state, {
      createdAt: Date.now(),
      idUsuario: sessionUserId,
      redirectUrl,
    });
    const authUrl = buildBinanceGmailOauthUrl({ state });
    return res.json({
      ok: true,
      auth_url: authUrl,
      state,
      redirect_uri: GMAIL_OAUTH_REDIRECT_URI || null,
      scopes: GMAIL_OAUTH_SCOPES,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    if (err?.code === "BINANCE_GMAIL_OAUTH_CONFIG_MISSING") {
      return res.status(500).json({ error: err.message });
    }
    console.error("[binance-gmail/oauth/url] error", err);
    return res.status(500).json({ error: "No se pudo generar la URL OAuth de Gmail." });
  }
});

app.get("/api/admin/binance-gmail/oauth/callback", async (req, res) => {
  try {
    cleanupBinanceGmailOauthStates();
    const code = String(req.query?.code || "").trim();
    const state = String(req.query?.state || "").trim();
    const googleError = String(req.query?.error || "").trim();
    const stateMeta = state ? binanceGmailOauthStateMap.get(state) : null;
    if (!stateMeta || Date.now() - Number(stateMeta?.createdAt || 0) > BINANCE_GMAIL_OAUTH_STATE_TTL_MS) {
      if (state) binanceGmailOauthStateMap.delete(state);
      return res.status(400).json({ error: "State OAuth inválido o expirado." });
    }
    binanceGmailOauthStateMap.delete(state);

    if (googleError) {
      if (stateMeta?.redirectUrl) {
        const redirect = new URL(stateMeta.redirectUrl);
        redirect.searchParams.set("gmail_oauth", "error");
        redirect.searchParams.set("reason", googleError);
        return res.redirect(302, redirect.toString());
      }
      return res.status(400).json({ error: `OAuth cancelado: ${googleError}` });
    }
    if (!code) {
      return res.status(400).json({ error: "Falta code en callback OAuth." });
    }

    const tokens = await exchangeGoogleOauthCodeForTokens({ code });
    const refreshToken = String(tokens?.refresh_token || "").trim();
    if (refreshToken) {
      cachedGmailRefreshToken = refreshToken;
    }

    if (stateMeta?.redirectUrl) {
      const redirect = new URL(stateMeta.redirectUrl);
      redirect.searchParams.set("gmail_oauth", "ok");
      redirect.searchParams.set("refresh_token_received", refreshToken ? "1" : "0");
      if (!refreshToken) {
        redirect.searchParams.set("reason", "no_refresh_token");
      }
      return res.redirect(302, redirect.toString());
    }

    return res.json({
      ok: true,
      refresh_token_received: !!refreshToken,
      refresh_token: refreshToken || null,
      scope: String(tokens?.scope || "").trim() || null,
      token_type: String(tokens?.token_type || "").trim() || null,
      expires_in: Number(tokens?.expires_in || 0) || null,
      warning: refreshToken
        ? null
        : "Google no devolvió refresh_token. Revoca el acceso y vuelve a autorizar con prompt=consent.",
    });
  } catch (err) {
    if (err?.code === "BINANCE_GMAIL_OAUTH_EXCHANGE_FAILED") {
      return res.status(400).json({
        error: err?.message || "No se pudo intercambiar el code OAuth.",
        details: err?.details || null,
      });
    }
    if (err?.code === "BINANCE_GMAIL_OAUTH_CONFIG_MISSING") {
      return res.status(500).json({ error: err.message });
    }
    console.error("[binance-gmail/oauth/callback] error", err);
    return res.status(500).json({ error: "Error procesando callback OAuth de Gmail." });
  }
});

app.post("/api/bdv/notify", express.text({ type: "*/*", limit: "200kb" }), async (req, res) => {
  clearCorsHeaders(res);
  try {
    const auth = req.headers.authorization || "";
    const token = process.env.BDV_WEBHOOK_TOKEN || "";
    const expected = `Bearer ${token}`;
    if (!token || auth !== expected) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const rawText = typeof req.body === "string" ? req.body : "";
    if (!rawText || !rawText.trim()) {
      return res.status(400).json({ error: "Invalid payload" });
    }

    const appName = "BDV";
    const titulo = "BDV";
    const texto = rawText;
    const textoForParsing = normalizeNotificationTextForParsing(rawText);
    const fecha = new Date().toISOString();
    const dispositivo = "unknown";

    const hash = crypto
      .createHash("sha256")
      .update([appName, titulo, texto, fecha, dispositivo].join("|"))
      .digest("hex");

    const { data: exists, error: existsErr } = await supabaseAdmin
      .from("pagomoviles")
      .select("hash")
      .eq("hash", hash)
      .maybeSingle();
    if (existsErr) throw existsErr;
    if (exists?.hash) {
      return res.json({ ok: true, duplicado: true, motivo: "hash_duplicado" });
    }

    const montoMatch =
      textoForParsing.match(/Bs\.?\s*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]+)?)/i)?.[1] ||
      textoForParsing.match(/Bs\.?\s*([0-9]+(?:[.,][0-9]+)?)/i)?.[1] ||
      textoForParsing.match(/Bs\.?\s*(0[.,][0-9]+)/i)?.[1] ||
      null;
    const normalizeMonto = (val) => {
      if (!val) return null;
      const raw = String(val).trim();
      if (raw.includes(".") && raw.includes(",")) {
        return raw.replace(/\./g, "").replace(",", ".");
      }
      return raw.replace(",", ".");
    };
    const monto = normalizeMonto(montoMatch);
    const referenciaRaw = pickPrimaryReferenceCandidate(textoForParsing);
    const referenciaDetectada = normalizeReferenceDigits(referenciaRaw);
    const referenciaValor = referenciaDetectada.length >= 6 ? referenciaDetectada : null;

    if (referenciaValor) {
      const { data: referenciaExists, error: refExistsErr } = await supabaseAdmin
        .from("pagomoviles")
        .select("id, referencia")
        .eq("referencia", referenciaValor)
        .limit(1)
        .maybeSingle();
      if (refExistsErr) throw refExistsErr;
      if (referenciaExists?.id) {
        return res.json({
          ok: true,
          duplicado: true,
          motivo: "referencia_duplicada",
          referencia: referenciaValor,
          id_existente: referenciaExists.id,
        });
      }
    }

    const payloadPagoMovil = {
      app: appName,
      titulo,
      texto,
      fecha,
      dispositivo,
      hash,
      monto_bs: monto,
      referencia: referenciaValor,
    };
    const { data: insertedPagoMovil, error: insErr } = await supabaseAdmin
      .from("pagomoviles")
      .insert(payloadPagoMovil)
      .select("id, referencia, texto, monto_bs")
      .single();
    if (insErr) {
      if (isUniqueViolationError(insErr, "referencia")) {
        return res.json({
          ok: true,
          duplicado: true,
          motivo: "referencia_duplicada",
          referencia: referenciaValor,
        });
      }
      if (isUniqueViolationError(insErr, "hash")) {
        return res.json({ ok: true, duplicado: true, motivo: "hash_duplicado" });
      }
      throw insErr;
    }

    let matchResult = { matched: false, reason: "not_checked" };
    let processResult = { processed: false, reason: "not_attempted" };
    try {
      matchResult = await autoMatchPagoMovilAgainstOrders(insertedPagoMovil || {});
      if (matchResult?.matched) {
        console.log("[bdv/notify] pago conciliado con orden", matchResult);
        processResult = await autoProcessMatchedOrder(matchResult);
        console.log("[bdv/notify] resultado procesamiento orden", processResult);
      } else {
        console.log("[bdv/notify] pago recibido sin orden coincidente", {
          pago_id: insertedPagoMovil?.id || null,
          reason: matchResult?.reason || "unknown",
        });
      }
    } catch (matchErr) {
      console.error("[bdv/notify] conciliacion automatica error", matchErr);
      processResult = {
        processed: false,
        reason: "auto_process_error",
        error: matchErr?.message || String(matchErr),
      };
    }

    return res.json({
      ok: true,
      duplicado: false,
      coincidencia_orden: matchResult?.matched === true,
      id_orden: matchResult?.id_orden || null,
      orden_procesada: processResult?.processed === true,
      razon_orden: processResult?.reason || null,
      detalle_error_orden: processResult?.error || null,
    });
  } catch (err) {
    console.error("bdv notify error", err);
    return res.status(500).json({ error: "Internal error" });
  }
});

app.use(jsonParser);

app.get("/api/admin/binance-gmail/status", async (req, res) => {
  try {
    const hasWorkerToken = hasValidInternalWorkerTriggerToken(req);
    if (!hasWorkerToken) {
      await requireAdminSession(req);
    }
    const metodoIds = await resolveBinanceMetodoPagoIds();
    return res.json({
      ok: true,
      enabled: BINANCE_GMAIL_SYNC_ENABLED,
      inProgress: binanceGmailSyncInProgress,
      lastRunAt: binanceGmailSyncLastRunAt,
      lastResult: binanceGmailSyncLastResult,
      lastError: binanceGmailSyncLastError,
      workerIntervalMs: BINANCE_GMAIL_SYNC_INTERVAL_MS,
      oauthConfigured:
        !!GMAIL_OAUTH_CLIENT_ID && !!GMAIL_OAUTH_CLIENT_SECRET && !!GMAIL_OAUTH_REDIRECT_URI,
      hasRefreshToken: !!getConfiguredGmailRefreshToken(),
      redirectUri: GMAIL_OAUTH_REDIRECT_URI || null,
      scopes: GMAIL_OAUTH_SCOPES,
      query: BINANCE_GMAIL_QUERY,
      lookbackMinutes: BINANCE_GMAIL_LOOKBACK_MINUTES,
      maxResults: BINANCE_GMAIL_MAX_RESULTS,
      amountTolerance: BINANCE_GMAIL_AMOUNT_TOLERANCE,
      orderWindowMs: BINANCE_GMAIL_ORDER_WINDOW_MS,
      binanceMetodoPagoIds: metodoIds,
      usingWorkerToken: hasWorkerToken,
      workerTokenConfigured: INTERNAL_WORKER_TRIGGER_TOKEN.length > 0,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[binance-gmail/status] error", err);
    return res.status(500).json({ error: "No se pudo consultar estado de Binance Gmail." });
  }
});

app.post("/api/admin/binance-gmail/sync", async (req, res) => {
  try {
    const hasWorkerToken = hasValidInternalWorkerTriggerToken(req);
    if (!hasWorkerToken) {
      await requireAdminSession(req);
    }
    const maxResults = Number(req.body?.max_results || req.body?.maxResults || BINANCE_GMAIL_MAX_RESULTS);
    const lookbackMinutes = Number(
      req.body?.lookback_minutes || req.body?.lookbackMinutes || BINANCE_GMAIL_LOOKBACK_MINUTES,
    );
    const source = String(req.body?.source || (hasWorkerToken ? "worker_token" : "admin_manual"))
      .trim()
      .slice(0, 80);
    const result = await syncBinanceGmailPayments({
      maxResults,
      lookbackMinutes,
      source: source || "manual",
    });
    return res.json({
      ok: true,
      usingWorkerToken: hasWorkerToken,
      ...result,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    if (err?.code === "BINANCE_GMAIL_TABLE_MISSING") {
      return res.status(400).json({ error: err.message });
    }
    if (err?.code === "BINANCE_GMAIL_OAUTH_CONFIG_MISSING") {
      return res.status(500).json({ error: err.message });
    }
    if (err?.code === "BINANCE_GMAIL_REFRESH_TOKEN_MISSING") {
      return res.status(400).json({ error: err.message });
    }
    console.error("[binance-gmail/sync] error", err);
    return res.status(500).json({ error: "No se pudo sincronizar correos Gmail de Binance." });
  }
});

app.get("/api/web-push/status", async (req, res) => {
  try {
    const idUsuario = requireSessionUserId(req);
    const { count, error } = await supabaseAdmin
      .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
      .select("id_subscription", { count: "exact", head: true })
      .eq("id_usuario", idUsuario)
      .is("disabled_at", null);
    if (error) throw error;

    return res.json({
      enabled: WEB_PUSH_ENABLED && WEB_PUSH_IS_CONFIGURED,
      configured: WEB_PUSH_IS_CONFIGURED,
      workerEnabled: WEB_PUSH_QUEUE_WORKER_ENABLED && WEB_PUSH_IS_CONFIGURED,
      deviceCount: Number(count || 0),
      lastRunAt: webPushQueueLastRunAt,
      lastResult: webPushQueueLastResult,
      lastError: webPushQueueLastError,
      tableMissing: webPushQueueTableMissing,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (String(err?.code || "") === "42P01") {
      return res.status(503).json({ error: "Tablas de web push aún no creadas." });
    }
    console.error("[web-push/status] error", err);
    return res.status(500).json({ error: "No se pudo consultar el estado de web push." });
  }
});

app.get("/api/web-push/public-key", (req, res) => {
  try {
    requireSessionUserId(req);
    return res.json({
      enabled: WEB_PUSH_ENABLED && WEB_PUSH_IS_CONFIGURED,
      publicKey: WEB_PUSH_IS_CONFIGURED ? String(webPushVapidPublicKey || "").trim() : null,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    return res.status(500).json({ error: "No se pudo leer la clave pública." });
  }
});

app.post("/api/web-push/subscribe", async (req, res) => {
  try {
    if (!WEB_PUSH_ENABLED) {
      return res.status(503).json({ error: "Web push está deshabilitado." });
    }
    if (!WEB_PUSH_IS_CONFIGURED) {
      return res.status(503).json({ error: "Faltan claves VAPID en el backend." });
    }

    const idUsuario = requireSessionUserId(req);
    const subscription = parseWebPushSubscriptionInput(req.body || {});
    const payload = {
      id_usuario: idUsuario,
      endpoint: subscription.endpoint,
      p256dh: subscription.p256dh,
      auth: subscription.auth,
      expiration_time: subscription.expirationTime,
      user_agent: subscription.userAgent || trimWebPushText(req.get("user-agent") || "", 1000) || null,
      device_label: subscription.deviceLabel,
      last_seen_at: new Date().toISOString(),
      disabled_at: null,
    };

    const { data: savedRow, error } = await supabaseAdmin
      .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
      .upsert(payload, { onConflict: "endpoint" })
      .select("id_subscription")
      .maybeSingle();
    if (error) throw error;

    const { count, error: countErr } = await supabaseAdmin
      .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
      .select("id_subscription", { count: "exact", head: true })
      .eq("id_usuario", idUsuario)
      .is("disabled_at", null);
    if (countErr) throw countErr;

    return res.json({
      ok: true,
      id_subscription: Number(savedRow?.id_subscription || 0) || null,
      deviceCount: Number(count || 0),
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "INVALID_PUSH_SUBSCRIPTION") {
      return res.status(400).json({ error: err.message });
    }
    if (String(err?.code || "") === "42P01") {
      return res.status(503).json({ error: "Tablas de web push aún no creadas." });
    }
    console.error("[web-push/subscribe] error", err);
    return res.status(500).json({ error: "No se pudo registrar el dispositivo." });
  }
});

app.delete("/api/web-push/subscribe", async (req, res) => {
  try {
    const idUsuario = requireSessionUserId(req);
    const endpoint = trimWebPushText(req.body?.endpoint || "", 3000);
    if (!endpoint) {
      return res.status(400).json({ error: "Endpoint requerido." });
    }

    const { error } = await supabaseAdmin
      .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
      .delete()
      .eq("id_usuario", idUsuario)
      .eq("endpoint", endpoint);
    if (error) throw error;

    const { count, error: countErr } = await supabaseAdmin
      .from(WEB_PUSH_SUBSCRIPTIONS_TABLE)
      .select("id_subscription", { count: "exact", head: true })
      .eq("id_usuario", idUsuario)
      .is("disabled_at", null);
    if (countErr) throw countErr;

    return res.json({ ok: true, deviceCount: Number(count || 0) });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (String(err?.code || "") === "42P01") {
      return res.status(503).json({ error: "Tablas de web push aún no creadas." });
    }
    console.error("[web-push/unsubscribe] error", err);
    return res.status(500).json({ error: "No se pudo desregistrar el dispositivo." });
  }
});

app.post("/api/web-push/test", async (req, res) => {
  try {
    if (!WEB_PUSH_ENABLED) {
      return res.status(503).json({ error: "Web push está deshabilitado." });
    }
    if (!WEB_PUSH_IS_CONFIGURED) {
      return res.status(503).json({ error: "Faltan claves VAPID en el backend." });
    }

    const idUsuario = requireSessionUserId(req);
    const title = trimWebPushText(req.body?.title || "Notificaciones activadas", 120);
    const body =
      trimWebPushText(
        req.body?.body || "Este dispositivo ya puede recibir notificaciones de Moose+.",
        400,
      ) || "Este dispositivo ya puede recibir notificaciones de Moose+.";
    const result = await sendWebPushPayloadToUserSubscriptions(
      idUsuario,
      buildWebPushPayload(
        { titulo: title || "Notificaciones activadas", mensaje: body },
        { url: buildWebPushTargetUrl("/notificaciones.html") },
      ),
    );

    if (!result.total) {
      return res.status(404).json({ error: "No hay dispositivos registrados para este usuario." });
    }

    return res.json({ ok: true, ...result });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (String(err?.code || "") === "42P01") {
      return res.status(503).json({ error: "Tablas de web push aún no creadas." });
    }
    console.error("[web-push/test] error", err);
    return res.status(500).json({ error: "No se pudo enviar la notificación de prueba." });
  }
});

app.post("/api/web-push/process-queue", async (req, res) => {
  try {
    await requireAdminSession(req);
    const result = await processWebPushDeliveryQueue();
    return res.json({
      ok: true,
      result,
      lastRunAt: webPushQueueLastRunAt,
      lastError: webPushQueueLastError,
      tableMissing: webPushQueueTableMissing,
    });
  } catch (err) {
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Acceso denegado" });
    }
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    console.error("[web-push/process-queue] error", err);
    return res.status(500).json({ error: "No se pudo procesar la cola de web push." });
  }
});

app.get("/api/sandbox/giftcards/catalog", async (req, res) => {
  try {
    await requireAdminSession(req);

    const [platformsRes, pricesRes] = await Promise.all([
      supabaseAdmin
        .from("plataformas")
        .select("id_plataforma, nombre, color_1, tarjeta_de_regalo")
        .eq("tarjeta_de_regalo", true)
        .order("nombre", { ascending: true }),
      supabaseAdmin
        .from("precios")
        .select(
          "id_precio, id_plataforma, precio_usd_detal, precio_usd_mayor, valor_tarjeta_de_regalo, moneda, region, plan",
        )
        .not("valor_tarjeta_de_regalo", "is", null)
        .order("id_precio", { ascending: true }),
    ]);
    if (platformsRes.error) throw platformsRes.error;
    if (pricesRes.error) throw pricesRes.error;

    return res.json({
      platforms: platformsRes.data || [],
      prices: pricesRes.data || [],
    });
  } catch (err) {
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Acceso denegado" });
    }
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    console.error("[sandbox/giftcards/catalog] error", err);
    return res.status(500).json({ error: "No se pudo cargar el catálogo sandbox." });
  }
});

app.get("/api/sandbox/giftcards/recent", async (req, res) => {
  try {
    await requireAdminSession(req);
    const limit = Math.max(1, Math.min(30, Number(req.query?.limit) || 15));

    const { data: orders, error: ordersErr } = await supabaseAdmin
      .from(SANDBOX_GIFTCARD_ORDERS_TABLE)
      .select(
        `
          id_sandbox_order,
          id_usuario_cliente,
          id_usuario_admin,
          id_plataforma,
          id_precio,
          cantidad,
          referencia,
          estado,
          total_usd,
          total_bs,
          tasa_bs,
          valor_tarjeta_de_regalo,
          moneda,
          notas,
          payload,
          creado_en,
          cliente:usuarios!sandbox_giftcard_orders_id_usuario_cliente_fkey(id_usuario, nombre, apellido),
          admin:usuarios!sandbox_giftcard_orders_id_usuario_admin_fkey(id_usuario, nombre, apellido),
          plataforma:plataformas!sandbox_giftcard_orders_id_plataforma_fkey(id_plataforma, nombre)
        `,
      )
      .order("id_sandbox_order", { ascending: false })
      .limit(limit);
    if (ordersErr) throw ordersErr;

    const orderIds = uniqPositiveIds((orders || []).map((row) => row.id_sandbox_order));
    if (!orderIds.length) {
      return res.json({ orders: [] });
    }

    const [itemsRes, historyRes] = await Promise.all([
      supabaseAdmin
        .from(SANDBOX_GIFTCARD_ORDER_ITEMS_TABLE)
        .select(
          "id_sandbox_item, id_sandbox_order, id_plataforma, id_precio, cantidad, precio_unitario_usd, total_usd, valor_tarjeta_de_regalo, moneda, region, detalle, payload, creado_en",
        )
        .in("id_sandbox_order", orderIds)
        .order("id_sandbox_item", { ascending: true }),
      supabaseAdmin
        .from(SANDBOX_GIFTCARD_HISTORY_TABLE)
        .select(
          "id_sandbox_historial, id_sandbox_order, id_usuario_cliente, id_usuario_admin, id_plataforma, monto_usd, monto_bs, referencia, venta_cliente, renovacion, detalle, payload, creado_en",
        )
        .in("id_sandbox_order", orderIds)
        .order("id_sandbox_historial", { ascending: true }),
    ]);
    if (itemsRes.error) throw itemsRes.error;
    if (historyRes.error) throw historyRes.error;

    const itemsByOrder = new Map();
    (itemsRes.data || []).forEach((row) => {
      const key = Number(row.id_sandbox_order);
      if (!itemsByOrder.has(key)) itemsByOrder.set(key, []);
      itemsByOrder.get(key).push(row);
    });
    const historyByOrder = new Map();
    (historyRes.data || []).forEach((row) => {
      const key = Number(row.id_sandbox_order);
      if (!historyByOrder.has(key)) historyByOrder.set(key, []);
      historyByOrder.get(key).push(row);
    });

    return res.json({
      orders: (orders || []).map((order) => ({
        ...order,
        items: itemsByOrder.get(Number(order.id_sandbox_order)) || [],
        history: historyByOrder.get(Number(order.id_sandbox_order)) || [],
      })),
    });
  } catch (err) {
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Acceso denegado" });
    }
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (String(err?.code || "") === "42P01") {
      return res.status(503).json({ error: "Las tablas sandbox aún no existen." });
    }
    console.error("[sandbox/giftcards/recent] error", err);
    return res.status(500).json({ error: "No se pudieron cargar las simulaciones." });
  }
});

app.post("/api/sandbox/giftcards/simulate-sale", async (req, res) => {
  const round2 = (value) => Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;
  const round6 = (value) => Math.round((Number(value || 0) + Number.EPSILON) * 1_000_000) / 1_000_000;

  try {
    await requireAdminSession(req);
    const idUsuarioAdmin = requireSessionUserId(req);
    const body = req.body && typeof req.body === "object" ? req.body : {};

    const idUsuarioCliente = toPositiveInt(body.id_usuario_cliente);
    const idPlataforma = toPositiveInt(body.id_plataforma);
    const idPrecio = toPositiveInt(body.id_precio);
    const cantidad = Math.max(1, Math.min(500, toPositiveInt(body.cantidad) || 1));
    const referencia = trimWebPushText(body.referencia || "", 120) || null;
    const notas = trimWebPushText(body.notas || "", 2000) || null;
    const tasaBsRaw = Number(body.tasa_bs);
    const tasaBs = Number.isFinite(tasaBsRaw) && tasaBsRaw > 0 ? round6(tasaBsRaw) : null;
    const unitUsdOverrideRaw = Number(body.precio_unitario_usd);
    const unitUsdOverride =
      Number.isFinite(unitUsdOverrideRaw) && unitUsdOverrideRaw >= 0 ? round2(unitUsdOverrideRaw) : null;
    const totalUsdOverrideRaw = Number(body.total_usd);
    const totalUsdOverride =
      Number.isFinite(totalUsdOverrideRaw) && totalUsdOverrideRaw >= 0 ? round2(totalUsdOverrideRaw) : null;
    const totalBsOverrideRaw = Number(body.total_bs);
    const totalBsOverride =
      Number.isFinite(totalBsOverrideRaw) && totalBsOverrideRaw >= 0 ? round2(totalBsOverrideRaw) : null;

    if (!idPlataforma || !idPrecio) {
      return res.status(400).json({ error: "id_plataforma e id_precio son obligatorios." });
    }

    const [platformRes, priceRes, userRes] = await Promise.all([
      supabaseAdmin
        .from("plataformas")
        .select("id_plataforma, nombre, tarjeta_de_regalo")
        .eq("id_plataforma", idPlataforma)
        .maybeSingle(),
      supabaseAdmin
        .from("precios")
        .select(
          "id_precio, id_plataforma, precio_usd_detal, precio_usd_mayor, valor_tarjeta_de_regalo, moneda, region, plan",
        )
        .eq("id_precio", idPrecio)
        .maybeSingle(),
      idUsuarioCliente
        ? supabaseAdmin
            .from("usuarios")
            .select("id_usuario, nombre, apellido, correo")
            .eq("id_usuario", idUsuarioCliente)
            .maybeSingle()
        : Promise.resolve({ data: null, error: null }),
    ]);
    if (platformRes.error) throw platformRes.error;
    if (priceRes.error) throw priceRes.error;
    if (userRes.error) throw userRes.error;

    const platformRow = platformRes.data;
    const priceRow = priceRes.data;
    if (!platformRow) {
      return res.status(404).json({ error: "Plataforma no encontrada." });
    }
    if (!priceRow) {
      return res.status(404).json({ error: "Precio no encontrado." });
    }
    if (!isTrue(platformRow?.tarjeta_de_regalo)) {
      return res.status(400).json({ error: "La plataforma seleccionada no es una gift card." });
    }
    if (Number(priceRow.id_plataforma) !== Number(idPlataforma)) {
      return res.status(400).json({ error: "El precio no pertenece a la plataforma seleccionada." });
    }
    if (idUsuarioCliente && !userRes.data?.id_usuario) {
      return res.status(404).json({ error: "Cliente no encontrado." });
    }

    const unitUsd =
      unitUsdOverride ??
      round2(
        Number.isFinite(Number(priceRow.precio_usd_detal))
          ? Number(priceRow.precio_usd_detal)
          : Number(priceRow.precio_usd_mayor) || 0,
      );
    const totalUsd = totalUsdOverride ?? round2(unitUsd * cantidad);
    const totalBs = totalBsOverride ?? (tasaBs ? round2(totalUsd * tasaBs) : null);
    const valorTarjeta =
      trimWebPushText(body.valor_tarjeta_de_regalo || priceRow.valor_tarjeta_de_regalo || "", 80) ||
      null;
    const moneda = trimWebPushText(body.moneda || priceRow.moneda || "", 20) || null;
    const detalle = [
      platformRow.nombre || `Plataforma ${idPlataforma}`,
      valorTarjeta ? `Valor: ${valorTarjeta}${moneda ? ` ${moneda}` : ""}` : "",
      referencia ? `Ref: ${referencia}` : "",
    ]
      .filter(Boolean)
      .join(" | ");

    const orderPayload = {
      id_usuario_cliente: idUsuarioCliente || null,
      id_usuario_admin: idUsuarioAdmin,
      id_plataforma: idPlataforma,
      id_precio: idPrecio,
      cantidad,
      referencia,
      estado: "simulada",
      total_usd: totalUsd,
      total_bs: totalBs,
      tasa_bs: tasaBs,
      valor_tarjeta_de_regalo: valorTarjeta,
      moneda,
      notas,
      payload: {
        source: "sandbox-giftcards",
        platform_name: platformRow.nombre || null,
        price: priceRow,
        client: userRes.data || null,
        request: body,
      },
    };

    const { data: insertedOrder, error: orderErr } = await supabaseAdmin
      .from(SANDBOX_GIFTCARD_ORDERS_TABLE)
      .insert(orderPayload)
      .select("*")
      .maybeSingle();
    if (orderErr) throw orderErr;

    const itemPayload = {
      id_sandbox_order: insertedOrder.id_sandbox_order,
      id_plataforma: idPlataforma,
      id_precio: idPrecio,
      cantidad,
      precio_unitario_usd: unitUsd,
      total_usd: totalUsd,
      valor_tarjeta_de_regalo: valorTarjeta,
      moneda,
      region: trimWebPushText(body.region || priceRow.region || "", 80) || null,
      detalle,
      payload: {
        plan: priceRow.plan || null,
        request: body,
      },
    };
    const { data: insertedItem, error: itemErr } = await supabaseAdmin
      .from(SANDBOX_GIFTCARD_ORDER_ITEMS_TABLE)
      .insert(itemPayload)
      .select("*")
      .maybeSingle();
    if (itemErr) throw itemErr;

    const historyPayload = {
      id_sandbox_order: insertedOrder.id_sandbox_order,
      id_usuario_cliente: idUsuarioCliente || null,
      id_usuario_admin: idUsuarioAdmin,
      id_plataforma: idPlataforma,
      monto_usd: totalUsd,
      monto_bs: totalBs,
      referencia,
      venta_cliente: true,
      renovacion: false,
      detalle,
      payload: {
        order: insertedOrder,
        item: insertedItem,
      },
    };
    const { data: insertedHistory, error: historyErr } = await supabaseAdmin
      .from(SANDBOX_GIFTCARD_HISTORY_TABLE)
      .insert(historyPayload)
      .select("*")
      .maybeSingle();
    if (historyErr) throw historyErr;

    return res.json({
      ok: true,
      order: insertedOrder,
      item: insertedItem,
      history: insertedHistory,
    });
  } catch (err) {
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Acceso denegado" });
    }
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (String(err?.code || "") === "42P01") {
      return res.status(503).json({ error: "Las tablas sandbox aún no existen." });
    }
    console.error("[sandbox/giftcards/simulate-sale] error", err);
    return res.status(500).json({ error: err?.message || "No se pudo guardar la simulación." });
  }
});

app.get("/api/precios-especiales/me", async (req, res) => {
  try {
    const idUsuario = await getSessionUsuario(req);
    const rows = await listUserSpecialPrices({ idUsuario });
    return res.json({
      items: (rows || []).map((row) => ({
        id: row.id,
        id_precio: row.id_precio,
        monto: row.monto,
      })),
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    console.error("[precios-especiales/me] error", err);
    return res.status(500).json({ error: "No se pudieron cargar los precios especiales." });
  }
});

app.get("/api/admin/precios-especiales/clientes", async (req, res) => {
  try {
    await requireSuperadminSession(req);
    const query = String(req.query?.q || "").trim();
    const limitRaw = Number(req.query?.limit);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(80, Math.trunc(limitRaw))) : 30;

    let usersQuery = supabaseAdmin
      .from("usuarios")
      .select("id_usuario, nombre, apellido, correo, acceso_cliente")
      .order("nombre", { ascending: true })
      .limit(limit);
    if (query) {
      const like = `%${query}%`;
      usersQuery = usersQuery.or(
        `nombre.ilike.${like},apellido.ilike.${like},correo.ilike.${like},id_usuario.eq.${toPositiveInt(query) || 0}`,
      );
    }
    const { data, error } = await usersQuery;
    if (error) throw error;

    return res.json({
      items: (data || []).map((row) => ({
        id_usuario: toPositiveInt(row?.id_usuario) || null,
        nombre: String(row?.nombre || "").trim(),
        apellido: String(row?.apellido || "").trim(),
        correo: String(row?.correo || "").trim(),
        acceso_cliente: row?.acceso_cliente,
      })),
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === SUPERADMIN_REQUIRED || err?.message === SUPERADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo superadmin" });
    }
    console.error("[admin/precios-especiales/clientes] error", err);
    return res.status(500).json({ error: "No se pudo buscar clientes." });
  }
});

app.get("/api/admin/precios-especiales", async (req, res) => {
  try {
    await requireSuperadminSession(req);
    const idUsuario = toPositiveInt(req.query?.id_usuario);
    if (!idUsuario) {
      return res.status(400).json({ error: "id_usuario inválido." });
    }

    const { data: userRow, error: userErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, nombre, apellido, correo, acceso_cliente")
      .eq("id_usuario", idUsuario)
      .maybeSingle();
    if (userErr) throw userErr;
    if (!userRow) {
      return res.status(404).json({ error: "Usuario no encontrado." });
    }

    const isMayorista =
      userRow?.acceso_cliente === false ||
      userRow?.acceso_cliente === "false" ||
      userRow?.acceso_cliente === 0 ||
      userRow?.acceso_cliente === "0";

    const [pricesRes, specialRows] = await Promise.all([
      supabaseAdmin
        .from("precios")
        .select(
          "id_precio, id_plataforma, plan, cantidad, completa, sub_cuenta, precio_usd_detal, precio_usd_mayor",
        )
        .order("id_plataforma", { ascending: true })
        .order("id_precio", { ascending: true }),
      listUserSpecialPrices({ idUsuario }),
    ]);
    if (pricesRes.error) throw pricesRes.error;

    const prices = pricesRes.data || [];
    const platformIds = uniqPositiveIds(prices.map((row) => row?.id_plataforma));
    const platformRes = platformIds.length
      ? await supabaseAdmin
          .from("plataformas")
          .select("id_plataforma, nombre, por_pantalla, por_acceso")
          .in("id_plataforma", platformIds)
      : { data: [], error: null };
    const { data: platforms, error: platErr } = platformRes;
    if (platErr) throw platErr;
    const platformById = (platforms || []).reduce((acc, row) => {
      const platformId = toPositiveInt(row?.id_plataforma);
      if (!platformId) return acc;
      acc[platformId] = row;
      return acc;
    }, {});
    const specialLookup = buildSpecialPriceLookup(specialRows || []);

    const items = prices.map((price) => {
      const idPrecio = toPositiveInt(price?.id_precio);
      const idPlataforma = toPositiveInt(price?.id_plataforma);
      const platformInfo = platformById[idPlataforma] || {};
      const montoBaseRaw = isMayorista
        ? parseOptionalCheckoutNumber(price?.precio_usd_mayor)
        : parseOptionalCheckoutNumber(price?.precio_usd_detal);
      const montoBaseFallback = isMayorista
        ? parseOptionalCheckoutNumber(price?.precio_usd_detal)
        : parseOptionalCheckoutNumber(price?.precio_usd_mayor);
      const montoBase = Number.isFinite(montoBaseRaw)
        ? roundCheckoutMoney(montoBaseRaw)
        : Number.isFinite(montoBaseFallback)
          ? roundCheckoutMoney(montoBaseFallback)
          : 0;
      const specialRow = specialLookup.byPrecioId?.[idPrecio] || null;
      return {
        id_precio: idPrecio,
        id_plataforma: idPlataforma,
        plataforma: String(platformInfo?.nombre || `Plataforma ${idPlataforma || "-"}`).trim(),
        tipo: resolvePrecioTipoLabel(price, platformInfo),
        precio_base: montoBase,
        id_precio_especial: toPositiveInt(specialRow?.id) || null,
        precio_especial: Number.isFinite(parseOptionalCheckoutNumber(specialRow?.monto))
          ? roundCheckoutMoney(specialRow.monto)
          : null,
      };
    });

    return res.json({
      cliente: {
        id_usuario: toPositiveInt(userRow?.id_usuario) || null,
        nombre: String(userRow?.nombre || "").trim(),
        apellido: String(userRow?.apellido || "").trim(),
        correo: String(userRow?.correo || "").trim(),
        acceso_cliente: userRow?.acceso_cliente,
      },
      items,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === SUPERADMIN_REQUIRED || err?.message === SUPERADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo superadmin" });
    }
    console.error("[admin/precios-especiales:get] error", err);
    return res.status(500).json({ error: "No se pudieron cargar los precios especiales." });
  }
});

app.put("/api/admin/precios-especiales", async (req, res) => {
  try {
    await requireSuperadminSession(req);
    const idUsuario = toPositiveInt(req.body?.id_usuario);
    const idPrecio = toPositiveInt(req.body?.id_precio);
    if (!idUsuario || !idPrecio) {
      return res.status(400).json({ error: "id_usuario e id_precio son obligatorios." });
    }
    const montoRaw = String(req.body?.monto ?? "").trim().replace(",", ".");
    const montoParsed = montoRaw ? Number(montoRaw) : null;
    if (montoRaw && (!Number.isFinite(montoParsed) || montoParsed < 0)) {
      return res.status(400).json({ error: "monto inválido." });
    }

    const existingRows = await listUserSpecialPrices({
      idUsuario,
      idPrecios: [idPrecio],
    });
    const current = existingRows[0] || null;
    const duplicateIds = existingRows
      .slice(1)
      .map((row) => toPositiveInt(row?.id))
      .filter(Boolean);
    if (duplicateIds.length) {
      const { error: duplicateDelErr } = await supabaseAdmin
        .from("precios_especiales")
        .delete()
        .in("id", duplicateIds);
      if (duplicateDelErr) throw duplicateDelErr;
    }

    if (!Number.isFinite(montoParsed)) {
      if (current?.id) {
        const { error: delErr } = await supabaseAdmin
          .from("precios_especiales")
          .delete()
          .eq("id", current.id);
        if (delErr) throw delErr;
      }
      return res.json({
        ok: true,
        removed: true,
        id_usuario: idUsuario,
        id_precio: idPrecio,
      });
    }

    const monto = roundCheckoutMoney(montoParsed);
    if (current?.id) {
      const { data: updated, error: updErr } = await supabaseAdmin
        .from("precios_especiales")
        .update({ monto })
        .eq("id", current.id)
        .select("id, id_precio, id_usuario, monto")
        .single();
      if (updErr) throw updErr;
      const normalized = normalizeSpecialPriceRow(updated);
      return res.json({ ok: true, item: normalized });
    }

    const { data: inserted, error: insErr } = await supabaseAdmin
      .from("precios_especiales")
      .insert({
        id_usuario: idUsuario,
        id_precio: idPrecio,
        monto,
      })
      .select("id, id_precio, id_usuario, monto")
      .single();
    if (insErr) throw insErr;
    const normalized = normalizeSpecialPriceRow(inserted);
    return res.json({ ok: true, item: normalized });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === SUPERADMIN_REQUIRED || err?.message === SUPERADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo superadmin" });
    }
    console.error("[admin/precios-especiales:put] error", err);
    return res.status(500).json({ error: "No se pudo guardar el precio especial." });
  }
});

app.get("/api/whatsapp/status", (req, res) => {
  const qrState = getWhatsappQrState();
  const ready = isWhatsappReady();
  res.json({
    enabledByEnv: shouldStartWhatsapp,
    persistentWorkerEnabled: whatsappHetznerPersistentWorkerEnabled,
    persistentWorkerLoaded: whatsappHetznerPersistentWorkerLoaded,
    ready,
    active: isWhatsappClientActive(),
    booting: whatsappBootInProgress,
    qrRaw: ready ? null : qrState?.raw || null,
    qrUpdatedAt: qrState?.updatedAt || null,
  });
});

app.get("/api/whatsapp/persistent-worker", async (req, res) => {
  try {
    await requireSuperadminSession(req);
    const enabled = await loadWhatsappHetznerPersistentWorkerEnabled({ refresh: true });
    return res.json({
      ok: true,
      enabled,
      ready: isWhatsappReady(),
      active: isWhatsappClientActive(),
      booting: whatsappBootInProgress,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === SUPERADMIN_REQUIRED || err?.message === SUPERADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo superadmin" });
    }
    return res.status(500).json({
      error: err?.message || "No se pudo consultar el estado persistente de WhatsApp",
    });
  }
});

app.post("/api/whatsapp/persistent-worker", async (req, res) => {
  try {
    await requireSuperadminSession(req);
    const enabled = req.body?.enabled === true;
    await saveWhatsappHetznerPersistentWorkerEnabled(enabled);

    if (enabled) {
      await ensureWhatsappClientStarted({
        reason: "persistent_worker_toggle_on",
        allowWhenDisabled: true,
      });
    } else {
      await shutdownWhatsappClient({
        reason: "persistent_worker_toggle_off",
        allowWhenDisabled: true,
        force: true,
      });
    }

    return res.json({
      ok: true,
      enabled: whatsappHetznerPersistentWorkerEnabled,
      ready: isWhatsappReady(),
      active: isWhatsappClientActive(),
      booting: whatsappBootInProgress,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === SUPERADMIN_REQUIRED || err?.message === SUPERADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo superadmin" });
    }
    return res.status(500).json({
      error: err?.message || "No se pudo actualizar el modo persistente de WhatsApp",
    });
  }
});

app.get("/api/whatsapp/qr", async (req, res) => {
  try {
    await requireAdminSession(req);
    const autoStart = String(req.query?.start ?? "true").toLowerCase() !== "false";
    if (autoStart && !isWhatsappReady()) {
      await ensureWhatsappClientStarted({
        reason: "manual_qr_request",
        allowWhenDisabled: true,
        waitForInitialize: false,
      });
    }
    let qrState = getWhatsappQrState();
    let ready = isWhatsappReady();
    const hasQr = Boolean(String(qrState?.raw || "").trim());
    if (!ready && !hasQr && (whatsappBootInProgress || isWhatsappClientActive() || autoStart)) {
      const waitedState = await waitForWhatsappQrOrReady();
      qrState = waitedState.qrState;
      ready = waitedState.ready;
    }
    return res.json({
      ok: true,
      ready,
      active: isWhatsappClientActive(),
      booting: whatsappBootInProgress,
      qrRaw: ready ? null : qrState?.raw || null,
      qrUpdatedAt: qrState?.updatedAt || null,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    return res.status(500).json({ error: err?.message || "No se pudo consultar el QR de WhatsApp" });
  }
});

app.get("/api/whatsapp/recordatorios/auto-status", async (req, res) => {
  const { dateStr, weekday } = getCaracasClock();
  ensureDailyRecordatoriosState(dateStr);
  let pendingMessages = null;
  let pendingError = null;
  try {
    const pending = await buildWhatsappRecordatorioItems();
    pendingMessages = Array.isArray(pending) ? pending.length : 0;
  } catch (err) {
    pendingError = err?.message || "No se pudo calcular pendientes";
  }

  res.json({
    enabled: shouldStartWhatsapp && WHATSAPP_AUTO_RECORDATORIOS_ENABLED,
    schedule: {
      sendHour: getWhatsappAutoRecordatoriosHour({ weekday }),
      weekdayHour: WHATSAPP_AUTO_RECORDATORIOS_WEEKDAY_HOUR,
      weekendHour: WHATSAPP_AUTO_RECORDATORIOS_WEEKEND_HOUR,
      weekday,
      resetHour: WHATSAPP_RESET_HOUR,
      timezone: "America/Caracas",
    },
    recordatorios_enviados: recordatoriosEnviados,
    recordatorios_enviados_date: recordatoriosEnviadosDate,
    autoRecordatoriosRetryPending,
    autoRecordatoriosRunInProgress,
    recordatoriosSendInProgress,
    recordatoriosSendLock: getRecordatoriosSendLockState(),
    pendingMessages,
    pendingError,
    lastAutoRecordatoriosRunDate,
    lastAutoRecordatoriosState,
  });
});

app.get("/api/whatsapp/recordatorios/pendientes", async (req, res) => {
  try {
    await requireAdminSession(req);
    const includeCutoffToday =
      String(req.query?.includeCutoffToday || "")
        .trim()
        .toLowerCase() === "true";

    const pendingItems = await buildWhatsappRecordatorioItems();
    let cutoffTodayItems = [];
    let cutoffTodayPlatform12Items = [];
    if (includeCutoffToday) {
      try {
        cutoffTodayItems = await buildWhatsappRecordatorioItems({
          mode: "cutoff_today",
        });
        cutoffTodayPlatform12Items = buildWhatsappCutoffPlatform12Items(cutoffTodayItems);
      } catch (err) {
        if (!isMissingColumnError(err, "recordatorio_corte_enviado")) throw err;
        cutoffTodayItems = [];
        cutoffTodayPlatform12Items = [];
      }
    }

    const items = [...pendingItems, ...cutoffTodayItems, ...cutoffTodayPlatform12Items];
    const previewItems = buildWhatsappRecordatorioPreviewItems(items);
    return res.json({
      ok: true,
      total: items.length,
      sendable: countWhatsappSendableRecordatorios(items),
      items: previewItems,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[whatsapp/recordatorios/pendientes] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudieron calcular los recordatorios pendientes" });
  }
});

app.get("/api/whatsapp/recordatorios/pending-no-phone", async (req, res) => {
  try {
    const idUsuarioSesion = await getOrCreateUsuario(req);
    if (!idUsuarioSesion) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }

    const { data: permRow, error: permErr } = await supabaseAdmin
      .from("usuarios")
      .select("permiso_superadmin")
      .eq("id_usuario", idUsuarioSesion)
      .maybeSingle();
    if (permErr) throw permErr;
    const isSuper = isTrue(permRow?.permiso_superadmin);
    if (!isSuper) {
      return res.status(403).json({ error: "Solo superadmin" });
    }

    const clients = await buildWhatsappPendingNoPhoneClients();
    return res.json({
      total: clients.length,
      clients,
    });
  } catch (err) {
    console.error("[whatsapp/recordatorios/pending-no-phone] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    return res.status(500).json({
      error: err?.message || "No se pudo calcular clientes sin teléfono para recordatorios",
    });
  }
});

app.post("/api/whatsapp/reportes/notificar", async (req, res) => {
  try {
    let sessionUserId = null;
    try {
      sessionUserId = await getSessionUsuario(req);
    } catch (authErr) {
      if (authErr?.code !== AUTH_REQUIRED && authErr?.message !== AUTH_REQUIRED) throw authErr;
      const bearerToken = getBearerTokenFromRequest(req);
      if (!bearerToken) throw authErr;
      sessionUserId = await resolveUsuarioFromAuthToken(bearerToken);
    }
    const reportId = toPositiveInt(req.body?.id_reporte ?? req.body?.idReporte);
    if (!reportId) {
      return res.status(400).json({ error: "id_reporte invalido" });
    }

    const reporte = await fetchReporteWhatsappContextById(reportId);
    if (!reporte?.id_reporte) {
      return res.status(404).json({ error: "Reporte no encontrado" });
    }

    const reportOwnerId = toPositiveInt(reporte?.id_usuario);
    if (reportOwnerId && reportOwnerId !== sessionUserId) {
      const { data: permRow, error: permErr } = await supabaseAdmin
        .from("usuarios")
        .select("permiso_admin, permiso_superadmin")
        .eq("id_usuario", sessionUserId)
        .maybeSingle();
      if (permErr) throw permErr;
      const isAdmin = isTrue(permRow?.permiso_admin) || isTrue(permRow?.permiso_superadmin);
      if (!isAdmin) {
        return res.status(403).json({ error: "No autorizado para notificar este reporte" });
      }
    }

    const result = await sendReporteCreatedToWhatsappGroup({
      reporte,
      manageWhatsappLifecycle: true,
    });

    if (result?.sent) {
      try {
        await markReporteWhatsappEnviado(reportId, true);
      } catch (markErr) {
        if (isMissingColumnError(markErr, "enviado_whatsapp")) {
          return res.status(500).json({
            error: "Falta columna reportes.enviado_whatsapp. No se pudo marcar el reporte como enviado.",
            sent: true,
            markedSent: false,
            id_reporte: reportId,
          });
        }
        throw markErr;
      }
      return res.json({ ok: true, markedSent: true, ...result });
    }

    if (result?.skipped && result?.reason === "target_destinations_not_found") {
      return res.status(503).json({
        error:
          "No se encontró el grupo WhatsApp para reportes. Configura WHATSAPP_REPORTES_GROUP_CHAT_ID o WHATSAPP_REPORTES_GROUP_NAME.",
        ...result,
      });
    }
    if (result?.skipped) return res.status(202).json({ ok: false, ...result });

    return res.status(500).json({
      error: result?.error || "No se pudo enviar el reporte por WhatsApp",
      ...result,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    console.error("[whatsapp/reportes/notificar] error", err);
    return res.status(500).json({
      error: err?.message || "No se pudo notificar el reporte por WhatsApp",
    });
  }
});

app.post("/api/whatsapp/reportes/notificar-solucion", async (req, res) => {
  try {
    let sessionUserId = null;
    try {
      sessionUserId = await getSessionUsuario(req);
    } catch (authErr) {
      if (authErr?.code !== AUTH_REQUIRED && authErr?.message !== AUTH_REQUIRED) throw authErr;
      const bearerToken = getBearerTokenFromRequest(req);
      if (!bearerToken) throw authErr;
      sessionUserId = await resolveUsuarioFromAuthToken(bearerToken);
    }
    const reportId = toPositiveInt(req.body?.id_reporte ?? req.body?.idReporte);
    if (!reportId) {
      return res.status(400).json({ error: "id_reporte invalido" });
    }

    const reporte = await fetchReporteWhatsappContextById(reportId);
    if (!reporte?.id_reporte) {
      return res.status(404).json({ error: "Reporte no encontrado" });
    }

    const reportOwnerId = toPositiveInt(reporte?.id_usuario);
    if (reportOwnerId && reportOwnerId !== sessionUserId) {
      const { data: permRow, error: permErr } = await supabaseAdmin
        .from("usuarios")
        .select("permiso_admin, permiso_superadmin")
        .eq("id_usuario", sessionUserId)
        .maybeSingle();
      if (permErr) throw permErr;
      const isAdmin = isTrue(permRow?.permiso_admin) || isTrue(permRow?.permiso_superadmin);
      if (!isAdmin) {
        return res.status(403).json({ error: "No autorizado para notificar este reporte" });
      }
    }

    const result = await sendReporteSolvedToWhatsappOwner({
      reporte,
      manageWhatsappLifecycle: true,
    });

    if (result?.skipped) return res.status(202).json({ ok: false, ...result });
    if (result?.sent) return res.json({ ok: true, ...result });
    return res.status(500).json({
      error: result?.error || "No se pudo enviar el reporte solucionado por WhatsApp",
      ...result,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    console.error("[whatsapp/reportes/notificar-solucion] error", err);
    return res.status(500).json({
      error: err?.message || "No se pudo notificar la solución del reporte por WhatsApp",
    });
  }
});

app.post("/api/whatsapp/reportes/notificar-datos-incorrectos", async (req, res) => {
  try {
    let sessionUserId = null;
    try {
      sessionUserId = await getSessionUsuario(req);
    } catch (authErr) {
      if (authErr?.code !== AUTH_REQUIRED && authErr?.message !== AUTH_REQUIRED) throw authErr;
      const bearerToken = getBearerTokenFromRequest(req);
      if (!bearerToken) throw authErr;
      sessionUserId = await resolveUsuarioFromAuthToken(bearerToken);
    }
    const reportId = toPositiveInt(req.body?.id_reporte ?? req.body?.idReporte);
    if (!reportId) {
      return res.status(400).json({ error: "id_reporte invalido" });
    }
    const incluirCorreo = isTrue(req.body?.correo ?? req.body?.incluirCorreo);
    const incluirClave = isTrue(req.body?.clave ?? req.body?.incluirClave);
    if (!incluirCorreo && !incluirClave) {
      return res.status(400).json({ error: "Debe seleccionar Correo o Contraseña." });
    }
    const imagen = String(req.body?.imagen || "").trim();

    const reporte = await fetchReporteWhatsappContextById(reportId);
    if (!reporte?.id_reporte) {
      return res.status(404).json({ error: "Reporte no encontrado" });
    }

    const reportOwnerId = toPositiveInt(reporte?.id_usuario);
    if (reportOwnerId && reportOwnerId !== sessionUserId) {
      const { data: permRow, error: permErr } = await supabaseAdmin
        .from("usuarios")
        .select("permiso_admin, permiso_superadmin")
        .eq("id_usuario", sessionUserId)
        .maybeSingle();
      if (permErr) throw permErr;
      const isAdmin = isTrue(permRow?.permiso_admin) || isTrue(permRow?.permiso_superadmin);
      if (!isAdmin) {
        return res.status(403).json({ error: "No autorizado para notificar este reporte" });
      }
    }

    const result = await sendReporteIncorrectDataToWhatsappOwner({
      reporte,
      incluirCorreo,
      incluirClave,
      imagen,
      manageWhatsappLifecycle: true,
    });

    if (result?.skipped) return res.status(202).json({ ok: false, ...result });
    if (result?.sent) {
      let datosIncorrectosActualizado = true;
      let notificacionWebCreada = true;
      let warning = "";
      const { error: markDatosErr } = await supabaseAdmin
        .from("reportes")
        .update({ datos_incorrectos: true, datos_corregidos: false })
        .eq("id_reporte", reportId);

      if (markDatosErr && !isMissingColumnError(markDatosErr, "datos_incorrectos")) {
        console.error("[whatsapp/reportes/notificar-datos-incorrectos] mark datos_incorrectos error", {
          id_reporte: reportId,
          error: markDatosErr,
        });
        datosIncorrectosActualizado = false;
        warning =
          "Mensaje enviado, pero no se pudo actualizar el estado de datos incorrectos del reporte.";
      }

      if (markDatosErr && isMissingColumnError(markDatosErr, "datos_incorrectos")) {
        console.warn(
          "[whatsapp/reportes/notificar-datos-incorrectos] Falta columna reportes.datos_incorrectos. Continúa sin marcar estado.",
        );
        datosIncorrectosActualizado = false;
        warning = "Falta columna reportes.datos_incorrectos. No se pudo marcar el estado.";
      }

      try {
        await createWebNotificationForIncorrectData({
          idUsuario: result?.id_usuario_destino || reporte?.id_usuario,
          idCuenta: reporte?.id_cuenta,
          titulo: "Datos incorrectos",
          incluirCorreo,
          incluirClave,
          idVenta: reporte?.id_venta,
        });
      } catch (notifErr) {
        console.error("[whatsapp/reportes/notificar-datos-incorrectos] web notification error", {
          id_reporte: reportId,
          error: notifErr,
        });
        notificacionWebCreada = false;
        warning = warning
          ? `${warning} No se pudo crear la notificación web.`
          : "Mensaje enviado, pero no se pudo crear la notificación web.";
      }

      return res.json({
        ok: true,
        ...result,
        datos_incorrectos_actualizado: datosIncorrectosActualizado,
        notificacion_web_creada: notificacionWebCreada,
        warning: warning || undefined,
      });
    }
    return res.status(500).json({
      error: result?.error || "No se pudo enviar la alerta de datos incorrectos por WhatsApp",
      ...result,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    console.error("[whatsapp/reportes/notificar-datos-incorrectos] error", err);
    return res.status(500).json({
      error: err?.message || "No se pudo notificar datos incorrectos por WhatsApp",
    });
  }
});

app.post("/api/whatsapp/ventas/notificar-reporte-admin", async (req, res) => {
  try {
    await requireAdminSession(req);
    const ventaId = toPositiveInt(req.body?.id_venta ?? req.body?.idVenta);
    if (!ventaId) {
      return res.status(400).json({ error: "id_venta inválido" });
    }
    const incluirCorreo = isTrue(req.body?.correo ?? req.body?.incluirCorreo);
    const incluirClave = isTrue(req.body?.clave ?? req.body?.incluirClave);
    const reportMode =
      String(req.body?.modo ?? req.body?.mode ?? "datos_incorrectos")
        .trim()
        .toLowerCase() === "problemas_correo"
        ? "problemas_correo"
        : "datos_incorrectos";
    if (reportMode === "datos_incorrectos" && !incluirCorreo && !incluirClave) {
      return res.status(400).json({ error: "Debe seleccionar Correo o Contraseña." });
    }
    const imagen = String(req.body?.imagen || "").trim();

    const result = await sendVentaAdminReportToWhatsappOwner({
      idVenta: ventaId,
      incluirCorreo,
      incluirClave,
      imagen,
      modo: reportMode,
      manageWhatsappLifecycle: true,
    });
    if (result?.sent) {
      let datosIncorrectosActualizado = true;
      let notificacionWebCreada = true;
      let warning = "";
      const { data: markVentaRows, error: markVentaErr } = await supabaseAdmin
        .from("ventas")
        .update({ reporte_admin: true })
        .eq("id_venta", ventaId)
        .select("id_venta, id_usuario, id_cuenta, id_cuenta_miembro")
        .maybeSingle();
      if (markVentaErr) {
        if (isMissingColumnError(markVentaErr, "reporte_admin")) {
          return res.status(500).json({
            error: "Falta columna ventas.reporte_admin. No se pudo marcar la venta.",
            ...result,
            markedSale: false,
          });
        }
        throw markVentaErr;
      }
      const { error: markDatosErr } = await supabaseAdmin
        .from("reportes")
        .update({ datos_incorrectos: true, datos_corregidos: false })
        .eq("id_venta", ventaId)
        .eq("solucionado", false);

      if (markDatosErr && !isMissingColumnError(markDatosErr, "datos_incorrectos")) {
        console.error("[whatsapp/ventas/notificar-reporte-admin] mark datos_incorrectos error", {
          id_venta: ventaId,
          error: markDatosErr,
        });
        datosIncorrectosActualizado = false;
        warning =
          "Mensaje enviado, pero no se pudo actualizar el estado de datos incorrectos de los reportes.";
      }

      if (markDatosErr && isMissingColumnError(markDatosErr, "datos_incorrectos")) {
        console.warn(
          "[whatsapp/ventas/notificar-reporte-admin] Falta columna reportes.datos_incorrectos. Continúa sin marcar estado.",
        );
        datosIncorrectosActualizado = false;
        warning = "Falta columna reportes.datos_incorrectos. No se pudo marcar el estado.";
      }

      try {
        const notifCorreo = reportMode === "problemas_correo" ? true : incluirCorreo;
        const notifClave = reportMode === "problemas_correo" ? false : incluirClave;
        await createWebNotificationForIncorrectData({
          idUsuario: result?.id_usuario_destino || markVentaRows?.id_usuario,
          idCuenta: markVentaRows?.id_cuenta_miembro || markVentaRows?.id_cuenta,
          titulo: reportMode === "problemas_correo" ? "Problemas con correo" : "Reporte de Admin.",
          incluirCorreo: notifCorreo,
          incluirClave: notifClave,
          idVenta: ventaId,
        });
      } catch (notifErr) {
        console.error("[whatsapp/ventas/notificar-reporte-admin] web notification error", {
          id_venta: ventaId,
          error: notifErr,
        });
        notificacionWebCreada = false;
        warning = warning
          ? `${warning} No se pudo crear la notificación web.`
          : "Mensaje enviado, pero no se pudo crear la notificación web.";
      }

      return res.json({
        ok: true,
        ...result,
        markedSale: true,
        datos_incorrectos_actualizado: datosIncorrectosActualizado,
        notificacion_web_creada: notificacionWebCreada,
        warning: warning || undefined,
      });
    }
    if (result?.skipped) return res.status(202).json({ ok: false, ...result });
    return res.status(500).json({
      error: result?.error || "No se pudo enviar el reporte de admin por WhatsApp",
      ...result,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[whatsapp/ventas/notificar-reporte-admin] error", err);
    return res.status(500).json({
      error: err?.message || "No se pudo notificar el reporte de admin por WhatsApp",
    });
  }
});

app.post("/api/whatsapp/ventas/notificar-entregada", async (req, res) => {
  try {
    await requireAdminSession(req);
    const ventaId = toPositiveInt(req.body?.id_venta ?? req.body?.idVenta);
    if (!ventaId) {
      return res.status(400).json({ error: "id_venta inválido" });
    }

    const result = await sendVentaServicioEntregadaToWhatsappOwner({
      idVenta: ventaId,
      manageWhatsappLifecycle: true,
    });
    if (result?.sent) return res.json({ ok: true, ...result });
    if (result?.skipped) return res.status(202).json({ ok: false, ...result });
    return res.status(500).json({
      error: result?.error || "No se pudo enviar la notificación de orden entregada por WhatsApp",
      ...result,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[whatsapp/ventas/notificar-entregada] error", err);
    return res.status(500).json({
      error: err?.message || "No se pudo notificar la orden entregada por WhatsApp",
    });
  }
});

app.get("/api/whatsapp/reportes/worker-status", async (req, res) => {
  try {
    const hasWorkerToken = hasValidInternalWorkerTriggerToken(req);
    if (!hasWorkerToken) {
      await requireAdminSession(req);
    }
    return res.json({
      ok: true,
      enabled: WHATSAPP_REPORTES_WATCHER_ENABLED && shouldStartWhatsapp,
      watcherEnabledConfig: WHATSAPP_REPORTES_WATCHER_ENABLED,
      shouldStartWhatsapp,
      enableWhatsappEnv: String(process.env.ENABLE_WHATSAPP || "").trim() || null,
      intervalMs: WHATSAPP_REPORTES_WATCHER_INTERVAL_MS,
      batch: WHATSAPP_REPORTES_WATCHER_BATCH,
      inProgress: reportesWhatsappWatcherInProgress,
      columnMissing: reportesWhatsappWatcherColumnMissing,
      lastRunAt: reportesWhatsappWatcherLastRunAt,
      lastResult: reportesWhatsappWatcherLastResult,
      lastError: reportesWhatsappWatcherLastError,
      usingWorkerToken: hasWorkerToken,
      workerTokenConfigured: INTERNAL_WORKER_TRIGGER_TOKEN.length > 0,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    return res.status(500).json({ error: err?.message || "No se pudo consultar estado del worker" });
  }
});

app.post("/api/whatsapp/reportes/procesar", async (req, res) => {
  try {
    const hasWorkerToken = hasValidInternalWorkerTriggerToken(req);
    if (!hasWorkerToken) {
      await requireAdminSession(req);
    }
    const reportId = toPositiveInt(req.body?.id_reporte ?? req.body?.idReporte);
    if (!reportId) {
      const result = await processPendingReportesWhatsappAlerts();
      return res.json({ ok: true, ...result });
    }

    const reporte = await fetchReporteWhatsappContextById(reportId);
    if (!reporte?.id_reporte) {
      return res.status(404).json({ error: "Reporte no encontrado" });
    }

    const { data: reporteStateRow, error: reporteStateErr } = await supabaseAdmin
      .from("reportes")
      .select("enviado_whatsapp")
      .eq("id_reporte", reportId)
      .maybeSingle();
    if (reporteStateErr && isMissingColumnError(reporteStateErr, "enviado_whatsapp")) {
      return res.status(500).json({
        error: "Falta columna reportes.enviado_whatsapp.",
      });
    }
    if (reporteStateErr) throw reporteStateErr;

    const alreadySent = reporteStateRow?.enviado_whatsapp === true;
    const force = isTrue(req.body?.force) || isTrue(req.body?.forzar);
    if (alreadySent && !force) {
      return res.json({
        ok: true,
        skipped: true,
        reason: "already_marked_whatsapp_sent",
        id_reporte: reportId,
      });
    }

    const result = await sendReporteCreatedToWhatsappGroup({
      reporte,
      manageWhatsappLifecycle: true,
    });
    if (!result?.sent) {
      if (result?.skipped) return res.status(202).json({ ok: false, ...result });
      return res.status(500).json({
        error: result?.error || "No se pudo enviar el reporte por WhatsApp",
        ...result,
      });
    }

    await markReporteWhatsappEnviado(reportId, true);
    return res.json({
      ok: true,
      markedSent: true,
      id_reporte: reportId,
      ...result,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[whatsapp/reportes/procesar] error", err);
    return res.status(500).json({
      error: err?.message || "No se pudo procesar reportes pendientes por WhatsApp",
    });
  }
});

app.get("/api/notificaciones/nuevo-servicio/worker-status", (req, res) => {
  res.json({
    enabled: NUEVO_SERVICIO_NOTIF_QUEUE_ENABLED,
    intervalMs: NUEVO_SERVICIO_NOTIF_QUEUE_INTERVAL_MS,
    batch: NUEVO_SERVICIO_NOTIF_QUEUE_BATCH,
    table: NUEVO_SERVICIO_NOTIF_QUEUE_TABLE,
    inProgress: nuevoServicioNotifQueueInProgress,
    tableMissing: nuevoServicioNotifQueueTableMissing,
    lastRunAt: nuevoServicioNotifQueueLastRunAt,
    lastResult: nuevoServicioNotifQueueLastResult,
    lastError: nuevoServicioNotifQueueLastError,
  });
});

app.post("/api/notificaciones/nuevo-servicio/procesar", async (req, res) => {
  try {
    await requireAdminSession(req);
    const result = await processNuevoServicioNotificationQueue();
    return res.json({ ok: true, ...result });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[notificaciones/nuevo-servicio/procesar] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo procesar la cola de notificaciones" });
  }
});

app.post("/api/whatsapp/send", async (req, res) => {
  try {
    await requireAdminSession(req);
    if (!isWhatsappReady()) {
      return res.status(503).json({ error: "WhatsApp no listo" });
    }

    const rawPhone = req.body?.phone;
    const message = req.body?.message;

    if (!rawPhone || !message) {
      return res.status(400).json({ error: "phone y message son requeridos" });
    }

    const phone = normalizeWhatsappPhone(rawPhone);
    if (!phone) {
      return res.status(400).json({ error: "phone invalido" });
    }

    const chatId = `${phone}@c.us`;
    const client = getWhatsappClient();
    await client.sendMessage(chatId, String(message), { linkPreview: false });

    return res.json({ ok: true });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[whatsapp/send] error", err);
    return res.status(500).json({ error: err.message || "No se pudo enviar el mensaje" });
  }
});

app.post("/api/whatsapp/send-recordatorio", async (req, res) => {
  try {
    await requireAdminSession(req);
    if (!isWhatsappReady()) {
      return res.status(503).json({ error: "WhatsApp no listo" });
    }

    const rawPhone = req.body?.phone;
    const message = req.body?.message;
    const ventaIds = uniqPositiveIds(req.body?.ventaIds || []);

    if (!rawPhone || !message) {
      return res.status(400).json({ error: "phone y message son requeridos" });
    }

    const phone = normalizeWhatsappPhone(rawPhone);
    if (!phone) {
      return res.status(400).json({ error: "phone invalido" });
    }

    const client = getWhatsappClient();
    await client.sendMessage(`${phone}@c.us`, String(message), {
      linkPreview: false,
      waitUntilMsgSent: true,
    });

    if (ventaIds.length) {
      const fechaRecordatorioEnviado = getCaracasDateStr(0);
      const { error: updateErr } = await runSupabaseQueryWithRetry(
        () =>
          supabaseAdmin
            .from("ventas")
            .update({
              recordatorio_enviado: true,
              fecha_recordatorio_enviado: fechaRecordatorioEnviado,
            })
            .in("id_venta", ventaIds),
        "recordatorios:update:single",
      );
      if (updateErr) throw updateErr;
    }

    return res.json({ ok: true, updatedVentas: ventaIds.length });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[whatsapp/send-recordatorio] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo enviar el recordatorio" });
  }
});

app.post("/api/whatsapp/recordatorios/enviar", async (req, res) => {
  let selectedItems = [];
  let shutdownClientAfter = false;
  try {
    await requireAdminSession(req);
    const targetUserIds = uniqPositiveIds(req.body?.targetUserIds || []);
    const startClient = req.body?.startClient === true;
    shutdownClientAfter = req.body?.shutdownClientAfter === true;

    selectedItems = targetUserIds.length
      ? await buildWhatsappRecordatorioItems({ targetUserIds })
      : await buildWhatsappRecordatorioItems();

    if (startClient) {
      await ensureWhatsappClientReady({
        reason: "manual_recordatorios_send",
        allowWhenDisabled: true,
      });
    }

    const result = await sendWhatsappRecordatorios({
      source: startClient ? "manual_managed_client" : "manual",
      itemsOverride: selectedItems,
      targetUserIds: targetUserIds.length ? targetUserIds : null,
    });
    const { dateStr } = getCaracasClock();
    ensureDailyRecordatoriosState(dateStr);
    if (didSendAllRecordatorios(result.primary || result)) {
      recordatoriosEnviados = true;
    }
    return res.json({ ok: true, ...result });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    if (err?.code === "RECORDATORIOS_SEND_IN_PROGRESS") {
      return res.status(409).json({
        error: "Ya hay un envío de recordatorios en progreso",
        lock: err?.lock || getRecordatoriosSendLockState(),
      });
    }
    if (
      err?.code === "WHATSAPP_NOT_READY" ||
      err?.code === "WHATSAPP_DISABLED" ||
      err?.code === "WHATSAPP_DISCONNECTED"
    ) {
      return res.status(503).json({
        error: err?.message || "WhatsApp no listo",
        total: selectedItems.length,
        sendable: countWhatsappSendableRecordatorios(selectedItems),
        items: buildWhatsappRecordatorioPreviewItems(selectedItems, {
          pendingReason: err?.message || "WhatsApp no listo",
        }),
      });
    }
    console.error("[whatsapp/recordatorios/enviar] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudieron enviar los recordatorios" });
  } finally {
    if (shutdownClientAfter) {
      await shutdownWhatsappClient({
        reason: "manual_recordatorios_send_completed",
        allowWhenDisabled: true,
      });
    }
  }
});

app.post("/api/whatsapp/recordatorios/trigger-user", async (req, res) => {
  try {
    const sessionUserId = await getSessionUsuario(req);
    const targetUserId = Number(req.body?.id_usuario);
    if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
      return res.status(400).json({ error: "id_usuario invalido" });
    }

    if (targetUserId !== sessionUserId) {
      await requireAdminSession(req);
    }

    const result = await attemptWhatsappRecordatoriosForUserOnPhoneUpdate(targetUserId, {
      source: "phone_update",
    });
    return res.json(result);
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    if (err?.code === "RECORDATORIOS_SEND_IN_PROGRESS") {
      return res.status(409).json({
        error: "Ya hay un envío de recordatorios en progreso",
        lock: err?.lock || getRecordatoriosSendLockState(),
      });
    }
    console.error("[whatsapp/recordatorios/trigger-user] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo disparar el envío del recordatorio" });
  }
});

const FRONTEND_DIR = path.join(__dirname, "..", "frontend");
[
  ["/styles", "styles"],
  ["/scripts", "scripts"],
  ["/public", "public"],
  ["/partials", "partials"],
  ["/assets", "assets"],
].forEach(([mountPath, dirName]) => {
  app.use(mountPath, express.static(path.join(FRONTEND_DIR, dirName)));
});

app.get("/service-worker.js", (_req, res) => {
  res.set("Cache-Control", "no-cache");
  res.sendFile(path.join(FRONTEND_DIR, "public", "service-worker.js"));
});

const INDEX_HTML_PATH = path.join(FRONTEND_DIR, "pages", "index.html");
const DEFAULT_LOGO_URL =
  "https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/logos/moose.png";

app.get("/", async (req, res) => {
  try {
    const html = await fs.readFile(INDEX_HTML_PATH, "utf8");
    let previewUrl = null;
    try {
      const { data: pageRow } = await supabaseAdmin
        .from("pagina")
        .select("preview, logo")
        .limit(1)
        .maybeSingle();
      previewUrl = pageRow?.preview || pageRow?.logo || null;
    } catch (err) {
      console.error("preview meta load error", err);
    }

    const hasOgImage = /property=["']og:image["']/i.test(html);
    if (!previewUrl || hasOgImage) {
      res.set("Content-Type", "text/html; charset=utf-8");
      return res.send(html);
    }

    const metaTags = [
      `<meta property="og:image" content="${previewUrl || DEFAULT_LOGO_URL}">`,
      `<meta property="og:type" content="website">`,
      `<meta property="og:title" content="Moose+">`,
      `<meta property="og:description" content="Tienda Moose+">`,
      `<meta property="og:url" content="https://mooseplus.com/">`,
      `<meta property="og:image:width" content="1200">`,
      `<meta property="og:image:height" content="630">`,
      `<meta property="og:image:type" content="image/jpeg">`,
      `<meta name="twitter:card" content="summary_large_image">`,
      `<meta name="twitter:title" content="Moose+">`,
      `<meta name="twitter:description" content="Tienda Moose+">`,
      `<meta name="twitter:image" content="${previewUrl || DEFAULT_LOGO_URL}">`,
    ].join("\n");

    const output = html.replace("</head>", `${metaTags}\n</head>`);
    res.set("Content-Type", "text/html; charset=utf-8");
    return res.send(output);
  } catch (err) {
    console.error("index html render error", err);
    return res.status(500).send("Error cargando la página");
  }
});

const AUTH_REQUIRED = "AUTH_REQUIRED";
const ADMIN_REQUIRED = "ADMIN_REQUIRED";
const SUPERADMIN_REQUIRED = "SUPERADMIN_REQUIRED";
const SESSION_COOKIE_NAME = "session_user_id";
const SESSION_COOKIE_SIGNING_SECRET = String(
  process.env.SESSION_COOKIE_SECRET ||
    process.env.SIGNUP_TOKEN_SECRET ||
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    "",
).trim();
const SESSION_COOKIE_TTL_SEC = Math.max(
  24 * 60 * 60,
  Number(process.env.SESSION_COOKIE_TTL_SEC) || 365 * 24 * 60 * 60,
);
const SESSION_COOKIE_TTL_MS = SESSION_COOKIE_TTL_SEC * 1000;
const SESSION_COOKIE_OPTIONS = {
  httpOnly: true,
  sameSite: "lax",
  secure: process.env.NODE_ENV === "production" || process.env.VERCEL === "1",
  path: "/",
};
const isProdLikeEnv = process.env.NODE_ENV === "production" || process.env.VERCEL === "1";

const signSessionCookieValue = (idUsuario) => {
  if (!SESSION_COOKIE_SIGNING_SECRET) return "";
  const id = Number(idUsuario);
  if (!Number.isFinite(id) || id <= 0) return "";
  const payload = String(Math.trunc(id));
  const signature = crypto
    .createHmac("sha256", SESSION_COOKIE_SIGNING_SECRET)
    .update(payload)
    .digest("hex");
  return `${payload}.${signature}`;
};

const parseSignedSessionCookieValue = (rawValue = "") => {
  const value = String(rawValue || "").trim();
  if (!value) return null;
  if (!value.includes(".")) {
    if (isProdLikeEnv) return null;
    const legacyId = Number(value);
    return Number.isFinite(legacyId) && legacyId > 0 ? Math.trunc(legacyId) : null;
  }
  if (!SESSION_COOKIE_SIGNING_SECRET) return null;
  const [payload, signature] = value.split(".");
  if (!payload || !signature || !/^\d+$/.test(payload) || !/^[a-f0-9]{64}$/i.test(signature)) {
    return null;
  }
  const expected = crypto
    .createHmac("sha256", SESSION_COOKIE_SIGNING_SECRET)
    .update(payload)
    .digest("hex");
  const expectedBuf = Buffer.from(expected, "utf8");
  const receivedBuf = Buffer.from(signature.toLowerCase(), "utf8");
  if (expectedBuf.length !== receivedBuf.length) return null;
  if (!crypto.timingSafeEqual(expectedBuf, receivedBuf)) return null;
  const id = Number(payload);
  return Number.isFinite(id) && id > 0 ? Math.trunc(id) : null;
};
const BINANCE_P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search";
const SIGNUP_TOKEN_TTL_SEC = Math.max(
  300,
  Number(process.env.SIGNUP_TOKEN_TTL_SEC) || 24 * 60 * 60,
);
const SIGNUP_ACCESS_TOKEN_TTL_SEC = Math.max(
  300,
  Number(process.env.SIGNUP_ACCESS_TOKEN_TTL_SEC) || 30 * 24 * 60 * 60,
);
const RENEWAL_CART_TOKEN_TTL_SEC = Math.max(
  14 * 24 * 60 * 60,
  Number(process.env.RENEWAL_CART_TOKEN_TTL_SEC) || 30 * 24 * 60 * 60,
);
const SIGNUP_TOKEN_SECRET = String(
  process.env.SIGNUP_TOKEN_SECRET ||
    process.env.REGISTRATION_LINK_SECRET ||
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    "",
).trim();
const SIGNUP_RESEND_COOLDOWN_MS = 60 * 1000;
const signupResendCooldownMap = new Map();
const NEW_AUTH_SIGNUP_NOTIFY_USER_ID = 23;
const isUsuarioWebRegistrado = (row = null) =>
  Boolean(row?.fecha_registro) || Boolean(String(row?.id_auth || "").trim());

const todayInVenezuela = () => {
  // Retorna fecha actual en huso horario de Venezuela (America/Caracas) en formato YYYY-MM-DD
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(new Date());
};

// Suma meses manteniendo el día (sin desfase de zona horaria); si el mes destino no tiene ese día, usa el último día del mes.
function addMonthsKeepDay(baseDate, months) {
  const baseStr =
    typeof baseDate === "string"
      ? baseDate
      : new Date(baseDate).toISOString().slice(0, 10);
  const [y, m, d] = baseStr.split("-").map(Number);
  let mm = (m - 1) + months;
  let yy = y + Math.floor(mm / 12);
  mm = mm % 12;
  if (mm < 0) {
    mm += 12;
    yy -= 1;
  }
  const daysInTarget = new Date(Date.UTC(yy, mm + 1, 0)).getUTCDate();
  const day = Math.min(d, daysInTarget);
  const result = new Date(Date.UTC(yy, mm, day));
  return result.toISOString().slice(0, 10);
}

const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const isInactive = (v) => isTrue(v);
const toPositiveInt = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.trunc(parsed);
};

const encodeBase64Url = (input) =>
  Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");

const decodeBase64Url = (input) => {
  const normalized = String(input || "")
    .replace(/-/g, "+")
    .replace(/_/g, "/");
  const padding = normalized.length % 4 === 0 ? "" : "=".repeat(4 - (normalized.length % 4));
  return Buffer.from(`${normalized}${padding}`, "base64");
};

const tokenError = (code, message) => {
  const err = new Error(message);
  err.code = code;
  return err;
};

const getSignupTokenSecret = () => {
  if (!SIGNUP_TOKEN_SECRET) {
    throw tokenError(
      "SIGNUP_TOKEN_SECRET_MISSING",
      "SIGNUP_TOKEN_SECRET no está configurado en el backend.",
    );
  }
  return SIGNUP_TOKEN_SECRET;
};

const signSignupTokenPayload = (payloadPart) => {
  const secret = getSignupTokenSecret();
  const digest = crypto.createHmac("sha256", secret).update(payloadPart).digest();
  return encodeBase64Url(digest);
};

const SIGNUP_ULTRA_PREFIX = "x";
const SIGNUP_ULTRA_MAC_BYTES = 6;
const SIGNUP_COMPACT_MAC_BYTES = 10;
const SIGNUP_COMPACT_MAC_BYTES_LEGACY = 16;

const encodeUInt24BE = (num) => {
  const value = Number(num);
  if (!Number.isFinite(value) || value < 0 || value > 0xffffff) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }
  const out = Buffer.allocUnsafe(3);
  out[0] = (value >>> 16) & 0xff;
  out[1] = (value >>> 8) & 0xff;
  out[2] = value & 0xff;
  return out;
};

const decodeUInt24BE = (buffer, offset = 0) =>
  ((buffer[offset] << 16) | (buffer[offset + 1] << 8) | buffer[offset + 2]) >>> 0;

const signSignupTokenCompact = (payloadPart) => {
  const secret = getSignupTokenSecret();
  // 80-bit MAC truncado para URL más corta.
  const digest = crypto
    .createHmac("sha256", secret)
    .update(payloadPart)
    .digest()
    .subarray(0, SIGNUP_COMPACT_MAC_BYTES);
  return encodeBase64Url(digest);
};

const isValidSignupCompactSignature = (payloadPart, signaturePart) => {
  const candidates = [SIGNUP_COMPACT_MAC_BYTES, SIGNUP_COMPACT_MAC_BYTES_LEGACY];
  const receivedSigBuffer = Buffer.from(signaturePart || "", "utf8");
  return candidates.some((macBytes) => {
    const secret = getSignupTokenSecret();
    const expectedSig = encodeBase64Url(
      crypto.createHmac("sha256", secret).update(payloadPart).digest().subarray(0, macBytes),
    );
    const expectedSigBuffer = Buffer.from(expectedSig, "utf8");
    if (receivedSigBuffer.length !== expectedSigBuffer.length) return false;
    return crypto.timingSafeEqual(receivedSigBuffer, expectedSigBuffer);
  });
};

const signSignupTokenUltra = (payloadPart) => {
  const secret = getSignupTokenSecret();
  const digest = crypto
    .createHmac("sha256", secret)
    .update(payloadPart)
    .digest()
    .subarray(0, SIGNUP_ULTRA_MAC_BYTES);
  return encodeBase64Url(digest);
};

const buildSignupRegistrationToken = (idUsuario) => {
  const uid = toPositiveInt(idUsuario);
  if (!uid) throw tokenError("INVALID_UID", "id_usuario inválido.");
  if (uid > 0xffffff) throw tokenError("INVALID_UID", "id_usuario fuera de rango.");
  const nowSec = Math.floor(Date.now() / 1000);
  const exp = nowSec + SIGNUP_TOKEN_TTL_SEC;
  // Formato ultracorto: "x" + base64url([uid24|expHour24]) + mac48.
  const expHour = Math.floor(exp / 3600);
  if (expHour > 0xffffff) throw tokenError("TOKEN_INVALID", "exp fuera de rango.");
  const payloadBin = Buffer.concat([encodeUInt24BE(uid), encodeUInt24BE(expHour)]);
  const payloadPart = encodeBase64Url(payloadBin);
  const signaturePart = signSignupTokenUltra(payloadPart);
  return `${SIGNUP_ULTRA_PREFIX}${payloadPart}${signaturePart}`;
};

const normalizeSignupNextPath = (rawValue = "") => {
  const value = String(rawValue || "").trim();
  if (!value) return "";
  try {
    const parsed = new URL(value, PUBLIC_SITE_URL);
    if (!/^https?:$/i.test(parsed.protocol)) return "";
    if (!isAllowedPublicRedirectUrl(parsed.toString())) return "";
    const pathName = String(parsed.pathname || "/").trim() || "/";
    return `${pathName.startsWith("/") ? pathName : `/${pathName}`}${parsed.search || ""}${
      parsed.hash || ""
    }`;
  } catch (_err) {
    return "";
  }
};

const buildSignupRegistrationUrl = (idUsuario, options = {}) => {
  const token = buildSignupRegistrationToken(idUsuario);
  const signupUrl = new URL("/signup.html", PUBLIC_SITE_URL);
  signupUrl.searchParams.set("t", token);
  const renewalToken = String(options?.renewalToken || options?.rr || "").trim();
  if (renewalToken) {
    signupUrl.searchParams.set("rr", renewalToken);
  }
  const nextPath = normalizeSignupNextPath(
    options?.nextUrl || options?.redirectUrl || options?.next || "",
  );
  if (nextPath) {
    signupUrl.searchParams.set("next", nextPath);
  }
  return signupUrl.toString();
};

const buildSignupShortUrl = (idUsuario) => {
  const token = buildSignupRegistrationToken(idUsuario);
  const shortUrl = new URL(`/s/${encodeURIComponent(token)}`, PUBLIC_SITE_URL);
  return shortUrl.toString();
};

const buildSignupAccessToken = ({ accesoCliente = true } = {}) => {
  const nowSec = Math.floor(Date.now() / 1000);
  const exp = nowSec + SIGNUP_ACCESS_TOKEN_TTL_SEC;
  const accessPart = accesoCliente === false ? "0" : "1";
  const expPart = exp.toString(36);
  const payloadPart = `${accessPart}.${expPart}`;
  const signaturePart = signSignupTokenCompact(payloadPart);
  return `${payloadPart}.${signaturePart}`;
};

const verifySignupAccessToken = (tokenValue, options = {}) => {
  const token = String(tokenValue || "").trim();
  if (!token) throw tokenError("TOKEN_REQUIRED", "Token requerido.");
  const parts = token.split(".");
  if (parts.length !== 3) throw tokenError("TOKEN_INVALID", "Token inválido.");
  const [accessPart, expPart, signaturePart] = parts;
  if (!accessPart || !expPart || !signaturePart) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }
  if (accessPart !== "0" && accessPart !== "1") {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }
  const base36Regex = /^[0-9a-z]+$/i;
  if (!base36Regex.test(expPart)) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }
  const payloadPart = `${accessPart}.${expPart}`;
  if (!isValidSignupCompactSignature(payloadPart, signaturePart)) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }
  const exp = Number.parseInt(expPart, 36);
  if (!Number.isFinite(exp) || exp <= 0) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }
  const nowSec = Math.floor(Date.now() / 1000);
  const allowExpired = options?.allowExpired === true;
  if (!allowExpired && exp <= nowSec) {
    throw tokenError("TOKEN_EXPIRED", "El link de registro ya venció.");
  }
  return {
    accesoCliente: accessPart === "1",
    exp,
  };
};

const buildRenewalCartToken = ({ idUsuario, ventaIds = [] } = {}) => {
  const uid = toPositiveInt(idUsuario);
  if (!uid) throw tokenError("INVALID_UID", "id_usuario inválido.");
  const ventas = uniqPositiveIds(ventaIds).sort((a, b) => a - b);
  if (!ventas.length) throw tokenError("TOKEN_INVALID", "Token inválido.");

  const nowSec = Math.floor(Date.now() / 1000);
  const exp = nowSec + RENEWAL_CART_TOKEN_TTL_SEC;
  const uidPart = uid.toString(36);
  const expPart = exp.toString(36);
  const ventasPart = ventas.map((id) => id.toString(36)).join(".");
  const payloadPart = `${uidPart}.${expPart}.${ventasPart}`;
  const signaturePart = signSignupTokenCompact(payloadPart);
  return `${payloadPart}.${signaturePart}`;
};

const verifyRenewalCartToken = (tokenValue, options = {}) => {
  const token = String(tokenValue || "").trim();
  if (!token) throw tokenError("TOKEN_REQUIRED", "Token requerido.");
  const parts = token.split(".");
  if (parts.length < 4) throw tokenError("TOKEN_INVALID", "Token inválido.");

  const signaturePart = parts.pop();
  const [uidPart, expPart, ...ventaParts] = parts;
  if (!uidPart || !expPart || !ventaParts.length || !signaturePart) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }

  const base36Regex = /^[0-9a-z]+$/i;
  if (
    !base36Regex.test(uidPart) ||
    !base36Regex.test(expPart) ||
    ventaParts.some((part) => !base36Regex.test(String(part || "")))
  ) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }

  const payloadPart = `${uidPart}.${expPart}.${ventaParts.join(".")}`;
  if (!isValidSignupCompactSignature(payloadPart, signaturePart)) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }

  const uid = toPositiveInt(Number.parseInt(uidPart, 36));
  const exp = Number.parseInt(expPart, 36);
  const ventaIds = uniqPositiveIds(ventaParts.map((part) => Number.parseInt(part, 36))).sort(
    (a, b) => a - b,
  );
  if (!uid || !Number.isFinite(exp) || !ventaIds.length) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }

  const nowSec = Math.floor(Date.now() / 1000);
  const allowExpired = options?.allowExpired === true;
  if (!allowExpired && exp <= nowSec) {
    throw tokenError("TOKEN_EXPIRED", "El link de renovaciones ya venció.");
  }

  return { uid, exp, ventaIds };
};

const buildRenewalCartUrl = ({ idUsuario, ventaIds = [] } = {}) => {
  const token = buildRenewalCartToken({ idUsuario, ventaIds });
  const renewUrl = new URL(`/r/${encodeURIComponent(token)}`, PUBLIC_SITE_URL);
  return renewUrl.toString();
};

const verifySignupRegistrationToken = (tokenValue, options = {}) => {
  const token = String(tokenValue || "").trim();
  if (!token) throw tokenError("TOKEN_REQUIRED", "Token requerido.");
  const parts = token.split(".");
  let uid = null;
  let exp = null;
  let iatRaw = null;
  let iat = null;
  const nowSec = Math.floor(Date.now() / 1000);

  if (
    parts.length === 1 &&
    token.startsWith(SIGNUP_ULTRA_PREFIX) &&
    token.length > 1
  ) {
    const body = token.slice(1);
    const payloadChars = 8; // 6 bytes -> 8 chars base64url
    const signaturePart = body.slice(payloadChars);
    const payloadPart = body.slice(0, payloadChars);
    if (!payloadPart || !signaturePart) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }
    const expectedSig = signSignupTokenUltra(payloadPart);
    const receivedSigBuffer = Buffer.from(signaturePart, "utf8");
    const expectedSigBuffer = Buffer.from(expectedSig, "utf8");
    if (
      receivedSigBuffer.length !== expectedSigBuffer.length ||
      !crypto.timingSafeEqual(receivedSigBuffer, expectedSigBuffer)
    ) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }
    const payloadBin = decodeBase64Url(payloadPart);
    if (!payloadBin || payloadBin.length !== 6) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }
    const uidDecoded = decodeUInt24BE(payloadBin, 0);
    const expHourDecoded = decodeUInt24BE(payloadBin, 3);
    uid = toPositiveInt(uidDecoded);
    exp = Number(expHourDecoded) * 3600;
  } else if (parts.length === 3) {
    const [uidPart, expPart, signaturePart] = parts;
    if (!uidPart || !expPart || !signaturePart) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }
    const payloadPart = `${uidPart}.${expPart}`;
    if (!isValidSignupCompactSignature(payloadPart, signaturePart)) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }
    const uidDecoded = parseInt(uidPart, 36);
    const expDecoded = parseInt(expPart, 36);
    uid = toPositiveInt(uidDecoded);
    exp = Number.isFinite(expDecoded) ? expDecoded : null;
  } else if (parts.length === 2) {
    // Legacy: payload JSON base64url + firma sha256 completa.
    const [payloadPart, signaturePart] = parts;
    if (!payloadPart || !signaturePart) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }
    const expectedSig = signSignupTokenPayload(payloadPart);
    const receivedSigBuffer = Buffer.from(signaturePart, "utf8");
    const expectedSigBuffer = Buffer.from(expectedSig, "utf8");
    if (
      receivedSigBuffer.length !== expectedSigBuffer.length ||
      !crypto.timingSafeEqual(receivedSigBuffer, expectedSigBuffer)
    ) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }

    let payload = null;
    try {
      payload = JSON.parse(decodeBase64Url(payloadPart).toString("utf8"));
    } catch (_err) {
      throw tokenError("TOKEN_INVALID", "Token inválido.");
    }
    uid = toPositiveInt(payload?.u ?? payload?.uid);
    exp = Number(payload?.e ?? payload?.exp);
    iatRaw = payload?.iat;
    iat = Number(iatRaw);
  } else {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }

  if (!uid || !Number.isFinite(exp)) throw tokenError("TOKEN_INVALID", "Token inválido.");
  if (iatRaw !== undefined && iatRaw !== null && Number.isFinite(iat) && iat > nowSec + 300) {
    throw tokenError("TOKEN_INVALID", "Token inválido.");
  }
  const allowExpired = options?.allowExpired === true;
  if (!allowExpired && exp <= nowSec) {
    throw tokenError("TOKEN_EXPIRED", "El link de registro ya venció.");
  }

  return { uid, exp, iat };
};

const signupTokenErrorStatus = (code) => {
  if (code === "TOKEN_EXPIRED") return 410;
  if (code === "TOKEN_INVALID" || code === "TOKEN_REQUIRED") return 400;
  if (code === "SIGNUP_TOKEN_SECRET_MISSING") return 500;
  return 500;
};

const buildReemplazosBlocklist = (rows) => {
  const cuentas = new Set();
  const perfiles = new Set();
  (rows || []).forEach((row) => {
    const cuentaId = toPositiveInt(row?.id_cuenta);
    const perfilId = toPositiveInt(row?.id_perfil);
    if (cuentaId) cuentas.add(cuentaId);
    if (perfilId) perfiles.add(perfilId);
  });
  return { cuentas, perfiles };
};

const orderSpotifyProfilesByPriority = (profiles = [], preferredMotherId = null) => {
  const groups = new Map();
  (profiles || []).forEach((profile) => {
    const perfilId = toPositiveInt(profile?.id_perfil);
    const cuentaId = toPositiveInt(profile?.id_cuenta);
    if (!perfilId || !cuentaId) return;
    if (!groups.has(cuentaId)) groups.set(cuentaId, []);
    groups.get(cuentaId).push(profile);
  });
  if (!groups.size) return [];

  const sortProfileList = (list = []) =>
    [...list].sort((a, b) => {
      const na = Number.isFinite(Number(a?.n_perfil)) ? Number(a.n_perfil) : Number.MAX_SAFE_INTEGER;
      const nb = Number.isFinite(Number(b?.n_perfil)) ? Number(b.n_perfil) : Number.MAX_SAFE_INTEGER;
      if (na !== nb) return na - nb;
      const ida = toPositiveInt(a?.id_perfil) || Number.MAX_SAFE_INTEGER;
      const idb = toPositiveInt(b?.id_perfil) || Number.MAX_SAFE_INTEGER;
      return ida - idb;
    });

  const preferredId = toPositiveInt(preferredMotherId);
  const sortedMotherIds = Array.from(groups.keys()).sort((a, b) => {
    const freeA = groups.get(a)?.length || 0;
    const freeB = groups.get(b)?.length || 0;
    if (freeA !== freeB) return freeA - freeB;
    return a - b;
  });

  const ordered = [];
  if (preferredId && groups.has(preferredId)) {
    ordered.push(...sortProfileList(groups.get(preferredId)));
  }
  sortedMotherIds.forEach((motherId) => {
    if (preferredId && motherId === preferredId) return;
    ordered.push(...sortProfileList(groups.get(motherId)));
  });
  return ordered;
};

const autoAssignReportedPendingVentas = async ({
  plataformaIds = [],
  includeUnreported = false,
} = {}) => {
  const summary = { scanned: 0, resolved: 0, skipped: 0, errors: 0 };
  const platformFilter = uniqPositiveIds(plataformaIds);

  const { data: reemplazosRows, error: reemplazosErr } = await supabaseAdmin
    .from("reemplazos")
    .select("id_cuenta, id_perfil");
  if (reemplazosErr) throw reemplazosErr;
  const reemplazosBloqueados = buildReemplazosBlocklist(reemplazosRows);

  let ventasQuery = supabaseAdmin
    .from("ventas")
    .select(
      `
        id_venta,
        id_usuario,
        id_precio,
        id_cuenta,
        id_cuenta_miembro,
        id_perfil,
        pendiente,
        reportado,
        precios:precios(id_plataforma, completa, sub_cuenta),
        cuenta_base:cuentas!ventas_id_cuenta_fkey(
          id_cuenta,
          id_plataforma,
          inactiva,
          venta_perfil,
          venta_miembro,
          cuenta_madre
        ),
        cuenta_miembro:cuentas!ventas_id_cuenta_miembro_fkey(
          id_cuenta,
          id_plataforma,
          inactiva,
          venta_perfil,
          venta_miembro,
          cuenta_madre,
          id_cuenta_madre
        ),
        perfiles:perfiles(id_perfil, n_perfil, perfil_hogar)
      `
    )
    .eq("pendiente", true)
    .order("id_venta", { ascending: true })
    .limit(400);
  if (!includeUnreported) {
    ventasQuery = ventasQuery.eq("reportado", true);
  }
  const { data: ventasRows, error: ventasErr } = await ventasQuery;
  if (ventasErr) throw ventasErr;

  const ventas = (ventasRows || []).filter((venta) => {
    const platId = toPositiveInt(
      venta?.precios?.id_plataforma ||
      venta?.cuenta_base?.id_plataforma ||
      venta?.cuenta_miembro?.id_plataforma
    );
    if (!platId) return false;
    if (platformFilter.length && !platformFilter.includes(platId)) return false;
    return true;
  });
  const plataformaIdsEnVentas = uniqPositiveIds(
    ventas.map((venta) =>
      toPositiveInt(
        venta?.precios?.id_plataforma ||
          venta?.cuenta_base?.id_plataforma ||
          venta?.cuenta_miembro?.id_plataforma
      )
    )
  );
  const { data: plataformaRows, error: plataformaErr } = plataformaIdsEnVentas.length
    ? await supabaseAdmin
        .from("plataformas")
        .select("id_plataforma, entrega_inmediata, por_pantalla, por_acceso, cuenta_madre")
        .in("id_plataforma", plataformaIdsEnVentas)
    : { data: [], error: null };
  if (plataformaErr) throw plataformaErr;
  const plataformaById = (plataformaRows || []).reduce((acc, row) => {
    const platId = toPositiveInt(row?.id_plataforma);
    if (platId) acc[platId] = row;
    return acc;
  }, {});

  const findCuentaCompletaLibre = async (plataformaId, excludeCuentaIds = []) => {
    const excludeIds = new Set(uniqPositiveIds(excludeCuentaIds));
    const { data, error } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo, clave, inactiva, ocupado, venta_perfil, venta_miembro")
      .eq("id_plataforma", plataformaId)
      .eq("venta_perfil", false)
      .eq("venta_miembro", false)
      .or("ocupado.is.null,ocupado.eq.false")
      .or("inactiva.is.null,inactiva.eq.false")
      .order("id_cuenta", { ascending: true })
      .limit(120);
    if (error) return { error };
    const libre = (data || []).find((cuenta) => {
      const cuentaId = toPositiveInt(cuenta?.id_cuenta);
      return (
        !!cuentaId &&
        !excludeIds.has(cuentaId) &&
        !isInactive(cuenta?.inactiva) &&
        !isTrue(cuenta?.ocupado) &&
        !reemplazosBloqueados.cuentas.has(cuentaId)
      );
    });
    return { data: libre || null };
  };

  const findCuentaMiembroLibre = async (plataformaId, excludeCuentaIds = []) => {
    const excludeIds = new Set(uniqPositiveIds(excludeCuentaIds));
    const { data, error } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo, clave, inactiva, ocupado, venta_perfil, venta_miembro")
      .eq("id_plataforma", plataformaId)
      .eq("venta_perfil", false)
      .eq("venta_miembro", true)
      .eq("ocupado", false)
      .or("inactiva.is.null,inactiva.eq.false")
      .order("id_cuenta", { ascending: true })
      .limit(120);
    if (error) return { error };
    const libre = (data || []).find((cuenta) => {
      const cuentaId = toPositiveInt(cuenta?.id_cuenta);
      return (
        !!cuentaId &&
        !excludeIds.has(cuentaId) &&
        !isInactive(cuenta?.inactiva) &&
        !reemplazosBloqueados.cuentas.has(cuentaId)
      );
    });
    return { data: libre || null };
  };

  const findPerfilLibre = async ({
    plataformaId,
    perfilHogar = false,
    onlyCuentaMadre = false,
    excludeCuentaIds = [],
  }) => {
    const excludeIds = new Set(uniqPositiveIds(excludeCuentaIds));
    let query = supabaseAdmin
      .from("perfiles")
      .select(
        `
          id_perfil,
          id_cuenta,
          n_perfil,
          perfil_hogar,
          ocupado,
          cuentas:cuentas!perfiles_id_cuenta_fkey!inner(
            id_cuenta,
            id_plataforma,
            inactiva,
            venta_perfil,
            cuenta_madre
          )
        `
      )
      .eq("cuentas.id_plataforma", plataformaId)
      .eq("ocupado", false)
      .order("id_perfil", { ascending: true })
      .limit(240);
    if (perfilHogar === true) {
      query = query.eq("perfil_hogar", true);
    } else {
      query = query.or("perfil_hogar.is.null,perfil_hogar.eq.false");
    }
    query = query.or("inactiva.is.null,inactiva.eq.false", { foreignTable: "cuentas" });
    query = onlyCuentaMadre
      ? query.eq("cuentas.cuenta_madre", true).eq("cuentas.venta_perfil", false)
      : query
          .eq("cuentas.venta_perfil", true)
          .or("cuenta_madre.is.null,cuenta_madre.eq.false", { foreignTable: "cuentas" });
    const { data, error } = await query;
    if (error) return { error };
    const disponibles = (data || []).filter((perfil) => {
      const perfilId = toPositiveInt(perfil?.id_perfil);
      const cuentaId = toPositiveInt(perfil?.id_cuenta);
      return (
        !!perfilId &&
        !!cuentaId &&
        !excludeIds.has(cuentaId) &&
        !isInactive(perfil?.cuentas?.inactiva) &&
        !reemplazosBloqueados.perfiles.has(perfilId)
      );
    });
    const ordenados =
      onlyCuentaMadre && Number(plataformaId) === 9
        ? orderSpotifyProfilesByPriority(disponibles)
        : disponibles;
    return { data: ordenados[0] || null };
  };

  const findOpenReporte = async (venta) => {
    const userId = toPositiveInt(venta?.id_usuario);
    const cuentaId = toPositiveInt(venta?.id_cuenta);
    const perfilId = toPositiveInt(venta?.id_perfil);
    if (!userId || !cuentaId) return null;

    let query = supabaseAdmin
      .from("reportes")
      .select("id_reporte")
      .eq("id_usuario", userId)
      .eq("id_cuenta", cuentaId)
      .eq("solucionado", false)
      .order("id_reporte", { ascending: true })
      .limit(1);
    query = perfilId ? query.eq("id_perfil", perfilId) : query.is("id_perfil", null);
    const { data, error } = await query;
    if (error) throw error;
    return data?.[0] || null;
  };

  for (const venta of ventas) {
      summary.scanned += 1;
      try {
        const ventaId = toPositiveInt(venta?.id_venta);
        const plataformaId = toPositiveInt(
          venta?.precios?.id_plataforma ||
        venta?.cuenta_base?.id_plataforma ||
        venta?.cuenta_miembro?.id_plataforma
      );
        const priceId = toPositiveInt(venta?.id_precio);
        const isNetflixPlan2 = Number(plataformaId) === 1 && (priceId === 4 || priceId === 5);
        if (!ventaId || !plataformaId) {
          summary.skipped += 1;
          continue;
        }
        const plataforma = plataformaById[plataformaId] || null;
        const esReportada = isTrue(venta?.reportado);
        const isPlatform9 = Number(plataformaId) === 9;
        if (esReportada && isPlatform9) {
          summary.skipped += 1;
          continue;
        }

        const oldCuentaId = toPositiveInt(venta?.id_cuenta);
        const oldPerfilId = toPositiveInt(venta?.id_perfil);
        const oldCuentaMiembroId = toPositiveInt(venta?.id_cuenta_miembro);
        const currentCuenta = venta?.cuenta_base || venta?.cuenta_miembro || null;
        const tieneRecursoAsignado = !!oldCuentaId || !!oldCuentaMiembroId || !!oldPerfilId;

        if (!esReportada) {
          const entregaInmediata = isTrue(plataforma?.entrega_inmediata);
          const autoAssignBloqueado =
            Number(plataformaId) === 9 || isTrue(plataforma?.cuenta_madre);
          if (!entregaInmediata || autoAssignBloqueado || tieneRecursoAsignado) {
            summary.skipped += 1;
            continue;
          }
        }

        let perfilHogar = venta?.perfiles?.perfil_hogar === true;
        let onlyCuentaMadre =
          Number(plataformaId) === 9 &&
          (isTrue(currentCuenta?.cuenta_madre) || !!oldPerfilId);
        let ventaMiembro =
          isTrue(currentCuenta?.venta_miembro) || (!!oldCuentaMiembroId && !oldPerfilId);
        let ventaPerfil =
          !!oldPerfilId || isTrue(currentCuenta?.venta_perfil) || onlyCuentaMadre;
        let ventaCompleta =
          !ventaPerfil &&
          !ventaMiembro &&
          (isTrue(venta?.precios?.completa) ||
            (!isTrue(currentCuenta?.venta_perfil) && !isTrue(currentCuenta?.venta_miembro)));
        let useNetflixPlan2Flow = isNetflixPlan2 && !ventaCompleta;

        if (!esReportada && !tieneRecursoAsignado) {
          const platCuentaMadre = isTrue(plataforma?.cuenta_madre) || Number(plataformaId) === 9;
          const platPorAcceso = isTrue(plataforma?.por_acceso);
          const precioCompleta = isTrue(venta?.precios?.completa);
          if (isNetflixPlan2) {
            // Igual que checkout principal para Netflix Plan 2:
            // intentar perfil hogar y, si no hay, cuenta miembro.
            ventaCompleta = false;
            ventaMiembro = false;
            ventaPerfil = true;
            onlyCuentaMadre = false;
            perfilHogar = true;
            useNetflixPlan2Flow = true;
          } else if (precioCompleta) {
            ventaCompleta = true;
            ventaMiembro = false;
            ventaPerfil = false;
            onlyCuentaMadre = false;
            perfilHogar = false;
          } else if (platCuentaMadre) {
            ventaCompleta = false;
            ventaMiembro = false;
            ventaPerfil = true;
            onlyCuentaMadre = true;
            perfilHogar = false;
          } else if (platPorAcceso) {
            ventaCompleta = false;
            ventaMiembro = true;
            ventaPerfil = false;
            onlyCuentaMadre = false;
            perfilHogar = false;
          } else {
            ventaCompleta = false;
            ventaMiembro = false;
            ventaPerfil = true;
            onlyCuentaMadre = false;
            perfilHogar = false;
          }
        }

        let nuevoCuentaId = null;
        let nuevoPerfilId = null;
      if (useNetflixPlan2Flow) {
        const { data: perfilHogarLibre, error: perfilHogarErr } = await findPerfilLibre({
          plataformaId,
          perfilHogar: true,
          onlyCuentaMadre: false,
          excludeCuentaIds: [oldCuentaId, oldCuentaMiembroId],
        });
        if (perfilHogarErr) throw perfilHogarErr;
        nuevoCuentaId = toPositiveInt(perfilHogarLibre?.id_cuenta);
        nuevoPerfilId = toPositiveInt(perfilHogarLibre?.id_perfil);

        if (!nuevoCuentaId) {
          const { data: cuentaMiembroLibre, error: cuentaMiembroErr } = await findCuentaMiembroLibre(
            plataformaId,
            [oldCuentaId, oldCuentaMiembroId]
          );
          if (cuentaMiembroErr) throw cuentaMiembroErr;
          nuevoCuentaId = toPositiveInt(cuentaMiembroLibre?.id_cuenta);
          nuevoPerfilId = null;
        }
        console.log("[autoAssignPending][netflix-plan2] resolved", {
          id_venta: ventaId,
          id_precio: priceId,
          assigned_cuenta: nuevoCuentaId || null,
          assigned_perfil: nuevoPerfilId || null,
          route: nuevoPerfilId ? "perfil_hogar" : nuevoCuentaId ? "cuenta_miembro" : "sin_stock",
        });
      } else if (ventaPerfil) {
        const { data: perfilLibre, error: perfilErr } = await findPerfilLibre({
          plataformaId,
          perfilHogar,
          onlyCuentaMadre,
          excludeCuentaIds: [oldCuentaId, oldCuentaMiembroId],
        });
        if (perfilErr) throw perfilErr;
        nuevoCuentaId = toPositiveInt(perfilLibre?.id_cuenta);
        nuevoPerfilId = toPositiveInt(perfilLibre?.id_perfil);
      } else if (ventaMiembro) {
        const { data: cuentaLibre, error: cuentaErr } = await findCuentaMiembroLibre(
          plataformaId,
          [oldCuentaId, oldCuentaMiembroId]
        );
        if (cuentaErr) throw cuentaErr;
        nuevoCuentaId = toPositiveInt(cuentaLibre?.id_cuenta);
      } else if (ventaCompleta) {
        const { data: cuentaLibre, error: cuentaErr } = await findCuentaCompletaLibre(
          plataformaId,
          [oldCuentaId, oldCuentaMiembroId]
        );
        if (cuentaErr) throw cuentaErr;
        nuevoCuentaId = toPositiveInt(cuentaLibre?.id_cuenta);
      }

      if (!nuevoCuentaId) {
        summary.skipped += 1;
        continue;
      }

      const updateVenta = {
        id_cuenta: nuevoCuentaId,
        id_perfil: nuevoPerfilId || null,
        pendiente: false,
        entrega_aviso: false,
      };
      if (esReportada) {
        updateVenta.reportado = false;
        updateVenta.aviso_admin = true;
      }
      if (oldCuentaMiembroId) {
        updateVenta.id_cuenta_miembro = null;
      }
      const { error: updVentaErr } = await supabaseAdmin
        .from("ventas")
        .update(updateVenta)
        .eq("id_venta", ventaId);
      if (updVentaErr) throw updVentaErr;
      if (esReportada) {
        console.log("[autoAssignReportedPendingVentas] whatsapp servicio entregado omitido", {
          id_venta: ventaId,
          reason: "sale_was_reported",
        });
      } else {
        try {
          await notifyVentaDeliveredWhatsappBestEffort(ventaId);
        } catch (waDeliveredErr) {
          console.error("[autoAssignReportedPendingVentas] whatsapp entregada error", {
            id_venta: ventaId,
            error: waDeliveredErr?.message || waDeliveredErr,
          });
        }
      }

      if (nuevoPerfilId) {
        const { error: occPerfilErr } = await supabaseAdmin
          .from("perfiles")
          .update({ ocupado: true })
          .eq("id_perfil", nuevoPerfilId);
        if (occPerfilErr) throw occPerfilErr;
      }
      const { error: occCuentaErr } = await supabaseAdmin
        .from("cuentas")
        .update({ ocupado: true })
        .eq("id_cuenta", nuevoCuentaId);
      if (occCuentaErr) throw occCuentaErr;

      if (oldPerfilId && oldPerfilId !== nuevoPerfilId) {
        const { data: otherVentas, error: otherVentasErr } = await supabaseAdmin
          .from("ventas")
          .select("id_venta")
          .eq("id_perfil", oldPerfilId)
          .neq("id_venta", ventaId)
          .limit(1);
        if (otherVentasErr) throw otherVentasErr;
        if (!(otherVentas || []).length) {
          await supabaseAdmin.from("perfiles").update({ ocupado: false }).eq("id_perfil", oldPerfilId);
        }
      }

      if (esReportada && !isPlatform9 && (oldCuentaId || oldPerfilId)) {
        const { error: replErr } = await supabaseAdmin
          .from("reemplazos")
          .insert({
            id_cuenta: oldCuentaId || null,
            id_perfil: oldPerfilId || null,
            id_sub_cuenta: null,
            id_venta: ventaId,
          });
        if (replErr) throw replErr;
      }

      if (esReportada && !isPlatform9) {
        const reporteAbierto = await findOpenReporte(venta);
        if (reporteAbierto?.id_reporte) {
          const { error: repErr } = await supabaseAdmin
            .from("reportes")
            .update({
              en_revision: false,
              solucionado: true,
              descripcion_solucion: "Reemplazo automatico por stock disponible",
            })
            .eq("id_reporte", reporteAbierto.id_reporte);
          if (repErr) throw repErr;
          try {
            const reporteCtx = await fetchReporteWhatsappContextById(reporteAbierto.id_reporte);
            if (reporteCtx?.id_reporte && reporteCtx?.solucionado === true) {
              const waSolvedRes = await sendReporteSolvedToWhatsappOwner({
                reporte: reporteCtx,
                manageWhatsappLifecycle: true,
              });
              if (!waSolvedRes?.sent) {
                console.warn("[autoAssignReportedPendingVentas] whatsapp solucionado skipped", {
                  id_reporte: reporteAbierto.id_reporte,
                  reason: waSolvedRes?.reason || "unknown",
                  error: waSolvedRes?.error || null,
                });
              }
            }
          } catch (waSolvedErr) {
            console.error("[autoAssignReportedPendingVentas] whatsapp solucionado error", waSolvedErr);
          }
        }
      }

      summary.resolved += 1;
    } catch (err) {
      summary.errors += 1;
      console.error("[autoAssignReportedPendingVentas] item error", err);
    }
  }

  return summary;
};

const processAutoPendingStockDeliveries = async () => {
  if (!AUTO_PENDING_STOCK_DELIVERY_ENABLED) {
    return { skipped: true, reason: "disabled" };
  }
  if (autoPendingStockDeliveryInProgress) {
    return { skipped: true, reason: "in_progress" };
  }
  autoPendingStockDeliveryInProgress = true;
  try {
    const summary = await autoAssignReportedPendingVentas({
      includeUnreported: true,
    });
    if (summary?.resolved > 0 || summary?.errors > 0) {
      console.log("[ventas:auto-pending-stock] ciclo", summary);
    }
    return summary;
  } finally {
    autoPendingStockDeliveryInProgress = false;
  }
};

const normalizeFilesArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return [];
    if (trimmed.startsWith("[")) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) return parsed;
      } catch (err) {
        // ignore JSON parse errors, fallback to plain string
      }
    }
    return [trimmed];
  }
  return [];
};

const normalizeSpecialPriceRow = (row = {}) => {
  const id = toPositiveInt(row?.id);
  const idPrecio = toPositiveInt(row?.id_precio);
  const idUsuario = toPositiveInt(row?.id_usuario);
  const monto = parseOptionalCheckoutNumber(row?.monto);
  if (!id || !idPrecio || !idUsuario) return null;
  return {
    id,
    id_precio: idPrecio,
    id_usuario: idUsuario,
    monto: Number.isFinite(monto) ? roundCheckoutMoney(monto) : null,
  };
};

const listUserSpecialPrices = async ({
  idUsuario,
  idPrecios = null,
  idEspeciales = null,
} = {}) => {
  const usuarioId = toPositiveInt(idUsuario);
  if (!usuarioId) return [];
  const precioIds = uniqPositiveIds(idPrecios || []);
  const especialIds = uniqPositiveIds(idEspeciales || []);

  let query = supabaseAdmin
    .from("precios_especiales")
    .select("id, id_precio, id_usuario, monto")
    .eq("id_usuario", usuarioId)
    .order("id", { ascending: false });
  if (precioIds.length) {
    query = query.in("id_precio", precioIds);
  }
  if (especialIds.length) {
    query = query.in("id", especialIds);
  }
  const { data, error } = await query;
  if (error) throw error;
  return (data || []).map((row) => normalizeSpecialPriceRow(row)).filter(Boolean);
};

const buildSpecialPriceLookup = (rows = []) => {
  const byId = {};
  const byPrecioId = {};
  (rows || []).forEach((row) => {
    const normalized = normalizeSpecialPriceRow(row);
    if (!normalized) return;
    if (!Number.isFinite(parseOptionalCheckoutNumber(normalized?.monto))) return;
    byId[normalized.id] = normalized;
    if (!byPrecioId[normalized.id_precio]) {
      byPrecioId[normalized.id_precio] = normalized;
    }
  });
  return { byId, byPrecioId };
};

const resolveCartSpecialPriceForUser = async ({
  idUsuario,
  idPrecio,
  requestedIdPrecioEspecial = undefined,
} = {}) => {
  const usuarioId = toPositiveInt(idUsuario);
  const precioId = toPositiveInt(idPrecio);
  if (!usuarioId || !precioId) return null;

  if (requestedIdPrecioEspecial === null) {
    return null;
  }

  const requestedSpecialId = toPositiveInt(requestedIdPrecioEspecial);
  if (requestedSpecialId) {
    const rows = await listUserSpecialPrices({
      idUsuario: usuarioId,
      idEspeciales: [requestedSpecialId],
    });
    const selected = rows.find((row) => row?.id === requestedSpecialId) || null;
    if (!selected) {
      const err = new Error("id_precio_especial inválido para este usuario.");
      err.code = "INVALID_PRECIO_ESPECIAL";
      err.httpStatus = 400;
      throw err;
    }
    if (Number(selected.id_precio) !== Number(precioId)) {
      const err = new Error("id_precio_especial no corresponde al id_precio enviado.");
      err.code = "INVALID_PRECIO_ESPECIAL";
      err.httpStatus = 400;
      throw err;
    }
    if (!Number.isFinite(parseOptionalCheckoutNumber(selected?.monto))) {
      return null;
    }
    return selected;
  }

  const rows = await listUserSpecialPrices({
    idUsuario: usuarioId,
    idPrecios: [precioId],
  });
  return (
    rows.find(
      (row) =>
        Number(row?.id_precio) === Number(precioId) &&
        Number.isFinite(parseOptionalCheckoutNumber(row?.monto)),
    ) || null
  );
};

const resolvePrecioTipoLabel = (priceRow = {}, platformRow = {}) => {
  const plan = String(priceRow?.plan || "").trim();
  if (plan) return plan;
  if (isTrue(priceRow?.completa) && !isTrue(priceRow?.sub_cuenta)) {
    return "Cuenta completa";
  }
  if (isTrue(platformRow?.por_pantalla)) return "Pantalla";
  if (isTrue(platformRow?.por_acceso)) return "Acceso";
  if (isTrue(priceRow?.sub_cuenta)) return "Subcuenta";
  return "Plan";
};

const getPrecioPicker = async (idUsuarioVentas) => {
  const { data: usuarioVenta, error: userVentaErr } = await supabaseAdmin
    .from("usuarios")
    .select("acceso_cliente")
    .eq("id_usuario", idUsuarioVentas)
    .single();
  if (userVentaErr && userVentaErr.code !== "PGRST116") throw userVentaErr;
  const accesoCliente = usuarioVenta?.acceso_cliente;
  const esMayorista =
    accesoCliente === false || accesoCliente === "false" || accesoCliente === 0 || accesoCliente === "0";
  const pickPrecio = (price) => {
    const detal = Number(price?.precio_usd_detal) || 0;
    const mayor = Number(price?.precio_usd_mayor) || 0;
    return esMayorista ? mayor || detal : detal || mayor;
  };
  return { accesoCliente: accesoCliente ?? null, esMayorista, pickPrecio };
};

const isValidPrecioId = (value) => {
  const id = Number(value);
  return Number.isFinite(id) && id > 0;
};

const assertItemsValidPrecioId = (items = []) => {
  const invalidItem = (items || []).find((it) => !isValidPrecioId(it?.id_precio));
  if (!invalidItem) return;
  const err = new Error(
    `No se puede procesar la venta: id_precio inválido (${invalidItem?.id_precio ?? "null"}).`,
  );
  err.code = "INVALID_PRECIO_ID";
  err.httpStatus = 400;
  err.details = {
    id_item: invalidItem?.id_item ?? null,
    id_venta: invalidItem?.id_venta ?? null,
    id_precio: invalidItem?.id_precio ?? null,
  };
  throw err;
};

const buildRenewalDiscountQtyByPrecio = (items = []) => {
  const qtyByPrecio = new Map();
  (items || []).forEach((item) => {
    if (item?.renovacion !== true) return;
    if (!toPositiveInt(item?.id_venta)) return;
    const precioId = toPositiveInt(item?.id_precio);
    if (!precioId) return;
    const qty = Math.max(1, Number(item?.cantidad || 1) || 1);
    const current = Number(qtyByPrecio.get(precioId) || 0);
    qtyByPrecio.set(precioId, current + qty);
  });
  return qtyByPrecio;
};

const applyRenewalDiscountQtyContext = (items = []) => {
  const list = Array.isArray(items) ? items : [];
  const renewalQtyByPrecio = buildRenewalDiscountQtyByPrecio(list);
  return list.map((item) => {
    const qty = Math.max(1, Number(item?.cantidad || 1) || 1);
    const precioId = toPositiveInt(item?.id_precio);
    const isRenewalWithVenta = item?.renovacion === true && toPositiveInt(item?.id_venta);
    const qtyDescuento =
      isRenewalWithVenta && precioId
        ? Math.max(1, Number(renewalQtyByPrecio.get(precioId) || qty) || qty)
        : qty;
    return {
      ...item,
      qty_descuento: qtyDescuento,
    };
  });
};

const roundCheckoutMoney = (value) =>
  Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;

const parseOptionalCheckoutNumber = (value) => {
  if (value === null || value === undefined) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = Number(raw.replace(",", "."));
  return Number.isFinite(normalized) ? normalized : null;
};

const getStrictStoredTasaActual = async () => {
  const { data, error } = await supabaseAdmin
    .from("tasa_config")
    .select("tasa_actual")
    .eq("id", 1)
    .maybeSingle();
  if (error) throw error;
  const tasaActual = parseOptionalCheckoutNumber(data?.tasa_actual);
  if (!Number.isFinite(tasaActual) || tasaActual <= 0) {
    const err = new Error("tasa_config.tasa_actual no disponible");
    err.code = "TASA_ACTUAL_REQUIRED";
    throw err;
  }
  return tasaActual;
};

const isCheckoutExplicitFalse = (value) =>
  value === false || value === 0 || value === "0" || value === "false" || value === "f";

const normalizeCheckoutPlatformRow = (row = {}) => {
  const idDescMesBase = row.id_descuento_mes ?? 1;
  const idDescCantidadBase = row.id_descuento_cantidad ?? 2;
  return {
    ...row,
    id_descuento_mes: idDescMesBase,
    id_descuento_cantidad: idDescCantidadBase,
    id_descuento_mes_detal: row.id_descuento_mes_detal ?? idDescMesBase,
    id_descuento_mes_mayor: row.id_descuento_mes_mayor ?? idDescMesBase,
    id_descuento_cantidad_detal: row.id_descuento_cantidad_detal ?? idDescCantidadBase,
    id_descuento_cantidad_mayor: row.id_descuento_cantidad_mayor ?? idDescCantidadBase,
    aplica_descuento_mes_detal: isCheckoutExplicitFalse(row.aplica_descuento_mes_detal)
      ? false
      : true,
    aplica_descuento_mes_mayor: isCheckoutExplicitFalse(row.aplica_descuento_mes_mayor)
      ? false
      : true,
    aplica_descuento_cantidad_detal: isCheckoutExplicitFalse(row.aplica_descuento_cantidad_detal)
      ? false
      : true,
    aplica_descuento_cantidad_mayor: isCheckoutExplicitFalse(row.aplica_descuento_cantidad_mayor)
      ? false
      : true,
  };
};

const getClosestCheckoutDiscountPct = (rows = [], value, column) => {
  const key = Number(value) || 0;
  if (!Array.isArray(rows) || key <= 0 || !column) return 0;
  const exact = rows.find((row) => Number(row?.meses) === key);
  const exactVal = exact?.[column];
  if (exactVal !== null && exactVal !== undefined && exactVal !== "") {
    return Number(exactVal) || 0;
  }
  let best = null;
  for (const row of rows) {
    const meses = Number(row?.meses);
    if (!Number.isFinite(meses) || meses > key) continue;
    const raw = row?.[column];
    if (raw === null || raw === undefined || raw === "") continue;
    if (!best || meses > Number(best?.meses)) best = row;
  }
  return Number(best?.[column]) || 0;
};

const getCheckoutDiscountColumnsFromRows = (rows = []) => {
  const cols = new Set();
  (rows || []).forEach((row) => {
    Object.keys(row || {}).forEach((key) => {
      const normalized = String(key || "").toLowerCase();
      if (/^descuento_\d+$/i.test(normalized)) cols.add(normalized);
    });
  });
  const out = Array.from(cols).sort((a, b) => {
    const na = Number(a.split("_")[1]) || 0;
    const nb = Number(b.split("_")[1]) || 0;
    return na - nb;
  });
  return out.length
    ? out
    : ["descuento_1", "descuento_2", "descuento_3", "descuento_4", "descuento_5"];
};

const buildCheckoutDiscountColumnByIdMap = (rows = [], cols = []) => {
  const ids = [];
  (rows || []).forEach((row) => {
    const id = Number(row?.id_descuento);
    if (!Number.isFinite(id) || ids.includes(id)) return;
    ids.push(id);
  });
  return ids.reduce((acc, id, index) => {
    if (cols[index]) acc[id] = cols[index];
    return acc;
  }, {});
};

const resolveCheckoutDiscountColumn = (
  platformInfo,
  mode,
  discountColumns,
  discountColumnById,
  isCliente = true,
) => {
  const isItemsMode = mode === "items";
  const groupField = isItemsMode
    ? isCliente
      ? "id_descuento_cantidad_detal"
      : "id_descuento_cantidad_mayor"
    : isCliente
      ? "id_descuento_mes_detal"
      : "id_descuento_mes_mayor";
  const legacyField = isItemsMode ? "id_descuento_cantidad" : "id_descuento_mes";
  const preferredRaw = platformInfo?.[groupField];
  const hasPreferredRaw =
    preferredRaw !== null &&
    preferredRaw !== undefined &&
    String(preferredRaw).trim() !== "";
  const raw = hasPreferredRaw ? preferredRaw : platformInfo?.[legacyField];
  const asText = String(raw || "").trim();
  if (/^descuento_\d+$/i.test(asText)) return asText.toLowerCase();
  const asNum = Number(raw);
  if (Number.isFinite(asNum) && asNum >= 1) {
    const direct = `descuento_${Math.trunc(asNum)}`;
    if ((discountColumns || []).includes(direct)) return direct;
    const mapped = discountColumnById?.[Math.trunc(asNum)];
    if (mapped) return mapped;
  }
  return mode === "items" ? "descuento_2" : "descuento_1";
};

const isCheckoutDiscountEnabledForAudience = (platformInfo, mode = "months", isCliente = true) => {
  if (mode === "items") {
    return isCliente
      ? !isCheckoutExplicitFalse(platformInfo?.aplica_descuento_cantidad_detal)
      : !isCheckoutExplicitFalse(platformInfo?.aplica_descuento_cantidad_mayor);
  }
  return isCliente
    ? !isCheckoutExplicitFalse(platformInfo?.aplica_descuento_mes_detal)
    : !isCheckoutExplicitFalse(platformInfo?.aplica_descuento_mes_mayor);
};

const computeCheckoutItemPricing = ({
  item,
  priceInfo,
  platformInfo,
  pickPrecio,
  descuentos = [],
  discountColumns = [],
  discountColumnById = {},
  isCliente = true,
}) => {
  const qty = Math.max(1, Number(item?.cantidad || priceInfo?.cantidad || 1) || 1);
  const qtyDescuento = Math.max(1, Number(item?.qty_descuento || qty) || qty);
  const isGiftCard = isTrue(platformInfo?.tarjeta_de_regalo);
  const mesesVal = isGiftCard
    ? 1
    : Math.max(1, Number(item?.meses || priceInfo?.duracion || 1) || 1);
  const unitPrice = Number(pickPrecio?.(priceInfo, item)) || 0;
  const baseSubtotalUsd = roundCheckoutMoney(unitPrice * qty * (isGiftCard ? 1 : mesesVal));
  const monthEnabled =
    !!platformInfo?.descuento_meses &&
    !isGiftCard &&
    isCheckoutDiscountEnabledForAudience(platformInfo, "months", isCliente);
  const qtyEnabled = isCheckoutDiscountEnabledForAudience(platformInfo, "items", isCliente);
  const monthColumn = resolveCheckoutDiscountColumn(
    platformInfo,
    "months",
    discountColumns,
    discountColumnById,
    isCliente,
  );
  const qtyColumn = resolveCheckoutDiscountColumn(
    platformInfo,
    "items",
    discountColumns,
    discountColumnById,
    isCliente,
  );
  const rawRateMeses = monthEnabled
    ? getClosestCheckoutDiscountPct(descuentos, mesesVal, monthColumn)
    : 0;
  const rawRateQty = qtyEnabled
    ? getClosestCheckoutDiscountPct(descuentos, qtyDescuento, qtyColumn)
    : 0;
  const rateMeses = rawRateMeses > 1 ? rawRateMeses / 100 : rawRateMeses;
  const rateQty = rawRateQty > 1 ? rawRateQty / 100 : rawRateQty;
  const descuentoMesesUsd = rateMeses > 0 ? roundCheckoutMoney(baseSubtotalUsd * rateMeses) : 0;
  const descuentoCantidadUsd = rateQty > 0 ? roundCheckoutMoney(baseSubtotalUsd * rateQty) : 0;
  const descuentoUsd = roundCheckoutMoney(descuentoMesesUsd + descuentoCantidadUsd);
  const subtotalUsd = roundCheckoutMoney(baseSubtotalUsd - descuentoUsd);
  return {
    qty,
    qtyDescuento,
    mesesVal,
    unitPrice,
    baseSubtotalUsd,
    descuentoMesesUsd,
    descuentoCantidadUsd,
    descuentoUsd,
    subtotalUsd,
    isGiftCard,
  };
};

const computeCheckoutItemBaseAmount = ({
  item,
  priceInfo,
  platformInfo,
  pickPrecio,
  descuentos = [],
  discountColumns = [],
  discountColumnById = {},
  isCliente = true,
}) =>
  computeCheckoutItemPricing({
    item,
    priceInfo,
    platformInfo,
    pickPrecio,
    descuentos,
    discountColumns,
    discountColumnById,
    isCliente,
  }).subtotalUsd;

const normalizeCheckoutCustomItemAmounts = (input = []) => {
  if (input == null) return new Map();
  if (!Array.isArray(input)) {
    const err = new Error("custom_item_amounts debe ser un arreglo.");
    err.code = "INVALID_CUSTOM_ITEM_AMOUNTS";
    err.httpStatus = 400;
    throw err;
  }

  return input.reduce((acc, row) => {
    const itemId = toPositiveInt(row?.id_item);
    if (!itemId) {
      const err = new Error("Cada monto personalizado debe incluir id_item válido.");
      err.code = "INVALID_CUSTOM_ITEM_AMOUNTS";
      err.httpStatus = 400;
      throw err;
    }
    const amountRaw = Number(String(row?.monto_usd ?? "").trim().replace(",", "."));
    if (!Number.isFinite(amountRaw) || amountRaw < 0) {
      const err = new Error(`Monto personalizado inválido para el item ${itemId}.`);
      err.code = "INVALID_CUSTOM_ITEM_AMOUNTS";
      err.httpStatus = 400;
      throw err;
    }
    acc.set(itemId, roundCheckoutMoney(amountRaw));
    return acc;
  }, new Map());
};

const buildCheckoutContext = async ({ idUsuarioVentas, carritoId, totalCliente }) => {
  const { accesoCliente, esMayorista, pickPrecio: pickPrecioBase } = await getPrecioPicker(
    idUsuarioVentas,
  );
  const isCliente = !esMayorista;
  const totalClienteParsed = parseOptionalCheckoutNumber(totalCliente);
  const tasaBsParsed = await getStrictStoredTasaActual();
  const { data: items, error: itemErr } = await supabaseAdmin
    .from("carrito_items")
    .select(
      "id_item, id_precio, id_precio_especial, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil",
    )
    .eq("id_carrito", carritoId);
  if (itemErr) throw itemErr;
  const itemsWithDiscountQty = applyRenewalDiscountQtyContext(items || []);

  if (!itemsWithDiscountQty.length) {
    const total = totalClienteParsed ?? 0;
    const tasaBs = Number.isFinite(tasaBsParsed) ? tasaBsParsed : null;
    return {
      items: [],
      priceMap: {},
      platInfoById: {},
      platNameById: {},
      pickPrecio: pickPrecioBase,
      total,
      tasaBs,
      accesoCliente,
      esMayorista,
      isCliente,
      descuentos: [],
      discountColumns: [
        "descuento_1",
        "descuento_2",
        "descuento_3",
        "descuento_4",
        "descuento_5",
      ],
      discountColumnById: {},
    };
  }

  assertItemsValidPrecioId(itemsWithDiscountQty);
  const specialPriceIds = uniqPositiveIds(
    (itemsWithDiscountQty || []).map((item) => item?.id_precio_especial),
  );
  const specialPriceRows = specialPriceIds.length
    ? await listUserSpecialPrices({
        idUsuario: idUsuarioVentas,
        idEspeciales: specialPriceIds,
      })
    : [];
  const specialPriceLookup = buildSpecialPriceLookup(specialPriceRows);
  const pickPrecio = (priceInfo = {}, item = null) => {
    const specialId = toPositiveInt(item?.id_precio_especial);
    if (specialId) {
      const special = specialPriceLookup.byId?.[specialId] || null;
      const specialPriceId = toPositiveInt(special?.id_precio);
      const priceId = toPositiveInt(priceInfo?.id_precio);
      const specialAmount = parseOptionalCheckoutNumber(special?.monto);
      if (
        special &&
        Number.isFinite(specialAmount) &&
        (!specialPriceId || !priceId || Number(specialPriceId) === Number(priceId))
      ) {
        return roundCheckoutMoney(specialAmount);
      }
    }
    return pickPrecioBase(priceInfo);
  };

  const preciosIds = (itemsWithDiscountQty || []).map((i) => i.id_precio).filter(Boolean);
  const { data: precios, error: precioErr } = await supabaseAdmin
    .from("precios")
    .select(
      "id_precio, cantidad, duracion, precio_usd_detal, precio_usd_mayor, id_plataforma, completa, sub_cuenta, valor_tarjeta_de_regalo",
    )
    .in("id_precio", preciosIds);
  if (precioErr) throw precioErr;
  const priceMap = (precios || []).reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});

  const plataformaIds = [...new Set((precios || []).map((p) => p.id_plataforma).filter(Boolean))];
  const [
    { data: plataformas, error: platErr },
    { data: descuentos, error: descuentosErr },
  ] = await Promise.all([
    supabaseAdmin.from("plataformas").select("*").in("id_plataforma", plataformaIds),
    supabaseAdmin.from("descuentos").select("*").order("meses", { ascending: true }),
  ]);
  if (platErr) throw platErr;
  if (descuentosErr) throw descuentosErr;
  const normalizedPlataformas = (plataformas || []).map((row) => normalizeCheckoutPlatformRow(row));
  const platInfoById = normalizedPlataformas.reduce((acc, p) => {
    acc[p.id_plataforma] = p;
    return acc;
  }, {});
  const platNameById = normalizedPlataformas.reduce((acc, p) => {
    acc[p.id_plataforma] = p.nombre || `Plataforma ${p.id_plataforma}`;
    return acc;
  }, {});
  const discountColumns = getCheckoutDiscountColumnsFromRows(descuentos || []);
  const discountColumnById = buildCheckoutDiscountColumnByIdMap(descuentos || [], discountColumns);

  const totalCalc = (itemsWithDiscountQty || []).reduce((sum, it) => {
    const priceInfo = priceMap[it.id_precio] || {};
    const platId = Number(priceInfo?.id_plataforma) || null;
    const platformInfo = platId ? platInfoById?.[platId] || {} : {};
    return (
      sum +
      computeCheckoutItemBaseAmount({
        item: it,
        priceInfo,
        platformInfo,
        pickPrecio,
        descuentos,
        discountColumns,
        discountColumnById,
        isCliente,
      })
    );
  }, 0);
  const total = totalClienteParsed ?? totalCalc;
  const tasaBs = Number.isFinite(tasaBsParsed) ? tasaBsParsed : null;

  return {
    items: itemsWithDiscountQty || [],
    priceMap,
    platInfoById,
    platNameById,
    pickPrecio,
    total,
    tasaBs,
    accesoCliente,
    esMayorista,
    isCliente,
    descuentos: descuentos || [],
    discountColumns,
    discountColumnById,
  };
};

const parseOrdenSnapshotPrecioIdFromDetalle = (detalle = "") => {
  const text = String(detalle || "");
  const match = text.match(/precio\s*#\s*(\d+)/i);
  return toPositiveInt(match?.[1]) || null;
};

const parseOrdenSnapshotQtyMesesFromDetalle = (detalle = "") => {
  const text = String(detalle || "");
  const qtyMatch = text.match(
    /(?:^|\|)\s*(\d+)\s+(?:pantalla|pantallas|acceso|accesos|item|items|tarjeta|tarjetas)\b/i,
  );
  const mesesMatch = text.match(/(?:^|\|)\s*(\d+)\s+mes(?:es)?\b/i);
  return {
    cantidad: toPositiveInt(qtyMatch?.[1]) || null,
    meses: toPositiveInt(mesesMatch?.[1]) || null,
  };
};

const resolveOrderProcessingContextFromSnapshot = async ({
  ordenId,
  totalCliente = null,
  fallbackContext = null,
} = {}) => {
  const orderIdNum = toPositiveInt(ordenId);
  if (!orderIdNum) {
    return {
      source: "carrito_items",
      context: fallbackContext || null,
      itemAmountMapById: null,
    };
  }

  const { data: snapshotRows, error: snapshotErr } = await supabaseAdmin
    .from("ordenes_items")
    .select("id_item_orden, id_plataforma, id_venta, renovacion, detalle, monto_usd")
    .eq("id_orden", orderIdNum)
    .order("id_item_orden", { ascending: true });
  if (snapshotErr) throw snapshotErr;

  if (!Array.isArray(snapshotRows) || !snapshotRows.length) {
    return {
      source: "carrito_items",
      context: fallbackContext || null,
      itemAmountMapById: null,
    };
  }

  const renewalVentaIds = uniqPositiveIds(
    snapshotRows.map((row) => toPositiveInt(row?.id_venta)).filter(Boolean),
  );
  const renewalPriceByVenta = {};
  if (renewalVentaIds.length) {
    const { data: renewalSalesRows, error: renewalSalesErr } = await supabaseAdmin
      .from("ventas")
      .select("id_venta, id_precio")
      .in("id_venta", renewalVentaIds);
    if (renewalSalesErr) throw renewalSalesErr;
    (renewalSalesRows || []).forEach((row) => {
      const ventaId = toPositiveInt(row?.id_venta);
      const precioId = toPositiveInt(row?.id_precio);
      if (ventaId && precioId) renewalPriceByVenta[ventaId] = precioId;
    });
  }

  const parseErrors = [];
  const snapshotItemAmountMap = new Map();
  const fallbackItems = Array.isArray(fallbackContext?.items) ? fallbackContext.items : [];
  const snapshotItems = snapshotRows.map((row, index) => {
    const itemId = toPositiveInt(row?.id_item_orden) || index + 1;
    const ventaId = toPositiveInt(row?.id_venta) || null;
    const renew = isTrue(row?.renovacion);
    const detalle = String(row?.detalle || "");
    const fallbackItem = fallbackItems[index] || null;
    const fallbackPriceId = toPositiveInt(fallbackItem?.id_precio) || null;
    const parsedPriceId = parseOrdenSnapshotPrecioIdFromDetalle(detalle);
    const fallbackRenewalPriceId = renew && ventaId ? toPositiveInt(renewalPriceByVenta[ventaId]) : null;
    const priceId = fallbackPriceId || parsedPriceId || fallbackRenewalPriceId || null;
    if (!priceId) {
      parseErrors.push({
        id_item_orden: itemId,
        id_venta: ventaId,
        detalle,
        reason: "precio_no_detectado_en_snapshot",
      });
    }

    const parsedQtyMeses = parseOrdenSnapshotQtyMesesFromDetalle(detalle);
    const cantidad =
      Number.isFinite(Number(fallbackItem?.cantidad)) && Number(fallbackItem?.cantidad) > 0
        ? Math.max(1, Math.round(Number(fallbackItem.cantidad)))
        : parsedQtyMeses.cantidad || 1;
    const meses =
      Number.isFinite(Number(fallbackItem?.meses)) && Number(fallbackItem?.meses) > 0
        ? Math.max(1, Math.round(Number(fallbackItem.meses)))
        : parsedQtyMeses.meses || 1;
    const montoUsd = parseOptionalCheckoutNumber(row?.monto_usd);
    if (Number.isFinite(montoUsd)) {
      snapshotItemAmountMap.set(itemId, roundCheckoutMoney(montoUsd));
    }

    return {
      id_item: itemId,
      id_precio: priceId,
      cantidad: cantidad || 1,
      meses: meses || 1,
      renovacion: renew,
      id_venta: ventaId,
      id_cuenta: null,
      id_perfil: null,
    };
  });

  if (parseErrors.length) {
    const err = new Error(
      "No se pudo reconstruir la orden desde el snapshot. Revisa los items de la orden antes de procesar.",
    );
    err.code = "ORDEN_SNAPSHOT_INVALID";
    err.httpStatus = 409;
    err.details = {
      id_orden: orderIdNum,
      errors: parseErrors.slice(0, 10),
    };
    throw err;
  }

  const snapshotPriceIds = uniqPositiveIds(snapshotItems.map((item) => item?.id_precio).filter(Boolean));
  const fallbackPriceMap = fallbackContext?.priceMap || {};
  const missingPriceIds = snapshotPriceIds.filter((priceId) => !fallbackPriceMap[priceId]);
  let fetchedPrices = [];
  if (missingPriceIds.length) {
    const { data: pricesRows, error: pricesErr } = await supabaseAdmin
      .from("precios")
      .select(
        "id_precio, cantidad, duracion, precio_usd_detal, precio_usd_mayor, id_plataforma, completa, sub_cuenta, valor_tarjeta_de_regalo",
      )
      .in("id_precio", missingPriceIds);
    if (pricesErr) throw pricesErr;
    fetchedPrices = pricesRows || [];
  }
  const mergedPriceMap = { ...fallbackPriceMap };
  fetchedPrices.forEach((row) => {
    const priceId = toPositiveInt(row?.id_precio);
    if (priceId) mergedPriceMap[priceId] = row;
  });

  const unresolvedPriceIds = snapshotPriceIds.filter((priceId) => !mergedPriceMap[priceId]);
  if (unresolvedPriceIds.length) {
    const err = new Error(
      `No se encontraron precios del snapshot para la orden: ${unresolvedPriceIds.join(", ")}`,
    );
    err.code = "ORDEN_SNAPSHOT_INVALID";
    err.httpStatus = 409;
    err.details = { id_orden: orderIdNum, missing_price_ids: unresolvedPriceIds };
    throw err;
  }

  const platformMismatch = snapshotItems.find((item, index) => {
    const snapshotPlatformId = toPositiveInt(snapshotRows[index]?.id_plataforma);
    const pricePlatformId = toPositiveInt(mergedPriceMap[item?.id_precio]?.id_plataforma);
    return snapshotPlatformId && pricePlatformId && snapshotPlatformId !== pricePlatformId;
  });
  if (platformMismatch) {
    const mismatchIdx = snapshotItems.findIndex((item, index) => {
      const snapshotPlatformId = toPositiveInt(snapshotRows[index]?.id_plataforma);
      const pricePlatformId = toPositiveInt(mergedPriceMap[item?.id_precio]?.id_plataforma);
      return snapshotPlatformId && pricePlatformId && snapshotPlatformId !== pricePlatformId;
    });
    const err = new Error(
      "Conflicto de plataforma detectado entre ordenes_items y precios. Se bloquea la entrega para evitar asignaciones incorrectas.",
    );
    err.code = "ORDEN_SNAPSHOT_PLATFORM_MISMATCH";
    err.httpStatus = 409;
    err.details = {
      id_orden: orderIdNum,
      id_item_orden: toPositiveInt(snapshotRows?.[mismatchIdx]?.id_item_orden) || null,
      id_precio: toPositiveInt(platformMismatch?.id_precio) || null,
      id_plataforma_snapshot: toPositiveInt(snapshotRows?.[mismatchIdx]?.id_plataforma) || null,
      id_plataforma_precio: toPositiveInt(
        mergedPriceMap[platformMismatch?.id_precio]?.id_plataforma,
      ) || null,
    };
    throw err;
  }

  const mergedPlatInfoById = { ...(fallbackContext?.platInfoById || {}) };
  const mergedPlatNameById = { ...(fallbackContext?.platNameById || {}) };
  const requiredPlatformIds = uniqPositiveIds([
    ...snapshotRows.map((row) => toPositiveInt(row?.id_plataforma)).filter(Boolean),
    ...Object.values(mergedPriceMap)
      .map((price) => toPositiveInt(price?.id_plataforma))
      .filter(Boolean),
  ]);
  const missingPlatformIds = requiredPlatformIds.filter((platId) => !mergedPlatInfoById[platId]);
  if (missingPlatformIds.length) {
    const { data: platformsRows, error: platformsErr } = await supabaseAdmin
      .from("plataformas")
      .select("*")
      .in("id_plataforma", missingPlatformIds);
    if (platformsErr) throw platformsErr;
    (platformsRows || []).forEach((row) => {
      const normalized = normalizeCheckoutPlatformRow(row);
      const platformId = toPositiveInt(normalized?.id_plataforma);
      if (!platformId) return;
      mergedPlatInfoById[platformId] = normalized;
      mergedPlatNameById[platformId] = normalized?.nombre || `Plataforma ${platformId}`;
    });
  }

  const totalClienteParsed = parseOptionalCheckoutNumber(totalCliente);
  const snapshotHasCompleteAmounts =
    snapshotRows.length > 0 &&
    snapshotRows.every((row) => Number.isFinite(parseOptionalCheckoutNumber(row?.monto_usd)));
  const snapshotTotalUsd = snapshotHasCompleteAmounts
    ? roundCheckoutMoney(
        snapshotRows.reduce(
          (sum, row) => sum + (parseOptionalCheckoutNumber(row?.monto_usd) || 0),
          0,
        ),
      )
    : null;
  const fallbackTotalParsed = parseOptionalCheckoutNumber(fallbackContext?.total);
  const resolvedTotal = Number.isFinite(totalClienteParsed)
    ? roundCheckoutMoney(totalClienteParsed)
    : Number.isFinite(snapshotTotalUsd)
      ? snapshotTotalUsd
      : roundCheckoutMoney(fallbackTotalParsed || 0);

  return {
    source: "ordenes_items",
    context: {
      ...(fallbackContext || {}),
      items: snapshotItems,
      priceMap: mergedPriceMap,
      platInfoById: mergedPlatInfoById,
      platNameById: mergedPlatNameById,
      total: resolvedTotal,
    },
    itemAmountMapById: snapshotItemAmountMap.size ? snapshotItemAmountMap : null,
  };
};

const resolveCarritoMontoUsdFromItems = async ({
  idUsuarioVentas,
  carritoId,
  fallbackMontoUsd = null,
} = {}) => {
  const carritoNum = toPositiveInt(carritoId);
  const usuarioNum = toPositiveInt(idUsuarioVentas);
  const fallback = parseOptionalCheckoutNumber(fallbackMontoUsd);
  if (!carritoNum || !usuarioNum) {
    return Number.isFinite(fallback) ? roundCheckoutMoney(fallback) : 0;
  }
  const context = await buildCheckoutContext({
    idUsuarioVentas: usuarioNum,
    carritoId: carritoNum,
    totalCliente: null,
  });
  const totalFromItems = parseOptionalCheckoutNumber(context?.total);
  if (Number.isFinite(totalFromItems)) {
    return roundCheckoutMoney(totalFromItems);
  }
  return Number.isFinite(fallback) ? roundCheckoutMoney(fallback) : 0;
};

const buildOrdenItemDetalle = ({
  item,
  priceInfo,
  platformInfo,
  platformName,
  cuentaMap,
  perfilMap,
  ventaMap,
}) => {
  const qty = Math.max(1, Number(item?.cantidad) || 1);
  const mesesVal = Math.max(1, Number(item?.meses) || 1);
  const isGiftCard = isTrue(platformInfo?.tarjeta_de_regalo);
  const unitBase = isGiftCard
    ? "tarjeta"
    : isTrue(platformInfo?.por_pantalla)
      ? "pantalla"
      : isTrue(platformInfo?.por_acceso)
        ? "acceso"
        : "item";
  const unitLabel = qty === 1 ? unitBase : `${unitBase}s`;
  const parts = [
    platformName || "Plataforma",
    item?.renovacion === true ? "Renovacion" : "Nuevo",
    `${qty} ${unitLabel}`,
  ];

  if (!isGiftCard) {
    parts.push(`${mesesVal} mes${mesesVal === 1 ? "" : "es"}`);
  }

  const ventaInfo = item?.id_venta ? ventaMap[item.id_venta] || null : null;
  const cuentaDirecta = item?.id_cuenta ? cuentaMap[item.id_cuenta] || null : null;
  const perfilDirecto = item?.id_perfil ? perfilMap[item.id_perfil] || null : null;
  const correoRenovacion =
    cuentaDirecta?.correo ||
    ventaInfo?.correo_miembro ||
    ventaInfo?.cuentas_miembro?.correo ||
    ventaInfo?.cuentas?.correo ||
    "";
  const nPerfil = perfilDirecto?.n_perfil || ventaInfo?.perfiles?.n_perfil || null;

  if (correoRenovacion) {
    parts.push(`Correo: ${correoRenovacion}`);
  }
  const showPerfilByScreen = isTrue(platformInfo?.por_pantalla);
  const showPerfilByAccess = isTrue(platformInfo?.por_acceso);
  if (!showPerfilByScreen && showPerfilByAccess) {
    parts.push("Acceso: 1 dispositivo");
  } else if (showPerfilByScreen && nPerfil) {
    parts.push(`Perfil: M${nPerfil}`);
  }
  if (item?.id_venta) {
    parts.push(`Venta #${item.id_venta}`);
  }
  if (Number(priceInfo?.id_precio) > 0) {
    parts.push(`Precio #${priceInfo.id_precio}`);
  }

  return parts.filter(Boolean).join(" | ");
};

const buildCheckoutItemSummaryRows = async ({
  items,
  priceMap,
  platInfoById,
  platNameById,
  pickPrecio,
  descuentos = [],
  discountColumns = [],
  discountColumnById = {},
  isCliente = true,
  totalUsd,
  montoBsTotal,
  tasaBs,
  customItemAmountMap = null,
}) => {
  const itemsList = Array.isArray(items) ? items : [];
  const ventaIds = uniqPositiveIds(itemsList.map((item) => item?.id_venta));
  const cuentaIds = uniqPositiveIds(itemsList.map((item) => item?.id_cuenta));
  const perfilIds = uniqPositiveIds(itemsList.map((item) => item?.id_perfil));

  const [
    { data: ventasData, error: ventasErr },
    { data: cuentasData, error: cuentasErr },
    { data: perfilesData, error: perfilesErr },
  ] = await Promise.all([
    ventaIds.length
      ? supabaseAdmin
          .from("ventas")
          .select(
            "id_venta, id_cuenta, id_cuenta_miembro, id_perfil, correo_miembro, cuentas:cuentas!ventas_id_cuenta_fkey(correo), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(correo), perfiles:perfiles(n_perfil)",
          )
          .in("id_venta", ventaIds)
      : Promise.resolve({ data: [], error: null }),
    cuentaIds.length
      ? supabaseAdmin
          .from("cuentas")
          .select("id_cuenta, correo")
          .in("id_cuenta", cuentaIds)
      : Promise.resolve({ data: [], error: null }),
    perfilIds.length
      ? supabaseAdmin
          .from("perfiles")
          .select("id_perfil, n_perfil")
          .in("id_perfil", perfilIds)
      : Promise.resolve({ data: [], error: null }),
  ]);
  if (ventasErr) throw ventasErr;
  if (cuentasErr) throw cuentasErr;
  if (perfilesErr) throw perfilesErr;

  const ventaMap = (ventasData || []).reduce((acc, venta) => {
    acc[venta.id_venta] = venta;
    return acc;
  }, {});
  const cuentaMap = (cuentasData || []).reduce((acc, cuenta) => {
    acc[cuenta.id_cuenta] = cuenta;
    return acc;
  }, {});
  const perfilMap = (perfilesData || []).reduce((acc, perfil) => {
    acc[perfil.id_perfil] = perfil;
    return acc;
  }, {});

  const totalUsdOverride = parseOptionalCheckoutNumber(totalUsd);
  const montoBsOverride = parseOptionalCheckoutNumber(montoBsTotal);
  const tasaBsValue = parseOptionalCheckoutNumber(tasaBs);
  const hasCustomAmounts = customItemAmountMap instanceof Map && customItemAmountMap.size > 0;
  const draftRows = itemsList.map((item) => {
    const priceInfo = priceMap?.[item?.id_precio] || {};
    const platId = Number(priceInfo?.id_plataforma) || null;
    const platformInfo = platId ? platInfoById?.[platId] || {} : {};
    const platformName = platId ? platNameById?.[platId] || `Plataforma ${platId}` : "Plataforma";
    const pricing = computeCheckoutItemPricing({
      item,
      priceInfo,
      platformInfo,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
    });
    return {
      id_item: toPositiveInt(item?.id_item) || null,
      id_precio: toPositiveInt(item?.id_precio) || null,
      id_plataforma: platId,
      renovacion: item?.renovacion === true,
      id_venta: toPositiveInt(item?.id_venta) || null,
      cantidad: Math.max(1, Number(item?.cantidad) || 1),
      meses: Math.max(1, Number(item?.meses) || 1),
      detalle: buildOrdenItemDetalle({
        item,
        priceInfo,
        platformInfo,
        platformName,
        cuentaMap,
        perfilMap,
        ventaMap,
      }),
      monto_original_usd: pricing.baseSubtotalUsd,
      descuento_usd: pricing.descuentoUsd,
      monto_base_usd: pricing.subtotalUsd,
    };
  });

  let rows = [];
  let resolvedTotalUsd = 0;
  let resolvedMontoBsTotal = null;

  if (hasCustomAmounts) {
    rows = draftRows.map((row) => ({
      ...row,
      monto_usd: customItemAmountMap.has(row.id_item)
        ? roundCheckoutMoney(customItemAmountMap.get(row.id_item))
        : roundCheckoutMoney(row.monto_base_usd),
    }));
    resolvedTotalUsd = roundCheckoutMoney(
      rows.reduce((sum, row) => sum + (Number(row?.monto_usd) || 0), 0),
    );
    resolvedMontoBsTotal = Number.isFinite(montoBsOverride)
      ? roundCheckoutMoney(montoBsOverride)
      : Number.isFinite(tasaBsValue)
        ? roundCheckoutMoney(resolvedTotalUsd * tasaBsValue)
        : null;
  } else {
    const baseTotalUsd = roundCheckoutMoney(
      draftRows.reduce((sum, row) => sum + (Number(row?.monto_base_usd) || 0), 0),
    );
    resolvedTotalUsd = Number.isFinite(totalUsdOverride)
      ? roundCheckoutMoney(totalUsdOverride)
      : baseTotalUsd;
    resolvedMontoBsTotal = Number.isFinite(montoBsOverride)
      ? roundCheckoutMoney(montoBsOverride)
      : Number.isFinite(resolvedTotalUsd) && Number.isFinite(tasaBsValue)
        ? roundCheckoutMoney(resolvedTotalUsd * tasaBsValue)
        : null;

    let usdAssigned = 0;
    rows = draftRows.map((row, index) => {
      const isLast = index === draftRows.length - 1;
      const ratio =
        baseTotalUsd > 0
          ? (Number(row?.monto_base_usd) || 0) / baseTotalUsd
          : draftRows.length
            ? 1 / draftRows.length
            : 0;
      const montoUsd = isLast
        ? roundCheckoutMoney(resolvedTotalUsd - usdAssigned)
        : roundCheckoutMoney(resolvedTotalUsd * ratio);
      usdAssigned = roundCheckoutMoney(usdAssigned + montoUsd);
      return {
        ...row,
        monto_usd: montoUsd,
      };
    });
  }

  let bsAssigned = 0;
  rows = rows.map((row, index) => {
    let montoBs = null;
    if (Number.isFinite(Number(resolvedMontoBsTotal))) {
      const isLast = index === rows.length - 1;
      const ratio =
        resolvedTotalUsd > 0
          ? (Number(row?.monto_usd) || 0) / resolvedTotalUsd
          : rows.length
            ? 1 / rows.length
            : 0;
      montoBs = isLast
        ? roundCheckoutMoney(resolvedMontoBsTotal - bsAssigned)
        : roundCheckoutMoney(resolvedMontoBsTotal * ratio);
      bsAssigned = roundCheckoutMoney(bsAssigned + montoBs);
    }
    return {
      ...row,
      monto_bs: montoBs,
    };
  });

  return {
    rows,
    totalUsd: resolvedTotalUsd,
    montoBsTotal: resolvedMontoBsTotal,
  };
};

const syncOrdenItemsSnapshot = async ({
  ordenId,
  items,
  priceMap,
  platInfoById,
  platNameById,
  pickPrecio,
  descuentos = [],
  discountColumns = [],
  discountColumnById = {},
  isCliente = true,
  totalUsd,
  montoBsTotal,
  tasaBs,
  customItemAmountMap = null,
}) => {
  const orderIdNum = Number(ordenId);
  if (!Number.isFinite(orderIdNum) || orderIdNum <= 0) return;
  const { rows } = await buildCheckoutItemSummaryRows({
    items,
    priceMap,
    platInfoById,
    platNameById,
    pickPrecio,
    descuentos,
    discountColumns,
    discountColumnById,
    isCliente,
    totalUsd,
    montoBsTotal,
    tasaBs,
    customItemAmountMap,
  });

  const { error: delErr } = await supabaseAdmin
    .from("ordenes_items")
    .delete()
    .eq("id_orden", orderIdNum);
  if (delErr) throw delErr;

  if (!rows.length) return;

  const snapshotRows = rows.map((row) => ({
    id_orden: orderIdNum,
    id_plataforma: row.id_plataforma,
    renovacion: row.renovacion,
    id_venta: toPositiveInt(row?.id_venta) || null,
    detalle: row.detalle,
    monto_usd: row.monto_usd,
    monto_bs: row.monto_bs,
  }));

  const { error: insErr } = await supabaseAdmin
    .from("ordenes_items")
    .insert(snapshotRows);
  if (insErr) throw insErr;
};

const resolveMontoBaseCarrito = async ({ carritoId, fallbackTotal }) => {
  const fallback = Number.isFinite(Number(fallbackTotal)) ? Number(fallbackTotal) : 0;
  const carritoNum = Number(carritoId);
  if (!Number.isFinite(carritoNum) || carritoNum <= 0) return fallback;
  const { data: carritoRow, error: cartErr } = await supabaseAdmin
    .from("carritos")
    .select("monto_usd, monto_final, usa_saldo")
    .eq("id_carrito", carritoNum)
    .maybeSingle();
  if (cartErr) throw cartErr;
  if (!carritoRow) return fallback;
  const usaSaldo = isTrue(carritoRow?.usa_saldo);
  const montoFinal = Number(carritoRow?.monto_final);
  const montoUsd = Number(carritoRow?.monto_usd);
  if (usaSaldo && Number.isFinite(montoFinal) && montoFinal >= 0) {
    return montoFinal;
  }
  if (Number.isFinite(montoUsd) && montoUsd >= 0) {
    return montoUsd;
  }
  return fallback;
};

const processOrderFromItems = async ({
  ordenId,
  idUsuarioSesion,
  idUsuarioVentas,
  historialRegistradoPor = null,
  adminInvolucradoId = null,
  items,
  priceMap,
  platInfoById,
  platNameById,
  pickPrecio,
  descuentos = [],
  discountColumns = [],
  discountColumnById = {},
  isCliente = true,
  referencia,
  archivos,
  id_metodo_de_pago,
  carritoId,
  montoHistorialTotalOverride,
  itemAmountMapById = null,
  snapshotTotalUsd = null,
  snapshotMontoBsTotal = null,
  snapshotTasaBs = null,
  saldoAplicadoExpectedUsd = 0,
  montoExtraHistorialUsd = 0,
  allowSaldoInsuficiente = false,
}) => {
  console.log("[checkout] processOrderFromItems start", {
    ordenId,
    idUsuarioSesion,
    idUsuarioVentas,
    itemsCount: items?.length || 0,
    carritoId,
  });
  let saldoConsumption = null;
  try {
    saldoConsumption = await consumeSaldoFromCarritoIfNeeded({
      carritoId,
      source: "processOrderFromItems",
      saldoAplicadoFallback: saldoAplicadoExpectedUsd,
      idUsuarioFallback: idUsuarioVentas,
    });
    const saldoExpected = toFiniteMoney(saldoAplicadoExpectedUsd);
    if (!saldoConsumption?.consumed && Number.isFinite(saldoExpected) && saldoExpected > 0) {
      const skipReason = String(saldoConsumption?.reason || "");
      const canForceDebit = skipReason === "saldo_not_enabled" || skipReason === "saldo_amount_zero";
      if (canForceDebit) {
        const forcedDebit = await debitSaldoUsdFromUser({
          idUsuario: idUsuarioVentas,
          montoUsd: saldoExpected,
          source: `processOrderFromItems:forced_${skipReason || "unknown"}`,
        });
        saldoConsumption = {
          consumed: !!forcedDebit?.debitado,
          id_carrito: Number.isFinite(Number(carritoId)) ? Number(carritoId) : null,
          id_usuario: forcedDebit?.id_usuario || toPositiveInt(idUsuarioVentas) || null,
          monto: Number(forcedDebit?.monto) || 0,
          saldo_aplicado: saldoExpected,
          saldo_nuevo: forcedDebit?.saldoNuevo ?? null,
          saldo_origen: `forced_${skipReason || "unknown"}`,
        };
        console.log("[checkout] saldo forzado por monto esperado", {
          ordenId,
          id_carrito: carritoId,
          reason: skipReason || "unknown",
          id_usuario: saldoConsumption?.id_usuario || null,
          monto: saldoConsumption?.monto || 0,
          saldo_nuevo: saldoConsumption?.saldo_nuevo ?? null,
        });
      }
    }
  } catch (saldoErr) {
    if (allowSaldoInsuficiente === true && isSaldoInsuficienteError(saldoErr)) {
      console.warn("[checkout] saldo insuficiente ignorado durante reproceso de orden", {
        id_orden: toPositiveInt(ordenId) || null,
        id_usuario: toPositiveInt(idUsuarioVentas) || null,
        id_carrito: toPositiveInt(carritoId) || null,
        saldo_aplicado_esperado: toFiniteMoney(saldoAplicadoExpectedUsd) || 0,
        error: saldoErr?.message || String(saldoErr || ""),
      });
      saldoConsumption = {
        consumed: false,
        skipped: true,
        reason: "saldo_insuficiente_ignored",
        id_carrito: toPositiveInt(carritoId) || null,
        id_usuario: toPositiveInt(idUsuarioVentas) || null,
      };
    } else {
      throw saldoErr;
    }
  }
  if (saldoConsumption?.consumed) {
    console.log("[checkout] saldo consumido en procesamiento", {
      ordenId,
      id_carrito: carritoId,
      id_usuario: saldoConsumption?.id_usuario || null,
      monto: saldoConsumption?.monto || 0,
      saldo_nuevo: saldoConsumption?.saldo_nuevo ?? null,
    });
  }
  const isoHoy = todayInVenezuela();
  const historialRegistradoPorId =
    Number.isFinite(Number(historialRegistradoPor)) && Number(historialRegistradoPor) > 0
      ? Math.trunc(Number(historialRegistradoPor))
      : null;
  const referenciaNum = Number.isFinite(Number(referencia)) ? Number(referencia) : null;
  const archivosArr = Array.isArray(archivos) ? archivos : [];
  const comprobanteHist = archivosArr?.[0] || null;
  const isCuentaCompletaByFlags = (cuentaRow) =>
    !!cuentaRow && !isTrue(cuentaRow?.venta_perfil) && !isTrue(cuentaRow?.venta_miembro);
  const cuentaFlagsById = {};
  const hasCustomItemAmounts = itemAmountMapById instanceof Map && itemAmountMapById.size > 0;
  const getLineAmountForItem = (item, fallbackAmount = 0) => {
    const itemId = toPositiveInt(item?.id_item);
    if (itemId && hasCustomItemAmounts && itemAmountMapById.has(itemId)) {
      return roundCheckoutMoney(itemAmountMapById.get(itemId));
    }
    return roundCheckoutMoney(fallbackAmount);
  };
  const buildDistributedAmounts = (totalAmount, count) => {
    const safeCount = Math.max(0, Number(count) || 0);
    if (safeCount <= 0) return [];
    const resolvedTotal = roundCheckoutMoney(totalAmount);
    let assigned = 0;
    return Array.from({ length: safeCount }, (_row, index) => {
      const isLast = index === safeCount - 1;
      const value = isLast
        ? roundCheckoutMoney(resolvedTotal - assigned)
        : roundCheckoutMoney(resolvedTotal / safeCount);
      assigned = roundCheckoutMoney(assigned + value);
      return value;
    });
  };

  // Renovaciones (no asignan stock nuevo)
  // Se consolidan por id_venta para evitar dobles updates de la misma venta.
  const renovacionesRaw = (items || []).filter((it) => it.renovacion === true && it.id_venta);
  const renewalGroupsByVenta = new Map();
  for (const renewalItem of renovacionesRaw) {
    const ventaId = toPositiveInt(renewalItem?.id_venta);
    if (!ventaId) continue;
    const price = priceMap[renewalItem.id_precio] || {};
    const platId = Number(price?.id_plataforma) || null;
    const platformInfo = platId ? platInfoById?.[platId] || {} : {};
    const montoLinea = getLineAmountForItem(
      renewalItem,
      computeCheckoutItemBaseAmount({
        item: renewalItem,
        priceInfo: price,
        platformInfo,
        pickPrecio,
        descuentos,
        discountColumns,
        discountColumnById,
        isCliente,
      }),
    );
    const qtyVal =
      Number.isFinite(Number(renewalItem?.cantidad)) && Number(renewalItem?.cantidad) > 0
        ? Math.max(1, Math.round(Number(renewalItem.cantidad)))
        : 1;
    const mesesVal =
      Number.isFinite(Number(renewalItem?.meses)) && Number(renewalItem?.meses) > 0
        ? Math.max(1, Math.round(Number(renewalItem.meses)))
        : 1;
    const mesesContribution = qtyVal * mesesVal;
    const precioId = toPositiveInt(renewalItem?.id_precio);

    if (renewalGroupsByVenta.has(ventaId)) {
      const existing = renewalGroupsByVenta.get(ventaId);
      if (
        existing?.id_precio &&
        precioId &&
        Number(existing.id_precio) !== Number(precioId)
      ) {
        console.warn("[checkout] renovacion con id_precio distinto para misma venta; se acumula", {
          ordenId,
          id_venta: ventaId,
          id_precio_prev: existing.id_precio,
          id_precio_new: precioId,
        });
      }
      existing.meses += mesesContribution;
      existing.monto = roundCheckoutMoney((Number(existing?.monto) || 0) + (Number(montoLinea) || 0));
      existing.items_count += 1;
      if (!existing.id_precio && precioId) existing.id_precio = precioId;
      if (!existing.id_plataforma && platId) existing.id_plataforma = platId;
    } else {
      renewalGroupsByVenta.set(ventaId, {
        id_venta: ventaId,
        id_precio: precioId,
        id_plataforma: platId,
        meses: mesesContribution,
        monto: roundCheckoutMoney(montoLinea),
        items_count: 1,
      });
    }
  }
  const renovaciones = Array.from(renewalGroupsByVenta.values()).map((row) => ({
    ...row,
    meses:
      Number.isFinite(Number(row?.meses)) && Number(row?.meses) > 0
        ? Math.max(1, Math.round(Number(row.meses)))
        : 1,
    monto: roundCheckoutMoney(row?.monto),
  }));
  const idsVentasRenovar = renovaciones.map((r) => toPositiveInt(r?.id_venta)).filter(Boolean);
  const ventaMap = {};
  if (idsVentasRenovar.length) {
    const { data: ventasExistentes, error: ventErr } = await supabaseAdmin
      .from("ventas")
      .select(
        "id_venta, fecha_corte, id_cuenta, id_cuenta_miembro, id_usuario, suspendido, completa, cuenta_principal:cuentas!ventas_id_cuenta_fkey(id_cuenta, venta_perfil, venta_miembro, correo, clave), cuenta_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, venta_perfil, venta_miembro, correo, clave)"
      )
      .in("id_venta", idsVentasRenovar);
    if (ventErr) throw ventErr;
    (ventasExistentes || []).forEach((v) => {
      ventaMap[v.id_venta] = v;
    });
  }

  let renovacionesPendientesCount = 0;
  const appliedRenovaciones = [];
  const explicitRenewalAppliedVentaIds = new Set();
  if (toPositiveInt(ordenId) && idsVentasRenovar.length) {
    const { data: explicitRenewRows, error: explicitRenewErr } = await supabaseAdmin
      .from("historial_ventas")
      .select("id_venta")
      .eq("id_orden", ordenId)
      .eq("renovacion", true)
      .in("id_venta", idsVentasRenovar);
    if (explicitRenewErr) throw explicitRenewErr;
    (explicitRenewRows || []).forEach((row) => {
      const appliedVentaId = toPositiveInt(row?.id_venta);
      if (appliedVentaId) explicitRenewalAppliedVentaIds.add(appliedVentaId);
    });
  }
  for (const it of renovaciones) {
    const renewalVentaId = toPositiveInt(it?.id_venta);
    if (renewalVentaId && explicitRenewalAppliedVentaIds.has(renewalVentaId)) {
      console.warn("[checkout] renovacion omitida por historial existente", {
        ordenId,
        id_venta: renewalVentaId,
      });
      continue;
    }
    const price = priceMap[it.id_precio] || {};
    const platId = Number(it?.id_plataforma || price?.id_plataforma) || null;
    const monto = roundCheckoutMoney(Number(it?.monto) || 0);
    const mesesVal =
      Number.isFinite(Number(it.meses)) && Number(it.meses) > 0 ? Math.round(Number(it.meses)) : 1;
    const ventaAnt = ventaMap[it.id_venta] || {};
    const isSuspendidaAnt = isTrue(ventaAnt?.suspendido);
    const cuentaVenta = ventaAnt?.cuenta_principal || ventaAnt?.cuenta_miembro || null;
    const isCuentaCompletaRenov =
      isCuentaCompletaByFlags(cuentaVenta) ||
      (isTrue(price?.completa) && !isTrue(price?.sub_cuenta));
    const isVentaCompletaRenov = isTrue(ventaAnt?.completa);
    const fechaBaseSrc = ventaAnt?.fecha_corte || isoHoy;
    const fecha_corte = addMonthsKeepDay(fechaBaseSrc, mesesVal) || isoHoy;
    const updatePayload = {
      fecha_pago: isoHoy,
      fecha_corte,
      monto,
      renovacion: true,
      meses_contratados: mesesVal,
      recordatorio_enviado: false,
      recordatorio_corte_enviado: false,
      aviso_admin: false,
      pendiente: false,
      suspendido: false,
    };
    if (isSuspendidaAnt) {
      updatePayload.cuenta_nueva = false;
    }
    if (isCuentaCompletaRenov || isVentaCompletaRenov) {
      if (platId === 9) {
        updatePayload.cuenta_pagada_admin = true;
      } else {
        updatePayload.cuenta_pagada_admin = false;
      }
    }
    const { data: updatedRows, error: renovErr } = await supabaseAdmin
      .from("ventas")
      .update(updatePayload)
      .eq("id_venta", it.id_venta)
      .select("id_venta");
    if (renovErr) throw renovErr;
    if ((updatedRows || []).length > 0) {
      appliedRenovaciones.push(it);
      if (renewalVentaId) explicitRenewalAppliedVentaIds.add(renewalVentaId);
    } else {
      console.warn("[checkout] renovacion omitida por idempotencia", {
        ordenId,
        id_venta: toPositiveInt(it?.id_venta) || null,
      });
    }
  }
  // Filtra items nuevos (no renovaciones) para asignación de stock
  const itemsNuevos = (items || []).filter((it) => !it.id_venta);
  if (!itemsNuevos.length) {
    console.log("[checkout] processOrderFromItems sin items nuevos", {
      ordenId,
      renovaciones: renovaciones.length,
    });
  }

  // Verificación de stock y asignación de recursos
  const asignaciones = [];
  const pendientes = [];
  const subCuentasAsignadas = [];
  const { data: reemplazosRows, error: reemplazosErr } = await supabaseAdmin
    .from("reemplazos")
    .select("id_cuenta, id_perfil");
  if (reemplazosErr) throw reemplazosErr;
  const reemplazosBloqueados = buildReemplazosBlocklist(reemplazosRows);
  let spotifyPreferredMotherId = null;
  let spotifyPreferredMotherResolved = false;
  const resolveSpotifyPreferredMotherId = async () => {
    if (spotifyPreferredMotherResolved) return spotifyPreferredMotherId;
    spotifyPreferredMotherResolved = true;
    const usuarioId = toPositiveInt(idUsuarioVentas);
    if (!usuarioId) return spotifyPreferredMotherId;

    const { data: rows, error } = await supabaseAdmin
      .from("ventas")
      .select(
        "id_venta, id_cuenta, cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, id_plataforma, cuenta_madre, inactiva), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, id_cuenta_madre)"
      )
      .eq("id_usuario", usuarioId)
      .order("id_venta", { ascending: false })
      .limit(180);
    if (error) throw error;

    for (const row of rows || []) {
      const madreDirectaId = toPositiveInt(row?.cuentas?.id_cuenta);
      const madreDirectaValida =
        madreDirectaId &&
        Number(row?.cuentas?.id_plataforma) === 9 &&
        isTrue(row?.cuentas?.cuenta_madre) &&
        !isInactive(row?.cuentas?.inactiva) &&
        !reemplazosBloqueados.cuentas.has(madreDirectaId);
      if (madreDirectaValida) {
        spotifyPreferredMotherId = madreDirectaId;
        return spotifyPreferredMotherId;
      }

      const madreDesdeMiembroId = toPositiveInt(row?.cuentas_miembro?.id_cuenta_madre);
      if (madreDesdeMiembroId && !reemplazosBloqueados.cuentas.has(madreDesdeMiembroId)) {
        spotifyPreferredMotherId = madreDesdeMiembroId;
        return spotifyPreferredMotherId;
      }
    }
    return spotifyPreferredMotherId;
  };
  const stockScanLimit = (cantidadReq) => {
    const qty = Number.isFinite(Number(cantidadReq)) ? Number(cantidadReq) : 1;
    return Math.max(40, qty * 10);
  };
  for (const it of itemsNuevos) {
    const price = priceMap[it.id_precio];
    if (!price) {
      throw new Error(`Precio no encontrado para item ${it.id_precio}`);
    }
    const cantidad = it.cantidad || 0;
    if (cantidad <= 0) continue;
    const platId = Number(price.id_plataforma) || null;
    const isGiftCardSale = isTrue(platInfoById[platId]?.tarjeta_de_regalo);
    const entregaInmediata = isTrue(platInfoById[platId]?.entrega_inmediata);
    const cuentaMadrePlat = isTrue(platInfoById[platId]?.cuenta_madre);
    const pendienteVenta = !entregaInmediata || cuentaMadrePlat;
    const mesesItemRaw = it.meses || 1;
    const mesesItem = Number.isFinite(Number(mesesItemRaw))
      ? Math.max(1, Math.round(Number(mesesItemRaw)))
      : 1;
    console.log("[checkout] item", it, "mesesRaw", mesesItemRaw, "meses", mesesItem);

    const priceId = Number(price.id_precio) || Number(it.id_precio) || null;
    const isNetflixPlan2 = platId === 1 && [4, 5].includes(priceId);
    console.log("[checkout] asignacion start", {
      id_precio: price?.id_precio,
      id_plataforma: platId,
      completa: price?.completa,
      isNetflixPlan2,
      cantidad,
    });

    // Si la plataforma no es de entrega inmediata, la venta debe permanecer pendiente
    // y la asignación/entrega se realiza manualmente desde pedidos pendientes.
    if (!entregaInmediata && !isGiftCardSale) {
      for (let i = 0; i < cantidad; i += 1) {
        pendientes.push({
          id_item_carrito: toPositiveInt(it?.id_item) || null,
          id_precio: price.id_precio,
          monto: pickPrecio(price, it),
          id_cuenta: null,
          id_perfil: null,
          id_sub_cuenta: null,
          meses: mesesItem,
          pendiente: true,
        });
      }
      console.log("[checkout] item marcado manual (sin auto-asignacion)", {
        id_precio: price?.id_precio,
        id_plataforma: platId,
        cantidad,
      });
      continue;
    }

    if (isGiftCardSale) {
      const giftCardsStock = await fetchAvailableGiftCardStock({
        idPlataforma: platId,
        valorTarjetaDeRegalo: price?.valor_tarjeta_de_regalo,
        limit: stockScanLimit(cantidad),
      });
      const disponibles = (giftCardsStock || []).filter((row) => toPositiveInt(row?.id_tarjeta_de_regalo));
      const faltantes = Math.max(0, cantidad - disponibles.length);
      disponibles.slice(0, cantidad).forEach((row) => {
        asignaciones.push({
          id_item_carrito: toPositiveInt(it?.id_item) || null,
          id_precio: price.id_precio,
          monto: pickPrecio(price, it),
          id_cuenta: null,
          id_perfil: null,
          id_sub_cuenta: null,
          id_tarjeta_de_regalo: row.id_tarjeta_de_regalo,
          meses: 1,
          pendiente: false,
        });
      });
      if (faltantes > 0) {
        for (let i = 0; i < faltantes; i += 1) {
          pendientes.push({
            id_item_carrito: toPositiveInt(it?.id_item) || null,
            id_precio: price.id_precio,
            monto: pickPrecio(price, it),
            id_cuenta: null,
            id_perfil: null,
            id_sub_cuenta: null,
            id_tarjeta_de_regalo: null,
            meses: 1,
            pendiente: true,
          });
        }
      }
    } else if (price.completa) {
      let cuentasQuery = supabaseAdmin
        .from("cuentas")
        .select("id_cuenta, id_plataforma, ocupado, inactiva")
        .eq("id_plataforma", platId)
        .eq("venta_perfil", false)
        .eq("venta_miembro", false)
        .eq("ocupado", false)
        .or("inactiva.is.null,inactiva.eq.false");
      if (entregaInmediata) {
        cuentasQuery = cuentasQuery.or("venta_dominio.is.null,venta_dominio.eq.false");
      }
      const { data: cuentasLibres, error: ctaErr } = await cuentasQuery.limit(
        stockScanLimit(cantidad)
      );
      if (ctaErr) throw ctaErr;
      const disponibles = (cuentasLibres || []).filter((c) => {
        const cuentaId = toPositiveInt(c?.id_cuenta);
        return (
          !isInactive(c?.inactiva) &&
          !!cuentaId &&
          !reemplazosBloqueados.cuentas.has(cuentaId)
        );
      });
      const faltantes = Math.max(0, cantidad - disponibles.length);
      disponibles.slice(0, cantidad).forEach((cta) => {
        asignaciones.push({
          id_item_carrito: toPositiveInt(it?.id_item) || null,
          id_precio: price.id_precio,
          monto: pickPrecio(price, it),
          id_cuenta: cta.id_cuenta,
          id_perfil: null,
          id_sub_cuenta: null,
          meses: mesesItem,
          pendiente: pendienteVenta,
        });
      });
      if (faltantes > 0) {
        for (let i = 0; i < faltantes; i += 1) {
          pendientes.push({
            id_item_carrito: toPositiveInt(it?.id_item) || null,
            id_precio: price.id_precio,
            monto: pickPrecio(price, it),
            id_cuenta: null,
            id_perfil: null,
            id_sub_cuenta: null,
            meses: mesesItem,
            pendiente: true,
          });
        }
      }
    } else if (isNetflixPlan2) {
      const usedPerfiles = [];
      const { data: perfilesHogar, error: perfErr } = await supabaseAdmin
        .from("perfiles")
        .select(
          "id_perfil, id_cuenta, ocupado, perfil_hogar, cuentas!perfiles_id_cuenta_fkey!inner(id_plataforma, inactiva, venta_perfil)",
        )
        .eq("perfil_hogar", true)
        .eq("cuentas.id_plataforma", platId)
        .eq("cuentas.venta_perfil", true)
        .eq("ocupado", false)
        .or("inactiva.is.null,inactiva.eq.false", { foreignTable: "cuentas" })
        .limit(stockScanLimit(cantidad));
      if (perfErr) throw perfErr;
      const libresHogar = (perfilesHogar || []).filter(
        (p) => {
          const perfilId = toPositiveInt(p?.id_perfil);
          return (
            !isInactive(p?.cuentas?.inactiva) &&
            p?.ocupado === false &&
            !!perfilId &&
            !reemplazosBloqueados.perfiles.has(perfilId)
          );
        }
      );
      const takeHogar = libresHogar.slice(0, cantidad);
      takeHogar.forEach((p) => {
        asignaciones.push({
          id_item_carrito: toPositiveInt(it?.id_item) || null,
          id_precio: price.id_precio,
          monto: pickPrecio(price, it),
          id_cuenta: p.id_cuenta,
          id_perfil: p.id_perfil,
          id_sub_cuenta: null,
          meses: mesesItem,
          pendiente: pendienteVenta,
        });
        usedPerfiles.push(p.id_perfil);
      });

      const faltantesPerf = Math.max(0, cantidad - usedPerfiles.length);
      if (faltantesPerf > 0) {
        const { data: cuentasMiembro, error: ctaMiembroErr } = await supabaseAdmin
          .from("cuentas")
          .select("id_cuenta, ocupado, inactiva, venta_miembro, venta_perfil")
          .eq("id_plataforma", platId)
          .eq("venta_perfil", false)
          .eq("venta_miembro", true)
          .eq("ocupado", false)
          .or("inactiva.is.null,inactiva.eq.false")
          .limit(stockScanLimit(faltantesPerf));
        if (ctaMiembroErr) throw ctaMiembroErr;
        const cuentasLibres = (cuentasMiembro || []).filter((c) => {
          const cuentaId = toPositiveInt(c?.id_cuenta);
          return (
            c?.inactiva === false &&
            c?.ocupado === false &&
            !!cuentaId &&
            !reemplazosBloqueados.cuentas.has(cuentaId)
          );
        });
        const takeCtas = cuentasLibres.slice(0, faltantesPerf);
        takeCtas.forEach((cta) => {
          asignaciones.push({
            id_item_carrito: toPositiveInt(it?.id_item) || null,
            id_precio: price.id_precio,
            monto: pickPrecio(price, it),
            id_cuenta: cta.id_cuenta,
            id_perfil: null,
            id_sub_cuenta: null,
            meses: mesesItem,
            pendiente: pendienteVenta,
          });
        });
        const faltantesPerf2 = Math.max(0, faltantesPerf - takeCtas.length);
        if (faltantesPerf2 > 0) {
          for (let i = 0; i < faltantesPerf2; i += 1) {
            pendientes.push({
              id_item_carrito: toPositiveInt(it?.id_item) || null,
              id_precio: price.id_precio,
              monto: pickPrecio(price, it),
              id_cuenta: null,
              id_perfil: null,
              id_sub_cuenta: null,
              meses: mesesItem,
              pendiente: true,
            });
          }
        }
      }
    } else {
      const isSpotify = platId === 9;
      let perfilesQuery = supabaseAdmin
        .from("perfiles")
        .select(
          "id_perfil, id_cuenta, n_perfil, perfil_hogar, cuentas!perfiles_id_cuenta_fkey!inner(id_plataforma, inactiva, venta_perfil, cuenta_madre)"
        )
        .eq("cuentas.id_plataforma", platId)
        .eq("cuentas.venta_perfil", isSpotify ? false : true)
        .eq("perfil_hogar", false)
        .eq("ocupado", false)
        .or("inactiva.is.null,inactiva.eq.false", { foreignTable: "cuentas" })
        .limit(stockScanLimit(cantidad));
      perfilesQuery = isSpotify
        ? perfilesQuery.eq("cuentas.cuenta_madre", true)
        : perfilesQuery.or("cuenta_madre.is.null,cuenta_madre.eq.false", { foreignTable: "cuentas" });
      const { data: perfilesLibres, error: perfErr } = await perfilesQuery;
      if (perfErr) throw perfErr;
      console.log("[checkout] perfiles libres raw", {
        platId,
        count: perfilesLibres?.length || 0,
        first: perfilesLibres?.[0] || null,
      });
      if (platId === 1 || platId === 9) {
        console.log("[checkout][netflix] filtros", {
          platId,
          venta_perfil: isSpotify ? false : true,
          cuenta_madre: isSpotify ? true : false,
          perfil_hogar: false,
          ocupado: false,
          inactiva: "null|false",
        });
        const rawSample = (perfilesLibres || []).slice(0, 5).map((p) => ({
          id_perfil: p.id_perfil,
          id_cuenta: p.id_cuenta,
          perfil_hogar: p.perfil_hogar,
          ocupado: p.ocupado,
          cuenta_plat: p.cuentas?.id_plataforma,
          cuenta_inactiva: p.cuentas?.inactiva,
          cuenta_venta_perfil: p.cuentas?.venta_perfil,
          cuenta_madre: p.cuentas?.cuenta_madre,
        }));
        console.log("[checkout][stock] raw sample", rawSample);
      }
      const disponibles = (perfilesLibres || []).filter((p) => {
        const perfilId = toPositiveInt(p?.id_perfil);
        return (
          !isInactive(p?.cuentas?.inactiva) &&
          !!perfilId &&
          !reemplazosBloqueados.perfiles.has(perfilId)
        );
      });
      const preferredMotherId = isSpotify ? await resolveSpotifyPreferredMotherId() : null;
      const disponiblesPriorizados = isSpotify
        ? orderSpotifyProfilesByPriority(disponibles, preferredMotherId)
        : disponibles;
      console.log("[checkout] perfiles libres disponibles", {
        platId,
        count: disponiblesPriorizados.length,
        first: disponiblesPriorizados[0] || null,
        preferredMotherId: preferredMotherId || null,
      });
      if (platId === 1 || platId === 9) {
        const dispSample = disponiblesPriorizados.slice(0, 5).map((p) => ({
          id_perfil: p.id_perfil,
          id_cuenta: p.id_cuenta,
          n_perfil: p.n_perfil,
          perfil_hogar: p.perfil_hogar,
          ocupado: p.ocupado,
          cuenta_inactiva: p.cuentas?.inactiva,
        }));
        console.log("[checkout][stock] disponibles sample", dispSample);
      }
      const faltantes = Math.max(0, cantidad - disponiblesPriorizados.length);
      disponiblesPriorizados.slice(0, cantidad).forEach((p) => {
        asignaciones.push({
          id_item_carrito: toPositiveInt(it?.id_item) || null,
          id_precio: price.id_precio,
          monto: pickPrecio(price, it),
          id_cuenta: p.id_cuenta,
          id_perfil: p.id_perfil,
          id_sub_cuenta: null,
          meses: mesesItem,
          pendiente: pendienteVenta,
        });
      });
      if (faltantes > 0) {
        for (let i = 0; i < faltantes; i += 1) {
          pendientes.push({
            id_item_carrito: toPositiveInt(it?.id_item) || null,
            id_precio: price.id_precio,
            monto: pickPrecio(price, it),
            id_cuenta: null,
            id_perfil: null,
            id_sub_cuenta: null,
            meses: mesesItem,
            pendiente: true,
          });
        }
      }
    }
    console.log("[checkout] asignacion end", {
      id_precio: price?.id_precio,
      platId,
      asignaciones: asignaciones.map((a) => ({
        id_precio: a.id_precio,
        id_cuenta: a.id_cuenta,
        id_perfil: a.id_perfil,
      })),
    });
  }

  // Validación final: jamás usar cuentas inactivas
  const assignedCuentaIds = Array.from(new Set(asignaciones.map((a) => a.id_cuenta).filter(Boolean)));
  const assignedPerfilIds = Array.from(new Set(asignaciones.map((a) => a.id_perfil).filter(Boolean)));
  const hasBlockedPerfil = (asignaciones || []).some((a) => {
    const perfilId = toPositiveInt(a?.id_perfil);
    return !!perfilId && reemplazosBloqueados.perfiles.has(perfilId);
  });
  if (hasBlockedPerfil) {
    throw new Error("Se intentó asignar un perfil bloqueado por reemplazo.");
  }
  const hasBlockedCuenta = (asignaciones || []).some((a) => {
    const perfilId = toPositiveInt(a?.id_perfil);
    if (perfilId) return false;
    const cuentaId = toPositiveInt(a?.id_cuenta);
    return !!cuentaId && reemplazosBloqueados.cuentas.has(cuentaId);
  });
  if (hasBlockedCuenta) {
    throw new Error("Se intentó asignar una cuenta bloqueada por reemplazo.");
  }
  if (assignedCuentaIds.length) {
    const { data: cuentasAsignadas, error: ctaValErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, id_plataforma, inactiva, venta_perfil, venta_miembro, correo, clave")
      .in("id_cuenta", assignedCuentaIds);
    if (ctaValErr) throw ctaValErr;
    const bad = (cuentasAsignadas || []).find((c) => isInactive(c.inactiva));
    if (bad) {
      throw new Error("Se intentó asignar una cuenta inactiva.");
    }
    (cuentasAsignadas || []).forEach((c) => {
      cuentaFlagsById[c.id_cuenta] = c;
    });
    const cuentaPlatMap = (cuentasAsignadas || []).reduce((acc, c) => {
      acc[c.id_cuenta] = c.id_plataforma;
      return acc;
    }, {});
    const badPlat = (asignaciones || []).find((a) => {
      const platId = priceMap[a.id_precio]?.id_plataforma || null;
      const cuentaPlat = cuentaPlatMap[a.id_cuenta];
      return platId && cuentaPlat && Number(cuentaPlat) !== Number(platId);
    });
    if (badPlat) {
      throw new Error("Asignación con plataforma incorrecta.");
    }
  }
  if (assignedPerfilIds.length) {
    const { data: perfilesAsignados, error: perfValErr } = await supabaseAdmin
      .from("perfiles")
      .select("id_perfil, cuentas:cuentas!perfiles_id_cuenta_fkey(inactiva)")
      .in("id_perfil", assignedPerfilIds);
    if (perfValErr) throw perfValErr;
    const bad = (perfilesAsignados || []).find((p) => isInactive(p?.cuentas?.inactiva));
    if (bad) {
      throw new Error("Se intentó asignar una cuenta inactiva.");
    }
  }

  const lineDistributedAmountsByItemId = new Map();
  const rowsByItemId = new Map();
  [...asignaciones, ...pendientes].forEach((row) => {
    const itemId = toPositiveInt(row?.id_item_carrito);
    if (!itemId) return;
    const bucket = rowsByItemId.get(itemId) || [];
    bucket.push(row);
    rowsByItemId.set(itemId, bucket);
  });
  items.forEach((item) => {
    const itemId = toPositiveInt(item?.id_item);
    if (!itemId) return;
    const itemRows = rowsByItemId.get(itemId) || [];
    const defaultLineAmount = computeCheckoutItemBaseAmount({
      item,
      priceInfo: priceMap[item.id_precio] || {},
      platformInfo:
        platInfoById[Number(priceMap[item.id_precio]?.id_plataforma) || 0] || {},
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
    });
    const lineAmount = getLineAmountForItem(item, defaultLineAmount);
    lineDistributedAmountsByItemId.set(itemId, buildDistributedAmounts(lineAmount, itemRows.length));
  });

  const sourceRowsForVentas = [...asignaciones, ...pendientes].map((row, index) => ({
    ...row,
    __source_row_idx: index,
    __is_asignacion: index < asignaciones.length,
  }));
  const sourceRowsConvertedToRenewals = new Set();
  const sourceRowsForInsertedVentas = [];
  const sourceRowsFinalAmountByIdx = new Map();
  const sourceRowsInsertedVentaByIdx = new Map();
  const sourceRowsImplicitRenewalByIdx = new Map();
  const spotifyImplicitRenewals = [];
  const implicitRenewalItemIds = new Set();
  const normalizeComparableEmail = (value) => String(value || "").trim().toLowerCase();
  const explicitRenewalVentaIdsSet = new Set(
    idsVentasRenovar.map((id) => toPositiveInt(id)).filter((id) => id > 0),
  );
  const spotifyRenewalLookupEmails = Array.from(
    new Set(
      sourceRowsForVentas
        .map((row) => {
          const priceInfo = priceMap[row?.id_precio] || {};
          const platId = Number(priceInfo?.id_plataforma) || null;
          if (platId !== 9) return "";
          if (isTrue(row?.pendiente)) return "";
          const cuentaId = toPositiveInt(row?.id_cuenta);
          if (!cuentaId) return "";
          return normalizeComparableEmail(cuentaFlagsById[cuentaId]?.correo);
        })
        .filter(Boolean),
    ),
  );
  const spotifyRenewalLookupEmailSet = new Set(spotifyRenewalLookupEmails);
  const spotifyCandidateSalesByCorreo = new Map();
  if (spotifyRenewalLookupEmails.length && toPositiveInt(idUsuarioVentas)) {
    const spotifyCandidateSalesRows = await fetchAllSupabaseRows(
      (from, to) =>
        supabaseAdmin
          .from("ventas")
          .select(
            "id_venta, id_usuario, id_precio, fecha_corte, id_cuenta, id_cuenta_miembro, suspendido, pendiente, pago_rechazado, completa, correo_miembro, cuenta_principal:cuentas!ventas_id_cuenta_fkey(id_cuenta, venta_perfil, venta_miembro, correo, clave), cuenta_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, venta_perfil, venta_miembro, correo, clave), precios:precios!ventas_id_precio_fkey(id_plataforma)",
          )
          .eq("id_usuario", toPositiveInt(idUsuarioVentas))
          .not("correo_miembro", "is", null)
          .order("id_venta", { ascending: false })
          .range(from, to),
      { label: `checkout:spotify_implicit_renewal_candidates:${ordenId}` },
    );

    (spotifyCandidateSalesRows || []).forEach((sale) => {
      const saleId = toPositiveInt(sale?.id_venta);
      if (!saleId || explicitRenewalVentaIdsSet.has(saleId)) return;
      if (isTrue(sale?.pendiente) || isTrue(sale?.pago_rechazado)) return;
      const salePlatId = Number(sale?.precios?.id_plataforma) || null;
      if (salePlatId !== 9) return;
      const correoNorm = normalizeComparableEmail(sale?.correo_miembro);
      if (!correoNorm || !spotifyRenewalLookupEmailSet.has(correoNorm)) return;
      const bucket = spotifyCandidateSalesByCorreo.get(correoNorm) || [];
      bucket.push(sale);
      spotifyCandidateSalesByCorreo.set(correoNorm, bucket);
    });
  }
  const usedImplicitRenewVentaIds = new Set();
  const ventasToInsert = [];
  for (const a of sourceRowsForVentas) {
    const sourceRowIdx =
      Number.isFinite(Number(a?.__source_row_idx)) && Number(a.__source_row_idx) >= 0
        ? Math.trunc(Number(a.__source_row_idx))
        : null;
    const mesesValRaw = a.meses || 1;
    const mesesVal =
      Number.isFinite(Number(mesesValRaw)) && Number(mesesValRaw) > 0
        ? Math.max(1, Math.round(Number(mesesValRaw)))
        : 1;
    const fechaCorte = a.pendiente ? null : addMonthsKeepDay(isoHoy, mesesVal);
    const priceInfo = priceMap[a.id_precio] || {};
    const platId = priceInfo.id_plataforma || null;
    const isCompleta =
      priceInfo.completa === true ||
      priceInfo.completa === "true" ||
      priceInfo.completa === 1 ||
      priceInfo.completa === "1";
    const cuentaId = toPositiveInt(a.id_cuenta);
    const cuentaFlags = cuentaId ? cuentaFlagsById[cuentaId] || null : null;
    const isCuentaCompletaVenta = cuentaFlags
      ? isCuentaCompletaByFlags(cuentaFlags)
      : isCompleta && !isTrue(priceInfo.sub_cuenta);
    const isSpotifyCompletaCompra = isCuentaCompletaVenta && Number(platId) === 9;
    const itemId = toPositiveInt(a?.id_item_carrito);
    const montoLinea = (() => {
      if (itemId && lineDistributedAmountsByItemId.has(itemId)) {
        const bucket = lineDistributedAmountsByItemId.get(itemId) || [];
        const nextAmount = bucket.length ? bucket.shift() : null;
        lineDistributedAmountsByItemId.set(itemId, bucket);
        if (Number.isFinite(Number(nextAmount))) return Number(nextAmount);
      }
      return Number(a.monto) || 0;
    })();
    if (sourceRowIdx !== null) {
      sourceRowsFinalAmountByIdx.set(sourceRowIdx, roundCheckoutMoney(montoLinea));
    }

    const correoCuentaNorm = normalizeComparableEmail(cuentaFlags?.correo);
    if (Number(platId) === 9 && !isTrue(a?.pendiente) && correoCuentaNorm) {
      const saleCandidates = spotifyCandidateSalesByCorreo.get(correoCuentaNorm) || [];
      const matchedSale = saleCandidates.find((sale) => {
        const saleId = toPositiveInt(sale?.id_venta);
        return !!saleId && !usedImplicitRenewVentaIds.has(saleId);
      });
      const matchedSaleId = toPositiveInt(matchedSale?.id_venta);
      if (matchedSaleId) {
        usedImplicitRenewVentaIds.add(matchedSaleId);
        if (sourceRowIdx !== null) {
          sourceRowsConvertedToRenewals.add(sourceRowIdx);
          sourceRowsImplicitRenewalByIdx.set(sourceRowIdx, matchedSaleId);
        }
        spotifyImplicitRenewals.push({
          source_row_idx: sourceRowIdx,
          id_item_carrito: itemId || null,
          id_venta: matchedSaleId,
          id_precio: a.id_precio,
          meses: mesesVal,
          monto: montoLinea,
        });
        if (itemId) {
          implicitRenewalItemIds.add(itemId);
        }
        continue;
      }
    }

    ventasToInsert.push({
      id_usuario: idUsuarioVentas,
      id_precio: a.id_precio,
      id_tarjeta_de_regalo: toPositiveInt(a.id_tarjeta_de_regalo) || null,
      id_cuenta: a.id_cuenta,
      id_perfil: a.id_perfil,
      // id_sub_cuenta no existe en la tabla ventas; si se requiere, agregar columna en DB
      monto: montoLinea,
      pendiente: !!a.pendiente,
      entrega_aviso: !isTrue(a?.pendiente),
      meses_contratados: mesesVal,
      fecha_corte: fechaCorte,
      fecha_pago: isoHoy,
      renovacion: false,
      recordatorio_enviado: false,
      recordatorio_corte_enviado: false,
      aviso_admin: false,
      completa: isCompleta ? true : null,
      cuenta_pagada_admin: isCuentaCompletaVenta ? (isSpotifyCompletaCompra ? true : false) : null,
    });
    sourceRowsForInsertedVentas.push(a);
  }
  console.log("[checkout] asignaciones", asignaciones);
  console.log("[checkout] pendientes", pendientes);
  console.log("[checkout] ventasToInsert", ventasToInsert);

  let insertedVentas = [];
  if (ventasToInsert.length) {
    const { data: ventasRes, error: ventaErr } = await supabaseAdmin
      .from("ventas")
      .insert(ventasToInsert)
      .select("id_venta, id_cuenta, id_precio, id_tarjeta_de_regalo");
    if (ventaErr) throw ventaErr;
    insertedVentas = ventasRes || [];
  }
  insertedVentas.forEach((venta, idx) => {
    const source = sourceRowsForInsertedVentas[idx] || {};
    const sourceIdx =
      Number.isFinite(Number(source?.__source_row_idx)) && Number(source.__source_row_idx) >= 0
        ? Math.trunc(Number(source.__source_row_idx))
        : null;
    const ventaId = toPositiveInt(venta?.id_venta);
    if (sourceIdx !== null && ventaId) {
      sourceRowsInsertedVentaByIdx.set(sourceIdx, ventaId);
    }
  });
  let implicitRenovacionesPendientesCount = 0;
  const implicitRenewalHistRows = [];
  if (spotifyImplicitRenewals.length) {
    const implicitVentaIds = uniqPositiveIds(spotifyImplicitRenewals.map((row) => row?.id_venta));
    const implicitVentaMap = {};
    const implicitRenewalAppliedVentaIds = new Set();
    if (toPositiveInt(ordenId) && implicitVentaIds.length) {
      const { data: implicitRenewRows, error: implicitRenewErr } = await supabaseAdmin
        .from("historial_ventas")
        .select("id_venta")
        .eq("id_orden", ordenId)
        .eq("renovacion", true)
        .in("id_venta", implicitVentaIds);
      if (implicitRenewErr) throw implicitRenewErr;
      (implicitRenewRows || []).forEach((histRow) => {
        const appliedVentaId = toPositiveInt(histRow?.id_venta);
        if (appliedVentaId) implicitRenewalAppliedVentaIds.add(appliedVentaId);
      });
    }
    if (implicitVentaIds.length) {
      const { data: implicitVentas, error: implicitVentasErr } = await supabaseAdmin
        .from("ventas")
        .select(
          "id_venta, fecha_corte, id_cuenta, id_cuenta_miembro, id_usuario, suspendido, completa, cuenta_principal:cuentas!ventas_id_cuenta_fkey(id_cuenta, venta_perfil, venta_miembro, correo, clave), cuenta_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, venta_perfil, venta_miembro, correo, clave)",
        )
        .in("id_venta", implicitVentaIds);
      if (implicitVentasErr) throw implicitVentasErr;
      (implicitVentas || []).forEach((row) => {
        const ventaId = toPositiveInt(row?.id_venta);
        if (ventaId) implicitVentaMap[ventaId] = row;
      });
    }

    for (const row of spotifyImplicitRenewals) {
      const ventaId = toPositiveInt(row?.id_venta);
      if (!ventaId) continue;
      if (implicitRenewalAppliedVentaIds.has(ventaId)) {
        console.warn("[checkout] renovacion implicita omitida por historial existente", {
          ordenId,
          id_venta: ventaId,
        });
        continue;
      }
      const price = priceMap[row?.id_precio] || {};
      const platId = Number(price?.id_plataforma) || 9;
      const platformInfo = platInfoById[platId] || {};
      const monto = Number(row?.monto) || 0;
      const mesesValRaw = row?.meses || 1;
      const mesesVal =
        Number.isFinite(Number(mesesValRaw)) && Number(mesesValRaw) > 0
          ? Math.max(1, Math.round(Number(mesesValRaw)))
          : 1;
      const ventaAnt = implicitVentaMap[ventaId] || {};
      const isSuspendidaAnt = isTrue(ventaAnt?.suspendido);
      const cuentaVenta = ventaAnt?.cuenta_principal || ventaAnt?.cuenta_miembro || null;
      const isCuentaCompletaRenov =
        isCuentaCompletaByFlags(cuentaVenta) ||
        (isTrue(price?.completa) && !isTrue(price?.sub_cuenta));
      const isVentaCompletaRenov = isTrue(ventaAnt?.completa);
      const fechaBaseSrc = ventaAnt?.fecha_corte || isoHoy;
      const fecha_corte = addMonthsKeepDay(fechaBaseSrc, mesesVal) || isoHoy;
      const updatePayload = {
        fecha_pago: isoHoy,
        fecha_corte,
        monto,
        renovacion: true,
        meses_contratados: mesesVal,
        recordatorio_enviado: false,
        recordatorio_corte_enviado: false,
        aviso_admin: false,
        pendiente: false,
        suspendido: false,
      };
      if (isSuspendidaAnt) {
        updatePayload.cuenta_nueva = false;
      }
      if (isCuentaCompletaRenov || isVentaCompletaRenov) {
        if (platId === 9) {
          updatePayload.cuenta_pagada_admin = true;
        } else {
          updatePayload.cuenta_pagada_admin = false;
        }
      }
      const { data: implicitUpdatedRows, error: implicitUpdErr } = await supabaseAdmin
        .from("ventas")
        .update(updatePayload)
        .eq("id_venta", ventaId)
        .select("id_venta");
      if (implicitUpdErr) throw implicitUpdErr;
      if ((implicitUpdatedRows || []).length === 0) {
        console.warn("[checkout] renovacion implicita omitida por idempotencia", {
          ordenId,
          id_venta: ventaId,
        });
        continue;
      }
      implicitRenewalAppliedVentaIds.add(ventaId);

      implicitRenewalHistRows.push({
        id_usuario_cliente: ventaAnt?.id_usuario || idUsuarioVentas,
        id_proveedor: null,
        monto,
        fecha_pago: isoHoy,
        venta_cliente: true,
        renovacion: true,
        id_venta: ventaId,
        id_orden: ordenId,
        id_plataforma: platId,
        id_cuenta: ventaAnt?.id_cuenta || null,
        registrado_por: historialRegistradoPorId,
        id_metodo_de_pago,
        referencia: referenciaNum,
        comprobante: comprobanteHist,
        hora_pago: null,
      });
    }
  }
  const giftSoldIds = Array.from(
    new Set(
      (insertedVentas || [])
        .map((row) => toPositiveInt(row?.id_tarjeta_de_regalo))
        .filter((id) => Number.isFinite(id) && id > 0),
    ),
  );
  if (giftSoldIds.length) {
    const { error: updGiftErr } = await supabaseAdmin
      .from("tarjetas_de_regalo")
      .update({
        usado: true,
        vendido_a: toPositiveInt(idUsuarioVentas) || null,
        fecha_uso: isoHoy,
        admin_involucrado: toPositiveInt(adminInvolucradoId) || null,
        id_orden: toPositiveInt(ordenId) || null,
      })
      .eq("para_venta", true)
      .in("id_tarjeta_de_regalo", giftSoldIds);
    if (updGiftErr) throw updGiftErr;
  }

  // Historial de ventas (nuevas + renovaciones) con monto como float completo
  const caracasNowPago = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
  const pad2Pago = (val) => String(val).padStart(2, "0");
  const horaPago = `${pad2Pago(caracasNowPago.getHours())}:${pad2Pago(
    caracasNowPago.getMinutes()
  )}:${pad2Pago(caracasNowPago.getSeconds())}`;
  const histRows = [];
  // Nuevas (insertadas recién)
  insertedVentas.forEach((v, idx) => {
    const src = ventasToInsert[idx] || {};
    const platId = priceMap[v.id_precio]?.id_plataforma || null;
    histRows.push({
      id_usuario_cliente: idUsuarioVentas,
      id_proveedor: null,
      monto: Number(src.monto) || 0,
      fecha_pago: isoHoy,
      venta_cliente: true,
      renovacion: false,
      id_venta: v.id_venta,
      id_orden: ordenId,
      id_plataforma: platId,
      id_cuenta: v.id_cuenta,
      registrado_por: historialRegistradoPorId,
      id_metodo_de_pago,
      referencia: referenciaNum,
      comprobante: comprobanteHist,
      hora_pago: horaPago,
      id_tarjeta_de_regalo: toPositiveInt(v.id_tarjeta_de_regalo) || null,
    });
  });
  // Renovaciones
  appliedRenovaciones.forEach((it) => {
    const price = priceMap[it.id_precio] || {};
    const platId = Number(it?.id_plataforma || price?.id_plataforma) || null;
    const ventaAnt = ventaMap[it.id_venta] || {};
    const cuentaAnt = ventaAnt.id_cuenta || null;
    const usuarioAnt = ventaAnt.id_usuario || idUsuarioVentas;
    const monto = roundCheckoutMoney(Number(it?.monto) || 0);
    histRows.push({
      id_usuario_cliente: usuarioAnt,
      id_proveedor: null,
      monto: Number(monto) || 0,
      fecha_pago: isoHoy,
      venta_cliente: true,
      renovacion: true,
      id_venta: it.id_venta,
      id_orden: ordenId,
      id_plataforma: platId,
      id_cuenta: cuentaAnt,
      registrado_por: historialRegistradoPorId,
      id_metodo_de_pago,
      referencia: referenciaNum,
      comprobante: comprobanteHist,
      hora_pago: horaPago,
    });
  });
  if (implicitRenewalHistRows.length) {
    implicitRenewalHistRows.forEach((row) => {
      row.hora_pago = horaPago;
    });
    histRows.push(...implicitRenewalHistRows);
  }
  if (histRows.length) {
    let historialGiftCardLinkFallback = false;
    const saldoConsumidoHist = (() => {
      const consumed = toFiniteMoney(saldoConsumption?.monto);
      if (Number.isFinite(consumed) && consumed > 0) return consumed;
      const expected = toFiniteMoney(saldoAplicadoExpectedUsd);
      if (Number.isFinite(expected) && expected > 0) return expected;
      return 0;
    })();
    const extraHist = (() => {
      const extra = toFiniteMoney(montoExtraHistorialUsd);
      return Number.isFinite(extra) && extra > 0 ? extra : 0;
    })();
    const targetHistTotalNum = hasCustomItemAmounts
      ? Number.NaN
      : Number(toFiniteMoney(montoHistorialTotalOverride) || 0) + saldoConsumidoHist + extraHist;
    if (Number.isFinite(targetHistTotalNum) && targetHistTotalNum >= 0) {
      const targetHistTotal = Math.round(targetHistTotalNum * 100) / 100;
      const baseTotal = histRows.reduce((acc, row) => acc + (Number(row?.monto) || 0), 0);
      if (baseTotal > 0) {
        const factor = targetHistTotal / baseTotal;
        let acumulado = 0;
        histRows.forEach((row, idx) => {
          const scaledRaw = (Number(row?.monto) || 0) * factor;
          const scaled = Math.round(scaledRaw * 100) / 100;
          if (idx === histRows.length - 1) {
            row.monto = Math.round((targetHistTotal - acumulado) * 100) / 100;
          } else {
            row.monto = scaled;
            acumulado += scaled;
          }
        });
      } else {
        histRows.forEach((row, idx) => {
          row.monto = idx === 0 ? targetHistTotal : 0;
        });
      }
    }
    const existingHistKeys = new Set();
    const histVentaIds = uniqPositiveIds(histRows.map((row) => row?.id_venta));
    if (toPositiveInt(ordenId) && histVentaIds.length) {
      const { data: existingHistRows, error: existingHistErr } = await supabaseAdmin
        .from("historial_ventas")
        .select("id_venta, renovacion")
        .eq("id_orden", ordenId)
        .in("id_venta", histVentaIds);
      if (existingHistErr) throw existingHistErr;
      (existingHistRows || []).forEach((row) => {
        const ventaId = toPositiveInt(row?.id_venta);
        if (!ventaId) return;
        const key = `${ventaId}:${isTrue(row?.renovacion) ? 1 : 0}`;
        existingHistKeys.add(key);
      });
    }
    const histRowsToInsert = histRows.filter((row) => {
      const ventaId = toPositiveInt(row?.id_venta);
      if (!ventaId) return true;
      const key = `${ventaId}:${isTrue(row?.renovacion) ? 1 : 0}`;
      return !existingHistKeys.has(key);
    });
    if (histRowsToInsert.length !== histRows.length) {
      console.warn("[checkout] historial_ventas duplicados evitados", {
        ordenId,
        originales: histRows.length,
        insertados: histRowsToInsert.length,
        omitidos: histRows.length - histRowsToInsert.length,
      });
    }
    if (!histRowsToInsert.length) {
      histRows.length = 0;
    }
    const { error: histErr } = histRowsToInsert.length
      ? await supabaseAdmin.from("historial_ventas").insert(histRowsToInsert)
      : { error: null };
    if (histErr) {
      if (!isMissingHistorialGiftCardColumnError(histErr)) throw histErr;
      historialGiftCardLinkFallback = true;
      const { error: histFallbackErr } = await supabaseAdmin
        .from("historial_ventas")
        .insert(stripHistorialGiftCardColumn(histRowsToInsert));
      if (histFallbackErr) throw histFallbackErr;
    }

    if (historialGiftCardLinkFallback) {
      console.warn("[checkout] historial gift card sin columna id_tarjeta_de_regalo", {
        ordenId,
      });
    }
  }

  const snapshotItems = [];
  const snapshotAmountMap = new Map();
  let snapshotItemSeq = 1;
  const pushSnapshotItem = (row, montoUsd = null) => {
    const normalizedItem = {
      id_item: snapshotItemSeq,
      id_precio: toPositiveInt(row?.id_precio) || null,
      cantidad:
        Number.isFinite(Number(row?.cantidad)) && Number(row?.cantidad) > 0
          ? Math.max(1, Math.round(Number(row?.cantidad)))
          : 1,
      meses:
        Number.isFinite(Number(row?.meses)) && Number(row?.meses) > 0
          ? Math.max(1, Math.round(Number(row?.meses)))
          : 1,
      renovacion: isTrue(row?.renovacion),
      id_venta: toPositiveInt(row?.id_venta) || null,
      id_cuenta: toPositiveInt(row?.id_cuenta) || null,
      id_perfil: toPositiveInt(row?.id_perfil) || null,
    };
    snapshotItems.push(normalizedItem);
    const parsedMonto = parseOptionalCheckoutNumber(montoUsd);
    if (Number.isFinite(parsedMonto)) {
      snapshotAmountMap.set(snapshotItemSeq, roundCheckoutMoney(parsedMonto));
    }
    snapshotItemSeq += 1;
  };
  const readSourceRowIdx = (row) =>
    Number.isFinite(Number(row?.__source_row_idx)) && Number(row.__source_row_idx) >= 0
      ? Math.trunc(Number(row.__source_row_idx))
      : null;

  const renewalSnapshotItems = (items || []).filter((item) => toPositiveInt(item?.id_venta));
  renewalSnapshotItems.forEach((item) => {
    const itemId = toPositiveInt(item?.id_item);
    const defaultLineAmount = computeCheckoutItemBaseAmount({
      item,
      priceInfo: priceMap[item.id_precio] || {},
      platformInfo: platInfoById[Number(priceMap[item.id_precio]?.id_plataforma) || 0] || {},
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
    });
    const lineAmount = getLineAmountForItem(item, defaultLineAmount);
    pushSnapshotItem(
      {
        ...item,
        renovacion: implicitRenewalItemIds.has(itemId) ? true : isTrue(item?.renovacion),
      },
      lineAmount,
    );
  });

  const sourceRowsOrdered = [...sourceRowsForVentas].sort((a, b) => {
    const idxA = readSourceRowIdx(a);
    const idxB = readSourceRowIdx(b);
    return (idxA ?? 0) - (idxB ?? 0);
  });
  sourceRowsOrdered.forEach((row) => {
    const sourceIdx = readSourceRowIdx(row);
    const implicitVentaId =
      sourceIdx !== null ? toPositiveInt(sourceRowsImplicitRenewalByIdx.get(sourceIdx)) : null;
    const insertedVentaId =
      sourceIdx !== null ? toPositiveInt(sourceRowsInsertedVentaByIdx.get(sourceIdx)) : null;
    const resolvedVentaId = implicitVentaId || insertedVentaId || null;
    const amountFromSourceIdx =
      sourceIdx !== null ? sourceRowsFinalAmountByIdx.get(sourceIdx) : Number(row?.monto) || 0;
    const mesesValRaw = row?.meses || 1;
    const mesesVal =
      Number.isFinite(Number(mesesValRaw)) && Number(mesesValRaw) > 0
        ? Math.max(1, Math.round(Number(mesesValRaw)))
        : 1;
    const isRenewal = sourceIdx !== null ? sourceRowsConvertedToRenewals.has(sourceIdx) : false;

    pushSnapshotItem(
      {
        id_precio: toPositiveInt(row?.id_precio) || null,
        cantidad: 1,
        meses: mesesVal,
        renovacion: isRenewal,
        id_venta: resolvedVentaId,
        id_cuenta: toPositiveInt(row?.id_cuenta) || null,
        id_perfil: toPositiveInt(row?.id_perfil) || null,
      },
      amountFromSourceIdx,
    );
  });

  await syncOrdenItemsSnapshot({
    ordenId,
    items: snapshotItems,
    priceMap,
    platInfoById,
    platNameById,
    pickPrecio,
    descuentos,
    discountColumns,
    discountColumnById,
    isCliente,
    totalUsd: snapshotTotalUsd,
    montoBsTotal: snapshotMontoBsTotal,
    tasaBs: snapshotTasaBs,
    customItemAmountMap: snapshotAmountMap.size ? snapshotAmountMap : hasCustomItemAmounts ? itemAmountMapById : null,
  });

  // marca recursos como ocupados
  const asignacionesEfectivas = sourceRowsForVentas.filter(
    (row) => row.__is_asignacion && !sourceRowsConvertedToRenewals.has(row.__source_row_idx),
  );
  const perfilesIds = uniqPositiveIds(asignacionesEfectivas.map((a) => a.id_perfil));
  const cuentasIds = uniqPositiveIds(
    asignacionesEfectivas
      .filter((a) => a.id_perfil === null && a.id_cuenta)
      .map((a) => a.id_cuenta),
  );
  const cuentasCompletasIds = uniqPositiveIds(
    asignacionesEfectivas
      .filter((a) => {
        if (a?.id_perfil !== null && a?.id_perfil !== undefined) return false;
        const cuentaId = toPositiveInt(a?.id_cuenta);
        if (!cuentaId) return false;
        const priceInfo = priceMap[a?.id_precio] || {};
        return isTrue(priceInfo?.completa) && !isTrue(priceInfo?.sub_cuenta);
      })
      .map((a) => a.id_cuenta),
  );
  if (perfilesIds.length) {
    const { error: updPerfErr } = await supabaseAdmin
      .from("perfiles")
      .update({ ocupado: true })
      .in("id_perfil", perfilesIds);
    if (updPerfErr) throw updPerfErr;
  }
  if (cuentasIds.length) {
    const { error: updCtaErr } = await supabaseAdmin
      .from("cuentas")
      .update({ ocupado: true })
      .in("id_cuenta", cuentasIds);
    if (updCtaErr) throw updCtaErr;
  }
  if (cuentasCompletasIds.length) {
    const { error: updCompletaErr } = await supabaseAdmin
      .from("cuentas")
      .update({ completa: true })
      .in("id_cuenta", cuentasCompletasIds);
    if (updCompletaErr) throw updCompletaErr;
  }

  const idUsuarioVenta = toPositiveInt(idUsuarioVentas);
  const huboVentasProcesadas =
    insertedVentas.length > 0 || renovaciones.length > 0 || spotifyImplicitRenewals.length > 0;
  if (idUsuarioVenta && huboVentasProcesadas) {
    const { error: updUserErr } = await supabaseAdmin
      .from("usuarios")
      .update({ fecha_ultima_compra: isoHoy })
      .eq("id_usuario", idUsuarioVenta);
    if (updUserErr) {
      console.error("[checkout] update fecha_ultima_compra error", {
        id_usuario: idUsuarioVenta,
        fecha_ultima_compra: isoHoy,
        error: updUserErr?.message || updUserErr,
      });
    }
  }

  try {
    const providerNotifyResult = await maybeAutoNotifySpotifyProviderForOrder({
      idOrden: ordenId,
      providerId: 11,
      manageWhatsappLifecycle: true,
      source: "process_order_auto_single",
    });
    if (!providerNotifyResult?.sent) {
      console.log("[checkout] spotify proveedor notify auto", {
        id_orden: toPositiveInt(ordenId) || null,
        sent: false,
        skipped: providerNotifyResult?.skipped === true,
        reason: providerNotifyResult?.reason || null,
        total_pendientes: providerNotifyResult?.total_pendientes ?? null,
      });
    }
  } catch (providerNotifyErr) {
    console.error("[checkout] spotify proveedor notify auto error", {
      id_orden: toPositiveInt(ordenId) || null,
      error: providerNotifyErr?.message || providerNotifyErr,
    });
  }

  console.log("[checkout] processOrderFromItems end", {
    ordenId,
    ventasCount: ventasToInsert.length,
    pendientesCount: pendientes.length + renovacionesPendientesCount + implicitRenovacionesPendientesCount,
  });
  return {
    ventasCount: ventasToInsert.length,
    pendientesCount: pendientes.length + renovacionesPendientesCount + implicitRenovacionesPendientesCount,
  };
};

// Usa solo el id de usuario autenticado en cookie httpOnly.
const parseSessionUserId = (req) => {
  const raw = req?.headers?.cookie || "";
  const parts = raw.split(";").map((c) => c.trim().split("="));
  const cookieMap = parts.reduce((acc, [k, v]) => {
    if (k) acc[k] = decodeURIComponent(v || "");
    return acc;
  }, {});
  return parseSignedSessionCookieValue(cookieMap[SESSION_COOKIE_NAME]);
};

const requireSessionUserId = (req) => {
  const idUsuario = parseSessionUserId(req);
  if (Number.isFinite(Number(idUsuario)) && Number(idUsuario) > 0) {
    return Math.trunc(Number(idUsuario));
  }
  const err = new Error(AUTH_REQUIRED);
  err.code = AUTH_REQUIRED;
  throw err;
};

const getBearerTokenFromRequest = (req) => {
  const authHeader = String(req.get("authorization") || "").trim();
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match?.[1]?.trim() || "";
};

const resolveUsuarioFromAuthToken = async (token) => {
  const accessToken = String(token || "").trim();
  if (!accessToken) {
    const err = new Error(AUTH_REQUIRED);
    err.code = AUTH_REQUIRED;
    throw err;
  }

  const { data: authData, error: authErr } = await supabaseAdmin.auth.getUser(accessToken);
  if (authErr || !authData?.user) {
    const err = new Error(AUTH_REQUIRED);
    err.code = AUTH_REQUIRED;
    throw err;
  }

  const authEmail = String(authData.user.email || "")
    .trim()
    .toLowerCase();
  const authUserId = String(authData.user.id || "").trim();
  if (!authUserId) {
    const err = new Error(AUTH_REQUIRED);
    err.code = AUTH_REQUIRED;
    throw err;
  }
  if (!authEmail) {
    const err = new Error(AUTH_REQUIRED);
    err.code = AUTH_REQUIRED;
    throw err;
  }

  const asCodeError = (code, message = code) => {
    const err = new Error(message);
    err.code = code;
    return err;
  };
  const normalizeText = (value) => String(value || "").trim();
  const normalizePhoneDigits = (value) => {
    const digits = normalizeText(value)
      .replace(/\D+/g, "")
      .replace(/^0+/, "");
    if (!digits) return "";
    if (digits.startsWith("58")) {
      const national = digits.slice(2).replace(/^0+/, "");
      return national ? `58${national}` : "58";
    }
    return digits;
  };
  const normalizeEmail = (value) => normalizeText(value).toLowerCase();
  const parseMetaBoolean = (value) => {
    if (value === true || value === 1) return true;
    if (value === false || value === 0) return false;
    const raw = normalizeText(value).toLowerCase();
    if (!raw) return null;
    if (raw === "true" || raw === "1" || raw === "t" || raw === "si" || raw === "sí") return true;
    if (raw === "false" || raw === "0" || raw === "f" || raw === "no") return false;
    return null;
  };
  const mapUniqueUsuariosError = (dbErr) => {
    if (String(dbErr?.code || "") !== "23505") return "";
    const detail = `${dbErr?.message || ""} ${dbErr?.details || ""} ${dbErr?.hint || ""}`.toLowerCase();
    if (detail.includes("id_auth")) return "USER_AUTH_DUPLICATED";
    if (detail.includes("correo")) return "USER_EMAIL_DUPLICATED";
    return "USER_AUTH_DUPLICATED";
  };
  const throwIfUniqueUsuariosError = (dbErr) => {
    const mappedCode = mapUniqueUsuariosError(dbErr);
    if (mappedCode) throw asCodeError(mappedCode);
  };

  const rawMeta =
    authData.user?.user_metadata && typeof authData.user.user_metadata === "object"
      ? authData.user.user_metadata
      : {};
  const displayName = normalizeText(rawMeta.display_name || rawMeta.full_name || rawMeta.name);
  let metaNombre = normalizeText(rawMeta.nombre);
  let metaApellido = normalizeText(rawMeta.apellido);
  if (displayName && (!metaNombre || !metaApellido)) {
    const parts = displayName.split(/\s+/).filter(Boolean);
    if (!metaNombre && parts.length) metaNombre = parts[0];
    if (!metaApellido && parts.length > 1) metaApellido = parts.slice(1).join(" ");
  }
  const emailLocal = normalizeText(authEmail.split("@")[0]).replace(/[._-]+/g, " ");
  if (emailLocal && !metaNombre) {
    const parts = emailLocal.split(/\s+/).filter(Boolean);
    if (parts.length) metaNombre = parts[0];
    if (!metaApellido && parts.length > 1) metaApellido = parts.slice(1).join(" ");
  }
  const metaTelefono = normalizePhoneDigits(
    rawMeta.telefono || rawMeta.phone || rawMeta.phone_number || rawMeta.num_telefono,
  );
  const signupRegistrationToken = normalizeText(
    rawMeta.signup_registration_token || rawMeta.registro_token || rawMeta.registration_token,
  );
  const signupAccessToken = normalizeText(
    rawMeta.signup_access_token || rawMeta.signup_access_mode_token || rawMeta.sat,
  );
  const metaAccesoCliente = parseMetaBoolean(
    rawMeta.signup_access_cliente ??
      rawMeta.acceso_cliente ??
      rawMeta.registro_acceso_cliente ??
      rawMeta.signup_acceso_cliente,
  );
  let metaAccesoClienteResolved = metaAccesoCliente;
  if (signupAccessToken) {
    try {
      const accessPayload = verifySignupAccessToken(signupAccessToken);
      metaAccesoClienteResolved = accessPayload?.accesoCliente === true;
      console.log("[auth/new-signup] signup access token resolved", {
        acceso_cliente: metaAccesoClienteResolved,
        exp: accessPayload?.exp || null,
      });
    } catch (_err) {
      // Si el token de acceso no es válido, no bloquea el signup; se usa fallback.
      console.warn("[auth/new-signup] invalid signup access token; fallback metadata/default");
    }
  }

  const buildUsuarioPatch = (currentRow, options = {}) => {
    const forceProfile = options?.forceProfile === true;
    const patch = {};

    const currentCorreo = normalizeEmail(currentRow?.correo);
    if (currentCorreo !== authEmail) {
      patch.correo = authEmail;
    }
    if (!currentRow?.fecha_registro) {
      patch.fecha_registro = todayInVenezuela();
    }
    const currentTelefono = normalizePhoneDigits(currentRow?.telefono);
    if (metaTelefono && (forceProfile || !currentTelefono)) {
      patch.telefono = metaTelefono;
    }
    if (metaNombre && (forceProfile || !normalizeText(currentRow?.nombre))) {
      patch.nombre = metaNombre;
    }
    if (metaApellido && (forceProfile || !normalizeText(currentRow?.apellido))) {
      patch.apellido = metaApellido;
    }
    return patch;
  };

  const updateUsuarioPatch = async (idUsuario, patch) => {
    if (!patch || !Object.keys(patch).length) return;
    const { error: updErr } = await supabaseAdmin
      .from("usuarios")
      .update(patch)
      .eq("id_usuario", idUsuario);
    if (updErr) {
      throwIfUniqueUsuariosError(updErr);
      throw updErr;
    }
  };

  const ensureAuthLinked = async (idUsuario, currentIdAuth) => {
    const idAuthGuard = normalizeText(currentIdAuth);
    if (idAuthGuard && idAuthGuard !== authUserId) {
      throw asCodeError("USER_NOT_LINKED");
    }
    if (idAuthGuard === authUserId) return;

    const { data: linkRow, error: linkErr } = await supabaseAdmin
      .from("usuarios")
      .update({ id_auth: authUserId })
      .eq("id_usuario", idUsuario)
      .is("id_auth", null)
      .select("id_usuario, id_auth")
      .maybeSingle();
    if (linkErr) {
      throwIfUniqueUsuariosError(linkErr);
      throw linkErr;
    }
    if (linkRow) return;

    const { data: verifyRow, error: verifyErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, id_auth")
      .eq("id_usuario", idUsuario)
      .maybeSingle();
    if (verifyErr) throw verifyErr;
    const afterAuth = normalizeText(verifyRow?.id_auth);
    if (!afterAuth || afterAuth !== authUserId) {
      throw asCodeError("USER_AUTH_DUPLICATED");
    }
  };

  const fetchUsuariosByCorreo = async (correo, excludeId = null) => {
    let query = supabaseAdmin
      .from("usuarios")
      .select("id_usuario, correo, id_auth, nombre, apellido, telefono, fecha_registro")
      .ilike("correo", correo)
      .limit(2);
    if (excludeId) {
      query = query.neq("id_usuario", excludeId);
    }
    const { data, error } = await query;
    if (error) throw error;
    return Array.isArray(data) ? data : [];
  };

  if (signupRegistrationToken) {
    let tokenPayload = null;
    try {
      tokenPayload = verifySignupRegistrationToken(signupRegistrationToken, { allowExpired: true });
    } catch (_err) {
      tokenPayload = null;
    }
    const idUsuarioToken = toPositiveInt(tokenPayload?.uid);
    if (!idUsuarioToken) {
      throw asCodeError("USER_NOT_LINKED");
    }

    const { data: targetRow, error: targetErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, correo, id_auth, nombre, apellido, telefono, fecha_registro")
      .eq("id_usuario", idUsuarioToken)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      throw asCodeError("USER_NOT_LINKED");
    }

    const conflictingEmailRows = await fetchUsuariosByCorreo(authEmail, idUsuarioToken);
    if (conflictingEmailRows.length) {
      throw asCodeError("USER_EMAIL_DUPLICATED");
    }

    await ensureAuthLinked(idUsuarioToken, targetRow.id_auth);
    const patch = buildUsuarioPatch(targetRow, { forceProfile: true });
    await updateUsuarioPatch(idUsuarioToken, patch);
    return idUsuarioToken;
  }

  const { data: byAuthRows, error: byAuthErr } = await supabaseAdmin
    .from("usuarios")
    .select("id_usuario, correo, id_auth, nombre, apellido, telefono, fecha_registro")
    .eq("id_auth", authUserId)
    .limit(2);
  if (byAuthErr) throw byAuthErr;
  if (Array.isArray(byAuthRows) && byAuthRows.length > 1) {
    throw asCodeError("USER_AUTH_DUPLICATED");
  }
  if (Array.isArray(byAuthRows) && byAuthRows.length === 1) {
    const idUsuarioByAuth = Number(byAuthRows[0]?.id_usuario);
    if (Number.isFinite(idUsuarioByAuth) && idUsuarioByAuth > 0) {
      const patch = buildUsuarioPatch(byAuthRows[0], { forceProfile: false });
      await updateUsuarioPatch(idUsuarioByAuth, patch);
      return idUsuarioByAuth;
    }
  }

  const rows = await fetchUsuariosByCorreo(authEmail);
  if (rows.length > 1) {
    throw asCodeError("USER_EMAIL_DUPLICATED");
  }
  if (rows.length === 1) {
    const row = rows[0];
    const idUsuario = Number(row?.id_usuario);
    if (!Number.isFinite(idUsuario) || idUsuario <= 0) {
      throw asCodeError(AUTH_REQUIRED);
    }
    await ensureAuthLinked(idUsuario, row.id_auth);
    const patch = buildUsuarioPatch(row, { forceProfile: false });
    await updateUsuarioPatch(idUsuario, patch);
    return idUsuario;
  }

  const insertPayload = {
    nombre: metaNombre || "Cliente",
    apellido: metaApellido || null,
    telefono: metaTelefono || null,
    correo: authEmail,
    id_auth: authUserId,
    fecha_registro: todayInVenezuela(),
    acceso_cliente: metaAccesoClienteResolved === false ? false : true,
  };

  const notifyNewAuthSignup = async (idUsuarioNuevo) => {
    const targetUserId = Number(NEW_AUTH_SIGNUP_NOTIFY_USER_ID);
    if (!Number.isFinite(targetUserId) || targetUserId <= 0) return;
    if (!Number.isFinite(Number(idUsuarioNuevo)) || Number(idUsuarioNuevo) <= 0) return;

    const clienteNombre = String(insertPayload.nombre || "").trim();
    const clienteApellido = String(insertPayload.apellido || "").trim();
    const cliente = [clienteNombre, clienteApellido].filter(Boolean).join(" ").trim() || "Cliente";
    const correo = String(insertPayload.correo || authEmail || "").trim().toLowerCase();

    try {
      const { error: notifErr } = await supabaseAdmin.from("notificaciones").insert({
        id_usuario: targetUserId,
        titulo: "Nuevo cliente registrado",
        mensaje: `${cliente} se ha registrado con el correo ${correo}`,
        fecha: todayInVenezuela(),
        leido: false,
      });
      if (notifErr) throw notifErr;
    } catch (err) {
      console.error("[auth/new-signup] notify admin error", {
        targetUserId,
        idUsuarioNuevo,
        correo,
        error: err?.message || err,
      });
    }
  };

  const { data: insertedRow, error: insertErr } = await supabaseAdmin
    .from("usuarios")
    .insert(insertPayload)
    .select("id_usuario")
    .maybeSingle();
  if (insertErr) {
    throwIfUniqueUsuariosError(insertErr);
    throw insertErr;
  }

  const insertedId = Number(insertedRow?.id_usuario);
  if (Number.isFinite(insertedId) && insertedId > 0) {
    await notifyNewAuthSignup(insertedId);
    return insertedId;
  }

  const { data: fallbackAuthRows, error: fallbackAuthErr } = await supabaseAdmin
    .from("usuarios")
    .select("id_usuario")
    .eq("id_auth", authUserId)
    .limit(2);
  if (fallbackAuthErr) throw fallbackAuthErr;
  if (Array.isArray(fallbackAuthRows) && fallbackAuthRows.length === 1) {
    const fallbackId = Number(fallbackAuthRows[0]?.id_usuario);
    if (Number.isFinite(fallbackId) && fallbackId > 0) {
      await notifyNewAuthSignup(fallbackId);
      return fallbackId;
    }
  }

  throw asCodeError("USER_NOT_LINKED");
};

const getOrCreateUsuario = async (req) => {
  return getSessionUsuario(req);
};

const getSessionUsuario = async (req) => {
  const fromSession = parseSessionUserId(req);
  if (fromSession && Number.isFinite(Number(fromSession)) && Number(fromSession) > 0) {
    return Number(fromSession);
  }
  const err = new Error(AUTH_REQUIRED);
  err.code = AUTH_REQUIRED;
  throw err;
};

const requireAdminSession = async (req) => {
  const idUsuario = await getSessionUsuario(req);
  const { data: permRow, error: permErr } = await supabaseAdmin
    .from("usuarios")
    .select("permiso_admin, permiso_superadmin")
    .eq("id_usuario", idUsuario)
    .maybeSingle();
  if (permErr) throw permErr;
  const isAdmin = isTrue(permRow?.permiso_admin) || isTrue(permRow?.permiso_superadmin);
  if (!isAdmin) {
    const err = new Error(ADMIN_REQUIRED);
    err.code = ADMIN_REQUIRED;
    throw err;
  }
  return idUsuario;
};

const requireSuperadminSession = async (req) => {
  const idUsuario = await getSessionUsuario(req);
  const { data: permRow, error: permErr } = await supabaseAdmin
    .from("usuarios")
    .select("permiso_superadmin")
    .eq("id_usuario", idUsuario)
    .maybeSingle();
  if (permErr) throw permErr;
  const isSuperadmin = isTrue(permRow?.permiso_superadmin);
  if (!isSuperadmin) {
    const err = new Error(SUPERADMIN_REQUIRED);
    err.code = SUPERADMIN_REQUIRED;
    throw err;
  }
  return idUsuario;
};

const AUTH_ADMIN_USERS_PAGE_SIZE = 1000;

const isAuthUserVerified = (authUser) => {
  const emailConfirmedAt = String(authUser?.email_confirmed_at || "").trim();
  const phoneConfirmedAt = String(authUser?.phone_confirmed_at || "").trim();
  const confirmedAt = String(authUser?.confirmed_at || "").trim();
  return !!(emailConfirmedAt || phoneConfirmedAt || confirmedAt);
};

const listAllAuthUsers = async () => {
  let page = 1;
  const allUsers = [];

  while (true) {
    const { data, error } = await supabaseAdmin.auth.admin.listUsers({
      page,
      perPage: AUTH_ADMIN_USERS_PAGE_SIZE,
    });
    if (error) throw error;
    const users = Array.isArray(data?.users) ? data.users : [];
    allUsers.push(...users);
    if (users.length < AUTH_ADMIN_USERS_PAGE_SIZE) break;
    page += 1;
  }

  return allUsers;
};

const getAuthVerifiedUsersStats = async () => {
  const users = await listAllAuthUsers();
  const totalUsers = users.length;
  const verifiedUsers = users.filter((user) => isAuthUserVerified(user)).length;

  return {
    totalUsers,
    verifiedUsers,
    unverifiedUsers: Math.max(0, totalUsers - verifiedUsers),
  };
};

const getCurrentCarrito = async (idUsuario) => {
  const { data, error } = await runSupabaseQueryWithRetry(
    () =>
      supabaseAdmin
        .from("carritos")
        .select("id_carrito")
        .eq("id_usuario", idUsuario)
        // fecha_creacion es DATE; para evitar empates del mismo día usamos id_carrito.
        .order("id_carrito", { ascending: false })
        .limit(1)
        .maybeSingle(),
    "cart:getCurrentCarrito",
  );
  if (error) throw error;
  return data?.id_carrito || null;
};

const createCarritoForUser = async (idUsuario) => {
  const userId = toPositiveInt(idUsuario);
  if (!userId) return null;
  const { data: inserted, error: insertErr } = await runSupabaseQueryWithRetry(
    () =>
      supabaseAdmin
        .from("carritos")
        .insert({
          id_usuario: userId,
          fecha_creacion: new Date().toISOString(),
          usa_saldo: false,
          saldo_usado: 0,
        })
        .select("id_carrito")
        .single(),
    "cart:createCarritoForUser",
  );
  if (insertErr) throw insertErr;
  return toPositiveInt(inserted?.id_carrito) || null;
};

const shouldRotateLatestCarrito = async (carritoId) => {
  const carritoNum = toPositiveInt(carritoId);
  if (!carritoNum) return false;
  const { data: orderRow, error: orderErr } = await supabaseAdmin
    .from("ordenes")
    .select("id_orden, checkout_finalizado")
    .eq("id_carrito", carritoNum)
    .order("id_orden", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (orderErr) throw orderErr;
  return isTrue(orderRow?.checkout_finalizado);
};

const rotateUserCarritoAfterCheckout = async ({
  idUsuario = null,
  previousCarritoId = null,
  source = "checkout",
} = {}) => {
  const userId = toPositiveInt(idUsuario);
  if (!userId) return null;
  const newCarritoId = await createCarritoForUser(userId);
  console.log("[cart] nuevo carrito post-checkout", {
    source,
    id_usuario: userId,
    id_carrito_anterior: toPositiveInt(previousCarritoId) || null,
    id_carrito_nuevo: newCarritoId,
  });
  return newCarritoId;
};

const cleanupClosedOrderCarrito = async ({
  idOrden = null,
  idCarrito = null,
  source = "order_closed",
} = {}) => {
  const carritoNum = toPositiveInt(idCarrito);
  if (!carritoNum) return { cleaned: false, reason: "invalid_carrito" };

  const { data: orderRows, error: orderErr } = await supabaseAdmin
    .from("ordenes")
    .select("id_orden, pago_verificado, orden_cancelada")
    .eq("id_carrito", carritoNum);
  if (orderErr) throw orderErr;
  const rows = Array.isArray(orderRows) ? orderRows : [];
  const hasOpenOrders = rows.some(
    (row) => !isTrue(row?.pago_verificado) && !isTrue(row?.orden_cancelada),
  );
  if (hasOpenOrders) {
    return { cleaned: false, reason: "open_orders_referencing_cart" };
  }

  if (rows.length) {
    const { error: detachErr } = await supabaseAdmin
      .from("ordenes")
      .update({ id_carrito: null })
      .eq("id_carrito", carritoNum);
    if (detachErr) throw detachErr;
  }

  const { error: deleteItemsErr } = await supabaseAdmin
    .from("carrito_items")
    .delete()
    .eq("id_carrito", carritoNum);
  if (deleteItemsErr) throw deleteItemsErr;

  const { error: deleteCartErr } = await supabaseAdmin
    .from("carritos")
    .delete()
    .eq("id_carrito", carritoNum);
  if (deleteCartErr) throw deleteCartErr;

  console.log("[cart] carrito cerrado eliminado", {
    source,
    id_orden: toPositiveInt(idOrden) || null,
    id_carrito: carritoNum,
    referencias_ordenes: rows.length,
  });
  return { cleaned: true, id_carrito: carritoNum };
};

const buildCarritoLockedError = (pendingOrderId = null) => {
  const err = new Error(
    "Este carrito tiene una orden en verificacion. No se puede modificar hasta que se procese o cancele.",
  );
  err.code = "CARRITO_BLOQUEADO_POR_ORDEN";
  err.httpStatus = 409;
  err.id_orden = toPositiveInt(pendingOrderId) || null;
  return err;
};

const findPendingUnverifiedOrderByCarrito = async (carritoId) => {
  const carritoNum = toPositiveInt(carritoId);
  if (!carritoNum) return null;
  const { data: orderRows, error: orderErr } = await supabaseAdmin
    .from("ordenes")
    .select("id_orden, id_carrito, checkout_finalizado, pago_verificado, orden_cancelada")
    .eq("id_carrito", carritoNum)
    .eq("checkout_finalizado", true)
    .eq("pago_verificado", false)
    .order("id_orden", { ascending: false })
    .limit(1);
  if (orderErr) throw orderErr;
  const orderRow = Array.isArray(orderRows) ? orderRows[0] : null;
  if (!orderRow?.id_orden || isTrue(orderRow?.orden_cancelada)) return null;
  return orderRow;
};

const assertCarritoUnlockedForMutation = async ({ carritoId, source = "cart_mutation" } = {}) => {
  const pendingOrder = await findPendingUnverifiedOrderByCarrito(carritoId);
  if (!pendingOrder) return null;
  console.warn("[cart] bloqueo por orden pendiente", {
    source,
    id_carrito: toPositiveInt(carritoId) || null,
    id_orden: toPositiveInt(pendingOrder?.id_orden) || null,
  });
  throw buildCarritoLockedError(pendingOrder?.id_orden);
};

const roundMoney = (value) => Math.round((Number(value) + Number.EPSILON) * 100) / 100;
const toFiniteMoney = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? roundMoney(num) : null;
};
const resolveSaldoAplicadoFromCarrito = ({ montoUsd, montoFinal }) => {
  const montoBase = toFiniteMoney(montoUsd);
  const montoFinalResolved = toFiniteMoney(montoFinal);
  if (!Number.isFinite(montoBase) || !Number.isFinite(montoFinalResolved)) return 0;
  const aplicado = roundMoney(montoBase - montoFinalResolved);
  return aplicado > 0 ? aplicado : 0;
};

const resolveSaldoAplicadoFromOrderSnapshot = ({
  usaSaldo = false,
  saldoAplicadoUsd = null,
  totalBrutoUsd = null,
  totalUsd = null,
  referencia = null,
} = {}) => {
  if (!isTrue(usaSaldo)) return 0;

  const storedSaldoAplicado = toFiniteMoney(saldoAplicadoUsd);
  if (Number.isFinite(storedSaldoAplicado) && storedSaldoAplicado > 0) {
    return storedSaldoAplicado;
  }

  const totalBruto = toFiniteMoney(totalBrutoUsd);
  const totalNeto = toFiniteMoney(totalUsd);
  if (Number.isFinite(totalBruto) && Number.isFinite(totalNeto)) {
    const diff = roundMoney(totalBruto - totalNeto);
    if (diff > 0) return diff;
  }

  if (String(referencia || "").trim().toUpperCase() === "SALDO") {
    if (Number.isFinite(totalBruto) && totalBruto > 0) return totalBruto;
    if (Number.isFinite(totalNeto) && totalNeto > 0) return totalNeto;
  }

  return 0;
};

const resolveSaldoAplicadoForOrderVerification = async ({
  orden = null,
  idCarrito = null,
} = {}) => {
  if (!orden || !isTrue(orden?.usa_saldo)) return 0;

  const storedSaldo = toFiniteMoney(orden?.saldo_aplicado_usd);
  if (Number.isFinite(storedSaldo) && storedSaldo > 0) {
    return storedSaldo;
  }

  const carritoNum = toPositiveInt(idCarrito ?? orden?.id_carrito);
  if (carritoNum) {
    const { data: carritoRow, error: carritoErr } = await supabaseAdmin
      .from("carritos")
      .select("saldo_usado, monto_usd, monto_final")
      .eq("id_carrito", carritoNum)
      .maybeSingle();
    if (carritoErr) throw carritoErr;
    if (carritoRow) {
      const saldoUsado = toFiniteMoney(carritoRow?.saldo_usado);
      if (Number.isFinite(saldoUsado) && saldoUsado > 0) {
        return saldoUsado;
      }
      const saldoByDiff = resolveSaldoAplicadoFromCarrito({
        montoUsd: carritoRow?.monto_usd,
        montoFinal: carritoRow?.monto_final,
      });
      if (Number.isFinite(saldoByDiff) && saldoByDiff > 0) {
        return saldoByDiff;
      }
    }
  }

  return resolveSaldoAplicadoFromOrderSnapshot({
    usaSaldo: orden?.usa_saldo,
    saldoAplicadoUsd: orden?.saldo_aplicado_usd,
    totalBrutoUsd: orden?.total_bruto_usd,
    totalUsd: orden?.total,
    referencia: orden?.referencia,
  });
};

const debitSaldoUsdFromUser = async ({
  idUsuario = null,
  montoUsd = 0,
  source = "saldo_debit",
} = {}) => {
  const targetUserId = toPositiveInt(idUsuario);
  const amount = toFiniteMoney(montoUsd);
  if (!targetUserId || !(amount > 0)) {
    return { debitado: false, monto: 0, saldoNuevo: null };
  }

  const { data: userRow, error: userErr } = await supabaseAdmin
    .from("usuarios")
    .select("saldo")
    .eq("id_usuario", targetUserId)
    .maybeSingle();
  if (userErr) throw userErr;

  const saldoActual = toFiniteMoney(userRow?.saldo);
  const saldoDisponible = Number.isFinite(saldoActual) && saldoActual > 0 ? saldoActual : 0;
  const saldoADebitar = Math.min(amount, saldoDisponible);
  if (!(saldoADebitar > 0) || roundMoney(saldoADebitar) < roundMoney(amount)) {
    const err = new Error("Saldo insuficiente para aplicar el descuento del carrito.");
    err.code = "SALDO_INSUFICIENTE";
    err.httpStatus = 409;
    throw err;
  }

  const saldoNuevo = roundMoney(Math.max(0, saldoDisponible - saldoADebitar));
  const { error: updSaldoErr } = await supabaseAdmin
    .from("usuarios")
    .update({ saldo: saldoNuevo })
    .eq("id_usuario", targetUserId);
  if (updSaldoErr) throw updSaldoErr;

  console.log("[checkout][saldo] debitado", {
    source,
    id_usuario: targetUserId,
    saldo_aplicado: amount,
    saldo_debitado: saldoADebitar,
    saldo_nuevo: saldoNuevo,
  });

  return {
    debitado: true,
    id_usuario: targetUserId,
    monto: saldoADebitar,
    saldoAplicado: amount,
    saldoNuevo,
  };
};
const isSaldoInsuficienteError = (err) => {
  const code = String(err?.code || "").trim().toUpperCase();
  if (code === "SALDO_INSUFICIENTE") return true;
  const msg = String(err?.message || "").toLowerCase();
  return msg.includes("saldo insuficiente");
};

const consumeSaldoFromCarritoIfNeeded = async ({
  carritoId,
  source = "checkout_process",
  saldoAplicadoFallback = 0,
  idUsuarioFallback = null,
} = {}) => {
  const carritoNum = Number(carritoId);
  if (!Number.isFinite(carritoNum) || carritoNum <= 0) {
    return { consumed: false, skipped: true, reason: "invalid_carrito" };
  }
  const saldoFallback = toFiniteMoney(saldoAplicadoFallback);
  const fallbackUserId = toPositiveInt(idUsuarioFallback);

  const { data: carritoRow, error: carritoErr } = await supabaseAdmin
    .from("carritos")
    .select("id_carrito, id_usuario, usa_saldo, monto_usd, monto_final, saldo_usado")
    .eq("id_carrito", carritoNum)
    .maybeSingle();
  if (carritoErr) throw carritoErr;
  if (!carritoRow?.id_carrito) {
    return { consumed: false, skipped: true, reason: "carrito_not_found" };
  }

  const usaSaldoCarrito = isTrue(carritoRow?.usa_saldo);
  const saldoUsadoRaw = toFiniteMoney(carritoRow?.saldo_usado);
  const saldoFallbackEnabled = Number.isFinite(saldoFallback) && saldoFallback > 0;
  const saldoUsadoEnabled = Number.isFinite(saldoUsadoRaw) && saldoUsadoRaw > 0;
  if (!usaSaldoCarrito && !saldoUsadoEnabled && !saldoFallbackEnabled) {
    return { consumed: false, skipped: true, reason: "saldo_not_enabled" };
  }

  let targetUserId = fallbackUserId || toPositiveInt(carritoRow?.id_usuario);
  if (!targetUserId) {
    throw new Error("No se pudo debitar saldo: carrito sin usuario válido.");
  }

  let saldoAplicado =
    Number.isFinite(saldoUsadoRaw) && saldoUsadoRaw > 0
      ? saldoUsadoRaw
      : resolveSaldoAplicadoFromCarrito({
          montoUsd: carritoRow?.monto_usd,
          montoFinal: carritoRow?.monto_final,
        });
  let saldoOrigen =
    Number.isFinite(saldoUsadoRaw) && saldoUsadoRaw > 0 ? "saldo_usado" : "carrito";
  if (!(saldoAplicado > 0) && Number.isFinite(saldoFallback) && saldoFallback > 0) {
    saldoAplicado = saldoFallback;
    saldoOrigen = "fallback";
  }
  if (!(saldoAplicado > 0)) {
    return {
      consumed: false,
      skipped: true,
      reason: "saldo_amount_zero",
      monto: 0,
      id_usuario: targetUserId,
    };
  }

  const debitResult = await debitSaldoUsdFromUser({
    idUsuario: targetUserId,
    montoUsd: saldoAplicado,
    source: `${source}:${saldoOrigen}`,
  });

  return {
    consumed: true,
    id_carrito: carritoNum,
    id_usuario: debitResult?.id_usuario || targetUserId,
    monto: debitResult?.monto || 0,
    saldo_aplicado: saldoAplicado,
    saldo_nuevo: debitResult?.saldoNuevo ?? null,
    saldo_origen: saldoOrigen,
  };
};
const resolveMontoFinal = ({ montoUsd, usaSaldo, saldoUsuario }) => {
  const montoBase = toFiniteMoney(montoUsd);
  if (!Number.isFinite(montoBase)) return null;
  if (!isTrue(usaSaldo)) return montoBase;
  const saldo = toFiniteMoney(saldoUsuario);
  const saldoAplicable = Number.isFinite(saldo) && saldo > 0 ? saldo : 0;
  return Math.max(0, roundMoney(montoBase - saldoAplicable));
};

const areCloseNumbers = (left, right, epsilon = 0.01) => {
  const a = Number(left);
  const b = Number(right);
  if (!Number.isFinite(a) && !Number.isFinite(b)) return true;
  if (!Number.isFinite(a) || !Number.isFinite(b)) return false;
  return Math.abs(a - b) <= epsilon;
};

const parseCaracasDateTimeValue = (fechaStr, horaStr) => {
  const fecha = String(fechaStr || "").trim().match(/\d{4}-\d{2}-\d{2}/)?.[0] || "";
  const hora = String(horaStr || "").trim().match(/\d{2}:\d{2}:\d{2}/)?.[0] || "";
  if (!fecha || !hora) return null;
  const dt = new Date(`${fecha}T${hora}-04:00`);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const isStoredRateWithinCurrentSlot = (fechaStr, horaStr, slotWindow = null) => {
  if (!slotWindow?.start || !slotWindow?.end) return false;
  const storedAt = parseCaracasDateTimeValue(fechaStr, horaStr);
  if (!storedAt) return false;
  return storedAt.getTime() >= slotWindow.start.getTime() && storedAt.getTime() < slotWindow.end.getTime();
};

const syncCarritoOfficialRate = async ({
  carritoId,
  idUsuario = null,
  carritoInfo = null,
  saldoUsuario = null,
  force = false,
} = {}) => {
  const carritoNum = Number(carritoId);
  if (!Number.isFinite(carritoNum) || carritoNum <= 0) {
    return {
      tasaBs: null,
      montoBs: null,
      montoFinal: null,
      snapshot: null,
      updated: false,
    };
  }

  let currentCarrito = carritoInfo;
  if (!currentCarrito) {
    const { data, error } = await supabaseAdmin
      .from("carritos")
      .select("monto_usd, monto_bs, tasa_bs, usa_saldo, monto_final, saldo_usado, fecha, hora")
      .eq("id_carrito", carritoNum)
      .maybeSingle();
    if (error) throw error;
    currentCarrito = data || null;
  }
  if (!currentCarrito) {
    return {
      tasaBs: null,
      montoBs: null,
      montoFinal: null,
      snapshot: null,
      updated: false,
    };
  }

  let saldoResolved = toFiniteMoney(saldoUsuario);
  if (isTrue(currentCarrito?.usa_saldo) && !Number.isFinite(saldoResolved)) {
    const usuarioNum = Number(idUsuario);
    if (Number.isFinite(usuarioNum) && usuarioNum > 0) {
      const { data: usuarioRow, error: usuarioErr } = await supabaseAdmin
        .from("usuarios")
        .select("saldo")
        .eq("id_usuario", usuarioNum)
        .maybeSingle();
      if (usuarioErr) throw usuarioErr;
      saldoResolved = toFiniteMoney(usuarioRow?.saldo);
    }
  }

  const montoFinal = resolveMontoFinal({
    montoUsd: currentCarrito?.monto_usd,
    usaSaldo: currentCarrito?.usa_saldo,
    saldoUsuario: saldoResolved,
  });
  const tasaBs = await getStrictStoredTasaActual();
  const snapshot = { rate: tasaBs };
  const montoBs =
    Number.isFinite(montoFinal) && Number.isFinite(tasaBs)
      ? roundMoney(montoFinal * tasaBs)
      : null;
  const saldoUsado = resolveSaldoAplicadoFromCarrito({
    montoUsd: currentCarrito?.monto_usd,
    montoFinal,
  });
  const slotWindow = getCurrentTasaSlotWindow();
  const storedTasaBs = parseOptionalCheckoutNumber(currentCarrito?.tasa_bs);
  const storedMontoBs = parseOptionalCheckoutNumber(currentCarrito?.monto_bs);
  const storedMontoFinal = parseOptionalCheckoutNumber(currentCarrito?.monto_final);
  const storedSaldoUsado = parseOptionalCheckoutNumber(currentCarrito?.saldo_usado);
  const slotCurrent = isStoredRateWithinCurrentSlot(
    currentCarrito?.fecha,
    currentCarrito?.hora,
    slotWindow,
  );

  const needsUpdate =
    force ||
    !slotCurrent ||
    !areCloseNumbers(storedTasaBs, tasaBs, 0.000001) ||
    !areCloseNumbers(storedMontoBs, montoBs, 0.01) ||
    !areCloseNumbers(storedMontoFinal, montoFinal, 0.01) ||
    !areCloseNumbers(storedSaldoUsado, saldoUsado, 0.01);
  let fechaResolved = currentCarrito?.fecha ?? null;
  let horaResolved = currentCarrito?.hora ?? null;

  if (needsUpdate) {
    const caracasNow = getCaracasDateTimeNow();
    fechaResolved = caracasNow.fecha;
    horaResolved = caracasNow.hora;
    const { error: updateErr } = await supabaseAdmin
      .from("carritos")
      .update({
        tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
        monto_bs: Number.isFinite(montoBs) ? montoBs : null,
        monto_final: Number.isFinite(montoFinal) ? montoFinal : null,
        saldo_usado: Number.isFinite(saldoUsado) ? saldoUsado : 0,
        fecha: fechaResolved,
        hora: horaResolved,
      })
      .eq("id_carrito", carritoNum);
    if (updateErr) throw updateErr;
  }

  return {
    tasaBs: Number.isFinite(tasaBs) ? tasaBs : null,
    montoBs: Number.isFinite(montoBs) ? montoBs : null,
    montoFinal: Number.isFinite(montoFinal) ? montoFinal : null,
    saldoUsado: Number.isFinite(saldoUsado) ? saldoUsado : 0,
    snapshot,
    fecha: fechaResolved,
    hora: horaResolved,
    updated: needsUpdate,
  };
};

const getOrCreateCarrito = async (idUsuario) => {
  const userId = toPositiveInt(idUsuario);
  if (!userId) return null;
  const { data, error } = await runSupabaseQueryWithRetry(
    () =>
      supabaseAdmin
        .from("carritos")
        .select("id_carrito")
        .eq("id_usuario", userId)
        // fecha_creacion es DATE; para evitar empates del mismo día usamos id_carrito.
        .order("id_carrito", { ascending: false })
        .limit(1)
        .maybeSingle(),
    "cart:getOrCreateCarrito:select",
  );
  if (error) throw error;
  if (data?.id_carrito) {
    const shouldRotate = await shouldRotateLatestCarrito(data.id_carrito);
    if (!shouldRotate) return data.id_carrito;
    return createCarritoForUser(userId);
  }
  return createCarritoForUser(userId);
};

const BINANCE_RAW_CACHE_MS = 2 * 60 * 1000;
const TASA_OFICIAL_WINDOW_MS = 2 * 60 * 60 * 1000;
const TASA_OFICIAL_SLOT_HOURS = 2;
const TASA_CONFIG_CACHE_MS = 60 * 1000;
const DEFAULT_TASA_MARKUP = 1.06;
const TASA_OFICIAL_STORAGE_FIELDS =
  "markup, actualizado_en, actualizado_por, tasa_actual";
let cachedBinanceP2PRawRate = { value: null, ts: 0 };
let cachedTasaMarkup = { value: DEFAULT_TASA_MARKUP, ts: 0 };
let cachedOfficialP2PRate = { slotKey: "", snapshot: null, ts: 0 };
const TASA_REFRESH_BUFFER_MS = 600;
let tasaRefreshSchedulerStarted = false;
let tasaRefreshTimeoutId = null;

const isMissingTasaConfigTableError = (err) => {
  const msg = String(err?.message || err?.details || err?.hint || "").toLowerCase();
  return err?.code === "42P01" || msg.includes("tasa_config") && msg.includes("does not exist");
};

const normalizeTasaMarkupValue = (value, fallback = DEFAULT_TASA_MARKUP) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1 || parsed > 5) return fallback;
  return parsed;
};

const roundRateValue = (value, precision = 6) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  const factor = 10 ** precision;
  return Math.round((parsed + Number.EPSILON) * factor) / factor;
};

const pad2 = (value) => String(value).padStart(2, "0");

const buildCaracasDateTime = (dateStr, hour = 0, minute = 0, second = 0) => {
  const normalizedDate = String(dateStr || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)) return null;
  const hh = Math.max(0, Math.min(23, Number(hour) || 0));
  const mm = Math.max(0, Math.min(59, Number(minute) || 0));
  const ss = Math.max(0, Math.min(59, Number(second) || 0));
  const dt = new Date(`${normalizedDate}T${pad2(hh)}:${pad2(mm)}:${pad2(ss)}-04:00`);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const getCaracasDateTimeNow = () => {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    hourCycle: "h23",
  }).formatToParts(now);
  const get = (type, fallback = "00") => parts.find((part) => part.type === type)?.value || fallback;
  const rawHour = Number(get("hour", "0"));
  const hour = rawHour === 24 ? 0 : rawHour;
  const fecha = `${get("year", "0000")}-${get("month")}-${get("day")}`;
  const hora = `${pad2(hour)}:${get("minute")}:${get("second")}`;
  const date = buildCaracasDateTime(fecha, hour, Number(get("minute", "0")), Number(get("second", "0")));
  return { fecha, hora, date };
};

const getCurrentTasaSlotWindow = () => {
  const { dateStr, hour, minute } = getCaracasClock();
  const safeHour = Math.max(0, Number(hour) || 0);
  const safeMinute = Math.max(0, Number(minute) || 0);
  const slotStartHour = Math.floor((safeHour * 60 + safeMinute) / (TASA_OFICIAL_SLOT_HOURS * 60))
    * TASA_OFICIAL_SLOT_HOURS;
  const start = buildCaracasDateTime(dateStr, slotStartHour, 0, 0);
  if (!start) return null;
  const end = new Date(start.getTime() + TASA_OFICIAL_WINDOW_MS);
  return {
    key: `${dateStr}:${pad2(slotStartHour)}`,
    start,
    end,
  };
};

const getNextTasaRefreshDelayMs = () => {
  const slotWindow = getCurrentTasaSlotWindow();
  const nextAt = slotWindow?.end?.getTime();
  if (!Number.isFinite(nextAt)) {
    return TASA_OFICIAL_WINDOW_MS;
  }
  return Math.max(1000, nextAt - Date.now() + TASA_REFRESH_BUFFER_MS);
};

const isMissingTasaOfficialStorageError = (err) => {
  const msg = String(err?.message || err?.details || err?.hint || "").toLowerCase();
  return (
    err?.code === "42P01" ||
    err?.code === "42703" ||
    (msg.includes("tasa_config") && msg.includes("does not exist")) ||
    msg.includes("tasa_actual") ||
    msg.includes("tasa_oficial") ||
    msg.includes("tasa_binance") ||
    msg.includes("markup_aplicado") ||
    msg.includes("vigente_desde") ||
    msg.includes("vigente_hasta") ||
    msg.includes("tasa_generada_en")
  );
};

const buildOfficialRateSnapshot = (row = {}, fallbackMarkup = DEFAULT_TASA_MARKUP) => ({
  rate: Number.isFinite(Number(row?.tasa_actual))
    ? roundMoney(Number(row.tasa_actual))
    : null,
  rawRate: null,
  markup: normalizeTasaMarkupValue(row?.markup, fallbackMarkup),
  generatedAt: row?.actualizado_en || null,
  validFrom: null,
  validUntil: null,
  stale: Boolean(row?.stale),
});

const isOfficialRateSnapshotCurrent = (snapshot = {}, slotWindow = null) => {
  if (!slotWindow || !Number.isFinite(Number(snapshot?.rate))) return false;
  const validFrom = new Date(snapshot?.validFrom || "");
  const validUntil = new Date(snapshot?.validUntil || "");
  if (!Number.isNaN(validFrom.getTime()) && !Number.isNaN(validUntil.getTime())) {
    return (
      validFrom.getTime() === slotWindow.start.getTime() &&
      validUntil.getTime() === slotWindow.end.getTime()
    );
  }
  const generatedAt = new Date(snapshot?.generatedAt || "");
  if (Number.isNaN(generatedAt.getTime())) return false;
  return (
    generatedAt.getTime() >= slotWindow.start.getTime() &&
    generatedAt.getTime() < slotWindow.end.getTime()
  );
};

const getCachedOfficialRateSnapshot = (slotKey = "") => {
  if (!slotKey) return null;
  if (!cachedOfficialP2PRate?.snapshot) return null;
  if (cachedOfficialP2PRate.slotKey !== slotKey) return null;
  return cachedOfficialP2PRate.snapshot;
};

const setCachedOfficialRateSnapshot = (slotKey = "", snapshot = null) => {
  cachedOfficialP2PRate = {
    slotKey: slotKey || "",
    snapshot: snapshot || null,
    ts: Date.now(),
  };
  return snapshot;
};

const getGlobalTasaMarkup = async ({ force = false } = {}) => {
  const now = Date.now();
  if (!force && now - cachedTasaMarkup.ts < TASA_CONFIG_CACHE_MS) {
    return cachedTasaMarkup.value;
  }
  try {
    const { data, error } = await supabaseAdmin
      .from("tasa_config")
      .select("markup")
      .eq("id", 1)
      .maybeSingle();
    if (error) throw error;
    const markup = normalizeTasaMarkupValue(data?.markup, DEFAULT_TASA_MARKUP);
    cachedTasaMarkup = { value: markup, ts: now };
    return markup;
  } catch (err) {
    if (isMissingTasaConfigTableError(err)) {
      return cachedTasaMarkup.value || DEFAULT_TASA_MARKUP;
    }
    if (cachedTasaMarkup.value) return cachedTasaMarkup.value;
    return DEFAULT_TASA_MARKUP;
  }
};

const setGlobalTasaMarkup = async ({ markup, updatedBy = null }) => {
  const nextMarkup = normalizeTasaMarkupValue(markup, Number.NaN);
  if (!Number.isFinite(nextMarkup)) {
    const err = new Error("Markup inválido. Debe ser >= 1 y <= 5.");
    err.code = "INVALID_TASA_MARKUP";
    throw err;
  }
  const payload = {
    id: 1,
    markup: nextMarkup,
    actualizado_en: new Date().toISOString(),
    actualizado_por: Number.isFinite(Number(updatedBy)) ? Number(updatedBy) : null,
  };
  const { data, error } = await supabaseAdmin
    .from("tasa_config")
    .upsert(payload, { onConflict: "id" })
    .select("markup")
    .single();
  if (error) {
    if (isMissingTasaConfigTableError(error)) {
      const err = new Error("Falta aplicar la migracion de tasa_config en Supabase.");
      err.code = "TASA_CONFIG_MISSING";
      throw err;
    }
    throw error;
  }
  const saved = normalizeTasaMarkupValue(data?.markup, nextMarkup);
  cachedTasaMarkup = { value: saved, ts: Date.now() };
  cachedOfficialP2PRate = { slotKey: "", snapshot: null, ts: 0 };
  return saved;
};

const fetchBinanceP2PRawRate = async (asset = "USDT", fiat = "VES") => {
  const now = Date.now();
  if (cachedBinanceP2PRawRate.value && now - cachedBinanceP2PRawRate.ts < BINANCE_RAW_CACHE_MS) {
    return cachedBinanceP2PRawRate.value;
  }

  const body = {
    page: 1,
    rows: 10,
    payTypes: [],
    asset,
    fiat,
    tradeType: "BUY",
    publisherType: null,
  };

  const resp = await fetch(BINANCE_P2P_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    throw new Error(`Binance P2P error ${resp.status}`);
  }
  const json = await resp.json();
  const precios = (json?.data || [])
    .map((item) => Number(item?.adv?.price))
    .filter((n) => Number.isFinite(n));
  if (!precios.length) {
    throw new Error("Binance P2P sin precios");
  }
  const top = precios.slice(0, 5);
  const rate =
    top.reduce((acc, val) => acc + val, 0) / top.length;
  cachedBinanceP2PRawRate = { value: rate, ts: now };
  return rate;
};

const getOfficialP2PRateSnapshotFallback = async ({
  force = false,
  markup = DEFAULT_TASA_MARKUP,
  slotWindow = null,
} = {}) => {
  const resolvedSlot = slotWindow || getCurrentTasaSlotWindow();
  const slotKey = resolvedSlot?.key || "fallback";
  const cached = !force ? getCachedOfficialRateSnapshot(slotKey) : null;
  if (cached) return cached;

  const rawRate = await fetchBinanceP2PRawRate();
  const normalizedMarkup = normalizeTasaMarkupValue(markup, DEFAULT_TASA_MARKUP);
  const snapshot = {
    rate: roundMoney(rawRate * normalizedMarkup),
    rawRate: null,
    markup: normalizedMarkup,
    generatedAt: new Date().toISOString(),
    validFrom: null,
    validUntil: null,
    stale: false,
  };
  return setCachedOfficialRateSnapshot(slotKey, snapshot);
};

const getStoredOfficialP2PRateSnapshot = async (fallbackMarkup = DEFAULT_TASA_MARKUP) => {
  const { data, error } = await supabaseAdmin
    .from("tasa_config")
    .select(TASA_OFICIAL_STORAGE_FIELDS)
    .eq("id", 1)
    .maybeSingle();
  if (error) throw error;
  return buildOfficialRateSnapshot(data || {}, fallbackMarkup);
};

const getOfficialP2PRateSnapshot = async ({ force = false } = {}) => {
  const slotWindow = getCurrentTasaSlotWindow();
  const slotKey = slotWindow?.key || "official";
  const cached = !force ? getCachedOfficialRateSnapshot(slotKey) : null;
  if (cached) return cached;

  const currentMarkup = await getGlobalTasaMarkup({ force });
  let storedSnapshot = null;
  try {
    storedSnapshot = await getStoredOfficialP2PRateSnapshot(currentMarkup);
    if (!force && isOfficialRateSnapshotCurrent(storedSnapshot, slotWindow)) {
      return setCachedOfficialRateSnapshot(slotKey, storedSnapshot);
    }
  } catch (err) {
    if (isMissingTasaOfficialStorageError(err)) {
      return getOfficialP2PRateSnapshotFallback({
        force,
        markup: currentMarkup,
        slotWindow,
      });
    }
    throw err;
  }

  try {
    const rawRate = await fetchBinanceP2PRawRate();
    const rate = roundMoney(rawRate * currentMarkup);
    const payload = {
      id: 1,
      markup: currentMarkup,
      tasa_actual: rate,
      actualizado_en: new Date().toISOString(),
      actualizado_por: null,
    };
    const { data: saved, error: saveErr } = await supabaseAdmin
      .from("tasa_config")
      .upsert(payload, { onConflict: "id" })
      .select(TASA_OFICIAL_STORAGE_FIELDS)
      .single();
    if (saveErr) throw saveErr;
    const snapshot = buildOfficialRateSnapshot(saved || payload, currentMarkup);
    return setCachedOfficialRateSnapshot(slotKey, snapshot);
  } catch (err) {
    if (isMissingTasaOfficialStorageError(err)) {
      return getOfficialP2PRateSnapshotFallback({
        force,
        markup: currentMarkup,
        slotWindow,
      });
    }
    if (storedSnapshot?.rate) {
      return setCachedOfficialRateSnapshot(slotKey, {
        ...storedSnapshot,
        stale: true,
      });
    }
    throw err;
  }
};

const refreshCurrentTasaActual = async ({ reason = "scheduler", force = false } = {}) => {
  try {
    const snapshot = await getOfficialP2PRateSnapshot({ force });
    console.log(
      `[Tasa] ${reason}: tasa_actual=${snapshot?.rate ?? "n/a"} vigente_desde=${snapshot?.validFrom || "n/a"} vigente_hasta=${snapshot?.validUntil || "n/a"}`,
    );
  } catch (err) {
    console.error(`[Tasa] ${reason} error`, err);
  }
};

const startTasaActualScheduler = () => {
  if (tasaRefreshSchedulerStarted) return;
  tasaRefreshSchedulerStarted = true;

  const scheduleNext = () => {
    if (!tasaRefreshSchedulerStarted) return;
    if (tasaRefreshTimeoutId) {
      clearTimeout(tasaRefreshTimeoutId);
      tasaRefreshTimeoutId = null;
    }
    const waitMs = getNextTasaRefreshDelayMs();
    tasaRefreshTimeoutId = setTimeout(async () => {
      try {
        await refreshCurrentTasaActual({ reason: "slot" });
      } finally {
        scheduleNext();
      }
    }, waitMs);
  };

  console.log(`[Tasa] Scheduler activo; revisa tasa_config.tasa_actual por slots de 2 horas.`);
  refreshCurrentTasaActual({ reason: "init" });
  scheduleNext();
};

const isMissingMonthlyMetricsStorageError = (err) => {
  return (
    isMissingTableError(err, MONTHLY_METRICS_TABLE) ||
    isMissingTableError(err, MONTHLY_ACTIVE_CLIENTS_TABLE) ||
    isMissingColumnError(err, "periodo_mes") ||
    isMissingColumnError(err, "clientes_activos") ||
    isMissingColumnError(err, "ventas_activas") ||
    isMissingColumnError(err, "id_usuario")
  );
};

const getPreviousMonthEndDateFromCaracasDate = (dateStr = "") => {
  const match = String(dateStr || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
    return null;
  }
  const prevMonthEnd = new Date(Date.UTC(year, month - 1, 0));
  return [
    prevMonthEnd.getUTCFullYear(),
    pad2(prevMonthEnd.getUTCMonth() + 1),
    pad2(prevMonthEnd.getUTCDate()),
  ].join("-");
};

const getMonthKeyFromDate = (dateStr = "") => {
  const value = String(dateStr || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return "";
  return value.slice(0, 7);
};

const monthKeyToIndex = (value = "") => {
  const match = String(value || "").match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) return null;
  return year * 12 + (month - 1);
};

const indexToMonthKey = (idx) => {
  if (!Number.isInteger(idx)) return "";
  const year = Math.floor(idx / 12);
  const month = (idx % 12) + 1;
  return `${year}-${pad2(month)}`;
};

const getMonthEndDateFromMonthKey = (monthKey = "") => {
  const idx = monthKeyToIndex(monthKey);
  if (idx === null) return "";
  const normalized = indexToMonthKey(idx);
  const match = normalized.match(/^(\d{4})-(\d{2})$/);
  if (!match) return "";
  const year = Number(match[1]);
  const month = Number(match[2]);
  const lastDay = new Date(Date.UTC(year, month, 0)).getUTCDate();
  return `${match[1]}-${match[2]}-${pad2(lastDay)}`;
};

const buildMonthKeyRange = (startMonthKey = "", endMonthKey = "") => {
  const startIdx = monthKeyToIndex(startMonthKey);
  const endIdx = monthKeyToIndex(endMonthKey);
  if (startIdx === null || endIdx === null || startIdx > endIdx) return [];
  const rows = [];
  for (let idx = startIdx; idx <= endIdx; idx += 1) {
    rows.push(indexToMonthKey(idx));
  }
  return rows.filter(Boolean);
};

const listClosedMonthlyMetricMonthKeys = async ({ periodEnd = "" } = {}) => {
  const periodEndVal = String(periodEnd || "").trim();
  const { data, error } = await runSupabaseQueryWithRetry(
    () => {
      let query = supabaseAdmin
        .from(MONTHLY_METRICS_TABLE)
        .select("periodo_mes")
        .not("periodo_mes", "is", null)
        .order("periodo_mes", { ascending: true });
      if (periodEndVal) {
        query = query.lte("periodo_mes", periodEndVal);
      }
      return query;
    },
    `monthly_metrics:closed_months:list:${periodEndVal || "all"}`,
  );
  if (error) throw error;
  return [...new Set((data || []).map((row) => getMonthKeyFromDate(row?.periodo_mes)).filter(Boolean))];
};

const getFirstVentasMonthKeyUntil = async ({ periodEnd = "" } = {}) => {
  const periodEndVal = String(periodEnd || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodEndVal)) return "";
  const { data, error } = await runSupabaseQueryWithRetry(
    () =>
      supabaseAdmin
        .from("ventas")
        .select("fecha_pago")
        .not("fecha_pago", "is", null)
        .lte("fecha_pago", periodEndVal)
        .order("fecha_pago", { ascending: true })
        .limit(1),
    `monthly_metrics:first_sale_month:${periodEndVal}`,
  );
  if (error) throw error;
  const firstDate = String(data?.[0]?.fecha_pago || "").trim();
  return getMonthKeyFromDate(firstDate);
};

const getFirstHistorialMonthKeyUntil = async ({ periodEnd = "" } = {}) => {
  const periodEndVal = String(periodEnd || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodEndVal)) return "";
  const { data, error } = await runSupabaseQueryWithRetry(
    () =>
      supabaseAdmin
        .from("historial_ventas")
        .select("fecha_pago")
        .not("fecha_pago", "is", null)
        .not("id_usuario_cliente", "is", null)
        .lte("fecha_pago", periodEndVal)
        .order("fecha_pago", { ascending: true })
        .limit(1),
    `monthly_metrics:first_historial_month:${periodEndVal}`,
  );
  if (error) throw error;
  const firstDate = String(data?.[0]?.fecha_pago || "").trim();
  return getMonthKeyFromDate(firstDate);
};

const fetchVentasRowsUntil = async (periodEnd = "") => {
  const periodEndVal = String(periodEnd || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodEndVal)) return [];
  return fetchAllSupabaseRows(
    (from, to) =>
      supabaseAdmin
        .from("ventas")
        .select(
          "id_venta, id_usuario, fecha_pago, fecha_corte, infinito, suspendido, pendiente, pago_rechazado",
        )
        .not("id_usuario", "is", null)
        .not("fecha_pago", "is", null)
        .lte("fecha_pago", periodEndVal)
        .range(from, to),
    { label: `monthly_metrics:ventas:${periodEndVal}` },
  );
};

const fetchHistorialClienteRowsUntil = async (periodEnd = "") => {
  const periodEndVal = String(periodEnd || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodEndVal)) return [];
  return fetchAllSupabaseRows(
    (from, to) =>
      supabaseAdmin
        .from("historial_ventas")
        .select("fecha_pago, id_usuario_cliente")
        .not("fecha_pago", "is", null)
        .not("id_usuario_cliente", "is", null)
        .lte("fecha_pago", periodEndVal)
        .range(from, to),
    { label: `monthly_metrics:historial_clientes:${periodEndVal}` },
  );
};

const buildMonthlyBuyerUserIdsFromHistorialRows = (historialRows = [], monthKey = "") => {
  const monthKeyVal = String(monthKey || "").trim();
  if (!/^\d{4}-\d{2}$/.test(monthKeyVal)) return [];
  return uniqPositiveIds(
    (historialRows || [])
      .filter((row) => getMonthKeyFromDate(row?.fecha_pago) === monthKeyVal)
      .map((row) => row?.id_usuario_cliente),
  );
};

const buildMonthlyActiveMetricsSnapshotFromVentasRows = (
  ventasRows = [],
  periodEnd = "",
  monthlyBuyerUserIds = [],
) => {
  const periodEndVal = String(periodEnd || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodEndVal)) {
    throw new Error("periodo_mes inválido para calcular snapshot mensual.");
  }
  const monthKey = getMonthKeyFromDate(periodEndVal);
  const buyerUserIds = uniqPositiveIds(monthlyBuyerUserIds);
  const activeRows = (ventasRows || []).filter((row) => {
    const notSuspended = !isTrue(row?.suspendido);
    const notPending = !isTrue(row?.pendiente);
    const notRejected = !isTrue(row?.pago_rechazado);
    const paidBeforeOrOnPeriod = String(row?.fecha_pago || "").trim() <= periodEndVal;
    const activeByDate =
      isTrue(row?.infinito) || (row?.fecha_corte && String(row.fecha_corte) >= periodEndVal);
    return paidBeforeOrOnPeriod && notSuspended && notPending && notRejected && activeByDate;
  });

  return {
    periodo_mes: periodEndVal,
    monthKey,
    clientes_activos: buyerUserIds.length,
    ventas_activas: activeRows.length,
    activeUserIds: buyerUserIds,
  };
};

const computeMonthlyActiveMetricsSnapshot = async (periodEnd) => {
  const ventasRows = await fetchVentasRowsUntil(periodEnd);
  const historialRows = await fetchHistorialClienteRowsUntil(periodEnd);
  const monthKey = getMonthKeyFromDate(periodEnd);
  const monthlyBuyerUserIds = buildMonthlyBuyerUserIdsFromHistorialRows(historialRows, monthKey);
  return buildMonthlyActiveMetricsSnapshotFromVentasRows(
    ventasRows,
    periodEnd,
    monthlyBuyerUserIds,
  );
};

const persistMonthlyActiveClientsSnapshot = async (snapshot = {}) => {
  const periodEnd = String(snapshot?.periodo_mes || "").trim();
  const activeUserIds = uniqPositiveIds(snapshot?.activeUserIds || []);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodEnd)) {
    throw new Error("periodo_mes inválido para guardar clientes activos.");
  }

  const { error: deleteErr } = await runSupabaseQueryWithRetry(
    () => supabaseAdmin.from(MONTHLY_ACTIVE_CLIENTS_TABLE).delete().eq("periodo_mes", periodEnd),
    `monthly_metrics:active_clients:delete:${periodEnd}`,
  );
  if (deleteErr) throw deleteErr;

  if (!activeUserIds.length) return;

  const chunkSize = 500;
  for (let i = 0; i < activeUserIds.length; i += chunkSize) {
    const batchRows = activeUserIds.slice(i, i + chunkSize).map((idUsuario) => ({
      periodo_mes: periodEnd,
      id_usuario: idUsuario,
    }));
    const { error: insertErr } = await runSupabaseQueryWithRetry(
      () => supabaseAdmin.from(MONTHLY_ACTIVE_CLIENTS_TABLE).insert(batchRows),
      `monthly_metrics:active_clients:insert:${periodEnd}:chunk:${Math.floor(i / chunkSize) + 1}`,
    );
    if (insertErr) throw insertErr;
  }
};

const persistMonthlyMetricsSummary = async (snapshot = {}) => {
  const periodEnd = String(snapshot?.periodo_mes || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodEnd)) {
    throw new Error("periodo_mes inválido para guardar métricas mensuales.");
  }
  const payload = {
    periodo_mes: periodEnd,
    clientes_activos: Number(snapshot?.clientes_activos) || 0,
    ventas_activas: Number(snapshot?.ventas_activas) || 0,
  };

  const { data: existingRows, error: existingErr } = await runSupabaseQueryWithRetry(
    () => supabaseAdmin.from(MONTHLY_METRICS_TABLE).select("id").eq("periodo_mes", periodEnd),
    `monthly_metrics:summary:select:${periodEnd}`,
  );
  if (existingErr) throw existingErr;
  const existingIds = uniqPositiveIds((existingRows || []).map((row) => row?.id));

  if (existingIds.length) {
    if (existingIds.length > 1) {
      const duplicateIds = existingIds.slice(1);
      const { error: duplicateDeleteErr } = await runSupabaseQueryWithRetry(
        () => supabaseAdmin.from(MONTHLY_METRICS_TABLE).delete().in("id", duplicateIds),
        `monthly_metrics:summary:dedupe_delete:${periodEnd}`,
      );
      if (duplicateDeleteErr) throw duplicateDeleteErr;
    }
    const targetId = existingIds[0];
    const { error: updateErr } = await runSupabaseQueryWithRetry(
      () => supabaseAdmin.from(MONTHLY_METRICS_TABLE).update(payload).eq("id", targetId),
      `monthly_metrics:summary:update:${periodEnd}`,
    );
    if (updateErr) throw updateErr;
    return;
  }

  const { error: insertErr } = await runSupabaseQueryWithRetry(
    () => supabaseAdmin.from(MONTHLY_METRICS_TABLE).insert(payload),
    `monthly_metrics:summary:insert:${periodEnd}`,
  );
  if (insertErr) throw insertErr;
};

const runMonthlyMetricsClosureIfNeeded = async ({ reason = "scheduler", force = false } = {}) => {
  if (!MONTHLY_METRICS_CLOSURE_ENABLED) {
    return { ok: false, skipped: true, reason: "disabled" };
  }
  if (monthlyMetricsClosureSchemaMissing) {
    return { ok: false, skipped: true, reason: "schema_missing" };
  }
  if (monthlyMetricsClosureInProgress) {
    return { ok: false, skipped: true, reason: "in_progress" };
  }

  monthlyMetricsClosureInProgress = true;
  monthlyMetricsClosureLastRunAt = new Date().toISOString();
  monthlyMetricsClosureLastError = null;

  try {
    const { dateStr } = getCaracasClock();
    const periodEnd = getPreviousMonthEndDateFromCaracasDate(dateStr);
    if (!periodEnd) {
      return { ok: false, skipped: true, reason: "invalid_date" };
    }
    const targetMonthKey = getMonthKeyFromDate(periodEnd);

    if (!force && targetMonthKey && targetMonthKey === monthlyMetricsClosureLastMonthKey) {
      return {
        ok: true,
        skipped: true,
        reason: "already_processed_runtime",
        periodEnd,
        monthKey: targetMonthKey,
      };
    }

    const closedMonthKeys = await listClosedMonthlyMetricMonthKeys({ periodEnd });
    const closedMonthSet = new Set(closedMonthKeys);
    let targetMonthKeys = [];

    if (force) {
      targetMonthKeys = targetMonthKey ? [targetMonthKey] : [];
    } else {
      const firstHistorialMonthKey = await getFirstHistorialMonthKeyUntil({ periodEnd });
      const firstVentasMonthKey = await getFirstVentasMonthKeyUntil({ periodEnd });
      const firstClosedMonthKey = closedMonthKeys[0] || "";
      const startMonthKey =
        firstHistorialMonthKey ||
        firstVentasMonthKey ||
        firstClosedMonthKey ||
        targetMonthKey;
      const fullRange = buildMonthKeyRange(startMonthKey, targetMonthKey);
      targetMonthKeys = fullRange.filter((monthKey) => !closedMonthSet.has(monthKey));
    }

    if (!targetMonthKeys.length) {
      monthlyMetricsClosureLastMonthKey = targetMonthKey || monthlyMetricsClosureLastMonthKey;
      return {
        ok: true,
        skipped: true,
        reason: "already_closed",
        periodEnd,
        monthKey: targetMonthKey,
      };
    }

    const ventasRows = await fetchVentasRowsUntil(periodEnd);
    const historialRows = await fetchHistorialClienteRowsUntil(periodEnd);
    const processedMonths = [];
    for (const monthKey of targetMonthKeys) {
      const monthPeriodEnd = getMonthEndDateFromMonthKey(monthKey);
      if (!monthPeriodEnd) continue;
      const monthlyBuyerUserIds = buildMonthlyBuyerUserIdsFromHistorialRows(
        historialRows,
        monthKey,
      );
      const snapshot = buildMonthlyActiveMetricsSnapshotFromVentasRows(
        ventasRows,
        monthPeriodEnd,
        monthlyBuyerUserIds,
      );
      await persistMonthlyActiveClientsSnapshot(snapshot);
      await persistMonthlyMetricsSummary(snapshot);
      processedMonths.push({
        monthKey,
        periodEnd: monthPeriodEnd,
        clientesActivos: snapshot.clientes_activos,
        ventasActivas: snapshot.ventas_activas,
      });
    }

    const lastProcessed = processedMonths[processedMonths.length - 1] || null;
    monthlyMetricsClosureLastMonthKey =
      lastProcessed?.monthKey || targetMonthKey || monthlyMetricsClosureLastMonthKey;
    if (processedMonths.length === 1) {
      const row = processedMonths[0];
      console.log(
        `[Metricas] Cierre mensual ${row.monthKey} guardado (${reason}): clientes_activos=${row.clientesActivos}, ventas_activas=${row.ventasActivas}`,
      );
    } else {
      console.log(
        `[Metricas] Cierre mensual backfill guardado (${reason}): meses=${processedMonths.length}, desde=${processedMonths[0]?.monthKey || "-"}, hasta=${lastProcessed?.monthKey || "-"}`,
      );
    }

    return {
      ok: true,
      skipped: false,
      reason,
      periodEnd: lastProcessed?.periodEnd || periodEnd,
      monthKey: lastProcessed?.monthKey || targetMonthKey,
      clientesActivos: lastProcessed?.clientesActivos || 0,
      ventasActivas: lastProcessed?.ventasActivas || 0,
      monthsProcessed: processedMonths.length,
      processedMonths,
    };
  } catch (err) {
    monthlyMetricsClosureLastError = err?.message || String(err);
    if (isMissingMonthlyMetricsStorageError(err)) {
      monthlyMetricsClosureSchemaMissing = true;
      console.warn(
        `[Metricas] Worker de cierre mensual desactivado: faltan tablas/columnas (${err?.message || err}).`,
      );
      return {
        ok: false,
        skipped: true,
        reason: "schema_missing",
        error: err?.message || String(err),
      };
    }
    console.error("[Metricas] Error al guardar cierre mensual", err);
    return {
      ok: false,
      skipped: false,
      reason: "error",
      error: err?.message || String(err),
    };
  } finally {
    monthlyMetricsClosureInProgress = false;
  }
};

const triggerMonthlyMetricsClosureCheck = (label = "tick") => {
  runMonthlyMetricsClosureIfNeeded({ reason: label }).catch((err) => {
    console.error(`[Metricas] Scheduler cierre mensual ${label} error`, err);
  });
};

const startMonthlyMetricsClosureScheduler = () => {
  if (!MONTHLY_METRICS_CLOSURE_ENABLED) return;
  if (monthlyMetricsClosureSchedulerStarted) return;
  monthlyMetricsClosureSchedulerStarted = true;
  console.log(
    `[Metricas] Worker de cierre mensual activo (cada ${Math.round(
      MONTHLY_METRICS_CLOSURE_INTERVAL_MS / 1000,
    )}s).`,
  );
  monthlyMetricsClosureIntervalId = setInterval(() => {
    triggerMonthlyMetricsClosureCheck("tick");
  }, MONTHLY_METRICS_CLOSURE_INTERVAL_MS);
  triggerMonthlyMetricsClosureCheck("init");
};

// Tasa Binance P2P USDT/VES (promedio top ofertas BUY) con markup global aplicado.
app.get("/api/p2p/rate", async (_req, res) => {
  try {
    const snapshot = await getStoredOfficialP2PRateSnapshot();
    res.json({
      rate: snapshot?.rate ?? null,
      tasa_actual: snapshot?.rate ?? null,
      raw_rate: snapshot?.rawRate ?? null,
      markup: snapshot?.markup ?? null,
      generated_at: snapshot?.generatedAt ?? null,
      valid_from: snapshot?.validFrom ?? null,
      valid_until: snapshot?.validUntil ?? null,
      stale: snapshot?.stale === true,
    });
  } catch (err) {
    console.error("[p2p rate] error", err);
    res.status(502).json({ error: "No se pudo obtener la tasa P2P" });
  }
});

app.get("/api/p2p/markup", async (_req, res) => {
  try {
    const markup = await getGlobalTasaMarkup();
    res.json({ markup });
  } catch (err) {
    console.error("[p2p markup get] error", err);
    res.status(500).json({ error: "No se pudo obtener el markup de tasa" });
  }
});

app.put("/api/p2p/markup", async (req, res) => {
  try {
    const idUsuario = await requireSuperadminSession(req);
    const markup = Number(req.body?.markup);
    if (!Number.isFinite(markup) || markup < 1 || markup > 5) {
      return res.status(400).json({ error: "markup inválido. Debe ser >= 1 y <= 5" });
    }
    const savedMarkup = await setGlobalTasaMarkup({ markup, updatedBy: idUsuario });
    await refreshCurrentTasaActual({ reason: "markup_update", force: true });
    return res.json({ ok: true, markup: savedMarkup });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === SUPERADMIN_REQUIRED || err?.message === SUPERADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo superadmin" });
    }
    if (err?.code === "INVALID_TASA_MARKUP") {
      return res.status(400).json({ error: err.message });
    }
    if (err?.code === "TASA_CONFIG_MISSING") {
      return res.status(500).json({ error: err.message });
    }
    console.error("[p2p markup put] error", err);
    return res.status(500).json({ error: "No se pudo actualizar el markup de tasa" });
  }
});

// Endpoint para agregar/actualizar/eliminar items del carrito.
// Se maneja por delta: cantidad positiva suma, negativa resta; si el resultado es <=0 se elimina el item.
app.post("/api/cart/item", async (req, res) => {
  console.log("[cart:item] body", req.body);
  const {
    id_precio: idPrecioRaw,
    id_precio_especial: idPrecioEspecialRaw,
    delta,
    meses,
    renovacion = false,
    id_venta: idVentaRaw,
    id_cuenta: idCuentaRaw,
    id_perfil: idPerfilRaw,
  } = req.body || {};
  if (!idPrecioRaw || delta === undefined) {
    return res
      .status(400)
      .json({ error: "id_precio y delta son requeridos" });
  }

  const parsedDelta = Number(delta);
  if (Number.isNaN(parsedDelta)) {
    return res.status(400).json({ error: "delta debe ser numérico" });
  }
  const idPrecio = toPositiveInt(idPrecioRaw);
  if (!idPrecio) {
    return res.status(400).json({ error: "id_precio inválido" });
  }

  const normalizeCartNullableBodyId = (rawValue, fieldName) => {
    if (rawValue === undefined) return { ok: true, value: undefined };
    if (rawValue === null) return { ok: true, value: null };
    if (typeof rawValue === "string" && rawValue.trim() === "") return { ok: true, value: null };
    const parsed = toPositiveInt(rawValue);
    if (!parsed) {
      return { ok: false, error: `${fieldName} inválido` };
    }
    return { ok: true, value: parsed };
  };
  const parsedVentaId = normalizeCartNullableBodyId(idVentaRaw, "id_venta");
  if (!parsedVentaId.ok) return res.status(400).json({ error: parsedVentaId.error });
  const parsedCuentaId = normalizeCartNullableBodyId(idCuentaRaw, "id_cuenta");
  if (!parsedCuentaId.ok) return res.status(400).json({ error: parsedCuentaId.error });
  const parsedPerfilId = normalizeCartNullableBodyId(idPerfilRaw, "id_perfil");
  if (!parsedPerfilId.ok) return res.status(400).json({ error: parsedPerfilId.error });
  const parsedPrecioEspecialId = normalizeCartNullableBodyId(
    idPrecioEspecialRaw,
    "id_precio_especial",
  );
  if (!parsedPrecioEspecialId.ok) {
    return res.status(400).json({ error: parsedPrecioEspecialId.error });
  }
  const idVenta = parsedVentaId.value;
  const idCuenta = parsedCuentaId.value;
  const idPerfil = parsedPerfilId.value;
  const requestedIdPrecioEspecial = parsedPrecioEspecialId.value;

  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    const specialPriceRow = await resolveCartSpecialPriceForUser({
      idUsuario,
      idPrecio,
      requestedIdPrecioEspecial,
    });
    const idPrecioEspecial = toPositiveInt(specialPriceRow?.id) || null;
    const idCarrito = await getOrCreateCarrito(idUsuario);
    await assertCarritoUnlockedForMutation({
      carritoId: idCarrito,
      source: "api/cart/item",
    });
    const bodyIdItem = toPositiveInt(req.body?.id_item);
    const mesesVal = (() => {
      const num = Number(meses);
      if (Number.isFinite(num) && num > 0) return Math.max(1, Math.round(num));
      return null;
    })();

    // Trae item existente (prioriza id_item si viene)
    let existing = null;
    if (bodyIdItem) {
      const { data, error } = await supabaseAdmin
        .from("carrito_items")
        .select(
          "id_item, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil, id_precio_especial",
        )
        .eq("id_carrito", idCarrito)
        .eq("id_item", bodyIdItem)
        .maybeSingle();
      if (error) throw error;
      existing = data || null;
    }
    if (!existing) {
      let selQuery = supabaseAdmin
        .from("carrito_items")
        .select(
          "id_item, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil, id_precio_especial",
        )
        .eq("id_carrito", idCarrito)
        .eq("id_precio", idPrecio)
        .eq("renovacion", renovacion === true);
      if (mesesVal != null) {
        selQuery = selQuery.eq("meses", mesesVal);
      }
      selQuery =
        idVenta === undefined || idVenta === null
          ? selQuery.is("id_venta", null)
          : selQuery.eq("id_venta", idVenta);
      selQuery =
        idCuenta === undefined || idCuenta === null
          ? selQuery.is("id_cuenta", null)
          : selQuery.eq("id_cuenta", idCuenta);
      selQuery =
        idPerfil === undefined || idPerfil === null
          ? selQuery.is("id_perfil", null)
          : selQuery.eq("id_perfil", idPerfil);
      selQuery =
        idPrecioEspecial === undefined || idPrecioEspecial === null
          ? selQuery.is("id_precio_especial", null)
          : selQuery.eq("id_precio_especial", idPrecioEspecial);
      const { data, error: selErr } = await selQuery.order("id_item", { ascending: true });
      if (selErr) throw selErr;
      const matchedRows = Array.isArray(data) ? data : [];
      if (matchedRows.length > 1) {
        const [primaryRow, ...duplicateRows] = matchedRows;
        const primaryItemId = toPositiveInt(primaryRow?.id_item);
        const mergedCantidad = Math.max(
          0,
          Math.round(
            matchedRows.reduce((acc, row) => {
              const qty = Number(row?.cantidad);
              return acc + (Number.isFinite(qty) ? qty : 0);
            }, 0),
          ),
        );
        if (primaryItemId) {
          const primaryQty = Number(primaryRow?.cantidad);
          if (!Number.isFinite(primaryQty) || Math.round(primaryQty) !== mergedCantidad) {
            const { error: mergeUpdErr } = await supabaseAdmin
              .from("carrito_items")
              .update({ cantidad: mergedCantidad })
              .eq("id_item", primaryItemId);
            if (mergeUpdErr) throw mergeUpdErr;
          }
          const duplicateIds = uniqPositiveIds(duplicateRows.map((row) => row?.id_item));
          if (duplicateIds.length) {
            const { error: mergeDelErr } = await supabaseAdmin
              .from("carrito_items")
              .delete()
              .in("id_item", duplicateIds);
            if (mergeDelErr) throw mergeDelErr;
          }
          existing = { ...primaryRow, cantidad: mergedCantidad };
          console.warn("[cart:item] consolidando duplicados", {
            id_carrito: idCarrito,
            id_precio: idPrecio,
            id_venta: idVenta ?? null,
            duplicates_removed: duplicateRows.length,
            merged_qty: mergedCantidad,
          });
        } else {
          existing = primaryRow || null;
        }
      } else {
        existing = matchedRows[0] || null;
      }
    }
    // Filtra por id_venta según sea null o definido
    const existingVentaId = toPositiveInt(existing?.id_venta);
    const existingCuentaId = toPositiveInt(existing?.id_cuenta);
    const existingPerfilId = toPositiveInt(existing?.id_perfil);
    const existingPrecioEspecialId = toPositiveInt(existing?.id_precio_especial);
    const matchesVenta =
      idVenta === undefined || idVenta === null
        ? !existingVentaId
        : existingVentaId === idVenta;
    const matchesMeses =
      mesesVal == null ? true : Number(existing?.meses) === Number(mesesVal);
    const matchesCuenta =
      idCuenta === undefined || idCuenta === null
        ? !existingCuentaId
        : existingCuentaId === idCuenta;
    const matchesPerfil =
      idPerfil === undefined || idPerfil === null
        ? !existingPerfilId
        : existingPerfilId === idPerfil;
    const matchesPrecioEspecial =
      idPrecioEspecial === undefined || idPrecioEspecial === null
        ? !existingPrecioEspecialId
        : existingPrecioEspecialId === idPrecioEspecial;
    // Si llega id_item, siempre tratamos ese registro como el existente (aunque cambien meses).
    const matchExisting =
      existing &&
      (bodyIdItem
        ? true
        : matchesVenta &&
          matchesCuenta &&
          matchesPerfil &&
          matchesMeses &&
          matchesPrecioEspecial);

    const existingQty = Number(matchExisting ? existing?.cantidad : 0) || 0;
    const newQty = existingQty + parsedDelta;

    // Permite delta=0 para sincronizar solo meses (u otros campos) de un item existente.
    if (matchExisting && parsedDelta === 0) {
      const { error: updErr } = await supabaseAdmin
        .from("carrito_items")
        .update({
          meses: mesesVal ?? existing.meses ?? null,
          renovacion: renovacion === true,
          id_venta: idVenta ?? existingVentaId ?? null,
          id_cuenta: idCuenta ?? existingCuentaId ?? null,
          id_perfil: idPerfil ?? existingPerfilId ?? null,
          id_precio_especial: idPrecioEspecial ?? null,
        })
        .eq("id_item", existing.id_item);
      if (updErr) throw updErr;
    } else

    if (matchExisting && newQty <= 0) {
      const { error: delErr } = await supabaseAdmin
        .from("carrito_items")
        .delete()
        .eq("id_item", existing.id_item);
      if (delErr) throw delErr;
    } else if (matchExisting) {
      const { error: updErr } = await supabaseAdmin
        .from("carrito_items")
        .update({
          cantidad: newQty,
          meses: mesesVal ?? existing.meses ?? null,
          renovacion: renovacion === true,
          id_venta: idVenta ?? existingVentaId ?? null,
          id_cuenta: idCuenta ?? existingCuentaId ?? null,
          id_perfil: idPerfil ?? existingPerfilId ?? null,
          id_precio_especial: idPrecioEspecial ?? null,
        })
        .eq("id_item", existing.id_item);
      if (updErr) throw updErr;
    } else if (newQty > 0) {
      const { error: insErr } = await supabaseAdmin
        .from("carrito_items")
        .insert({
          id_carrito: idCarrito,
          id_precio: idPrecio,
          cantidad: newQty,
          meses: mesesVal,
          renovacion: renovacion === true,
          id_venta: idVenta ?? null,
          id_cuenta: idCuenta ?? null,
          id_perfil: idPerfil ?? null,
          id_precio_especial: idPrecioEspecial ?? null,
        });
      if (insErr) throw insErr;
    }

    // Si no quedan items, elimina el carrito
    const { data: countData, count, error: cntErr } = await supabaseAdmin
      .from("carrito_items")
      .select("id_item", { count: "exact", head: true })
      .eq("id_carrito", idCarrito);
    if (cntErr) throw cntErr;
    const remaining = typeof count === "number" ? count : countData?.length ?? 0;
    if (remaining === 0) {
      await supabaseAdmin.from("carritos").delete().eq("id_carrito", idCarrito);
      console.log("[cart:item] carrito vacío, eliminado", idCarrito);
    }

    console.log(
      "[cart:item] usuario",
      idUsuario,
      "carrito",
      idCarrito,
      "delta",
      parsedDelta,
      "id_precio",
      idPrecio,
      "id_precio_especial",
      idPrecioEspecial,
      "remaining",
      remaining,
    );
    res.json({ ok: true, id_carrito: idCarrito, remaining });
  } catch (err) {
    console.error("[cart:item] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "CARRITO_BLOQUEADO_POR_ORDEN") {
      return res.status(Number(err?.httpStatus) || 409).json({
        error: err?.message || "Carrito bloqueado por orden pendiente de verificación.",
        id_orden: err?.id_orden || null,
      });
    }
    if (err?.code === "INVALID_PRECIO_ESPECIAL") {
      return res.status(Number(err?.httpStatus) || 400).json({
        error: err?.message || "id_precio_especial inválido.",
      });
    }
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/cart/renewal-link/apply", async (req, res) => {
  try {
    const token = String(req.body?.token || req.query?.token || "").trim();
    const payload = verifyRenewalCartToken(token);
    const sessionUserId = await getSessionUsuario(req);
    if (Number(sessionUserId) !== Number(payload.uid)) {
      return res.status(403).json({
        code: "SESSION_USER_MISMATCH",
        error: "El link no corresponde al usuario de la sesión actual.",
      });
    }

    const ventaIds = uniqPositiveIds(payload.ventaIds || []).sort((a, b) => a - b);
    if (!ventaIds.length) {
      return res.status(400).json({ error: "Link sin ventas para procesar." });
    }

    const { data: ventasRows, error: ventasErr } = await supabaseAdmin
      .from("ventas")
      .select("id_venta, id_usuario, id_precio, id_cuenta, id_cuenta_miembro, id_perfil")
      .eq("id_usuario", payload.uid)
      .in("id_venta", ventaIds);
    if (ventasErr) throw ventasErr;

    const ventas = Array.isArray(ventasRows) ? ventasRows : [];
    const renewalPriceIds = uniqPositiveIds((ventas || []).map((venta) => venta?.id_precio));
    const renewalSpecialRows = renewalPriceIds.length
      ? await listUserSpecialPrices({
          idUsuario: payload.uid,
          idPrecios: renewalPriceIds,
        })
      : [];
    const renewalSpecialLookup = buildSpecialPriceLookup(renewalSpecialRows || []);
    const ventasById = ventas.reduce((acc, venta) => {
      const ventaId = toPositiveInt(venta?.id_venta);
      if (!ventaId) return acc;
      acc[ventaId] = venta;
      return acc;
    }, {});

    const carritoId = await getOrCreateCarrito(payload.uid);
    await assertCarritoUnlockedForMutation({
      carritoId,
      source: "api/cart/renewal-link/apply",
    });
    const existingVentaIds = uniqPositiveIds(
      ventas.map((venta) => venta?.id_venta),
    );
    const existingRows = existingVentaIds.length
      ? await supabaseAdmin
          .from("carrito_items")
          .select(
            "id_item, id_venta, id_precio, id_precio_especial, cantidad, meses, renovacion, id_cuenta, id_perfil",
          )
          .eq("id_carrito", carritoId)
          .eq("renovacion", true)
          .in("id_venta", existingVentaIds)
      : { data: [], error: null };
    if (existingRows.error) throw existingRows.error;

    const existingRowsByVenta = (existingRows.data || []).reduce((acc, row) => {
      const ventaId = toPositiveInt(row?.id_venta);
      if (!ventaId) return acc;
      if (!acc[ventaId]) acc[ventaId] = [];
      acc[ventaId].push(row);
      return acc;
    }, {});
    const existingByVenta = {};
    for (const [ventaIdKey, rows] of Object.entries(existingRowsByVenta)) {
      const ventaIdNum = toPositiveInt(ventaIdKey);
      if (!ventaIdNum) continue;
      const sortedRows = [...(rows || [])].sort((a, b) => {
        const ida = toPositiveInt(a?.id_item) || Number.MAX_SAFE_INTEGER;
        const idb = toPositiveInt(b?.id_item) || Number.MAX_SAFE_INTEGER;
        return ida - idb;
      });
      const [primaryRow, ...duplicateRows] = sortedRows;
      if (!primaryRow) continue;
      if (duplicateRows.length) {
        const primaryItemId = toPositiveInt(primaryRow?.id_item);
        const mergedCantidad = Math.max(
          0,
          Math.round(
            sortedRows.reduce((acc, row) => {
              const qty = Number(row?.cantidad);
              return acc + (Number.isFinite(qty) ? qty : 0);
            }, 0),
          ),
        );
        if (primaryItemId) {
          const primaryQty = Number(primaryRow?.cantidad);
          if (!Number.isFinite(primaryQty) || Math.round(primaryQty) !== mergedCantidad) {
            const { error: mergeUpdErr } = await supabaseAdmin
              .from("carrito_items")
              .update({ cantidad: mergedCantidad })
              .eq("id_item", primaryItemId);
            if (mergeUpdErr) throw mergeUpdErr;
          }
        }
        const duplicateIds = uniqPositiveIds(duplicateRows.map((row) => row?.id_item));
        if (duplicateIds.length) {
          const { error: mergeDelErr } = await supabaseAdmin
            .from("carrito_items")
            .delete()
            .in("id_item", duplicateIds);
          if (mergeDelErr) throw mergeDelErr;
        }
        existingByVenta[ventaIdNum] = {
          ...primaryRow,
          cantidad: mergedCantidad,
        };
        console.warn("[cart/renewal-link/apply] consolidando duplicados de renovacion", {
          id_carrito: carritoId,
          id_venta: ventaIdNum,
          duplicates_removed: duplicateRows.length,
          merged_qty: mergedCantidad,
        });
      } else {
        existingByVenta[ventaIdNum] = primaryRow;
      }
    }

    const normalizeNullableId = (value) => toPositiveInt(value) || null;
    const toInsert = [];
    let added = 0;
    let alreadyInCart = 0;
    let updated = 0;
    let missing = 0;

    for (const ventaId of ventaIds) {
      const venta = ventasById[ventaId];
      if (!venta) {
        missing += 1;
        continue;
      }
      const precioId = toPositiveInt(venta?.id_precio);
      if (!precioId) {
        missing += 1;
        continue;
      }
      const cuentaId = normalizeNullableId(venta?.id_cuenta) || normalizeNullableId(venta?.id_cuenta_miembro);
      const perfilId = normalizeNullableId(venta?.id_perfil);
      const precioEspecialId =
        toPositiveInt(renewalSpecialLookup?.byPrecioId?.[precioId]?.id) || null;
      const existing = existingByVenta[ventaId] || null;

      if (existing) {
        alreadyInCart += 1;
        const patch = {};
        const existingCantidad = Number(existing?.cantidad);
        const existingMeses = Number(existing?.meses);
        if (!Number.isFinite(existingCantidad) || existingCantidad <= 0) patch.cantidad = 1;
        if (!Number.isFinite(existingMeses) || existingMeses <= 0) patch.meses = 1;
        if (existing?.renovacion !== true) patch.renovacion = true;
        if (toPositiveInt(existing?.id_precio) !== precioId) patch.id_precio = precioId;
        if (normalizeNullableId(existing?.id_precio_especial) !== precioEspecialId) {
          patch.id_precio_especial = precioEspecialId;
        }
        if (normalizeNullableId(existing?.id_cuenta) !== cuentaId) patch.id_cuenta = cuentaId;
        if (normalizeNullableId(existing?.id_perfil) !== perfilId) patch.id_perfil = perfilId;
        if (Object.keys(patch).length) {
          const itemId = toPositiveInt(existing?.id_item);
          if (itemId) {
            const { error: updErr } = await supabaseAdmin
              .from("carrito_items")
              .update(patch)
              .eq("id_item", itemId);
            if (updErr) throw updErr;
            updated += 1;
          }
        }
        continue;
      }

      toInsert.push({
        id_carrito: carritoId,
        id_precio: precioId,
        cantidad: 1,
        meses: 1,
        renovacion: true,
        id_venta: ventaId,
        id_precio_especial: precioEspecialId,
        id_cuenta: cuentaId,
        id_perfil: perfilId,
      });
    }

    if (toInsert.length) {
      const { error: insertErr } = await supabaseAdmin
        .from("carrito_items")
        .insert(toInsert);
      if (insertErr) throw insertErr;
      added = toInsert.length;
    }

    return res.json({
      ok: true,
      id_carrito: carritoId,
      total_requested: ventaIds.length,
      total_found: ventas.length,
      added,
      already_in_cart: alreadyInCart,
      updated,
      missing,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (
      err?.code === "TOKEN_INVALID" ||
      err?.code === "TOKEN_REQUIRED" ||
      err?.code === "TOKEN_EXPIRED" ||
      err?.code === "SIGNUP_TOKEN_SECRET_MISSING"
    ) {
      return res.status(signupTokenErrorStatus(err.code)).json({ error: err.message });
    }
    if (err?.code === "CARRITO_BLOQUEADO_POR_ORDEN") {
      return res.status(Number(err?.httpStatus) || 409).json({
        error: err?.message || "Carrito bloqueado por orden pendiente de verificación.",
        id_orden: err?.id_orden || null,
      });
    }
    console.error("[cart/renewal-link/apply] error", err);
    return res.status(500).json({ error: err?.message || "No se pudieron agregar las renovaciones al carrito." });
  }
});

// Crear (o devolver) carrito del usuario activo
app.post("/api/cart", async (_req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(_req);
    const idCarrito = await getOrCreateCarrito(idUsuario);
    res.json({ ok: true, id_carrito: idCarrito });
  } catch (err) {
    console.error("[cart:create] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Obtener carrito existente y sus items
app.get("/api/cart", async (_req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(_req);
    const carritoId = await getCurrentCarrito(idUsuario);
    if (!carritoId) return res.json({ items: [] });

    const { data: carritoInfoData, error: carritoErr } = await runSupabaseQueryWithRetry(
      () =>
        supabaseAdmin
          .from("carritos")
          .select("monto_usd, monto_bs, tasa_bs, descuento, monto_final, saldo_usado, hora, fecha, usa_saldo")
          .eq("id_carrito", carritoId)
          .maybeSingle(),
      "cart:get:carritoInfo",
    );
    if (carritoErr) throw carritoErr;
    let carritoInfo = carritoInfoData;
    const { data: usuarioInfo, error: userErr } = await runSupabaseQueryWithRetry(
      () =>
        supabaseAdmin
          .from("usuarios")
          .select("saldo")
          .eq("id_usuario", idUsuario)
          .maybeSingle(),
      "cart:get:usuarioSaldo",
    );
    if (userErr) throw userErr;
    const montoUsdResolved = await resolveCarritoMontoUsdFromItems({
      idUsuarioVentas: idUsuario,
      carritoId,
      fallbackMontoUsd: carritoInfo?.monto_usd,
    });
    if (!areCloseNumbers(parseOptionalCheckoutNumber(carritoInfo?.monto_usd), montoUsdResolved, 0.01)) {
      const { error: updMontoErr } = await runSupabaseQueryWithRetry(
        () =>
          supabaseAdmin
            .from("carritos")
            .update({ monto_usd: montoUsdResolved })
            .eq("id_carrito", carritoId),
        "cart:get:updateMontoUsd",
      );
      if (updMontoErr) throw updMontoErr;
    }
    carritoInfo = {
      ...(carritoInfo || {}),
      monto_usd: montoUsdResolved,
    };
    const montoFinalResolved = resolveMontoFinal({
      montoUsd: carritoInfo?.monto_usd,
      usaSaldo: carritoInfo?.usa_saldo,
      saldoUsuario: usuarioInfo?.saldo,
    });
    const carritoRateState = await syncCarritoOfficialRate({
      carritoId,
      idUsuario,
      carritoInfo,
      saldoUsuario: usuarioInfo?.saldo,
    });

    const { data: items, error: itemErr } = await runSupabaseQueryWithRetry(
        () =>
          supabaseAdmin
            .from("carrito_items")
            .select(
              "id_item, id_precio, id_precio_especial, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil",
            )
            .eq("id_carrito", carritoId),
      "cart:get:items",
    );
    if (itemErr) throw itemErr;
    const cartSpecialIds = uniqPositiveIds((items || []).map((item) => item?.id_precio_especial));
    const cartSpecialRows = cartSpecialIds.length
      ? await listUserSpecialPrices({
          idUsuario,
          idEspeciales: cartSpecialIds,
        })
      : [];
    const cartSpecialLookup = buildSpecialPriceLookup(cartSpecialRows || []);

    // Enriquecer con datos de venta/cuenta/perfil para renovaciones
    const cuentaIds = Array.from(
      new Set(
        (items || [])
          .map((it) => Number(it?.id_cuenta))
          .filter((idCuenta) => Number.isFinite(idCuenta) && idCuenta > 0),
      ),
    );
    let cuentaMap = {};
    if (cuentaIds.length) {
      const { data: cuentasExtra, error: cuentasErr } = await runSupabaseQueryWithRetry(
        () =>
          supabaseAdmin
            .from("cuentas")
            .select("id_cuenta, correo")
            .in("id_cuenta", cuentaIds),
        "cart:get:cuentasExtra",
      );
      if (cuentasErr) throw cuentasErr;
      cuentaMap = (cuentasExtra || []).reduce((acc, cta) => {
        acc[cta.id_cuenta] = cta;
        return acc;
      }, {});
    }

    const ventaIds = (items || []).map((i) => i.id_venta).filter(Boolean);
    let ventaMap = {};
    if (ventaIds.length) {
      const { data: ventasExtra, error: ventErr } = await runSupabaseQueryWithRetry(
        () =>
          supabaseAdmin
            .from("ventas")
            .select(
              "id_venta, id_cuenta, id_cuenta_miembro, id_perfil, correo_miembro, cuentas:cuentas!ventas_id_cuenta_fkey(correo), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(correo), perfiles:perfiles(n_perfil)"
            )
            .in("id_venta", ventaIds),
        "cart:get:ventasExtra",
      );
      if (ventErr) throw ventErr;
      ventaMap = (ventasExtra || []).reduce((acc, v) => {
        acc[v.id_venta] = v;
        return acc;
      }, {});
    }

    const itemsWithDiscountQty = applyRenewalDiscountQtyContext(items || []);
    const enriched = itemsWithDiscountQty.map((it) => {
      const ventaInfo = it.id_venta ? ventaMap[it.id_venta] || {} : {};
      const cuentaItem = it.id_cuenta ? cuentaMap[it.id_cuenta] || null : null;
      const correoResolved =
        cuentaItem?.correo ||
        ventaInfo?.correo_miembro ||
        ventaInfo?.cuentas_miembro?.correo ||
        ventaInfo?.cuentas?.correo ||
        null;
      return {
        ...it,
        correo: correoResolved,
        n_perfil: ventaInfo?.perfiles?.n_perfil || null,
        precio_especial_monto: Number.isFinite(
          parseOptionalCheckoutNumber(cartSpecialLookup?.byId?.[toPositiveInt(it?.id_precio_especial)]?.monto),
        )
          ? roundCheckoutMoney(
              cartSpecialLookup.byId[toPositiveInt(it?.id_precio_especial)]?.monto,
            )
          : null,
      };
    });

    res.json({
      id_carrito: carritoId,
      items: enriched,
      monto_usd: Number.isFinite(montoUsdResolved) ? montoUsdResolved : null,
      monto_bs:
        carritoRateState?.montoBs ?? carritoInfo?.monto_bs ?? null,
      tasa_bs: carritoRateState?.tasaBs ?? null,
      descuento: carritoInfo?.descuento ?? null,
      monto_final:
        carritoRateState?.montoFinal ?? montoFinalResolved,
      saldo_usado:
        carritoRateState?.saldoUsado ??
        resolveSaldoAplicadoFromCarrito({
          montoUsd: carritoInfo?.monto_usd,
          montoFinal: carritoRateState?.montoFinal ?? montoFinalResolved,
        }),
      usa_saldo: carritoInfo?.usa_saldo ?? null,
      hora: carritoRateState?.hora ?? carritoInfo?.hora ?? null,
      fecha: carritoRateState?.fecha ?? carritoInfo?.fecha ?? null,
    });
  } catch (err) {
    console.error("[cart:get] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Actualizar montos fijos del carrito (USD y BS)
app.post("/api/cart/montos", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    const carritoId = await getCurrentCarrito(idUsuario);
    if (!carritoId) {
      return res.status(400).json({ error: "Carrito no encontrado" });
    }
    await assertCarritoUnlockedForMutation({
      carritoId,
      source: "api/cart/montos",
    });
    let { data: carritoInfo, error: carritoErr } = await supabaseAdmin
      .from("carritos")
      .select("monto_usd, monto_bs, tasa_bs, usa_saldo, monto_final, saldo_usado, fecha, hora")
      .eq("id_carrito", carritoId)
      .maybeSingle();
    if (carritoErr) throw carritoErr;
    const { data: usuarioInfo, error: usuarioErr } = await supabaseAdmin
      .from("usuarios")
      .select("saldo")
      .eq("id_usuario", idUsuario)
      .maybeSingle();
    if (usuarioErr) throw usuarioErr;
    const montoUsdResolved = await resolveCarritoMontoUsdFromItems({
      idUsuarioVentas: idUsuario,
      carritoId,
      fallbackMontoUsd: carritoInfo?.monto_usd,
    });
    if (!areCloseNumbers(parseOptionalCheckoutNumber(carritoInfo?.monto_usd), montoUsdResolved, 0.01)) {
      const { error: updMontoErr } = await supabaseAdmin
        .from("carritos")
        .update({ monto_usd: montoUsdResolved })
        .eq("id_carrito", carritoId);
      if (updMontoErr) throw updMontoErr;
    }
    carritoInfo = {
      ...(carritoInfo || {}),
      monto_usd: montoUsdResolved,
    };
    const carritoRateState = await syncCarritoOfficialRate({
      carritoId,
      idUsuario,
      carritoInfo,
      saldoUsuario: usuarioInfo?.saldo,
      force: true,
    });
    return res.json({
      ok: true,
      id_carrito: carritoId,
      monto_usd: Number.isFinite(montoUsdResolved) ? montoUsdResolved : null,
      tasa_bs: carritoRateState?.tasaBs ?? null,
      monto_bs: carritoRateState?.montoBs ?? null,
      monto_final: carritoRateState?.montoFinal ?? null,
      saldo_usado: carritoRateState?.saldoUsado ?? null,
      fecha: carritoRateState?.fecha ?? null,
      hora: carritoRateState?.hora ?? null,
    });
  } catch (err) {
    console.error("[cart:montos] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "CARRITO_BLOQUEADO_POR_ORDEN") {
      return res.status(Number(err?.httpStatus) || 409).json({
        error: err?.message || "Carrito bloqueado por orden pendiente de verificación.",
        id_orden: err?.id_orden || null,
      });
    }
    return res.status(500).json({ error: err.message });
  }
});

// Actualizar banderas del carrito (ej: usa_saldo)
app.post("/api/cart/flags", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    const carritoId = await getCurrentCarrito(idUsuario);
    if (!carritoId) {
      return res.status(400).json({ error: "Carrito no encontrado" });
    }
    await assertCarritoUnlockedForMutation({
      carritoId,
      source: "api/cart/flags",
    });
    const usa_saldo = isTrue(req.body?.usa_saldo);
    let { data: carritoInfo, error: cartErr } = await supabaseAdmin
      .from("carritos")
      .select("monto_usd, monto_bs, tasa_bs, monto_final, saldo_usado, fecha, hora")
      .eq("id_carrito", carritoId)
      .maybeSingle();
    if (cartErr) throw cartErr;
    const { data: usuarioInfo, error: userErr } = await supabaseAdmin
      .from("usuarios")
      .select("saldo")
      .eq("id_usuario", idUsuario)
      .maybeSingle();
    if (userErr) throw userErr;
    const montoUsdResolved = await resolveCarritoMontoUsdFromItems({
      idUsuarioVentas: idUsuario,
      carritoId,
      fallbackMontoUsd: carritoInfo?.monto_usd,
    });
    const montoFinal = resolveMontoFinal({
      montoUsd: montoUsdResolved,
      usaSaldo: usa_saldo,
      saldoUsuario: usuarioInfo?.saldo,
    });
    const saldoUsado = resolveSaldoAplicadoFromCarrito({
      montoUsd: montoUsdResolved,
      montoFinal,
    });
    const { error: updErr } = await supabaseAdmin
      .from("carritos")
      .update({
        usa_saldo,
        monto_usd: montoUsdResolved,
        monto_final: montoFinal,
        saldo_usado: saldoUsado,
      })
      .eq("id_carrito", carritoId);
    if (updErr) throw updErr;
    carritoInfo = {
      ...(carritoInfo || {}),
      monto_usd: montoUsdResolved,
    };
    const carritoRateState = await syncCarritoOfficialRate({
      carritoId,
      idUsuario,
      carritoInfo: {
        ...carritoInfo,
        usa_saldo,
        monto_final: montoFinal,
      },
      saldoUsuario: usuarioInfo?.saldo,
      force: true,
    });
    return res.json({
      ok: true,
      id_carrito: carritoId,
      usa_saldo,
      monto_usd: Number.isFinite(montoUsdResolved) ? montoUsdResolved : null,
      monto_final: carritoRateState?.montoFinal ?? montoFinal,
      saldo_usado: carritoRateState?.saldoUsado ?? saldoUsado,
      monto_bs: carritoRateState?.montoBs ?? null,
      tasa_bs: carritoRateState?.tasaBs ?? null,
      fecha: carritoRateState?.fecha ?? null,
      hora: carritoRateState?.hora ?? null,
    });
  } catch (err) {
    console.error("[cart:flags] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "CARRITO_BLOQUEADO_POR_ORDEN") {
      return res.status(Number(err?.httpStatus) || 409).json({
        error: err?.message || "Carrito bloqueado por orden pendiente de verificación.",
        id_orden: err?.id_orden || null,
      });
    }
    return res.status(500).json({ error: err.message });
  }
});

// Obtiene o crea una orden borrador para el carrito activo
app.post("/api/checkout/draft", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    const carritoId = await getCurrentCarrito(idUsuario);
    if (!carritoId) {
      return res.status(400).json({ error: "No hay carrito activo" });
    }
    const carritoRateState = await syncCarritoOfficialRate({
      carritoId,
      idUsuario,
    });

    const context = await buildCheckoutContext({
      idUsuarioVentas: idUsuario,
      carritoId,
      totalCliente: null,
    });
    const {
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
    } = context;
    if (!items?.length) {
      return res.status(400).json({ error: "El carrito está vacío" });
    }

    const { data: existingRows, error: existingErr } = await supabaseAdmin
      .from("ordenes")
      .select("id_orden, orden_cancelada, checkout_finalizado")
      .eq("id_usuario", idUsuario)
      .eq("id_carrito", carritoId)
      .order("id_orden", { ascending: false })
      .limit(20);
    if (existingErr) throw existingErr;

    const openDraft = (existingRows || []).find(
      (row) => !isTrue(row?.orden_cancelada) && !isTrue(row?.checkout_finalizado),
    );
    const { data: carritoData, error: carritoErr } = await supabaseAdmin
      .from("carritos")
      .select("monto_usd, tasa_bs, monto_bs")
      .eq("id_carrito", carritoId)
      .maybeSingle();
    if (carritoErr) throw carritoErr;

    const total = parseOptionalCheckoutNumber(carritoData?.monto_usd);
    const tasaBs = parseOptionalCheckoutNumber(carritoRateState?.tasaBs);
    const montoBsRaw = parseOptionalCheckoutNumber(carritoRateState?.montoBs ?? carritoData?.monto_bs);
    const montoBs = Number.isFinite(montoBsRaw)
      ? montoBsRaw
      : Number.isFinite(total) && Number.isFinite(tasaBs)
        ? Math.round(total * tasaBs * 100) / 100
        : null;

    const existingId = Number(openDraft?.id_orden || 0);
    if (existingId > 0) {
      await syncOrdenItemsSnapshot({
        ordenId: existingId,
        items,
        priceMap,
        platInfoById,
        platNameById,
        pickPrecio,
        descuentos,
        discountColumns,
        discountColumnById,
        isCliente,
        totalUsd: total,
        montoBsTotal: montoBs,
        tasaBs,
      });
      return res.json({ ok: true, id_orden: existingId, existing: true });
    }

    const { data: created, error: createErr } = await supabaseAdmin
      .from("ordenes")
      .insert({
        id_usuario: idUsuario,
        id_carrito: carritoId,
        total: Number.isFinite(total) ? total : null,
        tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
        monto_bs: Number.isFinite(montoBs) ? montoBs : null,
        en_espera: true,
        pago_verificado: false,
        orden_cancelada: false,
        checkout_finalizado: false,
      })
      .select("id_orden")
      .single();
    if (createErr) throw createErr;

    await syncOrdenItemsSnapshot({
      ordenId: created?.id_orden,
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
      totalUsd: total,
      montoBsTotal: montoBs,
      tasaBs,
    });

    return res.json({
      ok: true,
      id_orden: created?.id_orden || null,
      existing: false,
    });
  } catch (err) {
    console.error("[checkout:draft] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    return res.status(500).json({ error: err.message });
  }
});

app.post("/api/checkout/summary", async (req, res) => {
  try {
    const idUsuarioSesion = await getOrCreateUsuario(req);
    const sessionUserId = parseSessionUserId(req) || idUsuarioSesion;
    const idUsuarioOverride =
      req.body?.id_usuario_override && Number.isFinite(Number(req.body?.id_usuario_override))
        ? Number(req.body.id_usuario_override)
        : null;

    if (idUsuarioOverride) {
      const { data: permRow, error: permErr } = await supabaseAdmin
        .from("usuarios")
        .select("permiso_superadmin")
        .eq("id_usuario", sessionUserId)
        .maybeSingle();
      if (permErr) throw permErr;
      if (!isTrue(permRow?.permiso_superadmin)) {
        return res.status(403).json({ error: "Solo superadmin puede crear órdenes para otros usuarios" });
      }
    }

    const idUsuarioVentas = idUsuarioOverride || idUsuarioSesion;
    const carritoId = await getCurrentCarrito(idUsuarioSesion);
    if (!carritoId) {
      return res.status(400).json({ error: "No hay carrito activo" });
    }
    const carritoRateState = await syncCarritoOfficialRate({
      carritoId,
      idUsuario: idUsuarioSesion,
    });

    const context = await buildCheckoutContext({
      idUsuarioVentas,
      carritoId,
      totalCliente: null,
    });
    const {
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
      total: totalByAccess,
      accesoCliente,
      esMayorista,
    } = context;
    if (!items?.length) {
      return res.status(400).json({ error: "El carrito está vacío" });
    }

    const tasaBs = parseOptionalCheckoutNumber(carritoRateState?.tasaBs);
    const total = roundCheckoutMoney(parseOptionalCheckoutNumber(totalByAccess) ?? 0);
    const montoBs = Number.isFinite(tasaBs) ? roundCheckoutMoney(total * tasaBs) : null;

    const summary = await buildCheckoutItemSummaryRows({
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
      totalUsd: total,
      montoBsTotal: montoBs,
      tasaBs,
    });

    return res.json({
      ok: true,
      carrito_id: carritoId,
      id_usuario_ventas: idUsuarioVentas,
      acceso_cliente: accesoCliente,
      es_mayorista: esMayorista,
      tipo_precio: esMayorista ? "mayor" : "detal",
      total_usd: summary.totalUsd,
      monto_bs: summary.montoBsTotal,
      tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
      items: summary.rows.map((row) => ({
        id_item: row.id_item,
        id_precio: row.id_precio,
        id_plataforma: row.id_plataforma,
        detalle: row.detalle,
        cantidad: row.cantidad,
        meses: row.meses,
        renovacion: row.renovacion,
        monto_usd: row.monto_usd,
        monto_base_usd: row.monto_base_usd,
        monto_original_usd: row.monto_original_usd,
        descuento_usd: row.descuento_usd,
        monto_bs: row.monto_bs,
      })),
      tasa_actual: Number.isFinite(tasaBs) ? tasaBs : null,
    });
  } catch (err) {
    console.error("[checkout:summary] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    return res.status(500).json({ error: err.message });
  }
});

// Sube comprobantes al bucket usando la clave de servicio (evita RLS en cliente)
app.post("/api/checkout/upload", async (req, res) => {
  const files = req.body?.files;
  if (!Array.isArray(files) || !files.length) {
    return res.status(400).json({ error: "files es requerido" });
  }

  try {
    const idUsuario = await getOrCreateUsuario(req);
    const urls = [];

    const sanitizeFileName = (name = "file") => {
      const cleaned = String(name)
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "-")
        .replace(/[^a-z0-9._-]/g, "");
      return cleaned || "file";
    };

    for (const file of files) {
      const { name, content, type } = file || {};
      if (!name || !content) {
        return res
          .status(400)
          .json({ error: "Cada archivo necesita name y content en base64" });
      }
      const buffer = Buffer.from(content, "base64");
      const safeName = sanitizeFileName(name);
      const path = `comprobantes/${idUsuario}/${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}-${safeName}`;

      const { error } = await supabaseAdmin.storage
        .from("private_assets")
        .upload(path, buffer, {
          contentType: type || "application/octet-stream",
        });
      if (error) throw error;
      const { data } = supabaseAdmin.storage.from("private_assets").getPublicUrl(path);
      if (data?.publicUrl) urls.push(data.publicUrl);
    }

    res.json({ urls });
  } catch (err) {
    console.error("[checkout:upload] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

const normalizePublicAssetsFolder = (rawFolder = "") => {
  const cleaned = String(rawFolder || "logos")
    .trim()
    .toLowerCase()
    .replace(/\\/g, "/")
    .replace(/^\/+|\/+$/g, "")
    .replace(/[^a-z0-9/_-]/g, "")
    .replace(/\/{2,}/g, "/");
  return cleaned || "logos";
};

const isAllowedPublicAssetsFolder = (folder = "") => {
  const allowed = new Set([
    "logos",
    "logos/mooseplus",
    "logos/metodos-pago",
    "logos/plataformas/tarjeta",
    "logos/plataformas/banner",
    "icono-perfil",
    "banners-index",
  ]);
  return allowed.has(folder);
};

const isImageFileName = (name = "") =>
  /\.(png|jpe?g|webp|gif|bmp|svg|avif)$/i.test(String(name || "").trim());

const imageMimeFromExt = (ext = "") => {
  const normalized = String(ext || "").trim().toLowerCase();
  if (normalized === "png") return "image/png";
  if (normalized === "jpg" || normalized === "jpeg") return "image/jpeg";
  if (normalized === "webp") return "image/webp";
  if (normalized === "gif") return "image/gif";
  if (normalized === "bmp") return "image/bmp";
  if (normalized === "avif") return "image/avif";
  if (normalized === "svg") return "image/svg+xml";
  return "";
};

const imageExtFromMime = (mime = "") => {
  const normalized = String(mime || "").trim().toLowerCase();
  if (normalized === "image/png") return "png";
  if (normalized === "image/jpeg") return "jpg";
  if (normalized === "image/webp") return "webp";
  if (normalized === "image/gif") return "gif";
  if (normalized === "image/bmp") return "bmp";
  if (normalized === "image/avif") return "avif";
  if (normalized === "image/svg+xml") return "svg";
  return "";
};

const getFileExt = (name = "") => {
  const match = String(name || "")
    .trim()
    .toLowerCase()
    .match(/\.([a-z0-9]+)$/);
  return match?.[1] || "";
};

const normalizePublicAssetFileMeta = (rawName = "file", rawType = "") => {
  const sanitizeFileName = (name = "file") => {
    const cleaned = String(name)
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "-")
      .replace(/[^a-z0-9._-]/g, "");
    return cleaned || "file";
  };

  let safeName = sanitizeFileName(rawName);
  const extFromName = getFileExt(safeName);
  const mimeFromName = imageMimeFromExt(extFromName);
  const normalizedType = String(rawType || "").trim().toLowerCase();

  let contentType = "application/octet-stream";
  if (normalizedType === "image/webp" || mimeFromName === "image/webp") {
    contentType = "image/webp";
  } else if (normalizedType.startsWith("image/")) {
    contentType = normalizedType;
  } else if (mimeFromName) {
    contentType = mimeFromName;
  }

  const targetExt = imageExtFromMime(contentType);
  if (targetExt) {
    if (extFromName) {
      safeName = safeName.replace(/\.[a-z0-9]+$/i, `.${targetExt}`);
    } else {
      safeName = `${safeName}.${targetExt}`;
    }
  }

  return {
    safeName,
    contentType,
  };
};

const normalizePublicAssetsPath = (rawPath = "") =>
  String(rawPath || "")
    .trim()
    .replace(/\\/g, "/")
    .replace(/^\/+|\/+$/g, "")
    .replace(/\/{2,}/g, "/");

const extractPublicAssetsPathFromUrl = (rawUrl = "") => {
  const value = String(rawUrl || "").trim();
  if (!value) return "";
  try {
    const parsed = new URL(value);
    const marker = "/storage/v1/object/public/public_assets/";
    const idx = parsed.pathname.indexOf(marker);
    if (idx === -1) return "";
    const rawPath = parsed.pathname.slice(idx + marker.length);
    return normalizePublicAssetsPath(decodeURIComponent(rawPath));
  } catch (_err) {
    return "";
  }
};

const isAllowedPublicAssetsPath = (rawPath = "") => {
  const path = normalizePublicAssetsPath(rawPath);
  if (!path || path.includes("..")) return false;
  const [folder, ...rest] = path.split("/");
  if (!isAllowedPublicAssetsFolder(folder)) return false;
  if (!rest.length) return false;
  const fileName = rest[rest.length - 1];
  return isImageFileName(fileName);
};

// Sube logos de plataformas al bucket público (public_assets/logos por defecto)
app.post("/api/logos/upload", async (req, res) => {
  const files = req.body?.files;
  if (!Array.isArray(files) || !files.length) {
    return res.status(400).json({ error: "files es requerido" });
  }

  try {
    const idUsuario = await getOrCreateUsuario(req);
    const folder = normalizePublicAssetsFolder(req.body?.folder);
    if (!isAllowedPublicAssetsFolder(folder)) {
      return res
        .status(400)
        .json({
          error:
            "folder no permitido. Usa logos/mooseplus, logos/metodos-pago, logos/plataformas/tarjeta, logos/plataformas/banner, icono-perfil o banners-index.",
        });
    }
    const overwriteByName = isTrue(req.body?.overwrite_by_name);
    const urls = [];

    for (const file of files) {
      const { name, content, type } = file || {};
      if (!name || !content) {
        return res
          .status(400)
          .json({ error: "Cada archivo necesita name y content en base64" });
      }
      const buffer = Buffer.from(content, "base64");
      const { safeName, contentType } = normalizePublicAssetFileMeta(name, type);
      const path = overwriteByName
        ? `${folder}/${safeName}`
        : `${folder}/${Date.now()}-${Math.random()
            .toString(36)
            .slice(2)}-${safeName}`;

      if (overwriteByName) {
        await supabaseAdmin.storage.from("public_assets").remove([path]);
      }

      const { error } = await supabaseAdmin.storage
        .from("public_assets")
        .upload(path, buffer, {
          contentType,
          upsert: overwriteByName,
        });
      if (error) throw error;
      const { data } = supabaseAdmin.storage.from("public_assets").getPublicUrl(path);
      if (data?.publicUrl) urls.push(data.publicUrl);
    }

    res.json({ urls });
  } catch (err) {
    console.error("[logos:upload] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Elimina archivos en carpetas permitidas del bucket público (public_assets)
app.post("/api/logos/delete", jsonParser, async (req, res) => {
  try {
    await requireAdminSession(req);
    const pathsRaw = Array.isArray(req.body?.paths) ? req.body.paths : [];
    const publicUrlsRaw = Array.isArray(req.body?.public_urls) ? req.body.public_urls : [];

    const normalized = [
      ...pathsRaw.map((row) => normalizePublicAssetsPath(row)),
      ...publicUrlsRaw.map((row) => extractPublicAssetsPathFromUrl(row)),
    ]
      .filter((row) => isAllowedPublicAssetsPath(row));

    const uniquePaths = [...new Set(normalized)];
    if (!uniquePaths.length) {
      return res.status(400).json({
        error:
          "Debes enviar paths/public_urls válidos de logos, logos/mooseplus, logos/metodos-pago, logos/plataformas/tarjeta, logos/plataformas/banner, icono-perfil o banners-index.",
      });
    }

    const { error } = await supabaseAdmin.storage
      .from("public_assets")
      .remove(uniquePaths);
    if (error) throw error;

    res.json({
      ok: true,
      removed_count: uniquePaths.length,
      removed_paths: uniquePaths,
    });
  } catch (err) {
    console.error("[logos:delete] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "ADMIN_REQUIRED") {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message || "No se pudieron eliminar archivos." });
  }
});

// Lista archivos en carpetas permitidas del bucket público (public_assets)
app.get("/api/logos/list", async (req, res) => {
  try {
    await getOrCreateUsuario(req);
    const folder = normalizePublicAssetsFolder(req.query?.folder);
    if (!isAllowedPublicAssetsFolder(folder)) {
      return res
        .status(400)
        .json({
          error:
            "folder no permitido. Usa logos/mooseplus, logos/metodos-pago, logos/plataformas/tarjeta, logos/plataformas/banner, icono-perfil o banners-index.",
        });
    }

    const { data, error } = await supabaseAdmin.storage
      .from("public_assets")
      .list(folder, {
        limit: 500,
        sortBy: { column: "name", order: "asc" },
      });
    if (error) throw error;

    const items = (data || [])
      .filter((row) => row?.name && !row.name.endsWith("/"))
      .filter((row) => !String(row.name).startsWith("."))
      .filter((row) => isImageFileName(row.name))
      .map((row) => {
        const path = `${folder}/${row.name}`;
        const { data: publicData } = supabaseAdmin.storage
          .from("public_assets")
          .getPublicUrl(path);
        return {
          name: row.name,
          path,
          publicUrl: publicData?.publicUrl || null,
          created_at: row.created_at || null,
          updated_at: row.updated_at || null,
        };
      })
      .filter((row) => !!row.publicUrl);

    res.json({ folder, items });
  } catch (err) {
    console.error("[logos:list] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Lista banners del home (público). Si include_inactive=true, requiere sesión admin.
app.get("/api/home-banners", async (req, res) => {
  try {
    const includeInactiveRequested = isTrue(req.query?.include_inactive);
    let includeInactive = false;
    if (includeInactiveRequested) {
      try {
        await requireAdminSession(req);
        includeInactive = true;
      } catch (_err) {
        includeInactive = false;
      }
    }

    const runBannersListQuery = async (selectClause = "") => {
      let query = supabaseAdmin
        .from(HOME_BANNERS_TABLE)
        .select(selectClause)
        .order("posicion", { ascending: true })
        .order("id_banner", { ascending: true });
      if (!includeInactive) {
        query = query.eq("oculto", false);
      }
      return await query;
    };

    let supportsExtendedColumns = true;
    let { data, error } = await runBannersListQuery(
      "id_banner, titulo, imagen, imagen_movil, redireccion, oculto, posicion",
    );
    if (error && (isMissingColumnError(error, "titulo") || isMissingColumnError(error, "imagen_movil"))) {
      supportsExtendedColumns = false;
      ({ data, error } = await runBannersListQuery("id_banner, imagen, redireccion, oculto, posicion"));
    }
    if (error) {
      if (isMissingTableError(error, HOME_BANNERS_TABLE)) {
        return res.json({ items: [], tableMissing: true });
      }
      throw error;
    }

    const items = (data || []).map((row) => ({
      id_banner: Number(row?.id_banner) || 0,
      title: supportsExtendedColumns ? String(row?.titulo || "").trim() : "",
      image_url: String(row?.imagen || "").trim(),
      image_url_mobile: supportsExtendedColumns ? String(row?.imagen_movil || "").trim() : "",
      redirect_url: String(row?.redireccion || "").trim(),
      oculto: row?.oculto === true,
      posicion: Number.isFinite(Number(row?.posicion))
        ? Math.max(1, Math.trunc(Number(row?.posicion)))
        : null,
    }));

    res.json({ items });
  } catch (err) {
    console.error("[home-banners:list] error", err);
    res.status(500).json({ error: err.message || "No se pudieron listar banners." });
  }
});

// Crea banner del home (solo admin/superadmin autenticado)
app.post("/api/home-banners", jsonParser, async (req, res) => {
  try {
    await requireAdminSession(req);
    const imageUrl = String(req.body?.image_url || "").trim();
    const imageUrlMobile = String(req.body?.image_url_mobile || "").trim();
    const title = String(req.body?.title ?? req.body?.titulo ?? "").trim();
    const redirectUrl = String(req.body?.redirect_url || "").trim();
    const activoRaw = req.body?.activo;
    const ocultoRaw = req.body?.oculto;
    const posicionRaw = Number(req.body?.posicion);

    if (!imageUrl) {
      return res.status(400).json({ error: "image_url es requerido." });
    }
    if (!redirectUrl) {
      return res.status(400).json({ error: "redirect_url es requerido." });
    }

    let posicion = Number.isFinite(posicionRaw) ? Math.max(1, Math.trunc(posicionRaw)) : null;
    if (!posicion) {
      const { data: maxRow, error: maxErr } = await supabaseAdmin
        .from(HOME_BANNERS_TABLE)
        .select("posicion")
        .order("posicion", { ascending: false })
        .limit(1)
        .maybeSingle();
      if (maxErr) {
        if (isMissingTableError(maxErr, HOME_BANNERS_TABLE)) {
          return res.status(400).json({ error: "Falta tabla banners en Supabase." });
        }
        throw maxErr;
      }
      const maxPos = Number(maxRow?.posicion);
      posicion = Number.isFinite(maxPos) && maxPos > 0 ? Math.trunc(maxPos) + 1 : 1;
    }

    const payload = {
      titulo: title || null,
      imagen: imageUrl,
      imagen_movil: imageUrlMobile || imageUrl,
      redireccion: redirectUrl,
      oculto:
        ocultoRaw == null
          ? activoRaw == null
            ? false
            : !isTrue(activoRaw)
          : isTrue(ocultoRaw),
      posicion,
    };

    const { data, error } = await supabaseAdmin
      .from(HOME_BANNERS_TABLE)
      .insert(payload)
      .select("id_banner, titulo, imagen, imagen_movil, redireccion, oculto, posicion")
      .single();
    if (error) {
      if (isMissingTableError(error, HOME_BANNERS_TABLE)) {
        return res.status(400).json({
          error: "Falta tabla banners en Supabase.",
        });
      }
      throw error;
    }
    res.json({
      item: {
        id_banner: Number(data?.id_banner) || 0,
        title: String(data?.titulo || "").trim(),
        image_url: String(data?.imagen || "").trim(),
        image_url_mobile: String(data?.imagen_movil || "").trim(),
        redirect_url: String(data?.redireccion || "").trim(),
        oculto: data?.oculto === true,
        posicion: Number.isFinite(Number(data?.posicion))
          ? Math.max(1, Math.trunc(Number(data?.posicion)))
          : null,
      },
    });
  } catch (err) {
    console.error("[home-banners:create] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "ADMIN_REQUIRED") {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message || "No se pudo crear el banner." });
  }
});

// Actualiza dirección/imagen/estado de un banner (solo admin/superadmin)
app.put("/api/home-banners/:id_banner", jsonParser, async (req, res) => {
  try {
    await requireAdminSession(req);
    const idBanner = Number(req.params?.id_banner);
    if (!Number.isFinite(idBanner) || idBanner <= 0) {
      return res.status(400).json({ error: "id_banner inválido." });
    }

    const payload = {};
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "redirect_url")) {
      const redirectUrl = String(req.body?.redirect_url || "").trim();
      if (!redirectUrl) {
        return res.status(400).json({ error: "redirect_url no puede estar vacío." });
      }
      payload.redireccion = redirectUrl;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "orden")) {
      // La tabla banners no usa "orden". Se ignora para mantener compatibilidad de API.
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "posicion")) {
      const posicionRaw = Number(req.body?.posicion);
      payload.posicion = Number.isFinite(posicionRaw)
        ? Math.max(1, Math.trunc(posicionRaw))
        : 1;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "activo")) {
      payload.oculto = !isTrue(req.body?.activo);
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "oculto")) {
      payload.oculto = isTrue(req.body?.oculto);
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "image_url")) {
      const imageUrl = String(req.body?.image_url || "").trim();
      if (!imageUrl) {
        return res.status(400).json({ error: "image_url no puede estar vacío." });
      }
      payload.imagen = imageUrl;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "image_url_mobile")) {
      const imageUrlMobile = String(req.body?.image_url_mobile || "").trim();
      if (!imageUrlMobile) {
        return res.status(400).json({ error: "image_url_mobile no puede estar vacío." });
      }
      payload.imagen_movil = imageUrlMobile;
    }
    if (
      Object.prototype.hasOwnProperty.call(req.body || {}, "title") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "titulo")
    ) {
      payload.titulo = String(req.body?.title ?? req.body?.titulo ?? "").trim() || null;
    }

    if (!Object.keys(payload).length) {
      return res.status(400).json({ error: "Sin campos para actualizar." });
    }
    const { data, error } = await supabaseAdmin
      .from(HOME_BANNERS_TABLE)
      .update(payload)
      .eq("id_banner", idBanner)
      .select("id_banner, titulo, imagen, imagen_movil, redireccion, oculto, posicion")
      .maybeSingle();
    if (error) {
      if (isMissingTableError(error, HOME_BANNERS_TABLE)) {
        return res.status(400).json({
          error: "Falta tabla banners en Supabase.",
        });
      }
      throw error;
    }
    if (!data) {
      return res.status(404).json({ error: "Banner no encontrado." });
    }

    res.json({
      item: {
        id_banner: Number(data?.id_banner) || 0,
        title: String(data?.titulo || "").trim(),
        image_url: String(data?.imagen || "").trim(),
        image_url_mobile: String(data?.imagen_movil || "").trim(),
        redirect_url: String(data?.redireccion || "").trim(),
        oculto: data?.oculto === true,
        posicion: Number.isFinite(Number(data?.posicion))
          ? Math.max(1, Math.trunc(Number(data?.posicion)))
          : null,
      },
    });
  } catch (err) {
    console.error("[home-banners:update] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "ADMIN_REQUIRED") {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message || "No se pudo actualizar el banner." });
  }
});

// Actualiza correo de usuario en tabla usuarios y en Supabase Auth (solo admin/superadmin)
app.put("/api/admin/usuarios/:id_usuario/correo", jsonParser, async (req, res) => {
  try {
    await requireAdminSession(req);
    const idUsuarioTarget = toPositiveInt(req.params?.id_usuario);
    if (!idUsuarioTarget) {
      return res.status(400).json({ error: "id_usuario inválido." });
    }

    const correo = String(req.body?.correo || "")
      .trim()
      .toLowerCase();
    if (!correo || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(correo)) {
      return res.status(400).json({ error: "Correo inválido." });
    }

    const { data: targetRow, error: targetErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, correo, id_auth")
      .eq("id_usuario", idUsuarioTarget)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      return res.status(404).json({ error: "Usuario no encontrado." });
    }

    const previousCorreo = String(targetRow?.correo || "")
      .trim()
      .toLowerCase();
    const authUserId = String(targetRow?.id_auth || "").trim();

    const { data: duplicateRows, error: duplicateErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario")
      .ilike("correo", correo)
      .neq("id_usuario", idUsuarioTarget)
      .limit(1);
    if (duplicateErr) throw duplicateErr;
    if (Array.isArray(duplicateRows) && duplicateRows.length) {
      return res.status(409).json({ error: "El correo ya está en uso por otro usuario." });
    }

    let usuariosUpdated = false;
    if (previousCorreo !== correo) {
      const { error: updErr } = await supabaseAdmin
        .from("usuarios")
        .update({ correo })
        .eq("id_usuario", idUsuarioTarget);
      if (updErr) {
        if (String(updErr?.code || "") === "23505") {
          return res.status(409).json({ error: "El correo ya está en uso por otro usuario." });
        }
        throw updErr;
      }
      usuariosUpdated = true;
    }

    if (!authUserId) {
      return res.json({
        ok: true,
        id_usuario: idUsuarioTarget,
        correo,
        auth_synced: false,
        auth_user_missing: true,
      });
    }

    const { data: authUpdatedData, error: authUpdErr } = await supabaseAdmin.auth.admin.updateUserById(
      authUserId,
      { email: correo },
    );
    if (authUpdErr) {
      if (usuariosUpdated && previousCorreo && previousCorreo !== correo) {
        const { error: rollbackErr } = await supabaseAdmin
          .from("usuarios")
          .update({ correo: previousCorreo })
          .eq("id_usuario", idUsuarioTarget);
        if (rollbackErr) {
          console.error("[admin/usuarios/correo] rollback usuarios error", {
            id_usuario: idUsuarioTarget,
            rollbackErr,
          });
        }
      }
      const authErrText = `${authUpdErr?.message || ""} ${authUpdErr?.code || ""}`.toLowerCase();
      if (
        authErrText.includes("already") ||
        authErrText.includes("exists") ||
        authErrText.includes("duplicate") ||
        authErrText.includes("taken")
      ) {
        return res.status(409).json({ error: "El correo ya existe en Auth." });
      }
      return res
        .status(400)
        .json({ error: authUpdErr?.message || "No se pudo actualizar el correo en Auth." });
    }

    return res.json({
      ok: true,
      id_usuario: idUsuarioTarget,
      correo,
      auth_synced: true,
      auth_user_id: String(authUpdatedData?.user?.id || authUserId),
    });
  } catch (err) {
    console.error("[admin/usuarios/correo] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo actualizar el correo del usuario." });
  }
});

// Genera link de registro firmado para un usuario existente (solo admin/superadmin)
app.post("/api/usuarios/:id_usuario/signup-link", async (req, res) => {
  try {
    await requireAdminSession(req);
    const idUsuarioTarget = toPositiveInt(req.params?.id_usuario);
    if (!idUsuarioTarget) {
      return res.status(400).json({ error: "id_usuario inválido." });
    }

    const { data: targetRow, error: targetErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, fecha_registro, id_auth")
      .eq("id_usuario", idUsuarioTarget)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      return res.status(404).json({ error: "Usuario no encontrado." });
    }
    if (isUsuarioWebRegistrado(targetRow)) {
      return res.status(409).json({ error: "El usuario ya está registrado." });
    }

    const nowSec = Math.floor(Date.now() / 1000);
    const expiresAtIso = new Date((nowSec + SIGNUP_TOKEN_TTL_SEC) * 1000).toISOString();
    const signupUrl = buildSignupRegistrationUrl(idUsuarioTarget);

    return res.json({
      ok: true,
      url: signupUrl,
      expires_at: expiresAtIso,
      expires_in_sec: SIGNUP_TOKEN_TTL_SEC,
    });
  } catch (err) {
    console.error("[usuarios/signup-link] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "ADMIN_REQUIRED") {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    if (err?.code === "SIGNUP_TOKEN_SECRET_MISSING") {
      return res
        .status(500)
        .json({ error: "Token de registro no configurado en el backend." });
    }
    return res.status(500).json({ error: err?.message || "No se pudo generar el link de registro." });
  }
});

// Genera link de registro para usuarios nuevos con acceso_cliente tokenizado (solo admin/superadmin)
app.post("/api/signup-link/public", async (req, res) => {
  try {
    await requireAdminSession(req);
    const accesoCliente = isTrue(req.body?.acceso_cliente);
    console.log("[signup-link/public] request", {
      acceso_cliente: accesoCliente,
      has_cookie: !!String(req.headers?.cookie || "").trim(),
    });
    const token = buildSignupAccessToken({ accesoCliente });
    const nowSec = Math.floor(Date.now() / 1000);
    const expiresAtIso = new Date((nowSec + SIGNUP_ACCESS_TOKEN_TTL_SEC) * 1000).toISOString();
    const signupUrl = new URL("/signup.html", PUBLIC_SITE_URL);
    signupUrl.searchParams.set("sat", token);
    console.log("[signup-link/public] generated", {
      acceso_cliente: accesoCliente,
      expires_at: expiresAtIso,
      url_preview: `${signupUrl.origin}${signupUrl.pathname}?sat=...`,
    });
    return res.json({
      ok: true,
      url: signupUrl.toString(),
      expires_at: expiresAtIso,
      expires_in_sec: SIGNUP_ACCESS_TOKEN_TTL_SEC,
      acceso_cliente: accesoCliente,
    });
  } catch (err) {
    console.error("[signup-link/public] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "ADMIN_REQUIRED") {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    if (err?.code === "SIGNUP_TOKEN_SECRET_MISSING") {
      return res.status(500).json({ error: "Token de registro no configurado en el backend." });
    }
    return res.status(500).json({ error: err?.message || "No se pudo generar el link de registro." });
  }
});

app.get("/api/auth/stats/verified-users", async (req, res) => {
  try {
    await requireSuperadminSession(req);
    const stats = await getAuthVerifiedUsersStats();
    return res.json({
      ok: true,
      total_users: stats.totalUsers,
      verified_users: stats.verifiedUsers,
      unverified_users: stats.unverifiedUsers,
    });
  } catch (err) {
    console.error("[auth/stats/verified-users] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === SUPERADMIN_REQUIRED || err?.message === SUPERADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo superadmin" });
    }
    return res.status(500).json({ error: "No se pudo obtener el conteo de usuarios verificados en Auth." });
  }
});

// Redirección corta para links de registro: /s/:token -> /signup.html?t=token
app.get("/s/:token", async (req, res) => {
  try {
    const token = String(req.params?.token || "").trim();
    if (!token) {
      return res.redirect(302, new URL("/signup.html", PUBLIC_SITE_URL).toString());
    }
    const signupUrl = new URL("/signup.html", PUBLIC_SITE_URL);
    signupUrl.searchParams.set("t", token);
    return res.redirect(302, signupUrl.toString());
  } catch (_err) {
    return res.redirect(302, new URL("/signup.html", PUBLIC_SITE_URL).toString());
  }
});

// Redirección corta para links de renovaciones: /r/:token -> /cart.html?rr=token
app.get("/r/:token", async (req, res) => {
  try {
    const token = String(req.params?.token || "").trim();
    const cartUrl = new URL("/cart.html", PUBLIC_SITE_URL);
    if (token) {
      cartUrl.searchParams.set("rr", token);
    }
    return res.redirect(302, cartUrl.toString());
  } catch (_err) {
    return res.redirect(302, new URL("/cart.html", PUBLIC_SITE_URL).toString());
  }
});

// Reenvía confirmación de signup vía service role (sin captcha de cliente).
app.post("/api/auth/resend-signup-confirmation", async (req, res) => {
  try {
    const email = String(req.body?.email || "")
      .trim()
      .toLowerCase();
    const redirectToRaw = String(req.body?.redirect_to || "").trim();
    const emailRedirectTo = isAllowedPublicRedirectUrl(redirectToRaw)
      ? redirectToRaw
      : `${PUBLIC_SITE_URL}/login.html`;

    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return res.status(400).json({ error: "Correo inválido." });
    }

    const nowMs = Date.now();
    const lastMs = Number(signupResendCooldownMap.get(email) || 0);
    const waitMs = SIGNUP_RESEND_COOLDOWN_MS - (nowMs - lastMs);
    if (waitMs > 0) {
      return res.status(429).json({
        error: `Espera ${Math.ceil(waitMs / 1000)}s antes de reenviar.`,
      });
    }
    signupResendCooldownMap.set(email, nowMs);

    const { error } = await supabaseAdmin.auth.resend({
      type: "signup",
      email,
      options: {
        emailRedirectTo,
      },
    });
    if (error) {
      console.error("[auth/resend-signup-confirmation] resend error", {
        email,
        message: error?.message || "",
        code: error?.code || "",
        status: error?.status || "",
      });
      return res
        .status(500)
        .json({ error: error?.message || "No se pudo reenviar la confirmación." });
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error("[auth/resend-signup-confirmation] unexpected error", err);
    return res.status(500).json({ error: "No se pudo reenviar la confirmación." });
  }
});

// Valida token de link de registro y devuelve el usuario objetivo
app.get("/api/signup-link/validate", async (req, res) => {
  try {
    const token = String(req.query?.token || "").trim();
    const payload = verifySignupRegistrationToken(token);
    const { data: targetRow, error: targetErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, nombre, apellido, correo, fecha_registro, id_auth")
      .eq("id_usuario", payload.uid)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      return res.status(404).json({ error: "Usuario del link no encontrado." });
    }
    if (isUsuarioWebRegistrado(targetRow)) {
      return res.status(409).json({ error: "El usuario del link ya está registrado." });
    }

    return res.json({
      ok: true,
      id_usuario: targetRow.id_usuario,
      usuario: {
        nombre: String(targetRow?.nombre || "").trim(),
        apellido: String(targetRow?.apellido || "").trim(),
        correo: String(targetRow?.correo || "").trim().toLowerCase(),
      },
      expires_at: new Date(payload.exp * 1000).toISOString(),
    });
  } catch (err) {
    if (
      err?.code === "TOKEN_INVALID" ||
      err?.code === "TOKEN_REQUIRED" ||
      err?.code === "TOKEN_EXPIRED" ||
      err?.code === "SIGNUP_TOKEN_SECRET_MISSING"
    ) {
      return res.status(signupTokenErrorStatus(err.code)).json({ error: err.message });
    }
    console.error("[signup-link/validate] error", err);
    return res.status(500).json({ error: err?.message || "No se pudo validar el token." });
  }
});

// Completa registro usando token firmado: siempre vincula al id_usuario del token
app.post("/api/signup-link/complete", async (req, res) => {
  try {
    const token = String(req.body?.token || "").trim();
    const payload = verifySignupRegistrationToken(token);

    const normalizeSignupPhone = (value) => {
      const digits = String(value || "")
        .replace(/\D+/g, "")
        .replace(/^0+/, "");
      if (!digits) return "";
      if (digits.startsWith("58")) {
        const national = digits.slice(2).replace(/^0+/, "");
        return national ? `58${national}` : "58";
      }
      return digits;
    };

    const nombre = String(req.body?.nombre || "").trim();
    const apellido = String(req.body?.apellido || "").trim();
    const telefono = normalizeSignupPhone(req.body?.telefono);
    const correo = String(req.body?.correo || "")
      .trim()
      .toLowerCase();

    if (!nombre || !apellido || !telefono || !correo) {
      return res
        .status(400)
        .json({ error: "nombre, apellido, telefono y correo son requeridos." });
    }

    const { data: targetRow, error: targetErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, fecha_registro, id_auth")
      .eq("id_usuario", payload.uid)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      return res.status(404).json({ error: "Usuario del link no encontrado." });
    }
    if (isUsuarioWebRegistrado(targetRow)) {
      return res.status(409).json({ error: "El usuario del link ya está registrado." });
    }

    const { data: correoUso, error: correoErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario")
      .ilike("correo", correo)
      .neq("id_usuario", payload.uid)
      .limit(1);
    if (correoErr) throw correoErr;
    if (Array.isArray(correoUso) && correoUso.length) {
      return res.status(409).json({ error: "Ese correo ya está asociado a otro usuario." });
    }

    const { data: updated, error: updErr } = await supabaseAdmin
      .from("usuarios")
      .update({
        nombre,
        apellido,
        telefono,
        correo,
        fecha_registro: todayInVenezuela(),
      })
      .eq("id_usuario", payload.uid)
      .select("id_usuario")
      .maybeSingle();
    if (updErr) throw updErr;
    if (!updated?.id_usuario) {
      return res.status(500).json({ error: "No se pudo completar el registro." });
    }

    return res.json({ ok: true, id_usuario: updated.id_usuario });
  } catch (err) {
    if (
      err?.code === "TOKEN_INVALID" ||
      err?.code === "TOKEN_REQUIRED" ||
      err?.code === "TOKEN_EXPIRED" ||
      err?.code === "SIGNUP_TOKEN_SECRET_MISSING"
    ) {
      return res.status(signupTokenErrorStatus(err.code)).json({ error: err.message });
    }
    console.error("[signup-link/complete] error", err);
    return res.status(500).json({ error: err?.message || "No se pudo completar el registro." });
  }
});

const SESSION_USER_SELECT_FIELDS =
  "id_usuario, nombre, apellido, correo, telefono, foto_perfil, fondo_perfil, permiso_admin, permiso_superadmin, acceso_cliente, notificacion_inventario, saldo, recordatorio_dias_antes, tutorial_completado";

const fetchSessionUserProfile = async (idUsuario) => {
  const normalizedId = toPositiveInt(idUsuario);
  if (!normalizedId) return null;
  const { data, error } = await supabaseAdmin
    .from("usuarios")
    .select(SESSION_USER_SELECT_FIELDS)
    .eq("id_usuario", normalizedId)
    .maybeSingle();
  if (error) throw error;
  return data || null;
};

// Sesión: setea cookie httpOnly con el id de usuario autenticado en Supabase Auth.
app.post("/api/session", async (req, res) => {
  const requestMeta = {
    host: String(req.get("host") || "").slice(0, 160),
    origin: String(req.get("origin") || "").slice(0, 200),
    referer: String(req.get("referer") || "").slice(0, 240),
    userAgent: String(req.get("user-agent") || "").slice(0, 240),
    hasBearerToken: Boolean(getBearerTokenFromRequest(req)),
  };
  console.info("[session] start", requestMeta);
  try {
    const token = getBearerTokenFromRequest(req);
    const idUsuario = await resolveUsuarioFromAuthToken(token);
    const user = await fetchSessionUserProfile(idUsuario);
    const cookieValue = signSessionCookieValue(idUsuario);
    if (!cookieValue) {
      console.warn("[session] invalid cookie configuration", {
        ...requestMeta,
        id_usuario: idUsuario,
      });
      return res.status(500).json({ error: "Configuración de sesión inválida." });
    }
    res.cookie(SESSION_COOKIE_NAME, cookieValue, {
      ...SESSION_COOKIE_OPTIONS,
      maxAge: SESSION_COOKIE_TTL_MS,
    });
    console.info("[session] success", {
      ...requestMeta,
      id_usuario: idUsuario,
      has_user: Boolean(user),
    });
    return res.json({ ok: true, id_usuario: idUsuario, user });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      console.warn("[session] auth required", requestMeta);
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "USER_NOT_LINKED") {
      console.warn("[session] user not linked", requestMeta);
      return res.status(403).json({ error: "Usuario de auth no vinculado en usuarios." });
    }
    if (err?.code === "USER_EMAIL_DUPLICATED") {
      console.warn("[session] duplicated email", requestMeta);
      return res.status(409).json({ error: "Correo duplicado en usuarios. Contacta soporte." });
    }
    if (err?.code === "USER_AUTH_DUPLICATED") {
      console.warn("[session] duplicated auth user", requestMeta);
      return res
        .status(409)
        .json({ error: "La cuenta auth está vinculada a más de un usuario. Contacta soporte." });
    }
    console.error("[session] error", {
      ...requestMeta,
      name: err?.name || "",
      code: err?.code || "",
      message: err?.message || String(err || ""),
      stack: err?.stack || "",
    });
    return res.status(500).json({ error: "No se pudo establecer la sesión." });
  }
});

app.get("/api/session/user", async (req, res) => {
  try {
    const idUsuario = requireSessionUserId(req);
    const user = await fetchSessionUserProfile(idUsuario);
    if (!user) {
      return res.status(404).json({ error: "Usuario no encontrado." });
    }
    return res.json({ ok: true, user });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    console.error("[session/user] error", {
      name: err?.name || "",
      code: err?.code || "",
      message: err?.message || String(err || ""),
      stack: err?.stack || "",
    });
    return res.status(500).json({ error: "No se pudo cargar el usuario de sesión." });
  }
});

app.delete("/api/session", (_req, res) => {
  res.clearCookie(SESSION_COOKIE_NAME, SESSION_COOKIE_OPTIONS);
  res.json({ ok: true });
});

const truncateText = (value, max = 2000) => {
  const txt = String(value ?? "");
  return txt.length > max ? `${txt.slice(0, max)}…` : txt;
};

const isAllowedTrafficUrl = (value = "") => {
  const raw = String(value || "").trim();
  if (!raw) return false;
  try {
    const parsed = new URL(raw);
    const host = String(parsed.hostname || "").trim().toLowerCase();
    return host === "mooseplus.com" || host === "www.mooseplus.com";
  } catch (_err) {
    return false;
  }
};

const toCaracasDateKey = (value) => {
  if (!value) return "";
  try {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/Caracas",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date(value));
  } catch (_err) {
    return "";
  }
};

const toCaracasHourNumber = (value) => {
  if (!value) return null;
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Caracas",
      hour: "2-digit",
      hour12: false,
      hourCycle: "h23",
    }).formatToParts(new Date(value));
    const hourTxt = parts.find((part) => part.type === "hour")?.value || "";
    const hour = Number(hourTxt);
    return Number.isInteger(hour) && hour >= 0 && hour <= 23 ? hour : null;
  } catch (_err) {
    return null;
  }
};

const rememberTrafficDailyUniqueUser = async ({ idUsuario = null, fechaHora = null } = {}) => {
  const userId = toPositiveInt(idUsuario);
  if (!userId) return { ok: false, skipped: true, reason: "invalid_user" };
  const fecha = toCaracasDateKey(fechaHora) || todayInVenezuela();
  if (!fecha) return { ok: false, skipped: true, reason: "invalid_date" };
  const payload = { fecha, id_usuario: userId };
  const { error } = await supabaseAdmin
    .from(WEB_TRAFFIC_DAILY_TABLE)
    .upsert(payload, { onConflict: "fecha,id_usuario", ignoreDuplicates: true });
  if (error) {
    if (isMissingTableError(error, WEB_TRAFFIC_DAILY_TABLE)) {
      return { ok: false, skipped: true, tableMissing: true };
    }
    throw error;
  }
  return { ok: true };
};

const rememberTrafficHourlyUniqueUser = async ({ idUsuario = null, fechaHora = null } = {}) => {
  const userId = toPositiveInt(idUsuario);
  if (!userId) return { ok: false, skipped: true, reason: "invalid_user" };
  const fecha = toCaracasDateKey(fechaHora) || todayInVenezuela();
  const hora = toCaracasHourNumber(fechaHora);
  if (!fecha) return { ok: false, skipped: true, reason: "invalid_date" };
  if (!Number.isInteger(hora) || hora < 0 || hora > 23) {
    return { ok: false, skipped: true, reason: "invalid_hour" };
  }
  const payload = { fecha, hora, id_usuario: userId };
  const { error } = await supabaseAdmin
    .from(WEB_TRAFFIC_HOURLY_TABLE)
    .upsert(payload, { onConflict: "fecha,hora,id_usuario", ignoreDuplicates: true });
  if (error) {
    if (isMissingTableError(error, WEB_TRAFFIC_HOURLY_TABLE)) {
      return { ok: false, skipped: true, tableMissing: true };
    }
    throw error;
  }
  return { ok: true };
};

app.post("/api/client-errors", jsonParser, async (req, res) => {
  try {
    const sessionUserId = toPositiveInt(parseSessionUserId(req));
    const body = req?.body && typeof req.body === "object" ? req.body : {};
    const asInt = (value) => {
      const num = Number(value);
      return Number.isFinite(num) ? Math.trunc(num) : null;
    };
    const safeMetadata =
      body?.metadata && typeof body.metadata === "object" ? body.metadata : null;

    const payload = {
      id_usuario: sessionUserId,
      level: truncateText(body?.level || "error", 20),
      kind: truncateText(body?.kind || "runtime", 50),
      message: truncateText(body?.message || "Frontend error", 4000),
      stack: truncateText(body?.stack || "", 12000),
      source: truncateText(body?.source || "", 1200),
      line: asInt(body?.line),
      column_number: asInt(body?.column),
      page_url: truncateText(body?.page_url || "", 2000),
      page_path: truncateText(body?.page_path || "", 600),
      user_agent: truncateText(body?.user_agent || req.get("user-agent") || "", 1200),
      metadata: safeMetadata,
      occurred_at: body?.occurred_at || new Date().toISOString(),
    };

    const { error } = await supabaseAdmin.from("frontend_error_logs").insert(payload);
    if (error) {
      if (String(error.code || "") === "42P01") {
        return res
          .status(503)
          .json({ ok: false, tableMissing: true, error: "Tabla frontend_error_logs no existe." });
      }
      throw error;
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error("[client-errors] save error", err);
    return res.status(500).json({ ok: false, error: "No se pudo guardar el error de cliente." });
  }
});

app.post("/api/eventos-trafico-web", jsonParser, async (req, res) => {
  try {
    const sessionUserId = toPositiveInt(parseSessionUserId(req));
    const body = req?.body && typeof req.body === "object" ? req.body : {};
    const allowedTiposEvento = new Set([
      "inicio_sesion_web",
      "vista_pagina",
      "latido_sesion",
    ]);
    const idSesion = truncateText(body?.id_sesion || "", 120).trim().toLowerCase();
    const tipoEvento = truncateText(body?.tipo_evento || "vista_pagina", 40).trim();
    const ruta = truncateText(body?.ruta || "", 600).trim();
    const safeMetadata =
      body?.metadatos && typeof body.metadatos === "object" ? body.metadatos : null;

    if (!idSesion || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(idSesion)) {
      return res.status(400).json({ ok: false, error: "id_sesion inválido." });
    }
    if (!allowedTiposEvento.has(tipoEvento)) {
      return res.status(400).json({ ok: false, error: "tipo_evento inválido." });
    }
    if (!ruta) {
      return res.status(400).json({ ok: false, error: "ruta es requerida." });
    }
    if (!Number.isFinite(sessionUserId) || sessionUserId <= 0) {
      return res.json({ ok: true, ignored: true, reason: "invalid_user" });
    }
    if (!isAllowedTrafficUrl(body?.url_completa)) {
      return res.json({ ok: true, ignored: true, reason: "host_not_allowed" });
    }

    const payload = {
      id_usuario: sessionUserId,
      tipo_evento: tipoEvento,
      id_sesion: idSesion,
      ruta,
      url_completa: truncateText(body?.url_completa || "", 2000),
      referidor: truncateText(body?.referidor || req.get("referer") || "", 2000),
      agente_usuario: truncateText(body?.agente_usuario || req.get("user-agent") || "", 1200),
      metadatos: safeMetadata,
      fecha_hora: body?.fecha_hora || new Date().toISOString(),
    };

    const { error } = await supabaseAdmin.from(WEB_TRAFFIC_EVENTS_TABLE).insert(payload);
    if (error) {
      if (isMissingTableError(error, WEB_TRAFFIC_EVENTS_TABLE)) {
        return res.status(503).json({
          ok: false,
          tableMissing: true,
          error: "Tabla eventos_trafico_web no existe.",
        });
      }
      throw error;
    }

    try {
      await rememberTrafficDailyUniqueUser({
        idUsuario: payload.id_usuario,
        fechaHora: payload.fecha_hora,
      });
    } catch (dailyErr) {
      console.error("[eventos-trafico-web] upsert diario unico error", dailyErr);
    }
    try {
      await rememberTrafficHourlyUniqueUser({
        idUsuario: payload.id_usuario,
        fechaHora: payload.fecha_hora,
      });
    } catch (hourlyErr) {
      console.error("[eventos-trafico-web] upsert horario unico error", hourlyErr);
    }

    if (payload.id_usuario) {
      const fechaConexion = todayInVenezuela();
      const { error: userErr } = await supabaseAdmin
        .from("usuarios")
        .update({ ultima_conexion: fechaConexion })
        .eq("id_usuario", payload.id_usuario);
      if (userErr) {
        console.error("[eventos-trafico-web] update ultima_conexion error", userErr);
      }
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error("[eventos-trafico-web] save error", err);
    return res.status(500).json({ ok: false, error: "No se pudo guardar el evento de tráfico." });
  }
});

app.get("/api/dashboard/analitica-web", async (req, res) => {
  try {
    await requireAdminSession(req);

    const monthRegex = /^\d{4}-\d{2}$/;
    const buildCaracasMonthKey = () => {
      const parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "America/Caracas",
        year: "numeric",
        month: "2-digit",
      }).formatToParts(new Date());
      const pick = (type) => parts.find((part) => part.type === type)?.value || "";
      return `${pick("year")}-${pick("month")}`;
    };
    const monthValRaw = String(req.query?.month || "").trim();
    const monthVal = monthRegex.test(monthValRaw) ? monthValRaw : buildCaracasMonthKey();
    const currentMonthVal = buildCaracasMonthKey();
    const monthToIndex = (value = "") => {
      const match = String(value || "").match(/^(\d{4})-(\d{2})$/);
      if (!match) return null;
      const year = Number(match[1]);
      const month = Number(match[2]);
      if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
        return null;
      }
      return year * 12 + (month - 1);
    };
    const indexToMonthKey = (idx) => {
      if (!Number.isInteger(idx)) return null;
      const year = Math.floor(idx / 12);
      const month = (idx % 12) + 1;
      return `${year}-${String(month).padStart(2, "0")}`;
    };
    const buildMonthRange = (value = "") => {
      const match = String(value || "").match(/^(\d{4})-(\d{2})$/);
      if (!match) return null;
      const year = Number(match[1]);
      const month = Number(match[2]);
      if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
        return null;
      }
      const lastDay = new Date(year, month, 0).getDate();
      const monthTxt = String(month).padStart(2, "0");
      return {
        start: `${year}-${monthTxt}-01`,
        end: `${year}-${monthTxt}-${String(lastDay).padStart(2, "0")}`,
        nextStart:
          month === 12
            ? `${year + 1}-01-01`
            : `${year}-${String(month + 1).padStart(2, "0")}-01`,
      };
    };
    const addDaysToDateKey = (dateKey = "", delta = 0) => {
      const match = String(dateKey || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!match) return "";
      const year = Number(match[1]);
      const month = Number(match[2]);
      const day = Number(match[3]);
      if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return "";
      const dt = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
      dt.setUTCDate(dt.getUTCDate() + Math.trunc(Number(delta) || 0));
      const yyyy = dt.getUTCFullYear();
      const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(dt.getUTCDate()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd}`;
    };
    const buildRollingRangeFromEnd = (endDateKey, days = 30) => {
      const safeDays = Math.max(1, Math.trunc(Number(days) || 30));
      const end = String(endDateKey || "").trim();
      const start = addDaysToDateKey(end, -(safeDays - 1));
      const nextStart = addDaysToDateKey(end, 1);
      if (!start || !end || !nextStart) return null;
      return { start, end, nextStart, days: safeDays };
    };
    const buildMonthRangeUntilDay = (value = "", dayLimit = null) => {
      const base = buildMonthRange(value);
      if (!base) return null;
      const parsedDay = Math.trunc(Number(dayLimit));
      if (!Number.isInteger(parsedDay) || parsedDay < 1) return base;
      const endDay = Math.min(parsedDay, Number(String(base.end).slice(-2)) || parsedDay);
      const end = `${String(base.start).slice(0, 8)}${String(endDay).padStart(2, "0")}`;
      return {
        ...base,
        end,
        nextStart: addDaysToDateKey(end, 1),
      };
    };
    const previousMonthVal = indexToMonthKey((monthToIndex(monthVal) || 0) - 1);
    const range = buildMonthRange(monthVal);
    const prevRange = buildMonthRange(previousMonthVal);
    const currentMonthRange = buildMonthRange(currentMonthVal);
    const todayCaracas = todayInVenezuela();
    const todayCaracasDay = Number(String(todayCaracas || "").slice(-2)) || 1;
    const authCoverageRange =
      monthVal === currentMonthVal ? buildMonthRangeUntilDay(monthVal, todayCaracasDay) : range;
    const trafficMonthCurrentVal = currentMonthVal;
    const trafficMonthPreviousVal = indexToMonthKey((monthToIndex(currentMonthVal) || 0) - 1);
    const trafficMonthCurrentRange = buildMonthRange(trafficMonthCurrentVal);
    const trafficMonthPreviousRange = buildMonthRange(trafficMonthPreviousVal);
    const trafficRangeCurrent = buildRollingRangeFromEnd(todayCaracas, 30);
    const trafficRangePrev = buildRollingRangeFromEnd(addDaysToDateKey(todayCaracas, -30), 30);
    if (!range || !prevRange || !currentMonthRange) {
      return res.status(400).json({ error: "Mes inválido." });
    }
    if (
      !trafficRangeCurrent ||
      !trafficRangePrev ||
      !trafficMonthCurrentRange ||
      !trafficMonthPreviousRange ||
      !authCoverageRange
    ) {
      return res.status(500).json({ error: "No se pudo calcular el rango de tráfico." });
    }

    const toCaracasDate = (value) => {
      if (!value) return "";
      try {
        return new Intl.DateTimeFormat("en-CA", {
          timeZone: "America/Caracas",
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
        }).format(new Date(value));
      } catch (_err) {
        return "";
      }
    };
    const toCaracasMonth = (value) => {
      const dateKey = toCaracasDate(value);
      return dateKey ? dateKey.slice(0, 7) : "";
    };
    const weekdayMap = {
      Mon: "lunes",
      Tue: "martes",
      Wed: "miercoles",
      Thu: "jueves",
      Fri: "viernes",
      Sat: "sabado",
      Sun: "domingo",
    };
    const weekdayOrder = [
      { key: "lunes", label: "Lun" },
      { key: "martes", label: "Mar" },
      { key: "miercoles", label: "Mie" },
      { key: "jueves", label: "Jue" },
      { key: "viernes", label: "Vie" },
      { key: "sabado", label: "Sab" },
      { key: "domingo", label: "Dom" },
    ];
    const toCaracasWeekdayKey = (value) => {
      if (!value) return "";
      try {
        const key = new Intl.DateTimeFormat("en-US", {
          timeZone: "America/Caracas",
          weekday: "short",
        }).format(new Date(value));
        return weekdayMap[key] || "";
      } catch (_err) {
        return "";
      }
    };
    const toCaracasHour = (value) => {
      return toCaracasHourNumber(value);
    };
    const roundTrafficAverage = (value) =>
      Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;
    const buildDateKeysFromRange = (dateRange = null) => {
      const startRaw = String(dateRange?.start || "").trim();
      const endRaw = String(dateRange?.end || "").trim();
      const startMatch = startRaw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      const endMatch = endRaw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!startMatch || !endMatch) return [];
      const startUtc = Date.UTC(
        Number(startMatch[1]),
        Number(startMatch[2]) - 1,
        Number(startMatch[3]),
        0,
        0,
        0,
        0,
      );
      const endUtc = Date.UTC(
        Number(endMatch[1]),
        Number(endMatch[2]) - 1,
        Number(endMatch[3]),
        0,
        0,
        0,
        0,
      );
      if (!Number.isFinite(startUtc) || !Number.isFinite(endUtc) || endUtc < startUtc) return [];
      const result = [];
      for (let cursor = startUtc; cursor <= endUtc; cursor += 86400000) {
        const dt = new Date(cursor);
        const yyyy = dt.getUTCFullYear();
        const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
        const dd = String(dt.getUTCDate()).padStart(2, "0");
        result.push(`${yyyy}-${mm}-${dd}`);
      }
      return result;
    };
    const [
      usuariosResp,
      authUsers,
      traficoActualResp,
      traficoPrevResp,
      traficoActualDailyResp,
      traficoPrevDailyResp,
      traficoActualHourlyResp,
      traficoPrevHourlyResp,
      traficoMesActualResp,
      traficoMesAnteriorResp,
      traficoMesActualDailyResp,
      traficoMesAnteriorDailyResp,
      historialClientesActivosResp,
    ] = await Promise.all([
      supabaseAdmin
        .from("usuarios")
        .select("id_usuario, id_auth, acceso_cliente"),
      listAllAuthUsers(),
      supabaseAdmin
        .from(WEB_TRAFFIC_EVENTS_TABLE)
        .select("fecha_hora, id_usuario, id_sesion, url_completa, tipo_evento")
        .gte("fecha_hora", `${trafficRangeCurrent.start}T00:00:00-04:00`)
        .lt("fecha_hora", `${trafficRangeCurrent.nextStart}T00:00:00-04:00`),
      supabaseAdmin
        .from(WEB_TRAFFIC_EVENTS_TABLE)
        .select("fecha_hora, id_usuario, id_sesion, url_completa, tipo_evento")
        .gte("fecha_hora", `${trafficRangePrev.start}T00:00:00-04:00`)
        .lt("fecha_hora", `${trafficRangePrev.nextStart}T00:00:00-04:00`),
      supabaseAdmin
        .from(WEB_TRAFFIC_DAILY_TABLE)
        .select("fecha, id_usuario")
        .gte("fecha", trafficRangeCurrent.start)
        .lt("fecha", trafficRangeCurrent.nextStart),
      supabaseAdmin
        .from(WEB_TRAFFIC_DAILY_TABLE)
        .select("fecha, id_usuario")
        .gte("fecha", trafficRangePrev.start)
        .lt("fecha", trafficRangePrev.nextStart),
      supabaseAdmin
        .from(WEB_TRAFFIC_HOURLY_TABLE)
        .select("fecha, hora, id_usuario")
        .gte("fecha", trafficRangeCurrent.start)
        .lt("fecha", trafficRangeCurrent.nextStart),
      supabaseAdmin
        .from(WEB_TRAFFIC_HOURLY_TABLE)
        .select("fecha, hora, id_usuario")
        .gte("fecha", trafficRangePrev.start)
        .lt("fecha", trafficRangePrev.nextStart),
      supabaseAdmin
        .from(WEB_TRAFFIC_EVENTS_TABLE)
        .select("fecha_hora, id_usuario, id_sesion, url_completa, tipo_evento")
        .gte("fecha_hora", `${trafficMonthCurrentRange.start}T00:00:00-04:00`)
        .lt("fecha_hora", `${trafficMonthCurrentRange.nextStart}T00:00:00-04:00`),
      supabaseAdmin
        .from(WEB_TRAFFIC_EVENTS_TABLE)
        .select("fecha_hora, id_usuario, id_sesion, url_completa, tipo_evento")
        .gte("fecha_hora", `${trafficMonthPreviousRange.start}T00:00:00-04:00`)
        .lt("fecha_hora", `${trafficMonthPreviousRange.nextStart}T00:00:00-04:00`),
      supabaseAdmin
        .from(WEB_TRAFFIC_DAILY_TABLE)
        .select("fecha, id_usuario")
        .gte("fecha", trafficMonthCurrentRange.start)
        .lt("fecha", trafficMonthCurrentRange.nextStart),
      supabaseAdmin
        .from(WEB_TRAFFIC_DAILY_TABLE)
        .select("fecha, id_usuario")
        .gte("fecha", trafficMonthPreviousRange.start)
        .lt("fecha", trafficMonthPreviousRange.nextStart),
      supabaseAdmin
        .from("historial_ventas")
        .select("id_usuario_cliente, fecha_pago")
        .not("id_usuario_cliente", "is", null)
        .gte("fecha_pago", authCoverageRange.start)
        .lt("fecha_pago", authCoverageRange.nextStart),
    ]);

    if (usuariosResp.error) throw usuariosResp.error;

    let traficoActualRows = traficoActualResp.data || [];
    let traficoPrevRows = traficoPrevResp.data || [];
    let traficoActualDailyRows = traficoActualDailyResp.data || [];
    let traficoPrevDailyRows = traficoPrevDailyResp.data || [];
    let traficoActualHourlyRows = traficoActualHourlyResp.data || [];
    let traficoPrevHourlyRows = traficoPrevHourlyResp.data || [];
    let traficoMesActualRows = traficoMesActualResp.data || [];
    let traficoMesAnteriorRows = traficoMesAnteriorResp.data || [];
    let traficoMesActualDailyRows = traficoMesActualDailyResp.data || [];
    let traficoMesAnteriorDailyRows = traficoMesAnteriorDailyResp.data || [];
    let historialClientesActivosRows = historialClientesActivosResp.data || [];
    if (traficoActualResp.error) {
      if (!isMissingTableError(traficoActualResp.error, WEB_TRAFFIC_EVENTS_TABLE)) {
        throw traficoActualResp.error;
      }
      traficoActualRows = [];
    }
    if (traficoPrevResp.error) {
      if (!isMissingTableError(traficoPrevResp.error, WEB_TRAFFIC_EVENTS_TABLE)) {
        throw traficoPrevResp.error;
      }
      traficoPrevRows = [];
    }
    if (traficoActualDailyResp.error) {
      if (!isMissingTableError(traficoActualDailyResp.error, WEB_TRAFFIC_DAILY_TABLE)) {
        throw traficoActualDailyResp.error;
      }
      traficoActualDailyRows = [];
    }
    if (traficoPrevDailyResp.error) {
      if (!isMissingTableError(traficoPrevDailyResp.error, WEB_TRAFFIC_DAILY_TABLE)) {
        throw traficoPrevDailyResp.error;
      }
      traficoPrevDailyRows = [];
    }
    if (traficoActualHourlyResp.error) {
      if (!isMissingTableError(traficoActualHourlyResp.error, WEB_TRAFFIC_HOURLY_TABLE)) {
        throw traficoActualHourlyResp.error;
      }
      traficoActualHourlyRows = [];
    }
    if (traficoPrevHourlyResp.error) {
      if (!isMissingTableError(traficoPrevHourlyResp.error, WEB_TRAFFIC_HOURLY_TABLE)) {
        throw traficoPrevHourlyResp.error;
      }
      traficoPrevHourlyRows = [];
    }
    if (traficoMesActualResp.error) {
      if (!isMissingTableError(traficoMesActualResp.error, WEB_TRAFFIC_EVENTS_TABLE)) {
        throw traficoMesActualResp.error;
      }
      traficoMesActualRows = [];
    }
    if (traficoMesAnteriorResp.error) {
      if (!isMissingTableError(traficoMesAnteriorResp.error, WEB_TRAFFIC_EVENTS_TABLE)) {
        throw traficoMesAnteriorResp.error;
      }
      traficoMesAnteriorRows = [];
    }
    if (traficoMesActualDailyResp.error) {
      if (!isMissingTableError(traficoMesActualDailyResp.error, WEB_TRAFFIC_DAILY_TABLE)) {
        throw traficoMesActualDailyResp.error;
      }
      traficoMesActualDailyRows = [];
    }
    if (traficoMesAnteriorDailyResp.error) {
      if (!isMissingTableError(traficoMesAnteriorDailyResp.error, WEB_TRAFFIC_DAILY_TABLE)) {
        throw traficoMesAnteriorDailyResp.error;
      }
      traficoMesAnteriorDailyRows = [];
    }
    if (historialClientesActivosResp.error) {
      throw historialClientesActivosResp.error;
    }

    const countedTrafficEventTypes = new Set(["vista_pagina", "inicio_sesion_web"]);
    const hasValidTrafficUser = (row = {}) => {
      const idUsuario = Number(row?.id_usuario);
      return Number.isFinite(idUsuario) && idUsuario > 0;
    };
    const aggregateTrafficRows = (
      rows = [],
      dateRange = null,
      dailyRows = [],
      hourlyRows = [],
    ) => {
      const monthDateKeys = buildDateKeysFromRange(dateRange);
      const monthDateKeySet = new Set(monthDateKeys);
      const daysInMonth = monthDateKeys.length;
      const uniqueUsers = new Set();
      const dailyUsersByDate = new Map(monthDateKeys.map((dateKey) => [dateKey, new Set()]));
      const hourlyUsersByDate = Array.from({ length: 24 }, () =>
        new Map(monthDateKeys.map((dateKey) => [dateKey, new Set()])),
      );
      const weekdayDateKeys = weekdayOrder.map(() => []);

      monthDateKeys.forEach((dateKey) => {
        const weekdayKey = toCaracasWeekdayKey(`${dateKey}T12:00:00-04:00`);
        const weekdayIdx = weekdayOrder.findIndex((item) => item.key === weekdayKey);
        if (weekdayIdx >= 0) {
          weekdayDateKeys[weekdayIdx].push(dateKey);
        }
      });

      const addUserToDay = (dateKeyRaw, userIdRaw) => {
        const dateKey = String(dateKeyRaw || "").trim();
        if (!dateKey || !monthDateKeySet.has(dateKey)) return;
        const userId = String(Math.trunc(Number(userIdRaw)));
        if (!userId) return;
        uniqueUsers.add(userId);
        dailyUsersByDate.get(dateKey)?.add(userId);
      };

      (dailyRows || []).forEach((row) => {
        const dateKey = String(row?.fecha || "").trim();
        addUserToDay(dateKey, row?.id_usuario);
      });
      (rows || []).forEach((row) => {
        if (!hasValidTrafficUser(row) || !isAllowedTrafficUrl(row?.url_completa)) return;
        const tipoEvento = String(row?.tipo_evento || "").trim().toLowerCase();
        if (tipoEvento && !countedTrafficEventTypes.has(tipoEvento)) return;
        const dateKey = toCaracasDate(row?.fecha_hora);
        addUserToDay(dateKey, row?.id_usuario);
      });

      const addUserToHour = (dateKeyRaw, hourRaw, userIdRaw) => {
        const dateKey = String(dateKeyRaw || "").trim();
        if (!dateKey || !monthDateKeySet.has(dateKey)) return;
        const hour = Number(hourRaw);
        if (!Number.isInteger(hour) || hour < 0 || hour > 23) return;
        const userId = String(Math.trunc(Number(userIdRaw)));
        if (!userId) return;
        hourlyUsersByDate[hour]?.get(dateKey)?.add(userId);
      };

      (hourlyRows || []).forEach((row) => {
        addUserToHour(row?.fecha, row?.hora, row?.id_usuario);
      });
      (rows || []).forEach((row) => {
        if (!hasValidTrafficUser(row) || !isAllowedTrafficUrl(row?.url_completa)) return;
        const tipoEvento = String(row?.tipo_evento || "").trim().toLowerCase();
        if (tipoEvento && !countedTrafficEventTypes.has(tipoEvento)) return;
        addUserToHour(toCaracasDate(row?.fecha_hora), toCaracasHour(row?.fecha_hora), row?.id_usuario);
      });

      const totalDailyUniqueUsers = monthDateKeys.reduce(
        (acc, dateKey) => acc + (dailyUsersByDate.get(dateKey)?.size || 0),
        0,
      );
      const averageDailyUniqueUsers =
        daysInMonth > 0 ? totalDailyUniqueUsers / daysInMonth : 0;

      return {
        usuarios_unicos: uniqueUsers.size,
        clientes_unicos: uniqueUsers.size,
        promedio_diario_clientes_unicos: roundTrafficAverage(averageDailyUniqueUsers),
        dias_en_mes: daysInMonth,
        por_dia_semana: weekdayOrder.map((row, idx) => ({
          key: row.key,
          label: row.label,
          cantidad: roundTrafficAverage(
            weekdayDateKeys[idx]?.length
              ? weekdayDateKeys[idx].reduce(
                  (acc, dateKey) => acc + (dailyUsersByDate.get(dateKey)?.size || 0),
                  0,
                ) / weekdayDateKeys[idx].length
              : 0,
          ),
          dias: weekdayDateKeys[idx]?.length || 0,
        })),
        por_hora: hourlyUsersByDate.map((usersByDate, hour) => ({
          hora: hour,
          label: `${String(hour).padStart(2, "0")}h`,
          cantidad: roundTrafficAverage(
            daysInMonth > 0
              ? monthDateKeys.reduce(
                  (acc, dateKey) => acc + (usersByDate.get(dateKey)?.size || 0),
                  0,
                ) / daysInMonth
              : 0,
          ),
          dias: daysInMonth,
        })),
      };
    };
    const aggregateTrafficDailySeries = (rows = [], dailyRows = [], dateRange = null) => {
      const dateKeys = buildDateKeysFromRange(dateRange);
      const dateKeySet = new Set(dateKeys);
      const dailyUsersByDate = new Map(dateKeys.map((dateKey) => [dateKey, new Set()]));
      const addUserToDay = (dateKeyRaw, userIdRaw) => {
        const dateKey = String(dateKeyRaw || "").trim();
        if (!dateKey || !dateKeySet.has(dateKey)) return;
        const userId = String(Math.trunc(Number(userIdRaw)));
        if (!userId) return;
        dailyUsersByDate.get(dateKey)?.add(userId);
      };
      (dailyRows || []).forEach((row) => {
        addUserToDay(row?.fecha, row?.id_usuario);
      });
      (rows || []).forEach((row) => {
        if (!hasValidTrafficUser(row) || !isAllowedTrafficUrl(row?.url_completa)) return;
        const tipoEvento = String(row?.tipo_evento || "").trim().toLowerCase();
        if (tipoEvento && !countedTrafficEventTypes.has(tipoEvento)) return;
        addUserToDay(toCaracasDate(row?.fecha_hora), row?.id_usuario);
      });
      return dateKeys.map((dateKey) => ({
        fecha: dateKey,
        dia: Number(String(dateKey).slice(-2)) || 0,
        cantidad: dailyUsersByDate.get(dateKey)?.size || 0,
      }));
    };

    const usuarioByAuthId = new Map(
      (usuariosResp.data || [])
        .map((row) => [String(row?.id_auth || "").trim().toLowerCase(), row])
        .filter(([authId]) => !!authId),
    );
    const authConfirmedRows = (authUsers || [])
      .map((row) => {
        if (!isAuthUserVerified(row)) return null;
        const fechaConfirmacion = toCaracasDate(
          row?.email_confirmed_at || row?.phone_confirmed_at || row?.confirmed_at,
        );
        if (!fechaConfirmacion) return null;
        const authId = String(row?.id || "").trim().toLowerCase();
        const usuario = authId ? usuarioByAuthId.get(authId) : null;
        return {
          ...row,
          fecha_confirmacion: fechaConfirmacion,
          usuario,
        };
      })
      .filter(Boolean);
    const authConfirmedLinkedRows = authConfirmedRows
      .map((row) => (row?.usuario ? row : null))
      .filter(Boolean);

    const registrosPorMesMap = new Map();
    authConfirmedRows.forEach((row) => {
      const monthKey = toCaracasMonth(
        row?.email_confirmed_at || row?.phone_confirmed_at || row?.confirmed_at,
      );
      if (!monthKey) return;
      registrosPorMesMap.set(monthKey, (registrosPorMesMap.get(monthKey) || 0) + 1);
    });
    const registrosPorMes = Array.from(registrosPorMesMap.entries())
      .map(([monthKey, cantidad]) => ({ monthKey, cantidad }))
      .sort((a, b) => String(a.monthKey).localeCompare(String(b.monthKey)));
    const registrosPorDiaMesActualMap = new Map(
      buildDateKeysFromRange(currentMonthRange).map((dateKey) => [dateKey, 0]),
    );
    authConfirmedRows.forEach((row) => {
      const dateKey = String(row?.fecha_confirmacion || "").trim();
      if (!dateKey || !registrosPorDiaMesActualMap.has(dateKey)) return;
      registrosPorDiaMesActualMap.set(dateKey, (registrosPorDiaMesActualMap.get(dateKey) || 0) + 1);
    });
    const registrosPorDiaMesActual = Array.from(registrosPorDiaMesActualMap.entries()).map(
      ([dateKey, cantidad]) => ({
        fecha: dateKey,
        dia: Number(String(dateKey).slice(-2)) || 0,
        cantidad: Number(cantidad) || 0,
      }),
    );

    const countConfirmedUntil = (rows, dateLimit, predicate = null) =>
      rows.filter((row) => {
        const matchesDate = String(row?.fecha_confirmacion || "").trim() <= String(dateLimit || "");
        if (!matchesDate) return false;
        if (typeof predicate !== "function") return true;
        return predicate(row);
      }).length;

    const resolveAuthAccesoCliente = (row = {}) => {
      if (row?.usuario && row?.usuario?.acceso_cliente != null) {
        return isTrue(row.usuario.acceso_cliente);
      }
      const rawFromAuth = row?.user_metadata?.acceso_cliente ?? row?.app_metadata?.acceso_cliente;
      if (rawFromAuth == null || String(rawFromAuth).trim() === "") {
        return false;
      }
      return isTrue(rawFromAuth);
    };
    const isClienteAuthRow = (row) => resolveAuthAccesoCliente(row) === true;
    const isVendedorAuthRow = (row) => resolveAuthAccesoCliente(row) !== true;
    const clientesActivosPeriodoIds = uniqPositiveIds(
      (historialClientesActivosRows || []).map((row) => row?.id_usuario_cliente),
    );
    const clientesAuthConfirmadosLinkedHastaPeriodo = new Set(
      authConfirmedRows
        .filter((row) => {
          if (!isClienteAuthRow(row)) return false;
          const usuarioId = toPositiveInt(row?.usuario?.id_usuario);
          if (!usuarioId) return false;
          return String(row?.fecha_confirmacion || "").trim() <= String(authCoverageRange.end || "");
        })
        .map((row) => toPositiveInt(row?.usuario?.id_usuario))
        .filter(Boolean),
    );
    const clientesActivosRegistradosWeb = clientesActivosPeriodoIds.reduce(
      (acc, idUsuario) => acc + (clientesAuthConfirmadosLinkedHastaPeriodo.has(idUsuario) ? 1 : 0),
      0,
    );
    const clientesActivosBase = clientesActivosPeriodoIds.length;
    const clientesActivosNoRegistradosWeb = Math.max(
      0,
      clientesActivosBase - clientesActivosRegistradosWeb,
    );
    const porcentajeRegistradosWeb =
      clientesActivosBase > 0 ? (clientesActivosRegistradosWeb / clientesActivosBase) * 100 : 0;
    const porcentajeNoRegistradosWeb =
      clientesActivosBase > 0 ? (clientesActivosNoRegistradosWeb / clientesActivosBase) * 100 : 0;

    const usuariosAuthConfirmados = countConfirmedUntil(authConfirmedRows, range.end);
    const usuariosAuthConfirmadosPrev = countConfirmedUntil(authConfirmedRows, prevRange.end);
    const usuariosAuthConfirmadosLinked = countConfirmedUntil(authConfirmedLinkedRows, range.end);
    const usuariosAuthConfirmadosLinkedPrev = countConfirmedUntil(
      authConfirmedLinkedRows,
      prevRange.end,
    );
    const clientesAuthConfirmados = countConfirmedUntil(
      authConfirmedRows,
      range.end,
      isClienteAuthRow,
    );
    const clientesAuthConfirmadosPrev = countConfirmedUntil(
      authConfirmedRows,
      prevRange.end,
      isClienteAuthRow,
    );
    const vendedoresAuthConfirmados = countConfirmedUntil(
      authConfirmedRows,
      range.end,
      isVendedorAuthRow,
    );
    const vendedoresAuthConfirmadosPrev = countConfirmedUntil(
      authConfirmedRows,
      prevRange.end,
      isVendedorAuthRow,
    );

    return res.json({
      ok: true,
      mes: monthVal,
      auth: {
        usuarios_auth_confirmados: usuariosAuthConfirmados,
        usuarios_auth_confirmados_mes_anterior: usuariosAuthConfirmadosPrev,
        usuarios_auth_confirmados_enlazados: usuariosAuthConfirmadosLinked,
        usuarios_auth_confirmados_enlazados_mes_anterior: usuariosAuthConfirmadosLinkedPrev,
        clientes_auth_confirmados: clientesAuthConfirmados,
        clientes_auth_confirmados_mes_anterior: clientesAuthConfirmadosPrev,
        vendedores_auth_confirmados: vendedoresAuthConfirmados,
        vendedores_auth_confirmados_mes_anterior: vendedoresAuthConfirmadosPrev,
        clientes_activos_base: clientesActivosBase,
        clientes_activos_registrados_web: clientesActivosRegistradosWeb,
        clientes_activos_no_registrados_web: clientesActivosNoRegistradosWeb,
        clientes_activos_pct_registrados_web: roundTrafficAverage(porcentajeRegistradosWeb),
        clientes_activos_pct_no_registrados_web: roundTrafficAverage(porcentajeNoRegistradosWeb),
        clientes_activos_rango: authCoverageRange,
        registros_por_mes: registrosPorMes,
        mes_actual: currentMonthVal,
        registros_por_dia_mes_actual: registrosPorDiaMesActual,
      },
      trafico: {
        actual: aggregateTrafficRows(
          traficoActualRows,
          trafficRangeCurrent,
          traficoActualDailyRows,
          traficoActualHourlyRows,
        ),
        anterior: aggregateTrafficRows(
          traficoPrevRows,
          trafficRangePrev,
          traficoPrevDailyRows,
          traficoPrevHourlyRows,
        ),
        rango_actual: trafficRangeCurrent,
        rango_anterior: trafficRangePrev,
        diario_mes: {
          mes_actual: trafficMonthCurrentVal,
          mes_anterior: trafficMonthPreviousVal,
          rango_actual: trafficMonthCurrentRange,
          rango_anterior: trafficMonthPreviousRange,
          actual: aggregateTrafficDailySeries(
            traficoMesActualRows,
            traficoMesActualDailyRows,
            trafficMonthCurrentRange,
          ),
          anterior: aggregateTrafficDailySeries(
            traficoMesAnteriorRows,
            traficoMesAnteriorDailyRows,
            trafficMonthPreviousRange,
          ),
        },
      },
    });
  } catch (err) {
    console.error("[dashboard/analitica-web] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    return res.status(500).json({ error: err?.message || "No se pudo cargar la analítica web." });
  }
});

// Inventario: ventas por usuario autenticado agrupadas por plataforma
app.get("/api/inventario", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) throw new Error("Usuario no autenticado");

    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select(
        `
        id_venta,
        nombre_cliente,
        correo_miembro,
        fecha_corte,
        id_precio,
        id_cuenta,
        id_cuenta_miembro,
        id_perfil,
        id_tarjeta_de_regalo,
        completa,
        cuentas:cuentas!ventas_id_cuenta_fkey(
          id_cuenta,
          id_plataforma,
          correo,
          pin,
          clave,
          venta_perfil,
          venta_miembro,
          plataformas(
            nombre,
            color_1,
            color_2,
            color_3,
            usa_pines,
            por_pantalla,
            por_acceso,
            correo_cliente,
            clave_cliente
          )
        ),
        cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(
          id_cuenta,
          id_plataforma,
          correo,
          pin,
          clave,
          venta_perfil,
          venta_miembro,
          plataformas(
            nombre,
            color_1,
            color_2,
            color_3,
            usa_pines,
            por_pantalla,
            por_acceso,
            correo_cliente,
            clave_cliente
          )
        ),
        perfiles:perfiles(
          id_perfil,
          n_perfil,
          pin,
          perfil_hogar
        ),
        tarjetas_de_regalo:tarjetas_de_regalo!ventas_id_tarjeta_de_regalo_fkey(
          id_tarjeta_de_regalo,
          pin
        ),
        precios:precios(plan, sub_cuenta, completa, id_plataforma)
      `
      )
      .eq("id_usuario", idUsuario);
    if (error) throw error;

    const plataformaIds = Array.from(
      new Set(
        (data || []).map(
          (row) =>
            row?.cuentas?.id_plataforma ||
            row?.cuentas_miembro?.id_plataforma ||
            row?.precios?.id_plataforma ||
            null,
        ),
      ),
    ).filter(Boolean);
    let plataformaMap = {};
    if (plataformaIds.length) {
      const { data: plataformaRows, error: plataformaErr } = await supabaseAdmin
        .from("plataformas")
        .select(
          "id_plataforma, nombre, color_1, color_2, color_3, usa_pines, tarjeta_de_regalo, por_pantalla, por_acceso, correo_cliente, clave_cliente",
        )
        .in("id_plataforma", plataformaIds);
      if (plataformaErr) throw plataformaErr;
      plataformaMap = (plataformaRows || []).reduce((acc, row) => {
        acc[row.id_plataforma] = row;
        return acc;
      }, {});
    }

    const memberIds = Array.from(
      new Set(
        (data || []).flatMap((row) => [
          row?.id_cuenta_miembro,
          row?.cuentas_miembro?.id_cuenta,
        ]),
      ),
    ).filter(Boolean);
    let memberCuentaMap = {};
    if (memberIds.length) {
      const { data: memberCuentas, error: memberErr } = await supabaseAdmin
        .from("cuentas")
        .select("id_cuenta, correo, clave, pin")
        .in("id_cuenta", memberIds);
      if (memberErr) throw memberErr;
      memberCuentaMap = (memberCuentas || []).reduce((acc, c) => {
        acc[c.id_cuenta] = c;
        return acc;
      }, {});
    }

    const items = (data || []).map((row) => {
      const plataformaId =
        row.cuentas?.id_plataforma || row.cuentas_miembro?.id_plataforma || row.precios?.id_plataforma || null;
      const plataformaInfo =
        row.cuentas?.plataformas || row.cuentas_miembro?.plataformas || plataformaMap[plataformaId] || null;
      const plataforma = plataformaInfo?.nombre || "Sin plataforma";
      const color_1 = plataformaInfo?.color_1 || null;
      const color_2 = plataformaInfo?.color_2 || null;
      const color_3 = plataformaInfo?.color_3 || null;
      const usa_pines = plataformaInfo?.usa_pines ?? null;
      const por_pantalla = plataformaInfo?.por_pantalla ?? null;
      const por_acceso = plataformaInfo?.por_acceso ?? null;
      const correo_cliente_flag = plataformaInfo?.correo_cliente ?? null;
      const clave_cliente_flag = plataformaInfo?.clave_cliente ?? null;
      const isCompleta = isTrue(row?.completa) || isTrue(row?.precios?.completa);
      const plan = (row.precios?.plan || "").trim() || (isCompleta ? "Cuenta completa" : "Sin plan");
      const sub_cuenta = row.precios?.sub_cuenta ?? null;
      const memberId =
        row.id_cuenta_miembro ||
        row.cuentas_miembro?.id_cuenta ||
        null;
      const memberCuenta = memberId ? memberCuentaMap[memberId] || row.cuentas_miembro : null;
      const cuentaData = row.cuentas || row.cuentas_miembro || null;
      return {
        plataforma,
        color_1,
        color_2,
        color_3,
        usa_pines,
        tarjeta_de_regalo: plataformaInfo?.tarjeta_de_regalo ?? null,
        por_pantalla,
        por_acceso,
        plat_correo_cliente: correo_cliente_flag,
        plat_clave_cliente: clave_cliente_flag,
        plan,
        id_venta: row.id_venta,
        nombre_cliente: row.nombre_cliente || "",
        id_precio: row.id_precio || null,
        id_plataforma: plataformaId,
        id_cuenta:
          memberId || row.id_cuenta || row.id_cuenta_miembro || row.cuentas?.id_cuenta || null,
        id_perfil: row.id_perfil || row.perfiles?.id_perfil || null,
        correo: memberCuenta?.correo || cuentaData?.correo || "",
        correo_cliente: row.correo_miembro || "",
        clave: memberCuenta?.clave || cuentaData?.clave || "",
        n_perfil: row.perfiles?.n_perfil ?? null,
        pin:
          row.perfiles?.pin ??
          row.tarjetas_de_regalo?.pin ??
          memberCuenta?.pin ??
          cuentaData?.pin ??
          null,
        perfil_hogar: row.perfiles?.perfil_hogar ?? null,
        fecha_corte: row.fecha_corte,
        fecha_corte_venta: row.fecha_corte,
        venta_perfil: row.cuentas?.venta_perfil ?? row.cuentas_miembro?.venta_perfil,
        venta_miembro: row.cuentas?.venta_miembro ?? row.cuentas_miembro?.venta_miembro,
        completa: isCompleta,
        sub_cuenta,
      };
    });

    const grouped = items.reduce((acc, item) => {
      const key = item.plataforma || "Sin plataforma";
      if (!acc[key]) {
        acc[key] = {
          color_1: item.color_1,
          color_2: item.color_2,
          id_plataforma: item.id_plataforma,
          usa_pines: item.usa_pines,
          tarjeta_de_regalo: item.tarjeta_de_regalo,
          por_pantalla: item.por_pantalla,
          por_acceso: item.por_acceso,
          plat_correo_cliente: item.plat_correo_cliente,
          plat_clave_cliente: item.plat_clave_cliente,
          color_3: item.color_3,
          planes: {},
        };
      }
      if (!acc[key].planes[item.plan]) {
        acc[key].planes[item.plan] = {
          plan: item.plan,
          completa: isTrue(item.completa),
          ventas: [],
        };
      }
      acc[key].planes[item.plan].completa =
        acc[key].planes[item.plan].completa || isTrue(item.completa);
      acc[key].planes[item.plan].ventas.push(item);
      return acc;
    }, {});

    const plataformas = Object.entries(grouped).map(([nombre, payload]) => ({
      nombre,
      color_1: payload.color_1 || null,
      color_2: payload.color_2 || null,
      color_3: payload.color_3 || null,
      id_plataforma: payload.id_plataforma || null,
      usa_pines: payload.usa_pines ?? null,
      tarjeta_de_regalo: payload.tarjeta_de_regalo ?? null,
      por_pantalla: payload.por_pantalla ?? null,
      por_acceso: payload.por_acceso ?? null,
      plat_correo_cliente: payload.plat_correo_cliente ?? null,
      plat_clave_cliente: payload.plat_clave_cliente ?? null,
      planes: Object.values(payload.planes).map((entry) => ({
        plan: entry.plan,
        completa: entry.completa,
        ventas: entry.ventas,
      })),
    }));

    res.json({ plataformas });
  } catch (err) {
    console.error("[inventario] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/admin/ventas/entregar-giftcard", async (req, res) => {
  try {
    await requireAdminSession(req);
    const adminId = requireSessionUserId(req);
    const idVenta = toPositiveInt(req.body?.id_venta);
    if (!idVenta) {
      return res.status(400).json({ error: "id_venta inválido" });
    }
    const result = await deliverPendingGiftCardVenta({
      idVenta,
      adminInvolucradoId: adminId,
      source: "manual_admin",
    });
    return res.json(result);
  } catch (err) {
    console.error("[admin/ventas/entregar-giftcard] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    if (err?.code === "GIFT_CARD_SALE_NOT_FOUND") {
      return res.status(404).json({ error: err.message });
    }
    if (err?.code === "GIFT_CARD_NOT_FOUND") {
      return res.status(404).json({ error: err.message });
    }
    if (
      err?.code === "GIFT_CARD_SALE_NOT_PENDING" ||
      err?.code === "GIFT_CARD_STOCK_EMPTY" ||
      err?.code === "GIFT_CARD_NOT_FOR_SALE" ||
      err?.code === "GIFT_CARD_ALREADY_USED" ||
      err?.code === "GIFT_CARD_PLATFORM_MISMATCH" ||
      err?.code === "GIFT_CARD_AMOUNT_MISMATCH" ||
      err?.code === "GIFT_CARD_STOCK_UPDATE_CONFLICT"
    ) {
      return res.status(409).json({ error: err.message });
    }
    if (
      err?.code === "INVALID_ID_VENTA" ||
      err?.code === "GIFT_CARD_SALE_NOT_GIFTCARD" ||
      err?.code === "GIFT_CARD_SALE_INVALID_CONTEXT"
    ) {
      return res.status(400).json({ error: err.message });
    }
    return res.status(500).json({ error: err.message });
  }
});

app.get("/api/ventas/orden-por-venta", async (req, res) => {
  try {
    const idVenta = toPositiveInt(req.query?.id_venta);
    if (!idVenta) {
      return res.status(400).json({ error: "id_venta inválido" });
    }
    const idUsuarioSesion = await getOrCreateUsuario(req);
    if (!idUsuarioSesion) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }

    const { data: permRow, error: permErr } = await supabaseAdmin
      .from("usuarios")
      .select("permiso_superadmin")
      .eq("id_usuario", idUsuarioSesion)
      .maybeSingle();
    if (permErr) throw permErr;
    const isSuper = isTrue(permRow?.permiso_superadmin);

    const { data: ventaRow, error: ventaErr } = await supabaseAdmin
      .from("ventas")
      .select("id_venta, id_usuario")
      .eq("id_venta", idVenta)
      .maybeSingle();
    if (ventaErr) throw ventaErr;
    if (!ventaRow?.id_venta) {
      return res.status(404).json({ error: "Venta no encontrada." });
    }

    const idOrden = await resolveOrderIdByVentaId({ idVenta });
    if (!idOrden) {
      return res.status(409).json({ error: "La venta no tiene orden asociada." });
    }

    let ownerId = toPositiveInt(ventaRow?.id_usuario);
    if (!ownerId) {
      const { data: ordenRow, error: ordenErr } = await supabaseAdmin
        .from("ordenes")
        .select("id_orden, id_usuario")
        .eq("id_orden", idOrden)
        .maybeSingle();
      if (ordenErr) throw ordenErr;
      ownerId = toPositiveInt(ordenRow?.id_usuario);
    }

    if (!isSuper && (!ownerId || Number(ownerId) !== Number(idUsuarioSesion))) {
      return res.status(403).json({ error: "La venta no pertenece al usuario." });
    }

    return res.json({ id_venta: idVenta, id_orden: idOrden });
  } catch (err) {
    console.error("[ventas/orden-por-venta] error", err);
    return res.status(500).json({ error: err.message });
  }
});

// Ventas por orden (para entregar_servicios con id_orden)
app.get("/api/ventas/orden", async (req, res) => {
  try {
    const idOrden = Number(req.query?.id_orden);
    if (!Number.isFinite(idOrden)) {
      return res.status(400).json({ error: "id_orden inválido" });
    }
    const idUsuarioSesion = await getOrCreateUsuario(req);
    if (!idUsuarioSesion) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    let isSuper = false;
    const { data: permRow, error: permErr } = await supabaseAdmin
      .from("usuarios")
      .select("permiso_superadmin")
      .eq("id_usuario", idUsuarioSesion)
      .maybeSingle();
    if (permErr) throw permErr;
    isSuper = isTrue(permRow?.permiso_superadmin);
    const { data: ordenRow, error: ordErr } = await supabaseAdmin
      .from("ordenes")
      .select("id_orden, id_usuario")
      .eq("id_orden", idOrden)
      .maybeSingle();
    if (ordErr) throw ordErr;
    if (!ordenRow) {
      return res.status(404).json({ error: "Orden no encontrada" });
    }
    if (!isSuper && Number(ordenRow.id_usuario) !== Number(idUsuarioSesion)) {
      return res.status(403).json({ error: "Orden no pertenece al usuario." });
    }

    const usuarioIdOrden = toPositiveInt(ordenRow?.id_usuario);
    let usuarioRegistrado = null;
    let signupRegistroUrlCorto = "";
    if (usuarioIdOrden) {
      const { data: usuarioOrdenRow, error: usuarioOrdenErr } = await supabaseAdmin
        .from("usuarios")
        .select("id_usuario, fecha_registro, id_auth")
        .eq("id_usuario", usuarioIdOrden)
        .maybeSingle();
      if (usuarioOrdenErr) throw usuarioOrdenErr;
      if (usuarioOrdenRow) {
        usuarioRegistrado = isUsuarioWebRegistrado(usuarioOrdenRow);
        if (!usuarioRegistrado) {
          try {
            signupRegistroUrlCorto = buildSignupShortUrl(usuarioIdOrden);
          } catch (signupUrlErr) {
            console.warn("[ventas/orden] no se pudo generar signup short link", {
              id_orden: idOrden,
              id_usuario: usuarioIdOrden,
              err: signupUrlErr?.message || signupUrlErr,
            });
          }
        }
      }
    }
    console.log("[ventas/orden] request", { id_orden: idOrden });
    const liveVentasRows = await fetchVentasByOrder({
      idOrden,
      select: `
        id_venta,
        meses_contratados,
        renovacion,
        fecha_corte,
        id_perfil,
        id_cuenta_miembro,
        id_precio,
        id_tarjeta_de_regalo,
        pendiente,
        completa,
        mensaje_proveedor,
        correo_miembro,
        clave_miembro,
        cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, clave, pin, id_plataforma, venta_perfil, venta_miembro),
        cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, correo, clave, pin, id_plataforma, id_cuenta_madre),
        perfiles:perfiles(id_perfil, n_perfil, pin, perfil_hogar),
        tarjetas_de_regalo:tarjetas_de_regalo!ventas_id_tarjeta_de_regalo_fkey(id_tarjeta_de_regalo, pin, vendido_a),
        precios:precios(id_precio, id_plataforma, plan, completa, sub_cuenta, region, valor_tarjeta_de_regalo, moneda)
      `,
      ascending: false,
    });

    const liveVentas = Array.isArray(liveVentasRows)
      ? liveVentasRows.map((row) => ({ ...row, id_orden: idOrden }))
      : [];
    const liveVentaIds = new Set(
      liveVentas.map((row) => toPositiveInt(row?.id_venta)).filter((value) => value > 0),
    );

    let fulfilledGiftCardVentas = [];
    if (Number(ordenRow?.id_usuario) > 0) {
      const historialGiftRows = await fetchHistorialGiftCardRowsByOrder(
        idOrden,
        Number(ordenRow.id_usuario),
      );
      fulfilledGiftCardVentas = (historialGiftRows || [])
        .filter((row) => {
          const saleId = toPositiveInt(row?.id_venta);
          return !saleId || !liveVentaIds.has(saleId);
        })
        .map((row) => {
          const matchedPrice = row?.precio_tarjeta_de_regalo || null;
          const fallbackMonto = row?.tarjetas_de_regalo?.monto ?? null;
          const fallbackValue =
            Number.isFinite(Number(fallbackMonto)) && Number(fallbackMonto) > 0
              ? Number(fallbackMonto)
              : fallbackMonto;
          return {
            id_venta: toPositiveInt(row?.id_venta) || null,
            meses_contratados: 1,
            renovacion: isTrue(row?.renovacion),
            fecha_corte: null,
            id_perfil: null,
            id_cuenta_miembro: null,
            id_precio: null,
            id_tarjeta_de_regalo: toPositiveInt(row?.id_tarjeta_de_regalo) || null,
            pendiente: false,
            id_orden: idOrden,
            completa: false,
            mensaje_proveedor: true,
            correo_miembro: "",
            clave_miembro: "",
            cuentas: null,
            cuentas_miembro: null,
            perfiles: null,
            tarjetas_de_regalo: row?.tarjetas_de_regalo || null,
            precios: {
              id_precio: toPositiveInt(matchedPrice?.id_precio) || null,
              id_plataforma: toPositiveInt(row?.id_plataforma) || null,
              plan:
                String(matchedPrice?.plan || row?.plataformas?.nombre || "").trim() || "Gift Card",
              completa: false,
              sub_cuenta: null,
              region: String(matchedPrice?.region || "").trim() || null,
              valor_tarjeta_de_regalo: matchedPrice?.valor_tarjeta_de_regalo ?? fallbackValue ?? null,
              moneda: String(matchedPrice?.moneda || "").trim() || null,
            },
          };
        });
    }

    const ventas = [...liveVentas, ...fulfilledGiftCardVentas].sort((a, b) => {
      const aId = Number(a?.id_venta) || 0;
      const bId = Number(b?.id_venta) || 0;
      return bId - aId;
    });
    console.log("[ventas/orden] result", { id_orden: idOrden, ventas: ventas.length });
    res.json({
      ventas,
      usuario: {
        id_usuario: usuarioIdOrden || null,
        registrado: usuarioRegistrado,
        signup_url_corto: signupRegistroUrlCorto || "",
      },
    });
  } catch (err) {
    console.error("[ventas/orden] error", err);
    res.status(500).json({ error: err.message });
  }
});

// Venta directa (para entregar_servicios con id_venta sin id_orden)
app.get("/api/ventas/entrega-por-venta", async (req, res) => {
  try {
    const idVenta = toPositiveInt(req.query?.id_venta);
    if (!idVenta) {
      return res.status(400).json({ error: "id_venta inválido" });
    }
    const idUsuarioSesion = await getOrCreateUsuario(req);
    if (!idUsuarioSesion) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }

    const { data: permRow, error: permErr } = await supabaseAdmin
      .from("usuarios")
      .select("permiso_superadmin")
      .eq("id_usuario", idUsuarioSesion)
      .maybeSingle();
    if (permErr) throw permErr;
    const isSuper = isTrue(permRow?.permiso_superadmin);

    const { data: ventaRow, error: ventaErr } = await supabaseAdmin
      .from("ventas")
      .select(
        `
        id_venta,
        id_usuario,
        meses_contratados,
        renovacion,
        fecha_corte,
        id_perfil,
        id_cuenta_miembro,
        id_precio,
        id_tarjeta_de_regalo,
        pendiente,
        completa,
        mensaje_proveedor,
        correo_miembro,
        clave_miembro,
        cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, clave, pin, id_plataforma, venta_perfil, venta_miembro),
        cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, correo, clave, pin, id_plataforma, id_cuenta_madre),
        perfiles:perfiles(id_perfil, n_perfil, pin, perfil_hogar),
        tarjetas_de_regalo:tarjetas_de_regalo!ventas_id_tarjeta_de_regalo_fkey(id_tarjeta_de_regalo, pin, vendido_a),
        precios:precios(id_precio, id_plataforma, plan, completa, sub_cuenta, region, valor_tarjeta_de_regalo, moneda)
      `
      )
      .eq("id_venta", idVenta)
      .maybeSingle();
    if (ventaErr) throw ventaErr;
    if (!ventaRow?.id_venta) {
      return res.status(404).json({ error: "Venta no encontrada." });
    }

    let ownerId = toPositiveInt(ventaRow?.id_usuario);
    const orderIdFromSale = await resolveOrderIdByVentaId({ idVenta });
    if (!ownerId && orderIdFromSale) {
      const { data: ordenRow, error: ordenErr } = await supabaseAdmin
        .from("ordenes")
        .select("id_orden, id_usuario")
        .eq("id_orden", orderIdFromSale)
        .maybeSingle();
      if (ordenErr) throw ordenErr;
      ownerId = toPositiveInt(ordenRow?.id_usuario);
    }

    if (!isSuper && (!ownerId || Number(ownerId) !== Number(idUsuarioSesion))) {
      return res.status(403).json({ error: "La venta no pertenece al usuario." });
    }

    let usuarioRegistrado = null;
    let signupRegistroUrlCorto = "";
    if (ownerId) {
      const { data: usuarioOrdenRow, error: usuarioOrdenErr } = await supabaseAdmin
        .from("usuarios")
        .select("id_usuario, fecha_registro, id_auth")
        .eq("id_usuario", ownerId)
        .maybeSingle();
      if (usuarioOrdenErr) throw usuarioOrdenErr;
      if (usuarioOrdenRow) {
        usuarioRegistrado = isUsuarioWebRegistrado(usuarioOrdenRow);
        if (!usuarioRegistrado) {
          try {
            signupRegistroUrlCorto = buildSignupShortUrl(ownerId);
          } catch (signupUrlErr) {
            console.warn("[ventas/entrega-por-venta] no se pudo generar signup short link", {
              id_venta: idVenta,
              id_usuario: ownerId,
              err: signupUrlErr?.message || signupUrlErr,
            });
          }
        }
      }
    }

    return res.json({
      ventas: [{ ...ventaRow, id_orden: orderIdFromSale || null }],
      usuario: {
        id_usuario: ownerId || null,
        registrado: usuarioRegistrado,
        signup_url_corto: signupRegistroUrlCorto || "",
      },
    });
  } catch (err) {
    console.error("[ventas/entrega-por-venta] error", err);
    return res.status(500).json({ error: err.message });
  }
});

app.post("/api/ventas/proveedor/spotify/notificar-pendientes", async (req, res) => {
  try {
    await requireAdminSession(req);
    const idOrden = toPositiveInt(req.body?.id_orden);
    const saleIds = uniqPositiveIds(req.body?.id_ventas || req.body?.sale_ids || []);
    if (!idOrden && !saleIds.length) {
      return res
        .status(400)
        .json({ error: "Debes enviar id_orden o id_ventas para notificar al proveedor." });
    }

    const source = String(req.body?.source || "api_spotify_provider_pending").trim() || "api_spotify_provider_pending";
    const result = await notifySpotifyProviderPendingSales({
      idOrden,
      saleIds,
      providerId: 11,
      source,
    });
    return res.json({ ok: true, ...result });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[ventas/proveedor/spotify/notificar-pendientes] error", err);
    return res.status(500).json({ error: err?.message || "No se pudo notificar al proveedor." });
  }
});

// Detalle de orden (info + items del carrito asociado)
app.get("/api/ordenes/detalle", async (req, res) => {
  try {
    const idOrden = Number(req.query?.id_orden);
    if (!Number.isFinite(idOrden)) {
      return res.status(400).json({ error: "id_orden inválido" });
    }
    const idUsuarioSesion = await getOrCreateUsuario(req);
    if (!idUsuarioSesion) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    const { data: permRow, error: permErr } = await supabaseAdmin
      .from("usuarios")
      .select("permiso_superadmin")
      .eq("id_usuario", idUsuarioSesion)
      .maybeSingle();
    if (permErr) throw permErr;
    const isSuper = isTrue(permRow?.permiso_superadmin);

    const { data: orden, error: ordErr } = await supabaseAdmin
      .from("ordenes")
      .select(
        "id_orden, id_usuario, id_admin, id_carrito, fecha, hora_orden, referencia, total, tasa_bs, monto_bs, pago_verificado, en_espera, orden_cancelada, id_metodo_de_pago, comprobante"
      )
      .eq("id_orden", idOrden)
      .maybeSingle();
    if (ordErr) throw ordErr;
    if (!orden) {
      return res.status(404).json({ error: "Orden no encontrada" });
    }
    if (!isSuper && Number(orden.id_usuario) !== Number(idUsuarioSesion)) {
      return res.status(403).json({ error: "Orden no pertenece al usuario." });
    }

    let usuario = null;
    if (orden.id_usuario) {
      const { data: usuarioRow, error: usuarioErr } = await supabaseAdmin
        .from("usuarios")
        .select("id_usuario, nombre, apellido")
        .eq("id_usuario", orden.id_usuario)
        .maybeSingle();
      if (usuarioErr) throw usuarioErr;
      usuario = usuarioRow || null;
    }

    let metodoPago = null;
    if (orden.id_metodo_de_pago) {
      const { data: metodoRow, error: metodoErr } = await supabaseAdmin
        .from("metodos_de_pago")
        .select(
          "id_metodo_de_pago, nombre, correo, id, cedula, telefono, bolivares, verificacion_automatica",
        )
        .eq("id_metodo_de_pago", orden.id_metodo_de_pago)
        .maybeSingle();
      if (metodoErr) throw metodoErr;
      metodoPago = metodoRow || null;
    }

    let items = [];
    let itemsSource = "none";
    const { data: ordenesItems, error: ordenesItemsErr } = await supabaseAdmin
      .from("ordenes_items")
      .select(
        "id_item_orden, id_orden, id_plataforma, id_venta, renovacion, detalle, monto_usd, monto_bs, plataformas:plataformas(nombre, imagen, tarjeta_de_regalo)",
      )
      .eq("id_orden", idOrden)
      .order("id_item_orden", { ascending: true });
    if (ordenesItemsErr) throw ordenesItemsErr;
    if (Array.isArray(ordenesItems) && ordenesItems.length) {
      items = ordenesItems;
      itemsSource = "ordenes_items";
    } else if (orden.id_carrito) {
      const { data: cartItems, error: itemErr } = await supabaseAdmin
        .from("carrito_items")
        .select("id_item, id_precio, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
        .eq("id_carrito", orden.id_carrito);
      if (itemErr) throw itemErr;
      items = cartItems || [];
      itemsSource = "carrito_items";
    }

    res.json({ orden, items, items_source: itemsSource, usuario, metodo_pago: metodoPago });
  } catch (err) {
    console.error("[ordenes/detalle] error", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin: importar cuentas (CSV)
app.post("/api/admin/import-cuentas", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }

  const boolVal = (v) => {
    if (typeof v === "boolean") return v;
    const s = String(v || "").trim().toLowerCase();
    if (!s) return false;
    return s === "true";
  };
  const toDate = (value) => {
    if (!value) return null;
    const d = new Date(value);
    return Number.isNaN(d.valueOf()) ? null : d.toISOString().slice(0, 10);
  };

  try {
    await requireAdminSession(req);

    const normalized = rows
      .map((r) => {
        const correo = (r.correo || "").trim().toLowerCase();
        const id_plataforma = Number(r.id_plataforma);
        const id_proveedor =
          r.id_proveedor === null || r.id_proveedor === undefined || r.id_proveedor === ""
            ? null
            : Number(r.id_proveedor);
        return {
          correo,
          clave: (r.clave || "").trim() || null,
          fecha_corte: toDate(r.fecha_corte),
          fecha_pagada: toDate(r.fecha_pagada),
          inactiva: boolVal(r.inactiva),
          ocupado: boolVal(r.ocupado),
          id_plataforma: Number.isFinite(id_plataforma) ? id_plataforma : null,
          id_proveedor: Number.isFinite(id_proveedor) ? id_proveedor : null,
          region: (r.region || "").trim() || null,
          correo_codigo: (r.correo_codigo || "").trim() || null,
          clave_codigo: (r.clave_codigo || "").trim() || null,
          link_codigo: (r.link_codigo || "").trim() || null,
          pin_codigo: (r.pin_codigo || "").trim() || null,
          instaddr: boolVal(r.instaddr),
          venta_perfil: boolVal(r.venta_perfil),
          venta_miembro: boolVal(r.venta_miembro),
        };
      })
      .filter((r) => r.correo && Number.isFinite(r.id_plataforma));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const correos = [...new Set(normalized.map((r) => r.correo))];
    const { data: cuentasExist, error: cuentasErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo")
      .in("correo", correos);
    if (cuentasErr) throw cuentasErr;
    const cuentaByCorreo = (cuentasExist || []).reduce((acc, c) => {
      acc[c.correo?.toLowerCase?.()] = c.id_cuenta;
      return acc;
    }, {});

    const mergedByCorreo = new Map();
    normalized.forEach((r) => {
      const id_cuenta = cuentaByCorreo[r.correo] || null;
      const current = mergedByCorreo.get(r.correo) || {};
      mergedByCorreo.set(r.correo, { ...current, ...r, id_cuenta });
    });

    const upsertRows = Array.from(mergedByCorreo.values()).map((r) => ({
      ...r,
      id_cuenta: r.id_cuenta || undefined,
    }));

    if (!upsertRows.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const { error: upsertErr } = await supabaseAdmin
      .from("cuentas")
      .upsert(upsertRows, { onConflict: "id_cuenta" });
    if (upsertErr) throw upsertErr;

    const nuevas = upsertRows.filter((r) => !r.id_cuenta).length;
    const actualizadas = upsertRows.length - nuevas;
    const plataformasProcesadas = uniqPositiveIds(upsertRows.map((r) => r.id_plataforma));
    let autoAsignadasReportadas = {
      scanned: 0,
      resolved: 0,
      skipped: 0,
      errors: 0,
    };
    try {
      autoAsignadasReportadas = await autoAssignReportedPendingVentas({
        plataformaIds: plataformasProcesadas,
      });
    } catch (autoAssignErr) {
      console.error("[admin:import-cuentas] auto assign reportadas error", autoAssignErr);
    }

    res.json({
      ok: true,
      cuentas: upsertRows.length,
      nuevas,
      actualizadas,
      ventas_reportadas_revisadas: autoAsignadasReportadas.scanned,
      ventas_reportadas_asignadas: autoAsignadasReportadas.resolved,
      ventas_reportadas_omitidas: autoAsignadasReportadas.skipped,
      ventas_reportadas_errores: autoAsignadasReportadas.errors,
    });
  } catch (err) {
    console.error("[admin:import-cuentas] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Admin: importar clientes (ventas y perfiles ocupados)
app.post("/api/admin/import-clientes", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }

  const boolVal = (v) =>
    v === true || v === "true" || v === "1" || v === 1 || v === "t" || v === "on";

  try {
    await requireAdminSession(req);

    const makeNameKey = (nombre, apellido) =>
      `${(nombre || "").trim().toLowerCase()}|${(apellido || "").trim().toLowerCase()}`;

    const normalized = rows
      .map((r) => {
        const correo = (r.correo || "").trim().toLowerCase();
        const rawVenta = (r.id_venta || "").toString().replace(/#/g, "").trim();
        const id_venta = rawVenta ? Number(rawVenta) : null;
        const n_perfil =
          r.n_perfil === "" || r.n_perfil === null || r.n_perfil === undefined
            ? null
            : Number(r.n_perfil);
        const fecha_corte_str = (r.fecha_corte || "").trim();
        const fecha_corte =
          /^\d{4}-\d{2}-\d{2}$/.test(fecha_corte_str) ? fecha_corte_str : null;
        const suspendido = boolVal(r.suspendido);
        const meses_contratados =
          r.meses_contratados === "" || r.meses_contratados === null || r.meses_contratados === undefined
            ? null
            : Number(r.meses_contratados);
        const corte_reemplazo = boolVal(r.corte_reemplazo);
        const nombreCompleto = (r.nombre || "").trim();
        const [nombre, ...resto] = nombreCompleto.split(/\s+/);
        const apellido = resto.join(" ").trim() || null;

        return {
          correo,
          id_venta: Number.isNaN(id_venta) ? null : id_venta,
          n_perfil: Number.isInteger(n_perfil) ? n_perfil : null,
          fecha_corte,
          suspendido,
          meses_contratados: Number.isFinite(meses_contratados) ? meses_contratados : null,
          corte_reemplazo,
          nombre: nombre || null,
          apellido,
        };
      })
      .filter((r) => r.correo && r.n_perfil !== null);

    console.log("[import-clientes] normalized sample", normalized.slice(0, 5));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const correos = [...new Set(normalized.map((r) => r.correo))];
    const { data: cuentas, error: cuentasErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo")
      .in("correo", correos);
    if (cuentasErr) throw cuentasErr;
    const cuentaByCorreo = (cuentas || []).reduce((acc, c) => {
      acc[c.correo?.toLowerCase?.()] = c.id_cuenta;
      return acc;
    }, {});

    const rowsWithCuenta = normalized.map((r) => ({
      ...r,
      id_cuenta: cuentaByCorreo[r.correo] || null,
      name_key: makeNameKey(r.nombre, r.apellido),
    }));

    const cuentasIds = [...new Set(rowsWithCuenta.map((r) => r.id_cuenta).filter(Boolean))];

    const nameKeys = Array.from(
      new Set(rowsWithCuenta.map((r) => r.name_key).filter((k) => k && k !== "|"))
    );

    let usuarioByName = {};
    if (nameKeys.length) {
      // Trae todos los usuarios y normaliza a llave nombre|apellido en minúsculas
      const { data: usuariosExist, error: usrErr } = await supabaseAdmin
        .from("usuarios")
        .select("id_usuario, nombre, apellido");
      if (usrErr) throw usrErr;
      usuarioByName = (usuariosExist || []).reduce((acc, u) => {
        const key = makeNameKey(u.nombre, u.apellido);
        if (key && key !== "|") acc[key] = u.id_usuario;
        return acc;
      }, {});
    }

    const nuevosUsuarios = [];
    const newKeys = new Set();
    rowsWithCuenta.forEach((r) => {
      if (
        !r.id_cuenta ||
        !r.name_key ||
        r.name_key === "|" ||
        usuarioByName[r.name_key] ||
        newKeys.has(r.name_key)
      ) {
        return;
      }
      newKeys.add(r.name_key);
      nuevosUsuarios.push({
        nombre: r.nombre || "Cliente",
        apellido: r.apellido,
      });
    });

    if (nuevosUsuarios.length) {
      const { data: insertedUsers, error: insUsrErr } = await supabaseAdmin
        .from("usuarios")
        .insert(nuevosUsuarios)
        .select("id_usuario, nombre, apellido");
      if (insUsrErr) throw insUsrErr;
      (insertedUsers || []).forEach((u) => {
        const key = makeNameKey(u.nombre, u.apellido);
        usuarioByName[key] = u.id_usuario;
      });
    }

    const { data: perfiles, error: perfErr } = await supabaseAdmin
      .from("perfiles")
      .select("id_perfil, id_cuenta, n_perfil")
      .in("id_cuenta", cuentasIds);
    if (perfErr) throw perfErr;
    const perfilMap = {};
    (perfiles || []).forEach((p) => {
      if (!perfilMap[p.id_cuenta]) perfilMap[p.id_cuenta] = {};
      perfilMap[p.id_cuenta][p.n_perfil] = p.id_perfil;
    });

    const perfilesToUpdate = new Set();
    const ventasToUpsert = [];

    rowsWithCuenta.forEach((r) => {
      if (!r.id_cuenta) return;
      const id_usuario = usuarioByName[r.name_key] || null;
      const id_perfil = perfilMap[r.id_cuenta]?.[r.n_perfil] || null;
      if (id_perfil) perfilesToUpdate.add(id_perfil);

      // Solo procesa filas con id_venta; upsert sin tocar fecha_pago
      if (r.id_venta) {
        ventasToUpsert.push({
          id_venta: r.id_venta,
          id_usuario,
          id_cuenta: r.id_cuenta,
          id_perfil,
          fecha_corte: r.fecha_corte || null,
          suspendido: r.suspendido,
          meses_contratados: r.meses_contratados,
          corte_reemplazo: r.corte_reemplazo,
          fecha_pago: null, // no llenar fecha_pago desde importación
        });
      }
    });

    // Upsert ventas con id_venta (sin tocar fecha_pago)
    if (ventasToUpsert.length) {
      console.log(
        "[import-clientes] ventasToUpsert sample",
        ventasToUpsert.slice(0, 5).map((v) => ({
          id_venta: v.id_venta,
          fecha_corte: v.fecha_corte,
          id_cuenta: v.id_cuenta,
          id_perfil: v.id_perfil,
        }))
      );
      const { error: ventaErr } = await supabaseAdmin
        .from("ventas")
        .upsert(ventasToUpsert, { onConflict: "id_venta" });
      if (ventaErr) throw ventaErr;
    }

    if (perfilesToUpdate.size) {
      const { error: updPerfErr } = await supabaseAdmin
        .from("perfiles")
        .update({ ocupado: true })
        .in("id_perfil", Array.from(perfilesToUpdate));
      if (updPerfErr) throw updPerfErr;
    }

    res.json({
      ok: true,
      ventas: ventasToUpsert.length,
      perfiles_ocupados: perfilesToUpdate.size,
      usuarios: Object.keys(usuarioByName).length,
    });
  } catch (err) {
    console.error("[admin:import-clientes] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Importar fechas: actualizar fecha_corte de ventas a partir de CSV (id_venta, fecha_corte)
app.post("/api/admin/import-fechas", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }
  try {
    await requireAdminSession(req);
    const normalized = rows
      .map((r) => {
        const rawVenta = (r.id_venta || "").toString().replace(/#/g, "").trim();
        const id_venta = rawVenta ? Number(rawVenta) : null;
        const fecha_corte_str = (r.fecha_corte || "").trim();
        const fecha_corte =
          /^\d{4}-\d{2}-\d{2}$/.test(fecha_corte_str) ? fecha_corte_str : null;
        return {
          id_venta: Number.isFinite(id_venta) && id_venta > 0 ? id_venta : null,
          fecha_corte,
        };
      })
      .filter((r) => r.id_venta && r.fecha_corte);

    console.log("[import-fechas] normalized sample", normalized.slice(0, 5));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas" });
    }

    const updates = normalized.map((r) =>
      supabaseAdmin.from("ventas").update({ fecha_corte: r.fecha_corte }).eq("id_venta", r.id_venta)
    );
    const results = await Promise.all(updates);
    const errUpd = results.find((r) => r?.error);
    if (errUpd?.error) throw errUpd.error;

    res.json({ ok: true, actualizadas: normalized.length });
  } catch (err) {
    console.error("[admin:import-fechas] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Admin: importar pines de perfiles desde CSV parseado en frontend
app.post("/api/admin/import-pines", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }

  const boolVal = (v) =>
    v === true || v === "true" || v === "1" || v === 1 || v === "t" || v === "on";

  try {
    await requireAdminSession(req);

    const normalized = rows
      .map((r) => ({
        correo: (r.correo || "").trim().toLowerCase(),
        n_perfil:
          r.n_perfil === null || r.n_perfil === undefined || r.n_perfil === ""
            ? null
            : Number(r.n_perfil),
        pin: (() => {
          if (r.pin === null || r.pin === undefined || r.pin === "") return null;
          const num = Number(r.pin);
          return Number.isNaN(num) || num < -32768 || num > 32767 ? null : num;
        })(),
        perfil_hogar: boolVal(r.perfil_hogar),
        ocupado: boolVal(r.ocupado),
      }))
      .filter((r) => r.correo);
    console.log("[import-pines] normalized sample", normalized.slice(0, 5));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const correos = [...new Set(normalized.map((r) => r.correo))];
    const { data: cuentas, error: cuentasErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo")
      .in("correo", correos);
    if (cuentasErr) throw cuentasErr;

    const cuentaByCorreo = (cuentas || []).reduce((acc, c) => {
      acc[c.correo?.toLowerCase?.()] = c.id_cuenta;
      return acc;
    }, {});

    const rowsWithCuenta = normalized.map((r) => ({
      ...r,
      id_cuenta: cuentaByCorreo[r.correo] || null,
    }));
    console.log("[import-pines] rowsWithCuenta sample", rowsWithCuenta.slice(0, 5));
    const sinCuenta = rowsWithCuenta.filter((r) => !r.id_cuenta).map((r) => r.correo);

    // Inserta todo en perfiles, enlazando por correo/id_cuenta; se ignora lógica especial de perfil_hogar
    const perfilesToInsert = rowsWithCuenta
      .filter((r) => r.id_cuenta)
      .map((r) => ({
        id_cuenta: r.id_cuenta,
        n_perfil: Number.isInteger(r.n_perfil) ? r.n_perfil : null,
        pin: r.pin,
        perfil_hogar: !!r.perfil_hogar,
        ocupado: r.ocupado || false,
      }));

    if (perfilesToInsert.length) {
      const { error: insertErr } = await supabaseAdmin.from("perfiles").insert(perfilesToInsert);
      if (insertErr) throw insertErr;
    }

    console.log("[import-pines] inserted perfiles", perfilesToInsert.length);

    res.json({
      ok: true,
      perfiles_insertados: perfilesToInsert.length,
      sin_cuenta: [...new Set(sinCuenta)],
    });
  } catch (err) {
    console.error("[admin:import-pines] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Admin: importar contactos (usuario, telefono)
app.post("/api/admin/import-contactos", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }

  const sanitizeValue = (val) =>
    String(val ?? "")
      .replace(/[\u200B-\u200D\u2060\uFEFF]/g, "")
      .replace(/\u00A0/g, " ")
      .trim();
  const normalizeHeader = (val) =>
    sanitizeValue(val)
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  const pickRowValue = (row, aliases = []) => {
    if (!row || typeof row !== "object") return "";
    for (const alias of aliases) {
      const direct = sanitizeValue(row[alias]);
      if (direct) return direct;
    }
    const aliasKeys = new Set(aliases.map((a) => normalizeHeader(a)));
    for (const [key, value] of Object.entries(row)) {
      if (!aliasKeys.has(normalizeHeader(key))) continue;
      const parsed = sanitizeValue(value);
      if (parsed) return parsed;
    }
    return "";
  };
  const stripClienteSuffix = (val) => {
    let out = sanitizeValue(val).replace(/\s+/g, " ");
    if (!out) return "";
    let prev = "";
    while (out && out !== prev) {
      prev = out;
      out = out
        .replace(/\s*[-–—|]?\s*cliente\s*moose\s*[+＋﹢]?\s*$/i, "")
        .replace(/\s*[-–—|]?\s*moose\s*[+＋﹢]?\s*$/i, "")
        .replace(/\s*[-–—|]?\s*cliente\s*$/i, "")
        .trim();
    }
    return out;
  };
  const normalizeFullName = (val) =>
    sanitizeValue(val)
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9\s]/gi, " ")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
  const makeNameKey = (nombre, apellido) =>
    normalizeFullName(`${(nombre || "").trim()} ${(apellido || "").trim()}`.trim());

  try {
    await requireAdminSession(req);

    const normalized = rows
      .map((r) => {
        const rawUser = pickRowValue(r, [
          "usuario",
          "nombre",
          "nombre completo",
          "name",
          "full name",
          "full_name",
          "display name",
          "display_name",
        ]);
        const telefono = pickRowValue(r, [
          "telefono",
          "teléfono",
          "phone",
          "phone 1 - value",
          "phone 1 value",
          "phone1",
          "mobile",
          "celular",
          "numero",
          "número",
        ]);
        if (!rawUser || !telefono) return null;
        const cleaned = stripClienteSuffix(rawUser);
        if (!cleaned) return null;
        const fullName = normalizeFullName(cleaned);
        if (!fullName) return null;
        return {
          full_name: fullName,
          telefono: sanitizeValue(telefono).replace(/^['"]+|['"]+$/g, ""),
          name_key: fullName,
        };
      })
      .filter(Boolean);

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const { data: usuariosExist, error: usrErr } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, nombre, apellido");
    if (usrErr) throw usrErr;
    const usuariosList = (usuariosExist || [])
      .map((u) => ({
        id_usuario: u.id_usuario,
        name_key: makeNameKey(u.nombre, u.apellido),
      }))
      .filter((u) => u.name_key);
    const usuarioByName = usuariosList.reduce((acc, u) => {
      acc[u.name_key] = u.id_usuario;
      return acc;
    }, {});

    const findUserIdByName = (nameKey) => {
      const exact = usuarioByName[nameKey];
      if (exact) return exact;
      let best = null;
      for (const u of usuariosList) {
        if (
          nameKey === u.name_key ||
          nameKey.startsWith(`${u.name_key} `) ||
          u.name_key.startsWith(`${nameKey} `)
        ) {
          if (!best || u.name_key.length > best.name_key.length) {
            best = u;
          }
        }
      }
      return best?.id_usuario || null;
    };

    const updatesById = new Map();
    let faltantes = 0;
    normalized.forEach((r) => {
      const id = findUserIdByName(r.name_key);
      if (!id) {
        faltantes += 1;
        console.warn("[admin:import-contactos] sin match", {
          usuario: r.full_name,
          telefono: r.telefono,
        });
        return;
      }
      updatesById.set(id, r.telefono);
    });

    const updates = Array.from(updatesById.entries()).map(([id_usuario, telefono]) => ({
      id_usuario,
      telefono,
    }));

    if (!updates.length) {
      return res.json({
        ok: true,
        filas: rows.length,
        actualizados: 0,
        faltantes,
      });
    }

    let actualizados = 0;
    const chunkSize = 50;
    for (let i = 0; i < updates.length; i += chunkSize) {
      const batch = updates.slice(i, i + chunkSize);
      const results = await Promise.all(
        batch.map(({ id_usuario, telefono }) =>
          supabaseAdmin
            .from("usuarios")
            .update({ telefono })
            .eq("id_usuario", id_usuario)
            .select("id_usuario")
        )
      );
      results.forEach((r) => {
        if (r.error) throw r.error;
        actualizados += (r.data || []).length;
      });
    }

    const updatedUserIds = updates.map(({ id_usuario }) => id_usuario);
    res.json({
      ok: true,
      filas: rows.length,
      actualizados,
      faltantes,
    });

    setImmediate(() => {
      attemptWhatsappRecordatoriosForUsersOnPhoneUpdate(updatedUserIds, {
        source: "phone_update_import",
      }).catch((triggerErr) => {
        console.error("[admin:import-contactos] trigger recordatorio batch error", {
          totalUsuarios: updatedUserIds.length,
          err: triggerErr,
        });
      });
    });
  } catch (err) {
    console.error("[admin:import-contactos] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Checkout: crea orden (y procesa ventas si ya está verificado)
app.post("/api/checkout", async (req, res) => {
  const {
    id_orden,
    id_metodo_de_pago,
    referencia,
    comprobantes,
    comprobante,
    tasa_bs,
    monto_transferido,
    id_usuario_override,
    bypass_verificacion,
    id_admin,
    custom_item_amounts,
    historial_registrado_por_origen,
  } = req.body || {};
  const archivos = Array.isArray(comprobantes) ? comprobantes : Array.isArray(comprobante) ? comprobante : [];
  if (!id_metodo_de_pago || !referencia || !Array.isArray(archivos)) {
    return res
      .status(400)
      .json({ error: "id_metodo_de_pago, referencia y comprobante(s) son requeridos en formato válido" });
  }

  try {
    const idUsuarioSesion = await getOrCreateUsuario(req);
    const sessionUserId = parseSessionUserId(req) || idUsuarioSesion;
    const adminFromBody = Number.isFinite(Number(id_admin)) ? Number(id_admin) : null;
    const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
    const bypassRequested = isTrue(bypass_verificacion);
    const hasOverride =
      id_usuario_override && Number.isFinite(Number(id_usuario_override));
    const needsAdminCheck = Boolean(bypassRequested || hasOverride || adminFromBody);
    let sessionIsSuper = false;
    let adminCandidate = sessionUserId || adminFromBody;
    if (needsAdminCheck) {
      if (!adminCandidate) {
        return res.status(403).json({ error: "Acceso denegado" });
      }
      const { data: permRow, error: permErr } = await supabaseAdmin
        .from("usuarios")
        .select("permiso_superadmin")
        .eq("id_usuario", adminCandidate)
        .maybeSingle();
      if (permErr) throw permErr;
      sessionIsSuper = isTrue(permRow?.permiso_superadmin);
    }
    if (hasOverride && !sessionIsSuper) {
      return res.status(403).json({ error: "Solo superadmin puede crear órdenes para otros usuarios" });
    }
    if (bypassRequested && !sessionIsSuper) {
      return res.status(403).json({ error: "No autorizado para omitir verificación" });
    }
    const bypassVerificacion = sessionIsSuper || bypassRequested;
    const registraHistorialDesdeCuentaNueva =
      String(historial_registrado_por_origen || "")
        .trim()
        .toLowerCase() === "cuenta_nueva" &&
      hasOverride &&
      sessionIsSuper;
    const historialRegistradoPor = registraHistorialDesdeCuentaNueva ? idUsuarioSesion : null;
    const customItemAmountMap = normalizeCheckoutCustomItemAmounts(custom_item_amounts);
    const hasCustomItemAmounts = customItemAmountMap.size > 0;
    const adminInvolucradoGiftId =
      sessionIsSuper && Number.isFinite(Number(adminCandidate)) && Number(adminCandidate) > 0
        ? Number(adminCandidate)
        : null;
    const idUsuarioVentas =
      hasOverride ? Number(id_usuario_override)
        : idUsuarioSesion;
    const carritoId = await getCurrentCarrito(idUsuarioSesion);
    if (!carritoId) return res.status(400).json({ error: "No hay carrito activo" });
    console.log("[checkout] session", {
      idUsuarioSesion,
      sessionUserId,
      adminFromBody,
      adminCandidate,
      sessionIsSuper,
      bypassRequested,
      bypassVerificacion,
      idUsuarioVentas,
      id_metodo_de_pago,
      referencia,
      carritoId,
    });

    const context = await buildCheckoutContext({
      idUsuarioVentas,
      carritoId,
      totalCliente: null,
    });
    const {
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
      total,
      tasaBs,
    } = context;
    if (!items?.length) {
      return res.status(400).json({ error: "El carrito está vacío" });
    }
    const itemIds = new Set((items || []).map((item) => toPositiveInt(item?.id_item)).filter(Boolean));
    const invalidCustomItemId = [...customItemAmountMap.keys()].find((itemId) => !itemIds.has(itemId));
    if (invalidCustomItemId) {
      return res.status(400).json({ error: `El item ${invalidCustomItemId} no existe en el carrito actual.` });
    }
    const customSummary = hasCustomItemAmounts
      ? await buildCheckoutItemSummaryRows({
          items,
          priceMap,
          platInfoById,
          platNameById,
          pickPrecio,
          descuentos,
          discountColumns,
          discountColumnById,
          isCliente,
          totalUsd: total,
          montoBsTotal: null,
          tasaBs,
          customItemAmountMap,
        })
      : null;
    const totalServidor = Number.isFinite(Number(total)) ? roundCheckoutMoney(total) : 0;
    const referenciaTrim = String(referencia || "").trim();
    let carritoSnapshot = null;
    if (!hasCustomItemAmounts) {
      const { data: carritoSnapshotData, error: carritoSnapshotErr } = await supabaseAdmin
        .from("carritos")
        .select("monto_final, usa_saldo, saldo_usado")
        .eq("id_carrito", carritoId)
        .maybeSingle();
      if (carritoSnapshotErr) throw carritoSnapshotErr;
      carritoSnapshot = carritoSnapshotData || null;
    }
    const usaSaldoCarrito = !hasCustomItemAmounts && isTrue(carritoSnapshot?.usa_saldo);
    const montoFinalCarrito = parseOptionalCheckoutNumber(carritoSnapshot?.monto_final);
    const saldoUsadoCarrito = parseOptionalCheckoutNumber(carritoSnapshot?.saldo_usado);
    let checkoutTotal = hasCustomItemAmounts ? customSummary?.totalUsd || 0 : totalServidor;
    let saldoAplicadoUsd = 0;
    if (!hasCustomItemAmounts && usaSaldoCarrito) {
      if (Number.isFinite(saldoUsadoCarrito) && saldoUsadoCarrito > 0) {
        saldoAplicadoUsd = roundCheckoutMoney(Math.min(totalServidor, saldoUsadoCarrito));
        checkoutTotal = roundCheckoutMoney(Math.max(0, totalServidor - saldoAplicadoUsd));
      } else if (Number.isFinite(montoFinalCarrito)) {
        checkoutTotal = roundCheckoutMoney(montoFinalCarrito);
        saldoAplicadoUsd = roundCheckoutMoney(Math.max(0, totalServidor - checkoutTotal));
      } else {
        const { data: saldoUserRow, error: saldoUserErr } = await supabaseAdmin
          .from("usuarios")
          .select("saldo")
          .eq("id_usuario", idUsuarioVentas)
          .maybeSingle();
        if (saldoUserErr) throw saldoUserErr;
        const saldoDisponible = toFiniteMoney(saldoUserRow?.saldo);
        if (Number.isFinite(saldoDisponible) && saldoDisponible > 0) {
          saldoAplicadoUsd = roundCheckoutMoney(Math.min(totalServidor, saldoDisponible));
        }
        checkoutTotal = roundCheckoutMoney(Math.max(0, totalServidor - saldoAplicadoUsd));
      }
      if (referenciaTrim.toUpperCase() === "SALDO") {
        saldoAplicadoUsd = roundCheckoutMoney(totalServidor);
        checkoutTotal = 0;
      }
    }
    let montoBaseCobrado = checkoutTotal;
    const ordenSaldoUsage = {
      usaSaldo: !!usaSaldoCarrito,
      saldoAplicadoUsd,
      totalBrutoUsd: hasCustomItemAmounts ? roundCheckoutMoney(checkoutTotal) : totalServidor,
    };

    const caracasNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
    const pad2 = (val) => String(val).padStart(2, "0");
    const hora_orden = `${pad2(caracasNow.getHours())}:${pad2(caracasNow.getMinutes())}:${pad2(
      caracasNow.getSeconds()
    )}`;
    const { data: metodoPagoRow, error: metodoPagoErr } = await supabaseAdmin
      .from("metodos_de_pago")
      .select("id_metodo_de_pago, nombre, verificacion_automatica, bolivares")
      .eq("id_metodo_de_pago", id_metodo_de_pago)
      .maybeSingle();
    if (metodoPagoErr) throw metodoPagoErr;
    if (!metodoPagoRow) {
      return res.status(400).json({ error: "Método de pago inválido." });
    }
    const metodoVerificacionAutomatica = isTrue(metodoPagoRow?.verificacion_automatica);
    const metodoEsBolivares = isTrue(metodoPagoRow?.bolivares);
    const metodoEsBinance = await isBinanceMetodoPago(metodoPagoRow);
    const parsedOrderId = Number(id_orden);
    if (metodoVerificacionAutomatica === false && archivos.length === 0) {
      return res.status(400).json({ error: "comprobante es requerido para este método." });
    }
    let binanceUniqueAmountMeta = null;
    if (!metodoEsBolivares && metodoEsBinance && referenciaTrim.toUpperCase() !== "SALDO") {
      binanceUniqueAmountMeta = await resolveUniqueBinanceCheckoutTotal({
        baseTotalUsd: checkoutTotal,
        metodoPagoRow,
        reuseOrderId: parsedOrderId,
      });
      if (binanceUniqueAmountMeta?.applied && Number(binanceUniqueAmountMeta?.total) > 0) {
        checkoutTotal = Number(binanceUniqueAmountMeta.total);
      }
    }
    montoBaseCobrado = checkoutTotal;
    if (hasCustomItemAmounts) {
      ordenSaldoUsage.totalBrutoUsd = roundCheckoutMoney(checkoutTotal);
    }
    console.log("[checkout] carrito items", items);
    console.log("[checkout] precios usados", priceMap);

    const metodoPagoIdNum = Number(id_metodo_de_pago);
    const roundByMethod = (n, metodoId) => {
      const idMetodo = Number(metodoId);
      const decimals = metodoEsBinance && idMetodo !== 3 && idMetodo !== 4 ? 3 : 2;
      const factor = decimals === 3 ? 1000 : 100;
      return Math.round((Number(n) || 0) * factor) / factor;
    };
    const calcularMontoRecibidoReal = (montoTransferidoRaw, metodoId) => {
      const monto = Number(montoTransferidoRaw);
      if (!Number.isFinite(monto) || monto <= 0) return 0;
      const idMetodo = Number(metodoId);
      if (idMetodo === 4) {
        return roundByMethod(monto - (monto * 0.054 + 0.3), metodoId);
      }
      if (idMetodo === 3) {
        return roundByMethod(monto * 0.8, metodoId);
      }
      return roundByMethod(monto, metodoId);
    };
    const montoTransferidoNum = parseFlexibleMoneyNumber(
      String(monto_transferido ?? "")
        .trim()
        .replace(",", "."),
    );
    const montoTransferido =
      !metodoEsBolivares && binanceUniqueAmountMeta?.applied
        ? roundByMethod(checkoutTotal, metodoPagoIdNum)
        : !metodoEsBolivares && Number.isFinite(montoTransferidoNum) && montoTransferidoNum > 0
          ? roundByMethod(montoTransferidoNum, metodoPagoIdNum)
        : null;
    if (!metodoEsBolivares && montoTransferido === null) {
      return res.status(400).json({ error: "monto_transferido es requerido para este método." });
    }
    const requiereVerificacionPago =
      !bypassVerificacion &&
      referenciaTrim.toUpperCase() !== "SALDO" &&
      (Number(id_metodo_de_pago) === 1 || metodoEsBinance);
    const requiereEntregaManual = !bypassVerificacion && metodoVerificacionAutomatica === false;
    const monto_bs =
      Number.isFinite(checkoutTotal) && Number.isFinite(tasaBs)
        ? Math.round(checkoutTotal * tasaBs * 100) / 100
        : null;
    const montoRecibidoReal = calcularMontoRecibidoReal(montoTransferido, metodoPagoIdNum);
    const excedenteTransferido =
      Number.isFinite(montoRecibidoReal) && Number.isFinite(checkoutTotal)
        ? Math.round((montoRecibidoReal - checkoutTotal) * 100) / 100
        : 0;
    const montoMayor = excedenteTransferido > 0;
    const requierePendiente = requiereVerificacionPago || requiereEntregaManual || montoMayor;
    const en_espera = requierePendiente;
    const isBinanceObjective = !metodoEsBolivares && metodoEsBinance && referenciaTrim.toUpperCase() !== "SALDO";
    const montoObjetivoGeneradoEn = isBinanceObjective ? new Date().toISOString() : null;
    const montoObjetivoExpiraEn = isBinanceObjective
      ? new Date(Date.now() + BINANCE_GMAIL_ORDER_WINDOW_MS).toISOString()
      : null;
    const montoUsdtObjetivo = isBinanceObjective && Number.isFinite(checkoutTotal)
      ? Math.round(checkoutTotal * 1000) / 1000
      : null;
    const binanceDecimalSlot = isBinanceObjective
      ? toPositiveInt(binanceUniqueAmountMeta?.step) || 1
      : null;
    console.log("[checkout] contexto", {
      itemsCount: items?.length || 0,
      total: checkoutTotal,
      tasaBs,
      monto_bs,
      monto_transferido: montoTransferido,
      monto_recibido_real: montoRecibidoReal,
      excedente_transferido: excedenteTransferido,
      monto_mayor: montoMayor,
      metodoVerificacionAutomatica,
      metodoEsBinance,
      binance_unique_applied: !!binanceUniqueAmountMeta?.applied,
      binance_unique_step: binanceUniqueAmountMeta?.step || null,
      binance_unique_reused: !!binanceUniqueAmountMeta?.reused,
      monto_usdt_objetivo: montoUsdtObjetivo,
      binance_decimal_slot: binanceDecimalSlot,
      monto_objetivo_expira_en: montoObjetivoExpiraEn,
      requiereVerificacionPago,
      requiereEntregaManual,
      requierePendiente,
      en_espera,
    });

    const checkoutOrderPayload = {
      id_usuario: idUsuarioVentas,
      id_admin:
        sessionIsSuper && Number.isFinite(Number(adminCandidate)) && Number(adminCandidate) > 0
          ? Number(adminCandidate)
          : null,
      total: checkoutTotal,
      tasa_bs: tasaBs,
      monto_bs,
      monto_transferido: montoTransferido,
      monto_usdt_objetivo: montoUsdtObjetivo,
      binance_decimal_slot: binanceDecimalSlot,
      monto_objetivo_generado_en: montoObjetivoGeneradoEn,
      monto_objetivo_expira_en: montoObjetivoExpiraEn,
      monto_mayor: montoMayor,
      id_metodo_de_pago,
      marcado_pago: true,
      referencia,
      comprobante: archivos,
      en_espera,
      hora_orden,
      id_carrito: carritoId,
      pago_verificado: montoMayor ? false : bypassVerificacion && !requiereEntregaManual ? true : false,
      monto_completo: null,
      checkout_finalizado: true,
      aviso_verificacion_manual: false,
      aviso_verificacion_pago: montoMayor ? false : bypassVerificacion && !requiereEntregaManual ? true : false,
      usa_saldo: ordenSaldoUsage.usaSaldo,
      saldo_aplicado_usd: Number.isFinite(Number(ordenSaldoUsage.saldoAplicadoUsd))
        ? Number(ordenSaldoUsage.saldoAplicadoUsd)
        : 0,
      total_bruto_usd: Number.isFinite(Number(ordenSaldoUsage.totalBrutoUsd))
        ? Number(ordenSaldoUsage.totalBrutoUsd)
        : null,
    };
    let ordenId = null;
    if (Number.isFinite(parsedOrderId) && parsedOrderId > 0) {
      const { data: existingOrder, error: existingOrderErr } = await supabaseAdmin
        .from("ordenes")
        .select("id_orden, id_usuario, id_carrito, orden_cancelada")
        .eq("id_orden", parsedOrderId)
        .maybeSingle();
      if (existingOrderErr) throw existingOrderErr;
      const canReuseOrder =
        Number(existingOrder?.id_orden) > 0 &&
        Number(existingOrder?.id_usuario) === Number(idUsuarioVentas) &&
        Number(existingOrder?.id_carrito) === Number(carritoId) &&
        existingOrder?.orden_cancelada !== true;
      if (canReuseOrder) {
        const { data: updatedOrder, error: updOrderErr } = await supabaseAdmin
          .from("ordenes")
          .update(checkoutOrderPayload)
          .eq("id_orden", parsedOrderId)
          .select("id_orden")
          .single();
        if (updOrderErr) throw updOrderErr;
        ordenId = Number(updatedOrder?.id_orden || 0);
        console.log("[checkout] orden reutilizada", { id_orden: ordenId });
      }
    }
    if (!ordenId) {
      const { data: orden, error: ordErr } = await supabaseAdmin
        .from("ordenes")
        .insert(checkoutOrderPayload)
        .select("id_orden")
        .single();
      if (ordErr) throw ordErr;
      ordenId = Number(orden?.id_orden || 0);
      console.log("[checkout] orden creada", {
        id_orden: ordenId,
        en_espera,
        pago_verificado: bypassVerificacion ? true : false,
      });
    }
    if (!ordenId) {
      throw new Error("No se pudo crear/actualizar la orden");
    }

    await syncOrdenItemsSnapshot({
      ordenId,
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
      totalUsd: checkoutTotal,
      montoBsTotal: monto_bs,
      tasaBs,
      customItemAmountMap: hasCustomItemAmounts ? customItemAmountMap : null,
    });

    let nuevoCarritoId = null;
    try {
      nuevoCarritoId = await rotateUserCarritoAfterCheckout({
        idUsuario: idUsuarioSesion,
        previousCarritoId: carritoId,
        source: "checkout",
      });
    } catch (rotateErr) {
      console.error("[checkout] error creando nuevo carrito post-checkout", {
        id_usuario: toPositiveInt(idUsuarioSesion) || null,
        id_carrito_anterior: toPositiveInt(carritoId) || null,
        error: rotateErr?.message || rotateErr,
      });
    }

    if (requierePendiente) {
      if (requiereEntregaManual && Number.isFinite(ordenId) && ordenId > 0) {
        try {
          const notifyResult = await notifyManualVerificationToWhatsappAdmin({
            idOrden: ordenId,
            source: "checkout_manual_payment_method",
          });
          console.log("[checkout] whatsapp verificacion manual", {
            id_orden: ordenId,
            sent: !!notifyResult?.sent,
            skipped: !!notifyResult?.skipped,
            reason: notifyResult?.reason || null,
          });
        } catch (notifyErr) {
          console.error("[checkout] whatsapp verificacion manual error", {
            id_orden: ordenId,
            error: notifyErr?.message || notifyErr,
          });
        }
      }
      return res.json({
        ok: true,
        id_orden: ordenId,
        id_carrito_nuevo: nuevoCarritoId,
        total: checkoutTotal,
        ventas: 0,
        pendiente_verificacion: true,
        entrega_manual: requiereEntregaManual,
      });
    }

    const result = await processOrderFromItems({
      ordenId,
      idUsuarioSesion,
      idUsuarioVentas,
      historialRegistradoPor,
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      descuentos,
      discountColumns,
      discountColumnById,
      isCliente,
      referencia,
      archivos,
      id_metodo_de_pago,
      carritoId,
      montoHistorialTotalOverride: hasCustomItemAmounts ? null : montoBaseCobrado,
      itemAmountMapById: hasCustomItemAmounts ? customItemAmountMap : null,
      adminInvolucradoId: adminInvolucradoGiftId,
      snapshotTotalUsd: checkoutTotal,
      snapshotMontoBsTotal: monto_bs,
      snapshotTasaBs: tasaBs,
      saldoAplicadoExpectedUsd: ordenSaldoUsage.saldoAplicadoUsd,
    });
    console.log("[checkout] procesado", {
      id_orden: ordenId,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
    });

    await supabaseAdmin
      .from("ordenes")
      .update({ pago_verificado: true, en_espera: result.pendientesCount > 0 })
      .eq("id_orden", ordenId);
    try {
      await cleanupClosedOrderCarrito({
        idOrden: ordenId,
        idCarrito: carritoId,
        source: "checkout_verified",
      });
    } catch (cleanupErr) {
      console.error("[checkout] error limpiando carrito de orden cerrada", {
        id_orden: toPositiveInt(ordenId) || null,
        id_carrito: toPositiveInt(carritoId) || null,
        error: cleanupErr?.message || cleanupErr,
      });
    }

    res.json({
      ok: true,
      id_orden: ordenId,
      id_carrito_nuevo: nuevoCarritoId,
      total: checkoutTotal,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
    });
  } catch (err) {
    console.error("[checkout] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "INVALID_CUSTOM_ITEM_AMOUNTS") {
      return res.status(Number(err?.httpStatus) || 400).json({ error: err.message });
    }
    if (err?.code === "INVALID_PRECIO_ID") {
      return res.status(Number(err?.httpStatus) || 400).json({
        error: err?.message || "id_precio inválido en la venta",
        details: err?.details || null,
      });
    }
    if (err?.code === "SALDO_INSUFICIENTE") {
      return res.status(Number(err?.httpStatus) || 409).json({
        error: err?.message || "Saldo insuficiente para completar el checkout.",
      });
    }
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/ordenes/notificar-verificacion-manual", async (req, res) => {
  const idOrden = Number(req.body?.id_orden);
  const source = String(req.body?.source || "frontend_timeout").trim().slice(0, 80);
  if (!Number.isFinite(idOrden) || idOrden <= 0) {
    return res.status(400).json({ error: "id_orden inválido" });
  }

  try {
    const idUsuarioSesion = await getOrCreateUsuario(req);
    const sessionUserId = Number(idUsuarioSesion);
    if (!Number.isFinite(sessionUserId) || sessionUserId <= 0) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }

    const { data: sessionPerms, error: sessionPermErr } = await supabaseAdmin
      .from("usuarios")
      .select("permiso_superadmin")
      .eq("id_usuario", sessionUserId)
      .maybeSingle();
    if (sessionPermErr) throw sessionPermErr;
    const sessionIsSuper = isTrue(sessionPerms?.permiso_superadmin);

    const { data: ordenRow, error: ordenErr } = await supabaseAdmin
      .from("ordenes")
      .select("id_orden, id_usuario, id_admin, pago_verificado, orden_cancelada")
      .eq("id_orden", idOrden)
      .maybeSingle();
    if (ordenErr) throw ordenErr;
    if (!ordenRow?.id_orden) {
      return res.status(404).json({ error: "Orden no encontrada" });
    }

    const orderUserId = Number(ordenRow?.id_usuario);
    const orderAdminId = Number(ordenRow?.id_admin);
    const canAccessOrder =
      sessionIsSuper ||
      (Number.isFinite(orderUserId) && orderUserId > 0 && orderUserId === sessionUserId) ||
      (Number.isFinite(orderAdminId) && orderAdminId > 0 && orderAdminId === sessionUserId);
    if (!canAccessOrder) {
      return res.status(403).json({ error: "Orden no pertenece al usuario." });
    }

    if (isTrue(ordenRow?.pago_verificado)) {
      return res.json({
        ok: true,
        sent: false,
        skipped: true,
        reason: "order_already_verified",
      });
    }
    if (isTrue(ordenRow?.orden_cancelada)) {
      return res.json({
        ok: true,
        sent: false,
        skipped: true,
        reason: "order_cancelled",
      });
    }

    const notifyResult = await notifyManualVerificationToWhatsappAdmin({
      idOrden,
      source,
    });
    return res.json({
      ok: true,
      ...notifyResult,
    });
  } catch (err) {
    console.error("[ordenes/notificar-verificacion-manual] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo enviar notificación de verificación manual" });
  }
});

app.get("/api/ordenes/verificacion-manual/worker-status", async (req, res) => {
  try {
    const hasWorkerToken = hasValidInternalWorkerTriggerToken(req);
    if (!hasWorkerToken) {
      await requireAdminSession(req);
    }
    return res.json({
      ok: true,
      enabled: WHATSAPP_MANUAL_VERIFICATION_WATCHER_ENABLED,
      intervalMs: WHATSAPP_MANUAL_VERIFICATION_WATCHER_INTERVAL_MS,
      windowMs: WHATSAPP_MANUAL_VERIFICATION_WINDOW_MS,
      batch: WHATSAPP_MANUAL_VERIFICATION_WATCHER_BATCH,
      inProgress: manualVerificationWatcherInProgress,
      lastRunAt: manualVerificationWatcherLastRunAt,
      lastResult: manualVerificationWatcherLastResult,
      lastError: manualVerificationWatcherLastError,
      usingWorkerToken: hasWorkerToken,
      workerTokenConfigured: INTERNAL_WORKER_TRIGGER_TOKEN.length > 0,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo consultar estado del worker manual" });
  }
});

app.post("/api/ordenes/verificacion-manual/procesar", async (req, res) => {
  try {
    const hasWorkerToken = hasValidInternalWorkerTriggerToken(req);
    if (!hasWorkerToken) {
      await requireAdminSession(req);
    }
    const idOrden = Number(req.body?.id_orden);
    const force = isTrue(req.body?.force) || isTrue(req.body?.forzar);
    const source = String(req.body?.source || "").trim().slice(0, 80);

    if (Number.isFinite(idOrden) && idOrden > 0) {
      const triggerSource = source || (force ? "manual_trigger_force" : "manual_trigger_single");
      if (!force) {
        const { data: ordenRow, error: ordenErr } = await supabaseAdmin
          .from("ordenes")
          .select(
            "id_orden, fecha, hora_orden, marcado_pago, checkout_finalizado, pago_verificado, orden_cancelada, aviso_verificacion_manual",
          )
          .eq("id_orden", idOrden)
          .maybeSingle();
        if (ordenErr) throw ordenErr;
        if (!ordenRow?.id_orden) {
          return res.status(404).json({ error: "Orden no encontrada" });
        }
        if (!isTrue(ordenRow?.marcado_pago) || !isTrue(ordenRow?.checkout_finalizado)) {
          return res.json({
            ok: true,
            skipped: true,
            reason: "order_not_ready_for_manual_verification",
            id_orden: idOrden,
          });
        }
        if (isTrue(ordenRow?.pago_verificado)) {
          return res.json({
            ok: true,
            skipped: true,
            reason: "order_already_verified",
            id_orden: idOrden,
          });
        }
        if (isTrue(ordenRow?.orden_cancelada)) {
          return res.json({
            ok: true,
            skipped: true,
            reason: "order_cancelled",
            id_orden: idOrden,
          });
        }
        if (isTrue(ordenRow?.aviso_verificacion_manual)) {
          return res.json({
            ok: true,
            skipped: true,
            reason: "already_flagged_manual_verification",
            id_orden: idOrden,
          });
        }
        const windowElapsed = hasManualVerificationWindowElapsed(ordenRow);
        if (windowElapsed !== true) {
          return res.json({
            ok: true,
            skipped: true,
            reason: windowElapsed === null ? "missing_order_datetime" : "window_not_elapsed",
            id_orden: idOrden,
          });
        }
      }

      const notifyResult = await notifyManualVerificationToWhatsappAdmin({
        idOrden,
        source: triggerSource,
        manageWhatsappLifecycle: true,
      });
      return res.json({
        ok: true,
        mode: force ? "single_force" : "single",
        id_orden: idOrden,
        usingWorkerToken: hasWorkerToken,
        ...notifyResult,
      });
    }

    const batchResult = await processPendingManualVerificationAlerts();
    return res.json({
      ok: true,
      mode: "batch",
      usingWorkerToken: hasWorkerToken,
      ...batchResult,
    });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === ADMIN_REQUIRED || err?.message === ADMIN_REQUIRED) {
      return res.status(403).json({ error: "Solo admin/superadmin" });
    }
    console.error("[ordenes/verificacion-manual/procesar] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo procesar verificación manual" });
  }
});

// Procesar orden luego de verificación de pago
app.post("/api/ordenes/procesar", async (req, res) => {
  const idOrden = Number(req.body?.id_orden);
  if (!Number.isFinite(idOrden)) {
    return res.status(400).json({ error: "id_orden inválido" });
  }
  const saldoAFavorRaw = req.body?.saldo_a_favor;
  const saldoAFavorNum = Number(String(saldoAFavorRaw ?? "").trim().replace(",", "."));
  const saldoAFavor =
    saldoAFavorRaw === undefined || saldoAFavorRaw === null || String(saldoAFavorRaw).trim() === ""
      ? 0
      : saldoAFavorNum;
  if (!Number.isFinite(saldoAFavor) || saldoAFavor < 0) {
    return res.status(400).json({ error: "saldo_a_favor inválido. Debe ser mayor o igual a 0." });
  }
  const forceWhatsappPagoVerificado = isTrue(req.body?.force_whatsapp_pago_verificado);
  const orderProcessLock = await acquireSharedOrderProcessLock(idOrden, {
    source: "api_ordenes_procesar",
  });
  if (!orderProcessLock?.acquired) {
    return res.status(409).json({
      error: "La orden ya está siendo procesada. Intenta nuevamente en unos segundos.",
      code: "ORDER_ALREADY_PROCESSING",
      id_orden: toPositiveInt(idOrden),
      lock: orderProcessLock?.lock || null,
    });
  }

  try {
    const idUsuarioSesion = await getOrCreateUsuario(req);
    console.log("[ordenes/procesar] request", { idOrden, idUsuarioSesion });
    let sessionIsSuper = false;
    if (idUsuarioSesion) {
      const { data: permRow, error: permErr } = await supabaseAdmin
        .from("usuarios")
        .select("permiso_superadmin")
        .eq("id_usuario", idUsuarioSesion)
        .maybeSingle();
      if (permErr) throw permErr;
      sessionIsSuper = permRow?.permiso_superadmin === true || permRow?.permiso_superadmin === "true" || permRow?.permiso_superadmin === "1" || permRow?.permiso_superadmin === 1 || permRow?.permiso_superadmin === "t";
    }
    const { data: orden, error: ordErr } = await supabaseAdmin
      .from("ordenes")
      .select(
        "id_orden, id_usuario, id_admin, id_carrito, referencia, comprobante, id_metodo_de_pago, total, total_bruto_usd, usa_saldo, saldo_aplicado_usd, tasa_bs, monto_bs, monto_transferido, monto_mayor, pago_verificado, en_espera, orden_cancelada"
      )
      .eq("id_orden", idOrden)
      .single();
    if (ordErr) throw ordErr;
    console.log("[ordenes/procesar] orden", {
      id_orden: orden?.id_orden,
      id_usuario: orden?.id_usuario,
      id_admin: orden?.id_admin,
      id_carrito: orden?.id_carrito,
      monto_bs: orden?.monto_bs,
      pago_verificado: orden?.pago_verificado,
      en_espera: orden?.en_espera,
      orden_cancelada: orden?.orden_cancelada,
    });

    if (orden?.orden_cancelada === true) {
      return res.status(400).json({ error: "Orden cancelada. No se pueden asignar servicios." });
    }

    const idUsuarioVentas = Number(orden?.id_usuario) || idUsuarioSesion;
    const syncPagoMovilCredito = async () => {
      const syncResult = await markPagoMovilCreditedForOrder({
        orden,
        idUsuario: idUsuarioVentas,
      });
      console.log("[ordenes/procesar] pagomoviles sync", {
        id_orden: idOrden,
        matched: !!syncResult?.matched,
        reason: syncResult?.reason || null,
        pago_id: syncResult?.pago_id || null,
        saldo_acreditado_a: syncResult?.saldo_acreditado_a || null,
      });
      return syncResult;
    };
    const acreditarSaldoManual = async (montoInput) => {
      const amount = Math.round((Number(montoInput) || 0) * 100) / 100;
      if (!(amount > 0)) {
        return { acreditado: false, monto: 0, saldoNuevo: null };
      }
      const targetUserId = Number(idUsuarioVentas);
      if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
        return { acreditado: false, monto: 0, saldoNuevo: null };
      }
      const { data: userRow, error: userErr } = await supabaseAdmin
        .from("usuarios")
        .select("saldo")
        .eq("id_usuario", targetUserId)
        .maybeSingle();
      if (userErr) throw userErr;
      const saldoActual = Number(userRow?.saldo);
      const saldoBase = Number.isFinite(saldoActual) ? saldoActual : 0;
      const saldoNuevo = Math.round((saldoBase + amount) * 100) / 100;
      const { error: updSaldoErr } = await supabaseAdmin
        .from("usuarios")
        .update({ saldo: saldoNuevo })
        .eq("id_usuario", targetUserId);
      if (updSaldoErr) throw updSaldoErr;
      return { acreditado: true, monto: amount, saldoNuevo };
    };
    const processNoItemsOrder = async ({
      montoAuto = 0,
      motivo = "sin_items_relacionados",
    } = {}) => {
      const montoAutoNorm = Math.round((Number(montoAuto) || 0) * 100) / 100;
      const montoManualNorm = Math.round((Number(saldoAFavor) || 0) * 100) / 100;
      const montoTotalAcreditar = Math.max(0, montoAutoNorm) + Math.max(0, montoManualNorm);
      const saldoAcreditadoInfo = await acreditarSaldoManual(montoTotalAcreditar);

      const ordenUpdate = {
        pago_verificado: true,
        en_espera: false,
      };
      if (canAssignDeliverAdmin) {
        ordenUpdate.id_admin = sessionAdminId;
      }
      await supabaseAdmin
        .from("ordenes")
        .update(ordenUpdate)
        .eq("id_orden", idOrden);

      await syncPagoMovilCredito();
      try {
        await cleanupClosedOrderCarrito({
          idOrden: idOrden,
          idCarrito: orden?.id_carrito,
          source: "ordenes_procesar:no_items",
        });
      } catch (cleanupErr) {
        console.error("[ordenes/procesar] cleanup carrito no-items error", {
          id_orden: toPositiveInt(idOrden) || null,
          id_carrito: toPositiveInt(orden?.id_carrito) || null,
          error: cleanupErr?.message || cleanupErr,
        });
      }

      return res.json({
        ok: true,
        id_orden: idOrden,
        ventas: 0,
        pendientes: 0,
        id_admin: idAdminEntrega,
        saldo_acreditado: saldoAcreditadoInfo.acreditado,
        excedente_acreditado: saldoAcreditadoInfo.monto,
        saldo_nuevo: saldoAcreditadoInfo.saldoNuevo,
        sin_items: true,
        motivo_sin_items: motivo,
      });
    };
    const isOrderAdmin =
      idUsuarioSesion && orden?.id_admin && Number(orden.id_admin) === Number(idUsuarioSesion);
    const sessionAdminId = Number.isFinite(Number(idUsuarioSesion)) ? Number(idUsuarioSesion) : null;
    const ordenAdminId = Number.isFinite(Number(orden?.id_admin)) ? Number(orden.id_admin) : null;
    const canAssignDeliverAdmin = sessionIsSuper && sessionAdminId && !ordenAdminId;
    const idAdminEntrega = canAssignDeliverAdmin ? sessionAdminId : ordenAdminId;
    const verifiedByAdminInThisProcess = Boolean(sessionIsSuper || isOrderAdmin || canAssignDeliverAdmin);
    if (
      idUsuarioSesion &&
      orden?.id_usuario &&
      Number(orden.id_usuario) !== Number(idUsuarioSesion) &&
      !sessionIsSuper &&
      !isOrderAdmin
    ) {
      return res.status(403).json({ error: "Orden no pertenece al usuario." });
    }
    const alreadyProcessed = await hasHistorialVentasForOrder(idOrden);
    if (alreadyProcessed) {
      const ventasExist = await fetchVentasByOrder({
        idOrden,
        select: "id_venta",
      });
      const ordenPatch = {};
      let saldoReconciliado = { debitado: false, monto: 0, saldoNuevo: null };
      let saldoReconciliacionError = null;
      const saldoEsperadoOrden = await resolveSaldoAplicadoForOrderVerification({ orden });
      if (!(toFiniteMoney(orden?.saldo_aplicado_usd) > 0) && saldoEsperadoOrden > 0) {
        try {
          const saldoDebitRes = await debitSaldoUsdFromUser({
            idUsuario: idUsuarioVentas,
            montoUsd: saldoEsperadoOrden,
            source: "ordenes_procesar:already_processed",
          });
          saldoReconciliado = {
            debitado: !!saldoDebitRes?.debitado,
            monto: Number(saldoDebitRes?.monto) || 0,
            saldoNuevo: saldoDebitRes?.saldoNuevo ?? null,
          };
          ordenPatch.saldo_aplicado_usd = saldoEsperadoOrden;
        } catch (saldoErr) {
          saldoReconciliacionError = saldoErr?.message || "saldo_reconciliation_failed";
          console.error("[ordenes/procesar] saldo reconciliation error", {
            id_orden: idOrden,
            id_usuario: idUsuarioVentas,
            expected_saldo_usd: saldoEsperadoOrden,
            error: saldoReconciliacionError,
          });
        }
      }
      if (!orden?.pago_verificado) {
        const pendRows = await fetchVentasByOrder({
          idOrden,
          select: "id_venta",
          onlyPending: true,
        });
        ordenPatch.pago_verificado = true;
        ordenPatch.en_espera = (pendRows || []).length > 0;
      }
      if (canAssignDeliverAdmin) {
        ordenPatch.id_admin = sessionAdminId;
      }
      if (Object.keys(ordenPatch).length) {
        await supabaseAdmin
          .from("ordenes")
          .update(ordenPatch)
          .eq("id_orden", idOrden);
      }
      console.log("[ordenes/procesar] ya procesada", { id_orden: idOrden, ventas: ventasExist.length });
      await syncPagoMovilCredito();
      const shouldNotifyPagoVerificado =
        forceWhatsappPagoVerificado === true ||
        ordenPatch.pago_verificado === true ||
        isTrue(orden?.pago_verificado);
      if (shouldNotifyPagoVerificado) {
        try {
          const waDeliveredRes = await notifyVentaEntregadaAfterOrderVerified({
            idOrden,
            manageWhatsappLifecycle: true,
            verifiedByAdmin:
              forceWhatsappPagoVerificado === true ? true : verifiedByAdminInThisProcess,
            forceResend: forceWhatsappPagoVerificado === true,
          });
          if (!waDeliveredRes?.sent) {
            console.log("[ordenes/procesar] whatsapp pago verificado skipped", {
              id_orden: idOrden,
              reason: waDeliveredRes?.reason || "unknown",
              error: waDeliveredRes?.error || null,
            });
          }
        } catch (waDeliveredErr) {
          console.error("[ordenes/procesar] whatsapp pago verificado error", {
            id_orden: idOrden,
            error: waDeliveredErr?.message || waDeliveredErr,
          });
        }
      }
      try {
        await cleanupClosedOrderCarrito({
          idOrden: idOrden,
          idCarrito: orden?.id_carrito,
          source: "ordenes_procesar:already_processed",
        });
      } catch (cleanupErr) {
        console.error("[ordenes/procesar] cleanup carrito already_processed error", {
          id_orden: toPositiveInt(idOrden) || null,
          id_carrito: toPositiveInt(orden?.id_carrito) || null,
          error: cleanupErr?.message || cleanupErr,
        });
      }
      return res.json({
        ok: true,
        id_orden: idOrden,
        already_processed: true,
        ventas: ventasExist.length,
        id_admin: idAdminEntrega,
        saldo_acreditado: false,
        excedente_acreditado: 0,
        saldo_nuevo: null,
        saldo_debitado: saldoReconciliado.debitado,
        saldo_debitado_usd: saldoReconciliado.monto,
        saldo_usuario_nuevo: saldoReconciliado.saldoNuevo,
        saldo_debitado_error: saldoReconciliacionError,
      });
    }
    if (!orden?.id_carrito) {
      console.log("[ordenes/procesar] orden sin carrito; se acredita saldo", {
        id_orden: idOrden,
        total: orden?.total,
      });
      return processNoItemsOrder({
        montoAuto: Number(orden?.total) || 0,
        motivo: "orden_sin_carrito",
      });
    }

    const fallbackContext = await buildCheckoutContext({
      idUsuarioVentas,
      carritoId: orden.id_carrito,
      totalCliente: orden.total,
    });
    const processingSnapshot = await resolveOrderProcessingContextFromSnapshot({
      ordenId: idOrden,
      totalCliente: orden?.total ?? fallbackContext?.total ?? null,
      fallbackContext,
    });
    const context = processingSnapshot?.context || fallbackContext;
    const processingSource = String(processingSnapshot?.source || "carrito_items");
    const processingItemAmountMap =
      processingSnapshot?.itemAmountMapById instanceof Map &&
      processingSnapshot.itemAmountMapById.size > 0
        ? processingSnapshot.itemAmountMapById
        : null;
    const montoBaseCobradoFallback = await resolveMontoBaseCarrito({
      carritoId: orden.id_carrito,
      fallbackTotal: context.total,
    });
    const orderTotalProcesado = parseOptionalCheckoutNumber(orden?.total);
    const montoBaseCobrado = Number.isFinite(orderTotalProcesado)
      ? roundCheckoutMoney(orderTotalProcesado)
      : montoBaseCobradoFallback;
    if (!context.items?.length) {
      console.log("[ordenes/procesar] carrito vacío; se acredita saldo", {
        id_orden: idOrden,
        carritoId: orden?.id_carrito,
        montoBaseCobrado,
        source: processingSource,
      });
      return processNoItemsOrder({
        montoAuto: Number(montoBaseCobrado) || Number(context.total) || 0,
        motivo: "carrito_sin_items",
      });
    }
    console.log("[ordenes/procesar] contexto", {
      id_orden: idOrden,
      itemsCount: context.items?.length || 0,
      total: context.total,
      tasaBs: context.tasaBs,
      source: processingSource,
    });

    const archivos = normalizeFilesArray(orden?.comprobante);
    const result = await processOrderFromItems({
      ordenId: idOrden,
      idUsuarioSesion,
      idUsuarioVentas,
      items: context.items,
      priceMap: context.priceMap,
      platInfoById: context.platInfoById,
      platNameById: context.platNameById,
      pickPrecio: context.pickPrecio,
      descuentos: context.descuentos,
      discountColumns: context.discountColumns,
      discountColumnById: context.discountColumnById,
      isCliente: context.isCliente,
      referencia: orden?.referencia,
      archivos,
      id_metodo_de_pago: orden?.id_metodo_de_pago,
      carritoId: orden.id_carrito,
      montoHistorialTotalOverride: processingItemAmountMap ? null : montoBaseCobrado,
      itemAmountMapById: processingItemAmountMap,
      adminInvolucradoId: idAdminEntrega,
      snapshotTotalUsd: orden?.total ?? context?.total ?? null,
      snapshotMontoBsTotal: orden?.monto_bs ?? null,
      snapshotTasaBs: orden?.tasa_bs ?? context?.tasaBs ?? null,
      saldoAplicadoExpectedUsd: await resolveSaldoAplicadoForOrderVerification({ orden }),
      montoExtraHistorialUsd: saldoAFavor,
      allowSaldoInsuficiente: true,
    });
    console.log("[ordenes/procesar] procesado", {
      id_orden: idOrden,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
    });

    let saldoAcreditadoInfo = { acreditado: false, monto: 0, saldoNuevo: null };
    saldoAcreditadoInfo = await acreditarSaldoManual(saldoAFavor);

    const ordenUpdate = {
      pago_verificado: true,
      en_espera: result.pendientesCount > 0,
    };
    if (canAssignDeliverAdmin) {
      ordenUpdate.id_admin = sessionAdminId;
    }
    await supabaseAdmin
      .from("ordenes")
      .update(ordenUpdate)
      .eq("id_orden", idOrden);
    try {
      await cleanupClosedOrderCarrito({
        idOrden: idOrden,
        idCarrito: orden?.id_carrito,
        source: "ordenes_procesar:verified",
      });
    } catch (cleanupErr) {
      console.error("[ordenes/procesar] cleanup carrito verified error", {
        id_orden: toPositiveInt(idOrden) || null,
        id_carrito: toPositiveInt(orden?.id_carrito) || null,
        error: cleanupErr?.message || cleanupErr,
      });
    }

    await syncPagoMovilCredito();
    try {
      const waDeliveredRes = await notifyVentaEntregadaAfterOrderVerified({
        idOrden,
        manageWhatsappLifecycle: true,
        verifiedByAdmin: forceWhatsappPagoVerificado === true ? true : verifiedByAdminInThisProcess,
        forceResend: forceWhatsappPagoVerificado === true,
      });
      if (!waDeliveredRes?.sent) {
        console.log("[ordenes/procesar] whatsapp pago verificado post-procesado skipped", {
          id_orden: idOrden,
          reason: waDeliveredRes?.reason || "unknown",
          error: waDeliveredRes?.error || null,
        });
      }
    } catch (waDeliveredErr) {
      console.error("[ordenes/procesar] whatsapp pago verificado post-procesado error", {
        id_orden: idOrden,
        error: waDeliveredErr?.message || waDeliveredErr,
      });
    }

    res.json({
      ok: true,
      id_orden: idOrden,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
      id_admin: idAdminEntrega,
      saldo_acreditado: saldoAcreditadoInfo.acreditado,
      excedente_acreditado: saldoAcreditadoInfo.monto,
      saldo_nuevo: saldoAcreditadoInfo.saldoNuevo,
    });
  } catch (err) {
    console.error("[ordenes/procesar] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "INVALID_PRECIO_ID") {
      return res.status(Number(err?.httpStatus) || 400).json({
        error: err?.message || "id_precio inválido en la venta",
        details: err?.details || null,
      });
    }
    if (err?.code === "SALDO_INSUFICIENTE") {
      return res.status(Number(err?.httpStatus) || 409).json({
        error: err?.message || "Saldo insuficiente para procesar la orden.",
      });
    }
    if (
      err?.code === "ORDEN_SNAPSHOT_INVALID" ||
      err?.code === "ORDEN_SNAPSHOT_PLATFORM_MISMATCH"
    ) {
      return res.status(Number(err?.httpStatus) || 409).json({
        error:
          err?.message ||
          "No se pudo procesar la orden por conflicto con el snapshot de checkout.",
        details: err?.details || null,
      });
    }
    res.status(500).json({ error: err.message });
  } finally {
    await releaseSharedOrderProcessLock(idOrden);
  }
});

// Ventas entregadas (pendiente = false) por usuario
app.get("/api/ventas/entregadas", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) throw new Error("Usuario no autenticado");

    const { data: ventasRows, error: ventasErr } = await supabaseAdmin
      .from("ventas")
      .select("id_venta")
      .eq("id_usuario", idUsuario)
      .eq("pendiente", false);
    if (ventasErr) throw ventasErr;

    const ventasIds = new Set(
      (ventasRows || []).map((row) => toPositiveInt(row?.id_venta)).filter((value) => value > 0),
    );

    let extraGiftDelivered = 0;
    const { data: historialGiftRows, error: historialGiftErr } = await supabaseAdmin
      .from("historial_ventas")
      .select("id_historial_ventas, id_venta")
      .eq("id_usuario_cliente", idUsuario)
      .not("id_tarjeta_de_regalo", "is", null);
    if (historialGiftErr) {
      if (!isMissingHistorialGiftCardSchemaError(historialGiftErr)) throw historialGiftErr;
    } else {
      extraGiftDelivered = (historialGiftRows || []).filter((row) => {
        const saleId = toPositiveInt(row?.id_venta);
        return !saleId || !ventasIds.has(saleId);
      }).length;
    }

    res.json({ entregadas: ventasIds.size + extraGiftDelivered });
  } catch (err) {
    console.error("[ventas entregadas] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

startTasaActualScheduler();
startMonthlyMetricsClosureScheduler();

if (require.main === module) {
  app.listen(port, () => {
    console.log(`Servidor escuchando en puerto ${port}`);
  });
}

module.exports = app;
