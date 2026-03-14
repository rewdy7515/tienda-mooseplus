const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs/promises");
const { supabaseAdmin } = require("../database/db");
const { port } = require("../../config/config");
const {
  startWhatsappClient,
  stopWhatsappClient,
  getWhatsappClient,
  isWhatsappReady,
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
const WHATSAPP_SEND_DELAY_MIN_MS = 8000;
const WHATSAPP_SEND_DELAY_MAX_MS = 15000;
const WHATSAPP_SEND_TIMEOUT_MS = Math.max(
  15000,
  Number(process.env.WHATSAPP_SEND_TIMEOUT_MS) || 45000,
);
const WHATSAPP_RESET_HOUR = 0;
let lastAutoRecordatoriosRunDate = null;
let autoRecordatoriosRetryPending = false;
let autoRecordatoriosRunInProgress = false;
let recordatoriosSendInProgress = false;
let recordatoriosEnviados = false;
let recordatoriosEnviadosDate = null;
let autoRecordatoriosSchedulerStarted = false;
let autoRecordatoriosIntervalId = null;
let whatsappBootInProgress = false;
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
const HOME_BANNERS_TABLE = "banners";
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

const uniqPositiveIds = (values = []) =>
  Array.from(
    new Set(
      (values || [])
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value > 0),
    ),
  );

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
  }).formatToParts(now);
  const year = parts.find((part) => part.type === "year")?.value || "0000";
  const month = parts.find((part) => part.type === "month")?.value || "00";
  const day = parts.find((part) => part.type === "day")?.value || "00";
  const hour = Number(parts.find((part) => part.type === "hour")?.value || 0);
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

const randomWhatsappDelayMs = () => {
  return (
    WHATSAPP_SEND_DELAY_MIN_MS +
    Math.floor(
      Math.random() * (WHATSAPP_SEND_DELAY_MAX_MS - WHATSAPP_SEND_DELAY_MIN_MS + 1),
    )
  );
};

const ensureWhatsappClientStarted = async ({ reason = "unspecified" } = {}) => {
  if (!shouldStartWhatsapp) {
    const err = new Error("WhatsApp deshabilitado");
    err.code = "WHATSAPP_DISABLED";
    throw err;
  }
  if (isWhatsappReady()) return true;
  if (whatsappBootInProgress) return false;

  whatsappBootInProgress = true;
  try {
    console.log(`[WhatsApp] Iniciando cliente (${reason}).`);
    await startWhatsappClient();
    return isWhatsappReady();
  } catch (err) {
    console.error(`[WhatsApp] init error (${reason}):`, err);
    throw err;
  } finally {
    whatsappBootInProgress = false;
  }
};

const shutdownWhatsappClient = async ({ reason = "unspecified" } = {}) => {
  if (!shouldStartWhatsapp) return;
  if (recordatoriosSendInProgress || autoRecordatoriosRunInProgress) return;
  if (!isWhatsappReady() && !whatsappBootInProgress) return;
  console.log(`[WhatsApp] Apagando cliente (${reason}).`);
  await stopWhatsappClient();
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

const SUPABASE_TRANSIENT_RETRIES = 2;
const SUPABASE_TRANSIENT_RETRY_BASE_MS = 150;

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

const normalizePerfilText = (perfilRaw) => {
  const perfil = String(perfilRaw || "").trim();
  if (!perfil) return "";
  if (/^m\d+$/i.test(perfil)) return `M${perfil.replace(/^m/i, "")}`;
  if (/^\d+$/.test(perfil)) return `M${perfil}`;
  return perfil;
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
    recordatoriosEnviados: false,
    error: null,
  };
  console.log(
    `[WhatsApp] Reset diario ${dateStr} ${String(WHATSAPP_RESET_HOUR).padStart(2, "0")}:00 America/Caracas: recordatorios_enviados=false`,
  );
};

const buildWhatsappRecordatorioItems = async ({ targetUserIds = null } = {}) => {
  const userIdsFilter = uniqPositiveIds(targetUserIds || []);
  if (targetUserIds && !userIdsFilter.length) return [];
  const fechaManana = getCaracasDateStr(1);
  let ventasQuery = supabaseAdmin
    .from("ventas")
    .select(
      "id_usuario, id_cuenta, id_precio, id_venta, id_perfil, fecha_corte, correo_miembro, recordatorio_enviado",
    )
    .lte("fecha_corte", fechaManana)
    .or("recordatorio_enviado.eq.false,recordatorio_enviado.is.null");
  if (userIdsFilter.length) {
    ventasQuery = ventasQuery.in("id_usuario", userIdsFilter);
  }
  const { data: ventas, error: ventErr } = await ventasQuery;
  if (ventErr) throw ventErr;

  const ventasList = Array.isArray(ventas) ? ventas : [];
  if (!ventasList.length) return [];

  const cuentasIds = uniqPositiveIds(ventasList.map((venta) => venta.id_cuenta));
  const precioIds = uniqPositiveIds(ventasList.map((venta) => venta.id_precio));
  const userIds = uniqPositiveIds(ventasList.map((venta) => venta.id_usuario));
  const perfilIds = uniqPositiveIds(ventasList.map((venta) => venta.id_perfil));

  const [
    { data: cuentas, error: cErr },
    { data: precios, error: pErr },
    { data: users, error: uErr },
    { data: perfiles, error: perfErr },
  ] = await Promise.all([
    cuentasIds.length
      ? supabaseAdmin
          .from("cuentas")
          .select("id_cuenta, correo, id_plataforma")
          .in("id_cuenta", cuentasIds)
      : Promise.resolve({ data: [], error: null }),
    precioIds.length
      ? supabaseAdmin
          .from("precios")
          .select("id_precio, id_plataforma")
          .in("id_precio", precioIds)
      : Promise.resolve({ data: [], error: null }),
    userIds.length
      ? supabaseAdmin
          .from("usuarios")
          .select("id_usuario, nombre, apellido, telefono, fecha_registro")
          .in("id_usuario", userIds)
      : Promise.resolve({ data: [], error: null }),
    perfilIds.length
      ? supabaseAdmin
          .from("perfiles")
          .select("id_perfil, n_perfil, perfil_hogar")
          .in("id_perfil", perfilIds)
      : Promise.resolve({ data: [], error: null }),
  ]);
  if (cErr) throw cErr;
  if (pErr) throw pErr;
  if (uErr) throw uErr;
  if (perfErr) throw perfErr;

  const platIds = uniqPositiveIds([
    ...(cuentas || []).map((cuenta) => cuenta.id_plataforma),
    ...(precios || []).map((precio) => precio.id_plataforma),
  ]);
  const { data: plats, error: platErr } = platIds.length
    ? await supabaseAdmin
        .from("plataformas")
        .select("id_plataforma, nombre, correo_cliente")
        .in("id_plataforma", platIds)
    : { data: [], error: null };
  if (platErr) throw platErr;

  const mapCuenta = (cuentas || []).reduce((acc, cuenta) => {
    acc[cuenta.id_cuenta] = cuenta;
    return acc;
  }, {});
  const mapPrecio = (precios || []).reduce((acc, precio) => {
    acc[precio.id_precio] = precio;
    return acc;
  }, {});
  const mapPlat = (plats || []).reduce((acc, plat) => {
    acc[plat.id_plataforma] = plat;
    return acc;
  }, {});
  const mapUser = (users || []).reduce((acc, user) => {
    const userId = Number(user.id_usuario);
    if (!Number.isFinite(userId) || userId <= 0) return acc;
    const isRegistered = Boolean(user?.fecha_registro);
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
    };
    return acc;
  }, {});
  const mapPerf = (perfiles || []).reduce((acc, perf) => {
    acc[perf.id_perfil] = { n: perf.n_perfil, hogar: perf.perfil_hogar === true };
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
    const correo = useCorreoCliente ? venta.correo_miembro || "-" : cuenta.correo || "-";
    const perfInfo = venta.id_perfil ? mapPerf[venta.id_perfil] : null;
    const perfilTxt = perfInfo?.n ? `Perfil: M${perfInfo.n}` : "";
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
        signupUrl,
        plataformas: {},
        ventaIds: [],
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

    const fechaPago = venta.fecha_corte ? formatDDMMYYYY(venta.fecha_corte) : "-";
    const detalle = [
      `\`ID VENTA: #${venta.id_venta}\``,
      `Correo: ${correo}`,
      perfilTxt || null,
      `Fecha de pago: ${fechaPago}`,
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
    const saludo = `*¡Hola ${group.cliente}! ❤️🫎*`;
    const signupUrl = String(group.signupUrl || "").trim();
    const renewUrl = signupUrl || buildPublicSiteUrl();
    const intro = `Renueva tus membresías por nuestra nueva pagina web:\n${renewUrl}`;
    const plain = `${saludo}\n${intro}\n\n${bloques}\n\nRenueva ahora para seguir disfrutando de nuestros servicios sin interrupciones 🔁✨`;
    return {
      idUsuario: group.idUsuario,
      cliente: group.cliente,
      telefonoRaw: group.telefono,
      phone: normalizeWhatsappPhone(group.telefono),
      plain,
      ventaIds: uniqPositiveIds(group.ventaIds),
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
  if (!userItems.length) {
    return { ok: false, skipped: true, reason: "no_pending_for_user" };
  }
  if (countWhatsappSendableRecordatorios(userItems) === 0) {
    return { ok: false, skipped: true, reason: "no_sendable_pending_for_user" };
  }

  try {
    const result = await sendWhatsappRecordatorios({
      source,
      itemsOverride: userItems,
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
  if (!items.length) {
    return { ok: false, skipped: true, reason: "no_pending_for_users" };
  }
  if (countWhatsappSendableRecordatorios(items) === 0) {
    return { ok: false, skipped: true, reason: "no_sendable_pending_for_users" };
  }

  try {
    const result = await sendWhatsappRecordatorios({
      source,
      itemsOverride: items,
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

const sendWhatsappRecordatorios = async ({ source = "manual", itemsOverride = null } = {}) => {
  if (recordatoriosSendInProgress) {
    const lockErr = new Error("Ya hay un envío de recordatorios en progreso");
    lockErr.code = "RECORDATORIOS_SEND_IN_PROGRESS";
    throw lockErr;
  }

  recordatoriosSendInProgress = true;
  try {
    const items = Array.isArray(itemsOverride) ? itemsOverride : await buildWhatsappRecordatorioItems();
    if (!items.length) {
      return {
        source,
        total: 0,
        sent: 0,
        failed: 0,
        skippedNoPhone: 0,
        skippedInvalidPhone: 0,
        updatedVentas: 0,
        items: [],
      };
    }

    if (!isWhatsappReady()) {
      const notReadyErr = new Error("WhatsApp no listo");
      notReadyErr.code = "WHATSAPP_NOT_READY";
      throw notReadyErr;
    }

    const client = getWhatsappClient();
    const updatedVentaIds = new Set();
    const processedItems = [];
    const sendableItemsCount = countWhatsappSendableRecordatorios(items);
    console.log(
      `[WhatsApp] Recordatorios ${source}: inicio procesamiento total=${items.length}, enviables=${sendableItemsCount}`,
    );
    let sendableProcessed = 0;
    let sent = 0;
    let failed = 0;
    let skippedNoPhone = 0;
    let skippedInvalidPhone = 0;

    for (const item of items) {
      const hasRawPhone = Boolean(String(item.telefonoRaw || "").trim());
      if (!hasRawPhone) {
        skippedNoPhone += 1;
        processedItems.push({ ...item, status: "skipped_no_phone" });
        console.log(
          `[WhatsApp] Recordatorios ${source}: omitido_sin_telefono cliente="${item.cliente || "Cliente"}"`,
        );
        continue;
      }
      if (!item.phone) {
        skippedInvalidPhone += 1;
        processedItems.push({ ...item, status: "skipped_invalid_phone" });
        console.log(
          `[WhatsApp] Recordatorios ${source}: omitido_telefono_invalido cliente="${item.cliente || "Cliente"}" telefono="${item.telefonoRaw || ""}"`,
        );
        continue;
      }

      const progressIndex = sendableProcessed + 1;
      const progressTag = `${progressIndex}/${sendableItemsCount}`;
      const chatId = `${item.phone}@c.us`;
      try {
        console.log(
          `[WhatsApp] Recordatorios ${source}: ${progressTag} enviando cliente="${item.cliente || "Cliente"}" phone="${item.phone}"`,
        );
        await withTimeout(
          client.sendMessage(chatId, item.plain, { linkPreview: false }),
          WHATSAPP_SEND_TIMEOUT_MS,
          `Timeout enviando WhatsApp a ${item.phone}`,
        );
        const ventaIdsItem = uniqPositiveIds(item.ventaIds || []);
        if (ventaIdsItem.length) {
          const { error: updateErr } = await supabaseAdmin
            .from("ventas")
            .update({ recordatorio_enviado: true })
            .in("id_venta", ventaIdsItem);
          if (updateErr) throw updateErr;
          ventaIdsItem.forEach((id) => updatedVentaIds.add(id));
        }
        sent += 1;
        processedItems.push({ ...item, status: "sent" });
        console.log(
          `[WhatsApp] Recordatorios ${source}: ${progressTag} enviado cliente="${item.cliente || "Cliente"}" phone="${item.phone}" ventas=${ventaIdsItem.length}`,
        );
      } catch (err) {
        failed += 1;
        const errLog = buildWhatsappErrorLog(err);
        processedItems.push({
          ...item,
          status: "failed",
          error: err?.message || "No se pudo enviar",
        });
        console.error(
          `[WhatsApp] Recordatorios ${source}: ${progressTag} fallido cliente="${item.cliente || "Cliente"}" phone="${item.phone}" raw_phone="${item.telefonoRaw || ""}" chat_id="${chatId}"`,
          errLog,
        );
      }

      sendableProcessed += 1;
      if (sendableProcessed < sendableItemsCount) {
        const delayMs = randomWhatsappDelayMs();
        console.log(
          `[WhatsApp] Recordatorios ${source}: espera ${Math.round(delayMs / 1000)}s antes del siguiente envío`,
        );
        await sleep(delayMs);
      }
    }

    return {
      source,
      total: items.length,
      sent,
      failed,
      skippedNoPhone,
      skippedInvalidPhone,
      updatedVentas: updatedVentaIds.size,
      items: processedItems,
    };
  } finally {
    recordatoriosSendInProgress = false;
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
  if (!isWhatsappReady()) {
    await ensureWhatsappClientStarted({ reason: "auto_schedule" });
    if (!isWhatsappReady()) return;
  }
  const pendingItems = await buildWhatsappRecordatorioItems();
  autoRecordatoriosRunInProgress = true;

  const attemptedAt = new Date().toISOString();
  lastAutoRecordatoriosState = {
    date: dateStr,
    status: "running",
    attemptedAt,
    completedAt: null,
    total: pendingItems.length,
    sent: 0,
    failed: 0,
    skippedNoPhone: 0,
    skippedInvalidPhone: 0,
    updatedVentas: 0,
    recordatoriosEnviados,
    error: null,
  };

  try {
    const result = await sendWhatsappRecordatorios({ source: "auto", itemsOverride: pendingItems });
    const sentAll = didSendAllRecordatorios(result);
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
      recordatoriosEnviados,
      error: null,
    };
    console.log(
      `[WhatsApp] Recordatorios auto ${dateStr} ${weekday}: total=${result.total}, sent=${result.sent}, failed=${result.failed}, skipped_no_phone=${result.skippedNoPhone}, skipped_invalid_phone=${result.skippedInvalidPhone}, recordatorios_enviados=${recordatoriosEnviados}`,
    );
  } catch (err) {
    if (err?.code === "RECORDATORIOS_SEND_IN_PROGRESS") {
      console.warn(
        `[WhatsApp] Recordatorios auto ${dateStr}: ya hay un envío en progreso, se espera próximo ciclo.`,
      );
      return;
    }
    if (err?.code === "WHATSAPP_NOT_READY") {
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
        recordatoriosEnviados,
        error: "WhatsApp no listo",
      };
      console.warn(
        `[WhatsApp] Recordatorios auto ${dateStr}: cliente no listo, se reintentará en la próxima verificación hasta conectar sesión.`,
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

const extractRefCandidates = (text) => {
  const raw = String(text || "");
  const matches = raw.match(/\d{4,}/g) || [];
  return matches.map((m) => String(m).trim()).filter(Boolean);
};

const autoMatchPagoMovilAgainstOrders = async (pagoMovilRow = {}) => {
  const pagoId = Number(pagoMovilRow?.id || 0);
  if (!pagoId) return { matched: false, reason: "invalid_pago_id" };

  const pagoMonto = normalizeMontoBs(pagoMovilRow?.monto_bs);
  const refCandidates = [
    ...extractRefCandidates(pagoMovilRow?.texto || ""),
    ...extractRefCandidates(pagoMovilRow?.referencia || ""),
  ];
  const refLast4Set = new Set(
    refCandidates
      .map((r) => normalizeReferenceDigits(r))
      .filter((r) => r.length >= 4)
      .map((r) => r.slice(-4)),
  );
  if (!refLast4Set.size || !Number.isFinite(pagoMonto)) {
    return { matched: false, reason: "missing_ref_or_amount" };
  }

  const { data: pendingOrders, error: ordErr } = await supabaseAdmin
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
  if (ordErr) throw ordErr;

  const computeOrderMontoBs = (orden) => {
    const direct = normalizeMontoBs(orden?.monto_bs);
    if (Number.isFinite(direct)) return direct;
    const total = Number(orden?.total);
    const tasa = Number(orden?.tasa_bs);
    if (Number.isFinite(total) && Number.isFinite(tasa)) {
      return Math.round(total * tasa * 100) / 100;
    }
    return null;
  };

  const matchedOrder = (pendingOrders || []).find((orden) => {
    const refDigits = normalizeReferenceDigits(orden?.referencia);
    if (refDigits.length < 4) return false;
    const refLast4 = refDigits.slice(-4);
    if (!refLast4Set.has(refLast4)) return false;
    const montoOrdenBs = computeOrderMontoBs(orden);
    if (!Number.isFinite(montoOrdenBs)) return false;
    return Math.abs(montoOrdenBs - pagoMonto) <= 0.01;
  });

  if (!matchedOrder?.id_orden) {
    return { matched: false, reason: "no_order_match" };
  }

  const refMatch =
    refCandidates
      .map((r) => normalizeReferenceDigits(r))
      .find((r) => r.length >= 4 && r.slice(-4) === normalizeReferenceDigits(matchedOrder.referencia).slice(-4)) ||
    null;

  return {
    matched: true,
    id_orden: matchedOrder.id_orden,
    id_usuario: matchedOrder.id_usuario || null,
    pago_id: pagoId,
    referencia_match: refMatch,
    monto_bs_pago: pagoMonto,
  };
};

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
      return res.json({ ok: true, duplicado: true });
    }

    const montoMatch =
      texto.match(/Bs\.?\s*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]+)?)/i)?.[1] ||
      texto.match(/Bs\.?\s*([0-9]+(?:[.,][0-9]+)?)/i)?.[1] ||
      texto.match(/Bs\.?\s*(0[.,][0-9]+)/i)?.[1] ||
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

    const payloadPagoMovil = {
      app: appName,
      titulo,
      texto,
      fecha,
      dispositivo,
      hash,
      monto_bs: monto,
    };
    const { data: insertedPagoMovil, error: insErr } = await supabaseAdmin
      .from("pagomoviles")
      .insert(payloadPagoMovil)
      .select("id, referencia, texto, monto_bs")
      .single();
    if (insErr) throw insErr;

    let matchResult = { matched: false, reason: "not_checked" };
    try {
      matchResult = await autoMatchPagoMovilAgainstOrders(insertedPagoMovil || {});
      if (matchResult?.matched) {
        console.log("[bdv/notify] pago conciliado con orden", matchResult);
      } else {
        console.log("[bdv/notify] pago recibido sin orden coincidente", {
          pago_id: insertedPagoMovil?.id || null,
          reason: matchResult?.reason || "unknown",
        });
      }
    } catch (matchErr) {
      console.error("[bdv/notify] conciliacion automatica error", matchErr);
    }

    return res.json({
      ok: true,
      duplicado: false,
      coincidencia_orden: matchResult?.matched === true,
      id_orden: matchResult?.id_orden || null,
    });
  } catch (err) {
    console.error("bdv notify error", err);
    return res.status(500).json({ error: "Internal error" });
  }
});

app.use(jsonParser);

app.get("/api/whatsapp/status", (req, res) => {
  res.json({ ready: isWhatsappReady() });
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
    pendingMessages,
    pendingError,
    lastAutoRecordatoriosRunDate,
    lastAutoRecordatoriosState,
  });
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
    await client.sendMessage(`${phone}@c.us`, String(message), { linkPreview: false });

    if (ventaIds.length) {
      const { error: updateErr } = await supabaseAdmin
        .from("ventas")
        .update({ recordatorio_enviado: true })
        .in("id_venta", ventaIds);
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
  try {
    await requireAdminSession(req);
    const result = await sendWhatsappRecordatorios({ source: "manual" });
    const { dateStr } = getCaracasClock();
    ensureDailyRecordatoriosState(dateStr);
    if (didSendAllRecordatorios(result)) {
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
      return res.status(409).json({ error: "Ya hay un envío de recordatorios en progreso" });
    }
    if (err?.code === "WHATSAPP_NOT_READY") {
      return res.status(503).json({ error: "WhatsApp no listo" });
    }
    console.error("[whatsapp/recordatorios/enviar] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudieron enviar los recordatorios" });
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
      return res.status(409).json({ error: "Ya hay un envío de recordatorios en progreso" });
    }
    console.error("[whatsapp/recordatorios/trigger-user] error", err);
    return res
      .status(500)
      .json({ error: err?.message || "No se pudo disparar el envío del recordatorio" });
  }
});

const INDEX_HTML_PATH = path.join(__dirname, "..", "frontend", "pages", "index.html");
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
const SESSION_COOKIE_NAME = "session_user_id";
const SESSION_COOKIE_SIGNING_SECRET = String(
  process.env.SESSION_COOKIE_SECRET ||
    process.env.SIGNUP_TOKEN_SECRET ||
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    "",
).trim();
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
const SIGNUP_TOKEN_SECRET = String(
  process.env.SIGNUP_TOKEN_SECRET ||
    process.env.REGISTRATION_LINK_SECRET ||
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    "",
).trim();
const SIGNUP_RESEND_COOLDOWN_MS = 60 * 1000;
const signupResendCooldownMap = new Map();
const NEW_AUTH_SIGNUP_NOTIFY_USER_ID = 23;

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

const buildSignupRegistrationUrl = (idUsuario) => {
  const token = buildSignupRegistrationToken(idUsuario);
  const signupUrl = new URL("/signup", PUBLIC_SITE_URL);
  signupUrl.searchParams.set("t", token);
  return signupUrl.toString();
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

const autoAssignReportedPendingVentas = async ({ plataformaIds = [] } = {}) => {
  const summary = { scanned: 0, resolved: 0, skipped: 0, errors: 0 };
  const platformFilter = uniqPositiveIds(plataformaIds);

  const { data: reemplazosRows, error: reemplazosErr } = await supabaseAdmin
    .from("reemplazos")
    .select("id_cuenta, id_perfil");
  if (reemplazosErr) throw reemplazosErr;
  const reemplazosBloqueados = buildReemplazosBlocklist(reemplazosRows);

  const { data: ventasRows, error: ventasErr } = await supabaseAdmin
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
    .eq("reportado", true)
    .order("id_venta", { ascending: true })
    .limit(250);
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
        !reemplazosBloqueados.perfiles.has(perfilId) &&
        !reemplazosBloqueados.cuentas.has(cuentaId)
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
      if (!ventaId || !plataformaId) {
        summary.skipped += 1;
        continue;
      }

      const oldCuentaId = toPositiveInt(venta?.id_cuenta);
      const oldPerfilId = toPositiveInt(venta?.id_perfil);
      const oldCuentaMiembroId = toPositiveInt(venta?.id_cuenta_miembro);
      const currentCuenta = venta?.cuenta_base || venta?.cuenta_miembro || null;
      const perfilHogar = venta?.perfiles?.perfil_hogar === true;
      const onlyCuentaMadre =
        Number(plataformaId) === 9 &&
        (isTrue(currentCuenta?.cuenta_madre) || !!oldPerfilId);
      const ventaMiembro =
        isTrue(currentCuenta?.venta_miembro) || (!!oldCuentaMiembroId && !oldPerfilId);
      const ventaPerfil =
        !!oldPerfilId || isTrue(currentCuenta?.venta_perfil) || onlyCuentaMadre;
      const ventaCompleta =
        !ventaPerfil &&
        !ventaMiembro &&
        (isTrue(venta?.precios?.completa) ||
          (!isTrue(currentCuenta?.venta_perfil) && !isTrue(currentCuenta?.venta_miembro)));

      let nuevoCuentaId = null;
      let nuevoPerfilId = null;
      if (ventaPerfil) {
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
        reportado: false,
      };
      if (oldCuentaMiembroId) {
        updateVenta.id_cuenta_miembro = null;
      }
      const { error: updVentaErr } = await supabaseAdmin
        .from("ventas")
        .update(updateVenta)
        .eq("id_venta", ventaId);
      if (updVentaErr) throw updVentaErr;

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

      if (oldCuentaId || oldPerfilId) {
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
      }

      summary.resolved += 1;
    } catch (err) {
      summary.errors += 1;
      console.error("[autoAssignReportedPendingVentas] item error", err);
    }
  }

  return summary;
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
  return { esMayorista, pickPrecio };
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

const buildCheckoutContext = async ({ idUsuarioVentas, carritoId, totalCliente, tasa_bs }) => {
  const { pickPrecio } = await getPrecioPicker(idUsuarioVentas);
  const { data: items, error: itemErr } = await supabaseAdmin
    .from("carrito_items")
    .select("id_precio, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
    .eq("id_carrito", carritoId);
  if (itemErr) throw itemErr;

  if (!items?.length) {
    const total = Number.isFinite(Number(totalCliente)) ? Number(totalCliente) : 0;
    const tasaBs = Number.isFinite(Number(tasa_bs)) ? Number(tasa_bs) : 400;
    return { items: [], priceMap: {}, platInfoById: {}, platNameById: {}, pickPrecio, total, tasaBs };
  }

  assertItemsValidPrecioId(items);

  const preciosIds = (items || []).map((i) => i.id_precio).filter(Boolean);
  const { data: precios, error: precioErr } = await supabaseAdmin
    .from("precios")
    .select("id_precio, precio_usd_detal, precio_usd_mayor, id_plataforma, completa, sub_cuenta")
    .in("id_precio", preciosIds);
  if (precioErr) throw precioErr;
  const priceMap = (precios || []).reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});

  const plataformaIds = [...new Set((precios || []).map((p) => p.id_plataforma).filter(Boolean))];
  const { data: plataformas, error: platErr } = await supabaseAdmin
    .from("plataformas")
    .select(
      "id_plataforma, nombre, entrega_inmediata, cuenta_madre, correo_cliente, tarjeta_de_regalo, por_pantalla, por_acceso",
    )
    .in("id_plataforma", plataformaIds);
  if (platErr) throw platErr;
  const platInfoById = (plataformas || []).reduce((acc, p) => {
    acc[p.id_plataforma] = p;
    return acc;
  }, {});
  const platNameById = (plataformas || []).reduce((acc, p) => {
    acc[p.id_plataforma] = p.nombre || `Plataforma ${p.id_plataforma}`;
    return acc;
  }, {});

  const totalCalc = (items || []).reduce((sum, it) => {
    const unit = pickPrecio(priceMap[it.id_precio]);
    const mesesVal = it.meses || 1;
    return sum + unit * (it.cantidad || 0) * mesesVal;
  }, 0);
  const total = Number.isFinite(Number(totalCliente)) ? Number(totalCliente) : totalCalc;
  const tasaBs = Number.isFinite(Number(tasa_bs)) ? Number(tasa_bs) : 400;

  return { items: items || [], priceMap, platInfoById, platNameById, pickPrecio, total, tasaBs };
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
  if (nPerfil) {
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

const syncOrdenItemsSnapshot = async ({
  ordenId,
  items,
  priceMap,
  platInfoById,
  platNameById,
  pickPrecio,
  totalUsd,
  montoBsTotal,
  tasaBs,
}) => {
  const orderIdNum = Number(ordenId);
  if (!Number.isFinite(orderIdNum) || orderIdNum <= 0) return;

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

  const round2 = (value) => Math.round((Number(value) + Number.EPSILON) * 100) / 100;
  const draftRows = itemsList.map((item) => {
    const priceInfo = priceMap?.[item?.id_precio] || {};
    const platId = Number(priceInfo?.id_plataforma) || null;
    const platformInfo = platId ? platInfoById?.[platId] || {} : {};
    const isGiftCard = isTrue(platformInfo?.tarjeta_de_regalo);
    const qty = Math.max(1, Number(item?.cantidad) || 1);
    const mesesVal = Math.max(1, Number(item?.meses) || 1);
    const unitPrice = Number(pickPrecio?.(priceInfo)) || 0;
    const multiplier = isGiftCard ? qty : qty * mesesVal;
    const montoBaseUsd = round2(unitPrice * multiplier);
    const platformName = platId ? platNameById?.[platId] || `Plataforma ${platId}` : "Plataforma";

    return {
      id_orden: orderIdNum,
      id_plataforma: platId,
      renovacion: item?.renovacion === true,
      detalle: buildOrdenItemDetalle({
        item,
        priceInfo,
        platformInfo,
        platformName,
        cuentaMap,
        perfilMap,
        ventaMap,
      }),
      monto_usd: 0,
      monto_bs: 0,
      _montoBaseUsd: montoBaseUsd,
    };
  });

  const baseTotalUsd = round2(
    draftRows.reduce((sum, row) => sum + (Number(row?._montoBaseUsd) || 0), 0),
  );
  const targetTotalUsd = Number.isFinite(Number(totalUsd)) ? round2(totalUsd) : baseTotalUsd;
  const targetMontoBs = Number.isFinite(Number(montoBsTotal))
    ? round2(montoBsTotal)
    : Number.isFinite(Number(targetTotalUsd)) && Number.isFinite(Number(tasaBs))
      ? round2(targetTotalUsd * Number(tasaBs))
      : null;

  let usdAssigned = 0;
  let bsAssigned = 0;
  const snapshotRows = draftRows.map((row, index) => {
    const isLast = index === draftRows.length - 1;
    const ratio =
      baseTotalUsd > 0 ? (Number(row?._montoBaseUsd) || 0) / baseTotalUsd : draftRows.length ? 1 / draftRows.length : 0;
    const montoUsd = isLast ? round2(targetTotalUsd - usdAssigned) : round2(targetTotalUsd * ratio);
    usdAssigned = round2(usdAssigned + montoUsd);

    let montoBs = null;
    if (Number.isFinite(targetMontoBs)) {
      montoBs = isLast ? round2(targetMontoBs - bsAssigned) : round2(targetMontoBs * ratio);
      bsAssigned = round2(bsAssigned + montoBs);
    }

    return {
      id_orden: row.id_orden,
      id_plataforma: row.id_plataforma,
      renovacion: row.renovacion,
      detalle: row.detalle,
      monto_usd: montoUsd,
      monto_bs: montoBs,
    };
  });

  const { error: delErr } = await supabaseAdmin
    .from("ordenes_items")
    .delete()
    .eq("id_orden", orderIdNum);
  if (delErr) throw delErr;

  if (!snapshotRows.length) return;

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
  items,
  priceMap,
  platInfoById,
  platNameById,
  pickPrecio,
  referencia,
  archivos,
  id_metodo_de_pago,
  carritoId,
  montoHistorialTotalOverride,
}) => {
  console.log("[checkout] processOrderFromItems start", {
    ordenId,
    idUsuarioSesion,
    idUsuarioVentas,
    itemsCount: items?.length || 0,
    carritoId,
  });
  const isoHoy = todayInVenezuela();
  const referenciaNum = Number.isFinite(Number(referencia)) ? Number(referencia) : null;
  const archivosArr = Array.isArray(archivos) ? archivos : [];
  const comprobanteHist = archivosArr?.[0] || null;
  const isCuentaCompletaByFlags = (cuentaRow) =>
    !!cuentaRow && !isTrue(cuentaRow?.venta_perfil) && !isTrue(cuentaRow?.venta_miembro);
  const cuentaFlagsById = {};

  // Renovaciones (no asignan stock nuevo)
  const renovaciones = (items || []).filter((it) => it.renovacion === true && it.id_venta);
  const idsVentasRenovar = renovaciones.map((r) => r.id_venta).filter(Boolean);
  const ventaMap = {};
  if (idsVentasRenovar.length) {
    const { data: ventasExistentes, error: ventErr } = await supabaseAdmin
      .from("ventas")
      .select(
        "id_venta, fecha_corte, id_cuenta, id_cuenta_miembro, id_usuario, suspendido, cuenta_principal:cuentas!ventas_id_cuenta_fkey(venta_perfil, venta_miembro), cuenta_miembro:cuentas!ventas_id_cuenta_miembro_fkey(venta_perfil, venta_miembro)"
      )
      .in("id_venta", idsVentasRenovar);
    if (ventErr) throw ventErr;
    (ventasExistentes || []).forEach((v) => {
      ventaMap[v.id_venta] = v;
    });
  }

  let renovacionesPendientesCount = 0;
  const renovPromises = renovaciones.map((it) => {
    const price = priceMap[it.id_precio] || {};
    const mesesVal = Number.isFinite(Number(it.meses)) && Number(it.meses) > 0 ? Math.round(Number(it.meses)) : 1;
    const cantidadVal = Number.isFinite(Number(it.cantidad)) && Number(it.cantidad) > 0 ? Number(it.cantidad) : 0;
    const base = pickPrecio(price) * cantidadVal * mesesVal;
    const monto = Number(base.toFixed(2));
    const ventaAnt = ventaMap[it.id_venta] || {};
    const isSuspendidaAnt = isTrue(ventaAnt?.suspendido);
    const renovarPendiente = isSuspendidaAnt;
    if (renovarPendiente) renovacionesPendientesCount += 1;
    const cuentaVenta = ventaAnt?.cuenta_principal || ventaAnt?.cuenta_miembro || null;
    const isCuentaCompletaRenov =
      isCuentaCompletaByFlags(cuentaVenta) ||
      (isTrue(price?.completa) && !isTrue(price?.sub_cuenta));
    const fechaBaseSrc = ventaAnt?.fecha_corte || isoHoy;
    const fecha_corte = addMonthsKeepDay(fechaBaseSrc, mesesVal) || isoHoy;
    const updatePayload = {
      fecha_pago: isoHoy,
      fecha_corte,
      monto,
      id_orden: ordenId,
      renovacion: true,
      pendiente: renovarPendiente,
      suspendido: false,
    };
    if (isCuentaCompletaRenov) {
      updatePayload.cuenta_pagada_admin = false;
    }
    return supabaseAdmin
      .from("ventas")
      .update(updatePayload)
      .eq("id_venta", it.id_venta);
  });
  if (renovPromises.length) {
    const renovRes = await Promise.all(renovPromises);
    const renovErr = renovRes.find((r) => r?.error);
    if (renovErr?.error) throw renovErr.error;
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

    if (isGiftCardSale) {
      const { data: giftCardsStock, error: giftErr } = await supabaseAdmin
        .from("tarjetas_de_regalo")
        .select("id_tarjeta_de_regalo, id_plataforma, pin, para_venta, usado")
        .eq("id_plataforma", platId)
        .eq("para_venta", true)
        .eq("usado", false)
        .order("id_tarjeta_de_regalo", { ascending: true })
        .limit(stockScanLimit(cantidad));
      if (giftErr) throw giftErr;
      const disponibles = (giftCardsStock || []).filter((row) => toPositiveInt(row?.id_tarjeta_de_regalo));
      const faltantes = Math.max(0, cantidad - disponibles.length);
      disponibles.slice(0, cantidad).forEach((row) => {
        asignaciones.push({
          id_precio: price.id_precio,
          monto: pickPrecio(price),
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
            id_precio: price.id_precio,
            monto: pickPrecio(price),
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
          id_precio: price.id_precio,
          monto: pickPrecio(price),
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
            id_precio: price.id_precio,
            monto: pickPrecio(price),
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
          id_precio: price.id_precio,
          monto: pickPrecio(price),
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
            id_precio: price.id_precio,
            monto: pickPrecio(price),
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
              id_precio: price.id_precio,
              monto: pickPrecio(price),
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
          id_precio: price.id_precio,
          monto: pickPrecio(price),
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
            id_precio: price.id_precio,
            monto: pickPrecio(price),
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
      .select("id_cuenta, id_plataforma, inactiva, venta_perfil, venta_miembro")
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

  const ventasToInsert = [...asignaciones, ...pendientes].map((a) => {
    const mesesValRaw = a.meses || 1;
    const mesesVal =
      Number.isFinite(Number(mesesValRaw)) && Number(mesesValRaw) > 0
        ? Math.max(1, Math.round(Number(mesesValRaw)))
        : 1;
    const fechaCorte = a.pendiente ? null : addMonthsKeepDay(isoHoy, mesesVal);
    const priceInfo = priceMap[a.id_precio] || {};
    const platId = priceInfo.id_plataforma || null;
    const platInfo = platInfoById[platId] || {};
    const isCompleta =
      priceInfo.completa === true ||
      priceInfo.completa === "true" ||
      priceInfo.completa === 1 ||
      priceInfo.completa === "1";
    const isCorreoCliente =
      platInfo.correo_cliente === true ||
      platInfo.correo_cliente === "true" ||
      platInfo.correo_cliente === 1 ||
      platInfo.correo_cliente === "1";
    const cuentaId = toPositiveInt(a.id_cuenta);
    const cuentaFlags = cuentaId ? cuentaFlagsById[cuentaId] || null : null;
    const isCuentaCompletaVenta = cuentaFlags
      ? isCuentaCompletaByFlags(cuentaFlags)
      : isCompleta && !isTrue(priceInfo.sub_cuenta);
    return {
      id_usuario: idUsuarioVentas,
      id_precio: a.id_precio,
      id_tarjeta_de_regalo: toPositiveInt(a.id_tarjeta_de_regalo) || null,
      id_cuenta: a.id_cuenta,
      id_perfil: a.id_perfil,
      // id_sub_cuenta no existe en la tabla ventas; si se requiere, agregar columna en DB
      id_orden: ordenId,
      monto: Number(a.monto) || 0,
      pendiente: !!a.pendiente,
      meses_contratados: mesesVal,
      fecha_corte: fechaCorte,
      fecha_pago: isoHoy,
      renovacion: false,
      completa: isCompleta && isCorreoCliente ? true : null,
      cuenta_pagada_admin: isCuentaCompletaVenta ? false : null,
    };
  });
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
      registrado_por: idUsuarioSesion,
      id_metodo_de_pago,
      referencia: referenciaNum,
      comprobante: comprobanteHist,
      hora_pago: horaPago,
    });
  });
  // Renovaciones
  renovaciones.forEach((it) => {
    const platId = priceMap[it.id_precio]?.id_plataforma || null;
    const ventaAnt = ventaMap[it.id_venta] || {};
    const cuentaAnt = ventaAnt.id_cuenta || null;
    const usuarioAnt = ventaAnt.id_usuario || idUsuarioVentas;
    // monto ya calculado arriba como base (unit * cantidad * meses)
    const price = priceMap[it.id_precio] || {};
    const mesesVal = Number.isFinite(Number(it.meses)) && Number(it.meses) > 0 ? Math.round(Number(it.meses)) : 1;
    const cantidadVal = Number.isFinite(Number(it.cantidad)) && Number(it.cantidad) > 0 ? Number(it.cantidad) : 0;
    const base = pickPrecio(price) * cantidadVal * mesesVal;
    histRows.push({
      id_usuario_cliente: usuarioAnt,
      id_proveedor: null,
      monto: Number(base) || 0,
      fecha_pago: isoHoy,
      venta_cliente: true,
      renovacion: true,
      id_venta: it.id_venta,
      id_orden: ordenId,
      id_plataforma: platId,
      id_cuenta: cuentaAnt,
      registrado_por: idUsuarioSesion,
      id_metodo_de_pago,
      referencia: referenciaNum,
      comprobante: comprobanteHist,
      hora_pago: horaPago,
    });
  });
  if (histRows.length) {
    const targetHistTotalNum = Number(montoHistorialTotalOverride);
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
    const { error: histErr } = await supabaseAdmin.from("historial_ventas").insert(histRows);
    if (histErr) throw histErr;
  }

  // marca recursos como ocupados
  const perfilesIds = uniqPositiveIds(asignaciones.map((a) => a.id_perfil));
  const cuentasIds = uniqPositiveIds(
    asignaciones
      .filter((a) => a.id_perfil === null && a.id_cuenta)
      .map((a) => a.id_cuenta),
  );
  const cuentasCompletasIds = uniqPositiveIds(
    asignaciones
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

  // limpia carrito (desvincula orden para evitar FK)
  await supabaseAdmin.from("ordenes").update({ id_carrito: null }).eq("id_orden", ordenId);
  await supabaseAdmin.from("carrito_items").delete().eq("id_carrito", carritoId);
  await supabaseAdmin.from("carritos").delete().eq("id_carrito", carritoId);

  const idUsuarioVenta = toPositiveInt(idUsuarioVentas);
  const huboVentasProcesadas = insertedVentas.length > 0 || renovaciones.length > 0;
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

  console.log("[checkout] processOrderFromItems end", {
    ordenId,
    ventasCount: ventasToInsert.length,
    pendientesCount: pendientes.length + renovacionesPendientesCount,
  });
  return {
    ventasCount: ventasToInsert.length,
    pendientesCount: pendientes.length + renovacionesPendientesCount,
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
    acceso_cliente: true,
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

const roundMoney = (value) => Math.round((Number(value) + Number.EPSILON) * 100) / 100;
const toFiniteMoney = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? roundMoney(num) : null;
};
const resolveMontoFinal = ({ montoUsd, usaSaldo, saldoUsuario }) => {
  const montoBase = toFiniteMoney(montoUsd);
  if (!Number.isFinite(montoBase)) return null;
  if (!isTrue(usaSaldo)) return montoBase;
  const saldo = toFiniteMoney(saldoUsuario);
  const saldoAplicable = Number.isFinite(saldo) && saldo > 0 ? saldo : 0;
  return Math.max(0, roundMoney(montoBase - saldoAplicable));
};

const getOrCreateCarrito = async (idUsuario) => {
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
    "cart:getOrCreateCarrito:select",
  );
  if (error) throw error;
  if (data) return data.id_carrito;

  const { data: inserted, error: insertErr } = await runSupabaseQueryWithRetry(
    () =>
      supabaseAdmin
        .from("carritos")
        .insert({ id_usuario: idUsuario, fecha_creacion: new Date().toISOString() })
        .select("id_carrito")
        .single(),
    "cart:getOrCreateCarrito:insert",
  );
  if (insertErr) throw insertErr;
  return inserted.id_carrito;
};

const BINANCE_CACHE_MS = 2 * 60 * 1000;
let cachedP2PRate = { value: null, ts: 0 };

const fetchP2PRate = async (asset = "USDT", fiat = "VES") => {
  const now = Date.now();
  if (cachedP2PRate.value && now - cachedP2PRate.ts < BINANCE_CACHE_MS) {
    return cachedP2PRate.value;
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
  cachedP2PRate = { value: rate, ts: now };
  return rate;
};

// Tasa Binance P2P USDT/VES (promedio top ofertas BUY)
app.get("/api/p2p/rate", async (_req, res) => {
  try {
    const rate = await fetchP2PRate();
    res.json({ rate });
  } catch (err) {
    console.error("[p2p rate] error", err);
    res.status(502).json({ error: "No se pudo obtener la tasa P2P" });
  }
});

// Endpoint para agregar/actualizar/eliminar items del carrito.
// Se maneja por delta: cantidad positiva suma, negativa resta; si el resultado es <=0 se elimina el item.
app.post("/api/cart/item", async (req, res) => {
  console.log("[cart:item] body", req.body);
  const {
    id_precio,
    delta,
    meses,
    renovacion = false,
    id_venta = null,
    id_cuenta = null,
    id_perfil = null,
  } = req.body || {};
  if (!id_precio || delta === undefined) {
    return res
      .status(400)
      .json({ error: "id_precio y delta son requeridos" });
  }

  const parsedDelta = Number(delta);
  if (Number.isNaN(parsedDelta)) {
    return res.status(400).json({ error: "delta debe ser numérico" });
  }

  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    const idCarrito = await getOrCreateCarrito(idUsuario);
    const bodyIdItem = req.body?.id_item ? Number(req.body.id_item) : null;
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
        .select("id_item, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
        .eq("id_carrito", idCarrito)
        .eq("id_item", bodyIdItem)
        .maybeSingle();
      if (error) throw error;
      existing = data || null;
    }
    if (!existing) {
      let selQuery = supabaseAdmin
        .from("carrito_items")
        .select("id_item, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
        .eq("id_carrito", idCarrito)
        .eq("id_precio", id_precio)
        .eq("renovacion", renovacion === true);
      if (mesesVal != null) {
        selQuery = selQuery.eq("meses", mesesVal);
      }
      selQuery =
        id_venta === undefined || id_venta === null
          ? selQuery.is("id_venta", null)
          : selQuery.eq("id_venta", id_venta);
      selQuery =
        id_cuenta === undefined || id_cuenta === null
          ? selQuery.is("id_cuenta", null)
          : selQuery.eq("id_cuenta", id_cuenta);
      selQuery =
        id_perfil === undefined || id_perfil === null
          ? selQuery.is("id_perfil", null)
          : selQuery.eq("id_perfil", id_perfil);
      const { data, error: selErr } = await selQuery.maybeSingle();
      if (selErr) throw selErr;
      existing = data || null;
    }
    // Filtra por id_venta según sea null o definido
    const matchesVenta =
      id_venta === undefined || id_venta === null
        ? existing?.id_venta === null || existing?.id_venta === undefined
        : existing?.id_venta === id_venta;
    const matchesMeses =
      mesesVal == null ? true : Number(existing?.meses) === Number(mesesVal);
    const matchesCuenta =
      id_cuenta === undefined || id_cuenta === null
        ? existing?.id_cuenta === null || existing?.id_cuenta === undefined
        : existing?.id_cuenta === id_cuenta;
    const matchesPerfil =
      id_perfil === undefined || id_perfil === null
        ? existing?.id_perfil === null || existing?.id_perfil === undefined
        : existing?.id_perfil === id_perfil;
    // Si llega id_item, siempre tratamos ese registro como el existente (aunque cambien meses).
    const matchExisting =
      existing &&
      (bodyIdItem ? true : matchesVenta && matchesCuenta && matchesPerfil && matchesMeses);
    
    const newQty = (matchExisting ? existing.cantidad : 0) + parsedDelta;

    // Permite delta=0 para sincronizar solo meses (u otros campos) de un item existente.
    if (matchExisting && parsedDelta === 0) {
      const { error: updErr } = await supabaseAdmin
        .from("carrito_items")
        .update({
          meses: mesesVal ?? existing.meses ?? null,
          renovacion: renovacion === true,
          id_venta: id_venta ?? existing.id_venta ?? null,
          id_cuenta: id_cuenta ?? existing.id_cuenta ?? null,
          id_perfil: id_perfil ?? existing.id_perfil ?? null,
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
          id_venta: id_venta ?? existing.id_venta ?? null,
          id_cuenta: id_cuenta ?? existing.id_cuenta ?? null,
          id_perfil: id_perfil ?? existing.id_perfil ?? null,
        })
        .eq("id_item", existing.id_item);
      if (updErr) throw updErr;
    } else if (newQty > 0) {
      const { error: insErr } = await supabaseAdmin
        .from("carrito_items")
        .insert({
          id_carrito: idCarrito,
          id_precio,
          cantidad: newQty,
          meses: mesesVal,
          renovacion: renovacion === true,
          id_venta: id_venta ?? null,
          id_cuenta: id_cuenta ?? null,
          id_perfil: id_perfil ?? null,
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

    console.log("[cart:item] usuario", idUsuario, "carrito", idCarrito, "delta", parsedDelta, "id_precio", id_precio, "remaining", remaining);
    res.json({ ok: true, id_carrito: idCarrito, remaining });
  } catch (err) {
    console.error("[cart:item] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
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

    const { data: carritoInfo, error: carritoErr } = await runSupabaseQueryWithRetry(
      () =>
        supabaseAdmin
          .from("carritos")
          .select("monto_usd, monto_bs, tasa_bs, descuento, monto_final, hora, fecha, usa_saldo")
          .eq("id_carrito", carritoId)
          .maybeSingle(),
      "cart:get:carritoInfo",
    );
    if (carritoErr) throw carritoErr;
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
    const montoFinalResolved = resolveMontoFinal({
      montoUsd: carritoInfo?.monto_usd,
      usaSaldo: carritoInfo?.usa_saldo,
      saldoUsuario: usuarioInfo?.saldo,
    });

    const { data: items, error: itemErr } = await runSupabaseQueryWithRetry(
      () =>
        supabaseAdmin
          .from("carrito_items")
          .select("id_item, id_precio, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
          .eq("id_carrito", carritoId),
      "cart:get:items",
    );
    if (itemErr) throw itemErr;

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

    const enriched = (items || []).map((it) => {
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
      };
    });

    res.json({
      id_carrito: carritoId,
      items: enriched,
      monto_usd: carritoInfo?.monto_usd ?? null,
      monto_bs: carritoInfo?.monto_bs ?? null,
      tasa_bs: carritoInfo?.tasa_bs ?? null,
      descuento: carritoInfo?.descuento ?? null,
      monto_final: montoFinalResolved,
      usa_saldo: carritoInfo?.usa_saldo ?? null,
      hora: carritoInfo?.hora ?? null,
      fecha: carritoInfo?.fecha ?? null,
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
    const tasa_bs = req.body?.tasa_bs === null ? null : Number(req.body?.tasa_bs);
    const { data: carritoInfo, error: carritoErr } = await supabaseAdmin
      .from("carritos")
      .select("monto_usd, usa_saldo")
      .eq("id_carrito", carritoId)
      .maybeSingle();
    if (carritoErr) throw carritoErr;
    const { data: usuarioInfo, error: usuarioErr } = await supabaseAdmin
      .from("usuarios")
      .select("saldo")
      .eq("id_usuario", idUsuario)
      .maybeSingle();
    if (usuarioErr) throw usuarioErr;
    const montoFinal = resolveMontoFinal({
      montoUsd: carritoInfo?.monto_usd,
      usaSaldo: carritoInfo?.usa_saldo,
      saldoUsuario: usuarioInfo?.saldo,
    });
    const caracasNow = new Date(
      new Date().toLocaleString("en-US", { timeZone: "America/Caracas" })
    );
    const pad2 = (val) => String(val).padStart(2, "0");
    const fecha = `${caracasNow.getFullYear()}-${pad2(caracasNow.getMonth() + 1)}-${pad2(
      caracasNow.getDate()
    )}`;
    const hora = `${pad2(caracasNow.getHours())}:${pad2(caracasNow.getMinutes())}:${pad2(
      caracasNow.getSeconds()
    )}`;
    const { error: updErr } = await supabaseAdmin
      .from("carritos")
      .update({
        tasa_bs: Number.isFinite(tasa_bs) ? tasa_bs : null,
        monto_final: montoFinal,
        hora,
        fecha,
      })
      .eq("id_carrito", carritoId);
    if (updErr) throw updErr;
    return res.json({ ok: true, id_carrito: carritoId });
  } catch (err) {
    console.error("[cart:montos] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
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
    const usa_saldo = isTrue(req.body?.usa_saldo);
    const { data: carritoInfo, error: cartErr } = await supabaseAdmin
      .from("carritos")
      .select("monto_usd")
      .eq("id_carrito", carritoId)
      .maybeSingle();
    if (cartErr) throw cartErr;
    const { data: usuarioInfo, error: userErr } = await supabaseAdmin
      .from("usuarios")
      .select("saldo")
      .eq("id_usuario", idUsuario)
      .maybeSingle();
    if (userErr) throw userErr;
    const montoFinal = resolveMontoFinal({
      montoUsd: carritoInfo?.monto_usd,
      usaSaldo: usa_saldo,
      saldoUsuario: usuarioInfo?.saldo,
    });
    const { error: updErr } = await supabaseAdmin
      .from("carritos")
      .update({ usa_saldo, monto_final: montoFinal })
      .eq("id_carrito", carritoId);
    if (updErr) throw updErr;
    return res.json({ ok: true, id_carrito: carritoId, usa_saldo, monto_final: montoFinal });
  } catch (err) {
    console.error("[cart:flags] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
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

    const context = await buildCheckoutContext({
      idUsuarioVentas: idUsuario,
      carritoId,
      totalCliente: null,
      tasa_bs: null,
    });
    const { items, priceMap, platInfoById, platNameById, pickPrecio } = context;
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

    const total = Number(carritoData?.monto_usd);
    const tasaBs = Number(carritoData?.tasa_bs);
    const montoBsRaw = Number(carritoData?.monto_bs);
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
      .select("id_usuario, fecha_registro")
      .eq("id_usuario", idUsuarioTarget)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      return res.status(404).json({ error: "Usuario no encontrado." });
    }
    if (targetRow?.fecha_registro) {
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
      .select("id_usuario, nombre, apellido, correo, fecha_registro")
      .eq("id_usuario", payload.uid)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      return res.status(404).json({ error: "Usuario del link no encontrado." });
    }
    if (targetRow?.fecha_registro) {
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
      .select("id_usuario, fecha_registro")
      .eq("id_usuario", payload.uid)
      .maybeSingle();
    if (targetErr) throw targetErr;
    if (!targetRow) {
      return res.status(404).json({ error: "Usuario del link no encontrado." });
    }
    if (targetRow?.fecha_registro) {
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

// Sesión: setea cookie httpOnly con el id de usuario autenticado en Supabase Auth.
app.post("/api/session", async (req, res) => {
  try {
    const token = getBearerTokenFromRequest(req);
    const idUsuario = await resolveUsuarioFromAuthToken(token);
    const cookieValue = signSessionCookieValue(idUsuario);
    if (!cookieValue) {
      return res.status(500).json({ error: "Configuración de sesión inválida." });
    }
    res.cookie(SESSION_COOKIE_NAME, cookieValue, SESSION_COOKIE_OPTIONS);
    return res.json({ ok: true, id_usuario: idUsuario });
  } catch (err) {
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "USER_NOT_LINKED") {
      return res.status(403).json({ error: "Usuario de auth no vinculado en usuarios." });
    }
    if (err?.code === "USER_EMAIL_DUPLICATED") {
      return res.status(409).json({ error: "Correo duplicado en usuarios. Contacta soporte." });
    }
    if (err?.code === "USER_AUTH_DUPLICATED") {
      return res
        .status(409)
        .json({ error: "La cuenta auth está vinculada a más de un usuario. Contacta soporte." });
    }
    console.error("[session] error", err);
    return res.status(500).json({ error: "No se pudo establecer la sesión." });
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

app.post("/api/client-errors", jsonParser, async (req, res) => {
  try {
    const sessionUserId = parseSessionUserId(req);
    const body = req?.body && typeof req.body === "object" ? req.body : {};
    const asInt = (value) => {
      const num = Number(value);
      return Number.isFinite(num) ? Math.trunc(num) : null;
    };
    const safeMetadata =
      body?.metadata && typeof body.metadata === "object" ? body.metadata : null;

    const payload = {
      id_usuario: Number.isFinite(Number(sessionUserId)) ? Number(sessionUserId) : null,
      level: truncateText(body?.level || "error", 20),
      kind: truncateText(body?.kind || "runtime", 50),
      message: truncateText(body?.message || "Frontend error", 4000),
      stack: truncateText(body?.stack || "", 12000),
      source: truncateText(body?.source || "", 1200),
      line: asInt(body?.line),
      column: asInt(body?.column),
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
          id_cuenta_miembro,
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
          row?.perfiles?.id_cuenta_miembro,
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
        row.perfiles?.id_cuenta_miembro ||
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
    if (!isSuper) {
      const { data: ordenRow, error: ordErr } = await supabaseAdmin
        .from("ordenes")
        .select("id_orden, id_usuario")
        .eq("id_orden", idOrden)
        .maybeSingle();
      if (ordErr) throw ordErr;
      if (!ordenRow) {
        return res.status(404).json({ error: "Orden no encontrada" });
      }
      if (Number(ordenRow.id_usuario) !== Number(idUsuarioSesion)) {
        return res.status(403).json({ error: "Orden no pertenece al usuario." });
      }
    }
    console.log("[ventas/orden] request", { id_orden: idOrden });
    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select(
        `
        id_venta,
        meses_contratados,
        renovacion,
        fecha_corte,
        id_perfil,
        id_cuenta_miembro,
        id_precio,
        id_tarjeta_de_regalo,
        pendiente,
        id_orden,
        correo_miembro,
        clave_miembro,
        cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, clave, pin, id_plataforma, venta_perfil, venta_miembro),
        cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, correo, clave, pin, id_plataforma, id_cuenta_madre),
        perfiles:perfiles(id_perfil, n_perfil, pin, perfil_hogar),
        tarjetas_de_regalo:tarjetas_de_regalo!ventas_id_tarjeta_de_regalo_fkey(id_tarjeta_de_regalo, pin),
        precios:precios(id_precio, id_plataforma, plan, completa, sub_cuenta)
      `
      )
      .eq("id_orden", idOrden)
      .order("id_venta", { ascending: false });
    if (error) throw error;
    console.log("[ventas/orden] result", { id_orden: idOrden, ventas: data?.length || 0 });
    res.json({ ventas: data || [] });
  } catch (err) {
    console.error("[ventas/orden] error", err);
    res.status(500).json({ error: err.message });
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

    let items = [];
    let itemsSource = "none";
    const { data: ordenesItems, error: ordenesItemsErr } = await supabaseAdmin
      .from("ordenes_items")
      .select(
        "id_item_orden, id_orden, id_plataforma, renovacion, detalle, monto_usd, monto_bs, plataformas:plataformas(nombre, imagen)",
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

    res.json({ orden, items, items_source: itemsSource, usuario });
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
    total: totalCliente,
    tasa_bs,
    monto_transferido,
    id_usuario_override,
    bypass_verificacion,
    id_admin,
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
      totalCliente,
      tasa_bs,
    });
    const { items, priceMap, platInfoById, platNameById, pickPrecio, total, tasaBs } = context;
    const montoBaseCobrado = await resolveMontoBaseCarrito({ carritoId, fallbackTotal: total });
    if (!items?.length) {
      return res.status(400).json({ error: "El carrito está vacío" });
    }
    console.log("[checkout] carrito items", items);
    console.log("[checkout] precios usados", priceMap);

    const caracasNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
    const pad2 = (val) => String(val).padStart(2, "0");
    const hora_orden = `${pad2(caracasNow.getHours())}:${pad2(caracasNow.getMinutes())}:${pad2(
      caracasNow.getSeconds()
    )}`;
    const referenciaTrim = String(referencia || "").trim();
    const { data: metodoPagoRow, error: metodoPagoErr } = await supabaseAdmin
      .from("metodos_de_pago")
      .select("id_metodo_de_pago, verificacion_automatica, bolivares")
      .eq("id_metodo_de_pago", id_metodo_de_pago)
      .maybeSingle();
    if (metodoPagoErr) throw metodoPagoErr;
    if (!metodoPagoRow) {
      return res.status(400).json({ error: "Método de pago inválido." });
    }
    const metodoVerificacionAutomatica = isTrue(metodoPagoRow?.verificacion_automatica);
    const metodoEsBolivares = isTrue(metodoPagoRow?.bolivares);
    if (metodoVerificacionAutomatica === false && archivos.length === 0) {
      return res.status(400).json({ error: "comprobante es requerido para este método." });
    }
    const metodoPagoIdNum = Number(id_metodo_de_pago);
    const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;
    const calcularMontoRecibidoReal = (montoTransferidoRaw, metodoId) => {
      const monto = Number(montoTransferidoRaw);
      if (!Number.isFinite(monto) || monto <= 0) return 0;
      const idMetodo = Number(metodoId);
      if (idMetodo === 4) {
        return round2(monto - (monto * 0.054 + 0.3));
      }
      if (idMetodo === 3) {
        return round2(monto * 0.8);
      }
      return round2(monto);
    };
    const montoTransferidoNum = Number(
      String(monto_transferido ?? "")
        .trim()
        .replace(",", ".")
    );
    const montoTransferido =
      !metodoEsBolivares && Number.isFinite(montoTransferidoNum) && montoTransferidoNum > 0
        ? Math.round(montoTransferidoNum * 100) / 100
        : null;
    if (!metodoEsBolivares && montoTransferido === null) {
      return res.status(400).json({ error: "monto_transferido es requerido para este método." });
    }
    const requiereVerificacionPago =
      !bypassVerificacion && Number(id_metodo_de_pago) === 1 && referenciaTrim.toUpperCase() !== "SALDO";
    const requiereEntregaManual = !bypassVerificacion && metodoVerificacionAutomatica === false;
    const monto_bs =
      Number.isFinite(total) && Number.isFinite(tasaBs)
        ? Math.round(total * tasaBs * 100) / 100
        : null;
    const montoRecibidoReal = calcularMontoRecibidoReal(montoTransferido, metodoPagoIdNum);
    const excedenteTransferido =
      Number.isFinite(montoRecibidoReal) && Number.isFinite(total)
        ? Math.round((montoRecibidoReal - total) * 100) / 100
        : 0;
    const montoMayor = excedenteTransferido > 0;
    const requierePendiente = requiereVerificacionPago || requiereEntregaManual || montoMayor;
    const en_espera = requierePendiente;
    console.log("[checkout] contexto", {
      itemsCount: items?.length || 0,
      total,
      tasaBs,
      monto_bs,
      monto_transferido: montoTransferido,
      monto_recibido_real: montoRecibidoReal,
      excedente_transferido: excedenteTransferido,
      monto_mayor: montoMayor,
      metodoVerificacionAutomatica,
      requiereVerificacionPago,
      requiereEntregaManual,
      requierePendiente,
      en_espera,
    });

    const parsedOrderId = Number(id_orden);
    const checkoutOrderPayload = {
      id_usuario: idUsuarioVentas,
      total,
      tasa_bs: tasaBs,
      monto_bs,
      monto_transferido: montoTransferido,
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
      totalUsd: total,
      montoBsTotal: monto_bs,
      tasaBs,
    });

    if (requierePendiente) {
      try {
        await supabaseAdmin
          .from("carritos")
          .insert({ id_usuario: idUsuarioSesion, fecha_creacion: new Date().toISOString() });
      } catch (cartErr) {
        console.error("[checkout] crear carrito nuevo error", cartErr);
      }
      return res.json({
        ok: true,
        id_orden: ordenId,
        total,
        ventas: 0,
        pendiente_verificacion: true,
        entrega_manual: requiereEntregaManual,
      });
    }

    const result = await processOrderFromItems({
      ordenId,
      idUsuarioSesion,
      idUsuarioVentas,
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      referencia,
      archivos,
      id_metodo_de_pago,
      carritoId,
      montoHistorialTotalOverride: montoBaseCobrado,
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

    res.json({
      ok: true,
      id_orden: ordenId,
      total,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
    });
  } catch (err) {
    console.error("[checkout] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    if (err?.code === "INVALID_PRECIO_ID") {
      return res.status(Number(err?.httpStatus) || 400).json({
        error: err?.message || "id_precio inválido en la venta",
        details: err?.details || null,
      });
    }
    res.status(500).json({ error: err.message });
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
        "id_orden, id_usuario, id_admin, id_carrito, referencia, comprobante, id_metodo_de_pago, total, tasa_bs, monto_transferido, monto_mayor, pago_verificado, en_espera, orden_cancelada"
      )
      .eq("id_orden", idOrden)
      .single();
    if (ordErr) throw ordErr;
    console.log("[ordenes/procesar] orden", {
      id_orden: orden?.id_orden,
      id_usuario: orden?.id_usuario,
      id_admin: orden?.id_admin,
      id_carrito: orden?.id_carrito,
      pago_verificado: orden?.pago_verificado,
      en_espera: orden?.en_espera,
      orden_cancelada: orden?.orden_cancelada,
    });

    if (orden?.orden_cancelada === true) {
      return res.status(400).json({ error: "Orden cancelada. No se pueden asignar servicios." });
    }

    const idUsuarioVentas = Number(orden?.id_usuario) || idUsuarioSesion;
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
    if (
      idUsuarioSesion &&
      orden?.id_usuario &&
      Number(orden.id_usuario) !== Number(idUsuarioSesion) &&
      !sessionIsSuper &&
      !isOrderAdmin
    ) {
      return res.status(403).json({ error: "Orden no pertenece al usuario." });
    }
    const { data: ventasExist, error: ventasErr } = await supabaseAdmin
      .from("ventas")
      .select("id_venta")
      .eq("id_orden", idOrden)
      .limit(1);
    if (ventasErr) throw ventasErr;
    if (ventasExist?.length) {
      const ordenPatch = {};
      if (!orden?.pago_verificado) {
        const { data: pendRows, error: pendErr } = await supabaseAdmin
          .from("ventas")
          .select("id_venta")
          .eq("id_orden", idOrden)
          .eq("pendiente", true);
        if (pendErr) throw pendErr;
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
      return res.json({
        ok: true,
        id_orden: idOrden,
        already_processed: true,
        ventas: ventasExist.length,
        id_admin: idAdminEntrega,
        saldo_acreditado: false,
        excedente_acreditado: 0,
        saldo_nuevo: null,
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

    const context = await buildCheckoutContext({
      idUsuarioVentas,
      carritoId: orden.id_carrito,
      totalCliente: orden.total,
      tasa_bs: orden.tasa_bs,
    });
    const montoBaseCobrado = await resolveMontoBaseCarrito({
      carritoId: orden.id_carrito,
      fallbackTotal: context.total,
    });
    if (!context.items?.length) {
      console.log("[ordenes/procesar] carrito vacío; se acredita saldo", {
        id_orden: idOrden,
        carritoId: orden?.id_carrito,
        montoBaseCobrado,
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
      referencia: orden?.referencia,
      archivos,
      id_metodo_de_pago: orden?.id_metodo_de_pago,
      carritoId: orden.id_carrito,
      montoHistorialTotalOverride: montoBaseCobrado,
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
    res.status(500).json({ error: err.message });
  }
});

// Ventas entregadas (pendiente = false) por usuario
app.get("/api/ventas/entregadas", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) throw new Error("Usuario no autenticado");

    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select("id_venta", { count: "exact" })
      .eq("id_usuario", idUsuario)
      .eq("pendiente", false);
    if (error) throw error;

    res.json({ entregadas: data?.length || 0 });
  } catch (err) {
    console.error("[ventas entregadas] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

if (require.main === module) {
  app.listen(port, () => {
    console.log(`Servidor escuchando en puerto ${port}`);
  });
}

module.exports = app;
