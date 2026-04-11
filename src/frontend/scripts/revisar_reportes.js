import {
  requireSession,
  attachLogout,
  setSessionRoles,
  getSessionRoles,
  attachLogoHome,
} from "./session.js";
import {
  clearServerSession,
  loadCurrentUser,
  supabase,
  ensureServerSession,
  notifyReporteSolvedWhatsapp,
  notifyReporteIncorrectDataWhatsapp,
  uploadComprobantes,
} from "./api.js";
import { formatDDMMYYYY } from "./date-format.js";
import { pickNotificationUserIds } from "./notification-templates.js";
import { copyTextNotify } from "./copy-toast.js";

requireSession();

const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const toPositiveId = (value) => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
};
const formatPlataformaReemplazo = ({
  plataforma = "",
  idPrecio = null,
  perfilHogar = false,
} = {}) => {
  const rawName = String(plataforma || "").trim();
  const isNetflix = /netflix/i.test(rawName);
  const precioNum = Number(idPrecio);
  const isPlan2Precio = precioNum === 4 || precioNum === 5;
  const isNetflixPlan2 = isNetflix && (isPlan2Precio || perfilHogar === true);
  if (isNetflixPlan2) return "*NETFLIX (HOGAR ACTUALIZADO)*";
  return rawName;
};
const getCaracasDateISO = () => {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value || "0000";
  const month = parts.find((part) => part.type === "month")?.value || "00";
  const day = parts.find((part) => part.type === "day")?.value || "00";
  return `${year}-${month}-${day}`;
};
const statusEl = document.querySelector("#revisar-status");
const listEl = document.querySelector("#reportes-list");
const toggleAutoReemplazoWrapEl = document.querySelector("#auto-reemplazo-toggle-wrap");
const toggleAutoReemplazoEl = document.querySelector("#toggle-auto-reemplazo");
const modal = document.querySelector("#modal-detalle");
const modalPlatTitle = document.querySelector("#modal-plat-title");
const modalPlatSubtitle = document.querySelector("#modal-plat-subtitle");
const modalCorreo = document.querySelector("#modal-correo");
const modalFechaCorte = document.querySelector("#modal-fecha-corte");
const modalClave = document.querySelector("#modal-clave");
const modalMotivo = document.querySelector("#modal-motivo");
const modalPerfil = document.querySelector("#modal-perfil");
const modalPin = document.querySelector("#modal-pin");
const btnCopyClave = document.querySelector("#btn-copy-clave");
const btnCopyPin = document.querySelector("#btn-copy-pin");
const btnOpenAdminCuenta = document.querySelector("#btn-open-admin-cuenta");
const perfilRows = document.querySelectorAll(".perfil-row");
const modalImagenWrapper = document.querySelector("#modal-imagen-wrapper");
const modalImagen = document.querySelector("#modal-imagen");
const modalSinImagen = document.querySelector("#modal-sin-imagen");
const modalResumen = document.querySelector("#modal-resumen");
const resumenClaveRow = document.querySelector("#resumen-clave");
const resumenClaveText = document.querySelector("#resumen-clave-text");
const resumenPinRow = document.querySelector("#resumen-pin");
const resumenPinText = document.querySelector("#resumen-pin-text");
const btnGuardarCampos = document.querySelector("#btn-guardar-campos");
const btnCerrarReporte = document.querySelector("#btn-cerrar-reporte");
const btnDatosIncorrectos = document.querySelector("#btn-datos-incorrectos");
const btnFaltanRecaudos = document.querySelector("#btn-faltan-recaudos");
const btnReemplazar = document.querySelector("#btn-reemplazar");
const modalResumenClose = document.querySelector(".modal-resumen-close");
const checkOtro = document.querySelector("#check-otro");
const resumenOtroText = document.querySelector("#resumen-otro-text");
const checkAgregarDias = document.querySelector("#check-agregar-dias");
const checkAgregarDiasLabel = checkAgregarDias?.closest("label") || null;
const checkSuscripcion = document.querySelector("#check-suscripcion");
const checkPerfiles = document.querySelector("#check-perfiles");
const checkIngreso = document.querySelector("#check-ingreso");
const checkSuscripcionActiva = document.querySelector("#check-suscripcion-activa");
const checkPinSame = document.querySelector("#check-pin-same");
const checkCodigoAyudaWrap = document.querySelector("#check-codigo-ayuda-wrap");
const checkCodigoAyuda = document.querySelector("#check-codigo-ayuda");
const checkCodigoWhatsapp = document.querySelector("#check-codigo-whatsapp");
const resumenGroupEls = document.querySelectorAll("[data-resumen-group]");
const modalDatosIncorrectos = document.querySelector("#modal-datos-incorrectos");
const modalDatosIncorrectosClose = document.querySelector(".modal-datos-incorrectos-close");
const datosIncorrectosImagenInput = document.querySelector("#datos-incorrectos-imagen");
const datosIncorrectosImagenPreview = document.querySelector("#datos-incorrectos-imagen-preview");
const datosIncorrectosCheckCorreo = document.querySelector("#datos-incorrectos-check-correo");
const datosIncorrectosCheckClave = document.querySelector("#datos-incorrectos-check-clave");
const datosIncorrectosMensajePreview = document.querySelector("#datos-incorrectos-mensaje-preview");
const btnEnviarDatosIncorrectos = document.querySelector("#btn-enviar-datos-incorrectos");

let currentRow = null;
let oldClave = "";
let oldPin = "";
let cambioClave = false;
let cambioPin = false;
let currentImageDownloadUrl = "";
let currentImageDownloadName = "";
let autoReemplazoCuentaInactivaEnabled = false;
const reportesById = new Map();
const AUTO_REEMPLAZO_CFG_KEY = "auto_reemplazo_cuenta_inactiva";

const formatDate = (iso) => formatDDMMYYYY(iso) || "-";
const normalizeIsoDateOnly = (value) => {
  const match = String(value || "")
    .trim()
    .match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return "";
  return `${match[1]}-${match[2]}-${match[3]}`;
};
const isFechaCorteOnOrBeforeToday = (fechaCorteRaw) => {
  const fechaCorteIso = normalizeIsoDateOnly(fechaCorteRaw);
  if (!fechaCorteIso) return false;
  const hoyIso = getCaracasDateISO();
  if (!hoyIso) return false;
  return fechaCorteIso <= hoyIso;
};

const normalizeHexColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  return /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash) ? withHash : null;
};

const isDarkHex = (hex) => {
  const c = normalizeHexColor(hex);
  if (!c) return false;
  const full = c.length === 4 ? `#${c[1]}${c[1]}${c[2]}${c[2]}${c[3]}${c[3]}` : c;
  const r = parseInt(full.slice(1, 3), 16);
  const g = parseInt(full.slice(3, 5), 16);
  const b = parseInt(full.slice(5, 7), 16);
  const yiq = (r * 299 + g * 587 + b * 114) / 1000;
  return yiq < 140;
};

const buttonStyleForColor = (color) => {
  const c = normalizeHexColor(color);
  if (!c) return "";
  const textColor = isDarkHex(c) ? "#fff" : "#111";
  return ` style="background:${c};border-color:${c};color:${textColor};"`;
};

const tableStyleForColor = (color) => {
  const c = normalizeHexColor(color);
  if (!c) return "";
  return ` style="--table-header-bg:${c};"`;
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const normalizeCopyValue = (value) => {
  const text = String(value ?? "").trim();
  if (!text || text === "-") return "";
  return text;
};

const copyValue = (value, toastText) => {
  const text = normalizeCopyValue(value);
  if (!text) return;
  copyTextNotify(text, toastText);
};

const normalizePlatformLink = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  return `https://${raw.replace(/^\/+/, "")}`;
};

const openAdminCuentasByCorreo = (rawCorreo) => {
  const correo = normalizeCopyValue(rawCorreo);
  if (!correo) return;
  const targetUrl = new URL("../admin/admin_cuentas.html", window.location.href);
  targetUrl.searchParams.set("correo", correo);
  window.open(targetUrl.toString(), "_blank", "noopener");
};

const normalizePublicMooseUrl = (rawUrl = "") => {
  try {
    const parsed = new URL(String(rawUrl || "").trim(), window.location.href);
    const host = String(parsed.hostname || "").trim().toLowerCase();
    if (host === "mooseplus.com" || host === "www.mooseplus.com") {
      parsed.protocol = "https:";
      parsed.hostname = "mooseplus.com";
    }
    return parsed.toString();
  } catch (_err) {
    return String(rawUrl || "").trim();
  }
};

const buildInventarioLinkByCorreo = (rawCorreo) => {
  const targetUrl = new URL("../inventario.html", window.location.href);
  const correo = normalizeCopyValue(rawCorreo);
  if (correo) targetUrl.searchParams.set("correo", correo);
  return normalizePublicMooseUrl(targetUrl.toString());
};

const sanitizeFileName = (name = "") =>
  String(name || "imagen.jpg")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9._-]/g, "_");

const resetResumenChecks = () => {
  [
    checkSuscripcion,
    checkPerfiles,
    checkIngreso,
    checkSuscripcionActiva,
    checkPinSame,
    checkCodigoAyuda,
    checkCodigoWhatsapp,
    checkOtro,
  ].forEach((input) => {
    if (input) input.checked = false;
  });
  if (resumenOtroText) {
    resumenOtroText.value = "";
    resumenOtroText.classList.add("hidden");
  }
};

const syncCodigoAyudaOptionForRow = (row) => {
  const platformId = getReportePlatformId(row);
  const showCodigoAyuda = platformId === 1;
  if (checkCodigoAyudaWrap) checkCodigoAyudaWrap.classList.toggle("hidden", !showCodigoAyuda);
  if (!showCodigoAyuda && checkCodigoAyuda) checkCodigoAyuda.checked = false;
};

const collapseResumenGroups = ({ keepGroup = null } = {}) => {
  resumenGroupEls.forEach((group) => {
    if (!group) return;
    const content = group.querySelector("[data-resumen-group-content]");
    const shouldKeepOpen = keepGroup && group === keepGroup;
    if (content) content.classList.toggle("hidden", !shouldKeepOpen);
    group.classList.toggle("is-open", !!shouldKeepOpen);
  });
};

const syncDatosIncorrectosButtonForRow = (row) => {
  const platformId = getReportePlatformId(row);
  const showButton = platformId === 9;
  if (btnDatosIncorrectos) btnDatosIncorrectos.classList.toggle("hidden", !showButton);
};

const buildDatosIncorrectosPreviewMessage = () => {
  const reportId = toPositiveId(currentRow?.id_reporte) || "-";
  const correoMarcado = datosIncorrectosCheckCorreo?.checked === true;
  const claveMarcada = datosIncorrectosCheckClave?.checked === true;
  const hasImage = Boolean((datosIncorrectosImagenInput?.files || [])[0]);
  if (!correoMarcado && !claveMarcada) {
    return "Selecciona Correo o Contraseña para ver el mensaje.";
  }
  const bullets = [];
  if (correoMarcado) bullets.push("* Correo");
  if (claveMarcada) bullets.push("* Contraseña");
  const bodyBullets = bullets.length ? bullets.join("\n") : "* -";
  const resetClaveMsg =
    claveMarcada
      ? "\n\nPuedes restablecer tu contraseña de Spotify a través de este link:\nhttps://accounts.spotify.com/es/password-reset"
      : "";
  const imageSection = hasImage ? "[Imagen adjunta]\n" : "";
  return `${imageSection}\`Reporte #${reportId}\`\nRespuesta:\nDatos erroneos:\n${bodyBullets}${resetClaveMsg}`;
};

const updateDatosIncorrectosMessagePreview = () => {
  if (!datosIncorrectosMensajePreview) return;
  datosIncorrectosMensajePreview.textContent = buildDatosIncorrectosPreviewMessage();
};

const resetDatosIncorrectosModalState = () => {
  if (datosIncorrectosImagenInput) datosIncorrectosImagenInput.value = "";
  if (datosIncorrectosImagenPreview) {
    datosIncorrectosImagenPreview.textContent = "Sin imagen seleccionada.";
  }
  if (datosIncorrectosCheckCorreo) datosIncorrectosCheckCorreo.checked = false;
  if (datosIncorrectosCheckClave) datosIncorrectosCheckClave.checked = false;
  updateDatosIncorrectosMessagePreview();
};

const closeDatosIncorrectosModal = () => {
  modalDatosIncorrectos?.classList.add("hidden");
  resetDatosIncorrectosModalState();
};

const uploadDatosIncorrectosImage = async (file, reportUserId) => {
  if (!file) return "";
  const safeName = sanitizeFileName(file.name || "imagen.jpg");
  const uniqueName = `datos-incorrectos-${reportUserId}-${Date.now()}-${safeName}`;
  const payloadFile = {
    name: uniqueName,
    type: file.type || "image/jpeg",
    arrayBuffer: () => file.arrayBuffer(),
  };
  const { urls, error } = await uploadComprobantes([payloadFile]);
  if (error || !urls?.length) {
    throw error || new Error("No se pudo subir la imagen.");
  }
  return String(urls[0] || "").trim();
};

const getDescripcion = (row) => {
  return row.descripcion || "Otro...";
};

const getPlanLabelFromReporte = (row) => {
  const cta = row?.cuentas || {};
  const ventaPerfil = isTrue(cta?.venta_perfil);
  const ventaMiembro = isTrue(cta?.venta_miembro);
  const perfilHogar = isTrue(row?.perfiles?.perfil_hogar);
  if (ventaMiembro && perfilHogar) return "Miembro";
  if (ventaPerfil) return "Perfil";
  if (!ventaPerfil && ventaMiembro) return "Miembro";
  if (!ventaPerfil && !ventaMiembro) return "Cuenta completa";
  if (Number(row?.id_perfil) > 0) return "Perfil";
  return "Sin plan";
};

const getReportePlatformId = (row) => {
  const platformId =
    Number(row?.id_plataforma) ||
    Number(row?.cuentas?.id_plataforma) ||
    Number(row?.plataformas?.id_plataforma) ||
    0;
  return Number.isFinite(platformId) && platformId > 0 ? platformId : null;
};

const pickReportePlataformaNombre = (row) => {
  const fromPlataforma = String(row?.plataformas?.nombre || "").trim();
  if (fromPlataforma) return fromPlataforma;
  const fromCuentaPlataforma = String(row?.cuentas?.plataformas?.nombre || "").trim();
  if (fromCuentaPlataforma) return fromCuentaPlataforma;
  return "Plataforma";
};

const pickReporteCorreo = (row) => {
  const correoMiembro = String(row?.correo_miembro || "").trim();
  if (correoMiembro) return correoMiembro;
  const correoCuenta = String(row?.cuentas?.correo || "").trim();
  if (correoCuenta) return correoCuenta;
  const correoMadre = String(row?.cuenta_madre_correo || "").trim();
  if (correoMadre) return correoMadre;
  return "";
};

const notifyReporteCerrado = async (row, options = {}) => {
  const reportId = Number(row?.id_reporte);
  const targetUserId = Number(row?.id_usuario);
  if (!Number.isFinite(reportId) || !Number.isFinite(targetUserId)) return;
  const plataformaTxt = escapeHtml(pickReportePlataformaNombre(row));
  const correoRaw = pickReporteCorreo(row);
  const correoTxt = correoRaw ? escapeHtml(correoRaw) : "-";
  const notaRaw = String(options?.nota ?? row?.descripcion_solucion ?? row?.descripcion ?? "").trim();
  const notaTxt = notaRaw ? escapeHtml(notaRaw) : "-";
  const diasAgregadosRaw = Number(options?.diasAgregados || 0);
  const diasAgregados = Number.isFinite(diasAgregadosRaw) ? Math.max(0, Math.trunc(diasAgregadosRaw)) : 0;
  const diasMsg = diasAgregados > 0 ? `<br>Se sumó ${diasAgregados} dias a tu fecha de pago.` : "";
  const mensaje = `<strong>${plataformaTxt}</strong><br>Correo: ${correoTxt}<br>Nota: ${notaTxt}${diasMsg}<br><a href="reportes/report.html" class="link-inline">Más detalles.</a>`;
  const fecha = getCaracasDateISO();
  const payload = {
    titulo: `Reporte ${reportId} cerrado.`,
    mensaje,
    fecha,
    leido: false,
    id_usuario: targetUserId,
  };
  const { error } = await supabase.from("notificaciones").insert(payload);
  if (error) throw error;
};

const parseDateParts = (isoDate = "") => {
  const m = String(isoDate || "").trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  return { year: Number(m[1]), month: Number(m[2]), day: Number(m[3]) };
};

const diffIsoDatesInDays = (fromIso, toIso) => {
  const from = parseDateParts(fromIso);
  const to = parseDateParts(toIso);
  if (!from || !to) return 0;
  const fromUtc = Date.UTC(from.year, from.month - 1, from.day, 0, 0, 0, 0);
  const toUtc = Date.UTC(to.year, to.month - 1, to.day, 0, 0, 0, 0);
  if (toUtc <= fromUtc) return 0;
  return Math.floor((toUtc - fromUtc) / 86400000);
};

const isReporteCreadoHoyCaracas = (row) => {
  const fechaReporteIso = normalizeIsoDateOnly(row?.fecha_creacion);
  const hoyIso = getCaracasDateISO();
  return Boolean(fechaReporteIso && hoyIso && fechaReporteIso === hoyIso);
};

const calcDiasDiferenciaReporte = (row) => {
  const fechaReporteIso = normalizeIsoDateOnly(row?.fecha_creacion);
  const fechaSolucionIso = getCaracasDateISO();
  if (!fechaReporteIso || !fechaSolucionIso) return 0;
  return diffIsoDatesInDays(fechaReporteIso, fechaSolucionIso);
};

const addDaysToIsoDate = (dateIso, days) => {
  const d = parseDateParts(dateIso);
  const nDays = Number(days);
  if (!d || !Number.isFinite(nDays) || nDays <= 0) return dateIso;
  const dt = new Date(Date.UTC(d.year, d.month - 1, d.day, 0, 0, 0));
  dt.setUTCDate(dt.getUTCDate() + Math.trunc(nDays));
  const yyyy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
};

async function findVentaAsociadaFromReporte(row) {
  const ventaIdDirecta = toPositiveId(row?.id_venta);
  if (ventaIdDirecta) {
    const { data: ventaDirecta, error: ventaDirectaErr } = await supabase
      .from("ventas")
      .select("id_venta, id_usuario, fecha_corte")
      .eq("id_venta", ventaIdDirecta)
      .maybeSingle();
    if (ventaDirectaErr) throw ventaDirectaErr;
    if (ventaDirecta?.id_venta) return ventaDirecta;
  }

  const cuentaId = Number(row?.id_cuenta);
  if (!Number.isFinite(cuentaId) || cuentaId <= 0) return null;
  const perfilId = Number(row?.id_perfil);
  const withPerfil = Number.isFinite(perfilId) && perfilId > 0;

  const findWithFilter = async (withUserFilter = true, strictPerfil = true) => {
    let query = supabase
      .from("ventas")
      .select("id_venta, id_usuario, fecha_corte")
      .or(`id_cuenta.eq.${cuentaId},id_cuenta_miembro.eq.${cuentaId}`)
      .order("id_venta", { ascending: false })
      .limit(1);
    if (withUserFilter && row?.id_usuario) query = query.eq("id_usuario", row.id_usuario);
    if (withPerfil) {
      if (strictPerfil) query = query.eq("id_perfil", perfilId);
    } else {
      query = query.is("id_perfil", null);
    }
    const { data, error } = await query;
    if (error) return { data: null, error };
    return { data: data?.[0] || null, error: null };
  };

  let res = await findWithFilter(true, true);
  if (res.error) throw res.error;
  if (res.data) return res.data;

  res = await findWithFilter(false, true);
  if (res.error) throw res.error;
  if (res.data) return res.data;

  if (!withPerfil) {
    return null;
  }
  res = await findWithFilter(true, false);
  if (res.error) throw res.error;
  if (res.data) return res.data;

  res = await findWithFilter(false, false);
  if (res.error) throw res.error;
  return res.data || null;
}

async function applyDiasExtraToVentaFechaCorte(row, ventaInfoFromFlow = null) {
  const dias = calcDiasDiferenciaReporte(row);
  if (dias < 1) return { dias: 0, applied: false, venta: null };

  const ventaInfo = ventaInfoFromFlow || (await findVentaAsociadaFromReporte(row));
  if (!ventaInfo?.id_venta) return { dias, applied: false, venta: null };
  if (!ventaInfo?.fecha_corte) return { dias, applied: false, venta: ventaInfo };

  const nuevaFechaCorte = addDaysToIsoDate(ventaInfo.fecha_corte, dias);
  const { error } = await supabase
    .from("ventas")
    .update({ fecha_corte: nuevaFechaCorte })
    .eq("id_venta", ventaInfo.id_venta);
  if (error) throw error;

  return {
    dias,
    applied: true,
    venta: { ...ventaInfo, fecha_corte: nuevaFechaCorte },
  };
}

async function clearVentaReportadoFlag(row, ventaInfoFromFlow = null) {
  const ventaIds = new Set();
  const pushVentaId = (value) => {
    const parsed = toPositiveId(value);
    if (parsed) ventaIds.add(parsed);
  };

  pushVentaId(ventaInfoFromFlow?.id_venta);
  pushVentaId(row?.id_venta);

  let ventaInfo = ventaInfoFromFlow || null;
  if (!ventaIds.size) {
    ventaInfo = await findVentaAsociadaFromReporte(row);
    pushVentaId(ventaInfo?.id_venta);
  }

  if (ventaIds.size) {
    const ids = Array.from(ventaIds);
    const { error } = await supabase
      .from("ventas")
      .update({ reportado: false, aviso_admin: true })
      .in("id_venta", ids);
    if (error) throw error;
    return ventaInfo || { id_venta: ids[0] };
  }

  const cuentaId = toPositiveId(row?.id_cuenta);
  if (!cuentaId) return null;

  let fallbackQuery = supabase
    .from("ventas")
    .update({ reportado: false, aviso_admin: true })
    .eq("reportado", true)
    .or(`id_cuenta.eq.${cuentaId},id_cuenta_miembro.eq.${cuentaId}`);

  const userId = toPositiveId(row?.id_usuario);
  if (userId) fallbackQuery = fallbackQuery.eq("id_usuario", userId);

  const perfilId = toPositiveId(row?.id_perfil);
  if (perfilId) {
    fallbackQuery = fallbackQuery.eq("id_perfil", perfilId);
  } else {
    fallbackQuery = fallbackQuery.is("id_perfil", null);
  }

  const { error: fallbackErr } = await fallbackQuery;
  if (fallbackErr) throw fallbackErr;
  return ventaInfo;
}

async function reactivarVentaPendienteFromReporte(row) {
  const platformId = getReportePlatformId(row);
  if (platformId !== 9) {
    return { ok: false, reason: "platform_not_supported" };
  }

  let ventaId = toPositiveId(row?.id_venta);
  if (!ventaId) {
    const ventaInfo = await findVentaAsociadaFromReporte(row);
    ventaId = toPositiveId(ventaInfo?.id_venta);
  }
  if (!ventaId) {
    return { ok: false, reason: "venta_not_found" };
  }

  const { error } = await supabase
    .from("ventas")
    .update({ pendiente: true, aviso_admin: false })
    .eq("id_venta", ventaId);
  if (error) throw error;
  row.id_venta = ventaId;
  return { ok: true, id_venta: ventaId };
}

async function notifyDiasSumados({ row, ventaInfo, dias }) {
  const targetUserId = Number(ventaInfo?.id_usuario || row?.id_usuario);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) return;
  const fecha = getCaracasDateISO();
  const payload = {
    titulo: "Reporte solucionado",
    mensaje: `Se sumó ${dias} dias a tu fecha de pago.`,
    fecha,
    leido: false,
    id_usuario: targetUserId,
    id_cuenta: Number.isFinite(Number(row?.id_cuenta)) ? Number(row.id_cuenta) : null,
  };
  const { error } = await supabase.from("notificaciones").insert(payload);
  if (error) throw error;
}

async function notifyReemplazoReporte({ row, plataforma, correoViejo, correoNuevo, dias }) {
  const targetUserId = Number(row?.id_usuario);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) return;
  const fecha = getCaracasDateISO();
  const correoNuevoTxt = String(correoNuevo || "").trim();
  const correoViejoTxt = String(correoViejo || "").trim();
  const plataformaTxt = escapeHtml(plataforma || "la plataforma");
  const correoViejoSafe = escapeHtml(correoViejoTxt || "-");
  const correoNuevoSafe = escapeHtml(correoNuevoTxt);
  const correoLink = correoNuevoTxt
    ? `<a href="inventario.html?correo=${encodeURIComponent(correoNuevoTxt)}" class="link-inline">${correoNuevoSafe}</a>`
    : "-";
  const diasInt = Number.isFinite(Number(dias)) ? Math.max(0, Number(dias)) : 0;
  const diasMsg = diasInt > 0 ? `<br><br>Se sumó ${diasInt} dias a tu fecha de pago.` : "";
  const payload = {
    titulo: "Servicio reemplazado",
    mensaje: `Tu servicio de <strong>${plataformaTxt}</strong> pasó de ${correoViejoSafe} a:<br>Correo: ${correoLink}${diasMsg}<br>Presiona el correo para ver los datos de la cuenta.`,
    fecha,
    leido: false,
    id_usuario: targetUserId,
    id_cuenta: Number.isFinite(Number(row?.id_cuenta)) ? Number(row.id_cuenta) : null,
  };
  const { error } = await supabase.from("notificaciones").insert(payload);
  if (error) throw error;
}

async function notifyReporteSolvedWhatsappBestEffort(
  idReporte,
  { row = null, ventaId = null, pendingState = undefined } = {},
) {
  const reportId = toPositiveId(idReporte);
  if (!reportId) return { ok: false, skipped: true, reason: "invalid_report" };
  try {
    let pendingValue = pendingState;
    if (pendingValue === undefined) {
      let resolvedVentaId = toPositiveId(ventaId);
      if (!resolvedVentaId && row) {
        const ventaInfo = await findVentaAsociadaFromReporte(row);
        resolvedVentaId = toPositiveId(ventaInfo?.id_venta);
      }
      if (!resolvedVentaId) {
        return { ok: false, skipped: true, reason: "venta_not_found" };
      }
      const { data: ventaRow, error: ventaErr } = await supabase
        .from("ventas")
        .select("id_venta, pendiente")
        .eq("id_venta", resolvedVentaId)
        .maybeSingle();
      if (ventaErr) {
        console.error("whatsapp reporte solucionado venta lookup error", ventaErr);
        return { ok: false, skipped: true, reason: "venta_lookup_error", error: ventaErr.message };
      }
      pendingValue = ventaRow?.pendiente;
    }

    if (pendingValue !== false) {
      return { ok: false, skipped: true, reason: "venta_pending_flag_not_false" };
    }

    const waResult = await notifyReporteSolvedWhatsapp(reportId);
    if (waResult?.error) {
      console.error("whatsapp reporte solucionado error", waResult);
      return { ok: false, error: waResult.error, status: waResult.status || null };
    }
    if (waResult?.sent === true || waResult?.ok === true) {
      return { ok: true, sent: true };
    }
    return { ok: false, skipped: true, reason: waResult?.reason || "not_sent" };
  } catch (err) {
    console.error("whatsapp reporte solucionado exception", err);
    return { ok: false, error: err?.message || String(err) };
  }
}

async function loadAutoReemplazoConfig() {
  const { data, error } = await supabase
    .from("configuracion_sistema")
    .select("valor_bool")
    .eq("clave", AUTO_REEMPLAZO_CFG_KEY)
    .maybeSingle();
  if (error) throw error;
  return data?.valor_bool === true;
}

async function saveAutoReemplazoConfig(value) {
  const next = value === true;
  const { error } = await supabase
    .from("configuracion_sistema")
    .update({
      valor_bool: next,
      actualizado_en: new Date().toISOString(),
    })
    .eq("clave", AUTO_REEMPLAZO_CFG_KEY);
  if (error) throw error;
}

async function initAutoReemplazoToggle({ isSuperadmin = false } = {}) {
  let currentValue = false;
  try {
    currentValue = await loadAutoReemplazoConfig();
  } catch (err) {
    console.error("load auto reemplazo config error", err);
    currentValue = false;
  }
  autoReemplazoCuentaInactivaEnabled = currentValue;

  if (!toggleAutoReemplazoEl) return currentValue;
  if (!isSuperadmin) {
    toggleAutoReemplazoWrapEl?.classList.add("hidden");
    toggleAutoReemplazoEl.checked = currentValue;
    toggleAutoReemplazoEl.disabled = true;
    return currentValue;
  }
  toggleAutoReemplazoWrapEl?.classList.remove("hidden");
  toggleAutoReemplazoEl.disabled = true;
  toggleAutoReemplazoEl.checked = currentValue;

  toggleAutoReemplazoEl.disabled = false;
  toggleAutoReemplazoEl.addEventListener("change", async () => {
    const prev = !toggleAutoReemplazoEl.checked;
    const next = toggleAutoReemplazoEl.checked;
    toggleAutoReemplazoEl.disabled = true;
    try {
      await saveAutoReemplazoConfig(next);
      autoReemplazoCuentaInactivaEnabled = next;
    } catch (err) {
      console.error("save auto reemplazo config error", err);
      toggleAutoReemplazoEl.checked = prev;
      autoReemplazoCuentaInactivaEnabled = prev;
      alert("No se pudo actualizar el estado del auto reemplazo.");
    } finally {
      toggleAutoReemplazoEl.disabled = false;
    }
  });
  return currentValue;
}

const extractStorageRef = (rawPath) => {
  const raw = String(rawPath || "").trim();
  if (!raw) return null;
  if (!/^https?:\/\//i.test(raw)) {
    return { bucket: "private_assets", path: raw.replace(/^\/+/, "") };
  }
  try {
    const parsed = new URL(raw);
    const match = parsed.pathname.match(
      /\/storage\/v1\/object\/(?:public|sign|authenticated)\/([^/]+)\/(.+)$/,
    );
    if (match && match[1] && match[2]) {
      return { bucket: match[1], path: decodeURIComponent(match[2]) };
    }
    const idx = parsed.pathname.indexOf("/private_assets/");
    if (idx >= 0) {
      return {
        bucket: "private_assets",
        path: decodeURIComponent(parsed.pathname.slice(idx + "/private_assets/".length)),
      };
    }
  } catch (_err) {
    return null;
  }
  return null;
};

async function getImageUrl(path) {
  if (!path) return null;
  const storageRef = extractStorageRef(path);
  if (!storageRef) {
    return /^https?:\/\//i.test(path) ? path : null;
  }
  const fallbackPublicUrl = () => {
    try {
      const { data } = supabase.storage.from(storageRef.bucket).getPublicUrl(storageRef.path);
      return String(data?.publicUrl || "").trim() || null;
    } catch (_err) {
      return /^https?:\/\//i.test(path) ? path : null;
    }
  };
  try {
    const { data, error } = await supabase.storage
      .from(storageRef.bucket)
      .createSignedUrl(storageRef.path, 3600);
    if (error) {
      console.error("signed url error", error);
      return fallbackPublicUrl();
    }
    return data?.signedUrl || null;
  } catch (err) {
    console.error("signed url error", err);
    return fallbackPublicUrl();
  }
}

const renderReportesList = (plataformas = []) => {
  if (!listEl) return;
  if (!plataformas.length) {
    listEl.innerHTML = "";
    return;
  }
  listEl.innerHTML = plataformas
    .map((p, idx) => {
      const planGroups = new Map();
      (p.items || []).forEach((r) => {
        const plan = getPlanLabelFromReporte(r);
        if (!planGroups.has(plan)) planGroups.set(plan, []);
        planGroups.get(plan).push(r);
      });
      const rowsHtml = Array.from(planGroups.entries())
        .map(([plan, rows]) => {
          const groupedRows = (rows || [])
            .map((r) => {
              const idFmt = r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-";
              const cliente =
                [r.usuarios?.nombre, r.usuarios?.apellido].filter(Boolean).join(" ").trim() || "-";
              const platformId = getReportePlatformId(r);
              const correo = r.cuentas?.correo || "-";
              const correoText = escapeHtml(correo);
              const correoCopyAttr = escapeHtml(correo);
              const datosCorregidos = isTrue(r?.datos_corregidos);
              const datosIncorrectos = !datosCorregidos && isTrue(r?.datos_incorrectos);
              const madreCorreo = normalizeCopyValue(r?.cuenta_madre_correo);
              const madreCopyAttr = escapeHtml(madreCorreo);
              const cuentaInactiva = isTrue(r?.cuenta_inactiva_resuelta);
              const cuentaTieneMadre = isTrue(r?.cuenta_tiene_madre);
              const cuentaActivaTrasReemplazo =
                (isTrue(r?._reemplazo_activo) || cuentaTieneMadre) && !cuentaInactiva;
              const fechaCorteVencida = isTrue(r?.cuenta_fecha_corte_vencida);
              const activaDotTitle = fechaCorteVencida
                ? "Cuenta activa tras reemplazo (fecha de corte vencida)"
                : "Cuenta activa tras reemplazo";
              const activaDotExtraClass = fechaCorteVencida ? " reporte-activa-dot-warning" : "";
              const estadoCuentaDot = cuentaInactiva
                ? '<span class="reporte-inactiva-dot" title="Cuenta inactiva" aria-label="Cuenta inactiva"></span>'
                : cuentaActivaTrasReemplazo
                  ? `<span class="reporte-activa-dot${activaDotExtraClass}" title="${escapeHtml(activaDotTitle)}" aria-label="${escapeHtml(activaDotTitle)}"></span>`
                  : "";
              const datosBadge = datosCorregidos
                ? '<span class="reporte-datos-badge is-corregido">Datos corregidos</span>'
                : datosIncorrectos
                  ? '<span class="reporte-datos-badge is-incorrecto">Datos incorrectos</span>'
                  : "";
              const correoCell = normalizeCopyValue(correo)
                ? `<span class="correo-actions-inline">
                    <span class="correo-main-inline">
                      <span class="copyable-field reporte-copy" data-copy="${correoCopyAttr}" title="Copiar correo">${correoText}</span>
                    </span>
                    <span class="correo-admin-actions">
                      ${datosBadge}
                      ${estadoCuentaDot}
                      <button type="button" class="btn-outline btn-small btn-admin-inline" data-open-admin-correo="${correoCopyAttr}" title="Abrir en Admin Cuentas" aria-label="Abrir en Admin Cuentas">↗</button>
                      ${
                        madreCorreo
                          ? `<button type="button" class="btn-outline btn-small btn-admin-inline btn-admin-madre-inline" data-open-admin-madre-correo="${madreCopyAttr}" title="Abrir cuenta madre en Admin Cuentas" aria-label="Abrir cuenta madre en Admin Cuentas">↗</button>`
                          : ""
                      }
                    </span>
                  </span>`
                : `<span class="correo-actions-inline">
                    <span class="correo-main-inline">${correoText}</span>
                    <span class="correo-admin-actions">${datosBadge}${estadoCuentaDot}</span>
                  </span>`;
              const motivo = getDescripcion(r);
              const canReactivate = platformId === 9;
              const motivoCell = canReactivate
                ? `<div class="reporte-motivo-wrap">
                    <span class="reporte-motivo-text" title="${escapeHtml(motivo)}">${escapeHtml(motivo)}</span>
                    <button class="btn-outline btn-small btn-reactivar-venta" data-id="${r.id_reporte}" data-action="reactivar">Reactivar</button>
                  </div>`
                : escapeHtml(motivo);
              const fecha = formatDate(r.fecha_creacion || null);
              return `
                <tr>
                  <td>${escapeHtml(idFmt)}</td>
                  <td>${escapeHtml(cliente)}</td>
                  <td>${correoCell}</td>
                  <td>${motivoCell}</td>
                  <td>${escapeHtml(fecha)}</td>
                  <td>
                    <div class="actions-inline">
                      <button class="btn-outline btn-small" data-id="${r.id_reporte}" data-action="detalle">Más detalles</button>
                    </div>
                  </td>
                </tr>
              `;
            })
            .join("");
          return `
            <tr class="plan-divider-row">
              <td colspan="6">Plan: ${escapeHtml(plan)}</td>
            </tr>
            ${groupedRows}
          `;
        })
        .join("");
      return `
        <section class="inventario-item" data-plat="${p.id}">
          <button type="button" class="btn-outline inventario-btn" data-toggle-plat="${p.id}" data-idx="${idx}"${buttonStyleForColor(p.buttonColor)}>
            <span class="plat-btn-main">
              <span class="plat-btn-label">${escapeHtml(p.nombre || "Plataforma")}</span>
              <span class="plat-count-icon" aria-hidden="true">!</span>
            </span>
            <span class="plat-count-badge" aria-label="${(p.items || []).length} reportes">
              <span>${(p.items || []).length}</span>
            </span>
          </button>
          <div class="inventario-plan hidden" data-plat-content="${p.id}">
            <div class="tabla-wrapper">
              <table class="table-base reportes-table"${tableStyleForColor(p.headerColor)}>
                <thead>
                  <tr>
                    <th>ID Reporte</th>
                    <th>Cliente</th>
                    <th>Correo</th>
                    <th>Motivo</th>
                    <th>Fecha</th>
                    <th>Acción</th>
                  </tr>
                </thead>
                <tbody>
                  ${rowsHtml || '<tr><td colspan="6" class="status">No hay reportes pendientes para esta plataforma.</td></tr>'}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      `;
    })
    .join("");
};

const buildPlataformasFromRows = (rows = []) => {
  const porPlat = new Map();
  (rows || []).forEach((r) => {
    const id = r.id_plataforma || r.cuentas?.id_plataforma || r.plataformas?.id_plataforma;
    const nombre = r.plataformas?.nombre || "Plataforma";
    const buttonColor = r.plataformas?.color_1 || null;
    const headerColor = r.plataformas?.color_2 || null;
    if (!id) return;
    if (!porPlat.has(id)) {
      porPlat.set(id, { id, nombre, buttonColor, headerColor, items: [] });
    }
    porPlat.get(id).items.push(r);
  });
  return Array.from(porPlat.values()).map((plataforma) => {
    const sortedItems = [...(plataforma.items || [])].sort((a, b) => {
      const aId = toPositiveId(a?.id_reporte) || Number.MAX_SAFE_INTEGER;
      const bId = toPositiveId(b?.id_reporte) || Number.MAX_SAFE_INTEGER;
      return aId - bId;
    });
    return { ...plataforma, items: sortedItems };
  });
};

const rerenderActivosDesdeMap = () => {
  const activos = Array.from(reportesById.values()).filter(
    (r) => r?.en_revision !== false && r?.solucionado === false,
  );
  if (!activos.length) {
    if (listEl) listEl.innerHTML = "";
    if (statusEl) statusEl.textContent = "No hay reportes pendientes.";
    return;
  }
  const plataformas = buildPlataformasFromRows(activos);
  renderReportesList(plataformas);
  if (statusEl) statusEl.textContent = "";
};

const removeReporteRowFromUI = (reportIdRaw) => {
  const reportId = Number(reportIdRaw);
  if (!Number.isFinite(reportId) || reportId <= 0 || !listEl) return;

  reportesById.delete(String(reportId));

  const actionBtn = listEl.querySelector(
    `button[data-action="detalle"][data-id="${String(reportId)}"]`,
  );
  const row = actionBtn?.closest("tr");
  if (!row) return;
  const tbody = row.closest("tbody");
  const section = row.closest(".inventario-item");
  const prev = row.previousElementSibling;
  const next = row.nextElementSibling;
  row.remove();

  // Si el plan quedó vacío, elimina el divisor "Plan: ..."
  if (
    prev?.classList?.contains("plan-divider-row") &&
    (!next || next.classList?.contains("plan-divider-row"))
  ) {
    prev.remove();
  }

  if (section) {
    const leftInSection = section.querySelectorAll('button[data-action="detalle"]').length;
    const badge = section.querySelector(".plat-count-badge span");
    const badgeWrap = section.querySelector(".plat-count-badge");
    if (badge) badge.textContent = String(leftInSection);
    if (badgeWrap) badgeWrap.setAttribute("aria-label", `${leftInSection} reportes`);
    if (leftInSection === 0) {
      section.remove();
    }
  }

  if (tbody && !tbody.querySelector("tr")) {
    tbody.innerHTML =
      '<tr><td colspan="6" class="status">No hay reportes pendientes para esta plataforma.</td></tr>';
  }

  const remaining = listEl.querySelectorAll('button[data-action="detalle"]').length;
  if (!remaining && statusEl) {
    statusEl.textContent = "No hay reportes pendientes.";
  }
};

async function loadReportes() {
  const baseSelect =
    "id_reporte,id_venta,id_plataforma,plataformas(id_plataforma,nombre,color_1,color_2,link_pagina),id_usuario,usuarios:usuarios!reportes_id_usuario_fkey(nombre,apellido),id_cuenta,cuentas(id_cuenta,correo,clave,fecha_corte,id_plataforma,venta_perfil,venta_miembro,inactiva,id_cuenta_madre,cuenta_madre),id_perfil,perfiles(id_perfil,n_perfil,pin,perfil_hogar,id_cuenta),descripcion,imagen,en_revision,solucionado,fecha_creacion,hora_creacion";
  const selectWithDatosFlags = `${baseSelect},datos_incorrectos,datos_corregidos`;
  const isMissingDatosColumnError = (err) => {
    const msg = String(err?.message || "").toLowerCase();
    const details = String(err?.details || "").toLowerCase();
    const hint = String(err?.hint || "").toLowerCase();
    const blob = `${msg} ${details} ${hint}`;
    return (
      String(err?.code || "").trim() === "42703" ||
      blob.includes("datos_incorrectos") ||
      blob.includes("datos_corregidos")
    );
  };
  let { data, error } = await supabase
    .from("reportes")
    .select(selectWithDatosFlags)
    .eq("solucionado", false);
  if (error && isMissingDatosColumnError(error)) {
    const fallbackResp = await supabase
      .from("reportes")
      .select(baseSelect)
      .eq("solucionado", false);
    data = fallbackResp.data || [];
    error = fallbackResp.error || null;
  }
  if (error) throw error;
  const rows = (data || []).map((row) => ({
    ...row,
    datos_incorrectos: isTrue(row?.datos_incorrectos),
    datos_corregidos: isTrue(row?.datos_corregidos),
  }));

  const ventaIds = Array.from(
    new Set(rows.map((row) => toPositiveId(row?.id_venta)).filter((value) => !!value)),
  );
  const ventaCompletaById = new Map();
  if (ventaIds.length) {
    const { data: ventasData, error: ventasErr } = await supabase
      .from("ventas")
      .select("id_venta, completa")
      .in("id_venta", ventaIds);
    if (ventasErr) {
      console.error("load reportes ventas completa error", ventasErr);
    } else {
      (ventasData || []).forEach((venta) => {
        ventaCompletaById.set(toPositiveId(venta?.id_venta), isTrue(venta?.completa));
      });
    }
  }

  rows.forEach((row) => {
    const ventaId = toPositiveId(row?.id_venta);
    row.venta_completa = ventaId ? ventaCompletaById.get(ventaId) === true : false;
  });

  const cuentaIds = new Set();
  rows.forEach((row) => {
    const cuentaId = toPositiveId(row?.cuentas?.id_cuenta);
    const cuentaMadreId = toPositiveId(row?.cuentas?.id_cuenta_madre);
    if (cuentaId) cuentaIds.add(cuentaId);
    if (cuentaMadreId) cuentaIds.add(cuentaMadreId);
  });
  const cuentasById = new Map();
  if (cuentaIds.size) {
    const { data: cuentasData, error: cuentasErr } = await supabase
      .from("cuentas")
      .select("id_cuenta, id_cuenta_madre, cuenta_madre, correo, inactiva, fecha_corte")
      .in("id_cuenta", Array.from(cuentaIds));
    if (cuentasErr) {
      console.error("load reportes cuentas madre error", cuentasErr);
    } else {
      (cuentasData || []).forEach((cta) => {
        const idCuenta = toPositiveId(cta?.id_cuenta);
        if (idCuenta) cuentasById.set(idCuenta, cta);
      });
    }
  }
  rows.forEach((row) => {
    const cuentaRow = row?.cuentas || {};
    const cuentaId = toPositiveId(cuentaRow?.id_cuenta);
    const cuentaMadreId = toPositiveId(cuentaRow?.id_cuenta_madre);
    const cuentaEsMadre = isTrue(cuentaRow?.cuenta_madre);
    const madreId = cuentaMadreId || (cuentaEsMadre ? cuentaId : null);
    const madreRow = madreId ? cuentasById.get(madreId) : null;
    const cuentaBaseInactiva = isTrue(cuentaRow?.inactiva);
    const madreInactiva =
      madreRow && Object.prototype.hasOwnProperty.call(madreRow, "inactiva")
        ? isTrue(madreRow?.inactiva)
        : null;
    const cuentaInactivaResuelta =
      madreInactiva === null ? cuentaBaseInactiva : madreInactiva;
    const madreCorreo = normalizeCopyValue(
      madreRow?.correo || (cuentaMadreId ? "" : cuentaRow?.correo || ""),
    );
    row.cuenta_madre_correo = madreCorreo;
    row.cuenta_madre_inactiva = madreInactiva;
    row.cuenta_tiene_madre = !!madreId;
    row.cuenta_inactiva_resuelta = cuentaInactivaResuelta;
    const fechaCorteCuentaBase = normalizeIsoDateOnly(cuentaRow?.fecha_corte || "");
    const fechaCorteMadre = normalizeIsoDateOnly(madreRow?.fecha_corte || "");
    const fechaCorteResuelta = fechaCorteMadre || fechaCorteCuentaBase || "";
    row.cuenta_fecha_corte_resuelta = fechaCorteResuelta;
    row.cuenta_fecha_corte_vencida = isFechaCorteOnOrBeforeToday(fechaCorteResuelta);
  });

  return rows;
}

const resetImageFrame = () => {
  modalImagenWrapper?.style.removeProperty("--report-image-ratio");
};

const applyImageFrame = (imgEl) => {
  const width = Number(imgEl?.naturalWidth) || 0;
  const height = Number(imgEl?.naturalHeight) || 0;
  if (width <= 0 || height <= 0) {
    resetImageFrame();
    return;
  }
  const ratio = width / height;
  const boundedRatio = Math.min(2.1, Math.max(0.56, ratio));
  modalImagenWrapper?.style.setProperty("--report-image-ratio", String(boundedRatio));
};

const buildImageDownloadName = (row, imageUrl) => {
  const reportId = Number(row?.id_reporte);
  const idPart = Number.isFinite(reportId) && reportId > 0
    ? String(reportId).padStart(4, "0")
    : "evidencia";
  let ext = "jpg";
  try {
    const parsed = new URL(String(imageUrl || ""), window.location.href);
    const match = (parsed.pathname || "").match(/\.([a-z0-9]+)$/i);
    if (match?.[1]) ext = String(match[1]).toLowerCase();
  } catch (_err) {
    const match = String(imageUrl || "").match(/\.([a-z0-9]+)(?:$|[?#])/i);
    if (match?.[1]) ext = String(match[1]).toLowerCase();
  }
  return `reporte-${idPart}.${ext}`;
};

const triggerImageDownload = async () => {
  const url = String(currentImageDownloadUrl || "").trim();
  if (!url) return;
  const fileName = String(currentImageDownloadName || "evidencia.jpg");
  try {
    const resp = await fetch(url, { credentials: "omit" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const blob = await resp.blob();
    const blobUrl = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = blobUrl;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(blobUrl);
  } catch (_err) {
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName;
    link.target = "_blank";
    link.rel = "noopener";
    document.body.appendChild(link);
    link.click();
    link.remove();
  }
};

function closeModal() {
  modal?.classList.add("hidden");
  if (btnDatosIncorrectos) btnDatosIncorrectos.classList.add("hidden");
  closeDatosIncorrectosModal();
  modalImagenWrapper?.classList.add("hidden");
  modalImagenWrapper?.classList.remove("no-image");
  modalImagenWrapper?.classList.remove("downloadable");
  modalImagenWrapper?.removeAttribute("title");
  modalSinImagen?.classList.add("hidden");
  if (modalImagen) modalImagen.src = "";
  resetImageFrame();
  currentImageDownloadUrl = "";
  currentImageDownloadName = "";
  if (modalPlatSubtitle) {
    modalPlatSubtitle.textContent = "";
    modalPlatSubtitle.classList.add("hidden");
  }
  currentRow = null;
  cambioClave = false;
  cambioPin = false;
}

async function openModal(row) {
  if (!modal) return;
  syncDatosIncorrectosButtonForRow(row);
  const platName = row.plataformas?.nombre || "-";
  const platColor = normalizeHexColor(row.plataformas?.color_1) || "#111";
  const platLink = normalizePlatformLink(row.plataformas?.link_pagina);
  const isCuentaCompleta = isTrue(row?.ventas?.completa) || row?.venta_completa === true;
  modalPlatTitle.textContent = platName;
  modalPlatTitle.style.color = platColor;
  if (modalPlatSubtitle) {
    modalPlatSubtitle.textContent = isCuentaCompleta ? "Cuenta completa" : "";
    modalPlatSubtitle.classList.toggle("hidden", !isCuentaCompleta);
  }
  if (platLink) {
    modalPlatTitle.classList.add("is-link");
    modalPlatTitle.dataset.href = platLink;
    modalPlatTitle.setAttribute("title", "Abrir página de la plataforma");
    modalPlatTitle.setAttribute("role", "link");
    modalPlatTitle.setAttribute("tabindex", "0");
  } else {
    modalPlatTitle.classList.remove("is-link");
    delete modalPlatTitle.dataset.href;
    modalPlatTitle.removeAttribute("title");
    modalPlatTitle.removeAttribute("role");
    modalPlatTitle.removeAttribute("tabindex");
  }
  if (modalCorreo) modalCorreo.value = row.cuentas?.correo || "-";
  if (modalFechaCorte) modalFechaCorte.value = formatDate(row.cuentas?.fecha_corte || null);
  oldClave = row.cuentas?.clave || "";
  const rawPin = row.perfiles?.pin ?? row.pin ?? "";
  oldPin = rawPin === null || rawPin === undefined ? "" : String(rawPin);
  if (modalClave) modalClave.value = oldClave;
  const perfilDatos = row.perfiles || {};
  const showPerfil = !!row.id_perfil;
  perfilRows.forEach((el) => el.classList.toggle("hidden", !showPerfil));
  if (showPerfil) {
    modalPerfil.textContent = perfilDatos.n_perfil ? `Perfil ${perfilDatos.n_perfil}` : (perfilDatos.id_perfil || row.id_perfil || "-");
    if (modalPin) modalPin.value = oldPin;
  } else {
    modalPerfil.textContent = "";
    if (modalPin) modalPin.value = "";
  }
  modalMotivo.textContent = getDescripcion(row);

  const showNoImageBox = () => {
    if (modalImagen) modalImagen.src = "";
    modalImagenWrapper?.classList.remove("hidden");
    modalImagenWrapper?.classList.add("no-image");
    modalImagenWrapper?.classList.remove("downloadable");
    modalImagenWrapper?.removeAttribute("title");
    modalSinImagen?.classList.add("hidden");
    resetImageFrame();
    currentImageDownloadUrl = "";
    currentImageDownloadName = "";
  };

  if (row.imagen) {
    const url = await getImageUrl(row.imagen);
    if (url && modalImagen) {
      modalImagen.onload = () => {
        modalImagenWrapper?.classList.remove("hidden");
        modalImagenWrapper?.classList.remove("no-image");
        modalSinImagen?.classList.add("hidden");
        applyImageFrame(modalImagen);
      };
      modalImagen.onerror = () => {
        showNoImageBox();
      };
      modalImagen.src = url;
      modalImagenWrapper?.classList.remove("hidden");
      modalImagenWrapper?.classList.remove("no-image");
      modalImagenWrapper?.classList.add("downloadable");
      modalImagenWrapper?.setAttribute("title", "Presiona para descargar imagen");
      modalSinImagen?.classList.add("hidden");
      currentImageDownloadUrl = url;
      currentImageDownloadName = buildImageDownloadName(row, url);
      if (modalImagen.complete) {
        applyImageFrame(modalImagen);
      }
    } else {
      showNoImageBox();
    }
  } else {
    showNoImageBox();
  }
  modal.classList.remove("hidden");
  currentRow = row;
  cambioClave = false;
  cambioPin = false;
}

async function init() {
  try {
    requireSession();
    await ensureServerSession();
    const user = await loadCurrentUser();
    setSessionRoles(user || {});
    if (user && usernameEl) {
      const fullName = [user.nombre, user.apellido].filter(Boolean).join(" ").trim();
      usernameEl.textContent = fullName || user.correo || "Usuario";
    }
    const sessionRoles = getSessionRoles();
    const isAdmin =
      isTrue(sessionRoles?.permiso_admin) ||
      isTrue(sessionRoles?.permiso_superadmin) ||
      isTrue(user?.permiso_admin) ||
      isTrue(user?.permiso_superadmin);
    const isSuperadmin =
      isTrue(sessionRoles?.permiso_superadmin) || isTrue(user?.permiso_superadmin);
    if (adminLink) {
      adminLink.classList.toggle("hidden", !isAdmin);
      adminLink.style.display = isAdmin ? "block" : "none";
    }
    await initAutoReemplazoToggle({ isSuperadmin });
    attachLogoHome();

    const all = await loadReportes();
    let activos = (all || []).filter((r) => r.en_revision !== false && r.solucionado === false);
    if (autoReemplazoCuentaInactivaEnabled) {
      const processed = await autoReplaceInactiveReportes(activos);
      if (processed) {
        const refreshed = await loadReportes();
        activos = (refreshed || []).filter((r) => r.en_revision !== false && r.solucionado === false);
      }
    }
    if (!activos.length) {
      if (statusEl) statusEl.textContent = "No hay reportes pendientes.";
      if (listEl) listEl.innerHTML = "";
      return;
    }

    reportesById.clear();
    activos.forEach((r) => {
      if (r?.id_reporte) reportesById.set(String(r.id_reporte), r);
    });

    const plataformas = buildPlataformasFromRows(activos);
    renderReportesList(plataformas);
    if (statusEl) statusEl.textContent = "";

    listEl?.addEventListener("click", (e) => {
      const openAdminMadreTarget = e.target.closest("[data-open-admin-madre-correo]");
      if (openAdminMadreTarget) {
        e.preventDefault();
        e.stopPropagation();
        openAdminCuentasByCorreo(openAdminMadreTarget.dataset.openAdminMadreCorreo || "");
        return;
      }

      const openAdminTarget = e.target.closest("[data-open-admin-correo]");
      if (openAdminTarget) {
        e.preventDefault();
        e.stopPropagation();
        openAdminCuentasByCorreo(openAdminTarget.dataset.openAdminCorreo || "");
        return;
      }

      const copyTarget = e.target.closest("[data-copy]");
      if (copyTarget) {
        e.preventDefault();
        e.stopPropagation();
        copyValue(copyTarget.dataset.copy || "", "Correo copiado");
        return;
      }

      const toggleBtn = e.target.closest("button[data-toggle-plat]");
      if (toggleBtn) {
        const platId = String(toggleBtn.dataset.togglePlat || "");
        const section = toggleBtn.closest(".inventario-item");
        const content = section?.querySelector(`[data-plat-content="${platId}"]`);
        if (content) {
          content.classList.toggle("hidden");
          section?.classList.toggle("open", !content.classList.contains("hidden"));
        }
        return;
      }

      const btn = e.target.closest("button[data-action]");
      if (!btn) return;
      const id = btn.dataset.id;
      if (!id) return;
      const row = reportesById.get(String(id));
      if (!row) return;
      const action = btn.dataset.action;
      if (action === "detalle") {
        openModal(row);
        return;
      }
      if (action === "reactivar") {
        const originalText = btn.textContent;
        btn.disabled = true;
        btn.textContent = "Reactivando...";
        reactivarVentaPendienteFromReporte(row)
          .then((result) => {
            if (!result?.ok) {
              if (result?.reason === "venta_not_found") {
                alert("No se encontró la venta asociada para reactivar.");
              } else {
                alert("No se pudo reactivar esta venta.");
              }
              return;
            }
            alert(`Venta #${result.id_venta} reactivada.`);
          })
          .catch((err) => {
            console.error("reactivar venta desde reporte error", err);
            alert("No se pudo reactivar la venta.");
          })
          .finally(() => {
            btn.disabled = false;
            btn.textContent = originalText || "Reactivar";
          });
      }
      // acción de cerrar reporte no implementada aquí
    });

    modal?.addEventListener("click", (e) => {
      if (e.target.classList.contains("modal-backdrop") || e.target.classList.contains("modal-close")) {
        closeModal();
      }
    });

    btnCerrarReporte?.addEventListener("click", () => {
      if (!currentRow) return;
      resetResumenChecks();
      syncCodigoAyudaOptionForRow(currentRow);
      collapseResumenGroups();
      const nuevaClave = modalClave?.value || "";
      const nuevoPin = (modalPin?.value || "").trim();
      const claveCambia = nuevaClave !== oldClave;
      const pinCambia = !!currentRow.id_perfil && nuevoPin !== oldPin;
      cambioClave = claveCambia;
      cambioPin = pinCambia;

      if (resumenClaveRow) resumenClaveRow.classList.toggle("hidden", !claveCambia);
      if (resumenClaveText) {
        resumenClaveText.textContent = claveCambia
          ? `${oldClave || "(vacía)"} -> ${nuevaClave || "(vacía)"}`
          : "";
      }
      const showPin = pinCambia;
      if (resumenPinRow) resumenPinRow.classList.toggle("hidden", !showPin);
      if (resumenPinText) {
        resumenPinText.textContent = showPin ? `${oldPin || "(vacío)"} -> ${nuevoPin || "(vacío)"}` : "";
      }

      modalResumen?.classList.remove("hidden");
      const reporteCreadoHoy = isReporteCreadoHoyCaracas(currentRow);
      if (checkAgregarDiasLabel) checkAgregarDiasLabel.classList.toggle("hidden", reporteCreadoHoy);
      if (checkAgregarDias) {
        checkAgregarDias.disabled = reporteCreadoHoy;
        checkAgregarDias.checked = !reporteCreadoHoy;
      }
    });

    modalResumen?.addEventListener("click", (e) => {
      const toggleBtn = e.target.closest("[data-resumen-group-toggle]");
      if (toggleBtn) {
        const group = toggleBtn.closest("[data-resumen-group]");
        if (!group) return;
        const content = group.querySelector("[data-resumen-group-content]");
        if (!content) return;
        const shouldOpen = content.classList.contains("hidden");
        collapseResumenGroups({ keepGroup: shouldOpen ? group : null });
        return;
      }
      if (
        e.target.classList.contains("modal-backdrop") ||
        e.target.classList.contains("modal-close") ||
        e.target.classList.contains("modal-resumen-close")
      ) {
        modalResumen.classList.add("hidden");
      }
    });

  modalResumenClose?.addEventListener("click", () => modalResumen?.classList.add("hidden"));

  btnGuardarCampos?.addEventListener("click", () => {
    guardarCambios();
  });

  btnFaltanRecaudos?.addEventListener("click", () => {
    // Placeholder para futura lógica
    alert("Marca los recaudos pendientes.");
  });

  checkOtro?.addEventListener("change", () => {
    const show = checkOtro.checked;
    if (resumenOtroText) {
      resumenOtroText.classList.toggle("hidden", !show);
      if (!show) resumenOtroText.value = "";
    }
  });

  btnDatosIncorrectos?.addEventListener("click", () => {
    if (!currentRow) return;
    if (getReportePlatformId(currentRow) !== 9) return;
    resetDatosIncorrectosModalState();
    modalDatosIncorrectos?.classList.remove("hidden");
    updateDatosIncorrectosMessagePreview();
  });

  modalDatosIncorrectos?.addEventListener("click", (e) => {
    if (
      e.target.classList.contains("modal-backdrop") ||
      e.target.classList.contains("modal-close") ||
      e.target.classList.contains("modal-datos-incorrectos-close")
    ) {
      closeDatosIncorrectosModal();
    }
  });

  modalDatosIncorrectosClose?.addEventListener("click", () => closeDatosIncorrectosModal());

  datosIncorrectosImagenInput?.addEventListener("change", () => {
    const file = (datosIncorrectosImagenInput.files || [])[0] || null;
    if (datosIncorrectosImagenPreview) {
      datosIncorrectosImagenPreview.textContent = file ? file.name : "Sin imagen seleccionada.";
    }
    updateDatosIncorrectosMessagePreview();
  });

  datosIncorrectosCheckCorreo?.addEventListener("change", updateDatosIncorrectosMessagePreview);
  datosIncorrectosCheckClave?.addEventListener("change", updateDatosIncorrectosMessagePreview);

  btnEnviarDatosIncorrectos?.addEventListener("click", async () => {
    if (!currentRow) return;
    const reportId = toPositiveId(currentRow?.id_reporte);
    if (!reportId) {
      alert("No se encontró el reporte.");
      return;
    }
    const correoMarcado = datosIncorrectosCheckCorreo?.checked === true;
    const claveMarcada = datosIncorrectosCheckClave?.checked === true;
    if (!correoMarcado && !claveMarcada) {
      alert("Selecciona al menos Correo o Contraseña.");
      return;
    }
    const btn = btnEnviarDatosIncorrectos;
    if (btn) btn.disabled = true;
    try {
      let imageUrl = "";
      const imageFile = (datosIncorrectosImagenInput?.files || [])[0] || null;
      if (imageFile) {
        const reportUserId = toPositiveId(currentRow?.id_usuario) || toPositiveId(requireSession()) || 0;
        imageUrl = await uploadDatosIncorrectosImage(imageFile, reportUserId);
      }
      const waRes = await notifyReporteIncorrectDataWhatsapp(reportId, {
        correo: correoMarcado,
        clave: claveMarcada,
        imagen: imageUrl,
      });
      if (waRes?.error) {
        alert(waRes.error || "No se pudo enviar el mensaje.");
        return;
      }
      alert("Mensaje enviado al cliente.");
      closeDatosIncorrectosModal();
    } catch (err) {
      console.error("enviar datos incorrectos whatsapp error", err);
      alert("No se pudo enviar el mensaje.");
    } finally {
      if (btn) btn.disabled = false;
    }
  });

  btnReemplazar?.addEventListener("click", () => {
    reemplazarServicio();
  });

  modalCorreo?.addEventListener("click", () => {
    if (modalCorreo?.select) modalCorreo.select();
    copyValue(modalCorreo?.value || "", "Correo copiado");
  });

  modalPlatTitle?.addEventListener("click", () => {
    const href = String(modalPlatTitle?.dataset?.href || "").trim();
    if (!href) return;
    window.open(href, "_blank", "noopener,noreferrer");
  });

  modalPlatTitle?.addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const href = String(modalPlatTitle?.dataset?.href || "").trim();
    if (!href) return;
    e.preventDefault();
    window.open(href, "_blank", "noopener,noreferrer");
  });

  btnOpenAdminCuenta?.addEventListener("click", () => {
    openAdminCuentasByCorreo(modalCorreo?.value || "");
  });

  btnCopyClave?.addEventListener("click", () => {
    copyValue(modalClave?.value || "", "Clave copiada");
  });

  btnCopyPin?.addEventListener("click", () => {
    copyValue(modalPin?.value || "", "PIN copiado");
  });

  modalImagenWrapper?.addEventListener("click", () => {
    if (!modalImagenWrapper.classList.contains("downloadable")) return;
    triggerImageDownload();
  });
} catch (err) {
    console.error("revisar reportes error", err);
    if (statusEl) statusEl.textContent = "No se pudieron cargar los reportes.";
  }
}

init();
attachLogout(clearServerSession);

async function guardarCambios() {
  if (!currentRow) return;
  const closedReportId = Number(currentRow?.id_reporte);
  const btn = btnGuardarCampos;
  if (btn) btn.disabled = true;
  try {
    const id_usuario = requireSession();
    const nuevaClave = modalClave?.value || "";
    const nuevoPin = (modalPin?.value || "").trim();
    const updates = [];

    // Actualiza clave de cuenta si cambió
    if (nuevaClave !== oldClave) {
      updates.push(
        supabase.from("cuentas").update({ clave: nuevaClave }).eq("id_cuenta", currentRow.id_cuenta)
      );
    }

    // Actualiza pin si aplica y cambió
    if (currentRow.id_perfil && nuevoPin !== oldPin) {
      const pinNum = nuevoPin === "" ? null : Number(nuevoPin);
      updates.push(
        supabase.from("perfiles").update({ pin: pinNum }).eq("id_perfil", currentRow.id_perfil)
      );
    }

    if (updates.length) {
      const results = await Promise.all(updates);
      const err = results.find((r) => r.error);
      if (err?.error) throw err.error;
    }

    // Construir descripcion/descripción_solucion con cambios primero
    const textos = [];
    if (cambioClave) textos.push("Se actualizó la contraseña");
    if (cambioPin) textos.push("Se actualizó el pin");
    if (checkSuscripcion?.checked) {
      textos.push("La cuenta ya cuenta con suscripción activa. Cierre y vuelva a abrir la aplicación por favor.");
    }
    if (checkPerfiles?.checked) {
      textos.push("Se actualizó el pin de su perfil. Cierre y vuelva a abrir la aplicación por favor.");
    }
    if (checkPinSame?.checked) {
      textos.push("Se volvió a poner el mismo pin al perfil. Cierre y vuelva a abrir la aplicación por favor.");
    }
    if (checkIngreso?.checked) {
      const inventarioLink = buildInventarioLinkByCorreo(
        currentRow?.cuentas?.correo || modalCorreo?.value || "",
      );
      textos.push(
        `Se pudo ingresar sin problemas con los datos actuales de la cuenta. Verifíquelos en ${inventarioLink}.`,
      );
      textos.push("Puedes intentar también cerrar sesión y volver a ingresar a la cuenta con los datos actuales.");
    }
    if (checkSuscripcionActiva?.checked) {
      textos.push("La suscripción de la cuenta está activa.");
    }
    if (checkCodigoAyuda?.checked) {
      textos.push('Presione "obtener ayuda" y luego "usar contraseña".');
    }
    if (checkCodigoWhatsapp?.checked) {
      textos.push("Un admin se comunicará con usted vía WhatsApp.");
    }
    if (checkOtro?.checked && resumenOtroText) {
      const extra = resumenOtroText.value.trim();
      if (extra) textos.push(extra);
    }
    const descripcion_solucion = textos.join("; ");

    const { error } = await supabase
      .from("reportes")
      .update({
        descripcion: descripcion_solucion || null,
        descripcion_solucion,
        en_revision: false,
        solucionado: true,
        solucionado_por: id_usuario,
      })
      .eq("id_reporte", currentRow.id_reporte);
    if (error) throw error;

    const waSolvedRes = await notifyReporteSolvedWhatsappBestEffort(currentRow.id_reporte, {
      row: currentRow,
    });
    if (!waSolvedRes?.ok) {
      console.warn("reporte cerrado sin WhatsApp de solucion", {
        id_reporte: currentRow.id_reporte,
        reason: waSolvedRes?.reason || null,
        error: waSolvedRes?.error || null,
      });
    }

    let diasAgregados = 0;
    const shouldAgregarDias = checkAgregarDias ? checkAgregarDias.checked === true : true;
    if (shouldAgregarDias) {
      try {
        const fechaRes = await applyDiasExtraToVentaFechaCorte(currentRow);
        if (fechaRes.applied && fechaRes.dias >= 1) {
          diasAgregados = Number(fechaRes.dias) || 0;
        }
      } catch (diasErr) {
        console.error("sumar dias fecha_corte reporte cerrado error", diasErr);
      }
    }

    await clearVentaReportadoFlag(currentRow);

    try {
      await notifyReporteCerrado(currentRow, {
        diasAgregados,
        nota: descripcion_solucion,
      });
    } catch (notifErr) {
      console.error("notificacion reporte cerrado error", notifErr);
    }

    alert("Campos guardados y reporte cerrado.");
    removeReporteRowFromUI(closedReportId);
    modalResumen?.classList.add("hidden");
    closeModal();
  } catch (err) {
    console.error("guardar cambios error", err);
    alert("No se pudieron guardar los cambios.");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function reemplazarServicio(options = {}) {
  const rowFromOptions = options?.row || null;
  const silent = options?.silent === true;
  const shouldCloseModal = options?.closeModal !== false;
  const notify = (msg) => {
    if (!silent) alert(msg);
  };
  const targetRow = rowFromOptions || currentRow;
  if (!targetRow) {
    notify("Selecciona un reporte.");
    return { ok: false, reason: "no_row" };
  }
  const selectedRow = targetRow;
  const closedReportId = Number(selectedRow?.id_reporte);

  try {
    const idUsuarioSesion = requireSession();
    await ensureServerSession();

    const plataformaId =
      selectedRow.id_plataforma ??
      selectedRow.cuentas?.id_plataforma ??
      selectedRow.plataformas?.id_plataforma;
    const cuentaId = selectedRow.id_cuenta;
    const rowPerfil = selectedRow.id_perfil
      ? {
          id_perfil: selectedRow.id_perfil,
          n_raw: selectedRow.perfiles?.n_perfil ?? null,
          perfil: selectedRow.perfiles?.n_perfil ? `M${selectedRow.perfiles.n_perfil}` : "",
          hogar: selectedRow.perfiles?.perfil_hogar === true,
          fecha_corte: null,
        }
      : null;

    if (!plataformaId || !cuentaId) {
      notify("Faltan datos de plataforma o cuenta.");
      return { ok: false, reason: "missing_data" };
    }

    const ventaAsociada = await findVentaAsociadaFromReporte(selectedRow);
    const ventaAsociadaId = toPositiveId(ventaAsociada?.id_venta);
    if (!ventaAsociadaId) {
      notify("No se encontró la venta asociada.");
      return { ok: false, reason: "venta_not_found" };
    }

    const { data: ventaInfo, error: ventaErr } = await supabase
      .from("ventas")
      .select(
        "id_venta, id_usuario, fecha_corte, id_precio, id_cuenta, id_cuenta_miembro, id_perfil, correo_miembro, cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(correo)",
      )
      .eq("id_venta", ventaAsociadaId)
      .maybeSingle();
    if (ventaErr || !ventaInfo?.id_venta) {
      notify("No se encontró la venta asociada.");
      return { ok: false, reason: "venta_not_found" };
    }

    const ventaId = ventaInfo.id_venta;

    const ventaPerfil = isTrue(selectedRow.cuentas?.venta_perfil);
    const ventaMiembro = isTrue(selectedRow.cuentas?.venta_miembro);
    const perfilHogar = rowPerfil?.hogar === true;
    const cuentaMadreActualId =
      toPositiveId(ventaInfo?.id_cuenta) ||
      toPositiveId(selectedRow?.cuentas?.id_cuenta_madre) ||
      null;
    const cuentaMiembroVentaId = toPositiveId(ventaInfo?.id_cuenta_miembro);

    let cuentaMadreActualInactiva = false;
    if (cuentaMadreActualId) {
      const { data: cuentaMadreActual, error: cuentaMadreActualErr } = await supabase
        .from("cuentas")
        .select("id_cuenta, inactiva")
        .eq("id_cuenta", cuentaMadreActualId)
        .maybeSingle();
      if (cuentaMadreActualErr) throw cuentaMadreActualErr;
      cuentaMadreActualInactiva = isTrue(cuentaMadreActual?.inactiva);
    }
    const forceCambiarCuentaMadre = !!cuentaMiembroVentaId && cuentaMadreActualInactiva;

    const loadReemplazosBloqueados = async () => {
      const { data, error } = await supabase.from("reemplazos").select("id_cuenta, id_perfil");
      if (error) throw error;
      const cuentas = new Set();
      const perfiles = new Set();
      (data || []).forEach((row) => {
        const cuentaId = toPositiveId(row?.id_cuenta);
        const perfilId = toPositiveId(row?.id_perfil);
        if (cuentaId) cuentas.add(cuentaId);
        if (perfilId) perfiles.add(perfilId);
      });
      return { cuentas, perfiles };
    };

    const reemplazosBloqueados = await loadReemplazosBloqueados();

    const findPerfilLibre = async (platId, isHogar, excludeCuenta) => {
      let query = supabase
        .from("perfiles")
        .select(
          "id_perfil, perfil_hogar, id_cuenta, pin, n_perfil, ocupado, cuentas:cuentas!perfiles_id_cuenta_fkey!inner(id_plataforma, inactiva, correo, clave)",
        )
        .eq("cuentas.id_plataforma", platId)
        .eq("ocupado", false)
        .order("id_perfil", { ascending: true });
      if (isHogar === true) {
        query = query.eq("perfil_hogar", true);
      } else {
        query = query.or("perfil_hogar.is.null,perfil_hogar.eq.false");
      }
      query = query.or("inactiva.is.null,inactiva.eq.false", {
        foreignTable: "cuentas",
      });
      if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
      const { data, error } = await query;
      if (error) return { error };
      const libre = (data || []).find((perfil) => {
        const perfilId = toPositiveId(perfil?.id_perfil);
        const cuentaId = toPositiveId(perfil?.id_cuenta);
        return (
          !!perfilId &&
          !!cuentaId &&
          !reemplazosBloqueados.perfiles.has(perfilId) &&
          !reemplazosBloqueados.cuentas.has(cuentaId)
        );
      });
      return { data: libre || null };
    };

    const findPerfilLibreCuentaMadre = async (platId, isHogar, excludeCuenta) => {
      let query = supabase
        .from("perfiles")
        .select(
          "id_perfil, perfil_hogar, id_cuenta, pin, n_perfil, ocupado, cuentas:cuentas!perfiles_id_cuenta_fkey!inner(id_cuenta, id_plataforma, cuenta_madre, inactiva, correo, clave)",
        )
        .eq("cuentas.id_plataforma", platId)
        .eq("cuentas.cuenta_madre", true)
        .eq("ocupado", false)
        .order("id_perfil", { ascending: true });
      if (isHogar === true) {
        query = query.eq("perfil_hogar", true);
      } else {
        query = query.or("perfil_hogar.is.null,perfil_hogar.eq.false");
      }
      query = query.or("inactiva.is.null,inactiva.eq.false", {
        foreignTable: "cuentas",
      });
      if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
      const { data, error } = await query;
      if (error) return { error };
      const libre = (data || []).find((perfil) => {
        const perfilId = toPositiveId(perfil?.id_perfil);
        const cuentaId = toPositiveId(perfil?.id_cuenta);
        return (
          !!perfilId &&
          !!cuentaId &&
          !reemplazosBloqueados.perfiles.has(perfilId) &&
          !reemplazosBloqueados.cuentas.has(cuentaId)
        );
      });
      return { data: libre || null };
    };

    const findCuentaMiembroLibre = async (platId, excludeCuenta) => {
      let query = supabase
        .from("cuentas")
        .select("id_cuenta, correo, clave, inactiva, ocupado, venta_perfil, venta_miembro")
        .eq("id_plataforma", platId)
        .eq("venta_perfil", false)
        .eq("venta_miembro", true)
        .eq("ocupado", false)
        .order("id_cuenta", { ascending: true });
      query = query.or("inactiva.is.null,inactiva.eq.false");
      if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
      const { data, error } = await query;
      if (error) return { error };
      const libre = (data || []).find((cuenta) => {
        const cuentaId = toPositiveId(cuenta?.id_cuenta);
        return !!cuentaId && !reemplazosBloqueados.cuentas.has(cuentaId);
      });
      return { data: libre || null };
    };

    const findCuentaCompletaLibre = async (platId, excludeCuenta) => {
      let query = supabase
        .from("cuentas")
        .select("id_cuenta, correo, clave, inactiva, ocupado, venta_perfil, venta_miembro")
        .eq("id_plataforma", platId)
        .eq("venta_perfil", false)
        .eq("venta_miembro", false)
        .or("ocupado.is.null,ocupado.eq.false")
        .order("id_cuenta", { ascending: true });
      query = query.or("inactiva.is.null,inactiva.eq.false");
      if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
      const { data, error } = await query;
      if (error) return { error };
      const libre = (data || []).find((cuenta) => {
        const cuentaId = toPositiveId(cuenta?.id_cuenta);
        return !!cuentaId && !reemplazosBloqueados.cuentas.has(cuentaId);
      });
      return { data: libre || null };
    };

    let nuevoCuenta = null;
    let nuevoPerfil = null;
    let dataDestino = {};
    let destinoEsCuentaMiembro = false;
    let asignadoDesdeCuentaMadre = false;
    const isPlanMiembroEquivalente = (ventaMiembro && !ventaPerfil) || perfilHogar;

    const tryAsignarPerfilCuentaMadre = async (isHogar) => {
      const { data: perfilDestino, error: perfilErr } = await findPerfilLibreCuentaMadre(
        plataformaId,
        isHogar,
        cuentaMadreActualId || cuentaId,
      );
      if (perfilErr) throw perfilErr;
      if (!perfilDestino) return false;
      nuevoPerfil = perfilDestino.id_perfil;
      nuevoCuenta = perfilDestino.id_cuenta;
      dataDestino = {
        correo: perfilDestino.cuentas?.correo || "",
        clave: perfilDestino.cuentas?.clave || "",
        pin: perfilDestino.pin,
        n_perfil: perfilDestino.n_perfil,
      };
      destinoEsCuentaMiembro = false;
      asignadoDesdeCuentaMadre = true;
      return true;
    };

    const tryAsignarPerfil = async (isHogar) => {
      const { data: perfilDestino, error: perfilErr } = await findPerfilLibre(
        plataformaId,
        isHogar,
        cuentaId,
      );
      if (perfilErr) throw perfilErr;
      if (!perfilDestino) return false;
      nuevoPerfil = perfilDestino.id_perfil;
      nuevoCuenta = perfilDestino.id_cuenta;
      dataDestino = {
        correo: perfilDestino.cuentas?.correo || "",
        clave: perfilDestino.cuentas?.clave || "",
        pin: perfilDestino.pin,
        n_perfil: perfilDestino.n_perfil,
      };
      destinoEsCuentaMiembro = false;
      return true;
    };

    const tryAsignarCuentaMiembro = async () => {
      const { data: cuentaDestino, error: cuentaErr } = await findCuentaMiembroLibre(
        plataformaId,
        cuentaId,
      );
      if (cuentaErr) throw cuentaErr;
      if (!cuentaDestino) return false;
      nuevoPerfil = null;
      nuevoCuenta = cuentaDestino.id_cuenta;
      dataDestino = {
        correo: cuentaDestino.correo || "",
        clave: cuentaDestino.clave || "",
      };
      destinoEsCuentaMiembro = true;
      return true;
    };

    let assigned = false;
    if (forceCambiarCuentaMadre) {
      assigned = await tryAsignarPerfilCuentaMadre(perfilHogar);
      if (!assigned && perfilHogar !== true) {
        assigned = await tryAsignarPerfilCuentaMadre(true);
      }
      if (!assigned) {
        notify("Sin stock de cuentas madre activas.");
        return { ok: false, reason: "sin_stock_cuenta_madre" };
      }
    } else {
      if (rowPerfil?.id_perfil) {
        assigned = await tryAsignarPerfil(perfilHogar);
        if (!assigned && isPlanMiembroEquivalente) {
          assigned = await tryAsignarCuentaMiembro();
        }
      } else if (ventaMiembro && !ventaPerfil) {
        assigned = await tryAsignarCuentaMiembro();
        if (!assigned) {
          assigned = await tryAsignarPerfil(true);
        }
      }
    }

    if (!assigned && !(ventaMiembro && !ventaPerfil) && !rowPerfil?.id_perfil) {
      const { data: cuentaCompletaDestino, error: cuentaCompletaErr } = await findCuentaCompletaLibre(
        plataformaId,
        cuentaId,
      );
      if (cuentaCompletaErr) throw cuentaCompletaErr;
      if (cuentaCompletaDestino) {
        nuevoPerfil = null;
        nuevoCuenta = cuentaCompletaDestino.id_cuenta;
        dataDestino = {
          correo: cuentaCompletaDestino.correo || "",
          clave: cuentaCompletaDestino.clave || "",
        };
        destinoEsCuentaMiembro = false;
        assigned = true;
      }
    }

    if (!assigned) {
      notify("Sin stock");
      return { ok: false, reason: "sin_stock" };
    }

    const { error: updVentaErr } = await supabase
      .from("ventas")
      .update({
        id_cuenta: nuevoCuenta || null,
        id_perfil: nuevoPerfil || null,
        id_sub_cuenta: null,
        id_precio: ventaInfo?.id_precio ?? null,
      })
      .eq("id_venta", ventaId);
    if (updVentaErr) throw updVentaErr;
    if (asignadoDesdeCuentaMadre && cuentaMiembroVentaId && nuevoCuenta) {
      const { error: updMiembroErr } = await supabase
        .from("cuentas")
        .update({ id_cuenta_madre: nuevoCuenta, inactiva: false })
        .eq("id_cuenta", cuentaMiembroVentaId);
      if (updMiembroErr) throw updMiembroErr;
    }

    const perfilAnteriorId = toPositiveId(rowPerfil?.id_perfil);
    let perfilTieneOtraVenta = false;
    if (perfilAnteriorId) {
      const { data: perfilVentasRestantes, error: perfilVentasErr } = await supabase
        .from("ventas")
        .select("id_venta")
        .eq("id_perfil", perfilAnteriorId)
        .neq("id_venta", ventaId)
        .limit(1);
      if (perfilVentasErr) throw perfilVentasErr;
      perfilTieneOtraVenta = (perfilVentasRestantes || []).length > 0;
    }

    if (perfilAnteriorId && !perfilTieneOtraVenta) {
      const { error: freeErr } = await supabase
        .from("perfiles")
        .update({ ocupado: false })
        .eq("id_perfil", perfilAnteriorId);
      if (freeErr) console.error("[reemplazo] liberar perfil previo error", freeErr);
    } else if (perfilAnteriorId && perfilTieneOtraVenta) {
      const { error: keepOccErr } = await supabase
        .from("perfiles")
        .update({ ocupado: true })
        .eq("id_perfil", perfilAnteriorId);
      if (keepOccErr) console.error("[reemplazo] mantener perfil previo ocupado error", keepOccErr);
    }
    if (nuevoPerfil) {
      const { error: occErr } = await supabase
        .from("perfiles")
        .update({ ocupado: true })
        .eq("id_perfil", nuevoPerfil);
      if (occErr) console.error("[reemplazo] marcar perfil nuevo error", occErr);
    }
    if (nuevoCuenta) {
      const { error: occCuentaErr } = await supabase
        .from("cuentas")
        .update({ ocupado: true })
        .eq("id_cuenta", nuevoCuenta);
      if (occCuentaErr) console.error("[reemplazo] marcar cuenta nueva error", occCuentaErr);
    }

    const debeRegistrarReemplazo =
      Number(plataformaId) !== 9 && !(perfilAnteriorId && perfilTieneOtraVenta);
    if (debeRegistrarReemplazo) {
      await supabase.from("reemplazos").insert({
        id_cuenta: cuentaId,
        id_perfil: perfilAnteriorId || null,
        id_sub_cuenta: null,
        id_venta: ventaId,
      });
    }

    const { data: ventaEstadoRow, error: ventaEstadoErr } = await supabase
      .from("ventas")
      .select("pendiente")
      .eq("id_venta", ventaId)
      .maybeSingle();
    if (ventaEstadoErr) throw ventaEstadoErr;
    const mantenerReporteAbiertoPlat9 =
      Number(plataformaId) === 9 && ventaEstadoRow?.pendiente !== false;

    let diasSumados = 0;
    if (!mantenerReporteAbiertoPlat9) {
      try {
        const fechaRes = await applyDiasExtraToVentaFechaCorte(selectedRow, ventaInfo);
        diasSumados = Number(fechaRes?.dias || 0);
      } catch (diasErr) {
        console.error("sumar dias fecha_corte en reemplazo error", diasErr);
      }
      await clearVentaReportadoFlag(selectedRow, ventaInfo);
    } else {
      const { error: keepReportadoErr } = await supabase
        .from("ventas")
        .update({ reportado: true })
        .eq("id_venta", ventaId);
      if (keepReportadoErr) throw keepReportadoErr;
    }

    try {
      const userIds = pickNotificationUserIds("servicio_reemplazado", {
        ventaUserId: ventaInfo?.id_usuario,
      });
      const normalizeCorreo = (value) => String(value || "").trim().toLowerCase();
      let correoViejoNotif = selectedRow.cuentas?.correo || "";
      let correoNuevoNotif = dataDestino.correo || "";
      let shouldNotifyReemplazo = true;
      if (Number(plataformaId) === 9) {
        const oldMemberCorreo = String(
          ventaInfo?.correo_miembro ||
            ventaInfo?.cuentas_miembro?.correo ||
            selectedRow?.cuentas?.correo ||
            "",
        ).trim();
        const newMemberCorreo = destinoEsCuentaMiembro ? String(dataDestino?.correo || "").trim() : "";
        const oldMemberNorm = normalizeCorreo(oldMemberCorreo);
        const newMemberNorm = normalizeCorreo(newMemberCorreo);
        shouldNotifyReemplazo = Boolean(
          destinoEsCuentaMiembro && oldMemberNorm && newMemberNorm && oldMemberNorm !== newMemberNorm,
        );
        correoViejoNotif = oldMemberCorreo;
        correoNuevoNotif = newMemberCorreo;
      }
      if (userIds.length && shouldNotifyReemplazo) {
        await notifyReemplazoReporte({
          row: { ...selectedRow, id_usuario: userIds[0] },
          plataforma: formatPlataformaReemplazo({
            plataforma: selectedRow.plataformas?.nombre || "",
            idPrecio: ventaInfo?.id_precio ?? null,
            perfilHogar: rowPerfil?.hogar === true,
          }),
          correoViejo: correoViejoNotif,
          correoNuevo: correoNuevoNotif,
          dias: diasSumados,
        });
      }
    } catch (nErr) {
      console.error("notificacion servicio_reemplazado error", nErr);
    }

    if (mantenerReporteAbiertoPlat9) {
      const { error: keepOpenRepErr } = await supabase
        .from("reportes")
        .update({
          en_revision: true,
          solucionado: false,
          solucionado_por: null,
          descripcion_solucion: null,
        })
        .eq("id_reporte", selectedRow.id_reporte);
      if (keepOpenRepErr) throw keepOpenRepErr;

      if (selectedRow?.cuentas && typeof selectedRow.cuentas === "object") {
        selectedRow.cuentas.inactiva = false;
      }
      if (dataDestino?.correo) {
        selectedRow.cuenta_madre_correo = dataDestino.correo;
      }
      selectedRow.cuenta_madre_inactiva = false;
      selectedRow.cuenta_inactiva_resuelta = false;
      selectedRow.cuenta_tiene_madre = true;
      selectedRow._reemplazo_activo = true;
      if (selectedRow?.id_reporte) {
        reportesById.set(String(selectedRow.id_reporte), selectedRow);
      }
      rerenderActivosDesdeMap();

      notify("Reemplazo realizado. El reporte seguirá abierto hasta que pendiente sea false.");
      if (shouldCloseModal) closeModal();
      return { ok: true, kept_open: true };
    }

    const correoNuevoReemplazo = String(dataDestino?.correo || "").trim();
    const inventarioLinkReemplazo = buildInventarioLinkByCorreo(correoNuevoReemplazo);
    const descripcionSolucion = correoNuevoReemplazo
      ? `Su servicio fue reemplazado a una cuenta funcional. Verifique sus nuevos datos en: ${inventarioLinkReemplazo}`
      : "Su servicio fue reemplazado a una cuenta funcional.";
    const { error: repErr } = await supabase
      .from("reportes")
      .update({
        descripcion: descripcionSolucion,
        descripcion_solucion: descripcionSolucion,
        en_revision: false,
        solucionado: true,
        solucionado_por: idUsuarioSesion,
      })
      .eq("id_reporte", selectedRow.id_reporte);
    if (repErr) throw repErr;

    const waSolvedRes = await notifyReporteSolvedWhatsappBestEffort(selectedRow.id_reporte, {
      row: selectedRow,
      ventaId,
      pendingState: ventaEstadoRow?.pendiente,
    });
    if (!waSolvedRes?.ok) {
      console.warn("reemplazo cerrado sin WhatsApp de solucion", {
        id_reporte: selectedRow.id_reporte,
        reason: waSolvedRes?.reason || null,
        error: waSolvedRes?.error || null,
      });
    }

    try {
      await notifyReporteCerrado(selectedRow, {
        diasAgregados: diasSumados,
        nota: descripcionSolucion,
      });
    } catch (notifErr) {
      console.error("notificacion reporte cerrado error", notifErr);
    }

    notify("Reemplazo realizado.");
    removeReporteRowFromUI(closedReportId);
    if (shouldCloseModal) closeModal();
    return { ok: true };
  } catch (err) {
    console.error("reemplazo reporte error", err);
    notify("No se pudo reemplazar.");
    return { ok: false, reason: "error", error: err };
  }
}

async function autoReplaceInactiveReportes(reportes = []) {
  if (!autoReemplazoCuentaInactivaEnabled) return false;
  const targets = (reportes || []).filter((r) => isTrue(r?.cuenta_inactiva_resuelta));
  if (!targets.length) return false;

  for (const row of targets) {
    try {
      await reemplazarServicio({ row, silent: true, closeModal: false });
    } catch (err) {
      console.error("auto reemplazo por cuenta inactiva error", err);
    }
  }
  return true;
}
