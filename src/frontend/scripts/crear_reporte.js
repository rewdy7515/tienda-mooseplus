import {
  requireSession,
  attachLogout,
  getSessionRoles,
  setSessionRoles,
  attachLogoHome,
} from "./session.js";
import {
  clearServerSession,
  loadCurrentUser,
  supabase,
  uploadComprobantes,
} from "./api.js";
import { buildNotificationPayload, pickNotificationUserIds } from "./notification-templates.js";

requireSession();

const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const getCaracasDateTime = () => {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value || "0000";
  const month = parts.find((part) => part.type === "month")?.value || "00";
  const day = parts.find((part) => part.type === "day")?.value || "00";
  const hour = parts.find((part) => part.type === "hour")?.value || "00";
  const minute = parts.find((part) => part.type === "minute")?.value || "00";
  const second = parts.find((part) => part.type === "second")?.value || "00";
  return {
    fecha: `${year}-${month}-${day}`,
    hora: `${hour}:${minute}:${second}`,
  };
};
const selectPlataforma = document.querySelector("#select-plataforma");
const selectMotivo = document.querySelector("#select-motivo");
const selectPerfil = document.querySelector("#select-perfil");
const perfilWrapper = document.querySelector("#perfil-wrapper");
const correoPerfilGrid = document.querySelector("#correo-perfil-grid");
const inputCorreoCuenta = document.querySelector("#input-correo-cuenta");
const correoAvisoEl = document.querySelector("#correo-aviso");
const correoSugerenciasEl = document.querySelector("#correo-sugerencias");
const motivoOtroWrapper = document.querySelector("#motivo-otro-wrapper");
const motivoExtraWrapper = document.querySelector("#motivo-extra-wrapper");
const inputMotivoOtro = document.querySelector("#input-motivo-otro");
const inputMotivoExtra = document.querySelector("#input-motivo-extra");
const chkNoScreenshot = document.querySelector("#chk-no-screenshot");
const btnAddImage = document.querySelector("#btn-add-image");
const inputFiles = document.querySelector("#input-files");
const dropzone = document.querySelector("#dropzone");
const filePreview = document.querySelector("#file-preview");
const form = document.querySelector("#reporte-form");

let selectedCuentaId = null;
let selectedPerfilId = null;
let selectedPlataformaId = null;
let cuentasPorPlataforma = new Map();
let plataformasMeta = new Map();
let tiposReporteCatalog = [];
let correosSugerenciasVisibles = [];
const AUTO_REEMPLAZO_CFG_KEY = "auto_reemplazo_cuenta_inactiva";
const queryParams = new URLSearchParams(window.location.search);
const prefillPlataforma = queryParams.get("plataforma");
const prefillCuenta = queryParams.get("cuenta");
const prefillPerfil = queryParams.get("perfil");
const prefillCorreo = queryParams.get("correo");
const normalizeMotivo = (val) =>
  String(val || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
const isOtroMotivo = (val) => {
  const n = normalizeMotivo(val);
  return n.startsWith("otro") || n.startsWith("otra");
};
const getSelectedMotivoTitle = () => {
  const opt = selectMotivo?.selectedOptions?.[0];
  return String(opt?.dataset?.titulo || opt?.textContent || "").trim();
};

const sanitizeFileName = (name) => {
  const base = name?.split?.("/")?.pop?.() || "archivo";
  return base.replace(/[^a-zA-Z0-9._-]/g, "_");
};

const asInt = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const getPlataformaMeta = (plataformaId = selectedPlataformaId) => {
  const key = String(plataformaId || "").trim();
  if (!key) return null;
  return plataformasMeta.get(key) || null;
};

const isPlataformaPorAcceso = (plataformaId = selectedPlataformaId) =>
  isTrue(getPlataformaMeta(plataformaId)?.por_acceso);

async function isAutoReemplazoCuentaInactivaEnabled() {
  try {
    const { data, error } = await supabase
      .from("configuracion_sistema")
      .select("valor_bool")
      .eq("clave", AUTO_REEMPLAZO_CFG_KEY)
      .maybeSingle();
    if (error) throw error;
    return data?.valor_bool === true;
  } catch (err) {
    console.error("load auto reemplazo config error", err);
    return true;
  }
}

async function findVentaAsociadaParaReporte({ idUsuario, idCuenta, idPerfil }) {
  const userIdNum = Number(idUsuario);
  const cuentaIdNum = Number(idCuenta);
  if (!Number.isFinite(userIdNum) || userIdNum <= 0) return null;
  if (!Number.isFinite(cuentaIdNum) || cuentaIdNum <= 0) return null;

  let query = supabase
    .from("ventas")
    .select("id_venta, reportado, id_perfil")
    .eq("id_usuario", userIdNum)
    .or(`id_cuenta.eq.${cuentaIdNum},id_cuenta_miembro.eq.${cuentaIdNum}`)
    .order("id_venta", { ascending: false })
    .limit(1);

  const idPerfilNum = Number(idPerfil);
  if (Number.isFinite(idPerfilNum) && idPerfilNum > 0) {
    query = query.eq("id_perfil", idPerfilNum);
  }

  const { data, error } = await query;
  if (error) throw error;
  const found = data?.[0] || null;
  if (found) return found;

  if (Number.isFinite(idPerfilNum) && idPerfilNum > 0) {
    const { data: fallbackData, error: fallbackErr } = await supabase
      .from("ventas")
      .select("id_venta, reportado, id_perfil")
      .eq("id_usuario", userIdNum)
      .or(`id_cuenta.eq.${cuentaIdNum},id_cuenta_miembro.eq.${cuentaIdNum}`)
      .order("id_venta", { ascending: false })
      .limit(1);
    if (fallbackErr) throw fallbackErr;
    return fallbackData?.[0] || null;
  }

  return null;
}

async function markVentaPendienteByReporteRule({
  ventaId,
  idUsuario,
  idCuenta,
  idPerfil,
  idPlataforma,
  motivoTipoId,
}) {
  const motivoIdNum = Number(motivoTipoId);
  if (motivoIdNum !== 3) return;
  const plataformaIdNum = Number(idPlataforma);
  const cuentaIdNum = Number(idCuenta);
  if (!Number.isFinite(plataformaIdNum) || plataformaIdNum <= 0) return;
  if (!Number.isFinite(cuentaIdNum) || cuentaIdNum <= 0) return;

  const { data: plataformaData, error: plataformaErr } = await supabase
    .from("plataformas")
    .select("entrega_inmediata")
    .eq("id_plataforma", plataformaIdNum)
    .maybeSingle();
  if (plataformaErr) throw plataformaErr;
  if (isTrue(plataformaData?.entrega_inmediata)) return;

  const ventaIdNum = Number(ventaId);
  let ventaFinalId = Number.isFinite(ventaIdNum) && ventaIdNum > 0 ? ventaIdNum : null;
  if (!ventaFinalId) {
    const ventaInfo = await findVentaAsociadaParaReporte({
      idUsuario,
      idCuenta: cuentaIdNum,
      idPerfil,
    });
    const foundVentaId = Number(ventaInfo?.id_venta);
    if (!Number.isFinite(foundVentaId) || foundVentaId <= 0) return;
    ventaFinalId = foundVentaId;
  }

  const { error: updErr } = await supabase
    .from("ventas")
    .update({ pendiente: true })
    .eq("id_venta", ventaFinalId);
  if (updErr) throw updErr;
}

const getCuentasDePlataformaSeleccionada = () => {
  const key = String(selectedPlataformaId || "").trim();
  if (!key) return [];
  const cuentasMap = cuentasPorPlataforma.get(key);
  if (!cuentasMap) return [];
  return Array.from(cuentasMap.entries()).map(([idCuenta, correo]) => ({
    idCuenta: Number(idCuenta),
    correo: String(correo || "").trim(),
  }));
};

const resolveCuentaIdByCorreo = (correoRaw) => {
  const key = String(selectedPlataformaId || "").trim();
  if (!key) return null;
  const cuentasMap = cuentasPorPlataforma.get(key);
  if (!cuentasMap) return null;
  const value = String(correoRaw || "").trim().toLowerCase();
  if (!value) return null;
  const entries = Array.from(cuentasMap.entries());
  const exact = entries.find(([, correo]) => String(correo || "").trim().toLowerCase() === value);
  if (exact) return exact[0];
  const partials = entries.filter(([, correo]) =>
    String(correo || "").trim().toLowerCase().includes(value)
  );
  if (partials.length === 1) return partials[0][0];
  return null;
};

const resolveCuentaIdByCorreoRobusto = async (correoRaw) => {
  const fromMap = resolveCuentaIdByCorreo(correoRaw);
  if (fromMap) return Number(fromMap);

  const correo = String(correoRaw || "").trim();
  const plataformaId = Number(selectedPlataformaId);
  const userId = requireSession();
  if (!correo || !Number.isFinite(plataformaId) || plataformaId <= 0 || !userId) {
    return null;
  }

  const { data: cuentasRows, error: cuentasErr } = await supabase
    .from("cuentas")
    .select("id_cuenta")
    .eq("id_plataforma", plataformaId)
    .ilike("correo", correo)
    .limit(20);
  if (cuentasErr) {
    console.error("resolve cuenta por correo error", cuentasErr);
    return null;
  }

  const candidateIds = Array.from(
    new Set(
      (cuentasRows || [])
        .map((r) => Number(r?.id_cuenta))
        .filter((id) => Number.isFinite(id) && id > 0),
    ),
  );
  if (!candidateIds.length) return null;

  const idsCsv = candidateIds.join(",");
  const { data: ventasRows, error: ventasErr } = await supabase
    .from("ventas")
    .select("id_cuenta, id_cuenta_miembro")
    .eq("id_usuario", userId)
    .or(`id_cuenta.in.(${idsCsv}),id_cuenta_miembro.in.(${idsCsv})`)
    .limit(50);
  if (ventasErr) {
    console.error("resolve cuenta desde ventas error", ventasErr);
    return null;
  }

  const vinculadas = new Set();
  (ventasRows || []).forEach((row) => {
    const main = Number(row?.id_cuenta);
    const member = Number(row?.id_cuenta_miembro);
    if (Number.isFinite(main) && main > 0) vinculadas.add(main);
    if (Number.isFinite(member) && member > 0) vinculadas.add(member);
  });

  for (const id of candidateIds) {
    if (vinculadas.has(id)) return id;
  }
  return null;
};

const hideCorreoSugerencias = () => {
  correosSugerenciasVisibles = [];
  if (!correoSugerenciasEl) return;
  correoSugerenciasEl.innerHTML = "";
  correoSugerenciasEl.classList.add("hidden");
};

const hideCorreoAviso = () => {
  if (!correoAvisoEl) return;
  correoAvisoEl.textContent = "";
  correoAvisoEl.classList.add("hidden");
};

const showCorreoAviso = (msg) => {
  if (!correoAvisoEl) return;
  correoAvisoEl.textContent = String(msg || "").trim();
  correoAvisoEl.classList.toggle("hidden", !correoAvisoEl.textContent);
};

const getCuentaDisponibleByCorreo = (correoRaw) => {
  const correo = String(correoRaw || "").trim().toLowerCase();
  if (!correo) return null;
  const cuentas = getCuentasDePlataformaSeleccionada();
  const exact = cuentas.find((item) => String(item.correo || "").trim().toLowerCase() === correo);
  if (exact) return exact;
  const partials = cuentas.filter((item) =>
    String(item.correo || "").trim().toLowerCase().includes(correo)
  );
  return partials.length === 1 ? partials[0] : null;
};

const renderCorreoSugerencias = (termRaw = "") => {
  if (!correoSugerenciasEl) return;
  const term = String(termRaw || "").trim().toLowerCase();
  const cuentas = getCuentasDePlataformaSeleccionada();
  const filtered = cuentas
    .filter((item) => {
      if (!item.correo) return false;
      if (!term) return true;
      return item.correo.toLowerCase().includes(term);
    })
    .slice(0, 10);
  correosSugerenciasVisibles = filtered;
  if (!filtered.length) {
    hideCorreoSugerencias();
    return;
  }
  correoSugerenciasEl.innerHTML = filtered
    .map(
      (item, idx) =>
        `<button type="button" class="correo-sugerencia-item" data-sugerencia-idx="${idx}">${item.correo}</button>`
    )
    .join("");
  correoSugerenciasEl.classList.remove("hidden");
};

const findPerfilLibre = async (plataformaId, perfilHogar, excludeCuenta) => {
  let query = supabase
    .from("perfiles")
    .select(
      "id_perfil, perfil_hogar, id_cuenta, pin, n_perfil, ocupado, cuentas!perfiles_id_cuenta_fkey!inner(id_plataforma, inactiva, correo, clave)",
    )
    .eq("cuentas.id_plataforma", plataformaId)
    .eq("ocupado", false)
    .order("id_perfil", { ascending: true })
    .limit(1);
  if (perfilHogar === true) {
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
  return { data: data?.[0] || null };
};

const findCuentaMiembroLibre = async (plataformaId, excludeCuenta) => {
  let query = supabase
    .from("cuentas")
    .select("id_cuenta, correo, clave, inactiva, ocupado, venta_perfil, venta_miembro")
    .eq("id_plataforma", plataformaId)
    .eq("venta_perfil", false)
    .eq("venta_miembro", true)
    .eq("ocupado", false)
    .order("id_cuenta", { ascending: true })
    .limit(1);
  query = query.or("inactiva.is.null,inactiva.eq.false");
  if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
  const { data, error } = await query;
  if (error) return { error };
  return { data: data?.[0] || null };
};

const findCuentaCompletaLibre = async (plataformaId, excludeCuenta) => {
  let query = supabase
    .from("cuentas")
    .select("id_cuenta, correo, clave, inactiva, ocupado, venta_perfil, venta_miembro")
    .eq("id_plataforma", plataformaId)
    .eq("venta_perfil", false)
    .eq("venta_miembro", false)
    .eq("inactiva", false)
    .order("id_cuenta", { ascending: true })
    .limit(1);
  query = query.or("ocupado.is.null,ocupado.eq.false");
  if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
  const { data, error } = await query;
  if (error) return { error };
  return { data: data?.[0] || null };
};

async function intentarReemplazoAutomaticoCuentaInactiva({
  idUsuario,
  idCuenta,
  idPerfil,
  idPlataforma,
}) {
  if (!idUsuario || !idCuenta) {
    return { reemplazado: false, aplica: false, sinStock: false };
  }

  const { data: cuentaActual, error: cuentaErr } = await supabase
    .from("cuentas")
    .select("id_cuenta, correo, clave, id_plataforma, inactiva, venta_perfil, venta_miembro")
    .eq("id_cuenta", idCuenta)
    .maybeSingle();
  if (cuentaErr) throw cuentaErr;
  if (!cuentaActual || cuentaActual.inactiva !== true) {
    return { reemplazado: false, aplica: false, sinStock: false };
  }

  const plataformaId = asInt(idPlataforma || cuentaActual.id_plataforma);
  if (!plataformaId) {
    return { reemplazado: false, aplica: true, sinStock: true };
  }

  let ventaQuery = supabase
    .from("ventas")
    .select("id_venta, id_usuario, id_cuenta, id_perfil")
    .eq("id_usuario", idUsuario)
    .eq("id_cuenta", idCuenta)
    .order("id_venta", { ascending: false })
    .limit(1);
  if (idPerfil) {
    ventaQuery = ventaQuery.eq("id_perfil", idPerfil);
  } else {
    ventaQuery = ventaQuery.is("id_perfil", null);
  }
  const { data: ventaRows, error: ventaErr } = await ventaQuery;
  if (ventaErr) throw ventaErr;
  let ventaInfo = ventaRows?.[0] || null;
  if (!ventaInfo) {
    const { data: fallbackRows, error: fallbackErr } = await supabase
      .from("ventas")
      .select("id_venta, id_usuario, id_cuenta, id_perfil")
      .eq("id_usuario", idUsuario)
      .eq("id_cuenta", idCuenta)
      .order("id_venta", { ascending: false })
      .limit(1);
    if (fallbackErr) throw fallbackErr;
    ventaInfo = fallbackRows?.[0] || null;
  }
  if (!ventaInfo?.id_venta) {
    return { reemplazado: false, aplica: true, sinStock: true };
  }

  let perfilActual = null;
  if (idPerfil) {
    const { data: perfilData, error: perfilErr } = await supabase
      .from("perfiles")
      .select("id_perfil, n_perfil, perfil_hogar")
      .eq("id_perfil", idPerfil)
      .maybeSingle();
    if (perfilErr) throw perfilErr;
    perfilActual = perfilData || null;
  }

  let nuevoCuenta = null;
  let nuevoPerfil = null;
  let dataDestino = {};

  if (idPerfil) {
    const { data: perfilDestino, error: perfilDestinoErr } = await findPerfilLibre(
      plataformaId,
      perfilActual?.perfil_hogar === true,
      idCuenta,
    );
    if (perfilDestinoErr) throw perfilDestinoErr;
    if (!perfilDestino) {
      return { reemplazado: false, aplica: true, sinStock: true };
    }
    nuevoCuenta = asInt(perfilDestino.id_cuenta);
    nuevoPerfil = asInt(perfilDestino.id_perfil);
    dataDestino = {
      correo: perfilDestino.cuentas?.correo || "",
      clave: perfilDestino.cuentas?.clave || "",
      n_perfil: perfilDestino.n_perfil,
    };
  } else if (isTrue(cuentaActual.venta_miembro) && !isTrue(cuentaActual.venta_perfil)) {
    const { data: cuentaDestino, error: cuentaDestinoErr } = await findCuentaMiembroLibre(
      plataformaId,
      idCuenta,
    );
    if (cuentaDestinoErr) throw cuentaDestinoErr;
    if (!cuentaDestino) {
      return { reemplazado: false, aplica: true, sinStock: true };
    }
    nuevoCuenta = asInt(cuentaDestino.id_cuenta);
    nuevoPerfil = null;
    dataDestino = {
      correo: cuentaDestino.correo || "",
      clave: cuentaDestino.clave || "",
      n_perfil: null,
    };
  } else {
    const { data: cuentaDestino, error: cuentaDestinoErr } = await findCuentaCompletaLibre(
      plataformaId,
      idCuenta,
    );
    if (cuentaDestinoErr) throw cuentaDestinoErr;
    if (!cuentaDestino) {
      return { reemplazado: false, aplica: true, sinStock: true };
    }
    nuevoCuenta = asInt(cuentaDestino.id_cuenta);
    nuevoPerfil = null;
    dataDestino = {
      correo: cuentaDestino.correo || "",
      clave: cuentaDestino.clave || "",
      n_perfil: null,
    };
  }

  if (!nuevoCuenta) {
    return { reemplazado: false, aplica: true, sinStock: true };
  }

  const { error: updVentaErr } = await supabase
    .from("ventas")
    .update({ id_cuenta: nuevoCuenta, id_perfil: nuevoPerfil, id_sub_cuenta: null })
    .eq("id_venta", ventaInfo.id_venta);
  if (updVentaErr) throw updVentaErr;

  if (perfilActual?.id_perfil) {
    const { error: freePerfilErr } = await supabase
      .from("perfiles")
      .update({ ocupado: false })
      .eq("id_perfil", perfilActual.id_perfil);
    if (freePerfilErr) console.error("auto reemplazo liberar perfil error", freePerfilErr);
  }
  if (nuevoPerfil) {
    const { error: occPerfilErr } = await supabase
      .from("perfiles")
      .update({ ocupado: true })
      .eq("id_perfil", nuevoPerfil);
    if (occPerfilErr) console.error("auto reemplazo ocupar perfil error", occPerfilErr);
  }
  {
    const { error: occCuentaErr } = await supabase
      .from("cuentas")
      .update({ ocupado: true })
      .eq("id_cuenta", nuevoCuenta);
    if (occCuentaErr) console.error("auto reemplazo ocupar cuenta error", occCuentaErr);
  }

  await supabase.from("reemplazos").insert({
    id_cuenta: idCuenta,
    id_perfil: idPerfil || null,
    id_sub_cuenta: null,
    id_venta: ventaInfo.id_venta,
  });

  try {
    const userIds = pickNotificationUserIds("servicio_reemplazado", {
      ventaUserId: ventaInfo.id_usuario || idUsuario,
    });
    if (userIds.length) {
      const notif = buildNotificationPayload(
        "servicio_reemplazado",
        {
          plataforma:
            selectPlataforma?.selectedOptions?.[0]?.textContent?.trim() || "Plataforma",
          correoViejo: cuentaActual.correo || "",
          perfilViejo: perfilActual?.n_perfil ? `M${perfilActual.n_perfil}` : "",
          correoNuevo: dataDestino.correo || "",
          perfilNuevo: dataDestino.n_perfil ? `M${dataDestino.n_perfil}` : "",
          claveNuevo: dataDestino.clave || "",
        },
        { idCuenta: nuevoCuenta },
      );
      const rows = userIds.map((uid) => ({ ...notif, id_usuario: uid }));
      const { error: notifErr } = await supabase.from("notificaciones").insert(rows);
      if (notifErr) console.error("auto reemplazo notificacion error", notifErr);
    }
  } catch (notifCatchErr) {
    console.error("auto reemplazo notificacion error", notifCatchErr);
  }

  return {
    reemplazado: true,
    aplica: true,
    sinStock: false,
    correoNuevo: dataDestino.correo || "",
  };
}

async function init() {
  try {
    const user = await loadCurrentUser();
    if (user && usernameEl) {
      const fullName = [user.nombre, user.apellido]
        .filter(Boolean)
        .join(" ")
        .trim();
      usernameEl.textContent = fullName || user.correo || "Usuario";
    }
    setSessionRoles(user || {});
    const sessionRoles = getSessionRoles();
    const isAdmin =
      isTrue(sessionRoles?.permiso_admin) ||
      isTrue(sessionRoles?.permiso_superadmin) ||
      isTrue(user?.permiso_admin) ||
      isTrue(user?.permiso_superadmin);
    if (adminLink) {
      adminLink.classList.toggle("hidden", !isAdmin);
      adminLink.style.display = isAdmin ? "block" : "none";
    }

    await cargarPlataformas();
    await cargarCorreos();
    await cargarMotivos();
    renderMotivosPorPlataforma(selectedPlataformaId);
    initMotivoOtro();
    initNoScreenshotOption();
    initUploadUI();
    initPerfilPorCorreo();
    await aplicarPrefillQuery();
  } catch (err) {
    console.error("crear reporte init error", err);
  }
}

async function cargarPlataformas() {
  if (!selectPlataforma) return;
  const userId = requireSession();
  const { data, error } = await supabase
    .from("ventas")
    .select(
      "id_cuenta, id_cuenta_miembro, cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, id_plataforma, plataformas(nombre, por_acceso)), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, id_plataforma, plataformas(nombre, por_acceso))"
    )
    .eq("id_usuario", userId);
  if (error) {
    console.error("plataformas ventas error", error);
    return;
  }
  const unique = new Map();
  plataformasMeta = new Map();
  (data || []).forEach((row) => {
    const ctaMain = row.cuentas || null;
    const ctaMiembro = row.cuentas_miembro || null;
    const opciones = [ctaMain, ctaMiembro];
    opciones.forEach((cta) => {
      const platId = cta?.id_plataforma;
      const nombre = cta?.plataformas?.nombre;
      const porAcceso = cta?.plataformas?.por_acceso;
      if (platId && nombre && !unique.has(platId)) {
        unique.set(platId, nombre);
      }
      if (platId && !plataformasMeta.has(String(platId))) {
        plataformasMeta.set(String(platId), {
          por_acceso: porAcceso,
        });
      }
    });
  });
  if (!unique.size) {
    selectPlataforma.innerHTML = '<option value="">No tienes plataformas</option>';
    selectPlataforma.disabled = true;
    return;
  }
  const opts = Array.from(unique.entries())
    .map(([id, nombre]) => `<option value="${id}">${nombre}</option>`)
    .join("");
  selectPlataforma.insertAdjacentHTML("beforeend", opts);
  // Bloquea cuentas hasta que se elija plataforma
  if (inputCorreoCuenta) {
    inputCorreoCuenta.disabled = true;
    inputCorreoCuenta.classList.add("input-disabled");
  }
  selectPlataforma.addEventListener("change", () => {
    const platId = selectPlataforma.value;
    selectedPlataformaId = platId || null;
    if (inputCorreoCuenta) {
      inputCorreoCuenta.disabled = false;
      inputCorreoCuenta.classList.remove("input-disabled");
      inputCorreoCuenta.value = "";
    }
    hideCorreoSugerencias();
    poblarCorreosPorPlataforma(platId);
    renderMotivosPorPlataforma(platId);
    selectedCuentaId = null;
    resetPerfilSelectionUI();
  });
}

async function cargarCorreos() {
  if (!inputCorreoCuenta) return;
  const userId = requireSession();
  const { data, error } = await supabase
    .from("ventas")
    .select(
      "id_usuario, id_cuenta, id_cuenta_miembro, correo_miembro, cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, id_plataforma), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, correo, id_plataforma)"
    )
    .eq("id_usuario", userId);
  if (error) {
    console.error("correos ventas error", error);
    return;
  }
  // mapa de platId -> map de cuentas (id_cuenta -> correo)
  cuentasPorPlataforma = new Map();
  const upsertCuentaCorreo = (platKey, idCuentaRaw, correoRaw) => {
    const idCuenta = Number(idCuentaRaw);
    const correo = String(correoRaw || "").trim();
    if (!platKey || !Number.isFinite(idCuenta) || idCuenta <= 0 || !correo) return;
    if (!cuentasPorPlataforma.has(platKey)) cuentasPorPlataforma.set(platKey, new Map());
    const map = cuentasPorPlataforma.get(platKey);
    if (!map.has(idCuenta)) {
      map.set(idCuenta, correo);
    } else if (!String(map.get(idCuenta) || "").trim() && correo) {
      map.set(idCuenta, correo);
    }
  };
  (data || []).forEach((row) => {
    const ctaMain = row.cuentas || null;
    const ctaMiembro = row.cuentas_miembro || null;
    const platMain = ctaMain?.id_plataforma;
    const platMiembro = ctaMiembro?.id_plataforma;
    const platKeyMain =
      platMain === null || platMain === undefined ? null : String(platMain);
    const platKeyMiembro =
      platMiembro === null || platMiembro === undefined ? null : String(platMiembro);

    upsertCuentaCorreo(platKeyMain, ctaMain?.id_cuenta || row.id_cuenta, ctaMain?.correo);
    upsertCuentaCorreo(
      platKeyMiembro,
      ctaMiembro?.id_cuenta || row.id_cuenta_miembro,
      ctaMiembro?.correo
    );

    const correoMiembro = String(row?.correo_miembro || "").trim();
    if (correoMiembro) {
      const targetId = ctaMiembro?.id_cuenta || row.id_cuenta_miembro || ctaMain?.id_cuenta || row.id_cuenta;
      const targetPlatKey = platKeyMiembro || platKeyMain;
      upsertCuentaCorreo(targetPlatKey, targetId, correoMiembro);
    }
  });
  hideCorreoSugerencias();
  inputCorreoCuenta.disabled = true;
  inputCorreoCuenta.classList.add("input-disabled");
}

async function aplicarPrefillQuery() {
  if (!prefillPlataforma || !selectPlataforma) return;
  selectPlataforma.value = prefillPlataforma;
  selectedPlataformaId = prefillPlataforma;
  renderMotivosPorPlataforma(prefillPlataforma);
  if (inputCorreoCuenta) {
    inputCorreoCuenta.disabled = false;
    inputCorreoCuenta.classList.remove("input-disabled");
  }
  poblarCorreosPorPlataforma(prefillPlataforma);
  if (prefillCorreo && inputCorreoCuenta) {
    inputCorreoCuenta.value = prefillCorreo;
  }
  if (prefillCuenta) {
    const key = String(prefillPlataforma);
    const cuentasMap = cuentasPorPlataforma.get(key);
    const prefillCuentaNum = Number(prefillCuenta);
    const prefillCuentaId =
      Number.isFinite(prefillCuentaNum) && prefillCuentaNum > 0 ? prefillCuentaNum : null;
    let correoPrefill = "";
    if (cuentasMap && prefillCuentaId) {
      correoPrefill = cuentasMap.get(prefillCuentaNum) || "";
    }
    if (inputCorreoCuenta && correoPrefill) {
      inputCorreoCuenta.value = correoPrefill;
    }
    if (prefillCuentaId) {
      selectedCuentaId = prefillCuentaId;
      hideCorreoAviso();
      await cargarPerfilesPorCorreo(prefillCuentaId);
    }
    if (
      !isPlataformaPorAcceso(prefillPlataforma) &&
      prefillPerfil &&
      selectPerfil &&
      selectPerfil.querySelector(`option[value="${prefillPerfil}"]`)
    ) {
      selectPerfil.value = prefillPerfil;
      selectedPerfilId = prefillPerfil;
      perfilWrapper?.classList.remove("hidden");
      selectPerfil.required = true;
    }
  } else if (prefillCorreo) {
    const key = String(prefillPlataforma || "").trim();
    const cuentasMap = cuentasPorPlataforma.get(key);
    if (cuentasMap) {
      const correoBusca = String(prefillCorreo).trim().toLowerCase();
      const exact = Array.from(cuentasMap.entries()).find(
        ([, correo]) => String(correo || "").trim().toLowerCase() === correoBusca
      );
      if (exact?.[0]) {
        selectedCuentaId = exact[0];
        if (inputCorreoCuenta) inputCorreoCuenta.value = exact[1] || prefillCorreo;
        hideCorreoAviso();
        await cargarPerfilesPorCorreo(exact[0]);
      }
    }
  }
}

async function cargarMotivos() {
  if (!selectMotivo) return;
  const { data, error } = await supabase
    .from("reporte_tipos")
    .select("*")
    .order("titulo", { ascending: true });
  if (error) {
    console.error("motivos reporte error", error);
    return;
  }
  const unique = [];
  const seen = new Set();
  (data || []).forEach((row) => {
    const titulo = String(row?.titulo || "").trim();
    if (!titulo) return;
    if (isOtroMotivo(titulo)) return;
    const parsedId = Number(
      row?.id_tipo_reporte ?? row?.id_reporte_tipo ?? row?.id_tipo ?? row?.id
    );
    const idTipo = Number.isFinite(parsedId) ? parsedId : null;
    if (!idTipo) return;
    const key = normalizeMotivo(titulo);
    if (seen.has(key)) return;
    seen.add(key);
    unique.push({ id: idTipo, titulo });
  });
  tiposReporteCatalog = unique;
}

function renderMotivosPorPlataforma(platId) {
  if (!selectMotivo) return;
  const hasPlat = !!String(platId || "").trim();
  selectMotivo.disabled = !hasPlat;
  selectMotivo.classList.toggle("input-disabled", !hasPlat);
  const selectedPrev = selectMotivo.value;
  if (!hasPlat) {
    selectMotivo.innerHTML = '<option value="">Seleccione motivo</option>';
    selectMotivo.value = "";
    updateMotivoExtra();
    motivoOtroWrapper?.classList.add("hidden");
    if (inputMotivoOtro) {
      inputMotivoOtro.required = false;
      inputMotivoOtro.value = "";
    }
    return;
  }
  const showTipo4 = String(platId || "") === "1";
  const motivosFiltrados = (tiposReporteCatalog || []).filter((item) =>
    showTipo4 ? true : Number(item.id) !== 4
  );
  selectMotivo.innerHTML = '<option value="">Seleccione motivo</option>';
  motivosFiltrados.forEach((item) => {
    const opt = document.createElement("option");
    opt.value = String(item.id);
    opt.textContent = item.titulo;
    opt.dataset.titulo = item.titulo;
    selectMotivo.appendChild(opt);
  });
  const optOtro = document.createElement("option");
  optOtro.value = "__otro__";
  optOtro.textContent = "Otro";
  optOtro.dataset.titulo = "Otro";
  selectMotivo.appendChild(optOtro);
  if (selectedPrev && selectMotivo.querySelector(`option[value="${selectedPrev}"]`)) {
    selectMotivo.value = selectedPrev;
  } else {
    selectMotivo.value = "";
  }
  updateMotivoExtra();
  if (!isOtroMotivo(getSelectedMotivoTitle())) {
    motivoOtroWrapper?.classList.add("hidden");
    if (inputMotivoOtro) {
      inputMotivoOtro.required = false;
      inputMotivoOtro.value = "";
    }
  }
}

function poblarCorreosPorPlataforma(platId) {
  if (!inputCorreoCuenta) return;
  hideCorreoSugerencias();
  hideCorreoAviso();
  selectedCuentaId = null;
  // siempre desbloquea al elegir plataforma
  inputCorreoCuenta.disabled = false;
  inputCorreoCuenta.classList.remove("input-disabled");
}

function resetPerfilSelectionUI() {
  correoPerfilGrid?.classList.add("is-single");
  perfilWrapper?.classList.add("hidden");
  if (selectPerfil) {
    selectPerfil.required = false;
    selectPerfil.value = "";
    selectPerfil.innerHTML = '<option value="">Seleccione perfil</option>';
  }
  selectedPerfilId = null;
}

async function cargarPerfilesPorCorreo(cuentaId) {
  if (!cuentaId || !selectPerfil || !perfilWrapper) {
    resetPerfilSelectionUI();
    selectedCuentaId = null;
    return;
  }
  resetPerfilSelectionUI();
  const cuentaNum = Number(cuentaId);
  selectedCuentaId = Number.isFinite(cuentaNum) ? cuentaNum : cuentaId;

  if (isPlataformaPorAcceso()) {
    return;
  }

  const userId = requireSession();
  // Busca ventas del usuario con ese id_cuenta e id_perfil asignado
  const { data, error } = await supabase
    .from("ventas")
    .select("id_cuenta, id_cuenta_miembro, id_perfil, perfiles(id_perfil, n_perfil)")
    .eq("id_usuario", userId)
    .or(`id_cuenta.eq.${cuentaId},id_cuenta_miembro.eq.${cuentaId}`);
  if (error) {
    console.error("perfiles por correo error", error);
    resetPerfilSelectionUI();
    return;
  }
  const ventasFiltradas = (data || []).filter(
    (row) => row.id_perfil !== null && row.id_perfil !== undefined
  );
  const perfilesRaw = ventasFiltradas.filter(
    (row) =>
      Number(row.id_cuenta) === Number(selectedCuentaId) ||
      Number(row.id_cuenta_miembro) === Number(selectedCuentaId)
  );
  if (!perfilesRaw.length) {
    resetPerfilSelectionUI();
    return;
  }
  const perfilesMap = new Map();
  perfilesRaw.forEach((row) => {
    if (!selectedCuentaId) selectedCuentaId = row.id_cuenta || null;
    const perf = row.perfiles || {};
    const idp = perf.id_perfil || row.id_perfil;
    if (idp && !perfilesMap.has(idp)) {
      perfilesMap.set(idp, perf.n_perfil);
    }
  });

  if (!perfilesMap.size) {
    resetPerfilSelectionUI();
    return;
  }

  perfilesMap.forEach((nPerfil, idp) => {
    const opt = document.createElement("option");
    opt.value = idp;
    opt.textContent = nPerfil !== null && nPerfil !== undefined ? nPerfil : "Perfil";
    selectPerfil.appendChild(opt);
  });
  correoPerfilGrid?.classList.remove("is-single");
  perfilWrapper.classList.remove("hidden");
  if (selectPerfil) selectPerfil.required = true;
  const perfilIds = Array.from(perfilesMap.keys()).map((id) => String(id));
  if (perfilIds.length === 1) {
    selectPerfil.value = perfilIds[0];
    selectedPerfilId = perfilIds[0];
  } else {
    selectPerfil.value = "";
    selectedPerfilId = null;
  }
}

function initPerfilPorCorreo() {
  if (!inputCorreoCuenta) return;
  resetPerfilSelectionUI();
  const updateCuentaFromCorreoInput = () => {
    const typedCorreo = String(inputCorreoCuenta.value || "").trim();
    const found = getCuentaDisponibleByCorreo(typedCorreo);
    const cuentaSel = found?.idCuenta || resolveCuentaIdByCorreo(typedCorreo);
    selectedCuentaId = cuentaSel || null;
    if (found) {
      inputCorreoCuenta.value = found.correo;
      hideCorreoAviso();
    } else if (typedCorreo) {
      showCorreoAviso("El correo ingresado no está relacionado a tu cuenta.");
    } else {
      hideCorreoAviso();
    }
    cargarPerfilesPorCorreo(cuentaSel);
  };
  inputCorreoCuenta.addEventListener("focus", () => {
    if (!selectedPlataformaId) return;
    renderCorreoSugerencias(inputCorreoCuenta.value);
  });
  inputCorreoCuenta.addEventListener("input", () => {
    if (!selectedPlataformaId) {
      hideCorreoSugerencias();
      hideCorreoAviso();
      return;
    }
    selectedCuentaId = null;
    hideCorreoAviso();
    resetPerfilSelectionUI();
    renderCorreoSugerencias(inputCorreoCuenta.value);
  });
  inputCorreoCuenta.addEventListener("change", updateCuentaFromCorreoInput);
  inputCorreoCuenta.addEventListener("blur", () => {
    setTimeout(() => hideCorreoSugerencias(), 120);
    updateCuentaFromCorreoInput();
  });
  correoSugerenciasEl?.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-sugerencia-idx]");
    if (!btn) return;
    const idx = Number(btn.dataset.sugerenciaIdx);
    const item = Number.isFinite(idx) ? correosSugerenciasVisibles[idx] : null;
    if (!item) return;
    inputCorreoCuenta.value = item.correo;
    selectedCuentaId = item.idCuenta;
    hideCorreoAviso();
    hideCorreoSugerencias();
    cargarPerfilesPorCorreo(item.idCuenta);
  });
  document.addEventListener("click", (event) => {
    const insideInput = event.target.closest("#input-correo-cuenta");
    const insideList = event.target.closest("#correo-sugerencias");
    if (!insideInput && !insideList) {
      hideCorreoSugerencias();
    }
  });
  selectPerfil?.addEventListener("change", (e) => {
    const val = e.target.value;
    selectedPerfilId = val || null;
  });
}

function initMotivoOtro() {
  if (!selectMotivo || !motivoOtroWrapper) return;
  selectMotivo.addEventListener("change", () => {
    const show = isOtroMotivo(getSelectedMotivoTitle());
    motivoOtroWrapper.classList.toggle("hidden", !show);
    if (inputMotivoOtro) {
      inputMotivoOtro.required = show;
      if (!show) inputMotivoOtro.value = "";
    }
    updateMotivoExtra();
  });
}

function initNoScreenshotOption() {
  toggleUploadDisabled(chkNoScreenshot?.checked);
  updateMotivoExtra();
  chkNoScreenshot?.addEventListener("change", () => {
    toggleUploadDisabled(chkNoScreenshot.checked);
    updateMotivoExtra();
  });
  selectMotivo?.addEventListener("change", updateMotivoExtra);
}

function updateMotivoExtra() {
  motivoExtraWrapper?.classList.add("hidden");
  if (inputMotivoExtra) {
    inputMotivoExtra.required = false;
    inputMotivoExtra.value = "";
  }
}

function toggleUploadDisabled(disabled) {
  if (btnAddImage) btnAddImage.disabled = disabled;
  if (inputFiles) {
    inputFiles.disabled = disabled;
    if (disabled) inputFiles.value = "";
  }
  if (dropzone) {
    dropzone.classList.toggle("input-disabled", disabled);
    dropzone.classList.toggle("dropzone-disabled", disabled);
  }
  if (disabled) {
    filePreview.innerHTML = "";
  }
}

function initUploadUI() {
  const setImageFile = (file) => {
    if (chkNoScreenshot?.checked) return;
    if (!file || !file.type?.startsWith("image/")) {
      filePreview.innerHTML = "";
      if (inputFiles) inputFiles.value = "";
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      filePreview.innerHTML = `<img src="${reader.result}" alt="${file.name}" />`;
    };
    reader.readAsDataURL(file);
    const dt = new DataTransfer();
    dt.items.add(file);
    if (inputFiles) inputFiles.files = dt.files;
  };

  if (btnAddImage) {
    btnAddImage.addEventListener("click", () => inputFiles?.click());
  }
  if (inputFiles) {
    inputFiles.addEventListener("change", () => {
      const files = Array.from(inputFiles.files || []).filter((f) => f.type?.startsWith("image/"));
      setImageFile(files[0]);
      if (!files[0]) {
        alert("Solo se permiten archivos de imagen.");
      }
    });
  }
  if (dropzone) {
    dropzone.addEventListener("dragover", (e) => {
      if (chkNoScreenshot?.checked) return;
      e.preventDefault();
      dropzone.classList.add("dragover");
    });
    dropzone.addEventListener("dragleave", (e) => {
      e.preventDefault();
      dropzone.classList.remove("dragover");
    });
    dropzone.addEventListener("drop", (e) => {
      if (chkNoScreenshot?.checked) return;
      e.preventDefault();
      dropzone.classList.remove("dragover");
      const files = Array.from(e.dataTransfer?.files || []).filter((f) =>
        f.type?.startsWith("image/")
      );
      setImageFile(files[0]);
      if (!files[0]) {
        alert("Solo se permiten archivos de imagen.");
      }
    });
  }
}

async function uploadEvidence(file, userId) {
  if (!file) return null;
  const safeName = sanitizeFileName(file.name);
  const uniqueName = `reporte-${userId}-${Date.now()}-${safeName}`;
  const payloadFile = {
    name: uniqueName,
    type: file.type,
    arrayBuffer: () => file.arrayBuffer(),
  };
  const { urls, error } = await uploadComprobantes([payloadFile]);
  if (error || !urls?.length) {
    console.error("upload evidence error", error);
    throw error || new Error("No se pudo subir la evidencia");
  }
  const first = urls[0] || null;
  if (!first) return null;
  return String(first).trim();
}

async function handleSubmit(e) {
  e.preventDefault();
  const submitBtn = form?.querySelector("button[type='submit']");
  submitBtn && (submitBtn.disabled = true);
  try {
    const id_usuario = requireSession();
    const id_plataforma = selectedPlataformaId ? Number(selectedPlataformaId) : null;
    if (!selectedCuentaId) {
      const cuentaFromInput = await resolveCuentaIdByCorreoRobusto(
        inputCorreoCuenta?.value,
      );
      if (cuentaFromInput) {
        selectedCuentaId = cuentaFromInput;
      }
    }
    if (!selectedCuentaId) {
      alert("Selecciona un correo válido de tu cuenta.");
      inputCorreoCuenta?.focus();
      submitBtn && (submitBtn.disabled = false);
      return;
    }
    const id_cuenta = selectedCuentaId ? Number(selectedCuentaId) : null;
    const id_perfil = selectedPerfilId ? Number(selectedPerfilId) : null;
    const ventaAsociada = await findVentaAsociadaParaReporte({
      idUsuario: id_usuario,
      idCuenta: id_cuenta,
      idPerfil: id_perfil,
    });
    if (isTrue(ventaAsociada?.reportado)) {
      alert("Ya hay un reporte activo de esta venta");
      submitBtn && (submitBtn.disabled = false);
      return;
    }
    const ventaAsociadaId = Number(ventaAsociada?.id_venta) || null;
    const motivoOption = selectMotivo?.selectedOptions?.[0] || null;
    const motivoTipoRaw = String(motivoOption?.value || "").trim();
    const motivoLabel = (motivoOption?.dataset?.titulo || motivoOption?.textContent || "").trim();
    const motivoEsOtro = isOtroMotivo(motivoLabel);
    const parsedTipo = Number(motivoTipoRaw);
    const motivoTipoId = motivoEsOtro
      ? null
      : Number.isFinite(parsedTipo)
        ? parsedTipo
        : null;
    if (!motivoLabel || (!motivoEsOtro && !Number.isFinite(motivoTipoId))) {
      alert("Selecciona un motivo.");
      submitBtn && (submitBtn.disabled = false);
      return;
    }

    let descripcion = null;
    if (motivoEsOtro) {
      descripcion = (inputMotivoOtro?.value || "").trim() || null;
      if (!descripcion || descripcion.length < 3) {
        alert("Describe el problema (mínimo 3 caracteres).");
        inputMotivoOtro?.focus();
        submitBtn && (submitBtn.disabled = false);
        return;
      }
    } else {
      descripcion = motivoLabel;
    }

    const file = (inputFiles?.files && inputFiles.files[0]) || null;
    let imagenPath = null;
    let imagenResuelta = false;
    const resolveImagenPath = async () => {
      if (imagenResuelta) return imagenPath;
      imagenResuelta = true;
      if (file && file.type?.startsWith("image/")) {
        try {
          imagenPath = await uploadEvidence(file, id_usuario);
        } catch (uploadErr) {
          console.error("upload reporte imagen error", uploadErr);
          imagenPath = null;
          alert("No se pudo subir la imagen. Se enviará el reporte sin imagen.");
        }
      } else {
        imagenPath = null;
      }
      return imagenPath;
    };

    const autoReemplazoEnabled = await isAutoReemplazoCuentaInactivaEnabled();
    const autoReplaceResult = autoReemplazoEnabled
      ? await intentarReemplazoAutomaticoCuentaInactiva({
          idUsuario: id_usuario,
          idCuenta: id_cuenta,
          idPerfil: id_perfil,
          idPlataforma: id_plataforma,
        })
      : { reemplazado: false, aplica: false, sinStock: false };
    if (autoReplaceResult?.reemplazado) {
      const caracasNow = getCaracasDateTime();
      const imagenAuto = await resolveImagenPath();
      const correoNuevoAuto = String(autoReplaceResult?.correoNuevo || "").trim();
      const descripcionSolucionAuto = correoNuevoAuto
        ? `Reemplazo automático por cuenta inactiva: ${correoNuevoAuto}`
        : "Reemplazo automático por cuenta inactiva";
      const payloadAuto = {
        id_usuario,
        id_plataforma,
        id_cuenta,
        id_perfil,
        id_tipo_reporte: motivoTipoId,
        descripcion,
        imagen: imagenAuto,
        en_revision: false,
        solucionado: true,
        descripcion_solucion: descripcionSolucionAuto,
        fecha_creacion: caracasNow.fecha,
        hora_creacion: caracasNow.hora,
      };
      const { error: insertAutoErr } = await supabase.from("reportes").insert([payloadAuto]);
      if (insertAutoErr) {
        console.error("insert reporte auto solucionado error", insertAutoErr);
        alert("Se reemplazó el servicio, pero no se pudo guardar el reporte.");
        window.location.href = "./report.html";
        return;
      }
      if (ventaAsociadaId) {
        await supabase.from("ventas").update({ reportado: true }).eq("id_venta", ventaAsociadaId);
      }
      try {
        await markVentaPendienteByReporteRule({
          ventaId: ventaAsociadaId,
          idUsuario: id_usuario,
          idCuenta: id_cuenta,
          idPerfil: id_perfil,
          idPlataforma: id_plataforma,
          motivoTipoId: motivoTipoId,
        });
      } catch (pendErr) {
        console.error("mark venta pendiente by reporte rule error", pendErr);
      }
      alert("Servicio reemplazado automáticamente.");
      window.location.href = "./report.html";
      return;
    }

    const imagenNormal = await resolveImagenPath();
    const caracasNow = getCaracasDateTime();

    const payload = {
      id_usuario,
      id_plataforma,
      id_cuenta,
      id_perfil,
      id_tipo_reporte: motivoTipoId,
      descripcion,
      imagen: imagenNormal,
      en_revision: true,
      solucionado: false,
      fecha_creacion: caracasNow.fecha,
      hora_creacion: caracasNow.hora,
    };

    const { error } = await supabase.from("reportes").insert([payload]);
    if (error) {
      console.error("insert reporte error", error);
      alert("No se pudo enviar el reporte. Intenta nuevamente.");
      return;
    }
    if (ventaAsociadaId) {
      await supabase.from("ventas").update({ reportado: true }).eq("id_venta", ventaAsociadaId);
    }
    try {
      await markVentaPendienteByReporteRule({
        ventaId: ventaAsociadaId,
        idUsuario: id_usuario,
        idCuenta: id_cuenta,
        idPerfil: id_perfil,
        idPlataforma: id_plataforma,
        motivoTipoId: motivoTipoId,
      });
    } catch (pendErr) {
      console.error("mark venta pendiente by reporte rule error", pendErr);
    }
    alert("Reporte enviado correctamente.");
    window.location.href = "./report.html";
  } catch (err) {
    console.error("crear reporte submit error", err);
    alert("Ocurrió un error enviando el reporte.");
  } finally {
    submitBtn && (submitBtn.disabled = false);
  }
}

if (form) {
  form.addEventListener("submit", handleSubmit);
}

init();
attachLogout(clearServerSession);
attachLogoHome();
