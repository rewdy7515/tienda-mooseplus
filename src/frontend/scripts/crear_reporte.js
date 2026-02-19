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
const selectPlataforma = document.querySelector("#select-plataforma");
const selectMotivo = document.querySelector("#select-motivo");
const selectPerfil = document.querySelector("#select-perfil");
const perfilWrapper = document.querySelector("#perfil-wrapper");
const selectCorreo = document.querySelector("#select-correo");
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
let tiposReporteCatalog = [];
const queryParams = new URLSearchParams(window.location.search);
const prefillPlataforma = queryParams.get("plataforma");
const prefillCuenta = queryParams.get("cuenta");
const prefillPerfil = queryParams.get("perfil");
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

  return { reemplazado: true, aplica: true, sinStock: false };
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
    .select("id_cuenta, cuentas:cuentas!ventas_id_cuenta_fkey(id_plataforma, plataformas(nombre))")
    .eq("id_usuario", userId);
  if (error) {
    console.error("plataformas ventas error", error);
    return;
  }
  const unique = new Map();
  (data || []).forEach((row) => {
    const platId = row.cuentas?.id_plataforma;
    const nombre = row.cuentas?.plataformas?.nombre;
    if (platId && nombre && !unique.has(platId)) {
      unique.set(platId, nombre);
    }
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
  selectCorreo.disabled = true;
  selectCorreo.classList.add("input-disabled");
  selectPlataforma.addEventListener("change", () => {
    const platId = selectPlataforma.value;
    selectedPlataformaId = platId || null;
    selectCorreo.disabled = false;
    selectCorreo.classList.remove("input-disabled");
    poblarCorreosPorPlataforma(platId);
    renderMotivosPorPlataforma(platId);
    perfilWrapper?.classList.add("hidden");
    selectCorreo.value = "";
  });
}

async function cargarCorreos() {
  if (!selectCorreo) return;
  const userId = requireSession();
  const { data, error } = await supabase
    .from("ventas")
    .select("id_usuario, id_cuenta, cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, id_plataforma)")
    .eq("id_usuario", userId);
  if (error) {
    console.error("correos ventas error", error);
    return;
  }
  // mapa de platId -> map de cuentas (id_cuenta -> correo)
  cuentasPorPlataforma = new Map();
  (data || []).forEach((row) => {
    const platId = row.cuentas?.id_plataforma;
    const platKey = platId === null || platId === undefined ? null : String(platId);
    const correo = row.cuentas?.correo;
    const idCuenta = row.cuentas?.id_cuenta || row.id_cuenta;
    if (platKey && idCuenta) {
      if (!cuentasPorPlataforma.has(platKey)) cuentasPorPlataforma.set(platKey, new Map());
      const map = cuentasPorPlataforma.get(platKey);
      if (!map.has(idCuenta)) map.set(idCuenta, correo || "");
    }
  });
  selectCorreo.innerHTML = '<option value="">Seleccione cuenta</option>';
  selectCorreo.disabled = true;
  selectCorreo.classList.add("input-disabled");
}

async function aplicarPrefillQuery() {
  if (!prefillPlataforma || !selectPlataforma) return;
  selectPlataforma.value = prefillPlataforma;
  selectedPlataformaId = prefillPlataforma;
  renderMotivosPorPlataforma(prefillPlataforma);
  selectCorreo.disabled = false;
  selectCorreo.classList.remove("input-disabled");
  poblarCorreosPorPlataforma(prefillPlataforma);
  if (prefillCuenta && selectCorreo.querySelector(`option[value="${prefillCuenta}"]`)) {
    selectCorreo.value = prefillCuenta;
    await cargarPerfilesPorCorreo(prefillCuenta);
    if (prefillPerfil && selectPerfil && selectPerfil.querySelector(`option[value="${prefillPerfil}"]`)) {
      selectPerfil.value = prefillPerfil;
      selectedPerfilId = prefillPerfil;
      perfilWrapper?.classList.remove("hidden");
      selectPerfil.required = true;
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
  if (!selectCorreo) return;
  const key = platId ? String(platId) : "";
  selectCorreo.innerHTML = '<option value="">Seleccione cuenta</option>';
  // siempre desbloquea al elegir plataforma
  selectCorreo.disabled = false;
  selectCorreo.classList.remove("input-disabled");
  if (!key || !cuentasPorPlataforma.has(key)) {
    return;
  }
  const cuentasMap = cuentasPorPlataforma.get(key);
  const cuentas = Array.from(cuentasMap.entries());
  if (!cuentas.length) {
    return;
  }
  const opts = cuentas
    .map(
      ([idCuenta, correo]) =>
        `<option value="${idCuenta}">${correo || `Cuenta ${idCuenta}`}</option>`
    )
    .join("");
  selectCorreo.insertAdjacentHTML("beforeend", opts);
}

async function cargarPerfilesPorCorreo(cuentaId) {
  if (!cuentaId || !selectPerfil || !perfilWrapper) {
    perfilWrapper?.classList.add("hidden");
    if (selectPerfil) selectPerfil.required = false;
    selectedCuentaId = null;
    selectedPerfilId = null;
    return;
  }
  perfilWrapper.classList.remove("hidden");
  selectPerfil.innerHTML = '<option value="">Seleccione perfil</option>';
  selectPerfil.required = false;
  selectedCuentaId = cuentaId;
  selectedPerfilId = null;

  const userId = requireSession();
  // Busca ventas del usuario con ese id_cuenta e id_perfil asignado
  const { data, error } = await supabase
    .from("ventas")
    .select("id_cuenta, id_perfil, perfiles(id_perfil, n_perfil)")
    .eq("id_usuario", userId)
    .eq("id_cuenta", cuentaId);
  if (error) {
    console.error("perfiles por correo error", error);
    return;
  }
  const ventasFiltradas = (data || []).filter(
    (row) => row.id_perfil !== null && row.id_perfil !== undefined
  );
  const cuentasUnicas = [...new Set(ventasFiltradas.map((r) => r.id_cuenta).filter(Boolean))];
  selectedCuentaId = cuentasUnicas[0] || null;

  const perfilesRaw = ventasFiltradas.filter((row) => row.id_cuenta === selectedCuentaId);
  if (!perfilesRaw.length) {
    selectPerfil.innerHTML = '<option value="">Seleccione perfil</option>';
    selectedPerfilId = null;
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
    if (selectPerfil) selectPerfil.required = false;
    return;
  }

  perfilesMap.forEach((nPerfil, idp) => {
    const opt = document.createElement("option");
    opt.value = idp;
    opt.textContent = nPerfil !== null && nPerfil !== undefined ? nPerfil : "Perfil";
    selectPerfil.appendChild(opt);
  });
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
  if (!selectCorreo) return;
  perfilWrapper?.classList.add("hidden");
  selectCorreo.addEventListener("change", (e) => {
    const cuentaSel = e.target.value;
    selectedCuentaId = cuentaSel || null;
    cargarPerfilesPorCorreo(cuentaSel);
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
  const renamedFile = new File([file], uniqueName, { type: file.type });
  const { urls, error } = await uploadComprobantes([renamedFile]);
  if (error || !urls?.length) {
    console.error("upload evidence error", error);
    throw error || new Error("No se pudo subir la evidencia");
  }
  return urls[0] || null;
}

async function handleSubmit(e) {
  e.preventDefault();
  const submitBtn = form?.querySelector("button[type='submit']");
  submitBtn && (submitBtn.disabled = true);
  try {
    const id_usuario = requireSession();
    const id_plataforma = selectedPlataformaId ? Number(selectedPlataformaId) : null;
    const id_cuenta = selectedCuentaId ? Number(selectedCuentaId) : null;
    const id_perfil = selectedPerfilId ? Number(selectedPerfilId) : null;
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
        imagenPath = await uploadEvidence(file, id_usuario);
      } else {
        imagenPath = null;
      }
      return imagenPath;
    };

    const autoReplaceResult = await intentarReemplazoAutomaticoCuentaInactiva({
      idUsuario: id_usuario,
      idCuenta: id_cuenta,
      idPerfil: id_perfil,
      idPlataforma: id_plataforma,
    });
    if (autoReplaceResult?.reemplazado) {
      const imagenAuto = await resolveImagenPath();
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
        descripcion_solucion: "Reemplazo automático por cuenta inactiva",
      };
      const { error: insertAutoErr } = await supabase.from("reportes").insert([payloadAuto]);
      if (insertAutoErr) {
        console.error("insert reporte auto solucionado error", insertAutoErr);
        alert("Se reemplazó el servicio, pero no se pudo guardar el reporte.");
        window.location.href = "./report.html";
        return;
      }
      alert("Servicio reemplazado automáticamente.");
      window.location.href = "./report.html";
      return;
    }

    const imagenNormal = await resolveImagenPath();

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
    };

    const { error } = await supabase.from("reportes").insert([payload]);
    if (error) {
      console.error("insert reporte error", error);
      alert("No se pudo enviar el reporte. Intenta nuevamente.");
      return;
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
