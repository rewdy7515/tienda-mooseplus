import {
  requireSession,
  attachLogout,
  getSessionRoles,
  setSessionRoles,
  attachLogoHome,
} from "./session.js";
import { clearServerSession, loadCurrentUser, supabase, ensureServerSession } from "./api.js";

requireSession();

const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const statusEl = document.querySelector("#reportes-status");
const filtrosEl = document.querySelector("#reportes-plataformas");
const tbody = document.querySelector("#reportes-body");
const tablaWrapper = document.querySelector("#tabla-wrapper");
const tablaPendientes = document.querySelector("#tabla-pendientes");
const btnCrear = document.querySelector("#btn-crear-reporte");
const logo = document.querySelector(".logo");
const modalSol = document.querySelector("#modal-solucionado");
const modalSolTitle = document.querySelector("#sol-modal-title");
const modalSolCorreo = document.querySelector("#sol-modal-correo");
const modalSolClave = document.querySelector("#sol-modal-clave");
const modalSolPerfil = document.querySelector("#sol-modal-perfil");
const modalSolPin = document.querySelector("#sol-modal-pin");
const modalSolNotas = document.querySelector("#sol-modal-notas");
const modalSolClose = document.querySelector(".modal-sol-close");

let porPlataforma = new Map();
let porPlataformaSol = new Map();
const btnSolucionados = document.querySelector("#btn-solucionados");
const solSection = document.querySelector("#solucionados-section");
const solFiltros = document.querySelector("#solucionados-plataformas");
const solWrapper = document.querySelector("#solucionados-wrapper");
const solBody = document.querySelector("#solucionados-body");
const tablaSolucionados = document.querySelector("#tabla-solucionados");

const normalizeHexColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  return /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash) ? withHash : null;
};

const isDarkHex = (hex) => {
  const c = normalizeHexColor(hex);
  if (!c) return false;
  const full =
    c.length === 4
      ? `#${c[1]}${c[1]}${c[2]}${c[2]}${c[3]}${c[3]}`
      : c;
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

const setTableHeaderColor = (tableEl, color) => {
  if (!tableEl) return;
  const c = normalizeHexColor(color);
  if (!c) {
    tableEl.style.removeProperty("--table-header-bg");
    tableEl.style.removeProperty("--table-header-color");
    return;
  }
  tableEl.style.setProperty("--table-header-bg", c);
  tableEl.style.setProperty("--table-header-color", isDarkHex(c) ? "#fff" : "#111");
};

async function init() {
  try {
    const userId = requireSession();
    await ensureServerSession();
    const user = await loadCurrentUser();
    if (user && usernameEl) {
      const fullName = [user.nombre, user.apellido].filter(Boolean).join(" ").trim();
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

    await loadReportes();
    await loadReportesSolucionados();
  } catch (err) {
    console.error("report init error", err);
  }
}

function renderReportes(items = []) {
  if (!filtrosEl || !tbody) return;
  if (!items.length) {
    filtrosEl.innerHTML = "";
    setTableHeaderColor(tablaPendientes, null);
    tbody.innerHTML = `<tr><td colspan="3" class="status">No tienes reportes pendientes.</td></tr>`;
    return;
  }
  porPlataforma = new Map();
  items.forEach((r) => {
    const id = r.id_plataforma;
    const nombre = r.plataformas?.nombre || `Plataforma ${id}`;
    const color = r.plataformas?.color_1 || null;
    if (!porPlataforma.has(id)) porPlataforma.set(id, { id, nombre, color, items: [] });
    porPlataforma.get(id).items.push(r);
  });
  filtrosEl.innerHTML = Array.from(porPlataforma.values())
    .map(
      (p, idx) =>
        `<button class="btn-outline admin-action" data-plat="${p.id}" data-idx="${idx}"${buttonStyleForColor(p.color)}>${p.nombre}</button>`
    )
    .join("");
  let currentPlat = null;
  filtrosEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-plat]");
    if (!btn) return;
    const platId = Number(btn.dataset.plat);
    if (currentPlat === platId) {
      // toggle off
      tablaWrapper?.classList.add("hidden");
      setTableHeaderColor(tablaPendientes, null);
      tbody.innerHTML = `<tr><td colspan="3" class="status">Selecciona una plataforma.</td></tr>`;
      currentPlat = null;
      return;
    }
    currentPlat = platId;
    const plataforma = porPlataforma.get(platId);
    renderTabla(plataforma?.items || [], plataforma?.color || null);
  });
}

async function loadReportes() {
  if (statusEl) statusEl.textContent = "Cargando reportes...";
  try {
    const id = requireSession();
    await ensureServerSession();
    const { data, error } = await supabase
      .from("reportes")
      .select("*, plataformas(nombre, color_1), cuentas(correo)")
      .eq("id_usuario", id)
      .eq("en_revision", true)
      .order("id_reporte", { ascending: false });
    if (error) throw error;
    renderReportes(data || []);
    if (statusEl) statusEl.textContent = "";
  } catch (err) {
    console.error("load reportes error", err);
    if (statusEl) statusEl.textContent = "No se pudieron cargar los reportes.";
  }
}

btnCrear?.addEventListener("click", () => {
  window.location.href = "./crear_reporte.html";
});

logo?.addEventListener("click", () => {
  window.location.href = "../index.html";
});

init();
attachLogout(clearServerSession);
attachLogoHome();

async function loadReportesSolucionados() {
  try {
    const id = requireSession();
    await ensureServerSession();
    const { data, error } = await supabase
      .from("reportes")
      .select("*, plataformas(nombre, color_1), cuentas(correo, clave), perfiles(n_perfil, pin)")
      .eq("id_usuario", id)
      .eq("solucionado", true)
      .order("id_reporte", { ascending: false });
    if (error) throw error;
    porPlataformaSol = new Map();
    (data || []).forEach((r) => {
      const idp = r.id_plataforma;
      const nombre = r.plataformas?.nombre || `Plataforma ${idp}`;
      const color = r.plataformas?.color_1 || null;
      if (!porPlataformaSol.has(idp)) porPlataformaSol.set(idp, { id: idp, nombre, color, items: [] });
      porPlataformaSol.get(idp).items.push(r);
    });
    renderSolucionadosFiltros();
  } catch (err) {
    console.error("load solucionados error", err);
  }
}

function renderSolucionadosFiltros() {
  if (!solFiltros) return;
  if (!porPlataformaSol.size) {
    setTableHeaderColor(tablaSolucionados, null);
    solFiltros.innerHTML = `<p class="status">No hay reportes solucionados en los últimos 30 días.</p>`;
    return;
  }
  solFiltros.innerHTML = Array.from(porPlataformaSol.values())
    .map(
      (p) =>
        `<button class="btn-outline admin-action" data-plat="${p.id}"${buttonStyleForColor(p.color)}>${p.nombre}</button>`
    )
    .join("");
  solFiltros.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-plat]");
    if (!btn) return;
    const platId = Number(btn.dataset.plat);
    const plataforma = porPlataformaSol.get(platId);
    renderTablaSol(plataforma?.items || [], plataforma?.color || null);
  });
}

function renderTablaSol(items = [], headerColor = null) {
  if (!solBody) return;
  setTableHeaderColor(tablaSolucionados, headerColor);
  if (!items.length) {
    solBody.innerHTML = `<tr><td colspan="4" class="status">No hay reportes solucionados para esta plataforma.</td></tr>`;
    solWrapper?.classList.add("hidden");
    return;
  }
  solWrapper?.classList.remove("hidden");
  solBody.innerHTML = items
    .map(
      (r) => `<tr>
        <td>${r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-"}</td>
        <td>${r.cuentas?.correo || "-"}</td>
        <td>${r.fecha_creacion || "-"}</td>
        <td><button class="btn-outline btn-small" data-id="${r.id_reporte}" data-action="detalle-sol">Ver detalles</button></td>
      </tr>`
    )
    .join("");
}

btnSolucionados?.addEventListener("click", () => {
  if (!solSection) return;
  const isHidden = solSection.classList.contains("hidden");
  solSection.classList.toggle("hidden", !isHidden);
});

solBody?.addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-action='detalle-sol']");
  if (!btn) return;
  const id = btn.dataset.id;
  const allItems = Array.from(porPlataformaSol.values()).flatMap((p) => p.items || []);
  const row = allItems.find((r) => String(r.id_reporte) === String(id));
  if (row) openModalSol(row);
});

function openModalSol(row) {
  if (!modalSol) return;
  modalSolTitle.textContent = row.plataformas?.nombre || "Detalle de reporte";
  modalSolCorreo.textContent = row.cuentas?.correo || "-";
  modalSolClave.textContent = row.cuentas?.clave || "-";
  modalSolPerfil.textContent =
    row.perfiles?.n_perfil !== undefined && row.perfiles?.n_perfil !== null
      ? row.perfiles.n_perfil
      : "-";
  modalSolPin.textContent =
    row.perfiles?.pin !== undefined && row.perfiles?.pin !== null ? row.perfiles.pin : "-";
  const notasRaw = row.descripcion_solucion || "";
  if (notasRaw) {
    const parts = notasRaw
      .split(";")
      .map((s) => s.trim())
      .filter(Boolean);
    if (parts.length) {
      modalSolNotas.innerHTML = `<ul>${parts.map((p) => `<li>${p}</li>`).join("")}</ul>`;
    } else {
      modalSolNotas.textContent = "-";
    }
  } else {
    modalSolNotas.textContent = "-";
  }
  modalSol.classList.remove("hidden");
}

function closeModalSol() {
  modalSol?.classList.add("hidden");
}

modalSol?.addEventListener("click", (e) => {
  if (
    e.target.classList.contains("modal-backdrop") ||
    e.target.classList.contains("modal-close") ||
    e.target.classList.contains("modal-sol-close")
  ) {
    closeModalSol();
  }
});

modalSolClose?.addEventListener("click", closeModalSol);

function renderTabla(items = [], headerColor = null) {
  if (!tbody) return;
  setTableHeaderColor(tablaPendientes, headerColor);
  if (!items.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="status">No hay reportes en revisión para esta plataforma.</td></tr>`;
    tablaWrapper?.classList.add("hidden");
    return;
  }
  tablaWrapper?.classList.remove("hidden");
  const rows = items
    .map((r) => {
      const correo = r.cuentas?.correo || "-";
      const fecha = r.fecha_creacion || "-";
      const estado = r.en_revision ? "En revisión" : "Pendiente";
      return `<tr>
        <td>${r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-"}</td>
        <td>${correo}</td>
        <td>${fecha}</td>
        <td>${estado}</td>
      </tr>`;
    })
    .join("");
  tbody.innerHTML = rows;
}
