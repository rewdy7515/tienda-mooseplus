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
const pendientesListEl = document.querySelector("#reportes-pendientes-list");
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
let reportesSolRows = [];
let reportesSolById = new Map();
const solListEl = document.querySelector("#reportes-solucionados-list");
const solFilterSelect = document.querySelector("#reportes-solucionados-filtro");
const adminSearchWrap = document.querySelector("#reportes-admin-search");
const adminSearchInput = document.querySelector("#reportes-admin-input");
const adminSearchResults = document.querySelector("#reportes-admin-results");
const adminSearchTarget = document.querySelector("#reportes-admin-target");
let selectedUserId = null;
let selectedUserLabel = "";

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

const REEMPLAZO_NOTA_PREFIX = "Reemplazo automático por cuenta inactiva:";

const renderSolucionNota = (notaRaw = "") => {
  const nota = String(notaRaw || "").trim();
  if (!nota) return "";
  if (nota.startsWith(REEMPLAZO_NOTA_PREFIX)) {
    const correo = nota.slice(REEMPLAZO_NOTA_PREFIX.length).trim();
    if (correo) {
      const href = `../inventario.html?correo=${encodeURIComponent(correo)}`;
      return `${escapeHtml(REEMPLAZO_NOTA_PREFIX)} <a href="${href}">${escapeHtml(correo)}</a>`;
    }
  }
  return escapeHtml(nota);
};

async function init() {
  try {
    const userId = Number(requireSession() || 0);
    selectedUserId = Number.isFinite(userId) && userId > 0 ? userId : null;
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
    const isSuperadmin =
      isTrue(sessionRoles?.permiso_superadmin) || isTrue(user?.permiso_superadmin);
    if (adminLink) {
      adminLink.classList.toggle("hidden", !isAdmin);
      adminLink.style.display = isAdmin ? "block" : "none";
    }
    const selfName =
      [user?.nombre, user?.apellido].filter(Boolean).join(" ").trim() || "Mi cuenta";
    selectedUserLabel = selfName;
    if (isSuperadmin) {
      bindSuperadminSearch({ selfName });
    }
    await loadReportes();
    await loadReportesSolucionados();
  } catch (err) {
    console.error("report init error", err);
  }
}

function renderReportes(items = []) {
  if (!pendientesListEl) return;
  if (!items.length) {
    porPlataforma = new Map();
    pendientesListEl.innerHTML = `<p class="status">No tienes reportes pendientes.</p>`;
    return;
  }
  porPlataforma = new Map();
  items.forEach((r) => {
    const id = r.id_plataforma;
    const nombre = r.plataformas?.nombre || `Plataforma ${id}`;
    const buttonColor = r.plataformas?.color_1 || null;
    const headerColor = r.plataformas?.color_2 || null;
    if (!porPlataforma.has(id)) {
      porPlataforma.set(id, { id, nombre, buttonColor, headerColor, items: [] });
    }
    porPlataforma.get(id).items.push(r);
  });
  pendientesListEl.innerHTML = Array.from(porPlataforma.values())
    .map((p, idx) => {
      const rows = (p.items || [])
        .map((r) => {
          const idReporte = r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-";
          const correo = r.cuentas?.correo || "-";
          const fecha = r.fecha_creacion || "-";
          const estado = r.en_revision ? "En revisión" : "Pendiente";
          return `
            <tr>
              <td>${escapeHtml(idReporte)}</td>
              <td>${escapeHtml(correo)}</td>
              <td>${escapeHtml(fecha)}</td>
              <td>${escapeHtml(estado)}</td>
            </tr>
          `;
        })
        .join("");
      return `
        <section class="inventario-item" data-plat="${p.id}">
          <button type="button" class="btn-outline inventario-btn" data-toggle-plat="${p.id}" data-idx="${idx}"${buttonStyleForColor(p.buttonColor)}>
            ${escapeHtml(p.nombre)}
          </button>
          <div class="inventario-plan hidden" data-plat-content="${p.id}">
            <div class="tabla-wrapper">
              <table class="table-base reportes-table"${tableStyleForColor(p.headerColor)}>
                <thead>
                  <tr>
                    <th>ID Reporte</th>
                    <th>Correo</th>
                    <th>Fecha del reporte</th>
                    <th>Estado</th>
                  </tr>
                </thead>
                <tbody>
                  ${rows || `<tr><td colspan="4" class="status">No hay reportes en revisión para esta plataforma.</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      `;
    })
    .join("");
}

async function loadReportes() {
  if (statusEl) statusEl.textContent = "Cargando reportes...";
  try {
    const id = Number(selectedUserId || requireSession() || 0);
    if (!Number.isFinite(id) || id <= 0) {
      renderReportes([]);
      if (statusEl) statusEl.textContent = "";
      return;
    }
    await ensureServerSession();
    const { data, error } = await supabase
      .from("reportes")
      .select("*, plataformas(nombre, color_1, color_2), cuentas(correo)")
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

pendientesListEl?.addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-toggle-plat]");
  if (!btn) return;
  const platId = String(btn.dataset.togglePlat || "");
  const section = btn.closest(".inventario-item");
  const content = section?.querySelector(`[data-plat-content="${platId}"]`);
  if (!content) return;
  content.classList.toggle("hidden");
  section?.classList.toggle("open", !content.classList.contains("hidden"));
});

async function loadReportesSolucionados() {
  try {
    const id = Number(selectedUserId || requireSession() || 0);
    if (!Number.isFinite(id) || id <= 0) {
      reportesSolRows = [];
      reportesSolById = new Map();
      if (solFilterSelect) {
        solFilterSelect.innerHTML = '<option value="">Todas</option>';
        solFilterSelect.value = "";
      }
      renderSolucionadosList();
      return;
    }
    await ensureServerSession();
    const { data, error } = await supabase
      .from("reportes")
      .select("*, plataformas(nombre, color_1, color_2), cuentas(correo, clave), perfiles(n_perfil, pin)")
      .eq("id_usuario", id)
      .eq("solucionado", true)
      .order("id_reporte", { ascending: false });
    if (error) throw error;
    reportesSolRows = data || [];
    if (solFilterSelect) {
      const selectedPrev = String(solFilterSelect.value || "");
      const plataformasMap = new Map();
      (reportesSolRows || []).forEach((row) => {
        const platId = Number(row?.id_plataforma);
        if (!Number.isFinite(platId) || platId <= 0) return;
        if (plataformasMap.has(platId)) return;
        const nombre = String(row?.plataformas?.nombre || "").trim() || `Plataforma ${platId}`;
        plataformasMap.set(platId, nombre);
      });
      const optionsHtml = Array.from(plataformasMap.entries())
        .sort((a, b) => String(a[1]).localeCompare(String(b[1]), "es", { sensitivity: "base" }))
        .map(([platId, nombre]) => `<option value="${platId}">${escapeHtml(nombre)}</option>`)
        .join("");
      solFilterSelect.innerHTML = `<option value="">Todas</option>${optionsHtml}`;
      if (selectedPrev && solFilterSelect.querySelector(`option[value="${selectedPrev}"]`)) {
        solFilterSelect.value = selectedPrev;
      } else {
        solFilterSelect.value = "";
      }
    }
    renderSolucionadosList();
  } catch (err) {
    console.error("load solucionados error", err);
  }
}

async function searchUsuarios(termRaw = "") {
  const term = String(termRaw || "").trim().replaceAll(",", " ");
  if (term.length < 2) return [];
  const q = `%${term}%`;
  const { data, error } = await supabase
    .from("usuarios")
    .select("id_usuario, nombre, apellido")
    .or(`nombre.ilike.${q},apellido.ilike.${q}`)
    .order("nombre", { ascending: true })
    .limit(20);
  if (error) throw error;
  return data || [];
}

function setAdminTarget(text = "") {
  if (!adminSearchTarget) return;
  const safe = String(text || "").trim();
  adminSearchTarget.textContent = safe;
  adminSearchTarget.classList.toggle("hidden", !safe);
}

function bindSuperadminSearch({ selfName = "Mi cuenta" } = {}) {
  if (!adminSearchWrap || !adminSearchInput || !adminSearchResults) return;
  adminSearchWrap.classList.remove("hidden");
  setAdminTarget(`Mostrando reportes de: ${selfName}`);
  let debounceTimer = null;
  let latestQuery = "";
  const hideResults = () => {
    adminSearchResults.innerHTML = "";
    adminSearchResults.classList.add("hidden");
  };
  const selectUser = async (row) => {
    const id = Number(row?.id_usuario);
    if (!Number.isFinite(id) || id <= 0) return;
    const fullName =
      [row?.nombre, row?.apellido].filter(Boolean).join(" ").trim() || `Usuario ${id}`;
    adminSearchInput.value = fullName;
    selectedUserId = id;
    selectedUserLabel = fullName;
    hideResults();
    setAdminTarget(`Mostrando reportes de: ${fullName}`);
    await Promise.all([loadReportes(), loadReportesSolucionados()]);
  };
  adminSearchInput.addEventListener("input", () => {
    if (debounceTimer) clearTimeout(debounceTimer);
    latestQuery = adminSearchInput.value || "";
    debounceTimer = setTimeout(async () => {
      try {
        if ((latestQuery || "").trim().length < 2) {
          hideResults();
          return;
        }
        const rows = await searchUsuarios(latestQuery);
        if (!rows.length) {
          adminSearchResults.innerHTML =
            '<div class="status" style="padding:10px 12px;">Sin resultados.</div>';
          adminSearchResults.classList.remove("hidden");
          return;
        }
        adminSearchResults.innerHTML = rows
          .map((r) => {
            const fullName =
              [r.nombre, r.apellido].filter(Boolean).join(" ").trim() ||
              `Usuario ${r.id_usuario}`;
            return `<button type="button" class="reportes-admin-result" data-id="${r.id_usuario}" data-name="${escapeHtml(
              fullName
            )}">${escapeHtml(fullName)}</button>`;
          })
          .join("");
        adminSearchResults.classList.remove("hidden");
      } catch (err) {
        console.error("buscar usuarios reportes error", err);
      }
    }, 220);
  });
  adminSearchResults.addEventListener("click", async (e) => {
    const btn = e.target.closest(".reportes-admin-result");
    if (!btn) return;
    const id = Number(btn.dataset.id);
    const name = String(btn.dataset.name || "").trim();
    await selectUser({ id_usuario: id, nombre: name, apellido: "" });
  });
  document.addEventListener("click", (e) => {
    if (!adminSearchWrap.contains(e.target)) hideResults();
  });
}

function renderSolucionadosList() {
  if (!solListEl) return;
  const selectedPlatId = String(solFilterSelect?.value || "").trim();
  const rows = selectedPlatId
    ? (reportesSolRows || []).filter(
        (row) => String(row?.id_plataforma || "") === selectedPlatId,
      )
    : reportesSolRows || [];
  if (!rows.length) {
    reportesSolById = new Map();
    solListEl.innerHTML = `<p class="status">No hay reportes solucionados.</p>`;
    return;
  }
  reportesSolById = new Map();
  const rowsHtml = rows
    .map((r) => {
      if (r?.id_reporte) reportesSolById.set(String(r.id_reporte), r);
      const idReporte = r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-";
      const plataforma = r.plataformas?.nombre || `Plataforma ${r.id_plataforma || "-"}`;
      const correo = r.cuentas?.correo || "-";
      const fecha = r.fecha_creacion || "-";
      return `
        <tr>
          <td>${escapeHtml(idReporte)}</td>
          <td>${escapeHtml(plataforma)}</td>
          <td>${escapeHtml(correo)}</td>
          <td>${escapeHtml(fecha)}</td>
          <td><button class="btn-outline btn-small" data-id="${r.id_reporte}" data-action="detalle-sol">Ver detalles</button></td>
        </tr>
      `;
    })
    .join("");
  solListEl.innerHTML = `
    <div class="tabla-wrapper">
      <table class="table-base reportes-table">
        <thead>
          <tr>
            <th>ID Reporte</th>
            <th>Plataforma</th>
            <th>Correo</th>
            <th>Fecha del reporte</th>
            <th>Acción</th>
          </tr>
        </thead>
        <tbody>
          ${rowsHtml}
        </tbody>
      </table>
    </div>
  `;
}

solListEl?.addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-action='detalle-sol']");
  if (!btn) return;
  const id = btn.dataset.id;
  const row = reportesSolById.get(String(id));
  if (row) openModalSol(row);
});

solFilterSelect?.addEventListener("change", () => {
  renderSolucionadosList();
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
      modalSolNotas.innerHTML = `<ul>${parts
        .map((p) => `<li>${renderSolucionNota(p)}</li>`)
        .join("")}</ul>`;
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
