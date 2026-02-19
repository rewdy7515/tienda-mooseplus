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
let porPlataformaSol = new Map();
let reportesSolById = new Map();
const btnSolucionados = document.querySelector("#btn-solucionados");
const solSection = document.querySelector("#solucionados-section");
const solListEl = document.querySelector("#reportes-solucionados-list");

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
  const textColor = isDarkHex(c) ? "#fff" : "#111";
  return ` style="--table-header-bg:${c};--table-header-color:${textColor};"`;
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

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
    const id = requireSession();
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
    const id = requireSession();
    await ensureServerSession();
    const { data, error } = await supabase
      .from("reportes")
      .select("*, plataformas(nombre, color_1, color_2), cuentas(correo, clave), perfiles(n_perfil, pin)")
      .eq("id_usuario", id)
      .eq("solucionado", true)
      .order("id_reporte", { ascending: false });
    if (error) throw error;
    porPlataformaSol = new Map();
    (data || []).forEach((r) => {
      const idp = r.id_plataforma;
      const nombre = r.plataformas?.nombre || `Plataforma ${idp}`;
      const buttonColor = r.plataformas?.color_1 || null;
      const headerColor = r.plataformas?.color_2 || null;
      if (!porPlataformaSol.has(idp)) {
        porPlataformaSol.set(idp, { id: idp, nombre, buttonColor, headerColor, items: [] });
      }
      porPlataformaSol.get(idp).items.push(r);
    });
    renderSolucionadosList();
  } catch (err) {
    console.error("load solucionados error", err);
  }
}

function renderSolucionadosList() {
  if (!solListEl) return;
  if (!porPlataformaSol.size) {
    reportesSolById = new Map();
    solListEl.innerHTML = `<p class="status">No hay reportes solucionados en los últimos 30 días.</p>`;
    return;
  }
  reportesSolById = new Map();
  Array.from(porPlataformaSol.values()).forEach((p) => {
    (p.items || []).forEach((r) => {
      if (r?.id_reporte) reportesSolById.set(String(r.id_reporte), r);
    });
  });
  solListEl.innerHTML = Array.from(porPlataformaSol.values())
    .map((p, idx) => {
      const rows = (p.items || [])
        .map((r) => {
          const idReporte = r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-";
          const correo = r.cuentas?.correo || "-";
          const fecha = r.fecha_creacion || "-";
          return `
            <tr>
              <td>${escapeHtml(idReporte)}</td>
              <td>${escapeHtml(correo)}</td>
              <td>${escapeHtml(fecha)}</td>
              <td><button class="btn-outline btn-small" data-id="${r.id_reporte}" data-action="detalle-sol">Ver detalles</button></td>
            </tr>
          `;
        })
        .join("");
      return `
        <section class="inventario-item" data-plat="${p.id}">
          <button type="button" class="btn-outline inventario-btn" data-toggle-sol="${p.id}" data-idx="${idx}"${buttonStyleForColor(p.buttonColor)}>
            ${escapeHtml(p.nombre)}
          </button>
          <div class="inventario-plan hidden" data-sol-content="${p.id}">
            <div class="tabla-wrapper">
              <table class="table-base reportes-table"${tableStyleForColor(p.headerColor)}>
                <thead>
                  <tr>
                    <th>ID Reporte</th>
                    <th>Correo</th>
                    <th>Fecha del reporte</th>
                    <th>Acción</th>
                  </tr>
                </thead>
                <tbody>
                  ${rows || `<tr><td colspan="4" class="status">No hay reportes solucionados para esta plataforma.</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      `;
    })
    .join("");
}

btnSolucionados?.addEventListener("click", () => {
  if (!solSection) return;
  const isHidden = solSection.classList.contains("hidden");
  solSection.classList.toggle("hidden", !isHidden);
});

solListEl?.addEventListener("click", (e) => {
  const toggleBtn = e.target.closest("button[data-toggle-sol]");
  if (toggleBtn) {
    const platId = String(toggleBtn.dataset.toggleSol || "");
    const section = toggleBtn.closest(".inventario-item");
    const content = section?.querySelector(`[data-sol-content="${platId}"]`);
    if (!content) return;
    content.classList.toggle("hidden");
    section?.classList.toggle("open", !content.classList.contains("hidden"));
    return;
  }

  const btn = e.target.closest("button[data-action='detalle-sol']");
  if (!btn) return;
  const id = btn.dataset.id;
  const row = reportesSolById.get(String(id));
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
