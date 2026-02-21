import {
  API_BASE,
  clearServerSession,
  ensureServerSession,
  loadCurrentUser,
  supabase,
} from "./api.js";
import { attachLogoHome, attachLogout, requireSession } from "./session.js";

const sessionUserId = requireSession();
const DEFAULT_AVATAR_URL =
  "https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/default-icono-perfil.png";
const DEFAULT_BG_COLOR = "#f3f4f6";

const statusEl = document.querySelector("#perfil-status");
const cardEl = document.querySelector("#perfil-card");
const avatarBgEl = document.querySelector("#perfil-avatar-bg");
const avatarEl = document.querySelector("#perfil-avatar");
const btnEditarFotoEl = document.querySelector("#btn-editar-foto");
const nombreInputEl = document.querySelector("#perfil-nombre-input");
const apellidoInputEl = document.querySelector("#perfil-apellido-input");
const correoInputEl = document.querySelector("#perfil-correo-input");
const telefonoInputEl = document.querySelector("#perfil-telefono-input");
const avatarModalEl = document.querySelector("#avatar-modal");
const avatarModalCloseEl = document.querySelector("#avatar-modal-close");
const avatarModalGridEl = document.querySelector("#avatar-modal-grid");
const avatarModalStatusEl = document.querySelector("#avatar-modal-status");
const btnAvatarSaveEl = document.querySelector("#btn-avatar-save");

let currentUserId = null;
let savedAvatarUrl = DEFAULT_AVATAR_URL;
let savedBgColor = DEFAULT_BG_COLOR;
let pendingAvatarUrl = DEFAULT_AVATAR_URL;
let pendingBgColor = DEFAULT_BG_COLOR;

const setInput = (el, value) => {
  if (!el) return;
  el.value = String(value || "").trim();
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const normalizeColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  if (/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash)) return withHash.toLowerCase();
  return "";
};

const isImageName = (name) =>
  /\.(png|jpe?g|webp|gif|bmp|svg|avif)$/i.test(String(name || ""));

const setAvatarModalStatus = (msg, isError = false) => {
  if (!avatarModalStatusEl) return;
  avatarModalStatusEl.textContent = msg || "";
  avatarModalStatusEl.classList.toggle("is-error", isError);
  avatarModalStatusEl.classList.toggle("is-success", !isError && !!msg);
};

const updateSelectionStyles = () => {
  const selectedUrl = String(pendingAvatarUrl || "");
  document.querySelectorAll(".avatar-option").forEach((btn) => {
    btn.classList.toggle("selected", btn.dataset.avatarUrl === selectedUrl);
  });
  const selectedColor = normalizeColor(pendingBgColor);
  document.querySelectorAll(".avatar-palette-dot[data-color]").forEach((dot) => {
    const dotColor = normalizeColor(dot.dataset.color);
    dot.classList.toggle("selected", !!selectedColor && dotColor === selectedColor);
  });
};

const applyAvatarPreview = ({ url, color } = {}) => {
  const nextUrl = String(url || "").trim() || DEFAULT_AVATAR_URL;
  const nextColor = normalizeColor(color) || DEFAULT_BG_COLOR;
  if (avatarBgEl) {
    avatarBgEl.style.backgroundColor = nextColor;
  }
  if (avatarEl) {
    avatarEl.src = nextUrl;
    avatarEl.style.backgroundColor = "transparent";
  }
  document.querySelectorAll(".avatar").forEach((img) => {
    img.src = nextUrl;
    img.style.backgroundColor = nextColor;
  });
  document.querySelectorAll(".avatar-option").forEach((btn) => {
    btn.style.setProperty("--avatar-bg", nextColor);
  });
  updateSelectionStyles();
};

const closeAvatarModal = (revert = true) => {
  if (revert) {
    pendingAvatarUrl = savedAvatarUrl;
    pendingBgColor = savedBgColor;
    applyAvatarPreview({ url: savedAvatarUrl, color: savedBgColor });
  }
  avatarModalEl?.classList.add("hidden");
};

const openAvatarModal = () => {
  pendingAvatarUrl = savedAvatarUrl;
  pendingBgColor = savedBgColor;
  avatarModalEl?.classList.remove("hidden");
  updateSelectionStyles();
};

const renderAvatarOptions = (items = []) => {
  if (!avatarModalGridEl) return;
  const rows = Array.isArray(items)
    ? items
        .filter((it) => it?.publicUrl)
        .filter((it) => !String(it?.name || "").startsWith("."))
        .filter((it) => isImageName(it?.name))
    : [];
  if (!rows.length) {
    avatarModalGridEl.innerHTML = '<p class="status">Sin iconos disponibles.</p>';
    return;
  }
  avatarModalGridEl.innerHTML = rows
    .map(
      (item) => `
        <button
          type="button"
          class="avatar-option"
          data-avatar-url="${escapeHtml(item.publicUrl || "")}"
          title="${escapeHtml(item.name || "Icono")}"
        >
          <img src="${escapeHtml(item.publicUrl || "")}" alt="${escapeHtml(item.name || "Icono")}" loading="lazy" decoding="async" />
        </button>
      `,
    )
    .join("");
  applyAvatarPreview({ url: pendingAvatarUrl, color: pendingBgColor });
};

const loadAvatarOptions = async () => {
  try {
    setAvatarModalStatus("Cargando iconos...");
    const url = new URL(`${API_BASE}/api/logos/list`);
    url.searchParams.set("folder", "icono-perfil");
    if (sessionUserId) url.searchParams.set("id_usuario", String(sessionUserId));
    const res = await fetch(url.toString(), {
      credentials: "include",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data?.error || "No se pudo cargar la lista de iconos.");
    }
    renderAvatarOptions(data?.items || []);
    setAvatarModalStatus("");
  } catch (err) {
    console.error("load avatar options error", err);
    renderAvatarOptions([]);
    setAvatarModalStatus("No se pudieron cargar los iconos.", true);
  }
};

const saveAvatarProfile = async () => {
  if (!currentUserId) {
    setAvatarModalStatus("No se pudo identificar el usuario.", true);
    return;
  }
  if (!pendingAvatarUrl) {
    setAvatarModalStatus("Selecciona una imagen de perfil.", true);
    return;
  }
  try {
    btnAvatarSaveEl && (btnAvatarSaveEl.disabled = true);
    setAvatarModalStatus("Guardando...");
    const payload = {
      foto_perfil: pendingAvatarUrl,
      fondo_perfil: normalizeColor(pendingBgColor) || DEFAULT_BG_COLOR,
    };
    const { error } = await supabase
      .from("usuarios")
      .update(payload)
      .eq("id_usuario", currentUserId);
    if (error) throw error;

    savedAvatarUrl = payload.foto_perfil;
    savedBgColor = payload.fondo_perfil;
    applyAvatarPreview({ url: savedAvatarUrl, color: savedBgColor });
    setAvatarModalStatus("Foto guardada correctamente.");
    closeAvatarModal(false);
  } catch (err) {
    console.error("save avatar profile error", err);
    setAvatarModalStatus("No se pudo guardar la foto de perfil.", true);
  } finally {
    btnAvatarSaveEl && (btnAvatarSaveEl.disabled = false);
  }
};

const init = async () => {
  try {
    await ensureServerSession();
    const user = await loadCurrentUser();
    if (!user?.id_usuario) {
      if (statusEl) statusEl.textContent = "No se pudo cargar la sesion del usuario.";
      return;
    }

    currentUserId = Number(user.id_usuario) || null;
    savedAvatarUrl = String(user.foto_perfil || "").trim() || DEFAULT_AVATAR_URL;
    savedBgColor = normalizeColor(user.fondo_perfil) || DEFAULT_BG_COLOR;
    pendingAvatarUrl = savedAvatarUrl;
    pendingBgColor = savedBgColor;

    setInput(nombreInputEl, user.nombre);
    setInput(apellidoInputEl, user.apellido);
    setInput(correoInputEl, user.correo);
    setInput(telefonoInputEl, user.telefono);

    applyAvatarPreview({ url: savedAvatarUrl, color: savedBgColor });

    cardEl?.classList.remove("hidden");
    if (statusEl) statusEl.textContent = "";
  } catch (err) {
    console.error("mi_perfil init error", err);
    if (statusEl) statusEl.textContent = "No se pudo cargar el perfil.";
  }
};

btnEditarFotoEl?.addEventListener("click", async () => {
  openAvatarModal();
  await loadAvatarOptions();
});

avatarModalEl?.addEventListener("click", (e) => {
  const option = e.target.closest(".avatar-option[data-avatar-url]");
  if (option) {
    pendingAvatarUrl = option.dataset.avatarUrl || pendingAvatarUrl;
    applyAvatarPreview({ url: pendingAvatarUrl, color: pendingBgColor });
    return;
  }
  const colorDot = e.target.closest(".avatar-palette-dot[data-color]");
  if (colorDot) {
    pendingBgColor = normalizeColor(colorDot.dataset.color) || pendingBgColor;
    applyAvatarPreview({ url: pendingAvatarUrl, color: pendingBgColor });
    return;
  }
  if (e.target.classList.contains("modal-backdrop")) {
    closeAvatarModal(true);
  }
});

avatarModalCloseEl?.addEventListener("click", () => closeAvatarModal(true));
btnAvatarSaveEl?.addEventListener("click", saveAvatarProfile);

init();
attachLogoHome();
attachLogout(clearServerSession);
