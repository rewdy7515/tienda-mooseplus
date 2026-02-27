import {
  API_BASE,
  loadCatalog,
  fetchCart,
  clearServerSession,
  loadCurrentUser,
  supabase,
  fetchEntregadas,
  fetchPendingReminderNoPhoneClients,
  fetchTestingFlag,
  updateTestingFlag,
  fetchP2PRate,
  fetchHomeBanners,
} from "./api.js";
import { initCart } from "./cart.js";
import {
  initModal,
  openModal,
  setPrecios,
  setStockData,
  setDescuentos,
  setDiscountAudience,
} from "./modal.js";
import { initSearch, updateSearchData } from "./search.js";
import { renderCategorias } from "./render.js";
import { AVATAR_RANDOM_COLORS, applyAvatarImage, resolveAvatarForDisplay } from "./avatar-fallback.js";
import { STATIC_HEADER_LOGO_HREF } from "./branding.js";
import {
  attachLogout,
  getCachedCart,
  setCachedCart,
  clearCachedCart,
  getSessionRoles,
  setSessionRoles,
  attachLogoHome,
  showDeliveryNotice,
  getDeliverySeen,
  setDeliverySeen,
  requireSession,
} from "./session.js";
import { TASA_MARKUP } from "./rate-config.js";

const contenedor = document.querySelector("#categorias-container");
const estado = document.querySelector("#categorias-status");

const setEstado = (msg) => (estado.textContent = msg);

const cartDrawer = document.querySelector("#cart-drawer");
const cartBackdrop = document.querySelector("#cart-drawer .cart-backdrop");
const cartClose = document.querySelector("#cart-close");
const cartIcon = document.querySelector(".carrito");
const cartItemsEl = document.querySelector("#cart-items");
const logo = document.querySelector(".logo");
const testingBtn = document.querySelector("#btn-testing-toggle");
const missingDataWrap = document.querySelector("#missing-data-wrap");
const missingDataBtn = document.querySelector("#missing-data-btn");
const tasaActualEl = document.querySelector("#tasa-actual");
const pageLoaderLogoEl = document.querySelector(".page-loader__logo");
const homeBannersWrap = document.querySelector("#home-banners");
const homeBannersViewport = document.querySelector(".home-banners-viewport");
const homeBannersTrack = document.querySelector("#home-banners-track");
const homeBannersDots = document.querySelector("#home-banners-dots");
const HOME_BANNERS_SLIDE_INTERVAL_MS = 5000;
const HOME_BANNERS_DESIGN_WIDTH = 1650;
const HOME_BANNERS_DESIGN_HEIGHT = 828;
const HOME_BANNERS_MIN_RENDER_WIDTH = 480;
const HOME_BANNERS_SWIPE_MIN_PX = 36;
const HOME_BANNERS_SWIPE_RATIO = 0.12;
const HOME_BANNERS_SWIPE_MAX_PX = 84;
const HOME_BANNERS_SWIPE_AXIS_LOCK_PX = 12;
const HOME_BANNERS_CLICK_SUPPRESS_MS = 320;
const HOME_BANNERS_WHEEL_MIN_DELTA = 16;
const HOME_BANNERS_WHEEL_AXIS_RATIO = 1.08;
const HOME_BANNERS_WHEEL_MIN_DOMINANCE = 6;
const HOME_BANNERS_WHEEL_MAX_VERTICAL_DRIFT = 26;
const HOME_BANNERS_WHEEL_COOLDOWN_MS = 220;
const HOME_BANNERS_WHEEL_GESTURE_RESET_MS = 180;
const MOBILE_PULL_REFRESH_THRESHOLD_PX = 160;
const MOBILE_PULL_REFRESH_AXIS_LOCK_PX = 14;
const HOME_BANNER_ROUTE_PREFIX = "/src/frontend/pages/";
const PAGE_LOADER_LOGO_FALLBACK =
  STATIC_HEADER_LOGO_HREF;
let homeBannersSliderTimer = null;
let homeBannersSliderRunId = 0;
let homeBannersCurrentIndex = 0;
let homeBannersTotalSlides = 0;
let homeBannersSwipeActive = false;
let homeBannersSwipePointerId = null;
let homeBannersSwipeTouchId = null;
let homeBannersSwipeInputType = "";
let homeBannersSwipeStartX = 0;
let homeBannersSwipeStartY = 0;
let homeBannersSwipeDeltaX = 0;
let homeBannersSwipeDeltaY = 0;
let homeBannersSwipeAxis = "";
let homeBannersSuppressClickUntil = 0;
let homeBannersWheelAccumX = 0;
let homeBannersWheelResetTimer = null;
let homeBannersWheelLastSlideAt = 0;
let homeBannersWheelDidSlideInGesture = false;
const recordatoriosPendientesWrap = document.querySelector("#recordatorios-pendientes-wrap");
const recordatoriosPendientesList = document.querySelector("#recordatorios-pendientes-list");
const recordatoriosPendientesEmpty = document.querySelector("#recordatorios-pendientes-empty");
const recordatoriosSinTelefonoWrap = document.querySelector("#recordatorios-sin-telefono-wrap");
const recordatoriosSinTelefonoList = document.querySelector("#recordatorios-sin-telefono-list");
const recordatoriosSinTelefonoEmpty = document.querySelector("#recordatorios-sin-telefono-empty");
const loaderAvatarLayerEl = document.querySelector(".page-loader__avatar-layer");
const loaderAvatarBgEl = document.querySelector("#page-loader-avatar-bg");
const loaderAvatarEl = document.querySelector("#page-loader-avatar");
const avatarModalEl = document.querySelector("#avatar-modal");
const avatarModalCloseEl = document.querySelector("#avatar-modal-close");
const avatarModalGridEl = document.querySelector("#avatar-modal-grid");
const avatarModalStatusEl = document.querySelector("#avatar-modal-status");
const btnAvatarSaveEl = document.querySelector("#btn-avatar-save");

const modalEls = {
  modal: document.querySelector("#platform-modal"),
  modalImg: document.querySelector("#modal-image"),
  modalName: document.querySelector("#modal-name"),
  modalCategory: document.querySelector("#modal-category"),
  modalBadge: document.querySelector("#modal-badge"),
  modalPrecios: document.querySelector("#modal-precios"),
  modalQtyMonths: document.querySelectorAll(".modal-qty")[0],
  modalQtyItems: document.querySelectorAll(".modal-qty")[1],
  monthsDiscount: document.querySelector("#months-discount"),
  itemsDiscount: document.querySelector("#items-discount"),
  modalTotal: document.querySelector("#modal-total"),
  qtyMonthsValue: document.querySelector("#qty-months-value"),
  modalQty: document.querySelector(".modal-qty"),
  qtyValue: document.querySelector("#qty-value"),
  btnMinus: document.querySelector("#qty-minus"),
  btnPlus: document.querySelector("#qty-plus"),
  btnMonthsMinus: document.querySelector("#qty-months-minus"),
  btnMonthsPlus: document.querySelector("#qty-months-plus"),
  btnAdd: document.querySelector("#add-cart"),
  closeBtn: document.querySelector("#platform-modal .modal-close"),
  backdrop: document.querySelector("#platform-modal .modal-backdrop"),
};

const searchInput = document.querySelector("#search-input");
const searchResults = document.querySelector("#search-results");
const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const isExplicitFalse = (v) =>
  v === false || v === 0 || v === "0" || v === "false" || v === "f";
const RATE_SLOT_SECONDS = 2 * 60 * 60;
const AVATAR_MODAL_DEFAULT_COLOR = AVATAR_RANDOM_COLORS[0] || "#ffa4a4";
let avatarModalUserId = null;
let avatarModalSavedUrl = "";
let avatarModalSavedColor = AVATAR_MODAL_DEFAULT_COLOR;
let avatarModalPendingUrl = "";
let avatarModalPendingColor = AVATAR_MODAL_DEFAULT_COLOR;
let avatarModalWasPrompted = false;
let tasaRefreshTimer = null;
let tasaAutoRefreshEnabled = false;

const getCaracasNow = () => {
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
  const get = (type) => parts.find((p) => p.type === type)?.value || "";
  return {
    fecha: `${get("year")}-${get("month")}-${get("day")}`,
    hora: `${get("hour")}:${get("minute")}:${get("second")}`,
  };
};

const parseCaracasDate = (fechaStr, horaStr) => {
  if (!fechaStr || !horaStr) return null;
  const dt = new Date(`${fechaStr}T${horaStr}-04:00`);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const parseCaracasClock = (horaStr) => {
  const [h = "0", m = "0", s = "0"] = String(horaStr || "00:00:00").split(":");
  const hh = Number(h);
  const mm = Number(m);
  const ss = Number(s);
  if (!Number.isFinite(hh) || !Number.isFinite(mm) || !Number.isFinite(ss)) return null;
  return { hh, mm, ss };
};

const getNextRateUpdateDate = () => {
  const nowVz = getCaracasNow();
  const nowDt = parseCaracasDate(nowVz.fecha, nowVz.hora);
  const clock = parseCaracasClock(nowVz.hora);
  if (!nowDt || !clock) return null;
  const secSinceMidnight = clock.hh * 3600 + clock.mm * 60 + clock.ss;
  const mod = secSinceMidnight % RATE_SLOT_SECONDS;
  const secToNextSlot = mod === 0 ? RATE_SLOT_SECONDS : RATE_SLOT_SECONDS - mod;
  return new Date(nowDt.getTime() + secToNextSlot * 1000);
};

const getTasaBsValue = (rate) => {
  if (!Number.isFinite(rate)) return null;
  const tasaVal = Math.round(rate * TASA_MARKUP * 100) / 100;
  return Number.isFinite(tasaVal) ? tasaVal : null;
};

const renderTasaActual = (rate) => {
  if (!tasaActualEl) return;
  const tasaVal = getTasaBsValue(rate);
  if (!Number.isFinite(tasaVal)) {
    tasaActualEl.classList.add("hidden");
    return;
  }
  tasaActualEl.textContent = `Tasa actual: Bs. ${tasaVal.toFixed(2)}`;
  tasaActualEl.classList.remove("hidden");
};

const stopTasaAutoRefresh = () => {
  tasaAutoRefreshEnabled = false;
  if (!tasaRefreshTimer) return;
  window.clearTimeout(tasaRefreshTimer);
  tasaRefreshTimer = null;
};

const startTasaAutoRefresh = () => {
  stopTasaAutoRefresh();
  tasaAutoRefreshEnabled = true;
  const scheduleNext = () => {
    if (!tasaAutoRefreshEnabled) return;
    const nextRateUpdate = getNextRateUpdateDate();
    if (!nextRateUpdate) return;
    const waitMs = Math.max(1000, nextRateUpdate.getTime() - Date.now());
    tasaRefreshTimer = window.setTimeout(async () => {
      try {
        const rate = await fetchP2PRate();
        renderTasaActual(rate);
      } finally {
        scheduleNext();
      }
    }, waitMs + 600);
  };
  scheduleNext();
};

const normalizeHexColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  if (/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash)) return withHash.toLowerCase();
  return "";
};

const applyPageLoaderLogo = (url = "") => {
  if (!pageLoaderLogoEl) return;
  const logoUrl = String(url || "").trim() || PAGE_LOADER_LOGO_FALLBACK;
  pageLoaderLogoEl.style.backgroundImage = `url(${JSON.stringify(logoUrl)})`;
};

const loadPageLoaderLogo = async () => {
  applyPageLoaderLogo(PAGE_LOADER_LOGO_FALLBACK);
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const isImageName = (name) =>
  /\.(png|jpe?g|webp|gif|bmp|svg|avif)$/i.test(String(name || ""));

const setAvatarModalStatus = (msg, isError = false) => {
  if (!avatarModalStatusEl) return;
  avatarModalStatusEl.textContent = msg || "";
  avatarModalStatusEl.classList.toggle("is-error", isError);
  avatarModalStatusEl.classList.toggle("is-success", !isError && !!msg);
};

const updateAvatarModalSelectionStyles = () => {
  const selectedUrl = String(avatarModalPendingUrl || "");
  avatarModalEl?.querySelectorAll(".avatar-option[data-avatar-url]").forEach((btn) => {
    btn.classList.toggle("selected", btn.dataset.avatarUrl === selectedUrl);
  });

  const selectedColor = normalizeHexColor(avatarModalPendingColor);
  avatarModalEl?.querySelectorAll(".avatar-palette-dot[data-color]").forEach((dot) => {
    const dotColor = normalizeHexColor(dot.dataset.color);
    dot.classList.toggle("selected", !!selectedColor && dotColor === selectedColor);
  });
};

const applyAvatarModalPreview = ({ url, color } = {}) => {
  const nextUrl = String(url || "").trim();
  const nextColor = normalizeHexColor(color) || AVATAR_MODAL_DEFAULT_COLOR;
  if (nextUrl) {
    avatarModalEl?.querySelectorAll(".avatar-option img").forEach((img) => {
      if (!img.getAttribute("src")) img.setAttribute("src", nextUrl);
    });
  }
  avatarModalEl?.querySelectorAll(".avatar-option").forEach((btn) => {
    btn.style.setProperty("--avatar-bg", nextColor);
  });
  updateAvatarModalSelectionStyles();
};

const closeAvatarModal = (revert = true) => {
  if (!avatarModalEl) return;
  if (revert) {
    avatarModalPendingUrl = avatarModalSavedUrl;
    avatarModalPendingColor = avatarModalSavedColor;
    applyAvatarModalPreview({
      url: avatarModalSavedUrl,
      color: avatarModalSavedColor,
    });
  }
  avatarModalEl.classList.add("hidden");
  document.body.classList.remove("modal-open");
};

const isAvatarImageUrlValid = async (url, timeoutMs = 2500) => {
  const candidate = String(url || "").trim();
  if (!candidate) return false;
  return await new Promise((resolve) => {
    let done = false;
    const finish = (value) => {
      if (done) return;
      done = true;
      resolve(!!value);
    };
    const img = new Image();
    const timer = setTimeout(() => finish(false), timeoutMs);
    img.onload = () => {
      clearTimeout(timer);
      finish(true);
    };
    img.onerror = () => {
      clearTimeout(timer);
      finish(false);
    };
    img.src = candidate;
  });
};

const openAvatarModal = () => {
  if (!avatarModalEl) return;
  avatarModalPendingUrl = avatarModalSavedUrl;
  avatarModalPendingColor = avatarModalSavedColor || AVATAR_MODAL_DEFAULT_COLOR;
  avatarModalEl.classList.remove("hidden");
  document.body.classList.add("modal-open");
  applyAvatarModalPreview({
    url: avatarModalPendingUrl,
    color: avatarModalPendingColor,
  });
};

const renderAvatarModalOptions = (items = []) => {
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

  const hasCurrent = rows.some(
    (item) => String(item?.publicUrl || "").trim() === String(avatarModalPendingUrl || "").trim(),
  );
  if (!hasCurrent) {
    avatarModalPendingUrl = String(rows[0]?.publicUrl || "").trim();
  }
  applyAvatarModalPreview({
    url: avatarModalPendingUrl,
    color: avatarModalPendingColor,
  });
};

const loadAvatarModalOptions = async () => {
  if (!avatarModalEl) return;
  try {
    setAvatarModalStatus("Cargando iconos...");
    const url = new URL(`${API_BASE}/api/logos/list`);
    url.searchParams.set("folder", "icono-perfil");
    if (avatarModalUserId) url.searchParams.set("id_usuario", String(avatarModalUserId));
    const res = await fetch(url.toString(), { credentials: "include" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data?.error || "No se pudo cargar la lista de iconos.");

    renderAvatarModalOptions(data?.items || []);
    setAvatarModalStatus("");
  } catch (err) {
    console.error("avatar modal load options error", err);
    renderAvatarModalOptions([]);
    setAvatarModalStatus("No se pudieron cargar los iconos.", true);
  }
};

const saveAvatarModalProfile = async () => {
  if (!avatarModalUserId) {
    setAvatarModalStatus("No se pudo identificar el usuario.", true);
    return;
  }
  if (!avatarModalPendingUrl) {
    setAvatarModalStatus("Selecciona una imagen de perfil.", true);
    return;
  }
  try {
    if (btnAvatarSaveEl) btnAvatarSaveEl.disabled = true;
    setAvatarModalStatus("Guardando...");
    const payload = {
      foto_perfil: avatarModalPendingUrl,
      fondo_perfil: normalizeHexColor(avatarModalPendingColor) || AVATAR_MODAL_DEFAULT_COLOR,
    };
    const { error } = await supabase
      .from("usuarios")
      .update(payload)
      .eq("id_usuario", avatarModalUserId);
    if (error) throw error;

    const refreshedUser = await loadCurrentUser();
    const refreshedAvatar = await resolveAvatarForDisplay({
      user: refreshedUser || payload,
      idUsuario: avatarModalUserId,
    });
    avatarModalSavedUrl =
      String(refreshedUser?.foto_perfil || payload.foto_perfil || refreshedAvatar?.url || "").trim();
    avatarModalSavedColor =
      normalizeHexColor(refreshedUser?.fondo_perfil) ||
      normalizeHexColor(payload.fondo_perfil) ||
      normalizeHexColor(refreshedAvatar?.color) ||
      AVATAR_MODAL_DEFAULT_COLOR;

    document.querySelectorAll(".avatar").forEach((img) => {
      applyAvatarImage(img, avatarModalSavedUrl);
      img.style.backgroundColor = avatarModalSavedColor;
    });
    await applyLoaderAvatar(
      {
        id_usuario: avatarModalUserId,
        foto_perfil: avatarModalSavedUrl,
        fondo_perfil: avatarModalSavedColor,
      },
      avatarModalUserId,
    );
    setAvatarModalStatus("Foto guardada correctamente.");
    closeAvatarModal(false);
  } catch (err) {
    console.error("avatar modal save error", err);
    setAvatarModalStatus("No se pudo guardar la foto de perfil.", true);
  } finally {
    if (btnAvatarSaveEl) btnAvatarSaveEl.disabled = false;
  }
};

const maybePromptAvatarProfileSetup = async (currentUser) => {
  if (!avatarModalEl || avatarModalWasPrompted) return;
  const userId = Number(currentUser?.id_usuario) || null;
  if (!userId) return;

  const foto = String(currentUser?.foto_perfil || "").trim();
  const fondo = normalizeHexColor(currentUser?.fondo_perfil);
  const fotoValida = await isAvatarImageUrlValid(foto);
  if (fotoValida) return;

  avatarModalWasPrompted = true;
  avatarModalUserId = userId;
  const fallbackAvatar = await resolveAvatarForDisplay({
    user: currentUser || null,
    idUsuario: userId,
  });
  avatarModalSavedUrl = String(fallbackAvatar?.url || "").trim();
  avatarModalSavedColor =
    fondo || normalizeHexColor(fallbackAvatar?.color) || AVATAR_MODAL_DEFAULT_COLOR;

  openAvatarModal();
  await loadAvatarModalOptions();
};

avatarModalEl?.addEventListener("click", (e) => {
  const option = e.target.closest(".avatar-option[data-avatar-url]");
  if (option) {
    avatarModalPendingUrl = String(option.dataset.avatarUrl || "").trim() || avatarModalPendingUrl;
    applyAvatarModalPreview({
      url: avatarModalPendingUrl,
      color: avatarModalPendingColor,
    });
    return;
  }

  const colorDot = e.target.closest(".avatar-palette-dot[data-color]");
  if (colorDot) {
    avatarModalPendingColor =
      normalizeHexColor(colorDot.dataset.color) || avatarModalPendingColor;
    applyAvatarModalPreview({
      url: avatarModalPendingUrl,
      color: avatarModalPendingColor,
    });
    return;
  }

  if (e.target.classList.contains("modal-backdrop")) {
    closeAvatarModal(true);
  }
});

avatarModalCloseEl?.addEventListener("click", () => closeAvatarModal(true));
btnAvatarSaveEl?.addEventListener("click", saveAvatarModalProfile);

const applyLoaderAvatar = async (user = null, idUsuario = null) => {
  if (!loaderAvatarLayerEl && !loaderAvatarEl && !loaderAvatarBgEl) return;

  const hasSession = Number(idUsuario || user?.id_usuario) > 0;
  const fotoPerfil = String(user?.foto_perfil || "").trim();
  if (!hasSession || !fotoPerfil) {
    if (loaderAvatarLayerEl) loaderAvatarLayerEl.classList.add("hidden");
    if (loaderAvatarEl) applyAvatarImage(loaderAvatarEl, "");
    if (loaderAvatarBgEl) loaderAvatarBgEl.style.backgroundColor = "";
    return;
  }

  const fotoValida = await isAvatarImageUrlValid(fotoPerfil);
  if (!fotoValida) {
    if (loaderAvatarLayerEl) loaderAvatarLayerEl.classList.add("hidden");
    if (loaderAvatarEl) applyAvatarImage(loaderAvatarEl, "");
    if (loaderAvatarBgEl) loaderAvatarBgEl.style.backgroundColor = "";
    return;
  }

  if (loaderAvatarLayerEl) {
    loaderAvatarLayerEl.classList.remove("hidden");
  }

  const fallbackAvatar = await resolveAvatarForDisplay({ user, idUsuario });
  const bgColor =
    normalizeHexColor(user?.fondo_perfil) ||
    normalizeHexColor(fallbackAvatar?.color) ||
    AVATAR_MODAL_DEFAULT_COLOR;
  if (loaderAvatarEl) applyAvatarImage(loaderAvatarEl, fotoPerfil);
  if (loaderAvatarBgEl) loaderAvatarBgEl.style.backgroundColor = bgColor;
};

const getCaracasDateStr = (offsetDays = 0) => {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const year = Number(parts.find((p) => p.type === "year")?.value || 0);
  const month = Number(parts.find((p) => p.type === "month")?.value || 0);
  const day = Number(parts.find((p) => p.type === "day")?.value || 0);
  const base = new Date(Date.UTC(year, month - 1, day));
  base.setUTCDate(base.getUTCDate() + offsetDays);
  return base.toISOString().slice(0, 10);
};

const formatToDDMMYYYY = (value) => {
  const str = String(value || "").trim().slice(0, 10);
  const [year, month, day] = str.split("-");
  if (!year || !month || !day) return str;
  return `${day}-${month}-${year}`;
};

const normalizeBannerRedirect = (value = "") => {
  let raw = String(value || "").trim();
  if (!raw) return "";

  const getPagesRoutePrefix = () => {
    if (typeof window === "undefined") return "/";
    const marker = "/src/frontend/pages/";
    const currentPath = String(window.location.pathname || "");
    return currentPath.includes(marker) ? marker : "/";
  };

  // Los banners del admin pueden guardar rutas internas con /src/frontend/pages/.
  // Cuando ya vienen completas se respetan tal cual.
  const toAppRoute = (pathLike = "") => {
    const txt = String(pathLike || "").trim();
    if (!txt) return txt;
    const prefix = getPagesRoutePrefix();
    const joinWithPrefix = (relativePath = "") => {
      const rel = String(relativePath || "").replace(/^\/+/, "");
      if (!rel) return prefix === "/" ? "/" : prefix;
      return prefix === "/" ? `/${rel}` : `${prefix}${rel}`;
    };

    // Si ya viene en ruta absoluta interna del proyecto, respetarla tal cual.
    if (txt.startsWith(HOME_BANNER_ROUTE_PREFIX)) {
      return txt;
    }
    if (txt.startsWith("/src/frontend/pages/")) {
      return txt;
    }
    if (txt.startsWith("src/frontend/pages/")) {
      return `/${txt.replace(/^\/+/, "")}`;
    }
    if (prefix !== "/" && /^\/.+\.html(?:[?#].*)?$/i.test(txt)) {
      return joinWithPrefix(txt.slice(1));
    }
    return txt;
  };

  if (/^https?:\/\//i.test(raw)) {
    try {
      const parsed = new URL(raw);
      const mappedPath = toAppRoute(parsed.pathname || "");
      if (
        mappedPath !== parsed.pathname &&
        typeof window !== "undefined" &&
        parsed.origin === window.location.origin
      ) {
        return `${mappedPath}${parsed.search || ""}${parsed.hash || ""}`;
      }
      return raw;
    } catch (_err) {
      return raw;
    }
  }
  if (/^(mailto:|tel:)/i.test(raw)) return raw;

  raw = toAppRoute(raw);
  if (raw.startsWith("/")) return raw;
  if (raw.startsWith("./") || raw.startsWith("../")) {
    try {
      return new URL(raw, window.location.href).href;
    } catch (_err) {
      return raw;
    }
  }

  const pathPart = raw.split(/[?#]/, 1)[0] || raw;

  // Si parece ruta local (ej: "checkout.html", "admin/ordenes.html"), mantenerla como ruta del sitio.
  const localPathLike =
    pathPart.includes("/") ||
    pathPart.endsWith(".html") ||
    pathPart.endsWith(".htm") ||
    pathPart.endsWith(".php") ||
    pathPart.startsWith("index");
  if (localPathLike) return toAppRoute(`/${raw.replace(/^\/+/, "")}`);

  // Solo tratar como dominio externo si tiene formato de host real.
  const looksLikeDomain = /^[a-z0-9.-]+\.[a-z]{2,}(?:[/:?#]|$)/i.test(raw);
  if (!/\s/.test(raw) && looksLikeDomain) return `https://${raw}`;

  return raw;
};

const normalizePathnameForCompare = (pathname = "") => {
  const raw = String(pathname || "").trim();
  if (!raw) return "/";
  const normalized = raw.replace(/\/+$/g, "");
  return normalized || "/";
};

const isIndexLikePath = (pathname = "") => {
  const normalized = normalizePathnameForCompare(pathname).toLowerCase();
  return (
    normalized === "/" ||
    normalized.endsWith("/index.html") ||
    normalized.endsWith("/src/frontend/pages/index.html")
  );
};

const applyInPageHashNavigation = (hashValue = "") => {
  const hash = String(hashValue || "").trim();
  if (!hash.startsWith("#")) return false;
  try {
    const el = document.querySelector(hash);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "start" });
      window.history.replaceState(null, "", hash);
      return true;
    }
  } catch (_err) {
    // ignore and fallback below
  }
  window.location.hash = hash;
  return true;
};

const navigateBannerRedirect = (value = "") => {
  const raw = String(value || "").trim();
  if (!raw) return false;
  if (raw.startsWith("#")) {
    return applyInPageHashNavigation(raw);
  }

  const redirectHref = normalizeBannerRedirect(raw);
  if (!redirectHref) return false;

  try {
    const target = new URL(redirectHref, window.location.href);
    const current = new URL(window.location.href);
    const sameOrigin = target.origin === current.origin;
    const samePath =
      normalizePathnameForCompare(target.pathname) ===
      normalizePathnameForCompare(current.pathname);
    const bothIndex =
      isIndexLikePath(target.pathname) && isIndexLikePath(current.pathname);
    if (sameOrigin && target.hash && (samePath || bothIndex)) {
      return applyInPageHashNavigation(target.hash);
    }
  } catch (_err) {
    // fallback to normal navigation below
  }

  window.location.href = redirectHref;
  return true;
};

const stopHomeBannersSlider = () => {
  homeBannersSliderRunId += 1;
  if (homeBannersSliderTimer) {
    window.clearTimeout(homeBannersSliderTimer);
    homeBannersSliderTimer = null;
  }
};

const updateHomeBannersDots = (activeIndex = 0) => {
  if (!homeBannersDots) return;
  const safeIndex = Number.isFinite(Number(activeIndex))
    ? Math.max(0, Math.trunc(Number(activeIndex)))
    : 0;
  homeBannersDots
    .querySelectorAll(".home-banner-dot[data-index]")
    .forEach((dotEl) => {
      const dotIndex = Number(dotEl.dataset.index);
      dotEl.classList.toggle("is-active", dotIndex === safeIndex);
    });
};

const renderHomeBannersDots = (total = 0) => {
  if (!homeBannersDots) return;
  const count = Number.isFinite(Number(total)) ? Math.max(0, Math.trunc(Number(total))) : 0;
  if (count <= 0) {
    homeBannersDots.innerHTML = "";
    homeBannersDots.classList.add("hidden");
    return;
  }
  homeBannersDots.innerHTML = Array.from({ length: count }, (_, idx) => {
    return `<span class="home-banner-dot" data-index="${idx}"></span>`;
  }).join("");
  homeBannersDots.classList.remove("hidden");
  updateHomeBannersDots(0);
};

const setHomeBannersSlide = (index = 0) => {
  if (!homeBannersTrack) return;
  const total =
    homeBannersTotalSlides > 0
      ? homeBannersTotalSlides
      : Math.max(0, homeBannersTrack.children?.length || 0);
  let safeIndex = Number.isFinite(Number(index)) ? Math.trunc(Number(index)) : 0;
  if (total > 0) {
    safeIndex = ((safeIndex % total) + total) % total;
  } else {
    safeIndex = Math.max(0, safeIndex);
  }
  homeBannersCurrentIndex = safeIndex;
  homeBannersTrack.style.transform = `translateX(-${safeIndex * 100}%)`;
  updateHomeBannersDots(safeIndex);
};

const startHomeBannersSlider = (total = 0, startIndex = 0) => {
  stopHomeBannersSlider();
  if (!homeBannersTrack) return;

  const count = Number.isFinite(Number(total)) ? Math.max(0, Math.trunc(Number(total))) : 0;
  homeBannersTotalSlides = count;
  const initialIndex = Number.isFinite(Number(startIndex))
    ? Math.trunc(Number(startIndex))
    : 0;
  setHomeBannersSlide(initialIndex);
  if (count <= 1) return;
  const runId = homeBannersSliderRunId;

  const scheduleNext = () => {
    if (runId !== homeBannersSliderRunId) return;
    if (homeBannersSliderTimer) {
      window.clearTimeout(homeBannersSliderTimer);
      homeBannersSliderTimer = null;
    }
    homeBannersSliderTimer = window.setTimeout(() => {
      if (runId !== homeBannersSliderRunId) return;
      if (!homeBannersTrack || homeBannersTotalSlides <= 1) {
        stopHomeBannersSlider();
        return;
      }
      homeBannersCurrentIndex = (homeBannersCurrentIndex + 1) % homeBannersTotalSlides;
      setHomeBannersSlide(homeBannersCurrentIndex);
      // Reinicia el contador en cada nueva posición.
      scheduleNext();
    }, HOME_BANNERS_SLIDE_INTERVAL_MS);
  };

  // Inicia el contador desde cero para la posición actual.
  scheduleNext();
};

const attachHomeBannersSwipe = () => {
  if (!homeBannersViewport || homeBannersViewport.dataset.swipeBound === "1") return;
  homeBannersViewport.dataset.swipeBound = "1";

  const resetSwipeState = () => {
    homeBannersSwipeActive = false;
    homeBannersSwipePointerId = null;
    homeBannersSwipeTouchId = null;
    homeBannersSwipeInputType = "";
    homeBannersSwipeStartX = 0;
    homeBannersSwipeStartY = 0;
    homeBannersSwipeDeltaX = 0;
    homeBannersSwipeDeltaY = 0;
    homeBannersSwipeAxis = "";
  };

  const beginSwipe = ({ x = 0, y = 0, pointerId = null, touchId = null, inputType = "" } = {}) => {
    homeBannersSwipeActive = true;
    homeBannersSwipePointerId = pointerId;
    homeBannersSwipeTouchId = touchId;
    homeBannersSwipeInputType = String(inputType || "");
    homeBannersSwipeStartX = Number.isFinite(Number(x)) ? Number(x) : 0;
    homeBannersSwipeStartY = Number.isFinite(Number(y)) ? Number(y) : 0;
    homeBannersSwipeDeltaX = 0;
    homeBannersSwipeDeltaY = 0;
    homeBannersSwipeAxis = "";
  };

  const suppressBannerClick = (extraMs = 0) => {
    const waitMs = Math.max(HOME_BANNERS_CLICK_SUPPRESS_MS, Number(extraMs) || 0);
    homeBannersSuppressClickUntil = Date.now() + waitMs;
  };

  const updateSwipeDeltasAndAxis = ({ x = 0, y = 0 } = {}) => {
    if (!homeBannersSwipeActive) return "";
    homeBannersSwipeDeltaX = Number(x) - homeBannersSwipeStartX;
    homeBannersSwipeDeltaY = Number(y) - homeBannersSwipeStartY;

    if (!homeBannersSwipeAxis) {
      const absX = Math.abs(homeBannersSwipeDeltaX);
      const absY = Math.abs(homeBannersSwipeDeltaY);
      if (Math.max(absX, absY) < HOME_BANNERS_SWIPE_AXIS_LOCK_PX) return "";
      if (absY > absX) {
        // Gesto vertical: bloquear interacción del banner y dejar scroll del HTML.
        resetSwipeState();
        return "y";
      }
      homeBannersSwipeAxis = "x";
      stopHomeBannersSlider();
    }

    return homeBannersSwipeAxis;
  };

  const getTouchById = (touchList, touchId) => {
    if (!touchList || touchId === null || touchId === undefined) return null;
    for (let i = 0; i < touchList.length; i += 1) {
      const touch = touchList.item(i);
      if (touch && touch.identifier === touchId) return touch;
    }
    return null;
  };

  const finalizeSwipe = ({ cancelled = false } = {}) => {
    if (!homeBannersSwipeActive) return;
    const total = Math.max(0, homeBannersTotalSlides);
    const isMouseSwipe = homeBannersSwipeInputType === "mouse";
    const threshold = Math.max(
      HOME_BANNERS_SWIPE_MIN_PX,
      Math.min(
        isMouseSwipe ? 48 : HOME_BANNERS_SWIPE_MAX_PX,
        Math.round((homeBannersViewport.clientWidth || 0) * HOME_BANNERS_SWIPE_RATIO),
      ),
    );
    const absDelta = Math.abs(homeBannersSwipeDeltaX);
    const isHorizontalSwipe = homeBannersSwipeAxis === "x";
    if (!cancelled && isHorizontalSwipe && total > 1 && absDelta >= threshold) {
      if (homeBannersSwipeDeltaX < 0) {
        homeBannersCurrentIndex = (homeBannersCurrentIndex + 1) % total;
      } else {
        homeBannersCurrentIndex = (homeBannersCurrentIndex - 1 + total) % total;
      }
      suppressBannerClick();
    }

    setHomeBannersSlide(homeBannersCurrentIndex);
    if (total > 1) {
      startHomeBannersSlider(total, homeBannersCurrentIndex);
    }
    resetSwipeState();
  };

  homeBannersViewport.addEventListener("pointerdown", (e) => {
    const total = Math.max(0, homeBannersTotalSlides);
    if (total <= 1) return;
    if (e.pointerType === "touch") return;
    if (e.pointerType === "mouse" && e.button !== 0) return;
    beginSwipe({
      x: e.clientX,
      y: e.clientY,
      pointerId: e.pointerId,
      touchId: null,
      inputType: e.pointerType || "mouse",
    });
  });

  homeBannersViewport.addEventListener("pointermove", (e) => {
    if (!homeBannersSwipeActive || e.pointerId !== homeBannersSwipePointerId) return;
    const axis = updateSwipeDeltasAndAxis({
      x: e.clientX,
      y: e.clientY,
    });
    if (axis === "x") {
      // Evita que el browser secuestre el gesto horizontal y cancele el swipe.
      e.preventDefault();
    }
  }, { passive: false });

  homeBannersViewport.addEventListener("pointerup", (e) => {
    if (!homeBannersSwipeActive || e.pointerId !== homeBannersSwipePointerId) return;
    finalizeSwipe({ cancelled: false });
  });

  homeBannersViewport.addEventListener("pointercancel", (e) => {
    if (!homeBannersSwipeActive || e.pointerId !== homeBannersSwipePointerId) return;
    finalizeSwipe({ cancelled: true });
  });

  window.addEventListener("pointerup", (e) => {
    if (!homeBannersSwipeActive || e.pointerId !== homeBannersSwipePointerId) return;
    finalizeSwipe({ cancelled: false });
  });

  window.addEventListener("pointercancel", (e) => {
    if (!homeBannersSwipeActive || e.pointerId !== homeBannersSwipePointerId) return;
    finalizeSwipe({ cancelled: true });
  });

  homeBannersViewport.addEventListener("touchstart", (e) => {
    const total = Math.max(0, homeBannersTotalSlides);
    if (total <= 1) return;
    if (homeBannersSwipeActive) return;
    const touch = e.changedTouches?.item(0);
    if (!touch) return;
    beginSwipe({
      x: touch.clientX,
      y: touch.clientY,
      pointerId: null,
      touchId: touch.identifier,
      inputType: "touch",
    });
  }, { passive: true });

  homeBannersViewport.addEventListener("touchmove", (e) => {
    if (!homeBannersSwipeActive || homeBannersSwipeTouchId === null) return;
    const touch = getTouchById(e.touches, homeBannersSwipeTouchId);
    if (!touch) return;
    const axis = updateSwipeDeltasAndAxis({
      x: touch.clientX,
      y: touch.clientY,
    });
    if (axis === "x") {
      e.preventDefault();
    }
  }, { passive: false });

  homeBannersViewport.addEventListener("touchend", (e) => {
    if (!homeBannersSwipeActive || homeBannersSwipeTouchId === null) return;
    const touch = getTouchById(e.changedTouches, homeBannersSwipeTouchId);
    if (!touch) return;
    homeBannersSwipeDeltaX = touch.clientX - homeBannersSwipeStartX;
    homeBannersSwipeDeltaY = touch.clientY - homeBannersSwipeStartY;
    finalizeSwipe({ cancelled: false });
  });

  homeBannersViewport.addEventListener("touchcancel", (e) => {
    if (!homeBannersSwipeActive || homeBannersSwipeTouchId === null) return;
    const touch = getTouchById(e.changedTouches, homeBannersSwipeTouchId);
    if (touch) {
      homeBannersSwipeDeltaX = touch.clientX - homeBannersSwipeStartX;
      homeBannersSwipeDeltaY = touch.clientY - homeBannersSwipeStartY;
    }
    finalizeSwipe({ cancelled: true });
  });

  homeBannersViewport.addEventListener("wheel", (e) => {
    const total = Math.max(0, homeBannersTotalSlides);
    if (total <= 1) return;

    const deltaX = Number(e.deltaX) || 0;
    const deltaY = Number(e.deltaY) || 0;
    const absX = Math.abs(deltaX);
    const absY = Math.abs(deltaY);
    const isVerticalIntent = absY > absX * 1.02 && absY > 2;
    if (isVerticalIntent) {
      // Asegura scroll vertical del body cuando el cursor está sobre el banner.
      e.preventDefault();
      window.scrollBy({ top: deltaY, left: 0, behavior: "auto" });
      return;
    }

    const dominance = absX - absY;
    const isHorizontalIntent =
      absX >= HOME_BANNERS_WHEEL_MIN_DELTA &&
      absX >= absY * HOME_BANNERS_WHEEL_AXIS_RATIO &&
      dominance >= HOME_BANNERS_WHEEL_MIN_DOMINANCE;
    if (!isHorizontalIntent) return;
    // Si hay deriva vertical apreciable, dejar que el HTML haga scroll.
    if (absY > HOME_BANNERS_WHEEL_MAX_VERTICAL_DRIFT) return;

    e.preventDefault();
    stopHomeBannersSlider();

    homeBannersWheelAccumX += deltaX;
    if (homeBannersWheelResetTimer) {
      window.clearTimeout(homeBannersWheelResetTimer);
    }
    homeBannersWheelResetTimer = window.setTimeout(() => {
      homeBannersWheelAccumX = 0;
      homeBannersWheelResetTimer = null;
      homeBannersWheelDidSlideInGesture = false;
    }, HOME_BANNERS_WHEEL_GESTURE_RESET_MS);

    if (homeBannersWheelDidSlideInGesture) return;
    if (Math.abs(homeBannersWheelAccumX) < HOME_BANNERS_WHEEL_MIN_DELTA) return;
    const now = Date.now();
    if (now - homeBannersWheelLastSlideAt < HOME_BANNERS_WHEEL_COOLDOWN_MS) return;

    if (homeBannersWheelAccumX > 0) {
      homeBannersCurrentIndex = (homeBannersCurrentIndex + 1) % total;
    } else {
      homeBannersCurrentIndex = (homeBannersCurrentIndex - 1 + total) % total;
    }
    homeBannersWheelAccumX = 0;
    homeBannersWheelLastSlideAt = now;
    homeBannersWheelDidSlideInGesture = true;
    suppressBannerClick();
    setHomeBannersSlide(homeBannersCurrentIndex);
    startHomeBannersSlider(total, homeBannersCurrentIndex);
  }, { passive: false });

  homeBannersViewport.addEventListener("dragstart", (e) => {
    e.preventDefault();
  });

  homeBannersViewport.addEventListener("click", (e) => {
    if (Date.now() < homeBannersSuppressClickUntil) return;
    const target = e.target;
    if (target && typeof target.closest === "function" && target.closest(".home-banner-card")) {
      return;
    }
    const cards = homeBannersTrack?.querySelectorAll?.(".home-banner-card");
    const activeCard = cards?.[homeBannersCurrentIndex] || null;
    if (!activeCard) return;
    activeCard.dispatchEvent(
      new MouseEvent("click", {
        bubbles: true,
        cancelable: true,
        view: window,
      }),
    );
  });
};

const syncHomeBannersViewportSize = () => {
  if (!homeBannersViewport || !homeBannersTrack) return;
  const imgs = Array.from(homeBannersTrack.querySelectorAll(".home-banner-card img"));
  if (!imgs.length) {
    homeBannersViewport.style.removeProperty("--home-banners-render-width");
    homeBannersViewport.style.removeProperty("--home-banners-aspect-ratio");
    return;
  }

  const loadedImgs = imgs.filter((img) => img.naturalWidth > 0 && img.naturalHeight > 0);
  if (!loadedImgs.length) return;

  const minNaturalWidth = loadedImgs.reduce(
    (acc, img) => Math.min(acc, img.naturalWidth),
    HOME_BANNERS_DESIGN_WIDTH,
  );
  const renderWidth = Math.min(
    HOME_BANNERS_DESIGN_WIDTH,
    Math.max(HOME_BANNERS_MIN_RENDER_WIDTH, Math.round(minNaturalWidth)),
  );
  homeBannersViewport.style.setProperty("--home-banners-render-width", `${renderWidth}px`);

  const first = loadedImgs[0];
  const aspectW = Number(first?.naturalWidth) || HOME_BANNERS_DESIGN_WIDTH;
  const aspectH = Number(first?.naturalHeight) || HOME_BANNERS_DESIGN_HEIGHT;
  if (aspectW > 0 && aspectH > 0) {
    homeBannersViewport.style.setProperty("--home-banners-aspect-ratio", `${aspectW} / ${aspectH}`);
  }
};

const attachMobilePullToRefresh = () => {
  if (typeof window === "undefined" || typeof document === "undefined") return;
  if (document.body?.dataset.mobilePullRefreshBound === "1") return;

  const coarsePointer = window.matchMedia?.("(pointer: coarse)")?.matches === true;
  const smallScreen = window.matchMedia?.("(max-width: 900px)")?.matches === true;
  if (!coarsePointer && !smallScreen) return;

  document.body.dataset.mobilePullRefreshBound = "1";

  let active = false;
  let startX = 0;
  let startY = 0;
  let maxDeltaY = 0;
  let axis = "";

  const reset = () => {
    active = false;
    startX = 0;
    startY = 0;
    maxDeltaY = 0;
    axis = "";
  };

  const isAtTop = () =>
    (window.scrollY || window.pageYOffset || document.documentElement?.scrollTop || 0) <= 0;

  const canStartGesture = (target) => {
    if (!isAtTop()) return false;
    if (document.body.classList.contains("modal-open")) return false;
    if (document.body.classList.contains("cart-drawer-open")) return false;
    const element = target instanceof Element ? target : null;
    if (!element) return true;
    if (element.closest("input, textarea, select, [contenteditable='true']")) return false;
    return true;
  };

  window.addEventListener(
    "touchstart",
    (e) => {
      const touch = e.touches?.item?.(0);
      if (!touch || !canStartGesture(e.target)) {
        reset();
        return;
      }
      active = true;
      startX = touch.clientX;
      startY = touch.clientY;
      maxDeltaY = 0;
      axis = "";
    },
    { passive: true },
  );

  window.addEventListener(
    "touchmove",
    (e) => {
      if (!active) return;
      const touch = e.touches?.item?.(0);
      if (!touch) return;
      const deltaX = touch.clientX - startX;
      const deltaY = touch.clientY - startY;
      if (!axis) {
        const absX = Math.abs(deltaX);
        const absY = Math.abs(deltaY);
        if (Math.max(absX, absY) < MOBILE_PULL_REFRESH_AXIS_LOCK_PX) return;
        axis = absY >= absX ? "y" : "x";
      }
      if (axis !== "y") {
        reset();
        return;
      }
      if (deltaY > 0) {
        maxDeltaY = Math.max(maxDeltaY, deltaY);
      }
    },
    { passive: true },
  );

  const onGestureEnd = () => {
    if (!active) return;
    if (axis === "y" && isAtTop() && maxDeltaY >= MOBILE_PULL_REFRESH_THRESHOLD_PX) {
      window.location.reload();
      return;
    }
    reset();
  };

  window.addEventListener("touchend", onGestureEnd, { passive: true });
  window.addEventListener("touchcancel", reset, { passive: true });
};

const renderHomeBanners = (plataformas = [], categorias = [], customBanners = []) => {
  if (!homeBannersWrap || !homeBannersTrack || !homeBannersViewport) return;
  const useMobileBannerImage =
    typeof window !== "undefined" &&
    (window.matchMedia?.("(max-width: 700px)")?.matches ||
      window.matchMedia?.("(pointer: coarse)")?.matches);

  const source = Array.isArray(plataformas) ? plataformas : [];
  const customRows = Array.isArray(customBanners)
    ? customBanners
        .map((row) => {
          const desktopImage = String(row?.image_url ?? row?.imagen ?? "").trim();
          const mobileImage = String(row?.image_url_mobile ?? row?.imagen_movil ?? "").trim();
          const pickedImage = String(
            useMobileBannerImage ? mobileImage || desktopImage : desktopImage || mobileImage,
          ).trim();
          return {
            id: Number(row?.id_banner) || 0,
            nombre: String(row?.title || "Banner"),
            image: pickedImage,
            redirect: String(
              row?.redirect_url ?? row?.redirect ?? row?.redireccion ?? "",
            ).trim(),
            sourceType: "custom",
          };
        })
        .filter((row) => !!row.image)
    : [];

  const unique = [];
  const seen = new Set();
  if (customRows.length) {
    customRows.forEach((row) => {
      const key = `${row.id}|${row.image}|${row.redirect}`;
      if (seen.has(key)) return;
      seen.add(key);
      unique.push(row);
    });
  } else {
    source.forEach((plat) => {
      const banner = String(plat?.banner || "").trim();
      if (!banner) return;
      const id = Number(plat?.id_plataforma) || 0;
      const key = `${id}|${banner}`;
      if (seen.has(key)) return;
      seen.add(key);
      unique.push({
        id,
        nombre: String(plat?.nombre || "Plataforma"),
        image: banner,
        redirect: "",
        sourceType: "platform",
      });
    });
  }

  if (!unique.length) {
    homeBannersTrack.innerHTML = "";
    renderHomeBannersDots(0);
    homeBannersTotalSlides = 0;
    stopHomeBannersSlider();
    syncHomeBannersViewportSize();
    homeBannersWrap.classList.add("hidden");
    return;
  }

  homeBannersTrack.innerHTML = unique
    .map(
      (item, idx) => `
        <button
          type="button"
          class="home-banner-card"
          data-id-plataforma="${item.id || ""}"
          data-source-type="${item.sourceType || ""}"
          data-redirect-url="${escapeHtml(item.redirect || "")}"
          aria-label="Abrir ${escapeHtml(item.nombre)}"
          title="${escapeHtml(item.nombre)}"
        >
          <img
            src="${escapeHtml(item.image)}"
            alt="${escapeHtml(item.nombre)}"
            draggable="false"
            loading="${idx === 0 ? "eager" : "lazy"}"
            fetchpriority="${idx === 0 ? "high" : "auto"}"
            sizes="(max-width: 700px) calc(100vw - 24px), min(1650px, calc(100vw - 40px))"
            decoding="async"
          />
        </button>
      `,
    )
    .join("");

  homeBannersTrack.querySelectorAll(".home-banner-card img").forEach((img) => {
    if (img.complete) return;
    img.addEventListener("load", syncHomeBannersViewportSize, { once: true });
    img.addEventListener("error", syncHomeBannersViewportSize, { once: true });
  });
  syncHomeBannersViewportSize();
  renderHomeBannersDots(unique.length);

  homeBannersTrack.querySelectorAll(".home-banner-card[data-id-plataforma]").forEach((card, cardIndex) => {
    const bannerItem = unique[cardIndex] || {};
    card.addEventListener("click", () => {
      if (Date.now() < homeBannersSuppressClickUntil) return;
      const redirectRaw = String(
        bannerItem?.redirect || card.dataset.redirectUrl || "",
      ).trim();
      if (redirectRaw) {
        if (navigateBannerRedirect(redirectRaw)) {
          return;
        }
      }
      const sourceType = String(bannerItem?.sourceType || card.dataset.sourceType || "").trim();
      if (sourceType === "custom") {
        return;
      }
      const id = Number(card.dataset.idPlataforma);
      if (!Number.isFinite(id) || id <= 0) return;
      const plataforma = source.find((plat) => Number(plat?.id_plataforma) === id);
      if (!plataforma) return;
      const categoria = (Array.isArray(categorias) ? categorias : []).find(
        (cat) => Number(cat?.id_categoria) === Number(plataforma?.id_categoria),
      )?.nombre;
      openModal({
        ...plataforma,
        categoria: categoria || "",
      });
    });
  });

  startHomeBannersSlider(unique.length);
  attachHomeBannersSwipe();
  homeBannersWrap.classList.remove("hidden");
};

const buildPreciosMap = (precios) =>
  (precios || []).reduce((acc, precio) => {
    if (!precio.id_plataforma) return acc;
    if (!acc[precio.id_plataforma]) acc[precio.id_plataforma] = [];
    acc[precio.id_plataforma].push(precio);
    return acc;
  }, {});

const mapCartItems = (items, precios, plataformas) => {
  const priceById = (precios || []).reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});
  const platformById = (plataformas || []).reduce((acc, p) => {
    acc[p.id_plataforma] = p;
    return acc;
  }, {});

  return (items || []).map((item) => {
    const price = priceById[item.id_precio] || {};
    const platform = platformById[price.id_plataforma] || {};
    const flags = {
      por_pantalla: platform.por_pantalla,
      por_acceso: platform.por_acceso,
      tarjeta_de_regalo: platform.tarjeta_de_regalo,
    };
    const detalle = (() => {
      if (flags.tarjeta_de_regalo) {
        const region = price.region || "-";
        const monto = `${price.valor_tarjeta_de_regalo || ""} ${price.moneda || ""} $${price.precio_usd_detal}`;
        return `Región: ${region} · Monto: ${monto}`;
      }
      const qty = item.cantidad || price.cantidad || 1;
      const meses = item.meses || price.duracion || 1;
      const baseUnit = flags.por_pantalla
        ? "pantalla"
        : flags.por_acceso
        ? "dispositivo"
        : "mes";
      const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
      const mesesTxt = baseUnit === "mes" ? ` · ${meses} mes${meses === 1 ? "" : "es"}` : "";
      return `${qty} ${baseUnit}${plural}${mesesTxt} $${price.precio_usd_detal || ""}`;
    })();

    return {
      id_precio: item.id_precio,
      id_item: item.id_item,
      id_venta: item.id_venta,
      id_plataforma: price.id_plataforma,
      nombre: platform.nombre || `Precio ${item.id_precio}`,
      imagen: platform.imagen,
      plan: price.plan,
      precio: price.precio_usd_detal,
      cantidad: item.cantidad,
      meses: item.meses,
      detalle,
      flags,
      renovacion: item.renovacion,
    };
  });
};

const attachPlatformClicks = (onClick) => {
  document.querySelectorAll(".plataforma-card").forEach((card) => {
    card.addEventListener("click", () =>
      onClick({
        id_plataforma: card.dataset.idPlataforma,
        nombre: card.dataset.nombre,
        categoria: card.dataset.categoria,
        imagen: card.dataset.imagen,
        banner: card.dataset.banner,
        por_pantalla: card.dataset.porPantalla,
        por_acceso: card.dataset.porAcceso,
        tarjeta_de_regalo: card.dataset.tarjetaDeRegalo,
        entrega_inmediata: card.dataset.entregaInmediata,
        descuento_meses: isTrue(card.dataset.descuentoMeses),
        id_descuento_mes: card.dataset.idDescuentoMes,
        id_descuento_cantidad: card.dataset.idDescuentoCantidad,
        id_descuento_mes_detal: card.dataset.idDescuentoMesDetal,
        id_descuento_mes_mayor: card.dataset.idDescuentoMesMayor,
        id_descuento_cantidad_detal: card.dataset.idDescuentoCantidadDetal,
        id_descuento_cantidad_mayor: card.dataset.idDescuentoCantidadMayor,
        aplica_descuento_mes_detal: card.dataset.aplicaDescuentoMesDetal,
        aplica_descuento_mes_mayor: card.dataset.aplicaDescuentoMesMayor,
        aplica_descuento_cantidad_detal: card.dataset.aplicaDescuentoCantidadDetal,
        aplica_descuento_cantidad_mayor: card.dataset.aplicaDescuentoCantidadMayor,
        mostrar_stock: card.dataset.mostrarStock,
        num_max_dispositivos: card.dataset.numMaxDispositivos,
        id_descuento: null,
      })
    );
  });
};

const loadStockSummary = async (canLogStock = false) => {
  const logStock = (...args) => {
    if (!canLogStock) return;
    console.log(...args);
  };
  const logStockError = (...args) => {
    if (!canLogStock) return;
    console.error(...args);
  };

  const [
    { data: perfiles, error: perfErr },
    { data: cuentasMiembro, error: ctaErr },
    { data: cuentasCompletas, error: compErr },
    { data: giftPins, error: giftPinsErr },
    { data: giftPlatforms, error: giftPlatErr },
  ] = await Promise.all([
    supabase
      .from("perfiles")
      .select(
        "id_perfil, n_perfil, ocupado, perfil_hogar, cuentas:cuentas!perfiles_id_cuenta_fkey(id_plataforma, inactiva, venta_perfil, correo, plataformas(nombre))"
      )
      .eq("ocupado", false)
      .eq("cuentas.venta_perfil", true)
      .eq("cuentas.inactiva", false)
      .not("id_cuenta", "is", null),
    supabase
      .from("cuentas")
      .select("id_cuenta, id_plataforma, venta_miembro, venta_perfil, ocupado, inactiva, correo")
      .eq("id_plataforma", 1)
      .eq("venta_perfil", false)
      .eq("venta_miembro", true)
      .eq("ocupado", false)
      .eq("inactiva", false),
    supabase
      .from("cuentas")
      .select("id_cuenta, id_plataforma, venta_miembro, venta_perfil, ocupado, inactiva")
      .eq("venta_perfil", false)
      .eq("venta_miembro", false)
      .eq("ocupado", false)
      .eq("inactiva", false),
    supabase
      .from("tarjetas_de_regalo")
      .select("id_plataforma")
      .eq("para_venta", true)
      .eq("usado", false)
      .not("id_plataforma", "is", null),
    supabase
      .from("plataformas")
      .select("id_plataforma")
      .eq("tarjeta_de_regalo", true),
  ]);
  if (perfErr || ctaErr || compErr || giftPinsErr || giftPlatErr) {
    logStockError("stock summary error", perfErr || ctaErr || compErr || giftPinsErr || giftPlatErr);
    return {};
  }
  let stockObj = {};
  let netflixPlan1 = 0;
  let netflixPlan2 = 0;
  const libresPlan1Correos = [];
  const libresPlan2Correos = [];
  const libresPlan1Perf = [];
  const libresPlan2Perf = [];
  const libresPorPlataforma = {};
  (perfiles || []).forEach((p) => {
    const platId = p.cuentas?.id_plataforma;
    const correoCuenta = p.cuentas?.correo || "";
    const platNombre =
      p.cuentas?.plataformas?.nombre || `Plataforma ${platId || "-"}`;
    const perfilLabel = p.n_perfil != null ? `M${p.n_perfil}` : "";
    if (!platId) return;

    if (platId === 1) {
      if (p.perfil_hogar === true) {
        netflixPlan2 += 1;
        if (correoCuenta) libresPlan2Correos.push(correoCuenta);
        if (correoCuenta || perfilLabel)
          libresPlan2Perf.push({ correo: correoCuenta, perfil: perfilLabel });
      } else if (p.perfil_hogar === false) {
        netflixPlan1 += 1;
        if (correoCuenta) libresPlan1Correos.push(correoCuenta);
        if (correoCuenta || perfilLabel)
          libresPlan1Perf.push({ correo: correoCuenta, perfil: perfilLabel });
      }
    }

    if (!stockObj[platId]) stockObj[platId] = 0;
    if (p.perfil_hogar === false) {
      stockObj[platId] += 1;
    }

    if (!libresPorPlataforma[platId]) {
      libresPorPlataforma[platId] = { nombre: platNombre, items: [] };
    }
    if (p.perfil_hogar === false) {
      libresPorPlataforma[platId].items.push({
        correo: correoCuenta,
        perfil: perfilLabel,
      });
    }
  });

  if (cuentasMiembro?.length) {
    netflixPlan2 += cuentasMiembro.length;
    stockObj[1] = (stockObj[1] || 0) + cuentasMiembro.length;
    cuentasMiembro.forEach((c) => {
      if (c.correo) libresPlan2Correos.push(c.correo);
    });
  }

  const cuentasCompletasFiltradas = (cuentasCompletas || []).filter(
    (c) => c?.venta_perfil === false && c?.venta_miembro === false && c?.inactiva === false
  );
  const completasCount = {};
  cuentasCompletasFiltradas.forEach((c) => {
    const platId = c.id_plataforma || "unknown";
    completasCount[platId] = (completasCount[platId] || 0) + 1;
  });
  Object.keys(completasCount).forEach((platId) => {
    stockObj[`${platId}_completas`] = completasCount[platId];
  });

  const giftStockByPlat = (giftPins || []).reduce((acc, row) => {
    const platId = Number(row?.id_plataforma);
    if (!Number.isFinite(platId) || platId <= 0) return acc;
    acc[platId] = (acc[platId] || 0) + 1;
    return acc;
  }, {});
  (giftPlatforms || []).forEach((plat) => {
    const platId = Number(plat?.id_plataforma);
    if (!Number.isFinite(platId) || platId <= 0) return;
    stockObj[platId] = giftStockByPlat[platId] || 0;
  });

  stockObj["1_plan1"] = netflixPlan1;
  stockObj["1_plan2"] = netflixPlan2;
  stockObj[1] = netflixPlan1 + netflixPlan2;
  logStock("[stock] Netflix plan 1 libres:", netflixPlan1, libresPlan1Correos);
  logStock("[stock] Netflix plan 2 (hogar actualizado) libres:", netflixPlan2, libresPlan2Correos);
  if (libresPlan1Correos.length) {
    logStock("[stock] Netflix plan 1 correo libre:", libresPlan1Correos[0]);
  }
  if (libresPlan2Correos.length) {
    logStock("[stock] Netflix plan 2 correo libre:", libresPlan2Correos[0]);
  }
  if (libresPlan1Perf.length) {
    logStock("[stock] Netflix plan 1 perfil libre:", libresPlan1Perf[0]);
  }
  if (libresPlan2Perf.length) {
    logStock("[stock] Netflix plan 2 perfil libre:", libresPlan2Perf[0]);
  }
  if (typeof stockObj[2] !== "undefined") {
    const plat2 = libresPorPlataforma[2];
    const nombre2 = plat2?.nombre || "Plataforma 2";
    logStock("[stock] Plataforma 2 libres:", stockObj[2]);
    if (plat2?.items?.length) {
      logStock("[stock] Plataforma 2 libres (detalle):", nombre2, plat2.items);
    }
  } else {
    logStock("[stock] Plataforma 2 libres: 0");
  }
  if (typeof stockObj[13] !== "undefined") {
    const plat13 = libresPorPlataforma[13];
    const nombre13 = plat13?.nombre || "Plataforma 13";
    logStock("[stock] Plataforma 13 libres:", stockObj[13]);
    if (plat13?.items?.length) {
      logStock("[stock] Plataforma 13 libres (detalle):", nombre13, plat13.items);
    }
  } else {
    logStock("[stock] Plataforma 13 libres: 0");
  }
  Object.entries(libresPorPlataforma).forEach(([platId, info]) => {
    const nombre = info?.nombre || `Plataforma ${platId}`;
    const items = info?.items || [];
    if (!items.length) {
      logStock(`[stock] Plataforma ${platId} libres (detalle):`, nombre, []);
      return;
    }
    logStock(`[stock] Plataforma ${platId} libres (detalle):`, nombre, items);
  });
  return stockObj;
};

const checkMissingDataNotice = async (currentUser) => {
  if (!missingDataWrap || !missingDataBtn) return;
  if (!currentUser?.id_usuario) {
    missingDataWrap.classList.add("hidden");
    return;
  }
  try {
    const userId = Number(currentUser.id_usuario);
    const { data: ventas, error } = await supabase
      .from("ventas")
      .select("id_venta, pendiente, correo_miembro, clave_miembro, id_precio, precios:precios(id_plataforma, plataformas:plataformas(correo_cliente, clave_cliente))")
      .eq("pendiente", true)
      .or(`id_usuario.eq.${userId},id_admin_venta.eq.${userId}`);
    if (error) throw error;
    const hasMissing = (ventas || []).some((v) => {
      const plat = v.precios?.plataformas || {};
      const needsCorreo = plat.correo_cliente === true || plat.correo_cliente === "true" || plat.correo_cliente === "1";
      if (!needsCorreo) return false;
      const missingCorreo = !v.correo_miembro;
      const needsClave = plat.clave_cliente === true || plat.clave_cliente === "true" || plat.clave_cliente === "1";
      const missingClave = needsClave ? !v.clave_miembro : false;
      return missingCorreo || missingClave;
    });
    missingDataWrap.classList.toggle("hidden", !hasMissing);
    if (hasMissing && !missingDataBtn.dataset.bound) {
      missingDataBtn.addEventListener("click", () => {
        window.location.href = "entregar_servicios.html?faltantes=1";
      });
      missingDataBtn.dataset.bound = "1";
    }
  } catch (err) {
    console.error("missing data notice error", err);
    missingDataWrap.classList.add("hidden");
  }
};

const loadPendingReminderDates = async ({ isSuperadmin = false } = {}) => {
  if (!recordatoriosPendientesWrap || !recordatoriosPendientesList || !recordatoriosPendientesEmpty) return;
  if (!isSuperadmin) {
    recordatoriosPendientesList.innerHTML = "";
    recordatoriosPendientesEmpty.classList.add("hidden");
    recordatoriosPendientesWrap.classList.add("hidden");
    return;
  }

  try {
    const fechaManana = getCaracasDateStr(1);
    const { data, error } = await supabase
      .from("ventas")
      .select("fecha_corte")
      .lte("fecha_corte", fechaManana)
      .or("recordatorio_enviado.eq.false,recordatorio_enviado.is.null");
    if (error) throw error;

    const uniqueDates = Array.from(
      new Set(
        (data || [])
          .map((row) => String(row?.fecha_corte || "").trim().slice(0, 10))
          .filter(Boolean),
      ),
    ).sort();

    recordatoriosPendientesList.innerHTML = "";
    if (!uniqueDates.length) {
      recordatoriosPendientesEmpty.classList.add("hidden");
      recordatoriosPendientesWrap.classList.add("hidden");
      return;
    }

    uniqueDates.forEach((dateStr) => {
      const li = document.createElement("li");
      li.textContent = formatToDDMMYYYY(dateStr);
      recordatoriosPendientesList.appendChild(li);
    });

    recordatoriosPendientesEmpty.classList.add("hidden");
    recordatoriosPendientesWrap.classList.remove("hidden");
  } catch (err) {
    console.error("recordatorios pendientes error", err);
    recordatoriosPendientesEmpty.classList.add("hidden");
    recordatoriosPendientesWrap.classList.add("hidden");
  }
};

const loadPendingReminderNoPhoneClients = async ({ isSuperadmin = false } = {}) => {
  if (!recordatoriosSinTelefonoWrap || !recordatoriosSinTelefonoList || !recordatoriosSinTelefonoEmpty) return;
  if (!isSuperadmin) {
    recordatoriosSinTelefonoList.innerHTML = "";
    recordatoriosSinTelefonoEmpty.classList.add("hidden");
    recordatoriosSinTelefonoWrap.classList.add("hidden");
    return;
  }

  try {
    const resp = await fetchPendingReminderNoPhoneClients();
    if (resp?.error) throw new Error(resp.error);

    const clients = Array.isArray(resp?.clients) ? resp.clients : [];
    recordatoriosSinTelefonoList.innerHTML = "";
    if (!clients.length) {
      recordatoriosSinTelefonoEmpty.classList.add("hidden");
      recordatoriosSinTelefonoWrap.classList.add("hidden");
      return;
    }

    clients.forEach((client) => {
      const li = document.createElement("li");
      const nombre = String(client?.cliente || "Cliente");
      const telefono = String(client?.telefono || "").trim();
      const reason = String(client?.reason || "");
      if (reason === "invalid_phone" && telefono) {
        li.textContent = `${nombre} (número inválido: ${telefono})`;
      } else if (reason === "invalid_phone") {
        li.textContent = `${nombre} (número inválido)`;
      } else {
        li.textContent = nombre;
      }
      recordatoriosSinTelefonoList.appendChild(li);
    });

    recordatoriosSinTelefonoEmpty.classList.add("hidden");
    recordatoriosSinTelefonoWrap.classList.remove("hidden");
  } catch (err) {
    console.error("recordatorios sin telefono pendientes error", err);
    recordatoriosSinTelefonoEmpty.classList.add("hidden");
    recordatoriosSinTelefonoWrap.classList.add("hidden");
  }
};

async function init() {
  setEstado("Cargando categorias y plataformas...");
  initModal(modalEls);
  await loadPageLoaderLogo();
  await applyLoaderAvatar(null, requireSession());

  try {
    try {
      const { data, error } = await supabase.auth.getSession();
      if (error) {
        console.warn("[auth] getSession error", error);
      }
    } catch (err) {
      console.warn("[auth] getSession exception", err);
    }

    const currentUser = await loadCurrentUser();
    await applyLoaderAvatar(currentUser || null, currentUser?.id_usuario || requireSession());
    const btnAssign = document.querySelector("#btn-assign-client");
    if (usernameEl && currentUser) {
      const fullName = [currentUser.nombre, currentUser.apellido]
        .filter(Boolean)
        .join(" ")
        .trim();
      usernameEl.textContent = fullName || currentUser.correo || "Usuario";
    }
    setSessionRoles(currentUser || {});
    maybePromptAvatarProfileSetup(currentUser).catch((err) => {
      console.error("avatar onboarding prompt error", err);
    });
    const sessionRoles = getSessionRoles();
    const canSeeRate = !!currentUser && isExplicitFalse(currentUser?.acceso_cliente);
    if (tasaActualEl) {
      if (!canSeeRate) {
        stopTasaAutoRefresh();
        tasaActualEl.classList.add("hidden");
      } else {
        const rate = await fetchP2PRate();
        renderTasaActual(rate);
        startTasaAutoRefresh();
      }
    }
    const isAdmin =
      isTrue(sessionRoles?.permiso_admin) ||
      isTrue(sessionRoles?.permiso_superadmin) ||
      isTrue(currentUser?.permiso_admin) ||
      isTrue(currentUser?.permiso_superadmin);
    const isSuper =
      isTrue(sessionRoles?.permiso_superadmin) || isTrue(currentUser?.permiso_superadmin);
    if (adminLink) {
      adminLink.classList.toggle("hidden", !isAdmin);
      adminLink.style.display = isAdmin ? "block" : "none";
    }
    if (btnAssign && !btnAssign.dataset.bound) {
      btnAssign.classList.toggle("hidden", !isSuper);
      btnAssign.addEventListener("click", () => {
        window.location.href = "cuenta_nueva.html";
      });
      btnAssign.dataset.bound = "1";
    }
    if (testingBtn) {
      testingBtn.classList.toggle("hidden", !isSuper);
      testingBtn.style.display = isSuper ? "inline-flex" : "none";
    }
    await checkMissingDataNotice(currentUser);
    await loadPendingReminderDates({ isSuperadmin: isSuper });
    await loadPendingReminderNoPhoneClients({ isSuperadmin: isSuper });

    const cachedCart = getCachedCart();
    const cartData = await fetchCart();
    setCachedCart(cartData);
    const [catalog, stockMap, homeBannersResp] = await Promise.all([
      loadCatalog(),
      loadStockSummary(isAdmin),
      fetchHomeBanners(),
    ]);
    const { categorias, plataformas, precios, descuentos } = catalog;
    const plataformasDisponibles = (plataformas || []).filter(
      (plat) => !isTrue(plat?.no_disponible),
    );
    const customHomeBanners = Array.isArray(homeBannersResp?.items)
      ? homeBannersResp.items
      : [];
    renderHomeBanners(plataformasDisponibles, categorias, customHomeBanners);
    // Si no hay sesión, mostrar precios detal por defecto.
    const esCliente =
      !currentUser ||
      isTrue(sessionRoles?.acceso_cliente) ||
      isTrue(currentUser?.acceso_cliente);
    setDiscountAudience(esCliente);
    const usarMayor = !esCliente;
    const preciosVisibles = (precios || [])
      .map((p) => {
        const valor = esCliente ? p.precio_usd_detal : p.precio_usd_mayor;
        if (valor == null) return null;
        return { ...p, precio_usd_detal: valor, precio_usd_mayor: undefined };
      })
      .filter(Boolean);
    setStockData(stockMap);
    const preciosMap = buildPreciosMap(preciosVisibles);
    setPrecios(preciosMap);
    setDescuentos(descuentos || []);
    updateSearchData(plataformasDisponibles);

    const plataformasPorCategoria = (plataformasDisponibles || []).reduce((acc, plat) => {
      if (!acc[plat.id_categoria]) acc[plat.id_categoria] = [];
      acc[plat.id_categoria].push(plat);
      return acc;
    }, {});
    Object.keys(plataformasPorCategoria).forEach((catId) => {
      plataformasPorCategoria[catId].sort((a, b) => {
        const pa = Number(a?.posicion);
        const pb = Number(b?.posicion);
        const hasPa = Number.isFinite(pa) && pa > 0;
        const hasPb = Number.isFinite(pb) && pb > 0;
        if (hasPa && hasPb && pa !== pb) return pa - pb;
        if (hasPa !== hasPb) return hasPa ? -1 : 1;
        const ia = Number(a.id_plataforma);
        const ib = Number(b.id_plataforma);
        if (Number.isFinite(ia) && Number.isFinite(ib)) return ia - ib;
        return String(a?.nombre || "").localeCompare(String(b?.nombre || ""), "es");
      });
    });

    const preciosMinByPlat = Object.entries(preciosMap || {}).reduce((acc, [platId, list]) => {
      const min = (list || []).reduce((m, p) => {
        const val = Number(p?.precio_usd_detal);
        if (!Number.isFinite(val)) return m;
        return m === null || val < m ? val : m;
      }, null);
      if (min !== null) acc[platId] = min;
      return acc;
    }, {});

    setEstado("");
    renderCategorias(contenedor, categorias, plataformasPorCategoria, preciosMinByPlat);
    attachPlatformClicks(openModal);

    initCart({
      drawerEl: cartDrawer,
      backdropEl: cartBackdrop,
      closeEl: cartClose,
      iconEl: cartIcon,
      itemsContainer: cartItemsEl,
      catalog,
      initialItems: mapCartItems(cartData.items || [], preciosVisibles, plataformas),
      initialRawItems: cartData.items || [],
    });
    initSearch({
      input: searchInput,
      results: searchResults,
      data: plataformasDisponibles,
      onSelectItem: (plataforma) => {
        openModal({
          ...plataforma,
          categoria: categorias.find((c) => c.id_categoria === plataforma.id_categoria)?.nombre,
        });
      },
    });

    // Aviso de servicios entregados (solo si hay nuevos) sin badge
    try {
      const userId = requireSession();
      const entregas = await fetchEntregadas();
      if (!entregas?.error) {
        const count = entregas.entregadas || 0;
        const seen = getDeliverySeen(userId);
        if (count > seen) {
          showDeliveryNotice();
          setDeliverySeen(userId, count);
        }
      }
    } catch (err) {
      console.warn("No se pudo cargar entregas", err);
    }

    // Toggle Testing/Production
    if (testingBtn && !testingBtn.dataset.bound) {
      testingBtn.dataset.bound = "1";
      const applyState = (isTesting) => {
        testingBtn.textContent = isTesting === true ? "Testing" : "Production";
        testingBtn.classList.toggle("testing-off", !isTesting);
      };
      fetchTestingFlag().then((flag) => applyState(flag === true));
      testingBtn.addEventListener("click", async () => {
        testingBtn.disabled = true;
        try {
          const currentIsTesting = testingBtn.textContent?.toLowerCase() === "testing";
          const next = !currentIsTesting;
          const updated = await updateTestingFlag(next);
          applyState(updated === true);
        } catch (err) {
          console.error("toggle testing error", err);
        } finally {
          testingBtn.disabled = false;
        }
      });
    }
  } catch (err) {
    setEstado(`Error: ${err.message}`);
  } finally {
    const loader = document.getElementById("page-loader");
    const shell = document.getElementById("app-shell");
    if (shell) shell.classList.remove("hidden");
    if (loader) loader.classList.add("hidden");
  }
}

init();
attachMobilePullToRefresh();
attachLogout(clearServerSession, clearCachedCart);

// Redirección a la página de carrito
const viewCartBtn = document.querySelector("#btn-view-cart");
viewCartBtn?.addEventListener("click", () => {
  window.location.href = "cart.html";
});

// Redirección al inicio al hacer clic en el logo
attachLogoHome();

window.addEventListener("beforeunload", () => {
  stopTasaAutoRefresh();
});
