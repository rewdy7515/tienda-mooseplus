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
} from "./api.js";
import { initCart } from "./cart.js";
import { initModal, openModal, setPrecios, setStockData, setDescuentos } from "./modal.js";
import { initSearch, updateSearchData } from "./search.js";
import { renderCategorias } from "./render.js";
import { AVATAR_RANDOM_COLORS, resolveAvatarForDisplay } from "./avatar-fallback.js";
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
const activeOrderWrap = document.querySelector("#active-order-wrap");
const activeOrderLink = document.querySelector("#active-order-link");
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
const AVATAR_MODAL_DEFAULT_COLOR = AVATAR_RANDOM_COLORS[0] || "#ffa4a4";
let avatarModalUserId = null;
let avatarModalSavedUrl = "";
let avatarModalSavedColor = AVATAR_MODAL_DEFAULT_COLOR;
let avatarModalPendingUrl = "";
let avatarModalPendingColor = AVATAR_MODAL_DEFAULT_COLOR;
let avatarModalWasPrompted = false;

const normalizeHexColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  if (/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash)) return withHash.toLowerCase();
  return "";
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

  if (nextUrl && loaderAvatarEl) loaderAvatarEl.src = nextUrl;
  if (loaderAvatarBgEl) loaderAvatarBgEl.style.backgroundColor = nextColor;
  document.querySelectorAll(".avatar").forEach((img) => {
    if (nextUrl) img.src = nextUrl;
    img.style.backgroundColor = nextColor;
  });
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

    avatarModalSavedUrl = payload.foto_perfil;
    avatarModalSavedColor = payload.fondo_perfil;
    applyAvatarModalPreview({
      url: avatarModalSavedUrl,
      color: avatarModalSavedColor,
    });
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
  if (foto && fondo) return;

  avatarModalWasPrompted = true;
  avatarModalUserId = userId;
  const fallbackAvatar = await resolveAvatarForDisplay({
    user: currentUser || null,
    idUsuario: userId,
  });
  avatarModalSavedUrl = foto || String(fallbackAvatar?.url || "").trim();
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
  const shouldShow = hasSession && !!fotoPerfil;
  if (loaderAvatarLayerEl) {
    loaderAvatarLayerEl.classList.toggle("hidden", !shouldShow);
  }
  if (!shouldShow) return;

  const fallbackAvatar = await resolveAvatarForDisplay({ user, idUsuario });
  const bgColor =
    normalizeHexColor(user?.fondo_perfil) ||
    normalizeHexColor(fallbackAvatar?.color) ||
    AVATAR_MODAL_DEFAULT_COLOR;
  if (loaderAvatarEl) loaderAvatarEl.src = fotoPerfil;
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
        mostrar_stock: card.dataset.mostrarStock,
        num_max_dispositivos: card.dataset.numMaxDispositivos,
        id_descuento: null,
      })
    );
  });
};

const loadStockSummary = async (_hasSession = false) => {
  const [
    { data: perfiles, error: perfErr },
    { data: cuentasMiembro, error: ctaErr },
    { data: cuentasCompletas, error: compErr },
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
  ]);
  if (perfErr || ctaErr || compErr) {
    console.error("stock summary error", perfErr || ctaErr || compErr);
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

  stockObj["1_plan1"] = netflixPlan1;
  stockObj["1_plan2"] = netflixPlan2;
  stockObj[1] = netflixPlan1 + netflixPlan2;
  console.log("[stock] Netflix plan 1 libres:", netflixPlan1, libresPlan1Correos);
  console.log("[stock] Netflix plan 2 (hogar actualizado) libres:", netflixPlan2, libresPlan2Correos);
  if (libresPlan1Correos.length) {
    console.log("[stock] Netflix plan 1 correo libre:", libresPlan1Correos[0]);
  }
  if (libresPlan2Correos.length) {
    console.log("[stock] Netflix plan 2 correo libre:", libresPlan2Correos[0]);
  }
  if (libresPlan1Perf.length) {
    console.log("[stock] Netflix plan 1 perfil libre:", libresPlan1Perf[0]);
  }
  if (libresPlan2Perf.length) {
    console.log("[stock] Netflix plan 2 perfil libre:", libresPlan2Perf[0]);
  }
  if (typeof stockObj[2] !== "undefined") {
    const plat2 = libresPorPlataforma[2];
    const nombre2 = plat2?.nombre || "Plataforma 2";
    console.log("[stock] Plataforma 2 libres:", stockObj[2]);
    if (plat2?.items?.length) {
      console.log("[stock] Plataforma 2 libres (detalle):", nombre2, plat2.items);
    }
  } else {
    console.log("[stock] Plataforma 2 libres: 0");
  }
  if (typeof stockObj[13] !== "undefined") {
    const plat13 = libresPorPlataforma[13];
    const nombre13 = plat13?.nombre || "Plataforma 13";
    console.log("[stock] Plataforma 13 libres:", stockObj[13]);
    if (plat13?.items?.length) {
      console.log("[stock] Plataforma 13 libres (detalle):", nombre13, plat13.items);
    }
  } else {
    console.log("[stock] Plataforma 13 libres: 0");
  }
  Object.entries(libresPorPlataforma).forEach(([platId, info]) => {
    const nombre = info?.nombre || `Plataforma ${platId}`;
    const items = info?.items || [];
    if (!items.length) {
      console.log(`[stock] Plataforma ${platId} libres (detalle):`, nombre, []);
      return;
    }
    console.log(`[stock] Plataforma ${platId} libres (detalle):`, nombre, items);
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

const loadActiveOrderNotice = async (currentUser) => {
  if (!activeOrderWrap || !activeOrderLink) return;
  const userId = Number(currentUser?.id_usuario);
  if (!Number.isFinite(userId) || userId <= 0) {
    activeOrderWrap.classList.add("hidden");
    activeOrderLink.removeAttribute("href");
    return;
  }
  try {
    const { data, error } = await supabase
      .from("ordenes")
      .select("id_orden, id_carrito, recargar_saldo, checkout_finalizado")
      .eq("id_usuario", userId)
      .eq("checkout_finalizado", false)
      .or("orden_cancelada.is.null,orden_cancelada.eq.false")
      .order("id_orden", { ascending: false })
      .limit(1);
    if (error) throw error;
    const orden = Array.isArray(data) && data.length ? data[0] : null;
    const idOrden = Number(orden?.id_orden);
    if (!Number.isFinite(idOrden) || idOrden <= 0) {
      activeOrderWrap.classList.add("hidden");
      activeOrderLink.removeAttribute("href");
      return;
    }
    const isSaldoOrder = isTrue(orden?.recargar_saldo) && !orden?.id_carrito;
    const href = isSaldoOrder
      ? `checkout.html?id_orden=${encodeURIComponent(idOrden)}&from=saldo`
      : `checkout.html?id_orden=${encodeURIComponent(idOrden)}`;
    activeOrderLink.href = href;
    activeOrderWrap.classList.remove("hidden");
  } catch (err) {
    console.error("active order notice error", err);
    activeOrderWrap.classList.add("hidden");
    activeOrderLink.removeAttribute("href");
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
  await applyLoaderAvatar(null, requireSession());

  try {
    try {
      const { data, error } = await supabase.auth.getSession();
      if (error) {
        console.warn("[auth] getSession error", error);
      } else {
        console.log("[auth] session", data?.session);
      }
    } catch (err) {
      console.warn("[auth] getSession exception", err);
    }

    const currentUser = await loadCurrentUser();
    await applyLoaderAvatar(currentUser || null, currentUser?.id_usuario || requireSession());
    console.log("[user] currentUser", currentUser);
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
    const isClienteRate =
      isTrue(sessionRoles?.acceso_cliente) || isTrue(currentUser?.acceso_cliente);
    if (tasaActualEl) {
      if (!currentUser || isClienteRate) {
        tasaActualEl.classList.add("hidden");
      } else {
        fetchP2PRate()
          .then((rate) => {
            const tasaVal = rate
              ? Math.round(rate * TASA_MARKUP * 100) / 100
              : null;
            if (!Number.isFinite(tasaVal)) {
              tasaActualEl.classList.add("hidden");
              return;
            }
            tasaActualEl.textContent = `Tasa actual: Bs. ${tasaVal.toFixed(2)}`;
            tasaActualEl.classList.remove("hidden");
          })
          .catch(() => {
            tasaActualEl.classList.add("hidden");
          });
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
    await loadActiveOrderNotice(currentUser);
    await checkMissingDataNotice(currentUser);
    await loadPendingReminderDates({ isSuperadmin: isSuper });
    await loadPendingReminderNoPhoneClients({ isSuperadmin: isSuper });

    const cachedCart = getCachedCart();
    const cartData = await fetchCart();
    setCachedCart(cartData);
    const [catalog, stockMap] = await Promise.all([loadCatalog(), loadStockSummary(!!currentUser)]);
    const { categorias, plataformas, precios, descuentos } = catalog;
    const plataformasDisponibles = (plataformas || []).filter(
      (plat) => !isTrue(plat?.no_disponible),
    );
    // Si no hay sesión, mostrar precios detal por defecto.
    const esCliente =
      !currentUser ||
      isTrue(sessionRoles?.acceso_cliente) ||
      isTrue(currentUser?.acceso_cliente);
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
        const ia = Number(a.id_plataforma);
        const ib = Number(b.id_plataforma);
        if (Number.isFinite(ia) && Number.isFinite(ib)) return ia - ib;
        return 0;
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
attachLogout(clearServerSession, clearCachedCart);

// Redirección a la página de carrito
const viewCartBtn = document.querySelector("#btn-view-cart");
viewCartBtn?.addEventListener("click", () => {
  window.location.href = "cart.html";
});

// Redirección al inicio al hacer clic en el logo
attachLogoHome();
