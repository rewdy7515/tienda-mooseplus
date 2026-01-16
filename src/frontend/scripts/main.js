import {
  loadCatalog,
  fetchCart,
  clearServerSession,
  loadCurrentUser,
  supabase,
  fetchEntregadas,
  fetchTestingFlag,
  updateTestingFlag,
} from "./api.js";
import { initCart } from "./cart.js";
import { initModal, openModal, setPrecios, setStockData, setDescuentos } from "./modal.js";
import { initSearch, updateSearchData } from "./search.js";
import { renderCategorias } from "./render.js";
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

const modalEls = {
  modal: document.querySelector("#platform-modal"),
  modalImg: document.querySelector("#modal-image"),
  modalName: document.querySelector("#modal-name"),
  modalCategory: document.querySelector("#modal-category"),
  modalBadge: document.querySelector("#modal-badge"),
  modalStock: document.querySelector("#modal-stock"),
  modalPrecios: document.querySelector("#modal-precios"),
  modalQtyMonths: document.querySelectorAll(".modal-qty")[0],
  modalQtyItems: document.querySelectorAll(".modal-qty")[1],
  monthsDiscount: document.querySelector("#months-discount"),
  qtyMonthsValue: document.querySelector("#qty-months-value"),
  modalQty: document.querySelector(".modal-qty"),
  qtyValue: document.querySelector("#qty-value"),
  btnMinus: document.querySelector("#qty-minus"),
  btnPlus: document.querySelector("#qty-plus"),
  btnMonthsMinus: document.querySelector("#qty-months-minus"),
  btnMonthsPlus: document.querySelector("#qty-months-plus"),
  btnAdd: document.querySelector("#add-cart"),
  closeBtn: document.querySelector(".modal-close"),
  backdrop: document.querySelector(".modal-backdrop"),
};

const searchInput = document.querySelector("#search-input");
const searchResults = document.querySelector("#search-results");
const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";

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
        por_pantalla: card.dataset.porPantalla,
        por_acceso: card.dataset.porAcceso,
        tarjeta_de_regalo: card.dataset.tarjetaDeRegalo,
        entrega_inmediata: card.dataset.entregaInmediata,
        descuento_meses: isTrue(card.dataset.descuentoMeses),
        id_descuento: null,
      })
    );
  });
};

const loadStockSummary = async () => {
  const { data, error } = await supabase
    .from("perfiles")
    .select("id_perfil, ocupado, cuentas:cuentas(id_plataforma, inactiva, venta_perfil)")
    .not("id_cuenta", "is", null);
  if (error) {
    console.error("stock summary error", error);
    return {};
  }
  return (data || []).reduce((acc, p) => {
    const platId = p.cuentas?.id_plataforma;
    const inactiva = p.cuentas?.inactiva;
    const ventaPerfil = p.cuentas?.venta_perfil;
    if (!platId || inactiva) return acc;
    if (!acc[platId]) acc[platId] = 0;
    if (!p.ocupado) {
      acc[platId] += 1;
    }
    return acc;
  }, {});
};

async function init() {
  setEstado("Cargando categorias y plataformas...");
  initModal(modalEls);

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
    const sessionRoles = getSessionRoles();
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

    const cachedCart = getCachedCart();
    const cartData = await fetchCart();
    setCachedCart(cartData);
    const [catalog, stockMap] = await Promise.all([loadCatalog(), loadStockSummary()]);
    const { categorias, plataformas, precios, descuentos } = catalog;
    const esCliente =
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
    updateSearchData(plataformas);

    const plataformasPorCategoria = (plataformas || []).reduce((acc, plat) => {
      if (!acc[plat.id_categoria]) acc[plat.id_categoria] = [];
      acc[plat.id_categoria].push(plat);
      return acc;
    }, {});

    setEstado("");
    renderCategorias(contenedor, categorias, plataformasPorCategoria);
    attachPlatformClicks(openModal);

    initCart({
      drawerEl: cartDrawer,
      backdropEl: cartBackdrop,
      closeEl: cartClose,
      iconEl: cartIcon,
      itemsContainer: cartItemsEl,
      initialItems: mapCartItems(cartData.items || [], preciosVisibles, plataformas),
    });
    initSearch({
      input: searchInput,
      results: searchResults,
      data: plataformas,
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

const btnCheckout = document.querySelector("#btn-checkout");
btnCheckout?.addEventListener("click", () => {
  window.location.href = "checkout.html";
});
