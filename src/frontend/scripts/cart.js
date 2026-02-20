import { sendCartDelta, fetchCart, loadCatalog, ensureServerSession, loadCurrentUser } from "./api.js";

let cartItems = [];
let drawer;
let backdrop;
let closeBtn;
let iconBtn;
let itemsEl;
let actionsEl;
let btnCheckout;
let btnViewCart;
let btnAssign;
let catalogCache = null;
let userAcceso = null;
let cartRawItems = [];
let refreshFromServerFn = null;
let cartBound = false;

const mapCartItems = (items = [], catalog = {}, acceso = null) => {
  const precios = catalog.precios || [];
  const plataformas = catalog.plataformas || [];
  const priceById = precios.reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});
  const platformById = plataformas.reduce((acc, p) => {
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
        const monto = `${price.valor_tarjeta_de_regalo || ""} ${price.moneda || ""} $${price.precio_usd_detal || ""}`;
        return `Región: ${region} · Monto: ${monto}`;
      }
      const qty = item.cantidad || price.cantidad || 1;
      const meses = item.meses || price.duracion || 1;
      const baseUnit = flags.por_pantalla ? "pantalla" : flags.por_acceso ? "dispositivo" : "mes";
      const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
      const mesesTxt = baseUnit === "mes" ? ` · ${meses} mes${meses === 1 ? "" : "es"}` : "";
      const useMayor = acceso === false;
      const unit =
        useMayor && price.precio_usd_mayor != null && price.precio_usd_mayor !== undefined
          ? price.precio_usd_mayor
          : price.precio_usd_detal;
      return `${qty} ${baseUnit}${plural}${mesesTxt} $${unit || 0}`;
    })();
    return {
      id_precio: item.id_precio,
      id_item: item.id_item,
      id_plataforma: price.id_plataforma,
      id_cuenta: item.id_cuenta || null,
      id_perfil: item.id_perfil || null,
      nombre: platform.nombre || `Precio ${item.id_precio}`,
      imagen: platform.imagen,
      plan: price.plan,
      precio: price.precio_usd_detal,
      cantidad: item.cantidad,
      meses: item.meses,
      detalle,
      flags,
      renovacion: item.renovacion,
      id_venta: item.id_venta,
      correo: item.correo || null,
      n_perfil: item.n_perfil || null,
    };
  });
};

const normalizeRawItem = (item = {}) => ({
  id_item: item.id_item ?? null,
  id_precio: item.id_precio ?? null,
  cantidad: Number(item.cantidad) || 1,
  meses: item.meses ?? null,
  renovacion: item.renovacion === true,
  id_venta: item.id_venta ?? null,
  id_cuenta: item.id_cuenta ?? null,
  id_perfil: item.id_perfil ?? null,
});

const isSameCartLine = (a, b) => {
  if (!a || !b) return false;
  const mesesA = Number(a.meses || 0);
  const mesesB = Number(b.meses || 0);
  return (
    a.id_precio === b.id_precio &&
    a.renovacion === b.renovacion &&
    (a.id_venta ?? null) === (b.id_venta ?? null) &&
    (a.id_cuenta ?? null) === (b.id_cuenta ?? null) &&
    (a.id_perfil ?? null) === (b.id_perfil ?? null) &&
    mesesA === mesesB
  );
};

const applyOptimisticItem = (rawItem) => {
  const normalized = normalizeRawItem(rawItem);
  if (!normalized.id_precio) return;
  const idx = cartRawItems.findIndex((it) => isSameCartLine(it, normalized));
  if (idx >= 0) {
    const currentQty = Number(cartRawItems[idx]?.cantidad) || 0;
    cartRawItems[idx] = {
      ...cartRawItems[idx],
      cantidad: currentQty + normalized.cantidad,
      meses: normalized.meses ?? cartRawItems[idx].meses,
    };
  } else {
    cartRawItems.push(normalized);
  }
  if (catalogCache) {
    cartItems = mapCartItems(cartRawItems, catalogCache, userAcceso);
  }
  renderCart();
};

const toggleCart = (open) => {
  if (!drawer) return;
  if (open) {
    drawer.classList.add("open");
    document.body.classList.add("cart-drawer-open");
  } else {
    drawer.classList.remove("open");
    document.body.classList.remove("cart-drawer-open");
  }
};

const toggleActions = (hasItems) => {
  if (actionsEl) actionsEl.classList.toggle("hidden", !hasItems);
  btnCheckout?.classList.toggle("hidden", !hasItems);
  btnViewCart?.classList.toggle("hidden", !hasItems);
  btnAssign?.classList.toggle("hidden", !hasItems);
};

const renderCart = () => {
  if (!itemsEl) return;
  if (!cartItems.length) {
    itemsEl.innerHTML = '<p class="cart-empty">Tu carrito está vacío.</p>';
    toggleActions(false);
    return;
  }
  toggleActions(true);
  itemsEl.innerHTML = cartItems
    .map(
      (item, idx) => `
      <div class="cart-item">
        <div class="cart-thumb"><img src="${item.imagen || ""}" alt="${item.nombre}" loading="lazy" decoding="async" /></div>
        <div class="cart-info">
          <p class="cart-name">${item.nombre}</p>
          <p class="cart-detail">${item.plan || ""} · ${item.detalle}</p>
          ${item.id_cuenta ? `<p class="cart-detail renewal-detail">Renovación: ${item.correo || ""}</p>` : ""}
          ${
            item.id_perfil
              ? `<p class="cart-detail renewal-detail">Perfil: M${item.n_perfil || ""}</p>`
              : ""
          }
          ${
            item.meses
              ? `<p class="cart-duration">${item.meses} mes${item.meses === 1 ? "" : "es"}</p>`
              : ""
          }
        </div>
      <div class="cart-controls">
        <button class="cart-remove" data-index="${idx}" data-id-item="${item.id_item || ""}" aria-label="Eliminar">×</button>
      </div>
      </div>`
    )
    .join("");
};

export function initCart({
  drawerEl,
  backdropEl,
  closeEl,
  iconEl,
  itemsContainer,
  initialItems = [],
  initialRawItems = [],
  catalog = null,
}) {
  drawer = drawerEl;
  backdrop = backdropEl;
  closeBtn = closeEl;
  iconBtn = iconEl;
  itemsEl = itemsContainer;
  actionsEl = drawerEl?.querySelector(".cart-actions") || null;
  btnCheckout = drawerEl?.querySelector("#btn-checkout") || null;
  btnViewCart = drawerEl?.querySelector("#btn-view-cart") || null;
  btnAssign = drawerEl?.querySelector("#btn-assign-client") || null;

  if (catalog) {
    catalogCache = catalog;
  }

  cartItems = initialItems;
  cartRawItems = (initialRawItems || []).map((it) => normalizeRawItem(it));
  renderCart();

  const refreshFromServer = async () => {
    try {
      await ensureServerSession();
      if (!userAcceso) {
        try {
          const user = await loadCurrentUser();
          userAcceso = user?.acceso_cliente;
        } catch (err) {
          console.error("load user acceso error", err);
        }
      }
      if (!catalogCache) catalogCache = await loadCatalog();
      const cartResp = await fetchCart();
      const items = cartResp?.items || [];
      cartRawItems = items;
      cartItems = mapCartItems(cartRawItems, catalogCache, userAcceso);
      renderCart();
    } catch (err) {
      console.error("refresh cart error", err);
    }
  };
  refreshFromServerFn = refreshFromServer;

  if (cartBound) {
    refreshFromServer();
    return;
  }
  cartBound = true;

  if (btnAssign) {
    btnAssign.style.display = "block";
    btnAssign.classList.remove("hidden");
  }

  iconBtn?.addEventListener("click", async () => {
    await refreshFromServer();
    toggleCart(true);
  });
  closeBtn?.addEventListener("click", () => toggleCart(false));
  backdrop?.addEventListener("click", () => toggleCart(false));

  itemsEl?.addEventListener("click", (e) => {
    const btn = e.target.closest(".cart-remove");
    if (btn) {
      const idx = Number(btn.dataset.index);
      if (Number.isNaN(idx)) return;
      const item = cartItems[idx];
      const key = item?.id_precio || null;
      const qtyToRemove = Math.max(1, Number(item?.cantidad) || 1);
      const meses = item?.meses || null;
      const idVenta = item?.id_venta || null;
      const idItem = item?.id_item || btn.dataset.idItem || null;
      // Si tenemos id_item, forzamos eliminación total de la línea (no solo -1).
      const removeDelta = idItem ? -999999 : -qtyToRemove;
      cartItems.splice(idx, 1);
      if (key)
        sendCartDelta(key, removeDelta, meses, {
          id_venta: idVenta,
          id_item: idItem,
          renovacion: item?.renovacion,
          id_cuenta: item?.id_cuenta || null,
          id_perfil: item?.id_perfil || null,
        }).finally(
          () => refreshFromServerFn && refreshFromServerFn()
        );
      renderCart();
      return;
    }
  });

  cartItems = initialItems;
  renderCart();
  // Refresca con datos reales del backend + precio por tier
  refreshFromServer();
}

export function addToCart(options = {}) {
  const shouldOpen = options?.open !== false;
  const shouldRefresh = options?.refresh !== false;
  if (options?.optimisticItem) {
    applyOptimisticItem(options.optimisticItem);
  }
  if (!shouldRefresh) {
    if (shouldOpen) openCart();
    return;
  }
  // Forzamos sincronizar con el servidor; no añadimos items optimistas
  if (refreshFromServerFn) {
    refreshFromServerFn().then(() => {
      if (shouldOpen) openCart();
    });
    return;
  }
  if (shouldOpen) openCart();
}

export function openCart() {
  toggleCart(true);
}

export async function refreshCartFromServer() {
  if (refreshFromServerFn) {
    await refreshFromServerFn();
  }
}
