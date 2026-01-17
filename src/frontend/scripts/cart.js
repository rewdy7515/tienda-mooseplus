import { sendCartDelta, fetchCart, loadCatalog, ensureServerSession } from "./api.js";

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

const mapCartItems = (items = [], catalog = {}) => {
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
      return `${qty} ${baseUnit}${plural}${mesesTxt} $${price.precio_usd_detal || ""}`;
    })();
    return {
      id_precio: item.id_precio,
      id_item: item.id_item,
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
      id_venta: item.id_venta,
    };
  });
};

const toggleCart = (open) => {
  if (!drawer) return;
  if (open) drawer.classList.add("open");
  else drawer.classList.remove("open");
};

const toggleActions = (hasItems) => {
  // Mantén visible el contenedor para botones extra; solo ocultamos checkout/view.
  if (actionsEl) actionsEl.classList.remove("hidden");
  if (btnAssign) btnAssign.classList.remove("hidden");
  btnCheckout?.classList.toggle("hidden", !hasItems);
  btnViewCart?.classList.toggle("hidden", !hasItems);
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
        <div class="cart-thumb"><img src="${item.imagen || ""}" alt="${item.nombre}" /></div>
        <div class="cart-info">
          <p class="cart-name">${item.nombre}</p>
          <p class="cart-detail">${item.plan || ""} · ${item.detalle}</p>
          ${
            item.meses
              ? `<p class="cart-duration">${item.meses} mes${item.meses === 1 ? "" : "es"}</p>`
              : ""
          }
        </div>
      <div class="cart-controls">
        <div class="cart-qty-inline" data-index="${idx}">
          <button class="cart-minus" aria-label="Disminuir">-</button>
          <span>${item.cantidad}</span>
          <button class="cart-plus" aria-label="Aumentar">+</button>
          </div>
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

  if (btnAssign) {
    btnAssign.style.display = "block";
    btnAssign.classList.remove("hidden");
  }

  const refreshFromServer = async () => {
    try {
      await ensureServerSession();
      if (!catalogCache) catalogCache = await loadCatalog();
      const cartResp = await fetchCart();
      const items = cartResp?.items || [];
      cartItems = mapCartItems(items, catalogCache);
      renderCart();
    } catch (err) {
      console.error("refresh cart error", err);
    }
  };

  iconBtn?.addEventListener("click", async () => {
    await refreshFromServer();
    toggleCart(true);
  });
  closeBtn?.addEventListener("click", () => toggleCart(false));
  backdrop?.addEventListener("click", () => toggleCart(false));

  itemsEl?.addEventListener("click", (e) => {
    const btn = e.target.closest(".cart-remove");
    const minus = e.target.closest(".cart-minus");
    const plus = e.target.closest(".cart-plus");
    if (btn) {
      const idx = Number(btn.dataset.index);
      if (Number.isNaN(idx)) return;
      const item = cartItems[idx];
      const key = item.id_precio || item.id_plataforma;
      const qtyToRemove = item?.cantidad || 0;
      const meses = item?.meses || null;
      const idVenta = item?.id_venta || null;
      const idItem = btn.dataset.idItem || null;
      cartItems.splice(idx, 1);
      if (key && qtyToRemove > 0)
        sendCartDelta(key, -qtyToRemove, meses, { id_venta: idVenta, id_item: idItem, renovacion: item?.renovacion });
      renderCart();
      return;
    }
    if (minus || plus) {
      const wrapper = e.target.closest(".cart-qty-inline");
      const idx = Number(wrapper?.dataset.index);
      if (Number.isNaN(idx)) return;
      const delta = minus ? -1 : 1;
      const item = cartItems[idx];
      if (!item) return;
      const key = item.id_precio || item.id_plataforma;
      const newQty = (item.cantidad || 0) + delta;
      if (newQty <= 0) {
        cartItems.splice(idx, 1);
      } else {
        item.cantidad = newQty;
      }
      if (key) sendCartDelta(key, delta);
      renderCart();
    }
  });

  cartItems = initialItems;
  renderCart();
}

export function addToCart(newItem) {
  const key = newItem.id_precio || newItem.id_plataforma;
  const existing = cartItems.find(
    (item) =>
      (item.id_precio || item.id_plataforma) === key &&
      item.plan === newItem.plan &&
      (item.meses || 1) === (newItem.meses || 1) &&
      (!!item.renovacion) === (!!newItem.renovacion) &&
      (item.id_venta || null) === (newItem.id_venta || null)
  );
  if (existing) {
    existing.cantidad += newItem.cantidad;
    existing.detalle = newItem.detalle;
    existing.meses = newItem.meses;
  } else {
    cartItems.push(newItem);
  }
  renderCart();
  // abrir el carrito para feedback
  openCart();
}

export function openCart() {
  toggleCart(true);
}
