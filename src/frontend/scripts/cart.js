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
let totalBarEl;
let totalDrawerEl;
let savingsDrawerEl;
let catalogCache = null;
let userAcceso = null;
let cartRawItems = [];
let refreshFromServerFn = null;
let cartBound = false;
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;

const getClosestDiscountPct = (rows, value, column) => {
  const key = Number(value) || 0;
  if (!Array.isArray(rows) || key <= 0) return 0;
  const exact = rows.find((d) => Number(d.meses) === key);
  const exactVal = exact?.[column];
  if (exactVal !== null && exactVal !== undefined && exactVal !== "") {
    return Number(exactVal) || 0;
  }
  let best = null;
  for (const d of rows) {
    const n = Number(d?.meses);
    if (!Number.isFinite(n) || n > key) continue;
    const raw = d?.[column];
    if (raw === null || raw === undefined || raw === "") continue;
    if (!best || n > Number(best.meses)) best = d;
  }
  return Number(best?.[column]) || 0;
};

const getDiscountColumnsFromRows = (rows = []) => {
  const cols = new Set();
  (rows || []).forEach((row) => {
    Object.keys(row || {}).forEach((k) => {
      const key = String(k || "").toLowerCase();
      if (/^descuento_\\d+$/i.test(key)) cols.add(key);
    });
  });
  const out = Array.from(cols).sort((a, b) => {
    const na = Number(a.split("_")[1]) || 0;
    const nb = Number(b.split("_")[1]) || 0;
    return na - nb;
  });
  return out.length ? out : ["descuento_1", "descuento_2"];
};

const buildDiscountColumnByIdMap = (rows = [], cols = []) => {
  const ids = Array.from(
    new Set(
      (rows || [])
        .map((row) => Number(row?.id_descuento))
        .filter((n) => Number.isFinite(n)),
    ),
  ).sort((a, b) => a - b);
  const map = {};
  ids.forEach((id, idx) => {
    if (cols[idx]) map[id] = cols[idx];
  });
  return map;
};

const resolveDiscountColumn = (platform, mode, discountColumns, discountColumnById) => {
  const raw = mode === "items" ? platform?.id_descuento_cantidad : platform?.id_descuento_mes;
  const asText = String(raw || "").trim();
  if (/^descuento_\\d+$/i.test(asText)) return asText.toLowerCase();
  const asNum = Number(raw);
  if (Number.isFinite(asNum) && asNum >= 1) {
    const mapped = discountColumnById[Math.trunc(asNum)];
    if (mapped) return mapped;
    const direct = `descuento_${Math.trunc(asNum)}`;
    if (discountColumns.includes(direct)) return direct;
  }
  return mode === "items" ? "descuento_2" : "descuento_1";
};

const isDiscountEnabledForAudience = (platform, mode = "months", isCliente = true) => {
  if (mode === "items") {
    return isCliente
      ? !(
          platform?.aplica_descuento_cantidad_detal === false ||
          platform?.aplica_descuento_cantidad_detal === "false" ||
          platform?.aplica_descuento_cantidad_detal === 0 ||
          platform?.aplica_descuento_cantidad_detal === "0"
        )
      : !(
          platform?.aplica_descuento_cantidad_mayor === false ||
          platform?.aplica_descuento_cantidad_mayor === "false" ||
          platform?.aplica_descuento_cantidad_mayor === 0 ||
          platform?.aplica_descuento_cantidad_mayor === "0"
        );
  }
  return isCliente
    ? !(
        platform?.aplica_descuento_mes_detal === false ||
        platform?.aplica_descuento_mes_detal === "false" ||
        platform?.aplica_descuento_mes_detal === 0 ||
        platform?.aplica_descuento_mes_detal === "0"
      )
    : !(
        platform?.aplica_descuento_mes_mayor === false ||
        platform?.aplica_descuento_mes_mayor === "false" ||
        platform?.aplica_descuento_mes_mayor === 0 ||
        platform?.aplica_descuento_mes_mayor === "0"
      );
};

const mapCartItems = (items = [], catalog = {}, acceso = null) => {
  const precios = catalog.precios || [];
  const plataformas = catalog.plataformas || [];
  const descuentos = catalog.descuentos || [];
  const discountColumns = getDiscountColumnsFromRows(descuentos);
  const discountColumnById = buildDiscountColumnByIdMap(descuentos, discountColumns);
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
    const qty = Number(item.cantidad || price.cantidad || 1) || 1;
    const meses = Number(item.meses || price.duracion || 1) || 1;
    const baseUnit = flags.por_pantalla ? "pantalla" : flags.por_acceso ? "dispositivo" : "mes";
    const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
    const mesesTxt = `${meses} mes${meses === 1 ? "" : "es"}`;
    const useMayor = acceso === false;
    const isCliente = acceso === false ? false : true;
    const unit =
      useMayor && price.precio_usd_mayor != null && price.precio_usd_mayor !== undefined
        ? Number(price.precio_usd_mayor) || 0
        : Number(price.precio_usd_detal) || 0;
    const baseSubtotal = round2(unit * qty * (flags.tarjeta_de_regalo ? 1 : meses));
    const monthEnabled =
      !!platform?.descuento_meses &&
      !flags.tarjeta_de_regalo &&
      isDiscountEnabledForAudience(platform, "months", isCliente);
    const qtyEnabled = isDiscountEnabledForAudience(platform, "items", isCliente);
    const monthColumn = resolveDiscountColumn(
      platform,
      "months",
      discountColumns,
      discountColumnById,
    );
    const qtyColumn = resolveDiscountColumn(
      platform,
      "items",
      discountColumns,
      discountColumnById,
    );
    const rawRateMeses = monthEnabled
      ? getClosestDiscountPct(descuentos, meses, monthColumn)
      : 0;
    const rawRateQty = qtyEnabled ? getClosestDiscountPct(descuentos, qty, qtyColumn) : 0;
    const rateMeses = rawRateMeses > 1 ? rawRateMeses / 100 : rawRateMeses;
    const rateQty = rawRateQty > 1 ? rawRateQty / 100 : rawRateQty;
    const descuentoMesesVal = rateMeses > 0 ? round2(baseSubtotal * rateMeses) : 0;
    const descuentoCantidadVal = rateQty > 0 ? round2(baseSubtotal * rateQty) : 0;
    const descuentoTotal = round2(descuentoMesesVal + descuentoCantidadVal);
    const montoUsd = round2(baseSubtotal - descuentoTotal);
    const detalle = (() => {
      if (flags.tarjeta_de_regalo) {
        const region = price.region || "-";
        return `Región: ${region} · Cantidad: ${qty} · ${mesesTxt}`;
      }
      return `${qty} ${baseUnit}${plural} · ${mesesTxt}`;
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
      cantidad: qty,
      meses,
      detalle,
      monto_usd: montoUsd,
      monto_original: baseSubtotal,
      descuento_total: descuentoTotal,
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
  totalBarEl?.classList.toggle("hidden", !hasItems);
};

const renderCart = () => {
  if (!itemsEl) return;
  if (!cartItems.length) {
    itemsEl.innerHTML = '<p class="cart-empty">Tu carrito está vacío.</p>';
    if (totalDrawerEl) totalDrawerEl.textContent = "$0.00";
    if (savingsDrawerEl) savingsDrawerEl.textContent = "$0.00";
    toggleActions(false);
    return;
  }
  toggleActions(true);
  const totalCarrito = round2(
    cartItems.reduce((acc, item) => acc + (Number(item?.monto_usd) || 0), 0),
  );
  const totalAhorro = round2(
    cartItems.reduce((acc, item) => acc + (Number(item?.descuento_total) || 0), 0),
  );
  if (totalDrawerEl) totalDrawerEl.textContent = `$${totalCarrito.toFixed(2)}`;
  if (savingsDrawerEl) savingsDrawerEl.textContent = `$${totalAhorro.toFixed(2)}`;
  itemsEl.innerHTML = cartItems
    .map(
      (item, idx) => {
        const detailLine = [item.plan, item.detalle].filter(Boolean).join(" · ");
        const original = Number(item.monto_original || 0);
        const actual = Number(item.monto_usd || 0);
        const showOriginal = original > actual + 0.0001;
        return `
      <div class="cart-item">
        <div class="cart-thumb"><img src="${item.imagen || ""}" alt="${item.nombre}" loading="lazy" decoding="async" /></div>
        <div class="cart-info">
          <p class="cart-name">${item.nombre}</p>
          ${detailLine ? `<p class="cart-detail">${detailLine}</p>` : ""}
          <p class="cart-amount-row">
            <strong class="cart-amount-usd">$${actual.toFixed(2)}</strong>
            ${
              showOriginal
                ? `<span class="cart-amount-original">$${original.toFixed(2)}</span>`
                : ""
            }
          </p>
          ${item.id_cuenta ? `<p class="cart-detail renewal-detail">Renovación: ${item.correo || ""}</p>` : ""}
          ${
            item.id_perfil
              ? `<p class="cart-detail renewal-detail">Perfil: M${item.n_perfil || ""}</p>`
              : ""
          }
        </div>
      <div class="cart-controls">
        <button class="cart-remove" data-index="${idx}" data-id-item="${item.id_item || ""}" aria-label="Eliminar">×</button>
      </div>
      </div>`;
      }
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
  totalBarEl = drawerEl?.querySelector(".cart-total-bar") || null;
  totalDrawerEl = drawerEl?.querySelector("#cart-total-drawer") || null;
  savingsDrawerEl = drawerEl?.querySelector("#cart-savings-drawer") || null;

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
