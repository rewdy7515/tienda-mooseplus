import {
  fetchCart,
  loadCatalog,
  fetchP2PRate,
  sendCartDelta,
  clearServerSession,
  loadCurrentUser,
  ensureServerSession,
  startSession,
  submitCheckout,
  supabase,
} from "./api.js";
import { TASA_MARKUP } from "./rate-config.js";
import {
  requireSession,
  attachLogout,
  getCachedCart,
  setCachedCart,
  clearCachedCart,
  getSessionRoles,
  setSessionRoles,
  attachLogoHome,
  setSessionUserId,
} from "./session.js";

requireSession();

const statusEl = document.querySelector("#cart-status");
const itemsEl = document.querySelector("#cart-page-items");
const btnContinue = document.querySelector("#btn-page-continue");
const btnPay = document.querySelector("#btn-page-pay");
const cartSaldoEl = document.querySelector("#cart-saldo");
let cartItems = [];
let precios = [];
let plataformas = [];
let descuentos = [];
let tasaBs = null;
let currentUser = null;
let cartMontoUsd = null;
let cartDescuento = null;
let cartNeedsSync = false;
const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) =>
  v === true || v === 1 || v === "1" || v === "true" || v === "t";
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;

const getClosestDiscountPct = (rows, value, column) => {
  const key = Number(value) || 0;
  if (!Array.isArray(rows) || key <= 0) return 0;
  const exact = rows.find((d) => Number(d.meses) === key);
  if (exact) return Number(exact?.[column]) || 0;
  let best = null;
  for (const d of rows) {
    const n = Number(d?.meses);
    if (!Number.isFinite(n) || n > key) continue;
    if (!best || n > Number(best.meses)) best = d;
  }
  return Number(best?.[column]) || 0;
};

async function syncAuthSession() {
  try {
    const { data, error } = await supabase.auth.getSession();
    if (error) {
      console.warn("[auth] getSession error", error);
      return null;
    }
    const email = data?.session?.user?.email;
    if (!email) return null;
    const { data: user, error: userErr } = await supabase
      .from("usuarios")
      .select(
        "id_usuario, nombre, apellido, acceso_cliente, permiso_admin, permiso_superadmin, saldo",
      )
      .ilike("correo", email)
      .maybeSingle();
    if (userErr) {
      console.warn("[auth] load usuario error", userErr);
      return null;
    }
    if (!user?.id_usuario) return null;
    setSessionUserId(user.id_usuario);
    await startSession(user.id_usuario);
    return user;
  } catch (err) {
    console.warn("syncAuthSession error", err);
    return null;
  }
}

const setStatus = (msg) => {
  if (statusEl) statusEl.textContent = msg;
};

const placePayButton = () => {
  if (!btnPay) return;
  const rightCol = document.querySelector(".cart-right");
  if (!rightCol) return;
  let wrap = rightCol.querySelector(".cart-pay-wrap");
  if (!wrap) {
    wrap = document.createElement("div");
    wrap.className = "cart-pay-wrap";
    rightCol.appendChild(wrap);
  }
  wrap.appendChild(btnPay);
};

const updateRefreshButtonState = () => {
  const refreshBtn = itemsEl?.querySelector('[data-cart-action="refresh"]');
  if (!refreshBtn) return;
  refreshBtn.disabled = !cartNeedsSync;
  refreshBtn.classList.toggle("btn-disabled-soft", !cartNeedsSync);
};

const collectUiCartValues = () => {
  const rows = document.querySelectorAll("tr[data-cart-row]");
  const result = new Map();
  rows.forEach((row) => {
    const idx = Number(row.getAttribute("data-index"));
    if (!Number.isFinite(idx)) return;
    const mesesEl = row.querySelector(".cart-meses-value");
    const qtyEl = row.querySelector(".cart-cantidad-value");
    const meses = Math.max(1, Number((mesesEl?.textContent || "").trim()) || 1);
    const cantidad = Math.max(1, Number((qtyEl?.textContent || "").trim()) || 1);
    result.set(idx, { meses, cantidad });
  });
  return result;
};

const buildPrecioDetalle = (item, price, platform) => {
  const flags = {
    por_pantalla: platform?.por_pantalla,
    por_acceso: platform?.por_acceso,
    tarjeta_de_regalo: platform?.tarjeta_de_regalo,
  };
  if (flags.tarjeta_de_regalo) {
    const region = price.region || "-";
    const monto = `${price.valor_tarjeta_de_regalo || ""} ${price.moneda || ""} $${price.precio_usd_detal || ""}`;
    return `Región: ${region} · Monto: ${monto}`;
  }
  const qty = item.cantidad || price.cantidad || 1;
  const baseUnit = flags.por_pantalla
    ? "pantalla"
    : flags.por_acceso
      ? "dispositivo"
      : "mes";
  const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
  return `${qty} ${baseUnit}${plural} $${price.precio_usd_detal || ""}`;
};

const renderCart = () => {
  if (!itemsEl) return;
  if (!cartItems.length) {
    itemsEl.innerHTML = '<p class="cart-empty">Tu carrito está vacío.</p>';
    return;
  }
  const priceById = (precios || []).reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});
  const platformById = (plataformas || []).reduce((acc, p) => {
    acc[p.id_plataforma] = p;
    return acc;
  }, {});

  let total = 0; // total con descuentos
  let totalDescuento = 0;
  let subtotalBruto = 0;
  const rows = cartItems
    .map((item, idx) => {
      const price = priceById[item.id_precio] || {};
      const platform = platformById[price.id_plataforma] || {};
      const unit = price.precio_usd_detal || 0;
      const qtyVal = item.cantidad || 0;
      const mesesVal = item.meses || price.duracion || 1;
      const baseSubtotal = round2(unit * qtyVal * mesesVal);
      let descuentoVal = 0;
      let descuentoMesesVal = 0;
      let descuentoCantidadVal = 0;
      let rateMeses = 0;
      if (platform?.descuento_meses) {
        const rawRate = getClosestDiscountPct(descuentos, mesesVal, "descuento_1");
        rateMeses = rawRate > 1 ? rawRate / 100 : rawRate;
        descuentoMesesVal = round2(baseSubtotal * rateMeses);
      }
      const rawRateQty = getClosestDiscountPct(descuentos, qtyVal, "descuento_2");
      const rateQty = rawRateQty > 1 ? rawRateQty / 100 : rawRateQty;
      if (rateQty > 0) {
        descuentoCantidadVal = round2(baseSubtotal * rateQty);
      }
      descuentoVal = round2(descuentoMesesVal + descuentoCantidadVal);
      const subtotal = round2(baseSubtotal - descuentoVal);
      totalDescuento = round2(totalDescuento + descuentoVal);
      total = round2(total + subtotal);
      const detalle =
        price.plan ||
        (platform.tarjeta_de_regalo ? `Región: ${price.region || "-"}` : "");
      const tipo = item.renovacion ? "Renovación" : "Nuevo";
      subtotalBruto = round2(subtotalBruto + baseSubtotal);
      return `
        <tr data-cart-row="1" data-index="${idx}">
          <td>
            <div class="cart-info tight">
              <div class="cart-product">
                <button type="button" class="cart-remove btn-delete" data-index="${idx}" aria-label="Eliminar item">🗑️</button>
                <div class="cart-thumb-sm">
                  <img src="${platform.imagen || ""}" alt="${platform.nombre || ""}" />
                </div>
                <div class="cart-product-text">
                  <p class="cart-name">${platform.nombre || `Precio ${item.id_precio}`}</p>
                  <p class="cart-detail">${detalle || ""}</p>
                  <p class="cart-detail">Tipo: ${tipo}</p>
                </div>
              </div>
            </div>
          </td>
          <td class="cart-cell-center">
            <div class="modal-qty">
              <button type="button" class="meses-minus" data-index="${idx}" aria-label="menos">-</button>
              <span class="cart-meses-value">${mesesVal}</span>
              <button type="button" class="meses-plus" data-index="${idx}" aria-label="más">+</button>
            </div>
          </td>
          <td class="cart-cell-center">$${round2(unit).toFixed(2)}</td>
          <td class="cart-cell-center">
            ${
              item.renovacion
                ? `<span class="cart-cantidad-value">${qtyVal}</span>`
                : `
            <div class="modal-qty">
              <button type="button" class="cart-minus" data-index="${idx}" aria-label="Disminuir">-</button>
              <span class="cart-cantidad-value">${qtyVal}</span>
              <button type="button" class="cart-plus" data-index="${idx}" aria-label="Aumentar">+</button>
            </div>
            `
            }
          </td>
          <td class="cart-cell-center">
            $${baseSubtotal.toFixed(2)}
            <div class="cart-discount-line">
              ${
                descuentoMesesVal > 0
                  ? `<span class="discount-badge">-${(rateMeses * 100).toFixed(2)}% mes</span>`
                  : ""
              }
              ${
                descuentoCantidadVal > 0
                  ? `<span class="discount-badge">-${(rateQty * 100).toFixed(2)}% cant.</span>`
                  : ""
              }
            </div>
          </td>
        </tr>
      `;
    })
    .join("");

  const descuentoMostrar = round2(totalDescuento);
  const subtotalMostrar = round2(subtotalBruto);
  const totalMostrar = round2(subtotalMostrar - descuentoMostrar);
  if (Number.isFinite(Number(cartDescuento))) {
    const diff = Math.abs(Number(cartDescuento) - totalDescuento);
    if (diff >= 0.01) {
      console.warn("[cart] descuento UI/DB mismatch", {
        ui: totalDescuento,
        db: Number(cartDescuento),
      });
    }
  }
  itemsEl.innerHTML = `
    <div class="cart-layout">
      <div class="cart-left">
        <div class="cart-table-scroll">
          <table class="table-base cart-page-table">
            <thead>
              <tr>
                <th>Producto</th>
                <th>Meses</th>
                <th>Precio</th>
                <th>Cantidad</th>
                <th>Subtotal</th>
              </tr>
            </thead>
            <tbody>
              ${rows}
            </tbody>
          </table>
        </div>
        <div class="cart-actions-inline">
          <button type="button" class="btn-outline" data-cart-action="continue">Seguir comprando</button>
          <button type="button" class="btn-primary" data-cart-action="refresh">Actualizar carrito</button>
        </div>
      </div>
      <div class="cart-right">
        <table class="table-base cart-page-table cart-summary-table">
          <tbody>
            <tr class="cart-total-row">
              <td class="cart-cell-center">Subtotal</td>
              <td class="cart-cell-center">$${subtotalMostrar.toFixed(2)}</td>
            </tr>
            <tr class="cart-total-row cart-total-discount">
              <td class="cart-cell-center">Descuentos</td>
              <td class="cart-cell-center">-$${descuentoMostrar.toFixed(2)}</td>
            </tr>
            <tr class="cart-total-row cart-total-final">
              <td class="cart-cell-center">Total</td>
              <td class="cart-cell-center">$${totalMostrar.toFixed(2)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `;
  placePayButton();
  updateRefreshButtonState();
};

const handleCartClick = async (e) => {
  const cartActionBtn = e.target.closest("[data-cart-action]");
  if (cartActionBtn) {
    const action = cartActionBtn.dataset.cartAction;
    if (action === "continue") {
      window.location.href = "index.html";
      return;
    }
    if (action === "refresh") {
      try {
        await syncCartValuesBeforeCheckout();
        cartNeedsSync = false;
        updateRefreshButtonState();
        // No sobreescribir UI con datos de BD aquí.
        // Este botón debe empujar HTML -> BD.
        alert("Carrito actualizado.");
      } catch (err) {
        console.error("refresh cart error", err);
        alert("No se pudo actualizar el carrito.");
      }
      return;
    }
  }

  const btnRemove = e.target.closest(".cart-remove");
  const btnMinus = e.target.closest(".cart-minus");
  const btnPlus = e.target.closest(".cart-plus");
  const btnMesesMinus = e.target.closest(".meses-minus");
  const btnMesesPlus = e.target.closest(".meses-plus");

  if (!btnRemove && !btnMinus && !btnPlus && !btnMesesMinus && !btnMesesPlus)
    return;

  const wrapper = e.target.closest("[data-index]");
  const idx = Number(wrapper?.dataset.index);
  if (Number.isNaN(idx)) return;
  const item = cartItems[idx];
  if (!item) return;
  const price = precios.find((p) => p.id_precio === item.id_precio) || {};
  const platform = plataformas.find((p) => p.id_plataforma === price.id_plataforma) || {};

  if (btnRemove) {
    cartItems.splice(idx, 1);
    cartNeedsSync = true;
  } else {
    if (btnMinus || btnPlus) {
      const delta = btnMinus ? -1 : 1;
      const newQty = (item.cantidad || 0) + delta;
      if (newQty <= 0) {
        if (btnMinus) {
          const nombrePlat = platform.nombre || `Precio ${item.id_precio || ""}`;
          const ok = window.confirm(
            `¿Quieres eliminar ${nombrePlat} de tu carrito?\n\nSi/No`
          );
          if (!ok) {
            renderCart();
            return;
          }
        }
        cartItems.splice(idx, 1);
      } else {
        item.cantidad = newQty;
      }
      cartNeedsSync = true;
    }
    if (btnMesesMinus || btnMesesPlus) {
      const delta = btnMesesMinus ? -1 : 1;
      const current = item.meses || price.duracion || 1;
      const next = Math.max(1, current + delta);
      item.meses = next;
      cartNeedsSync = true;
    }
  }
  renderCart();
};

async function init() {
  setStatus("Cargando carrito...");
  try {
    await syncAuthSession();
    await ensureServerSession();
    currentUser = await loadCurrentUser();
    if (usernameEl && currentUser) {
      const fullName = [currentUser.nombre, currentUser.apellido]
        .filter(Boolean)
        .join(" ")
        .trim();
      usernameEl.textContent = fullName || currentUser.correo || "Usuario";
    }
    if (cartSaldoEl && currentUser) {
      const saldoVal = Number(currentUser?.saldo);
      const saldoNum = Number.isFinite(saldoVal) ? saldoVal : 0;
      cartSaldoEl.textContent = `Saldo: $${saldoNum.toFixed(2)}`;
    }
    setSessionRoles(currentUser || {});
    const sessionRoles = getSessionRoles();
    const isAdmin =
      isTrue(sessionRoles?.permiso_admin) ||
      isTrue(sessionRoles?.permiso_superadmin) ||
      isTrue(currentUser?.permiso_admin) ||
      isTrue(currentUser?.permiso_superadmin);
    if (adminLink) {
      adminLink.classList.toggle("hidden", !isAdmin);
      adminLink.style.display = isAdmin ? "block" : "none";
    }

    const cachedCart = getCachedCart();
    const [cartData, catalog, tasaResp] = await Promise.all([
      fetchCart(),
      loadCatalog(),
      fetchP2PRate(),
    ]);
    cartMontoUsd = Number.isFinite(Number(cartData?.monto_usd))
      ? Number(cartData.monto_usd)
      : null;
    cartDescuento = Number.isFinite(Number(cartData?.descuento))
      ? Number(cartData.descuento)
      : null;
    tasaBs = tasaResp ? Math.round(tasaResp * TASA_MARKUP * 100) / 100 : null;
    setCachedCart(cartData);
    const esCliente =
      isTrue(sessionRoles?.acceso_cliente) ||
      isTrue(currentUser?.acceso_cliente);
    const precioData = (catalog.precios || [])
      .map((p) => {
        const valor = esCliente ? p.precio_usd_detal : p.precio_usd_mayor;
        if (valor == null) return null;
        return { ...p, precio_usd_detal: valor, precio_usd_mayor: undefined };
      })
      .filter(Boolean);
    precios = precioData;
    plataformas = catalog.plataformas;
    descuentos = catalog.descuentos || [];
    cartItems = cartData.items || [];
    renderCart();
    setStatus("");
    itemsEl?.addEventListener("click", handleCartClick);
  } catch (err) {
    console.error("cart page error", err);
    setStatus("No se pudo cargar el carrito.");
  }
}

init();
attachLogout(clearServerSession, clearCachedCart);
attachLogoHome();

const syncCartValuesBeforeCheckout = async () => {
  const uiValues = collectUiCartValues();
  cartItems = (cartItems || []).map((it, idx) => {
    const ui = uiValues.get(idx);
    if (!ui) return it;
    return {
      ...it,
      cantidad: ui.cantidad,
      meses: ui.meses,
    };
  });

  // Sync exacto UI -> BD: actualiza usando endpoint backend (incluye meses y cantidad).
  const current = await fetchCart();
  const dbItems = current?.items || [];
  const dbById = new Map(
    dbItems
      .map((it) => [Number(it?.id_item), it])
      .filter(([id]) => Number.isFinite(id)),
  );
  const uiById = new Map(
    (cartItems || [])
      .map((it) => [Number(it?.id_item), it])
      .filter(([id]) => Number.isFinite(id)),
  );

  for (const [idItem, uiItem] of uiById.entries()) {
    const dbItem = dbById.get(idItem);
    if (!dbItem) continue;
    const dbQty = Math.max(1, Number(dbItem.cantidad) || 1);
    const dbMeses = Math.max(1, Number(dbItem.meses) || 1);
    const uiQty = Math.max(1, Number(uiItem.cantidad) || 1);
    const uiMeses = Math.max(1, Number(uiItem.meses) || 1);
    const delta = uiQty - dbQty;
    if (delta === 0 && uiMeses === dbMeses) continue;
    await sendCartDelta(dbItem.id_precio, delta, uiMeses, {
      id_item: dbItem.id_item,
      renovacion: dbItem.renovacion === true,
      id_venta: dbItem.id_venta ?? null,
      id_cuenta: dbItem.id_cuenta ?? null,
      id_perfil: dbItem.id_perfil ?? null,
    });
  }

  // Si el usuario quitó items en UI, eliminarlos en BD.
  for (const [idItem, dbItem] of dbById.entries()) {
    if (uiById.has(idItem)) continue;
    const qty = Math.max(1, Number(dbItem.cantidad) || 1);
    await sendCartDelta(dbItem.id_precio, -qty, dbItem.meses || 1, {
      id_item: dbItem.id_item,
      renovacion: dbItem.renovacion === true,
      id_venta: dbItem.id_venta ?? null,
      id_cuenta: dbItem.id_cuenta ?? null,
      id_perfil: dbItem.id_perfil ?? null,
    });
  }
};

btnContinue?.addEventListener("click", () => {
  window.location.href = "index.html";
});

btnPay?.addEventListener("click", () => {
  (async () => {
    try {
      await syncCartValuesBeforeCheckout();
      const freshCart = await fetchCart();
      cartItems = freshCart.items || cartItems;
      cartMontoUsd = Number.isFinite(Number(freshCart?.monto_usd))
        ? Number(freshCart.monto_usd)
        : cartMontoUsd;
      cartDescuento = Number.isFinite(Number(freshCart?.descuento))
        ? Number(freshCart.descuento)
        : cartDescuento;
      if (!currentUser) {
        window.location.href = "checkout.html";
        return;
      }
      const saldo = Number(currentUser?.saldo) || 0;
      if (!Number.isFinite(cartMontoUsd)) {
        window.location.href = "checkout.html";
        return;
      }
      if (saldo < cartMontoUsd) {
        window.location.href = "checkout.html";
        return;
      }

      const payload = {
        id_metodo_de_pago: 1,
        referencia: "SALDO",
        comprobantes: [],
        total: cartMontoUsd,
        tasa_bs: null,
      };
      const resp = await submitCheckout(payload);
      if (resp?.error) {
        alert(`Error al procesar: ${resp.error}`);
        return;
      }
      const nuevoSaldo = Math.max(0, Math.round((saldo - cartMontoUsd) * 100) / 100);
      await supabase
        .from("usuarios")
        .update({ saldo: nuevoSaldo })
        .eq("id_usuario", currentUser.id_usuario);
      const dest = resp?.id_orden
        ? `entregar_servicios.html?id_orden=${encodeURIComponent(resp.id_orden)}`
        : "entregar_servicios.html";
      window.location.href = dest;
    } catch (err) {
      console.error("saldo checkout error", err);
      window.location.href = "checkout.html";
    }
  })();
});
