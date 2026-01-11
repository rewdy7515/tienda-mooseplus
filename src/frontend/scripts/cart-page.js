import {
  fetchCart,
  loadCatalog,
  sendCartDelta,
  clearServerSession,
  loadCurrentUser,
  ensureServerSession,
} from "./api.js";
import {
  requireSession,
  attachLogout,
  getCachedCart,
  setCachedCart,
  clearCachedCart,
  getSessionRoles,
  setSessionRoles,
  attachLogoHome,
} from "./session.js";

requireSession();

const statusEl = document.querySelector("#cart-status");
const itemsEl = document.querySelector("#cart-page-items");
const btnContinue = document.querySelector("#btn-page-continue");
const btnPay = document.querySelector("#btn-page-pay");
let cartItems = [];
let precios = [];
let plataformas = [];
let descuentos = [];
const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";

const setStatus = (msg) => {
  if (statusEl) statusEl.textContent = msg;
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
      const baseSubtotal = +(unit * qtyVal * mesesVal).toFixed(2);
      let descuentoVal = 0;
      if (platform?.descuento_meses) {
        const descRow = descuentos.find((d) => Number(d.meses) === Number(mesesVal));
        const rawRate = Number(descRow?.descuento) || 0;
        const rate = rawRate > 1 ? rawRate / 100 : rawRate;
        descuentoVal = +(baseSubtotal * rate).toFixed(2);
      }
      const subtotal = +(baseSubtotal - descuentoVal).toFixed(2);
      totalDescuento += descuentoVal;
      total += subtotal;
      const detalle =
        price.plan ||
        (platform.tarjeta_de_regalo ? `Región: ${price.region || "-"}` : "");
      const tipo = item.renovacion ? "Renovación" : "Nuevo";
      subtotalBruto += baseSubtotal;
      return `
        <tr>
          <td>
            <div class="cart-info tight">
              <p class="cart-name">${platform.nombre || `Precio ${item.id_precio}`}</p>
              <p class="cart-detail">${detalle || ""}</p>
            </div>
          </td>
          <td class="cart-cell-center">${tipo}</td>
          <td class="cart-cell-center">
            <div class="modal-qty">
              <button type="button" class="meses-minus" data-index="${idx}" aria-label="menos">-</button>
              <span>${mesesVal}</span>
              <button type="button" class="meses-plus" data-index="${idx}" aria-label="más">+</button>
            </div>
          </td>
          <td class="cart-cell-center">$${unit}</td>
          <td class="cart-cell-center">
            <div class="modal-qty">
              <button type="button" class="cart-minus" data-index="${idx}" aria-label="Disminuir">-</button>
              <span>${qtyVal}</span>
              <button type="button" class="cart-plus" data-index="${idx}" aria-label="Aumentar">+</button>
            </div>
          </td>
          <td class="cart-cell-center">$${baseSubtotal.toFixed(2)}</td>
        </tr>
      `;
    })
    .join("");

  itemsEl.innerHTML = `
    <table class="reportes-table cart-page-table">
      <thead>
        <tr>
          <th>Producto</th>
          <th>Tipo</th>
          <th>Duración (meses)</th>
          <th>Precio unitario</th>
          <th>Cantidad</th>
          <th>Subtotal</th>
        </tr>
      </thead>
      <tbody>
        ${rows}
      </tbody>
      <tfoot>
        <tr class="cart-total-row">
          <td colspan="4"></td>
          <td class="cart-cell-center">Subtotal</td>
          <td class="cart-cell-center">$${subtotalBruto.toFixed(2)}</td>
        </tr>
        <tr class="cart-total-row cart-total-discount">
          <td colspan="4"></td>
          <td class="cart-cell-center">Descuentos</td>
          <td class="cart-cell-center">-$${totalDescuento.toFixed(2)}</td>
        </tr>
        <tr class="cart-total-row cart-total-final">
          <td colspan="4"></td>
          <td class="cart-cell-center">Total</td>
          <td class="cart-cell-center">$${(subtotalBruto - totalDescuento).toFixed(2)}</td>
        </tr>
        <tr class="cart-total-row cart-total-bs">
          <td colspan="4"></td>
          <td class="cart-cell-center">Total (Bs)</td>
          <td class="cart-cell-center">Bs. ${((subtotalBruto - totalDescuento) * 400).toFixed(2)}</td>
        </tr>
      </tfoot>
    </table>
  `;
};

const handleCartClick = (e) => {
  const btnRemove = e.target.closest(".cart-remove");
  const btnMinus = e.target.closest(".cart-minus");
  const btnPlus = e.target.closest(".cart-plus");
  const btnMesesMinus = e.target.closest(".meses-minus");
  const btnMesesPlus = e.target.closest(".meses-plus");

  if (!btnRemove && !btnMinus && !btnPlus && !btnMesesMinus && !btnMesesPlus) return;

  const wrapper = e.target.closest("[data-index]");
  const idx = Number(wrapper?.dataset.index);
  if (Number.isNaN(idx)) return;
  const item = cartItems[idx];
  if (!item) return;
  const key = item.id_precio || item.id_plataforma;
  const price = precios.find((p) => p.id_precio === item.id_precio) || {};

  const extraMatch = {
    renovacion: !!item.renovacion,
    id_venta: item.id_venta || null,
  };

  if (btnRemove) {
    const qty = item.cantidad || 0;
    cartItems.splice(idx, 1);
    if (key && qty > 0) sendCartDelta(key, -qty, null, extraMatch);
  } else {
    if (btnMinus || btnPlus) {
      const delta = btnMinus ? -1 : 1;
      const newQty = (item.cantidad || 0) + delta;
      if (newQty <= 0) {
        cartItems.splice(idx, 1);
      } else {
        item.cantidad = newQty;
      }
      if (key) sendCartDelta(key, delta, null, extraMatch);
    }
    if (btnMesesMinus || btnMesesPlus) {
      const delta = btnMesesMinus ? -1 : 1;
      const current = item.meses || price.duracion || 1;
      const next = Math.max(1, current + delta);
      item.meses = next;
      // No hay endpoint para meses, se refleja en UI solamente
    }
  }
  renderCart();
};

async function init() {
  setStatus("Cargando carrito...");
  try {
    await ensureServerSession();
    const currentUser = await loadCurrentUser();
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
    if (adminLink) {
      adminLink.classList.toggle("hidden", !isAdmin);
      adminLink.style.display = isAdmin ? "block" : "none";
    }

    const cachedCart = getCachedCart();
    const [cartData, catalog] = await Promise.all([fetchCart(), loadCatalog()]);
    setCachedCart(cartData);
    precios = catalog.precios;
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

btnContinue?.addEventListener("click", () => {
  window.location.href = "index.html";
});

btnPay?.addEventListener("click", () => {
  window.location.href = "checkout.html";
});
