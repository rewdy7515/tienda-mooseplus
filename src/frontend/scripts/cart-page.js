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
  updateCartFlags,
} from "./api.js";
import { TASA_MARKUP } from "./rate-config.js";
import { loadPaginaBranding } from "./branding.js";
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
const refreshNoteEl = document.querySelector("#cart-refresh-note");
const removeModalEl = document.querySelector("#cart-remove-modal");
const removeModalBackdropEl = removeModalEl?.querySelector(".modal-backdrop");
const removeModalCloseEl = document.querySelector("#cart-remove-modal-close");
const removeModalCancelEl = document.querySelector("#cart-remove-cancel");
const removeModalConfirmEl = document.querySelector("#cart-remove-confirm");
let cartItems = [];
let precios = [];
let plataformas = [];
let descuentos = [];
let tasaBs = null;
let currentUser = null;
let userSaldo = 0;
let cartMontoUsd = null;
let cartMontoFinal = null;
let cartDescuento = null;
let cartNeedsSync = false;
let cartUseSaldo = false;
let dbUseSaldo = false;
let dbCartSnapshot = new Map();
let currentUserIsCliente = true;
let discountColumns = [];
let discountColumnById = {};
let pendingRemoveIndex = null;
const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) =>
  v === true || v === 1 || v === "1" || v === "true" || v === "t";
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;

const getDiscountColumnsFromRows = (rows = []) => {
  const cols = new Set();
  (rows || []).forEach((row) => {
    Object.keys(row || {}).forEach((k) => {
      const key = String(k || "").toLowerCase();
      if (/^descuento_\d+$/i.test(key)) cols.add(key);
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

const updateUseSaldoButton = () => {
  const canUseSaldo = Number(userSaldo) > 0;
  if (!canUseSaldo) {
    cartUseSaldo = false;
  }
  const saldoToggleInput = document.querySelector(
    'input[data-cart-action="toggle-saldo"]',
  );
  const saldoToggleWrap = document.querySelector(".cart-saldo-toggle");
  if (saldoToggleInput) {
    saldoToggleInput.disabled = !canUseSaldo;
    saldoToggleInput.checked = canUseSaldo && !!cartUseSaldo;
  }
  if (saldoToggleWrap) {
    saldoToggleWrap.classList.toggle("is-active", canUseSaldo && !!cartUseSaldo);
    saldoToggleWrap.classList.toggle("is-disabled", !canUseSaldo);
    saldoToggleWrap.title = canUseSaldo
      ? "Aplicar saldo al total"
      : "No tienes saldo disponible";
  }
};

const setRefreshLoading = (btn, loading) => {
  if (!btn) return;
  if (loading) {
    if (!btn.dataset.originalText) {
      btn.dataset.originalText = btn.textContent || "Actualizar carrito";
      btn.dataset.originalWidth = String(btn.offsetWidth || "");
      btn.dataset.originalHeight = String(btn.offsetHeight || "");
    }
    btn.dataset.loading = "1";
    btn.classList.add("is-loading");
    const width = Number(btn.dataset.originalWidth);
    const height = Number(btn.dataset.originalHeight);
    if (Number.isFinite(width) && width > 0) btn.style.width = `${width}px`;
    if (Number.isFinite(height) && height > 0) btn.style.height = `${height}px`;
    btn.textContent = "Actualizando...";
    btn.disabled = true;
    return;
  }
  btn.dataset.loading = "";
  if (btn.dataset.originalText) {
    btn.textContent = btn.dataset.originalText;
    delete btn.dataset.originalText;
  }
  delete btn.dataset.originalWidth;
  delete btn.dataset.originalHeight;
  btn.style.width = "";
  btn.style.height = "";
  btn.classList.remove("is-loading");
};

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

const resolveDiscountColumn = (platform, mode = "months") => {
  const raw = mode === "items" ? platform?.id_descuento_cantidad : platform?.id_descuento_mes;
  const asText = String(raw || "").trim();
  if (/^descuento_\d+$/i.test(asText)) return asText.toLowerCase();
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

const closeRemoveModal = () => {
  pendingRemoveIndex = null;
  removeModalEl?.classList.add("hidden");
  removeModalEl?.setAttribute("aria-hidden", "true");
  document.body.classList.remove("modal-open");
};

const openRemoveModal = (idx) => {
  if (!Number.isFinite(idx) || idx < 0) return;
  pendingRemoveIndex = idx;
  removeModalEl?.classList.remove("hidden");
  removeModalEl?.setAttribute("aria-hidden", "false");
  document.body.classList.add("modal-open");
};

const confirmRemoveModal = () => {
  const idx = Number(pendingRemoveIndex);
  if (!Number.isFinite(idx) || idx < 0) {
    closeRemoveModal();
    return;
  }
  const item = cartItems[idx];
  if (!item) {
    closeRemoveModal();
    return;
  }
  cartItems.splice(idx, 1);
  cartNeedsSync = true;
  renderCart();
  closeRemoveModal();
  (async () => {
    if (removeModalConfirmEl) {
      removeModalConfirmEl.disabled = true;
    }
    try {
      await syncCartWithServer({ alertOnError: true });
    } finally {
      if (removeModalConfirmEl) {
        removeModalConfirmEl.disabled = false;
      }
    }
  })();
};

const placePayButton = () => {
  if (!btnPay) return;
  const payRow = document.querySelector(".cart-pay-row");
  if (!payRow) return;
  if (btnPay.parentElement !== payRow) {
    payRow.appendChild(btnPay);
  }
};

const updateRefreshButtonState = () => {
  const refreshBtn = itemsEl?.querySelector('[data-cart-action="refresh"]');
  if (!refreshBtn) return;
  const needsSync = !!cartNeedsSync;
  const isLoading = refreshBtn.dataset.loading === "1";
  refreshBtn.disabled = isLoading ? true : !needsSync;
  refreshBtn.classList.toggle("btn-disabled-soft", !needsSync && !isLoading);
  refreshBtn.classList.toggle("is-loading", isLoading);
  if (btnPay) {
    btnPay.disabled = needsSync || isLoading;
    btnPay.classList.toggle("btn-disabled-soft", needsSync);
  }
  const summaryTable = itemsEl?.querySelector(".cart-summary-table");
  summaryTable?.classList.toggle("is-dim", needsSync);
  refreshNoteEl?.classList.toggle("hidden", !needsSync);
};

const syncCartWithServer = async ({ refreshBtn = null, alertOnError = false } = {}) => {
  try {
    if (refreshBtn) setRefreshLoading(refreshBtn, true);
    await syncCartValuesBeforeCheckout();
    const freshCart = await fetchCart();
    if (freshCart?.items) {
      cartItems = freshCart.items;
    }
    dbUseSaldo = isTrue(freshCart?.usa_saldo);
    cartUseSaldo = dbUseSaldo;
    updateUseSaldoButton();
    cartMontoUsd = Number.isFinite(Number(freshCart?.monto_usd))
      ? Number(freshCart.monto_usd)
      : cartMontoUsd;
    cartMontoFinal = Number.isFinite(Number(freshCart?.monto_final))
      ? Number(freshCart.monto_final)
      : cartMontoFinal;
    cartDescuento = Number.isFinite(Number(freshCart?.descuento))
      ? Number(freshCart.descuento)
      : cartDescuento;
    dbCartSnapshot = buildCartSnapshot(cartItems);
    cartNeedsSync = false;
    updateCartNeedsSync();
    const cardCount = itemsEl?.querySelectorAll(".cart-item-card").length || 0;
    if (cardCount && cardCount !== cartItems.length) {
      renderCart();
    } else {
      (cartItems || []).forEach((_, idx) => updateCartRowUI(idx));
      updateCartSummaryUI();
    }
    return true;
  } catch (err) {
    console.error("refresh cart error", err);
    if (alertOnError) {
      alert("No se pudo actualizar el carrito.");
    }
    return false;
  } finally {
    if (refreshBtn) setRefreshLoading(refreshBtn, false);
    updateCartNeedsSync();
  }
};

const buildCartSnapshot = (items) => {
  const map = new Map();
  (items || []).forEach((item, idx) => {
    const key = item?.id_item ?? `p:${item?.id_precio ?? "x"}:${idx}`;
    map.set(key, {
      cantidad: Number(item?.cantidad) || 1,
      meses: Number(item?.meses) || Number(item?.duracion) || 1,
    });
  });
  return map;
};

const updateCartNeedsSync = () => {
  let needsSync = false;
  if (dbCartSnapshot && dbCartSnapshot.size > 0) {
    const current = buildCartSnapshot(cartItems);
    if (current.size !== dbCartSnapshot.size) {
      needsSync = true;
    } else {
      for (const [key, val] of current.entries()) {
        const base = dbCartSnapshot.get(key);
        if (!base) {
          needsSync = true;
          break;
        }
        if (Number(base.cantidad) !== Number(val.cantidad) || Number(base.meses) !== Number(val.meses)) {
          needsSync = true;
          break;
        }
      }
    }
  }
  if (cartUseSaldo !== dbUseSaldo) {
    needsSync = true;
  }
  cartNeedsSync = needsSync;
  updateRefreshButtonState();
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

const getPriceMaps = () => {
  const priceById = (precios || []).reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});
  const platformById = (plataformas || []).reduce((acc, p) => {
    acc[p.id_plataforma] = p;
    return acc;
  }, {});
  return { priceById, platformById };
};

const calcItemTotals = (item, price, platform) => {
  const unit = price?.precio_usd_detal || 0;
  const qtyVal = item?.cantidad || 0;
  const mesesVal = item?.meses || price?.duracion || 1;
  const baseSubtotal = round2(unit * qtyVal * mesesVal);
  let descuentoMesesVal = 0;
  let descuentoCantidadVal = 0;
  let rateMeses = 0;
  const monthColumn = resolveDiscountColumn(platform, "months");
  const qtyColumn = resolveDiscountColumn(platform, "items");
  const monthEnabled =
    !!platform?.descuento_meses &&
    isDiscountEnabledForAudience(platform, "months", currentUserIsCliente);
  const qtyEnabled = isDiscountEnabledForAudience(platform, "items", currentUserIsCliente);
  if (monthEnabled) {
    const rawRate = getClosestDiscountPct(descuentos, mesesVal, monthColumn);
    rateMeses = rawRate > 1 ? rawRate / 100 : rawRate;
    descuentoMesesVal = round2(baseSubtotal * rateMeses);
  }
  const rawRateQty = qtyEnabled ? getClosestDiscountPct(descuentos, qtyVal, qtyColumn) : 0;
  const rateQty = rawRateQty > 1 ? rawRateQty / 100 : rawRateQty;
  if (rateQty > 0) {
    descuentoCantidadVal = round2(baseSubtotal * rateQty);
  }
  const descuentoVal = round2(descuentoMesesVal + descuentoCantidadVal);
  const subtotal = round2(baseSubtotal - descuentoVal);
  return {
    unit,
    qtyVal,
    mesesVal,
    baseSubtotal,
    descuentoMesesVal,
    descuentoCantidadVal,
    rateMeses,
    rateQty,
    descuentoVal,
    subtotal,
  };
};

const updateCartSummaryUI = () => {
  if (!itemsEl) return;
  const { priceById, platformById } = getPriceMaps();
  let subtotalBruto = 0;
  let totalDescuento = 0;
  (cartItems || []).forEach((item) => {
    const price = priceById[item.id_precio] || {};
    const platform = platformById[price.id_plataforma] || {};
    const totals = calcItemTotals(item, price, platform);
    subtotalBruto = round2(subtotalBruto + totals.baseSubtotal);
    totalDescuento = round2(totalDescuento + totals.descuentoVal);
  });
  const subtotalMostrar = round2(subtotalBruto);
  const descuentoMostrar = round2(totalDescuento);
  const showSaldoRow = cartUseSaldo && userSaldo > 0;
  const saldoAplicado = showSaldoRow ? round2(userSaldo) : 0;
  const totalMostrar = round2(
    round2(subtotalMostrar) + round2(-descuentoMostrar) + round2(-saldoAplicado),
  );

  const subtotalEl = itemsEl.querySelector('[data-summary="subtotal"]');
  if (subtotalEl) subtotalEl.textContent = `$${subtotalMostrar.toFixed(2)}`;
  const descuentoEl = itemsEl.querySelector('[data-summary="descuento"]');
  if (descuentoEl) descuentoEl.textContent = `-$${descuentoMostrar.toFixed(2)}`;
  const saldoEl = itemsEl.querySelector('[data-summary="saldo"]');
  if (saldoEl) saldoEl.textContent = `-$${Number(saldoAplicado).toFixed(2)}`;
  const saldoRow = itemsEl.querySelector(".cart-total-saldo");
  if (saldoRow) {
    saldoRow.classList.toggle("hidden", !showSaldoRow);
  }
  const totalEl = itemsEl.querySelector('[data-summary="total"]');
  if (totalEl) totalEl.textContent = `$${Number(totalMostrar).toFixed(2)}`;
};

const updateCartRowUI = (idx) => {
  if (!itemsEl) return;
  const item = cartItems[idx];
  if (!item) return;
  const { priceById, platformById } = getPriceMaps();
  const price = priceById[item.id_precio] || {};
  const platform = platformById[price.id_plataforma] || {};
  const totals = calcItemTotals(item, price, platform);

  const renderDiscounts = () => {
    const parts = [];
    if (totals.descuentoMesesVal > 0) {
      parts.push(`<span class="discount-badge">-${(totals.rateMeses * 100).toFixed(2)}% mes</span>`);
    }
    if (totals.descuentoCantidadVal > 0) {
      parts.push(`<span class="discount-badge">-${(totals.rateQty * 100).toFixed(2)}% cant.</span>`);
    }
    return parts.join("");
  };

  const row = itemsEl.querySelector(`tr[data-index="${idx}"]`);
  if (row) {
    row.querySelectorAll(".cart-meses-value").forEach((el) => {
      el.textContent = totals.mesesVal;
    });
    row.querySelectorAll(".cart-cantidad-value").forEach((el) => {
      el.textContent = totals.qtyVal;
    });
    const subtotalEl = row.querySelector(".cart-subtotal-value");
    if (subtotalEl) subtotalEl.textContent = `$${totals.subtotal.toFixed(2)}`;
    const discountLine = row.querySelector(".cart-discount-line");
    if (discountLine) discountLine.innerHTML = renderDiscounts();
  }

  const card = itemsEl.querySelector(`.cart-item-card[data-index="${idx}"]`);
  if (card) {
    card.querySelectorAll(".cart-meses-value").forEach((el) => {
      el.textContent = totals.mesesVal;
    });
    card.querySelectorAll(".cart-cantidad-value").forEach((el) => {
      el.textContent = totals.qtyVal;
    });
    const subtotalEl = card.querySelector(".cart-subtotal-value");
    if (subtotalEl) subtotalEl.textContent = `$${totals.subtotal.toFixed(2)}`;
    const discountLine = card.querySelector(".cart-discount-line");
    if (discountLine) discountLine.innerHTML = renderDiscounts();
  }
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
  const renderedItems = cartItems.map((item, idx) => {
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
      const correoRenovacion =
        item.renovacion === true ? (item.correo || "-") : "";
      subtotalBruto = round2(subtotalBruto + baseSubtotal);
      const rowHtml = `
        <tr data-cart-row="1" data-index="${idx}">
          <td>
            <div class="cart-info tight">
              <div class="cart-product-scroll">
                <div class="cart-product">
                  <button type="button" class="cart-remove btn-delete" data-index="${idx}" aria-label="Eliminar item">
                    <img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/x-icon.png.webp" alt="" aria-hidden="true" />
                  </button>
                  <div class="cart-thumb-sm">
                    <img src="${platform.imagen || ""}" alt="${platform.nombre || ""}" loading="lazy" decoding="async" />
                  </div>
                  <div class="cart-product-text">
                    <p class="cart-name">${platform.nombre || `Precio ${item.id_precio}`}</p>
                    <p class="cart-detail">${detalle || ""}</p>
                    <p class="cart-detail">Tipo: ${tipo}</p>
                    ${
                      item.renovacion
                        ? `<p class="cart-detail cart-detail-email"><span class="cart-email-label">Correo:</span><span class="cart-email-value">${correoRenovacion}</span></p>`
                        : ""
                    }
                    <p class="cart-detail cart-price-line">Precio: $${round2(unit).toFixed(2)}</p>
                  </div>
                </div>
              </div>
            </div>
          </td>
          <td class="cart-cell-center">$${round2(unit).toFixed(2)}</td>
          <td class="cart-cell-center">
            <div class="modal-qty">
              <button type="button" class="meses-minus" data-index="${idx}" aria-label="menos">-</button>
              <span class="cart-meses-value">${mesesVal}</span>
              <button type="button" class="meses-plus" data-index="${idx}" aria-label="más">+</button>
            </div>
          </td>
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
            <span class="cart-subtotal-value">$${subtotal.toFixed(2)}</span>
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
      const qtyControl = item.renovacion
        ? `<span class="cart-cantidad-value">${qtyVal}</span>`
        : `
          <div class="modal-qty">
            <button type="button" class="cart-minus" data-index="${idx}" aria-label="Disminuir">-</button>
            <span class="cart-cantidad-value">${qtyVal}</span>
            <button type="button" class="cart-plus" data-index="${idx}" aria-label="Aumentar">+</button>
          </div>
        `;
      const cardImage = platform.banner || "";
      const cardHtml = `
        <section class="cart-item-card" data-index="${idx}">
          <div class="cart-item-top">
            <div class="cart-item-logo">
              <img src="${cardImage}" alt="${platform.nombre || ""}" loading="lazy" decoding="async" />
            </div>
            <div class="cart-item-info">
              <p class="cart-name">${platform.nombre || `Precio ${item.id_precio}`}</p>
              <p class="cart-detail">${detalle || ""}</p>
              <p class="cart-detail">Tipo: ${tipo}</p>
              ${
                item.renovacion
                  ? `<p class="cart-detail cart-detail-email"><span class="cart-email-label">Correo:</span><span class="cart-email-value">${correoRenovacion}</span></p>`
                  : ""
              }
              <p class="cart-detail">Precio: $${round2(unit).toFixed(2)}</p>
            </div>
            <button type="button" class="cart-remove btn-delete" data-index="${idx}" aria-label="Eliminar item">
              <img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/x-icon.png.webp" alt="" aria-hidden="true" />
            </button>
          </div>
          <div class="cart-item-bottom">
            <div class="cart-item-cell">
              <span class="cart-item-label">Meses</span>
              <div class="modal-qty">
                <button type="button" class="meses-minus" data-index="${idx}" aria-label="menos">-</button>
                <span class="cart-meses-value">${mesesVal}</span>
                <button type="button" class="meses-plus" data-index="${idx}" aria-label="más">+</button>
              </div>
            </div>
            <div class="cart-item-cell">
              <span class="cart-item-label">Cantidad</span>
              ${qtyControl}
            </div>
          </div>
          <div class="cart-item-subtotal">
            <span class="cart-item-label">Subtotal</span>
            <div class="cart-subtotal"><span class="cart-subtotal-value">$${subtotal.toFixed(2)}</span></div>
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
          </div>
        </section>
      `;
      return { rowHtml, cardHtml };
    });

  const rows = renderedItems.map((r) => r.rowHtml).join("");
  const cards = renderedItems.map((r) => r.cardHtml).join("");

  const descuentoMostrar = round2(totalDescuento);
  const subtotalMostrar = round2(subtotalBruto);
  const showSaldoRow = cartUseSaldo && userSaldo > 0;
  const saldoAplicado = showSaldoRow ? round2(userSaldo) : 0;
  const totalMostrar = round2(
    round2(subtotalMostrar) + round2(-descuentoMostrar) + round2(-saldoAplicado),
  );
  if (Number.isFinite(Number(cartDescuento))) {
    const diff = Math.abs(Number(cartDescuento) - totalDescuento);
    if (diff >= 0.01) {
      console.warn("[cart] descuento UI/DB mismatch", {
        ui: totalDescuento,
        db: Number(cartDescuento),
      });
    }
  }
  const saldoRow = `
            <tr class="cart-total-row cart-total-saldo ${showSaldoRow ? "" : "hidden"}">
              <td class="cart-cell-center">Saldo aplicado</td>
              <td class="cart-cell-center"><span data-summary="saldo">-$${Number(saldoAplicado).toFixed(2)}</span></td>
            </tr>
          `;
  const canUseSaldo = userSaldo > 0;
  itemsEl.innerHTML = `
    <div class="cart-layout">
      <div class="cart-left">
        <div class="cart-items-cards">
          ${cards}
        </div>
        <div class="cart-table-scroll">
          <table class="table-base cart-page-table">
            <thead>
              <tr>
                <th>Producto</th>
                <th>Precio</th>
                <th>Meses</th>
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
          <label class="cart-saldo-toggle ${canUseSaldo ? (showSaldoRow ? "is-active" : "") : "is-disabled"}" title="${canUseSaldo ? "Aplicar saldo al total" : "No tienes saldo disponible"}">
            <input type="checkbox" data-cart-action="toggle-saldo" ${showSaldoRow ? "checked" : ""} ${canUseSaldo ? "" : "disabled"} />
            <span>Usar saldo</span>
          </label>
          <button type="button" class="btn-cart-main" data-cart-action="refresh">Actualizar carrito</button>
        </div>
      </div>
      <div class="cart-right">
        <table class="table-base cart-page-table cart-summary-table">
          <tbody>
            <tr class="cart-total-row">
              <td class="cart-cell-center">Subtotal</td>
              <td class="cart-cell-center"><span data-summary="subtotal">$${subtotalMostrar.toFixed(2)}</span></td>
            </tr>
            <tr class="cart-total-row cart-total-discount">
              <td class="cart-cell-center">Descuentos aplicados</td>
              <td class="cart-cell-center"><span data-summary="descuento">-$${descuentoMostrar.toFixed(2)}</span></td>
            </tr>
            ${saldoRow}
            <tr class="cart-total-row cart-total-final">
              <td class="cart-cell-center">Total</td>
              <td class="cart-cell-center"><span data-summary="total">$${totalMostrar.toFixed(2)}</span></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `;
  placePayButton();
  updateCartNeedsSync();
};

const handleCartClick = async (e) => {
  const cartActionBtn = e.target.closest("[data-cart-action]");
  if (cartActionBtn) {
    const action = cartActionBtn.dataset.cartAction;
    if (action === "continue") {
      window.location.href = "index.html";
      return;
    }
    if (action === "toggle-saldo") {
      return;
    }
    if (action === "refresh") {
      const refreshBtn = cartActionBtn;
      await syncCartWithServer({ refreshBtn, alertOnError: true });
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

  if (btnRemove) {
    openRemoveModal(idx);
    return;
  }

  let didUpdate = false;
  if (btnMinus || btnPlus) {
    const delta = btnMinus ? -1 : 1;
    const newQty = (item.cantidad || 0) + delta;
    if (newQty <= 0) {
      openRemoveModal(idx);
      return;
    }
    item.cantidad = newQty;
    cartNeedsSync = true;
    didUpdate = true;
  }
  if (btnMesesMinus || btnMesesPlus) {
    const delta = btnMesesMinus ? -1 : 1;
    const current = item.meses || price.duracion || 1;
    const next = Math.max(1, current + delta);
    item.meses = next;
    cartNeedsSync = true;
    didUpdate = true;
  }
  if (didUpdate) {
    updateCartRowUI(idx);
    updateCartSummaryUI();
    updateCartNeedsSync();
  }
};

const handleCartChange = (e) => {
  const saldoToggle = e.target.closest('input[data-cart-action="toggle-saldo"]');
  if (!saldoToggle) return;
  if (saldoToggle.disabled) return;
  cartUseSaldo = !!saldoToggle.checked;
  updateUseSaldoButton();
  updateCartSummaryUI();
  updateCartNeedsSync();
};

removeModalBackdropEl?.addEventListener("click", closeRemoveModal);
removeModalCloseEl?.addEventListener("click", closeRemoveModal);
removeModalCancelEl?.addEventListener("click", closeRemoveModal);
removeModalConfirmEl?.addEventListener("click", confirmRemoveModal);
window.addEventListener("keydown", (ev) => {
  if (ev.key !== "Escape") return;
  if (removeModalEl?.classList.contains("hidden")) return;
  closeRemoveModal();
});

async function init() {
  setStatus("Cargando carrito...");
  await loadPaginaBranding({ logoSelectors: [".logo"], applyFavicon: true, forceRefresh: true }).catch(
    (err) => {
      console.warn("cart branding load error", err);
    },
  );
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
      userSaldo = saldoNum;
      cartSaldoEl.textContent = `$${saldoNum.toFixed(2)}`;
      cartSaldoEl.classList.toggle("is-zero", saldoNum <= 0);
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
    dbCartSnapshot = buildCartSnapshot(cartData?.items || []);
    dbUseSaldo = isTrue(cartData?.usa_saldo);
    cartUseSaldo = dbUseSaldo;
    const canUseSaldo = userSaldo > 0;
    if (!canUseSaldo) {
      cartUseSaldo = false;
      dbUseSaldo = false;
    }
    updateUseSaldoButton();
    cartMontoUsd = Number.isFinite(Number(cartData?.monto_usd))
      ? Number(cartData.monto_usd)
      : null;
    cartMontoFinal = Number.isFinite(Number(cartData?.monto_final))
      ? Number(cartData.monto_final)
      : null;
    cartDescuento = Number.isFinite(Number(cartData?.descuento))
      ? Number(cartData.descuento)
      : null;
    tasaBs = tasaResp ? Math.round(tasaResp * TASA_MARKUP * 100) / 100 : null;
    setCachedCart(cartData);
    const esCliente =
      isTrue(sessionRoles?.acceso_cliente) ||
      isTrue(currentUser?.acceso_cliente);
    currentUserIsCliente = !!esCliente;
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
    discountColumns = getDiscountColumnsFromRows(descuentos);
    discountColumnById = buildDiscountColumnByIdMap(descuentos, discountColumns);
    cartItems = cartData.items || [];
    renderCart();
    setStatus("");
    itemsEl?.addEventListener("click", handleCartClick);
    itemsEl?.addEventListener("change", handleCartChange);
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

  if (cartUseSaldo !== dbUseSaldo) {
    await updateCartFlags({ usa_saldo: cartUseSaldo });
    dbUseSaldo = cartUseSaldo;
  }
};

btnContinue?.addEventListener("click", () => {
  window.location.href = "index.html";
});

btnPay?.addEventListener("click", () => {
  (async () => {
    try {
      if (cartNeedsSync) {
        alert("Actualiza el carrito para continuar.");
        return;
      }
      await syncCartValuesBeforeCheckout();
      const freshCart = await fetchCart();
      cartItems = freshCart.items || cartItems;
      cartMontoUsd = Number.isFinite(Number(freshCart?.monto_usd))
        ? Number(freshCart.monto_usd)
        : cartMontoUsd;
      cartMontoFinal = Number.isFinite(Number(freshCart?.monto_final))
        ? Number(freshCart.monto_final)
        : cartMontoFinal;
      cartDescuento = Number.isFinite(Number(freshCart?.descuento))
        ? Number(freshCart.descuento)
        : cartDescuento;
      if (!currentUser) {
        window.location.href = "checkout.html";
        return;
      }
      const saldo = Number(currentUser?.saldo) || 0;
      const useSaldo = isTrue(freshCart?.usa_saldo);
      const montoFinal = Number(freshCart?.monto_final);
      const checkoutTotal =
        useSaldo && Number.isFinite(montoFinal)
          ? montoFinal
          : cartMontoUsd;
      if (!Number.isFinite(checkoutTotal)) {
        window.location.href = "checkout.html";
        return;
      }
      if (!useSaldo) {
        window.location.href = "checkout.html";
        return;
      }
      if (!Number.isFinite(cartMontoUsd) || saldo < cartMontoUsd) {
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
