import { sendCartDelta } from "./api.js";
import { requireSession } from "./session.js";
import { addToCart } from "./cart.js";

let modalEls = {};
let preciosPorPlataforma = {};
let descuentosPorMes = {};
let selectedPrecio = null;
let currentFlags = { por_pantalla: false, por_acceso: false, tarjeta_de_regalo: false, entrega_inmediata: false };
let currentPlatform = null;
let currentQty = 1; // items
let currentMonths = 1;
let stockByPlatform = {};
let selectedButtonEl = null;

const getDiscountPercent = (platform, months) => {
  const usa =
    platform?.descuento_meses === true ||
    platform?.descuento_meses === "true" ||
    platform?.descuento_meses === "1";
  if (!usa) return 0;
  const key = Number(months) || 0;
  const pct = descuentosPorMes[key] || 0;
  console.log("[discount] plataforma", platform?.nombre, "meses", key, "pct", pct, "map", descuentosPorMes);
  return pct;
};

const buildButtonLabel = (opcion, flags, months, platform) => {
  const isNetflixPlan2 =
    Number(platform?.id_plataforma) === 1 &&
    [4, 5].includes(Number(opcion.id_precio));
  const basePrice = Number(opcion.precio_usd_detal) || 0;
  const discount = getDiscountPercent(platform, months);
  const mesesFactor = months || 1;
  const precioMeses = basePrice * mesesFactor;
  const finalPrecio = discount > 0 ? precioMeses * (1 - discount / 100) : precioMeses;

  if (
    !isNetflixPlan2 &&
    (opcion.completa === true ||
      opcion.completa === "true" ||
      opcion.completa === 1 ||
      opcion.completa === "1")
  ) {
    return { text: `cuenta completa $${finalPrecio.toFixed(2)}` };
  }
  if (flags.tarjeta_de_regalo) {
    return {
      region: opcion.region || "-",
      text: `${opcion.valor_tarjeta_de_regalo || ""} ${opcion.moneda || ""} $${finalPrecio.toFixed(2)}`,
    };
  }
  const cantidad = opcion.cantidad || 1;
  const baseUnit = flags.por_pantalla
    ? "pantalla"
    : flags.por_acceso
    ? "dispositivo"
    : "mes";
  const plural = cantidad === 1 ? "" : baseUnit === "mes" ? "es" : "s";
  return { text: `${cantidad} ${baseUnit}${plural} $${finalPrecio.toFixed(2)}` };
};

const buildCartDetalle = (opcion, flags, cantidad) => {
  if (flags.tarjeta_de_regalo) {
    const region = opcion.region || "-";
    const monto = `${opcion.valor_tarjeta_de_regalo || ""} ${opcion.moneda || ""} $${opcion.precio_usd_detal}`;
    return `Región: ${region} · Monto: ${monto}`;
  }
  const qty = cantidad || opcion.cantidad || 1;
  const baseUnit = flags.por_pantalla
    ? "pantalla"
    : flags.por_acceso
    ? "dispositivo"
    : "mes";
  const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
  return `${qty} ${baseUnit}${plural} $${opcion.precio_usd_detal}`;
};

const renderPrecios = (plataformaId, flags) => {
  const {
    modalPrecios,
    modalQtyMonths,
    modalQtyItems,
    monthsDiscount,
    qtyValue,
    qtyMonthsValue,
    btnMinus,
    btnPlus,
    btnMonthsMinus,
    btnMonthsPlus,
    btnAdd,
  } = modalEls;
  modalPrecios.innerHTML = "";
  selectedPrecio = null;
  currentQty = 1;
  currentMonths = 1;
  qtyValue.textContent = currentQty;
  qtyMonthsValue.textContent = currentMonths;
  btnMinus.disabled = true;
  btnPlus.disabled = true;
  btnMonthsMinus.disabled = true;
  btnMonthsPlus.disabled = true;
  btnAdd.disabled = true;
  modalQtyMonths?.classList.add("modal-qty-disabled");
  modalQtyItems?.classList.add("modal-qty-disabled");
  if (monthsDiscount) monthsDiscount.classList.add("hidden");
  // Mostrar/ocultar control de meses según flags (solo si no es por pantalla/acceso)
  const hideMonths = !flags.por_pantalla && !flags.por_acceso;
  if (hideMonths) {
    modalQtyMonths?.classList.add("hidden");
  } else {
    modalQtyMonths?.classList.remove("hidden");
  }
  const opciones = [...(preciosPorPlataforma[plataformaId] || [])]
    .filter((p) => p.precio_usd_detal !== null && p.precio_usd_detal !== undefined)
    .sort((a, b) => (a.id_precio || 0) - (b.id_precio || 0));
  if (!opciones.length) {
    modalPrecios.innerHTML =
      '<p class="sin-plataformas">Sin precios configurados.</p>';
    return;
  }

  const agrupados = opciones.reduce((acc, opcion) => {
    const planKey = opcion.plan || "";
    if (!acc[planKey]) acc[planKey] = [];
    acc[planKey].push(opcion);
    return acc;
  }, {});

  Object.entries(agrupados).forEach(([plan, items]) => {
    const wrapper = document.createElement("div");
    wrapper.className = "plan-bloque";
    let stockPlan = stockByPlatform[plataformaId] ?? 0;
    let isPlan2 = false;
    if (Number(plataformaId) === 1) {
      const hasSubCuenta = items.some(
        (op) =>
          op.sub_cuenta === true ||
          op.sub_cuenta === "true" ||
          op.sub_cuenta === 1,
      );
      const planLower = (plan || "").toLowerCase();
      isPlan2 = hasSubCuenta || planLower.includes("hogar") || planLower.includes("extra");
      stockPlan = stockByPlatform[isPlan2 ? "1_plan2" : "1_plan1"] ?? stockPlan;
    }
    const completasKey = `${plataformaId}_completas`;
    const completasCount = stockByPlatform[completasKey] ?? 0;
    const stockLines = ["Stock:", `- Perfiles: ${stockPlan}`];
    if (!(Number(plataformaId) === 1 && isPlan2)) {
      stockLines.push(`- Cuentas completas: ${completasCount}`);
    }
    const planTitle = plan
      ? `${plan}<br>${stockLines.join("<br>")}`
      : stockLines.join("<br>");
    const planLabel = `<p class="plan-titulo">${planTitle}</p>`;
    wrapper.innerHTML = planLabel;

    const list = document.createElement("div");
    list.className = "plan-opciones";

    items.forEach((opcion) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "precio-opcion";
      const label = buildButtonLabel(opcion, flags, currentMonths, currentPlatform);
      if (label.region) {
        const regionEl = document.createElement("p");
        regionEl.className = "precio-region";
        regionEl.textContent = `Región: ${label.region}`;
        list.appendChild(regionEl);
      }
      btn.textContent = label.text;
      btn.dataset.qty = opcion.cantidad;
      btn.dataset.idPrecio = opcion.id_precio;
      btn.addEventListener("click", () => {
        const isSelected = btn.classList.contains("selected");
        modalPrecios
          .querySelectorAll(".precio-opcion")
          .forEach((b) => b.classList.remove("selected"));
        if (isSelected) {
          selectedPrecio = null;
          selectedButtonEl = null;
          currentQty = 1;
          currentMonths = 1;
          qtyValue.textContent = currentQty;
          qtyMonthsValue.textContent = currentMonths;
          btnMinus.disabled = true;
          btnPlus.disabled = true;
          btnMonthsMinus.disabled = true;
          btnMonthsPlus.disabled = true;
          btnAdd.disabled = true;
          modalQtyMonths?.classList.add("modal-qty-disabled");
          modalQtyItems?.classList.add("modal-qty-disabled");
          return;
        }
        btn.classList.add("selected");
        selectedPrecio = opcion;
        selectedButtonEl = btn;
        currentQty = opcion.cantidad || 1;
        // Mantén el valor de meses seleccionado por el usuario (inicia en 1)
        qtyValue.textContent = currentQty;
        qtyMonthsValue.textContent = currentMonths;
      btnMinus.disabled = false;
      btnPlus.disabled = false;
      btnAdd.disabled = false;
      modalQtyItems?.classList.remove("modal-qty-disabled");
      if (!hideMonths) {
        btnMonthsMinus.disabled = false;
        btnMonthsPlus.disabled = false;
        modalQtyMonths?.classList.remove("modal-qty-disabled");
          const pct = getDiscountPercent(currentPlatform, currentMonths);
          if (pct > 0 && monthsDiscount) {
            monthsDiscount.textContent = `-${pct}%`;
            monthsDiscount.classList.remove("hidden");
          } else {
            monthsDiscount?.classList.add("hidden");
          }
        } else {
          btnMonthsMinus.disabled = true;
          btnMonthsPlus.disabled = true;
          modalQtyMonths?.classList.add("modal-qty-disabled");
          monthsDiscount?.classList.add("hidden");
        }
      });
      list.appendChild(btn);
    });

    wrapper.appendChild(list);
    modalPrecios.appendChild(wrapper);
  });
};

const updateQtyItems = (delta) => {
  if (!selectedPrecio) return;
  const { qtyValue } = modalEls;
  currentQty = Math.max(1, currentQty + delta);
  qtyValue.textContent = currentQty;
};

const updateQtyMonths = (delta) => {
  if (!selectedPrecio) return;
  const { qtyMonthsValue, monthsDiscount } = modalEls;
  currentMonths = Math.max(1, Math.round(currentMonths + delta));
  qtyMonthsValue.textContent = currentMonths;
  const pct = getDiscountPercent(currentPlatform, currentMonths);
  const pctLabel = `-${pct}%`;
  console.log("[discount] update months", { currentMonths, pct });
  if (monthsDiscount) {
    if (pct > 0) {
      monthsDiscount.textContent = pctLabel;
      monthsDiscount.classList.remove("hidden");
    } else {
      monthsDiscount.classList.add("hidden");
    }
  }
  // refresca labels de precios
  const opciones = preciosPorPlataforma[currentPlatform?.id_plataforma] || [];
  const buttons = modalEls.modalPrecios?.querySelectorAll(".precio-opcion");
  if (buttons) {
    buttons.forEach((btn) => {
      const idPrecio = Number(btn.dataset.idPrecio);
      const opt = opciones.find((o) => o.id_precio === idPrecio);
      if (opt) {
        const label = buildButtonLabel(opt, currentFlags, currentMonths, currentPlatform);
        btn.textContent = label.text;
      }
    });
  }
};

const closeModal = () => modalEls.modal.classList.add("hidden");

const animateAddToCart = () => {
  if (!selectedButtonEl) return;
  const cartIcon = document.querySelector(".carrito");
  if (!cartIcon) return;
  const rect = selectedButtonEl.getBoundingClientRect();
  const cartRect = cartIcon.getBoundingClientRect();
  const clone = selectedButtonEl.cloneNode(true);
  clone.classList.add("flying-price");
  clone.style.position = "fixed";
  clone.style.left = `${rect.left}px`;
  clone.style.top = `${rect.top}px`;
  clone.style.width = `${rect.width}px`;
  clone.style.height = `${rect.height}px`;
  clone.style.pointerEvents = "none";
  clone.style.zIndex = "9999";
  document.body.appendChild(clone);
  const deltaX = cartRect.left + cartRect.width / 2 - (rect.left + rect.width / 2);
  const deltaY = cartRect.top + cartRect.height / 2 - (rect.top + rect.height / 2);
  clone.animate(
    [
      { transform: "translate(0, 0) scale(1)", opacity: 1 },
      { transform: `translate(${deltaX}px, ${deltaY}px) scale(0.4)`, opacity: 0 },
    ],
    { duration: 600, easing: "ease-out" }
  ).onfinish = () => clone.remove();
};

export const setPrecios = (map) => {
  preciosPorPlataforma = map;
};

export const setDescuentos = (rows = []) => {
  descuentosPorMes = (rows || []).reduce((acc, d) => {
    const meses = Number(d.meses);
    const desc = Number(d.descuento) || 0;
    if (Number.isFinite(meses) && meses > 0) {
      acc[meses] = desc;
    }
    return acc;
  }, {});
};

export const setStockData = (map) => {
  stockByPlatform = map || {};
};

export const initModal = (elements) => {
  modalEls = elements;
  const { btnMinus, btnPlus, btnMonthsMinus, btnMonthsPlus, closeBtn, backdrop, btnAdd } = modalEls;
  btnMinus?.addEventListener("click", () => updateQtyItems(-1));
  btnPlus?.addEventListener("click", () => updateQtyItems(1));
  btnMonthsMinus?.addEventListener("click", () => updateQtyMonths(-1));
  btnMonthsPlus?.addEventListener("click", () => updateQtyMonths(1));
  closeBtn?.addEventListener("click", closeModal);
  backdrop?.addEventListener("click", closeModal);
  btnAdd?.addEventListener("click", () => {
    // Si no hay sesión, redirige a login (requireSession hace redirect)
    try {
      requireSession();
    } catch (err) {
      return;
    }
    if (!selectedPrecio || !currentPlatform || !selectedPrecio.id_precio) {
      console.error("Falta id_precio en el precio seleccionado");
      return;
    }
    animateAddToCart();
    sendCartDelta(selectedPrecio.id_precio, currentQty, currentMonths, {
      renovacion: false,
      id_venta: null,
    }).finally(() => {
      addToCart();
    });
    closeModal();
  });
};

export const openModal = (platform) => {
  const {
    modal,
    modalImg,
    modalName,
    modalCategory,
    modalBadge,
  } = modalEls;
  const {
    id_plataforma,
    nombre,
    categoria,
    imagen,
    por_pantalla,
    por_acceso,
    tarjeta_de_regalo,
    entrega_inmediata,
    descuento_meses,
    id_descuento,
  } = platform;

  modalImg.src = imagen || "";
  modalImg.alt = nombre;
  modalName.textContent = nombre;
  modalCategory.textContent = categoria || "";
  if (modalBadge) {
    const stock = stockByPlatform[id_plataforma] ?? 0;
    if (
      (entrega_inmediata === true ||
        entrega_inmediata === "true" ||
        entrega_inmediata === "1") &&
      stock === 0
    ) {
      modalBadge.textContent = "Pronto mas stock";
      modalBadge.className = "modal-badge badge-warning";
    } else if (
      entrega_inmediata === true ||
      entrega_inmediata === "true" ||
      entrega_inmediata === "1"
    ) {
      modalBadge.textContent = "Entrega inmediata";
      modalBadge.className = "modal-badge badge-green";
    } else {
      modalBadge.textContent = "Por encargo";
      modalBadge.className = "modal-badge badge-gray";
    }
  }
  currentFlags = {
    por_pantalla:
      por_pantalla === true || por_pantalla === "true" || por_pantalla === "1",
    por_acceso:
      por_acceso === true || por_acceso === "true" || por_acceso === "1",
    tarjeta_de_regalo:
      tarjeta_de_regalo === true ||
      tarjeta_de_regalo === "true" ||
      tarjeta_de_regalo === "1",
    entrega_inmediata:
      entrega_inmediata === true ||
      entrega_inmediata === "true" ||
      entrega_inmediata === "1",
  };
  currentPlatform = {
    id_plataforma,
    nombre,
    imagen,
    descuento_meses:
      descuento_meses === true ||
      descuento_meses === "true" ||
      descuento_meses === "1",
    id_descuento: null,
  };
  if (descuento_meses !== undefined) {
    currentPlatform.descuento_meses = descuento_meses;
  }
  renderPrecios(id_plataforma, currentFlags);
  modal.classList.remove("hidden");
};
