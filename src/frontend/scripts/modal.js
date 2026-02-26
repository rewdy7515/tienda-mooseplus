import { sendCartDelta } from "./api.js";
import { requireSession } from "./session.js";
import { addToCart, refreshCartFromServer } from "./cart.js";

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
let modalImageListenerBound = false;
let modalTopEl = null;
let modalScrollHintEl = null;
let scrollHintBound = false;
let tooltipDismissBound = false;
let discountAudienceIsCliente = true;

const isTrueLike = (v) =>
  v === true || v === 1 || v === "1" || String(v || "").toLowerCase() === "true";

const isFalseLike = (v) =>
  v === false || v === 0 || v === "0" || String(v || "").toLowerCase() === "false";

const resolveDiscountColumn = (platform, mode = "months") => {
  const raw = mode === "items" ? platform?.id_descuento_cantidad : platform?.id_descuento_mes;
  const asNum = Number(raw);
  if (Number.isFinite(asNum) && asNum >= 1) {
    return `descuento_${Math.trunc(asNum)}`;
  }
  const asText = String(raw || "").trim();
  if (/^descuento_\d+$/i.test(asText)) return asText.toLowerCase();
  return mode === "items" ? "descuento_2" : "descuento_1";
};

const isDiscountEnabledForAudience = (platform, mode = "months") => {
  if (mode === "items") {
    return discountAudienceIsCliente
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
  return discountAudienceIsCliente
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

const updateScrollHint = () => {
  if (!modalTopEl || !modalScrollHintEl) return;
  if (!isBannerViewport()) {
    modalScrollHintEl.classList.remove("is-visible");
    return;
  }
  const hasOverflow = modalTopEl.scrollHeight - modalTopEl.clientHeight > 8;
  if (!hasOverflow) {
    modalScrollHintEl.classList.remove("is-visible");
    return;
  }
  const atTop = modalTopEl.scrollTop <= 2;
  let shouldShow = false;
  if (atTop) {
    const plans = modalTopEl.querySelectorAll(".plan-bloque");
    if (plans.length) {
      const lastPlan = plans[plans.length - 1];
      const topRect = modalTopEl.getBoundingClientRect();
      const lastRect = lastPlan.getBoundingClientRect();
      // Mostrar solo si el último plan no se ve ni un poco.
      shouldShow = lastRect.top >= topRect.bottom - 2;
    } else {
      shouldShow = true;
    }
  }
  modalScrollHintEl.classList.toggle("is-visible", shouldShow);
};

const isBannerViewport = () => {
  try {
    return window.matchMedia("(max-width: 700px)").matches;
  } catch (_) {
    return false;
  }
};

const resolveModalImage = (platform) => {
  const banner = platform?.banner;
  const imagen = platform?.imagen;
  if (isBannerViewport()) {
    return banner || "";
  }
  return imagen || banner || "";
};

const updateModalImage = () => {
  if (!modalEls?.modalImg || !currentPlatform) return;
  const src = resolveModalImage(currentPlatform);
  modalEls.modalImg.src = src || "";
  modalEls.modalImg.alt = currentPlatform.nombre || "";
};

const getClosestDiscountRow = (value, column) => {
  const key = Number(value) || 0;
  const exact = descuentosPorMes[key];
  const exactVal = exact?.[column];
  if (exactVal !== null && exactVal !== undefined && exactVal !== "") return exact;
  const nearest = Object.keys(descuentosPorMes)
    .map((k) => Number(k))
    .filter((n) => Number.isFinite(n) && n <= key)
    .sort((a, b) => b - a)
    .find((n) => {
      const row = descuentosPorMes[n];
      const val = row?.[column];
      return val !== null && val !== undefined && val !== "";
    });
  if (!Number.isFinite(nearest)) return null;
  return descuentosPorMes[nearest] || null;
};

const getDiscountPercent = (platform, value, mode = "months") => {
  const usa =
    platform?.descuento_meses === true ||
    platform?.descuento_meses === "true" ||
    platform?.descuento_meses === "1";
  if (mode === "months" && !usa) return 0;
  if (!isDiscountEnabledForAudience(platform, mode)) return 0;
  const key = Number(value) || 0;
  const col = resolveDiscountColumn(platform, mode);
  const row = getClosestDiscountRow(key, col) || {};
  const pct = Number(row[col]) || 0;
  console.log("[discount] plataforma", platform?.nombre, mode, key, "pct", pct, "map", descuentosPorMes);
  return pct;
};

const calculateFinalPrice = (basePrice, platform, flags, months, qty) => {
  const items =
    flags?.por_pantalla || flags?.por_acceso ? Number(qty) || 1 : 1;
  const monthFactor = flags?.tarjeta_de_regalo ? 1 : Number(months) || 1;
  const itemsDiscount =
    flags?.por_pantalla || flags?.por_acceso
      ? getDiscountPercent(platform, items, "items")
      : 0;
  const monthsDiscount = flags?.tarjeta_de_regalo
    ? 0
    : getDiscountPercent(platform, monthFactor, "months");
  const subtotal = (Number(basePrice) || 0) * items * monthFactor;
  const final =
    subtotal *
    (1 - Math.max(0, itemsDiscount) / 100) *
    (1 - Math.max(0, monthsDiscount) / 100);
  return { subtotal, final, itemsDiscount, monthsDiscount };
};

const setDiscountBadge = (el, pct) => {
  if (!el) return;
  const value = Number(pct) || 0;
  el.textContent = `-${value}%`;
  el.classList.remove("hidden");
  el.classList.toggle("is-zero", value <= 0);
};

const buildButtonLabel = (opcion, flags, _months, platform, _qty = 1) => {
  const isNetflixPlan2 =
    Number(platform?.id_plataforma) === 1 &&
    [4, 5].includes(Number(opcion.id_precio));
  const basePrice = Number(opcion.precio_usd_detal) || 0;
  const final = basePrice;

  if (
    !isNetflixPlan2 &&
    (opcion.completa === true ||
      opcion.completa === "true" ||
      opcion.completa === 1 ||
      opcion.completa === "1")
  ) {
    return { text: `cuenta completa $${final.toFixed(2)}` };
  }
  if (flags.tarjeta_de_regalo) {
    return {
      region: opcion.region || "-",
      text: `${opcion.valor_tarjeta_de_regalo || ""} ${opcion.moneda || ""} $${final.toFixed(2)}`,
    };
  }
  const baseUnit = flags.por_pantalla
    ? "pantalla"
    : flags.por_acceso
    ? "dispositivo"
    : "mes";
  const cantidad =
    baseUnit === "mes"
      ? Number(opcion.duracion) || 1
      : opcion.cantidad || 1;
  const plural = cantidad === 1 ? "" : baseUnit === "mes" ? "es" : "s";
  return { text: `${cantidad} ${baseUnit}${plural} $${final.toFixed(2)}` };
};

const buildCartDetalle = (opcion, flags, cantidad) => {
  if (flags.tarjeta_de_regalo) {
    const region = opcion.region || "-";
    const monto = `${opcion.valor_tarjeta_de_regalo || ""} ${opcion.moneda || ""} $${opcion.precio_usd_detal}`;
    return `Región: ${region} · Monto: ${monto}`;
  }
  const baseUnit = flags.por_pantalla
    ? "pantalla"
    : flags.por_acceso
    ? "dispositivo"
    : "mes";
  const qty =
    baseUnit === "mes"
      ? Number(opcion.duracion) || 1
      : cantidad || opcion.cantidad || 1;
  const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
  return `${qty} ${baseUnit}${plural} $${opcion.precio_usd_detal}`;
};

const updateModalTotal = () => {
  const totalEl = modalEls.modalTotal;
  if (!totalEl) return;
  if (!selectedPrecio) {
    totalEl.textContent = "Total: $0.00";
    return;
  }
  const basePrice = Number(selectedPrecio.precio_usd_detal) || 0;
  const { final } = calculateFinalPrice(
    basePrice,
    currentPlatform,
    currentFlags,
    currentMonths,
    currentQty
  );
  totalEl.textContent = `Total: $${final.toFixed(2)}`;
};

const normalizePositiveInt = (value, fallback = 1) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return Math.max(1, Math.trunc(Number(fallback) || 1));
  return Math.max(1, Math.trunc(parsed));
};

const setMonthsDisplayValue = (value) => {
  const el = modalEls?.qtyMonthsValue;
  if (!el) return;
  const safe = normalizePositiveInt(value, 1);
  if (String(el.tagName || "").toUpperCase() === "INPUT") {
    el.value = String(safe);
    return;
  }
  el.textContent = String(safe);
};

const readMonthsDisplayValue = () => {
  const el = modalEls?.qtyMonthsValue;
  if (!el) return 1;
  if (String(el.tagName || "").toUpperCase() === "INPUT") {
    return normalizePositiveInt(el.value, 1);
  }
  return normalizePositiveInt(el.textContent, 1);
};

const setQtyDisplayValue = (value) => {
  const el = modalEls?.qtyValue;
  if (!el) return;
  const safe = normalizePositiveInt(value, 1);
  if (String(el.tagName || "").toUpperCase() === "INPUT") {
    el.value = String(safe);
    return;
  }
  el.textContent = String(safe);
};

const readQtyDisplayValue = () => {
  const el = modalEls?.qtyValue;
  if (!el) return 1;
  if (String(el.tagName || "").toUpperCase() === "INPUT") {
    return normalizePositiveInt(el.value, 1);
  }
  return normalizePositiveInt(el.textContent, 1);
};

const renderPrecios = (plataformaId, flags) => {
  const {
    modalPrecios,
    modalQtyMonths,
    modalQtyItems,
    monthsDiscount,
    itemsDiscount,
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
  updateModalTotal();
  setQtyDisplayValue(currentQty);
  setMonthsDisplayValue(currentMonths);
  btnMinus.disabled = true;
  btnPlus.disabled = true;
  btnMonthsMinus.disabled = true;
  btnMonthsPlus.disabled = true;
  btnAdd.disabled = true;
  btnAdd.classList.add("btn-disabled-soft");
  modalQtyMonths?.classList.add("modal-qty-disabled");
  modalQtyItems?.classList.add("modal-qty-disabled");
  if (itemsDiscount) {
    const pctItems = getDiscountPercent(currentPlatform, currentQty, "items");
    setDiscountBadge(itemsDiscount, pctItems);
  }
  if (monthsDiscount) {
    const pctMonths = getDiscountPercent(currentPlatform, currentMonths, "months");
    setDiscountBadge(monthsDiscount, pctMonths);
  }
  // Mostrar/ocultar control de meses: solo ocultar en tarjetas de regalo
  const hideMonths = flags.tarjeta_de_regalo === true;
  if (hideMonths) {
    modalQtyMonths?.classList.add("hidden");
    monthsDiscount?.classList.add("hidden");
  } else {
    modalQtyMonths?.classList.remove("hidden");
    monthsDiscount?.classList.remove("hidden");
  }
  const createPlanInfo = (planName, planDesc) => {
    if (!planDesc) return null;
    const wrap = document.createElement("span");
    wrap.className = "plan-info-wrap";
    const infoBtn = document.createElement("button");
    infoBtn.type = "button";
    infoBtn.className = "plan-info-icon";
    infoBtn.textContent = "?";
    infoBtn.setAttribute("aria-label", `Detalle del plan ${planName || ""}`.trim());
      const tooltip = document.createElement("div");
      tooltip.className = "plan-tooltip";
    tooltip.textContent = planDesc;
    wrap.appendChild(infoBtn);
    wrap.appendChild(tooltip);
      infoBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        modalPrecios
          ?.querySelectorAll(".plan-tooltip.is-visible")
          .forEach((el) => {
            if (el !== tooltip) el.classList.remove("is-visible");
          });
        tooltip.classList.toggle("is-visible");
        if (!tooltip.classList.contains("is-visible")) return;
        requestAnimationFrame(() => {
          const plan = wrap.closest(".plan-bloque");
          if (!plan) return;
          const planRect = plan.getBoundingClientRect();
          const iconRect = infoBtn.getBoundingClientRect();
          const top = iconRect.bottom - planRect.top + 2;
          tooltip.style.top = `${top}px`;
          tooltip.style.left = "50%";
          tooltip.style.transform = "translateX(-50%)";
          tooltip.style.right = "auto";
          tooltip.style.bottom = "auto";
        });
      });
    return wrap;
  };
  const opciones = [...(preciosPorPlataforma[plataformaId] || [])]
    .filter((p) => p.precio_usd_detal !== null && p.precio_usd_detal !== undefined)
    .sort((a, b) => (a.id_precio || 0) - (b.id_precio || 0));
  if (!opciones.length) {
    modalPrecios.innerHTML =
      '<p class="sin-plataformas">Sin precios configurados.</p>';
    return;
  }

  const buildSimpleLabel = (opcion) => {
    const basePrice = Number(opcion.precio_usd_detal) || 0;
    const final = basePrice;
    if (opcion.completa === true || opcion.completa === "true" || opcion.completa === 1) {
      return `cuenta completa $${final.toFixed(2)}`;
    }
    const tipo =
      flags.por_pantalla ? "pantalla" : flags.por_acceso ? "acceso" : "cuenta";
    return `${tipo} $${final.toFixed(2)}`;
  };

  const opcionesPorPlan = opciones.reduce((acc, opcion) => {
    const planKey = (opcion.plan || "").trim();
    if (!acc[planKey]) acc[planKey] = [];
    acc[planKey].push(opcion);
    return acc;
  }, {});

  let agrupados = {};
  const isComplete = (op) =>
    op?.completa === true ||
    op?.completa === "true" ||
    op?.completa === 1 ||
    op?.completa === "1";
  if (flags.tarjeta_de_regalo) {
    agrupados = opcionesPorPlan;
  } else {
    agrupados = Object.entries(opcionesPorPlan).reduce((acc, [plan, items]) => {
      const nonComplete = items.find((p) => !isComplete(p));
      if (nonComplete) acc[plan] = [nonComplete];
      return acc;
    }, {});
  }

  if (!flags.tarjeta_de_regalo) {
    const completasMap = new Map();
    opciones.filter(isComplete).forEach((op) => {
      const key = Number(op.id_precio) || op.id_precio;
      if (!completasMap.has(key)) completasMap.set(key, op);
    });
    const completas = Array.from(completasMap.values());
    if (completas.length) {
      agrupados["Cuenta completa"] = completas;
    }
  }

  Object.entries(agrupados).forEach(([plan, items]) => {
    const wrapper = document.createElement("div");
    wrapper.className = "plan-bloque";
    wrapper.setAttribute("role", "button");
    wrapper.tabIndex = 0;
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
    const ms = currentPlatform?.mostrar_stock;
    const showStock = !(
      ms === false ||
      ms === 0 ||
      ms === "0" ||
      ms === "false"
    );
    const onlyComplete = items.length > 0 && items.every(isComplete);
    if (onlyComplete) {
      // Para "Cuenta completa" solo se debe usar el stock de cuentas completas.
      stockPlan = Number(stockByPlatform[`${plataformaId}_completas`]) || 0;
    }
    const planFallback = flags.por_acceso ? "Acceso" : "Perfil";
    const planName = onlyComplete
      ? "Cuenta completa"
      : plan || planFallback;
    let planDesc = items
      .map((op) => (op?.descripcion_plan || "").trim())
      .find((txt) => txt);
    if (onlyComplete) {
      const maxDevices = Number(currentPlatform?.num_max_dispositivos);
      if (Number.isFinite(maxDevices)) {
        const unit = flags.por_acceso ? "accesos" : "perfiles";
        planDesc = `- Incluyen ${maxDevices} ${unit}`;
      }
    }
    const opcionBase = items[0];
    const priceValue = Number(opcionBase?.precio_usd_detal) || 0;
    const appendPriceAndStock = (titleEl, stockLineText) => {
      const priceEl = document.createElement("span");
      priceEl.className = "plan-price-line";
      priceEl.textContent = `$${priceValue.toFixed(2)}`;
      titleEl.appendChild(priceEl);
      if (stockLineText) {
        const stockEl = document.createElement("span");
        stockEl.className = "stock-line";
        stockEl.textContent = stockLineText;
        titleEl.appendChild(stockEl);
      }
    };
    const appendPlanNameHeader = (titleEl) => {
      if (!planName) return;
      const nameEl = document.createElement("span");
      nameEl.className = "plan-name";
      nameEl.textContent = planName;
      titleEl.appendChild(nameEl);
      const infoWrap = createPlanInfo(planName, planDesc);
      if (infoWrap) titleEl.appendChild(infoWrap);
      if (onlyComplete) {
        const tagEl = document.createElement("span");
        tagEl.className = "plan-encargo-tag";
        tagEl.textContent = "Por encargo";
        titleEl.appendChild(tagEl);
      }
      titleEl.appendChild(document.createElement("br"));
    };

    if (showStock) {
      const stockLabel = onlyComplete
        ? "Disponibles"
        : flags.por_acceso
        ? "Accesos disponibles"
        : "Perfiles disponibles";
      const stockLine = `${stockLabel}: ${stockPlan}`;
      const titleEl = document.createElement("p");
      titleEl.className = "plan-titulo";
      appendPlanNameHeader(titleEl);
      appendPriceAndStock(titleEl, stockLine);
      wrapper.appendChild(titleEl);
    } else if (planName) {
      const titleEl = document.createElement("p");
      titleEl.className = "plan-titulo";
      appendPlanNameHeader(titleEl);
      appendPriceAndStock(titleEl, null);
      wrapper.appendChild(titleEl);
    }
    if (!wrapper.querySelector(".plan-titulo")) {
      const titleEl = document.createElement("p");
      titleEl.className = "plan-titulo";
      appendPlanNameHeader(titleEl);
      appendPriceAndStock(titleEl, null);
      wrapper.appendChild(titleEl);
    }

    const handleSelect = () => {
      const isSelected = wrapper.classList.contains("selected");
      modalPrecios
        .querySelectorAll(".plan-bloque.selected")
        .forEach((b) => b.classList.remove("selected"));
      if (isSelected) {
        selectedPrecio = null;
        selectedButtonEl = null;
        currentQty = 1;
        currentMonths = 1;
        setQtyDisplayValue(currentQty);
        setMonthsDisplayValue(currentMonths);
        btnMinus.disabled = true;
        btnPlus.disabled = true;
        btnMonthsMinus.disabled = true;
        btnMonthsPlus.disabled = true;
        btnAdd.disabled = true;
        btnAdd.classList.add("btn-disabled-soft");
        modalQtyMonths?.classList.add("modal-qty-disabled");
        modalQtyItems?.classList.add("modal-qty-disabled");
        updateModalTotal();
        return;
      }
      wrapper.classList.add("selected");
      selectedPrecio = opcionBase;
      selectedButtonEl = wrapper;
      const isMesUnit = !flags.por_pantalla && !flags.por_acceso;
      currentQty = isMesUnit ? 1 : opcionBase?.cantidad || 1;
      setQtyDisplayValue(currentQty);
      setMonthsDisplayValue(currentMonths);
      updateModalTotal();
      btnMinus.disabled = false;
      btnPlus.disabled = false;
      btnAdd.disabled = false;
      btnAdd.classList.remove("btn-disabled-soft");
      modalQtyItems?.classList.remove("modal-qty-disabled");
      if (!hideMonths) {
        btnMonthsMinus.disabled = false;
        btnMonthsPlus.disabled = false;
        modalQtyMonths?.classList.remove("modal-qty-disabled");
        const pctItems = getDiscountPercent(
          currentPlatform,
          currentQty,
          "items"
        );
        const pctMonths = getDiscountPercent(
          currentPlatform,
          currentMonths,
          "months"
        );
        setDiscountBadge(itemsDiscount, pctItems);
        setDiscountBadge(monthsDiscount, pctMonths);
      } else {
        btnMonthsMinus.disabled = true;
        btnMonthsPlus.disabled = true;
        modalQtyMonths?.classList.add("modal-qty-disabled");
        monthsDiscount?.classList.add("hidden");
      }
    };

    wrapper.addEventListener("click", handleSelect);
    wrapper.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter" || ev.key === " ") {
        ev.preventDefault();
        handleSelect();
      }
    });
    modalPrecios.appendChild(wrapper);
  });
};

const updateQtyItems = (delta) => {
  if (!selectedPrecio) return;
  const { monthsDiscount, itemsDiscount } = modalEls;
  currentQty = Math.max(1, currentQty + delta);
  setQtyDisplayValue(currentQty);
  updateModalTotal();
  const pctItems = getDiscountPercent(currentPlatform, currentQty, "items");
  const pctMonths = getDiscountPercent(currentPlatform, currentMonths, "months");
  setDiscountBadge(itemsDiscount, pctItems);
  setDiscountBadge(monthsDiscount, pctMonths);
  if (modalEls.modalQtyMonths?.classList.contains("hidden")) {
    monthsDiscount?.classList.add("hidden");
  }
  // No actualizar labels de botones con cambios de qty
};

const updateQtyMonths = (delta) => {
  if (!selectedPrecio) return;
  const { monthsDiscount, itemsDiscount } = modalEls;
  currentMonths = Math.max(1, Math.round(currentMonths + delta));
  setMonthsDisplayValue(currentMonths);
  updateModalTotal();
  const pctItems = getDiscountPercent(currentPlatform, currentQty, "items");
  const pctMonths = getDiscountPercent(currentPlatform, currentMonths, "months");
  console.log("[discount] update months", { currentMonths, pctMonths });
  setDiscountBadge(itemsDiscount, pctItems);
  setDiscountBadge(monthsDiscount, pctMonths);
  if (modalEls.modalQtyMonths?.classList.contains("hidden")) {
    monthsDiscount?.classList.add("hidden");
  }
  // refresca labels de precios
  // No actualizar labels de botones con cambios de meses
};

const closeModal = () => {
  if (modalTopEl) modalTopEl.scrollTop = 0;
  modalEls.modal.classList.add("hidden");
  document.body.classList.remove("modal-open");
};

const animateAddToCart = () => {
  const sourceEl = selectedButtonEl || modalEls?.btnAdd || null;
  if (!sourceEl) return;
  const cartIcon = document.querySelector(".carrito");
  if (!cartIcon) return;
  const rect = sourceEl.getBoundingClientRect();
  const cartRect = cartIcon.getBoundingClientRect();
  const startX = rect.left + rect.width / 2;
  const startY = rect.top + rect.height / 2;
  const endX = cartRect.left + cartRect.width / 2;
  const endY = cartRect.top + cartRect.height / 2;
  const deltaX = endX - startX;
  const deltaY = endY - startY;
  const arcY = Math.min(-24, deltaY * 0.35 - 26);

  const flyer = document.createElement("div");
  flyer.className = "flying-cart-chip";
  flyer.style.left = `${startX}px`;
  flyer.style.top = `${startY}px`;
  const thumbUrl = String(currentPlatform?.imagen || currentPlatform?.banner || "").trim();
  const thumbEl = document.createElement("span");
  thumbEl.className = "flying-cart-chip__thumb";
  if (thumbUrl) {
    const imgEl = document.createElement("img");
    imgEl.src = thumbUrl;
    imgEl.alt = "";
    imgEl.loading = "eager";
    imgEl.decoding = "async";
    thumbEl.appendChild(imgEl);
  } else {
    const dotEl = document.createElement("span");
    dotEl.className = "flying-cart-chip__dot";
    dotEl.setAttribute("aria-hidden", "true");
    thumbEl.appendChild(dotEl);
  }
  const qtyEl = document.createElement("span");
  qtyEl.className = "flying-cart-chip__qty";
  qtyEl.textContent = `+${Math.max(1, Number(currentQty) || 1)}`;
  flyer.append(thumbEl, qtyEl);
  document.body.appendChild(flyer);

  const animation = flyer.animate(
    [
      { transform: "translate(-50%, -50%) scale(1)", opacity: 0.95 },
      {
        transform: `translate(calc(-50% + ${deltaX * 0.55}px), calc(-50% + ${arcY}px)) scale(0.85)`,
        opacity: 0.9,
        offset: 0.45,
      },
      {
        transform: `translate(calc(-50% + ${deltaX}px), calc(-50% + ${deltaY}px)) scale(0.34)`,
        opacity: 0,
      },
    ],
    { duration: 760, easing: "cubic-bezier(0.2, 0.78, 0.2, 1)" }
  );

  animation.onfinish = () => {
    flyer.remove();
    cartIcon.classList.remove("cart-pop");
    // Forzar reflow para poder repetir la animacion en clicks consecutivos.
    void cartIcon.offsetWidth;
    cartIcon.classList.add("cart-pop");
    window.setTimeout(() => cartIcon.classList.remove("cart-pop"), 420);
  };
};

export const setPrecios = (map) => {
  preciosPorPlataforma = map;
};

export const setDescuentos = (rows = []) => {
  descuentosPorMes = (rows || []).reduce((acc, d) => {
    const meses = Number(d.meses);
    if (Number.isFinite(meses) && meses > 0) {
      const row = { ...d };
      Object.keys(row).forEach((k) => {
        if (/^descuento_/i.test(String(k))) {
          row[k] = Number(row[k]) || 0;
        }
      });
      acc[meses] = row;
    }
    return acc;
  }, {});
};

export const setStockData = (map) => {
  stockByPlatform = map || {};
};

export const setDiscountAudience = (isCliente = true) => {
  discountAudienceIsCliente = !!isCliente;
};

export const initModal = (elements) => {
  modalEls = elements;
  setMonthsDisplayValue(currentMonths);
  modalTopEl = document.querySelector("#platform-modal .modal-top");
  modalScrollHintEl = document.querySelector("#modal-scroll-hint");
  if (modalTopEl && modalScrollHintEl && !scrollHintBound) {
    scrollHintBound = true;
    modalTopEl.addEventListener("scroll", updateScrollHint, { passive: true });
    window.addEventListener("resize", updateScrollHint, { passive: true });
  }
  if (!tooltipDismissBound) {
    tooltipDismissBound = true;
    document.addEventListener("click", (ev) => {
      const target = ev.target;
      if (target?.closest(".plan-info-icon") || target?.closest(".plan-tooltip")) {
        return;
      }
      document
        .querySelectorAll(".plan-tooltip.is-visible")
        .forEach((el) => el.classList.remove("is-visible"));
    });
  }
  const {
    btnMinus,
    btnPlus,
    btnMonthsMinus,
    btnMonthsPlus,
    closeBtn,
    backdrop,
    btnAdd,
    qtyValue,
    qtyMonthsValue,
    monthsDiscount,
    itemsDiscount,
  } = modalEls;
  btnMinus?.addEventListener("click", () => updateQtyItems(-1));
  btnPlus?.addEventListener("click", () => updateQtyItems(1));
  btnMonthsMinus?.addEventListener("click", () => updateQtyMonths(-1));
  btnMonthsPlus?.addEventListener("click", () => updateQtyMonths(1));
  const isMonthsInput = String(qtyMonthsValue?.tagName || "").toUpperCase() === "INPUT";
  if (isMonthsInput && qtyMonthsValue && qtyMonthsValue.dataset.bound !== "1") {
    qtyMonthsValue.dataset.bound = "1";
    const commitMonthsInput = () => {
      currentMonths = readMonthsDisplayValue();
      setMonthsDisplayValue(currentMonths);
      if (!selectedPrecio) return;
      updateModalTotal();
      const pctItems = getDiscountPercent(currentPlatform, currentQty, "items");
      const pctMonths = getDiscountPercent(currentPlatform, currentMonths, "months");
      setDiscountBadge(itemsDiscount, pctItems);
      setDiscountBadge(monthsDiscount, pctMonths);
      if (modalEls.modalQtyMonths?.classList.contains("hidden")) {
        monthsDiscount?.classList.add("hidden");
      }
    };
    qtyMonthsValue.addEventListener("input", () => {
      const raw = String(qtyMonthsValue.value || "");
      const digitsOnly = raw.replace(/[^\d]/g, "");
      if (raw !== digitsOnly) qtyMonthsValue.value = digitsOnly;
      if (!digitsOnly) return;
      commitMonthsInput();
    });
    qtyMonthsValue.addEventListener("blur", commitMonthsInput);
    qtyMonthsValue.addEventListener("change", commitMonthsInput);
    qtyMonthsValue.addEventListener("keydown", (ev) => {
      if (ev.key !== "Enter") return;
      ev.preventDefault();
      commitMonthsInput();
      qtyMonthsValue.blur();
    });
  }
  const isQtyInput = String(qtyValue?.tagName || "").toUpperCase() === "INPUT";
  if (isQtyInput && qtyValue && qtyValue.dataset.bound !== "1") {
    qtyValue.dataset.bound = "1";
    const commitQtyInput = () => {
      currentQty = readQtyDisplayValue();
      setQtyDisplayValue(currentQty);
      if (!selectedPrecio) return;
      updateModalTotal();
      const pctItems = getDiscountPercent(currentPlatform, currentQty, "items");
      const pctMonths = getDiscountPercent(currentPlatform, currentMonths, "months");
      setDiscountBadge(itemsDiscount, pctItems);
      setDiscountBadge(monthsDiscount, pctMonths);
      if (modalEls.modalQtyMonths?.classList.contains("hidden")) {
        monthsDiscount?.classList.add("hidden");
      }
    };
    qtyValue.addEventListener("input", () => {
      const raw = String(qtyValue.value || "");
      const digitsOnly = raw.replace(/[^\d]/g, "");
      if (raw !== digitsOnly) qtyValue.value = digitsOnly;
      if (!digitsOnly) return;
      commitQtyInput();
    });
    qtyValue.addEventListener("blur", commitQtyInput);
    qtyValue.addEventListener("change", commitQtyInput);
    qtyValue.addEventListener("keydown", (ev) => {
      if (ev.key !== "Enter") return;
      ev.preventDefault();
      commitQtyInput();
      qtyValue.blur();
    });
  }
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
    const monthsToSend =
      !currentFlags.por_pantalla && !currentFlags.por_acceso
        ? Number(selectedPrecio.duracion) || 1
        : currentMonths;
    const unitPrice =
      Number(selectedPrecio.precio_usd_detal) ||
      Number(selectedPrecio.precio_usd_mayor) ||
      0;
    const detail = (() => {
      if (currentFlags.tarjeta_de_regalo) {
        const region = selectedPrecio.region || "-";
        const monto = `${selectedPrecio.valor_tarjeta_de_regalo || ""} ${selectedPrecio.moneda || ""} $${unitPrice}`;
        return `Región: ${region} · Monto: ${monto}`;
      }
      const qty = currentQty || 1;
      const meses = monthsToSend || 1;
      const baseUnit = currentFlags.por_pantalla
        ? "pantalla"
        : currentFlags.por_acceso
          ? "dispositivo"
          : "mes";
      const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
      const mesesTxt = baseUnit === "mes" ? ` · ${meses} mes${meses === 1 ? "" : "es"}` : "";
      return `${qty} ${baseUnit}${plural}${mesesTxt} $${unitPrice}`;
    })();

    addToCart({
      open: false,
      refresh: false,
      optimisticItem: {
        id_precio: selectedPrecio.id_precio,
        cantidad: currentQty,
        meses: monthsToSend,
        renovacion: false,
        id_venta: null,
        id_cuenta: null,
        id_perfil: null,
        // detalle visual inmediato
        detalle: detail,
        nombre: currentPlatform.nombre,
        imagen: currentPlatform.imagen,
        plan: selectedPrecio.plan,
      },
    });

    sendCartDelta(selectedPrecio.id_precio, currentQty, monthsToSend, {
      renovacion: false,
      id_venta: null,
    }).finally(() => {
      refreshCartFromServer();
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
  const modalNameMobile = document.querySelector("#modal-name-mobile");
  const modalCategoryMobile = document.querySelector("#modal-category-mobile");
  const modalBadgeMobile = document.querySelector("#modal-badge-mobile");
  const modalTitleMobile = document.querySelector("#modal-title-mobile");
  const {
    id_plataforma,
    nombre,
    categoria,
    imagen,
    banner,
    por_pantalla,
    por_acceso,
    tarjeta_de_regalo,
    entrega_inmediata,
    descuento_meses,
    id_descuento,
    id_descuento_mes,
    id_descuento_cantidad,
    aplica_descuento_mes_detal,
    aplica_descuento_mes_mayor,
    aplica_descuento_cantidad_detal,
    aplica_descuento_cantidad_mayor,
    mostrar_stock,
    num_max_dispositivos,
  } = platform;

  modalName.textContent = nombre;
  if (modalNameMobile) modalNameMobile.textContent = nombre;
  modalCategory.textContent = categoria || "";
  if (modalCategoryMobile) modalCategoryMobile.textContent = categoria || "";
  if (modalTitleMobile) {
    modalTitleMobile.textContent = categoria
      ? `${nombre} · ${categoria}`
      : nombre || "";
  }
  if (modalBadge) {
    const showStock = !(
      mostrar_stock === false ||
      mostrar_stock === 0 ||
      mostrar_stock === "0" ||
      mostrar_stock === "false"
    );
    const entrega =
      entrega_inmediata === true ||
      entrega_inmediata === "true" ||
      entrega_inmediata === "1";
    if (showStock && entrega && (stockByPlatform[id_plataforma] ?? 0) === 0) {
      modalBadge.textContent = "Pronto mas stock";
      modalBadge.className = "modal-badge badge-warning";
    } else if (entrega) {
      modalBadge.textContent = "Entrega inmediata";
      modalBadge.className = "modal-badge badge-green";
    } else {
      modalBadge.textContent = "Por encargo";
      modalBadge.className = "modal-badge badge-gray";
    }
    if (modalBadgeMobile) {
      modalBadgeMobile.textContent = modalBadge.textContent || "";
      modalBadgeMobile.className = modalBadge.className || "modal-badge";
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
    banner,
    num_max_dispositivos,
    descuento_meses:
      isTrueLike(descuento_meses),
    id_descuento: null,
    id_descuento_mes,
    id_descuento_cantidad,
    aplica_descuento_mes_detal: !isFalseLike(aplica_descuento_mes_detal),
    aplica_descuento_mes_mayor: !isFalseLike(aplica_descuento_mes_mayor),
    aplica_descuento_cantidad_detal: !isFalseLike(aplica_descuento_cantidad_detal),
    aplica_descuento_cantidad_mayor: !isFalseLike(aplica_descuento_cantidad_mayor),
    mostrar_stock,
  };
  updateModalImage();
  if (!modalImageListenerBound) {
    modalImageListenerBound = true;
    window.addEventListener("resize", updateModalImage, { passive: true });
  }
  if (descuento_meses !== undefined) {
    currentPlatform.descuento_meses = descuento_meses;
  }
  renderPrecios(id_plataforma, currentFlags);
  modal.classList.remove("hidden");
  document.body.classList.add("modal-open");
  if (modalTopEl) modalTopEl.scrollTop = 0;
  requestAnimationFrame(() => {
    requestAnimationFrame(updateScrollHint);
  });
};
