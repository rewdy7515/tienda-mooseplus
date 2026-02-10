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

const getClosestDiscountRow = (value) => {
  const key = Number(value) || 0;
  const exact = descuentosPorMes[key];
  if (exact) return exact;
  const nearest = Object.keys(descuentosPorMes)
    .map((k) => Number(k))
    .filter((n) => Number.isFinite(n) && n <= key)
    .sort((a, b) => b - a)[0];
  if (!Number.isFinite(nearest)) return null;
  return descuentosPorMes[nearest] || null;
};

const getDiscountPercent = (platform, value, mode = "months") => {
  const usa =
    platform?.descuento_meses === true ||
    platform?.descuento_meses === "true" ||
    platform?.descuento_meses === "1";
  if (!usa) return 0;
  const key = Number(value) || 0;
  const row = getClosestDiscountRow(key) || {};
  const pct =
    mode === "items"
      ? Number(row.descuento_2) || 0
      : Number(row.descuento_1) || 0;
  console.log("[discount] plataforma", platform?.nombre, mode, key, "pct", pct, "map", descuentosPorMes);
  return pct;
};

const buildButtonLabel = (opcion, flags, months, platform, qty = 1) => {
  const isNetflixPlan2 =
    Number(platform?.id_plataforma) === 1 &&
    [4, 5].includes(Number(opcion.id_precio));
  const basePrice = Number(opcion.precio_usd_detal) || 0;
  const discount = flags.por_pantalla || flags.por_acceso
    ? getDiscountPercent(platform, qty, "items")
    : getDiscountPercent(platform, months, "months");
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
  return { text: `${cantidad} ${baseUnit}${plural} $${finalPrecio.toFixed(2)}` };
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
  const useItems = currentFlags.por_pantalla || currentFlags.por_acceso;
  const discount = useItems
    ? getDiscountPercent(currentPlatform, currentQty, "items")
    : getDiscountPercent(currentPlatform, currentMonths, "months");
  const factor = useItems ? (currentQty || 1) : (currentMonths || 1);
  const subtotal = basePrice * factor;
  const final = discount > 0 ? subtotal * (1 - discount / 100) : subtotal;
  totalEl.textContent = `Total: $${final.toFixed(2)}`;
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
  if (itemsDiscount) itemsDiscount.classList.add("hidden");
  // Mostrar/ocultar control de meses: solo ocultar en tarjetas de regalo
  const hideMonths = flags.tarjeta_de_regalo === true;
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

  const buildSimpleLabel = (opcion) => {
    const basePrice = Number(opcion.precio_usd_detal) || 0;
    const discount = flags.por_pantalla || flags.por_acceso
      ? getDiscountPercent(currentPlatform, currentQty, "items")
      : getDiscountPercent(currentPlatform, currentMonths, "months");
    const mesesFactor = currentMonths || 1;
    const precioMeses = basePrice * mesesFactor;
    const finalPrecio = discount > 0 ? precioMeses * (1 - discount / 100) : precioMeses;
    if (opcion.completa === true || opcion.completa === "true" || opcion.completa === 1) {
      return `cuenta completa $${finalPrecio.toFixed(2)}`;
    }
    const tipo =
      flags.por_pantalla ? "pantalla" : flags.por_acceso ? "acceso" : "cuenta";
    return `${tipo} $${finalPrecio.toFixed(2)}`;
  };

  const opcionesPorPlan = opciones.reduce((acc, opcion) => {
    const planKey = (opcion.plan || "").trim();
    if (!acc[planKey]) acc[planKey] = [];
    acc[planKey].push(opcion);
    return acc;
  }, {});

  let agrupados = {};
  if (flags.tarjeta_de_regalo) {
    agrupados = opcionesPorPlan;
  } else {
    agrupados = Object.entries(opcionesPorPlan).reduce((acc, [plan, items]) => {
      const nonComplete = items.find(
        (p) =>
          !(
            p.completa === true ||
            p.completa === "true" ||
            p.completa === 1 ||
            p.completa === "1"
          )
      );
      const complete = items.find(
        (p) =>
          p.completa === true ||
          p.completa === "true" ||
          p.completa === 1 ||
          p.completa === "1"
      );
      const reduced = [];
      if (nonComplete) reduced.push(nonComplete);
      if (complete) reduced.push(complete);
      if (reduced.length) acc[plan] = reduced;
      return acc;
    }, {});
  }

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
    const ms = currentPlatform?.mostrar_stock;
    const showStock = !(
      ms === false ||
      ms === 0 ||
      ms === "0" ||
      ms === "false"
    );
    const isComplete = (op) =>
      op?.completa === true ||
      op?.completa === "true" ||
      op?.completa === 1 ||
      op?.completa === "1";
    const onlyComplete = items.length > 0 && items.every(isComplete);
    let planTitle = onlyComplete ? "Cuenta completa" : plan || "";
    if (showStock) {
      const stockLabel = onlyComplete
        ? "Disponibles"
        : flags.por_acceso
        ? "Accesos disponibles"
        : "Perfiles disponibles";
      const stockLine = `<span class="stock-line">${stockLabel}: ${stockPlan}</span>`;
      planTitle = planTitle ? `${planTitle}<br>${stockLine}` : stockLine;
    }
    const planLabel = `<p class="plan-titulo">${planTitle}</p>`;
    wrapper.innerHTML = planLabel;

    const list = document.createElement("div");
    list.className = "plan-opciones";

    items.forEach((opcion) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "precio-opcion";
      const label = flags.tarjeta_de_regalo
        ? buildButtonLabel(opcion, flags, currentMonths, currentPlatform)
        : { text: buildSimpleLabel(opcion) };
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
          updateModalTotal();
          return;
        }
        btn.classList.add("selected");
        selectedPrecio = opcion;
        selectedButtonEl = btn;
        const isMesUnit = !flags.por_pantalla && !flags.por_acceso;
        currentQty = isMesUnit ? 1 : opcion.cantidad || 1;
        // Mantén el valor de meses seleccionado por el usuario (inicia en 1)
        qtyValue.textContent = currentQty;
        qtyMonthsValue.textContent = currentMonths;
        updateModalTotal();
      btnMinus.disabled = false;
      btnPlus.disabled = false;
      btnAdd.disabled = false;
      modalQtyItems?.classList.remove("modal-qty-disabled");
      if (!hideMonths) {
        btnMonthsMinus.disabled = false;
        btnMonthsPlus.disabled = false;
        modalQtyMonths?.classList.remove("modal-qty-disabled");
          const pct = flags.por_pantalla || flags.por_acceso
            ? getDiscountPercent(currentPlatform, currentQty, "items")
            : getDiscountPercent(currentPlatform, currentMonths, "months");
          if (flags.por_pantalla || flags.por_acceso) {
            if (pct > 0 && itemsDiscount) {
              itemsDiscount.textContent = `-${pct}%`;
              itemsDiscount.classList.remove("hidden");
            } else {
              itemsDiscount?.classList.add("hidden");
            }
          } else {
            if (pct > 0 && monthsDiscount) {
              monthsDiscount.textContent = `-${pct}%`;
              monthsDiscount.classList.remove("hidden");
            } else {
              monthsDiscount?.classList.add("hidden");
            }
          }
        } else {
          btnMonthsMinus.disabled = true;
          btnMonthsPlus.disabled = true;
          modalQtyMonths?.classList.add("modal-qty-disabled");
          monthsDiscount?.classList.add("hidden");
          itemsDiscount?.classList.add("hidden");
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
  const { qtyValue, monthsDiscount, itemsDiscount } = modalEls;
  currentQty = Math.max(1, currentQty + delta);
  qtyValue.textContent = currentQty;
  updateModalTotal();
  const pct = getDiscountPercent(currentPlatform, currentQty, "items");
  const pctLabel = `-${pct}%`;
  if (itemsDiscount) {
    if (pct > 0) {
      itemsDiscount.textContent = pctLabel;
      itemsDiscount.classList.remove("hidden");
    } else {
      itemsDiscount.classList.add("hidden");
    }
  }
  const opciones = preciosPorPlataforma[currentPlatform?.id_plataforma] || [];
  const buttons = modalEls.modalPrecios?.querySelectorAll(".precio-opcion");
  if (buttons) {
    buttons.forEach((btn) => {
      const idPrecio = Number(btn.dataset.idPrecio);
      const opt = opciones.find((o) => o.id_precio === idPrecio);
      if (opt) {
        const label = buildButtonLabel(opt, currentFlags, currentMonths, currentPlatform, currentQty);
        btn.textContent = label.text;
      }
    });
  }
};

const updateQtyMonths = (delta) => {
  if (!selectedPrecio) return;
  const { qtyMonthsValue, monthsDiscount, itemsDiscount } = modalEls;
  currentMonths = Math.max(1, Math.round(currentMonths + delta));
  qtyMonthsValue.textContent = currentMonths;
  updateModalTotal();
  const pct = getDiscountPercent(currentPlatform, currentMonths, "months");
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
        const label = buildButtonLabel(opt, currentFlags, currentMonths, currentPlatform, currentQty);
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
    if (Number.isFinite(meses) && meses > 0) {
      acc[meses] = {
        descuento_1: Number(d.descuento_1) || 0,
        descuento_2: Number(d.descuento_2) || 0,
      };
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
    const monthsToSend =
      !currentFlags.por_pantalla && !currentFlags.por_acceso
        ? Number(selectedPrecio.duracion) || 1
        : currentMonths;
    sendCartDelta(selectedPrecio.id_precio, currentQty, monthsToSend, {
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
    mostrar_stock,
  } = platform;

  modalImg.src = imagen || "";
  modalImg.alt = nombre;
  modalName.textContent = nombre;
  modalCategory.textContent = categoria || "";
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
    mostrar_stock,
  };
  if (descuento_meses !== undefined) {
    currentPlatform.descuento_meses = descuento_meses;
  }
  renderPrecios(id_plataforma, currentFlags);
  modal.classList.remove("hidden");
};
