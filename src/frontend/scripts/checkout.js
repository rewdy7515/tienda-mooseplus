import {
  supabase,
  fetchCart,
  loadCatalog,
  submitCheckout,
  fetchCheckoutDraft,
  updateCartMontos,
  uploadComprobantes,
  fetchP2PRate,
  loadCurrentUser,
} from "./api.js";
import { requireSession, attachLogoHome } from "./session.js";
import { loadPaginaBranding } from "./branding.js";
import { buildNotificationPayload, pickNotificationUserIds } from "./notification-templates.js";
import { TASA_MARKUP } from "./rate-config.js";

requireSession();
attachLogoHome();

const urlParams = new URLSearchParams(window.location.search);
const saldoOrderId = Number(urlParams.get("id_orden"));
const saldoFrom = urlParams.get("from") === "saldo";
let isSaldoCheckout = false;
let saldoOrder = null;
let checkoutOrderId =
  Number.isFinite(saldoOrderId) && saldoOrderId > 0 ? saldoOrderId : null;

const metodoDetalle = document.querySelector("#metodo-detalle");
const metodoSelect = document.querySelector("#metodo-select");
const metodoMenu = document.querySelector("#checkout-metodo-menu");
const metodoButtonsWrap = document.querySelector("#checkout-metodo-buttons");
const btnMetodoContinue = document.querySelector("#btn-metodo-continue");
const checkoutMainContent = document.querySelector("#checkout-main-content");
const btnBackMetodo = document.querySelector("#btn-back-metodo");
const btnAddImage = document.querySelector("#btn-add-image");
const inputFiles = document.querySelector("#input-files");
const filePreview = document.querySelector("#file-preview");
const dropzone = document.querySelector("#dropzone");
const totalEl = document.querySelector("#checkout-total");
const checkoutVerificacionNoteEl = document.querySelector("#checkout-verificacion-note");
const orderTitleEl = document.querySelector("#checkout-order-title");
const btnSendPayment = document.querySelector("#btn-send-payment");
const refInput = document.querySelector("#input-ref");

let metodos = [];
let seleccionado = null;
let totalUsd = 0;
let cartItems = [];
let precios = [];
let plataformas = [];
let descuentos = [];
let cartId = null;
let tasaBs = null;
let precioTierLabel = "";
let userAcceso = null;
let currentUserId = null;
let currentUser = null;
let fixedMontoBs = null;
let fixedMontoUsd = null;
let fixedHora = null;
let fixedFecha = null;
let montoRefreshTimer = null;
let saldoWatcher = null;
let lastSaldoValue = null;
const RATE_SLOT_SECONDS = 2 * 60 * 60;
const METODO_RECARGO_USD_ID = 4;
const METODO_RECARGO_USD_PERCENT = 0.0349;
const METODO_RECARGO_USD_FIJO = 0.49;
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const normalizeReferenceDigits = (value) => String(value || "").replace(/\D/g, "");
const isMetodoVerificacionAutomatica = (metodo) => isTrue(metodo?.verificacion_automatica);
const CHECKOUT_PANEL_ANIM_MS = 260;
const CHECKOUT_PANEL_ANIM_CLASSES = [
  "checkout-panel-animating",
  "checkout-panel-enter-left",
  "checkout-panel-enter-right",
  "checkout-panel-exit-left",
  "checkout-panel-exit-right",
];
const getTotalUsdMostrado = (baseUsd, metodoId) => {
  const montoBase = Number(baseUsd);
  if (!Number.isFinite(montoBase)) return 0;
  if (Number(metodoId) !== METODO_RECARGO_USD_ID) return round2(montoBase);
  return round2(montoBase * (1 + METODO_RECARGO_USD_PERCENT) + METODO_RECARGO_USD_FIJO);
};
let isMetodoMenuAnimating = false;

const getCaracasNow = () => {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(new Date());
  const get = (type) => parts.find((p) => p.type === type)?.value || "";
  return {
    fecha: `${get("year")}-${get("month")}-${get("day")}`,
    hora: `${get("hour")}:${get("minute")}:${get("second")}`,
  };
};

const setCheckoutOrderId = (orderId, { updateUrl = true } = {}) => {
  const parsed = Number(orderId);
  checkoutOrderId = Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  if (!updateUrl || !checkoutOrderId) return;
  try {
    const url = new URL(window.location.href);
    url.searchParams.set("id_orden", String(checkoutOrderId));
    window.history.replaceState({}, "", url.toString());
  } catch (_err) {
    // noop
  }
};

const parseCaracasDate = (fechaStr, horaStr) => {
  if (!fechaStr || !horaStr) return null;
  const fecha = String(fechaStr).trim();
  const hora = String(horaStr).trim();
  if (!fecha || !hora) return null;
  const iso = `${fecha}T${hora}-04:00`;
  const dt = new Date(iso);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const parseCaracasClock = (horaStr) => {
  const [h = "0", m = "0", s = "0"] = String(horaStr || "00:00:00").split(":");
  const hh = Number(h);
  const mm = Number(m);
  const ss = Number(s);
  if (!Number.isFinite(hh) || !Number.isFinite(mm) || !Number.isFinite(ss)) return null;
  return { hh, mm, ss };
};

const getCurrentRateWindowStartDate = () => {
  const nowVz = getCaracasNow();
  const nowDt = parseCaracasDate(nowVz.fecha, nowVz.hora);
  const clock = parseCaracasClock(nowVz.hora);
  if (!nowDt || !clock) return null;
  const secSinceMidnight = clock.hh * 3600 + clock.mm * 60 + clock.ss;
  const elapsedInWindow = secSinceMidnight % RATE_SLOT_SECONDS;
  return new Date(nowDt.getTime() - elapsedInWindow * 1000);
};

const shouldRefreshMonto = (fechaStr, horaStr) => {
  const dt = parseCaracasDate(fechaStr, horaStr);
  if (!dt) return true;
  const windowStart = getCurrentRateWindowStartDate();
  if (!windowStart) return true;
  return dt.getTime() < windowStart.getTime();
};

const getNextRateUpdateDate = () => {
  const nowVz = getCaracasNow();
  const nowDt = parseCaracasDate(nowVz.fecha, nowVz.hora);
  const clock = parseCaracasClock(nowVz.hora);
  if (!nowDt || !clock) return null;
  const secSinceMidnight = clock.hh * 3600 + clock.mm * 60 + clock.ss;
  const mod = secSinceMidnight % RATE_SLOT_SECONDS;
  const secToNextSlot = mod === 0 ? RATE_SLOT_SECONDS : RATE_SLOT_SECONDS - mod;
  return new Date(nowDt.getTime() + secToNextSlot * 1000);
};

const formatCaracasHour = (date) => {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("es-VE", {
    timeZone: "America/Caracas",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  }).format(date);
};

const refreshRateBs = async () => {
  const rawRate = await fetchP2PRate();
  if (!Number.isFinite(rawRate)) return null;
  return Math.round(rawRate * TASA_MARKUP * 100) / 100;
};

const scheduleMontoRefresh = (totalUsdVal) => {
  if (montoRefreshTimer) {
    clearTimeout(montoRefreshTimer);
    montoRefreshTimer = null;
  }
  const nextRateUpdate = getNextRateUpdateDate();
  if (!nextRateUpdate) return;
  const waitMs = Math.max(1000, nextRateUpdate.getTime() - Date.now());
  if (waitMs <= 0) return;
  montoRefreshTimer = setTimeout(async () => {
    try {
      const cartData = await fetchCart();
      const refreshedRate = await refreshRateBs();
      if (Number.isFinite(refreshedRate)) {
        tasaBs = refreshedRate;
      } else {
        scheduleMontoRefresh(totalUsdVal);
        return;
      }
      await syncCartMontosIfNeeded(
        cartData,
        totalUsdVal,
        Number.isFinite(tasaBs) ? tasaBs : null
      );
      renderTotal();
    } catch (err) {
      console.warn("checkout monto refresh error", err);
      scheduleMontoRefresh(totalUsdVal);
    }
  }, waitMs + 600);
};

const startSaldoWatcher = () => {
  if (saldoWatcher) {
    clearInterval(saldoWatcher);
    saldoWatcher = null;
  }
  saldoWatcher = setInterval(async () => {
    try {
      const user = await loadCurrentUser();
      const saldoVal = Number(user?.saldo);
      const saldoNum = Number.isFinite(saldoVal) ? saldoVal : 0;
      if (lastSaldoValue === null) {
        lastSaldoValue = saldoNum;
        return;
      }
      if (Math.abs(saldoNum - lastSaldoValue) >= 0.01) {
        window.location.reload();
      }
    } catch (_err) {
      // noop
    }
  }, 8000);
};

const syncCartMontosIfNeeded = async (cartData, totalUsdVal, tasaVal) => {
  if (!cartId || !Number.isFinite(totalUsdVal)) return;
  const storedUsd = Number(cartData?.monto_usd);
  const storedBs = Number(cartData?.monto_bs);
  const storedHora = cartData?.hora ?? null;
  const storedFecha = cartData?.fecha ?? null;
  const sameUsd = Number.isFinite(storedUsd) && Math.abs(storedUsd - totalUsdVal) < 0.01;
  const montoBsCalc = Number.isFinite(tasaVal)
    ? Math.round(totalUsdVal * tasaVal * 100) / 100
    : null;

  if (!sameUsd) {
    fixedMontoUsd = totalUsdVal;
    fixedMontoBs = Number.isFinite(montoBsCalc) ? montoBsCalc : null;
    const nowVz = getCaracasNow();
    fixedFecha = nowVz.fecha;
    fixedHora = nowVz.hora;
    try {
      const resp = await updateCartMontos(totalUsdVal, tasaVal);
      if (resp?.error) console.warn("checkout cart monto update error", resp.error);
    } catch (err) {
      console.warn("checkout cart monto update error", err);
    }
    scheduleMontoRefresh(totalUsdVal);
    return;
  }

  fixedMontoUsd = storedUsd;
  fixedMontoBs = Number.isFinite(storedBs) ? storedBs : montoBsCalc;
  fixedHora = storedHora;
  fixedFecha = storedFecha;

  if (shouldRefreshMonto(fixedFecha, fixedHora) && Number.isFinite(montoBsCalc)) {
    fixedMontoBs = montoBsCalc;
    const nowVz = getCaracasNow();
    fixedFecha = nowVz.fecha;
    fixedHora = nowVz.hora;
    try {
      const resp = await updateCartMontos(totalUsdVal, tasaVal);
      if (resp?.error) console.warn("checkout cart monto update error", resp.error);
    } catch (err) {
      console.warn("checkout cart monto update error", err);
    }
    scheduleMontoRefresh(totalUsdVal);
  } else {
    scheduleMontoRefresh(totalUsdVal);
  }
};

const renderDetalle = () => {
  if (!metodoDetalle) return;
  if (seleccionado === null) {
    metodoDetalle.innerHTML = "";
    checkoutVerificacionNoteEl?.classList.add("hidden");
    return;
  }
  const m = metodos[seleccionado];
  const isMetodoBs = Number(m.id_metodo_de_pago ?? m.id) === 1;
  const nombreLabel = isMetodoBs ? "Banco" : "Nombre";
  const campos = [
    { label: nombreLabel, valor: m.nombre, copy: false },
    { label: "Correo", valor: m.correo, copy: true },
    { label: "ID", valor: m.id, copy: true },
    {
      label: "Cédula",
      valor: m.cedula,
      copy: true,
      copyValue: normalizeReferenceDigits(m.cedula),
    },
    {
      label: "Teléfono",
      valor: m.telefono,
      copy: true,
      copyValue: normalizeReferenceDigits(m.telefono),
    },
  ].filter((c) => c.valor !== null && c.valor !== undefined && c.valor !== "");

  const detalleHtml = campos
    .map((c) => {
      const valueToCopy = c.copy ? c.copyValue ?? c.valor : "";
      const safeVal = String(valueToCopy).replace(/"/g, "&quot;");
      const copyIcon = c.copy
        ? `<img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/copiar-portapapeles.png" alt="Copiar" class="copy-field-icon" data-copy="${safeVal}" style="width:14px; height:14px; margin-left:6px; cursor:pointer;" />`
        : "";
      return `<p><strong>${c.label}:</strong> <span>${c.valor}</span>${copyIcon}</p>`;
    })
    .join("");

  if (checkoutVerificacionNoteEl) {
    checkoutVerificacionNoteEl.classList.toggle(
      "hidden",
      !isMetodoVerificacionAutomatica(m)
    );
  }
  metodoDetalle.innerHTML =
    detalleHtml +
    (isMetodoBs
      ? `<button type="button" class="btn-primary copy-detalle-btn" style="margin-top:8px; display:flex; align-items:center; justify-content:center; gap:8px; width:100%;">
          <span>Copiar al portapapeles</span>
          <img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/copiar-portapapeles.png" alt="Copiar" style="width:18px; height:18px; filter: brightness(0) invert(1);" />
        </button>`
      : "");

  const imageUrl = String(m.imagen || "").trim();
  if (imageUrl) {
    const logoWrap = document.createElement("div");
    logoWrap.className = "metodo-detalle-logo-wrap";
    const logoImg = document.createElement("img");
    logoImg.className = "metodo-detalle-logo";
    logoImg.src = imageUrl;
    logoImg.alt = m.nombre ? `Logo ${m.nombre}` : "Logo metodo de pago";
    logoImg.loading = "lazy";
    logoImg.decoding = "async";
    logoWrap.appendChild(logoImg);
    metodoDetalle.prepend(logoWrap);
  }

  if (isMetodoBs) {
    const btnCopy = metodoDetalle.querySelector(".copy-detalle-btn");
    btnCopy?.addEventListener("click", async () => {
      const text = campos.map((c) => `${c.copyValue ?? c.valor}`).join("\n");
      try {
        if (navigator?.clipboard?.writeText) {
          await navigator.clipboard.writeText(text);
        } else {
          const textarea = document.createElement("textarea");
          textarea.value = text;
          document.body.appendChild(textarea);
          textarea.select();
          document.execCommand("copy");
          document.body.removeChild(textarea);
        }
        btnCopy.textContent = "Copiado!";
        setTimeout(() => (btnCopy.textContent = "Copiar al portapapeles"), 1500);
      } catch (err) {
        console.error("copy detalle error", err);
      }
    });
  }

  // Copy individual fields (correo, id)
  metodoDetalle.querySelectorAll(".copy-field-icon").forEach((icon) => {
    icon.addEventListener("click", async () => {
      const val = icon.dataset.copy || "";
      const decoded = val.replace(/&quot;/g, '"');
      try {
        if (navigator?.clipboard?.writeText) {
          await navigator.clipboard.writeText(decoded);
        } else {
          const textarea = document.createElement("textarea");
          textarea.value = decoded;
          document.body.appendChild(textarea);
          textarea.select();
          document.execCommand("copy");
          document.body.removeChild(textarea);
        }
        icon.style.opacity = "0.6";
        setTimeout(() => {
          icon.style.opacity = "1";
        }, 800);
      } catch (err) {
        console.error("copy field error", err);
      }
    });
  });
};

const populateSelect = (defaultIdx = null) => {
  if (!metodoSelect) return;
  metodoSelect.innerHTML = '<option value="">Seleccione un método</option>';
  metodos
    .map((m, idx) => ({ m, idx }))
    .forEach(({ m, idx }) => {
    const opt = document.createElement("option");
    opt.value = String(idx);
    opt.textContent = m.nombre || `Método ${idx + 1}`;
    metodoSelect.appendChild(opt);
  });
  const selIdx = defaultIdx !== null ? defaultIdx : seleccionado;
  if (selIdx !== null && selIdx >= 0) {
    metodoSelect.value = String(selIdx);
  } else {
    metodoSelect.value = "";
  }
};

const syncMetodoButtonsState = () => {
  if (!metodoButtonsWrap) return;
  metodoButtonsWrap.querySelectorAll(".checkout-metodo-btn").forEach((btn) => {
    const idx = Number(btn.dataset.idx);
    btn.classList.toggle("is-active", Number.isFinite(idx) && idx === seleccionado);
  });
};

const updateMetodoContinueState = () => {
  if (!btnMetodoContinue) return;
  btnMetodoContinue.disabled = !(Number.isInteger(seleccionado) && seleccionado >= 0);
};

const clearCheckoutPanelAnimation = (panel) => {
  if (!panel) return;
  panel.classList.remove(...CHECKOUT_PANEL_ANIM_CLASSES);
};

const runCheckoutPanelAnimation = (panel, animationClass) =>
  new Promise((resolve) => {
    if (!panel) {
      resolve();
      return;
    }
    clearCheckoutPanelAnimation(panel);
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      panel.removeEventListener("animationend", onEnd);
      clearTimeout(fallbackTimer);
      clearCheckoutPanelAnimation(panel);
      resolve();
    };
    const onEnd = () => finish();
    const fallbackTimer = setTimeout(finish, CHECKOUT_PANEL_ANIM_MS + 120);
    panel.addEventListener("animationend", onEnd, { once: true });
    void panel.offsetWidth;
    panel.classList.add("checkout-panel-animating", animationClass);
  });

const showMetodoMenu = async (show, { animate = false } = {}) => {
  if (!metodoMenu || !checkoutMainContent) return;
  const menuVisible = !metodoMenu.classList.contains("hidden");
  if ((show && menuVisible) || (!show && !menuVisible)) return;

  const reduceMotion = window?.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;
  if (!animate || reduceMotion || isMetodoMenuAnimating) {
    metodoMenu.classList.toggle("hidden", !show);
    checkoutMainContent.classList.toggle("hidden", !!show);
    clearCheckoutPanelAnimation(metodoMenu);
    clearCheckoutPanelAnimation(checkoutMainContent);
    return;
  }

  isMetodoMenuAnimating = true;
  try {
    if (show) {
      if (!checkoutMainContent.classList.contains("hidden")) {
        await runCheckoutPanelAnimation(checkoutMainContent, "checkout-panel-exit-right");
      }
      checkoutMainContent.classList.add("hidden");
      metodoMenu.classList.remove("hidden");
      await runCheckoutPanelAnimation(metodoMenu, "checkout-panel-enter-left");
    } else {
      if (!metodoMenu.classList.contains("hidden")) {
        await runCheckoutPanelAnimation(metodoMenu, "checkout-panel-exit-left");
      }
      metodoMenu.classList.add("hidden");
      checkoutMainContent.classList.remove("hidden");
      await runCheckoutPanelAnimation(checkoutMainContent, "checkout-panel-enter-right");
    }
  } finally {
    isMetodoMenuAnimating = false;
  }
};

const renderMetodoButtons = () => {
  if (!metodoButtonsWrap) return;
  metodoButtonsWrap.innerHTML = "";
  if (!Array.isArray(metodos) || !metodos.length) {
    metodoButtonsWrap.innerHTML =
      '<p class="checkout-metodo-empty">No hay métodos de pago disponibles.</p>';
    updateSelection(null);
    return;
  }

  const createMetodoButton = (m, idx) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "checkout-metodo-btn";
    btn.dataset.idx = String(idx);
    const metodoName = String(m.nombre || `Método ${idx + 1}`).trim();
    btn.setAttribute("aria-label", metodoName || `Método ${idx + 1}`);
    btn.title = metodoName || `Método ${idx + 1}`;

    const img = document.createElement("img");
    img.className = "checkout-metodo-btn-image";
    img.alt = metodoName || `Método ${idx + 1}`;
    img.loading = "lazy";
    img.decoding = "async";

    const imageUrl = String(m.imagen || "").trim();
    if (imageUrl) {
      img.src = imageUrl;
      btn.appendChild(img);
    } else {
      const fallback = document.createElement("span");
      fallback.className = "checkout-metodo-btn-image-fallback";
      fallback.setAttribute("aria-hidden", "true");
      btn.appendChild(fallback);
    }
    btn.addEventListener("click", () => {
      updateSelection(idx);
    });
    return btn;
  };

  const appendMetodoSection = (title, rows = [], sectionClass = "") => {
    if (!rows.length) return;
    const section = document.createElement("section");
    section.className = "checkout-metodo-section";
    if (sectionClass) section.classList.add(sectionClass);

    const titleEl = document.createElement("p");
    titleEl.className = "checkout-metodo-section-title";
    titleEl.textContent = title;
    section.appendChild(titleEl);

    const grid = document.createElement("div");
    grid.className = "checkout-metodo-section-grid";
    rows.forEach(({ m, idx }) => {
      grid.appendChild(createMetodoButton(m, idx));
    });
    section.appendChild(grid);
    metodoButtonsWrap.appendChild(section);
  };

  const rows = metodos.map((m, idx) => ({ m, idx }));
  const autoRows = rows.filter(({ m }) => isMetodoVerificacionAutomatica(m));
  const manualRows = rows.filter(({ m }) => !isMetodoVerificacionAutomatica(m));

  appendMetodoSection("Verificación automática", autoRows, "is-auto");
  appendMetodoSection("Verificación manual", manualRows, "is-manual");

  syncMetodoButtonsState();
  updateMetodoContinueState();
};

const updateSelection = (idx) => {
  if (idx === null || idx < 0 || idx >= metodos.length) {
    seleccionado = null;
  } else {
    seleccionado = idx;
  }
  renderDetalle();
  if (metodoSelect) {
    metodoSelect.value = seleccionado !== null ? String(seleccionado) : "";
  }
  syncMetodoButtonsState();
  updateMetodoContinueState();
  renderTotal();
  if (
    seleccionado !== null &&
    checkoutMainContent &&
    !checkoutMainContent.classList.contains("hidden")
  ) {
    persistMetodoSeleccionadoEnOrden().catch((err) => {
      console.warn("checkout persist metodo on selection change error", err);
    });
  }
};

metodoSelect?.addEventListener("change", (e) => {
  const idx = Number(e.target.value);
  if (Number.isNaN(idx)) {
    updateSelection(null);
  } else {
    updateSelection(idx);
  }
});

btnMetodoContinue?.addEventListener("click", async () => {
  if (seleccionado === null) {
    alert("Selecciona un método de pago para continuar.");
    return;
  }
  try {
    const persisted = await persistMetodoSeleccionadoEnOrden({ force: true });
    if (!persisted?.ok) {
      alert("No se pudo guardar el método de pago en la orden. Intenta de nuevo.");
      return;
    }
    await showMetodoMenu(false, { animate: true });
  } catch (err) {
    console.error("checkout persist metodo before continue error", err);
    alert("No se pudo guardar el método de pago en la orden. Intenta de nuevo.");
  }
});

btnBackMetodo?.addEventListener("click", async () => {
  await showMetodoMenu(true, { animate: true });
});

btnAddImage?.addEventListener("click", () => {
  inputFiles?.click();
});

inputFiles?.addEventListener("change", () => {
  if (!filePreview) return;
  const files = Array.from(inputFiles.files || []).filter((f) =>
    f.type?.startsWith("image/")
  );
  if (!files.length) {
    filePreview.innerHTML = "";
    inputFiles.value = "";
    return;
  }
  const file = files[0];
  const reader = new FileReader();
  reader.onload = () => {
    filePreview.innerHTML = `<img src="${reader.result}" alt="${file.name}" />`;
  };
  reader.readAsDataURL(file);
  // keep only one file
  const dt = new DataTransfer();
  dt.items.add(file);
  inputFiles.files = dt.files;
});

// Drag and drop visual feedback
["dragenter", "dragover"].forEach((eventName) => {
  dropzone?.addEventListener(eventName, (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropzone.classList.add("drag-over");
  });
});

["dragleave", "drop"].forEach((eventName) => {
  dropzone?.addEventListener(eventName, (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropzone.classList.remove("drag-over");
  });
});

dropzone?.addEventListener("drop", (e) => {
  e.preventDefault();
  const files = Array.from(e.dataTransfer?.files || []).filter((f) =>
    f.type?.startsWith("image/")
  );
  if (!files.length) {
    dropzone.classList.remove("drag-over");
    return;
  }
  const file = files[0];
  const dt = new DataTransfer();
  dt.items.add(file);
  inputFiles.files = dt.files;
  filePreview.innerHTML = "";
  const reader = new FileReader();
  reader.onload = () => {
    filePreview.innerHTML = `<img src="${reader.result}" alt="${file.name}" />`;
  };
  reader.readAsDataURL(file);
  dropzone.classList.remove("drag-over");
});

const renderTotal = () => {
  if (!totalEl) return;
  if (orderTitleEl) {
    if (checkoutOrderId) {
      orderTitleEl.textContent = `N de orden: #${checkoutOrderId}`;
      orderTitleEl.style.display = "block";
    } else {
      orderTitleEl.textContent = "";
      orderTitleEl.style.display = "none";
    }
  }
  const metodo = seleccionado !== null ? metodos[seleccionado] : null;
  const metodoId = metodo?.id_metodo_de_pago ?? metodo?.id;
  const onlyMetodoBs =
    Array.isArray(metodos) &&
    metodos.length > 0 &&
    metodos.every((m) => Number(m?.id_metodo_de_pago ?? m?.id) === 1);
  const isBs = (metodo && Number(metodoId) === 1) || (!metodo && onlyMetodoBs);
  const tasaVal = Number.isFinite(tasaBs) ? tasaBs : null;
  const totalUsdMostrado = getTotalUsdMostrado(totalUsd, metodoId);
  const isMetodoUsdt = Number(metodoId) === 2;
  const totalUsdText = isMetodoUsdt
    ? `${totalUsdMostrado.toFixed(2)} USDT`
    : `$${totalUsdMostrado.toFixed(2)}`;
  const usdLabel = Number(metodoId) === METODO_RECARGO_USD_ID ? "Monto Paypal" : "Total";
  const lineUsd = `${usdLabel}: ${totalUsdText} ${
    precioTierLabel ? `(${precioTierLabel})` : ""
  }`;
  let montoBsView = null;
  let countdownLine = "";
  if (isBs) {
    if (Number.isFinite(fixedMontoBs) && fixedMontoBs > 0) {
      montoBsView = fixedMontoBs;
    } else if (tasaVal) {
      montoBsView = round2(totalUsd * tasaVal);
    }
    const nextRateUpdate = getNextRateUpdateDate();
    if (nextRateUpdate) {
      countdownLine = `<div class="checkout-countdown">Proxima actualización de tasa a las ${formatCaracasHour(nextRateUpdate)} hora Venezuela</div>`;
    }
  }
  const lineBs = isBs && Number.isFinite(montoBsView)
    ? `<div>Bs. ${montoBsView.toFixed(2)}</div>`
    : "";
  totalEl.innerHTML = `<div>${lineUsd}</div>${lineBs}${countdownLine}`;
};

const verifyPagoMovil = async () => {
  const metodo = seleccionado !== null ? metodos[seleccionado] : null;
  const metodoId = Number(metodo?.id_metodo_de_pago ?? metodo?.id);
  const isMetodoBs = metodoId === 1;
  if (!isMetodoBs) {
    alert("Selecciona el método de pago 1 para verificar.");
    return { ok: false };
  }
  const refRaw = String(refInput?.value || "").trim();
  const refDigits = normalizeReferenceDigits(refRaw);
  if (!refRaw) {
    alert("Ingresa la referencia.");
    refInput.classList.add("input-error");
    return { ok: false };
  }
  if (refDigits.length < 4) {
    alert("La referencia debe tener al menos 4 dígitos.");
    refInput.classList.add("input-error");
    return { ok: false };
  }
  if (!Number.isFinite(tasaBs)) {
    alert("No se pudo obtener la tasa.");
    return { ok: false };
  }

  const refLast4 = refDigits.slice(-4);
  let montoBs = Math.round(totalUsd * tasaBs * 100) / 100;
  let cartMontoBs = null;
  let cartTasaBs = null;
  try {
    const cartData = await fetchCart();
    cartId = cartData.id_carrito || cartId;
    if (Number.isFinite(cartData?.monto_bs)) {
      montoBs = Number(cartData.monto_bs);
      cartMontoBs = montoBs;
    } else if (Number.isFinite(fixedMontoBs)) {
      montoBs = fixedMontoBs;
      cartMontoBs = fixedMontoBs;
    }
    if (Number.isFinite(cartData?.tasa_bs)) {
      cartTasaBs = Number(cartData.tasa_bs);
    } else if (Number.isFinite(tasaBs)) {
      cartTasaBs = tasaBs;
    }
  } catch (err) {
    console.warn("[pago movil] cart monto fetch error", err);
  }
  if (!Number.isFinite(cartMontoBs) || !Number.isFinite(cartTasaBs)) {
    alert("No se pudo obtener el monto o la tasa del carrito.");
    return { ok: false };
  }
  console.log("[pago movil] input", { refDigits, refLast4, montoBs, tasaBs, totalUsd, cartId });

  const resp = await supabase
    .from("pagomoviles")
    .select("id, referencia, texto, monto_bs, saldo_acreditado")
    .or("saldo_acreditado.is.null,saldo_acreditado.eq.false")
    .order("id", { ascending: false });
  const pagos = resp.data || [];
  const pagoErr = resp.error;
  if (pagoErr) throw pagoErr;
  console.log("[pago movil] candidatos", pagos);

  const montoNum = (val) => {
    if (val == null) return null;
    const raw = String(val).trim();
    let clean = raw;
    if (raw.includes(".") && raw.includes(",")) {
      // Assume dot as thousands, comma as decimal
      clean = raw.replace(/\./g, "").replace(",", ".");
    } else if (raw.includes(",")) {
      // Comma as decimal
      clean = raw.replace(",", ".");
    }
    const num = Number(clean);
    return Number.isFinite(num) ? num : null;
  };
  const extractRefMatches = (text) => {
    const raw = String(text || "");
    return raw.match(/\d{4,}/g) || [];
  };
  const match = (pagos || []).find((p) => {
    const textoRefs = extractRefMatches(p.texto || "");
    const pagoMonto = montoNum(p.monto_bs);
    console.log("[pago movil] eval", {
      id: p.id,
      textoRefs,
      pagoMonto,
      montoBs,
      diff: Number.isFinite(pagoMonto) ? Math.abs(pagoMonto - montoBs) : null,
      saldo_acreditado: p.saldo_acreditado,
    });
    const refMatch = textoRefs.some((n) => n.slice(-4) === refLast4);
    if (!refMatch) return false;
    return Number.isFinite(pagoMonto);
  });
  if (!match) {
    alert("Pago no encontrado");
    return { ok: false };
  }
  if (match.saldo_acreditado === true) {
    alert("Operación fallida: pago anteriormente registrado");
    return { ok: false };
  }

  const sessionUserId = currentUserId || requireSession();
  const pagoMonto = montoNum(match.monto_bs);
  if (!Number.isFinite(pagoMonto)) {
    alert("Pago no encontrado");
    return { ok: false };
  }

  if (sessionUserId) {
    const textoRefs = extractRefMatches(match.texto || "");
    const refMatch = textoRefs.find((n) => n.slice(-4) === refLast4);
    const updates = {
      saldo_acreditado_a: sessionUserId,
      saldo_acreditado: true,
      ...(refMatch ? { referencia: refMatch } : {}),
    };
    await supabase.from("pagomoviles").update(updates).eq("id", match.id);
  }

  const diff = Number((pagoMonto - cartMontoBs).toFixed(2));
  if (diff === 0) {
    alert("Pago verificado. Procesando pedido...");
    return { ok: true };
  }

  let saldoUsd = null;
  if (Number.isFinite(cartTasaBs) && cartTasaBs) {
    if (diff > 0) {
      saldoUsd = Number(((pagoMonto - cartMontoBs) / cartTasaBs).toFixed(2));
    } else {
      saldoUsd = Number((pagoMonto / cartTasaBs).toFixed(2));
    }
  }
  if (Number.isFinite(saldoUsd) && sessionUserId) {
    const { data: userSaldo, error: saldoErr } = await supabase
      .from("usuarios")
      .select("saldo")
      .eq("id_usuario", sessionUserId)
      .maybeSingle();
    if (!saldoErr) {
      const saldoActual = Number(userSaldo?.saldo) || 0;
      const nuevoSaldo = Math.round((saldoActual + saldoUsd) * 100) / 100;
      await supabase.from("usuarios").update({ saldo: nuevoSaldo }).eq("id_usuario", sessionUserId);
    }
  }

  if (diff > 0) {
    alert("Pago verificado. Saldo restante acreditado al perfil");
    return { ok: true };
  }

  alert("Pago verificado menor al del carrito. Saldo acreditado a su perfil");
  return { ok: false };
};

const calcularTotalRpc = async (id_carrito) => {
  if (!id_carrito) return null;
  try {
    const { data, error } = await supabase.rpc("calcular_total_carrito", { p_id_carrito: id_carrito });
    if (error) {
      console.error("rpc calcular_total_carrito error", error);
      return null;
    }
    const total = Number(data);
    return Number.isFinite(total) ? total : null;
  } catch (err) {
    console.error("rpc calcular_total_carrito catch", err);
    return null;
  }
};

const calcularTotalTier = (items = [], preciosMap = {}, acceso = null) => {
  if (!items.length) return { total: 0, label: "" };
  const useMayor = acceso === false;
  const label = useMayor ? "Precio mayor" : "Precio detal";
  const total = items.reduce((sum, it) => {
    const price = preciosMap[it.id_precio] || {};
    const unit = useMayor
      ? Number(price.precio_usd_mayor) || Number(price.precio_usd_detal) || 0
      : Number(price.precio_usd_detal) || 0;
    const qty = Number(it.cantidad) || 0;
    const meses = Number(it.meses) || 1;
    return sum + unit * qty * meses;
  }, 0);
  return { total, label };
};

async function init() {
  loadPaginaBranding({ logoSelectors: [".logo"], applyFavicon: true }).catch((err) => {
    console.warn("checkout branding load error", err);
  });
  try {
    const [metodosResp, tasaResp, user] = await Promise.all([
      supabase
        .from("metodos_de_pago")
        .select(
          "id_metodo_de_pago, nombre, imagen, correo, id, cedula, telefono, verificacion_automatica"
        ),
      fetchP2PRate(),
      loadCurrentUser(),
    ]);
    if (metodosResp.error) throw metodosResp.error;
    metodos = metodosResp.data || [];
    currentUser = user || null;
    tasaBs = Number.isFinite(tasaResp)
      ? Math.round(tasaResp * TASA_MARKUP * 100) / 100
      : null;
    userAcceso = currentUser?.acceso_cliente;
    currentUserId = currentUser?.id_usuario || null;
    const initialSaldo = Number(currentUser?.saldo);
    lastSaldoValue = Number.isFinite(initialSaldo) ? initialSaldo : 0;

    if (saldoFrom && Number.isFinite(saldoOrderId)) {
      const { data: ordenData, error: ordenErr } = await supabase
        .from("ordenes")
        .select("id_orden, id_carrito, total, tasa_bs, monto_bs, hora_orden, fecha, en_espera")
        .eq("id_orden", saldoOrderId)
        .maybeSingle();
      if (!ordenErr && ordenData && !ordenData.id_carrito) {
        isSaldoCheckout = true;
        saldoOrder = ordenData;
        if (ordenData?.id_orden) {
          setCheckoutOrderId(ordenData.id_orden, { updateUrl: false });
        }
        if (Number.isFinite(Number(ordenData.tasa_bs))) {
          tasaBs = Number(ordenData.tasa_bs);
        }
        totalUsd = Number.isFinite(Number(ordenData.total)) ? Number(ordenData.total) : 0;
        precioTierLabel = "";
        fixedMontoUsd = totalUsd;
        fixedMontoBs = Number.isFinite(Number(ordenData.monto_bs))
          ? Number(ordenData.monto_bs)
          : null;
        fixedHora = ordenData?.hora_orden ?? null;
        fixedFecha = ordenData?.fecha ?? null;
      }
    }

    if (!isSaldoCheckout) {
      const [cartData, catalog] = await Promise.all([fetchCart(), loadCatalog()]);
      cartItems = cartData.items || [];
      cartId = cartData.id_carrito || null;
      if (!cartId) {
        window.location.href = "cart.html";
        return;
      }
      precios = catalog.precios;
      plataformas = catalog.plataformas;
      descuentos = catalog.descuentos || [];
      const useSaldo = isTrue(cartData?.usa_saldo);
      const montoUsdRaw = Number(cartData?.monto_usd);
      const montoFinalRaw = Number(cartData?.monto_final);
      totalUsd =
        useSaldo && Number.isFinite(montoFinalRaw)
          ? Number(montoFinalRaw)
          : Number.isFinite(montoUsdRaw)
          ? Number(montoUsdRaw)
          : 0;
      precioTierLabel = "";
      fixedMontoUsd = Number.isFinite(montoUsdRaw)
        ? Number(montoUsdRaw)
        : totalUsd;
      fixedMontoBs = Number.isFinite(Number(cartData?.monto_bs))
        ? Number(cartData.monto_bs)
        : null;
      if (Number.isFinite(fixedMontoBs) && fixedMontoBs <= 0 && totalUsd > 0) {
        fixedMontoBs = null;
      }
      fixedHora = cartData?.hora ?? null;
      fixedFecha = cartData?.fecha ?? null;

      try {
        await syncCartMontosIfNeeded(cartData, totalUsd, tasaBs);
      } catch (syncErr) {
        console.warn("checkout sync cart montos error", syncErr);
      }
      if (!checkoutOrderId) {
        const draftResp = await fetchCheckoutDraft();
        if (!draftResp?.error) {
          const draftId = Number(draftResp?.id_orden);
          if (Number.isFinite(draftId) && draftId > 0) {
            setCheckoutOrderId(draftId);
          }
        }
      }
    }

    populateSelect(null);
    renderMetodoButtons();
    updateSelection(null);
    showMetodoMenu(true);
    startSaldoWatcher();
  } catch (err) {
    console.error("checkout load error", err);
    renderTotal();
  }
}

init();

window.addEventListener("beforeunload", () => {
  if (saldoWatcher) clearInterval(saldoWatcher);
  if (montoRefreshTimer) clearTimeout(montoRefreshTimer);
});

const uploadFiles = async () => {
  const files = Array.from(inputFiles.files || []);
  if (!files.length) return [];
  const resp = await uploadComprobantes(files);
  if (resp?.error) throw new Error(resp.error);
  return resp?.urls || [];
};

const updateOrdenConReferencia = async (idOrden, updatePayload) => {
  if (!idOrden) return false;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const { data, error } = await supabase
      .from("ordenes")
      .update(updatePayload)
      .eq("id_orden", idOrden)
      .select("id_orden");
    if (error) throw error;
    if (Array.isArray(data) && data.length > 0) return true;
    await new Promise((resolve) => setTimeout(resolve, 350));
  }
  return false;
};

let lastMetodoPersistOrderId = null;
let lastMetodoPersistId = null;

const getMetodoIdSeleccionado = () => {
  if (seleccionado === null || seleccionado < 0 || seleccionado >= metodos.length) return null;
  const metodo = metodos[seleccionado];
  const metodoId = Number(metodo?.id_metodo_de_pago ?? metodo?.id);
  if (!Number.isFinite(metodoId) || metodoId <= 0) return null;
  return metodoId;
};

const ensureOrderIdForMetodoPersist = async () => {
  if (Number.isFinite(checkoutOrderId) && checkoutOrderId > 0) return checkoutOrderId;

  if (isSaldoCheckout) {
    const saldoId = Number(saldoOrder?.id_orden ?? saldoOrderId);
    if (Number.isFinite(saldoId) && saldoId > 0) {
      setCheckoutOrderId(saldoId, { updateUrl: false });
      return saldoId;
    }
    return null;
  }

  const draftResp = await fetchCheckoutDraft();
  if (draftResp?.error) {
    throw new Error(draftResp.error);
  }
  const draftId = Number(draftResp?.id_orden);
  if (!Number.isFinite(draftId) || draftId <= 0) return null;
  setCheckoutOrderId(draftId);
  return draftId;
};

const persistMetodoSeleccionadoEnOrden = async ({ force = false } = {}) => {
  const metodoId = getMetodoIdSeleccionado();
  if (!metodoId) return { ok: false, reason: "metodo_invalido" };

  const orderId = await ensureOrderIdForMetodoPersist();
  if (!orderId) return { ok: false, reason: "orden_invalida" };

  if (!force && lastMetodoPersistOrderId === orderId && lastMetodoPersistId === metodoId) {
    return { ok: true, skipped: true, id_orden: orderId, id_metodo_de_pago: metodoId };
  }

  let updated = await updateOrdenConReferencia(orderId, { id_metodo_de_pago: metodoId });
  if (!updated && !isSaldoCheckout) {
    const refreshDraft = await fetchCheckoutDraft();
    if (refreshDraft?.error) {
      throw new Error(refreshDraft.error);
    }
    const refreshedId = Number(refreshDraft?.id_orden);
    if (Number.isFinite(refreshedId) && refreshedId > 0 && refreshedId !== orderId) {
      setCheckoutOrderId(refreshedId);
      updated = await updateOrdenConReferencia(refreshedId, { id_metodo_de_pago: metodoId });
      if (updated) {
        lastMetodoPersistOrderId = refreshedId;
        lastMetodoPersistId = metodoId;
        return { ok: true, id_orden: refreshedId, id_metodo_de_pago: metodoId };
      }
    }
  }

  if (!updated) return { ok: false, reason: "update_sin_filas", id_orden: orderId };

  lastMetodoPersistOrderId = orderId;
  lastMetodoPersistId = metodoId;
  return { ok: true, id_orden: orderId, id_metodo_de_pago: metodoId };
};

btnSendPayment?.addEventListener("click", async () => {
  // Toma montos guardados en BD, sin recálculo en frontend
  try {
    if (!isSaldoCheckout) {
      const cartData = await fetchCart();
      cartItems = cartData.items || [];
      cartId = cartData.id_carrito || null;
      const useSaldo = isTrue(cartData?.usa_saldo);
      const montoUsdRaw = Number(cartData?.monto_usd);
      const montoFinalRaw = Number(cartData?.monto_final);
      totalUsd =
        useSaldo && Number.isFinite(montoFinalRaw)
          ? Number(montoFinalRaw)
          : Number.isFinite(montoUsdRaw)
          ? Number(montoUsdRaw)
          : 0;
      precioTierLabel = "";
      fixedMontoUsd = Number.isFinite(montoUsdRaw)
        ? Number(montoUsdRaw)
        : totalUsd;
      fixedMontoBs = Number.isFinite(Number(cartData?.monto_bs))
        ? Number(cartData.monto_bs)
        : null;
      if (Number.isFinite(fixedMontoBs) && fixedMontoBs <= 0 && totalUsd > 0) {
        fixedMontoBs = null;
      }
      fixedHora = cartData?.hora ?? null;
      fixedFecha = cartData?.fecha ?? null;
      renderTotal();
    } else if (saldoOrder) {
      totalUsd = Number.isFinite(Number(saldoOrder?.total))
        ? Number(saldoOrder.total)
        : totalUsd;
      fixedMontoUsd = totalUsd;
      fixedMontoBs = Number.isFinite(Number(saldoOrder?.monto_bs))
        ? Number(saldoOrder.monto_bs)
        : fixedMontoBs;
      tasaBs = Number.isFinite(Number(saldoOrder?.tasa_bs)) ? Number(saldoOrder.tasa_bs) : tasaBs;
      renderTotal();
    }
  } catch (err) {
    console.error("recalc checkout error", err);
  }
  if (seleccionado === null) {
    alert("Selecciona un método de pago.");
    showMetodoMenu(true);
    return;
  }
  const metodo = metodos[seleccionado];
  const metodoId = Number(metodo?.id_metodo_de_pago ?? metodo?.id);
  const isMetodoBs = metodoId === 1;
  const referenciaRaw = String(refInput?.value || "").trim();
  if (!referenciaRaw) {
    alert("Ingresa la referencia.");
    refInput.classList.add("input-error");
    return;
  }
  const referenciaDigits = normalizeReferenceDigits(referenciaRaw);
  if (isMetodoBs && referenciaDigits.length < 4) {
    alert("La referencia debe tener al menos 4 dígitos.");
    refInput?.classList.add("input-error");
    return;
  }
  const referenciaValue = isMetodoBs ? referenciaDigits : referenciaRaw;
  if (isMetodoBs && refInput) refInput.value = referenciaValue;
  if (isMetodoBs) {
    if (!Number.isFinite(tasaBs)) {
      alert("No se pudo obtener la tasa.");
      return;
    }
  } else if (!inputFiles?.files?.length) {
    alert("Adjunta comprobantes de pago.");
    dropzone?.classList.add("input-error");
    return;
  }
  if (!isSaldoCheckout && !cartItems.length) {
    alert("No hay items en el carrito.");
    return;
  }
  try {
    const comprobantes = isMetodoBs ? [] : await uploadFiles();
    const { fecha, hora } = getCaracasNow();
    if (isSaldoCheckout) {
      const pendienteVerificacion = isMetodoVerificacionAutomatica(metodo);
      const montoBs = Number.isFinite(tasaBs) ? round2(totalUsd * tasaBs) : null;
      const { error: ordErr } = await supabase
        .from("ordenes")
        .update({
          id_metodo_de_pago: metodo.id_metodo_de_pago ?? metodo.id,
          referencia: referenciaValue,
          comprobante: comprobantes,
          total: totalUsd,
          tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
          monto_bs: montoBs,
          marcado_pago: true,
          en_espera: true,
          id_carrito: null,
          pago_verificado: false,
          monto_completo: null,
          orden_cancelada: null,
          checkout_finalizado: true,
          fecha,
          hora_orden: hora,
          hora_confirmacion: hora,
        })
        .eq("id_orden", saldoOrderId);
      if (ordErr) throw ordErr;
      const saldoNextUrl = pendienteVerificacion
        ? `verificando_pago.html?id_orden=${encodeURIComponent(saldoOrderId)}`
        : `entregar_servicios.html?id_orden=${encodeURIComponent(saldoOrderId)}`;
      window.location.href = saldoNextUrl;
      return;
    }
    const payload = {
      id_orden: checkoutOrderId,
      id_metodo_de_pago: metodo.id_metodo_de_pago ?? metodo.id,
      referencia: referenciaValue,
      comprobantes,
      total: totalUsd,
      tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
      marcado_pago: true,
    };
    const resp = await submitCheckout(payload);
    if (resp?.error) {
      alert(`Error en checkout: ${resp.error}`);
      return;
    }
    if (resp?.id_orden) {
      setCheckoutOrderId(resp.id_orden, { updateUrl: false });
    }
    if (resp?.id_orden) {
      try {
        const updated = await updateOrdenConReferencia(resp.id_orden, {
          fecha,
          hora_orden: hora,
          hora_confirmacion: hora,
          referencia: referenciaValue,
          comprobante: comprobantes,
        });
        if (!updated) {
          console.warn("Orden no encontrada para actualizar referencia", resp.id_orden);
        }
      } catch (horaErr) {
        console.warn("No se pudo actualizar orden en checkout", horaErr);
      }
    }
    const userId = requireSession();
    // Marcar notificación de inventario para el usuario en sesión
    try {
      await supabase.from("usuarios").update({ notificacion_inventario: true }).eq("id_usuario", userId);
    } catch (flagErr) {
      console.error("update notificacion_inventario error", flagErr);
    }

    // Notificaciones de nuevo servicio y renovaciones para el usuario en sesión por cada venta de la orden
    try {
      if (resp?.id_orden) {
        const { data: ventasOrden, error: ventasOrdErr } = await supabase
          .from("ventas")
          .select(
            "id_venta, id_usuario, fecha_corte, id_cuenta, id_cuenta_miembro, id_perfil, id_orden, renovacion, correo_miembro, clave_miembro, cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, clave, id_plataforma, plataformas:plataformas(nombre, correo_cliente, clave_cliente)), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, correo, clave, id_plataforma, plataformas:plataformas(nombre, correo_cliente, clave_cliente)), perfiles:perfiles(n_perfil)"
          )
          .eq("id_orden", resp.id_orden);
        if (ventasOrdErr) throw ventasOrdErr;
        const nuevosServiciosPorUsuario = new Map();
        const renovacionesPorUsuario = new Map();
        (ventasOrden || []).forEach((v) => {
          const cuentaRow = v.cuentas_miembro || v.cuentas || null;
          const platName = cuentaRow?.plataformas?.nombre || "Plataforma";
          const perfilTxt = v.perfiles?.n_perfil ? `M${v.perfiles.n_perfil}` : "";
          const correoTxt = v.correo_miembro || cuentaRow?.correo || "";
          const claveTxt = v.clave_miembro || cuentaRow?.clave || "";
          const ventaUserId = userId || v.id_usuario;

          if (v.renovacion) {
            if (!ventaUserId) return;
            const items = renovacionesPorUsuario.get(ventaUserId) || [];
            items.push({
              plataforma: platName,
              correoCuenta: correoTxt,
              clave: claveTxt,
              perfil: perfilTxt,
              fechaCorte: v.fecha_corte || "",
              idVenta: v.id_venta || null,
            });
            renovacionesPorUsuario.set(ventaUserId, items);
            return;
          }

          if (!ventaUserId || !correoTxt) return;
          const items = nuevosServiciosPorUsuario.get(ventaUserId) || [];
          items.push({
            plataforma: platName,
            correoCuenta: correoTxt,
            clave: claveTxt,
            perfil: perfilTxt,
            fechaCorte: v.fecha_corte || "",
            idVenta: v.id_venta || null,
          });
          nuevosServiciosPorUsuario.set(ventaUserId, items);
        });

        const rowsNotif = [];
        nuevosServiciosPorUsuario.forEach((items, uid) => {
          if (!items.length) return;
          const userIds = pickNotificationUserIds("nuevo_servicio", { ventaUserId: uid });
          if (!userIds.length) return;
          const payload = buildNotificationPayload("nuevo_servicio", { items });
          userIds.forEach((id) => rowsNotif.push({ ...payload, id_usuario: id }));
        });
        if (rowsNotif.length) {
          const { error: notifErr } = await supabase.from("notificaciones").insert(rowsNotif);
          if (notifErr) throw notifErr;
        }

        const renovacionRows = [];
        renovacionesPorUsuario.forEach((items, uid) => {
          if (!items.length) return;
          const userIds = pickNotificationUserIds("servicio_renovado", { ventaUserId: uid });
          if (!userIds.length) return;
          const payload = buildNotificationPayload("servicio_renovado", { items });
          userIds.forEach((id) => renovacionRows.push({ ...payload, id_usuario: id }));
        });
        if (renovacionRows.length) {
          const { error: renovErr } = await supabase.from("notificaciones").insert(renovacionRows);
          if (renovErr) throw renovErr;
        }
      }
    } catch (nErr) {
      console.error("notificaciones checkout error", nErr);
    }

    alert("Pago enviado correctamente.");
    const pendienteVerificacion = isMetodoVerificacionAutomatica(metodo);
    const nextUrl = resp?.id_orden
      ? pendienteVerificacion
        ? `verificando_pago.html?id_orden=${encodeURIComponent(resp.id_orden)}`
        : `entregar_servicios.html?id_orden=${encodeURIComponent(resp.id_orden)}`
      : pendienteVerificacion
      ? "verificando_pago.html"
      : "entregar_servicios.html";
    window.location.href = nextUrl;
  } catch (err) {
    console.error("checkout submit error", err);
    alert("No se pudo enviar el pago. Intenta de nuevo.");
  }
});

metodoSelect?.addEventListener("focus", () => metodoSelect.classList.remove("input-error"));
refInput?.addEventListener("focus", () => refInput.classList.remove("input-error"));
dropzone?.addEventListener("click", () => dropzone.classList.remove("input-error"));
