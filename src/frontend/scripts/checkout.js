import {
  supabase,
  fetchCart,
  loadCatalog,
  submitCheckout,
  fetchCheckoutDraft,
  fetchCheckoutSummary,
  uploadComprobantes,
  fetchP2PRate,
  loadCurrentUser,
} from "./api.js";
import { requireSession, attachLogoHome } from "./session.js";
import { loadPaginaBranding } from "./branding.js";
import { buildNotificationPayload, pickNotificationUserIds } from "./notification-templates.js";

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
const montoTransferidoWrapEl = document.querySelector("#monto-transferido-wrap");
const montoTransferidoInput = document.querySelector("#input-monto-transferido");
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
let checkoutSubmitInProgress = false;
let selectedComprobantePayload = null;
const RATE_SLOT_SECONDS = 2 * 60 * 60;
const METODO_RECARGO_USD_ID = 4;
const METODO_RECARGO_USD_PERCENT = 0.0349;
const METODO_RECARGO_USD_FIJO = 0.49;
const METODO_COMISION_20_ID = 3;
const METODO_COMISION_20_PERCENT = 0.2;
const METODO_BINANCE_USDT_ID = 2;
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const round3 = (n) => Math.round((Number(n) + Number.EPSILON) * 1000) / 1000;
const formatBsDisplay = (amount) => {
  const value = Number(amount);
  if (!Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("es-VE", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
};
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const isImageFile = (file) => {
  if (!file) return false;
  const mime = String(file.type || "").toLowerCase();
  if (mime.startsWith("image/")) return true;
  const name = String(file.name || "").toLowerCase().trim();
  return /\.(png|jpe?g|webp|gif|bmp|svg|avif|heic|heif|tiff?|ico)$/i.test(name);
};
const normalizeReferenceDigits = (value) => String(value || "").replace(/\D/g, "");
const isTrueFlag = (value) =>
  value === true || value === "true" || value === "1" || value === 1 || value === "t";
const toPositiveInt = (value) => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : 0;
};
const parseFlexibleDecimal = (value) => {
  if (value == null) return null;
  const raw = String(value).trim().replace(/\s+/g, "").replace(/[^0-9,.-]/g, "");
  if (!raw) return null;
  const sign = raw.startsWith("-") ? -1 : 1;
  const unsigned = raw.replace(/-/g, "");
  if (!unsigned) return null;
  const lastComma = unsigned.lastIndexOf(",");
  const lastDot = unsigned.lastIndexOf(".");
  const decimalIdx = Math.max(lastComma, lastDot);
  let normalized = "";
  for (let i = 0; i < unsigned.length; i += 1) {
    const ch = unsigned[i];
    if (ch >= "0" && ch <= "9") {
      normalized += ch;
      continue;
    }
    if ((ch === "," || ch === ".") && i === decimalIdx) {
      normalized += ".";
    }
  }
  if (!normalized || normalized === ".") return null;
  const parsed = Number(`${sign < 0 ? "-" : ""}${normalized}`);
  return Number.isFinite(parsed) ? parsed : null;
};
const hasTransientNetworkMessage = (value = "") => {
  const msg = String(value || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("failed to fetch") ||
    msg.includes("fetch failed") ||
    msg.includes("network request failed") ||
    msg.includes("networkerror") ||
    msg.includes("problema de conexión")
  );
};
const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
const isMetodoVerificacionAutomatica = (metodo) => isTrue(metodo?.verificacion_automatica);
const isMetodoNoBolivares = (metodo) => !isTrue(metodo?.bolivares);
const getCheckoutSubmitErrorMessage = (err) => {
  const message = String(err?.message || err || "").trim();
  if (hasTransientNetworkMessage(message)) {
    return "Se perdió la conexión mientras se enviaba el pago. Intenta de nuevo.";
  }
  return message || "No se pudo enviar el pago. Intenta de nuevo.";
};
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
  const id = Number(metodoId);
  if (id === METODO_BINANCE_USDT_ID) {
    const base2 = round2(montoBase);
    const alreadyUnique = Math.abs(montoBase - base2) >= 0.0005;
    return alreadyUnique ? round3(montoBase) : round3(base2 + 0.001);
  }
  if (id === METODO_RECARGO_USD_ID) {
    return round2(montoBase * (1 + METODO_RECARGO_USD_PERCENT) + METODO_RECARGO_USD_FIJO);
  }
  if (id === METODO_COMISION_20_ID) {
    return round2(montoBase * (1 + METODO_COMISION_20_PERCENT));
  }
  return round2(montoBase);
};

const resolveCheckoutTotalsFromCart = (cartData = null) => {
  const montoUsdRaw = Number(cartData?.monto_usd);
  const montoFinalRaw = Number(cartData?.monto_final);
  const montoBsRaw = Number(cartData?.monto_bs);
  const tasaBsRaw = Number(cartData?.tasa_bs);
  const totalNeto = Number.isFinite(montoFinalRaw)
    ? Number(montoFinalRaw)
    : Number.isFinite(montoUsdRaw)
      ? Number(montoUsdRaw)
      : null;
  const montoBsCalc =
    Number.isFinite(totalNeto) && Number.isFinite(tasaBsRaw) && tasaBsRaw > 0
      ? round2(totalNeto * tasaBsRaw)
      : null;

  return {
    totalNeto: Number.isFinite(totalNeto) ? round2(totalNeto) : null,
    montoUsdBruto: Number.isFinite(montoUsdRaw) ? round2(montoUsdRaw) : null,
    montoBs: Number.isFinite(montoBsCalc)
      ? montoBsCalc
      : Number.isFinite(montoBsRaw)
        ? round2(montoBsRaw)
        : null,
  };
};

const readFileAsDataUrl = (file) =>
  new Promise((resolve, reject) => {
    if (!(file instanceof Blob)) {
      reject(new Error("Archivo inválido."));
      return;
    }
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("No se pudo leer el archivo."));
    reader.readAsDataURL(file);
  });

const extractBase64FromDataUrl = (value = "") => {
  const raw = String(value || "");
  const commaIndex = raw.indexOf(",");
  return commaIndex >= 0 ? raw.slice(commaIndex + 1) : raw;
};

const clearSelectedComprobante = () => {
  selectedComprobantePayload = null;
  if (filePreview) filePreview.innerHTML = "";
  if (inputFiles) inputFiles.value = "";
};

const setSelectedComprobante = async (file) => {
  if (!isImageFile(file)) {
    clearSelectedComprobante();
    return false;
  }

  const dataUrl = await readFileAsDataUrl(file);
  const content = extractBase64FromDataUrl(dataUrl);
  if (!content) {
    throw new Error("No se pudo procesar la imagen seleccionada.");
  }

  selectedComprobantePayload = {
    name: String(file.name || "comprobante"),
    type: String(file.type || ""),
    content,
  };

  if (filePreview) {
    filePreview.innerHTML = "";
    const img = document.createElement("img");
    img.src = dataUrl;
    img.alt = String(file.name || "comprobante");
    filePreview.appendChild(img);
  }
  dropzone?.classList.remove("input-error");
  if (inputFiles) inputFiles.value = "";
  return true;
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
  return fetchP2PRate();
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
      fixedMontoBs = Number.isFinite(Number(cartData?.monto_bs))
        ? Number(cartData.monto_bs)
        : fixedMontoBs;
      fixedFecha = cartData?.fecha ?? fixedFecha;
      fixedHora = cartData?.hora ?? fixedHora;
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
      if (checkoutSubmitInProgress) return;
      const user = await loadCurrentUser();
      if (!user) return;
      const saldoVal = Number(user?.saldo);
      if (!Number.isFinite(saldoVal)) return;
      const saldoNum = saldoVal;
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
  if (!Number.isFinite(totalUsdVal)) return;
  const cartTotals = resolveCheckoutTotalsFromCart(cartData);
  const storedUsd = Number(cartTotals?.totalNeto);
  const storedBs = Number(cartTotals?.montoBs);
  const storedHora = cartData?.hora ?? null;
  const storedFecha = cartData?.fecha ?? null;
  const cartRate = Number(cartData?.tasa_bs);
  const tasaForCalc = Number.isFinite(cartRate) && cartRate > 0 ? cartRate : tasaVal;
  const montoBsCalc = Number.isFinite(tasaForCalc)
    ? Math.round(totalUsdVal * tasaForCalc * 100) / 100
    : null;

  fixedMontoUsd = Number.isFinite(storedUsd) ? storedUsd : round2(totalUsdVal);
  fixedMontoBs = Number.isFinite(storedBs) ? storedBs : montoBsCalc;
  fixedHora = storedHora;
  fixedFecha = storedFecha;

  if (!Number.isFinite(fixedMontoBs) && Number.isFinite(montoBsCalc)) {
    fixedMontoBs = montoBsCalc;
    const nowVz = getCaracasNow();
    fixedFecha = nowVz.fecha;
    fixedHora = nowVz.hora;
  }
  scheduleMontoRefresh(totalUsdVal);
};

const renderDetalle = () => {
  if (!metodoDetalle) return;
  if (seleccionado === null) {
    metodoDetalle.innerHTML = "";
    checkoutVerificacionNoteEl?.classList.add("hidden");
    montoTransferidoWrapEl?.classList.add("hidden");
    if (montoTransferidoInput) {
      montoTransferidoInput.value = "";
      montoTransferidoInput.classList.remove("input-error");
    }
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
      const safeVal = escapeHtml(valueToCopy);
      const copyIcon = c.copy
        ? `<img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/copiar-portapapeles.png" alt="Copiar" class="copy-field-icon" data-copy="${safeVal}" style="width:14px; height:14px; margin-left:6px; cursor:pointer;" />`
        : "";
      return `<p><strong>${escapeHtml(c.label)}:</strong> <span>${escapeHtml(c.valor)}</span>${copyIcon}</p>`;
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

  const showMontoTransferido = isMetodoNoBolivares(m);
  montoTransferidoWrapEl?.classList.toggle("hidden", !showMontoTransferido);
  if (!showMontoTransferido && montoTransferidoInput) {
    montoTransferidoInput.value = "";
    montoTransferidoInput.classList.remove("input-error");
  }
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
  await showMetodoMenu(false, { animate: true });
});

btnBackMetodo?.addEventListener("click", async () => {
  await showMetodoMenu(true, { animate: true });
});

btnAddImage?.addEventListener("click", () => {
  inputFiles?.click();
});

inputFiles?.addEventListener("change", () => {
  const files = Array.from(inputFiles.files || []).filter((f) => isImageFile(f));
  if (!files.length) {
    clearSelectedComprobante();
    return;
  }
  setSelectedComprobante(files[0]).catch((err) => {
    console.error("checkout comprobante read error", err);
    clearSelectedComprobante();
    alert("No se pudo leer la imagen seleccionada. Intenta con otra imagen.");
  });
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
  const files = Array.from(e.dataTransfer?.files || []).filter((f) => isImageFile(f));
  if (!files.length) {
    dropzone.classList.remove("drag-over");
    return;
  }
  setSelectedComprobante(files[0]).catch((err) => {
    console.error("checkout comprobante drop read error", err);
    clearSelectedComprobante();
    alert("No se pudo leer la imagen soltada. Intenta con otra imagen.");
  });
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
  const isMetodoUsdt = Number(metodoId) === METODO_BINANCE_USDT_ID;
  const totalUsdText = isMetodoUsdt
    ? `${totalUsdMostrado.toFixed(3)} USDT`
    : `$${totalUsdMostrado.toFixed(2)}`;
  const usdLabel =
    Number(metodoId) === METODO_RECARGO_USD_ID || Number(metodoId) === METODO_COMISION_20_ID
      ? "Monto a transferir"
      : "Total";
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
    ? `<div class="checkout-bs-line">Bs. ${formatBsDisplay(montoBsView)}</div>
       <div class="checkout-bs-exact-note">Envíe el monto exacto para la verificación automática de pago</div>`
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

  const targetReference = refDigits;
  let montoBs = Math.round(totalUsd * tasaBs * 100) / 100;
  let cartMontoBs = null;
  try {
    const cartData = await fetchCart();
    cartId = cartData.id_carrito || cartId;
    const cartTotals = resolveCheckoutTotalsFromCart(cartData);
    const cartRate = Number(cartData?.tasa_bs);
    if (Number.isFinite(cartRate) && cartRate > 0) {
      tasaBs = cartRate;
    }
    if (Number.isFinite(cartTotals?.totalNeto)) {
      totalUsd = Number(cartTotals.totalNeto);
    }
    if (Number.isFinite(cartTotals?.montoBs)) {
      montoBs = Number(cartTotals.montoBs);
      cartMontoBs = montoBs;
    } else if (Number.isFinite(fixedMontoBs)) {
      montoBs = fixedMontoBs;
      cartMontoBs = fixedMontoBs;
    }
  } catch (err) {
    console.warn("[pago movil] cart monto fetch error", err);
  }
  if (!Number.isFinite(cartMontoBs) || !Number.isFinite(tasaBs)) {
    alert("No se pudo obtener el monto o la tasa del carrito.");
    return { ok: false };
  }
  console.log("[pago movil] input", { refDigits, montoBs, tasaBs, totalUsd, cartId });

  const sessionUserId = toPositiveInt(currentUserId || requireSession());
  let pagosQuery = supabase
    .from("pagomoviles")
    .select("id, referencia, texto, monto_bs, saldo_acreditado, saldo_acreditado_a")
    .order("id", { ascending: false });
  if (sessionUserId > 0) {
    pagosQuery = pagosQuery.or(
      `saldo_acreditado.is.null,saldo_acreditado.eq.false,and(saldo_acreditado.eq.true,saldo_acreditado_a.eq.${sessionUserId})`,
    );
  } else {
    pagosQuery = pagosQuery.or("saldo_acreditado.is.null,saldo_acreditado.eq.false");
  }
  const resp = await pagosQuery;
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
  const extractPagoRefs = (pagoRow = {}) => {
    const refs = new Set();
    const refCol = normalizeReferenceDigits(pagoRow?.referencia);
    if (refCol.length >= 4) refs.add(refCol);
    extractRefMatches(pagoRow?.texto || "").forEach((refRaw) => {
      const refDigitsFound = normalizeReferenceDigits(refRaw);
      if (refDigitsFound.length >= 4) refs.add(refDigitsFound);
    });
    return Array.from(refs);
  };
  const getReferenceMatchScore = (targetRef = "", candidateRef = "") => {
    const target = normalizeReferenceDigits(targetRef);
    const candidate = normalizeReferenceDigits(candidateRef);
    if (target.length < 4 || candidate.length < 4) return 0;
    if (candidate === target) return 5;
    if (target.length > 4 || candidate.length > 4) {
      if (candidate.endsWith(target) || target.endsWith(candidate)) return 4;
    }
    if (candidate.slice(-4) === target.slice(-4)) return 1;
    return 0;
  };
  const resolveBestReferenceCandidate = (targetRef = "", refs = []) => {
    const normalizedRefs = Array.from(
      new Set(
        (Array.isArray(refs) ? refs : [])
          .map((ref) => normalizeReferenceDigits(ref))
          .filter((ref) => ref.length >= 4),
      ),
    );
    const target = normalizeReferenceDigits(targetRef);
    if (target.length < 4 || !normalizedRefs.length) return { score: 0, ref: null };
    let bestScore = 0;
    let bestRef = null;
    normalizedRefs.forEach((ref) => {
      const score = getReferenceMatchScore(target, ref);
      if (score > bestScore) {
        bestScore = score;
        bestRef = ref;
        return;
      }
      if (score > 0 && score === bestScore && bestRef && ref.length > bestRef.length) {
        bestRef = ref;
      }
    });
    return { score: bestScore, ref: bestRef };
  };

  const rankedMatches = (pagos || [])
    .map((p) => {
      const pagoMonto = montoNum(p.monto_bs);
      const refs = extractPagoRefs(p);
      const refResolution = resolveBestReferenceCandidate(targetReference, refs);
      console.log("[pago movil] eval", {
        id: p.id,
        refs,
        refScore: refResolution.score,
        pagoMonto,
        montoBs,
        diff: Number.isFinite(pagoMonto) ? Math.abs(pagoMonto - montoBs) : null,
        saldo_acreditado: p.saldo_acreditado,
      });
      if (refResolution.score <= 0 || !Number.isFinite(pagoMonto)) return null;
      return { row: p, pagoMonto, refResolution };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (b.refResolution.score !== a.refResolution.score) {
        return b.refResolution.score - a.refResolution.score;
      }
      const diffA = Math.abs(Number(a.pagoMonto) - Number(montoBs));
      const diffB = Math.abs(Number(b.pagoMonto) - Number(montoBs));
      if (diffA !== diffB) return diffA - diffB;
      return (Number(b.row?.id) || 0) - (Number(a.row?.id) || 0);
    });
  const bestMatch = rankedMatches[0] || null;
  const match = bestMatch?.row || null;
  if (!match) {
    alert("Pago no encontrado");
    return { ok: false };
  }
  if (isTrueFlag(match?.saldo_acreditado) && toPositiveInt(match?.saldo_acreditado_a) !== sessionUserId) {
    alert("Operación fallida: pago anteriormente registrado");
    return { ok: false };
  }

  const pagoMonto = montoNum(match.monto_bs);
  if (!Number.isFinite(pagoMonto)) {
    alert("Pago no encontrado");
    return { ok: false };
  }

  if (sessionUserId && !isTrueFlag(match?.saldo_acreditado)) {
    const refMatch = bestMatch?.refResolution?.ref || null;
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
  if (Number.isFinite(tasaBs) && tasaBs) {
    if (diff > 0) {
      saldoUsd = Number(((pagoMonto - cartMontoBs) / tasaBs).toFixed(2));
    } else {
      saldoUsd = Number((pagoMonto / tasaBs).toFixed(2));
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
          "id_metodo_de_pago, nombre, imagen, correo, id, cedula, telefono, verificacion_automatica, bolivares"
        ),
      fetchP2PRate(),
      loadCurrentUser(),
    ]);
    if (metodosResp.error) throw metodosResp.error;
    metodos = metodosResp.data || [];
    currentUser = user || null;
    tasaBs = Number.isFinite(tasaResp) ? tasaResp : null;
    userAcceso = currentUser?.acceso_cliente;
    currentUserId = currentUser?.id_usuario || null;
    const initialSaldo = Number(currentUser?.saldo);
    lastSaldoValue = Number.isFinite(initialSaldo) ? initialSaldo : 0;

    if (saldoFrom && Number.isFinite(saldoOrderId)) {
      const { data: ordenData, error: ordenErr } = await supabase
        .from("ordenes")
        .select("id_orden, id_carrito, total, monto_bs, hora_orden, fecha, en_espera")
        .eq("id_orden", saldoOrderId)
        .maybeSingle();
      if (!ordenErr && ordenData && !ordenData.id_carrito) {
        isSaldoCheckout = true;
        saldoOrder = ordenData;
        if (ordenData?.id_orden) {
          setCheckoutOrderId(ordenData.id_orden, { updateUrl: false });
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
      const cartTotals = resolveCheckoutTotalsFromCart(cartData);
      const cartRate = Number(cartData?.tasa_bs);
      if (Number.isFinite(cartRate) && cartRate > 0) {
        tasaBs = cartRate;
      }
      totalUsd = Number.isFinite(cartTotals?.totalNeto) ? Number(cartTotals.totalNeto) : 0;

      precioTierLabel = "";
      fixedMontoUsd = totalUsd;
      fixedMontoBs = Number.isFinite(cartTotals?.montoBs) ? Number(cartTotals.montoBs) : null;
      if (Number.isFinite(fixedMontoBs) && fixedMontoBs <= 0 && totalUsd > 0) {
        fixedMontoBs = null;
      }
      fixedHora = cartData?.hora ?? null;
      fixedFecha = cartData?.fecha ?? null;

      const summaryResp = await fetchCheckoutSummary();
      if (!summaryResp?.error) {
        const summaryTotalUsd = Number(summaryResp?.total_usd);
        const summaryMontoBs = Number(summaryResp?.monto_bs);
        const summaryTasaBs = Number(summaryResp?.tasa_bs);
        if (Number.isFinite(summaryTotalUsd)) {
          totalUsd = summaryTotalUsd;
          fixedMontoUsd = summaryTotalUsd;
        }
        if (Number.isFinite(summaryMontoBs)) {
          fixedMontoBs = summaryMontoBs;
        }
        if (Number.isFinite(summaryTasaBs) && summaryTasaBs > 0) {
          tasaBs = summaryTasaBs;
        }
      }

      try {
        await syncCartMontosIfNeeded(cartData, totalUsd, tasaBs);
      } catch (syncErr) {
        console.warn("checkout sync cart montos error", syncErr);
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
  if (!selectedComprobantePayload) return [];
  const resp = await uploadComprobantes([selectedComprobantePayload]);
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

const redirectAfterCheckoutSubmit = (url) => {
  const target = String(url || "").trim();
  if (!target) return;
  try {
    window.location.replace(target);
  } catch (_err) {
    window.location.href = target;
  }
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
  if (checkoutSubmitInProgress) return;
  let willNavigate = false;
  const sendPaymentOriginalText = btnSendPayment ? String(btnSendPayment.textContent || "").trim() : "";
  checkoutSubmitInProgress = true;
  if (btnSendPayment) {
    btnSendPayment.disabled = true;
    btnSendPayment.textContent = "Cargando...";
  }

  // Toma montos guardados en BD, sin recálculo en frontend
  try {
    try {
      if (!isSaldoCheckout) {
        const [cartData, summaryResp] = await Promise.all([fetchCart(), fetchCheckoutSummary()]);
        cartItems = cartData.items || [];
        cartId = cartData.id_carrito || null;
        const cartTotals = resolveCheckoutTotalsFromCart(cartData);
        const cartRate = Number(cartData?.tasa_bs);
        if (Number.isFinite(cartRate) && cartRate > 0) {
          tasaBs = cartRate;
        }
        totalUsd = Number.isFinite(cartTotals?.totalNeto) ? Number(cartTotals.totalNeto) : 0;
        precioTierLabel = "";
        fixedMontoUsd = totalUsd;
        fixedMontoBs = Number.isFinite(cartTotals?.montoBs) ? Number(cartTotals.montoBs) : null;
        if (Number.isFinite(fixedMontoBs) && fixedMontoBs <= 0 && totalUsd > 0) {
          fixedMontoBs = null;
        }
        if (!summaryResp?.error) {
          const summaryTotalUsd = Number(summaryResp?.total_usd);
          const summaryMontoBs = Number(summaryResp?.monto_bs);
          const summaryTasaBs = Number(summaryResp?.tasa_bs);
          if (Number.isFinite(summaryTotalUsd)) {
            totalUsd = summaryTotalUsd;
            fixedMontoUsd = summaryTotalUsd;
          }
          if (Number.isFinite(summaryMontoBs)) {
            fixedMontoBs = summaryMontoBs;
          }
          if (Number.isFinite(summaryTasaBs) && summaryTasaBs > 0) {
            tasaBs = summaryTasaBs;
          }
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
    const requiereComprobante = !isMetodoVerificacionAutomatica(metodo);
    const requiereMontoTransferido = isMetodoNoBolivares(metodo);
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
    let montoTransferidoValue = null;
    if (requiereMontoTransferido) {
      const rawMontoTransferido = String(montoTransferidoInput?.value || "").trim();
      const parsedMontoTransferido = parseFlexibleDecimal(rawMontoTransferido);
      if (!Number.isFinite(parsedMontoTransferido) || parsedMontoTransferido <= 0) {
        alert("Ingresa un monto transferido válido.");
        montoTransferidoInput?.classList.add("input-error");
        montoTransferidoInput?.focus();
        return;
      }
      const isMetodoBinance = metodoId === METODO_BINANCE_USDT_ID;
      montoTransferidoValue = isMetodoBinance ? round3(parsedMontoTransferido) : round2(parsedMontoTransferido);
      if (montoTransferidoInput) {
        montoTransferidoInput.value = String(
          isMetodoBinance ? montoTransferidoValue.toFixed(3) : montoTransferidoValue.toFixed(2),
        );
      }
    }
    if (isMetodoBs) {
      if (!Number.isFinite(tasaBs)) {
        alert("No se pudo obtener la tasa.");
        return;
      }
    }
    if (requiereComprobante && !selectedComprobantePayload) {
      alert("Adjunta comprobantes de pago.");
      dropzone?.classList.add("input-error");
      return;
    }
    if (!isSaldoCheckout && !cartItems.length) {
      alert("No hay items en el carrito.");
      return;
    }

    const comprobantes = selectedComprobantePayload ? await uploadFiles() : [];
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
          monto_transferido: montoTransferidoValue,
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
      willNavigate = true;
      redirectAfterCheckoutSubmit(saldoNextUrl);
      return;
    }
    const payload = {
      id_orden: checkoutOrderId,
      id_metodo_de_pago: metodo.id_metodo_de_pago ?? metodo.id,
      referencia: referenciaValue,
      comprobantes,
      total: totalUsd,
      tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
      monto_transferido: montoTransferidoValue,
      marcado_pago: true,
    };
    const resp = await submitCheckout(payload);
    if (resp?.error) {
      alert(resp.error || "No se pudo enviar el pago. Intenta de nuevo.");
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
            "id_venta, id_usuario, fecha_corte, id_cuenta, id_cuenta_miembro, id_perfil, id_orden, renovacion, pendiente, correo_miembro, clave_miembro, cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, clave, id_plataforma, plataformas:plataformas(nombre, correo_cliente, clave_cliente, entrega_inmediata, tarjeta_de_regalo)), cuentas_miembro:cuentas!ventas_id_cuenta_miembro_fkey(id_cuenta, correo, clave, id_plataforma, plataformas:plataformas(nombre, correo_cliente, clave_cliente, entrega_inmediata, tarjeta_de_regalo)), perfiles:perfiles(n_perfil), tarjetas_de_regalo:tarjetas_de_regalo!ventas_id_tarjeta_de_regalo_fkey(pin), precios:precios!ventas_id_precio_fkey(id_plataforma, region, valor_tarjeta_de_regalo, moneda, plataformas:plataformas(nombre, entrega_inmediata, tarjeta_de_regalo))"
          )
          .eq("id_orden", resp.id_orden);
        if (ventasOrdErr) throw ventasOrdErr;
        const nuevosServiciosPorUsuario = new Map();
        const serviciosEnProcesoPorUsuario = new Map();
        const renovacionesPorUsuario = new Map();
        (ventasOrden || []).forEach((v) => {
          const cuentaRow = v.cuentas_miembro || v.cuentas || null;
          const precioRow = v.precios || null;
          const plataformaFromCuenta = cuentaRow?.plataformas || null;
          const plataformaFromPrecio = precioRow?.plataformas || null;
          const plataformaInfo = plataformaFromCuenta || plataformaFromPrecio || null;
          const idPlataforma = Number(cuentaRow?.id_plataforma || precioRow?.id_plataforma) || null;
          const platName = plataformaInfo?.nombre || "Plataforma";
          const entregaInmediata = isTrue(plataformaInfo?.entrega_inmediata);
          const isGiftCard = isTrue(plataformaInfo?.tarjeta_de_regalo);
          const perfilTxt = v.perfiles?.n_perfil ? `M${v.perfiles.n_perfil}` : "";
          const correoTxt = v.correo_miembro || cuentaRow?.correo || "";
          const claveTxt = v.clave_miembro || cuentaRow?.clave || "";
          const pinTxt = String(v?.tarjetas_de_regalo?.pin || "").trim();
          const shouldNotifyInProcess = isTrue(v?.pendiente) || (idPlataforma && !entregaInmediata);
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

          if (!ventaUserId) return;
          if (shouldNotifyInProcess) {
            const itemsProc = serviciosEnProcesoPorUsuario.get(ventaUserId) || [];
            itemsProc.push({
              plataforma: platName,
              fechaCorte: v.fecha_corte || "",
              idVenta: v.id_venta || null,
            });
            serviciosEnProcesoPorUsuario.set(ventaUserId, itemsProc);
            return;
          }

          if (!correoTxt && !isGiftCard) return;
          const itemsNew = nuevosServiciosPorUsuario.get(ventaUserId) || [];
          itemsNew.push({
            plataforma: platName,
            correoCuenta: correoTxt,
            clave: claveTxt,
            perfil: perfilTxt,
            ...(isGiftCard
              ? {
                  region: precioRow?.region || "",
                  valorTarjeta: precioRow?.valor_tarjeta_de_regalo || "",
                  moneda: precioRow?.moneda || "",
                  pin: pinTxt,
                }
              : {}),
            fechaCorte: v.fecha_corte || "",
            idVenta: v.id_venta || null,
          });
          nuevosServiciosPorUsuario.set(ventaUserId, itemsNew);
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

        const rowsProcNotif = [];
        serviciosEnProcesoPorUsuario.forEach((items, uid) => {
          if (!items.length) return;
          const userIds = pickNotificationUserIds("servicio_en_proceso", { ventaUserId: uid });
          if (!userIds.length) return;
          const payload = buildNotificationPayload("servicio_en_proceso", { items });
          userIds.forEach((id) => rowsProcNotif.push({ ...payload, id_usuario: id }));
        });
        if (rowsProcNotif.length) {
          const { error: procErr } = await supabase.from("notificaciones").insert(rowsProcNotif);
          if (procErr) throw procErr;
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

    const entregaManual = resp?.entrega_manual === true;
    const resolvedOrderId = Number(resp?.id_orden ?? checkoutOrderId);
    const hasResolvedOrderId = Number.isFinite(resolvedOrderId) && resolvedOrderId > 0;
    if (entregaManual) {
      alert("Pago enviado. Tu orden será revisada y procesada manualmente por un admin.");
      const manualUrl = hasResolvedOrderId
        ? `historial_ordenes.html?id_orden=${encodeURIComponent(resolvedOrderId)}`
        : "historial_ordenes.html";
      willNavigate = true;
      redirectAfterCheckoutSubmit(manualUrl);
      return;
    }

    alert("Pago enviado correctamente.");
    const pendienteVerificacion =
      resp?.pendiente_verificacion === true || isMetodoVerificacionAutomatica(metodo);
    const nextUrl = hasResolvedOrderId
      ? pendienteVerificacion
        ? `verificando_pago.html?id_orden=${encodeURIComponent(resolvedOrderId)}`
        : `entregar_servicios.html?id_orden=${encodeURIComponent(resolvedOrderId)}`
      : pendienteVerificacion
      ? "verificando_pago.html"
      : "entregar_servicios.html";
    willNavigate = true;
    redirectAfterCheckoutSubmit(nextUrl);
  } catch (err) {
    console.error("checkout submit error", err);
    alert(getCheckoutSubmitErrorMessage(err));
  } finally {
    if (!willNavigate) {
      checkoutSubmitInProgress = false;
      if (btnSendPayment) {
        btnSendPayment.disabled = false;
        btnSendPayment.textContent = sendPaymentOriginalText || "Enviar pago";
      }
    }
  }
});

metodoSelect?.addEventListener("focus", () => metodoSelect.classList.remove("input-error"));
refInput?.addEventListener("focus", () => refInput.classList.remove("input-error"));
dropzone?.addEventListener("click", () => dropzone.classList.remove("input-error"));
montoTransferidoInput?.addEventListener("focus", () =>
  montoTransferidoInput.classList.remove("input-error")
);
montoTransferidoInput?.addEventListener("input", () => {
  const raw = String(montoTransferidoInput.value || "");
  const cleaned = raw.replace(/[^\d,.\s]/g, "").replace(/\s+/g, "");
  if (cleaned !== raw) montoTransferidoInput.value = cleaned;
});
montoTransferidoInput?.addEventListener("blur", () => {
  const parsed = parseFlexibleDecimal(montoTransferidoInput.value);
  if (!Number.isFinite(parsed) || parsed <= 0) return;
  const metodoId = getMetodoIdSeleccionado();
  if (Number(metodoId) === METODO_BINANCE_USDT_ID) {
    montoTransferidoInput.value = round3(parsed).toFixed(3);
    return;
  }
  montoTransferidoInput.value = round2(parsed).toFixed(2);
});
