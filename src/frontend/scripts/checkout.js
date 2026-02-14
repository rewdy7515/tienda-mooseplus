import {
  supabase,
  fetchCart,
  loadCatalog,
  submitCheckout,
  updateCartMontos,
  uploadComprobantes,
  fetchP2PRate,
  loadCurrentUser,
} from "./api.js";
import { requireSession, attachLogoHome } from "./session.js";
import { buildNotificationPayload, pickNotificationUserIds } from "./notification-templates.js";
import { TASA_MARKUP } from "./rate-config.js";

requireSession();
attachLogoHome();

const urlParams = new URLSearchParams(window.location.search);
const saldoOrderId = Number(urlParams.get("id_orden"));
const saldoFrom = urlParams.get("from") === "saldo";
let isSaldoCheckout = false;
let saldoOrder = null;

const metodoDetalle = document.querySelector("#metodo-detalle");
const metodoSelect = document.querySelector("#metodo-select");
const pagoMovilNote = document.querySelector("#pago-movil-note");
const btnAddImage = document.querySelector("#btn-add-image");
const inputFiles = document.querySelector("#input-files");
const filePreview = document.querySelector("#file-preview");
const dropzone = document.querySelector("#dropzone");
const totalEl = document.querySelector("#checkout-total");
const btnSendPayment = document.querySelector("#btn-send-payment");
const refInput = document.querySelector("#input-ref");
const btnVerifyPayment = document.querySelector("#btn-verify-payment");

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
let countdownTimer = null;
let saldoWatcher = null;
let lastSaldoValue = null;
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";

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

const parseCaracasDate = (fechaStr, horaStr) => {
  if (!fechaStr || !horaStr) return null;
  const fecha = String(fechaStr).trim();
  const hora = String(horaStr).trim();
  if (!fecha || !hora) return null;
  const iso = `${fecha}T${hora}-04:00`;
  const dt = new Date(iso);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const shouldRefreshMonto = (fechaStr, horaStr) => {
  const dt = parseCaracasDate(fechaStr, horaStr);
  if (!dt) return true;
  return Date.now() - dt.getTime() >= 30 * 60 * 1000;
};

const formatCountdown = (msLeft) => {
  const totalSec = Math.max(0, Math.floor(msLeft / 1000));
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  const mm = String(min).padStart(2, "0");
  const ss = String(sec).padStart(2, "0");
  return `${mm}min ${ss}seg`;
};

const getCountdownMs = (fechaStr, horaStr) => {
  const dt = parseCaracasDate(fechaStr, horaStr);
  if (!dt) return null;
  const elapsed = Date.now() - dt.getTime();
  const remaining = 30 * 60 * 1000 - elapsed;
  return remaining > 0 ? remaining : 0;
};

const startCountdown = () => {
  if (countdownTimer) {
    clearInterval(countdownTimer);
    countdownTimer = null;
  }
  countdownTimer = setInterval(() => {
    renderTotal();
  }, 1000);
};

const scheduleMontoRefresh = (fechaStr, horaStr, totalUsdVal, tasaVal) => {
  if (montoRefreshTimer) {
    clearTimeout(montoRefreshTimer);
    montoRefreshTimer = null;
  }
  const dt = parseCaracasDate(fechaStr, horaStr);
  if (!dt) return;
  const elapsed = Date.now() - dt.getTime();
  const waitMs = 30 * 60 * 1000 - elapsed;
  if (waitMs <= 0) return;
  montoRefreshTimer = setTimeout(async () => {
    try {
      const cartData = await fetchCart();
      await syncCartMontosIfNeeded(cartData, totalUsdVal, tasaVal);
      renderTotal();
    } catch (err) {
      console.warn("checkout monto refresh error", err);
    }
  }, waitMs + 1000);
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
    startCountdown();
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
    startCountdown();
  } else {
    scheduleMontoRefresh(fixedFecha, fixedHora, totalUsdVal, tasaVal);
    if (fixedHora) startCountdown();
  }
};

const renderDetalle = () => {
  if (!metodoDetalle) return;
  if (seleccionado === null) {
    metodoDetalle.innerHTML = "";
    return;
  }
  const m = metodos[seleccionado];
  const isMetodoBs = Number(m.id_metodo_de_pago ?? m.id) === 1;
  const nombreLabel = isMetodoBs ? "Banco" : "Nombre";
  const campos = [
    { label: nombreLabel, valor: m.nombre, copy: false },
    { label: "Correo", valor: m.correo, copy: true },
    { label: "ID", valor: m.id, copy: true },
    { label: "Cédula", valor: m.cedula, copy: false },
    { label: "Teléfono", valor: m.telefono, copy: false },
  ].filter((c) => c.valor !== null && c.valor !== undefined && c.valor !== "");

  const detalleHtml = campos
    .map((c) => {
      const safeVal = String(c.valor).replace(/"/g, "&quot;");
      const copyIcon = c.copy
        ? `<img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/copiar-portapapeles.png" alt="Copiar" class="copy-field-icon" data-copy="${safeVal}" style="width:14px; height:14px; margin-left:6px; cursor:pointer;" />`
        : "";
      return `<p><strong>${c.label}:</strong> <span>${c.valor}</span>${copyIcon}</p>`;
    })
    .join("");

  if (pagoMovilNote) {
    pagoMovilNote.classList.toggle("hidden", !isMetodoBs);
  }
  metodoDetalle.innerHTML =
    detalleHtml +
    (isMetodoBs
      ? `<button type="button" class="btn-primary copy-detalle-btn" style="margin-top:8px; display:flex; align-items:center; justify-content:center; gap:8px; width:100%;">
          <span>Copiar al portapapeles</span>
          <img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/copiar-portapapeles.png" alt="Copiar" style="width:18px; height:18px; filter: brightness(0) invert(1);" />
        </button>`
      : "");

  if (isMetodoBs) {
    const btnCopy = metodoDetalle.querySelector(".copy-detalle-btn");
    btnCopy?.addEventListener("click", async () => {
      const text = campos.map((c) => `${c.valor}`).join("\n");
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
    .filter(({ m }) => Number(m.id_metodo_de_pago ?? m.id) === 1)
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
    const first = metodoSelect.querySelector("option[value]:not([value=''])");
    if (first) {
      metodoSelect.value = first.value;
      const idx = Number(first.value);
      if (Number.isFinite(idx)) seleccionado = idx;
    }
  }
};

const updateSelection = (idx) => {
  if (idx === null || idx < 0 || idx >= metodos.length) {
    seleccionado = null;
  } else {
    seleccionado = idx;
  }
  renderDetalle();
  if (metodoSelect && seleccionado !== null) {
    metodoSelect.value = String(seleccionado);
  }
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
  const metodo = seleccionado !== null ? metodos[seleccionado] : null;
  const metodoId = metodo?.id_metodo_de_pago ?? metodo?.id;
  const onlyMetodoBs =
    Array.isArray(metodos) &&
    metodos.length > 0 &&
    metodos.every((m) => Number(m?.id_metodo_de_pago ?? m?.id) === 1);
  const isBs = (metodo && Number(metodoId) === 1) || (!metodo && onlyMetodoBs);
  const tasaVal = Number.isFinite(tasaBs) ? tasaBs : null;
  const lineUsd = `Total: $${totalUsd.toFixed(2)} ${precioTierLabel ? `(${precioTierLabel})` : ""}`;
  let montoBsView = null;
  let countdownLine = "";
  if (isBs) {
    if (Number.isFinite(fixedMontoBs) && fixedMontoBs > 0) {
      montoBsView = fixedMontoBs;
    } else if (tasaVal) {
      montoBsView = round2(totalUsd * tasaVal);
    }
    if (fixedHora) {
      const remaining = getCountdownMs(fixedFecha, fixedHora);
      if (remaining !== null) {
        countdownLine = `<div class="checkout-countdown">El monto en bolivares se actualizará en ${formatCountdown(remaining)}</div>`;
      }
    } else if (countdownTimer) {
      clearInterval(countdownTimer);
      countdownTimer = null;
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
  if (!refInput?.value.trim()) {
    alert("Ingresa la referencia.");
    refInput.classList.add("input-error");
    return { ok: false };
  }
  if (!Number.isFinite(tasaBs)) {
    alert("No se pudo obtener la tasa.");
    return { ok: false };
  }

  const refDigits = refInput.value.trim();
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
  console.log("[pago movil] input", { refDigits, montoBs, tasaBs, totalUsd, cartId });

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
    const refMatch = textoRefs.some((n) => n.slice(-4) === refDigits);
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
    const refMatch = textoRefs.find((n) => n.slice(-4) === refDigits);
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
  try {
    const [metodosResp, tasaResp, user] = await Promise.all([
      supabase
        .from("metodos_de_pago")
        .select("id_metodo_de_pago, nombre, correo, id, cedula, telefono"),
      fetchP2PRate(),
      loadCurrentUser(),
    ]);
    if (metodosResp.error) throw metodosResp.error;
    metodos = metodosResp.data || [];
    currentUser = user || null;
    tasaBs = tasaResp ? Math.round(tasaResp * TASA_MARKUP * 100) / 100 : null;
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
        if (fixedHora) startCountdown();
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
      if (fixedHora) startCountdown();

      try {
        await syncCartMontosIfNeeded(cartData, totalUsd, tasaBs);
      } catch (syncErr) {
        console.warn("checkout sync cart montos error", syncErr);
      }
    }

    // Selecciona por defecto el método con id 1 si existe
    // Prefill de pruebas: seleccionar índice 5 si existe, si no cae al método id 1
    let idxDefault = metodos.findIndex(
      (m) => Number(m.id_metodo_de_pago ?? m.id) === 1
    );
    if (idxDefault < 0 && metodos.length) idxDefault = 0;
    populateSelect(idxDefault >= 0 ? idxDefault : null);
    if (idxDefault >= 0) {
      updateSelection(idxDefault);
    } else if (seleccionado !== null) {
      updateSelection(seleccionado);
    } else {
      renderDetalle();
      renderTotal();
    }
    if (btnVerifyPayment) {
      const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
      const isSuper = isTrue(currentUser?.permiso_superadmin);
      btnVerifyPayment.classList.toggle("hidden", !isSuper);
      if (!isSuper) btnVerifyPayment.style.display = "none";
    }
    startSaldoWatcher();
  } catch (err) {
    console.error("checkout load error", err);
    renderTotal();
  }
}

init();

window.addEventListener("beforeunload", () => {
  if (saldoWatcher) clearInterval(saldoWatcher);
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
  // Forzamos método 1 por pruebas si existe
  if (seleccionado === null) {
    const idxDefault = metodos.findIndex(
      (m) => Number(m.id_metodo_de_pago ?? m.id) === 1
    );
    if (idxDefault >= 0) {
      seleccionado = idxDefault;
      if (metodoSelect) metodoSelect.value = String(idxDefault);
      renderDetalle();
    }
  }
  if (seleccionado === null) {
    alert("Selecciona un método de pago.");
    metodoSelect?.classList.add("input-error");
    return;
  }
  const metodo = metodos[seleccionado];
  const metodoId = Number(metodo?.id_metodo_de_pago ?? metodo?.id);
  const isMetodoBs = metodoId === 1;
  if (!refInput?.value.trim()) {
    alert("Ingresa la referencia.");
    refInput.classList.add("input-error");
    return;
  }
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
      const montoBs = Number.isFinite(tasaBs) ? round2(totalUsd * tasaBs) : null;
      const { error: ordErr } = await supabase
        .from("ordenes")
        .update({
          id_metodo_de_pago: metodo.id_metodo_de_pago ?? metodo.id,
          referencia: refInput.value.trim(),
          comprobante: comprobantes,
          total: totalUsd,
          tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
          monto_bs: montoBs,
          en_espera: true,
          id_carrito: null,
          pago_verificado: false,
          monto_completo: null,
          orden_cancelada: null,
          fecha,
          hora_orden: hora,
          hora_confirmacion: hora,
        })
        .eq("id_orden", saldoOrderId);
      if (ordErr) throw ordErr;
      window.location.href = `verificando_pago.html?id_orden=${encodeURIComponent(
        saldoOrderId
      )}`;
      return;
    }
    const payload = {
      id_metodo_de_pago: metodo.id_metodo_de_pago ?? metodo.id,
      referencia: refInput.value.trim(),
      comprobantes,
      total: totalUsd,
      tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
    };
    const resp = await submitCheckout(payload);
    if (resp?.error) {
      alert(`Error en checkout: ${resp.error}`);
      return;
    }
    if (resp?.id_orden) {
      try {
        const updated = await updateOrdenConReferencia(resp.id_orden, {
          fecha,
          hora_orden: hora,
          hora_confirmacion: hora,
          referencia: refInput.value.trim(),
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
    const pendienteVerificacion = resp?.pendiente_verificacion === true;
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

btnVerifyPayment?.addEventListener("click", async () => {
  try {
    await verifyPagoMovil();
  } catch (err) {
    console.error("verify pago movil error", err);
    alert("No se pudo verificar el pago.");
  }
});

metodoSelect?.addEventListener("focus", () => metodoSelect.classList.remove("input-error"));
refInput?.addEventListener("focus", () => refInput.classList.remove("input-error"));
dropzone?.addEventListener("click", () => dropzone.classList.remove("input-error"));
