import { requireSession, attachLogout, attachLogoHome } from "./session.js";
import {
  clearServerSession,
  supabase,
  loadCurrentUser,
  procesarOrden,
  fetchP2PRate,
  notificarVerificacionManualOrden,
} from "./api.js";

requireSession();
attachLogoHome();
attachLogout(clearServerSession);

const statusEl = document.querySelector("#verif-status");
const verifCard = document.querySelector(".verif-card");
const processingBlock = document.querySelector("#processing-block");
const ordenDisplay = document.querySelector("#orden-display");
const refDisplay = document.querySelector("#ref-display");
const montoDisplay = document.querySelector("#monto-display");
const refEditWrap = document.querySelector("#ref-edit-wrap");
const refInput = document.querySelector("#ref-input");
const btnEditRef = document.querySelector("#btn-edit-ref");
const btnSaveRef = document.querySelector("#btn-save-ref");
const progressBar = document.querySelector("#progress-bar");
const countdownEl = document.querySelector("#countdown");

const params = new URLSearchParams(window.location.search);
const idOrden = Number(params.get("id_orden"));

let orden = null;
let pagosInsertChannel = null;
let ordenUpdateChannel = null;
let countdownTimer = null;
let verifyPollTimer = null;
let currentUserId = null;
let processingOrder = false;
let orderProcessed = false;
let verifyRunning = false;
let verifyPending = false;
let cartMontoBs = null;
let currentRateBs = null;
let countdownExpired = false;
let manualVerificationNotified = false;
const VERIFY_WINDOW_MS = 3 * 60 * 1000;
const VERIFY_POLL_INTERVAL_MS = 12000;
const CHANNEL_WARN_MIN_INTERVAL_MS = 20000;
let lastPagosChannelWarnAt = 0;
let lastOrdenChannelWarnAt = 0;
const MANUAL_VERIFICATION_PENDING_MSG =
  "Pago no detectado, se envió una notificación a un admin para que verifique manualmente";
const normalizeReferenceDigits = (value) => String(value || "").replace(/\D/g, "");
const buildEntregaUrl = () => `entregar_servicios.html?id_orden=${encodeURIComponent(orden?.id_orden || idOrden)}`;

const setStatus = (msg) => {
  if (statusEl) statusEl.textContent = msg || "";
};

const shouldLogChannelWarning = (kind = "") => {
  const now = Date.now();
  if (kind === "pagomoviles") {
    if (now - lastPagosChannelWarnAt < CHANNEL_WARN_MIN_INTERVAL_MS) return false;
    lastPagosChannelWarnAt = now;
    return true;
  }
  if (kind === "orden") {
    if (now - lastOrdenChannelWarnAt < CHANNEL_WARN_MIN_INTERVAL_MS) return false;
    lastOrdenChannelWarnAt = now;
    return true;
  }
  return true;
};

const notifyManualVerificationAdmin = async (source = "countdown_expired") => {
  if (manualVerificationNotified) return true;
  const orderId = Number(orden?.id_orden ?? idOrden);
  if (!Number.isFinite(orderId) || orderId <= 0) return false;
  const resp = await notificarVerificacionManualOrden(orderId, { source });
  if (!resp?.error) {
    manualVerificationNotified = true;
    return true;
  }
  console.error("notificar verificacion manual error", resp?.error || "error desconocido");
  return false;
};

const showVerifiedFallbackView = () => {
  if (!verifCard) return;
  verifCard.innerHTML = `
    <h2>¡Tu pago fue verificado!</h2>
    <p class="verif-message">Gracias por tu compra</p>
    <button type="button" class="btn-primary btn-view-services" id="btn-view-services">Ver servicios</button>
  `;
  document.querySelector("#btn-view-services")?.addEventListener("click", () => {
    window.location.href = buildEntregaUrl();
  });
};

const redirectToEntregaServicios = () => {
  const targetUrl = buildEntregaUrl();
  setTimeout(() => {
    const stillHere = /\/verificando_pago\.html$/i.test(window.location.pathname);
    if (stillHere) {
      showVerifiedFallbackView();
    }
  }, 1200);
  try {
    window.location.href = targetUrl;
  } catch (_err) {
    showVerifiedFallbackView();
  }
};

const handlePagoVerificado = () => {
  if (orderProcessed) return;
  orderProcessed = true;
  countdownExpired = false;
  if (countdownTimer) {
    clearInterval(countdownTimer);
    countdownTimer = null;
  }
  if (verifyPollTimer) {
    clearInterval(verifyPollTimer);
    verifyPollTimer = null;
  }
  if (progressBar) progressBar.style.width = "0%";
  if (countdownEl) countdownEl.textContent = "0min 0seg";
  setStatus("Pago verificado. Redirigiendo...");
  redirectToEntregaServicios();
};

const formatCountdown = (msLeft) => {
  const totalSec = Math.max(0, Math.floor(msLeft / 1000));
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return `${min}min ${sec}seg`;
};

const parseCaracasDate = (fechaStr, horaStr) => {
  if (!fechaStr || !horaStr) return null;
  const fechaMatch = String(fechaStr).match(/\d{4}-\d{2}-\d{2}/);
  const horaMatch = String(horaStr).match(/\d{2}:\d{2}:\d{2}/);
  if (!fechaMatch || !horaMatch) return null;
  const iso = `${fechaMatch[0]}T${horaMatch[0]}-04:00`;
  const dt = new Date(iso);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const getElapsedFromOrdenMs = () => {
  if (!orden) return null;
  const dt = parseCaracasDate(orden.fecha, orden.hora_orden);
  if (!dt) return null;
  return Date.now() - dt.getTime();
};

const refreshCurrentRate = async () => {
  const rate = await fetchP2PRate();
  if (Number.isFinite(rate)) {
    currentRateBs = rate;
  }
  return currentRateBs;
};

const updateCountdown = () => {
  if (!orden) return;
  if (!orden.en_espera) {
    if (progressBar) progressBar.style.width = "0%";
    if (countdownEl) countdownEl.textContent = "0min 0seg";
    return;
  }
  const dt = parseCaracasDate(orden.fecha, orden.hora_orden);
  if (!dt) {
    if (countdownEl) countdownEl.textContent = "3min 0seg";
    if (progressBar) progressBar.style.width = "100%";
    return;
  }
  const elapsed = Date.now() - dt.getTime();
  const remaining = Math.max(0, VERIFY_WINDOW_MS - elapsed);
  if (remaining <= 0) {
    if (progressBar) progressBar.style.width = "0%";
    if (countdownEl) countdownEl.textContent = "0min 0seg";
    if (!countdownExpired) {
      countdownExpired = true;
      if (countdownTimer) {
        clearInterval(countdownTimer);
        countdownTimer = null;
      }
      if (!orderProcessed && !orden?.pago_verificado) {
        setStatus(MANUAL_VERIFICATION_PENDING_MSG);
        notifyManualVerificationAdmin("countdown_expired").catch((err) => {
          console.error("notificar admin countdown expired error", err);
        });
      }
    }
    return;
  }
  const pct = Math.max(0, Math.min(100, (remaining / VERIFY_WINDOW_MS) * 100));
  if (progressBar) progressBar.style.width = `${pct}%`;
  if (countdownEl) countdownEl.textContent = formatCountdown(remaining);
};

const startCountdown = () => {
  if (countdownTimer) clearInterval(countdownTimer);
  updateCountdown();
  countdownTimer = setInterval(updateCountdown, 1000);
};

const startVerifyPolling = () => {
  if (verifyPollTimer) clearInterval(verifyPollTimer);
  verifyPollTimer = setInterval(() => {
    if (orderProcessed || !orden?.id_orden || orden?.pago_verificado === true) return;
    triggerVerify().catch((err) => {
      console.warn("verificar pago polling error", err);
    });
  }, VERIFY_POLL_INTERVAL_MS);
};

const renderOrden = () => {
  if (!ordenDisplay) return;
  const id = Number(orden?.id_orden ?? idOrden);
  ordenDisplay.textContent = Number.isFinite(id) && id > 0 ? `#${id}` : "-";
};

const renderRef = () => {
  const ref = orden?.referencia || "";
  if (refDisplay) refDisplay.textContent = ref ? ref : "-";
};

const renderMonto = () => {
  if (!montoDisplay || !orden) return;
  let montoBs = null;
  if (Number.isFinite(Number(orden.monto_bs))) {
    montoBs = Number(orden.monto_bs);
  } else if (Number.isFinite(cartMontoBs)) {
    montoBs = cartMontoBs;
  } else {
    const totalUsd = Number(orden.total);
    const tasaBs = Number.isFinite(currentRateBs) ? currentRateBs : null;
    if (Number.isFinite(totalUsd) && Number.isFinite(tasaBs)) {
      montoBs = Math.round(totalUsd * tasaBs * 100) / 100;
    }
  }
  if (!Number.isFinite(montoBs)) {
    montoDisplay.textContent = "Bs. -";
    return;
  }
  montoDisplay.textContent = `Bs. ${Number(montoBs).toFixed(2)}`;
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
    const refDigits = normalizeReferenceDigits(refRaw);
    if (refDigits.length >= 4) refs.add(refDigits);
  });
  return Array.from(refs);
};

const pagoMatchesLast4 = (pagoRow = {}, last4 = "") => {
  const last4Digits = normalizeReferenceDigits(last4).slice(-4);
  if (last4Digits.length !== 4) return false;
  return extractPagoRefs(pagoRow).some((ref) => ref.slice(-4) === last4Digits);
};

const pickBestPagoMatch = (pagos = [], { last4 = "", montoBaseBs = null } = {}) => {
  const candidates = (Array.isArray(pagos) ? pagos : [])
    .map((row) => ({ row, pagoMonto: montoNum(row?.monto_bs) }))
    .filter(({ row, pagoMonto }) => pagoMatchesLast4(row, last4) && Number.isFinite(pagoMonto));

  if (!candidates.length) return null;
  if (!Number.isFinite(montoBaseBs)) {
    return candidates[0].row;
  }

  candidates.sort((a, b) => {
    const diffA = Math.abs(Number(a.pagoMonto) - Number(montoBaseBs));
    const diffB = Math.abs(Number(b.pagoMonto) - Number(montoBaseBs));
    if (diffA !== diffB) return diffA - diffB;
    return (Number(b.row?.id) || 0) - (Number(a.row?.id) || 0);
  });
  return candidates[0].row;
};

const montoNum = (val) => {
  if (val == null) return null;
  const raw = String(val).trim();
  let clean = raw;
  if (raw.includes(".") && raw.includes(",")) {
    clean = raw.replace(/\./g, "").replace(",", ".");
  } else if (raw.includes(",")) {
    clean = raw.replace(",", ".");
  }
  const num = Number(clean);
  return Number.isFinite(num) ? num : null;
};

const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;

const creditSaldo = async (amountUsd) => {
  const targetUserId = currentUserId || orden?.id_usuario || null;
  if (!targetUserId) return;
  const monto = Number(amountUsd);
  if (!Number.isFinite(monto) || monto <= 0) return;
  const { data: userSaldo, error: saldoErr } = await supabase
    .from("usuarios")
    .select("saldo")
    .eq("id_usuario", targetUserId)
    .maybeSingle();
  if (saldoErr) return;
  const saldoActual = Number(userSaldo?.saldo) || 0;
  const nuevoSaldo = round2(saldoActual + monto);
  await supabase.from("usuarios").update({ saldo: nuevoSaldo }).eq("id_usuario", targetUserId);
};

const verifyPago = async () => {
  if (!orden) return;
  if (processingOrder || orderProcessed) return;
  if (orden?.orden_cancelada) {
    orderProcessed = true;
    setStatus("Orden cancelada.");
    if (processingBlock) processingBlock.classList.add("hidden");
    return;
  }
  if (orden?.pago_verificado) {
    handlePagoVerificado();
    return;
  }
  const refStr = String(orden.referencia || "").trim();
  const refDigits = normalizeReferenceDigits(refStr);
  if (refDigits.length < 4) {
    if (countdownExpired) {
      setStatus(MANUAL_VERIFICATION_PENDING_MSG);
    } else {
      setStatus("Ingresa los últimos 4 dígitos de referencia.");
    }
    return;
  }
  const last4 = refDigits.slice(-4);
  const totalUsd = Number(orden.total);
  const tasaBs = await refreshCurrentRate();
  if (!Number.isFinite(tasaBs) || !Number.isFinite(totalUsd)) {
    if (countdownExpired) {
      setStatus(MANUAL_VERIFICATION_PENDING_MSG);
    } else {
      setStatus("No se pudo obtener la tasa actual o el total.");
    }
    return;
  }

  const ordenMontoBsRaw = Number(orden.monto_bs);
  const montoBsOrden = Number.isFinite(ordenMontoBsRaw)
    ? ordenMontoBsRaw
    : Math.round(totalUsd * tasaBs * 100) / 100;
  const montoBaseBs = Number.isFinite(montoBsOrden) ? montoBsOrden : null;
  const tasaBase = tasaBs;

  const resp = await supabase
    .from("pagomoviles")
    .select("id, referencia, texto, monto_bs, saldo_acreditado")
    .or("saldo_acreditado.is.null,saldo_acreditado.eq.false")
    .order("id", { ascending: false });
  if (resp.error) throw resp.error;
  const pagos = resp.data || [];
  const match = pickBestPagoMatch(pagos, { last4, montoBaseBs });
  if (!match) {
    if (countdownExpired) {
      setStatus(MANUAL_VERIFICATION_PENDING_MSG);
      return;
    }
    const elapsedMs = getElapsedFromOrdenMs();
    if (elapsedMs == null || elapsedMs >= 30 * 1000) {
      setStatus("Seguimos verificando tu pago...");
    }
    return;
  }
  if (match.saldo_acreditado === true) {
    setStatus("Pago ya procesado.");
    return;
  }

  const sessionUserId = currentUserId || requireSession();
  const pagoMonto = montoNum(match.monto_bs);
  if (!Number.isFinite(pagoMonto)) {
    if (countdownExpired) {
      setStatus(MANUAL_VERIFICATION_PENDING_MSG);
    } else {
      setStatus("Seguimos verificando tu pago...");
    }
    return;
  }

  if (sessionUserId) {
    const refMatch = extractPagoRefs(match).find((n) => n.slice(-4) === last4);
    const updates = {
      saldo_acreditado_a: sessionUserId,
      saldo_acreditado: true,
      ...(refMatch ? { referencia: refMatch } : {}),
    };
    await supabase.from("pagomoviles").update(updates).eq("id", match.id);
  }

  if (!Number.isFinite(montoBaseBs)) {
    if (countdownExpired) {
      setStatus(MANUAL_VERIFICATION_PENDING_MSG);
    } else {
      setStatus("No se pudo obtener el monto de la orden.");
    }
    return;
  }
  const diffReal = Number((pagoMonto - montoBaseBs).toFixed(2));
  if (diffReal < 0) {
    if (orden?.recargar_saldo) {
      await supabase
        .from("ordenes")
        .update({ pago_verificado: true, orden_cancelada: true, monto_completo: null, en_espera: false })
        .eq("id_orden", orden.id_orden);
      orden.pago_verificado = true;
      orden.orden_cancelada = true;
      orderProcessed = true;
      if (processingBlock) processingBlock.classList.add("hidden");
      setStatus("Pago insuficiente. Orden cancelada.");
      return;
    }
    const saldoUsd =
      Number.isFinite(montoBaseBs) && Number.isFinite(tasaBase) && tasaBase
        ? Number((montoBaseBs / tasaBase).toFixed(2))
        : null;
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
    await supabase
      .from("ordenes")
      .update({ pago_verificado: true, orden_cancelada: true, monto_completo: null, en_espera: false })
      .eq("id_orden", orden.id_orden);
    orden.pago_verificado = true;
    orden.orden_cancelada = true;
    orderProcessed = true;
    if (processingBlock) processingBlock.classList.add("hidden");
    setStatus("Pago insuficiente. Orden cancelada.");
    return;
  }

  if (diffReal >= 0 && Number.isFinite(tasaBase) && tasaBase) {
    if (diffReal > 0) {
      const saldoUsd = Number(((pagoMonto - montoBaseBs) / tasaBase).toFixed(2));
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
    }
    await supabase
      .from("ordenes")
      .update({ pago_verificado: true, monto_completo: true, orden_cancelada: false })
      .eq("id_orden", orden.id_orden);
  }

  if (orden?.recargar_saldo) {
    const baseUsd = Number(orden.total) || 0;
    const extraUsd =
      diffReal > 0 && Number.isFinite(tasaBase) && tasaBase
        ? Number(((pagoMonto - montoBaseBs) / tasaBase).toFixed(2))
        : 0;
    await creditSaldo(round2(baseUsd + extraUsd));
    orden.en_espera = false;
    orden.pago_verificado = true;
    orderProcessed = true;
    setStatus("Saldo recargado. Redirigiendo...");
    window.location.href = "saldo.html";
    return;
  }

  processingOrder = true;
  const procResp = await procesarOrden(orden.id_orden);
  if (procResp?.error) {
    processingOrder = false;
    setStatus("No se pudo procesar la orden.");
    return;
  }
  orden.en_espera = false;
  orden.pago_verificado = true;
  handlePagoVerificado();
};

const triggerVerify = async () => {
  if (verifyRunning) {
    verifyPending = true;
    return;
  }
  verifyRunning = true;
  try {
    do {
      verifyPending = false;
      await verifyPago();
    } while (verifyPending && !orderProcessed);
  } finally {
    verifyRunning = false;
  }
};

const stopPagosInsertSubscription = () => {
  if (!pagosInsertChannel) return;
  try {
    supabase.removeChannel(pagosInsertChannel);
  } catch (err) {
    console.warn("remove pagomoviles channel error", err);
  }
  pagosInsertChannel = null;
};

const stopOrdenUpdateSubscription = () => {
  if (!ordenUpdateChannel) return;
  try {
    supabase.removeChannel(ordenUpdateChannel);
  } catch (err) {
    console.warn("remove orden channel error", err);
  }
  ordenUpdateChannel = null;
};

const startPagosInsertSubscription = () => {
  if (pagosInsertChannel) return;
  pagosInsertChannel = supabase
    .channel("verificando-pago-pagomoviles-insert")
    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "pagomoviles" },
      () => {
        triggerVerify().catch((err) => {
          console.error("verificar pago por insercion error", err);
        });
      }
    )
    .subscribe((status) => {
      if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        if (shouldLogChannelWarning("pagomoviles")) {
          console.warn("pagomoviles subscription status", status);
        }
      }
    });
};

const bindEdit = () => {
  btnEditRef?.addEventListener("click", () => {
    if (!refEditWrap) return;
    refEditWrap.classList.remove("hidden");
    if (refInput) {
      refInput.value = orden?.referencia || "";
      refInput.focus();
    }
  });
  btnSaveRef?.addEventListener("click", async () => {
    const val = String(refInput?.value || "").trim();
    if (!val) {
      setStatus("Ingresa la referencia.");
      return;
    }
    const valDigits = normalizeReferenceDigits(val);
    if (valDigits.length < 4) {
      setStatus("La referencia debe tener al menos 4 dígitos.");
      return;
    }
    try {
      const { error } = await supabase
        .from("ordenes")
        .update({ referencia: valDigits })
        .eq("id_orden", orden.id_orden);
      if (error) throw error;
      orden.referencia = valDigits;
      renderRef();
      if (refEditWrap) refEditWrap.classList.add("hidden");
      setStatus("Referencia actualizada.");
    } catch (err) {
      console.error("update referencia error", err);
      setStatus("No se pudo actualizar la referencia.");
    }
  });
};

const startOrdenUpdateSubscription = () => {
  if (ordenUpdateChannel || !idOrden) return;
  ordenUpdateChannel = supabase
    .channel(`verificando-pago-orden-${idOrden}`)
    .on(
      "postgres_changes",
      { event: "UPDATE", schema: "public", table: "ordenes", filter: `id_orden=eq.${idOrden}` },
      (payload) => {
        const updated = payload?.new || null;
        if (!updated) return;
        orden = { ...(orden || {}), ...updated };
        renderRef();
        renderOrden();
        renderMonto();
        if (updated.pago_verificado === true) {
          handlePagoVerificado();
        }
      }
    )
    .subscribe((status) => {
      if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        if (shouldLogChannelWarning("orden")) {
          console.warn("orden subscription status", status);
        }
      }
    });
};

async function init() {
  try {
    if (!idOrden) {
      setStatus("Orden no encontrada.");
      return;
    }
    const user = await loadCurrentUser();
    currentUserId = user?.id_usuario || null;
    const { data, error } = await supabase
      .from("ordenes")
      .select(
        "id_orden, id_carrito, id_usuario, referencia, total, monto_bs, en_espera, hora_orden, fecha, id_metodo_de_pago, pago_verificado, monto_completo, orden_cancelada, recargar_saldo"
      )
      .eq("id_orden", idOrden)
      .single();
    if (error) throw error;
    orden = data;
    countdownExpired = false;
    manualVerificationNotified = false;
    if (orden?.id_carrito) {
      const { data: cartData, error: cartErr } = await supabase
        .from("carritos")
        .select("monto_bs")
        .eq("id_carrito", orden.id_carrito)
        .single();
      if (!cartErr) {
        const mb = Number(cartData?.monto_bs);
        cartMontoBs = Number.isFinite(mb) ? mb : null;
      }
    }
    await refreshCurrentRate();
    renderRef();
    renderOrden();
    renderMonto();
    if (orden?.pago_verificado) {
      handlePagoVerificado();
      return;
    }
    bindEdit();
    startCountdown();
    startVerifyPolling();
    triggerVerify().catch((err) => {
      console.error("verificar pago inicial error", err);
    });
    startPagosInsertSubscription();
    startOrdenUpdateSubscription();
  } catch (err) {
    console.error("verificando pago init error", err);
    setStatus("No se pudo cargar la orden.");
  }
}

init();

window.addEventListener("beforeunload", () => {
  stopPagosInsertSubscription();
  stopOrdenUpdateSubscription();
  if (countdownTimer) clearInterval(countdownTimer);
  if (verifyPollTimer) clearInterval(verifyPollTimer);
});
