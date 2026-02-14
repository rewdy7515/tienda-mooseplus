import { requireSession, attachLogout, attachLogoHome } from "./session.js";
import { clearServerSession, supabase, loadCurrentUser, procesarOrden } from "./api.js";

requireSession();
attachLogoHome();
attachLogout(clearServerSession);

const statusEl = document.querySelector("#verif-status");
const processingBlock = document.querySelector("#processing-block");
const refDisplay = document.querySelector("#ref-display");
const montoDisplay = document.querySelector("#monto-display");
const refEditWrap = document.querySelector("#ref-edit-wrap");
const refInput = document.querySelector("#ref-input");
const btnEditRef = document.querySelector("#btn-edit-ref");
const btnSaveRef = document.querySelector("#btn-save-ref");
const progressBar = document.querySelector("#progress-bar");
const countdownEl = document.querySelector("#countdown");
const retryActions = document.querySelector("#retry-actions");
const btnRetry = document.querySelector("#btn-retry");
const btnBackCart = document.querySelector("#btn-back-cart");

const params = new URLSearchParams(window.location.search);
const idOrden = Number(params.get("id_orden"));

let orden = null;
let verifyTimer = null;
let countdownTimer = null;
let currentUserId = null;
let expired = false;
let processingOrder = false;
let orderProcessed = false;
let cartMontoBs = null;
let cartTasaBs = null;

const setStatus = (msg) => {
  if (statusEl) statusEl.textContent = msg || "";
};

const toggleRetryActions = (show) => {
  if (!retryActions) return;
  retryActions.classList.toggle("hidden", !show);
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

const getCaracasParts = () => {
  const caracasNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
  const pad2 = (val) => String(val).padStart(2, "0");
  const fecha = `${caracasNow.getFullYear()}-${pad2(caracasNow.getMonth() + 1)}-${pad2(
    caracasNow.getDate()
  )}`;
  const hora = `${pad2(caracasNow.getHours())}:${pad2(caracasNow.getMinutes())}:${pad2(
    caracasNow.getSeconds()
  )}`;
  return { fecha, hora };
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
    if (countdownEl) countdownEl.textContent = "2min 0seg";
    if (progressBar) progressBar.style.width = "100%";
    return;
  }
  const elapsed = Date.now() - dt.getTime();
  const remaining = Math.max(0, 2 * 60 * 1000 - elapsed);
  if (remaining <= 0 && !expired) {
    expired = true;
    if (processingBlock) processingBlock.classList.add("hidden");
    if (progressBar) progressBar.style.width = "0%";
    if (countdownEl) countdownEl.textContent = "0min 0seg";
    setStatus("Pago no encontrado");
    toggleRetryActions(true);
    if (verifyTimer) {
      clearInterval(verifyTimer);
      verifyTimer = null;
    }
  }
  const pct = Math.max(0, Math.min(100, (remaining / (2 * 60 * 1000)) * 100));
  if (progressBar) progressBar.style.width = `${pct}%`;
  if (countdownEl) countdownEl.textContent = formatCountdown(remaining);
};

const startCountdown = () => {
  if (countdownTimer) clearInterval(countdownTimer);
  updateCountdown();
  countdownTimer = setInterval(updateCountdown, 1000);
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
    const tasaBs = Number(orden.tasa_bs);
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
  if (expired || processingOrder || orderProcessed) return;
  toggleRetryActions(false);
  if (orden?.orden_cancelada) {
    orderProcessed = true;
    setStatus("Orden cancelada.");
    if (processingBlock) processingBlock.classList.add("hidden");
    toggleRetryActions(false);
    return;
  }
  if (orden?.pago_verificado) {
    orderProcessed = true;
    setStatus("Pago verificado. Redirigiendo...");
    toggleRetryActions(false);
    window.location.href = `entregar_servicios.html?id_orden=${encodeURIComponent(orden.id_orden)}`;
    return;
  }
  const refStr = String(orden.referencia || "").trim();
  if (!refStr || refStr.length < 4) {
    setStatus("Ingresa los últimos 4 dígitos de referencia.");
    return;
  }
  const last4 = refStr.slice(-4);
  const tasaBs = Number(orden.tasa_bs);
  const totalUsd = Number(orden.total);
  if (!Number.isFinite(tasaBs) || !Number.isFinite(totalUsd)) {
    setStatus("No se pudo obtener la tasa o el total.");
    return;
  }

  const ordenMontoBsRaw = Number(orden.monto_bs);
  const montoBsOrden = Number.isFinite(ordenMontoBsRaw)
    ? ordenMontoBsRaw
    : Math.round(totalUsd * tasaBs * 100) / 100;
  const montoBaseBs = Number.isFinite(montoBsOrden) ? montoBsOrden : null;
  const tasaBase = Number.isFinite(tasaBs) ? tasaBs : cartTasaBs;

  const resp = await supabase
    .from("pagomoviles")
    .select("id, referencia, texto, monto_bs, saldo_acreditado")
    .or("saldo_acreditado.is.null,saldo_acreditado.eq.false")
    .order("id", { ascending: false });
  if (resp.error) throw resp.error;
  const pagos = resp.data || [];
  const match = pagos.find((p) => {
    const textoRefs = extractRefMatches(p.texto || "");
    const refMatch = textoRefs.some((n) => n.slice(-4) === last4);
    if (!refMatch) return false;
    const pagoMonto = montoNum(p.monto_bs);
    return Number.isFinite(pagoMonto);
  });
  if (!match) {
    const elapsedMs = getElapsedFromOrdenMs();
    if (elapsedMs == null || elapsedMs >= 30 * 1000) {
      setStatus("Pago no encontrado. Reintentando...");
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
    setStatus("Pago no encontrado.");
    return;
  }

  if (sessionUserId) {
    const textoRefs = extractRefMatches(match.texto || "");
    const refMatch = textoRefs.find((n) => n.slice(-4) === last4);
    const updates = {
      saldo_acreditado_a: sessionUserId,
      saldo_acreditado: true,
      ...(refMatch ? { referencia: refMatch } : {}),
    };
    await supabase.from("pagomoviles").update(updates).eq("id", match.id);
  }

  if (!Number.isFinite(montoBaseBs)) {
    setStatus("No se pudo obtener el monto de la orden.");
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
      toggleRetryActions(false);
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
    toggleRetryActions(false);
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
  orderProcessed = true;

  setStatus("Pago verificado. Redirigiendo...");
  window.location.href = `entregar_servicios.html?id_orden=${encodeURIComponent(orden.id_orden)}`;
};

const startVerifyLoop = () => {
  if (verifyTimer) clearInterval(verifyTimer);
  verifyTimer = setInterval(() => {
    verifyPago().catch((err) => {
      console.error("verificar pago error", err);
    });
  }, 10000);
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
    try {
      const { error } = await supabase
        .from("ordenes")
        .update({ referencia: val })
        .eq("id_orden", orden.id_orden);
      if (error) throw error;
      orden.referencia = val;
      renderRef();
      if (refEditWrap) refEditWrap.classList.add("hidden");
      setStatus("Referencia actualizada.");
    } catch (err) {
      console.error("update referencia error", err);
      setStatus("No se pudo actualizar la referencia.");
    }
  });
};

const bindRetryActions = () => {
  btnRetry?.addEventListener("click", async () => {
    if (!orden) return;
    try {
      const { fecha, hora } = getCaracasParts();
      const { error } = await supabase
        .from("ordenes")
        .update({ hora_orden: hora, fecha, en_espera: true })
        .eq("id_orden", orden.id_orden);
      if (error) throw error;
      orden.hora_orden = hora;
      orden.fecha = fecha;
      orden.en_espera = true;
      expired = false;
      if (processingBlock) processingBlock.classList.remove("hidden");
      toggleRetryActions(false);
      setStatus("Reintentando...");
      startCountdown();
      startVerifyLoop();
    } catch (err) {
      console.error("reintentar orden error", err);
      setStatus("No se pudo reintentar.");
    }
  });
  btnBackCart?.addEventListener("click", () => {
    (async () => {
      if (orden?.id_orden) {
        try {
          await supabase
            .from("ordenes")
            .update({ orden_cancelada: true })
            .eq("id_orden", orden.id_orden);
          orden.orden_cancelada = true;
        } catch (err) {
          console.error("cancelar orden error", err);
        }
      }
      window.location.href = "cart.html";
    })();
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
        "id_orden, id_carrito, id_usuario, referencia, total, tasa_bs, monto_bs, en_espera, hora_orden, fecha, id_metodo_de_pago, pago_verificado, monto_completo, orden_cancelada, recargar_saldo"
      )
      .eq("id_orden", idOrden)
      .single();
    if (error) throw error;
    orden = data;
    if (orden?.id_carrito) {
      const { data: cartData, error: cartErr } = await supabase
        .from("carritos")
        .select("monto_bs, tasa_bs")
        .eq("id_carrito", orden.id_carrito)
        .single();
      if (!cartErr) {
        const mb = Number(cartData?.monto_bs);
        const tb = Number(cartData?.tasa_bs);
        cartMontoBs = Number.isFinite(mb) ? mb : null;
        cartTasaBs = Number.isFinite(tb) ? tb : null;
      }
    }
    renderRef();
    renderMonto();
    bindEdit();
    bindRetryActions();
    startCountdown();
    verifyPago().catch((err) => {
      console.error("verificar pago inicial error", err);
    });
    startVerifyLoop();
  } catch (err) {
    console.error("verificando pago init error", err);
    setStatus("No se pudo cargar la orden.");
  }
}

init();

window.addEventListener("beforeunload", () => {
  if (verifyTimer) clearInterval(verifyTimer);
  if (countdownTimer) clearInterval(countdownTimer);
});
