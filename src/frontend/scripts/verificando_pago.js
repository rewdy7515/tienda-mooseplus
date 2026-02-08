import { requireSession, attachLogout, attachLogoHome } from "./session.js";
import { clearServerSession, supabase, loadCurrentUser } from "./api.js";

requireSession();
attachLogoHome();
attachLogout(clearServerSession);

const statusEl = document.querySelector("#verif-status");
const processingBlock = document.querySelector("#processing-block");
const refDisplay = document.querySelector("#ref-display");
const refEditWrap = document.querySelector("#ref-edit-wrap");
const refInput = document.querySelector("#ref-input");
const btnEditRef = document.querySelector("#btn-edit-ref");
const btnSaveRef = document.querySelector("#btn-save-ref");
const progressBar = document.querySelector("#progress-bar");
const countdownEl = document.querySelector("#countdown");

const params = new URLSearchParams(window.location.search);
const idOrden = Number(params.get("id_orden"));

let orden = null;
let verifyTimer = null;
let countdownTimer = null;
let currentUserId = null;
let expired = false;

const setStatus = (msg) => {
  if (statusEl) statusEl.textContent = msg || "";
};

const formatCountdown = (msLeft) => {
  const totalSec = Math.max(0, Math.floor(msLeft / 1000));
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return `${min}min ${sec}seg`;
};

const parseCaracasDate = (fechaStr, horaStr) => {
  if (!fechaStr || !horaStr) return null;
  const iso = `${fechaStr}T${horaStr}-04:00`;
  const dt = new Date(iso);
  return Number.isNaN(dt.getTime()) ? null : dt;
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

const verifyPago = async () => {
  if (!orden) return;
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

  const montoBsOrden = Math.round(totalUsd * tasaBs * 100) / 100;

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
    setStatus("Pago no encontrado. Reintentando...");
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

  const diff = Number((pagoMonto - montoBsOrden).toFixed(2));
  if (diff !== 0) {
    if (diff > 0 && Number.isFinite(tasaBs) && tasaBs) {
      const saldoUsd = Number(((pagoMonto - montoBsOrden) / tasaBs).toFixed(2));
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
        .update({ en_espera: false })
        .eq("id_orden", orden.id_orden);
      orden.en_espera = false;
      setStatus("Pago verificado. Redirigiendo...");
      window.location.href = `entregar_servicios.html?id_orden=${encodeURIComponent(orden.id_orden)}`;
      return;
    } else {
      setStatus("Pago verificado menor al del carrito. Saldo acreditado a su perfil.");
      return;
    }
  }

  await supabase
    .from("ordenes")
    .update({ en_espera: false })
    .eq("id_orden", orden.id_orden);
  orden.en_espera = false;

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
      .select("id_orden, referencia, total, tasa_bs, en_espera, hora_orden, fecha, id_metodo_de_pago")
      .eq("id_orden", idOrden)
      .single();
    if (error) throw error;
    orden = data;
    renderRef();
    bindEdit();
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
