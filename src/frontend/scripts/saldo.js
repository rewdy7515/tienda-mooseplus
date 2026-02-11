import { loadCurrentUser, supabase, fetchP2PRate } from "./api.js";
import { requireSession } from "./session.js";
import { TASA_MARKUP } from "./rate-config.js";

requireSession();

const saldoEl = document.querySelector("#saldo-display");
const saldoInput = document.querySelector("#saldo-input");
const saldoCheckoutBtn = document.querySelector("#saldo-checkout");

const getCaracasParts = () => {
  const caracasNow = new Date(
    new Date().toLocaleString("en-US", { timeZone: "America/Caracas" })
  );
  const pad2 = (val) => String(val).padStart(2, "0");
  const fecha = `${caracasNow.getFullYear()}-${pad2(caracasNow.getMonth() + 1)}-${pad2(
    caracasNow.getDate()
  )}`;
  const hora = `${pad2(caracasNow.getHours())}:${pad2(caracasNow.getMinutes())}:${pad2(
    caracasNow.getSeconds()
  )}`;
  return { fecha, hora };
};

const initSaldo = async () => {
  try {
    const user = await loadCurrentUser();
    const saldoVal = Number(user?.saldo);
    const saldoNum = Number.isFinite(saldoVal) ? saldoVal : 0;
    if (saldoEl) saldoEl.textContent = `Saldo: $${saldoNum.toFixed(2)}`;
  } catch (err) {
    console.error("saldo load error", err);
  }
};

initSaldo();

if (saldoInput) {
  saldoInput.value = "";
  saldoInput.addEventListener("input", () => {
    const raw = saldoInput.value || "";
    let normalized = raw.replace(/,/g, ".");
    normalized = normalized.replace(/[^\d.]/g, "");
    const firstDot = normalized.indexOf(".");
    if (firstDot !== -1) {
      normalized =
        normalized.slice(0, firstDot + 1) +
        normalized.slice(firstDot + 1).replace(/\./g, "");
      const decimal = normalized.slice(firstDot + 1);
      if (decimal.length > 2) {
        normalized = normalized.slice(0, firstDot + 1 + 2);
      }
    }
    if (normalized !== raw) saldoInput.value = normalized;
  });
}

saldoCheckoutBtn?.addEventListener("click", async () => {
  try {
    const total = Number(saldoInput?.value);
    if (!Number.isFinite(total) || total <= 0) {
      alert("Ingresa un monto válido.");
      return;
    }
    const tasaResp = await fetchP2PRate();
    const tasaBs = tasaResp ? Math.round(tasaResp * TASA_MARKUP * 100) / 100 : null;
    if (!Number.isFinite(tasaBs)) {
      alert("No se pudo obtener la tasa.");
      return;
    }
    const user = await loadCurrentUser();
    const userId = user?.id_usuario;
    if (!userId) {
      alert("Usuario no autenticado.");
      return;
    }
    const { fecha, hora } = getCaracasParts();
    const montoBs = Math.round(total * tasaBs * 100) / 100;
    const { data: orden, error } = await supabase
      .from("ordenes")
      .insert({
        id_usuario: userId,
        total,
        tasa_bs: tasaBs,
        monto_bs: montoBs,
        en_espera: true,
        id_carrito: null,
        recargar_saldo: true,
        fecha,
        hora_orden: hora,
      })
      .select("id_orden")
      .single();
    if (error) throw error;
    const idOrden = orden?.id_orden;
    if (idOrden) {
      const { fecha: fechaUpd, hora: horaUpd } = getCaracasParts();
      await supabase
        .from("ordenes")
        .update({ fecha: fechaUpd, hora_orden: horaUpd })
        .eq("id_orden", idOrden);
    }
    const nextUrl = idOrden
      ? `checkout.html?id_orden=${encodeURIComponent(idOrden)}&from=saldo`
      : "checkout.html";
    window.location.href = nextUrl;
  } catch (err) {
    console.error("saldo checkout error", err);
    alert("No se pudo crear la orden.");
  }
});
