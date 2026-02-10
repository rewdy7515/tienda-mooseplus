import { loadCurrentUser, supabase, fetchP2PRate } from "./api.js";
import { requireSession } from "./session.js";
import { TASA_MARKUP } from "./rate-config.js";

requireSession();

const saldoEl = document.querySelector("#saldo-display");
const saldoInput = document.querySelector("#saldo-input");
const saldoCheckoutBtn = document.querySelector("#saldo-checkout");

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
      })
      .select("id_orden")
      .single();
    if (error) throw error;
    window.location.href = "checkout.html";
  } catch (err) {
    console.error("saldo checkout error", err);
    alert("No se pudo crear la orden.");
  }
});
