import { loadCurrentUser } from "./api.js";
import { requireSession } from "./session.js";

requireSession();

const saldoEl = document.querySelector("#saldo-display");

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
