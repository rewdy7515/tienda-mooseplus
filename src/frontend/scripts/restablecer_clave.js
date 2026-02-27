import { supabase } from "./api.js";
import { attachLogoHome } from "./session.js";
import { loadPaginaBranding } from "./branding.js";

const formEl = document.getElementById("reset-password-form");
const passwordEl = document.getElementById("reset-password");
const passwordConfirmEl = document.getElementById("reset-password-confirm");
const passwordErrorEl = document.getElementById("reset-password-error");
const passwordConfirmErrorEl = document.getElementById("reset-password-confirm-error");
const statusEl = document.getElementById("reset-password-status");
const submitEl = document.getElementById("reset-password-submit");
const toggleButtons = document.querySelectorAll(".toggle-password");

const MIN_PASSWORD_LEN = 6;

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function clearFeedback() {
  if (passwordErrorEl) passwordErrorEl.textContent = "";
  if (passwordConfirmErrorEl) passwordConfirmErrorEl.textContent = "";
  if (statusEl) {
    statusEl.textContent = "";
    statusEl.classList.remove("is-error", "is-success");
  }
  passwordEl?.classList.remove("input-error");
  passwordConfirmEl?.classList.remove("input-error");
}

function setStatus(message, isError = false) {
  if (!statusEl) return;
  statusEl.textContent = message;
  statusEl.classList.toggle("is-error", isError);
  statusEl.classList.toggle("is-success", !isError);
}

function setLoading(isLoading) {
  if (!submitEl) return;
  submitEl.disabled = isLoading;
  submitEl.textContent = isLoading ? "Restableciendo..." : "Restablecer contraseña";
}

function disableForm() {
  formEl?.querySelectorAll("input.form-input").forEach((el) => {
    el.readOnly = true;
    el.classList.add("input-disabled");
  });
  if (submitEl) submitEl.disabled = true;
}

function initToggleButtons() {
  toggleButtons.forEach((btn) => {
    const wrapper = btn.closest(".input-with-toggle");
    const targetId = btn.dataset.target || "";
    const target =
      document.getElementById(targetId) || wrapper?.querySelector("input.form-input");
    if (!target) return;
    btn.addEventListener("click", () => {
      if (btn.disabled) return;
      const show = target.type === "password";
      target.type = show ? "text" : "password";
      btn.classList.toggle("is-hidden", target.type === "text");
      btn.setAttribute("aria-label", target.type === "text" ? "Ocultar clave" : "Mostrar clave");
      btn.setAttribute("aria-pressed", String(target.type === "text"));
    });
  });
}

function validatePasswords() {
  clearFeedback();
  const password = passwordEl?.value || "";
  const confirm = passwordConfirmEl?.value || "";

  let hasError = false;
  if (!password || password.length < MIN_PASSWORD_LEN) {
    if (passwordErrorEl) {
      passwordErrorEl.textContent = `La clave debe tener al menos ${MIN_PASSWORD_LEN} caracteres.`;
    }
    passwordEl?.classList.add("input-error");
    hasError = true;
  }
  if (!confirm) {
    if (passwordConfirmErrorEl) passwordConfirmErrorEl.textContent = "Confirma tu clave.";
    passwordConfirmEl?.classList.add("input-error");
    hasError = true;
  } else if (password !== confirm) {
    if (passwordConfirmErrorEl) passwordConfirmErrorEl.textContent = "Las claves no coinciden.";
    passwordConfirmEl?.classList.add("input-error");
    hasError = true;
  }
  return { hasError, password };
}

async function tryHydrateRecoverySession() {
  const url = new URL(window.location.href);
  const code = url.searchParams.get("code");
  const tokenHash = url.searchParams.get("token_hash");
  const type = (url.searchParams.get("type") || "").toLowerCase();
  let authErr = null;

  if (code) {
    const { error } = await supabase.auth.exchangeCodeForSession(code);
    if (error) authErr = error;
  } else if (tokenHash && type === "recovery") {
    const { error } = await supabase.auth.verifyOtp({
      type: "recovery",
      token_hash: tokenHash,
    });
    if (error) authErr = error;
  }

  let session = null;
  const { data, error: sessErr } = await supabase.auth.getSession();
  if (sessErr) authErr = authErr || sessErr;
  session = data?.session || null;

  if (!session && window.location.hash.includes("access_token")) {
    await wait(120);
    const { data: dataRetry } = await supabase.auth.getSession();
    session = dataRetry?.session || null;
  }

  if (session) {
    const clean = new URL(window.location.href);
    clean.searchParams.delete("code");
    clean.searchParams.delete("token_hash");
    clean.searchParams.delete("type");
    clean.hash = "";
    window.history.replaceState({}, "", `${clean.pathname}${clean.search}`);
  }

  return { session, error: authErr };
}

async function handleSubmit(event) {
  event.preventDefault();
  const { hasError, password } = validatePasswords();
  if (hasError) return;

  setLoading(true);
  try {
    const { error } = await supabase.auth.updateUser({ password });
    if (error) throw error;

    setStatus("Clave actualizada correctamente. Redirigiendo...", false);
    await supabase.auth.signOut();
    setTimeout(() => {
      window.location.href = "login.html";
    }, 900);
  } catch (err) {
    console.error("reset password update error", err);
    setStatus("No se pudo actualizar la clave. Solicita un nuevo enlace.", true);
  } finally {
    setLoading(false);
  }
}

async function init() {
  attachLogoHome();
  loadPaginaBranding({ logoSelectors: [".auth-logo"], applyFavicon: true }).catch((err) => {
    console.warn("reset password branding load error", err);
  });
  initToggleButtons();

  try {
    const { session, error } = await tryHydrateRecoverySession();
    if (!session) {
      if (error) console.warn("recovery session error", error);
      disableForm();
      setStatus(
        "El enlace de recuperación es inválido o venció. Vuelve a solicitar uno desde iniciar sesión.",
        true,
      );
      return;
    }
  } catch (err) {
    console.error("recovery init error", err);
    disableForm();
    setStatus(
      "No se pudo validar el enlace de recuperación. Solicita uno nuevo e intenta de nuevo.",
      true,
    );
    return;
  }

  formEl?.addEventListener("submit", handleSubmit);
}

init();
