import { supabase, startSession, fetchCart } from "./api.js";
import { setSessionUserId, setCachedCart, setSessionRoles, attachLogoHome } from "./session.js";
import { loadPaginaBranding } from "./branding.js";
import { initAuthCaptcha } from "./auth-captcha.js";

const form = document.querySelector(".auth-form");
const emailInput = document.getElementById("login-user");
const passwordInput = document.getElementById("login-password");
const emailError = document.getElementById("email-error");
const passwordError = document.getElementById("password-error");
const statusMessage = document.getElementById("login-status");
const toggleBtn = document.querySelector(".toggle-password");
const signupBtn = document.getElementById("login-signup-link");
const forgotBtn = document.getElementById("login-forgot-link");
const submitBtn = document.querySelector(".auth-submit");
let forgotLoading = false;
let captchaController = null;
let captchaInitPromise = null;

function clearFeedback() {
  emailError.textContent = "";
  passwordError.textContent = "";
  const captchaError = document.getElementById("login-captcha-error");
  if (captchaError) captchaError.textContent = "";
  statusMessage.textContent = "";
  emailInput.classList.remove("input-error");
  passwordInput.classList.remove("input-error");
  statusMessage.classList.remove("is-error", "is-success");
}

function setStatus(message, isError = false) {
  statusMessage.textContent = message;
  statusMessage.classList.toggle("is-error", isError);
  statusMessage.classList.toggle("is-success", !isError);
}

function setLoading(isLoading) {
  if (!submitBtn) return;
  submitBtn.disabled = isLoading;
  submitBtn.textContent = isLoading ? "Iniciando..." : "Iniciar sesión";
}

function maybeShowEmailConfirmedMessage() {
  const params = new URLSearchParams(window.location.search || "");
  const confirmed = String(params.get("email_confirmed") || "").trim();
  if (confirmed !== "1") return;
  setStatus("Correo verificado. Ahora inicia sesión.", false);
}

function getResetRedirectUrl() {
  return new URL("restablecer_clave.html", window.location.href).toString();
}

async function requireCaptchaToken() {
  const ctrl = captchaController || (captchaInitPromise ? await captchaInitPromise : null);
  if (!ctrl || !ctrl.enabled) {
    setStatus("Captcha no disponible. Recarga la página e intenta de nuevo.", true);
    return null;
  }
  return ctrl.ensureToken();
}

async function handleForgotPassword(event) {
  event.preventDefault();
  if (forgotLoading) return;
  clearFeedback();

  const email = emailInput.value.trim().toLowerCase();
  if (!email || !emailInput.checkValidity()) {
    emailError.textContent = "Ingresa tu correo para restablecer la clave.";
    emailInput.classList.add("input-error");
    return;
  }

  const captchaToken = await requireCaptchaToken();
  if (!captchaToken) return;

  forgotLoading = true;
  if (forgotBtn) {
    forgotBtn.classList.add("input-disabled");
    forgotBtn.setAttribute("aria-disabled", "true");
    forgotBtn.style.pointerEvents = "none";
  }
  try {
    const { error } = await supabase.auth.resetPasswordForEmail(email, {
      redirectTo: getResetRedirectUrl(),
      captchaToken,
    });
    if (error) throw error;
    setStatus("Te enviamos un enlace para restablecer tu contraseña.", false);
  } catch (err) {
    console.error("forgot password error", err);
    setStatus("No se pudo enviar el enlace de recuperación.", true);
  } finally {
    forgotLoading = false;
    if (forgotBtn) {
      forgotBtn.classList.remove("input-disabled");
      forgotBtn.removeAttribute("aria-disabled");
      forgotBtn.style.pointerEvents = "";
    }
    captchaController?.reset();
  }
}

async function handleLogin(event) {
  event.preventDefault();
  clearFeedback();

  const email = emailInput.value.trim().toLowerCase();
  const password = passwordInput.value;

  if (!email || !emailInput.checkValidity()) {
    emailError.textContent = "Ingresa un correo válido.";
    emailInput.classList.add("input-error");
    return;
  }

  if (!password) {
    passwordError.textContent = "Ingresa tu clave.";
    passwordInput.classList.add("input-error");
    return;
  }

  const captchaToken = await requireCaptchaToken();
  if (!captchaToken) return;

  setLoading(true);

  try {
    // 1) Iniciar sesión en Supabase Auth
    const { data: authData, error: authErr } = await supabase.auth.signInWithPassword({
      email,
      password,
      options: {
        captchaToken,
      },
    });
    if (authErr) {
      const msg = (authErr.message || "").toLowerCase();
      if (msg.includes("email not confirmed")) {
        setStatus("Debes verificar tu correo antes de iniciar sesión.", true);
        return;
      }
      if (msg.includes("invalid login credentials")) {
        try {
          const { data: userRow } = await supabase
            .from("usuarios")
            .select("id_usuario")
            .ilike("correo", email)
            .maybeSingle();
          if (!userRow) {
            emailError.innerHTML =
              'Correo no registrado. <a class="link-inline" href="signup.html">Registrate aquí</a>';
            emailInput.classList.add("input-error");
          } else {
            passwordInput.value = "";
            passwordError.textContent = "Contraseña incorrecta";
            passwordInput.classList.add("input-error");
          }
        } catch (_) {
          passwordInput.value = "";
          passwordError.textContent = "Contraseña incorrecta";
          passwordInput.classList.add("input-error");
        }
        return;
      }
      setStatus("No se pudo iniciar sesión. Intenta de nuevo.", true);
      return;
    }

    if (!authData?.user?.email_confirmed_at) {
      await supabase.auth.signOut().catch(() => {});
      setStatus("Debes verificar tu correo antes de iniciar sesión.", true);
      return;
    }

    // 2) Establecer cookie de sesión backend con token real de Supabase Auth.
    //    Aquí se vincula/crea el registro en `usuarios` si aún no existía.
    const serverSession = await startSession();
    if (serverSession?.error) {
      setStatus("No se pudo establecer la sesión segura. Intenta de nuevo.", true);
      return;
    }
    const idUsuarioServer = Number(serverSession?.id_usuario) || 0;
    if (!idUsuarioServer) {
      setStatus("No se pudo identificar tu usuario. Intenta de nuevo.", true);
      return;
    }

    // 3) Cargar usuario final desde tabla `usuarios`
    const { data: user, error } = await supabase
      .from("usuarios")
      .select("id_usuario, acceso_cliente, permiso_admin, permiso_superadmin")
      .eq("id_usuario", idUsuarioServer)
      .maybeSingle();

    if (error) {
      throw error;
    }

    // Si no existe en tabla usuarios, avisar y cortar
    if (!user) {
      setStatus("No se pudo cargar tu usuario. Intenta nuevamente.", true);
      return;
    }

    // 4) Setear sesión local con id_usuario de la tabla
    setSessionUserId(user.id_usuario);
    setSessionRoles({
      acceso_cliente: user.acceso_cliente,
      permiso_admin: user.permiso_admin,
      permiso_superadmin: user.permiso_superadmin,
    });

    try {
      const cartData = await fetchCart();
      setCachedCart(cartData);
    } catch (err) {
      console.warn("No se pudo precargar carrito", err);
    }
    setTimeout(() => {
      window.location.href = "index.html";
    }, 400);
  } catch (err) {
    console.error("Login error:", err);
    setStatus("No se pudo iniciar sesión. Intenta de nuevo.", true);
  } finally {
    setLoading(false);
    captchaController?.reset();
  }
}

function initToggle() {
  if (!toggleBtn || !passwordInput) return;
  toggleBtn.addEventListener("click", () => {
    const willShow = passwordInput.type === "password";
    passwordInput.type = willShow ? "text" : "password";
    const isVisible = passwordInput.type === "text";
    toggleBtn.classList.toggle("is-hidden", isVisible);
    toggleBtn.setAttribute("aria-label", isVisible ? "Ocultar clave" : "Mostrar clave");
    toggleBtn.setAttribute("aria-pressed", String(isVisible));
  });
}

function initSignupRedirect() {
  if (!signupBtn) return;
  signupBtn.addEventListener("click", (e) => {
    e.preventDefault();
    window.location.href = "signup.html";
  });
}

function init() {
  maybeShowEmailConfirmedMessage();
  captchaInitPromise = initAuthCaptcha({
    containerId: "login-captcha",
    errorId: "login-captcha-error",
  }).then((ctrl) => {
    captchaController = ctrl;
    return ctrl;
  });
  initToggle();
  initSignupRedirect();
  forgotBtn?.addEventListener("click", handleForgotPassword);
  if (form) {
    form.addEventListener("submit", handleLogin);
  }
  attachLogoHome();
  loadPaginaBranding({ logoSelectors: [".auth-logo"], applyFavicon: true }).catch((err) => {
    console.warn("login branding load error", err);
  });
}

init();
