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
const LOGIN_DEBUG_PREFIX = "[login-debug]";

const getDebugNow = () =>
  typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();

const getElapsedMs = (startedAt) => Math.round(getDebugNow() - startedAt);

const maskEmail = (value = "") => {
  const email = String(value || "").trim().toLowerCase();
  const [localPart, domain] = email.split("@");
  if (!localPart || !domain) return email;
  if (localPart.length <= 2) return `${localPart[0] || "*"}***@${domain}`;
  return `${localPart.slice(0, 2)}***@${domain}`;
};

const summarizeError = (err) => ({
  name: err?.name || "",
  status: Number(err?.status) || 0,
  code: err?.code || "",
  message: err?.message || "",
});

const logLoginDebug = (step, details = {}) => {
  console.info(`${LOGIN_DEBUG_PREFIX} ${step}`, details);
};

const warnLoginDebug = (step, details = {}) => {
  console.warn(`${LOGIN_DEBUG_PREFIX} ${step}`, details);
};

const traceLoginStep = async (step, work, { slowMs = 8000, meta = null } = {}) => {
  const startedAt = getDebugNow();
  const details = typeof meta === "function" ? meta() : meta || {};
  logLoginDebug(`${step}:start`, details);
  const slowTimer = window.setTimeout(() => {
    warnLoginDebug(`${step}:slow`, {
      ms: getElapsedMs(startedAt),
      ...details,
    });
  }, slowMs);
  try {
    const result = await work();
    logLoginDebug(`${step}:done`, {
      ms: getElapsedMs(startedAt),
      ...details,
    });
    return result;
  } catch (err) {
    console.error(`${LOGIN_DEBUG_PREFIX} ${step}:error`, {
      ms: getElapsedMs(startedAt),
      ...details,
      ...summarizeError(err),
    });
    throw err;
  } finally {
    window.clearTimeout(slowTimer);
  }
};

function isCaptchaAuthFailure(errorLike) {
  const message = String(errorLike?.message || "").toLowerCase();
  const code = String(errorLike?.code || "").toLowerCase();
  return (
    message.includes("captcha") ||
    message.includes("challenge") ||
    code.includes("captcha") ||
    code.includes("challenge")
  );
}

function isAuthSchemaFailure(errorLike) {
  const message = String(errorLike?.message || "").toLowerCase();
  return message.includes("database error querying schema") || message.includes("schema");
}

function logAuthError(scope, err) {
  console.error(`[login] ${scope} auth error`, {
    name: err?.name || "",
    status: Number(err?.status) || 0,
    code: err?.code || "",
    message: err?.message || "",
  });
}

async function signInWithPasswordSafe({ email, password, captchaToken }) {
  const signInPayload = { email, password };
  if (captchaToken) {
    signInPayload.options = { captchaToken };
  }

  const firstAttempt = await supabase.auth.signInWithPassword(signInPayload);
  const firstError = firstAttempt?.error || null;
  if (!firstError) return firstAttempt;

  logAuthError("signIn:first_attempt", firstError);

  const shouldRetryWithoutCaptcha =
    Boolean(captchaToken) &&
    isCaptchaAuthFailure(firstError);
  if (!shouldRetryWithoutCaptcha) return firstAttempt;

  // En algunos fallos transitorios del proveedor captcha, el login puede
  // completarse correctamente reintentando sin token.
  const retryAttempt = await supabase.auth.signInWithPassword({ email, password });
  if (retryAttempt?.error) {
    logAuthError("signIn:retry_without_captcha", retryAttempt.error);
  } else {
    console.warn("[login] signIn retry without captcha token succeeded");
  }
  return retryAttempt;
}

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
  logLoginDebug("ui.status", { message, isError });
}

function setLoading(isLoading) {
  if (!submitBtn) return;
  submitBtn.disabled = isLoading;
  submitBtn.textContent = isLoading ? "Iniciando..." : "Iniciar sesión";
  logLoginDebug("ui.loading", { isLoading });
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
  logLoginDebug("submit", {
    email: maskEmail(email),
    host: window.location.host,
    path: window.location.pathname,
    online: navigator.onLine,
  });

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

  const captchaToken = await traceLoginStep(
    "captcha.ensureToken",
    () => requireCaptchaToken(),
    { slowMs: 10000 },
  );
  if (!captchaToken) return;
  logLoginDebug("captcha.ensureToken:ready", {
    tokenLength: String(captchaToken || "").length,
  });

  setLoading(true);

  try {
    // 1) Iniciar sesión en Supabase Auth
    const { data: authData, error: authErr } = await traceLoginStep(
      "auth.signInWithPassword",
      () =>
        signInWithPasswordSafe({
          email,
          password,
          captchaToken,
        }),
      {
        slowMs: 12000,
        meta: { email: maskEmail(email) },
      },
    );
    if (authErr) {
      warnLoginDebug("auth.signInWithPassword:failed", summarizeError(authErr));
      const msg = (authErr.message || "").toLowerCase();
      if (isAuthSchemaFailure(authErr)) {
        setStatus("Error temporal del servidor de autenticación. Intenta de nuevo en unos minutos.", true);
        return;
      }
      if (isCaptchaAuthFailure(authErr)) {
        setStatus("No se pudo validar el captcha. Recarga la página e intenta de nuevo.", true);
        return;
      }
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
      if (Number(authErr?.status) >= 500) {
        setStatus("Error temporal del servidor de autenticación. Intenta de nuevo.", true);
        return;
      }
      setStatus("No se pudo iniciar sesión. Intenta de nuevo.", true);
      return;
    }
    logLoginDebug("auth.signInWithPassword:success", {
      authUserId: authData?.user?.id || "",
      emailConfirmed: Boolean(authData?.user?.email_confirmed_at),
      hasSession: Boolean(authData?.session),
      accessTokenLength: String(authData?.session?.access_token || "").length,
    });

    if (!authData?.user?.email_confirmed_at) {
      await supabase.auth.signOut().catch(() => {});
      setStatus("Debes verificar tu correo antes de iniciar sesión.", true);
      return;
    }

    // 2) Establecer cookie de sesión backend con token real de Supabase Auth.
    //    Aquí se vincula/crea el registro en `usuarios` si aún no existía.
    const serverSession = await traceLoginStep(
      "backend.startSession",
      () =>
        startSession({
          accessToken: authData?.session?.access_token || "",
          source: "login.signInWithPassword",
          timeoutMs: 4000,
        }),
      { slowMs: 8000 },
    );
    if (serverSession?.error) {
      warnLoginDebug("backend.startSession:failed", serverSession);
      setStatus("No se pudo establecer la sesión segura. Intenta de nuevo.", true);
      return;
    }
    const idUsuarioServer = Number(serverSession?.id_usuario) || 0;
    logLoginDebug("backend.startSession:success", { idUsuarioServer });
    if (!idUsuarioServer) {
      setStatus("No se pudo identificar tu usuario. Intenta de nuevo.", true);
      return;
    }

    // 3) Cargar usuario final desde tabla `usuarios`
    const { data: user, error } = await traceLoginStep(
      "usuarios.selectAfterLogin",
      () =>
        supabase
          .from("usuarios")
          .select("id_usuario, acceso_cliente, permiso_admin, permiso_superadmin")
          .eq("id_usuario", idUsuarioServer)
          .maybeSingle(),
      {
        slowMs: 10000,
        meta: { idUsuarioServer },
      },
    );

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
    logLoginDebug("usuarios.selectAfterLogin:success", {
      id_usuario: user.id_usuario,
      acceso_cliente: user.acceso_cliente,
      permiso_admin: user.permiso_admin,
      permiso_superadmin: user.permiso_superadmin,
    });

    try {
      const cartData = await traceLoginStep(
        "cart.prefetch",
        () => fetchCart(),
        {
          slowMs: 10000,
          meta: { idUsuarioServer },
        },
      );
      setCachedCart(cartData);
      logLoginDebug("cart.prefetch:success", {
        items: Array.isArray(cartData?.items) ? cartData.items.length : null,
      });
    } catch (err) {
      console.warn("No se pudo precargar carrito", err);
    }
    logLoginDebug("redirect:index", { delayMs: 400 });
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
  logLoginDebug("init", {
    host: window.location.host,
    path: window.location.pathname,
    online: navigator.onLine,
    existingLocalSessionId: window.localStorage.getItem("sessionUserId") || "",
  });
  maybeShowEmailConfirmedMessage();
  captchaInitPromise = traceLoginStep(
    "captcha.init",
    () =>
      initAuthCaptcha({
        containerId: "login-captcha",
        errorId: "login-captcha-error",
      }),
    { slowMs: 12000 },
  ).then((ctrl) => {
    captchaController = ctrl;
    logLoginDebug("captcha.init:ready", {
      enabled: Boolean(ctrl?.enabled),
      ready: Boolean(ctrl?.ready),
      provider: ctrl?.provider || "",
    });
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
