import { supabase, startSession, fetchCart } from "./api.js";
import { setSessionUserId, setCachedCart, setSessionRoles, attachLogoHome } from "./session.js";

const form = document.querySelector(".auth-form");
const emailInput = document.getElementById("login-user");
const passwordInput = document.getElementById("login-password");
const emailError = document.getElementById("email-error");
const passwordError = document.getElementById("password-error");
const statusMessage = document.getElementById("login-status");
const toggleBtn = document.querySelector(".toggle-password");
const signupBtn = document.getElementById("login-signup-link");
const submitBtn = document.querySelector(".auth-submit");

function clearFeedback() {
  emailError.textContent = "";
  passwordError.textContent = "";
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

  setLoading(true);

  try {
    // 1) Iniciar sesión en Supabase Auth
    const { data: authData, error: authErr } = await supabase.auth.signInWithPassword({
      email,
      password,
    });
    if (authErr) {
      const msg = (authErr.message || "").toLowerCase();
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

    // 2) Buscar usuario en tabla usuarios por correo (para id_usuario)
    const { data: user, error } = await supabase
      .from("usuarios")
      .select("id_usuario, clave, acceso_cliente, permiso_admin, permiso_superadmin")
      .ilike("correo", email)
      .maybeSingle();

    if (error) {
      throw error;
    }

    // Si no existe en tabla usuarios, avisar para registro
    if (!user) {
      emailError.innerHTML =
        'Este correo no está registrado en usuarios. <a class="link-inline" href="signup.html">Regístrate</a>';
      emailInput.classList.add("input-error");
      return;
    }

    // Opcional: valida clave local si aún se usa
    if (user.clave && user.clave !== password) {
      passwordError.textContent = "La clave no coincide.";
      passwordInput.classList.add("input-error");
      return;
    }

    // 3) Setear sesión local con id_usuario de la tabla
    setSessionUserId(user.id_usuario);
    setSessionRoles({
      acceso_cliente: user.acceso_cliente,
      permiso_admin: user.permiso_admin,
      permiso_superadmin: user.permiso_superadmin,
    });
    await startSession(user.id_usuario);

    try {
      const cartData = await fetchCart();
      setCachedCart(cartData);
    } catch (err) {
      console.warn("No se pudo precargar carrito", err);
    }
    setStatus("Sesión iniciada correctamente. Redirigiendo...", false);
    setTimeout(() => {
      window.location.href = "index.html";
    }, 400);
  } catch (err) {
    console.error("Login error:", err);
    setStatus("No se pudo iniciar sesión. Intenta de nuevo.", true);
  } finally {
    setLoading(false);
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
  initToggle();
  initSignupRedirect();
  if (form) {
    form.addEventListener("submit", handleLogin);
  }
  attachLogoHome();
}

init();
