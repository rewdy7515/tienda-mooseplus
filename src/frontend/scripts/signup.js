import {
  completeSignupWithRegistrationToken,
  supabase,
  validateSignupRegistrationToken,
} from "./api.js";
import { attachLogoHome } from "./session.js";
import { loadPaginaBranding } from "./branding.js";
import { initAuthCaptcha } from "./auth-captcha.js";

const form = document.getElementById("signup-form");
const fields = {
  nombre: document.getElementById("signup-nombre"),
  apellido: document.getElementById("signup-apellido"),
  telefono: document.getElementById("signup-telefono"),
  correo: document.getElementById("signup-correo"),
  clave: document.getElementById("signup-clave"),
  clave2: document.getElementById("signup-clave2"),
};

const errors = {
  nombre: document.getElementById("nombre-error"),
  apellido: document.getElementById("apellido-error"),
  telefono: document.getElementById("telefono-error"),
  correo: document.getElementById("correo-error"),
  clave: document.getElementById("clave-error"),
  clave2: document.getElementById("clave2-error"),
};

const statusEl = document.getElementById("signup-status");
const submitBtn = document.getElementById("signup-submit");
const goLoginBtn = document.getElementById("signup-login-link");
const signupCardEl = document.getElementById("signup-card");
const signupFlowEl = document.getElementById("signup-flow");
const signupSuccessStepEl = document.getElementById("signup-success-step");
let iti = null;
const toggleButtons = document.querySelectorAll(".toggle-password");
let phoneMaxDigits = null;
let phonePattern = "";
let captchaController = null;
let captchaInitPromise = null;
const signupToken =
  new URLSearchParams(window.location.search || "").get("t") ||
  new URLSearchParams(window.location.search || "").get("registro_token") ||
  "";
const signupSuccessPreviewMode =
  new URLSearchParams(window.location.search || "").get("preview_success") === "1";
let signupTokenContext = null;

function getSignupEmailRedirectUrl() {
  try {
    return new URL("login.html?email_confirmed=1", window.location.href).toString();
  } catch (_err) {
    return "";
  }
}

const limitMessage = () =>
  phoneMaxDigits ? `Puedes escribir hasta ${phoneMaxDigits} dígitos` : "";

function clearMessages() {
  Object.values(errors).forEach((el) => {
    if (el) el.textContent = "";
  });
  Object.values(fields).forEach((input) => input?.classList.remove("input-error"));
  const captchaError = document.getElementById("signup-captcha-error");
  if (captchaError) captchaError.textContent = "";
  statusEl.textContent = "";
  statusEl.classList.remove("is-error", "is-success");
}

function setStatus(msg, isError = false) {
  statusEl.textContent = msg;
  statusEl.classList.toggle("is-error", isError);
  statusEl.classList.toggle("is-success", !isError);
}

function showSignupSuccessView() {
  setStatus("", false);
  signupCardEl?.classList.add("signup-success-only");
  signupFlowEl?.classList.add("is-success");
  setTimeout(() => {
    signupSuccessStepEl?.focus();
  }, 250);
}

function setCorreoYaRegistradoError() {
  if (!errors?.correo || !fields?.correo) return;
  errors.correo.innerHTML =
    'Correo ya registrado. <a class="link-inline" href="login.html">Inicia sesión aquí</a>';
  fields.correo.classList.add("input-error");
  setStatus("", false);
}

function normalizeErrorMessage(err) {
  return String(err?.message || err?.error_description || err?.error || "")
    .trim()
    .toLowerCase();
}

function translateSignupError(err) {
  const msg = normalizeErrorMessage(err);
  if (!msg) return "No se pudo completar el registro. Intenta de nuevo.";
  if (msg.includes("password is known to be weak")) {
    return "La contraseña es demasiado débil o fácil de adivinar. Usa una más segura.";
  }
  if (msg.includes("password should be at least")) {
    return "La contraseña no cumple con el mínimo de caracteres.";
  }
  if (msg.includes("user already registered")) {
    return "Este correo ya está registrado. Inicia sesión.";
  }
  if (msg.includes("email not confirmed")) {
    return "Debes confirmar tu correo antes de continuar.";
  }
  if (msg.includes("captcha") || msg.includes("challenge")) {
    return "No se pudo validar el captcha. Intenta nuevamente.";
  }
  if (msg.includes("invalid email")) {
    return "El correo no es válido.";
  }
  return "No se pudo completar el registro. Intenta de nuevo.";
}

function setLoading(isLoading) {
  if (!submitBtn) return;
  submitBtn.disabled = isLoading;
  submitBtn.textContent = isLoading ? "Registrando..." : "Registrarse";
}

const setFormDisabled = (disabled) => {
  Object.values(fields).forEach((input) => {
    if (input) input.disabled = !!disabled;
  });
  toggleButtons.forEach((btn) => {
    btn.disabled = !!disabled;
  });
  if (submitBtn) submitBtn.disabled = !!disabled;
};

const preloadSignupTokenContext = async () => {
  if (!signupToken) return null;
  const result = await validateSignupRegistrationToken(signupToken);
  if (result?.error) {
    setFormDisabled(true);
    setStatus("Este link de registro es inválido o venció. Solicita uno nuevo.", true);
    return null;
  }

  signupTokenContext = result;
  const usuario = result?.usuario || {};
  if (!fields.nombre.value && usuario?.nombre) fields.nombre.value = String(usuario.nombre);
  if (!fields.apellido.value && usuario?.apellido) fields.apellido.value = String(usuario.apellido);
  if (!fields.correo.value && usuario?.correo) fields.correo.value = String(usuario.correo);
  return result;
};

async function correoExiste(correo) {
  const { data, error } = await supabase
    .from("usuarios")
    .select("id_usuario, fecha_registro")
    .ilike("correo", correo)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function handleSubmit(e) {
  e.preventDefault();
  clearMessages();

  const nombre = fields.nombre.value.trim();
  const apellido = fields.apellido.value.trim();
  const telefono = fields.telefono.value.replace(/\D+/g, "");
  const correo = fields.correo.value.trim().toLowerCase();
  const clave = fields.clave.value;
  const clave2 = fields.clave2.value;

  let hasError = false;
  if (!nombre) {
    errors.nombre.textContent = "El nombre es obligatorio.";
    fields.nombre.classList.add("input-error");
    hasError = true;
  }
  if (!apellido) {
    errors.apellido.textContent = "El apellido es obligatorio.";
    fields.apellido.classList.add("input-error");
    hasError = true;
  }
  if (!telefono) {
    errors.telefono.textContent = "El número es obligatorio.";
    fields.telefono.classList.add("input-error");
    hasError = true;
  }
  const digitsOnly = telefono.replace(/\D+/g, "");
  if (phoneMaxDigits && digitsOnly.length > 0 && digitsOnly.length < phoneMaxDigits) {
    errors.telefono.textContent = "Numero de telefono incompleto";
    fields.telefono.classList.add("input-error");
    hasError = true;
  }
  if (phoneMaxDigits && digitsOnly.length > phoneMaxDigits) {
    errors.telefono.textContent = limitMessage();
    fields.telefono.classList.add("input-error");
    hasError = true;
  }
  if (!correo || !fields.correo.checkValidity()) {
    errors.correo.textContent = "Correo invalido";
    fields.correo.classList.add("input-error");
    hasError = true;
  }
  if (!clave) {
    errors.clave.textContent = "La clave es obligatoria.";
    fields.clave.classList.add("input-error");
    hasError = true;
  }
  if (!clave2) {
    errors.clave2.textContent = "Confirma tu clave.";
    fields.clave2.classList.add("input-error");
    hasError = true;
  }
  if (clave && clave2 && clave !== clave2) {
    fields.clave.value = "";
    fields.clave2.value = "";
    errors.clave2.textContent = "Las claves no coinciden";
    fields.clave.classList.add("input-error");
    fields.clave2.classList.add("input-error");
    hasError = true;
  }

  if (hasError) return;

  const ctrl = captchaController || (captchaInitPromise ? await captchaInitPromise : null);
  if (!ctrl || !ctrl.enabled) {
    setStatus("Captcha no disponible. Recarga la página e intenta de nuevo.", true);
    return;
  }
  const captchaToken = ctrl.ensureToken();
  if (!captchaToken) return;

  setLoading(true);

  try {
    const tokenFlow = !!signupToken;
    let existente = null;
    let reutilizar = false;

    if (tokenFlow) {
      const valid = await validateSignupRegistrationToken(signupToken);
      if (valid?.error) {
        throw new Error(valid.error || "El link de registro no es válido.");
      }
      signupTokenContext = valid;
    } else {
      existente = await correoExiste(correo);
      const correoLibre = !existente;
      reutilizar =
        existente &&
        (existente.fecha_registro === null || typeof existente.fecha_registro === "undefined");
      if (!correoLibre && !reutilizar) {
        setCorreoYaRegistradoError();
        return;
      }
    }

    const phoneDial = iti?.getSelectedCountryData?.()?.dialCode;
    const phoneDialDigits = String(phoneDial || "").replace(/\D+/g, "");
    const phoneDigits = `${phoneDialDigits}${telefono}`.replace(/\D+/g, "") || telefono;

    // 1) Registrar en Auth de Supabase
    const { error: authErr } = await supabase.auth.signUp({
      email: correo.trim(),
      password: clave,
      options: {
        emailRedirectTo: getSignupEmailRedirectUrl(),
        data: {
          display_name: `${nombre} ${apellido}`.trim(),
          nombre,
          apellido,
          telefono: phoneDigits,
          phone: phoneDigits,
        },
        captchaToken,
      },
    });
    if (authErr) {
      console.error("auth.signUp error", authErr);
      throw authErr;
    }

    // 2) Registrar en tabla usuarios
    let idUsuarioCreado = null;
    if (tokenFlow) {
      const completeRes = await completeSignupWithRegistrationToken({
        token: signupToken,
        nombre,
        apellido,
        telefono: phoneDigits,
        correo,
      });
      if (completeRes?.error) {
        throw new Error(completeRes.error);
      }
      idUsuarioCreado = Number(completeRes?.id_usuario) || null;
    } else {
      const today = new Date().toISOString().slice(0, 10);
      let data = null;
      let error = null;
      if (reutilizar && existente?.id_usuario) {
        const upd = await supabase
          .from("usuarios")
          .update({ nombre, apellido, telefono: phoneDigits, correo, fecha_registro: today })
          .eq("id_usuario", existente.id_usuario)
          .select("id_usuario")
          .single();
        data = upd.data;
        error = upd.error;
      } else {
        const ins = await supabase
          .from("usuarios")
          .insert({ nombre, apellido, telefono: phoneDigits, correo, fecha_registro: today })
          .select("id_usuario")
          .single();
        data = ins.data;
        error = ins.error;
      }
      if (error) throw error;
      idUsuarioCreado = Number(data?.id_usuario) || null;
    }

    if (!idUsuarioCreado) {
      throw new Error("No se pudo obtener el usuario creado.");
    }

    showSignupSuccessView();
  } catch (err) {
    console.error("signup error", err);
    const msg = normalizeErrorMessage(err);
    if (msg.includes("user already registered")) {
      setCorreoYaRegistradoError();
      return;
    }
    setStatus(translateSignupError(err), true);
  } finally {
    setLoading(false);
    captchaController?.reset();
  }
}

function init() {
  attachLogoHome();
  loadPaginaBranding({ logoSelectors: [".auth-logo"], applyFavicon: true }).catch((err) => {
    console.warn("signup branding load error", err);
  });

  if (signupSuccessPreviewMode) {
    showSignupSuccessView();
    return;
  }

  captchaInitPromise = initAuthCaptcha({
    containerId: "signup-captcha",
    errorId: "signup-captcha-error",
  }).then((ctrl) => {
    captchaController = ctrl;
    return ctrl;
  });
  preloadSignupTokenContext().catch((err) => {
    console.error("signup token preload error", err);
    if (signupToken) {
      setFormDisabled(true);
      setStatus("No se pudo validar el link de registro.", true);
    }
  });
  if (fields.telefono && window.intlTelInput) {
    iti = window.intlTelInput(fields.telefono, {
      initialCountry: "ve",
      preferredCountries: ["ve", "co", "mx", "us", "es"],
      separateDialCode: true,
      allowDropdown: true,
      nationalMode: true, // placeholder sin prefijo
      autoPlaceholder: "aggressive",
      placeholderNumberType: "MOBILE",
      customPlaceholder: (placeholder) => {
        const cleaned = placeholder.replace(/^0+/, "").trim();
        return cleaned || placeholder;
      },
      utilsScript: "https://cdn.jsdelivr.net/npm/intl-tel-input@19.5.6/build/js/utils.js",
    });

    const computePlaceholderInfo = () => {
      const utils = window.intlTelInputUtils;
      if (!utils) return { placeholder: "", max: null, pattern: "" };
      const type = utils.numberType?.MOBILE ?? utils.numberType?.FIXED_LINE_OR_MOBILE;
      const iso2 = iti.getSelectedCountryData()?.iso2;
      const example = iso2 ? utils.getExampleNumber(iso2, true, type) : "";
      const rawPlaceholder =
        (iti?.getNumberPlaceholder && iti.getNumberPlaceholder(type)) || example || "";
      const cleaned = rawPlaceholder.replace(/^0+/, "").trim();
      const placeholderNormalized = cleaned.replace(/[()]/g, "").replace(/\s+/g, "-");
      const digitsOnly = placeholderNormalized.replace(/\D+/g, "").replace(/^0+/, "");
      const maxDigits = digitsOnly.length || null;
      const pattern = placeholderNormalized.replace(/\d/g, "X");
      return { placeholder: placeholderNormalized || cleaned || rawPlaceholder, max: maxDigits, pattern };
    };

    const formatWithPattern = (digits, pattern) => {
      if (!pattern) return digits;
      let result = "";
      let i = 0;
      for (const ch of pattern) {
        if (ch === "X") {
          if (i >= digits.length) break;
          result += digits[i++];
        } else {
          // Solo agrega separadores si ya hay algún dígito y todavía quedan dígitos por colocar
          if (i === 0 || i >= digits.length) continue;
          result += ch;
        }
      }
      return result;
    };

    const updatePlaceholder = () => {
      const info = computePlaceholderInfo();
      phoneMaxDigits = info.max;
      phonePattern = info.pattern || "";
      if (info.placeholder) fields.telefono.placeholder = info.placeholder;
      if (phoneMaxDigits) {
        const patternLength = phonePattern ? phonePattern.length : phoneMaxDigits;
        fields.telefono.maxLength = patternLength;
      } else {
        fields.telefono.removeAttribute("maxLength");
      }
    };

    iti.promise?.then(updatePlaceholder);
    updatePlaceholder();
    fields.telefono.addEventListener("countrychange", updatePlaceholder);

    fields.telefono.addEventListener("input", () => {
      let digits = fields.telefono.value.replace(/\D+/g, "");
      if (phoneMaxDigits && digits.length > phoneMaxDigits) {
        digits = digits.slice(0, phoneMaxDigits);
        fields.telefono.value = digits;
        errors.telefono.textContent = limitMessage();
        fields.telefono.classList.add("input-error");
        return;
      } else if (
        errors.telefono.textContent === limitMessage() ||
        errors.telefono.textContent === "Numero de telefono incompleto"
      ) {
        errors.telefono.textContent = "";
        fields.telefono.classList.remove("input-error");
      }
      const formatted = formatWithPattern(digits, phonePattern);
      fields.telefono.value = formatted;
      // Reaplica maxLength por seguridad
      if (phoneMaxDigits) {
        const patternLength = phonePattern ? phonePattern.length : phoneMaxDigits;
        fields.telefono.maxLength = patternLength;
      }
    });
    fields.telefono.addEventListener("blur", () => {
      const digits = fields.telefono.value.replace(/\D+/g, "");
      if (!digits) return;
      if (phoneMaxDigits && digits.length < phoneMaxDigits) {
        errors.telefono.textContent = "Numero de telefono incompleto";
        fields.telefono.classList.add("input-error");
      }
    });
  }

  form?.addEventListener("submit", handleSubmit);
  goLoginBtn?.addEventListener("click", (e) => {
    e.preventDefault();
    window.location.href = "login.html";
  });

  const capitalizeField = (input) => {
    input?.addEventListener("input", () => {
      const { selectionStart, selectionEnd, value } = input;
      if (!value) return;
      const capped = value.charAt(0).toUpperCase() + value.slice(1);
      if (capped !== value) {
        input.value = capped;
        if (selectionStart !== null && selectionEnd !== null) {
          input.setSelectionRange(selectionStart, selectionEnd);
        }
      }
    });
  };

  capitalizeField(fields.nombre);
  capitalizeField(fields.apellido);

  toggleButtons.forEach((btn) => {
    const targetId = btn.dataset.target;
    const input = document.getElementById(targetId);
    if (!input) return;
    btn.addEventListener("click", () => {
      const willShow = input.type === "password";
      input.type = willShow ? "text" : "password";
      const visible = input.type === "text";
      btn.classList.toggle("is-hidden", visible);
      btn.setAttribute("aria-label", visible ? "Ocultar clave" : "Mostrar clave");
      btn.setAttribute("aria-pressed", String(visible));
    });
  });

  fields.correo?.addEventListener("blur", () => {
    const val = fields.correo.value.trim();
    if (!val) return;
    if (!fields.correo.checkValidity()) {
      errors.correo.textContent = "Correo invalido";
      fields.correo.classList.add("input-error");
    }
  });
}

init();
