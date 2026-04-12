import {
  API_BASE,
  clearServerSession,
  ensureServerSession,
  loadCurrentUser,
  supabase,
  triggerWhatsappReminderForUser,
} from "./api.js";
import { attachLogoHome, attachLogout, requireSession } from "./session.js";
import { AVATAR_RANDOM_COLORS, applyAvatarImage, resolveAvatarForDisplay } from "./avatar-fallback.js";

const sessionUserId = requireSession();
const EMPTY_AVATAR_DATA_URL = "data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs=";
const DEFAULT_BG_COLOR = AVATAR_RANDOM_COLORS[0] || "#ffa4a4";
const AVATAR_PALETTE_COLORS = AVATAR_RANDOM_COLORS;
const DEFAULT_MODAL_COLOR = AVATAR_PALETTE_COLORS[0];
const DEFAULT_DIAL_CODE = "58";
const NAME_ALLOWED_CHARS_REGEX = /[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ\s]/g;
const NAME_VALIDATION_REGEX = /^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+(?:\s+[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+)*$/;

const statusEl = document.querySelector("#perfil-status");
const cardEl = document.querySelector("#perfil-card");
const avatarBgEl = document.querySelector("#perfil-avatar-bg");
const avatarEl = document.querySelector("#perfil-avatar");
const btnEditarFotoEl = document.querySelector("#btn-editar-foto");
const nombreInputEl = document.querySelector("#perfil-nombre-input");
const apellidoInputEl = document.querySelector("#perfil-apellido-input");
const correoInputEl = document.querySelector("#perfil-correo-input");
const telefonoInputEl = document.querySelector("#perfil-telefono-input");
const recordatorioDiasInputEl = document.querySelector("#perfil-recordatorio-dias-input");
const btnPerfilSaveEl = document.querySelector("#btn-perfil-save");
const perfilSaveStatusEl = document.querySelector("#perfil-save-status");
const avatarModalEl = document.querySelector("#avatar-modal");
const avatarModalCloseEl = document.querySelector("#avatar-modal-close");
const avatarModalGridEl = document.querySelector("#avatar-modal-grid");
const avatarModalStatusEl = document.querySelector("#avatar-modal-status");
const btnAvatarSaveEl = document.querySelector("#btn-avatar-save");

let currentUserId = null;
let savedAvatarUrl = EMPTY_AVATAR_DATA_URL;
let savedBgColor = DEFAULT_BG_COLOR;
let pendingAvatarUrl = EMPTY_AVATAR_DATA_URL;
let pendingBgColor = DEFAULT_BG_COLOR;
let savedNombre = "";
let savedApellido = "";
let savedCorreo = "";
let savedRecordatorioDiasAntes = null;
let savedTelefonoDigits = "";
let telefonoIti = null;
let telefonoMaxDigits = null;
let telefonoPattern = "";
let saveProfileInProgress = false;

const setInput = (el, value) => {
  if (!el) return;
  el.value = String(value ?? "").trim();
};

const normalizeTextValue = (value) => String(value ?? "").trim();
const sanitizePersonName = (value = "") =>
  String(value || "")
    .replace(NAME_ALLOWED_CHARS_REGEX, "")
    .replace(/\s+/g, " ")
    .replace(/^\s+/, "");
const isValidPersonName = (value = "") => NAME_VALIDATION_REGEX.test(normalizeTextValue(value));

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const normalizeColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  if (/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash)) return withHash.toLowerCase();
  return "";
};

const setProfileSaveStatus = (message = "", isError = false) => {
  if (!perfilSaveStatusEl) return;
  perfilSaveStatusEl.textContent = message;
  perfilSaveStatusEl.style.color = isError ? "#b91c1c" : "";
};

const parseRecordatorioDias = (value) => {
  const raw = String(value ?? "").trim();
  if (!raw) return { valid: false, value: null };
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0 || parsed > 5 || !Number.isInteger(parsed)) {
    return { valid: false, value: null };
  }
  return { valid: true, value: parsed };
};

const normalizePhoneDigits = (value) =>
  String(value || "")
    .replace(/\D+/g, "")
    .replace(/^0+/, "");

const normalizePhoneByDialCode = (rawValue, rawDialCode = "") => {
  const digits = normalizePhoneDigits(rawValue);
  const dialDigits = normalizePhoneDigits(rawDialCode);
  if (!digits) return "";
  if (!dialDigits) return digits;
  if (digits.startsWith(dialDigits)) {
    const national = digits.slice(dialDigits.length).replace(/^0+/, "");
    return national ? `${dialDigits}${national}` : dialDigits;
  }
  return `${dialDigits}${digits.replace(/^0+/, "")}`;
};

const extractLocalPhoneDigits = (rawValue, rawDialCode = "") => {
  const digits = normalizePhoneDigits(rawValue);
  const dialDigits = normalizePhoneDigits(rawDialCode);
  if (!digits) return "";
  if (dialDigits && digits.startsWith(dialDigits)) {
    return digits.slice(dialDigits.length).replace(/^0+/, "");
  }
  return digits.replace(/^0+/, "");
};

const formatTelefonoWithPattern = (digits, pattern) => {
  if (!pattern) return digits;
  let result = "";
  let i = 0;
  for (const ch of pattern) {
    if (ch === "X") {
      if (i >= digits.length) break;
      result += digits[i++];
    } else {
      if (i === 0 || i >= digits.length) continue;
      result += ch;
    }
  }
  return result;
};

const computeTelefonoPlaceholderInfo = () => {
  const utils = window.intlTelInputUtils;
  if (!utils || !telefonoIti) return { placeholder: "", max: null, pattern: "" };
  const type = utils.numberType?.MOBILE ?? utils.numberType?.FIXED_LINE_OR_MOBILE;
  const iso2 = telefonoIti.getSelectedCountryData()?.iso2;
  const example = iso2 ? utils.getExampleNumber(iso2, true, type) : "";
  const rawPlaceholder =
    (telefonoIti?.getNumberPlaceholder && telefonoIti.getNumberPlaceholder(type)) || example || "";
  const cleaned = rawPlaceholder.replace(/^0+/, "").trim();
  const placeholderNormalized = cleaned.replace(/[()]/g, "").replace(/\s+/g, "-");
  const digitsOnly = placeholderNormalized.replace(/\D+/g, "").replace(/^0+/, "");
  const maxDigits = digitsOnly.length || null;
  const pattern = placeholderNormalized.replace(/\d/g, "X");
  return {
    placeholder: placeholderNormalized || cleaned || rawPlaceholder,
    max: maxDigits,
    pattern,
  };
};

const updateTelefonoPlaceholder = () => {
  if (!telefonoInputEl) return;
  const info = computeTelefonoPlaceholderInfo();
  telefonoMaxDigits = info.max;
  telefonoPattern = info.pattern || "";
  if (info.placeholder) telefonoInputEl.placeholder = info.placeholder;
  if (telefonoMaxDigits) {
    const patternLength = telefonoPattern ? telefonoPattern.length : telefonoMaxDigits;
    telefonoInputEl.maxLength = patternLength;
  } else {
    telefonoInputEl.removeAttribute("maxLength");
  }
};

const setTelefonoFromUser = (value) => {
  if (!telefonoInputEl) return;
  const rawDigits = normalizePhoneDigits(value);
  const selectedDialDigits = normalizePhoneDigits(
    telefonoIti?.getSelectedCountryData?.()?.dialCode || "",
  );
  const guessedDialDigits = selectedDialDigits || (rawDigits.startsWith("58") ? "58" : "");
  const digits = normalizePhoneByDialCode(rawDigits, guessedDialDigits);
  telefonoInputEl.dataset.rawPhone = digits;
  if (!digits) {
    telefonoInputEl.value = "";
    return;
  }

  let localDigits = digits;
  if (guessedDialDigits && localDigits.startsWith(guessedDialDigits)) {
    localDigits = localDigits.slice(guessedDialDigits.length);
  }
  localDigits = localDigits.replace(/^0+/, "");

  if (telefonoIti?.setNumber) {
    try {
      telefonoIti.setNumber(`+${digits}`);
    } catch (_err) {
      // noop
    }
  }
  if (telefonoMaxDigits && localDigits.length > telefonoMaxDigits) {
    localDigits = localDigits.slice(-telefonoMaxDigits);
  }
  const formatted = formatTelefonoWithPattern(localDigits, telefonoPattern);
  telefonoInputEl.value = formatted || localDigits;
};

const getTelefonoDigitsForSave = () => {
  const localDigits = normalizePhoneDigits(telefonoInputEl?.value || "");
  const dialDigits = normalizePhoneDigits(
    telefonoIti?.getSelectedCountryData?.()?.dialCode || DEFAULT_DIAL_CODE,
  );
  if (localDigits && dialDigits) return normalizePhoneByDialCode(localDigits, dialDigits);
  if (localDigits) return localDigits;

  const e164Digits = normalizePhoneDigits(telefonoIti?.getNumber?.() || "");
  if (!e164Digits) return "";
  if (dialDigits) return normalizePhoneByDialCode(e164Digits, dialDigits);
  return e164Digits;
};

const normalizeTelefonoFallbackInput = () => {
  if (!telefonoInputEl) return;
  let digits = extractLocalPhoneDigits(telefonoInputEl.value);
  if (telefonoMaxDigits && digits.length > telefonoMaxDigits) {
    digits = digits.slice(0, telefonoMaxDigits);
  }
  telefonoInputEl.value = digits;
};

const initTelefonoInput = () => {
  if (!telefonoInputEl) return;
  if (!window.intlTelInput) {
    telefonoInputEl.addEventListener("input", normalizeTelefonoFallbackInput);
    telefonoInputEl.addEventListener("blur", normalizeTelefonoFallbackInput);
    return;
  }
  telefonoIti = window.intlTelInput(telefonoInputEl, {
    initialCountry: "ve",
    preferredCountries: ["ve", "co", "mx", "us", "es"],
    separateDialCode: true,
    allowDropdown: true,
    nationalMode: true,
    autoPlaceholder: "aggressive",
    placeholderNumberType: "MOBILE",
    customPlaceholder: (placeholder) => {
      const cleaned = placeholder.replace(/^0+/, "").trim();
      return cleaned || placeholder;
    },
  });

  telefonoIti.promise?.then(() => {
    updateTelefonoPlaceholder();
    const rawDigits = normalizePhoneDigits(telefonoInputEl.dataset.rawPhone || "");
    if (rawDigits) setTelefonoFromUser(rawDigits);
  });
  updateTelefonoPlaceholder();
  telefonoInputEl.addEventListener("countrychange", updateTelefonoPlaceholder);

  telefonoInputEl.addEventListener("input", () => {
    const dialDigits = normalizePhoneDigits(
      telefonoIti?.getSelectedCountryData?.()?.dialCode || "",
    );
    let digits = extractLocalPhoneDigits(telefonoInputEl.value, dialDigits);
    if (telefonoMaxDigits && digits.length > telefonoMaxDigits) {
      digits = digits.slice(0, telefonoMaxDigits);
    }
    const formatted = formatTelefonoWithPattern(digits, telefonoPattern);
    telefonoInputEl.value = formatted;
    if (telefonoMaxDigits) {
      const patternLength = telefonoPattern ? telefonoPattern.length : telefonoMaxDigits;
      telefonoInputEl.maxLength = patternLength;
    }
  });
};

const isImageName = (name) =>
  /\.(png|jpe?g|webp|gif|bmp|svg|avif)$/i.test(String(name || ""));

const setAvatarModalStatus = (msg, isError = false) => {
  if (!avatarModalStatusEl) return;
  avatarModalStatusEl.textContent = msg || "";
  avatarModalStatusEl.classList.toggle("is-error", isError);
  avatarModalStatusEl.classList.toggle("is-success", !isError && !!msg);
};

const updateSelectionStyles = () => {
  const selectedUrl = String(pendingAvatarUrl || "");
  document.querySelectorAll(".avatar-option").forEach((btn) => {
    btn.classList.toggle("selected", btn.dataset.avatarUrl === selectedUrl);
  });
  const selectedColor = normalizeColor(pendingBgColor);
  document.querySelectorAll(".avatar-palette-dot[data-color]").forEach((dot) => {
    const dotColor = normalizeColor(dot.dataset.color);
    dot.classList.toggle("selected", !!selectedColor && dotColor === selectedColor);
  });
};

const applyAvatarPreview = ({ url, color } = {}) => {
  const nextUrl = String(url || "").trim() || EMPTY_AVATAR_DATA_URL;
  const nextColor = normalizeColor(color) || DEFAULT_BG_COLOR;
  document.querySelectorAll(".avatar-option").forEach((btn) => {
    btn.style.setProperty("--avatar-bg", nextColor);
  });
  if (nextUrl) {
    avatarModalEl?.querySelectorAll(".avatar-option img").forEach((img) => {
      if (!img.getAttribute("src")) img.setAttribute("src", nextUrl);
    });
  }
  updateSelectionStyles();
};

const applyAvatarSavedState = ({ url, color } = {}) => {
  const nextUrl = String(url || "").trim() || EMPTY_AVATAR_DATA_URL;
  const nextColor = normalizeColor(color) || DEFAULT_BG_COLOR;
  if (avatarBgEl) {
    avatarBgEl.style.backgroundColor = nextColor;
  }
  if (avatarEl) {
    applyAvatarImage(avatarEl, nextUrl);
    avatarEl.style.backgroundColor = "transparent";
  }
  document.querySelectorAll(".avatar").forEach((img) => {
    applyAvatarImage(img, nextUrl);
    img.style.backgroundColor = nextColor;
  });
};

const closeAvatarModal = (revert = true) => {
  if (revert) {
    pendingAvatarUrl = savedAvatarUrl;
    pendingBgColor = savedBgColor;
    applyAvatarPreview({ url: savedAvatarUrl, color: savedBgColor });
  }
  avatarModalEl?.classList.add("hidden");
};

const openAvatarModal = () => {
  pendingAvatarUrl = savedAvatarUrl;
  pendingBgColor = DEFAULT_MODAL_COLOR;
  avatarModalEl?.classList.remove("hidden");
  applyAvatarPreview({ url: pendingAvatarUrl, color: pendingBgColor });
};

const renderAvatarOptions = (items = []) => {
  if (!avatarModalGridEl) return;
  const rows = Array.isArray(items)
    ? items
        .filter((it) => it?.publicUrl)
        .filter((it) => !String(it?.name || "").startsWith("."))
        .filter((it) => isImageName(it?.name))
    : [];
  if (!rows.length) {
    avatarModalGridEl.innerHTML = '<p class="status">Sin iconos disponibles.</p>';
    return;
  }
  avatarModalGridEl.innerHTML = rows
    .map(
      (item) => `
        <button
          type="button"
          class="avatar-option"
          data-avatar-url="${escapeHtml(item.publicUrl || "")}"
          title="${escapeHtml(item.name || "Icono")}"
        >
          <img src="${escapeHtml(item.publicUrl || "")}" alt="${escapeHtml(item.name || "Icono")}" loading="lazy" decoding="async" />
        </button>
      `,
    )
    .join("");
  applyAvatarPreview({ url: pendingAvatarUrl, color: pendingBgColor });
};

const loadAvatarOptions = async () => {
  try {
    setAvatarModalStatus("Cargando iconos...");
    const url = new URL(`${API_BASE}/api/logos/list`);
    url.searchParams.set("folder", "icono-perfil");
    if (sessionUserId) url.searchParams.set("id_usuario", String(sessionUserId));
    const res = await fetch(url.toString(), {
      credentials: "include",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data?.error || "No se pudo cargar la lista de iconos.");
    }
    renderAvatarOptions(data?.items || []);
    setAvatarModalStatus("");
  } catch (err) {
    console.error("load avatar options error", err);
    renderAvatarOptions([]);
    setAvatarModalStatus("No se pudieron cargar los iconos.", true);
  }
};

const saveAvatarProfile = async () => {
  if (!currentUserId) {
    setAvatarModalStatus("No se pudo identificar el usuario.", true);
    return;
  }
  if (!pendingAvatarUrl) {
    setAvatarModalStatus("Selecciona una imagen de perfil.", true);
    return;
  }
  try {
    btnAvatarSaveEl && (btnAvatarSaveEl.disabled = true);
    setAvatarModalStatus("Guardando...");
    const payload = {
      foto_perfil: pendingAvatarUrl,
      fondo_perfil: normalizeColor(pendingBgColor) || DEFAULT_BG_COLOR,
    };
    const { error } = await supabase
      .from("usuarios")
      .update(payload)
      .eq("id_usuario", currentUserId);
    if (error) throw error;

    const refreshedUser = await loadCurrentUser();
    savedAvatarUrl = String(refreshedUser?.foto_perfil || payload.foto_perfil || "").trim();
    savedBgColor =
      normalizeColor(refreshedUser?.fondo_perfil) ||
      normalizeColor(payload.fondo_perfil) ||
      DEFAULT_BG_COLOR;
    applyAvatarSavedState({ url: savedAvatarUrl, color: savedBgColor });
    setAvatarModalStatus("Foto guardada correctamente.");
    closeAvatarModal(false);
  } catch (err) {
    console.error("save avatar profile error", err);
    setAvatarModalStatus("No se pudo guardar la foto de perfil.", true);
  } finally {
    btnAvatarSaveEl && (btnAvatarSaveEl.disabled = false);
  }
};

const init = async () => {
  try {
    await ensureServerSession();
    const user = await loadCurrentUser();
    if (!user?.id_usuario) {
      if (statusEl) statusEl.textContent = "No se pudo cargar la sesion del usuario.";
      return;
    }

    currentUserId = Number(user.id_usuario) || null;
    const resolvedAvatar = await resolveAvatarForDisplay({
      user,
      idUsuario: currentUserId || sessionUserId,
    });
    savedAvatarUrl = String(resolvedAvatar?.url || "").trim() || EMPTY_AVATAR_DATA_URL;
    savedBgColor =
      normalizeColor(user.fondo_perfil) ||
      normalizeColor(resolvedAvatar?.color) ||
      DEFAULT_BG_COLOR;
    pendingAvatarUrl = savedAvatarUrl;
    pendingBgColor = savedBgColor;

    savedNombre = normalizeTextValue(user.nombre);
    savedApellido = normalizeTextValue(user.apellido);
    savedCorreo = normalizeTextValue(user.correo);
    setInput(nombreInputEl, savedNombre);
    setInput(apellidoInputEl, savedApellido);
    setInput(correoInputEl, savedCorreo);
    setTelefonoFromUser(user.telefono);
    savedTelefonoDigits = normalizePhoneDigits(user.telefono);
    const parsedRecordatorio = parseRecordatorioDias(user.recordatorio_dias_antes);
    savedRecordatorioDiasAntes = parsedRecordatorio.valid ? parsedRecordatorio.value : 1;
    setInput(recordatorioDiasInputEl, savedRecordatorioDiasAntes);

    applyAvatarSavedState({ url: savedAvatarUrl, color: savedBgColor });

    cardEl?.classList.remove("hidden");
    if (statusEl) statusEl.textContent = "";
  } catch (err) {
    console.error("mi_perfil init error", err);
    if (statusEl) statusEl.textContent = "No se pudo cargar el perfil.";
  }
};

const saveProfile = async () => {
  if (!currentUserId || saveProfileInProgress) return;

  const nombre = sanitizePersonName(normalizeTextValue(nombreInputEl?.value));
  const apellido = sanitizePersonName(normalizeTextValue(apellidoInputEl?.value));
  if (nombreInputEl) nombreInputEl.value = nombre;
  if (apellidoInputEl) apellidoInputEl.value = apellido;
  const correo = normalizeTextValue(correoInputEl?.value);
  const telefono = getTelefonoDigitsForSave();
  const parsedRecordatorio = parseRecordatorioDias(recordatorioDiasInputEl?.value);

  if (nombre && !isValidPersonName(nombre)) {
    setProfileSaveStatus("El nombre solo puede contener letras.", true);
    nombreInputEl?.focus();
    return;
  }
  if (apellido && !isValidPersonName(apellido)) {
    setProfileSaveStatus("El apellido solo puede contener letras.", true);
    apellidoInputEl?.focus();
    return;
  }

  if (correoInputEl && !correoInputEl.checkValidity()) {
    correoInputEl.reportValidity?.();
    setProfileSaveStatus("Ingresa un correo válido.", true);
    return;
  }

  if (!parsedRecordatorio.valid) {
    setProfileSaveStatus("Selecciona entre 1 y 5 días de anticipación.", true);
    setInput(recordatorioDiasInputEl, savedRecordatorioDiasAntes || 1);
    return;
  }

  const hasChanges =
    nombre !== savedNombre ||
    apellido !== savedApellido ||
    correo !== savedCorreo ||
    telefono !== savedTelefonoDigits ||
    parsedRecordatorio.value !== savedRecordatorioDiasAntes;

  if (!hasChanges) {
    setProfileSaveStatus("No hay cambios para guardar.");
    return;
  }

  try {
    saveProfileInProgress = true;
    btnPerfilSaveEl && (btnPerfilSaveEl.disabled = true);
    setProfileSaveStatus("Guardando...");

    const payload = {
      nombre: nombre || null,
      apellido: apellido || null,
      correo: correo || null,
      telefono: telefono || null,
      recordatorio_dias_antes: parsedRecordatorio.value,
    };

    const { error } = await supabase
      .from("usuarios")
      .update(payload)
      .eq("id_usuario", currentUserId);
    if (error) throw error;

    const telefonoChanged = telefono !== savedTelefonoDigits;
    savedNombre = nombre;
    savedApellido = apellido;
    savedCorreo = correo;
    savedTelefonoDigits = telefono;
    savedRecordatorioDiasAntes = parsedRecordatorio.value;

    setInput(nombreInputEl, savedNombre);
    setInput(apellidoInputEl, savedApellido);
    setInput(correoInputEl, savedCorreo);
    setTelefonoFromUser(savedTelefonoDigits);
    setInput(recordatorioDiasInputEl, savedRecordatorioDiasAntes);
    setProfileSaveStatus("Perfil guardado correctamente.");

    if (telefonoChanged && telefono) {
      try {
        await triggerWhatsappReminderForUser(currentUserId);
      } catch (err) {
        console.error("trigger whatsapp reminder error", err);
      }
    }
  } catch (err) {
    console.error("save profile error", err);
    setProfileSaveStatus(err?.message || "No se pudo guardar el perfil.", true);
  } finally {
    saveProfileInProgress = false;
    btnPerfilSaveEl && (btnPerfilSaveEl.disabled = false);
  }
};

const bindPersonNameInput = (inputEl) => {
  inputEl?.addEventListener("input", () => {
    const rawValue = String(inputEl.value || "");
    const sanitizedValue = sanitizePersonName(rawValue);
    if (sanitizedValue !== rawValue) {
      const cursorPos = inputEl.selectionStart;
      inputEl.value = sanitizedValue;
      if (cursorPos !== null) {
        const nextCursor = Math.min(sanitizedValue.length, cursorPos);
        inputEl.setSelectionRange(nextCursor, nextCursor);
      }
    }
  });
  inputEl?.addEventListener("blur", () => {
    inputEl.value = sanitizePersonName(normalizeTextValue(inputEl.value));
  });
};

bindPersonNameInput(nombreInputEl);
bindPersonNameInput(apellidoInputEl);

btnEditarFotoEl?.addEventListener("click", async () => {
  openAvatarModal();
  await loadAvatarOptions();
});

avatarModalEl?.addEventListener("click", (e) => {
  const option = e.target.closest(".avatar-option[data-avatar-url]");
  if (option) {
    pendingAvatarUrl = option.dataset.avatarUrl || pendingAvatarUrl;
    applyAvatarPreview({ url: pendingAvatarUrl, color: pendingBgColor });
    return;
  }
  const colorDot = e.target.closest(".avatar-palette-dot[data-color]");
  if (colorDot) {
    pendingBgColor = normalizeColor(colorDot.dataset.color) || pendingBgColor;
    applyAvatarPreview({ url: pendingAvatarUrl, color: pendingBgColor });
    return;
  }
  if (e.target.classList.contains("modal-backdrop")) {
    closeAvatarModal(true);
  }
});

avatarModalCloseEl?.addEventListener("click", () => closeAvatarModal(true));
btnAvatarSaveEl?.addEventListener("click", saveAvatarProfile);
btnPerfilSaveEl?.addEventListener("click", saveProfile);
[nombreInputEl, apellidoInputEl, correoInputEl, telefonoInputEl, recordatorioDiasInputEl].forEach((el) => {
  el?.addEventListener("keydown", (e) => {
    if (e.key !== "Enter") return;
    e.preventDefault();
    saveProfile();
  });
});

initTelefonoInput();
init();
attachLogoHome();
attachLogout(clearServerSession);
