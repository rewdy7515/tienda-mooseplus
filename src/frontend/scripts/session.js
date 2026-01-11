const SESSION_KEY = "sessionUserId";
const SESSION_ROLES_KEY = "sessionUserRoles";
const CART_CACHE_KEY = "cachedCart";

const normalizeFlag = (value) =>
  value === true ||
  value === "true" ||
  value === "t" ||
  value === "1" ||
  value === 1 ||
  value === "on";

const getPathPrefix = () => {
  const idx = window.location.pathname.indexOf("/pages/");
  if (idx === -1) return "";
  const after = window.location.pathname.slice(idx + "/pages/".length);
  const segments = after.split("/").length;
  const upLevels = Math.max(segments - 1, 0);
  return "../".repeat(upLevels);
};

const getCookie = (name) => {
  const match = document.cookie.match(new RegExp(`(?:^|; )${name}=([^;]*)`));
  return match ? decodeURIComponent(match[1]) : null;
};

const normalizeRoles = (roles = {}) => ({
  acceso_cliente: normalizeFlag(roles.acceso_cliente),
  acceso_vendedor: normalizeFlag(roles.acceso_vendedor),
  permiso_admin: normalizeFlag(roles.permiso_admin),
  permiso_superadmin: normalizeFlag(roles.permiso_superadmin),
});

export function getSessionUserId() {
  return localStorage.getItem(SESSION_KEY);
}

export function setSessionUserId(idUsuario) {
  if (!idUsuario) return;
  localStorage.setItem(SESSION_KEY, idUsuario);
}

export function setSessionRoles(roles = {}) {
  try {
    localStorage.setItem(SESSION_ROLES_KEY, JSON.stringify(normalizeRoles(roles)));
  } catch (_err) {
    // noop
  }
}

export function getSessionRoles() {
  const raw = localStorage.getItem(SESSION_ROLES_KEY);
  if (!raw) return null;
  try {
    return normalizeRoles(JSON.parse(raw));
  } catch (_err) {
    localStorage.removeItem(SESSION_ROLES_KEY);
    return null;
  }
}

export function clearSession() {
  localStorage.removeItem(SESSION_KEY);
  localStorage.removeItem(SESSION_ROLES_KEY);
}

export function setCachedCart(cartData) {
  if (!cartData) return;
  sessionStorage.setItem(CART_CACHE_KEY, JSON.stringify(cartData));
}

export function getCachedCart() {
  const raw = sessionStorage.getItem(CART_CACHE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (_err) {
    sessionStorage.removeItem(CART_CACHE_KEY);
    return null;
  }
}

export function clearCachedCart() {
  sessionStorage.removeItem(CART_CACHE_KEY);
}

export function redirectIfSession(target = "index.html") {
  if (getSessionUserId()) {
    const prefix = getPathPrefix();
    window.location.href = `${prefix}${target}`;
  }
}

export function requireSession() {
  let id = getSessionUserId();
  if (!id) {
    const cookieId = getCookie("session_user_id");
    if (cookieId) {
      setSessionUserId(cookieId);
      id = cookieId;
    }
  }
  if (!id) {
    const prefix = getPathPrefix();
    window.location.href = `${prefix}login.html`;
    throw new Error("No hay sesión activa");
  }
  return id;
}

export async function attachLogout(clearServerSession, clearCartCache) {
  const btn = document.querySelector(".logout-btn");
  if (!btn) return;
  btn.addEventListener("click", async () => {
    clearSession();
    if (typeof clearServerSession === "function") {
      await clearServerSession();
    }
    if (typeof clearCartCache === "function") {
      clearCartCache();
    }
    const prefix = getPathPrefix();
    window.location.href = `${prefix}login.html`;
  });
}

export function attachLogoHome() {
  const prefix = getPathPrefix();
  document.querySelectorAll(".logo").forEach((logo) => {
    logo.addEventListener("click", () => {
      window.location.href = `${prefix}index.html`;
    });
  });
}

const DELIVERY_SEEN_KEY = "deliverySeen:";

const deliveryKey = (userId) => `${DELIVERY_SEEN_KEY}${userId || "anon"}`;

export function getDeliverySeen(userId) {
  const raw = localStorage.getItem(deliveryKey(userId));
  const num = Number(raw);
  return Number.isFinite(num) && num >= 0 ? num : 0;
}

export function setDeliverySeen(userId, count) {
  localStorage.setItem(deliveryKey(userId), String(count || 0));
}

// Muestra aviso de servicio entregado
export function showDeliveryNotice() {
  if (document.querySelector(".delivery-toast")) return;
  const toast = document.createElement("div");
  toast.className = "delivery-toast";
  toast.textContent = "¡Servicio entregado!";
  const userMenu = document.querySelector(".user-menu");
  const container = userMenu || document.body || document.documentElement;
  container.appendChild(toast);
  const hideToast = () => {
    toast.classList.add("hidden");
    setTimeout(() => toast.remove(), 300);
  };
  if (userMenu) {
    userMenu.addEventListener("mouseenter", hideToast, { once: true });
  }
}

// Badge de entregas junto a Inventario
export function updateDeliveryBadge(count = 0) {
  const inventarioLink = Array.from(document.querySelectorAll("a")).find(
    (a) => a.textContent?.trim().toLowerCase() === "inventario"
  );
  if (!inventarioLink) return;
  let badge = inventarioLink.querySelector(".delivery-badge");
  if (!badge) {
    badge = document.createElement("span");
    badge.className = "delivery-badge";
    inventarioLink.appendChild(badge);
  }
  badge.textContent = String(count || 0);
  badge.classList.toggle("hidden", count <= 0);
}
