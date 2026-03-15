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

const normalizeSessionId = (value) => {
  const parsed = Number(String(value || "").trim());
  if (!Number.isFinite(parsed) || parsed <= 0) return "";
  return String(Math.trunc(parsed));
};

const normalizeRoles = (roles = {}) => ({
  acceso_cliente: normalizeFlag(roles.acceso_cliente),
  permiso_admin: normalizeFlag(roles.permiso_admin),
  permiso_superadmin: normalizeFlag(roles.permiso_superadmin),
});

export function getSessionUserId() {
  // La cookie del backend es httpOnly, así que el frontend no puede usarla
  // como fuente primaria. Si existe una cookie legible (legacy/local), se usa
  // para sincronizar; en caso contrario, se conserva el id local.
  const cookieId = normalizeSessionId(getCookie("session_user_id"));
  const storedId = normalizeSessionId(localStorage.getItem(SESSION_KEY));
  if (cookieId) {
    if (storedId !== cookieId) {
      localStorage.setItem(SESSION_KEY, cookieId);
    }
    return cookieId;
  }
  if (storedId) {
    return storedId;
  }
  clearSession();
  return null;
}

export function setSessionUserId(idUsuario) {
  const normalizedId = normalizeSessionId(idUsuario);
  if (!normalizedId) return;
  localStorage.setItem(SESSION_KEY, normalizedId);
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
  return getSessionUserId();
}

export async function attachLogout(clearServerSession, clearCartCache) {
  const btn = document.querySelector(".logout-btn");
  if (!btn) return;
  const ensureLogoutModal = () => {
    let modal = document.querySelector("#logout-confirm-modal");
    if (modal) return modal;

    const style = document.createElement("style");
    style.id = "logout-confirm-style";
    style.textContent = `
      .logout-confirm-modal {
        position: fixed;
        inset: 0;
        z-index: 9999;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .logout-confirm-modal.hidden {
        display: none;
      }
      .logout-confirm-backdrop {
        position: absolute;
        inset: 0;
        background: rgba(0, 0, 0, 0.5);
      }
      .logout-confirm-card {
        position: relative;
        width: min(92vw, 420px);
        border-radius: 14px;
        background: #fff;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.25);
        padding: 18px;
      }
      .logout-confirm-title {
        margin: 0 0 14px 0;
        font-size: 18px;
        font-weight: 800;
        color: #111827;
      }
      .logout-confirm-actions {
        display: flex;
        gap: 10px;
        justify-content: flex-end;
      }
      .logout-confirm-actions button {
        min-width: 112px;
      }
    `;
    if (!document.querySelector("#logout-confirm-style")) {
      document.head.appendChild(style);
    }

    modal = document.createElement("div");
    modal.id = "logout-confirm-modal";
    modal.className = "logout-confirm-modal hidden";
    modal.setAttribute("aria-hidden", "true");
    modal.innerHTML = `
      <div class="logout-confirm-backdrop" data-close="1"></div>
      <div class="logout-confirm-card" role="dialog" aria-modal="true" aria-labelledby="logout-confirm-title">
        <h3 id="logout-confirm-title" class="logout-confirm-title">¿Quieres cerrar sesión?</h3>
        <div class="logout-confirm-actions">
          <button type="button" class="btn-outline" id="logout-cancel-btn">Cancelar</button>
          <button type="button" class="btn-primary" id="logout-confirm-btn">Cerrar sesión</button>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    return modal;
  };

  const runLogout = async () => {
    clearSession();
    if (typeof clearServerSession === "function") {
      await clearServerSession();
    }
    if (typeof clearCartCache === "function") {
      clearCartCache();
    }
    const prefix = getPathPrefix();
    window.location.href = `${prefix}login.html`;
  };

  btn.addEventListener("click", async () => {
    const modal = ensureLogoutModal();
    const cancelBtn = modal.querySelector("#logout-cancel-btn");
    const confirmBtn = modal.querySelector("#logout-confirm-btn");
    const backdrop = modal.querySelector(".logout-confirm-backdrop");

    const close = () => {
      modal.classList.add("hidden");
      modal.setAttribute("aria-hidden", "true");
      document.removeEventListener("keydown", onKeydown);
    };
    const onKeydown = (e) => {
      if (e.key === "Escape") close();
    };
    const onCancel = () => close();
    const onBackdrop = (e) => {
      if (e.target?.dataset?.close === "1") close();
    };
    const onConfirm = async () => {
      confirmBtn.disabled = true;
      try {
        await runLogout();
      } finally {
        confirmBtn.disabled = false;
      }
    };

    cancelBtn.onclick = onCancel;
    backdrop.onclick = onBackdrop;
    confirmBtn.onclick = onConfirm;
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
    document.addEventListener("keydown", onKeydown);
  });
}

export function attachLogoHome() {
  const buildHomeHref = () => {
    const { origin, pathname } = window.location;
    const idx = pathname.indexOf("/pages/");
    if (idx >= 0) {
      const base = pathname.slice(0, idx + "/pages/".length);
      return `${origin}${base}index.html`;
    }
    return `${origin}/`;
  };
  const homeHref = buildHomeHref();
  document.querySelectorAll(".logo").forEach((logo) => {
    logo.addEventListener("click", () => {
      window.location.href = homeHref;
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

// Marca el dot de inventario si notificacion_inventario == true
export function updateDeliveryDot(show = false) {
  const inventarioLink = Array.from(document.querySelectorAll("a")).find((a) =>
    (a.getAttribute("href") || "").includes("inventario.html")
  );
  if (!inventarioLink) return;
  let dot = inventarioLink.querySelector(".delivery-dot");
  if (!dot) {
    dot = document.createElement("span");
    dot.className = "delivery-dot";
    inventarioLink.appendChild(dot);
  }
  dot.classList.toggle("hidden", !show);
}
