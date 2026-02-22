(function () {
  const container = document.getElementById("app-header");
  if (!container) return;
  const HEADER_AVATAR_CACHE_PREFIX = "headerAvatarCache";

  const getCookie = (name) => {
    try {
      const match = document.cookie.match(new RegExp(`(?:^|; )${name}=([^;]*)`));
      return match ? decodeURIComponent(match[1]) : null;
    } catch (_err) {
      return null;
    }
  };

  const normalizeColor = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const withHash = raw.startsWith("#") ? raw : `#${raw}`;
    if (/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash)) return withHash.toLowerCase();
    return "";
  };

  const readHeaderAvatarCache = (idUsuario = null) => {
    try {
      const key = `${HEADER_AVATAR_CACHE_PREFIX}:${idUsuario ? String(idUsuario) : "anon"}`;
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      const url = String(parsed?.url || "").trim();
      const color = normalizeColor(parsed?.color);
      if (!url || !color) return null;
      return { url, color };
    } catch (_err) {
      return null;
    }
  };

  const applyAvatarImage = (img, url = "") => {
    if (!img) return;
    const nextUrl = String(url || "").trim();
    if (!img.dataset.avatarHideOnErrorBound) {
      img.addEventListener("error", () => {
        img.classList.remove("hidden");
        img.removeAttribute("src");
        img.style.backgroundColor = "#ffffff";
      });
      img.addEventListener("load", () => img.classList.remove("hidden"));
      img.dataset.avatarHideOnErrorBound = "1";
    }
    if (!nextUrl) {
      img.classList.remove("hidden");
      img.removeAttribute("src");
      img.style.backgroundColor = "#ffffff";
      return;
    }
    img.classList.remove("hidden");
    img.src = nextUrl;
  };

  const applyCachedHeaderAvatar = () => {
    try {
      const sessionId =
        localStorage.getItem("sessionUserId") || getCookie("session_user_id") || null;
      if (!sessionId) return;
      const cached = readHeaderAvatarCache(sessionId);
      if (!cached?.url) return;
      container.querySelectorAll(".avatar").forEach((img) => {
        applyAvatarImage(img, cached.url);
        img.style.backgroundColor = cached.color || "";
      });
    } catch (_err) {
      // noop
    }
  };

  const root = container.dataset.headerRoot || "../";
  const pages = container.dataset.headerPages || "";
  const partialUrl = `${root}partials/header.html`;

  window.__headerRoot = root;
  window.__headerPages = pages;
  try {
    window.__headerPagesAbs = new URL(pages || "./", window.location.href).pathname;
    window.__headerRootAbs = new URL(root || "./", window.location.href).pathname;
  } catch (_) {
    window.__headerPagesAbs = pages;
    window.__headerRootAbs = root;
  }

  try {
    const xhr = new XMLHttpRequest();
    xhr.open("GET", partialUrl, false); // síncrono para asegurar que el header esté antes de los demás scripts
    xhr.send(null);
    if (xhr.status >= 200 && xhr.status < 300) {
      const html = xhr.responseText
        .replace(/__ROOT__/g, root)
        .replace(/__PAGES__/g, pages);
      container.innerHTML = html;
      applyCachedHeaderAvatar();

      // Inicializa widget de carrito una sola vez
      if (!window.__cartWidgetScriptLoaded) {
        window.__cartWidgetScriptLoaded = true;
        const script = document.createElement("script");
        script.type = "module";
        script.src = `${root}scripts/cart-widget.js`;
        document.body.appendChild(script);
      }
      if (!window.__headerActionsScriptLoaded) {
        window.__headerActionsScriptLoaded = true;
        const script2 = document.createElement("script");
        script2.type = "module";
        script2.src = `${root}scripts/header-actions.js`;
        document.body.appendChild(script2);
      }
    } else {
      console.error("No se pudo cargar el header:", xhr.status, partialUrl);
    }
  } catch (err) {
    console.error("header loader error", err);
  }
})();
