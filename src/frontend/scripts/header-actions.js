import {
  loadCatalog,
  ensureServerSession,
  fetchEntregadas,
  fetchCart,
  submitCheckout,
} from "./api.js";
import { initSearch } from "./search.js";
import {
  attachLogoHome,
  setSessionRoles,
  getSessionRoles,
  getSessionUserId,
  showDeliveryNotice,
  getDeliverySeen,
  setDeliverySeen,
} from "./session.js";
import { loadCurrentUser, supabase } from "./api.js";
import { applyAvatarImage, resolveAvatarForDisplay } from "./avatar-fallback.js";
import { loadPaginaBranding } from "./branding.js";

if (!window.__headerActionsInit) {
  window.__headerActionsInit = true;
  const HEADER_AVATAR_CACHE_PREFIX = "headerAvatarCache";
  const pagesRoot = window.__headerPages || "";
  const basePagesUrl = (() => {
    try {
      return new URL(pagesRoot || "./", window.location.href);
    } catch (_) {
      return new URL(window.location.href);
    }
  })();

  const toAbs = (path, baseOverride = null) => {
    try {
      return new URL(path, baseOverride || basePagesUrl).href;
    } catch (_) {
      return path;
    }
  };

  const normalizeColor = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const withHash = raw.startsWith("#") ? raw : `#${raw}`;
    if (/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash)) return withHash.toLowerCase();
    return "";
  };
  const loadHeaderBranding = async () => {
    const result = await loadPaginaBranding({
      logoSelectors: [".logo"],
      applyFavicon: true,
    });
    if (result?.error) {
      console.error("header logo load error", result.error);
    }
  };

  const writeHeaderAvatarCache = (idUsuario = null, avatar = null) => {
    try {
      if (!idUsuario) return;
      const url = String(avatar?.url || "").trim();
      const color = normalizeColor(avatar?.color);
      if (!url || !color) return;
      const key = `${HEADER_AVATAR_CACHE_PREFIX}:${String(idUsuario)}`;
      localStorage.setItem(key, JSON.stringify({ url, color }));
    } catch (_err) {
      // noop
    }
  };

  const applyHeaderAvatar = async (user = null, idUsuario = null) => {
    const rawFoto = String(user?.foto_perfil || "").trim();
    let foto = "";
    let fondo = "";
    if (rawFoto) {
      const avatar = await resolveAvatarForDisplay({ user, idUsuario });
      foto = String(avatar?.url || rawFoto).trim();
      fondo = normalizeColor(avatar?.color);
    }
    const effectiveUserId = idUsuario || user?.id_usuario || null;
    writeHeaderAvatarCache(effectiveUserId, { url: foto, color: fondo });
    document.querySelectorAll(".avatar").forEach((img) => {
      applyAvatarImage(img, foto, {
        hideOnInvalid: false,
        emptyFrameColor: "#ffffff",
      });
      img.style.backgroundColor = foto ? fondo : "#ffffff";
    });
  };

  const headerEl = document.querySelector(".header");
  const tutorialHeaderBtn = document.querySelector("#btn-tutorial-header");
  const adminHeaderBtn = document.querySelector("#btn-admin-header");
  const ordenesHeaderBtn = document.querySelector("#btn-ordenes-header");
  const ensureHeaderTutorialStyles = () => {
    if (document.querySelector("#header-tutorial-style")) return;
    const style = document.createElement("style");
    style.id = "header-tutorial-style";
    style.textContent = `
      .header-tutorial-overlay {
        position: fixed;
        inset: 0;
        z-index: 9998;
        background: transparent;
      }
      .header-tutorial-overlay.hidden {
        display: none;
      }
      .header-tutorial-spotlight {
        position: fixed;
        z-index: 10003;
        border-radius: 12px;
        border: 3px solid #22c55e;
        pointer-events: none;
        box-shadow: 0 0 0 9999px rgba(10, 10, 10, 0.58);
        opacity: 0;
        width: 0;
        height: 0;
      }
      .header-tutorial-card {
        position: fixed;
        width: min(92vw, 420px);
        max-width: calc(100vw - 16px);
        max-height: calc(100vh - 16px);
        overflow: auto;
        z-index: 10004;
        border-radius: 14px;
        background: #fff;
        color: #111827;
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.35);
        padding: 16px;
      }
      .header-tutorial-card h4 {
        margin: 0 0 8px 0;
        font-size: 18px;
        font-weight: 800;
      }
      .header-tutorial-card p {
        margin: 0;
        line-height: 1.45;
      }
      .header-tutorial-actions {
        margin-top: 14px;
        display: flex;
        gap: 8px;
        justify-content: flex-end;
      }
      .header-tutorial-target {
        z-index: 10000 !important;
      }
      @media (max-width: 640px) {
        .header-tutorial-card {
          left: max(8px, env(safe-area-inset-left));
          right: max(8px, env(safe-area-inset-right));
          width: auto;
          max-width: none;
          max-height: calc(100vh - 24px - env(safe-area-inset-top) - env(safe-area-inset-bottom));
          bottom: max(8px, env(safe-area-inset-bottom));
          padding: 12px;
          border-radius: 12px;
        }
        .header-tutorial-card h4 {
          font-size: 16px;
        }
        .header-tutorial-card p {
          font-size: 14px;
          line-height: 1.4;
        }
        .header-tutorial-actions {
          gap: 6px;
          flex-wrap: wrap;
        }
        .header-tutorial-actions .btn-outline,
        .header-tutorial-actions .btn-primary {
          flex: 1 1 calc(50% - 6px);
          min-height: 40px;
          font-size: 14px;
        }
      }
    `;
    document.head.appendChild(style);
  };
  const ensureHeaderTutorialUi = () => {
    ensureHeaderTutorialStyles();
    let overlay = document.querySelector("#header-tutorial-overlay");
    if (!overlay) {
      overlay = document.createElement("div");
      overlay.id = "header-tutorial-overlay";
      overlay.className = "header-tutorial-overlay hidden";
      overlay.innerHTML = `
        <div id="header-tutorial-spotlight" class="header-tutorial-spotlight"></div>
        <div class="header-tutorial-card" role="dialog" aria-modal="true" aria-labelledby="header-tutorial-title">
          <h4 id="header-tutorial-title">Tutorial</h4>
          <p id="header-tutorial-text"></p>
          <div class="header-tutorial-actions">
            <button type="button" class="btn-outline" id="header-tutorial-prev">Anterior</button>
            <button type="button" class="btn-outline" id="header-tutorial-next">Siguiente</button>
            <button type="button" class="btn-primary" id="header-tutorial-close">Finalizar</button>
          </div>
        </div>
      `;
      document.body.appendChild(overlay);
    }
    return overlay;
  };
  const startHeaderTutorial = ({ onFinish = null } = {}) => {
    const overlay = ensureHeaderTutorialUi();
    const titleEl = overlay.querySelector("#header-tutorial-title");
    const textEl = overlay.querySelector("#header-tutorial-text");
    const cardEl = overlay.querySelector(".header-tutorial-card");
    const spotlightEl = overlay.querySelector("#header-tutorial-spotlight");
    const prevBtn = overlay.querySelector("#header-tutorial-prev");
    const nextBtn = overlay.querySelector("#header-tutorial-next");
    const closeBtn = overlay.querySelector("#header-tutorial-close");
    const userMenuEl = document.querySelector(".user-menu");
    const userDropdownEl = document.querySelector(".user-dropdown");
    const platformModalEl = document.querySelector("#platform-modal");
    const platformModalCloseBtn = document.querySelector("#platform-modal .modal-close");
    const steps = [
      {
        selector: ".user-menu",
        title: "Menú de usuario",
        text: "Desde aquí accedes a tus opciones de cuenta.",
      },
      {
        selector: ".user-dropdown",
        title: "Botones del menú",
        text: "Aquí verás las opciones disponibles de tu cuenta.",
        openUserMenu: true,
      },
      {
        selector: '.user-dropdown a[href*="mi_perfil.html"]',
        title: "Mi perfil",
        text: "Aquí puedes actualizar tus datos personales.",
        openUserMenu: true,
      },
      {
        selector: ".saldo-item",
        title: "Saldo",
        text: "Aquí verás el saldo disponible de tu cuenta.",
        openUserMenu: true,
      },
      {
        selector: '.user-dropdown a[href*="inventario.html"]',
        title: "Mis cuentas",
        text: "Aquí encontrarás todos tus servicios activos. También podrás renovar y reportar problemas.",
        openUserMenu: true,
      },
      {
        selector: '.user-dropdown a[href*="notificaciones.html"]',
        title: "Notificaciones",
        text: "Aquí verás avisos importantes de tus servicios.",
        openUserMenu: true,
      },
      {
        selector: ".reportes-link",
        title: "Reportes",
        text: "Aquí puedes crear y revisar reportes de tus servicios.",
        openUserMenu: true,
      },
      {
        selector: '.user-dropdown a[href*="historial_ordenes.html"]',
        title: "Ordenes",
        text: "Aquí puedes consultar tus órdenes.",
        openUserMenu: true,
      },
      {
        selector: "#search-input",
        title: "Barra de buscar plataforma",
        text: "Usa esta barra para buscar plataformas rápidamente.",
      },
      {
        selector: ".plataformas-grid .plataforma-card:first-child",
        title: "Presionar el producto",
        text: "Primero presiona un producto para abrir sus planes disponibles.",
        openProductModal: false,
        closeProductModal: true,
      },
      {
        selector: "#modal-precios",
        title: "Presionar plan",
        text: "Luego presiona el plan que quieres comprar.",
        openProductModal: true,
      },
      {
        selector: ".modal-qty-group",
        title: "Meses y cantidad",
        text: "Aquí eliges meses y cantidad. Mientras más meses o cantidades elijas, más descuentos puedes obtener.",
        openProductModal: true,
      },
      {
        selector: "#add-cart",
        title: "Añadir al carrito",
        text: "Cuando termines de configurar, presiona Añadir al carrito.",
        openProductModal: true,
      },
      {
        selector: ".carrito",
        title: "Icono de carrito",
        text: "Aquí ves tu carrito y puedes continuar con la compra.",
        closeProductModal: true,
      },
    ];
    const hasProductCatalog = !!document.querySelector(".plataforma-card");
    const visibleSteps = steps.filter((step) => {
      if (step.openProductModal) return hasProductCatalog;
      const el = document.querySelector(step.selector);
      return el && getComputedStyle(el).display !== "none";
    });
    if (!visibleSteps.length) return;

    let index = 0;
    let highlighted = null;
    let openedUserMenuByTutorial = false;
    let forcedDropdownVisibleByTutorial = false;
    let currentTargetRect = null;
    let openedPlatformModalByTutorial = false;
    const blockedScrollKeys = new Set([
      "ArrowUp",
      "ArrowDown",
      "PageUp",
      "PageDown",
      "Home",
      "End",
      " ",
      "Spacebar",
    ]);
    const isPlatformModalOpen = () =>
      !!platformModalEl && !platformModalEl.classList.contains("hidden");
    const openFirstProductModalForTutorial = () => {
      if (isPlatformModalOpen()) return true;
      const firstProductCard = document.querySelector(".plataforma-card");
      if (!firstProductCard) return false;
      firstProductCard.scrollIntoView({ block: "center", behavior: "smooth" });
      firstProductCard.click();
      openedPlatformModalByTutorial = true;
      return isPlatformModalOpen();
    };
    const closeProductModalForTutorial = () => {
      if (!isPlatformModalOpen()) return;
      if (platformModalCloseBtn) {
        platformModalCloseBtn.click();
      } else {
        platformModalEl?.classList.add("hidden");
      }
    };
    const resetSpotlight = () => {
      if (!spotlightEl) return;
      spotlightEl.style.opacity = "0";
      spotlightEl.style.left = "0px";
      spotlightEl.style.top = "0px";
      spotlightEl.style.width = "0px";
      spotlightEl.style.height = "0px";
    };
    const paintSpotlight = (rect) => {
      if (!spotlightEl || !rect) return;
      spotlightEl.style.left = `${Math.max(0, rect.left - 6)}px`;
      spotlightEl.style.top = `${Math.max(0, rect.top - 6)}px`;
      spotlightEl.style.width = `${Math.max(0, rect.width + 12)}px`;
      spotlightEl.style.height = `${Math.max(0, rect.height + 12)}px`;
      spotlightEl.style.opacity = "1";
    };
    const blockScrollInput = (event) => {
      event.preventDefault();
    };
    const blockScrollKeyInput = (event) => {
      if (!blockedScrollKeys.has(event.key)) return;
      event.preventDefault();
    };
    const enableTutorialScrollLock = () => {
      window.addEventListener("wheel", blockScrollInput, { passive: false, capture: true });
      window.addEventListener("touchmove", blockScrollInput, { passive: false, capture: true });
      window.addEventListener("keydown", blockScrollKeyInput, { capture: true });
    };
    const disableTutorialScrollLock = () => {
      window.removeEventListener("wheel", blockScrollInput, { capture: true });
      window.removeEventListener("touchmove", blockScrollInput, { capture: true });
      window.removeEventListener("keydown", blockScrollKeyInput, { capture: true });
    };
    const positionTutorialCard = (targetRect) => {
      if (!cardEl || !targetRect) return;
      const viewportW = window.innerWidth || document.documentElement.clientWidth || 0;
      const viewportH = window.innerHeight || document.documentElement.clientHeight || 0;
      const cardRect = cardEl.getBoundingClientRect();
      const margin = 12;

      let left = targetRect.left;
      if (left + cardRect.width > viewportW - margin) {
        left = viewportW - cardRect.width - margin;
      }
      left = Math.max(margin, left);

      let top = targetRect.bottom + 10;
      if (top + cardRect.height > viewportH - margin) {
        top = targetRect.top - cardRect.height - 10;
      }
      if (top < margin) {
        top = Math.max(margin, Math.min(viewportH - cardRect.height - margin, margin));
      }

      cardEl.style.left = `${left}px`;
      cardEl.style.top = `${top}px`;
      cardEl.style.bottom = "auto";
      cardEl.style.transform = "none";
    };
    const clearHighlight = () => {
      if (highlighted) highlighted.classList.remove("header-tutorial-target");
      highlighted = null;
      currentTargetRect = null;
      resetSpotlight();
      if (forcedDropdownVisibleByTutorial && userDropdownEl) {
        userDropdownEl.style.opacity = "";
        userDropdownEl.style.pointerEvents = "";
        userDropdownEl.style.transform = "";
        forcedDropdownVisibleByTutorial = false;
      }
      if (openedUserMenuByTutorial && userMenuEl) {
        userMenuEl.classList.remove("open");
        openedUserMenuByTutorial = false;
      }
    };
    const close = () => {
      clearHighlight();
      tutorialGuideActive = false;
      if (openedPlatformModalByTutorial) {
        closeProductModalForTutorial();
      }
      overlay.classList.add("hidden");
      document.removeEventListener("keydown", onKeydown);
      window.removeEventListener("resize", onResizeReposition);
      window.removeEventListener("scroll", onResizeReposition, true);
      disableTutorialScrollLock();
    };
    const onKeydown = (e) => {
      if (e.key === "Escape") close();
    };
    const onResizeReposition = () => {
      if (!currentTargetRect) return;
      const rect = highlighted?.getBoundingClientRect();
      if (!rect) return;
      currentTargetRect = rect;
      positionTutorialCard(rect);
      paintSpotlight(rect);
    };
    const render = () => {
      const step = visibleSteps[index];
      if (!step) return;
      clearHighlight();
      if (step.openUserMenu && userMenuEl) {
        userMenuEl.classList.remove("suppress-dropdown");
        userMenuEl.classList.add("open");
        openedUserMenuByTutorial = true;
        if (userDropdownEl) {
          userDropdownEl.style.opacity = "1";
          userDropdownEl.style.pointerEvents = "auto";
          userDropdownEl.style.transform = "translateY(0)";
          forcedDropdownVisibleByTutorial = true;
        }
      }
      if (step.closeProductModal) {
        closeProductModalForTutorial();
      }
      if (step.openProductModal) {
        openFirstProductModalForTutorial();
      }
      const el = document.querySelector(step.selector);
      if (el) {
        el.classList.add("header-tutorial-target");
        highlighted = el;
        // Siempre desplaza al objetivo al cambiar de paso (Siguiente/Anterior).
        el.scrollIntoView({ block: "center", behavior: "smooth" });
        const syncHighlightPosition = () => {
          if (!highlighted) return;
          const liveRect = highlighted.getBoundingClientRect();
          currentTargetRect = liveRect;
          paintSpotlight(liveRect);
          positionTutorialCard(liveRect);
        };
        syncHighlightPosition();
        // Recalculo continuo corto para asegurar alineación durante/tras el scroll suave.
        window.requestAnimationFrame(syncHighlightPosition);
        window.setTimeout(syncHighlightPosition, 80);
        window.setTimeout(syncHighlightPosition, 180);
        window.setTimeout(syncHighlightPosition, 320);
      }
      titleEl.textContent = `${step.title} (${index + 1}/${visibleSteps.length})`;
      textEl.textContent = step.text;
      prevBtn.disabled = index === 0;
      const isLastStep = index >= visibleSteps.length - 1;
      nextBtn.disabled = isLastStep;
      nextBtn.style.display = isLastStep ? "none" : "";
    };

    prevBtn.onclick = () => {
      if (index > 0) {
        index -= 1;
        render();
      }
    };
    nextBtn.onclick = () => {
      if (index < visibleSteps.length - 1) {
        index += 1;
        render();
      }
    };
    closeBtn.onclick = async () => {
      try {
        if (typeof onFinish === "function") {
          await onFinish();
        }
      } catch (err) {
        console.error("tutorial finish callback error", err);
      }
      close();
    };

    overlay.classList.remove("hidden");
    tutorialGuideActive = true;
    document.addEventListener("keydown", onKeydown);
    window.addEventListener("resize", onResizeReposition);
    window.addEventListener("scroll", onResizeReposition, true);
    enableTutorialScrollLock();
    render();
  };
  const setOrdenesPendingBadge = (hasPending) => {
    if (!ordenesHeaderBtn) return;
    ordenesHeaderBtn.classList.toggle("has-pending-verification", !!hasPending);
    ordenesHeaderBtn.setAttribute(
      "title",
      hasPending
        ? "Hay comprobantes pendientes por verificar"
        : "Ordenes",
    );
  };
  const refreshOrdenesPendingBadge = async () => {
    if (!ordenesHeaderBtn) return;
    try {
      const { data: metodosRows, error: metodosErr } = await supabase
        .from("metodos_de_pago")
        .select("id_metodo_de_pago")
        .eq("verificacion_automatica", false);
      if (metodosErr) throw metodosErr;
      const metodosNoAutomaticos = (metodosRows || [])
        .map((row) => Number(row?.id_metodo_de_pago))
        .filter((id) => Number.isFinite(id) && id > 0);
      if (!metodosNoAutomaticos.length) {
        setOrdenesPendingBadge(false);
        return;
      }
      const { data: ordenesRows, error: ordenesErr } = await supabase
        .from("ordenes")
        .select("id_orden")
        .in("id_metodo_de_pago", metodosNoAutomaticos)
        .eq("marcado_pago", true)
        .neq("pago_verificado", true)
        .neq("orden_cancelada", true)
        .limit(1);
      if (ordenesErr) throw ordenesErr;
      setOrdenesPendingBadge((ordenesRows || []).length > 0);
    } catch (err) {
      console.error("ordenes pending badge error", err);
      setOrdenesPendingBadge(false);
    }
  };
  let headerHeight = 0;
  let hideOffset = 0;
  let tutorialGuideActive = false;
  const applyHeaderOffset = () => {
    const visible = Math.max(0, headerHeight - hideOffset);
    document.documentElement.style.setProperty("--header-offset", `${visible}px`);
  };
  const syncHeaderHeight = () => {
    if (!headerEl) return;
    headerHeight = headerEl.offsetHeight || 0;
    document.documentElement.style.setProperty("--header-height", `${headerHeight}px`);
    hideOffset = Math.min(hideOffset, headerHeight);
    headerEl.style.transform = `translateY(${-hideOffset}px)`;
    applyHeaderOffset();
  };

  const initHeaderAutoHide = () => {
    if (!headerEl) return;
    let lastY = window.scrollY || 0;
    let ticking = false;
    const onScroll = () => {
      const y = window.scrollY || 0;
      if (tutorialGuideActive) {
        hideOffset = 0;
        headerEl.style.transform = "translateY(0)";
        applyHeaderOffset();
        lastY = y;
        return;
      }
      if (!ticking) {
        window.requestAnimationFrame(() => {
          const delta = y - lastY;
          if (y <= 0) {
            hideOffset = 0;
          } else {
            hideOffset = Math.min(Math.max(hideOffset + delta, 0), headerHeight);
          }
          headerEl.style.transform = `translateY(${-hideOffset}px)`;
          applyHeaderOffset();
          lastY = y;
          ticking = false;
        });
        ticking = true;
      }
    };
    window.addEventListener("scroll", onScroll, { passive: true });
  };

  if (headerEl) {
    syncHeaderHeight();
    if (window.ResizeObserver) {
      const ro = new ResizeObserver(() => syncHeaderHeight());
      ro.observe(headerEl);
    }
    window.addEventListener("resize", syncHeaderHeight, { passive: true });
    initHeaderAutoHide();
  }

  const normalizeHeaderLinks = () => {
    const headerEl = document.querySelector(".header");
    if (!headerEl) return;
    headerEl.querySelectorAll("a[href]").forEach((a) => {
      const href = a.getAttribute("href") || "";
      if (!href || href === "#" || href.startsWith("http") || href.startsWith("mailto:") || href.startsWith("tel:")) {
        return;
      }
      const usePageBase = !(href.startsWith("./") || href.startsWith("../"));
      const base = usePageBase ? basePagesUrl : window.location.href;
      a.setAttribute("href", toAbs(href, base));
    });
  };

  const bindCartFallback = () => {
    if (window.__cartFallbackBound) return;
    const drawer = document.querySelector("#cart-drawer");
    const backdrop = document.querySelector("#cart-drawer .cart-backdrop");
    const closeBtn = document.querySelector("#cart-close");
    const icon = document.querySelector(".carrito");
    if (!drawer || !icon) return;
    const close = () => drawer.classList.remove("open");
    const open = () => drawer.classList.add("open");
    icon.addEventListener("click", open);
    icon.addEventListener("touchstart", open, { passive: true });
    backdrop?.addEventListener("click", close);
    closeBtn?.addEventListener("click", close);
    drawer.addEventListener("click", (e) => {
      if (e.target.classList.contains("cart-backdrop")) close();
    });
    window.__cartFallbackBound = true;
  };

  // Sesión / usuario
  const initUser = async () => {
    try {
      const userId = getSessionUserId();
      const loginBtn = document.querySelector("#btn-login");
      const userMenu = document.querySelector(".user-menu");
      const userDropdown = document.querySelector(".user-dropdown");
      if (userMenu && userDropdown) {
        const toggleMenu = (e) => {
          e?.stopPropagation();
          const isOpen = userMenu.classList.contains("open");
          if (isOpen) {
            userMenu.classList.remove("open");
            userMenu.classList.add("suppress-dropdown");
            return;
          }
          userMenu.classList.remove("suppress-dropdown");
          userMenu.classList.add("open");
        };
        userMenu.addEventListener("click", toggleMenu);
        userDropdown.addEventListener("click", (e) => e.stopPropagation());
        userMenu.addEventListener("mouseleave", () => {
          userMenu.classList.remove("suppress-dropdown");
        });
        document.addEventListener("click", (e) => {
          if (!userMenu.contains(e.target)) {
            userMenu.classList.remove("open");
            userMenu.classList.remove("suppress-dropdown");
          }
        });
      }
      const showLogin = () => {
        if (loginBtn) {
          loginBtn.classList.remove("hidden");
          loginBtn.addEventListener("click", () => {
            window.location.href = toAbs("login.html");
          });
        }
        if (userMenu) {
          userMenu.classList.remove("has-inventario-notificacion");
          userMenu.classList.add("hidden");
          userMenu.style.display = "none";
        }
      };
      if (!userId) {
        if (tutorialHeaderBtn) {
          tutorialHeaderBtn.classList.add("hidden");
          tutorialHeaderBtn.style.display = "none";
        }
        await applyHeaderAvatar(null, null);
        showLogin();
        bindCartFallback();
        return;
      }
      if (loginBtn) {
        loginBtn.classList.add("hidden");
        loginBtn.style.display = "none";
      }
      if (userMenu) {
        userMenu.classList.remove("hidden");
        userMenu.style.display = "flex";
      }
      bindCartFallback();
      await ensureServerSession();
      const user = await loadCurrentUser();
      const markTutorialCompleted = async () => {
        const uid = Number(user?.id_usuario || userId);
        if (!Number.isFinite(uid) || uid <= 0) return;
        const { error } = await supabase
          .from("usuarios")
          .update({ tutorial_completado: true })
          .eq("id_usuario", uid);
        if (error) throw error;
      };
      const tutorialValue = user?.tutorial_completado;
      const tutorialCompletado =
        tutorialValue === true ||
        tutorialValue === "true" ||
        tutorialValue === 1 ||
        tutorialValue === "1" ||
        tutorialValue === "t";
      const tutorialPendiente =
        tutorialValue == null ||
        tutorialValue === false ||
        tutorialValue === "false" ||
        tutorialValue === 0 ||
        tutorialValue === "0" ||
        tutorialValue === "f";
      const pathName = String(window.location.pathname || "");
      const isIndexPage =
        /\/index\.html$/i.test(pathName) ||
        pathName === "/" ||
        /\/pages\/$/i.test(pathName);
      const waitForIndexLoaderHidden = async () => {
        if (!isIndexPage) return;
        const loader = document.querySelector("#page-loader");
        if (!loader) return;
        const isHidden = () =>
          loader.classList.contains("hidden") ||
          loader.hidden === true ||
          window.getComputedStyle(loader).display === "none" ||
          window.getComputedStyle(loader).visibility === "hidden" ||
          Number(window.getComputedStyle(loader).opacity || "1") === 0;
        if (isHidden()) return;
        await new Promise((resolve) => {
          let done = false;
          const finish = () => {
            if (done) return;
            done = true;
            window.removeEventListener("moose:page-loader-hidden", onLoaderHidden);
            observer.disconnect();
            clearTimeout(safetyTimer);
            resolve();
          };
          const onLoaderHidden = () => finish();
          const observer = new MutationObserver(() => {
            if (isHidden()) finish();
          });
          observer.observe(loader, {
            attributes: true,
            attributeFilter: ["class", "style", "hidden", "aria-hidden"],
          });
          window.addEventListener("moose:page-loader-hidden", onLoaderHidden, { once: true });
          const safetyTimer = window.setTimeout(finish, 6000);
        });
      };
      setSessionRoles(user || {});
      await applyHeaderAvatar(user || null, userId);
      const roles = getSessionRoles();
      const usernameEl = document.querySelector(".username");
      if (user && usernameEl) {
        const fullName = [user.nombre, user.apellido].filter(Boolean).join(" ").trim();
        usernameEl.textContent = fullName || user.correo || "Usuario";
      }
      const saldoEl = document.querySelector(".saldo-item");
      if (saldoEl) {
        const saldoVal = Number(user?.saldo);
        const saldoNum = Number.isFinite(saldoVal) ? saldoVal : 0;
        saldoEl.textContent = `Saldo: $${saldoNum.toFixed(2)}`;
      }
      const adminLink = document.querySelector(".admin-link");
      const historialLink = document.querySelector(".historial-link");
      const reportesLink = document.querySelector(".reportes-link");
      const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
      const isSuper =
        isTrue(roles?.permiso_superadmin) || isTrue(user?.permiso_superadmin);
      const isAdmin =
        isTrue(roles?.permiso_admin) ||
        isSuper ||
        isTrue(user?.permiso_admin);
      const isSuperHist =
        isTrue(roles?.permiso_superadmin) || isTrue(user?.permiso_superadmin);

      // Bloqueo de acceso a páginas admin si no tiene permisos
      const isAdminPath = window.location.pathname.includes("/pages/admin/");
      const isUsuariosPath = window.location.pathname.includes(
        "/pages/admin/usuarios.html"
      );
      if (isUsuariosPath && !isSuper) {
        window.location.href = `${pagesRoot}index.html`;
        return;
      }
      if (isAdminPath && !isAdmin) {
        window.location.href = `${pagesRoot}index.html`;
        return;
      }

      if (adminLink) {
        adminLink.classList.toggle("hidden", !isAdmin);
        adminLink.style.display = isAdmin ? "block" : "none";
      }
      if (historialLink) {
        historialLink.classList.toggle("hidden", !isSuperHist);
        historialLink.style.display = isSuperHist ? "block" : "none";
      }
      if (reportesLink) {
        reportesLink.classList.remove("hidden");
        reportesLink.style.display = "block";
      }
      if (adminHeaderBtn) {
        adminHeaderBtn.classList.toggle("hidden", !isAdmin);
        adminHeaderBtn.style.display = isAdmin ? "inline-flex" : "none";
      }
      if (tutorialHeaderBtn) {
        tutorialHeaderBtn.classList.remove("hidden");
        tutorialHeaderBtn.style.display = "block";
      }
      if (ordenesHeaderBtn) {
        ordenesHeaderBtn.classList.toggle("hidden", !isSuper);
        ordenesHeaderBtn.style.display = isSuper ? "inline-flex" : "none";
        if (isSuper) {
          await refreshOrdenesPendingBadge();
        } else {
          setOrdenesPendingBadge(false);
        }
      }
      if (headerEl) {
        headerEl.classList.toggle("has-admin-btn", !!isAdmin);
      }
      // Punto verde en Notificaciones según notificacion_inventario
      const notifLink = Array.from(document.querySelectorAll("a")).find(
        (a) => a.textContent?.trim().toLowerCase().startsWith("notificaciones")
      );
      const notifDot = notifLink?.querySelector(".notify-dot");
      const hasInventarioNotificacion = isTrue(user?.notificacion_inventario);
      if (userMenu) {
        userMenu.classList.toggle("has-inventario-notificacion", hasInventarioNotificacion);
      }
      if (hasInventarioNotificacion) {
        if (notifDot) {
          notifDot.classList.remove("hidden");
        }
      } else if (notifDot) {
        notifDot.classList.add("hidden");
      }
      notifLink?.addEventListener("click", async (e) => {
        const href = notifLink.getAttribute("href");
        if (href) e.preventDefault();
        try {
          await supabase
            .from("usuarios")
            .update({ notificacion_inventario: false })
            .eq("id_usuario", userId);
        } catch (err) {
          console.error("clear notificacion_inventario error", err);
        } finally {
          if (href) window.location.href = href;
        }
      });

      if (isIndexPage && !tutorialCompletado && tutorialPendiente) {
        await waitForIndexLoaderHidden();
        startHeaderTutorial({
          onFinish: async () => {
            await markTutorialCompleted();
          },
        });
      }

      tutorialHeaderBtn?.addEventListener("click", () => {
        startHeaderTutorial({
          onFinish: async () => {
            await markTutorialCompleted();
          },
        });
      });
    } catch (err) {
      console.error("header user init error", err);
    }
  };
  initUser();
  loadHeaderBranding();

  attachLogoHome();
  normalizeHeaderLinks();

  // Botones de carrito / checkout / stock
  const btnViewCart = document.querySelector("#btn-view-cart");
  btnViewCart?.addEventListener("click", () => {
    window.location.href = toAbs("cart.html", basePagesUrl);
  });

  adminHeaderBtn?.addEventListener("click", () => {
    window.location.href = toAbs("admin/admin_cuentas.html", basePagesUrl);
  });

  ordenesHeaderBtn?.addEventListener("click", () => {
    window.location.href = toAbs("admin/ordenes.html", basePagesUrl);
  });

  const btnCheckout = document.querySelector("#btn-checkout");
  btnCheckout?.addEventListener("click", () => {
    (async () => {
      try {
        await ensureServerSession();
        const user = await loadCurrentUser();
        if (!user?.id_usuario) {
          window.location.href = toAbs("checkout.html", basePagesUrl);
          return;
        }
        const cartData = await fetchCart();
        const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
        const useSaldo = isTrue(cartData?.usa_saldo);
        const montoUsd = Number(cartData?.monto_usd);
        const montoFinal = Number(cartData?.monto_final);
        const checkoutTotal =
          useSaldo && Number.isFinite(montoFinal) ? montoFinal : montoUsd;
        const saldo = Number(user?.saldo) || 0;
        if (!Number.isFinite(checkoutTotal)) {
          window.location.href = toAbs("checkout.html", basePagesUrl);
          return;
        }
        if (!useSaldo) {
          window.location.href = toAbs("checkout.html", basePagesUrl);
          return;
        }
        if (!Number.isFinite(montoUsd) || saldo < montoUsd) {
          window.location.href = toAbs("checkout.html", basePagesUrl);
          return;
        }
        const payload = {
          id_metodo_de_pago: 1,
          referencia: "SALDO",
          comprobantes: [],
          total: montoUsd,
          tasa_bs: null,
        };
        const resp = await submitCheckout(payload);
        if (resp?.error) {
          alert(`Error al procesar: ${resp.error}`);
          return;
        }
        const nuevoSaldo = Math.max(0, Math.round((saldo - montoUsd) * 100) / 100);
        await supabase
          .from("usuarios")
          .update({ saldo: nuevoSaldo })
          .eq("id_usuario", user.id_usuario);
        const dest = resp?.id_orden
          ? `entregar_servicios.html?id_orden=${encodeURIComponent(resp.id_orden)}`
          : "entregar_servicios.html";
        window.location.href = toAbs(dest, basePagesUrl);
      } catch (err) {
        console.error("saldo checkout header error", err);
        window.location.href = toAbs("checkout.html", basePagesUrl);
      }
    })();
  });

  // Búsqueda en el header (usa catálogo para autocompletar)
  const searchInput = document.querySelector("#search-input");
  const searchResults = document.querySelector("#search-results");
  if (searchInput && searchResults) {
    loadCatalog()
      .then((catalog) => {
        const plataformas = catalog?.plataformas || [];
        initSearch({
          input: searchInput,
          results: searchResults,
          data: plataformas,
          onSelectItem: (plat) => {
            const id = plat.id_plataforma;
            if (id) window.location.href = toAbs(`index.html?plataforma=${id}`);
          },
        });
      })
      .catch((err) => console.error("header search init error", err));
  }
}
