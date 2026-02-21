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
import { resolveAvatarForDisplay } from "./avatar-fallback.js";

if (!window.__headerActionsInit) {
  window.__headerActionsInit = true;
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

  const applyHeaderAvatar = async (user = null, idUsuario = null) => {
    const avatar = await resolveAvatarForDisplay({ user, idUsuario });
    const foto = String(avatar?.url || "").trim();
    const fondo = String(avatar?.color || "").trim();
    document.querySelectorAll(".avatar").forEach((img) => {
      if (foto) img.src = foto;
      img.style.backgroundColor = fondo;
    });
  };

  const headerEl = document.querySelector(".header");
  const adminHeaderBtn = document.querySelector("#btn-admin-header");
  let headerHeight = 0;
  let hideOffset = 0;
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
          userMenu.classList.toggle("open");
        };
        userMenu.addEventListener("click", toggleMenu);
        userMenu.addEventListener("touchstart", toggleMenu, { passive: true });
        userDropdown.addEventListener("click", (e) => e.stopPropagation());
        userDropdown.addEventListener("touchstart", (e) => e.stopPropagation(), { passive: true });
        document.addEventListener("click", (e) => {
          if (!userMenu.contains(e.target)) {
            userMenu.classList.remove("open");
          }
        });
        document.addEventListener(
          "touchstart",
          (e) => {
            if (!userMenu.contains(e.target)) {
              userMenu.classList.remove("open");
            }
          },
          { passive: true }
        );
      }
      const showLogin = () => {
        if (loginBtn) {
          loginBtn.classList.remove("hidden");
          loginBtn.addEventListener("click", () => {
            window.location.href = toAbs("login.html");
          });
        }
        if (userMenu) {
          userMenu.classList.add("hidden");
          userMenu.style.display = "none";
        }
      };
      if (!userId) {
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
      if (headerEl) {
        headerEl.classList.toggle("has-admin-btn", !!isAdmin);
      }
      // Punto verde en Notificaciones según notificacion_inventario
      const notifLink = Array.from(document.querySelectorAll("a")).find(
        (a) => a.textContent?.trim().toLowerCase().startsWith("notificaciones")
      );
      const notifDot = notifLink?.querySelector(".notify-dot");
      if (isTrue(user?.notificacion_inventario)) {
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
    } catch (err) {
      console.error("header user init error", err);
    }
  };
  initUser();

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
