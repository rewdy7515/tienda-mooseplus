import { loadCatalog, ensureServerSession, fetchEntregadas } from "./api.js";
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

if (!window.__headerActionsInit) {
  window.__headerActionsInit = true;
  const pagesRoot = window.__headerPages || "";

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
            window.location.href = `${pagesRoot}login.html`;
          });
        }
        if (userMenu) {
          userMenu.classList.add("hidden");
          userMenu.style.display = "none";
        }
      };
      if (!userId) {
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
      const roles = getSessionRoles();
      const usernameEl = document.querySelector(".username");
      if (user && usernameEl) {
        const fullName = [user.nombre, user.apellido].filter(Boolean).join(" ").trim();
        usernameEl.textContent = fullName || user.correo || "Usuario";
      }
      const adminLink = document.querySelector(".admin-link");
      const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
      const isSuper =
        isTrue(roles?.permiso_superadmin) || isTrue(user?.permiso_superadmin);
      const isAdmin =
        isTrue(roles?.permiso_admin) ||
        isSuper ||
        isTrue(user?.permiso_admin);

      // Bloqueo de acceso a páginas admin si no tiene permisos
      const isAdminPath = window.location.pathname.includes("/pages/admin/");
      if (isAdminPath && !isAdmin) {
        window.location.href = `${pagesRoot}index.html`;
        return;
      }

      if (adminLink) {
        adminLink.classList.toggle("hidden", !isAdmin);
        adminLink.style.display = isAdmin ? "block" : "none";
      }
      if (adminHeaderBtn) {
        adminHeaderBtn.classList.toggle("hidden", !isAdmin);
        adminHeaderBtn.style.display = isAdmin ? "inline-flex" : "none";
      }
      // Dot de inventario según notificacion_inventario
      if (user?.notificacion_inventario) {
        const inventarioLink = Array.from(document.querySelectorAll("a")).find(
          (a) => a.textContent?.trim().toLowerCase().startsWith("inventario")
        );
        if (inventarioLink) {
          let dot = inventarioLink.querySelector(".delivery-dot");
          if (!dot) {
            dot = document.createElement("span");
            dot.className = "delivery-dot";
            inventarioLink.appendChild(dot);
          }
          dot.classList.remove("hidden");
          // al hacer click, limpiar flag
          inventarioLink.addEventListener(
            "click",
            async () => {
              try {
                await supabase.from("usuarios").update({ notificacion_inventario: false }).eq("id_usuario", userId);
              } catch (err) {
                console.error("clear notificacion_inventario error", err);
              }
            },
            { once: true }
          );
        }
      } else {
        const inventarioLink = Array.from(document.querySelectorAll("a")).find(
          (a) => a.textContent?.trim().toLowerCase().startsWith("inventario")
        );
        const dot = inventarioLink?.querySelector(".delivery-dot");
        if (dot) dot.classList.add("hidden");
      }
    } catch (err) {
      console.error("header user init error", err);
    }
  };
  initUser();

  attachLogoHome();

  // Botones de carrito / checkout / stock
  const btnViewCart = document.querySelector("#btn-view-cart");
  btnViewCart?.addEventListener("click", () => {
    window.location.href = `${pagesRoot}cart.html`;
  });

  const adminHeaderBtn = document.querySelector("#btn-admin-header");
  adminHeaderBtn?.addEventListener("click", () => {
    window.location.href = `${pagesRoot}admin/admin_cuentas.html`;
  });

  const btnCheckout = document.querySelector("#btn-checkout");
  btnCheckout?.addEventListener("click", () => {
    window.location.href = `${pagesRoot}checkout.html`;
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
            if (id) window.location.href = `${pagesRoot}index.html?plataforma=${id}`;
          },
        });
      })
      .catch((err) => console.error("header search init error", err));
  }
}
