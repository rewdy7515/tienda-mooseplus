import { loadCatalog, ensureServerSession, fetchEntregadas } from "./api.js";
import { initSearch } from "./search.js";
import { requireSession, attachLogoHome, setSessionRoles, getSessionRoles } from "./session.js";
import { loadCurrentUser } from "./api.js";

if (!window.__headerActionsInit) {
  window.__headerActionsInit = true;
  const pagesRoot = window.__headerPages || "";

  // Sesión / usuario
  const initUser = async () => {
    try {
      const userId = requireSession();
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
      const isAdmin =
        isTrue(roles?.permiso_admin) ||
        isTrue(roles?.permiso_superadmin) ||
        isTrue(user?.permiso_admin) ||
        isTrue(user?.permiso_superadmin);
      if (adminLink) {
        adminLink.classList.toggle("hidden", !isAdmin);
        adminLink.style.display = isAdmin ? "block" : "none";
      }
      // entregas badge
      fetchEntregadas()
        .then((resp) => {
          if (resp?.entregadas > 0) {
            const badge = document.querySelector("#btn-stock .delivery-badge");
            if (badge) {
              badge.textContent = resp.entregadas;
              badge.classList.remove("hidden");
            }
          }
        })
        .catch(() => {});
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

  const btnCheckout = document.querySelector("#btn-checkout");
  btnCheckout?.addEventListener("click", () => {
    window.location.href = `${pagesRoot}checkout.html`;
  });

  const btnStock = document.querySelector("#btn-stock");
  btnStock?.addEventListener("click", () => {
    window.location.href = `${pagesRoot}stock.html`;
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
