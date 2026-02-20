import { fetchCart, loadCatalog, ensureServerSession } from "./api.js";
import { initCart } from "./cart.js";

if (!window.__cartWidgetInit) {
  window.__cartWidgetInit = true;
  (async () => {
    try {
      await ensureServerSession();
      const [cartResp, catalog] = await Promise.all([fetchCart(), loadCatalog()]);
      const cartId = cartResp?.id_carrito;
      if (!cartId) {
        return;
      }
      const items = cartResp?.items || [];
      const precios = catalog?.precios || [];
      const plataformas = catalog?.plataformas || [];
      const priceById = precios.reduce((acc, p) => {
        acc[p.id_precio] = p;
        return acc;
      }, {});
      const platformById = plataformas.reduce((acc, p) => {
        acc[p.id_plataforma] = p;
        return acc;
      }, {});
      const mapped = items.map((item) => {
        const price = priceById[item.id_precio] || {};
        const platform = platformById[price.id_plataforma] || {};
        const flags = {
          por_pantalla: platform.por_pantalla,
          por_acceso: platform.por_acceso,
          tarjeta_de_regalo: platform.tarjeta_de_regalo,
        };
        const detalle = (() => {
          if (flags.tarjeta_de_regalo) {
            const region = price.region || "-";
            const monto = `${price.valor_tarjeta_de_regalo || ""} ${price.moneda || ""} $${price.precio_usd_detal || ""}`;
            return `Región: ${region} · Monto: ${monto}`;
          }
          const qty = item.cantidad || price.cantidad || 1;
          const meses = item.meses || price.duracion || 1;
          const baseUnit = flags.por_pantalla ? "pantalla" : flags.por_acceso ? "dispositivo" : "mes";
          const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
          const mesesTxt = baseUnit === "mes" ? ` · ${meses} mes${meses === 1 ? "" : "es"}` : "";
          return `${qty} ${baseUnit}${plural}${mesesTxt} $${price.precio_usd_detal || ""}`;
        })();
        return {
          id_item: item.id_item,
          id_precio: item.id_precio,
          id_plataforma: price.id_plataforma,
          id_cuenta: item.id_cuenta || null,
          id_perfil: item.id_perfil || null,
          nombre: platform.nombre || `Precio ${item.id_precio}`,
          imagen: platform.imagen,
          plan: price.plan,
          precio: price.precio_usd_detal,
          cantidad: item.cantidad,
          meses: item.meses,
          detalle,
          flags,
          renovacion: item.renovacion === true,
          id_venta: item.id_venta || null,
          correo: item.correo || null,
          n_perfil: item.n_perfil || null,
        };
      });

      const drawerEl = document.querySelector("#cart-drawer");
      const backdropEl = document.querySelector("#cart-drawer .cart-backdrop");
      const closeEl = document.querySelector("#cart-close");
      const iconEl = document.querySelector(".carrito");
      const itemsContainer = document.querySelector("#cart-items");
      if (drawerEl && backdropEl && closeEl && iconEl && itemsContainer) {
        initCart({
          drawerEl,
          backdropEl,
          closeEl,
          iconEl,
          itemsContainer,
          catalog,
          initialItems: mapped,
          initialRawItems: items,
        });
      }
    } catch (err) {
      console.error("cart widget init error", err);
    }
  })();
}
