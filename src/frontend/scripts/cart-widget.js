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
      const descuentos = catalog?.descuentos || [];
      const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
      const getClosestDiscountPct = (rows, value, column) => {
        const key = Number(value) || 0;
        if (!Array.isArray(rows) || key <= 0) return 0;
        const exact = rows.find((d) => Number(d.meses) === key);
        const exactVal = exact?.[column];
        if (exactVal !== null && exactVal !== undefined && exactVal !== "") {
          return Number(exactVal) || 0;
        }
        let best = null;
        for (const d of rows) {
          const n = Number(d?.meses);
          if (!Number.isFinite(n) || n > key) continue;
          const raw = d?.[column];
          if (raw === null || raw === undefined || raw === "") continue;
          if (!best || n > Number(best.meses)) best = d;
        }
        return Number(best?.[column]) || 0;
      };
      const getDiscountColumnsFromRows = (rows = []) => {
        const cols = new Set();
        (rows || []).forEach((row) => {
          Object.keys(row || {}).forEach((k) => {
            const key = String(k || "").toLowerCase();
            if (/^descuento_\d+$/i.test(key)) cols.add(key);
          });
        });
        const out = Array.from(cols).sort((a, b) => {
          const na = Number(a.split("_")[1]) || 0;
          const nb = Number(b.split("_")[1]) || 0;
          return na - nb;
        });
        return out.length ? out : ["descuento_1", "descuento_2"];
      };
      const buildDiscountColumnByIdMap = (rows = [], cols = []) => {
        const ids = [];
        (rows || []).forEach((row) => {
          const id = Number(row?.id_descuento);
          if (!Number.isFinite(id)) return;
          if (!ids.includes(id)) ids.push(id);
        });
        const map = {};
        ids.forEach((id, idx) => {
          if (cols[idx]) map[id] = cols[idx];
        });
        return map;
      };
      const resolveDiscountColumn = (
        platform,
        mode,
        discountColumns,
        discountColumnById,
        isCliente = true,
      ) => {
        const isItemsMode = mode === "items";
        const groupField = isItemsMode
          ? isCliente
            ? "id_descuento_cantidad_detal"
            : "id_descuento_cantidad_mayor"
          : isCliente
            ? "id_descuento_mes_detal"
            : "id_descuento_mes_mayor";
        const legacyField = isItemsMode ? "id_descuento_cantidad" : "id_descuento_mes";
        const preferredRaw = platform?.[groupField];
        const hasPreferredRaw =
          preferredRaw !== null &&
          preferredRaw !== undefined &&
          String(preferredRaw).trim() !== "";
        const raw = hasPreferredRaw ? preferredRaw : platform?.[legacyField];
        const asText = String(raw || "").trim();
        if (/^descuento_\d+$/i.test(asText)) return asText.toLowerCase();
        const asNum = Number(raw);
        if (Number.isFinite(asNum) && asNum >= 1) {
          const direct = `descuento_${Math.trunc(asNum)}`;
          if (discountColumns.includes(direct)) return direct;
          const mapped = discountColumnById[Math.trunc(asNum)];
          if (mapped) return mapped;
        }
        return mode === "items" ? "descuento_2" : "descuento_1";
      };
      const isDiscountEnabledForAudience = (platform, mode = "months", isCliente = true) => {
        if (mode === "items") {
          return isCliente
            ? !(
                platform?.aplica_descuento_cantidad_detal === false ||
                platform?.aplica_descuento_cantidad_detal === "false" ||
                platform?.aplica_descuento_cantidad_detal === 0 ||
                platform?.aplica_descuento_cantidad_detal === "0"
              )
            : !(
                platform?.aplica_descuento_cantidad_mayor === false ||
                platform?.aplica_descuento_cantidad_mayor === "false" ||
                platform?.aplica_descuento_cantidad_mayor === 0 ||
                platform?.aplica_descuento_cantidad_mayor === "0"
              );
        }
        return isCliente
          ? !(
              platform?.aplica_descuento_mes_detal === false ||
              platform?.aplica_descuento_mes_detal === "false" ||
              platform?.aplica_descuento_mes_detal === 0 ||
              platform?.aplica_descuento_mes_detal === "0"
            )
          : !(
              platform?.aplica_descuento_mes_mayor === false ||
              platform?.aplica_descuento_mes_mayor === "false" ||
              platform?.aplica_descuento_mes_mayor === 0 ||
              platform?.aplica_descuento_mes_mayor === "0"
            );
      };
      const discountColumns = getDiscountColumnsFromRows(descuentos);
      const discountColumnById = buildDiscountColumnByIdMap(descuentos, discountColumns);
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
        const qty = Number(item.cantidad || price.cantidad || 1) || 1;
        const meses = Number(item.meses || price.duracion || 1) || 1;
        const baseUnit = flags.por_pantalla ? "pantalla" : flags.por_acceso ? "dispositivo" : "mes";
        const plural = qty === 1 ? "" : baseUnit === "mes" ? "es" : "s";
        const mesesTxt = `${meses} mes${meses === 1 ? "" : "es"}`;
        const unit = Number(price.precio_usd_detal) || 0;
        const isCliente = true;
        const baseSubtotal = round2(unit * qty * (flags.tarjeta_de_regalo ? 1 : meses));
        const monthEnabled =
          !!platform?.descuento_meses &&
          !flags.tarjeta_de_regalo &&
          isDiscountEnabledForAudience(platform, "months", isCliente);
        const qtyEnabled = isDiscountEnabledForAudience(platform, "items", isCliente);
        const monthColumn = resolveDiscountColumn(
          platform,
          "months",
          discountColumns,
          discountColumnById,
          isCliente,
        );
        const qtyColumn = resolveDiscountColumn(
          platform,
          "items",
          discountColumns,
          discountColumnById,
          isCliente,
        );
        const rawRateMeses = monthEnabled
          ? getClosestDiscountPct(descuentos, meses, monthColumn)
          : 0;
        const rawRateQty = qtyEnabled ? getClosestDiscountPct(descuentos, qty, qtyColumn) : 0;
        const rateMeses = rawRateMeses > 1 ? rawRateMeses / 100 : rawRateMeses;
        const rateQty = rawRateQty > 1 ? rawRateQty / 100 : rawRateQty;
        const descuentoMesesVal = rateMeses > 0 ? round2(baseSubtotal * rateMeses) : 0;
        const descuentoCantidadVal = rateQty > 0 ? round2(baseSubtotal * rateQty) : 0;
        const descuentoTotal = round2(descuentoMesesVal + descuentoCantidadVal);
        const montoUsd = round2(baseSubtotal - descuentoTotal);
        const detalle = (() => {
          if (flags.tarjeta_de_regalo) {
            const region = price.region || "-";
            return `Región: ${region} · Cantidad: ${qty}`;
          }
          return `${qty} ${baseUnit}${plural} · ${mesesTxt}`;
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
          cantidad: qty,
          meses,
          detalle,
          monto_usd: montoUsd,
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
