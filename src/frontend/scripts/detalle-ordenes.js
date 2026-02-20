import { requireSession, attachLogoHome, attachLogout } from "./session.js";
import {
  clearServerSession,
  fetchOrdenDetalle,
  fetchVentasOrden,
  loadCatalog,
  loadCurrentUser,
  supabase,
} from "./api.js";
import { formatDDMMYYYY } from "./date-format.js";

requireSession();
attachLogoHome();
attachLogout(clearServerSession);

const statusEl = document.querySelector("#detalle-status");
const subtitleEl = document.querySelector("#detalle-subtitle");
const infoGridEl = document.querySelector("#orden-info-grid");
const itemsEl = document.querySelector("#orden-items");
const itemsTitleEl = document.querySelector("#orden-items-title");
const backBtn = document.querySelector("#btn-volver");

const setStatus = (msg) => {
  if (statusEl) statusEl.textContent = msg || "";
};

const setSubtitle = (msg) => {
  if (subtitleEl) subtitleEl.textContent = msg || "";
};

backBtn?.addEventListener("click", () => {
  window.location.href = "historial_ordenes.html";
});

const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;

const formatMoney = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `$${n.toFixed(2)}`;
};

const formatBs = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `Bs. ${n.toFixed(2)}`;
};

const formatHora12 = (hora) => {
  if (!hora) return "-";
  const raw = String(hora).trim();
  const match = raw.match(/^(\d{1,2}):(\d{2})(?::\d{2}(?:\.\d+)?)?$/);
  if (!match) return raw;
  let hh = Number(match[1]);
  const mm = match[2];
  if (!Number.isFinite(hh)) return raw;
  const suffix = hh >= 12 ? "pm" : "am";
  hh = hh % 12;
  if (hh === 0) hh = 12;
  return `${hh}:${mm} ${suffix}`;
};

const buildEstado = (orden) => {
  if (isTrue(orden?.orden_cancelada)) return "Cancelada";
  if (isTrue(orden?.pago_verificado)) return "Entregado";
  if (isTrue(orden?.en_espera)) return "En espera";
  return "Pendiente";
};

const renderInfo = (orden) => {
  if (!infoGridEl) return;
  const fecha = formatDDMMYYYY(orden?.fecha) || orden?.fecha || "-";
  const hora = formatHora12(orden?.hora_orden);
  const estado = buildEstado(orden);
  const rows = [
    { label: "N. orden", value: orden?.id_orden ?? "-" },
    { label: "Fecha", value: fecha },
    { label: "Hora", value: hora },
    { label: "Estado", value: estado },
    { label: "Total", value: formatMoney(orden?.total) },
    { label: "Tasa (Bs)", value: formatBs(orden?.tasa_bs) },
    { label: "Monto (Bs)", value: formatBs(orden?.monto_bs) },
    { label: "Referencia", value: orden?.referencia || "-" },
    { label: "Carrito", value: orden?.id_carrito ? `#${orden.id_carrito}` : "Sin carrito" },
  ];
  infoGridEl.innerHTML = rows
    .map(
      (row) => `
        <div class="label">${row.label}</div>
        <div class="value">${row.value}</div>
      `
    )
    .join("");
};

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

const calcItemTotals = (item, price, platform, descuentos) => {
  const unit = Number(price?.precio_usd_detal) || 0;
  const qtyVal = Math.max(1, Number(item?.cantidad) || 1);
  const mesesVal = Math.max(1, Number(item?.meses) || Number(price?.duracion) || 1);
  const baseSubtotal = round2(unit * qtyVal * mesesVal);
  let descuentoMesesVal = 0;
  let descuentoCantidadVal = 0;
  let rateMeses = 0;
  if (platform?.descuento_meses) {
    const rawRate = getClosestDiscountPct(descuentos, mesesVal, "descuento_1");
    rateMeses = rawRate > 1 ? rawRate / 100 : rawRate;
    descuentoMesesVal = round2(baseSubtotal * rateMeses);
  }
  const rawRateQty = getClosestDiscountPct(descuentos, qtyVal, "descuento_2");
  const rateQty = rawRateQty > 1 ? rawRateQty / 100 : rawRateQty;
  if (rateQty > 0) {
    descuentoCantidadVal = round2(baseSubtotal * rateQty);
  }
  const descuentoVal = round2(descuentoMesesVal + descuentoCantidadVal);
  const subtotal = round2(baseSubtotal - descuentoVal);
  return {
    unit,
    qtyVal,
    mesesVal,
    baseSubtotal,
    descuentoMesesVal,
    descuentoCantidadVal,
    rateMeses,
    rateQty,
    descuentoVal,
    subtotal,
  };
};

const renderItems = (items, catalog, useMayor) => {
  if (!itemsEl) return;
  if (!Array.isArray(items) || !items.length) {
    itemsEl.innerHTML = '<p class="cart-empty">Esta orden no tiene items asociados.</p>';
    itemsTitleEl?.classList.add("hidden");
    return;
  }
  itemsTitleEl?.classList.remove("hidden");

  const descuentos = catalog?.descuentos || [];
  const precios = (catalog?.precios || [])
    .map((p) => {
      const precio = useMayor && p.precio_usd_mayor != null ? p.precio_usd_mayor : p.precio_usd_detal;
      return { ...p, precio_usd_detal: precio, precio_usd_mayor: undefined };
    })
    .filter((p) => p.id_precio != null);
  const plataformas = catalog?.plataformas || [];

  const priceById = precios.reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});
  const platformById = plataformas.reduce((acc, p) => {
    acc[p.id_plataforma] = p;
    return acc;
  }, {});

  const renderDiscounts = (totals) => {
    const parts = [];
    if (totals.descuentoMesesVal > 0) {
      parts.push(`<span class="discount-badge">-${(totals.rateMeses * 100).toFixed(2)}% mes</span>`);
    }
    if (totals.descuentoCantidadVal > 0) {
      parts.push(`<span class="discount-badge">-${(totals.rateQty * 100).toFixed(2)}% cant.</span>`);
    }
    return parts.join("");
  };

  const rows = items
    .map((item, idx) => {
      const price = priceById[item.id_precio] || {};
      const platform = platformById[price.id_plataforma] || {};
      const totals = calcItemTotals(item, price, platform, descuentos);
      const detalle =
        price.plan || (platform.tarjeta_de_regalo ? `Región: ${price.region || "-"}` : "");
      const tipo = item?.renovacion ? "Renovación" : "Nuevo";
      const nombre = platform.nombre || `Precio ${item.id_precio}`;
      const imagen = platform.imagen || "";
      return `
        <tr data-index="${idx}">
          <td>
            <div class="cart-info tight">
              <div class="cart-product">
                <div class="cart-thumb-sm">
                  <img src="${imagen}" alt="${nombre}" loading="lazy" decoding="async" />
                </div>
                <div class="cart-product-text">
                  <p class="cart-name">${nombre}</p>
                  ${detalle ? `<p class="cart-detail">${detalle}</p>` : ""}
                  <p class="cart-detail">Tipo: ${tipo}</p>
                  <p class="cart-detail cart-price-line">Precio: ${formatMoney(totals.unit)}</p>
                </div>
              </div>
            </div>
          </td>
          <td class="cart-cell-center">
            <div class="cart-qty-inline"><span class="cart-meses-value">${totals.mesesVal}</span></div>
          </td>
          <td class="cart-cell-center">${formatMoney(totals.unit)}</td>
          <td class="cart-cell-center">
            <div class="cart-qty-inline"><span class="cart-cantidad-value">${totals.qtyVal}</span></div>
          </td>
          <td class="cart-cell-center">
            <span class="cart-subtotal-value">${formatMoney(totals.subtotal)}</span>
            <div class="cart-discount-line">${renderDiscounts(totals)}</div>
          </td>
        </tr>
      `;
    })
    .join("");

  const cards = items
    .map((item, idx) => {
      const price = priceById[item.id_precio] || {};
      const platform = platformById[price.id_plataforma] || {};
      const totals = calcItemTotals(item, price, platform, descuentos);
      const detalle =
        price.plan || (platform.tarjeta_de_regalo ? `Región: ${price.region || "-"}` : "");
      const tipo = item?.renovacion ? "Renovación" : "Nuevo";
      const nombre = platform.nombre || `Precio ${item.id_precio}`;
      const imagen = platform.imagen || "";
      return `
        <section class="cart-item-card" data-index="${idx}">
          <div class="cart-item-top">
            <div class="cart-item-logo">
              <img src="${imagen}" alt="${nombre}" loading="lazy" decoding="async" />
            </div>
            <div class="cart-item-info">
              <p class="cart-name">${nombre}</p>
              ${detalle ? `<p class="cart-detail">${detalle}</p>` : ""}
              <p class="cart-detail">Tipo: ${tipo}</p>
              <p class="cart-detail">Precio: ${formatMoney(totals.unit)}</p>
            </div>
          </div>
          <div class="cart-item-bottom">
            <div class="cart-item-cell">
              <span class="cart-item-label">Meses</span>
              <div class="cart-qty-inline"><span class="cart-meses-value">${totals.mesesVal}</span></div>
            </div>
            <div class="cart-item-cell">
              <span class="cart-item-label">Cantidad</span>
              <div class="cart-qty-inline"><span class="cart-cantidad-value">${totals.qtyVal}</span></div>
            </div>
          </div>
          <div class="cart-item-subtotal">
            <span class="cart-item-label">Subtotal</span>
            <div class="cart-subtotal">
              <span class="cart-subtotal-value">${formatMoney(totals.subtotal)}</span>
            </div>
            <div class="cart-discount-line">${renderDiscounts(totals)}</div>
          </div>
        </section>
      `;
    })
    .join("");

  itemsEl.innerHTML = `
    <div class="cart-layout">
      <div class="cart-left">
        <div class="cart-items-cards">
          ${cards}
        </div>
        <div class="cart-table-scroll">
          <table class="table-base cart-page-table">
            <thead>
              <tr>
                <th>Producto</th>
                <th>Meses</th>
                <th>Precio</th>
                <th>Cantidad</th>
                <th>Subtotal</th>
              </tr>
            </thead>
            <tbody>
              ${rows}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;
};

const getIdOrden = () => {
  const params = new URLSearchParams(window.location.search);
  return params.get("id_orden");
};

const fetchItemsByVentasOrden = async (idOrden) => {
  if (!idOrden) return [];
  const resp = await fetchVentasOrden(idOrden);
  if (resp?.error) {
    throw new Error(resp.error);
  }
  const rows = Array.isArray(resp?.ventas) ? resp.ventas : [];
  if (!rows.length) return [];

  const grouped = new Map();
  for (const row of rows) {
    const idPrecio = Number(row?.id_precio);
    if (!Number.isFinite(idPrecio) || idPrecio <= 0) continue;
    const meses = Math.max(1, Number(row?.meses_contratados) || 1);
    const renovacion = isTrue(row?.renovacion);
    const key = `${idPrecio}|${meses}|${renovacion ? 1 : 0}`;
    const prev = grouped.get(key);
    if (prev) {
      prev.cantidad += 1;
      continue;
    }
    grouped.set(key, {
      id_precio: idPrecio,
      cantidad: 1,
      meses,
      renovacion,
      id_venta: row?.id_venta ?? null,
    });
  }
  return Array.from(grouped.values());
};

const fetchItemsByHistorialOrden = async (idOrden, idUsuario) => {
  if (!idOrden || !idUsuario) return [];
  const { data: historialRows, error: histErr } = await supabase
    .from("historial_ventas")
    .select("id_historial_ventas, id_venta, id_orden, id_usuario_cliente, renovacion")
    .eq("id_orden", Number(idOrden))
    .eq("id_usuario_cliente", Number(idUsuario))
    .order("id_historial_ventas", { ascending: true });
  if (histErr) throw histErr;

  const rows = historialRows || [];
  if (!rows.length) return [];
  const ventaIds = Array.from(new Set(rows.map((r) => r.id_venta).filter(Boolean)));
  if (!ventaIds.length) return [];

  const { data: ventasRows, error: ventasErr } = await supabase
    .from("ventas")
    .select("id_venta, id_precio, meses_contratados")
    .in("id_venta", ventaIds);
  if (ventasErr) throw ventasErr;
  const ventaMap = (ventasRows || []).reduce((acc, row) => {
    acc[row.id_venta] = row;
    return acc;
  }, {});

  const grouped = new Map();
  for (const row of rows) {
    const venta = ventaMap[row.id_venta];
    const idPrecio = Number(venta?.id_precio);
    if (!Number.isFinite(idPrecio) || idPrecio <= 0) continue;
    const meses = Math.max(1, Number(venta?.meses_contratados) || 1);
    const renovacion = isTrue(row?.renovacion);
    const key = `${idPrecio}|${meses}|${renovacion ? 1 : 0}`;
    const prev = grouped.get(key);
    if (prev) {
      prev.cantidad += 1;
      continue;
    }
    grouped.set(key, {
      id_precio: idPrecio,
      cantidad: 1,
      meses,
      renovacion,
      id_venta: row?.id_venta ?? null,
    });
  }
  return Array.from(grouped.values());
};

const init = async () => {
  const idOrdenRaw = getIdOrden();
  const idOrden = Number(idOrdenRaw);
  if (!Number.isFinite(idOrden)) {
    setStatus("Falta el parámetro id_orden en la URL.");
    itemsTitleEl?.classList.add("hidden");
    return;
  }
  setStatus("Cargando orden...");
  setSubtitle(`Orden #${idOrden}`);
  try {
    const currentUser = await loadCurrentUser();
    const userId = currentUser?.id_usuario;
    if (!userId) {
      setStatus("Usuario no autenticado.");
      itemsTitleEl?.classList.add("hidden");
      return;
    }

    const detalleResp = await fetchOrdenDetalle(idOrden);
    if (detalleResp?.error) {
      setStatus(detalleResp.error || "No se pudo cargar la orden.");
      itemsTitleEl?.classList.add("hidden");
      return;
    }
    const orden = detalleResp?.orden || null;
    if (!orden) {
      setStatus("Orden no encontrada.");
      itemsTitleEl?.classList.add("hidden");
      return;
    }
    renderInfo(orden);

    setStatus("Cargando items...");
    let items = Array.isArray(detalleResp?.items) ? detalleResp.items : [];
    if (!items.length) {
      try {
        items = await fetchItemsByHistorialOrden(idOrden, userId);
      } catch (err) {
        console.error("fetchItemsByHistorialOrden error", err);
      }
    }
    if (!items.length) {
      try {
        items = await fetchItemsByVentasOrden(idOrden);
      } catch (err) {
        console.error("fetchItemsByVentasOrden error", err);
      }
    }
    if (!items.length) {
      setStatus("");
      renderItems([], null, false);
      return;
    }

    let catalog = null;
    try {
      catalog = await loadCatalog();
    } catch (err) {
      console.error("loadCatalog error", err);
    }
    if (!catalog) {
      setStatus("No se pudieron cargar los items de la orden.");
      itemsEl && (itemsEl.innerHTML = '<p class="cart-empty">No se pudieron cargar los items.</p>');
      return;
    }
    const useMayor = currentUser ? !isTrue(currentUser.acceso_cliente) : false;
    renderItems(items, catalog, useMayor);
    setStatus("");
  } catch (err) {
    console.error("detalle ordenes error", err);
    setStatus("No se pudo cargar el detalle de la orden.");
  }
};

init();
