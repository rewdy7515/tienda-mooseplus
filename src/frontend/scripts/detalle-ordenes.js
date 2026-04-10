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
const infoGridEl = document.querySelector("#orden-info-grid");
const itemsEl = document.querySelector("#orden-items");
const itemsTitleEl = document.querySelector("#orden-items-title");
const backBtn = document.querySelector("#btn-volver");

const setStatus = (msg) => {
  if (statusEl) statusEl.textContent = msg || "";
};

const toPositiveId = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.trunc(parsed);
};

const goToEntregarServiciosByOrden = (idOrden) => {
  const idOrdenNum = toPositiveId(idOrden);
  if (!idOrdenNum) return;
  window.location.href = `entregar_servicios.html?id_orden=${encodeURIComponent(idOrdenNum)}`;
};

const goToEntregarServiciosByOrdenVenta = (idVenta) => {
  const idVentaNum = toPositiveId(idVenta);
  if (!idVentaNum) return;
  window.location.href = `entregar_servicios.html?id_venta=${encodeURIComponent(idVentaNum)}`;
};

backBtn?.addEventListener("click", () => {
  window.location.href = "historial_ordenes.html";
});
infoGridEl?.addEventListener("click", (event) => {
  const btn = event.target?.closest(".btn-ver-servicios-orden");
  if (!btn) return;
  goToEntregarServiciosByOrden(btn.dataset.idOrden);
});
itemsEl?.addEventListener("click", (event) => {
  const btn = event.target?.closest(".btn-ver-venta-item");
  if (!btn) return;
  goToEntregarServiciosByOrdenVenta(btn.dataset.idVenta);
});

const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const METODO_RECARGO_USD_ID = 4;
const METODO_RECARGO_USD_PERCENT = 0.0349;
const METODO_RECARGO_USD_FIJO = 0.49;
const METODO_COMISION_20_ID = 3;
const METODO_COMISION_20_PERCENT = 0.2;

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

const normalizeTextValue = (value) => {
  const text = String(value ?? "").trim();
  if (!text) return "";
  const lowered = text.toLowerCase();
  if (lowered === "null" || lowered === "undefined") return "";
  return text;
};

const buildEstadoMeta = (orden) => {
  if (isTrue(orden?.orden_cancelada)) {
    return { label: "Cancelada", toneClass: "is-cancelled" };
  }
  if (isTrue(orden?.pago_verificado)) {
    return { label: "Procesado", toneClass: "is-processed" };
  }
  if (isTrue(orden?.en_espera)) {
    return { label: "En espera", toneClass: "is-waiting" };
  }
  return { label: "Pendiente", toneClass: "is-pending" };
};

const getTotalUsdMostrado = (baseUsd, metodoId) => {
  const montoBase = Number(baseUsd);
  if (!Number.isFinite(montoBase)) return 0;
  const id = Number(metodoId);
  if (id === METODO_RECARGO_USD_ID) {
    return round2(montoBase * (1 + METODO_RECARGO_USD_PERCENT) + METODO_RECARGO_USD_FIJO);
  }
  if (id === METODO_COMISION_20_ID) {
    return round2(montoBase * (1 + METODO_COMISION_20_PERCENT));
  }
  return round2(montoBase);
};


const formatNombreApellido = (user) => {
  const nombre = String(user?.nombre || "").trim();
  const apellido = String(user?.apellido || "").trim();
  return [nombre, apellido].filter(Boolean).join(" ").trim() || "-";
};

const buildMetodoPagoHtml = (metodo) => {
  if (!metodo) return "-";
  const nombre = String(metodo?.nombre || "").trim();
  return nombre || "-";
};

const buildGiftCardPinLookups = (ventasRows = [], targetUserId = null) => {
  const pinByVentaId = new Map();
  const pinsByPlatformId = new Map();
  const targetUserNum = Number(targetUserId);

  for (const row of Array.isArray(ventasRows) ? ventasRows : []) {
    const pin = normalizeTextValue(row?.tarjetas_de_regalo?.pin);
    if (!pin) continue;

    const vendidoA = Number(row?.tarjetas_de_regalo?.vendido_a);
    if (
      Number.isFinite(targetUserNum) &&
      targetUserNum > 0 &&
      Number.isFinite(vendidoA) &&
      vendidoA > 0 &&
      vendidoA !== targetUserNum
    ) {
      continue;
    }

    const ventaId = Number(row?.id_venta);
    if (Number.isFinite(ventaId) && ventaId > 0 && !pinByVentaId.has(ventaId)) {
      pinByVentaId.set(ventaId, pin);
    }

    const platformId = Number(row?.precios?.id_plataforma);
    if (!Number.isFinite(platformId) || platformId <= 0) continue;
    const currentPins = pinsByPlatformId.get(platformId) || [];
    if (!currentPins.includes(pin)) {
      currentPins.push(pin);
      pinsByPlatformId.set(platformId, currentPins);
    }
  }

  return { pinByVentaId, pinsByPlatformId };
};

const getGiftCardPinDisplay = ({ item = null, platformId = null, giftCardLookups = null } = {}) => {
  const pinByVentaId =
    giftCardLookups?.pinByVentaId instanceof Map ? giftCardLookups.pinByVentaId : new Map();
  const pinsByPlatformId =
    giftCardLookups?.pinsByPlatformId instanceof Map
      ? giftCardLookups.pinsByPlatformId
      : new Map();

  const ventaId = Number(item?.id_venta);
  if (Number.isFinite(ventaId) && ventaId > 0) {
    const pin = normalizeTextValue(pinByVentaId.get(ventaId));
    if (pin) return pin;
  }

  const platformIdNum = Number(platformId ?? item?.id_plataforma);
  if (Number.isFinite(platformIdNum) && platformIdNum > 0) {
    const pins = (pinsByPlatformId.get(platformIdNum) || [])
      .map((value) => normalizeTextValue(value))
      .filter(Boolean);
    if (pins.length) return pins.join(", ");
  }

  return "Pendiente";
};

const renderInfo = (orden, clienteNombre = "-", metodoPago = null, { isSuperadmin = false } = {}) => {
  if (!infoGridEl) return;
  const fecha = formatDDMMYYYY(orden?.fecha) || orden?.fecha || "-";
  const hora = formatHora12(orden?.hora_orden);
  const estado = buildEstadoMeta(orden);
  const idOrdenNum = Number(orden?.id_orden);
  const canOpenServicios = Number.isFinite(idOrdenNum) && idOrdenNum > 0;
  const rows = [
    { label: "N. orden", value: orden?.id_orden ?? "-" },
    ...(isSuperadmin ? [{ label: "Cliente", value: clienteNombre || "-" }] : []),
    { label: "Fecha", value: fecha },
    { label: "Hora", value: hora },
    { label: "Total", value: formatMoney(orden?.total), breakBefore: true },
    { label: "Monto (Bs)", value: formatBs(orden?.monto_bs) },
    { label: "Método de pago", value: buildMetodoPagoHtml(metodoPago) },
    { label: "Referencia", value: orden?.referencia || "-" },
  ];
  infoGridEl.innerHTML = `
    <div class="orden-estado-chip ${estado.toneClass}">${estado.label}</div>
    ${rows
      .map(
        (row) => `
        ${row.breakBefore ? '<div class="orden-info-break" aria-hidden="true"></div>' : ""}
        <div class="label">${row.label}</div>
        <div class="value">${row.value}</div>
      `
      )
      .join("")}
    <div class="orden-servicios-row">
      ${
        canOpenServicios
          ? `<button type="button" class="btn-ver-servicios-orden" data-id-orden="${idOrdenNum}">Ver servicios</button>`
          : "-"
      }
    </div>
  `;
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
  return out.length
    ? out
    : ["descuento_1", "descuento_2", "descuento_3", "descuento_4", "descuento_5"];
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

const calcItemTotals = (
  item,
  price,
  platform,
  descuentos,
  discountColumns = [],
  discountColumnById = {},
  isCliente = true,
) => {
  const unit = Number(price?.precio_usd_detal) || 0;
  const qtyVal = Math.max(1, Number(item?.cantidad) || 1);
  const isGiftCard = isTrue(platform?.tarjeta_de_regalo);
  const mesesVal = isGiftCard
    ? 1
    : Math.max(1, Number(item?.meses) || Number(price?.duracion) || 1);
  const baseSubtotal = round2(unit * qtyVal * mesesVal);
  let descuentoMesesVal = 0;
  let descuentoCantidadVal = 0;
  let rateMeses = 0;

  const monthEnabled =
    !!platform?.descuento_meses &&
    !isGiftCard &&
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

  if (monthEnabled) {
    const rawRate = getClosestDiscountPct(descuentos, mesesVal, monthColumn);
    rateMeses = rawRate > 1 ? rawRate / 100 : rawRate;
    descuentoMesesVal = round2(baseSubtotal * rateMeses);
  }
  const rawRateQty = qtyEnabled ? getClosestDiscountPct(descuentos, qtyVal, qtyColumn) : 0;
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

const renderItems = (items, catalog, useMayor, giftCardLookups = null, { idOrden = null } = {}) => {
  if (!itemsEl) return;
  if (!Array.isArray(items) || !items.length) {
    itemsEl.innerHTML = '<p class="cart-empty">Esta orden no tiene items asociados.</p>';
    itemsTitleEl?.classList.add("hidden");
    return;
  }
  itemsTitleEl?.classList.remove("hidden");

  const descuentos = catalog?.descuentos || [];
  const discountColumns = getDiscountColumnsFromRows(descuentos);
  const discountColumnById = buildDiscountColumnByIdMap(descuentos, discountColumns);
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
      const totals = calcItemTotals(
        item,
        price,
        platform,
        descuentos,
        discountColumns,
        discountColumnById,
        !useMayor,
      );
      const isGiftCard = isTrue(platform?.tarjeta_de_regalo);
      const detalle =
        price.plan || (platform.tarjeta_de_regalo ? `Región: ${price.region || "-"}` : "");
      const tipo = item?.renovacion ? "Renovación" : "Nuevo";
      const nombre = platform.nombre || `Precio ${item.id_precio}`;
      const imagen = platform.imagen || "";
      const totalVenta = Number.isFinite(Number(item?.monto)) ? Number(item.monto) : totals.subtotal;
      const estadoVenta = isTrue(item?.pendiente) ? "Procesandose" : "Entregado";
      const idVentaNum = toPositiveId(item?.id_venta);
      const itemMeta = [
        detalle ? `<p class="orden-item-meta">${detalle}</p>` : "",
        isGiftCard
          ? `<p class="orden-item-meta">PIN: ${getGiftCardPinDisplay({
              item,
              platformId: price?.id_plataforma,
              giftCardLookups,
            })}</p>`
          : `<p class="orden-item-meta">Tipo: ${tipo}</p>`,
        `<p class="orden-item-meta">Precio: ${formatMoney(totals.unit)}</p>`,
      ]
        .filter(Boolean)
        .join("");
      return `
        <tr data-index="${idx}">
          <td>
            <div class="cart-info tight">
              <div class="cart-product">
                <div class="cart-thumb-sm">
                  <img src="${imagen}" alt="${nombre}" loading="lazy" decoding="async" />
                </div>
                <div class="cart-product-text">
                  <div class="orden-item-head">
                    <div class="orden-item-main">
                      <p class="cart-name">${nombre}</p>
                      ${itemMeta}
                    </div>
                    ${
                      idVentaNum
                        ? `<button type="button" class="btn-outline btn-small btn-ver-venta-item" data-id-venta="${idVentaNum}">Ver</button>`
                        : ""
                    }
                  </div>
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
            <span class="cart-subtotal-value">${formatMoney(totalVenta)}</span>
          </td>
          <td class="cart-cell-center">
            <span class="status-chip">${estadoVenta}</span>
          </td>
        </tr>
      `;
    })
    .join("");

  itemsEl.innerHTML = `
    <div class="cart-layout">
      <div class="orden-items-panel">
        <div class="cart-table-scroll">
          <table class="table-base cart-page-table">
            <thead>
              <tr>
                <th>Producto</th>
                <th>Meses</th>
                <th>Precio</th>
                <th>Cantidad</th>
                <th>Total</th>
                <th>Estado</th>
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

const renderOrdenesItems = (items, giftCardLookups = null, { idOrden = null } = {}) => {
  if (!itemsEl) return;
  if (!Array.isArray(items) || !items.length) {
    itemsEl.innerHTML = '<p class="cart-empty">Esta orden no tiene items asociados.</p>';
    itemsTitleEl?.classList.add("hidden");
    return;
  }
  itemsTitleEl?.classList.remove("hidden");

  const rows = items
    .map((item, idx) => {
      const platformName = item?.plataformas?.nombre || `Plataforma ${item?.id_plataforma || "-"}`;
      const image = String(item?.plataformas?.imagen || "").trim();
      const isGiftCard = isTrue(item?.plataformas?.tarjeta_de_regalo);
      const tipo = isTrue(item?.renovacion) ? "Renovación" : "Nuevo";
      const idVentaNum = toPositiveId(item?.id_venta);
      const itemMeta = isGiftCard
        ? `<p class="orden-item-meta">PIN: ${getGiftCardPinDisplay({
            item,
            platformId: item?.id_plataforma,
            giftCardLookups,
          })}</p>`
        : `<p class="orden-item-meta">Tipo: ${tipo}</p>`;
      return `
        <tr data-index="${idx}">
          <td>
            <div class="cart-info tight">
              <div class="cart-product">
                <div class="cart-thumb-sm">
                  ${
                    image
                      ? `<img src="${image}" alt="${platformName}" loading="lazy" decoding="async" />`
                      : ""
                  }
                </div>
                <div class="cart-product-text">
                  <div class="orden-item-head">
                    <div class="orden-item-main">
                      <p class="cart-name">${platformName}</p>
                      ${itemMeta}
                    </div>
                    ${
                      idVentaNum
                        ? `<button type="button" class="btn-outline btn-small btn-ver-venta-item" data-id-venta="${idVentaNum}">Ver</button>`
                        : ""
                    }
                  </div>
                </div>
              </div>
            </div>
          </td>
          <td class="cart-cell-center">${formatMoney(item?.monto_usd)}</td>
          <td class="cart-cell-center">${formatBs(item?.monto_bs)}</td>
        </tr>
      `;
    })
    .join("");

  itemsEl.innerHTML = `
    <div class="cart-layout">
      <div class="orden-items-panel">
        <div class="cart-table-scroll">
          <table class="table-base cart-page-table">
            <thead>
              <tr>
                <th>Producto</th>
                <th>Monto USD</th>
                <th>Monto Bs</th>
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

const normalizeVentasOrdenRows = (rows = []) => {
  const normalized = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const idPrecio = Number(row?.id_precio || row?.precios?.id_precio);
    if (!Number.isFinite(idPrecio) || idPrecio <= 0) continue;
    const meses = Math.max(1, Number(row?.meses_contratados) || 1);
    normalized.push({
      id_precio: idPrecio,
      cantidad: 1,
      meses,
      renovacion: isTrue(row?.renovacion),
      id_venta: row?.id_venta ?? null,
      pendiente: isTrue(row?.pendiente),
      monto: null,
      id_plataforma: row?.precios?.id_plataforma ?? null,
    });
  }
  return normalized;
};

const getIdOrden = () => {
  const params = new URLSearchParams(window.location.search);
  return params.get("id_orden");
};

const fetchItemsByVentasOrden = async (idOrden, prefetchedVentas = null) => {
  if (!idOrden) return [];
  if (Array.isArray(prefetchedVentas)) {
    return normalizeVentasOrdenRows(prefetchedVentas);
  }
  const resp = await fetchVentasOrden(idOrden);
  if (resp?.error) {
    throw new Error(resp.error);
  }
  return normalizeVentasOrdenRows(resp?.ventas || []);
};

const fetchItemsByHistorialOrden = async (idOrden, idUsuario) => {
  if (!idOrden || !idUsuario) return [];
  const { data: historialRows, error: histErr } = await supabase
    .from("historial_ventas")
    .select("id_historial_ventas, id_venta, id_orden, id_usuario_cliente, renovacion, monto")
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
    .select("id_venta, id_precio, meses_contratados, pendiente")
    .in("id_venta", ventaIds);
  if (ventasErr) throw ventasErr;
  const ventaMap = (ventasRows || []).reduce((acc, row) => {
    acc[row.id_venta] = row;
    return acc;
  }, {});

  const normalized = [];
  for (const row of rows) {
    const venta = ventaMap[row.id_venta];
    const idPrecio = Number(venta?.id_precio);
    if (!Number.isFinite(idPrecio) || idPrecio <= 0) continue;
    const meses = Math.max(1, Number(venta?.meses_contratados) || 1);
    const renovacion = isTrue(row?.renovacion);
    normalized.push({
      id_precio: idPrecio,
      cantidad: 1,
      meses,
      renovacion,
      id_venta: row?.id_venta ?? null,
      pendiente: isTrue(venta?.pendiente),
      monto: Number(row?.monto),
    });
  }
  return normalized;
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
    const usuarioOrden = detalleResp?.usuario || null;
    const metodoPago = detalleResp?.metodo_pago || null;
    const nombreUsuarioOrden = formatNombreApellido(usuarioOrden);
    let clienteNombre = nombreUsuarioOrden;
    if (clienteNombre === "-" && Number(orden?.id_usuario) === Number(currentUser?.id_usuario)) {
      clienteNombre = formatNombreApellido(currentUser);
    }
    renderInfo(orden, clienteNombre, metodoPago, {
      isSuperadmin: isTrue(currentUser?.permiso_superadmin),
    });

    let ventasOrdenRows = [];
    try {
      const ventasResp = await fetchVentasOrden(idOrden);
      if (ventasResp?.error) {
        console.error("fetchVentasOrden prefetch error", ventasResp.error);
      } else {
        ventasOrdenRows = Array.isArray(ventasResp?.ventas) ? ventasResp.ventas : [];
      }
    } catch (err) {
      console.error("fetchVentasOrden prefetch error", err);
    }
    const giftCardLookups = buildGiftCardPinLookups(ventasOrdenRows, orden?.id_usuario);

    setStatus("Cargando items...");
    const snapshotItems = Array.isArray(detalleResp?.items) ? detalleResp.items : [];
    const itemsSource = String(detalleResp?.items_source || "").trim();
    if (itemsSource === "ordenes_items" && snapshotItems.length) {
      renderOrdenesItems(snapshotItems, giftCardLookups, { idOrden });
      setStatus("");
      return;
    }

    let items = [];
    try {
      items = await fetchItemsByHistorialOrden(idOrden, userId);
    } catch (err) {
      console.error("fetchItemsByHistorialOrden error", err);
    }
    if (!items.length) {
      try {
        items = await fetchItemsByVentasOrden(idOrden, ventasOrdenRows);
      } catch (err) {
        console.error("fetchItemsByVentasOrden error", err);
      }
    }
    if (!items.length) {
      items = snapshotItems;
    }
    if (!items.length) {
      setStatus("");
      renderItems([], null, false, null, { idOrden });
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
    renderItems(items, catalog, useMayor, giftCardLookups, { idOrden });
    setStatus("");
  } catch (err) {
    console.error("detalle ordenes error", err);
    setStatus("No se pudo cargar el detalle de la orden.");
  }
};

init();
