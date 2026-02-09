const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const { supabaseAdmin } = require("../database/db");
const { port } = require("../../config/config");

const app = express();
app.use(
  cors({
    origin: true,
    credentials: true,
  })
);
const jsonParser = express.json({ limit: "25mb" });

const clearCorsHeaders = (res) => {
  res.removeHeader("Access-Control-Allow-Origin");
  res.removeHeader("Access-Control-Allow-Credentials");
  res.removeHeader("Access-Control-Allow-Headers");
  res.removeHeader("Access-Control-Allow-Methods");
};

app.post("/api/bdv/notify", express.text({ type: "*/*", limit: "200kb" }), async (req, res) => {
  clearCorsHeaders(res);
  try {
    const auth = req.headers.authorization || "";
    const token = process.env.BDV_WEBHOOK_TOKEN || "";
    const expected = `Bearer ${token}`;
    if (!token || auth !== expected) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const rawText = typeof req.body === "string" ? req.body : "";
    if (!rawText || !rawText.trim()) {
      return res.status(400).json({ error: "Invalid payload" });
    }

    const appName = "BDV";
    const titulo = "BDV";
    const texto = rawText;
    const fecha = new Date().toISOString();
    const dispositivo = "unknown";

    const hash = crypto
      .createHash("sha256")
      .update([appName, titulo, texto, fecha, dispositivo].join("|"))
      .digest("hex");

    const { data: exists, error: existsErr } = await supabaseAdmin
      .from("pagomoviles")
      .select("hash")
      .eq("hash", hash)
      .maybeSingle();
    if (existsErr) throw existsErr;
    if (exists?.hash) {
      return res.json({ ok: true, duplicado: true });
    }

    const montoMatch =
      texto.match(/Bs\.?\s*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]+)?)/i)?.[1] ||
      texto.match(/Bs\.?\s*([0-9]+(?:[.,][0-9]+)?)/i)?.[1] ||
      texto.match(/Bs\.?\s*(0[.,][0-9]+)/i)?.[1] ||
      null;
    const normalizeMonto = (val) => {
      if (!val) return null;
      const raw = String(val).trim();
      if (raw.includes(".") && raw.includes(",")) {
        return raw.replace(/\./g, "").replace(",", ".");
      }
      return raw.replace(",", ".");
    };
    const monto = normalizeMonto(montoMatch);

    const { error: insErr } = await supabaseAdmin.from("pagomoviles").insert({
      app: appName,
      titulo,
      texto,
      fecha,
      dispositivo,
      hash,
      monto_bs: monto,
    });
    if (insErr) throw insErr;

    return res.json({ ok: true, duplicado: false });
  } catch (err) {
    console.error("bdv notify error", err);
    return res.status(500).json({ error: "Internal error" });
  }
});

app.use(jsonParser);

const AUTH_REQUIRED = "AUTH_REQUIRED";
const BINANCE_P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search";

const todayInVenezuela = () => {
  // Retorna fecha actual en huso horario de Venezuela (America/Caracas) en formato YYYY-MM-DD
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(new Date());
};

// Suma meses manteniendo el día (sin desfase de zona horaria); si el mes destino no tiene ese día, usa el último día del mes.
function addMonthsKeepDay(baseDate, months) {
  const baseStr =
    typeof baseDate === "string"
      ? baseDate
      : new Date(baseDate).toISOString().slice(0, 10);
  const [y, m, d] = baseStr.split("-").map(Number);
  let mm = (m - 1) + months;
  let yy = y + Math.floor(mm / 12);
  mm = mm % 12;
  if (mm < 0) {
    mm += 12;
    yy -= 1;
  }
  const daysInTarget = new Date(Date.UTC(yy, mm + 1, 0)).getUTCDate();
  const day = Math.min(d, daysInTarget);
  const result = new Date(Date.UTC(yy, mm, day));
  return result.toISOString().slice(0, 10);
}

const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const isInactive = (v) => isTrue(v);

const normalizeFilesArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return [];
    if (trimmed.startsWith("[")) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) return parsed;
      } catch (err) {
        // ignore JSON parse errors, fallback to plain string
      }
    }
    return [trimmed];
  }
  return [];
};

const getPrecioPicker = async (idUsuarioVentas) => {
  const { data: usuarioVenta, error: userVentaErr } = await supabaseAdmin
    .from("usuarios")
    .select("acceso_cliente")
    .eq("id_usuario", idUsuarioVentas)
    .single();
  if (userVentaErr && userVentaErr.code !== "PGRST116") throw userVentaErr;
  const accesoCliente = usuarioVenta?.acceso_cliente;
  const esMayorista =
    accesoCliente === false || accesoCliente === "false" || accesoCliente === 0 || accesoCliente === "0";
  const pickPrecio = (price) => {
    const detal = Number(price?.precio_usd_detal) || 0;
    const mayor = Number(price?.precio_usd_mayor) || 0;
    return esMayorista ? mayor || detal : detal || mayor;
  };
  return { esMayorista, pickPrecio };
};

const buildCheckoutContext = async ({ idUsuarioVentas, carritoId, totalCliente, tasa_bs }) => {
  const { pickPrecio } = await getPrecioPicker(idUsuarioVentas);
  const { data: items, error: itemErr } = await supabaseAdmin
    .from("carrito_items")
    .select("id_precio, cantidad, meses, renovacion, id_venta")
    .eq("id_carrito", carritoId);
  if (itemErr) throw itemErr;

  if (!items?.length) {
    const total = Number.isFinite(Number(totalCliente)) ? Number(totalCliente) : 0;
    const tasaBs = Number.isFinite(Number(tasa_bs)) ? Number(tasa_bs) : 400;
    return { items: [], priceMap: {}, platInfoById: {}, platNameById: {}, pickPrecio, total, tasaBs };
  }

  const preciosIds = (items || []).map((i) => i.id_precio).filter(Boolean);
  const { data: precios, error: precioErr } = await supabaseAdmin
    .from("precios")
    .select("id_precio, precio_usd_detal, precio_usd_mayor, id_plataforma, completa, sub_cuenta")
    .in("id_precio", preciosIds);
  if (precioErr) throw precioErr;
  const priceMap = (precios || []).reduce((acc, p) => {
    acc[p.id_precio] = p;
    return acc;
  }, {});

  const plataformaIds = [...new Set((precios || []).map((p) => p.id_plataforma).filter(Boolean))];
  const { data: plataformas, error: platErr } = await supabaseAdmin
    .from("plataformas")
    .select("id_plataforma, nombre, entrega_inmediata, cuenta_madre")
    .in("id_plataforma", plataformaIds);
  if (platErr) throw platErr;
  const platInfoById = (plataformas || []).reduce((acc, p) => {
    acc[p.id_plataforma] = p;
    return acc;
  }, {});
  const platNameById = (plataformas || []).reduce((acc, p) => {
    acc[p.id_plataforma] = p.nombre || `Plataforma ${p.id_plataforma}`;
    return acc;
  }, {});

  const totalCalc = (items || []).reduce((sum, it) => {
    const unit = pickPrecio(priceMap[it.id_precio]);
    const mesesVal = it.meses || 1;
    return sum + unit * (it.cantidad || 0) * mesesVal;
  }, 0);
  const total = Number.isFinite(Number(totalCliente)) ? Number(totalCliente) : totalCalc;
  const tasaBs = Number.isFinite(Number(tasa_bs)) ? Number(tasa_bs) : 400;

  return { items: items || [], priceMap, platInfoById, platNameById, pickPrecio, total, tasaBs };
};

const processOrderFromItems = async ({
  ordenId,
  idUsuarioSesion,
  idUsuarioVentas,
  items,
  priceMap,
  platInfoById,
  platNameById,
  pickPrecio,
  referencia,
  archivos,
  id_metodo_de_pago,
  carritoId,
}) => {
  const isoHoy = todayInVenezuela();
  const referenciaNum = Number.isFinite(Number(referencia)) ? Number(referencia) : null;
  const archivosArr = Array.isArray(archivos) ? archivos : [];
  const comprobanteHist = archivosArr?.[0] || null;

  // Renovaciones (no asignan stock nuevo)
  const renovaciones = (items || []).filter((it) => it.renovacion === true && it.id_venta);
  const idsVentasRenovar = renovaciones.map((r) => r.id_venta).filter(Boolean);
  const ventaMap = {};
  if (idsVentasRenovar.length) {
    const { data: ventasExistentes, error: ventErr } = await supabaseAdmin
      .from("ventas")
      .select("id_venta, fecha_corte, id_cuenta, id_usuario")
      .in("id_venta", idsVentasRenovar);
    if (ventErr) throw ventErr;
    (ventasExistentes || []).forEach((v) => {
      ventaMap[v.id_venta] = v;
    });
  }

  const renovPromises = renovaciones.map((it) => {
    const price = priceMap[it.id_precio] || {};
    const mesesVal = Number.isFinite(Number(it.meses)) && Number(it.meses) > 0 ? Math.round(Number(it.meses)) : 1;
    const cantidadVal = Number.isFinite(Number(it.cantidad)) && Number(it.cantidad) > 0 ? Number(it.cantidad) : 0;
    const base = pickPrecio(price) * cantidadVal * mesesVal;
    const monto = Number(base.toFixed(2));
    const fechaBaseSrc = ventaMap[it.id_venta]?.fecha_corte || isoHoy;
    const fecha_corte = addMonthsKeepDay(fechaBaseSrc, mesesVal) || isoHoy;
    return supabaseAdmin
      .from("ventas")
      .update({
        fecha_pago: isoHoy,
        fecha_corte,
        monto,
        id_orden: ordenId,
        renovacion: true,
      })
      .eq("id_venta", it.id_venta);
  });
  if (renovPromises.length) {
    const renovRes = await Promise.all(renovPromises);
    const renovErr = renovRes.find((r) => r?.error);
    if (renovErr?.error) throw renovErr.error;
  }

  // Filtra items nuevos (no renovaciones) para asignación de stock
  const itemsNuevos = (items || []).filter((it) => !it.id_venta);

  // Verificación de stock y asignación de recursos
  const asignaciones = [];
  const pendientes = [];
  const subCuentasAsignadas = [];
  for (const it of itemsNuevos) {
    const price = priceMap[it.id_precio];
    if (!price) {
      throw new Error(`Precio no encontrado para item ${it.id_precio}`);
    }
    const cantidad = it.cantidad || 0;
    if (cantidad <= 0) continue;
    const platId = Number(price.id_plataforma) || null;
    const entregaInmediata = isTrue(platInfoById[platId]?.entrega_inmediata);
    const cuentaMadrePlat = isTrue(platInfoById[platId]?.cuenta_madre);
    const pendienteVenta = !entregaInmediata || cuentaMadrePlat;
    const mesesItemRaw = it.meses || 1;
    const mesesItem = Number.isFinite(Number(mesesItemRaw))
      ? Math.max(1, Math.round(Number(mesesItemRaw)))
      : 1;
    console.log("[checkout] item", it, "mesesRaw", mesesItemRaw, "meses", mesesItem);

    const priceId = Number(price.id_precio) || Number(it.id_precio) || null;
    const isNetflixPlan2 = platId === 1 && [4, 5].includes(priceId);
    console.log("[checkout] asignacion start", {
      id_precio: price?.id_precio,
      id_plataforma: platId,
      completa: price?.completa,
      isNetflixPlan2,
      cantidad,
    });

    if (price.completa) {
      const { data: cuentasLibres, error: ctaErr } = await supabaseAdmin
        .from("cuentas")
        .select("id_cuenta, id_plataforma, ocupado, inactiva")
        .eq("id_plataforma", platId)
        .eq("venta_perfil", false)
        .eq("venta_miembro", false)
        .eq("ocupado", false)
        .or("inactiva.is.null,inactiva.eq.false")
        .limit(cantidad);
      if (ctaErr) throw ctaErr;
      const disponibles = (cuentasLibres || []).filter((c) => !isInactive(c.inactiva));
      const faltantes = Math.max(0, cantidad - disponibles.length);
      disponibles.slice(0, cantidad).forEach((cta) => {
        asignaciones.push({
          id_precio: price.id_precio,
          monto: pickPrecio(price),
          id_cuenta: cta.id_cuenta,
          id_perfil: null,
          id_sub_cuenta: null,
          meses: mesesItem,
          pendiente: pendienteVenta,
        });
      });
      if (faltantes > 0) {
        for (let i = 0; i < faltantes; i += 1) {
          pendientes.push({
            id_precio: price.id_precio,
            monto: pickPrecio(price),
            id_cuenta: null,
            id_perfil: null,
            id_sub_cuenta: null,
            meses: mesesItem,
            pendiente: true,
          });
        }
      }
    } else if (isNetflixPlan2) {
      const usedPerfiles = [];
      const { data: perfilesHogar, error: perfErr } = await supabaseAdmin
        .from("perfiles")
        .select(
          "id_perfil, id_cuenta, ocupado, perfil_hogar, cuentas!perfiles_id_cuenta_fkey!inner(id_plataforma, inactiva, venta_perfil)",
        )
        .eq("perfil_hogar", true)
        .eq("cuentas.id_plataforma", platId)
        .eq("cuentas.venta_perfil", true)
        .eq("ocupado", false)
        .or("inactiva.is.null,inactiva.eq.false", { foreignTable: "cuentas" })
        .limit(cantidad);
      if (perfErr) throw perfErr;
      const libresHogar = (perfilesHogar || []).filter(
        (p) => !isInactive(p?.cuentas?.inactiva) && p.ocupado === false
      );
      const takeHogar = libresHogar.slice(0, cantidad);
      takeHogar.forEach((p) => {
        asignaciones.push({
          id_precio: price.id_precio,
          monto: pickPrecio(price),
          id_cuenta: p.id_cuenta,
          id_perfil: p.id_perfil,
          id_sub_cuenta: null,
          meses: mesesItem,
          pendiente: pendienteVenta,
        });
        usedPerfiles.push(p.id_perfil);
      });

      const faltantesPerf = Math.max(0, cantidad - usedPerfiles.length);
      if (faltantesPerf > 0) {
        const { data: cuentasMiembro, error: ctaMiembroErr } = await supabaseAdmin
          .from("cuentas")
          .select("id_cuenta, ocupado, inactiva, venta_miembro, venta_perfil")
          .eq("id_plataforma", platId)
          .eq("venta_perfil", false)
          .eq("venta_miembro", true)
          .eq("ocupado", false)
          .or("inactiva.is.null,inactiva.eq.false")
          .limit(faltantesPerf);
        if (ctaMiembroErr) throw ctaMiembroErr;
        const cuentasLibres = (cuentasMiembro || []).filter(
          (c) => c.inactiva === false && c.ocupado === false
        );
        const takeCtas = cuentasLibres.slice(0, faltantesPerf);
        takeCtas.forEach((cta) => {
          asignaciones.push({
            id_precio: price.id_precio,
            monto: pickPrecio(price),
            id_cuenta: cta.id_cuenta,
            id_perfil: null,
            id_sub_cuenta: null,
            meses: mesesItem,
            pendiente: pendienteVenta,
          });
        });
        const faltantesPerf2 = Math.max(0, faltantesPerf - takeCtas.length);
        if (faltantesPerf2 > 0) {
          for (let i = 0; i < faltantesPerf2; i += 1) {
            pendientes.push({
              id_precio: price.id_precio,
              monto: pickPrecio(price),
              id_cuenta: null,
              id_perfil: null,
              id_sub_cuenta: null,
              meses: mesesItem,
              pendiente: true,
            });
          }
        }
      }
    } else {
      const isSpotify = platId === 9;
      let perfilesQuery = supabaseAdmin
        .from("perfiles")
        .select(
          "id_perfil, id_cuenta, perfil_hogar, cuentas!perfiles_id_cuenta_fkey!inner(id_plataforma, inactiva, venta_perfil, cuenta_madre)"
        )
        .eq("cuentas.id_plataforma", platId)
        .eq("cuentas.venta_perfil", isSpotify ? false : true)
        .eq("perfil_hogar", false)
        .eq("ocupado", false)
        .or("inactiva.is.null,inactiva.eq.false", { foreignTable: "cuentas" })
        .limit(cantidad);
      perfilesQuery = isSpotify
        ? perfilesQuery.eq("cuentas.cuenta_madre", true)
        : perfilesQuery.or("cuenta_madre.is.null,cuenta_madre.eq.false", { foreignTable: "cuentas" });
      const { data: perfilesLibres, error: perfErr } = await perfilesQuery;
      if (perfErr) throw perfErr;
      console.log("[checkout] perfiles libres raw", {
        platId,
        count: perfilesLibres?.length || 0,
        first: perfilesLibres?.[0] || null,
      });
      if (platId === 1 || platId === 9) {
        console.log("[checkout][netflix] filtros", {
          platId,
          venta_perfil: isSpotify ? false : true,
          cuenta_madre: isSpotify ? true : false,
          perfil_hogar: false,
          ocupado: false,
          inactiva: "null|false",
        });
        const rawSample = (perfilesLibres || []).slice(0, 5).map((p) => ({
          id_perfil: p.id_perfil,
          id_cuenta: p.id_cuenta,
          perfil_hogar: p.perfil_hogar,
          ocupado: p.ocupado,
          cuenta_plat: p.cuentas?.id_plataforma,
          cuenta_inactiva: p.cuentas?.inactiva,
          cuenta_venta_perfil: p.cuentas?.venta_perfil,
          cuenta_madre: p.cuentas?.cuenta_madre,
        }));
        console.log("[checkout][stock] raw sample", rawSample);
      }
      const disponibles = (perfilesLibres || []).filter((p) => !isInactive(p?.cuentas?.inactiva));
      console.log("[checkout] perfiles libres disponibles", {
        platId,
        count: disponibles.length,
        first: disponibles[0] || null,
      });
      if (platId === 1 || platId === 9) {
        const dispSample = disponibles.slice(0, 5).map((p) => ({
          id_perfil: p.id_perfil,
          id_cuenta: p.id_cuenta,
          perfil_hogar: p.perfil_hogar,
          ocupado: p.ocupado,
          cuenta_inactiva: p.cuentas?.inactiva,
        }));
        console.log("[checkout][stock] disponibles sample", dispSample);
      }
      const faltantes = Math.max(0, cantidad - disponibles.length);
      disponibles.slice(0, cantidad).forEach((p) => {
        asignaciones.push({
          id_precio: price.id_precio,
          monto: pickPrecio(price),
          id_cuenta: p.id_cuenta,
          id_perfil: p.id_perfil,
          id_sub_cuenta: null,
          meses: mesesItem,
          pendiente: pendienteVenta,
        });
      });
      if (faltantes > 0) {
        for (let i = 0; i < faltantes; i += 1) {
          pendientes.push({
            id_precio: price.id_precio,
            monto: pickPrecio(price),
            id_cuenta: null,
            id_perfil: null,
            id_sub_cuenta: null,
            meses: mesesItem,
            pendiente: true,
          });
        }
      }
    }
    console.log("[checkout] asignacion end", {
      id_precio: price?.id_precio,
      platId,
      asignaciones: asignaciones.map((a) => ({
        id_precio: a.id_precio,
        id_cuenta: a.id_cuenta,
        id_perfil: a.id_perfil,
      })),
    });
  }

  // Validación final: jamás usar cuentas inactivas
  const assignedCuentaIds = Array.from(new Set(asignaciones.map((a) => a.id_cuenta).filter(Boolean)));
  const assignedPerfilIds = Array.from(new Set(asignaciones.map((a) => a.id_perfil).filter(Boolean)));
  if (assignedCuentaIds.length) {
    const { data: cuentasAsignadas, error: ctaValErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, id_plataforma, inactiva")
      .in("id_cuenta", assignedCuentaIds);
    if (ctaValErr) throw ctaValErr;
    const bad = (cuentasAsignadas || []).find((c) => isInactive(c.inactiva));
    if (bad) {
      throw new Error("Se intentó asignar una cuenta inactiva.");
    }
    const cuentaPlatMap = (cuentasAsignadas || []).reduce((acc, c) => {
      acc[c.id_cuenta] = c.id_plataforma;
      return acc;
    }, {});
    const badPlat = (asignaciones || []).find((a) => {
      const platId = priceMap[a.id_precio]?.id_plataforma || null;
      const cuentaPlat = cuentaPlatMap[a.id_cuenta];
      return platId && cuentaPlat && Number(cuentaPlat) !== Number(platId);
    });
    if (badPlat) {
      throw new Error("Asignación con plataforma incorrecta.");
    }
  }
  if (assignedPerfilIds.length) {
    const { data: perfilesAsignados, error: perfValErr } = await supabaseAdmin
      .from("perfiles")
      .select("id_perfil, cuentas:cuentas!perfiles_id_cuenta_fkey(inactiva)")
      .in("id_perfil", assignedPerfilIds);
    if (perfValErr) throw perfValErr;
    const bad = (perfilesAsignados || []).find((p) => isInactive(p?.cuentas?.inactiva));
    if (bad) {
      throw new Error("Se intentó asignar una cuenta inactiva.");
    }
  }

  const ventasToInsert = [...asignaciones, ...pendientes].map((a) => {
    const mesesValRaw = a.meses || 1;
    const mesesVal =
      Number.isFinite(Number(mesesValRaw)) && Number(mesesValRaw) > 0
        ? Math.max(1, Math.round(Number(mesesValRaw)))
        : 1;
    const fechaCorte = a.pendiente ? null : addMonthsKeepDay(isoHoy, mesesVal);
    return {
      id_usuario: idUsuarioVentas,
      id_precio: a.id_precio,
      id_cuenta: a.id_cuenta,
      id_perfil: a.id_perfil,
      // id_sub_cuenta no existe en la tabla ventas; si se requiere, agregar columna en DB
      id_orden: ordenId,
      monto: Number(a.monto) || 0,
      pendiente: !!a.pendiente,
      meses_contratados: mesesVal,
      fecha_corte: fechaCorte,
      fecha_pago: isoHoy,
      renovacion: false,
    };
  });
  console.log("[checkout] asignaciones", asignaciones);
  console.log("[checkout] pendientes", pendientes);
  console.log("[checkout] ventasToInsert", ventasToInsert);

  let insertedVentas = [];
  if (ventasToInsert.length) {
    const { data: ventasRes, error: ventaErr } = await supabaseAdmin
      .from("ventas")
      .insert(ventasToInsert)
      .select("id_venta, id_cuenta, id_precio");
    if (ventaErr) throw ventaErr;
    insertedVentas = ventasRes || [];
  }

  // Historial de ventas (nuevas + renovaciones) con monto como float completo
  const caracasNowPago = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
  const pad2Pago = (val) => String(val).padStart(2, "0");
  const horaPago = `${pad2Pago(caracasNowPago.getHours())}:${pad2Pago(
    caracasNowPago.getMinutes()
  )}:${pad2Pago(caracasNowPago.getSeconds())}`;
  const histRows = [];
  // Nuevas (insertadas recién)
  insertedVentas.forEach((v, idx) => {
    const src = ventasToInsert[idx] || {};
    const platId = priceMap[v.id_precio]?.id_plataforma || null;
    histRows.push({
      id_usuario_cliente: idUsuarioVentas,
      id_proveedor: null,
      monto: Number(src.monto) || 0,
      fecha_pago: isoHoy,
      venta_cliente: true,
      renovacion: false,
      id_venta: v.id_venta,
      id_plataforma: platId,
      id_cuenta: v.id_cuenta,
      registrado_por: idUsuarioSesion,
      id_metodo_de_pago,
      referencia: referenciaNum,
      comprobante: comprobanteHist,
      hora_pago: horaPago,
    });
  });
  // Renovaciones
  renovaciones.forEach((it) => {
    const platId = priceMap[it.id_precio]?.id_plataforma || null;
    const ventaAnt = ventaMap[it.id_venta] || {};
    const cuentaAnt = ventaAnt.id_cuenta || null;
    const usuarioAnt = ventaAnt.id_usuario || idUsuarioVentas;
    // monto ya calculado arriba como base (unit * cantidad * meses)
    const price = priceMap[it.id_precio] || {};
    const mesesVal = Number.isFinite(Number(it.meses)) && Number(it.meses) > 0 ? Math.round(Number(it.meses)) : 1;
    const cantidadVal = Number.isFinite(Number(it.cantidad)) && Number(it.cantidad) > 0 ? Number(it.cantidad) : 0;
    const base = pickPrecio(price) * cantidadVal * mesesVal;
    histRows.push({
      id_usuario_cliente: usuarioAnt,
      id_proveedor: null,
      monto: Number(base) || 0,
      fecha_pago: isoHoy,
      venta_cliente: true,
      renovacion: true,
      id_venta: it.id_venta,
      id_plataforma: platId,
      id_cuenta: cuentaAnt,
      registrado_por: idUsuarioSesion,
      id_metodo_de_pago,
      referencia: referenciaNum,
      comprobante: comprobanteHist,
      hora_pago: horaPago,
    });
  });
  if (histRows.length) {
    const { error: histErr } = await supabaseAdmin.from("historial_ventas").insert(histRows);
    if (histErr) throw histErr;
  }

  // marca recursos como ocupados
  const perfilesIds = asignaciones.map((a) => a.id_perfil).filter(Boolean);
  const cuentasIds = asignaciones
    .filter((a) => a.id_perfil === null && a.id_cuenta)
    .map((a) => a.id_cuenta);
  if (perfilesIds.length) {
    const { error: updPerfErr } = await supabaseAdmin
      .from("perfiles")
      .update({ ocupado: true })
      .in("id_perfil", perfilesIds);
    if (updPerfErr) throw updPerfErr;
  }
  if (cuentasIds.length) {
    const { error: updCtaErr } = await supabaseAdmin
      .from("cuentas")
      .update({ ocupado: true })
      .in("id_cuenta", cuentasIds);
    if (updCtaErr) throw updCtaErr;
  }

  // limpia carrito (desvincula orden para evitar FK)
  await supabaseAdmin.from("ordenes").update({ id_carrito: null }).eq("id_orden", ordenId);
  await supabaseAdmin.from("carrito_items").delete().eq("id_carrito", carritoId);
  await supabaseAdmin.from("carritos").delete().eq("id_carrito", carritoId);

  return { ventasCount: ventasToInsert.length, pendientesCount: pendientes.length };
};

// Usa el id de usuario autenticado que envíe el cliente.
const parseSessionUserId = (req) => {
  const raw = req?.headers?.cookie || "";
  const parts = raw.split(";").map((c) => c.trim().split("="));
  const cookieMap = parts.reduce((acc, [k, v]) => {
    if (k) acc[k] = decodeURIComponent(v || "");
    return acc;
  }, {});
  const id = Number(cookieMap.session_user_id);
  return !Number.isNaN(id) && id > 0 ? id : null;
};

const getOrCreateUsuario = async (req) => {
  const fromSession = parseSessionUserId(req);
  if (fromSession) return fromSession;
  const incomingId = Number(req?.body?.id_usuario || req?.query?.id_usuario);
  if (!Number.isNaN(incomingId) && incomingId > 0) {
    return incomingId;
  }
  const err = new Error(AUTH_REQUIRED);
  err.code = AUTH_REQUIRED;
  throw err;
};

const getCurrentCarrito = async (idUsuario) => {
  const { data, error } = await supabaseAdmin
    .from("carritos")
    .select("id_carrito")
    .eq("id_usuario", idUsuario)
    .order("fecha_creacion", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data?.id_carrito || null;
};

const getOrCreateCarrito = async (idUsuario) => {
  const { data, error } = await supabaseAdmin
    .from("carritos")
    .select("id_carrito")
    .eq("id_usuario", idUsuario)
    .order("fecha_creacion", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  if (data) return data.id_carrito;

  const { data: inserted, error: insertErr } = await supabaseAdmin
    .from("carritos")
    .insert({ id_usuario: idUsuario, fecha_creacion: new Date().toISOString() })
    .select("id_carrito")
    .single();
  if (insertErr) throw insertErr;
  return inserted.id_carrito;
};

const BINANCE_CACHE_MS = 2 * 60 * 1000;
let cachedP2PRate = { value: null, ts: 0 };

const fetchP2PRate = async (asset = "USDT", fiat = "VES") => {
  const now = Date.now();
  if (cachedP2PRate.value && now - cachedP2PRate.ts < BINANCE_CACHE_MS) {
    return cachedP2PRate.value;
  }

  const body = {
    page: 1,
    rows: 10,
    payTypes: [],
    asset,
    fiat,
    tradeType: "BUY",
    publisherType: null,
  };

  const resp = await fetch(BINANCE_P2P_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    throw new Error(`Binance P2P error ${resp.status}`);
  }
  const json = await resp.json();
  const precios = (json?.data || [])
    .map((item) => Number(item?.adv?.price))
    .filter((n) => Number.isFinite(n));
  if (!precios.length) {
    throw new Error("Binance P2P sin precios");
  }
  const top = precios.slice(0, 5);
  const rate =
    top.reduce((acc, val) => acc + val, 0) / top.length;
  cachedP2PRate = { value: rate, ts: now };
  return rate;
};

// Tasa Binance P2P USDT/VES (promedio top ofertas BUY)
app.get("/api/p2p/rate", async (_req, res) => {
  try {
    const rate = await fetchP2PRate();
    res.json({ rate });
  } catch (err) {
    console.error("[p2p rate] error", err);
    res.status(502).json({ error: "No se pudo obtener la tasa P2P" });
  }
});

// Endpoint para agregar/actualizar/eliminar items del carrito.
// Se maneja por delta: cantidad positiva suma, negativa resta; si el resultado es <=0 se elimina el item.
app.post("/api/cart/item", async (req, res) => {
  console.log("[cart:item] body", req.body);
  const {
    id_precio,
    delta,
    meses,
    renovacion = false,
    id_venta = null,
    id_cuenta = null,
    id_perfil = null,
  } = req.body || {};
  if (!id_precio || delta === undefined) {
    return res
      .status(400)
      .json({ error: "id_precio y delta son requeridos" });
  }

  const parsedDelta = Number(delta);
  if (Number.isNaN(parsedDelta)) {
    return res.status(400).json({ error: "delta debe ser numérico" });
  }

  try {
    const bodyUserId = req.body?.id_usuario || null;
    const idUsuario = (await getOrCreateUsuario(req)) || bodyUserId;
    if (!idUsuario) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    const idCarrito = await getOrCreateCarrito(idUsuario);
    const bodyIdItem = req.body?.id_item ? Number(req.body.id_item) : null;
    const mesesVal = (() => {
      const num = Number(meses);
      if (Number.isFinite(num) && num > 0) return Math.max(1, Math.round(num));
      return null;
    })();

    // Trae item existente (prioriza id_item si viene)
    let existing = null;
    if (bodyIdItem) {
      const { data, error } = await supabaseAdmin
        .from("carrito_items")
        .select("id_item, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
        .eq("id_carrito", idCarrito)
        .eq("id_item", bodyIdItem)
        .maybeSingle();
      if (error) throw error;
      existing = data || null;
    }
    if (!existing) {
      let selQuery = supabaseAdmin
        .from("carrito_items")
        .select("id_item, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
        .eq("id_carrito", idCarrito)
        .eq("id_precio", id_precio)
        .eq("renovacion", renovacion === true);
      selQuery =
        id_venta === undefined || id_venta === null
          ? selQuery.is("id_venta", null)
          : selQuery.eq("id_venta", id_venta);
      selQuery =
        id_cuenta === undefined || id_cuenta === null
          ? selQuery.is("id_cuenta", null)
          : selQuery.eq("id_cuenta", id_cuenta);
      selQuery =
        id_perfil === undefined || id_perfil === null
          ? selQuery.is("id_perfil", null)
          : selQuery.eq("id_perfil", id_perfil);
      const { data, error: selErr } = await selQuery.maybeSingle();
      if (selErr) throw selErr;
      existing = data || null;
    }
    // Filtra por id_venta según sea null o definido
    const matchesVenta =
      id_venta === undefined || id_venta === null
        ? existing?.id_venta === null || existing?.id_venta === undefined
        : existing?.id_venta === id_venta;
    const matchesCuenta =
      id_cuenta === undefined || id_cuenta === null
        ? existing?.id_cuenta === null || existing?.id_cuenta === undefined
        : existing?.id_cuenta === id_cuenta;
    const matchesPerfil =
      id_perfil === undefined || id_perfil === null
        ? existing?.id_perfil === null || existing?.id_perfil === undefined
        : existing?.id_perfil === id_perfil;
    // Si llega id_item, siempre tratamos ese registro como el existente (aunque cambien meses).
    const matchExisting =
      existing &&
      (bodyIdItem ? true : matchesVenta && matchesCuenta && matchesPerfil);
    
    const newQty = (matchExisting ? existing.cantidad : 0) + parsedDelta;

    // Permite delta=0 para sincronizar solo meses (u otros campos) de un item existente.
    if (matchExisting && parsedDelta === 0) {
      const { error: updErr } = await supabaseAdmin
        .from("carrito_items")
        .update({
          meses: mesesVal ?? existing.meses ?? null,
          renovacion: renovacion === true,
          id_venta: id_venta ?? existing.id_venta ?? null,
          id_cuenta: id_cuenta ?? existing.id_cuenta ?? null,
          id_perfil: id_perfil ?? existing.id_perfil ?? null,
        })
        .eq("id_item", existing.id_item);
      if (updErr) throw updErr;
    } else

    if (matchExisting && newQty <= 0) {
      const { error: delErr } = await supabaseAdmin
        .from("carrito_items")
        .delete()
        .eq("id_item", existing.id_item);
      if (delErr) throw delErr;
    } else if (matchExisting) {
      const { error: updErr } = await supabaseAdmin
        .from("carrito_items")
        .update({
          cantidad: newQty,
          meses: mesesVal ?? existing.meses ?? null,
          renovacion: renovacion === true,
          id_venta: id_venta ?? existing.id_venta ?? null,
          id_cuenta: id_cuenta ?? existing.id_cuenta ?? null,
          id_perfil: id_perfil ?? existing.id_perfil ?? null,
        })
        .eq("id_item", existing.id_item);
      if (updErr) throw updErr;
    } else if (newQty > 0) {
      const { error: insErr } = await supabaseAdmin
        .from("carrito_items")
        .insert({
          id_carrito: idCarrito,
          id_precio,
          cantidad: newQty,
          meses: mesesVal,
          renovacion: renovacion === true,
          id_venta: id_venta ?? null,
          id_cuenta: id_cuenta ?? null,
          id_perfil: id_perfil ?? null,
        });
      if (insErr) throw insErr;
    }

    // Si no quedan items, elimina el carrito
    const { data: countData, count, error: cntErr } = await supabaseAdmin
      .from("carrito_items")
      .select("id_item", { count: "exact", head: true })
      .eq("id_carrito", idCarrito);
    if (cntErr) throw cntErr;
    const remaining = typeof count === "number" ? count : countData?.length ?? 0;
    if (remaining === 0) {
      await supabaseAdmin.from("carritos").delete().eq("id_carrito", idCarrito);
      console.log("[cart:item] carrito vacío, eliminado", idCarrito);
    } else {
      try {
        const { data: userRow, error: userErr } = await supabaseAdmin
          .from("usuarios")
          .select("acceso_cliente")
          .eq("id_usuario", idUsuario)
          .maybeSingle();
        if (userErr) throw userErr;
        const useMayor = userRow?.acceso_cliente === false;

        const { data: items, error: itemsErr } = await supabaseAdmin
          .from("carrito_items")
          .select("id_precio, cantidad, meses")
          .eq("id_carrito", idCarrito);
        if (itemsErr) throw itemsErr;
        const priceIds = (items || []).map((i) => i.id_precio).filter(Boolean);
        let totalUsd = 0;
        if (priceIds.length) {
          const { data: prices, error: pricesErr } = await supabaseAdmin
            .from("precios")
            .select("id_precio, precio_usd_detal, precio_usd_mayor")
            .in("id_precio", priceIds);
          if (pricesErr) throw pricesErr;
          const priceMap = (prices || []).reduce((acc, p) => {
            acc[p.id_precio] = p;
            return acc;
          }, {});
          totalUsd = (items || []).reduce((sum, it) => {
            const price = priceMap[it.id_precio] || {};
            const unit = useMayor
              ? Number(price.precio_usd_mayor) || Number(price.precio_usd_detal) || 0
              : Number(price.precio_usd_detal) || 0;
            const qty = Number(it.cantidad) || 0;
            const meses = Number(it.meses) || 1;
            return sum + unit * qty * meses;
          }, 0);
        }
        const { error: updMontoErr } = await supabaseAdmin
          .from("carritos")
          .update({ monto_usd: totalUsd })
          .eq("id_carrito", idCarrito);
        if (updMontoErr) throw updMontoErr;
      } catch (mErr) {
        console.error("[cart:item] update monto_usd error", mErr);
      }
    }

    console.log("[cart:item] usuario", idUsuario, "carrito", idCarrito, "delta", parsedDelta, "id_precio", id_precio, "remaining", remaining);
    res.json({ ok: true, id_carrito: idCarrito, remaining });
  } catch (err) {
    console.error("[cart:item] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Crear (o devolver) carrito del usuario activo
app.post("/api/cart", async (_req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(_req);
    const idCarrito = await getOrCreateCarrito(idUsuario);
    res.json({ ok: true, id_carrito: idCarrito });
  } catch (err) {
    console.error("[cart:create] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Obtener carrito existente y sus items
app.get("/api/cart", async (_req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(_req);
    const carritoId = await getCurrentCarrito(idUsuario);
    if (!carritoId) return res.json({ items: [] });

    const { data: carritoInfo, error: carritoErr } = await supabaseAdmin
      .from("carritos")
      .select("monto_usd, monto_bs, tasa_bs, descuento, monto_final, hora, fecha")
      .eq("id_carrito", carritoId)
      .maybeSingle();
    if (carritoErr) throw carritoErr;

    const { data: items, error: itemErr } = await supabaseAdmin
      .from("carrito_items")
      .select("id_item, id_precio, cantidad, meses, renovacion, id_venta, id_cuenta, id_perfil")
      .eq("id_carrito", carritoId);
    if (itemErr) throw itemErr;

    // Enriquecer con datos de venta/cuenta/perfil para renovaciones
    const ventaIds = (items || []).map((i) => i.id_venta).filter(Boolean);
    let ventaMap = {};
    if (ventaIds.length) {
      const { data: ventasExtra, error: ventErr } = await supabaseAdmin
        .from("ventas")
        .select(
          "id_venta, id_cuenta, id_perfil, cuentas:cuentas!ventas_id_cuenta_fkey(correo), perfiles:perfiles(n_perfil)"
        )
        .in("id_venta", ventaIds);
      if (ventErr) throw ventErr;
      ventaMap = (ventasExtra || []).reduce((acc, v) => {
        acc[v.id_venta] = v;
        return acc;
      }, {});
    }

    const enriched = (items || []).map((it) => {
      const ventaInfo = it.id_venta ? ventaMap[it.id_venta] || {} : {};
      return {
        ...it,
        correo: ventaInfo?.cuentas?.correo || null,
        n_perfil: ventaInfo?.perfiles?.n_perfil || null,
      };
    });

    res.json({
      id_carrito: carritoId,
      items: enriched,
      monto_usd: carritoInfo?.monto_usd ?? null,
      monto_bs: carritoInfo?.monto_bs ?? null,
      tasa_bs: carritoInfo?.tasa_bs ?? null,
      descuento: carritoInfo?.descuento ?? null,
      monto_final: carritoInfo?.monto_final ?? null,
      hora: carritoInfo?.hora ?? null,
      fecha: carritoInfo?.fecha ?? null,
    });
  } catch (err) {
    console.error("[cart:get] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Actualizar montos fijos del carrito (USD y BS)
app.post("/api/cart/montos", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    const carritoId = await getCurrentCarrito(idUsuario);
    if (!carritoId) {
      return res.status(400).json({ error: "Carrito no encontrado" });
    }
    const monto_usd = Number(req.body?.monto_usd);
    const tasa_bs = req.body?.tasa_bs === null ? null : Number(req.body?.tasa_bs);
    if (!Number.isFinite(monto_usd)) {
      return res.status(400).json({ error: "monto_usd inválido" });
    }
    const caracasNow = new Date(
      new Date().toLocaleString("en-US", { timeZone: "America/Caracas" })
    );
    const pad2 = (val) => String(val).padStart(2, "0");
    const fecha = `${caracasNow.getFullYear()}-${pad2(caracasNow.getMonth() + 1)}-${pad2(
      caracasNow.getDate()
    )}`;
    const hora = `${pad2(caracasNow.getHours())}:${pad2(caracasNow.getMinutes())}:${pad2(
      caracasNow.getSeconds()
    )}`;
    const { error: updErr } = await supabaseAdmin
      .from("carritos")
      .update({
        monto_usd,
        tasa_bs: Number.isFinite(tasa_bs) ? tasa_bs : null,
        hora,
        fecha,
      })
      .eq("id_carrito", carritoId);
    if (updErr) throw updErr;
    return res.json({ ok: true, id_carrito: carritoId });
  } catch (err) {
    console.error("[cart:montos] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    return res.status(500).json({ error: err.message });
  }
});

// Sube comprobantes al bucket usando la clave de servicio (evita RLS en cliente)
app.post("/api/checkout/upload", async (req, res) => {
  const files = req.body?.files;
  if (!Array.isArray(files) || !files.length) {
    return res.status(400).json({ error: "files es requerido" });
  }

  try {
    const idUsuario = await getOrCreateUsuario(req);
    const urls = [];

    const sanitizeFileName = (name = "file") => {
      const cleaned = String(name)
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "-")
        .replace(/[^a-z0-9._-]/g, "");
      return cleaned || "file";
    };

    for (const file of files) {
      const { name, content, type } = file || {};
      if (!name || !content) {
        return res
          .status(400)
          .json({ error: "Cada archivo necesita name y content en base64" });
      }
      const buffer = Buffer.from(content, "base64");
      const safeName = sanitizeFileName(name);
      const path = `comprobantes/${idUsuario}/${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}-${safeName}`;

      const { error } = await supabaseAdmin.storage
        .from("private_assets")
        .upload(path, buffer, {
          contentType: type || "application/octet-stream",
        });
      if (error) throw error;
      const { data } = supabaseAdmin.storage.from("private_assets").getPublicUrl(path);
      if (data?.publicUrl) urls.push(data.publicUrl);
    }

    res.json({ urls });
  } catch (err) {
    console.error("[checkout:upload] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Sube logos de plataformas al bucket público (public_assets/logos)
app.post("/api/logos/upload", async (req, res) => {
  const files = req.body?.files;
  if (!Array.isArray(files) || !files.length) {
    return res.status(400).json({ error: "files es requerido" });
  }

  try {
    const idUsuario = await getOrCreateUsuario(req);
    const urls = [];

    const sanitizeFileName = (name = "file") => {
      const cleaned = String(name)
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "-")
        .replace(/[^a-z0-9._-]/g, "");
      return cleaned || "file";
    };

    for (const file of files) {
      const { name, content, type } = file || {};
      if (!name || !content) {
        return res
          .status(400)
          .json({ error: "Cada archivo necesita name y content en base64" });
      }
      const buffer = Buffer.from(content, "base64");
      const safeName = sanitizeFileName(name);
      const path = `logos/${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}-${safeName}`;

      const { error } = await supabaseAdmin.storage
        .from("public_assets")
        .upload(path, buffer, {
          contentType: type || "application/octet-stream",
        });
      if (error) throw error;
      const { data } = supabaseAdmin.storage.from("public_assets").getPublicUrl(path);
      if (data?.publicUrl) urls.push(data.publicUrl);
    }

    res.json({ urls });
  } catch (err) {
    console.error("[logos:upload] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Sesión: setea cookie httpOnly con el id de usuario
app.post("/api/session", async (req, res) => {
  const { id_usuario } = req.body || {};
  const parsed = Number(id_usuario);
  if (!parsed || Number.isNaN(parsed)) {
    return res.status(400).json({ error: "id_usuario es requerido" });
  }
  res.cookie("session_user_id", parsed, {
    httpOnly: true,
    sameSite: "lax",
  });
  res.json({ ok: true });
});

app.delete("/api/session", (_req, res) => {
  res.clearCookie("session_user_id", { httpOnly: true, sameSite: "lax" });
  res.json({ ok: true });
});

// Inventario: ventas por usuario autenticado agrupadas por plataforma
app.get("/api/inventario", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) throw new Error("Usuario no autenticado");

    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select(
        `
        id_venta,
        correo_miembro,
        fecha_corte,
        id_precio,
        id_cuenta,
        id_perfil,
        cuentas:cuentas!ventas_id_cuenta_fkey(
          id_cuenta,
          id_plataforma,
          correo,
          clave,
          venta_perfil,
          venta_miembro,
          plataformas(nombre, color_1, color_2)
        ),
        perfiles:perfiles(
          id_perfil,
          n_perfil,
          pin,
          id_cuenta_miembro,
          perfil_hogar
        ),
        precios:precios(plan)
      `
      )
      .eq("id_usuario", idUsuario);
    if (error) throw error;

    const memberIds = Array.from(
      new Set(
        (data || [])
          .map((row) => row.perfiles?.id_cuenta_miembro)
          .filter(Boolean),
      ),
    );
    let memberCuentaMap = {};
    if (memberIds.length) {
      const { data: memberCuentas, error: memberErr } = await supabaseAdmin
        .from("cuentas")
        .select("id_cuenta, correo, clave")
        .in("id_cuenta", memberIds);
      if (memberErr) throw memberErr;
      memberCuentaMap = (memberCuentas || []).reduce((acc, c) => {
        acc[c.id_cuenta] = c;
        return acc;
      }, {});
    }

    const items = (data || []).map((row) => {
      const plataforma = row.cuentas?.plataformas?.nombre || "Sin plataforma";
      const color_1 = row.cuentas?.plataformas?.color_1 || null;
      const color_2 = row.cuentas?.plataformas?.color_2 || null;
      const plan = row.precios?.plan || "Sin plan";
      const memberId = row.perfiles?.id_cuenta_miembro || null;
      const memberCuenta = memberId ? memberCuentaMap[memberId] : null;
      return {
        plataforma,
        color_1,
        color_2,
        plan,
        id_venta: row.id_venta,
        id_precio: row.id_precio || null,
        id_plataforma: row.cuentas?.id_plataforma || null,
        id_cuenta: memberId || row.id_cuenta || row.cuentas?.id_cuenta || null,
        id_perfil: row.id_perfil || row.perfiles?.id_perfil || null,
        correo: memberCuenta?.correo || row.cuentas?.correo || "",
        correo_cliente: row.correo_miembro || "",
        clave: memberCuenta?.clave || row.cuentas?.clave || "",
        n_perfil: row.perfiles?.n_perfil ?? null,
        pin: row.perfiles?.pin ?? null,
        perfil_hogar: row.perfiles?.perfil_hogar ?? null,
        fecha_corte: row.fecha_corte,
        venta_perfil: row.cuentas?.venta_perfil,
        venta_miembro: row.cuentas?.venta_miembro,
      };
    });

    const grouped = items.reduce((acc, item) => {
      const key = item.plataforma || "Sin plataforma";
      if (!acc[key]) {
        acc[key] = {
          color_1: item.color_1,
          color_2: item.color_2,
          id_plataforma: item.id_plataforma,
          planes: {},
        };
      }
      if (!acc[key].planes[item.plan]) acc[key].planes[item.plan] = [];
      acc[key].planes[item.plan].push(item);
      return acc;
    }, {});

    const plataformas = Object.entries(grouped).map(([nombre, payload]) => ({
      nombre,
      color_1: payload.color_1 || null,
      color_2: payload.color_2 || null,
      id_plataforma: payload.id_plataforma || null,
      planes: Object.entries(payload.planes).map(([plan, ventas]) => ({ plan, ventas })),
    }));

    res.json({ plataformas });
  } catch (err) {
    console.error("[inventario] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Ventas por orden (para entregar_servicios con id_orden)
app.get("/api/ventas/orden", async (req, res) => {
  try {
    const idOrden = Number(req.query?.id_orden);
    if (!Number.isFinite(idOrden)) {
      return res.status(400).json({ error: "id_orden inválido" });
    }
    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select(
        `
        id_venta,
        fecha_corte,
        id_perfil,
        id_precio,
        pendiente,
        id_orden,
        correo_miembro,
        clave_miembro,
        cuentas:cuentas!ventas_id_cuenta_fkey(id_cuenta, correo, clave, id_plataforma, venta_perfil, venta_miembro),
        perfiles:perfiles(id_perfil, n_perfil, pin, perfil_hogar),
        precios:precios(id_precio, id_plataforma)
      `
      )
      .eq("id_orden", idOrden)
      .order("id_venta", { ascending: false });
    if (error) throw error;
    res.json({ ventas: data || [] });
  } catch (err) {
    console.error("[ventas/orden] error", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin: importar cuentas (CSV)
app.post("/api/admin/import-cuentas", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }

  const boolVal = (v) => {
    if (typeof v === "boolean") return v;
    const s = String(v || "").trim().toLowerCase();
    if (!s) return false;
    return s === "true";
  };
  const toDate = (value) => {
    if (!value) return null;
    const d = new Date(value);
    return Number.isNaN(d.valueOf()) ? null : d.toISOString().slice(0, 10);
  };

  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) throw new Error("Usuario no autenticado");

    const normalized = rows
      .map((r) => {
        const correo = (r.correo || "").trim().toLowerCase();
        const id_plataforma = Number(r.id_plataforma);
        const id_proveedor =
          r.id_proveedor === null || r.id_proveedor === undefined || r.id_proveedor === ""
            ? null
            : Number(r.id_proveedor);
        return {
          correo,
          clave: (r.clave || "").trim() || null,
          fecha_corte: toDate(r.fecha_corte),
          fecha_pagada: toDate(r.fecha_pagada),
          inactiva: boolVal(r.inactiva),
          ocupado: boolVal(r.ocupado),
          id_plataforma: Number.isFinite(id_plataforma) ? id_plataforma : null,
          id_proveedor: Number.isFinite(id_proveedor) ? id_proveedor : null,
          region: (r.region || "").trim() || null,
          correo_codigo: (r.correo_codigo || "").trim() || null,
          clave_codigo: (r.clave_codigo || "").trim() || null,
          link_codigo: (r.link_codigo || "").trim() || null,
          pin_codigo: (r.pin_codigo || "").trim() || null,
          instaddr: boolVal(r.instaddr),
          venta_perfil: boolVal(r.venta_perfil),
          venta_miembro: boolVal(r.venta_miembro),
        };
      })
      .filter((r) => r.correo && Number.isFinite(r.id_plataforma));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const correos = [...new Set(normalized.map((r) => r.correo))];
    const { data: cuentasExist, error: cuentasErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo")
      .in("correo", correos);
    if (cuentasErr) throw cuentasErr;
    const cuentaByCorreo = (cuentasExist || []).reduce((acc, c) => {
      acc[c.correo?.toLowerCase?.()] = c.id_cuenta;
      return acc;
    }, {});

    const mergedByCorreo = new Map();
    normalized.forEach((r) => {
      const id_cuenta = cuentaByCorreo[r.correo] || null;
      const current = mergedByCorreo.get(r.correo) || {};
      mergedByCorreo.set(r.correo, { ...current, ...r, id_cuenta });
    });

    const upsertRows = Array.from(mergedByCorreo.values()).map((r) => ({
      ...r,
      id_cuenta: r.id_cuenta || undefined,
    }));

    if (!upsertRows.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const { error: upsertErr } = await supabaseAdmin
      .from("cuentas")
      .upsert(upsertRows, { onConflict: "id_cuenta" });
    if (upsertErr) throw upsertErr;

    const nuevas = upsertRows.filter((r) => !r.id_cuenta).length;
    const actualizadas = upsertRows.length - nuevas;

    res.json({
      ok: true,
      cuentas: upsertRows.length,
      nuevas,
      actualizadas,
    });
  } catch (err) {
    console.error("[admin:import-cuentas] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Admin: importar clientes (ventas y perfiles ocupados)
app.post("/api/admin/import-clientes", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }

  const boolVal = (v) =>
    v === true || v === "true" || v === "1" || v === 1 || v === "t" || v === "on";

  try {
    const idUsuario = Number(req.body?.id_usuario) || null;

    const makeNameKey = (nombre, apellido) =>
      `${(nombre || "").trim().toLowerCase()}|${(apellido || "").trim().toLowerCase()}`;

    const normalized = rows
      .map((r) => {
        const correo = (r.correo || "").trim().toLowerCase();
        const rawVenta = (r.id_venta || "").toString().replace(/#/g, "").trim();
        const id_venta = rawVenta ? Number(rawVenta) : null;
        const n_perfil =
          r.n_perfil === "" || r.n_perfil === null || r.n_perfil === undefined
            ? null
            : Number(r.n_perfil);
        const fecha_corte_str = (r.fecha_corte || "").trim();
        const fecha_corte =
          /^\d{4}-\d{2}-\d{2}$/.test(fecha_corte_str) ? fecha_corte_str : null;
        const suspendido = boolVal(r.suspendido);
        const meses_contratados =
          r.meses_contratados === "" || r.meses_contratados === null || r.meses_contratados === undefined
            ? null
            : Number(r.meses_contratados);
        const corte_reemplazo = boolVal(r.corte_reemplazo);
        const nombreCompleto = (r.nombre || "").trim();
        const [nombre, ...resto] = nombreCompleto.split(/\s+/);
        const apellido = resto.join(" ").trim() || null;

        return {
          correo,
          id_venta: Number.isNaN(id_venta) ? null : id_venta,
          n_perfil: Number.isInteger(n_perfil) ? n_perfil : null,
          fecha_corte,
          suspendido,
          meses_contratados: Number.isFinite(meses_contratados) ? meses_contratados : null,
          corte_reemplazo,
          nombre: nombre || null,
          apellido,
        };
      })
      .filter((r) => r.correo && r.n_perfil !== null);

    console.log("[import-clientes] normalized sample", normalized.slice(0, 5));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const correos = [...new Set(normalized.map((r) => r.correo))];
    const { data: cuentas, error: cuentasErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo")
      .in("correo", correos);
    if (cuentasErr) throw cuentasErr;
    const cuentaByCorreo = (cuentas || []).reduce((acc, c) => {
      acc[c.correo?.toLowerCase?.()] = c.id_cuenta;
      return acc;
    }, {});

    const rowsWithCuenta = normalized.map((r) => ({
      ...r,
      id_cuenta: cuentaByCorreo[r.correo] || null,
      name_key: makeNameKey(r.nombre, r.apellido),
    }));

    const cuentasIds = [...new Set(rowsWithCuenta.map((r) => r.id_cuenta).filter(Boolean))];

    const nameKeys = Array.from(
      new Set(rowsWithCuenta.map((r) => r.name_key).filter((k) => k && k !== "|"))
    );

    let usuarioByName = {};
    if (nameKeys.length) {
      // Trae todos los usuarios y normaliza a llave nombre|apellido en minúsculas
      const { data: usuariosExist, error: usrErr } = await supabaseAdmin
        .from("usuarios")
        .select("id_usuario, nombre, apellido");
      if (usrErr) throw usrErr;
      usuarioByName = (usuariosExist || []).reduce((acc, u) => {
        const key = makeNameKey(u.nombre, u.apellido);
        if (key && key !== "|") acc[key] = u.id_usuario;
        return acc;
      }, {});
    }

    const nuevosUsuarios = [];
    const newKeys = new Set();
    rowsWithCuenta.forEach((r) => {
      if (
        !r.id_cuenta ||
        !r.name_key ||
        r.name_key === "|" ||
        usuarioByName[r.name_key] ||
        newKeys.has(r.name_key)
      ) {
        return;
      }
      newKeys.add(r.name_key);
      nuevosUsuarios.push({
        nombre: r.nombre || "Cliente",
        apellido: r.apellido,
      });
    });

    if (nuevosUsuarios.length) {
      const { data: insertedUsers, error: insUsrErr } = await supabaseAdmin
        .from("usuarios")
        .insert(nuevosUsuarios)
        .select("id_usuario, nombre, apellido");
      if (insUsrErr) throw insUsrErr;
      (insertedUsers || []).forEach((u) => {
        const key = makeNameKey(u.nombre, u.apellido);
        usuarioByName[key] = u.id_usuario;
      });
    }

    const { data: perfiles, error: perfErr } = await supabaseAdmin
      .from("perfiles")
      .select("id_perfil, id_cuenta, n_perfil")
      .in("id_cuenta", cuentasIds);
    if (perfErr) throw perfErr;
    const perfilMap = {};
    (perfiles || []).forEach((p) => {
      if (!perfilMap[p.id_cuenta]) perfilMap[p.id_cuenta] = {};
      perfilMap[p.id_cuenta][p.n_perfil] = p.id_perfil;
    });

    const perfilesToUpdate = new Set();
    const ventasToUpsert = [];

    rowsWithCuenta.forEach((r) => {
      if (!r.id_cuenta) return;
      const id_usuario = usuarioByName[r.name_key] || null;
      const id_perfil = perfilMap[r.id_cuenta]?.[r.n_perfil] || null;
      if (id_perfil) perfilesToUpdate.add(id_perfil);

      // Solo procesa filas con id_venta; upsert sin tocar fecha_pago
      if (r.id_venta) {
        ventasToUpsert.push({
          id_venta: r.id_venta,
          id_usuario,
          id_cuenta: r.id_cuenta,
          id_perfil,
          fecha_corte: r.fecha_corte || null,
          suspendido: r.suspendido,
          meses_contratados: r.meses_contratados,
          corte_reemplazo: r.corte_reemplazo,
          fecha_pago: null, // no llenar fecha_pago desde importación
        });
      }
    });

    // Upsert ventas con id_venta (sin tocar fecha_pago)
    if (ventasToUpsert.length) {
      console.log(
        "[import-clientes] ventasToUpsert sample",
        ventasToUpsert.slice(0, 5).map((v) => ({
          id_venta: v.id_venta,
          fecha_corte: v.fecha_corte,
          id_cuenta: v.id_cuenta,
          id_perfil: v.id_perfil,
        }))
      );
      const { error: ventaErr } = await supabaseAdmin
        .from("ventas")
        .upsert(ventasToUpsert, { onConflict: "id_venta" });
      if (ventaErr) throw ventaErr;
    }

    if (perfilesToUpdate.size) {
      const { error: updPerfErr } = await supabaseAdmin
        .from("perfiles")
        .update({ ocupado: true })
        .in("id_perfil", Array.from(perfilesToUpdate));
      if (updPerfErr) throw updPerfErr;
    }

    res.json({
      ok: true,
      ventas: ventasToUpsert.length,
      perfiles_ocupados: perfilesToUpdate.size,
      usuarios: Object.keys(usuarioByName).length,
    });
  } catch (err) {
    console.error("[admin:import-clientes] error", err);
    res.status(500).json({ error: err.message });
  }
});

// Importar fechas: actualizar fecha_corte de ventas a partir de CSV (id_venta, fecha_corte)
app.post("/api/admin/import-fechas", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }
  try {
    const normalized = rows
      .map((r) => {
        const rawVenta = (r.id_venta || "").toString().replace(/#/g, "").trim();
        const id_venta = rawVenta ? Number(rawVenta) : null;
        const fecha_corte_str = (r.fecha_corte || "").trim();
        const fecha_corte =
          /^\d{4}-\d{2}-\d{2}$/.test(fecha_corte_str) ? fecha_corte_str : null;
        return {
          id_venta: Number.isFinite(id_venta) && id_venta > 0 ? id_venta : null,
          fecha_corte,
        };
      })
      .filter((r) => r.id_venta && r.fecha_corte);

    console.log("[import-fechas] normalized sample", normalized.slice(0, 5));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas" });
    }

    const updates = normalized.map((r) =>
      supabaseAdmin.from("ventas").update({ fecha_corte: r.fecha_corte }).eq("id_venta", r.id_venta)
    );
    const results = await Promise.all(updates);
    const errUpd = results.find((r) => r?.error);
    if (errUpd?.error) throw errUpd.error;

    res.json({ ok: true, actualizadas: normalized.length });
  } catch (err) {
    console.error("[admin:import-fechas] error", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin: importar pines de perfiles desde CSV parseado en frontend
app.post("/api/admin/import-pines", async (req, res) => {
  const rows = req.body?.rows;
  if (!Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: "rows es requerido" });
  }

  const boolVal = (v) =>
    v === true || v === "true" || v === "1" || v === 1 || v === "t" || v === "on";

  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) throw new Error("Usuario no autenticado");

    const normalized = rows
      .map((r) => ({
        correo: (r.correo || "").trim().toLowerCase(),
        n_perfil:
          r.n_perfil === null || r.n_perfil === undefined || r.n_perfil === ""
            ? null
            : Number(r.n_perfil),
        pin: (() => {
          if (r.pin === null || r.pin === undefined || r.pin === "") return null;
          const num = Number(r.pin);
          return Number.isNaN(num) || num < -32768 || num > 32767 ? null : num;
        })(),
        perfil_hogar: boolVal(r.perfil_hogar),
        ocupado: boolVal(r.ocupado),
      }))
      .filter((r) => r.correo);
    console.log("[import-pines] normalized sample", normalized.slice(0, 5));

    if (!normalized.length) {
      return res.status(400).json({ error: "No hay filas válidas para importar" });
    }

    const correos = [...new Set(normalized.map((r) => r.correo))];
    const { data: cuentas, error: cuentasErr } = await supabaseAdmin
      .from("cuentas")
      .select("id_cuenta, correo")
      .in("correo", correos);
    if (cuentasErr) throw cuentasErr;

    const cuentaByCorreo = (cuentas || []).reduce((acc, c) => {
      acc[c.correo?.toLowerCase?.()] = c.id_cuenta;
      return acc;
    }, {});

    const rowsWithCuenta = normalized.map((r) => ({
      ...r,
      id_cuenta: cuentaByCorreo[r.correo] || null,
    }));
    console.log("[import-pines] rowsWithCuenta sample", rowsWithCuenta.slice(0, 5));
    const sinCuenta = rowsWithCuenta.filter((r) => !r.id_cuenta).map((r) => r.correo);

    // Inserta todo en perfiles, enlazando por correo/id_cuenta; se ignora lógica especial de perfil_hogar
    const perfilesToInsert = rowsWithCuenta
      .filter((r) => r.id_cuenta)
      .map((r) => ({
        id_cuenta: r.id_cuenta,
        n_perfil: Number.isInteger(r.n_perfil) ? r.n_perfil : null,
        pin: r.pin,
        perfil_hogar: !!r.perfil_hogar,
        ocupado: r.ocupado || false,
      }));

    if (perfilesToInsert.length) {
      const { error: insertErr } = await supabaseAdmin.from("perfiles").insert(perfilesToInsert);
      if (insertErr) throw insertErr;
    }

    console.log("[import-pines] inserted perfiles", perfilesToInsert.length);

    res.json({
      ok: true,
      perfiles_insertados: perfilesToInsert.length,
      sin_cuenta: [...new Set(sinCuenta)],
    });
  } catch (err) {
    console.error("[admin:import-pines] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Checkout: crea orden (y procesa ventas si ya está verificado)
app.post("/api/checkout", async (req, res) => {
  const {
    id_metodo_de_pago,
    referencia,
    comprobantes,
    comprobante,
    total: totalCliente,
    tasa_bs,
    id_usuario_override,
  } = req.body || {};
  const archivos = Array.isArray(comprobantes) ? comprobantes : Array.isArray(comprobante) ? comprobante : [];
  if (!id_metodo_de_pago || !referencia || !Array.isArray(archivos)) {
    return res
      .status(400)
      .json({ error: "id_metodo_de_pago, referencia y comprobante(s) son requeridos" });
  }

  try {
    const idUsuarioSesion = await getOrCreateUsuario(req);
    const idUsuarioVentas =
      id_usuario_override && Number.isFinite(Number(id_usuario_override))
        ? Number(id_usuario_override)
        : idUsuarioSesion;
    const carritoId = await getCurrentCarrito(idUsuarioSesion);
    if (!carritoId) return res.status(400).json({ error: "No hay carrito activo" });

    const context = await buildCheckoutContext({
      idUsuarioVentas,
      carritoId,
      totalCliente,
      tasa_bs,
    });
    const { items, priceMap, platInfoById, platNameById, pickPrecio, total, tasaBs } = context;
    if (!items?.length) {
      return res.status(400).json({ error: "El carrito está vacío" });
    }
    console.log("[checkout] carrito items", items);
    console.log("[checkout] precios usados", priceMap);

    const caracasNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
    const pad2 = (val) => String(val).padStart(2, "0");
    const hora_orden = `${pad2(caracasNow.getHours())}:${pad2(caracasNow.getMinutes())}:${pad2(
      caracasNow.getSeconds()
    )}`;
    const referenciaTrim = String(referencia || "").trim();
    const requiereVerificacion =
      Number(id_metodo_de_pago) === 1 && referenciaTrim.toUpperCase() !== "SALDO";
    const en_espera = requiereVerificacion;
    const monto_bs =
      Number.isFinite(total) && Number.isFinite(tasaBs)
        ? Math.round(total * tasaBs * 100) / 100
        : null;

    const { data: orden, error: ordErr } = await supabaseAdmin
      .from("ordenes")
      .insert({
        id_usuario: idUsuarioVentas,
        total,
        tasa_bs: tasaBs,
        monto_bs,
        id_metodo_de_pago,
        referencia,
        comprobante: archivos,
        en_espera,
        hora_orden,
        id_carrito: carritoId,
        pago_verificado: false,
        monto_completo: null,
      })
      .select("id_orden")
      .single();
    if (ordErr) throw ordErr;

    if (requiereVerificacion) {
      try {
        await supabaseAdmin
          .from("carritos")
          .insert({ id_usuario: idUsuarioSesion, fecha_creacion: new Date().toISOString() });
      } catch (cartErr) {
        console.error("[checkout] crear carrito nuevo error", cartErr);
      }
      return res.json({ ok: true, id_orden: orden.id_orden, total, ventas: 0, pendiente_verificacion: true });
    }

    const result = await processOrderFromItems({
      ordenId: orden.id_orden,
      idUsuarioSesion,
      idUsuarioVentas,
      items,
      priceMap,
      platInfoById,
      platNameById,
      pickPrecio,
      referencia,
      archivos,
      id_metodo_de_pago,
      carritoId,
    });

    await supabaseAdmin
      .from("ordenes")
      .update({ pago_verificado: true, en_espera: result.pendientesCount > 0 })
      .eq("id_orden", orden.id_orden);

    res.json({
      ok: true,
      id_orden: orden.id_orden,
      total,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
    });
  } catch (err) {
    console.error("[checkout] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Procesar orden luego de verificación de pago
app.post("/api/ordenes/procesar", async (req, res) => {
  const idOrden = Number(req.body?.id_orden);
  if (!Number.isFinite(idOrden)) {
    return res.status(400).json({ error: "id_orden inválido" });
  }

  try {
    const idUsuarioSesion = await getOrCreateUsuario(req);
    const { data: orden, error: ordErr } = await supabaseAdmin
      .from("ordenes")
      .select(
        "id_orden, id_usuario, id_carrito, referencia, comprobante, id_metodo_de_pago, total, tasa_bs, pago_verificado, en_espera, orden_cancelada"
      )
      .eq("id_orden", idOrden)
      .single();
    if (ordErr) throw ordErr;

    if (orden?.orden_cancelada === true) {
      return res.status(400).json({ error: "Orden cancelada. No se pueden asignar servicios." });
    }

    const idUsuarioVentas = Number(orden?.id_usuario) || idUsuarioSesion;
    if (idUsuarioSesion && orden?.id_usuario && Number(orden.id_usuario) !== Number(idUsuarioSesion)) {
      return res.status(403).json({ error: "Orden no pertenece al usuario." });
    }
    if (!orden?.id_carrito) {
      return res.status(400).json({ error: "Orden sin carrito asociado." });
    }

    const { data: ventasExist, error: ventasErr } = await supabaseAdmin
      .from("ventas")
      .select("id_venta")
      .eq("id_orden", idOrden)
      .limit(1);
    if (ventasErr) throw ventasErr;
    if (ventasExist?.length) {
      if (!orden?.pago_verificado) {
        const { data: pendRows, error: pendErr } = await supabaseAdmin
          .from("ventas")
          .select("id_venta")
          .eq("id_orden", idOrden)
          .eq("pendiente", true);
        if (pendErr) throw pendErr;
        await supabaseAdmin
          .from("ordenes")
          .update({ pago_verificado: true, en_espera: (pendRows || []).length > 0 })
          .eq("id_orden", idOrden);
      }
      return res.json({
        ok: true,
        id_orden: idOrden,
        already_processed: true,
        ventas: ventasExist.length,
      });
    }

    const context = await buildCheckoutContext({
      idUsuarioVentas,
      carritoId: orden.id_carrito,
      totalCliente: orden.total,
      tasa_bs: orden.tasa_bs,
    });
    if (!context.items?.length) {
      return res.status(400).json({ error: "El carrito está vacío" });
    }

    const archivos = normalizeFilesArray(orden?.comprobante);
    const result = await processOrderFromItems({
      ordenId: idOrden,
      idUsuarioSesion,
      idUsuarioVentas,
      items: context.items,
      priceMap: context.priceMap,
      platInfoById: context.platInfoById,
      platNameById: context.platNameById,
      pickPrecio: context.pickPrecio,
      referencia: orden?.referencia,
      archivos,
      id_metodo_de_pago: orden?.id_metodo_de_pago,
      carritoId: orden.id_carrito,
    });

    await supabaseAdmin
      .from("ordenes")
      .update({ pago_verificado: true, en_espera: result.pendientesCount > 0 })
      .eq("id_orden", idOrden);

    res.json({
      ok: true,
      id_orden: idOrden,
      ventas: result.ventasCount,
      pendientes: result.pendientesCount,
    });
  } catch (err) {
    console.error("[ordenes/procesar] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

// Ventas entregadas (pendiente = false) por usuario
app.get("/api/ventas/entregadas", async (req, res) => {
  try {
    const idUsuario = await getOrCreateUsuario(req);
    if (!idUsuario) throw new Error("Usuario no autenticado");

    const { data, error } = await supabaseAdmin
      .from("ventas")
      .select("id_venta", { count: "exact" })
      .eq("id_usuario", idUsuario)
      .eq("pendiente", false);
    if (error) throw error;

    res.json({ entregadas: data?.length || 0 });
  } catch (err) {
    console.error("[ventas entregadas] error", err);
    if (err?.code === AUTH_REQUIRED || err?.message === AUTH_REQUIRED) {
      return res.status(401).json({ error: "Usuario no autenticado" });
    }
    res.status(500).json({ error: err.message });
  }
});

if (require.main === module) {
  app.listen(port, () => {
    console.log(`Servidor escuchando en puerto ${port}`);
  });
}

module.exports = app;
