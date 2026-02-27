import { createClient } from "https://esm.sh/@supabase/supabase-js@2.86.0?no-check&sourcemap=0";
import { requireSession } from "./session.js";

const supabase = createClient(
  "https://ojigtjcwhcrnawdbtqkl.supabase.co",
  "sb_publishable_pUhdf8wgEJyUtUg6TZqcTA_qF9gwEjJ"
);

const normalizeApiBase = (value) => {
  if (!value) return "";
  const raw = String(value).trim().replace(/\/+$/, "");
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  return "";
};

const canUseApiBaseOverride = () => {
  if (typeof window === "undefined") return false;
  const host = String(window.location.hostname || "").trim().toLowerCase();
  return host === "localhost" || host === "127.0.0.1";
};

const readApiBaseOverride = () => {
  if (!canUseApiBaseOverride()) return "";
  if (typeof window === "undefined") return "";
  try {
    const query = new URLSearchParams(window.location.search || "").get("api_base");
    const fromQuery = normalizeApiBase(query);
    if (fromQuery) {
      window.localStorage.setItem("api_base", fromQuery);
      return fromQuery;
    }
  } catch (_err) {
    // noop
  }
  try {
    return normalizeApiBase(window.localStorage.getItem("api_base"));
  } catch (_err) {
    return "";
  }
};

// En local (ej. :5500) usa backend en :3000 con el mismo hostname.
const API_BASE = (() => {
  if (typeof window === "undefined") return "http://localhost:3000";
  const override = readApiBaseOverride();
  if (override) return override;
  const { protocol, hostname, host, port } = window.location;
  if (!host) return "http://localhost:3000";
  const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost";
  if (isLocalHost && port !== "3000") return `http://${hostname}:3000`;
  return `${protocol}//${host}`;
})();

const bufferToBase64 = (buffer) => {
  let binary = "";
  const bytes = new Uint8Array(buffer);
  const chunkSize = 8192;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
};

const IMAGE_MIME_BY_EXT = {
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  webp: "image/webp",
  gif: "image/gif",
  bmp: "image/bmp",
  avif: "image/avif",
  svg: "image/svg+xml",
};

const getImageMimeFromName = (name = "") => {
  const match = String(name || "")
    .trim()
    .toLowerCase()
    .match(/\.([a-z0-9]+)$/);
  const ext = match?.[1] || "";
  return IMAGE_MIME_BY_EXT[ext] || "";
};

const normalizeImageUploadType = (type = "", fileName = "") => {
  const rawType = String(type || "").trim().toLowerCase();
  const mimeFromName = getImageMimeFromName(fileName);
  if (rawType === "image/webp" || mimeFromName === "image/webp") return "image/webp";
  if (rawType.startsWith("image/")) return rawType;
  if (mimeFromName) return mimeFromName;
  return "application/octet-stream";
};

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const isTransientCartBackendError = (status, bodyText = "") => {
  if (Number(status) < 500) return false;
  const msg = String(bodyText || "").toLowerCase();
  return (
    msg.includes("fetch failed") ||
    msg.includes("network request failed") ||
    msg.includes("timeout") ||
    msg.includes("econnreset")
  );
};

export async function loadCatalog() {
  const normalizePlataformaRow = (row = {}) => {
    const idDescMesBase = row.id_descuento_mes ?? 1;
    const idDescCantidadBase = row.id_descuento_cantidad ?? 2;
    const isExplicitFalse = (value) =>
      value === false || value === 0 || value === "0" || value === "false" || value === "f";
    return {
      ...row,
      id_descuento_mes: idDescMesBase,
      id_descuento_cantidad: idDescCantidadBase,
      id_descuento_mes_detal: row.id_descuento_mes_detal ?? idDescMesBase,
      id_descuento_mes_mayor: row.id_descuento_mes_mayor ?? idDescMesBase,
      id_descuento_cantidad_detal: row.id_descuento_cantidad_detal ?? idDescCantidadBase,
      id_descuento_cantidad_mayor: row.id_descuento_cantidad_mayor ?? idDescCantidadBase,
      aplica_descuento_mes_detal: isExplicitFalse(row.aplica_descuento_mes_detal) ? false : true,
      aplica_descuento_mes_mayor: isExplicitFalse(row.aplica_descuento_mes_mayor) ? false : true,
      aplica_descuento_cantidad_detal: isExplicitFalse(row.aplica_descuento_cantidad_detal)
        ? false
        : true,
      aplica_descuento_cantidad_mayor: isExplicitFalse(row.aplica_descuento_cantidad_mayor)
        ? false
        : true,
    };
  };

  const fetchPlataformasCatalog = async () => {
    // `select("*")` evita 400 por columnas opcionales no presentes en algunos entornos.
    const base = await supabase.from("plataformas").select("*").order("nombre");
    if (base.error) return base;
    return {
      ...base,
      data: (base.data || []).map(normalizePlataformaRow),
    };
  };

  const [
    { data: categorias, error: errCat },
    { data: plataformas, error: errPlat },
    { data: precios, error: errPre },
    { data: descuentos, error: errDesc },
  ] = await Promise.all([
    supabase.from("categorias").select("id_categoria, nombre").order("id_categoria"),
    fetchPlataformasCatalog(),
    supabase
      .from("precios")
      .select(
        "id_precio, id_plataforma, cantidad, precio_usd_detal, precio_usd_mayor, duracion, completa, plan, region, valor_tarjeta_de_regalo, moneda, sub_cuenta, descripcion_plan"
      )
      .order("id_precio"),
    supabase.from("descuentos").select("*"),
  ]);

  if (errCat || errPlat || errPre || errDesc) {
    throw new Error(errCat?.message || errPlat?.message || errPre?.message || errDesc?.message);
  }

  return { categorias, plataformas, precios, descuentos };
}

export async function fetchHomeBanners(options = {}) {
  try {
    const url = new URL(`${API_BASE}/api/home-banners`);
    if (options?.includeInactive === true) {
      url.searchParams.set("include_inactive", "true");
    }
    const res = await fetch(url.toString(), {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      return { error: text || "No se pudieron cargar banners", items: [] };
    }
    const data = await res.json().catch(() => ({}));
    return {
      items: Array.isArray(data?.items) ? data.items : [],
      tableMissing: data?.tableMissing === true,
    };
  } catch (err) {
    console.error("No se pudieron cargar los banners del home:", err);
    return { error: err.message, items: [] };
  }
}

export async function createHomeBanner(payload = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const res = await fetch(`${API_BASE}/api/home-banners`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        ...payload,
        id_usuario,
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo crear el banner." };
    }
    return data;
  } catch (err) {
    console.error("No se pudo crear banner:", err);
    return { error: err.message };
  }
}

export async function updateHomeBanner(idBanner, payload = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const res = await fetch(`${API_BASE}/api/home-banners/${encodeURIComponent(idBanner)}`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        ...payload,
        id_usuario,
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo actualizar el banner." };
    }
    return data;
  } catch (err) {
    console.error("No se pudo actualizar banner:", err);
    return { error: err.message };
  }
}

export async function createUsuarioSignupLink(idUsuarioTarget) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const target = Number(idUsuarioTarget);
    if (!Number.isFinite(target) || target <= 0) {
      return { error: "id_usuario inválido" };
    }

    const res = await fetch(
      `${API_BASE}/api/usuarios/${encodeURIComponent(target)}/signup-link`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ id_usuario }),
      },
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo generar el link de registro." };
    }
    return data;
  } catch (err) {
    console.error("createUsuarioSignupLink error", err);
    return { error: err.message };
  }
}

export async function validateSignupRegistrationToken(token) {
  try {
    const value = String(token || "").trim();
    if (!value) return { error: "Token requerido." };
    const res = await fetch(
      `${API_BASE}/api/signup-link/validate?token=${encodeURIComponent(value)}`,
      {
        credentials: "include",
      },
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo validar el token." };
    }
    return data;
  } catch (err) {
    console.error("validateSignupRegistrationToken error", err);
    return { error: err.message };
  }
}

export async function completeSignupWithRegistrationToken(payload = {}) {
  try {
    const res = await fetch(`${API_BASE}/api/signup-link/complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudo completar el registro con token." };
    }
    return data;
  } catch (err) {
    console.error("completeSignupWithRegistrationToken error", err);
    return { error: err.message };
  }
}

// Enviar delta de cantidad (+ agrega / - resta). Si la cantidad resultante es <= 0 se borra el item, y si no quedan items se elimina el carrito.
export async function sendCartDelta(idPrecio, delta, meses, extra = {}) {
  await ensureServerSession();
  const id_usuario = requireSession();
  const res = await fetch(`${API_BASE}/api/cart/item`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
    body: JSON.stringify({ id_precio: idPrecio, delta, id_usuario, meses, ...extra }),
  });
  if (!res.ok) {
    const text = await res.text();
    console.error("cart/item response", res.status, text);
    throw new Error(text || "No se pudo actualizar el carrito");
  }
}

export async function createCart() {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const res = await fetch(`${API_BASE}/api/cart`, {
      method: "POST",
      credentials: "include",
      body: JSON.stringify({ id_usuario }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart create response", res.status, text);
      return null;
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo crear el carrito:", err);
    return null;
  }
}

export async function fetchCart() {
  await ensureServerSession();
  try {
    const id = requireSession();
    const url = `${API_BASE}/api/cart?id_usuario=${encodeURIComponent(id)}`;
    const startedAt = Date.now();
    let res = null;
    let data = null;
    let lastErrText = "";
    const maxAttempts = 2;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      res = await fetch(url, {
        credentials: "include",
      });
      const elapsedMs = Date.now() - startedAt;

      if (res.ok) {
        data = await res.json();
        break;
      }

      lastErrText = await res.text();
      const shouldRetry =
        attempt < maxAttempts &&
        isTransientCartBackendError(res.status, lastErrText);
      if (!shouldRetry) break;

      console.warn(
        "[fetchCart] transient backend error, reintentando...",
        { attempt, status: res.status, body: String(lastErrText || "").slice(0, 220) },
      );
      await wait(250 * attempt);
    }

    if (!res || !res.ok || !data) {
      console.error("cart get response", res?.status, String(lastErrText || "").slice(0, 800));
      return { items: [] };
    }

    return data;
  } catch (err) {
    console.error("No se pudo obtener el carrito:", err, {
      apiBase: API_BASE,
    });
    return { items: [] };
  }
}

export async function submitCheckout(payload) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const res = await fetch(`${API_BASE}/api/checkout`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ ...payload, id_usuario, comprobante: payload.comprobantes }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("checkout response", res.status, text);
      return { error: text || "Error en checkout" };
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo completar el checkout:", err);
    return { error: err.message };
  }
}

export async function fetchCheckoutDraft() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/checkout/draft`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({}),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("checkout/draft response", res.status, text);
      return { error: text || "No se pudo obtener la orden de checkout" };
    }
    return res.json();
  } catch (err) {
    console.error("fetchCheckoutDraft error", err);
    return { error: err.message };
  }
}

export async function procesarOrden(id_orden) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/ordenes/procesar`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ id_orden }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("ordenes/procesar response", res.status, text);
      return { error: text || "No se pudo procesar la orden" };
    }
    return res.json();
  } catch (err) {
    console.error("procesarOrden error", err);
    return { error: err.message };
  }
}

export async function updateCartMontos(monto_usd, tasa_bs) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/cart/montos`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ monto_usd, tasa_bs }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart/montos response", res.status, text);
      return { error: text || "No se pudo actualizar montos del carrito" };
    }
    return res.json();
  } catch (err) {
    console.error("updateCartMontos error", err);
    return { error: err.message };
  }
}

export async function updateCartFlags({ usa_saldo } = {}) {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/cart/flags`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ usa_saldo }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart/flags response", res.status, text);
      return { error: text || "No se pudo actualizar el carrito" };
    }
    return res.json();
  } catch (err) {
    console.error("updateCartFlags error", err);
    return { error: err.message };
  }
}

export async function uploadComprobantes(files = []) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const payloadFiles = await Promise.all(
      files.map(async (file) => ({
        name: file.name,
        type: file.type,
        content: bufferToBase64(await file.arrayBuffer()),
      }))
    );

    const res = await fetch(`${API_BASE}/api/checkout/upload`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({ files: payloadFiles, id_usuario }),
    });

    if (!res.ok) {
      const text = await res.text();
      console.error("upload comprobantes response", res.status, text);
      return { error: text || "No se pudieron subir los comprobantes" };
    }

    return res.json();
  } catch (err) {
    console.error("No se pudieron subir los comprobantes:", err);
    return { error: err.message };
  }
}

export async function uploadPlatformLogos(files = [], options = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const folder = String(options?.folder || "").trim();
    const overwriteByName = !!options?.overwriteByName;
    const payloadFiles = await Promise.all(
      files.map(async (file) => ({
        name: file.name,
        type: normalizeImageUploadType(file.type, file.name),
        content: bufferToBase64(await file.arrayBuffer()),
      }))
    );

    const res = await fetch(`${API_BASE}/api/logos/upload`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        files: payloadFiles,
        id_usuario,
        ...(folder ? { folder } : {}),
        ...(overwriteByName ? { overwrite_by_name: true } : {}),
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      console.error("upload logos response", res.status, text);
      return { error: text || "No se pudieron subir los logos" };
    }

    return res.json();
  } catch (err) {
    console.error("No se pudieron subir los logos:", err);
    return { error: err.message };
  }
}

export async function deletePublicAssets(payload = {}) {
  await ensureServerSession();
  try {
    const id_usuario = requireSession();
    const paths = Array.isArray(payload?.paths)
      ? payload.paths.map((row) => String(row || "").trim()).filter(Boolean)
      : [];
    const public_urls = Array.isArray(payload?.public_urls)
      ? payload.public_urls.map((row) => String(row || "").trim()).filter(Boolean)
      : [];

    const res = await fetch(`${API_BASE}/api/logos/delete`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        id_usuario,
        paths,
        public_urls,
      }),
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { error: data?.error || "No se pudieron eliminar archivos." };
    }
    return data;
  } catch (err) {
    console.error("No se pudieron eliminar assets públicos:", err);
    return { error: err.message };
  }
}

export async function fetchInventario() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/inventario`, {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("inventario response", res.status, text);
      return { error: text || "No se pudo cargar el inventario" };
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo cargar el inventario:", err);
    return { error: err.message };
  }
}

export async function fetchVentasOrden(idOrden) {
  await ensureServerSession();
  try {
    console.log("fetchVentasOrden request", { idOrden });
    const res = await fetch(
      `${API_BASE}/api/ventas/orden?id_orden=${encodeURIComponent(idOrden)}`,
      {
        credentials: "include",
      }
    );
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (err) {
        message = (await res.text()) || "";
      }
      if (!message && res.status === 403) {
        message = "Orden no pertenece al usuario.";
      }
      if (!message && res.status === 401) {
        message = "Usuario no autenticado.";
      }
      console.error("ventas/orden response", res.status, message);
      return { error: message || "No se pudo cargar ventas por orden", status: res.status };
    }
    const json = await res.json();
    console.log("fetchVentasOrden result", { idOrden, ventas: json?.ventas?.length || 0 });
    return json;
  } catch (err) {
    console.error("fetchVentasOrden error", err);
    return { error: err.message };
  }
}

export async function fetchOrdenDetalle(idOrden) {
  await ensureServerSession();
  try {
    const res = await fetch(
      `${API_BASE}/api/ordenes/detalle?id_orden=${encodeURIComponent(idOrden)}`,
      { credentials: "include" }
    );
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (err) {
        message = (await res.text()) || "";
      }
      if (!message && res.status === 403) {
        message = "Orden no pertenece al usuario.";
      }
      if (!message && res.status === 401) {
        message = "Usuario no autenticado.";
      }
      console.error("ordenes/detalle response", res.status, message);
      return { error: message || "No se pudo cargar el detalle de la orden", status: res.status };
    }
    return res.json();
  } catch (err) {
    console.error("ordenes/detalle error", err);
    return { error: err.message };
  }
}

export async function fetchEntregadas() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/ventas/entregadas`, {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("entregadas response", res.status, text);
      return { error: text || "No se pudo cargar entregas" };
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo cargar entregas:", err);
    return { error: err.message };
  }
}

export async function fetchPendingReminderNoPhoneClients() {
  await ensureServerSession();
  try {
    const res = await fetch(`${API_BASE}/api/whatsapp/recordatorios/pending-no-phone`, {
      credentials: "include",
    });
    if (!res.ok) {
      let message = "";
      try {
        const data = await res.json();
        message = data?.error || "";
      } catch (_err) {
        message = (await res.text()) || "";
      }
      return {
        error: message || "No se pudo cargar clientes pendientes sin teléfono",
        status: res.status,
      };
    }
    return res.json();
  } catch (err) {
    console.error("fetchPendingReminderNoPhoneClients error", err);
    return { error: err.message };
  }
}

export async function fetchP2PRate() {
  try {
    const res = await fetch(`${API_BASE}/api/p2p/rate`, {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("p2p rate response", res.status, text);
      return null;
    }
    const data = await res.json();
    return Number.isFinite(data?.rate) ? data.rate : null;
  } catch (err) {
    console.error("No se pudo obtener la tasa P2P:", err);
    return null;
  }
}

export async function fetchTestingFlag() {
  try {
    const { data, error } = await supabase
      .from("testing")
      .select("testing")
      .eq("id_testing", 1)
      .maybeSingle();
    if (error) throw error;
    return data?.testing ?? false;
  } catch (err) {
    console.error("fetchTestingFlag error", err);
    return false;
  }
}

export async function updateTestingFlag(value) {
  try {
    const { data, error } = await supabase
      .from("testing")
      .update({ testing: value })
      .eq("id_testing", 1)
      .select("testing")
      .maybeSingle();
    if (error) throw error;
    return data?.testing ?? value;
  } catch (err) {
    console.error("updateTestingFlag error", err);
    return value;
  }
}

export async function startSession(_idUsuario) {
  try {
    const { data: sessionData, error: sessionErr } = await supabase.auth.getSession();
    if (sessionErr) {
      console.error("startSession getSession error", sessionErr);
      return { error: "No se pudo leer la sesión de auth" };
    }
    const accessToken = String(sessionData?.session?.access_token || "").trim();
    if (!accessToken) {
      return { error: "Sesión de auth no disponible" };
    }

    const res = await fetch(`${API_BASE}/api/session`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
      credentials: "include",
      body: "{}",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("startSession response", res.status, text);
      return { error: text || "No se pudo establecer la sesión" };
    }
    return res.json();
  } catch (err) {
    console.error("startSession error", err);
    return { error: err.message };
  }
}

export async function clearServerSession() {
  try {
    await fetch(`${API_BASE}/api/session`, {
      method: "DELETE",
      credentials: "include",
    });
    // Fallback: limpia cookie en caso de que el delete falle silenciosamente
    document.cookie = "session_user_id=; Max-Age=0; path=/";
  } catch (err) {
    console.error("clearServerSession error", err);
  }
}

export async function ensureServerSession() {
  const id = requireSession();
  const result = await startSession(id);
  if (result?.error) {
    throw new Error(result.error);
  }
  return result;
}

export async function loadCurrentUser() {
  const idUsuario = requireSession();
  const { data, error } = await supabase
    .from("usuarios")
    .select(
      "id_usuario, nombre, apellido, correo, telefono, foto_perfil, fondo_perfil, permiso_admin, permiso_superadmin, acceso_cliente, notificacion_inventario, saldo, recordatorio_dias_antes"
    )
    .eq("id_usuario", idUsuario)
    .maybeSingle();
  if (error) {
    console.error("loadCurrentUser error", error);
    return null;
  }
  return data;
}

export { supabase, API_BASE };
