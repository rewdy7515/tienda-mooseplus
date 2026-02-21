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

const readApiBaseOverride = () => {
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

export async function loadCatalog() {
  const [
    { data: categorias, error: errCat },
    { data: plataformas, error: errPlat },
    { data: precios, error: errPre },
    { data: descuentos, error: errDesc },
  ] = await Promise.all([
    supabase.from("categorias").select("id_categoria, nombre").order("id_categoria"),
    supabase
      .from("plataformas")
      .select(
        "id_plataforma, id_categoria, nombre, imagen, banner, por_pantalla, por_acceso, tarjeta_de_regalo, entrega_inmediata, descuento_meses, mostrar_stock, no_disponible, num_max_dispositivos"
      )
      .order("nombre"),
    supabase
      .from("precios")
      .select(
        "id_precio, id_plataforma, cantidad, precio_usd_detal, precio_usd_mayor, duracion, completa, plan, region, valor_tarjeta_de_regalo, moneda, sub_cuenta, descripcion_plan"
      )
      .order("id_precio"),
    supabase.from("descuentos").select("id_descuento, meses, descuento_1, descuento_2"),
  ]);

  if (errCat || errPlat || errPre || errDesc) {
    throw new Error(errCat?.message || errPlat?.message || errPre?.message || errDesc?.message);
  }

  return { categorias, plataformas, precios, descuentos };
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
    console.log("[fetchCart] request", { url, apiBase: API_BASE, id_usuario: id });
    const res = await fetch(url, {
      credentials: "include",
    });
    const elapsedMs = Date.now() - startedAt;
    console.log("[fetchCart] response", {
      status: res.status,
      ok: res.ok,
      elapsedMs,
      contentType: res.headers.get("content-type"),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart get response", res.status, text?.slice(0, 800));
      return { items: [] };
    }
    const data = await res.json();
    console.log("[fetchCart] parsed", {
      hasCarrito: Boolean(data?.carrito),
      items: Array.isArray(data?.items) ? data.items.length : 0,
    });
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
    const payloadFiles = await Promise.all(
      files.map(async (file) => ({
        name: file.name,
        type: file.type,
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

export async function startSession(idUsuario) {
  try {
    const res = await fetch(`${API_BASE}/api/session`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ id_usuario: idUsuario }),
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
  await startSession(id);
}

export async function loadCurrentUser() {
  const idUsuario = requireSession();
  const { data, error } = await supabase
    .from("usuarios")
    .select(
      "id_usuario, nombre, apellido, correo, telefono, foto_perfil, fondo_perfil, permiso_admin, permiso_superadmin, acceso_cliente, notificacion_inventario, saldo"
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
