import { createClient } from "https://esm.sh/@supabase/supabase-js@2.86.0?no-check&sourcemap=0";
import { requireSession } from "./session.js";

const supabase = createClient(
  "https://ojigtjcwhcrnawdbtqkl.supabase.co",
  "sb_publishable_pUhdf8wgEJyUtUg6TZqcTA_qF9gwEjJ"
);

// Mantén mismo host; si estás en el servidor estático local (127.0.0.1:5500) apunta al backend local en 3000.
const API_BASE = (() => {
  if (typeof window === "undefined") return "http://localhost:3000";
  const { protocol, host } = window.location;
  if (host === "127.0.0.1:5500") return "http://127.0.0.1:3000";
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
        "id_plataforma, id_categoria, nombre, imagen, por_pantalla, por_acceso, tarjeta_de_regalo, entrega_inmediata, descuento_meses, mostrar_stock"
      )
      .order("nombre"),
    supabase
      .from("precios")
      .select(
        "id_precio, id_plataforma, cantidad, precio_usd_detal, precio_usd_mayor, duracion, completa, plan, region, valor_tarjeta_de_regalo, moneda, sub_cuenta"
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
    const res = await fetch(`${API_BASE}/api/cart?id_usuario=${encodeURIComponent(id)}`, {
      credentials: "include",
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("cart get response", res.status, text);
      return { items: [] };
    }
    return res.json();
  } catch (err) {
    console.error("No se pudo obtener el carrito:", err);
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
    const res = await fetch(
      `${API_BASE}/api/ventas/orden?id_orden=${encodeURIComponent(idOrden)}`,
      {
        credentials: "include",
      }
    );
    if (!res.ok) {
      const text = await res.text();
      console.error("ventas/orden response", res.status, text);
      return { error: text || "No se pudo cargar ventas por orden" };
    }
    return res.json();
  } catch (err) {
    console.error("fetchVentasOrden error", err);
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
      "id_usuario, nombre, apellido, correo, permiso_admin, permiso_superadmin, acceso_cliente, notificacion_inventario, saldo"
    )
    .eq("id_usuario", idUsuario)
    .maybeSingle();
  if (error) {
    console.error("loadCurrentUser error", error);
    return null;
  }
  return data;
}

export { supabase };
