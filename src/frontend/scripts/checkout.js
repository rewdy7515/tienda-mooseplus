import {
  supabase,
  fetchCart,
  loadCatalog,
  submitCheckout,
  uploadComprobantes,
  fetchP2PRate,
  loadCurrentUser,
} from "./api.js";
import { requireSession, attachLogoHome } from "./session.js";

requireSession();
attachLogoHome();

const metodosContainer = document.querySelector("#metodos-container");
const metodoDetalle = document.querySelector("#metodo-detalle");
const metodoSelect = document.querySelector("#metodo-select");
const btnAddImage = document.querySelector("#btn-add-image");
const inputFiles = document.querySelector("#input-files");
const filePreview = document.querySelector("#file-preview");
const dropzone = document.querySelector("#dropzone");
const totalEl = document.querySelector("#checkout-total");
const btnSendPayment = document.querySelector("#btn-send-payment");
const refInput = document.querySelector("#input-ref");

let metodos = [];
let seleccionado = null;
let totalUsd = 0;
let cartItems = [];
let precios = [];
let plataformas = [];
let descuentos = [];
let cartId = null;
let tasaBs = null;
let precioTierLabel = "";
let userAcceso = null;
const TASA_MARKUP = 1.015; // +1.5%

const renderMetodos = () => {
  if (!metodosContainer) return;
  if (!metodos.length) {
    metodosContainer.innerHTML = '<p class="status">No hay métodos de pago.</p>';
    return;
  }
  metodosContainer.innerHTML = metodos
    .map(
      (m, idx) => `
      <button class="metodo-btn ${seleccionado === idx ? "selected" : ""}" data-index="${idx}">
        ${m.nombre || "Método"}
      </button>`
    )
    .join("");
};

const renderDetalle = () => {
  if (!metodoDetalle) return;
  if (seleccionado === null) {
    metodoDetalle.innerHTML = "";
    return;
  }
  const m = metodos[seleccionado];
  const campos = [
    { label: "Nombre", valor: m.nombre, copy: false },
    { label: "Correo", valor: m.correo, copy: true },
    { label: "ID", valor: m.id, copy: true },
    { label: "Cédula", valor: m.cedula, copy: false },
    { label: "Teléfono", valor: m.telefono, copy: false },
  ].filter((c) => c.valor !== null && c.valor !== undefined && c.valor !== "");

  const detalleHtml = campos
    .map((c) => {
      const safeVal = String(c.valor).replace(/"/g, "&quot;");
      const copyIcon = c.copy
        ? `<img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/copiar-portapapeles.png" alt="Copiar" class="copy-field-icon" data-copy="${safeVal}" style="width:14px; height:14px; margin-left:6px; cursor:pointer;" />`
        : "";
      return `<p><strong>${c.label}:</strong> <span>${c.valor}</span>${copyIcon}</p>`;
    })
    .join("");

  const isMetodoBs = Number(m.id_metodo_de_pago ?? m.id) === 1;
  metodoDetalle.innerHTML =
    detalleHtml +
    (isMetodoBs
      ? `<button type="button" class="btn-primary copy-detalle-btn" style="margin-top:8px; display:flex; align-items:center; justify-content:center; gap:8px; width:100%;">
          <span>Copiar al portapapeles</span>
          <img src="https://ojigtjcwhcrnawdbtqkl.supabase.co/storage/v1/object/public/public_assets/iconos/copiar-portapapeles.png" alt="Copiar" style="width:18px; height:18px; filter: brightness(0) invert(1);" />
        </button>`
      : "");

  if (isMetodoBs) {
    const btnCopy = metodoDetalle.querySelector(".copy-detalle-btn");
    btnCopy?.addEventListener("click", async () => {
      const text = campos.map((c) => `${c.valor}`).join("\n");
      try {
        if (navigator?.clipboard?.writeText) {
          await navigator.clipboard.writeText(text);
        } else {
          const textarea = document.createElement("textarea");
          textarea.value = text;
          document.body.appendChild(textarea);
          textarea.select();
          document.execCommand("copy");
          document.body.removeChild(textarea);
        }
        btnCopy.textContent = "Copiado!";
        setTimeout(() => (btnCopy.textContent = "Copiar al portapapeles"), 1500);
      } catch (err) {
        console.error("copy detalle error", err);
      }
    });
  }

  // Copy individual fields (correo, id)
  metodoDetalle.querySelectorAll(".copy-field-icon").forEach((icon) => {
    icon.addEventListener("click", async () => {
      const val = icon.dataset.copy || "";
      const decoded = val.replace(/&quot;/g, '"');
      try {
        if (navigator?.clipboard?.writeText) {
          await navigator.clipboard.writeText(decoded);
        } else {
          const textarea = document.createElement("textarea");
          textarea.value = decoded;
          document.body.appendChild(textarea);
          textarea.select();
          document.execCommand("copy");
          document.body.removeChild(textarea);
        }
        icon.style.opacity = "0.6";
        setTimeout(() => {
          icon.style.opacity = "1";
        }, 800);
      } catch (err) {
        console.error("copy field error", err);
      }
    });
  });
};

const populateSelect = (defaultIdx = null) => {
  if (!metodoSelect) return;
  metodoSelect.innerHTML = '<option value="">Seleccione un método</option>';
  metodos.forEach((m, idx) => {
    const opt = document.createElement("option");
    opt.value = String(idx);
    opt.textContent = m.nombre || `Método ${idx + 1}`;
    metodoSelect.appendChild(opt);
  });
  const selIdx = defaultIdx !== null ? defaultIdx : seleccionado;
  if (selIdx !== null && selIdx >= 0) {
    metodoSelect.value = String(selIdx);
  }
};

const updateSelection = (idx) => {
  if (idx === null || idx < 0 || idx >= metodos.length) {
    seleccionado = null;
  } else {
    seleccionado = idx;
  }
  renderMetodos();
  renderDetalle();
  if (metodoSelect && seleccionado !== null) {
    metodoSelect.value = String(seleccionado);
  }
  renderTotal();
};

metodosContainer?.addEventListener("click", (e) => {
  const btn = e.target.closest(".metodo-btn");
  if (!btn) return;
  const idx = Number(btn.dataset.index);
  if (Number.isNaN(idx)) return;
  updateSelection(seleccionado === idx ? null : idx);
});

metodoSelect?.addEventListener("change", (e) => {
  const idx = Number(e.target.value);
  if (Number.isNaN(idx)) {
    updateSelection(null);
  } else {
    updateSelection(idx);
  }
});

btnAddImage?.addEventListener("click", () => {
  inputFiles?.click();
});

inputFiles?.addEventListener("change", () => {
  if (!filePreview) return;
  const files = Array.from(inputFiles.files || []).filter((f) =>
    f.type?.startsWith("image/")
  );
  if (!files.length) {
    filePreview.innerHTML = "";
    inputFiles.value = "";
    return;
  }
  const file = files[0];
  const reader = new FileReader();
  reader.onload = () => {
    filePreview.innerHTML = `<img src="${reader.result}" alt="${file.name}" />`;
  };
  reader.readAsDataURL(file);
  // keep only one file
  const dt = new DataTransfer();
  dt.items.add(file);
  inputFiles.files = dt.files;
});

// Drag and drop visual feedback
["dragenter", "dragover"].forEach((eventName) => {
  dropzone?.addEventListener(eventName, (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropzone.classList.add("drag-over");
  });
});

["dragleave", "drop"].forEach((eventName) => {
  dropzone?.addEventListener(eventName, (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropzone.classList.remove("drag-over");
  });
});

dropzone?.addEventListener("drop", (e) => {
  e.preventDefault();
  const files = Array.from(e.dataTransfer?.files || []).filter((f) =>
    f.type?.startsWith("image/")
  );
  if (!files.length) {
    dropzone.classList.remove("drag-over");
    return;
  }
  const file = files[0];
  const dt = new DataTransfer();
  dt.items.add(file);
  inputFiles.files = dt.files;
  filePreview.innerHTML = "";
  const reader = new FileReader();
  reader.onload = () => {
    filePreview.innerHTML = `<img src="${reader.result}" alt="${file.name}" />`;
  };
  reader.readAsDataURL(file);
  dropzone.classList.remove("drag-over");
});

const renderTotal = () => {
  if (!totalEl) return;
  const metodo = seleccionado !== null ? metodos[seleccionado] : null;
  const metodoId = metodo?.id_metodo_de_pago ?? metodo?.id;
  const isBs = metodo && Number(metodoId) === 1;
  const tasaVal = Number.isFinite(tasaBs) ? tasaBs : null;
  const lineUsd = `Total: $${totalUsd.toFixed(2)} ${precioTierLabel ? `(${precioTierLabel})` : ""}`;
  const lineBs =
    isBs && tasaVal ? `<div>Bs. ${(totalUsd * tasaVal).toFixed(2)}</div>` : "";
  totalEl.innerHTML = `<div>${lineUsd}</div>${lineBs}`;
};

const calcularTotalRpc = async (id_carrito) => {
  if (!id_carrito) return null;
  try {
    const { data, error } = await supabase.rpc("calcular_total_carrito", { p_id_carrito: id_carrito });
    if (error) {
      console.error("rpc calcular_total_carrito error", error);
      return null;
    }
    const total = Number(data);
    return Number.isFinite(total) ? total : null;
  } catch (err) {
    console.error("rpc calcular_total_carrito catch", err);
    return null;
  }
};

const calcularTotalTier = (items = [], preciosMap = {}, acceso = null) => {
  if (!items.length) return { total: 0, label: "" };
  const useMayor = acceso === false;
  const label = useMayor ? "Precio mayor" : "Precio detal";
  const total = items.reduce((sum, it) => {
    const price = preciosMap[it.id_precio] || {};
    const unit = useMayor
      ? Number(price.precio_usd_mayor) || Number(price.precio_usd_detal) || 0
      : Number(price.precio_usd_detal) || 0;
    const qty = Number(it.cantidad) || 0;
    const meses = Number(it.meses) || 1;
    return sum + unit * qty * meses;
  }, 0);
  return { total, label };
};

async function init() {
  try {
    const [cartData, catalog, metodosResp, tasaResp, user] = await Promise.all([
      fetchCart(),
      loadCatalog(),
      supabase.from("metodos_de_pago").select("id_metodo_de_pago, nombre, correo, id, cedula, telefono"),
      fetchP2PRate(),
      loadCurrentUser(),
    ]);
    if (metodosResp.error) throw metodosResp.error;
    metodos = metodosResp.data || [];
    tasaBs = tasaResp ? tasaResp * TASA_MARKUP : null;
    cartItems = cartData.items || [];
    cartId = cartData.id_carrito || null;
    precios = catalog.precios;
    plataformas = catalog.plataformas;
    descuentos = catalog.descuentos || [];
    userAcceso = user?.acceso_cliente;
    const preciosMap = (precios || []).reduce((acc, p) => {
      acc[p.id_precio] = p;
      return acc;
    }, {});
    const tierTotal = calcularTotalTier(cartItems, preciosMap, userAcceso);
    const rpcTotal = await calcularTotalRpc(cartId);
    totalUsd = tierTotal.total || rpcTotal || 0;
    precioTierLabel = tierTotal.label;

    // Selecciona por defecto el método con id 1 si existe
    // Prefill de pruebas: seleccionar índice 5 si existe, si no cae al método id 1
    let idxDefault = metodos.findIndex(
      (m) => Number(m.id_metodo_de_pago ?? m.id) === 1
    );
    if (idxDefault < 0 && metodos.length) idxDefault = 0;
    if (idxDefault >= 0) {
      seleccionado = idxDefault;
    }

    populateSelect(idxDefault >= 0 ? idxDefault : null);
    renderMetodos();
    renderDetalle();
    renderTotal();
  } catch (err) {
    console.error("checkout load error", err);
    if (metodosContainer) {
      metodosContainer.innerHTML = '<p class="status">No se pudieron cargar los métodos de pago.</p>';
    }
    renderTotal();
  }
}

init();

const uploadFiles = async () => {
  const files = Array.from(inputFiles.files || []);
  if (!files.length) return [];
  const resp = await uploadComprobantes(files);
  if (resp?.error) throw new Error(resp.error);
  return resp?.urls || [];
};

btnSendPayment?.addEventListener("click", async () => {
  // Recalcula totales con datos frescos del carrito
  try {
    const cartData = await fetchCart();
    cartItems = cartData.items || [];
    cartId = cartData.id_carrito || null;
    const preciosMap = (precios || []).reduce((acc, p) => {
      acc[p.id_precio] = p;
      return acc;
    }, {});
    const tierTotal = calcularTotalTier(cartItems, preciosMap, userAcceso);
    const rpcTotal = await calcularTotalRpc(cartId);
    totalUsd = tierTotal.total || rpcTotal || 0;
    precioTierLabel = tierTotal.label;
    renderTotal();
  } catch (err) {
    console.error("recalc checkout error", err);
  }
  // Forzamos método 1 por pruebas si existe
  if (seleccionado === null) {
    const idxDefault = metodos.findIndex(
      (m) => Number(m.id_metodo_de_pago ?? m.id) === 1
    );
    if (idxDefault >= 0) {
      seleccionado = idxDefault;
      if (metodoSelect) metodoSelect.value = String(idxDefault);
      renderMetodos();
      renderDetalle();
    }
  }
  if (seleccionado === null) {
    alert("Selecciona un método de pago.");
    metodoSelect?.classList.add("input-error");
    return;
  }
  if (!refInput?.value.trim()) {
    alert("Ingresa la referencia.");
    refInput.classList.add("input-error");
    return;
  }
  if (!inputFiles?.files?.length) {
    alert("Adjunta comprobantes de pago.");
    dropzone?.classList.add("input-error");
    return;
  }
  if (!cartItems.length) {
    alert("No hay items en el carrito.");
    return;
  }
  try {
    const comprobantes = await uploadFiles();
    const metodo = metodos[seleccionado];
    const payload = {
      id_metodo_de_pago: metodo.id_metodo_de_pago ?? metodo.id,
      referencia: refInput.value.trim(),
      comprobantes,
      total: totalUsd,
      tasa_bs: Number.isFinite(tasaBs) ? tasaBs : null,
    };
    const resp = await submitCheckout(payload);
    if (resp?.error) {
      alert(`Error en checkout: ${resp.error}`);
      return;
    }
    // Marcar notificación de inventario para el usuario en sesión
    try {
      const userId = requireSession();
      await supabase.from("usuarios").update({ notificacion_inventario: true }).eq("id_usuario", userId);
    } catch (flagErr) {
      console.error("update notificacion_inventario error", flagErr);
    }
    alert("Pago enviado correctamente.");
    window.location.href = "entregar_servicios.html";
  } catch (err) {
    console.error("checkout submit error", err);
    alert("No se pudo enviar el pago. Intenta de nuevo.");
  }
});

metodoSelect?.addEventListener("focus", () => metodoSelect.classList.remove("input-error"));
refInput?.addEventListener("focus", () => refInput.classList.remove("input-error"));
dropzone?.addEventListener("click", () => dropzone.classList.remove("input-error"));
