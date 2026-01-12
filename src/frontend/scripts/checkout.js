import { supabase, fetchCart, loadCatalog, submitCheckout, uploadComprobantes } from "./api.js";
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
// Prefills para pruebas
if (refInput) refInput.value = "1111";

let metodos = [];
let seleccionado = null;
let totalUsd = 0;
let cartItems = [];
let precios = [];
let plataformas = [];
let descuentos = [];
let cartId = null;

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
    { label: "Nombre", valor: m.nombre },
    { label: "Correo", valor: m.correo },
    { label: "ID", valor: m.id },
    { label: "Cédula", valor: m.cedula },
    { label: "Teléfono", valor: m.telefono },
  ].filter((c) => c.valor !== null && c.valor !== undefined && c.valor !== "");

  metodoDetalle.innerHTML = campos
    .map((c) => `<p><strong>${c.label}:</strong> ${c.valor}</p>`)
    .join("");
};

const populateSelect = () => {
  if (!metodoSelect) return;
  metodoSelect.innerHTML = '<option value="">Seleccione un método</option>';
  metodos.forEach((m, idx) => {
    const opt = document.createElement("option");
    opt.value = String(idx);
    opt.textContent = m.nombre || `Método ${idx + 1}`;
    metodoSelect.appendChild(opt);
  });
  // Prefill de pruebas: seleccionar índice 5 si existe
  if (metodos.length > 5) {
    metodoSelect.value = "5";
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
  const lineUsd = `Total: $${totalUsd.toFixed(2)}`;
  const metodo = seleccionado !== null ? metodos[seleccionado] : null;
  const metodoId = metodo?.id_metodo_de_pago ?? metodo?.id;
  const isBs = metodo && Number(metodoId) === 1;
  const lineBs = isBs ? `<div>Bs. ${(totalUsd * 400).toFixed(2)}</div>` : "";
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

async function init() {
  try {
    const [cartData, catalog, metodosResp] = await Promise.all([
      fetchCart(),
      loadCatalog(),
      supabase.from("metodos_de_pago").select("id_metodo_de_pago, nombre, correo, id, cedula, telefono"),
    ]);
    if (metodosResp.error) throw metodosResp.error;
    metodos = metodosResp.data || [];
    cartItems = cartData.items || [];
    cartId = cartData.id_carrito || null;
    precios = catalog.precios;
    plataformas = catalog.plataformas;
    descuentos = catalog.descuentos || [];
    const rpcTotal = await calcularTotalRpc(cartId);
    totalUsd = rpcTotal ?? 0;

    // Selecciona por defecto el método con id 1 si existe
    // Prefill de pruebas: seleccionar índice 5 si existe, si no cae al método id 1
    if (metodos.length > 5) {
      seleccionado = 5;
      if (metodoSelect) metodoSelect.value = "5";
    } else {
      const idxDefault = metodos.findIndex(
        (m) => Number(m.id_metodo_de_pago ?? m.id) === 1
      );
      if (idxDefault >= 0) {
        seleccionado = idxDefault;
        if (metodoSelect) metodoSelect.value = String(idxDefault);
      }
    }

    renderMetodos();
    renderDetalle();
    populateSelect();
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
    const rpcTotal = await calcularTotalRpc(cartId);
    totalUsd = rpcTotal ?? 0;
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
      tasa_bs: 400,
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
    window.location.href = "index.html";
  } catch (err) {
    console.error("checkout submit error", err);
    alert("No se pudo enviar el pago. Intenta de nuevo.");
  }
});

metodoSelect?.addEventListener("focus", () => metodoSelect.classList.remove("input-error"));
refInput?.addEventListener("focus", () => refInput.classList.remove("input-error"));
dropzone?.addEventListener("click", () => dropzone.classList.remove("input-error"));
