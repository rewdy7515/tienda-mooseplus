import {
  requireSession,
  attachLogout,
  setSessionRoles,
  getSessionRoles,
  attachLogoHome,
} from "./session.js";
import { clearServerSession, loadCurrentUser, supabase, ensureServerSession } from "./api.js";
import { formatDDMMYYYY } from "./date-format.js";

requireSession();

const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const statusEl = document.querySelector("#revisar-status");
const filtrosEl = document.querySelector("#plataformas-filtros");
const bodyEl = document.querySelector("#reportes-body");
const modal = document.querySelector("#modal-detalle");
const modalPlatTitle = document.querySelector("#modal-plat-title");
const modalCorreo = document.querySelector("#modal-correo");
const modalClave = document.querySelector("#modal-clave");
const modalMotivo = document.querySelector("#modal-motivo");
const modalPerfil = document.querySelector("#modal-perfil");
const modalPin = document.querySelector("#modal-pin");
const perfilRows = document.querySelectorAll(".perfil-row");
const modalImagenWrapper = document.querySelector("#modal-imagen-wrapper");
const modalImagen = document.querySelector("#modal-imagen");
const modalSinImagen = document.querySelector("#modal-sin-imagen");
const modalResumen = document.querySelector("#modal-resumen");
const resumenClaveRow = document.querySelector("#resumen-clave");
const resumenClaveText = document.querySelector("#resumen-clave-text");
const resumenPinRow = document.querySelector("#resumen-pin");
const resumenPinText = document.querySelector("#resumen-pin-text");
const btnGuardarCampos = document.querySelector("#btn-guardar-campos");
const btnCerrarReporte = document.querySelector("#btn-cerrar-reporte");
const btnFaltanRecaudos = document.querySelector("#btn-faltan-recaudos");
const btnReemplazar = document.querySelector("#btn-reemplazar");
const modalResumenClose = document.querySelector(".modal-resumen-close");
const checkOtro = document.querySelector("#check-otro");
const resumenOtroText = document.querySelector("#resumen-otro-text");
const checkSuscripcion = document.querySelector("#check-suscripcion");
const checkPerfiles = document.querySelector("#check-perfiles");
const checkIngreso = document.querySelector("#check-ingreso");
const checkPinSame = document.querySelector("#check-pin-same");
const modalReemplazoConfirm = document.querySelector("#modal-reemplazo-confirm");
const selectRazonReemplazo = document.querySelector("#select-razon-reemplazo");
const btnConfirmarReemplazo = document.querySelector("#btn-confirmar-reemplazo");
const btnCancelarReemplazo = document.querySelector("#btn-cancelar-reemplazo");

let currentRow = null;
let oldClave = "";
let oldPin = "";
let cambioClave = false;
let cambioPin = false;
let pendingReplacement = null;

const formatDate = (iso) => formatDDMMYYYY(iso) || "-";

const getDescripcion = (row) => {
  return row.descripcion || "Otro...";
};

async function getImageUrl(path) {
  if (!path) return null;
  // Si viene como URL de Supabase, intenta firmarla; si falla, usa la original
  if (/^https?:\/\//i.test(path)) {
    try {
      const url = new URL(path);
      const match = url.pathname.match(/storage\/v1\/object\/(?:public|sign)\/([^/]+)\/(.+)/);
      if (match && match[1] && match[2]) {
        const bucket = match[1];
        const objectPath = match[2];
        const { data, error } = await supabase.storage.from(bucket).createSignedUrl(objectPath, 3600);
        if (!error && data?.signedUrl) return data.signedUrl;
      }
      return path;
    } catch (_err) {
      return path;
    }
  }
  try {
    const { data, error } = await supabase.storage.from("private_assets").createSignedUrl(path, 3600);
    if (error) {
      console.error("signed url error", error);
      return null;
    }
    return data?.signedUrl || null;
  } catch (err) {
    console.error("signed url error", err);
    return null;
  }
}

const renderTable = (items = []) => {
  if (!bodyEl) return;
  if (!items.length) {
    bodyEl.innerHTML = `<tr><td colspan="6" class="status">No hay reportes pendientes para esta plataforma.</td></tr>`;
    return;
  }
  bodyEl.innerHTML = items
    .map((r) => {
      const idFmt = r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-";
      const cliente = [r.usuarios?.nombre, r.usuarios?.apellido].filter(Boolean).join(" ").trim() || "-";
      const correo = r.cuentas?.correo || "-";
      const motivo = getDescripcion(r);
      const fecha = formatDate(r.created_at || r.fecha || null);
      return `
        <tr>
          <td>${idFmt}</td>
          <td>${cliente}</td>
          <td>${correo}</td>
          <td>${motivo}</td>
          <td>${fecha}</td>
          <td>
            <div class="actions-inline">
              <button class="btn-outline btn-small" data-id="${r.id_reporte}" data-action="detalle">Más detalles</button>
              <button class="btn-primary btn-small" data-id="${r.id_reporte}" data-action="cerrar">Cerrar reporte</button>
            </div>
          </td>
        </tr>
      `;
    })
    .join("");
};

const renderFiltros = (plataformas = [], onSelect) => {
  if (!filtrosEl) return;
  if (!plataformas.length) {
    filtrosEl.innerHTML = "";
    return;
  }
  filtrosEl.innerHTML = plataformas
    .map(
      (p, idx) =>
        `<button class="btn-outline admin-action" data-plat="${p.id}" data-idx="${idx}">${p.nombre || "Plataforma"}</button>`
    )
    .join("");
  filtrosEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-plat]");
    if (!btn) return;
    onSelect?.(btn.dataset.plat);
  });
};

async function loadReportes() {
  const { data, error } = await supabase
    .from("reportes")
    .select(
      "id_reporte,id_plataforma,plataformas(nombre),id_usuario,usuarios(nombre,apellido),id_cuenta,cuentas(id_cuenta,correo,clave,id_plataforma,venta_perfil,venta_miembro),id_perfil,perfiles(id_perfil,n_perfil,pin,perfil_hogar,id_cuenta),descripcion,imagen,en_revision,solucionado"
    )
    .eq("solucionado", false);
  if (error) throw error;
  return data || [];
}

function closeModal() {
  modal?.classList.add("hidden");
  modalImagenWrapper?.classList.add("hidden");
  modalSinImagen?.classList.add("hidden");
  if (modalImagen) modalImagen.src = "";
  currentRow = null;
  cambioClave = false;
  cambioPin = false;
}

function closeReemplazoModal() {
  modalReemplazoConfirm?.classList.add("hidden");
  if (selectRazonReemplazo) selectRazonReemplazo.value = "";
  pendingReplacement = null;
}

async function openModal(row) {
  if (!modal) return;
  const platName = row.plataformas?.nombre || "-";
  modalPlatTitle.textContent = platName;
  modalCorreo.textContent = row.cuentas?.correo || "-";
  oldClave = row.cuentas?.clave || "";
  const rawPin = row.perfiles?.pin ?? row.pin ?? "";
  oldPin = rawPin === null || rawPin === undefined ? "" : String(rawPin);
  if (modalClave) modalClave.value = oldClave;
  const perfilDatos = row.perfiles || {};
  const showPerfil = !!row.id_perfil;
  perfilRows.forEach((el) => el.classList.toggle("hidden", !showPerfil));
  if (showPerfil) {
    modalPerfil.textContent = perfilDatos.n_perfil ? `Perfil ${perfilDatos.n_perfil}` : (perfilDatos.id_perfil || row.id_perfil || "-");
    if (modalPin) modalPin.value = oldPin;
  } else {
    modalPerfil.textContent = "";
    if (modalPin) modalPin.value = "";
  }
  modalMotivo.textContent = getDescripcion(row);

  if (row.imagen) {
    const url = await getImageUrl(row.imagen);
    if (url && modalImagen) {
      modalImagen.src = url;
      modalImagenWrapper?.classList.remove("hidden");
      modalSinImagen?.classList.add("hidden");
      modalImagen.onload = () => {
        modalImagenWrapper?.classList.remove("hidden");
        modalSinImagen?.classList.add("hidden");
      };
      modalImagen.onerror = () => {
        modalImagenWrapper?.classList.add("hidden");
        modalSinImagen?.classList.remove("hidden");
      };
    } else {
      modalImagenWrapper?.classList.add("hidden");
      modalSinImagen?.classList.remove("hidden");
    }
  } else {
    modalImagenWrapper?.classList.add("hidden");
    modalSinImagen?.classList.remove("hidden");
  }
  modal.classList.remove("hidden");
  currentRow = row;
  cambioClave = false;
  cambioPin = false;
}

async function init() {
  try {
    const userId = requireSession();
    await ensureServerSession();
    const user = await loadCurrentUser();
    setSessionRoles(user || {});
    if (user && usernameEl) {
      const fullName = [user.nombre, user.apellido].filter(Boolean).join(" ").trim();
      usernameEl.textContent = fullName || user.correo || "Usuario";
    }
    const sessionRoles = getSessionRoles();
    const isAdmin =
      isTrue(sessionRoles?.permiso_admin) ||
      isTrue(sessionRoles?.permiso_superadmin) ||
      isTrue(user?.permiso_admin) ||
      isTrue(user?.permiso_superadmin);
    if (adminLink) {
      adminLink.classList.toggle("hidden", !isAdmin);
      adminLink.style.display = isAdmin ? "block" : "none";
    }
    attachLogoHome();

    const all = await loadReportes();
    const activos = (all || []).filter((r) => r.en_revision !== false && r.solucionado === false);
    if (!activos.length) {
      if (statusEl) statusEl.textContent = "No hay reportes pendientes.";
      renderTable([]);
      return;
    }

    const porPlat = new Map();
    activos.forEach((r) => {
      const id = r.id_plataforma || r.cuentas?.id_plataforma || r.plataformas?.id_plataforma;
      const nombre = r.plataformas?.nombre || "Plataforma";
      if (!id) return;
      if (!porPlat.has(id)) porPlat.set(id, { id, nombre, items: [] });
      porPlat.get(id).items.push(r);
    });

    const plataformas = Array.from(porPlat.values());
    renderFiltros(plataformas, (platId) => {
      const selected = porPlat.get(Number(platId)) || porPlat.get(platId);
      renderTable(selected?.items || []);
      if (statusEl) statusEl.textContent = "";
    });

    // preselect first
    const first = plataformas[0];
    if (first) {
      renderTable(first.items || []);
      if (statusEl) statusEl.textContent = "";
    }

    bodyEl?.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-action]");
      if (!btn) return;
      const id = btn.dataset.id;
      if (!id) return;
      const allItems = Array.from(porPlat.values()).flatMap((p) => p.items || []);
      const row = allItems.find((r) => String(r.id_reporte) === String(id));
      if (!row) return;
      const action = btn.dataset.action;
      if (action === "detalle") {
        openModal(row);
      }
      // acción de cerrar reporte no implementada aquí
    });

    modal?.addEventListener("click", (e) => {
      if (e.target.classList.contains("modal-backdrop") || e.target.classList.contains("modal-close")) {
        closeModal();
      }
    });

  btnCerrarReporte?.addEventListener("click", () => {
    if (!currentRow) return;
    const nuevaClave = modalClave?.value || "";
    const nuevoPin = (modalPin?.value || "").trim();
    const claveCambia = nuevaClave !== oldClave;
    const pinCambia = !!currentRow.id_perfil && nuevoPin !== oldPin;
    cambioClave = claveCambia;
    cambioPin = pinCambia;

    if (resumenClaveRow) resumenClaveRow.classList.toggle("hidden", !claveCambia);
    if (resumenClaveText)
      resumenClaveText.textContent = claveCambia ? `${oldClave || "(vacía)"} -> ${nuevaClave || "(vacía)"}` : "";
      const showPin = pinCambia;
      if (resumenPinRow) resumenPinRow.classList.toggle("hidden", !showPin);
      if (resumenPinText)
        resumenPinText.textContent = showPin ? `${oldPin || "(vacío)"} -> ${nuevoPin || "(vacío)"}` : "";

      modalResumen?.classList.remove("hidden");
    });

    modalResumen?.addEventListener("click", (e) => {
      if (e.target.classList.contains("modal-backdrop") || e.target.classList.contains("modal-close") || e.target.classList.contains("modal-resumen-close")) {
        modalResumen.classList.add("hidden");
      }
  });

  modalResumenClose?.addEventListener("click", () => modalResumen?.classList.add("hidden"));

  btnGuardarCampos?.addEventListener("click", () => {
    guardarCambios();
  });

  btnFaltanRecaudos?.addEventListener("click", () => {
    // Placeholder para futura lógica
    alert("Marca los recaudos pendientes.");
  });

  checkOtro?.addEventListener("change", () => {
    const show = checkOtro.checked;
    if (resumenOtroText) {
      resumenOtroText.classList.toggle("hidden", !show);
      if (!show) resumenOtroText.value = "";
    }
  });

  btnReemplazar?.addEventListener("click", () => {
    if (!currentRow) {
      alert("Selecciona un reporte.");
      return;
    }
    const reporteCuentaId = currentRow.id_cuenta;
    const reportePerfilId = currentRow.id_perfil ?? null;
    const reportePlatId = currentRow.id_plataforma ?? currentRow.cuentas?.id_plataforma ?? currentRow.plataformas?.id_plataforma;
    reemplazarServicio({ id_cuenta: reporteCuentaId, id_perfil: reportePerfilId, id_plataforma: reportePlatId });
  });

  modalReemplazoConfirm?.addEventListener("click", (e) => {
    if (e.target.classList.contains("modal-backdrop") || e.target.classList.contains("modal-close")) {
      closeReemplazoModal();
    }
  });

  btnCancelarReemplazo?.addEventListener("click", () => {
    closeReemplazoModal();
  });

  btnConfirmarReemplazo?.addEventListener("click", () => {
    confirmarReemplazo();
  });
} catch (err) {
    console.error("revisar reportes error", err);
    if (statusEl) statusEl.textContent = "No se pudieron cargar los reportes.";
  }
}

init();
attachLogout(clearServerSession);

async function guardarCambios() {
  if (!currentRow) return;
  const btn = btnGuardarCampos;
  if (btn) btn.disabled = true;
  try {
    const id_usuario = requireSession();
    const nuevaClave = modalClave?.value || "";
    const nuevoPin = (modalPin?.value || "").trim();
    const updates = [];

    // Actualiza clave de cuenta si cambió
    if (nuevaClave !== oldClave) {
      updates.push(
        supabase.from("cuentas").update({ clave: nuevaClave }).eq("id_cuenta", currentRow.id_cuenta)
      );
    }

    // Actualiza pin si aplica y cambió
    if (currentRow.id_perfil && nuevoPin !== oldPin) {
      const pinNum = nuevoPin === "" ? null : Number(nuevoPin);
      updates.push(
        supabase.from("perfiles").update({ pin: pinNum }).eq("id_perfil", currentRow.id_perfil)
      );
    }

    if (updates.length) {
      const results = await Promise.all(updates);
      const err = results.find((r) => r.error);
      if (err?.error) throw err.error;
    }

    // Construir descripcion/descripción_solucion con cambios primero
    const textos = [];
    if (cambioClave) textos.push("Se actualizó la contraseña");
    if (cambioPin) textos.push("Se actualizó el pin");
    if (checkSuscripcion?.checked) textos.push("Suscripción activada");
    if (checkPerfiles?.checked) textos.push("Perfiles modificados");
    if (checkIngreso?.checked) textos.push("Se pudo ingresar sin problemas con los datos de la cuenta");
    if (checkPinSame?.checked) textos.push("Se volvió a poner el mismo pin al perfil");
    if (checkOtro?.checked && resumenOtroText) {
      const extra = resumenOtroText.value.trim();
      if (extra) textos.push(extra);
    }
    const descripcion_solucion = textos.join("; ");

    const { error } = await supabase
      .from("reportes")
      .update({
        descripcion: descripcion_solucion || null,
        descripcion_solucion,
        en_revision: false,
        solucionado: true,
        solucionado_por: id_usuario,
      })
      .eq("id_reporte", currentRow.id_reporte);
    if (error) throw error;

    alert("Campos guardados y reporte cerrado.");
    modalResumen?.classList.add("hidden");
    closeModal();
  } catch (err) {
    console.error("guardar cambios error", err);
    alert("No se pudieron guardar los cambios.");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function reemplazarServicio(reporteDatos = {}) {
  if (!currentRow) return alert("Selecciona un reporte.");
  const id_usuario = requireSession();
  await ensureServerSession();
  const plataformaId =
    reporteDatos.id_plataforma ??
    currentRow.id_plataforma ??
    currentRow.cuentas?.id_plataforma ??
    currentRow.plataformas?.id_plataforma;
  const cuentaId = reporteDatos.id_cuenta ?? currentRow.id_cuenta;
  const perfilId = reporteDatos.id_perfil ?? currentRow.id_perfil;
  console.log("reemplazarServicio init", { plataformaId, cuentaId, perfilId, currentRow });
  if (!plataformaId || !cuentaId) {
    alert("Faltan datos de plataforma o cuenta.");
    return;
  }

  const cuentaInfo = currentRow.cuentas || {};
  const ventaPerfil = !!cuentaInfo.venta_perfil;
  const ventaMiembro = !!cuentaInfo.venta_miembro;
  const esCuentaCompleta = !ventaPerfil && !ventaMiembro;
  let perfilHogar = currentRow.perfiles?.perfil_hogar === true;
  if (perfilId) {
    const { data: perfData, error: perfErr } = await supabase
      .from("perfiles")
      .select("perfil_hogar")
      .eq("id_perfil", perfilId)
      .maybeSingle();
    if (perfErr) {
      console.error("perfil_hogar lookup error", perfErr);
    } else if (perfData && typeof perfData.perfil_hogar === "boolean") {
      perfilHogar = perfData.perfil_hogar;
    }
  }

  // obtiene venta asociada
  let ventaErr = null;
  let ventasData = [];
  if (perfilId) {
    const { data, error } = await supabase
      .from("ventas")
      .select("id_venta")
      .eq("id_cuenta", cuentaId)
      .eq("id_perfil", perfilId)
      .limit(1);
    ventasData = data || [];
    ventaErr = error;
    if (!ventasData.length) {
      const { data: fallbackData, error: fallbackErr } = await supabase
        .from("ventas")
        .select("id_venta")
        .eq("id_cuenta", cuentaId)
        .is("id_perfil", null)
        .limit(1);
      ventasData = fallbackData || [];
      ventaErr = fallbackErr;
      if (!ventasData.length) {
        const { data: cuentaSolo, error: cuentaErr } = await supabase
          .from("ventas")
          .select("id_venta")
          .eq("id_cuenta", cuentaId)
          .limit(1);
        ventasData = cuentaSolo || [];
        ventaErr = cuentaErr;
      }
    }
  } else {
    const { data, error } = await supabase
      .from("ventas")
      .select("id_venta")
      .eq("id_cuenta", cuentaId)
      .is("id_perfil", null)
      .limit(1);
    ventasData = data || [];
    ventaErr = error;
    if (!ventasData.length) {
      const { data: cuentaSolo, error: cuentaErr } = await supabase
        .from("ventas")
        .select("id_venta")
        .eq("id_cuenta", cuentaId)
        .limit(1);
      ventasData = cuentaSolo || [];
      ventaErr = cuentaErr;
    }
  }
  console.log("venta asociada", { ventasData, ventaErr });
  if (ventaErr || !ventasData?.length) {
    alert("No se encontró la venta asociada.");
    return;
  }
  const idVenta = ventasData[0].id_venta;

  // helpers
  const findPerfilLibre = async (opts = {}) => {
    const { requireHogar = null, excludeCuenta, requireVentaPerfil = null } = opts;
    if (!excludeCuenta) {
      console.warn("findPerfilLibre sin excludeCuenta, se usará la cuenta reportada para excluir");
    }
    // perfiles que ya están en reemplazos (no reutilizar)
    let excluidos = [];
    try {
      const { data: repData } = await supabase.from("reemplazos").select("id_perfil").not("id_perfil", "is", null);
      excluidos = (repData || []).map((r) => r.id_perfil);
    } catch (err) {
      console.error("reemplazos lookup error", err);
    }
    let query = supabase
      .from("perfiles")
      .select("id_perfil, perfil_hogar, id_cuenta, cuentas!inner(id_plataforma, venta_perfil, venta_miembro, inactiva)")
      .eq("cuentas.id_plataforma", plataformaId)
      .or("ocupado.is.null,ocupado.eq.false")
      .or("inactiva.is.null,inactiva.eq.false", { foreignTable: "cuentas" })
      .order("id_perfil", { ascending: true })
      .limit(1);
    if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
    if (requireHogar !== null) query = query.eq("perfil_hogar", requireHogar);
    const { data, error } = await query;
    if (error) return { error };
    let rows = (data || []).filter((r) => r.id_cuenta !== excludeCuenta);
    if (excluidos.length) {
      rows = rows.filter((r) => !excluidos.includes(r.id_perfil));
    }
    if (requireVentaPerfil !== null) {
      rows = rows.filter((r) => r.cuentas?.venta_perfil === requireVentaPerfil);
    }
    const disponibles = rows.map((r) => r.id_perfil);
    console.log("findPerfilLibre", { opts, disponibles, rows, plataformaId, excludeCuenta });
    if (!disponibles.length) {
      console.warn("findPerfilLibre sin resultados", { opts, plataformaId, excludeCuenta });
    }
    return { data: rows[0] || null, disponibles };
  };

  const findSubCuentaLibre = async (_excludeCuenta) => {
    return { data: null, error: null };
  };

  const findCuentaCompleta = async (excludeCuenta) => {
    let query = supabase
      .from("cuentas")
      .select("id_cuenta, inactiva")
      .eq("id_plataforma", plataformaId)
      .eq("venta_perfil", false)
      .eq("venta_miembro", false)
      .or("ocupado.is.null,ocupado.eq.false")
      .or("inactiva.is.null,inactiva.eq.false")
      .order("id_cuenta", { ascending: true })
      .limit(1);
    if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
    const { data, error } = await query;
    const disponibles = (data || []).map((c) => c.id_cuenta);
    console.log("findCuentaCompleta", { excludeCuenta, data, error, disponibles });
    if (!disponibles.length) {
      console.warn("findCuentaCompleta sin resultados", { excludeCuenta, plataformaId });
    }
    if (error) return { error };
    return { data: data?.[0] || null, disponibles };
  };

  let nuevoCuenta = null;
  let nuevoPerfil = null;
  let nuevaSubCuenta = null;

  // lógica de selección
  if (esCuentaCompleta) {
    const { data: cuentaLibre, error, disponibles = [] } = await findCuentaCompleta(cuentaId);
    console.log("ruta cuenta completa", { cuentaLibre, error, disponibles });
    if (error) {
      alert("Error buscando cuenta libre.");
      return;
    }
    if (!cuentaLibre) {
      alert("Sin stock");
      return;
    }
    nuevoCuenta = cuentaLibre.id_cuenta;
    nuevoPerfil = null;
    nuevaSubCuenta = null;
  } else if (perfilId) {
    if (perfilHogar) {
      const { data: perfLibre, error, disponibles = [] } = await findPerfilLibre({
        requireHogar: true,
        excludeCuenta: cuentaId,
      });
      console.log("perfiles libres hogar", disponibles);
      if (error) {
        alert("Error buscando perfil libre.");
        return;
      }
      if (!perfLibre) {
        const { data: subLibre, error: subErr } = await findSubCuentaLibre(cuentaId);
        console.log("perfil hogar sin perfil libre, buscando subCuenta", { subLibre, subErr });
        if (subErr) {
          alert("Error buscando sub cuenta.");
          return;
        }
        if (!subLibre) {
          console.warn("Sin stock hogar", { disponibles });
          alert("Sin stock");
          return;
        }
        nuevaSubCuenta = subLibre.id_sub_cuenta;
        nuevoCuenta = subLibre.id_cuenta;
      } else {
        nuevoPerfil = perfLibre.id_perfil;
        nuevoCuenta = perfLibre.id_cuenta;
        console.log("perfil disponible (hogar)", nuevoPerfil);
      }
    } else {
      const { data: perfLibre, error, disponibles = [] } = await findPerfilLibre({
        requireHogar: false,
        excludeCuenta: cuentaId,
      });
      console.log("perfiles libres no hogar", disponibles);
      if (error) {
        alert("Error buscando perfil libre.");
        return;
      }
      if (!perfLibre) {
        console.warn("Sin stock no hogar", { disponibles });
        alert("Sin stock");
        return;
      }
      nuevoPerfil = perfLibre.id_perfil;
      nuevoCuenta = perfLibre.id_cuenta;
      console.log("perfil disponible", nuevoPerfil);
    }
  } else {
    if (ventaPerfil) {
      const { data: perfLibre, error, disponibles = [] } = await findPerfilLibre({
        excludeCuenta: cuentaId,
        requireVentaPerfil: true,
      });
      console.log("ruta ventaPerfil", { perfLibre, error, disponibles });
      if (error) {
        alert("Error buscando perfil libre.");
        return;
      }
      if (!perfLibre) {
        console.warn("Sin stock ventaPerfil", { disponibles });
        alert("Sin stock");
        return;
      }
      nuevoPerfil = perfLibre.id_perfil;
      nuevoCuenta = perfLibre.id_cuenta;
    } else if (ventaMiembro) {
      const { data: perfLibre, error, disponibles = [] } = await findPerfilLibre({ excludeCuenta: cuentaId });
      console.log("ruta ventaMiembro perfLibre", { perfLibre, error, disponibles });
      if (error) {
        alert("Error buscando perfil libre.");
        return;
      }
      if (perfLibre) {
        nuevoPerfil = perfLibre.id_perfil;
        nuevoCuenta = perfLibre.id_cuenta;
      } else {
        const { data: subLibre, error: subErr } = await findSubCuentaLibre(cuentaId);
        console.log("ruta ventaMiembro subLibre", { subLibre, subErr });
        if (subErr) {
          alert("Error buscando sub cuenta.");
          return;
        }
        if (!subLibre) {
          console.warn("Sin stock ventaMiembro", { disponibles });
          alert("Sin stock");
          return;
        }
        nuevaSubCuenta = subLibre.id_sub_cuenta;
        nuevoCuenta = subLibre.id_cuenta;
      }
    } else {
      const { data: cuentaLibre, error } = await findCuentaCompleta(cuentaId);
      console.log("ruta cuenta completa", { cuentaLibre, error });
      if (error) {
        alert("Error buscando cuenta libre.");
        return;
      }
      if (!cuentaLibre) {
        alert("Sin stock");
        return;
      }
      nuevoCuenta = cuentaLibre.id_cuenta;
      nuevoPerfil = null;
      nuevaSubCuenta = null;
    }
  }

  console.log("seleccion final", { nuevoCuenta, nuevoPerfil, nuevaSubCuenta });
  if (!nuevoCuenta && !nuevoPerfil && !nuevaSubCuenta) {
    alert("Sin stock");
    return;
  }

  pendingReplacement = {
    idVenta,
    nuevoCuenta: nuevoCuenta || null,
    nuevoPerfil: nuevoPerfil || null,
    nuevaSubCuenta: nuevaSubCuenta || null,
    cuentaOriginal: cuentaId,
    perfilOriginal: perfilId || null,
    plataformaId,
    esCuentaCompleta,
    ventaPerfil,
    ventaMiembro,
  };
  modalReemplazoConfirm?.classList.remove("hidden");
}

async function confirmarReemplazo() {
  if (!pendingReplacement || !currentRow) {
    closeReemplazoModal();
    return;
  }
  const razon = selectRazonReemplazo?.value?.trim();
  if (!razon) {
    alert("Selecciona la razón del reemplazo.");
    return;
  }
  const btn = btnConfirmarReemplazo;
  if (btn) btn.disabled = true;
  try {
    const id_usuario = requireSession();
    await ensureServerSession();
    const {
      idVenta,
      nuevoCuenta,
      nuevoPerfil,
      nuevaSubCuenta,
      cuentaOriginal,
      perfilOriginal,
      esCuentaCompleta,
      ventaPerfil,
      ventaMiembro,
    } = pendingReplacement;

    const updates = {
      id_cuenta: nuevoCuenta || null,
      id_perfil: nuevoPerfil || null,
      id_sub_cuenta: nuevaSubCuenta || null,
    };

    const updatesPromises = [];
    let ventasUpdate = supabase.from("ventas").update(updates).eq("id_cuenta", cuentaOriginal);
    ventasUpdate = perfilOriginal ? ventasUpdate.eq("id_perfil", perfilOriginal) : ventasUpdate.is("id_perfil", null);
    ventasUpdate = ventasUpdate.eq("id_venta", idVenta);
    updatesPromises.push(ventasUpdate);

    if (nuevoPerfil) {
      updatesPromises.push(
        supabase.from("perfiles").update({ ocupado: true }).eq("id_perfil", nuevoPerfil)
      );
    }
    if (esCuentaCompleta && nuevoCuenta) {
      updatesPromises.push(
        supabase.from("cuentas").update({ ocupado: true }).eq("id_cuenta", nuevoCuenta)
      );
    }

    updatesPromises.push(
      supabase
        .from("reemplazos")
        .insert([{ id_cuenta: cuentaOriginal, id_perfil: perfilOriginal || null }])
    );

    const descripcionSol = `Razon del reemplazo: ${razon}`;
    updatesPromises.push(
      supabase
        .from("reportes")
        .update({
          descripcion_solucion: descripcionSol,
          en_revision: false,
          solucionado: true,
          solucionado_por: id_usuario,
        })
        .eq("id_reporte", currentRow.id_reporte)
    );

    const results = await Promise.all(updatesPromises);
    const err = results.find((r) => r?.error);
    if (err?.error) {
      alert("No se pudo realizar el reemplazo.");
      return;
    }

    alert("Reemplazo realizado.");
    closeReemplazoModal();
    closeModal();
  } catch (err) {
    console.error("confirmar reemplazo error", err);
    alert("No se pudo realizar el reemplazo.");
  } finally {
    if (btn) btn.disabled = false;
  }
}
