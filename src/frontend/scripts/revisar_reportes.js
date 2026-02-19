import {
  requireSession,
  attachLogout,
  setSessionRoles,
  getSessionRoles,
  attachLogoHome,
} from "./session.js";
import { clearServerSession, loadCurrentUser, supabase, ensureServerSession } from "./api.js";
import { formatDDMMYYYY } from "./date-format.js";
import { buildNotificationPayload, pickNotificationUserIds } from "./notification-templates.js";

requireSession();

const usernameEl = document.querySelector(".username");
const adminLink = document.querySelector(".admin-link");
const isTrue = (v) => v === true || v === 1 || v === "1" || v === "true" || v === "t";
const statusEl = document.querySelector("#revisar-status");
const listEl = document.querySelector("#reportes-list");
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

let currentRow = null;
let oldClave = "";
let oldPin = "";
let cambioClave = false;
let cambioPin = false;
const reportesById = new Map();

const formatDate = (iso) => formatDDMMYYYY(iso) || "-";

const normalizeHexColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  return /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash) ? withHash : null;
};

const isDarkHex = (hex) => {
  const c = normalizeHexColor(hex);
  if (!c) return false;
  const full = c.length === 4 ? `#${c[1]}${c[1]}${c[2]}${c[2]}${c[3]}${c[3]}` : c;
  const r = parseInt(full.slice(1, 3), 16);
  const g = parseInt(full.slice(3, 5), 16);
  const b = parseInt(full.slice(5, 7), 16);
  const yiq = (r * 299 + g * 587 + b * 114) / 1000;
  return yiq < 140;
};

const buttonStyleForColor = (color) => {
  const c = normalizeHexColor(color);
  if (!c) return "";
  const textColor = isDarkHex(c) ? "#fff" : "#111";
  return ` style="background:${c};border-color:${c};color:${textColor};"`;
};

const tableStyleForColor = (color) => {
  const c = normalizeHexColor(color);
  if (!c) return "";
  const textColor = isDarkHex(c) ? "#fff" : "#111";
  return ` style="--table-header-bg:${c};--table-header-color:${textColor};"`;
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const getDescripcion = (row) => {
  return row.descripcion || "Otro...";
};

const notifyReporteCerrado = async (row) => {
  const reportId = Number(row?.id_reporte);
  const targetUserId = Number(row?.id_usuario);
  if (!Number.isFinite(reportId) || !Number.isFinite(targetUserId)) return;
  const fecha = new Date().toISOString().slice(0, 10);
  const payload = {
    titulo: `Reporte ${reportId} cerrado.`,
    mensaje: '<a href="reportes/report.html" class="link-inline">Más detalles</a>',
    fecha,
    leido: false,
    id_usuario: targetUserId,
  };
  const { error } = await supabase.from("notificaciones").insert(payload);
  if (error) throw error;
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

const renderReportesList = (plataformas = []) => {
  if (!listEl) return;
  if (!plataformas.length) {
    listEl.innerHTML = "";
    return;
  }
  listEl.innerHTML = plataformas
    .map((p, idx) => {
      const rowsHtml = (p.items || [])
        .map((r) => {
          const idFmt = r.id_reporte ? `#${String(r.id_reporte).padStart(4, "0")}` : "-";
          const cliente =
            [r.usuarios?.nombre, r.usuarios?.apellido].filter(Boolean).join(" ").trim() || "-";
          const correo = r.cuentas?.correo || "-";
          const motivo = getDescripcion(r);
          const fecha = formatDate(r.fecha_creacion || null);
          return `
            <tr>
              <td>${escapeHtml(idFmt)}</td>
              <td>${escapeHtml(cliente)}</td>
              <td>${escapeHtml(correo)}</td>
              <td>${escapeHtml(motivo)}</td>
              <td>${escapeHtml(fecha)}</td>
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
      return `
        <section class="inventario-item" data-plat="${p.id}">
          <button type="button" class="btn-outline inventario-btn" data-toggle-plat="${p.id}" data-idx="${idx}"${buttonStyleForColor(p.buttonColor)}>
            <span class="plat-btn-main">
              <span class="plat-btn-label">${escapeHtml(p.nombre || "Plataforma")}</span>
              <span class="plat-count-icon" aria-hidden="true">!</span>
            </span>
            <span class="plat-count-badge" aria-label="${(p.items || []).length} reportes">
              <span>${(p.items || []).length}</span>
            </span>
          </button>
          <div class="inventario-plan hidden" data-plat-content="${p.id}">
            <div class="tabla-wrapper">
              <table class="table-base reportes-table"${tableStyleForColor(p.headerColor)}>
                <thead>
                  <tr>
                    <th>ID Reporte</th>
                    <th>Cliente</th>
                    <th>Correo</th>
                    <th>Motivo</th>
                    <th>Fecha</th>
                    <th>Acción</th>
                  </tr>
                </thead>
                <tbody>
                  ${rowsHtml || '<tr><td colspan="6" class="status">No hay reportes pendientes para esta plataforma.</td></tr>'}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      `;
    })
    .join("");
};

async function loadReportes() {
  const { data, error } = await supabase
    .from("reportes")
    .select(
      "id_reporte,id_plataforma,plataformas(id_plataforma,nombre,color_1,color_2),id_usuario,usuarios(nombre,apellido),id_cuenta,cuentas(id_cuenta,correo,clave,id_plataforma,venta_perfil,venta_miembro),id_perfil,perfiles(id_perfil,n_perfil,pin,perfil_hogar,id_cuenta),descripcion,imagen,en_revision,solucionado,fecha_creacion"
    )
    .eq("solucionado", false);
  if (error) throw error;
  return data || [];
}

function closeModal() {
  modal?.classList.add("hidden");
  modalImagenWrapper?.classList.add("hidden");
  modalImagenWrapper?.classList.remove("no-image");
  modalSinImagen?.classList.add("hidden");
  if (modalImagen) modalImagen.src = "";
  currentRow = null;
  cambioClave = false;
  cambioPin = false;
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

  const showNoImageBox = () => {
    if (modalImagen) modalImagen.src = "";
    modalImagenWrapper?.classList.remove("hidden");
    modalImagenWrapper?.classList.add("no-image");
    modalSinImagen?.classList.add("hidden");
  };

  if (row.imagen) {
    const url = await getImageUrl(row.imagen);
    if (url && modalImagen) {
      modalImagen.src = url;
      modalImagenWrapper?.classList.remove("hidden");
      modalImagenWrapper?.classList.remove("no-image");
      modalSinImagen?.classList.add("hidden");
      modalImagen.onload = () => {
        modalImagenWrapper?.classList.remove("hidden");
        modalImagenWrapper?.classList.remove("no-image");
        modalSinImagen?.classList.add("hidden");
      };
      modalImagen.onerror = () => {
        showNoImageBox();
      };
    } else {
      showNoImageBox();
    }
  } else {
    showNoImageBox();
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
      if (listEl) listEl.innerHTML = "";
      return;
    }

    reportesById.clear();
    activos.forEach((r) => {
      if (r?.id_reporte) reportesById.set(String(r.id_reporte), r);
    });

    const porPlat = new Map();
    activos.forEach((r) => {
      const id = r.id_plataforma || r.cuentas?.id_plataforma || r.plataformas?.id_plataforma;
      const nombre = r.plataformas?.nombre || "Plataforma";
      const buttonColor = r.plataformas?.color_1 || null;
      const headerColor = r.plataformas?.color_2 || null;
      if (!id) return;
      if (!porPlat.has(id)) {
        porPlat.set(id, { id, nombre, buttonColor, headerColor, items: [] });
      }
      porPlat.get(id).items.push(r);
    });

    const plataformas = Array.from(porPlat.values());
    renderReportesList(plataformas);
    if (statusEl) statusEl.textContent = "";

    listEl?.addEventListener("click", (e) => {
      const toggleBtn = e.target.closest("button[data-toggle-plat]");
      if (toggleBtn) {
        const platId = String(toggleBtn.dataset.togglePlat || "");
        const section = toggleBtn.closest(".inventario-item");
        const content = section?.querySelector(`[data-plat-content="${platId}"]`);
        if (content) {
          content.classList.toggle("hidden");
          section?.classList.toggle("open", !content.classList.contains("hidden"));
        }
        return;
      }

      const btn = e.target.closest("button[data-action]");
      if (!btn) return;
      const id = btn.dataset.id;
      if (!id) return;
      const row = reportesById.get(String(id));
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
    reemplazarServicio();
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

    try {
      await notifyReporteCerrado(currentRow);
    } catch (notifErr) {
      console.error("notificacion reporte cerrado error", notifErr);
    }

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

async function reemplazarServicio() {
  if (!currentRow) {
    alert("Selecciona un reporte.");
    return;
  }

  try {
    const idUsuarioSesion = requireSession();
    await ensureServerSession();

    const plataformaId =
      currentRow.id_plataforma ??
      currentRow.cuentas?.id_plataforma ??
      currentRow.plataformas?.id_plataforma;
    const cuentaId = currentRow.id_cuenta;
    const rowPerfil = currentRow.id_perfil
      ? {
          id_perfil: currentRow.id_perfil,
          n_raw: currentRow.perfiles?.n_perfil ?? null,
          perfil: currentRow.perfiles?.n_perfil ? `M${currentRow.perfiles.n_perfil}` : "",
          hogar: currentRow.perfiles?.perfil_hogar === true,
          fecha_corte: null,
        }
      : null;

    if (!plataformaId || !cuentaId) {
      alert("Faltan datos de plataforma o cuenta.");
      return;
    }

    const findVentaAsociada = async (withUserFilter) => {
      let query = supabase
        .from("ventas")
        .select("id_venta, id_usuario, fecha_corte")
        .eq("id_cuenta", cuentaId)
        .order("id_venta", { ascending: false })
        .limit(1);
      if (withUserFilter && currentRow?.id_usuario) {
        query = query.eq("id_usuario", currentRow.id_usuario);
      }
      if (rowPerfil?.id_perfil) {
        query = query.eq("id_perfil", rowPerfil.id_perfil);
      } else {
        query = query.is("id_perfil", null);
      }
      const { data, error } = await query;
      return { data: data || [], error };
    };

    let { data: ventasData, error: ventaErr } = await findVentaAsociada(true);
    if (!ventasData.length) {
      const fallback = await findVentaAsociada(false);
      ventasData = fallback.data;
      ventaErr = fallback.error;
    }
    if (!ventasData.length && !rowPerfil?.id_perfil) {
      let fallbackAny = supabase
        .from("ventas")
        .select("id_venta, id_usuario, fecha_corte")
        .eq("id_cuenta", cuentaId)
        .order("id_venta", { ascending: false })
        .limit(1);
      if (currentRow?.id_usuario) fallbackAny = fallbackAny.eq("id_usuario", currentRow.id_usuario);
      const { data, error } = await fallbackAny;
      ventasData = data || [];
      ventaErr = error;
      if (!ventasData.length) {
        const finalFallback = await supabase
          .from("ventas")
          .select("id_venta, id_usuario, fecha_corte")
          .eq("id_cuenta", cuentaId)
          .order("id_venta", { ascending: false })
          .limit(1);
        ventasData = finalFallback.data || [];
        ventaErr = finalFallback.error;
      }
    }
    if (ventaErr || !ventasData?.length) {
      alert("No se encontró la venta asociada.");
      return;
    }
    const ventaInfo = ventasData[0];
    const ventaId = ventaInfo.id_venta;

    const ventaPerfil = isTrue(currentRow.cuentas?.venta_perfil);
    const ventaMiembro = isTrue(currentRow.cuentas?.venta_miembro);
    const perfilHogar = rowPerfil?.hogar === true;

    const findPerfilLibre = async (platId, isHogar, excludeCuenta) => {
      let query = supabase
        .from("perfiles")
        .select(
          "id_perfil, perfil_hogar, id_cuenta, pin, n_perfil, ocupado, cuentas:cuentas!perfiles_id_cuenta_fkey!inner(id_plataforma, inactiva, correo, clave)",
        )
        .eq("cuentas.id_plataforma", platId)
        .eq("ocupado", false)
        .order("id_perfil", { ascending: true })
        .limit(1);
      if (isHogar === true) {
        query = query.eq("perfil_hogar", true);
      } else {
        query = query.or("perfil_hogar.is.null,perfil_hogar.eq.false");
      }
      query = query.or("inactiva.is.null,inactiva.eq.false", {
        foreignTable: "cuentas",
      });
      if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
      const { data, error } = await query;
      if (error) return { error };
      return { data: data?.[0] || null };
    };

    const findCuentaMiembroLibre = async (platId, excludeCuenta) => {
      let query = supabase
        .from("cuentas")
        .select("id_cuenta, correo, clave, inactiva, ocupado, venta_perfil, venta_miembro")
        .eq("id_plataforma", platId)
        .eq("venta_perfil", false)
        .eq("venta_miembro", true)
        .eq("ocupado", false)
        .order("id_cuenta", { ascending: true })
        .limit(1);
      query = query.or("inactiva.is.null,inactiva.eq.false");
      if (excludeCuenta) query = query.neq("id_cuenta", excludeCuenta);
      const { data, error } = await query;
      if (error) return { error };
      return { data: data?.[0] || null };
    };

    let nuevoCuenta = null;
    let nuevoPerfil = null;
    let dataDestino = {};

    if (rowPerfil?.id_perfil) {
      const { data: perfilDestino, error: perfilErr } = await findPerfilLibre(
        plataformaId,
        perfilHogar,
        cuentaId,
      );
      if (perfilErr) throw perfilErr;
      if (!perfilDestino) {
        alert("Sin stock");
        return;
      }
      nuevoPerfil = perfilDestino.id_perfil;
      nuevoCuenta = perfilDestino.id_cuenta;
      dataDestino = {
        correo: perfilDestino.cuentas?.correo || "",
        clave: perfilDestino.cuentas?.clave || "",
        pin: perfilDestino.pin,
        n_perfil: perfilDestino.n_perfil,
      };
    } else if (ventaMiembro && !ventaPerfil) {
      const { data: cuentaDestino, error: cuentaErr } = await findCuentaMiembroLibre(
        plataformaId,
        cuentaId,
      );
      if (cuentaErr) throw cuentaErr;
      if (!cuentaDestino) {
        alert("Sin stock");
        return;
      }
      nuevoPerfil = null;
      nuevoCuenta = cuentaDestino.id_cuenta;
      dataDestino = {
        correo: cuentaDestino.correo || "",
        clave: cuentaDestino.clave || "",
      };
    } else {
      alert("Sin stock");
      return;
    }

    const { error: updVentaErr } = await supabase
      .from("ventas")
      .update({ id_cuenta: nuevoCuenta || null, id_perfil: nuevoPerfil || null, id_sub_cuenta: null })
      .eq("id_venta", ventaId);
    if (updVentaErr) throw updVentaErr;

    if (rowPerfil?.id_perfil) {
      const { error: freeErr } = await supabase
        .from("perfiles")
        .update({ ocupado: false })
        .eq("id_perfil", rowPerfil.id_perfil);
      if (freeErr) console.error("[reemplazo] liberar perfil previo error", freeErr);
    }
    if (nuevoPerfil) {
      const { error: occErr } = await supabase
        .from("perfiles")
        .update({ ocupado: true })
        .eq("id_perfil", nuevoPerfil);
      if (occErr) console.error("[reemplazo] marcar perfil nuevo error", occErr);
    }
    if (nuevoCuenta) {
      const { error: occCuentaErr } = await supabase
        .from("cuentas")
        .update({ ocupado: true })
        .eq("id_cuenta", nuevoCuenta);
      if (occCuentaErr) console.error("[reemplazo] marcar cuenta nueva error", occCuentaErr);
    }

    await supabase.from("reemplazos").insert({
      id_cuenta: cuentaId,
      id_perfil: rowPerfil?.id_perfil || null,
      id_sub_cuenta: null,
    });

    try {
      const userIds = pickNotificationUserIds("servicio_reemplazado", {
        ventaUserId: ventaInfo?.id_usuario,
      });
      if (userIds.length) {
        const notif = buildNotificationPayload(
          "servicio_reemplazado",
          {
            plataforma: currentRow.plataformas?.nombre || "",
            correoViejo: currentRow.cuentas?.correo || "",
            perfilViejo: rowPerfil?.perfil || (rowPerfil?.n_raw ? `M${rowPerfil.n_raw}` : ""),
            correoNuevo: dataDestino.correo || "",
            perfilNuevo: dataDestino.n_perfil ? `M${dataDestino.n_perfil}` : "",
            claveNuevo: dataDestino.clave || "",
          },
          { idCuenta: nuevoCuenta || null },
        );
        const rows = userIds.map((uid) => ({
          ...notif,
          id_usuario: uid,
        }));
        await supabase.from("notificaciones").insert(rows);
      }
    } catch (nErr) {
      console.error("notificacion servicio_reemplazado error", nErr);
    }

    const descripcionSolucion = "Servicio reemplazado";
    const { error: repErr } = await supabase
      .from("reportes")
      .update({
        descripcion: descripcionSolucion,
        descripcion_solucion: descripcionSolucion,
        en_revision: false,
        solucionado: true,
        solucionado_por: idUsuarioSesion,
      })
      .eq("id_reporte", currentRow.id_reporte);
    if (repErr) throw repErr;

    try {
      await notifyReporteCerrado(currentRow);
    } catch (notifErr) {
      console.error("notificacion reporte cerrado error", notifErr);
    }

    alert("Reemplazo realizado.");
    closeModal();
  } catch (err) {
    console.error("reemplazo reporte error", err);
    alert("No se pudo reemplazar.");
  }
}
