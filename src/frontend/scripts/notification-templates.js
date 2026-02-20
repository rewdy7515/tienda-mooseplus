// Catálogo centralizado de plantillas de notificaciones.
// Cada clave representa un tipo de evento y devuelve { titulo, mensaje } con datos dinámicos.

import { formatDDMMYYYY } from "./date-format.js";

export const notificationTemplates = {
  saldo_acreditado: ({ monto }) => ({
    titulo: "Saldo acreditado",
    mensaje: `Se ha agregado ${monto} al saldo de tu cuenta para poder comprar y renovar servicios.`,
  }),

  pin_actualizado: ({ plataforma, correoCuenta, perfil, pin }) => ({
    titulo: "PIN actualizado",
    mensaje: `${plataforma ? `<strong>${plataforma}</strong><br>` : ""}Correo: ${correoCuenta}<br>Perfil: ${perfil}<br><br>Nuevo PIN: <strong>${pin}</strong>`,
  }),

  clave_actualizada: ({ plataforma, correoCuenta, clave }) => ({
    titulo: "Clave actualizada",
    mensaje: `${plataforma ? `<strong>${plataforma}</strong><br>` : ""}Se actualizó la clave de ${correoCuenta}.<br><br>Nueva clave: <strong>${clave}</strong>`,
  }),

  servicio_reemplazado: ({
    plataforma,
    correoViejo,
    perfilViejo,
    correoNuevo,
    perfilNuevo,
    claveNuevo,
  }) => ({
    titulo: "Servicio reemplazado",
    mensaje: `Tu servicio de <strong>${plataforma || "la plataforma"}</strong> pasó de <span class="no-linkify">${correoViejo}</span> (${perfilViejo}) a:<br><br>Correo: ${correoNuevo}<br>Clave: ${claveNuevo || ""}<br>Perfil: ${perfilNuevo}`,
  }),

  servicio_renovado: (data = {}) => {
    const rawItems = Array.isArray(data.items) ? data.items : [];
    const items =
      rawItems.length > 0
        ? rawItems
        : [
            {
              plataforma: data.plataforma,
              correoCuenta: data.correoCuenta,
              clave: data.clave,
              perfil: data.perfil,
              fechaCorte: data.fechaCorte,
              idVenta: data.idVenta,
            },
          ].filter(
            (it) =>
              it.plataforma ||
              it.correoCuenta ||
              it.clave ||
              it.perfil ||
              it.fechaCorte
          );

    const blocks = items.map((item) => {
      const parts = [];
      if (item.plataforma || item.idVenta) {
        const idTxt = item.idVenta ? `ID Venta: #${item.idVenta}` : "";
        parts.push(
          `<div class="notif-line"><strong>${item.plataforma || ""}</strong>${
            idTxt ? ` <span class="notif-id-venta">${idTxt}</span>` : ""
          }</div>`
        );
      }
      if (item.correoCuenta) parts.push(`Correo: ${item.correoCuenta}`);
      if (item.fechaCorte) {
        parts.push(`Fecha de corte: ${formatDDMMYYYY(item.fechaCorte)}`);
      }
      return parts.join("<br>");
    });

    const cuerpo =
      blocks.length > 0 ? `<br><br>${blocks.join("<br><br>")}` : "";

    return {
      titulo: "Servicio renovado",
      mensaje: `Se han renovado los siguientes servicios:${cuerpo}`,
    };
  },

  nuevo_servicio: (data = {}) => {
    const rawItems = Array.isArray(data.items) ? data.items : [];
    const items =
      rawItems.length > 0
        ? rawItems
        : [
            {
              plataforma: data.plataforma,
              correoCuenta: data.correoCuenta,
              clave: data.clave,
              perfil: data.perfil,
              fechaCorte: data.fechaCorte,
              idVenta: data.idVenta,
            },
          ].filter(
            (it) =>
              it.plataforma ||
              it.correoCuenta ||
              it.clave ||
              it.perfil ||
              it.fechaCorte
          );

    const blocks = items.map((item) => {
      const parts = [];
      if (item.plataforma || item.idVenta) {
        const idTxt = item.idVenta ? `ID Venta: #${item.idVenta}` : "";
        parts.push(
          `<div class="notif-line"><strong>${item.plataforma || ""}</strong>${
            idTxt ? ` <span class="notif-id-venta">${idTxt}</span>` : ""
          }</div>`
        );
      }
      if (item.correoCuenta) parts.push(`Correo: ${item.correoCuenta}`);
      if (item.fechaCorte) {
        parts.push(`Fecha de corte: ${formatDDMMYYYY(item.fechaCorte)}`);
      }
      return parts.join("<br>");
    });

    const cuerpo =
      blocks.length > 0 ? `<br><br>${blocks.join("<br><br>")}` : "";

    return {
      titulo: "Nuevo servicio",
      mensaje: `Se asignaron los servicios:${cuerpo}`,
    };
  },

  recordatorio_corte: ({ plataforma, correoCuenta, perfil, fechaCorte }) => {
    const partesCuenta = [correoCuenta, perfil].filter(Boolean).join(" - ");
    const cuentaTxt = partesCuenta ? ` (${partesCuenta})` : "";
    return {
      titulo: "Pronto vence tu servicio",
      mensaje: `Tu servicio de <strong>${plataforma}</strong>${cuentaTxt} vence el ${formatDDMMYYYY(
        fechaCorte
      )}.<br>Renueva para evitar interrupciones.`,
    };
  },

  servicios_vencen_pronto: (data = {}) => {
    const rawItems = Array.isArray(data.items) ? data.items : [];
    const items =
      rawItems.length > 0
        ? rawItems
        : [
            {
              plataforma: data.plataforma,
              correoCuenta: data.correoCuenta,
              fechaCorte: data.fechaCorte,
              idVenta: data.idVenta,
            },
          ].filter((it) => it.plataforma || it.correoCuenta || it.fechaCorte || it.idVenta);

    const blocks = items.map((item) => {
      const parts = [];
      if (item.plataforma || item.idVenta) {
        const idTxt = item.idVenta ? `ID Venta: #${item.idVenta}` : "";
        parts.push(
          `<div class="notif-line"><strong>${item.plataforma || ""}</strong>${
            idTxt ? ` <span class="notif-id-venta">${idTxt}</span>` : ""
          }</div>`
        );
      }
      if (item.correoCuenta) parts.push(`Correo: ${item.correoCuenta}`);
      if (item.fechaCorte) {
        parts.push(`Fecha de corte: ${formatDDMMYYYY(item.fechaCorte)}`);
      }
      return parts.join("<br>");
    });

    return {
      titulo: "Tus servicios vencen pronto",
      mensaje: blocks.join("<br><br>"),
    };
  },
};

export const buildNotification = (tipo, data = {}) => {
  const tpl = notificationTemplates[tipo];
  if (!tpl) {
    throw new Error(`Tipo de notificación no soportado: ${tipo}`);
  }
  return tpl(data);
};

// Construye el payload listo para insertar en la tabla notificaciones,
// agregando campos comunes como leido=false, id_cuenta y fecha (yyyy-mm-dd).
export const buildNotificationPayload = (tipo, data = {}, { idCuenta = null, fecha = null } = {}) => {
  const { titulo, mensaje } = buildNotification(tipo, data);
  const today = new Date().toISOString().slice(0, 10);
  return {
    titulo,
    mensaje,
    leido: false,
    id_cuenta: idCuenta,
    fecha: fecha || today,
  };
};

// Selecciona los id_usuario destinatarios según el tipo de notificación.
// Params:
//  - tipo: clave de plantilla
//  - ventaUserId: id_usuario vinculado a la venta actual (para pin_actualizado, servicio_renovado, nuevo_servicio, servicio_reemplazado, recordatorio_corte, servicios_vencen_pronto)
//  - cuentaVentas: array de ventas de la cuenta [{ id_usuario, fecha_corte }]
export const pickNotificationUserIds = (tipo, { ventaUserId = null, cuentaVentas = [] } = {}) => {
  const today = new Date().toISOString().slice(0, 10);
  const toDate = (d) => {
    const parsed = new Date(d);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  };

  switch (tipo) {
    case "pin_actualizado":
    case "servicio_renovado":
    case "nuevo_servicio":
    case "servicio_reemplazado":
    case "recordatorio_corte":
    case "servicios_vencen_pronto":
      return ventaUserId ? [ventaUserId] : [];

    case "clave_actualizada": {
      // Solo usuarios de la cuenta cuya fecha_corte sea anterior a hoy.
      const todayDate = toDate(today);
      const ids = (cuentaVentas || [])
        .filter((v) => {
          const d = toDate(v?.fecha_corte);
          return d && d < todayDate;
        })
        .map((v) => v?.id_usuario)
        .filter(Boolean);
      // deduplicar
      return Array.from(new Set(ids));
    }

    default:
      return [];
  }
};
