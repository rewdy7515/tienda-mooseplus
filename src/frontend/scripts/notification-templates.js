// Catálogo centralizado de plantillas de notificaciones.
// Cada clave representa un tipo de evento y devuelve { titulo, mensaje } con datos dinámicos.

export const notificationTemplates = {
  pin_actualizado: ({ correoCuenta, perfil, pin }) => ({
    titulo: "PIN actualizado",
    mensaje: `Se actualizó el PIN de ${correoCuenta} (${perfil}) a ${pin}.`,
  }),

  clave_actualizada: ({ correoCuenta, clave }) => ({
    titulo: "Clave actualizada",
    mensaje: `Se actualizó la clave de ${correoCuenta} a ${clave}.`,
  }),

  servicio_reemplazado: ({ correoViejo, perfilViejo, correoNuevo, perfilNuevo, fechaCorte }) => ({
    titulo: "Servicio reemplazado",
    mensaje: `Tu servicio pasó de ${correoViejo} (${perfilViejo}) a ${correoNuevo} (${perfilNuevo}). Corte: ${fechaCorte}.`,
  }),

  servicio_renovado: ({ plataforma, correoCuenta, fechaCorte }) => ({
    titulo: "Servicio renovado",
    mensaje: `Renovamos ${plataforma} (${correoCuenta}). Próximo corte: ${fechaCorte}.`,
  }),

  nuevo_servicio: ({ plataforma, correoCuenta, perfil, fechaCorte, idOrden }) => ({
    titulo: "Nuevo servicio",
    mensaje: `Se asignó el servicio de ${plataforma} a <a href="inventario.html?correo=${encodeURIComponent(
      correoCuenta || ""
    )}">${correoCuenta || "-"}</a>.`,
  }),

  recordatorio_corte: ({ plataforma, correoCuenta, fechaCorte }) => ({
    titulo: "Pronto vence tu servicio",
    mensaje: `Tu servicio ${plataforma} vence el ${fechaCorte}. Renueva para evitar interrupciones.`,
  }),
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
//  - ventaUserId: id_usuario vinculado a la venta actual (para pin_actualizado, servicio_renovado, nuevo_servicio, servicio_reemplazado, recordatorio_corte)
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
