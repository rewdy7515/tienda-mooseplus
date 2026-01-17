export function buildServiceCopyText({
  plataforma = "",
  idVenta = "",
  correo = "",
  clave = "",
  nPerfil = "",
  pin = "",
  fechaCorte = "",
  porAcceso = false,
  usaPines = false,
  ventaPerfil = true,
  ventaMiembro = false,
  idPlataforma = null,
  perfilHogar = false,
  isSubCuenta = false,
} = {}) {
  const lines = [];
  const platLabelRaw = (plataforma || "").toString().toUpperCase();
  const showHogar =
    Number(idPlataforma) === 1 && (perfilHogar === true || isSubCuenta === true);
  const platLabel = showHogar ? `${platLabelRaw} (HOGAR ACTUALIZADO)` : platLabelRaw;
  lines.push(`*${platLabel}* 🫎 \`ID Venta: #${idVenta || ""}\``);
  lines.push("_Instagram: @moose.plus_");
  lines.push("");
  lines.push(`*Correo:* ${correo || ""}`);
  lines.push(`*Clave:* ${clave || ""}`);
  const hideExtras = !ventaPerfil && !ventaMiembro;
  if (porAcceso && !hideExtras) {
    lines.push(`*Acceso:* 1 acceso`);
  }
  if (nPerfil && (ventaPerfil || usaPines) && !hideExtras) {
    const perfilTxt = String(nPerfil).startsWith("M") ? String(nPerfil) : `M${nPerfil}`;
    lines.push(`*Perfil:* ${perfilTxt}`);
  }
  if (pin && usaPines && !hideExtras) lines.push(`*Pin:* ${pin}`);
  if (!hideExtras) {
    lines.push("");
    lines.push("*Próxima fecha de pago:*");
    lines.push(`_${fechaCorte || ""}_`);
    lines.push("");
    lines.push("*INDICACIONES*");
    lines.push("* Solo usar 1 dispositivo a la vez por pantalla comprada.");
    lines.push("_Si incumple las indicaciones se suspenderá el servicio._");
  } else {
    lines.push("");
    lines.push("*Próxima fecha de pago:*");
    lines.push(`_${fechaCorte || ""}_`);
  }
  return lines.join("\n");
}
