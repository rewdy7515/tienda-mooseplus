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
  idPlataforma = null,
  perfilHogar = false,
  isSubCuenta = false,
  ventaMiembro = false,
} = {}) {
  const lines = [];
  const platLabelRaw = (plataforma || "").toString().toUpperCase();
  const showHogar =
    Number(idPlataforma) === 1 &&
    (perfilHogar === true || isSubCuenta === true || ventaMiembro === true);
  const platLabel = showHogar ? `${platLabelRaw} (HOGAR ACTUALIZADO)` : platLabelRaw;
  const fmtFecha = (val) => {
    if (!val) return "";
    const d = new Date(val);
    if (Number.isNaN(d.valueOf())) return val;
    const dd = String(d.getDate()).padStart(2, "0");
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const yyyy = d.getFullYear();
    return `${dd}-${mm}-${yyyy}`;
  };
  const fechaFmt = fmtFecha(fechaCorte);
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
    lines.push(`_${fechaFmt}_`);
    lines.push("");
    lines.push("*INDICACIONES*");
    lines.push("* Solo usar 1 dispositivo a la vez por pantalla comprada.");
    lines.push("_Si incumple las indicaciones se suspenderá el servicio._");
  } else {
    lines.push("");
    lines.push("*Próxima fecha de pago:*");
    lines.push(`_${fechaFmt}_`);
  }
  return lines.join("\n");
}
