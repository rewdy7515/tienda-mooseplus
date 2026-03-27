import { formatDDMMYYYY } from "./date-format.js";

export function buildServiceCopyText({
  plataforma = "",
  idVenta = "",
  correo = "",
  clave = "",
  nPerfil = "",
  pin = "",
  fechaCorte = "",
  porAcceso = false,
  porPantalla = null,
  usaPines = false,
  ventaPerfil = true,
  idPlataforma = null,
  perfilHogar = false,
  isSubCuenta = false,
  ventaMiembro = false,
  esTarjetaDeRegalo = false,
  region = "",
  valorTarjeta = "",
  moneda = "",
} = {}) {
  const isTrue = (value) =>
    value === true || value === 1 || value === "1" || value === "true" || value === "t";
  const lines = [];
  const platLabelRaw = (plataforma || "").toString().toUpperCase();
  const regionTxt = String(region || "").trim() || "-";
  const giftValueTxt =
    [String(valorTarjeta || "").trim(), String(moneda || "").trim()].filter(Boolean).join(" ") || "-";
  const pinTxt = String(pin || "").trim() || "Pendiente";
  if (esTarjetaDeRegalo) {
    lines.push(`*${platLabelRaw || "GIFT CARD"}* 🫎 \`ID Venta: #${idVenta || ""}\``);
    lines.push(`(Región: ${regionTxt})`);
    lines.push("_Pagina Web: www.mooseplus.com_");
    lines.push("");
    lines.push(`\`${giftValueTxt}\``);
    lines.push(`PIN: ${pinTxt}`);
    return lines.join("\n");
  }
  const showHogar =
    Number(idPlataforma) === 1 &&
    (perfilHogar === true || isSubCuenta === true || ventaMiembro === true);
  const platLabel = showHogar ? `${platLabelRaw} (HOGAR ACTUALIZADO)` : platLabelRaw;
  const fechaFmt = formatDDMMYYYY(fechaCorte);
  lines.push(`*${platLabel}* 🫎 \`ID Venta: #${idVenta || ""}\``);
  lines.push("_Pagina Web: www.mooseplus.com_");
  lines.push("");
  lines.push(`*Correo:* ${correo || ""}`);
  lines.push(`*Clave:* ${clave || ""}`);
  const hideExtras = !ventaPerfil && !ventaMiembro;
  const hasPorAcceso = isTrue(porAcceso);
  const hasPorPantalla = porPantalla === null || porPantalla === undefined ? null : isTrue(porPantalla);
  const canShowPerfilLinea = hasPorPantalla === null ? true : hasPorPantalla || hasPorAcceso;
  if (!hideExtras && canShowPerfilLinea) {
    if (hasPorPantalla === false && hasPorAcceso) {
      lines.push("*Acceso:* 1 dispositivo");
    } else if (nPerfil && (ventaPerfil || usaPines)) {
      const perfilTxt = String(nPerfil).startsWith("M") ? String(nPerfil) : `M${nPerfil}`;
      lines.push(`*Perfil:* ${perfilTxt}`);
    }
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
