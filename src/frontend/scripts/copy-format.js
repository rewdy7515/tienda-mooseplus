export function buildServiceCopyText({
  plataforma = "",
  idVenta = "",
  correo = "",
  clave = "",
  nPerfil = "",
  pin = "",
  fechaCorte = "",
} = {}) {
  const lines = [];
  lines.push(`*${plataforma || ""}* 🫎 \`ID Venta: #${idVenta || ""}\``);
  lines.push("_Instagram: @moose.plus_");
  lines.push("");
  lines.push(`*Correo:* ${correo || ""}`);
  lines.push(`*Clave:* ${clave || ""}`);
  if (nPerfil) lines.push(`*Perfil:* ${nPerfil}`);
  if (pin) lines.push(`*Pin:* ${pin}`);
  lines.push("");
  lines.push("*Próxima fecha de pago:*");
  lines.push(`_${fechaCorte || ""}_`);
  lines.push("");
  lines.push("*INDICACIONES*");
  lines.push("* Solo usar 1 dispositivo a la vez por pantalla comprada.");
  lines.push("_Si incumple las indicaciones se suspenderá el servicio._");
  return lines.join("\n");
}
