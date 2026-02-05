// Formatea fechas en formato dd-mm-yyyy sin aplicar zona horaria ni corrimientos.
// Toma el string tal cual viene (ej. "2026-01-18") y lo reordena.
export const formatDDMMYYYY = (dateStr) => {
  if (!dateStr) return "";
  const parts = String(dateStr).split("-");
  if (parts.length === 3) {
    const [yyyy, mm, dd] = parts;
    return `${dd?.padStart(2, "0")}-${mm?.padStart(2, "0")}-${yyyy}`;
  }
  return dateStr;
};

