import { API_BASE } from "./api.js";

const EMPTY_AVATAR_DATA_URL = "data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs=";
const RANDOM_AVATAR_STORAGE_PREFIX = "avatar_random_fallback";
const AVATAR_ICON_FOLDER = "icono-perfil";

export const AVATAR_RANDOM_COLORS = [
  "#ffa4a4",
  "#ffd5a4",
  "#75cd7e",
  "#8cdbf4",
  "#bab6ff",
  "#f7b6ff",
];

const isImageName = (name) =>
  /\.(png|jpe?g|webp|gif|bmp|svg|avif)$/i.test(String(name || ""));

const normalizeColor = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  if (/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(withHash)) return withHash.toLowerCase();
  return "";
};

const pickRandom = (items = []) => {
  if (!Array.isArray(items) || !items.length) return "";
  const idx = Math.floor(Math.random() * items.length);
  return items[idx] || "";
};

const cacheKeyForUser = (idUsuario = null) =>
  `${RANDOM_AVATAR_STORAGE_PREFIX}:${idUsuario ? String(idUsuario) : "anon"}`;

const readCachedFallback = (idUsuario = null) => {
  try {
    const raw = localStorage.getItem(cacheKeyForUser(idUsuario));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const url = String(parsed?.url || "").trim();
    const color = normalizeColor(parsed?.color);
    if (!url || !color) return null;
    return { url, color };
  } catch (_err) {
    return null;
  }
};

const writeCachedFallback = (idUsuario = null, avatar = null) => {
  try {
    const url = String(avatar?.url || "").trim();
    const color = normalizeColor(avatar?.color);
    if (!url || !color) return;
    localStorage.setItem(cacheKeyForUser(idUsuario), JSON.stringify({ url, color }));
  } catch (_err) {
    // noop
  }
};

const buildGeneratedAvatarSvgDataUrl = (color = "#8cdbf4") => {
  const fill = normalizeColor(color) || "#8cdbf4";
  const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 128 128'><rect width='128' height='128' fill='${fill}'/><circle cx='64' cy='48' r='24' fill='rgba(255,255,255,0.92)'/><rect x='24' y='80' width='80' height='36' rx='18' fill='rgba(255,255,255,0.92)'/></svg>`;
  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
};

const fetchAvatarIconUrls = async (idUsuario = null) => {
  try {
    const url = new URL(`${API_BASE}/api/logos/list`);
    url.searchParams.set("folder", AVATAR_ICON_FOLDER);
    if (idUsuario) url.searchParams.set("id_usuario", String(idUsuario));

    const res = await fetch(url.toString(), { credentials: "include" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) return [];

    const rows = Array.isArray(data?.items) ? data.items : [];
    return rows
      .filter((it) => it?.publicUrl)
      .filter((it) => !String(it?.name || "").startsWith("."))
      .filter((it) => isImageName(it?.name))
      .map((it) => String(it.publicUrl || "").trim())
      .filter(Boolean);
  } catch (_err) {
    return [];
  }
};

const buildRandomFallback = async (idUsuario = null) => {
  const color = pickRandom(AVATAR_RANDOM_COLORS) || "#8cdbf4";
  const iconUrls = await fetchAvatarIconUrls(idUsuario);
  const url = pickRandom(iconUrls) || buildGeneratedAvatarSvgDataUrl(color) || EMPTY_AVATAR_DATA_URL;
  return { url, color };
};

export async function resolveAvatarForDisplay({ user = null, idUsuario = null } = {}) {
  const effectiveUserId = idUsuario || user?.id_usuario || null;
  const foto = String(user?.foto_perfil || "").trim();
  const fondo = normalizeColor(user?.fondo_perfil);

  if (foto) {
    if (fondo) return { url: foto, color: fondo };
    const cached = readCachedFallback(effectiveUserId);
    if (cached?.color) return { url: foto, color: cached.color };
    const fallback = await buildRandomFallback(effectiveUserId);
    writeCachedFallback(effectiveUserId, fallback);
    return { url: foto, color: fallback.color };
  }

  const cached = readCachedFallback(effectiveUserId);
  if (cached) return cached;

  const fallback = await buildRandomFallback(effectiveUserId);
  writeCachedFallback(effectiveUserId, fallback);
  return fallback;
}

