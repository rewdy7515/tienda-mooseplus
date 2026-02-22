import { supabase } from "./api.js";

const PAGINA_TARGET_ID = 2;
const LOGO_FALLBACK =
  "data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs=";

const ensureFaviconLink = () => {
  document
    .querySelectorAll(
      'link[rel="icon"], link[rel="shortcut icon"], link[rel~="icon"], link[rel="apple-touch-icon"], link[rel="mask-icon"]',
    )
    .forEach((el) => el.remove());
  const iconLink = document.createElement("link");
  iconLink.setAttribute("rel", "icon");
  document.head.appendChild(iconLink);
  const shortcutLink = document.createElement("link");
  shortcutLink.setAttribute("rel", "shortcut icon");
  document.head.appendChild(shortcutLink);
  return [iconLink, shortcutLink];
};

const applyFavicon = (url = "") => {
  const iconUrl = String(url || "").trim();
  if (!iconUrl) {
    return;
  }
  const cacheBustedUrl = `${iconUrl}${iconUrl.includes("?") ? "&" : "?"}v=${Date.now()}`;
  const links = ensureFaviconLink();
  links.forEach((link) => {
    link.setAttribute("href", cacheBustedUrl);
    link.setAttribute("type", "image/png");
  });
};

const applyLogos = (url = "", selectors = []) => {
  const logoUrl = String(url || "").trim() || LOGO_FALLBACK;
  const uniqueSelectors = Array.from(
    new Set(
      (Array.isArray(selectors) ? selectors : [])
        .map((v) => String(v || "").trim())
        .filter(Boolean),
    ),
  );
  uniqueSelectors.forEach((sel) => {
    document.querySelectorAll(sel).forEach((el) => {
      if (el instanceof HTMLImageElement) {
        el.src = logoUrl;
      }
    });
  });
};

export async function loadPaginaBranding(options = {}) {
  const selectors = Array.isArray(options?.logoSelectors)
    ? options.logoSelectors
    : [".logo", ".auth-logo"];
  const shouldApplyFavicon = options?.applyFavicon !== false;

  try {
    const { data, error } = await supabase
      .from("pagina")
      .select("logo, icono_pestana")
      .eq("id", PAGINA_TARGET_ID)
      .maybeSingle();
    if (error) throw error;
    if (!data) throw new Error(`No existe pagina.id = ${PAGINA_TARGET_ID}`);

    const logoUrl = String(data?.logo || "").trim();
    const iconUrl = String(data?.icono_pestana || "").trim();

    applyLogos(logoUrl, selectors);
    if (shouldApplyFavicon) applyFavicon(iconUrl);
    return { logo: logoUrl, icono_pestana: iconUrl, error: null };
  } catch (err) {
    applyLogos("", selectors);
    return { logo: "", icono_pestana: "", error: err };
  }
}
