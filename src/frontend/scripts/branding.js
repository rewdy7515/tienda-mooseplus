export const STATIC_HEADER_LOGO_HREF = new URL(
  "../assets/logo-header-carga/logo-blanco.webp",
  import.meta.url,
).href;
export const STATIC_FAVICON_HREF = new URL(
  "../assets/favicon/logo-corto-blanco-icono.png",
  import.meta.url,
).href;

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

const applyFavicon = () => {
  const links = ensureFaviconLink();
  links.forEach((link) => {
    link.setAttribute("href", STATIC_FAVICON_HREF);
    link.setAttribute("type", "image/png");
  });
};

const applyLogos = (selectors = []) => {
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
        el.src = STATIC_HEADER_LOGO_HREF;
      }
    });
  });
};

export async function loadPaginaBranding(options = {}) {
  const selectors = Array.isArray(options?.logoSelectors)
    ? options.logoSelectors
    : [".logo", ".auth-logo"];
  const shouldApplyFavicon = options?.applyFavicon !== false;

  applyLogos(selectors);
  if (shouldApplyFavicon) applyFavicon();

  return {
    logo: STATIC_HEADER_LOGO_HREF,
    icono_pestana: STATIC_FAVICON_HREF,
    error: null,
  };
}
