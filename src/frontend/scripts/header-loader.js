(function () {
  const container = document.getElementById("app-header");
  if (!container) return;
  const root = container.dataset.headerRoot || "../";
  const pages = container.dataset.headerPages || "";
  const partialUrl = `${root}partials/header.html`;

  window.__headerRoot = root;
  window.__headerPages = pages;
  try {
    window.__headerPagesAbs = new URL(pages || "./", window.location.href).pathname;
    window.__headerRootAbs = new URL(root || "./", window.location.href).pathname;
  } catch (_) {
    window.__headerPagesAbs = pages;
    window.__headerRootAbs = root;
  }

  try {
    const xhr = new XMLHttpRequest();
    xhr.open("GET", partialUrl, false); // síncrono para asegurar que el header esté antes de los demás scripts
    xhr.send(null);
    if (xhr.status >= 200 && xhr.status < 300) {
      const html = xhr.responseText
        .replace(/__ROOT__/g, root)
        .replace(/__PAGES__/g, pages);
      container.innerHTML = html;

      // Inicializa widget de carrito una sola vez
      if (!window.__cartWidgetScriptLoaded) {
        window.__cartWidgetScriptLoaded = true;
        const script = document.createElement("script");
        script.type = "module";
        script.src = `${root}scripts/cart-widget.js`;
        document.body.appendChild(script);
      }
      if (!window.__headerActionsScriptLoaded) {
        window.__headerActionsScriptLoaded = true;
        const script2 = document.createElement("script");
        script2.type = "module";
        script2.src = `${root}scripts/header-actions.js`;
        document.body.appendChild(script2);
      }
    } else {
      console.error("No se pudo cargar el header:", xhr.status, partialUrl);
    }
  } catch (err) {
    console.error("header loader error", err);
  }
})();
