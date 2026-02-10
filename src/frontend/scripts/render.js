export function renderCategorias(container, categorias, plataformasPorCategoria, preciosMinByPlat = {}) {
  container.innerHTML = categorias
    .map(({ id_categoria, nombre }) => {
      const items = plataformasPorCategoria[id_categoria] || [];
      if (!items.length) return "";
      const cards = items
        .map(
          ({
            id_plataforma,
            id,
            nombre: nomPlat,
            imagen,
            por_pantalla,
            por_acceso,
            tarjeta_de_regalo,
            entrega_inmediata,
            descuento_meses,
            id_descuento,
            mostrar_stock,
          }) => {
            const platId = id_plataforma || id;
            const minPrecio = Number(preciosMinByPlat?.[String(platId)] ?? preciosMinByPlat?.[platId]);
            const precioTxt = (() => {
              if (!Number.isFinite(minPrecio)) return "Desde $--";
              const [intPart, decPart] = minPrecio.toFixed(2).split(".");
              return `Desde $${intPart}<sup>${decPart}</sup>`;
            })();
            return `
            <div class="plataforma-card"
              data-nombre="${nomPlat}"
              data-imagen="${imagen || ""}"
              data-categoria="${nombre}"
              data-id-plataforma="${platId || ""}"
              data-por-pantalla="${por_pantalla}"
              data-por-acceso="${por_acceso}"
              data-tarjeta-de-regalo="${tarjeta_de_regalo}"
              data-entrega-inmediata="${entrega_inmediata}"
              data-descuento-meses="${descuento_meses}"
              data-mostrar-stock="${mostrar_stock}"
            data-id-descuento="">
              <div class="plataforma-thumb">
                <img src="${imagen || ""}" alt="${nomPlat}" loading="lazy" />
              </div>
              <div class="plataforma-nombre">${nomPlat}</div>
              <div class="plataforma-precio">${precioTxt}</div>
            </div>`;
          }
        )
        .join("");

      return `
        <article class="categoria-card">
          <h3>${nombre}</h3>
          <div class="plataformas-grid">
            ${cards}
          </div>
        </article>`;
    })
    .filter(Boolean)
    .join("");
}
