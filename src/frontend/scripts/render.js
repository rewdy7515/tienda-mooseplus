export function renderCategorias(container, categorias, plataformasPorCategoria) {
  container.innerHTML = categorias
    .map(({ id_categoria, nombre }) => {
      const items = plataformasPorCategoria[id_categoria] || [];
      const cards = items.length
        ? items
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
                </div>`;
              }
            )
            .join("")
        : `<p class="sin-plataformas">Sin plataformas en esta categoría.</p>`;

      return `
        <article class="categoria-card">
          <h3>${nombre}</h3>
          <div class="plataformas-grid">
            ${cards}
          </div>
        </article>`;
    })
    .join("");
}
