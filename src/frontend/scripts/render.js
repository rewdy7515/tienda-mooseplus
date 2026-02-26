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
            banner,
            por_pantalla,
            por_acceso,
            tarjeta_de_regalo,
            entrega_inmediata,
            descuento_meses,
            id_descuento_mes,
            id_descuento_cantidad,
            aplica_descuento_mes_detal,
            aplica_descuento_mes_mayor,
            aplica_descuento_cantidad_detal,
            aplica_descuento_cantidad_mayor,
            mostrar_stock,
            num_max_dispositivos,
          }) => {
            const platId = id_plataforma || id;
            const minPrecio = Number(preciosMinByPlat?.[String(platId)] ?? preciosMinByPlat?.[platId]);
            const precioTxt = (() => {
              if (!Number.isFinite(minPrecio)) return "Desde $--";
              const [intPart, decPart] = minPrecio.toFixed(2).split(".");
              return `Desde $<span class="plataforma-precio-int">${intPart}</span><sup class="plataforma-precio-dec">${decPart}</sup>`;
            })();
            return `
            <div class="plataforma-card"
              data-nombre="${nomPlat}"
              data-imagen="${imagen || ""}"
              data-banner="${banner || ""}"
              data-categoria="${nombre}"
              data-id-plataforma="${platId || ""}"
              data-por-pantalla="${por_pantalla}"
              data-por-acceso="${por_acceso}"
              data-tarjeta-de-regalo="${tarjeta_de_regalo}"
              data-entrega-inmediata="${entrega_inmediata}"
              data-descuento-meses="${descuento_meses}"
              data-id-descuento-mes="${id_descuento_mes ?? ""}"
              data-id-descuento-cantidad="${id_descuento_cantidad ?? ""}"
              data-aplica-descuento-mes-detal="${aplica_descuento_mes_detal}"
              data-aplica-descuento-mes-mayor="${aplica_descuento_mes_mayor}"
              data-aplica-descuento-cantidad-detal="${aplica_descuento_cantidad_detal}"
              data-aplica-descuento-cantidad-mayor="${aplica_descuento_cantidad_mayor}"
              data-mostrar-stock="${mostrar_stock}"
              data-num-max-dispositivos="${num_max_dispositivos ?? ""}"
            data-id-descuento="">
              <div class="plataforma-thumb">
                <img
                  src="${imagen || ""}"
                  alt="${nomPlat}"
                  loading="lazy"
                  decoding="async"
                  fetchpriority="low"
                />
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
