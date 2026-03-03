const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

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
            id_descuento_mes_detal,
            id_descuento_mes_mayor,
            id_descuento_cantidad_detal,
            id_descuento_cantidad_mayor,
            aplica_descuento_mes_detal,
            aplica_descuento_mes_mayor,
            aplica_descuento_cantidad_detal,
            aplica_descuento_cantidad_mayor,
            mostrar_stock,
            num_max_dispositivos,
          }) => {
            const platId = id_plataforma || id;
	            const minPrecio = Number(preciosMinByPlat?.[String(platId)] ?? preciosMinByPlat?.[platId]);
	            const safeNombreCategoria = escapeHtml(nombre);
	            const safeNomPlat = escapeHtml(nomPlat || "");
	            const safeImagen = escapeHtml(imagen || "");
	            const safeBanner = escapeHtml(banner || "");
	            const safePlatId = escapeHtml(platId || "");
	            const safePorPantalla = escapeHtml(por_pantalla);
	            const safePorAcceso = escapeHtml(por_acceso);
	            const safeTarjetaRegalo = escapeHtml(tarjeta_de_regalo);
	            const safeEntregaInmediata = escapeHtml(entrega_inmediata);
	            const safeDescuentoMeses = escapeHtml(descuento_meses);
	            const safeIdDescuentoMes = escapeHtml(id_descuento_mes ?? "");
	            const safeIdDescuentoCantidad = escapeHtml(id_descuento_cantidad ?? "");
	            const safeIdDescuentoMesDetal = escapeHtml(id_descuento_mes_detal ?? "");
	            const safeIdDescuentoMesMayor = escapeHtml(id_descuento_mes_mayor ?? "");
	            const safeIdDescuentoCantidadDetal = escapeHtml(id_descuento_cantidad_detal ?? "");
	            const safeIdDescuentoCantidadMayor = escapeHtml(id_descuento_cantidad_mayor ?? "");
	            const safeAplicaMesDetal = escapeHtml(aplica_descuento_mes_detal);
	            const safeAplicaMesMayor = escapeHtml(aplica_descuento_mes_mayor);
	            const safeAplicaCantDetal = escapeHtml(aplica_descuento_cantidad_detal);
	            const safeAplicaCantMayor = escapeHtml(aplica_descuento_cantidad_mayor);
	            const safeMostrarStock = escapeHtml(mostrar_stock);
	            const safeNumMaxDispositivos = escapeHtml(num_max_dispositivos ?? "");
	            const precioTxt = (() => {
	              if (!Number.isFinite(minPrecio)) return "Desde $--";
	              const [intPart, decPart] = minPrecio.toFixed(2).split(".");
	              return `Desde $<span class="plataforma-precio-int">${intPart}</span><sup class="plataforma-precio-dec">${decPart}</sup>`;
	            })();
	            return `
	            <div class="plataforma-card"
	              data-nombre="${safeNomPlat}"
	              data-imagen="${safeImagen}"
	              data-banner="${safeBanner}"
	              data-categoria="${safeNombreCategoria}"
	              data-id-plataforma="${safePlatId}"
	              data-por-pantalla="${safePorPantalla}"
	              data-por-acceso="${safePorAcceso}"
	              data-tarjeta-de-regalo="${safeTarjetaRegalo}"
	              data-entrega-inmediata="${safeEntregaInmediata}"
	              data-descuento-meses="${safeDescuentoMeses}"
	              data-id-descuento-mes="${safeIdDescuentoMes}"
	              data-id-descuento-cantidad="${safeIdDescuentoCantidad}"
	              data-id-descuento-mes-detal="${safeIdDescuentoMesDetal}"
	              data-id-descuento-mes-mayor="${safeIdDescuentoMesMayor}"
	              data-id-descuento-cantidad-detal="${safeIdDescuentoCantidadDetal}"
	              data-id-descuento-cantidad-mayor="${safeIdDescuentoCantidadMayor}"
	              data-aplica-descuento-mes-detal="${safeAplicaMesDetal}"
	              data-aplica-descuento-mes-mayor="${safeAplicaMesMayor}"
	              data-aplica-descuento-cantidad-detal="${safeAplicaCantDetal}"
	              data-aplica-descuento-cantidad-mayor="${safeAplicaCantMayor}"
	              data-mostrar-stock="${safeMostrarStock}"
	              data-num-max-dispositivos="${safeNumMaxDispositivos}"
	            data-id-descuento="">
	              <div class="plataforma-thumb">
	                <img
	                  src="${safeImagen}"
	                  alt="${safeNomPlat}"
	                  loading="lazy"
	                  decoding="async"
	                  fetchpriority="low"
	                />
	              </div>
	              <div class="plataforma-nombre">${safeNomPlat}</div>
	              <div class="plataforma-precio">${precioTxt}</div>
	            </div>`;
          }
        )
        .join("");

      return `
        <article class="categoria-card">
	          <h3>${escapeHtml(nombre)}</h3>
          <div class="plataformas-grid">
            ${cards}
          </div>
        </article>`;
    })
    .filter(Boolean)
    .join("");
}
