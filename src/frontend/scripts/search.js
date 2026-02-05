let plataformas = [];
let inputEl;
let resultsEl;
let onSelect;

const renderSearch = (term) => {
  if (!resultsEl) return;
  const value = term.trim().toLowerCase();
  if (!value) {
    resultsEl.classList.add("hidden");
    resultsEl.innerHTML = "";
    return;
  }
  const matches = plataformas.filter((p) =>
    p.nombre.toLowerCase().includes(value)
  );
  if (!matches.length) {
    resultsEl.classList.add("hidden");
    resultsEl.innerHTML = "";
    return;
  }
  resultsEl.innerHTML = matches
    .slice(0, 8)
    .map(
      (p) => `
      <div class="search-result-item" data-id="${p.id_plataforma}">
        <div class="search-result-thumb">
          <img src="${p.imagen || ""}" alt="${p.nombre}" />
        </div>
        <div class="search-result-name">${p.nombre}</div>
      </div>`
    )
    .join("");
  resultsEl.classList.remove("hidden");
};

export function initSearch({ input, results, data, onSelectItem }) {
  inputEl = input;
  resultsEl = results;
  plataformas = data || [];
  onSelect = onSelectItem;

  inputEl?.addEventListener("input", (e) => renderSearch(e.target.value));
  inputEl?.addEventListener("blur", () => {
    setTimeout(() => {
      resultsEl?.classList.add("hidden");
    }, 150);
  });

  resultsEl?.addEventListener("click", (e) => {
    const item = e.target.closest(".search-result-item");
    if (!item) return;
    const id = item.dataset.id;
    const plataforma = plataformas.find((p) => String(p.id_plataforma) === id);
    if (plataforma && onSelect) onSelect(plataforma);
    resultsEl.classList.add("hidden");
  });
}

export function updateSearchData(data) {
  plataformas = data || [];
}
