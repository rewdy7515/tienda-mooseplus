export const DEFAULT_TASA_MARKUP = 1.06;
export const RATE_MARKUP_STORAGE_KEY = "tasa_markup";

const normalizeMarkupValue = (value, fallback = DEFAULT_TASA_MARKUP) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 1) return fallback;
  return parsed;
};

export const getTasaMarkup = () => {
  try {
    if (typeof window === "undefined") return DEFAULT_TASA_MARKUP;
    const stored = window.localStorage.getItem(RATE_MARKUP_STORAGE_KEY);
    return normalizeMarkupValue(stored, DEFAULT_TASA_MARKUP);
  } catch (_err) {
    return DEFAULT_TASA_MARKUP;
  }
};

export const setTasaMarkup = (value) => {
  const nextValue = normalizeMarkupValue(value, DEFAULT_TASA_MARKUP);
  try {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(RATE_MARKUP_STORAGE_KEY, String(nextValue));
      window.dispatchEvent(
        new CustomEvent("tasa-markup-change", {
          detail: { value: nextValue },
        }),
      );
    }
  } catch (_err) {
    // noop
  }
  return nextValue;
};
