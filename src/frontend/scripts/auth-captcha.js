const PLACEHOLDER_PATTERN = /REEMPLAZAR|YOUR_|SITE_KEY|CAPTCHA/i;

const SCRIPT_SRC_BY_PROVIDER = {
  turnstile: "https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit",
  hcaptcha: "https://js.hcaptcha.com/1/api.js?render=explicit",
};

const GLOBAL_BY_PROVIDER = {
  turnstile: "turnstile",
  hcaptcha: "hcaptcha",
};

const normalizeProvider = (value) => {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "hcaptcha" || raw === "turnstile") return raw;
  return "turnstile";
};

const normalizeLanguage = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "es";
  return raw.toLowerCase();
};

const buildProviderScriptSrc = (provider, language) => {
  const base = SCRIPT_SRC_BY_PROVIDER[provider];
  if (!base) return "";
  const lang = normalizeLanguage(language);
  if (!lang) return base;
  if (provider === "hcaptcha") return `${base}&hl=${encodeURIComponent(lang)}`;
  if (provider === "turnstile") return `${base}&language=${encodeURIComponent(lang)}`;
  return base;
};

const readCaptchaConfig = () => {
  const provider = normalizeProvider(document.body?.dataset?.captchaProvider);
  const siteKey = String(document.body?.dataset?.captchaSiteKey || "").trim();
  const language = normalizeLanguage(document.body?.dataset?.captchaLanguage);
  return { provider, siteKey, language };
};

const loadProviderScript = async (provider, language) => {
  const globalName = GLOBAL_BY_PROVIDER[provider];
  if (window[globalName]) return window[globalName];

  const existingScript = document.querySelector(
    `script[data-captcha-provider="${provider}"][data-captcha-language="${normalizeLanguage(language)}"]`
  );
  if (existingScript && window[globalName]) return window[globalName];
  if (!existingScript) {
    const script = document.createElement("script");
    script.src = buildProviderScriptSrc(provider, language);
    script.async = true;
    script.defer = true;
    script.dataset.captchaProvider = provider;
    script.dataset.captchaLanguage = normalizeLanguage(language);
    document.head.appendChild(script);
  }

  await new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      if (window[globalName]) {
        resolve();
        return;
      }
      if (Date.now() - started > 12000) {
        reject(new Error(`No se pudo cargar captcha (${provider})`));
        return;
      }
      window.setTimeout(tick, 80);
    };
    tick();
  });

  return window[globalName];
};

const isValidSiteKey = (value) => value && !PLACEHOLDER_PATTERN.test(value);

const setInlineError = (el, message) => {
  if (!el) return;
  el.textContent = message || "";
};

export async function initAuthCaptcha({ containerId, errorId }) {
  const containerEl = document.getElementById(containerId);
  const errorEl = document.getElementById(errorId);
  const { provider, siteKey, language } = readCaptchaConfig();

  const controller = {
    provider,
    enabled: false,
    ready: false,
    getToken: () => "",
    ensureToken: () => null,
    reset: () => {},
  };

  if (!containerEl) {
    return controller;
  }
  if (!isValidSiteKey(siteKey)) {
    setInlineError(errorEl, "Captcha no configurado. Agrega data-captcha-site-key en el body.");
    return controller;
  }

  let widgetId = null;
  let token = "";

  try {
    const api = await loadProviderScript(provider, language);
    const options = {
      sitekey: siteKey,
      callback: (nextToken) => {
        token = String(nextToken || "");
        setInlineError(errorEl, "");
      },
      "expired-callback": () => {
        token = "";
      },
      "error-callback": () => {
        token = "";
        setInlineError(errorEl, "No se pudo validar el captcha. Intenta de nuevo.");
      },
    };
    if (provider === "hcaptcha") options.hl = language;
    if (provider === "turnstile") options.language = language;
    widgetId = api.render(containerEl, options);

    controller.enabled = true;
    controller.ready = true;
    controller.getToken = () => token;
    controller.ensureToken = () => {
      if (token) return token;
      setInlineError(errorEl, "Completa el captcha para continuar.");
      return null;
    };
    controller.reset = () => {
      token = "";
      try {
        api.reset(widgetId);
      } catch (_err) {
        // noop
      }
    };
  } catch (err) {
    console.error("captcha init error", err);
    setInlineError(errorEl, "No se pudo cargar el captcha.");
  }

  return controller;
}
