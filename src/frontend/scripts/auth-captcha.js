const PLACEHOLDER_PATTERN = /REEMPLAZAR|YOUR_|SITE_KEY|CAPTCHA/i;

const SCRIPT_SRC_BY_PROVIDER = {
  turnstile: "https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit",
  hcaptcha: "https://js.hcaptcha.com/1/api.js?render=explicit",
};

const GLOBAL_BY_PROVIDER = {
  turnstile: "turnstile",
  hcaptcha: "hcaptcha",
};
const CAPTCHA_DEBUG_PREFIX = "[captcha-debug]";

const logCaptchaDebug = (step, details = {}) => {
  console.info(`${CAPTCHA_DEBUG_PREFIX} ${step}`, details);
};

const warnCaptchaDebug = (step, details = {}) => {
  console.warn(`${CAPTCHA_DEBUG_PREFIX} ${step}`, details);
};

const summarizeError = (err) => ({
  name: err?.name || "",
  message: err?.message || String(err || ""),
  code: err?.code || "",
});

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
  if (window[globalName]) {
    logCaptchaDebug("script.alreadyLoaded", { provider, language });
    return window[globalName];
  }

  const existingScript = document.querySelector(
    `script[data-captcha-provider="${provider}"][data-captcha-language="${normalizeLanguage(language)}"]`
  );
  if (existingScript && window[globalName]) return window[globalName];
  if (!existingScript) {
    logCaptchaDebug("script.inject", { provider, language });
    const script = document.createElement("script");
    script.src = buildProviderScriptSrc(provider, language);
    script.async = true;
    script.defer = true;
    script.dataset.captchaProvider = provider;
    script.dataset.captchaLanguage = normalizeLanguage(language);
    document.head.appendChild(script);
  } else {
    logCaptchaDebug("script.waitExisting", { provider, language });
  }

  await new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      if (window[globalName]) {
        logCaptchaDebug("script.ready", {
          provider,
          language,
          ms: Date.now() - started,
        });
        resolve();
        return;
      }
      if (Date.now() - started > 12000) {
        const error = new Error(`No se pudo cargar captcha (${provider})`);
        console.error(`${CAPTCHA_DEBUG_PREFIX} script.timeout`, {
          provider,
          language,
          ms: Date.now() - started,
          ...summarizeError(error),
        });
        reject(error);
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
  logCaptchaDebug("init.start", {
    provider,
    language,
    containerId,
    errorId,
    hasContainer: Boolean(containerEl),
    hasErrorEl: Boolean(errorEl),
    hasSiteKey: Boolean(siteKey),
  });

  const controller = {
    provider,
    enabled: false,
    ready: false,
    getToken: () => "",
    ensureToken: () => null,
    reset: () => {},
  };

  if (!containerEl) {
    warnCaptchaDebug("init.missingContainer", { containerId });
    return controller;
  }
  if (!isValidSiteKey(siteKey)) {
    setInlineError(errorEl, "Captcha no configurado. Agrega data-captcha-site-key en el body.");
    warnCaptchaDebug("init.invalidSiteKey", {
      provider,
      language,
      containerId,
      siteKeyPreview: String(siteKey || "").slice(0, 8),
    });
    return controller;
  }

  let widgetId = null;
  let token = "";

  try {
    const api = await loadProviderScript(provider, language);
    logCaptchaDebug("render.start", { provider, language, containerId });
    const options = {
      sitekey: siteKey,
      callback: (nextToken) => {
        token = String(nextToken || "");
        setInlineError(errorEl, "");
        logCaptchaDebug("token.received", {
          provider,
          containerId,
          tokenLength: token.length,
        });
      },
      "expired-callback": () => {
        token = "";
        warnCaptchaDebug("token.expired", { provider, containerId });
      },
      "error-callback": () => {
        token = "";
        setInlineError(errorEl, "No se pudo validar el captcha. Intenta de nuevo.");
        warnCaptchaDebug("token.errorCallback", { provider, containerId });
      },
    };
    if (provider === "hcaptcha") options.hl = language;
    if (provider === "turnstile") options.language = language;
    widgetId = api.render(containerEl, options);
    logCaptchaDebug("render.done", {
      provider,
      containerId,
      widgetId,
    });

    controller.enabled = true;
    controller.ready = true;
    controller.getToken = () => token;
    controller.ensureToken = () => {
      logCaptchaDebug("token.ensure", {
        provider,
        containerId,
        hasToken: Boolean(token),
        tokenLength: token.length,
      });
      if (token) return token;
      setInlineError(errorEl, "Completa el captcha para continuar.");
      return null;
    };
    controller.reset = () => {
      logCaptchaDebug("token.reset", {
        provider,
        containerId,
        widgetId,
      });
      token = "";
      try {
        api.reset(widgetId);
      } catch (_err) {
        // noop
      }
    };
  } catch (err) {
    console.error("captcha init error", err);
    console.error(`${CAPTCHA_DEBUG_PREFIX} init.error`, {
      provider,
      language,
      containerId,
      ...summarizeError(err),
    });
    setInlineError(errorEl, "No se pudo cargar el captcha.");
  }

  logCaptchaDebug("init.done", {
    provider,
    containerId,
    enabled: controller.enabled,
    ready: controller.ready,
  });
  return controller;
}
