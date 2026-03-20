const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const PROJECT_ROOT = path.resolve(__dirname, "..");
const DEFAULT_PROJECT_AUTH_PATH = path.join(PROJECT_ROOT, ".wwebjs_auth");
const DEFAULT_PROJECT_CACHE_PATH = path.join(PROJECT_ROOT, ".wwebjs_cache");
const DEFAULT_PROJECT_RUNTIME_ROOT = path.join(PROJECT_ROOT, ".mooseplus-runtime", "whatsapp");
const DEFAULT_PROJECT_BROWSER_CACHE_PATH = path.join(PROJECT_ROOT, ".puppeteer-cache");

const expandHomePath = (value = "") => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (raw === "~") {
    const home = String(os.homedir() || "").trim();
    return home || raw;
  }
  if (raw.startsWith("~/")) {
    const home = String(os.homedir() || "").trim();
    return home ? path.join(home, raw.slice(2)) : raw;
  }
  return raw;
};

const normalizeAbsolutePath = (value = "") => {
  const raw = expandHomePath(value);
  return raw ? path.resolve(raw) : "";
};

const inheritedPuppeteerCacheDir = normalizeAbsolutePath(process.env.PUPPETEER_CACHE_DIR);
const initialBrowserCacheDir =
  normalizeAbsolutePath(process.env.WHATSAPP_BROWSER_CACHE_DIR) ||
  (normalizeAbsolutePath(process.env.WHATSAPP_RUNTIME_DIR)
    ? path.join(normalizeAbsolutePath(process.env.WHATSAPP_RUNTIME_DIR), "browser-cache")
    : "") ||
  DEFAULT_PROJECT_BROWSER_CACHE_PATH;

// Fuerza a Puppeteer a no depender del HOME del proceso al cargar la configuración.
process.env.PUPPETEER_CACHE_DIR = initialBrowserCacheDir;

const { Client, LocalAuth } = require("whatsapp-web.js");
const puppeteer = require("puppeteer");
const {
  install: installBrowser,
  computeExecutablePath,
  computeSystemExecutablePath,
  detectBrowserPlatform,
  resolveBuildId,
  Browser,
  ChromeReleaseChannel,
} = require("@puppeteer/browsers");
const qrcode = require("qrcode-terminal");

let clientInstance = null;
let hasInitialized = false;
let initializePromise = null;
let latestQrRaw = "";
let latestQrAscii = "";
let latestQrUpdatedAt = null;
const readyListeners = new Set();
const disconnectedListeners = new Set();

const uniqPaths = (values = []) =>
  Array.from(
    new Set(
      (values || [])
        .map((value) => normalizeAbsolutePath(value))
        .filter(Boolean),
    ),
  );

const buildWhatsappStorageCandidates = () => {
  const customRuntimeRoot = normalizeAbsolutePath(process.env.WHATSAPP_RUNTIME_DIR);
  const customAuthPath = normalizeAbsolutePath(process.env.WHATSAPP_AUTH_PATH);
  const customCachePath = normalizeAbsolutePath(process.env.WHATSAPP_CACHE_PATH);
  const customBrowserCachePath = normalizeAbsolutePath(process.env.WHATSAPP_BROWSER_CACHE_DIR);
  const tmpRoot = path.join(os.tmpdir(), "mooseplus-runtime", "whatsapp");

  return {
    authPaths: uniqPaths([
      customAuthPath,
      customRuntimeRoot ? path.join(customRuntimeRoot, "auth") : "",
      DEFAULT_PROJECT_AUTH_PATH,
      path.join(DEFAULT_PROJECT_RUNTIME_ROOT, "auth"),
      path.join(tmpRoot, "auth"),
    ]),
    cachePaths: uniqPaths([
      customCachePath,
      customRuntimeRoot ? path.join(customRuntimeRoot, "cache") : "",
      DEFAULT_PROJECT_CACHE_PATH,
      path.join(DEFAULT_PROJECT_RUNTIME_ROOT, "cache"),
      path.join(tmpRoot, "cache"),
    ]),
    browserCachePaths: uniqPaths([
      customBrowserCachePath,
      customRuntimeRoot ? path.join(customRuntimeRoot, "browser-cache") : "",
      inheritedPuppeteerCacheDir,
      DEFAULT_PROJECT_BROWSER_CACHE_PATH,
      path.join(DEFAULT_PROJECT_RUNTIME_ROOT, "browser-cache"),
      path.join(os.tmpdir(), "puppeteer-cache", "mooseplus"),
    ]),
  };
};

const WHATSAPP_STORAGE_CANDIDATES = buildWhatsappStorageCandidates();
let resolvedWhatsappPaths = {
  authPath: WHATSAPP_STORAGE_CANDIDATES.authPaths[0] || DEFAULT_PROJECT_AUTH_PATH,
  cachePath: WHATSAPP_STORAGE_CANDIDATES.cachePaths[0] || DEFAULT_PROJECT_CACHE_PATH,
  browserCacheDir:
    WHATSAPP_STORAGE_CANDIDATES.browserCachePaths[0] || DEFAULT_PROJECT_BROWSER_CACHE_PATH,
};
let resolvedWhatsappPathsReady = false;
let resolvedChromeExecutablePath = "";
let chromeExecutableResolvePromise = null;
const WHATSAPP_AUTO_INSTALL_BROWSER =
  String(process.env.WHATSAPP_AUTO_INSTALL_BROWSER || "true").trim().toLowerCase() !== "false";

const pathExists = async (targetPath = "") => {
  try {
    await fs.access(targetPath);
    return true;
  } catch (_err) {
    return false;
  }
};

const ensureWritableDirFromCandidates = async (candidates = [], label = "dir") => {
  let lastErr = null;
  for (const candidate of candidates) {
    try {
      await fs.mkdir(candidate, { recursive: true });
      return candidate;
    } catch (err) {
      lastErr = err;
      console.warn(
        `[WhatsApp] No se pudo preparar ${label} en ${candidate}: ${err?.message || err}`,
      );
    }
  }
  throw lastErr || new Error(`No se pudo preparar un directorio para ${label}`);
};

const ensureWhatsappRuntimeDirs = async () => {
  if (resolvedWhatsappPathsReady) return resolvedWhatsappPaths;

  const authPath = await ensureWritableDirFromCandidates(
    WHATSAPP_STORAGE_CANDIDATES.authPaths,
    "auth",
  );
  const cachePath = await ensureWritableDirFromCandidates(
    WHATSAPP_STORAGE_CANDIDATES.cachePaths,
    "cache",
  );
  const browserCacheDir = await ensureWritableDirFromCandidates(
    WHATSAPP_STORAGE_CANDIDATES.browserCachePaths,
    "browser-cache",
  );

  resolvedWhatsappPaths = { authPath, cachePath, browserCacheDir };
  resolvedWhatsappPathsReady = true;
  process.env.PUPPETEER_CACHE_DIR = browserCacheDir;
  console.log(
    `[WhatsApp] Runtime paths auth=${resolvedWhatsappPaths.authPath} cache=${resolvedWhatsappPaths.cachePath} browser_cache=${resolvedWhatsappPaths.browserCacheDir}`,
  );
  return resolvedWhatsappPaths;
};

const resetWhatsappClientState = async ({
  destroyClient = false,
  reason = "reset",
} = {}) => {
  const client = clientInstance;
  hasInitialized = false;
  initializePromise = null;
  clientInstance = null;
  clearWhatsappQrState();
  if (!destroyClient || !client) return;
  try {
    await client.destroy();
  } catch (err) {
    console.error(`[WhatsApp] destroy error (${reason}):`, err);
  }
};

const clearWhatsappQrState = () => {
  latestQrRaw = "";
  latestQrAscii = "";
  latestQrUpdatedAt = null;
};

const setWhatsappQrState = (rawQr = "", asciiQr = "") => {
  latestQrRaw = String(rawQr || "");
  latestQrAscii = String(asciiQr || "");
  latestQrUpdatedAt = new Date().toISOString();
};

const getWhatsappQrState = () => ({
  raw: latestQrRaw || null,
  ascii: latestQrAscii || null,
  updatedAt: latestQrUpdatedAt || null,
});

const notifyReadyListeners = () => {
  readyListeners.forEach((listener) => {
    try {
      listener();
    } catch (err) {
      console.error("[WhatsApp] ready listener error:", err);
    }
  });
};

const notifyDisconnectedListeners = (reason) => {
  disconnectedListeners.forEach((listener) => {
    try {
      listener(reason);
    } catch (err) {
      console.error("[WhatsApp] disconnected listener error:", err);
    }
  });
};

const getConfiguredChromePathCandidates = () =>
  uniqPaths([
    process.env.WHATSAPP_CHROME_PATH,
    process.env.PUPPETEER_EXECUTABLE_PATH,
    process.env.GOOGLE_CHROME_BIN,
    process.env.CHROME_BIN,
  ]);

const getStaticChromePathCandidates = () =>
  uniqPaths([
    "/usr/bin/google-chrome-stable",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  ]);

const resolveChromeBuildId = async () => {
  const platform = detectBrowserPlatform();
  if (!platform) {
    throw new Error(
      `Plataforma no soportada para Chrome: ${process.platform} ${process.arch}`,
    );
  }
  const targetBuild = String(puppeteer.PUPPETEER_REVISIONS?.chrome || "stable").trim();
  const buildId = await resolveBuildId(Browser.CHROME, platform, targetBuild);
  return { platform, buildId };
};

const findSystemChromeExecutable = async () => {
  for (const candidate of getConfiguredChromePathCandidates()) {
    if (await pathExists(candidate)) return candidate;
  }
  try {
    return computeSystemExecutablePath({
      browser: Browser.CHROME,
      channel: ChromeReleaseChannel.STABLE,
    });
  } catch (_err) {
    // fallback to common static paths below
  }
  for (const candidate of getStaticChromePathCandidates()) {
    if (await pathExists(candidate)) return candidate;
  }
  return "";
};

const findCachedChromeExecutable = async (browserCacheDir) => {
  try {
    const { platform, buildId } = await resolveChromeBuildId();
    const executablePath = computeExecutablePath({
      cacheDir: browserCacheDir,
      browser: Browser.CHROME,
      buildId,
      platform,
    });
    return (await pathExists(executablePath)) ? executablePath : "";
  } catch (_err) {
    return "";
  }
};

const ensureChromeExecutable = async ({ browserCacheDir } = {}) => {
  if (chromeExecutableResolvePromise) return chromeExecutableResolvePromise;

  chromeExecutableResolvePromise = (async () => {
    if (resolvedChromeExecutablePath && (await pathExists(resolvedChromeExecutablePath))) {
      return resolvedChromeExecutablePath;
    }

    const systemChromePath = await findSystemChromeExecutable();
    if (systemChromePath) {
      resolvedChromeExecutablePath = systemChromePath;
      console.log(`[WhatsApp] Usando Chrome del sistema: ${resolvedChromeExecutablePath}`);
      return resolvedChromeExecutablePath;
    }

    const cachedChromePath = await findCachedChromeExecutable(browserCacheDir);
    if (cachedChromePath) {
      resolvedChromeExecutablePath = cachedChromePath;
      console.log(`[WhatsApp] Usando Chrome descargado: ${resolvedChromeExecutablePath}`);
      return resolvedChromeExecutablePath;
    }

    if (!WHATSAPP_AUTO_INSTALL_BROWSER) {
      throw new Error(
        `Chrome no disponible. Define PUPPETEER_EXECUTABLE_PATH/WHATSAPP_CHROME_PATH o instala el navegador con 'npx puppeteer browsers install chrome'. Cache actual: ${browserCacheDir}`,
      );
    }

    const { platform, buildId } = await resolveChromeBuildId();
    console.log(
      `[WhatsApp] Descargando Chrome ${buildId} en cache ${browserCacheDir}.`,
    );
    const installedBrowser = await installBrowser({
      browser: Browser.CHROME,
      buildId,
      cacheDir: browserCacheDir,
      platform,
    });
    resolvedChromeExecutablePath = installedBrowser.executablePath;
    console.log(`[WhatsApp] Chrome instalado: ${resolvedChromeExecutablePath}`);
    return resolvedChromeExecutablePath;
  })();

  try {
    return await chromeExecutableResolvePromise;
  } finally {
    chromeExecutableResolvePromise = null;
  }
};

const normalizeWhatsappStartupError = (err, { browserCacheDir = "" } = {}) => {
  const originalMessage = String(err?.message || err || "").trim();
  let message = originalMessage || "No se pudo iniciar Chrome para WhatsApp.";

  if (/Could not find Chrome/i.test(originalMessage)) {
    message =
      `Chrome no disponible para Puppeteer. Cache actual: ${browserCacheDir}. ` +
      "Se intento usar Chrome del sistema y luego el cache del proyecto. " +
      "Si continua fallando, define PUPPETEER_EXECUTABLE_PATH o instala el navegador manualmente.";
  } else if (
    /(EACCES|ENOENT)/i.test(originalMessage) &&
    /(puppeteer|browser-cache|mkdir|chrome)/i.test(originalMessage)
  ) {
    message =
      `${originalMessage} ` +
      "No se pudo preparar el cache o los archivos de Chrome para WhatsApp. " +
      "Define WHATSAPP_BROWSER_CACHE_DIR/WHATSAPP_RUNTIME_DIR a una ruta escribible del servidor.";
  } else if (
    /Failed to launch the browser process|error while loading shared libraries/i.test(
      originalMessage,
    )
  ) {
    message =
      `${originalMessage} ` +
      "El navegador existe pero el sistema no puede lanzarlo. " +
      "Instala las dependencias de Chrome/Chromium en Hetzner o define PUPPETEER_EXECUTABLE_PATH a un binario funcional.";
  }

  const normalizedErr = new Error(message);
  normalizedErr.code = err?.code || "WHATSAPP_STARTUP_FAILED";
  normalizedErr.cause = err;
  return normalizedErr;
};

const getWhatsappClient = () => {
  if (clientInstance) return clientInstance;
  const { authPath, cachePath } = resolvedWhatsappPaths;
  const puppeteerArgs =
    process.platform === "linux" && typeof process.getuid === "function" && process.getuid() === 0
      ? ["--no-sandbox", "--disable-setuid-sandbox"]
      : [];

  clientInstance = new Client({
    authStrategy: new LocalAuth({
      clientId: "mooseplus-admin",
      dataPath: authPath,
    }),
    webVersionCache: {
      type: "local",
      path: cachePath,
    },
    puppeteer: {
      executablePath: resolvedChromeExecutablePath || undefined,
      args: puppeteerArgs,
    },
  });

  clientInstance.on("qr", (qr) => {
    qrcode.generate(qr, { small: true }, (asciiQr) => {
      setWhatsappQrState(qr, asciiQr);
    });
    if (!latestQrRaw) {
      setWhatsappQrState(qr, "");
    }
    console.log("[WhatsApp] Escanea el QR para iniciar sesion.");
  });

  clientInstance.on("authenticated", () => {
    console.log("[WhatsApp] Sesion autenticada.");
    clearWhatsappQrState();
  });

  clientInstance.on("ready", () => {
    console.log("[WhatsApp] Cliente listo.");
    clearWhatsappQrState();
    notifyReadyListeners();
  });

  clientInstance.on("auth_failure", (msg) => {
    console.error("[WhatsApp] Error de autenticacion:", msg);
  });

  clientInstance.on("disconnected", async (reason) => {
    console.warn("[WhatsApp] Cliente desconectado:", reason);
    await resetWhatsappClientState({
      destroyClient: false,
      reason: "disconnected",
    });
    notifyDisconnectedListeners(reason);
  });

  return clientInstance;
};

const startWhatsappClient = async () => {
  if (initializePromise) return initializePromise;

  const runtimePaths = await ensureWhatsappRuntimeDirs();
  try {
    await ensureChromeExecutable({ browserCacheDir: runtimePaths.browserCacheDir });
  } catch (err) {
    throw normalizeWhatsappStartupError(err, {
      browserCacheDir: runtimePaths.browserCacheDir,
    });
  }
  const client = getWhatsappClient();
  if (hasInitialized) return client;

  hasInitialized = true;
  initializePromise = client
    .initialize()
    .then(() => client)
    .catch(async (err) => {
      await resetWhatsappClientState({
        destroyClient: true,
        reason: "initialize_error",
      });
      throw normalizeWhatsappStartupError(err, {
        browserCacheDir: runtimePaths.browserCacheDir,
      });
    })
    .finally(() => {
      initializePromise = null;
    });
  return initializePromise;
};

const isWhatsappReady = () => {
  return Boolean(clientInstance?.info?.wid?._serialized);
};

const isWhatsappClientActive = () => {
  return Boolean(isWhatsappReady() || hasInitialized || initializePromise);
};

const stopWhatsappClient = async () => {
  await resetWhatsappClientState({
    destroyClient: true,
    reason: "stop",
  });
};

const onWhatsappReady = (listener) => {
  if (typeof listener !== "function") return () => {};
  readyListeners.add(listener);
  return () => readyListeners.delete(listener);
};

const onWhatsappDisconnected = (listener) => {
  if (typeof listener !== "function") return () => {};
  disconnectedListeners.add(listener);
  return () => disconnectedListeners.delete(listener);
};

module.exports = {
  getWhatsappClient,
  startWhatsappClient,
  stopWhatsappClient,
  isWhatsappReady,
  isWhatsappClientActive,
  getWhatsappQrState,
  onWhatsappReady,
  onWhatsappDisconnected,
};
