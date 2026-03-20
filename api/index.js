const app = require("../src/backend/app");

const normalizeProxyBase = (value = "") => {
  const raw = String(value || "").trim().replace(/\/+$/, "");
  if (!raw) return "";
  if (!/^https?:\/\//i.test(raw)) return "";
  return raw;
};

const WHATSAPP_PROXY_BASE = normalizeProxyBase(
  process.env.WHATSAPP_PROXY_BASE || process.env.WHATSAPP_BOT_BASE_URL,
);

const hasWhatsappProxyTarget = () => Boolean(WHATSAPP_PROXY_BASE);

const isWhatsappApiPath = (url = "") => /^\/api\/whatsapp(?:\/|$|\?)/i.test(String(url || ""));

const shouldBypassProxyForSameHost = (req) => {
  if (!hasWhatsappProxyTarget()) return true;
  try {
    const upstreamHost = new URL(WHATSAPP_PROXY_BASE).host.toLowerCase();
    const requestHost = String(req?.headers?.host || "").toLowerCase();
    return Boolean(upstreamHost && requestHost && upstreamHost === requestHost);
  } catch (_err) {
    return false;
  }
};

const collectRequestBody = async (req) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolve(chunks.length ? Buffer.concat(chunks) : null));
    req.on("error", reject);
  });

const buildUpstreamHeaders = (req, bodyToSend) => {
  const headers = new Headers();
  Object.entries(req.headers || {}).forEach(([key, value]) => {
    const headerName = String(key || "").toLowerCase();
    if (!headerName) return;
    if (headerName === "host" || headerName === "connection" || headerName === "content-length") {
      return;
    }
    if (Array.isArray(value)) {
      value.forEach((item) => headers.append(headerName, String(item)));
      return;
    }
    if (value != null) headers.set(headerName, String(value));
  });

  if (bodyToSend && !headers.has("content-type")) {
    headers.set("content-type", "application/json; charset=utf-8");
  }

  return headers;
};

const proxyWhatsappRequest = async (req, res) => {
  const upstreamUrl = new URL(String(req.url || ""), WHATSAPP_PROXY_BASE).toString();
  const method = String(req.method || "GET").toUpperCase();
  const shouldIncludeBody = !["GET", "HEAD"].includes(method);

  let bodyToSend = null;
  if (shouldIncludeBody) {
    if (req.body != null) {
      if (Buffer.isBuffer(req.body) || typeof req.body === "string") {
        bodyToSend = req.body;
      } else {
        bodyToSend = JSON.stringify(req.body);
      }
    } else {
      bodyToSend = await collectRequestBody(req);
    }
  }

  const upstreamResponse = await fetch(upstreamUrl, {
    method,
    headers: buildUpstreamHeaders(req, bodyToSend),
    body: shouldIncludeBody ? bodyToSend : undefined,
    redirect: "manual",
  });

  res.statusCode = upstreamResponse.status;

  upstreamResponse.headers.forEach((value, key) => {
    const headerName = String(key || "").toLowerCase();
    if (headerName === "transfer-encoding" || headerName === "content-length") return;
    res.setHeader(key, value);
  });

  const setCookies = upstreamResponse.headers.getSetCookie?.() || [];
  if (setCookies.length) {
    res.setHeader("set-cookie", setCookies);
  }

  const payload = Buffer.from(await upstreamResponse.arrayBuffer());
  res.setHeader("content-length", String(payload.length));
  res.end(payload);
};

module.exports = async (req, res) => {
  if (hasWhatsappProxyTarget() && !shouldBypassProxyForSameHost(req) && isWhatsappApiPath(req.url)) {
    try {
      await proxyWhatsappRequest(req, res);
      return;
    } catch (err) {
      res.statusCode = 502;
      res.setHeader("content-type", "application/json; charset=utf-8");
      res.end(
        JSON.stringify({
          error: "No se pudo contactar el servidor del bot de WhatsApp",
          details: err?.message || String(err),
        }),
      );
      return;
    }
  }

  return app(req, res);
};
