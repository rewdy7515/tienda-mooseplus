(function devRefreshDebugBootstrap() {
  if (typeof window === "undefined" || typeof document === "undefined") return;
  const host = String(window.location.hostname || "").trim().toLowerCase();
  const isLocalHost = host === "127.0.0.1" || host === "localhost";
  if (!isLocalHost) return;
  if (window.__DEV_REFRESH_DEBUG_INSTALLED__) return;
  window.__DEV_REFRESH_DEBUG_INSTALLED__ = true;

  const STORAGE_KEY = "dev_refresh_debug_log_v1";
  const MAX_ENTRIES = 80;
  const PREFIX = "[refresh-debug]";

  const nowIso = () => new Date().toISOString();

  const safeJson = (value) => {
    try {
      return JSON.stringify(value);
    } catch (_err) {
      return '"[unserializable]"';
    }
  };

  const truncate = (value, maxLen) => {
    const text = String(value == null ? "" : value);
    return text.length > maxLen ? `${text.slice(0, maxLen)}...` : text;
  };

  const sanitizeValue = (value, depth) => {
    if (depth > 2) return "[max-depth]";
    if (value == null) return value;
    if (typeof value === "string") return truncate(value, 500);
    if (typeof value === "number" || typeof value === "boolean") return value;
    if (value instanceof Error) {
      return {
        name: value.name || "Error",
        message: truncate(value.message || "", 300),
        stack: truncate(value.stack || "", 700),
      };
    }
    if (Array.isArray(value)) {
      return value.slice(0, 6).map((item) => sanitizeValue(item, depth + 1));
    }
    if (typeof value === "object") {
      const out = {};
      Object.keys(value)
        .slice(0, 12)
        .forEach((key) => {
          out[key] = sanitizeValue(value[key], depth + 1);
        });
      return out;
    }
    return truncate(String(value), 300);
  };

  const loadEntries = () => {
    try {
      const raw = window.sessionStorage.getItem(STORAGE_KEY);
      const parsed = raw ? JSON.parse(raw) : [];
      return Array.isArray(parsed) ? parsed : [];
    } catch (_err) {
      return [];
    }
  };

  const saveEntries = (entries) => {
    try {
      window.sessionStorage.setItem(STORAGE_KEY, JSON.stringify(entries.slice(-MAX_ENTRIES)));
    } catch (_err) {
      // noop
    }
  };

  const appendEntry = (type, details) => {
    const entry = {
      at: nowIso(),
      type: String(type || "event"),
      details: sanitizeValue(details || {}, 0),
      href: truncate(window.location.href || "", 500),
    };
    const entries = loadEntries();
    entries.push(entry);
    saveEntries(entries);
    try {
      console.warn(PREFIX, entry.type, entry.details);
    } catch (_err) {
      // noop
    }
    return entry;
  };

  const printBufferedEntries = () => {
    const entries = loadEntries();
    if (!entries.length) return;
    try {
      console.groupCollapsed(`${PREFIX} buffered entries: ${entries.length}`);
      entries.forEach((entry, index) => {
        console.log(`${index + 1}. ${entry.at} ${entry.type}`, entry.details);
      });
      console.groupEnd();
    } catch (_err) {
      // noop
    }
  };

  const getNavigationMeta = () => {
    const navEntry =
      typeof window.performance?.getEntriesByType === "function"
        ? window.performance.getEntriesByType("navigation")?.[0]
        : null;
    const legacyType = window.performance?.navigation?.type;
    return {
      navType: navEntry?.type || legacyType || "unknown",
      redirectCount: navEntry?.redirectCount ?? null,
      referrer: truncate(document.referrer || "", 400),
      visibility: document.visibilityState,
      readyState: document.readyState,
    };
  };

  const getStackSnippet = () => {
    try {
      const stack = new Error().stack || "";
      return truncate(
        stack
          .split("\n")
          .slice(2, 8)
          .join("\n"),
        800,
      );
    } catch (_err) {
      return "";
    }
  };

  const patchLocationMethod = (name) => {
    try {
      const proto = window.Location && window.Location.prototype;
      if (!proto) return;
      const original = proto[name];
      if (typeof original !== "function") return;
      if (original.__devRefreshWrapped) return;
      const wrapped = function patchedLocationMethod() {
        appendEntry(`location.${name}`, {
          args: Array.from(arguments).map((arg) => sanitizeValue(arg, 0)),
          stack: getStackSnippet(),
        });
        return original.apply(this, arguments);
      };
      wrapped.__devRefreshWrapped = true;
      Object.defineProperty(proto, name, {
        configurable: true,
        writable: true,
        value: wrapped,
      });
    } catch (err) {
      appendEntry("patch.location.failed", {
        method: name,
        error: err,
      });
    }
  };

  const patchHistoryMethod = (name) => {
    try {
      const original = window.history?.[name];
      if (typeof original !== "function") return;
      if (original.__devRefreshWrapped) return;
      const wrapped = function patchedHistoryMethod() {
        appendEntry(`history.${name}`, {
          args: Array.from(arguments).map((arg) => sanitizeValue(arg, 0)),
          stack: getStackSnippet(),
        });
        return original.apply(this, arguments);
      };
      wrapped.__devRefreshWrapped = true;
      window.history[name] = wrapped;
    } catch (err) {
      appendEntry("patch.history.failed", {
        method: name,
        error: err,
      });
    }
  };

  const patchEventSource = () => {
    const OriginalEventSource = window.EventSource;
    if (typeof OriginalEventSource !== "function") return;
    if (OriginalEventSource.__devRefreshWrapped) return;
    const DebugEventSource = function DebugEventSource(url, config) {
      const instance = new OriginalEventSource(url, config);
      appendEntry("eventsource.opened", {
        url,
        withCredentials: Boolean(config?.withCredentials),
      });
      instance.addEventListener("open", () => {
        appendEntry("eventsource.open", { url });
      });
      instance.addEventListener("error", () => {
        appendEntry("eventsource.error", { url });
      });
      instance.addEventListener("message", (event) => {
        const dataText = truncate(event?.data || "", 300);
        appendEntry("eventsource.message", {
          url,
          data: dataText,
          looksLikeReload: /reload|refresh|live/i.test(dataText),
        });
      });
      return instance;
    };
    DebugEventSource.__devRefreshWrapped = true;
    DebugEventSource.prototype = OriginalEventSource.prototype;
    Object.setPrototypeOf(DebugEventSource, OriginalEventSource);
    window.EventSource = DebugEventSource;
  };

  const patchWebSocket = () => {
    const OriginalWebSocket = window.WebSocket;
    if (typeof OriginalWebSocket !== "function") return;
    if (OriginalWebSocket.__devRefreshWrapped) return;
    const DebugWebSocket = function DebugWebSocket(url, protocols) {
      const instance =
        arguments.length > 1
          ? new OriginalWebSocket(url, protocols)
          : new OriginalWebSocket(url);
      appendEntry("websocket.created", {
        url,
        protocols: sanitizeValue(protocols, 0),
      });
      instance.addEventListener("open", () => {
        appendEntry("websocket.open", { url });
      });
      instance.addEventListener("close", (event) => {
        appendEntry("websocket.close", {
          url,
          code: event?.code ?? null,
          reason: truncate(event?.reason || "", 200),
          wasClean: Boolean(event?.wasClean),
        });
      });
      instance.addEventListener("error", () => {
        appendEntry("websocket.error", { url });
      });
      instance.addEventListener("message", (event) => {
        const dataText =
          typeof event?.data === "string"
            ? truncate(event.data, 400)
            : truncate(safeJson(sanitizeValue(event?.data, 0)), 400);
        appendEntry("websocket.message", {
          url,
          data: dataText,
          looksLikeReload: /reload|refresh|live/i.test(dataText),
        });
      });
      return instance;
    };
    DebugWebSocket.__devRefreshWrapped = true;
    DebugWebSocket.prototype = OriginalWebSocket.prototype;
    Object.getOwnPropertyNames(OriginalWebSocket).forEach((key) => {
      if (key in DebugWebSocket) return;
      try {
        Object.defineProperty(
          DebugWebSocket,
          key,
          Object.getOwnPropertyDescriptor(OriginalWebSocket, key),
        );
      } catch (_err) {
        // noop
      }
    });
    window.WebSocket = DebugWebSocket;
  };

  const installScriptObserver = () => {
    try {
      const observer = new MutationObserver((records) => {
        records.forEach((record) => {
          record.addedNodes.forEach((node) => {
            if (!(node instanceof HTMLScriptElement)) return;
            const src = String(node.src || "").trim();
            const looksLikeLiveReload = /reload|livereload|live-server/i.test(src);
            appendEntry("script.added", {
              src,
              inline: !src,
              looksLikeLiveReload,
            });
          });
        });
      });
      observer.observe(document.documentElement, {
        childList: true,
        subtree: true,
      });
    } catch (err) {
      appendEntry("observer.failed", { error: err });
    }
  };

  printBufferedEntries();
  appendEntry("script.boot", getNavigationMeta());

  patchLocationMethod("reload");
  patchLocationMethod("assign");
  patchLocationMethod("replace");
  patchHistoryMethod("pushState");
  patchHistoryMethod("replaceState");
  patchWebSocket();
  patchEventSource();
  installScriptObserver();

  window.addEventListener("beforeunload", () => {
    appendEntry("window.beforeunload", {
      visibility: document.visibilityState,
      readyState: document.readyState,
      stack: getStackSnippet(),
    });
  });

  window.addEventListener("pagehide", (event) => {
    appendEntry("window.pagehide", {
      persisted: Boolean(event?.persisted),
      visibility: document.visibilityState,
    });
  });

  window.addEventListener("pageshow", (event) => {
    appendEntry("window.pageshow", {
      persisted: Boolean(event?.persisted),
      visibility: document.visibilityState,
    });
  });

  window.addEventListener("load", () => {
    appendEntry("window.load", getNavigationMeta());
  });

  window.addEventListener("error", (event) => {
    appendEntry("window.error", {
      message: truncate(event?.message || "", 300),
      filename: truncate(event?.filename || "", 200),
      lineno: event?.lineno ?? null,
      colno: event?.colno ?? null,
    });
  });

  window.addEventListener("unhandledrejection", (event) => {
    appendEntry("window.unhandledrejection", {
      reason: sanitizeValue(event?.reason, 0),
    });
  });

  document.addEventListener("visibilitychange", () => {
    appendEntry("document.visibilitychange", {
      visibility: document.visibilityState,
    });
  });

  if (window.navigator?.serviceWorker) {
    window.navigator.serviceWorker.addEventListener("controllerchange", () => {
      appendEntry("serviceWorker.controllerchange", {});
    });
    window.navigator.serviceWorker.addEventListener("message", (event) => {
      appendEntry("serviceWorker.message", {
        data: sanitizeValue(event?.data, 0),
      });
    });
  }
})();
