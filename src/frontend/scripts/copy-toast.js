let toastEl = null;
let toastTimer = null;

export const ensureCopyToast = (text = "Copiado al portapapeles") => {
  if (toastEl && document.body.contains(toastEl)) {
    if (text) toastEl.textContent = text;
    return toastEl;
  }
  const existing = document.querySelector("#copy-toast");
  toastEl = existing || document.createElement("div");
  if (!existing) {
    toastEl.id = "copy-toast";
    toastEl.className = "copy-toast";
    document.body.appendChild(toastEl);
  }
  toastEl.textContent = text || "Copiado al portapapeles";
  return toastEl;
};

export const showCopyToast = (text) => {
  const el = ensureCopyToast(text);
  if (!el) return;
  el.classList.add("show");
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.remove("show"), 2000);
};

export const copyTextNotify = async (text, toastText) => {
  if (!text) return false;
  let copied = false;
  try {
    await navigator.clipboard.writeText(text);
    copied = true;
  } catch (_) {
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      ta.remove();
      copied = true;
    } catch (err) {
      console.error("copy error", err);
    }
  }
  if (copied) showCopyToast(toastText);
  return copied;
};
