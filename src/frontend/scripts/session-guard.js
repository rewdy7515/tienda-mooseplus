import { getSessionUserId, redirectIfSession } from "./session.js";

const params = new URLSearchParams(window.location.search || "");
const renewalToken = String(params.get("rr") || "").trim();
const sessionUserId = getSessionUserId();

if (sessionUserId && renewalToken) {
  const nextParams = new URLSearchParams();
  nextParams.set("rr", renewalToken);
  window.location.href = `cart.html?${nextParams.toString()}`;
} else {
  redirectIfSession();
}
