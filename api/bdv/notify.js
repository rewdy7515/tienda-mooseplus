import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.SUPABASE_URL || "";
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const webhookToken = process.env.BDV_WEBHOOK_TOKEN || "";

const supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const isNonEmptyString = (value) => typeof value === "string" && value.trim().length > 0;

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const auth = req.headers.authorization || "";
    const expected = `Bearer ${webhookToken}`;
    if (!webhookToken || auth !== expected) {
      console.warn("BDV notify auth failed", { hasToken: !!webhookToken });
      return res.status(401).json({ error: "Unauthorized" });
    }

    let rawText = "";
    if (typeof req.body === "string") {
      rawText = req.body;
    } else if (req.body && typeof req.body === "object") {
      rawText = req.body.texto || req.body.text || "";
      if (!rawText) {
        try {
          rawText = JSON.stringify(req.body);
        } catch (_) {
          rawText = "";
        }
      }
    }
    if (!isNonEmptyString(rawText)) {
      console.warn("BDV notify invalid payload", { body: req.body });
      return res.status(400).json({ error: "Invalid payload" });
    }

    const app = "BDV";
    const titulo = "BDV";
    const texto = rawText;
    const fecha = new Date().toISOString();
    const dispositivo = "unknown";

    if (!supabaseUrl || !supabaseServiceRoleKey) {
      console.error("BDV notify error: missing Supabase envs");
      return res.status(500).json({ error: "Server misconfigured" });
    }

    const hash = crypto
      .createHash("sha256")
      .update([app, titulo, texto, fecha, dispositivo].join("|"))
      .digest("hex");

    const { data: exists, error: existsErr } = await supabaseAdmin
      .from("pagomoviles")
      .select("hash")
      .eq("hash", hash)
      .maybeSingle();
    if (existsErr) throw existsErr;
    if (exists?.hash) {
      return res.status(200).json({ ok: true, duplicado: true });
    }

    const montoMatch =
      texto.match(/Bs\.?\s*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]+)?)/i)?.[1] ||
      texto.match(/Bs\.?\s*([0-9]+(?:[.,][0-9]+)?)/i)?.[1] ||
      texto.match(/Bs\.?\s*(0[.,][0-9]+)/i)?.[1] ||
      null;
    const normalizeMonto = (val) => {
      if (!val) return null;
      const raw = String(val).trim();
      if (raw.includes(".") && raw.includes(",")) {
        return raw.replace(/\./g, "").replace(",", ".");
      }
      return raw.replace(",", ".");
    };
    const monto = normalizeMonto(montoMatch);

    console.log("BDV notify received", { app, titulo, fecha, dispositivo, monto });
    const { error: insErr } = await supabaseAdmin.from("pagomoviles").insert({
      app,
      titulo,
      texto,
      fecha,
      dispositivo,
      hash,
      monto_bs: monto,
    });
    if (insErr) {
      console.error("BDV notify insert error", insErr);
      return res.status(500).json({ error: "Internal error" });
    }

    console.log("BDV notify stored");
    return res.status(200).json({ ok: true, duplicado: false });
  } catch (err) {
    console.error("BDV notify error", err);
    return res.status(500).json({ error: "Internal error" });
  }
}
