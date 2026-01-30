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

    const { app, titulo, texto, fecha, dispositivo } = req.body || {};
    if (
      !isNonEmptyString(app) ||
      !isNonEmptyString(titulo) ||
      !isNonEmptyString(texto) ||
      !isNonEmptyString(fecha) ||
      !isNonEmptyString(dispositivo)
    ) {
      console.warn("BDV notify invalid payload", { body: req.body });
      return res.status(400).json({ error: "Invalid payload" });
    }

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

    const ref = texto.match(/Ref:\s*([0-9]+)/i)?.[1] || null;
    const fechaRaw = texto.match(/fecha\s*([0-9]{2}-[0-9]{2}-[0-9]{2})/i)?.[1] || null;
    const monto = texto.match(/Bs\.?\s*([0-9]+,[0-9]+)/i)?.[1] || null;
    const telefono = texto.match(/del\s*([0-9]{4}-[0-9]{7})/i)?.[1] || null;
    const fechaPago = fechaRaw
      ? (() => {
          const [dd, mm, yy] = fechaRaw.split("-");
          const yyyy = Number(yy) <= 69 ? `20${yy}` : `19${yy}`;
          return `${yyyy}-${mm}-${dd}`;
        })()
      : null;

    console.log("BDV notify received", { app, titulo, fecha, dispositivo, ref, fechaPago, monto, telefono });
    const { error: insErr } = await supabaseAdmin.from("pagomoviles").insert({
      app,
      titulo,
      texto,
      fecha,
      dispositivo,
      hash,
      referencia: ref,
      fecha_pago: fechaPago,
      monto_bs: monto,
      num_telefono: telefono,
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
