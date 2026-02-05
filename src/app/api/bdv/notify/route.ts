import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.SUPABASE_URL || "";
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const webhookToken = process.env.BDV_WEBHOOK_TOKEN || "";

const supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const isNonEmptyString = (value: unknown) =>
  typeof value === "string" && value.trim().length > 0;

export async function POST(req: Request) {
  try {
    const auth = req.headers.get("authorization") || "";
    const expected = `Bearer ${webhookToken}`;
    if (!webhookToken || auth !== expected) {
      console.warn("BDV notify auth failed", { hasToken: !!webhookToken, auth });
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const rawBody = await req.text().catch(() => "");
    let rawText = rawBody;
    if (!isNonEmptyString(rawText)) {
      const body = await req.json().catch(() => null);
      if (body && typeof body === "object") {
        rawText = body.texto || body.text || "";
        if (!rawText) {
          try {
            rawText = JSON.stringify(body);
          } catch (_) {
            rawText = "";
          }
        }
      }
    }
    if (!isNonEmptyString(rawText)) {
      console.warn("BDV notify invalid payload", { body: rawBody });
      return NextResponse.json({ error: "Invalid payload" }, { status: 400 });
    }

    const app = "BDV";
    const titulo = "BDV";
    const texto = rawText;
    const fecha = new Date().toISOString();
    const dispositivo = "unknown";

    if (!supabaseUrl || !supabaseServiceRoleKey) {
      console.error("BDV notify error: missing Supabase envs");
      return NextResponse.json({ error: "Server misconfigured" }, { status: 500 });
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
    const { error } = await supabaseAdmin.from("pagomoviles").insert({
      app,
      titulo,
      texto,
      fecha,
      dispositivo,
      monto_bs: monto,
    });
    if (error) {
      console.error("BDV notify insert error", error);
      return NextResponse.json({ error: "Internal error" }, { status: 500 });
    }

    console.log("BDV notify stored");
    return NextResponse.json({ ok: true });
  } catch (err) {
    console.error("BDV notify error", err);
    return NextResponse.json({ error: "Internal error" }, { status: 500 });
  }
}
