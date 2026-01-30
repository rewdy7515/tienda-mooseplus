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

    const body = await req.json().catch(() => null);
    const { app, titulo, texto, fecha, dispositivo } = body || {};
    if (
      !isNonEmptyString(app) ||
      !isNonEmptyString(titulo) ||
      !isNonEmptyString(texto) ||
      !isNonEmptyString(fecha) ||
      !isNonEmptyString(dispositivo)
    ) {
      console.warn("BDV notify invalid payload", { body });
      return NextResponse.json({ error: "Invalid payload" }, { status: 400 });
    }

    if (!supabaseUrl || !supabaseServiceRoleKey) {
      console.error("BDV notify error: missing Supabase envs");
      return NextResponse.json({ error: "Server misconfigured" }, { status: 500 });
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
    const { error } = await supabaseAdmin.from("pagomoviles").insert({
      app,
      titulo,
      texto,
      fecha,
      dispositivo,
      referencia: ref,
      fecha_pago: fechaPago,
      monto_bs: monto,
      num_telefono: telefono,
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
