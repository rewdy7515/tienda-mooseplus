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

    console.log("BDV notify received", { app, titulo, fecha, dispositivo });
    const { error } = await supabaseAdmin.from("pagomoviles").insert({
      app,
      titulo,
      texto,
      fecha,
      dispositivo,
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
