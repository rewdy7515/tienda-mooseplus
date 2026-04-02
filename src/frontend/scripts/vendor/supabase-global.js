import "./supabase.umd.js";

const supabaseGlobal = globalThis?.supabase;

if (!supabaseGlobal || typeof supabaseGlobal.createClient !== "function") {
  throw new Error("No se pudo inicializar @supabase/supabase-js desde bundle local.");
}

export const createClient = (...args) => supabaseGlobal.createClient(...args);
