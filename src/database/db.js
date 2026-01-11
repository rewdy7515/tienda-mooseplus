const { createClient } = require("@supabase/supabase-js");
const {
  supabaseUrl,
  supabaseAnonKey,
  supabaseServiceRoleKey,
} = require("../../config/config");

if (!supabaseUrl) {
  throw new Error("Falta SUPABASE_URL en las variables de entorno");
}

const supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const supabasePublic = createClient(supabaseUrl, supabaseAnonKey, {
  auth: { autoRefreshToken: false, persistSession: false },
});

module.exports = {
  supabaseAdmin,
  supabasePublic,
};
