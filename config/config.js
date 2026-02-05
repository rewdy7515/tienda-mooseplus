const path = require("path");
const dotenv = require("dotenv");

// Carga variables de entorno desde config/.env por defecto
dotenv.config({
  path: path.resolve(__dirname, ".env"),
});

const {
  SUPABASE_URL,
  SUPABASE_ANON_KEY,
  SUPABASE_PUBLISHABLE_KEY,
  SUPABASE_SERVICE_ROLE_KEY,
  PORT,
} = process.env;

module.exports = {
  supabaseUrl: SUPABASE_URL,
  supabaseAnonKey: SUPABASE_ANON_KEY || SUPABASE_PUBLISHABLE_KEY,
  supabaseServiceRoleKey: SUPABASE_SERVICE_ROLE_KEY,
  port: PORT || 3000,
};
