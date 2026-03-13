module.exports = {
  apps: [
    {
      name: "mooseplus",
      script: "src/backend/app.js",
      cwd: "/var/www/tienda-mooseplus-main",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "500M",
      env: {
        NODE_ENV: "production",
        PORT: 3000,
      },
    },
  ],
};
