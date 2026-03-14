# Deploy en Hetzner

Servidor objetivo: `root@89.167.65.115`

Este proyecto corre con Node.js y Express. El proceso principal arranca con `npm start` y lee variables desde `config/.env`.

## 1. Entrar al servidor

```bash
ssh root@89.167.65.115
```

## 2. Instalar dependencias del sistema

```bash
apt update
apt install -y nginx git curl
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs
npm install -g pm2
```

Verifica versiones:

```bash
node -v
npm -v
pm2 -v
```

## 3. Clonar el proyecto

```bash
mkdir -p /var/www
cd /var/www
git clone TU_REPO_AQUI tienda-mooseplus-main
cd tienda-mooseplus-main
```

Si ya existe el repo:

```bash
cd /var/www/tienda-mooseplus-main
git pull origin main
```

## 4. Crear variables de entorno

Este repo usa `config/.env`. Copia la plantilla y rellena las claves reales:

```bash
cd /var/www/tienda-mooseplus-main
cp config/.env.example config/.env
nano config/.env
```

Valores mínimos que debes definir:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `PUBLIC_SITE_URL`
- `CORS_ORIGINS`
- `SESSION_COOKIE_SECRET`
- `SIGNUP_TOKEN_SECRET`
- `BDV_WEBHOOK_TOKEN`

Si no vas a usar WhatsApp Web en el servidor, deja:

```env
ENABLE_WHATSAPP=false
```

## 5. Instalar dependencias del proyecto

```bash
cd /var/www/tienda-mooseplus-main
npm install
```

## 6. Probar arranque manual

```bash
cd /var/www/tienda-mooseplus-main
npm start
```

Debes ver algo como:

```text
Servidor escuchando en puerto 3000
```

Detén el proceso con `Ctrl+C`.

## 7. Levantar con PM2

Ya está incluido `ecosystem.config.js` en el repo.

```bash
cd /var/www/tienda-mooseplus-main
pm2 start ecosystem.config.js
pm2 save
pm2 startup
```

Comandos útiles:

```bash
pm2 status
pm2 logs mooseplus
pm2 restart mooseplus
```

## 8. Configurar Nginx

Copia la plantilla del repo:

```bash
cp /var/www/tienda-mooseplus-main/deploy/nginx-mooseplus.conf.example /etc/nginx/sites-available/mooseplus
ln -s /etc/nginx/sites-available/mooseplus /etc/nginx/sites-enabled/mooseplus
nginx -t
systemctl reload nginx
```

Si vas a usar un dominio real, cambia `server_name` antes de recargar Nginx.

## 9. Activar HTTPS

Con dominio apuntando al servidor:

```bash
apt install -y certbot python3-certbot-nginx
certbot --nginx -d mooseplus.com -d www.mooseplus.com
```

Si todavía no tienes dominio, puedes probar primero por IP:

```text
http://89.167.65.115
```

## 10. Flujo de actualización

```bash
cd /var/www/tienda-mooseplus-main
git pull origin main
npm install
pm2 restart mooseplus
```

## 11. Deploy automatico con GitHub Actions

Ya está incluido el workflow:

- `.github/workflows/deploy-hetzner.yml`

Hace deploy cuando empujas cambios a `main` y también se puede lanzar manualmente desde GitHub.

### Secretos que debes crear en GitHub

En tu repositorio de GitHub entra a:

`Settings > Secrets and variables > Actions`

Crea estos secretos:

- `HETZNER_HOST`: `89.167.65.115`
- `HETZNER_USER`: `root`
- `HETZNER_PORT`: `22`
- `HETZNER_SSH_KEY`: tu clave privada SSH para entrar al servidor

### Preparar acceso SSH por clave

En tu Mac, si todavía no tienes una clave dedicada:

```bash
ssh-keygen -t ed25519 -C "github-actions-deploy" -f ~/.ssh/hetzner_actions
```

Sube la clave publica al servidor:

```bash
ssh-copy-id -i ~/.ssh/hetzner_actions.pub root@89.167.65.115
```

Luego copia el contenido de la clave privada:

```bash
cat ~/.ssh/hetzner_actions
```

Ese contenido completo va en el secreto `HETZNER_SSH_KEY`.

### Como funciona el workflow

En cada `push` a `main`, GitHub hace esto en el servidor:

```bash
cd /var/www/tienda-mooseplus-main
git pull --ff-only origin main
npm ci
pm2 restart mooseplus --update-env
```

Si el proceso `mooseplus` no existe todavía, lo crea con `ecosystem.config.js`.

### Primer arranque

Antes del primer deploy automatico, deja la aplicacion funcionando al menos una vez en el servidor:

```bash
cd /var/www/tienda-mooseplus-main
npm install
pm2 start ecosystem.config.js
pm2 save
```

Asi GitHub Actions ya solo tendra que actualizar y reiniciar.

## 12. Archivos del repo preparados para este deploy

- `ecosystem.config.js`
- `config/.env.example`
- `deploy/nginx-mooseplus.conf.example`
- `.github/workflows/deploy-hetzner.yml`

## 13. Notas importantes para este proyecto

- El backend usa Supabase, así que la base de datos sigue en Supabase; Hetzner solo hospeda la aplicación.
- El backend toma el puerto desde `PORT` y por defecto usa `3000`.
- La app aplica CORS según `CORS_ORIGINS`; si cambias dominio o subdominio, actualiza esa variable.
- Si activas `ENABLE_WHATSAPP=true`, probablemente necesitarás instalar dependencias extra del navegador para `whatsapp-web.js`.
