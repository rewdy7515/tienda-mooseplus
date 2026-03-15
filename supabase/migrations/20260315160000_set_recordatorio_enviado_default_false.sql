ALTER TABLE public.ventas
ALTER COLUMN recordatorio_enviado SET DEFAULT false;

UPDATE public.ventas
SET recordatorio_enviado = false
WHERE recordatorio_enviado IS NULL;
