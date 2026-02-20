-- Cancela automáticamente órdenes no verificadas con más de 3 horas.
-- Nota: un TRIGGER no se ejecuta por reloj; para esto se usa pg_cron.
-- Programado para correr diariamente a las 12:00 AM hora Venezuela
-- (equivale a 04:00 UTC, ya que Venezuela es UTC-4 sin DST).

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE OR REPLACE FUNCTION public.cancelar_ordenes_no_verificadas_3h()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_now_vz timestamp without time zone := now() AT TIME ZONE 'America/Caracas';
  v_updated integer := 0;
BEGIN
  UPDATE public.ordenes o
  SET orden_cancelada = true
  WHERE COALESCE(o.orden_cancelada, false) = false
    AND (o.pago_verificado = false OR o.pago_verificado IS NULL)
    AND (
      (COALESCE(o.fecha, v_now_vz::date)::timestamp + COALESCE(o.hora_orden, time '00:00:00'))
      <= (v_now_vz - interval '3 hours')
    );

  GET DIAGNOSTICS v_updated = ROW_COUNT;
  RETURN v_updated;
END;
$$;

-- Borra job previo si ya existe (idempotente).
DO $$
DECLARE
  v_job_id bigint;
BEGIN
  SELECT jobid
  INTO v_job_id
  FROM cron.job
  WHERE jobname = 'ordenes_autocancel_3h_daily_vz'
  LIMIT 1;

  IF v_job_id IS NOT NULL THEN
    PERFORM cron.unschedule(v_job_id);
  END IF;
END;
$$;

-- 04:00 UTC = 00:00 America/Caracas
SELECT cron.schedule(
  'ordenes_autocancel_3h_daily_vz',
  '0 4 * * *',
  $$SELECT public.cancelar_ordenes_no_verificadas_3h();$$
);

COMMIT;

-- Ejecución manual (opcional):
-- SELECT public.cancelar_ordenes_no_verificadas_3h();
