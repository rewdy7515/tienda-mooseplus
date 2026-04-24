-- Ensure adding gift-card pins only inserts into tarjetas_de_regalo.
-- This disables push-queue side effects and auto-delivery side effects.

-- A) Disable enqueue to web push queue from notificaciones
DROP TRIGGER IF EXISTS trg_enqueue_web_push_notification ON public.notificaciones;

CREATE OR REPLACE FUNCTION public.enqueue_web_push_notification()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- intentionally disabled
  RETURN NEW;
END;
$$;

-- B) Disable auto-delivery trigger on gift-card insert
-- (prevents insert into notificaciones + sales mutation/deletion while adding pins)
DROP TRIGGER IF EXISTS trg_auto_deliver_pending_giftcard_sale_on_insert ON public.tarjetas_de_regalo;
