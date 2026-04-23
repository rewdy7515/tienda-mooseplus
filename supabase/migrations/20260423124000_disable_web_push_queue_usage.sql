-- Disable web push queue side effects from notifications.
-- This keeps inserts (e.g. gift cards/pines flows) from failing when queue tables are absent.

-- 1) Stop enqueue trigger on notificaciones
DROP TRIGGER IF EXISTS trg_enqueue_web_push_notification ON public.notificaciones;

-- 2) Replace enqueue function with a harmless no-op (safe if some process recreates trigger later)
CREATE OR REPLACE FUNCTION public.enqueue_web_push_notification()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- web push queue intentionally disabled
  RETURN NEW;
END;
$$;

-- 3) Optional cleanup (kept safe with IF EXISTS)
DROP TRIGGER IF EXISTS trg_touch_web_push_subscription_updated_at ON public.web_push_subscriptions;
DROP FUNCTION IF EXISTS public.touch_web_push_subscription_updated_at();

DROP TABLE IF EXISTS public.web_push_delivery_queue;
DROP TABLE IF EXISTS public.web_push_subscriptions;
