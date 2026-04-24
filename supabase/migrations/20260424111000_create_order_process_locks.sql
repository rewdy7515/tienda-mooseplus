-- Shared lock to prevent the same order from being processed twice
-- across multiple backend instances.

CREATE TABLE IF NOT EXISTS public.order_process_locks (
  id_orden bigint PRIMARY KEY,
  owner text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION public.acquire_order_process_lock(
  p_order_id bigint,
  p_owner text DEFAULT NULL,
  p_stale_seconds integer DEFAULT 600
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_now timestamp with time zone := now();
  v_affected integer := 0;
  v_owner text := nullif(trim(coalesce(p_owner, '')), '');
  v_stale_seconds integer := GREATEST(COALESCE(p_stale_seconds, 600), 60);
BEGIN
  IF p_order_id IS NULL OR p_order_id <= 0 THEN
    RETURN false;
  END IF;

  INSERT INTO public.order_process_locks (id_orden, owner, created_at, updated_at)
  VALUES (p_order_id, v_owner, v_now, v_now)
  ON CONFLICT (id_orden) DO UPDATE
    SET owner = EXCLUDED.owner,
        updated_at = EXCLUDED.updated_at
  WHERE public.order_process_locks.updated_at < (v_now - make_interval(secs => v_stale_seconds));

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN v_affected > 0;
END;
$$;

CREATE OR REPLACE FUNCTION public.release_order_process_lock(
  p_order_id bigint,
  p_owner text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_owner text := nullif(trim(coalesce(p_owner, '')), '');
  v_affected integer := 0;
BEGIN
  IF p_order_id IS NULL OR p_order_id <= 0 THEN
    RETURN false;
  END IF;

  DELETE FROM public.order_process_locks
  WHERE id_orden = p_order_id
    AND (v_owner IS NULL OR owner = v_owner);

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN v_affected > 0;
END;
$$;

GRANT EXECUTE ON FUNCTION public.acquire_order_process_lock(bigint, text, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.release_order_process_lock(bigint, text) TO anon, authenticated, service_role;
