BEGIN;

CREATE TABLE IF NOT EXISTS public.home_banners (
  id_banner bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  image_url text NOT NULL,
  redirect_url text NOT NULL,
  orden integer NOT NULL DEFAULT 0,
  activo boolean NOT NULL DEFAULT true,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_home_banners_activo_orden
  ON public.home_banners (activo, orden, id_banner);

CREATE OR REPLACE FUNCTION public.trg_set_home_banners_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_home_banners_updated_at ON public.home_banners;
CREATE TRIGGER trg_home_banners_updated_at
BEFORE UPDATE ON public.home_banners
FOR EACH ROW
EXECUTE FUNCTION public.trg_set_home_banners_updated_at();

COMMIT;
