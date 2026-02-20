-- Cola SQL para notificaciones "Nuevo servicio" procesadas por backend JS.
-- Objetivo: mantener el formato en una sola parte (notification-templates-core.js).

BEGIN;

CREATE TABLE IF NOT EXISTS public.eventos_notificacion_nuevo_servicio (
  id_evento bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_venta bigint NOT NULL,
  id_usuario bigint NOT NULL,
  id_cuenta bigint,
  id_plataforma bigint,
  plataforma text,
  correo_cuenta text,
  perfil text,
  fecha_corte date,
  procesado boolean NOT NULL DEFAULT false,
  procesado_en timestamp with time zone,
  ultimo_error text,
  creado_en timestamp with time zone NOT NULL DEFAULT now(),
  actualizado_en timestamp with time zone NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_eventos_notif_nuevo_servicio_id_venta
  ON public.eventos_notificacion_nuevo_servicio(id_venta);

CREATE INDEX IF NOT EXISTS idx_eventos_notif_nuevo_servicio_procesado
  ON public.eventos_notificacion_nuevo_servicio(procesado, id_evento);

CREATE OR REPLACE FUNCTION public.enqueue_nuevo_servicio_event(p_id_venta bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_venta record;
  v_plat_id bigint;
  v_plat_nombre text;
  v_correo text;
  v_perfil text;
BEGIN
  IF p_id_venta IS NULL THEN
    RETURN;
  END IF;

  SELECT v.id_venta, v.id_usuario, v.id_cuenta, v.id_perfil, v.id_precio, v.fecha_corte, v.correo_miembro
  INTO v_venta
  FROM public.ventas v
  WHERE v.id_venta = p_id_venta
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  IF v_venta.id_usuario IS NULL THEN
    RETURN;
  END IF;

  -- Solo en ventas ya entregadas.
  IF EXISTS (
    SELECT 1
    FROM public.ventas x
    WHERE x.id_venta = p_id_venta
      AND (x.pendiente IS TRUE OR x.id_cuenta IS NULL)
  ) THEN
    RETURN;
  END IF;

  SELECT COALESCE(pr.id_plataforma, c.id_plataforma)
  INTO v_plat_id
  FROM public.ventas v
  LEFT JOIN public.precios pr ON pr.id_precio = v.id_precio
  LEFT JOIN public.cuentas c ON c.id_cuenta = v.id_cuenta
  WHERE v.id_venta = p_id_venta;

  IF v_plat_id IS NOT NULL THEN
    SELECT nombre INTO v_plat_nombre
    FROM public.plataformas
    WHERE id_plataforma = v_plat_id
    LIMIT 1;
  END IF;

  v_correo := NULL;
  SELECT COALESCE(v_venta.correo_miembro, c.correo)
  INTO v_correo
  FROM public.cuentas c
  WHERE c.id_cuenta = v_venta.id_cuenta
  LIMIT 1;

  v_perfil := NULL;
  IF v_venta.id_perfil IS NOT NULL THEN
    SELECT
      CASE
        WHEN p.n_perfil IS NOT NULL THEN 'M' || p.n_perfil::text
        ELSE NULL
      END
    INTO v_perfil
    FROM public.perfiles p
    WHERE p.id_perfil = v_venta.id_perfil
    LIMIT 1;
  END IF;

  INSERT INTO public.eventos_notificacion_nuevo_servicio (
    id_venta,
    id_usuario,
    id_cuenta,
    id_plataforma,
    plataforma,
    correo_cuenta,
    perfil,
    fecha_corte,
    procesado,
    procesado_en,
    ultimo_error,
    creado_en,
    actualizado_en
  )
  VALUES (
    v_venta.id_venta,
    v_venta.id_usuario,
    v_venta.id_cuenta,
    v_plat_id,
    COALESCE(v_plat_nombre, CASE WHEN v_plat_id IS NOT NULL THEN 'Plataforma ' || v_plat_id::text ELSE NULL END),
    v_correo,
    v_perfil,
    v_venta.fecha_corte,
    false,
    NULL,
    NULL,
    now(),
    now()
  )
  ON CONFLICT (id_venta)
  DO UPDATE SET
    id_usuario = EXCLUDED.id_usuario,
    id_cuenta = EXCLUDED.id_cuenta,
    id_plataforma = EXCLUDED.id_plataforma,
    plataforma = EXCLUDED.plataforma,
    correo_cuenta = EXCLUDED.correo_cuenta,
    perfil = EXCLUDED.perfil,
    fecha_corte = EXCLUDED.fecha_corte,
    procesado = false,
    procesado_en = NULL,
    ultimo_error = NULL,
    actualizado_en = now();
END;
$$;

-- Trigger para encolar cuando una venta pendiente pasa a entregada.
-- Evita depender de editar cada rama de asignación.
CREATE OR REPLACE FUNCTION public.trg_enqueue_nuevo_servicio_from_ventas()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_entrega_inmediata boolean := false;
BEGIN
  -- Debe ser transición real de pendiente->entregado con cuenta asignada.
  IF NOT (OLD.pendiente IS TRUE AND NEW.pendiente IS FALSE) THEN
    RETURN NEW;
  END IF;
  IF NEW.id_cuenta IS NULL THEN
    RETURN NEW;
  END IF;

  -- Solo plataformas de entrega inmediata.
  SELECT (pl.entrega_inmediata = true)
  INTO v_entrega_inmediata
  FROM public.precios pr
  JOIN public.plataformas pl ON pl.id_plataforma = pr.id_plataforma
  WHERE pr.id_precio = NEW.id_precio
  LIMIT 1;

  IF COALESCE(v_entrega_inmediata, false) = false THEN
    RETURN NEW;
  END IF;

  PERFORM public.enqueue_nuevo_servicio_event(NEW.id_venta);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_ventas_enqueue_nuevo_servicio ON public.ventas;
CREATE TRIGGER trg_ventas_enqueue_nuevo_servicio
AFTER UPDATE OF pendiente, id_cuenta, id_perfil, fecha_corte ON public.ventas
FOR EACH ROW
EXECUTE FUNCTION public.trg_enqueue_nuevo_servicio_from_ventas();

COMMIT;

-- Backfill opcional: encola ventas ya entregadas (entrega inmediata) que aún no
-- tengan evento en cola. Úsalo una sola vez si quieres recuperar historial.
--
-- INSERT INTO public.eventos_notificacion_nuevo_servicio (
--   id_venta, id_usuario, id_cuenta, id_plataforma, plataforma, correo_cuenta, perfil, fecha_corte, procesado
-- )
-- SELECT
--   v.id_venta,
--   v.id_usuario,
--   v.id_cuenta,
--   pl.id_plataforma,
--   pl.nombre,
--   COALESCE(v.correo_miembro, c.correo) AS correo_cuenta,
--   CASE WHEN pf.n_perfil IS NOT NULL THEN 'M' || pf.n_perfil::text ELSE NULL END AS perfil,
--   v.fecha_corte,
--   false
-- FROM public.ventas v
-- JOIN public.precios pr ON pr.id_precio = v.id_precio
-- JOIN public.plataformas pl ON pl.id_plataforma = pr.id_plataforma
-- LEFT JOIN public.cuentas c ON c.id_cuenta = v.id_cuenta
-- LEFT JOIN public.perfiles pf ON pf.id_perfil = v.id_perfil
-- WHERE v.pendiente = false
--   AND v.id_cuenta IS NOT NULL
--   AND pl.entrega_inmediata = true
-- ON CONFLICT (id_venta) DO NOTHING;

-- Nota:
-- Con esto, SQL solo detecta/encola eventos.
-- El backend JS (worker) crea la notificación usando el mismo formato centralizado.
