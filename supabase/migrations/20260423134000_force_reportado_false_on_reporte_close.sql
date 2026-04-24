-- Always clear ventas.reportado when a reporte is closed.
-- Applies to both manual and automatic closures.

CREATE OR REPLACE FUNCTION public.clear_venta_reportado_on_reporte_close()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Run only when reporte transitions to solved.
  IF NEW.solucionado IS TRUE AND COALESCE(OLD.solucionado, FALSE) IS DISTINCT FROM TRUE THEN
    IF NEW.id_venta IS NOT NULL THEN
      UPDATE public.ventas
      SET reportado = FALSE
      WHERE id_venta = NEW.id_venta;
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_clear_venta_reportado_on_reporte_close ON public.reportes;

CREATE TRIGGER trg_clear_venta_reportado_on_reporte_close
AFTER UPDATE ON public.reportes
FOR EACH ROW
EXECUTE FUNCTION public.clear_venta_reportado_on_reporte_close();
