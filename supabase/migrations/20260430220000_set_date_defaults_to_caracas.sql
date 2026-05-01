-- Keep business DATE defaults aligned with Venezuela time instead of UTC.
alter table public.carritos
  alter column fecha_creacion set default ((now() at time zone 'America/Caracas')::date);

alter table public.historial_reemplazos
  alter column fecha set default ((now() at time zone 'America/Caracas')::date);

alter table public.ordenes
  alter column fecha set default ((now() at time zone 'America/Caracas')::date);
