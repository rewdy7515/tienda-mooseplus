alter table public.historial_ventas
  add column if not exists id_tarjeta_de_regalo bigint;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'historial_ventas_id_tarjeta_de_regalo_fkey'
  ) then
    alter table public.historial_ventas
      add constraint historial_ventas_id_tarjeta_de_regalo_fkey
      foreign key (id_tarjeta_de_regalo)
      references public.tarjetas_de_regalo(id_tarjeta_de_regalo);
  end if;
end $$;

create index if not exists historial_ventas_id_tarjeta_de_regalo_idx
  on public.historial_ventas (id_tarjeta_de_regalo);
