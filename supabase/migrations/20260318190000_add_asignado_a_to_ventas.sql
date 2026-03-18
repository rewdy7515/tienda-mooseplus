alter table public.ventas
  add column if not exists asignado_a bigint;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ventas_asignado_a_fkey'
  ) then
    alter table public.ventas
      add constraint ventas_asignado_a_fkey
      foreign key (asignado_a) references public.usuarios(id_usuario);
  end if;
end $$;
