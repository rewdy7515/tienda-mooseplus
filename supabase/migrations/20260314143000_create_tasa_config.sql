create table if not exists public.tasa_config (
  id smallint primary key check (id = 1),
  markup double precision not null default 1.06,
  actualizado_en timestamp with time zone not null default now(),
  actualizado_por bigint null references public.usuarios(id_usuario)
);

insert into public.tasa_config (id, markup)
values (1, 1.06)
on conflict (id) do nothing;
