create table if not exists public.ordenes_items (
  id_item_orden bigint generated always as identity primary key,
  id_orden bigint not null references public.ordenes(id_orden),
  id_plataforma bigint references public.plataformas(id_plataforma),
  renovacion boolean,
  detalle text,
  monto_usd double precision,
  monto_bs real
);

create index if not exists ordenes_items_id_orden_idx
  on public.ordenes_items (id_orden);

create index if not exists ordenes_items_id_plataforma_idx
  on public.ordenes_items (id_plataforma);
