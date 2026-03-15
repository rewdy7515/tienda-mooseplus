create table public.sandbox_giftcard_orders (
  id_sandbox_order bigint generated always as identity not null,
  id_usuario_cliente bigint,
  id_usuario_admin bigint not null,
  id_plataforma bigint not null,
  id_precio bigint,
  cantidad integer not null default 1,
  referencia text,
  estado text not null default 'simulada',
  total_usd numeric(12, 2) not null default 0,
  total_bs numeric(14, 2),
  tasa_bs numeric(14, 6),
  valor_tarjeta_de_regalo text,
  moneda text,
  notas text,
  payload jsonb,
  creado_en timestamp with time zone not null default now(),
  constraint sandbox_giftcard_orders_pkey primary key (id_sandbox_order),
  constraint sandbox_giftcard_orders_cantidad_check check (cantidad > 0),
  constraint sandbox_giftcard_orders_id_usuario_cliente_fkey
    foreign key (id_usuario_cliente) references public.usuarios(id_usuario),
  constraint sandbox_giftcard_orders_id_usuario_admin_fkey
    foreign key (id_usuario_admin) references public.usuarios(id_usuario),
  constraint sandbox_giftcard_orders_id_plataforma_fkey
    foreign key (id_plataforma) references public.plataformas(id_plataforma),
  constraint sandbox_giftcard_orders_id_precio_fkey
    foreign key (id_precio) references public.precios(id_precio)
);

create index sandbox_giftcard_orders_admin_idx
  on public.sandbox_giftcard_orders (id_usuario_admin, creado_en desc);

create index sandbox_giftcard_orders_cliente_idx
  on public.sandbox_giftcard_orders (id_usuario_cliente, creado_en desc);

create table public.sandbox_giftcard_order_items (
  id_sandbox_item bigint generated always as identity not null,
  id_sandbox_order bigint not null,
  id_plataforma bigint not null,
  id_precio bigint,
  cantidad integer not null default 1,
  precio_unitario_usd numeric(12, 2) not null default 0,
  total_usd numeric(12, 2) not null default 0,
  valor_tarjeta_de_regalo text,
  moneda text,
  region text,
  detalle text,
  payload jsonb,
  creado_en timestamp with time zone not null default now(),
  constraint sandbox_giftcard_order_items_pkey primary key (id_sandbox_item),
  constraint sandbox_giftcard_order_items_cantidad_check check (cantidad > 0),
  constraint sandbox_giftcard_order_items_id_sandbox_order_fkey
    foreign key (id_sandbox_order)
    references public.sandbox_giftcard_orders(id_sandbox_order)
    on delete cascade,
  constraint sandbox_giftcard_order_items_id_plataforma_fkey
    foreign key (id_plataforma) references public.plataformas(id_plataforma),
  constraint sandbox_giftcard_order_items_id_precio_fkey
    foreign key (id_precio) references public.precios(id_precio)
);

create index sandbox_giftcard_order_items_order_idx
  on public.sandbox_giftcard_order_items (id_sandbox_order);

create table public.sandbox_giftcard_historial_ventas (
  id_sandbox_historial bigint generated always as identity not null,
  id_sandbox_order bigint not null,
  id_usuario_cliente bigint,
  id_usuario_admin bigint not null,
  id_plataforma bigint,
  monto_usd numeric(12, 2) not null default 0,
  monto_bs numeric(14, 2),
  referencia text,
  venta_cliente boolean not null default true,
  renovacion boolean not null default false,
  detalle text,
  payload jsonb,
  creado_en timestamp with time zone not null default now(),
  constraint sandbox_giftcard_historial_ventas_pkey primary key (id_sandbox_historial),
  constraint sandbox_giftcard_historial_ventas_id_sandbox_order_fkey
    foreign key (id_sandbox_order)
    references public.sandbox_giftcard_orders(id_sandbox_order)
    on delete cascade,
  constraint sandbox_giftcard_historial_ventas_id_usuario_cliente_fkey
    foreign key (id_usuario_cliente) references public.usuarios(id_usuario),
  constraint sandbox_giftcard_historial_ventas_id_usuario_admin_fkey
    foreign key (id_usuario_admin) references public.usuarios(id_usuario),
  constraint sandbox_giftcard_historial_ventas_id_plataforma_fkey
    foreign key (id_plataforma) references public.plataformas(id_plataforma)
);

create index sandbox_giftcard_historial_ventas_order_idx
  on public.sandbox_giftcard_historial_ventas (id_sandbox_order);
