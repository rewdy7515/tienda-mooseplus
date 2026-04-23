-- Restore web push tables/functions/triggers if they were removed.

create table if not exists public.web_push_subscriptions (
  id_subscription bigint generated always as identity not null,
  id_usuario bigint not null,
  endpoint text not null,
  p256dh text not null,
  auth text not null,
  expiration_time bigint,
  user_agent text,
  device_label text,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  last_seen_at timestamp with time zone not null default now(),
  disabled_at timestamp with time zone,
  constraint web_push_subscriptions_pkey primary key (id_subscription),
  constraint web_push_subscriptions_endpoint_key unique (endpoint),
  constraint web_push_subscriptions_id_usuario_fkey
    foreign key (id_usuario) references public.usuarios(id_usuario) on delete cascade
);

create index if not exists web_push_subscriptions_id_usuario_idx
  on public.web_push_subscriptions (id_usuario);

create index if not exists web_push_subscriptions_active_idx
  on public.web_push_subscriptions (id_usuario, disabled_at);

create table if not exists public.web_push_delivery_queue (
  id_queue bigint generated always as identity not null,
  id_notificacion bigint not null,
  id_subscription bigint not null,
  id_usuario bigint not null,
  estado text not null default 'pending',
  intentos smallint not null default 0,
  ultimo_error text,
  created_at timestamp with time zone not null default now(),
  procesado_en timestamp with time zone,
  constraint web_push_delivery_queue_pkey primary key (id_queue),
  constraint web_push_delivery_queue_estado_check
    check (estado in ('pending', 'sent', 'failed', 'skipped')),
  constraint web_push_delivery_queue_notif_subscription_key
    unique (id_notificacion, id_subscription),
  constraint web_push_delivery_queue_id_notificacion_fkey
    foreign key (id_notificacion) references public.notificaciones(id_notificacion) on delete cascade,
  constraint web_push_delivery_queue_id_subscription_fkey
    foreign key (id_subscription) references public.web_push_subscriptions(id_subscription) on delete cascade,
  constraint web_push_delivery_queue_id_usuario_fkey
    foreign key (id_usuario) references public.usuarios(id_usuario) on delete cascade
);

create index if not exists web_push_delivery_queue_estado_idx
  on public.web_push_delivery_queue (estado, id_queue);

create index if not exists web_push_delivery_queue_subscription_idx
  on public.web_push_delivery_queue (id_subscription, estado);

create or replace function public.touch_web_push_subscription_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_touch_web_push_subscription_updated_at'
      and tgrelid = 'public.web_push_subscriptions'::regclass
  ) then
    create trigger trg_touch_web_push_subscription_updated_at
    before update on public.web_push_subscriptions
    for each row
    execute function public.touch_web_push_subscription_updated_at();
  end if;
end
$$;

create or replace function public.enqueue_web_push_notification()
returns trigger
language plpgsql
as $$
begin
  insert into public.web_push_delivery_queue (id_notificacion, id_subscription, id_usuario)
  select
    new.id_notificacion,
    s.id_subscription,
    new.id_usuario
  from public.web_push_subscriptions as s
  where s.id_usuario = new.id_usuario
    and s.disabled_at is null
  on conflict (id_notificacion, id_subscription) do nothing;

  return new;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_enqueue_web_push_notification'
      and tgrelid = 'public.notificaciones'::regclass
  ) then
    create trigger trg_enqueue_web_push_notification
    after insert on public.notificaciones
    for each row
    when (new.id_usuario is not null)
    execute function public.enqueue_web_push_notification();
  end if;
end
$$;
