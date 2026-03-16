create or replace function public.normalize_giftcard_amount(raw_value text)
returns numeric
language plpgsql
immutable
as $$
declare
  normalized text;
begin
  normalized := replace(regexp_replace(coalesce(raw_value, ''), '\s+', '', 'g'), ',', '.');
  if normalized = '' then
    return null;
  end if;

  return round(normalized::numeric, 2);
exception
  when others then
    return null;
end;
$$;

create or replace function public.escape_html(raw_value text)
returns text
language sql
immutable
as $$
  select replace(
    replace(
      replace(
        replace(
          replace(coalesce(raw_value, ''), '&', '&amp;'),
          '<',
          '&lt;'
        ),
        '>',
        '&gt;'
      ),
      '"',
      '&quot;'
    ),
    '''',
    '&#39;'
  );
$$;

create or replace function public.build_giftcard_sale_notification_message(
  plataforma_nombre text,
  id_venta bigint,
  region text,
  valor_tarjeta text,
  moneda text,
  pin text
)
returns text
language plpgsql
immutable
as $$
declare
  plataforma_txt text := public.escape_html(plataforma_nombre);
  region_txt text := nullif(public.escape_html(region), '');
  valor_txt text := trim(
    both ' '
    from concat_ws(' ', public.escape_html(valor_tarjeta), public.escape_html(moneda))
  );
  pin_txt text := nullif(public.escape_html(coalesce(pin, 'Pendiente')), '');
  block_lines text := '';
begin
  block_lines := concat(
    '<div class="notif-line"><strong>',
    plataforma_txt,
    '</strong>',
    case
      when id_venta is not null then
        concat(' <span class="notif-id-venta">ID Venta: #', id_venta, '</span>')
      else
        ''
    end,
    '</div>'
  );

  if region_txt is not null then
    block_lines := concat(block_lines, '<br>Región: ', region_txt);
  end if;

  if valor_txt <> '' then
    block_lines := concat(block_lines, '<br>Valor: ', valor_txt);
  end if;

  if pin_txt is not null then
    block_lines := concat(block_lines, '<br>PIN: ', pin_txt);
  end if;

  return concat('Se asignaron los servicios:<br><br>', block_lines);
end;
$$;

create or replace function public.auto_deliver_pending_giftcard_sale_on_insert()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  target_amount numeric;
  local_today date;
  pending_sale record;
begin
  if coalesce(new.para_venta, false) is not true or coalesce(new.usado, false) is true then
    return new;
  end if;

  if new.id_tarjeta_de_regalo is null or new.id_plataforma is null then
    return new;
  end if;

  target_amount := public.normalize_giftcard_amount(new.monto::text);
  if target_amount is null or target_amount <= 0 then
    return new;
  end if;

  select
    v.id_venta,
    v.id_usuario,
    v.id_orden,
    v.id_precio,
    p.valor_tarjeta_de_regalo,
    p.region,
    p.moneda,
    pl.nombre as plataforma_nombre
  into pending_sale
  from public.ventas as v
  join public.precios as p
    on p.id_precio = v.id_precio
  join public.plataformas as pl
    on pl.id_plataforma = p.id_plataforma
  where coalesce(v.pendiente, false) is true
    and v.id_tarjeta_de_regalo is null
    and p.id_plataforma = new.id_plataforma
    and coalesce(pl.tarjeta_de_regalo, false) is true
    and public.normalize_giftcard_amount(p.valor_tarjeta_de_regalo) = target_amount
    and exists (
      select 1
      from public.historial_ventas as h
      where h.id_venta = v.id_venta
    )
  order by v.id_venta asc
  for update of v skip locked
  limit 1;

  if not found then
    return new;
  end if;

  local_today := (now() at time zone 'America/Caracas')::date;

  update public.tarjetas_de_regalo
  set usado = true,
      vendido_a = pending_sale.id_usuario,
      fecha_uso = local_today,
      id_orden = pending_sale.id_orden
  where id_tarjeta_de_regalo = new.id_tarjeta_de_regalo;

  update public.historial_ventas
  set id_tarjeta_de_regalo = new.id_tarjeta_de_regalo
  where id_venta = pending_sale.id_venta;

  if pending_sale.id_usuario is not null then
    insert into public.notificaciones (
      titulo,
      mensaje,
      fecha,
      leido,
      id_usuario,
      id_orden
    )
    values (
      'Nuevo servicio',
      public.build_giftcard_sale_notification_message(
        pending_sale.plataforma_nombre,
        pending_sale.id_venta,
        pending_sale.region,
        pending_sale.valor_tarjeta_de_regalo,
        pending_sale.moneda,
        new.pin
      ),
      local_today,
      false,
      pending_sale.id_usuario,
      pending_sale.id_orden
    );
  end if;

  delete from public.ventas
  where id_venta = pending_sale.id_venta;

  if pending_sale.id_orden is not null then
    update public.ordenes
    set en_espera = exists (
      select 1
      from public.ventas
      where id_orden = pending_sale.id_orden
        and coalesce(pendiente, false) is true
    )
    where id_orden = pending_sale.id_orden;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_auto_deliver_pending_giftcard_sale_on_insert
on public.tarjetas_de_regalo;

create trigger trg_auto_deliver_pending_giftcard_sale_on_insert
after insert on public.tarjetas_de_regalo
for each row
execute function public.auto_deliver_pending_giftcard_sale_on_insert();

create index if not exists ventas_pending_giftcard_lookup_idx
on public.ventas (id_precio, id_venta)
where pendiente is true and id_tarjeta_de_regalo is null;

create index if not exists ventas_pending_by_order_idx
on public.ventas (id_orden)
where pendiente is true;

create index if not exists historial_ventas_id_venta_idx
on public.historial_ventas (id_venta);
