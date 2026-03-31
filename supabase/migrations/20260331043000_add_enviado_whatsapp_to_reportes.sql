alter table public.reportes
add column if not exists enviado_whatsapp boolean;

update public.reportes
set enviado_whatsapp = false
where enviado_whatsapp is null;

alter table public.reportes
alter column enviado_whatsapp set default false;

alter table public.reportes
alter column enviado_whatsapp set not null;

create index if not exists reportes_enviado_whatsapp_pending_idx
on public.reportes (id_reporte)
where enviado_whatsapp = false;
