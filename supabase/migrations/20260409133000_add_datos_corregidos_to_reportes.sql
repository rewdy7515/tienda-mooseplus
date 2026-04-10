alter table public.reportes
add column if not exists datos_corregidos boolean;

update public.reportes
set datos_corregidos = false
where datos_corregidos is null;

alter table public.reportes
alter column datos_corregidos set default false;

alter table public.reportes
alter column datos_corregidos set not null;
