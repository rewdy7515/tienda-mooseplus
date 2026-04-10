alter table public.reportes
add column if not exists datos_incorrectos boolean;

update public.reportes
set datos_incorrectos = false
where datos_incorrectos is null;

alter table public.reportes
alter column datos_incorrectos set default false;

alter table public.reportes
alter column datos_incorrectos set not null;
