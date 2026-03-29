alter table public.ordenes
add column if not exists aviso_verificacion_manual boolean;

update public.ordenes
set aviso_verificacion_manual = false
where aviso_verificacion_manual is null;

alter table public.ordenes
alter column aviso_verificacion_manual set default false;

alter table public.ordenes
alter column aviso_verificacion_manual set not null;
