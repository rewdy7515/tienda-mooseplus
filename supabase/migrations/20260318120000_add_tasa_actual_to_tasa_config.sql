alter table public.tasa_config
  add column if not exists tasa_actual double precision;

update public.tasa_config
set tasa_actual = coalesce(tasa_actual, tasa_oficial)
where id = 1;
