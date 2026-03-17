alter table public.tasa_config
  add column if not exists tasa_binance double precision,
  add column if not exists tasa_oficial double precision,
  add column if not exists markup_aplicado double precision,
  add column if not exists tasa_generada_en timestamp with time zone,
  add column if not exists vigente_desde timestamp with time zone,
  add column if not exists vigente_hasta timestamp with time zone;
