create table if not exists public.eventos_trafico_web (
  id_evento_trafico bigint generated always as identity primary key,
  fecha_hora timestamp with time zone not null default now(),
  tipo_evento text not null,
  id_sesion uuid not null,
  id_usuario bigint null references public.usuarios(id_usuario) on delete set null,
  ruta text not null,
  url_completa text null,
  referidor text null,
  agente_usuario text null,
  metadatos jsonb null
);

create index if not exists eventos_trafico_web_fecha_hora_idx
  on public.eventos_trafico_web (fecha_hora desc);

create index if not exists eventos_trafico_web_tipo_fecha_idx
  on public.eventos_trafico_web (tipo_evento, fecha_hora desc);

create index if not exists eventos_trafico_web_usuario_fecha_idx
  on public.eventos_trafico_web (id_usuario, fecha_hora desc);

create index if not exists eventos_trafico_web_sesion_fecha_idx
  on public.eventos_trafico_web (id_sesion, fecha_hora desc);

create index if not exists eventos_trafico_web_ruta_fecha_idx
  on public.eventos_trafico_web (ruta, fecha_hora desc);
