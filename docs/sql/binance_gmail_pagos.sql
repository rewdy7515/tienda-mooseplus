BEGIN;

CREATE TABLE IF NOT EXISTS public.binance_gmail_pagos (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  titulo text NOT NULL,
  texto text,
  fecha text,
  dispositivo text,
  app text,
  hash text UNIQUE,
  referencia text,
  monto_usdt double precision,
  moneda text,
  remitente text,
  gmail_message_id text NOT NULL UNIQUE,
  gmail_thread_id text,
  fecha_correo timestamp with time zone,
  id_orden bigint,
  id_usuario bigint,
  pago_verificado boolean DEFAULT false,
  hora_recibido time without time zone,
  raw_payload jsonb,
  estado text NOT NULL DEFAULT 'pendiente',
  detalle_estado text,
  creado_en timestamp with time zone NOT NULL DEFAULT now(),
  procesado_en timestamp with time zone,
  CONSTRAINT binance_gmail_pagos_pkey PRIMARY KEY (id),
  CONSTRAINT binance_gmail_pagos_id_orden_fkey FOREIGN KEY (id_orden)
    REFERENCES public.ordenes(id_orden),
  CONSTRAINT binance_gmail_pagos_id_usuario_fkey FOREIGN KEY (id_usuario)
    REFERENCES public.usuarios(id_usuario)
);

CREATE INDEX IF NOT EXISTS idx_binance_gmail_pagos_estado
  ON public.binance_gmail_pagos (estado);

CREATE INDEX IF NOT EXISTS idx_binance_gmail_pagos_monto_usdt
  ON public.binance_gmail_pagos (monto_usdt);

CREATE INDEX IF NOT EXISTS idx_binance_gmail_pagos_pago_verificado
  ON public.binance_gmail_pagos (pago_verificado);

CREATE INDEX IF NOT EXISTS idx_binance_gmail_pagos_creado_en
  ON public.binance_gmail_pagos (creado_en DESC);

COMMIT;
