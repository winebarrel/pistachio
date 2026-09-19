CREATE TABLE public.events (
    id bigint NOT NULL,
    at timestamp with time zone NOT NULL,
    kind text NOT NULL,
    CONSTRAINT events_pkey PRIMARY KEY (id, at)
) PARTITION BY RANGE (at);

CREATE TABLE public.events_2024 PARTITION OF public.events
    FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');

CREATE TABLE public.events_2025 PARTITION OF public.events
    FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');
