CREATE TABLE public.events (
    id bigint NOT NULL,
    at timestamp with time zone NOT NULL,
    kind text NOT NULL,
    payload jsonb,
    CONSTRAINT events_pkey PRIMARY KEY (id, at, kind)
) PARTITION BY RANGE (at);

CREATE INDEX events_kind_idx ON public.events USING btree (kind);

CREATE TABLE public.events_2025 PARTITION OF public.events
    FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');

CREATE TABLE public.events_2026 PARTITION OF public.events
    FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')
    PARTITION BY LIST (kind);

CREATE TABLE public.events_2026_click PARTITION OF public.events_2026
    FOR VALUES IN ('click');

CREATE TABLE public.events_2026_other PARTITION OF public.events_2026 DEFAULT;

CREATE TABLE public.events_default PARTITION OF public.events DEFAULT;
