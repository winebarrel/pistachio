-- An index on the parent, written without ONLY, is pushed down to every
-- partition. The copies it creates must not show up as drift.
CREATE TABLE public.events (
    id bigint NOT NULL,
    at timestamp with time zone NOT NULL,
    kind text NOT NULL,
    payload jsonb,
    CONSTRAINT events_pkey PRIMARY KEY (id, at)
) PARTITION BY RANGE (at);

CREATE INDEX events_kind_idx ON public.events USING btree (kind);

CREATE TABLE public.events_2024 PARTITION OF public.events
    FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');

CREATE TABLE public.events_2025 PARTITION OF public.events
    FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');

CREATE TABLE public.events_default PARTITION OF public.events DEFAULT;
