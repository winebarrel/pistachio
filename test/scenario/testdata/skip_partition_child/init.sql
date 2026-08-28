CREATE TABLE public.events (
    id integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT events_pkey PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

-- Created out of band, the way pg_partman would.
CREATE TABLE public.events_p2024 PARTITION OF public.events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
