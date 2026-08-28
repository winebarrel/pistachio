CREATE TABLE public.events (
    id integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    data text,
    CONSTRAINT events_pkey PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
