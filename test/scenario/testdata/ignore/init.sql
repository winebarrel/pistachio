CREATE TABLE public.legacy (
    id integer NOT NULL,
    name text,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
