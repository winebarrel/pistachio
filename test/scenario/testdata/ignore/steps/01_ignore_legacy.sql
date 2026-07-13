-- legacy is ignored: its dropped "name" column must not be touched, and it
-- must not be dropped even though the desired body differs.
-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
