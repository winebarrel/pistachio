CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING hash (name);

CREATE UNIQUE INDEX CONCURRENTLY idx_users_email ON public.users USING btree (email);
