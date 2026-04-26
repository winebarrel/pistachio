CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE MATERIALIZED VIEW public.user_names AS
SELECT id, name FROM public.users;

CREATE INDEX CONCURRENTLY idx_user_names ON public.user_names USING btree (name);
