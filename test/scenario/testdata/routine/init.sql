CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE FUNCTION public.normalize_email(e text) RETURNS text
    LANGUAGE sql
    IMMUTABLE STRICT
    AS $$ SELECT lower(e) $$;
