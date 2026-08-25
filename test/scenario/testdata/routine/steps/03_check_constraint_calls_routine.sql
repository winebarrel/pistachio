-- The CHECK constraint calls the routine, so the routine has to be created
-- before the table DDL that references it.
CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_normalized CHECK (email = normalize_email(email))
);

CREATE FUNCTION public.normalize_email(e text) RETURNS text
    LANGUAGE sql
    IMMUTABLE STRICT
    AS $$ SELECT btrim(lower(e)) $$;

CREATE FUNCTION public.normalize_email(e text, keep_case boolean) RETURNS text
    LANGUAGE sql
    IMMUTABLE STRICT
    AS $$ SELECT CASE WHEN keep_case THEN e ELSE lower(e) END $$;
