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

CREATE FUNCTION public.stamp_email() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN NEW.email := normalize_email(NEW.email); RETURN NEW; END $$;

CREATE TRIGGER users_stamp_email BEFORE INSERT ON public.users
    FOR EACH ROW EXECUTE FUNCTION stamp_email();
