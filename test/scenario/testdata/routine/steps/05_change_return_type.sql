-- Changing a return type is a drop and a create; PostgreSQL rejects the
-- replace.
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

CREATE FUNCTION public.normalize_email(e text, keep_case boolean) RETURNS character varying
    LANGUAGE sql
    IMMUTABLE STRICT
    AS $$ SELECT CASE WHEN keep_case THEN e ELSE lower(e) END $$;

CREATE FUNCTION public.stamp_email() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN NEW.email := normalize_email(NEW.email); RETURN NEW; END $$;

CREATE PROCEDURE public.purge_users()
    LANGUAGE sql
    AS $$ DELETE FROM users WHERE id < 0 $$;

CREATE TRIGGER users_stamp_email BEFORE INSERT ON public.users
    FOR EACH ROW EXECUTE FUNCTION stamp_email();
