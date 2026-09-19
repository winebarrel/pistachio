-- A table with a row-level BEFORE trigger. The trigger functions are managed
-- too, so every pista run in this scenario sets PISTA_MANAGE_ROUTINE.
CREATE TABLE public.accounts (
    id integer NOT NULL,
    balance integer NOT NULL,
    note text,
    updated_at timestamp with time zone,
    CONSTRAINT accounts_pkey PRIMARY KEY (id)
);

CREATE FUNCTION public.touch_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN NEW.updated_at := now(); RETURN NEW; END $$;

CREATE FUNCTION public.audit_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN RETURN NULL; END $$;

CREATE TRIGGER accounts_touch BEFORE UPDATE ON public.accounts
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
