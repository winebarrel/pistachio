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

CREATE TRIGGER accounts_touch BEFORE UPDATE OF balance ON public.accounts
    FOR EACH ROW WHEN ((old.balance IS DISTINCT FROM new.balance))
    EXECUTE FUNCTION touch_updated_at();

CREATE TRIGGER accounts_audit AFTER INSERT OR DELETE OR UPDATE ON public.accounts
    FOR EACH STATEMENT EXECUTE FUNCTION audit_change();

ALTER TABLE public.accounts ENABLE ALWAYS TRIGGER accounts_audit;
