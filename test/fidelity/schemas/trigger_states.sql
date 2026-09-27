CREATE FUNCTION public.noop_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE public.docs (
    id integer NOT NULL,
    body text,
    CONSTRAINT docs_pkey PRIMARY KEY (id)
);
CREATE TRIGGER docs_off BEFORE INSERT ON public.docs FOR EACH ROW EXECUTE FUNCTION public.noop_trigger();
CREATE TRIGGER docs_always BEFORE UPDATE ON public.docs FOR EACH ROW EXECUTE FUNCTION public.noop_trigger();
CREATE TRIGGER docs_replica BEFORE DELETE ON public.docs FOR EACH ROW EXECUTE FUNCTION public.noop_trigger();
ALTER TABLE public.docs DISABLE TRIGGER docs_off;
ALTER TABLE public.docs ENABLE ALWAYS TRIGGER docs_always;
ALTER TABLE public.docs ENABLE REPLICA TRIGGER docs_replica;
