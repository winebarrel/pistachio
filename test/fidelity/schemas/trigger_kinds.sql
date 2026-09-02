CREATE FUNCTION public.noop_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN RETURN NULL; END $$;
CREATE TABLE public.docs (
    id integer NOT NULL,
    body text,
    CONSTRAINT docs_pkey PRIMARY KEY (id)
);
CREATE VIEW public.docs_v AS SELECT docs.id, docs.body FROM public.docs;
CREATE TRIGGER docs_truncate AFTER TRUNCATE ON public.docs FOR EACH STATEMENT EXECUTE FUNCTION public.noop_trigger();
CREATE TRIGGER docs_update_of BEFORE UPDATE OF body ON public.docs FOR EACH ROW EXECUTE FUNCTION public.noop_trigger();
CREATE TRIGGER docs_transition AFTER INSERT ON public.docs REFERENCING NEW TABLE AS newrows FOR EACH STATEMENT EXECUTE FUNCTION public.noop_trigger();
CREATE CONSTRAINT TRIGGER docs_deferred AFTER INSERT ON public.docs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.noop_trigger();
CREATE TRIGGER docs_v_instead INSTEAD OF INSERT ON public.docs_v FOR EACH ROW EXECUTE FUNCTION public.noop_trigger();
