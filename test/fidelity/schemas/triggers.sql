CREATE FUNCTION public.touch() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN NEW.at := now(); RETURN NEW; END $$;
CREATE TABLE public.audits (
    id integer NOT NULL,
    v text,
    at timestamp with time zone,
    CONSTRAINT audits_pkey PRIMARY KEY (id)
);
CREATE TRIGGER audits_touch BEFORE UPDATE ON public.audits FOR EACH ROW WHEN ((old.v IS DISTINCT FROM new.v)) EXECUTE FUNCTION public.touch();
CREATE TRIGGER audits_stmt AFTER INSERT ON public.audits FOR EACH STATEMENT EXECUTE FUNCTION public.touch();
