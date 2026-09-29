CREATE DOMAIN public.pos AS integer CONSTRAINT pos_check CHECK ((VALUE > 0));
CREATE FUNCTION public.stamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE public.owners (
    id integer NOT NULL,
    CONSTRAINT owners_pkey PRIMARY KEY (id)
);
CREATE TABLE public.items (
    id integer NOT NULL,
    owner_id integer,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_id_check CHECK ((id > 0))
);
ALTER TABLE ONLY public.items ADD CONSTRAINT items_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.owners(id);
ALTER TABLE public.items ENABLE ROW LEVEL SECURITY;
CREATE POLICY items_read ON public.items FOR SELECT USING (true);
CREATE TRIGGER items_stamp BEFORE INSERT ON public.items FOR EACH ROW EXECUTE FUNCTION public.stamp();
CREATE VIEW public.items_v AS SELECT items.id FROM public.items;
CREATE TRIGGER items_v_insert INSTEAD OF INSERT ON public.items_v FOR EACH ROW EXECUTE FUNCTION public.stamp();
COMMENT ON CONSTRAINT pos_check ON DOMAIN public.pos IS 'a domain constraint';
COMMENT ON CONSTRAINT items_pkey ON public.items IS 'a primary key';
COMMENT ON CONSTRAINT items_id_check ON public.items IS 'a check';
COMMENT ON CONSTRAINT items_owner_id_fkey ON public.items IS 'a foreign key';
COMMENT ON POLICY items_read ON public.items IS 'a policy';
COMMENT ON TRIGGER items_stamp ON public.items IS 'a trigger';
COMMENT ON TRIGGER items_v_insert ON public.items_v IS 'a view trigger';
