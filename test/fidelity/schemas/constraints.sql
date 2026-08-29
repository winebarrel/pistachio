CREATE TABLE public.parents (
    id integer NOT NULL,
    code text NOT NULL,
    CONSTRAINT parents_pkey PRIMARY KEY (id),
    CONSTRAINT parents_code_key UNIQUE (code)
);
CREATE TABLE public.children (
    id integer NOT NULL,
    parent_id integer NOT NULL,
    qty integer NOT NULL,
    CONSTRAINT children_pkey PRIMARY KEY (id),
    CONSTRAINT children_qty_check CHECK ((qty > 0)),
    CONSTRAINT children_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.parents(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
