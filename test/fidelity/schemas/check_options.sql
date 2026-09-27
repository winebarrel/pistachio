CREATE TABLE public.parents (
    id integer NOT NULL,
    v integer,
    CONSTRAINT parents_pkey PRIMARY KEY (id),
    CONSTRAINT parents_v_check CHECK (v > 0) NO INHERIT
);
-- The child does not get the NO INHERIT check.
CREATE TABLE public.kids (
    w integer
) INHERITS (public.parents);
CREATE TABLE public.refs (
    id integer NOT NULL,
    parent_id integer,
    CONSTRAINT refs_pkey PRIMARY KEY (id)
);
ALTER TABLE public.refs ADD CONSTRAINT refs_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.parents(id) NOT VALID;
