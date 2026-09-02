CREATE TABLE public.names (
    id integer NOT NULL,
    label text COLLATE pg_catalog."C" NOT NULL,
    alias text,
    CONSTRAINT names_pkey PRIMARY KEY (id)
);
CREATE INDEX names_alias_idx ON public.names USING btree (alias COLLATE pg_catalog."POSIX");
