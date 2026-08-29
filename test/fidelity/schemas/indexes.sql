CREATE TABLE public.items (
    id integer NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    active boolean NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX items_name_lower_idx ON public.items USING btree (lower(name)) WHERE active;
CREATE INDEX items_kind_name_idx ON public.items USING btree (kind, name DESC NULLS LAST);
CREATE INDEX items_kind_hash_idx ON public.items USING hash (kind);
CREATE INDEX items_name_pattern_idx ON public.items USING btree (name text_pattern_ops);
CREATE UNIQUE INDEX items_id_name_idx ON public.items USING btree (id) INCLUDE (name);
