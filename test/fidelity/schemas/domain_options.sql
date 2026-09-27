-- No NOT VALID domain constraint: a desired domain constraint is always
-- validated, so the dump writes it inline and apply validates it.
CREATE DOMAIN public.code AS text COLLATE "C" CONSTRAINT code_len CHECK (length(VALUE) <= 8);
CREATE DOMAIN public.qty AS integer DEFAULT 1 NOT NULL CONSTRAINT qty_pos CHECK (VALUE > 0) CONSTRAINT qty_max CHECK (VALUE < 1000);
CREATE TYPE public.label AS (name text COLLATE "C", weight numeric(5,2));
CREATE TABLE public.parts (
    id integer NOT NULL,
    c public.code,
    q public.qty,
    l public.label,
    CONSTRAINT parts_pkey PRIMARY KEY (id)
);
