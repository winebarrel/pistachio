CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TYPE public.point2 AS (x integer, y integer);
CREATE DOMAIN public.codes AS text[] CONSTRAINT codes_check CHECK ((array_length(VALUE, 1) > 0));
CREATE TABLE public.shapes (
    id integer NOT NULL,
    sts public.status[],
    pts public.point2[],
    cs public.codes,
    CONSTRAINT shapes_pkey PRIMARY KEY (id)
);
