CREATE TYPE public.color AS ENUM ('red', 'blue');
CREATE TYPE public.pair AS (a integer, b integer);
CREATE DOMAIN public.pos AS integer CONSTRAINT pos_check CHECK ((VALUE > 0));
CREATE SEQUENCE public.ticker;
CREATE FUNCTION public.answer() RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$SELECT 42$$;
CREATE PROCEDURE public.tick()
    LANGUAGE plpgsql
    AS $$ BEGIN END $$;
CREATE TABLE public.things (
    id integer NOT NULL,
    n public.pos,
    CONSTRAINT things_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.thing_count AS SELECT count(*) AS n FROM public.things;
COMMENT ON TYPE public.color IS 'an enum';
COMMENT ON TYPE public.pair IS 'a composite';
COMMENT ON COLUMN public.pair.a IS 'an attribute';
COMMENT ON DOMAIN public.pos IS 'a domain';
COMMENT ON SEQUENCE public.ticker IS 'a sequence';
COMMENT ON FUNCTION public.answer() IS 'a function';
COMMENT ON PROCEDURE public.tick() IS 'a procedure';
COMMENT ON MATERIALIZED VIEW public.thing_count IS 'a materialized view';
