CREATE FUNCTION public.add(a integer, b integer) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$SELECT a + b$$;
CREATE FUNCTION public.add(a bigint, b bigint) RETURNS bigint
    LANGUAGE sql IMMUTABLE
    AS $$SELECT a + b$$;
CREATE PROCEDURE public.noop()
    LANGUAGE plpgsql
    AS $$ BEGIN END $$;
