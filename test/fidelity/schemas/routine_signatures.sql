CREATE FUNCTION public.top_items(n integer DEFAULT 10) RETURNS TABLE(id integer, name text)
    LANGUAGE sql STABLE
    AS $$SELECT 1, 'x'::text LIMIT n$$;
CREATE FUNCTION public.each_id() RETURNS SETOF integer
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$SELECT 1$$;
CREATE FUNCTION public.split(INOUT a integer, OUT b integer)
    LANGUAGE plpgsql STRICT SECURITY DEFINER
    SET search_path TO 'public'
    AS $$ BEGIN b := a; END $$;
CREATE FUNCTION public.total(VARIADIC vals integer[]) RETURNS integer
    LANGUAGE sql IMMUTABLE COST 5
    AS $$SELECT 0$$;
