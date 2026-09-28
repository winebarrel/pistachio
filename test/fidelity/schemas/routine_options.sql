CREATE FUNCTION public.safe_div(a numeric, b numeric) RETURNS numeric
    LANGUAGE sql IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE COST 5
    AS $$SELECT a / b$$;
CREATE FUNCTION public.whoami() RETURNS text
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
    SET work_mem TO '64MB'
    AS $$SELECT current_user::text$$;
CREATE FUNCTION public.many() RETURNS SETOF integer
    LANGUAGE sql PARALLEL RESTRICTED ROWS 50
    AS $$SELECT generate_series(1, 3)$$;
CREATE PROCEDURE public.touch(IN n integer)
    LANGUAGE plpgsql SECURITY DEFINER
    SET lock_timeout TO '1s'
    AS $$ BEGIN PERFORM n; END $$;
