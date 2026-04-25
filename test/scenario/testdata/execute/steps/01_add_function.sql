CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'get_user_count')
CREATE OR REPLACE FUNCTION public.get_user_count() RETURNS bigint AS $$
  SELECT count(*) FROM public.users;
$$ LANGUAGE sql;
