CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

-- pista:execute
CREATE OR REPLACE FUNCTION public.get_user_count() RETURNS bigint AS $$
  SELECT count(*) FROM public.users;
$$ LANGUAGE sql;
