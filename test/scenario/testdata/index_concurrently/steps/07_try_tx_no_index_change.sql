CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    age integer,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);
