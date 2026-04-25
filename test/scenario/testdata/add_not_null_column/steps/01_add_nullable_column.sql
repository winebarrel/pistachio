CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
