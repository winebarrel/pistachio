CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE INDEX users_name_idx ON public.users (name);
