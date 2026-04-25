CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_users_name ON public.users USING btree (name);

CREATE UNIQUE INDEX idx_users_email ON public.users USING btree (email);
