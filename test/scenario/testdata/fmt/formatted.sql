CREATE TYPE public.status AS ENUM (
    'active',
    'inactive',
    'pending'
);

CREATE DOMAIN public.pos_int AS integer
    CONSTRAINT pos_check CHECK (value > 0);

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    status public.status,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_name_key UNIQUE (name)
);
CREATE INDEX idx_users_name ON public.users USING btree (name);

CREATE OR REPLACE VIEW public.active_users AS
SELECT id, name FROM public.users WHERE status = 'active'::public.status;
