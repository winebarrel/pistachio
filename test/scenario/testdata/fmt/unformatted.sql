CREATE TYPE public.status AS ENUM ('active','inactive','pending');
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
CREATE TABLE public.users (id integer NOT NULL, name text NOT NULL, status public.status, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE INDEX idx_users_name ON public.users (name);
ALTER TABLE public.users ADD CONSTRAINT users_name_key UNIQUE (name);
CREATE VIEW public.active_users AS SELECT id, name FROM public.users WHERE status = 'active'::public.status;
