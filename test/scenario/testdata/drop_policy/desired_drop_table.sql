-- Drop: table (posts)
-- Everything else unchanged.

CREATE TYPE public.status AS ENUM ('active', 'inactive');

CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text,
    age integer,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE OR REPLACE VIEW public.active_users AS
SELECT users.id,
    users.name
   FROM users;
