-- Drop: enum (status)
-- Everything else unchanged.

CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text,
    age integer,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE public.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    title text NOT NULL,
    body text,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);

ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id);

CREATE OR REPLACE VIEW public.active_users AS
SELECT users.id,
    users.name
   FROM users;

CREATE MATERIALIZED VIEW public.user_stats AS
SELECT count(*) AS cnt FROM public.users;
