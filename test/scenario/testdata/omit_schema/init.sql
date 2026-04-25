CREATE TYPE public.status AS ENUM ('active', 'inactive');

CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    status public.status,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_users_name ON public.users (name);

CREATE TABLE public.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    title text NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);

ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id);

CREATE VIEW public.active_users AS SELECT id, name FROM public.users WHERE status = 'active'::public.status;

CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;

CREATE INDEX idx_user_stats_cnt ON public.user_stats (cnt);
