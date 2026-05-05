CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text,
    age integer,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_id_pos CHECK (id > 0)
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer,
    total numeric(10,2),
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)
);
