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
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
