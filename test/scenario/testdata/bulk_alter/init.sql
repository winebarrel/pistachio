CREATE TABLE public.users (
    id integer NOT NULL,
    legacy text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
