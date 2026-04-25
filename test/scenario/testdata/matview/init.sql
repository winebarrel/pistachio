CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    amount integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id)
);
