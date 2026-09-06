CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    at date NOT NULL,
    user_id integer,
    CONSTRAINT orders_pkey PRIMARY KEY (id, at),
    CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)
) PARTITION BY RANGE (at);
CREATE TABLE public.orders_2025 PARTITION OF public.orders
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
