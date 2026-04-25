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

CREATE MATERIALIZED VIEW public.order_stats AS
    SELECT user_id, count(*) AS order_count, sum(amount) AS total_amount
    FROM public.orders
    GROUP BY user_id;

CREATE INDEX idx_order_stats_user ON public.order_stats (user_id);
