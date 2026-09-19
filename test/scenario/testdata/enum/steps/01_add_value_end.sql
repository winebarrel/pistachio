CREATE TYPE public.order_status AS ENUM (
    'pending',
    'shipped',
    'delivered'
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    status public.order_status DEFAULT 'pending'::public.order_status NOT NULL,
    history public.order_status[],
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
