CREATE TYPE public.order_status AS ENUM (
    'pending',
    'paid',
    'shipped',
    -- pista:renamed-from 'delivered'
    'done'
);

CREATE TYPE public.ship_method AS ENUM (
    'ground',
    'air'
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    status public.order_status DEFAULT 'pending'::public.order_status NOT NULL,
    history public.order_status[],
    method public.ship_method DEFAULT 'ground'::public.ship_method NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
