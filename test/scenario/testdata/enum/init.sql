-- An enum type used by a plain column, an array column and a column default.
-- Every step reshapes the type while the dependent columns stay in place.
CREATE TYPE public.order_status AS ENUM (
    'pending',
    'shipped'
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    status public.order_status DEFAULT 'pending'::public.order_status NOT NULL,
    history public.order_status[],
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
