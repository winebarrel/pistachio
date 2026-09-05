CREATE TABLE public.orders (
    id integer NOT NULL,
    qty integer NOT NULL,
    price numeric NOT NULL,
    status text NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_qty_check CHECK ((qty > 0)),
    CONSTRAINT orders_status_check CHECK ((status = ANY (ARRAY['new'::text, 'done'::text]))),
    CONSTRAINT orders_total_check CHECK (((price * (qty)::numeric) < (1000000)::numeric))
);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_price_check CHECK ((price > (0)::numeric)) NOT VALID;
