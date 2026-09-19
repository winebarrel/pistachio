-- customers is gone, so the keys that reference it have to go first.
CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL,
    parent_id integer,
    cust_code text,
    cust_region text,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);

ALTER TABLE public.orders ADD CONSTRAINT orders_parent_fk
    FOREIGN KEY (parent_id) REFERENCES public.orders(id) DEFERRABLE INITIALLY DEFERRED;
