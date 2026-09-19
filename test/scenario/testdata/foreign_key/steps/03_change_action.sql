CREATE TABLE public.customers (
    id integer NOT NULL,
    code text NOT NULL,
    region text NOT NULL,
    CONSTRAINT customers_pkey PRIMARY KEY (id),
    CONSTRAINT customers_code_region_key UNIQUE (code, region)
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL,
    parent_id integer,
    cust_code text,
    cust_region text,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);

ALTER TABLE public.orders ADD CONSTRAINT orders_customer_fk
    FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON UPDATE CASCADE ON DELETE RESTRICT;
