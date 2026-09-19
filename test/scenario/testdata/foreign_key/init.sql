-- Two tables that a foreign key is about to tie together, plus the unique
-- constraints a key can reference.
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
