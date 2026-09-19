-- Two new tables that reference each other, created in one plan. Neither can
-- carry its key inline, so the tables come first and the keys follow.
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

ALTER TABLE public.orders ADD CONSTRAINT orders_cust_code_fk
    FOREIGN KEY (cust_code, cust_region) REFERENCES public.customers(code, region) ON DELETE SET NULL;

ALTER TABLE public.orders ADD CONSTRAINT orders_parent_fk
    FOREIGN KEY (parent_id) REFERENCES public.orders(id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE public.invoices (
    id integer NOT NULL,
    shipment_id integer,
    CONSTRAINT invoices_pkey PRIMARY KEY (id)
);

CREATE TABLE public.shipments (
    id integer NOT NULL,
    invoice_id integer,
    CONSTRAINT shipments_pkey PRIMARY KEY (id)
);

ALTER TABLE public.invoices ADD CONSTRAINT invoices_shipment_fk
    FOREIGN KEY (shipment_id) REFERENCES public.shipments(id);

ALTER TABLE public.shipments ADD CONSTRAINT shipments_invoice_fk
    FOREIGN KEY (invoice_id) REFERENCES public.invoices(id);
