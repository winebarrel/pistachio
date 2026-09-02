CREATE TABLE public.warehouses (
    id integer NOT NULL,
    region text NOT NULL,
    code text NOT NULL,
    CONSTRAINT warehouses_pkey PRIMARY KEY (id),
    CONSTRAINT warehouses_region_code_key UNIQUE (region, code)
);
CREATE TABLE public.stocks (
    id integer NOT NULL,
    region text,
    code text,
    parent_id integer,
    supplier_id integer,
    CONSTRAINT stocks_pkey PRIMARY KEY (id),
    CONSTRAINT stocks_region_code_fkey FOREIGN KEY (region, code) REFERENCES public.warehouses(region, code) MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT stocks_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.stocks(id) ON UPDATE RESTRICT ON DELETE NO ACTION,
    CONSTRAINT stocks_supplier_id_fkey FOREIGN KEY (supplier_id) REFERENCES public.warehouses(id) DEFERRABLE
);
