CREATE TABLE public.stores (
    id integer NOT NULL,
    manager_id integer,
    CONSTRAINT stores_pkey PRIMARY KEY (id)
);
CREATE TABLE public.staff (
    id integer NOT NULL,
    store_id integer NOT NULL,
    CONSTRAINT staff_pkey PRIMARY KEY (id),
    CONSTRAINT staff_store_id_fkey FOREIGN KEY (store_id) REFERENCES public.stores(id)
);
ALTER TABLE public.stores
    ADD CONSTRAINT stores_manager_id_fkey FOREIGN KEY (manager_id) REFERENCES public.staff(id);
