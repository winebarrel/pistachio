-- Replaces the customers file a dump --split wrote, the way a user editing
-- one file out of a split dump would.
CREATE TABLE public.customers (
    id integer NOT NULL,
    addr email NOT NULL,
    home address,
    code integer DEFAULT nextval('code_seq'::regclass) NOT NULL,
    note text,
    CONSTRAINT customers_pkey PRIMARY KEY (id)
);
