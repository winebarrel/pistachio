CREATE DOMAIN public.qty AS integer CONSTRAINT qty_pos CHECK (VALUE > 0);
ALTER DOMAIN public.qty ADD CONSTRAINT qty_even CHECK (VALUE % 2 = 0) NOT VALID;
CREATE TABLE public.parts (
    id integer NOT NULL,
    q public.qty,
    CONSTRAINT parts_pkey PRIMARY KEY (id)
);
