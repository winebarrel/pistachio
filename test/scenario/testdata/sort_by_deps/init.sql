-- Objects whose name order is the reverse of their dependency order.
-- aref references zref by foreign key, but sorts before it by name.
-- a_derived selects from z_base, but sorts before it by name.
CREATE TYPE public.color AS ENUM ('red', 'green');

CREATE TABLE public.zref (
    id integer NOT NULL,
    CONSTRAINT zref_pkey PRIMARY KEY (id)
);

CREATE TABLE public.aref (
    id integer NOT NULL,
    zref_id integer,
    c public.color,
    CONSTRAINT aref_pkey PRIMARY KEY (id)
);
ALTER TABLE ONLY public.aref
    ADD CONSTRAINT aref_zref_fkey FOREIGN KEY (zref_id) REFERENCES public.zref(id);

CREATE VIEW public.z_base AS SELECT id FROM public.aref;
CREATE VIEW public.a_derived AS SELECT id FROM public.z_base;
