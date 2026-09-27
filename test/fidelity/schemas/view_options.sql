CREATE TABLE public.items (
    id integer NOT NULL,
    price numeric,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
CREATE VIEW public.cheap AS SELECT id, price FROM public.items WHERE price < 10 WITH LOCAL CHECK OPTION;
CREATE VIEW public.cheap2 AS SELECT id, price FROM public.cheap WHERE price > 1 WITH CASCADED CHECK OPTION;
CREATE RECURSIVE VIEW public.nums (n) AS VALUES (1) UNION ALL SELECT n + 1 FROM nums WHERE n < 5;
