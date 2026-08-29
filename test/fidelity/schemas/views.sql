CREATE TABLE public.sales (
    id integer NOT NULL,
    region text NOT NULL,
    amount numeric NOT NULL,
    CONSTRAINT sales_pkey PRIMARY KEY (id)
);
CREATE VIEW public.big_sales AS SELECT sales.id, sales.region FROM public.sales WHERE (sales.amount > (100)::numeric);
CREATE MATERIALIZED VIEW public.sales_by_region AS SELECT sales.region, count(*) AS n FROM public.sales GROUP BY sales.region;
CREATE UNIQUE INDEX sales_by_region_region_idx ON public.sales_by_region USING btree (region);
