CREATE TABLE public.raw (
    id integer NOT NULL,
    CONSTRAINT raw_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.raw_count AS SELECT count(*) AS n FROM public.raw WITH NO DATA;
