CREATE TABLE public.pages (
    id integer NOT NULL,
    payload jsonb,
    tags text[],
    area box,
    n integer,
    CONSTRAINT pages_pkey PRIMARY KEY (id)
);
CREATE INDEX pages_payload_idx ON public.pages USING gin (payload);
CREATE INDEX pages_payload_path_idx ON public.pages USING gin (payload jsonb_path_ops);
CREATE INDEX pages_tags_idx ON public.pages USING gin (tags);
CREATE INDEX pages_area_idx ON public.pages USING gist (area);
CREATE INDEX pages_n_idx ON public.pages USING brin (n) WITH (pages_per_range='64');
CREATE UNIQUE INDEX pages_n_key ON public.pages USING btree (n) NULLS NOT DISTINCT;
