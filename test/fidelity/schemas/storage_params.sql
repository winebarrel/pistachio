CREATE TABLE public.events (
    id integer NOT NULL,
    body text,
    CONSTRAINT events_pkey PRIMARY KEY (id)
) WITH (autovacuum_enabled='off', autovacuum_vacuum_scale_factor='0.05', fillfactor='70', toast.autovacuum_enabled='off');
CREATE INDEX events_body_idx ON public.events USING btree (body) WITH (deduplicate_items='off', fillfactor='80');
