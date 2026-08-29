CREATE TABLE public.records (
    id integer NOT NULL,
    owner text NOT NULL,
    CONSTRAINT records_pkey PRIMARY KEY (id)
);
ALTER TABLE public.records ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.records FORCE ROW LEVEL SECURITY;
CREATE POLICY records_select ON public.records FOR SELECT USING ((owner = CURRENT_USER));
CREATE POLICY records_insert ON public.records AS RESTRICTIVE FOR INSERT WITH CHECK ((owner IS NOT NULL));
