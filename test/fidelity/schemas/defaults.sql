CREATE TABLE public.settings (
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    ttl interval DEFAULT '01:00:00'::interval NOT NULL,
    flags text[] DEFAULT '{}'::text[] NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    ratio double precision DEFAULT '-1'::numeric NOT NULL,
    label text DEFAULT ('x'::text || 'y'::text) NOT NULL,
    kind character varying(10) DEFAULT 'a'::character varying NOT NULL,
    ok boolean DEFAULT true NOT NULL,
    CONSTRAINT settings_pkey PRIMARY KEY (id)
);
