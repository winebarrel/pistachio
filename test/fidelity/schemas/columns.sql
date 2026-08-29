CREATE TABLE public.users (
    id integer GENERATED ALWAYS AS IDENTITY,
    seq_id bigserial NOT NULL,
    first_name text NOT NULL,
    last_name text NOT NULL,
    full_name text GENERATED ALWAYS AS ((first_name || ' '::text) || last_name) STORED,
    joined date DEFAULT CURRENT_DATE NOT NULL,
    score numeric(10,2) DEFAULT 0.0,
    tags text[],
    payload jsonb,
    uid uuid,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
