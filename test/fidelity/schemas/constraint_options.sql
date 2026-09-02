CREATE TABLE public.tickets (
    id integer NOT NULL,
    code text NOT NULL,
    label text,
    slot integer,
    CONSTRAINT tickets_pkey PRIMARY KEY (id) INCLUDE (code),
    CONSTRAINT tickets_code_key UNIQUE (code) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT tickets_slot_key UNIQUE NULLS NOT DISTINCT (slot)
);
