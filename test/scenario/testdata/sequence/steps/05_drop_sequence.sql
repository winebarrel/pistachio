CREATE SEQUENCE public.code_seq;

CREATE TABLE public.items (
    id serial NOT NULL,
    ver integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    code integer DEFAULT nextval('code_seq'::regclass) NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
