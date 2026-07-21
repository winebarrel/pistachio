CREATE SEQUENCE public.code_seq;

CREATE SEQUENCE public.order_seq
    INCREMENT BY 2
    START WITH 1000
    MAXVALUE 999999
    CACHE 5
    CYCLE;

CREATE TABLE public.items (
    id serial NOT NULL,
    ver integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    code integer DEFAULT nextval('code_seq'::regclass) NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
