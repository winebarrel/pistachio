CREATE SEQUENCE public.code_seq;

-- pista:renamed-from public.order_seq
CREATE SEQUENCE public.seq_orders
    INCREMENT BY 2
    START WITH 1000
    MAXVALUE 999999
    CACHE 5
    CYCLE;

COMMENT ON SEQUENCE public.seq_orders IS 'Order id generator';

CREATE TABLE public.items (
    id serial NOT NULL,
    ver integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    code integer DEFAULT nextval('code_seq'::regclass) NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
