-- Mixed schema: a standalone sequence (managed) alongside a serial column and
-- an identity column (each owns an auto-created sequence that must stay
-- unmanaged). The standalone code_seq is referenced by a column default.
CREATE SEQUENCE public.code_seq;

CREATE TABLE public.items (
    id serial NOT NULL,
    ver integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    code integer DEFAULT nextval('code_seq'::regclass) NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
