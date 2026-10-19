-- Owned sequences with custom names, including a serial sequence that was
-- renamed, and a serial sequence whose options were changed. pista dump
-- writes each with its name, options and OWNED BY.
CREATE SEQUENCE public.custom_user_id_seq
    AS integer
    START WITH 100
    INCREMENT BY 5
    CACHE 10;

CREATE TABLE public.users (
    id integer DEFAULT nextval('public.custom_user_id_seq'::regclass) NOT NULL,
    note integer
);

ALTER SEQUENCE public.custom_user_id_seq OWNED BY public.users.id;

CREATE SEQUENCE public.note_seq;

ALTER SEQUENCE public.note_seq OWNED BY public.users.note;

CREATE TABLE public.items (
    id serial NOT NULL
);

ALTER SEQUENCE public.items_id_seq RENAME TO item_ids;

-- The catalog reads this one as it reads a bigserial column whose sequence
-- options were changed.
CREATE SEQUENCE public.orders_id_seq
    START WITH 1000
    INCREMENT BY 10
    CACHE 20;

CREATE TABLE public.orders (
    id bigint DEFAULT nextval('public.orders_id_seq'::regclass) NOT NULL
);

ALTER SEQUENCE public.orders_id_seq OWNED BY public.orders.id;

-- A serial column widened to bigint keeps its integer sequence.
CREATE TABLE public.events (
    id serial NOT NULL
);

ALTER TABLE public.events ALTER COLUMN id SET DATA TYPE bigint;
