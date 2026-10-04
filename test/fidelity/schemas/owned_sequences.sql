-- Sequences a column owns under a name serial would not give them, and one
-- behind a serial column whose table was renamed. pista dump writes each with
-- its name, its options and its OWNED BY.
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

ALTER TABLE public.items RENAME TO goods;
