CREATE TYPE public.status AS ENUM ('active', 'inactive', 'banned');
CREATE TYPE public.address AS (city text, zip text);
CREATE DOMAIN public.email AS text NOT NULL DEFAULT 'x@example.com'::text CONSTRAINT email_check CHECK ((VALUE ~ '@'::text));
CREATE TABLE public.accounts (
    id integer NOT NULL,
    st public.status DEFAULT 'active'::public.status NOT NULL,
    home public.address,
    mail public.email,
    CONSTRAINT accounts_pkey PRIMARY KEY (id)
);
