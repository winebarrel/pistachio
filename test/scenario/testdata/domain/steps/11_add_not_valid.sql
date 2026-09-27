CREATE DOMAIN public.email AS text
    CONSTRAINT email_format CHECK ((VALUE ~ '@'::text));
ALTER DOMAIN public.email ADD CONSTRAINT email_length CHECK ((length(VALUE) <= 320)) NOT VALID;

CREATE TABLE public.users (
    id integer NOT NULL,
    addr public.email NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
