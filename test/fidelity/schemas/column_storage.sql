CREATE TABLE public.docs (
    id integer NOT NULL,
    body text,
    note text,
    CONSTRAINT docs_pkey PRIMARY KEY (id)
);
ALTER TABLE public.docs ALTER COLUMN body SET STORAGE EXTERNAL;
ALTER TABLE public.docs ALTER COLUMN note SET COMPRESSION pglz;
