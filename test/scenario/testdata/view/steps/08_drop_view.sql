CREATE TABLE public.employees (
    id integer NOT NULL,
    name text NOT NULL,
    dept text NOT NULL,
    salary integer NOT NULL,
    CONSTRAINT employees_pkey PRIMARY KEY (id)
);
