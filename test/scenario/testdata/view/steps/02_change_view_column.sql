-- staff.name changes shape, which CREATE OR REPLACE cannot do, so both views
-- are dropped and re-created, the dependent one first.
CREATE TABLE public.employees (
    id integer NOT NULL,
    name text NOT NULL,
    dept text NOT NULL,
    salary integer NOT NULL,
    CONSTRAINT employees_pkey PRIMARY KEY (id)
);

CREATE VIEW public.staff AS
 SELECT employees.id,
    upper(employees.name) AS name,
    employees.dept,
    employees.salary
   FROM public.employees;

CREATE VIEW public.eng_staff AS
 SELECT staff.id,
    staff.name
   FROM public.staff
  WHERE (staff.dept = 'eng'::text);
