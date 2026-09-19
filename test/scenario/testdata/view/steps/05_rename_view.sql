CREATE TABLE public.employees (
    id integer NOT NULL,
    name text NOT NULL,
    dept text NOT NULL,
    salary integer NOT NULL,
    CONSTRAINT employees_pkey PRIMARY KEY (id)
);

CREATE VIEW public.staff WITH (security_invoker='true') AS
 SELECT employees.id,
    upper(employees.name) AS name,
    employees.dept,
    employees.salary
   FROM public.employees;

COMMENT ON VIEW public.staff IS 'Employees without the private columns';
COMMENT ON COLUMN public.staff.name IS 'Upper-cased display name';

-- pista:renamed-from public.eng_staff
CREATE VIEW public.engineering AS
 SELECT staff.id,
    staff.name
   FROM public.staff
  WHERE (staff.dept = 'eng'::text);
