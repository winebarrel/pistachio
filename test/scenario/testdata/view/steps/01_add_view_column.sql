CREATE TABLE public.employees (
    id integer NOT NULL,
    name text NOT NULL,
    dept text NOT NULL,
    salary integer NOT NULL,
    CONSTRAINT employees_pkey PRIMARY KEY (id)
);

CREATE VIEW public.staff AS
 SELECT employees.id,
    employees.name,
    employees.dept,
    employees.salary
   FROM public.employees;

CREATE VIEW public.eng_staff AS
 SELECT staff.id,
    staff.name
   FROM public.staff
  WHERE (staff.dept = 'eng'::text);
