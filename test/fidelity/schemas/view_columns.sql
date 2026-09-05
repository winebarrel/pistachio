CREATE TABLE public.staff (
    id integer NOT NULL,
    name text NOT NULL,
    dept text NOT NULL,
    CONSTRAINT staff_pkey PRIMARY KEY (id)
);
CREATE VIEW public.sales_staff (staff_id, staff_name) AS SELECT staff.id, staff.name FROM public.staff WHERE (staff.dept = 'sales'::text);
CREATE VIEW public.checked_staff AS SELECT staff.id, staff.name, staff.dept FROM public.staff WHERE (staff.dept = 'sales'::text)
  WITH CASCADED CHECK OPTION;
CREATE VIEW public.local_staff AS SELECT checked_staff.id, checked_staff.name FROM public.checked_staff WHERE (checked_staff.name <> ''::text)
  WITH LOCAL CHECK OPTION;
CREATE MATERIALIZED VIEW public.staff_by_dept (dept_name, n) AS SELECT staff.dept, count(*) AS count FROM public.staff GROUP BY staff.dept;
