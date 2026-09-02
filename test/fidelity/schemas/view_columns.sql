-- No WITH CHECK OPTION: it is not managed, so the dump restores the view
-- without it. TODO.md records it.
CREATE TABLE public.staff (
    id integer NOT NULL,
    name text NOT NULL,
    dept text NOT NULL,
    CONSTRAINT staff_pkey PRIMARY KEY (id)
);
CREATE VIEW public.sales_staff (staff_id, staff_name) AS SELECT staff.id, staff.name FROM public.staff WHERE (staff.dept = 'sales'::text);
CREATE MATERIALIZED VIEW public.staff_by_dept (dept_name, n) AS SELECT staff.dept, count(*) AS count FROM public.staff GROUP BY staff.dept;
