CREATE TABLE public.orders (id integer NOT NULL, amount numeric, CONSTRAINT orders_pkey PRIMARY KEY (id));
CREATE VIEW public.big_orders AS SELECT id, amount FROM public.orders WHERE amount > 100;
CREATE FUNCTION public.order_total() RETURNS numeric
    LANGUAGE sql STABLE
    BEGIN ATOMIC
        SELECT sum(amount) FROM public.big_orders;
    END;
CREATE FUNCTION public.twice(x numeric) RETURNS numeric
    LANGUAGE sql IMMUTABLE
    RETURN x * 2;
CREATE FUNCTION public.doubled_total() RETURNS numeric
    LANGUAGE sql STABLE
    RETURN public.twice(public.order_total());
CREATE PROCEDURE public.add_order(IN i integer, IN a numeric)
    LANGUAGE sql
    BEGIN ATOMIC
        INSERT INTO public.orders (id, amount) VALUES (i, a);
        SELECT public.order_total();
    END;
CREATE VIEW public.summary AS SELECT public.doubled_total() AS total;
