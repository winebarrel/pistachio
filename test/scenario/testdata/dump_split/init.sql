-- One of each object kind dump writes its own file for.
CREATE TYPE public.order_status AS ENUM (
    'pending',
    'shipped'
);

CREATE TYPE public.address AS (
    street text,
    city text
);

CREATE DOMAIN public.email AS text
    CONSTRAINT email_format CHECK ((VALUE ~ '@'::text));

CREATE SEQUENCE public.code_seq;

CREATE TABLE public.customers (
    id integer NOT NULL,
    addr public.email NOT NULL,
    home public.address,
    code integer DEFAULT nextval('code_seq'::regclass) NOT NULL,
    CONSTRAINT customers_pkey PRIMARY KEY (id)
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL,
    status public.order_status DEFAULT 'pending'::public.order_status NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES public.customers(id)
);

CREATE INDEX orders_status_idx ON public.orders USING btree (status);

CREATE VIEW public.open_orders AS
 SELECT orders.id,
    orders.customer_id
   FROM public.orders
  WHERE (orders.status = 'pending'::order_status);

CREATE MATERIALIZED VIEW public.order_counts AS
 SELECT orders.customer_id,
    count(*) AS total
   FROM public.orders
  GROUP BY orders.customer_id
  WITH NO DATA;
