CREATE TABLE public.tenants (
    id integer NOT NULL,
    kind text NOT NULL
) PARTITION BY LIST (kind);
CREATE TABLE public.tenants_ab PARTITION OF public.tenants FOR VALUES IN ('a', 'b');
CREATE TABLE public.tenants_rest PARTITION OF public.tenants DEFAULT;
CREATE TABLE public.shards (
    id integer NOT NULL
) PARTITION BY HASH (id);
CREATE TABLE public.shards_0 PARTITION OF public.shards FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE public.shards_1 PARTITION OF public.shards FOR VALUES WITH (MODULUS 2, REMAINDER 1);
