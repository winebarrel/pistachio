-- No index on the partitioned parent: `dump` writes each table with its own
-- indexes, so the parent's index is created before the partitions, and
-- PostgreSQL then creates the partition's copy itself under the name the dump
-- goes on to declare. TODO.md records it.
CREATE TABLE public.logs (
    id bigint NOT NULL,
    at date NOT NULL,
    body text
) PARTITION BY RANGE (at);
CREATE TABLE public.logs_2025 PARTITION OF public.logs FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE public.logs_2026 PARTITION OF public.logs FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE INDEX logs_2025_body_idx ON public.logs_2025 USING btree (body);
