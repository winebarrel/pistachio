CREATE TABLE public.logs (
    id bigint NOT NULL,
    at date NOT NULL,
    body text
) PARTITION BY RANGE (at);
CREATE TABLE public.logs_2025 PARTITION OF public.logs FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE public.logs_2026 PARTITION OF public.logs FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
-- The partitions take their copies of the parent's index under the names
-- PostgreSQL gives them.
CREATE INDEX logs_at_idx ON public.logs USING btree (at);
-- A copy attached under a name of its own.
CREATE INDEX logs_id_idx ON ONLY public.logs USING btree (id);
CREATE INDEX logs_2025_by_id ON public.logs_2025 USING btree (id);
CREATE INDEX logs_2026_by_id ON public.logs_2026 USING btree (id);
ALTER INDEX public.logs_id_idx ATTACH PARTITION public.logs_2025_by_id;
ALTER INDEX public.logs_id_idx ATTACH PARTITION public.logs_2026_by_id;
-- An index the partition has alone.
CREATE INDEX logs_2025_body_idx ON public.logs_2025 USING btree (body);
