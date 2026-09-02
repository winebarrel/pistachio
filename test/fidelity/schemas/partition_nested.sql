CREATE TABLE public.metrics (
    at date NOT NULL,
    kind text NOT NULL,
    v double precision
) PARTITION BY RANGE (at);
CREATE TABLE public.metrics_2025 PARTITION OF public.metrics FOR VALUES FROM ('2025-01-01') TO ('2026-01-01') PARTITION BY LIST (kind);
CREATE TABLE public.metrics_2025_cpu PARTITION OF public.metrics_2025 FOR VALUES IN ('cpu');
CREATE TABLE public.metrics_2025_rest PARTITION OF public.metrics_2025 DEFAULT;
