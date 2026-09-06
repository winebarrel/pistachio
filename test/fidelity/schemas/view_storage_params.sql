CREATE TABLE public.accounts (
    id integer NOT NULL,
    owner text NOT NULL,
    CONSTRAINT accounts_pkey PRIMARY KEY (id)
);
CREATE VIEW public.my_accounts WITH (security_barrier='true', security_invoker='true') AS SELECT accounts.id FROM public.accounts WHERE (accounts.id > 0);
CREATE MATERIALIZED VIEW public.account_counts WITH (autovacuum_enabled='off', fillfactor='70') AS SELECT count(*) AS n FROM public.accounts;
