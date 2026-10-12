-- The objects the proxy needs in the PostgreSQL behind it. Running it again
-- leaves the database as it was.

-- What DSQL has under sys: the version function pistachio asks for, the job
-- table and the job wait.
CREATE SCHEMA IF NOT EXISTS sys;

CREATE OR REPLACE FUNCTION sys.dsql_major_version() RETURNS integer
LANGUAGE sql IMMUTABLE AS 'SELECT 1';

CREATE TABLE IF NOT EXISTS sys.jobs (
    job_id text PRIMARY KEY,
    status text NOT NULL,
    details text,
    job_type text NOT NULL,
    object_id oid
);

-- DSQL returns one row with the column succeeded. An index stays invalid,
-- and a constraint NOT VALID, until its job is waited for: the proxy runs the
-- job at once, and the wait stands in for the job finishing.
CREATE OR REPLACE PROCEDURE sys.wait_for_job(job_id text, INOUT succeeded boolean DEFAULT NULL)
LANGUAGE plpgsql AS $$
DECLARE
    job sys.jobs;
BEGIN
    SELECT * INTO job FROM sys.jobs j WHERE j.job_id = wait_for_job.job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'job % not found', wait_for_job.job_id USING ERRCODE = '22023';
    END IF;
    IF job.job_type = 'INDEX_BUILD' AND job.status = 'completed' THEN
        UPDATE pg_catalog.pg_index SET indisvalid = true WHERE indexrelid = job.object_id;
    ELSIF job.job_type = 'VALIDATE_CONSTRAINT' AND job.status = 'completed' THEN
        UPDATE pg_catalog.pg_constraint SET convalidated = true WHERE oid = job.object_id;
    END IF;
    succeeded := job.status = 'completed';
END $$;

-- The functions the proxy rewrites statements to.
CREATE SCHEMA IF NOT EXISTS dsql_proxy;

-- run_job runs the statement of an ASYNC job at once and returns its job ID.
-- A job that finds a violating row fails, as on DSQL, and leaves the
-- statement undone. Any other error fails the statement itself. An index it
-- builds is marked invalid, and a constraint it validates NOT VALID, until
-- sys.wait_for_job, as on DSQL while the job runs.
CREATE OR REPLACE FUNCTION dsql_proxy.run_job(job_type text, stmt text)
RETURNS TABLE (job_id text)
LANGUAGE plpgsql AS $$
DECLARE
    id text := translate(substr(md5(random()::text || clock_timestamp()::text), 1, 26), '0189', 'wxyz');
    obj oid;
    before oid[];
BEGIN
    BEGIN
        IF job_type = 'INDEX_BUILD' THEN
            SELECT array_agg(indexrelid) INTO before FROM pg_catalog.pg_index;
        ELSE
            SELECT array_agg(oid) INTO before FROM pg_catalog.pg_constraint WHERE NOT convalidated;
        END IF;
        EXECUTE stmt;
        IF job_type = 'INDEX_BUILD' THEN
            -- The index the statement created. The xmin of its row is not the
            -- transaction's own: this block runs in a subtransaction.
            SELECT indexrelid INTO obj FROM pg_catalog.pg_index
            WHERE NOT indexrelid = ANY (before);
            UPDATE pg_catalog.pg_index SET indisvalid = false WHERE indexrelid = obj;
        ELSE
            -- The constraint the statement validated.
            SELECT oid INTO obj FROM pg_catalog.pg_constraint
            WHERE oid = ANY (before) AND convalidated;
            UPDATE pg_catalog.pg_constraint SET convalidated = false WHERE oid = obj;
        END IF;
        INSERT INTO sys.jobs VALUES (id, 'completed', NULL, job_type, obj);
    EXCEPTION WHEN unique_violation OR check_violation OR foreign_key_violation OR not_null_violation THEN
        INSERT INTO sys.jobs VALUES (id, 'failed', SQLERRM, job_type, NULL);
    END;
    RETURN QUERY SELECT id;
END $$;

-- refuse fails a statement DSQL does not take, with DSQL's code and message.
CREATE OR REPLACE FUNCTION dsql_proxy.refuse(code text, message text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = code, MESSAGE = message;
END $$;

-- refuse_drop_pkey fails the drop of a primary key, which DSQL does not take.
CREATE OR REPLACE FUNCTION dsql_proxy.refuse_drop_pkey(rel text, con text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_catalog.pg_constraint
        WHERE conrelid = rel::regclass AND conname = con AND contype = 'p'
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '0A000', MESSAGE = 'unsupported ALTER TABLE DROP CONSTRAINT statement';
    END IF;
END $$;

-- The catalog values DSQL reports differently from PostgreSQL. A primary key
-- holds every other column of its table as INCLUDE.
CREATE OR REPLACE FUNCTION dsql_proxy.pg_get_constraintdef(oid, boolean DEFAULT false) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN c.contype = 'p' AND inc.cols IS NOT NULL
        THEN pg_catalog.pg_get_constraintdef($1, $2) || ' INCLUDE (' || inc.cols || ')'
        ELSE pg_catalog.pg_get_constraintdef($1, $2)
    END
    FROM pg_catalog.pg_constraint c
    LEFT JOIN LATERAL (
        SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) AS cols
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = c.conrelid AND a.attnum > 0 AND NOT a.attisdropped
            AND NOT a.attnum = ANY (c.conkey)
    ) inc ON true
    WHERE c.oid = $1
$$;

-- Every index has the access method btree_index.
CREATE OR REPLACE FUNCTION dsql_proxy.pg_get_indexdef(oid) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT regexp_replace(pg_catalog.pg_get_indexdef($1), ' USING btree \(', ' USING btree_index (')
$$;

CREATE OR REPLACE FUNCTION dsql_proxy.pg_get_indexdef(oid, integer, boolean) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT regexp_replace(pg_catalog.pg_get_indexdef($1, $2, $3), ' USING btree \(', ' USING btree_index (')
$$;

-- Every column of a type that can be compressed is compressed with lz4.
CREATE OR REPLACE FUNCTION dsql_proxy.attcompression(a pg_catalog.pg_attribute) RETURNS "char"
LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN t.typstorage <> 'p' THEN 'l'::"char" ELSE a.attcompression END
    FROM pg_catalog.pg_type t
    WHERE t.oid = a.atttypid
$$;
