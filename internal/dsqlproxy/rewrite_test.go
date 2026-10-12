package dsqlproxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newSession(inTx bool) *session {
	return &session{inTx: func() bool { return inTx }}
}

func TestRewriteRefusal(t *testing.T) {
	tests := []struct {
		sql string
		msg string
	}{
		{"CREATE INDEX i ON t (c)", msgAsyncIndex},
		{"CREATE INDEX CONCURRENTLY i ON t (c)", msgIndexConcurrent},
		{"CREATE INDEX ASYNC i ON t USING btree (c)", msgIndexUsing},
		{"CREATE INDEX ASYNC CONCURRENTLY i ON t (c)", msgIndexConcurrent},
		{"DROP INDEX CONCURRENTLY i", msgDropConcurrent},
		{"ALTER TABLE t ADD COLUMN c int DEFAULT 0", msgAddColumn},
		{"ALTER TABLE t ADD COLUMN c int NOT NULL", msgAddColumn},
		{"ALTER TABLE t ADD COLUMN c bigint GENERATED ALWAYS AS IDENTITY (CACHE 1)", msgAddColumn},
		{"ALTER TABLE t ADD COLUMN c text COLLATE \"C\"", msgCollate},
		{"ALTER TABLE t ADD COLUMN c text COMPRESSION lz4", msgCompression},
		{"ALTER TABLE t ALTER COLUMN c SET NOT NULL", msgSetNotNull},
		{"ALTER TABLE t ALTER COLUMN c TYPE bigint", msgSetDataType},
		{"ALTER TABLE t ALTER COLUMN c SET COMPRESSION lz4", msgSetCompression},
		{"ALTER TABLE t ADD PRIMARY KEY (c)", msgAddConstraint},
		{"ALTER TABLE t ADD CONSTRAINT k UNIQUE (c)", msgAddConstraint},
		{"ALTER TABLE t ADD CONSTRAINT k CHECK (c > 0)", msgAddConstraint},
		{"ALTER TABLE t ADD CONSTRAINT k FOREIGN KEY (c) REFERENCES u (c)", msgAddConstraint},
		{"ALTER TABLE t VALIDATE CONSTRAINT k", msgValidate},
		{"ALTER TABLE ASYNC t ADD COLUMN c int", msgAsyncAlter},
		{"ALTER TABLE t ENABLE ROW LEVEL SECURITY", msgRowSecurity},
		{"CREATE TABLE t (c int) PARTITION BY RANGE (c)", msgPartitionBy},
		{"CREATE TABLE t1 PARTITION OF t FOR VALUES FROM (1) TO (2)", msgPartitionOf},
		{"CREATE TABLE t (c text COLLATE \"C\")", msgCollate},
		{"CREATE TABLE t (c text COMPRESSION lz4)", msgCompression},
		{"CREATE TABLE t (c bigint GENERATED ALWAYS AS IDENTITY)", msgIdentityCache},
		{"CREATE SEQUENCE s", msgSequenceCache},
		{"ALTER DOMAIN d ADD CONSTRAINT k CHECK (VALUE > 0)", msgAlterDomain},
		{"ALTER DOMAIN d SET NOT NULL", msgAlterDomain},
		{"CREATE TYPE e AS ENUM ('a')", "unsupported statement: CreateEnum"},
		{"CREATE TYPE c AS (a int)", "unsupported statement: CompositeType"},
		{"CREATE POLICY p ON t USING (true)", "unsupported statement: CreatePolicy"},
		{"CREATE EXTENSION pgcrypto", "unsupported statement: CreateExtension"},
		{"CREATE MATERIALIZED VIEW v AS SELECT 1", "unsupported statement: CreateTableAs"},
		{"SELECT pg_try_advisory_lock(1)", "function pg_try_advisory_lock not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got, tags := newSession(false).rewrite(tt.sql)
			assert.Equal(t, refuse(tt.msg), got)
			assert.Equal(t, []string{""}, tags)
		})
	}
}

func TestRewriteAllowed(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t (id bigint PRIMARY KEY, c text, n bigint GENERATED ALWAYS AS IDENTITY (CACHE 1))",
		"CREATE SEQUENCE s CACHE 1",
		"ALTER TABLE t ADD COLUMN c int",
		"ALTER TABLE t ALTER COLUMN c SET DEFAULT 0",
		"ALTER TABLE t ALTER COLUMN c DROP NOT NULL",
		"ALTER TABLE t DROP COLUMN c",
		"ALTER TABLE ONLY t ADD CONSTRAINT k CHECK (c > 0) NOT VALID",
		"ALTER TABLE t ADD CONSTRAINT k FOREIGN KEY (c) REFERENCES u (c) NOT VALID",
		"ALTER TABLE t ADD CONSTRAINT k UNIQUE USING INDEX i",
		"ALTER INDEX i RENAME TO j",
		"DROP INDEX i",
		"COMMENT ON TABLE t IS 'x'",
		"SET search_path TO public",
		"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
		"SELECT 1",
	} {
		t.Run(sql, func(t *testing.T) {
			got, tags := newSession(false).rewrite(sql)
			assert.Equal(t, sql, got)
			assert.Equal(t, []string{""}, tags)
		})
	}
}

func TestRewriteAsync(t *testing.T) {
	got, tags := newSession(false).rewrite("CREATE UNIQUE INDEX ASYNC i ON t (c)")
	assert.Equal(t, `SELECT job_id FROM dsql_proxy.run_job('INDEX_BUILD', 'CREATE UNIQUE INDEX i ON t (c)')`, got)
	assert.Equal(t, []string{"CREATE INDEX"}, tags)

	got, tags = newSession(false).rewrite("ALTER TABLE ASYNC ONLY t VALIDATE CONSTRAINT k")
	assert.Equal(t, `SELECT job_id FROM dsql_proxy.run_job('VALIDATE_CONSTRAINT', 'ALTER TABLE ONLY t VALIDATE CONSTRAINT k')`, got)
	assert.Equal(t, []string{"ALTER TABLE"}, tags)
}

func TestRewriteDropConstraint(t *testing.T) {
	got, tags := newSession(false).rewrite("ALTER TABLE public.t DROP CONSTRAINT k")
	assert.Equal(t, `DO $dsql_proxy$BEGIN PERFORM dsql_proxy.refuse_drop_pkey('"public"."t"', 'k'); EXECUTE 'ALTER TABLE public.t DROP CONSTRAINT k'; END$dsql_proxy$`, got)
	assert.Equal(t, []string{"ALTER TABLE"}, tags)
}

func TestRewriteCatalog(t *testing.T) {
	got, _ := newSession(false).rewrite(
		"SELECT pg_catalog.pg_get_constraintdef(c.oid, true), pg_get_indexdef(i.indexrelid), a.attcompression FROM x")
	assert.Equal(t,
		"SELECT dsql_proxy.pg_get_constraintdef(c.oid, true), dsql_proxy.pg_get_indexdef(i.indexrelid), dsql_proxy.attcompression(a) FROM x", got)
}

func TestRewriteMultipleDDL(t *testing.T) {
	// The second DDL statement of an explicit transaction.
	s := newSession(true)
	got, _ := s.rewrite("CREATE TABLE t (c int)")
	assert.Equal(t, "CREATE TABLE t (c int)", got)
	got, _ = s.rewrite("CREATE TABLE u (c int)")
	assert.Equal(t, refuse(msgMultipleDDL), got)

	// The count starts again after the transaction.
	s.txEnded()
	got, _ = s.rewrite("CREATE TABLE u (c int)")
	assert.Equal(t, "CREATE TABLE u (c int)", got)

	// One query string runs in one transaction.
	got, tags := newSession(false).rewrite("CREATE TABLE t (c int); CREATE TABLE u (c int)")
	assert.Equal(t, "CREATE TABLE t (c int);\n"+refuse(msgMultipleDDL), got)
	assert.Equal(t, []string{"", ""}, tags)

	// Outside a transaction, each statement is one of its own.
	s = newSession(false)
	s.rewrite("CREATE TABLE t (c int)")
	got, _ = s.rewrite("CREATE TABLE u (c int)")
	assert.Equal(t, "CREATE TABLE u (c int)", got)
}

func TestRewriteUnparsable(t *testing.T) {
	got, tags := newSession(false).rewrite("CREATE TABLE (")
	assert.Equal(t, "CREATE TABLE (", got)
	assert.Equal(t, []string{""}, tags)
}
