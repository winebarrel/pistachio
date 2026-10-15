package dsqlproxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newSession(inTx bool) *session {
	return &session{inTx: func() bool { return inTx }}
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

func TestRewriteMultipleDDLAsync(t *testing.T) {
	// CREATE INDEX ASYNC as the second DDL statement of a transaction.
	s := newSession(true)
	s.rewrite("CREATE TABLE t (c int)")
	got, tags := s.rewrite("CREATE INDEX ASYNC i ON t (c)")
	assert.Equal(t, refuse(msgMultipleDDL), got)
	assert.Equal(t, []string{""}, tags)

	// ALTER TABLE ASYNC ... VALIDATE CONSTRAINT as the second one.
	s = newSession(true)
	s.rewrite("CREATE TABLE t (c int)")
	got, tags = s.rewrite("ALTER TABLE ASYNC t VALIDATE CONSTRAINT k")
	assert.Equal(t, refuse(msgMultipleDDL), got)
	assert.Equal(t, []string{""}, tags)
}

func TestRewriteTableLevelPrimaryKey(t *testing.T) {
	got, tags := newSession(false).rewrite("CREATE TABLE t (id bigint, PRIMARY KEY (id))")
	assert.Equal(t, "CREATE TABLE t (id bigint, PRIMARY KEY (id))", got)
	assert.Equal(t, []string{""}, tags)
}

func TestRewriteUnparsable(t *testing.T) {
	got, tags := newSession(false).rewrite("CREATE TABLE (")
	assert.Equal(t, "CREATE TABLE (", got)
	assert.Equal(t, []string{""}, tags)
}
