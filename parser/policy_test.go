package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/parser"
)

// Each test exercises a specific branch in parser/policy.go via the public
// ParseSQL entry point so coverage credits the parser package directly
// (integration tests in pistachio_test do not contribute here).

func TestParseSQL_Policy_DefaultSchemaFallback(t *testing.T) {
	// Unqualified table reference in CREATE POLICY → schema falls back to
	// defaultSchema (public).
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON t USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	tbl, _ := result.Tables.GetOk("public.t")
	pol, ok := tbl.Policies.GetOk("p")
	require.True(t, ok)
	assert.Equal(t, "public", pol.Schema)
}

func TestParseSQL_Policy_RoleSpec_Public(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t TO PUBLIC USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, []string{"public"}, pol.Roles)
}

func TestParseSQL_Policy_RoleSpec_CurrentUser(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t TO current_user USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, []string{"current_user"}, pol.Roles)
}

func TestParseSQL_Policy_RoleSpec_SessionUser(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t TO session_user USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, []string{"session_user"}, pol.Roles)
}

func TestParseSQL_Policy_RoleSpec_CurrentRole(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t TO current_role USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, []string{"current_role"}, pol.Roles)
}

func TestParseSQL_Policy_RoleSpec_NamedRole(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t TO app_user USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, []string{"app_user"}, pol.Roles)
}

func TestParseSQL_Policy_Command_Insert(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t FOR INSERT WITH CHECK (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, "INSERT", pol.Command.String())
}

func TestParseSQL_Policy_Command_Update(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t FOR UPDATE USING (true) WITH CHECK (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, "UPDATE", pol.Command.String())
}

func TestParseSQL_Policy_Command_Delete(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t FOR DELETE USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	pol, _ := result.Tables.Get("public.t").Policies.GetOk("p")
	assert.Equal(t, "DELETE", pol.Command.String())
}

func TestParseSQL_Policy_DisableRowSecurity(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.t DISABLE ROW LEVEL SECURITY;`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	tbl, _ := result.Tables.GetOk("public.t")
	assert.False(t, tbl.RowSecurity)
}

func TestParseSQL_Policy_NoForceRowSecurity(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.t FORCE ROW LEVEL SECURITY;
ALTER TABLE public.t NO FORCE ROW LEVEL SECURITY;`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	tbl, _ := result.Tables.GetOk("public.t")
	assert.False(t, tbl.ForceRowSecurity)
}

func TestParseSQL_Policy_RestrictivePermissive(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.t AS RESTRICTIVE FOR ALL USING (true);
CREATE POLICY q ON public.t AS PERMISSIVE FOR ALL USING (true);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	tbl, _ := result.Tables.GetOk("public.t")
	p, _ := tbl.Policies.GetOk("p")
	q, _ := tbl.Policies.GetOk("q")
	assert.False(t, p.Permissive)
	assert.True(t, q.Permissive)
}
