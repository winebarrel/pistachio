package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func newPolicy(name string, command model.PolicyCommand, opts ...func(*model.Policy)) *model.Policy {
	p := &model.Policy{
		Name:       name,
		Schema:     "public",
		Table:      "documents",
		Permissive: true,
		Command:    command,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func withUsing(s string) func(*model.Policy)     { return func(p *model.Policy) { p.Using = &s } }
func withWithCheck(s string) func(*model.Policy) { return func(p *model.Policy) { p.WithCheck = &s } }
func restrictive() func(*model.Policy)           { return func(p *model.Policy) { p.Permissive = false } }
func renameFrom(s string) func(*model.Policy)    { return func(p *model.Policy) { p.RenameFrom = &s } }

func TestDiffRLS_NoChange(t *testing.T) {
	cur := &model.Table{RowSecurity: true, ForceRowSecurity: true}
	des := &model.Table{RowSecurity: true, ForceRowSecurity: true}
	assert.Empty(t, diffRLS("public.documents", cur, des))
}

func TestDiffRLS_EnableDisable(t *testing.T) {
	cur := &model.Table{}
	des := &model.Table{RowSecurity: true}
	assert.Equal(t,
		[]string{"ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;"},
		diffRLS("public.documents", cur, des))

	cur, des = &model.Table{RowSecurity: true}, &model.Table{}
	assert.Equal(t,
		[]string{"ALTER TABLE public.documents DISABLE ROW LEVEL SECURITY;"},
		diffRLS("public.documents", cur, des))
}

func TestDiffRLS_ForceNoForce(t *testing.T) {
	cur := &model.Table{}
	des := &model.Table{ForceRowSecurity: true}
	assert.Equal(t,
		[]string{"ALTER TABLE public.documents FORCE ROW LEVEL SECURITY;"},
		diffRLS("public.documents", cur, des))

	cur, des = &model.Table{ForceRowSecurity: true}, &model.Table{}
	assert.Equal(t,
		[]string{"ALTER TABLE public.documents NO FORCE ROW LEVEL SECURITY;"},
		diffRLS("public.documents", cur, des))
}

func TestDiffPolicies_NoChange(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	cur.Set("p", newPolicy("p", 'r', withUsing("true")))
	des.Set("p", newPolicy("p", 'r', withUsing("true")))

	stmts, disallowed, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Empty(t, disallowed)
}

func TestDiffPolicies_AddPolicy(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	des.Set("p", newPolicy("p", 'r', withUsing("true")))

	stmts, _, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"CREATE POLICY p ON public.documents FOR SELECT USING (true);"}, stmts)
}

func TestDiffPolicies_DropPolicy_Allowed(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	cur.Set("p", newPolicy("p", 'r', withUsing("true")))
	des := orderedmap.New[string, *model.Policy]()

	stmts, disallowed, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP POLICY p ON public.documents;"}, stmts)
	assert.Empty(t, disallowed)
}

func TestDiffPolicies_DropPolicy_Disallowed(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	cur.Set("p", newPolicy("p", 'r', withUsing("true")))
	des := orderedmap.New[string, *model.Policy]()

	stmts, disallowed, err := diffPolicies("public.documents", cur, des, DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Equal(t, []string{"-- skipped: DROP POLICY p ON public.documents;"}, disallowed)
}

func TestDiffPolicies_AlterUsing(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	cur.Set("p", newPolicy("p", 'r', withUsing("a")))
	des.Set("p", newPolicy("p", 'r', withUsing("b")))

	stmts, _, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER POLICY p ON public.documents USING (b);"}, stmts)
}

func TestDiffPolicies_RecreateOnCommandChange(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	cur.Set("p", newPolicy("p", 'r', withUsing("true")))
	des.Set("p", newPolicy("p", '*', withUsing("true")))

	stmts, _, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"DROP POLICY p ON public.documents;",
		"CREATE POLICY p ON public.documents USING (true);",
	}, stmts)
}

func TestDiffPolicies_RecreateOnPermissiveChange(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	cur.Set("p", newPolicy("p", '*', withUsing("true")))
	des.Set("p", newPolicy("p", '*', withUsing("true"), restrictive()))

	stmts, _, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"DROP POLICY p ON public.documents;",
		"CREATE POLICY p ON public.documents AS RESTRICTIVE USING (true);",
	}, stmts)
}

func TestDiffPolicies_RecreateOnUsingRemoval(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	cur.Set("p", newPolicy("p", '*', withUsing("a"), withWithCheck("b")))
	des.Set("p", newPolicy("p", '*', withWithCheck("b")))

	stmts, _, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"DROP POLICY p ON public.documents;",
		"CREATE POLICY p ON public.documents WITH CHECK (b);",
	}, stmts)
}

func TestDiffPolicies_Rename(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	cur.Set("old", newPolicy("old", 'r', withUsing("true")))
	des.Set("new", newPolicy("new", 'r', withUsing("true"), renameFrom("old")))

	stmts, _, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER POLICY old ON public.documents RENAME TO new;"}, stmts)
}

func TestDiffPolicies_RenameSourceMissing(t *testing.T) {
	cur := orderedmap.New[string, *model.Policy]()
	des := orderedmap.New[string, *model.Policy]()
	des.Set("new", newPolicy("new", 'r', withUsing("true"), renameFrom("nonexistent")))

	_, _, err := diffPolicies("public.documents", cur, des, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source policy nonexistent not found")
}

func TestEqualRoles(t *testing.T) {
	assert.True(t, equalRoles(nil, nil))
	assert.True(t, equalRoles([]string{"public"}, nil), "empty == [public]")
	assert.True(t, equalRoles([]string{"a", "b"}, []string{"b", "a"}), "order-insensitive")
	assert.False(t, equalRoles([]string{"a"}, []string{"b"}))
}

func TestExprChanged(t *testing.T) {
	a := "owner = current_user"
	b := "owner = current_user"
	c := "owner = session_user"
	assert.False(t, exprChanged(nil, nil))
	assert.True(t, exprChanged(&a, nil))
	assert.True(t, exprChanged(nil, &a))
	assert.False(t, exprChanged(&a, &b), "same expression")
	assert.True(t, exprChanged(&a, &c), "different expression")
}

func TestEqualPolicyExpr_NormalizesParens(t *testing.T) {
	// pg_get_expr wraps the top-level boolean in parentheses; the parser
	// deparses without them. Normalization through parse/deparse should
	// treat both forms as equal.
	assert.True(t, equalPolicyExpr("(owner = current_user)", "owner = current_user"))
}
