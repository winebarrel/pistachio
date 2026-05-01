package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPolicyCommand_String(t *testing.T) {
	assert.Equal(t, "ALL", PolicyCommand('*').String())
	assert.Equal(t, "SELECT", PolicyCommand('r').String())
	assert.Equal(t, "INSERT", PolicyCommand('a').String())
	assert.Equal(t, "UPDATE", PolicyCommand('w').String())
	assert.Equal(t, "DELETE", PolicyCommand('d').String())
	assert.Equal(t, "", PolicyCommand(0).String())
}

func TestPolicyCommand_IsAll(t *testing.T) {
	assert.True(t, PolicyCommand('*').IsAll())
	assert.False(t, PolicyCommand('r').IsAll())
}

func TestPolicy_String(t *testing.T) {
	p := &Policy{Name: "p", Schema: "public", Table: "t", Command: '*', Permissive: true}
	s := p.String()
	assert.Contains(t, s, "p")
}

func TestPolicy_SQL_minimal(t *testing.T) {
	using := "true"
	p := Policy{Name: "p", Schema: "public", Table: "t", Permissive: true, Command: '*', Using: &using}
	assert.Equal(t, "CREATE POLICY p ON public.t USING (true);", p.SQL())
}

func TestPolicy_SQL_restrictive(t *testing.T) {
	using := "owner = current_user"
	p := Policy{Name: "p", Schema: "public", Table: "t", Permissive: false, Command: '*', Using: &using}
	assert.Equal(t, "CREATE POLICY p ON public.t AS RESTRICTIVE USING (owner = current_user);", p.SQL())
}

func TestPolicy_SQL_specific_command(t *testing.T) {
	using := "true"
	p := Policy{Name: "p", Schema: "public", Table: "t", Permissive: true, Command: 'r', Using: &using}
	assert.Equal(t, "CREATE POLICY p ON public.t FOR SELECT USING (true);", p.SQL())
}

func TestPolicy_SQL_with_check(t *testing.T) {
	using := "true"
	wc := "owner = current_user"
	p := Policy{
		Name: "p", Schema: "public", Table: "t", Permissive: true, Command: '*',
		Using: &using, WithCheck: &wc,
	}
	assert.Equal(t,
		"CREATE POLICY p ON public.t USING (true) WITH CHECK (owner = current_user);",
		p.SQL())
}

func TestPolicy_SQL_with_check_only(t *testing.T) {
	wc := "owner = current_user"
	p := Policy{Name: "p", Schema: "public", Table: "t", Permissive: true, Command: 'a', WithCheck: &wc}
	assert.Equal(t, "CREATE POLICY p ON public.t FOR INSERT WITH CHECK (owner = current_user);", p.SQL())
}

func TestPolicy_SQL_role_named(t *testing.T) {
	using := "true"
	p := Policy{
		Name: "p", Schema: "public", Table: "t", Permissive: true, Command: '*',
		Roles: []string{"app_user"}, Using: &using,
	}
	assert.Equal(t, "CREATE POLICY p ON public.t TO app_user USING (true);", p.SQL())
}

func TestPolicy_SQL_role_reserved(t *testing.T) {
	using := "true"
	p := Policy{
		Name: "p", Schema: "public", Table: "t", Permissive: true, Command: '*',
		Roles: []string{"current_user"}, Using: &using,
	}
	assert.Equal(t, "CREATE POLICY p ON public.t TO current_user USING (true);", p.SQL())
}

func TestPolicy_SQL_role_multiple(t *testing.T) {
	using := "true"
	p := Policy{
		Name: "p", Schema: "public", Table: "t", Permissive: true, Command: '*',
		Roles: []string{"a", "b"}, Using: &using,
	}
	assert.Equal(t, "CREATE POLICY p ON public.t TO a, b USING (true);", p.SQL())
}

// rolesClause returns "" when only PUBLIC is in the role list (catalog +
// parser both normalize the omitted-TO case to ["public"]), so emit must
// drop the TO clause entirely.
func TestPolicy_SQL_role_public_only(t *testing.T) {
	using := "true"
	p := Policy{
		Name: "p", Schema: "public", Table: "t", Permissive: true, Command: '*',
		Roles: []string{"public"}, Using: &using,
	}
	assert.Equal(t, "CREATE POLICY p ON public.t USING (true);", p.SQL())
}

// Empty Roles list: should also collapse to no TO clause. This branch is
// dead code from the parser/catalog paths (both inject PUBLIC) but is
// reachable from direct construction and worth covering.
func TestPolicy_SQL_role_empty(t *testing.T) {
	using := "true"
	p := Policy{
		Name: "p", Schema: "public", Table: "t", Permissive: true, Command: '*',
		Using: &using,
	}
	assert.Equal(t, "CREATE POLICY p ON public.t USING (true);", p.SQL())
}
