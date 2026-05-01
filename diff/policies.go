package diff

import (
	"fmt"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// diffRLS emits ALTER TABLE ... ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL
// SECURITY statements for changes to the table-level RLS flags.
func diffRLS(fqtn string, current, desired *model.Table) []string {
	var stmts []string
	if current.RowSecurity != desired.RowSecurity {
		if desired.RowSecurity {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ENABLE ROW LEVEL SECURITY;")
		} else {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" DISABLE ROW LEVEL SECURITY;")
		}
	}
	if current.ForceRowSecurity != desired.ForceRowSecurity {
		if desired.ForceRowSecurity {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" FORCE ROW LEVEL SECURITY;")
		} else {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" NO FORCE ROW LEVEL SECURITY;")
		}
	}
	return stmts
}

// diffPolicies emits CREATE POLICY / ALTER POLICY / DROP POLICY statements for
// changes between current and desired policies on the same table.
//
// Pure removals (policy absent from desired) honor the policy-drop policy via
// dc; definition changes still run as DROP+CREATE when a property that cannot
// be modified in place (Command / Permissive) changes. USING / WITH CHECK /
// roles changes use ALTER POLICY for an in-place update.
func diffPolicies(
	fqtn string,
	current, desired *orderedmap.Map[string, *model.Policy],
	dc DropChecker,
) (stmts []string, disallowed []string, err error) {
	dc = NormalizeDropChecker(dc)

	// Callers (diffTable) always pass initialized maps from parser/catalog,
	// so no nil-guard is needed here.

	// Detect renames first so subsequent diff steps see the renamed policy
	// under its new name in the adjusted current map.
	renameStmts, current, renamedFrom, err := detectPolicyRenames(fqtn, current, desired)
	if err != nil {
		return nil, nil, err
	}

	// Renamed policies whose definition also requires DROP+CREATE: skip the
	// RENAME and let the recreate-with-new-name cover the change.
	needsRecreateRenamed := map[string]bool{}
	for newName, oldName := range renamedFrom {
		cur, ok := current.GetOk(newName)
		des, dok := desired.GetOk(newName)
		if !ok || !dok {
			continue
		}
		if needsRecreate(cur, des) {
			needsRecreateRenamed[oldName+"->"+newName] = true
		}
	}
	for newName, oldName := range renamedFrom {
		if needsRecreateRenamed[oldName+"->"+newName] {
			continue
		}
		// Find the matching rename statement (ordering preserved from desired).
		needle := "ALTER POLICY " + model.Ident(oldName) + " ON " + fqtn + " RENAME TO " + model.Ident(newName) + ";"
		for _, stmt := range renameStmts {
			if stmt == needle {
				stmts = append(stmts, stmt)
				break
			}
		}
	}

	policyAllowed := dc.IsDropAllowed("policy")

	// Drop removed or recreate-required policies first so a CREATE for the same
	// name later does not conflict. When a policy was both renamed and needs
	// recreation, the DROP must reference the old name because the RENAME was
	// suppressed above.
	for name, cur := range current.All() {
		des, ok := desired.GetOk(name)
		if !ok {
			drop := dropPolicySQL(fqtn, name)
			if !policyAllowed {
				disallowed = append(disallowed, "-- skipped: "+drop)
				continue
			}
			stmts = append(stmts, drop)
			continue
		}
		if needsRecreate(cur, des) {
			dropName := name
			if oldName, renamed := renamedFrom[name]; renamed {
				dropName = oldName
			}
			stmts = append(stmts, dropPolicySQL(fqtn, dropName))
		}
	}

	// Add new or recreated policies, then ALTER for in-place changes.
	for name, des := range desired.All() {
		cur, ok := current.GetOk(name)
		if !ok {
			stmts = append(stmts, des.SQL())
			continue
		}
		if needsRecreate(cur, des) {
			stmts = append(stmts, des.SQL())
			continue
		}
		if alterStmt := alterPolicySQL(fqtn, cur, des); alterStmt != "" {
			stmts = append(stmts, alterStmt)
		}
	}

	return stmts, disallowed, nil
}

// needsRecreate reports whether two policies differ in a way that cannot be
// expressed via ALTER POLICY. PostgreSQL's ALTER POLICY can only set clauses;
// it cannot remove a clause that was previously set, and Command / Permissive
// are immutable in place.
func needsRecreate(a, b *model.Policy) bool {
	return a.Command != b.Command ||
		a.Permissive != b.Permissive ||
		(a.Using != nil && b.Using == nil) ||
		(a.WithCheck != nil && b.WithCheck == nil)
}

func dropPolicySQL(fqtn, name string) string {
	return "DROP POLICY " + model.Ident(name) + " ON " + fqtn + ";"
}

// alterPolicySQL returns a single ALTER POLICY statement covering the
// in-place modifiable attributes (TO, USING, WITH CHECK). Returns "" if no
// in-place change is needed. The caller must have already routed clause
// removals (current set, desired nil) to DROP+CREATE via needsRecreate.
func alterPolicySQL(fqtn string, current, desired *model.Policy) string {
	rolesChanged := !equalRoles(current.Roles, desired.Roles)
	usingChanged := exprChanged(current.Using, desired.Using)
	withCheckChanged := exprChanged(current.WithCheck, desired.WithCheck)
	if !rolesChanged && !usingChanged && !withCheckChanged {
		return ""
	}

	var b strings.Builder
	b.WriteString("ALTER POLICY ")
	b.WriteString(model.Ident(desired.Name))
	b.WriteString(" ON ")
	b.WriteString(fqtn)
	if rolesChanged {
		// Treat empty desired roles as PUBLIC so ALTER POLICY ... TO is always
		// well-formed; PostgreSQL stores no-roles policies as PUBLIC anyway.
		roles := desired.Roles
		if len(roles) == 0 {
			roles = []string{"public"}
		}
		b.WriteString(" TO ")
		b.WriteString(strings.Join(formatRoles(roles), ", "))
	}
	if usingChanged && desired.Using != nil {
		b.WriteString(" USING (")
		b.WriteString(*desired.Using)
		b.WriteString(")")
	}
	if withCheckChanged && desired.WithCheck != nil {
		b.WriteString(" WITH CHECK (")
		b.WriteString(*desired.WithCheck)
		b.WriteString(")")
	}
	b.WriteString(";")
	return b.String()
}

// equalRoles compares two role lists, treating an empty list as equivalent to
// ["public"] (PostgreSQL stores both the same way).
func equalRoles(a, b []string) bool {
	na := normalizeRoles(a)
	nb := normalizeRoles(b)
	return slices.Equal(na, nb)
}

func normalizeRoles(r []string) []string {
	if len(r) == 0 {
		return []string{"public"}
	}
	out := slices.Clone(r)
	slices.Sort(out)
	return out
}

func formatRoles(roles []string) []string {
	out := make([]string, len(roles))
	for i, r := range roles {
		switch r {
		case "public", "current_user", "current_role", "session_user":
			out[i] = r
		default:
			out[i] = model.Ident(r)
		}
	}
	return out
}

// exprChanged compares two optional expression strings by parsing them as
// SELECT expressions so that pg_get_expr-added casts and formatting noise do
// not cause false diffs.
func exprChanged(a, b *string) bool {
	if a == nil && b == nil {
		return false
	}
	if a == nil || b == nil {
		return true
	}
	return !equalPolicyExpr(*a, *b)
}

// equalPolicyExpr returns true if two expressions are semantically equal,
// using the same normalization as constraint expressions (strips text-like
// casts, canonicalises = ANY(ARRAY[...]) → IN (...)).
//
// Comparison goes through parse → normalize → deparse → string compare so that
// pg_get_expr-added casts, parentheses, and AST `location` field differences
// don't cause false diffs.
func equalPolicyExpr(a, b string) bool {
	if a == b {
		return true
	}
	normA, errA := normalizePolicyExpr(a)
	normB, errB := normalizePolicyExpr(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return normA == normB
}

func normalizePolicyExpr(expr string) (string, error) {
	result, err := pg_query.Parse("SELECT " + expr)
	if err != nil {
		return "", err
	}
	if len(result.Stmts) == 0 {
		return "", fmt.Errorf("unexpected parse result for expression: %s", expr)
	}
	stmt := result.Stmts[0].Stmt.GetSelectStmt()
	if stmt == nil || len(stmt.TargetList) == 0 {
		return "", fmt.Errorf("unexpected parse result for expression: %s", expr)
	}
	target := stmt.TargetList[0].GetResTarget()
	if target == nil {
		return "", fmt.Errorf("unexpected parse result for expression: %s", expr)
	}
	target.Val = normalizeCheckExpr(target.Val)
	return pg_query.Deparse(result)
}
