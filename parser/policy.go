package parser

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// applyAlterTableRLS scans an ALTER TABLE statement for row-level security
// toggle subcommands (ENABLE / DISABLE / FORCE / NO FORCE) and applies them
// to the table's flags. Non-RLS subcommands are ignored, so the caller can
// invoke parseAlterTableConstraint on the same statement to pick up any
// AT_AddConstraint subcommands — PostgreSQL allows mixing the two in one
// ALTER TABLE.
func applyAlterTableRLS(as *pg_query.AlterTableStmt, t *model.Table) {
	for _, cmdNode := range as.Cmds {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil {
			continue
		}
		switch cmd.Subtype {
		case pg_query.AlterTableType_AT_EnableRowSecurity:
			t.RowSecurity = true
		case pg_query.AlterTableType_AT_DisableRowSecurity:
			t.RowSecurity = false
		case pg_query.AlterTableType_AT_ForceRowSecurity:
			t.ForceRowSecurity = true
		case pg_query.AlterTableType_AT_NoForceRowSecurity:
			t.ForceRowSecurity = false
		}
	}
}

// parseCreatePolicyStmt converts a CreatePolicyStmt into a model.Policy and
// attaches it to the owning table. Returns the created policy so the caller
// can apply post-parse decorations (e.g. RenameFrom). The table must already
// exist in `tables`, otherwise an error is returned.
func parseCreatePolicyStmt(
	cps *pg_query.CreatePolicyStmt,
	defaultSchema string,
	tables *orderedmap.Map[string, *model.Table],
) (*model.Policy, error) {
	if cps.Table == nil {
		return nil, fmt.Errorf("CREATE POLICY: missing table reference")
	}
	schema := cps.Table.Schemaname
	if schema == "" {
		schema = defaultSchema
	}
	fqtn := model.Ident(schema, cps.Table.Relname)
	tbl, ok := tables.GetOk(fqtn)
	if !ok {
		return nil, fmt.Errorf("CREATE POLICY %s: parent table %s not defined", cps.PolicyName, fqtn)
	}

	cmd, err := parsePolicyCommand(cps.CmdName)
	if err != nil {
		return nil, fmt.Errorf("CREATE POLICY %s: %w", cps.PolicyName, err)
	}

	roles, err := parsePolicyRoles(cps.Roles)
	if err != nil {
		return nil, fmt.Errorf("CREATE POLICY %s: %w", cps.PolicyName, err)
	}

	policy := &model.Policy{
		Name:       cps.PolicyName,
		Schema:     schema,
		Table:      cps.Table.Relname,
		Permissive: cps.Permissive,
		Command:    cmd,
		Roles:      roles,
	}

	if cps.Qual != nil {
		using, err := deparseExpr(cps.Qual)
		if err != nil {
			return nil, fmt.Errorf("CREATE POLICY %s: failed to deparse USING: %w", cps.PolicyName, err)
		}
		policy.Using = &using
	}
	if cps.WithCheck != nil {
		wc, err := deparseExpr(cps.WithCheck)
		if err != nil {
			return nil, fmt.Errorf("CREATE POLICY %s: failed to deparse WITH CHECK: %w", cps.PolicyName, err)
		}
		policy.WithCheck = &wc
	}

	if err := setUnique(tbl.Policies, policy.Name, "policy", policy); err != nil {
		return nil, err
	}
	return policy, nil
}

// parsePolicyCommand maps the parser-emitted command name to the catalog
// representation in pg_policy.polcmd.
func parsePolicyCommand(name string) (model.PolicyCommand, error) {
	switch name {
	case "", "all":
		return '*', nil
	case "select":
		return 'r', nil
	case "insert":
		return 'a', nil
	case "update":
		return 'w', nil
	case "delete":
		return 'd', nil
	default:
		return 0, fmt.Errorf("unsupported policy command: %s", name)
	}
}

// parsePolicyRoles converts a list of RoleSpec nodes into role name strings.
// Reserved role specifiers (CURRENT_USER, CURRENT_ROLE, SESSION_USER, PUBLIC)
// are kept lower-cased so the diff layer can reliably compare them.
func parsePolicyRoles(nodes []*pg_query.Node) ([]string, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	roles := make([]string, 0, len(nodes))
	for _, n := range nodes {
		rs := n.GetRoleSpec()
		if rs == nil {
			return nil, fmt.Errorf("expected RoleSpec in role list")
		}
		switch rs.Roletype {
		case pg_query.RoleSpecType_ROLESPEC_PUBLIC:
			roles = append(roles, "public")
		case pg_query.RoleSpecType_ROLESPEC_CURRENT_USER:
			roles = append(roles, "current_user")
		case pg_query.RoleSpecType_ROLESPEC_CURRENT_ROLE:
			roles = append(roles, "current_role")
		case pg_query.RoleSpecType_ROLESPEC_SESSION_USER:
			roles = append(roles, "session_user")
		default:
			roles = append(roles, rs.Rolename)
		}
	}
	return roles, nil
}
