package model

import (
	"fmt"
	"strings"
)

// PolicyCommand mirrors pg_policy.polcmd:
//
//	'*' ALL, 'r' SELECT, 'a' INSERT, 'w' UPDATE, 'd' DELETE
type PolicyCommand byte

func (c PolicyCommand) String() string {
	switch c {
	case '*':
		return "ALL"
	case 'r':
		return "SELECT"
	case 'a':
		return "INSERT"
	case 'w':
		return "UPDATE"
	case 'd':
		return "DELETE"
	default:
		return ""
	}
}

func (c PolicyCommand) IsAll() bool { return c == '*' }

// MarshalJSON writes the command keyword instead of the pg_policy character,
// since the JSON is read outside pistachio.
func (c PolicyCommand) MarshalJSON() ([]byte, error) {
	return []byte(`"` + c.String() + `"`), nil
}

type Policy struct {
	Name       string        `json:"name"`
	RenameFrom *string       `json:"rename_from"`
	Schema     string        `json:"schema"`
	Table      string        `json:"table"`
	Permissive bool          `json:"permissive"`
	Command    PolicyCommand `json:"command"`
	Roles      []string      `json:"roles"`
	Using      *string       `json:"using"`
	WithCheck  *string       `json:"with_check"`
}

func (p *Policy) String() string {
	return fmt.Sprintf("%#v", *p)
}

// SQL renders a CREATE POLICY statement.
func (p Policy) SQL() string {
	var b strings.Builder
	b.WriteString("CREATE POLICY ")
	b.WriteString(Ident(p.Name))
	b.WriteString(" ON ")
	b.WriteString(Ident(p.Schema, p.Table))
	if !p.Permissive {
		b.WriteString(" AS RESTRICTIVE")
	}
	if !p.Command.IsAll() {
		b.WriteString(" FOR ")
		b.WriteString(p.Command.String())
	}
	b.WriteString(p.rolesClause())
	if p.Using != nil {
		b.WriteString(" USING (")
		b.WriteString(*p.Using)
		b.WriteString(")")
	}
	if p.WithCheck != nil {
		b.WriteString(" WITH CHECK (")
		b.WriteString(*p.WithCheck)
		b.WriteString(")")
	}
	b.WriteString(";")
	return b.String()
}

func (p Policy) rolesClause() string {
	if len(p.Roles) == 0 {
		return ""
	}
	if len(p.Roles) == 1 && p.Roles[0] == "public" {
		return ""
	}
	return " TO " + RolesSQL(p.Roles)
}

// RolesSQL renders a policy's role list. PUBLIC and the CURRENT_USER family
// are keywords, so they are written bare; any other name is an identifier.
func RolesSQL(roles []string) string {
	parts := make([]string, len(roles))
	for i, r := range roles {
		switch r {
		case "public", "current_user", "current_role", "session_user":
			parts[i] = r
		default:
			parts[i] = Ident(r)
		}
	}
	return strings.Join(parts, ", ")
}
