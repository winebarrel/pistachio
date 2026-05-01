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

type Policy struct {
	Name       string
	RenameFrom *string
	Schema     string
	Table      string
	Permissive bool
	Command    PolicyCommand
	Roles      []string
	Using      *string
	WithCheck  *string
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
	parts := make([]string, len(p.Roles))
	for i, r := range p.Roles {
		if r == "public" || r == "current_user" || r == "current_role" || r == "session_user" {
			parts[i] = r
			continue
		}
		parts[i] = Ident(r)
	}
	return " TO " + strings.Join(parts, ", ")
}
