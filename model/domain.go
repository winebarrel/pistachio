package model

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

type DomainConstraint struct {
	Name       string  `json:"name"`
	Definition string  `json:"definition"`
	Validated  bool    `json:"validated"`
	Comment    *string `json:"comment"`
}

// CommentSQL renders the constraint's COMMENT ON, or an empty string when it
// carries none. fqdn is the domain the constraint is on.
func (c DomainConstraint) CommentSQL(fqdn string) string {
	if c.Comment == nil {
		return ""
	}
	return "COMMENT ON CONSTRAINT " + Ident(c.Name) + " ON DOMAIN " + fqdn + " IS " + QuoteLiteral(*c.Comment) + ";"
}

type Domain struct {
	OID        uint32  `json:"oid"`
	Schema     string  `json:"schema"`
	Name       string  `json:"name"`
	RenameFrom *string `json:"rename_from"`
	BaseType   string  `json:"base_type"`
	NotNull    bool    `json:"not_null"`
	Default    *string `json:"default"`
	// Collation in quoted SQL form, ready to follow COLLATE
	// (e.g. `pg_catalog."C"`). nil for the default collation.
	Collation   *string             `json:"collation"`
	Constraints []*DomainConstraint `json:"constraints"`
	Comment     *string             `json:"comment"`
	// Ignore marks the domain as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the
	// catalog side.
	Ignore bool `json:"ignore"`
}

func (d Domain) FQDN() string {
	return Ident(d.Schema, d.Name)
}

func (d Domain) SQL() string {
	var sql strings.Builder
	sql.WriteString("CREATE DOMAIN " + d.FQDN() + " AS " + d.BaseType)

	sql.WriteString(collateClause(d.Collation))

	if d.Default != nil {
		sql.WriteString(" DEFAULT " + *d.Default)
	}

	if d.NotNull {
		sql.WriteString(" NOT NULL")
	}

	// CREATE DOMAIN cannot write NOT VALID, so NotValidConSQL writes those
	// constraints instead.
	for _, c := range d.Constraints {
		if !c.Validated {
			continue
		}
		sql.WriteString("\n    CONSTRAINT " + Ident(c.Name) + " " + c.Definition)
	}

	return sql.String() + ";"
}

// NotValidConSQL writes each NOT VALID constraint as an ALTER DOMAIN.
func (d Domain) NotValidConSQL() []string {
	var stmts []string
	for _, c := range d.Constraints {
		if c.Validated {
			continue
		}
		stmts = append(stmts, "ALTER DOMAIN "+d.FQDN()+" ADD CONSTRAINT "+Ident(c.Name)+" "+c.Definition+" NOT VALID;")
	}
	return stmts
}

func (d Domain) CommentSQL() string {
	if d.Comment != nil {
		return "COMMENT ON DOMAIN " + d.FQDN() + " IS " + QuoteLiteral(*d.Comment) + ";"
	}
	return ""
}

// ConstraintCommentSQL renders the comments on the domain's constraints.
func (d Domain) ConstraintCommentSQL() []string {
	var stmts []string
	for _, c := range d.Constraints {
		if s := c.CommentSQL(d.FQDN()); s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}

func DomainToSQL(d *Domain) string {
	parts := []string{"-- " + d.FQDN(), d.SQL()}
	parts = append(parts, d.NotValidConSQL()...)
	if s := d.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	parts = append(parts, d.ConstraintCommentSQL()...)
	return strings.Join(parts, "\n")
}

func DomainsToSQL(domains *orderedmap.Map[string, *Domain]) string {
	return joinSQL(domains, DomainToSQL)
}
