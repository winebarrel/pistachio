package model

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

type DomainConstraint struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
	// Validated is written even when true, like Constraint.Validated.
	Validated bool `json:"validated"`
}

type Domain struct {
	OID        uint32  `json:"-"`
	Schema     string  `json:"schema"`
	Name       string  `json:"name"`
	RenameFrom *string `json:"rename_from,omitempty"`
	BaseType   string  `json:"base_type"`
	NotNull    bool    `json:"not_null,omitzero"`
	Default    *string `json:"default,omitempty"`
	// Collation in quoted SQL form, ready to follow COLLATE
	// (e.g. `pg_catalog."C"`). nil for the default collation.
	Collation   *string             `json:"collation,omitempty"`
	Constraints []*DomainConstraint `json:"constraints,omitempty"`
	Comment     *string             `json:"comment,omitempty"`
	// Ignore marks the domain as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the
	// catalog side.
	Ignore bool `json:"ignore,omitzero"`
}

func (d Domain) FQDN() string {
	return Ident(d.Schema, d.Name)
}

func (d Domain) SQL() string {
	var sql strings.Builder
	sql.WriteString("CREATE DOMAIN " + Ident(d.Schema, d.Name) + " AS " + d.BaseType)

	if d.Collation != nil {
		sql.WriteString(" COLLATE " + *d.Collation)
	}

	if d.Default != nil {
		sql.WriteString(" DEFAULT " + *d.Default)
	}

	if d.NotNull {
		sql.WriteString(" NOT NULL")
	}

	for _, c := range d.Constraints {
		sql.WriteString("\n    CONSTRAINT " + Ident(c.Name) + " " + c.Definition)
	}

	return sql.String() + ";"
}

func (d Domain) CommentSQL() string {
	if d.Comment != nil {
		return "COMMENT ON DOMAIN " + Ident(d.Schema, d.Name) + " IS " + QuoteLiteral(*d.Comment) + ";"
	}
	return ""
}

func DomainToSQL(d *Domain) string {
	parts := []string{"-- " + d.FQDN(), d.SQL()}
	if s := d.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	return strings.Join(parts, "\n")
}

func DomainsToSQL(domains *orderedmap.Map[string, *Domain]) string {
	return strings.Join(
		domains.TransformSlice(func(_ string, d *Domain) string {
			return DomainToSQL(d)
		}),
		"\n\n",
	)
}
