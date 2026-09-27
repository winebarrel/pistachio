package model

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

type DomainConstraint struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
	Validated  bool   `json:"validated"`
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

	// A NOT VALID constraint is left out, since CREATE DOMAIN cannot spell the
	// clause and writing it inline would create it validated; NotValidConSQL
	// adds it back.
	for _, c := range d.Constraints {
		if !c.Validated {
			continue
		}
		sql.WriteString("\n    CONSTRAINT " + Ident(c.Name) + " " + c.Definition)
	}

	return sql.String() + ";"
}

// NotValidConSQL renders the domain's NOT VALID constraints, each as its own
// ALTER DOMAIN after the CREATE DOMAIN.
func (d Domain) NotValidConSQL() []string {
	var stmts []string
	for _, c := range d.Constraints {
		if c.Validated {
			continue
		}
		stmts = append(stmts, "ALTER DOMAIN "+Ident(d.Schema, d.Name)+" ADD CONSTRAINT "+Ident(c.Name)+" "+c.Definition+" NOT VALID;")
	}
	return stmts
}

func (d Domain) CommentSQL() string {
	if d.Comment != nil {
		return "COMMENT ON DOMAIN " + Ident(d.Schema, d.Name) + " IS " + QuoteLiteral(*d.Comment) + ";"
	}
	return ""
}

func DomainToSQL(d *Domain) string {
	parts := []string{"-- " + d.FQDN(), d.SQL()}
	parts = append(parts, d.NotValidConSQL()...)
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
