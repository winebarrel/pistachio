package model

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

type DomainConstraint struct {
	Name       string
	Definition string
	Validated  bool
}

type Domain struct {
	OID        uint32
	Schema     string
	Name       string
	RenameFrom *string
	BaseType   string
	NotNull    bool
	Default    *string
	// Collation in quoted SQL form, ready to follow COLLATE
	// (e.g. `pg_catalog."C"`). nil for the default collation.
	Collation   *string
	Constraints []*DomainConstraint
	Comment     *string
	// Ignore marks the domain as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the
	// catalog side.
	Ignore bool
}

func (d Domain) FQDN() string {
	return Ident(d.Schema, d.Name)
}

func (d Domain) SQL() string {
	sql := "CREATE DOMAIN " + Ident(d.Schema, d.Name) + " AS " + d.BaseType

	if d.Collation != nil {
		sql += " COLLATE " + *d.Collation
	}

	if d.Default != nil {
		sql += " DEFAULT " + *d.Default
	}

	if d.NotNull {
		sql += " NOT NULL"
	}

	for _, c := range d.Constraints {
		sql += "\n    CONSTRAINT " + Ident(c.Name) + " " + c.Definition
	}

	return sql + ";"
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
		orderedmap.TransformSlice(domains, func(_ string, d *Domain) string {
			return DomainToSQL(d)
		}),
		"\n\n",
	)
}
