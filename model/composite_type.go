package model

import (
	"strings"

	"github.com/winebarrel/orderedmap"
)

// CompositeAttribute is one field of a composite type (CREATE TYPE ... AS (...)).
type CompositeAttribute struct {
	Name     string
	TypeName string
	// Collation in quoted SQL form, ready to follow COLLATE
	// (e.g. `pg_catalog."C"`). nil for the default collation.
	Collation *string
	// RenameFrom maps the attribute to the current attribute it renames. Set by
	// the parser from an inline -- pista:renamed-from directive; always nil on
	// the catalog side.
	RenameFrom *string
	Comment    *string
}

type CompositeType struct {
	OID        uint32
	Schema     string
	Name       string
	RenameFrom *string
	Attributes []*CompositeAttribute
	Comment    *string
	// Ignore marks the composite type as unmanaged (set by -- pista:ignore).
	// Ignored objects are not created, altered, or dropped; always false on the
	// catalog side.
	Ignore bool
}

func (ct CompositeType) FQCN() string {
	return Ident(ct.Schema, ct.Name)
}

// TypeSQL renders the attribute's type with its COLLATE clause, e.g.
// "text" or "text COLLATE \"C\"".
func (a CompositeAttribute) TypeSQL() string {
	return a.TypeName + collateClause(a.Collation)
}

// collateClause renders a COLLATE clause. The stored name is already quoted.
func collateClause(collation *string) string {
	if collation == nil {
		return ""
	}
	return " COLLATE " + *collation
}

func (ct CompositeType) SQL() string {
	lines := make([]string, len(ct.Attributes))
	for i, a := range ct.Attributes {
		lines[i] = "    " + Ident(a.Name) + " " + a.TypeName + collateClause(a.Collation)
	}
	return "CREATE TYPE " + Ident(ct.Schema, ct.Name) + " AS (\n" +
		strings.Join(lines, ",\n") + "\n);"
}

func (ct CompositeType) CommentSQL() string {
	if ct.Comment != nil {
		return "COMMENT ON TYPE " + Ident(ct.Schema, ct.Name) + " IS " + QuoteLiteral(*ct.Comment) + ";"
	}
	return ""
}

// AttributeCommentSQLs returns a COMMENT ON COLUMN statement for each attribute
// that carries a comment.
func (ct CompositeType) AttributeCommentSQLs() []string {
	var stmts []string
	for _, a := range ct.Attributes {
		if a.Comment != nil {
			stmts = append(stmts, "COMMENT ON COLUMN "+Ident(ct.Schema, ct.Name, a.Name)+" IS "+QuoteLiteral(*a.Comment)+";")
		}
	}
	return stmts
}

func CompositeTypeToSQL(ct *CompositeType) string {
	parts := []string{"-- " + ct.FQCN(), ct.SQL()}
	if s := ct.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	parts = append(parts, ct.AttributeCommentSQLs()...)
	return strings.Join(parts, "\n")
}

func CompositeTypesToSQL(compositeTypes *orderedmap.Map[string, *CompositeType]) string {
	return strings.Join(
		orderedmap.TransformSlice(compositeTypes, func(_ string, ct *CompositeType) string {
			return CompositeTypeToSQL(ct)
		}),
		"\n\n",
	)
}
