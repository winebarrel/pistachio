package model

import (
	"strings"

	"github.com/winebarrel/orderedmap"
)

type Enum struct {
	OID     uint32
	Schema  string
	Name    string
	Values  []string
	Comment *string
}

func (e Enum) FQEN() string {
	return Ident(e.Schema, e.Name)
}

func (e Enum) SQL() string {
	quoted := make([]string, len(e.Values))
	for i, v := range e.Values {
		quoted[i] = QuoteLiteral(v)
	}
	return "CREATE TYPE " + Ident(e.Schema, e.Name) + " AS ENUM (\n    " +
		strings.Join(quoted, ",\n    ") + "\n);"
}

func (e Enum) CommentSQL() string {
	if e.Comment != nil {
		return "COMMENT ON TYPE " + Ident(e.Schema, e.Name) + " IS " + QuoteLiteral(*e.Comment) + ";"
	}
	return ""
}

func EnumToSQL(e *Enum) string {
	parts := []string{"-- " + e.FQEN(), e.SQL()}
	if s := e.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	return strings.Join(parts, "\n")
}

func EnumsToSQL(enums *orderedmap.Map[string, *Enum]) string {
	return strings.Join(
		orderedmap.TransformSlice(enums, func(_ string, e *Enum) string {
			return EnumToSQL(e)
		}),
		"\n\n",
	)
}
