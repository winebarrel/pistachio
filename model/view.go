package model

import (
	"strings"

	"github.com/winebarrel/orderedmap"
)

type View struct {
	OID        uint32
	Schema     string
	Name       string
	RenameFrom *string
	Definition string
	Comment    *string
}

func (v View) FQVN() string {
	return Ident(v.Schema, v.Name)
}

func (v View) SQL() string {
	def := strings.TrimSpace(v.Definition)
	def = strings.TrimSuffix(def, ";")
	return "CREATE OR REPLACE VIEW " + Ident(v.Schema, v.Name) + " AS\n" + def + ";"
}

func (v View) CommentSQL() string {
	if v.Comment != nil {
		return "COMMENT ON VIEW " + Ident(v.Schema, v.Name) + " IS " + QuoteLiteral(*v.Comment) + ";"
	}
	return ""
}

func ViewToSQL(v *View) string {
	parts := []string{"-- " + v.FQVN(), v.SQL()}
	if s := v.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	return strings.Join(parts, "\n")
}

func ViewsToSQL(views *orderedmap.Map[string, *View]) string {
	return strings.Join(
		orderedmap.TransformSlice(views, func(_ string, v *View) string {
			return ViewToSQL(v)
		}),
		"\n\n",
	)
}
