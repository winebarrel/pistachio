package model

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

type View struct {
	OID          uint32
	Schema       string
	Name         string
	RenameFrom   *string
	Definition   string
	Materialized bool
	Indexes      *orderedmap.Map[string, *Index]
	Triggers     *orderedmap.Map[string, *Trigger]
	Comment      *string
	// Ignore marks the view as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the
	// catalog side.
	Ignore bool
}

func (v View) FQVN() string {
	return Ident(v.Schema, v.Name)
}

func (v View) SQL() string {
	def := strings.TrimSpace(v.Definition)
	def = strings.TrimSuffix(def, ";")
	if v.Materialized {
		return "CREATE MATERIALIZED VIEW " + Ident(v.Schema, v.Name) + " AS\n" + def + ";"
	}
	return "CREATE OR REPLACE VIEW " + Ident(v.Schema, v.Name) + " AS\n" + def + ";"
}

// TrigSQL renders the view's INSTEAD OF triggers. PostgreSQL rejects
// ALTER TABLE ... DISABLE TRIGGER on a view, so there is no state to write.
func (v View) TrigSQL() string {
	var stmts []string
	if v.Triggers != nil {
		for _, trg := range v.Triggers.CollectValues() {
			stmts = append(stmts, trg.SQL())
		}
	}
	return strings.Join(stmts, "\n")
}

func (v View) CommentSQL() string {
	objType := "VIEW"
	if v.Materialized {
		objType = "MATERIALIZED VIEW"
	}
	if v.Comment != nil {
		return "COMMENT ON " + objType + " " + Ident(v.Schema, v.Name) + " IS " + QuoteLiteral(*v.Comment) + ";"
	}
	return ""
}

func ViewToSQL(v *View) string {
	parts := []string{"-- " + v.FQVN(), v.SQL()}
	if v.Indexes != nil {
		for _, idx := range v.Indexes.CollectValues() {
			parts = append(parts, idx.SQL())
		}
	}
	if s := v.TrigSQL(); s != "" {
		parts = append(parts, "\n"+s)
	}
	if s := v.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	return strings.Join(parts, "\n")
}

func ViewsToSQL(views *orderedmap.Map[string, *View]) string {
	return strings.Join(
		views.TransformSlice(func(_ string, v *View) string {
			return ViewToSQL(v)
		}),
		"\n\n",
	)
}
