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
	// CheckOption is the view's WITH CHECK OPTION, "local" or "cascaded", and
	// empty when the view has none. A materialized view never has one.
	CheckOption string
	Indexes     *orderedmap.Map[string, *Index]
	Triggers    *orderedmap.Map[string, *Trigger]
	Comment     *string
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
	return "CREATE OR REPLACE VIEW " + Ident(v.Schema, v.Name) + " AS\n" + def + v.checkOptionClause() + ";"
}

// checkOptionClause renders the WITH CHECK OPTION that follows the query,
// indented the way pg_dump writes it, or nothing for a view without one.
func (v View) checkOptionClause() string {
	if v.CheckOption == "" {
		return ""
	}
	return "\n  WITH " + strings.ToUpper(v.CheckOption) + " CHECK OPTION"
}

// SetCheckOptionSQL returns the statement that puts the view's check option
// at option, or resets it when option is empty. Both forms leave the
// definition alone.
func SetCheckOptionSQL(fqvn, option string) string {
	if option == "" {
		return "ALTER VIEW " + fqvn + " RESET (check_option);"
	}
	return "ALTER VIEW " + fqvn + " SET (check_option=" + QuoteLiteral(option) + ");"
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
