package model

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

type View struct {
	OID          uint32  `json:"oid"`
	Schema       string  `json:"schema"`
	Name         string  `json:"name"`
	RenameFrom   *string `json:"rename_from"`
	Definition   string  `json:"definition"`
	Materialized bool    `json:"materialized"`
	// CheckOption is the view's WITH CHECK OPTION, "local" or "cascaded", and
	// empty when the view has none. A materialized view never has one.
	CheckOption string `json:"check_option"`
	// StorageParams holds the view's storage parameters, pg_class.reloptions,
	// keyed by parameter name and ordered by it. check_option sits in the same
	// column but is read as the view's WITH CHECK OPTION, so it is not here. A
	// plain view takes security_barrier and security_invoker, a materialized
	// view what a table takes, including a `toast.` parameter of its TOAST
	// relation.
	StorageParams *orderedmap.Map[string, string]   `json:"storage_params"`
	Indexes       *orderedmap.Map[string, *Index]   `json:"indexes"`
	Triggers      *orderedmap.Map[string, *Trigger] `json:"triggers"`
	Comment       *string                           `json:"comment"`
	// Ignore marks the view as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the
	// catalog side.
	Ignore bool `json:"ignore"`
}

func (v View) FQVN() string {
	return Ident(v.Schema, v.Name)
}

// ObjType returns the keyword a statement names the view with, VIEW or
// MATERIALIZED VIEW.
func (v View) ObjType() string {
	if v.Materialized {
		return "MATERIALIZED VIEW"
	}
	return "VIEW"
}

func (v View) SQL() string {
	def := strings.TrimSpace(v.Definition)
	def = strings.TrimSuffix(def, ";")
	if v.Materialized {
		return "CREATE MATERIALIZED VIEW " + Ident(v.Schema, v.Name) + v.storageParamsClause() + " AS\n" + def + ";"
	}
	return "CREATE OR REPLACE VIEW " + Ident(v.Schema, v.Name) + v.storageParamsClause() + " AS\n" + def + v.checkOptionClause() + ";"
}

// storageParamsClause renders the WITH clause that precedes AS, or nothing for
// a view without parameters. Every value is quoted, as the table renderer does.
// A replaced view carries the clause too: CREATE OR REPLACE VIEW replaces the
// options as a whole, so a parameter left off it is reset.
func (v View) storageParamsClause() string {
	if v.StorageParams == nil || v.StorageParams.Len() == 0 {
		return ""
	}
	return " WITH (" + strings.Join(
		v.StorageParams.TransformSlice(func(name, value string) string {
			return name + "=" + QuoteLiteral(value)
		}),
		", ",
	) + ")"
}

// SetViewStorageParamsSQL returns the statement that sets the named storage
// parameters on a view, objType being what View.ObjType reports. It leaves the
// definition alone.
func SetViewStorageParamsSQL(fqvn, objType string, params []string) string {
	return "ALTER " + objType + " " + fqvn + " SET (" + strings.Join(params, ", ") + ");"
}

// ResetViewStorageParamsSQL returns the statement that hands the named storage
// parameters back to their defaults. RESET is the only way to say that: the
// catalog holds no entry for a parameter that was never set, so a SET has no
// default value to name.
func ResetViewStorageParamsSQL(fqvn, objType string, names []string) string {
	return "ALTER " + objType + " " + fqvn + " RESET (" + strings.Join(names, ", ") + ");"
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
	var stmts []string
	if v.Comment != nil {
		stmts = append(stmts, "COMMENT ON "+v.ObjType()+" "+Ident(v.Schema, v.Name)+" IS "+QuoteLiteral(*v.Comment)+";")
	}
	if v.Indexes != nil {
		for _, idx := range v.Indexes.CollectValues() {
			if s := idx.CommentSQL(); s != "" {
				stmts = append(stmts, s)
			}
		}
	}
	return strings.Join(stmts, "\n")
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
