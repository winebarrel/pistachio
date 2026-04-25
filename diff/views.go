package diff

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// normalizeViewDef normalizes a view definition by parsing and deparsing it
// through pg_query, so that formatting differences are eliminated.
func normalizeViewDef(def string) (string, error) {
	sql := "CREATE VIEW _v AS " + def
	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", err
	}
	return pg_query.Deparse(result)
}

// equalViewDef compares two view definitions by normalizing them through
// pg_query parse/deparse to ignore formatting differences.
// proto.Equal cannot be used here because parse trees include source
// location information that differs when the same query has different formatting.
// Schema qualification differences (e.g., "users" vs "public.users") are also
// ignored because pg_get_viewdef may omit the schema for the current search_path.
func equalViewDef(a, b string) bool {
	if a == b {
		return true
	}
	normA, errA := normalizeViewDef(a)
	normB, errB := normalizeViewDef(b)
	if errA != nil || errB != nil {
		return a == b
	}
	if normA == normB {
		return true
	}
	// Also compare with schema/column qualifications stripped, since
	// pg_get_viewdef adds table-qualified columns and omits the default schema
	// but parsed SQL preserves the schema and doesn't qualify columns.
	return normalizeForComparison(normA) == normalizeForComparison(normB)
}

// normalizeForComparison applies aggressive normalization to a view definition
// for comparison purposes. pg_get_viewdef adds table-qualified column names
// (e.g., "users.id") and omits schema prefixes, while the parser's deparse
// preserves schema prefixes and does not qualify columns.
func normalizeForComparison(sql string) string {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return sql
	}
	// Walk AST and strip schema from RangeVars and table prefix from ColumnRefs
	for _, stmt := range result.Stmts {
		stripQualifications(stmt.Stmt)
	}
	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return sql
	}
	return deparsed
}

// stripQualifications recursively strips schema from RangeVars and
// table qualifications from ColumnRefs in the AST.
func stripQualifications(node *pg_query.Node) {
	if node == nil {
		return
	}

	if vs := node.GetViewStmt(); vs != nil {
		stripQualifications(vs.Query)
		return
	}

	if rv := node.GetRangeVar(); rv != nil {
		rv.Schemaname = ""
		return
	}

	if cr := node.GetColumnRef(); cr != nil {
		// "table.column" → "column" (remove table prefix)
		// Only strip when both parts are plain identifiers (not table.*)
		if len(cr.Fields) == 2 && cr.Fields[1].GetString_() != nil {
			cr.Fields = cr.Fields[1:]
		}
		return
	}

	if ss := node.GetSelectStmt(); ss != nil {
		if ss.WithClause != nil {
			for _, cte := range ss.WithClause.Ctes {
				if c := cte.GetCommonTableExpr(); c != nil {
					stripQualifications(c.Ctequery)
				}
			}
		}
		for _, from := range ss.FromClause {
			stripQualifications(from)
		}
		for _, target := range ss.TargetList {
			stripQualifications(target)
		}
		if ss.WhereClause != nil {
			stripQualifications(ss.WhereClause)
		}
		if ss.HavingClause != nil {
			stripQualifications(ss.HavingClause)
		}
		for _, gb := range ss.GroupClause {
			stripQualifications(gb)
		}
		for _, ob := range ss.SortClause {
			stripQualifications(ob)
		}
		if ss.LimitCount != nil {
			stripQualifications(ss.LimitCount)
		}
		if ss.LimitOffset != nil {
			stripQualifications(ss.LimitOffset)
		}
		if ss.Larg != nil {
			stripQualifications(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Larg}})
		}
		if ss.Rarg != nil {
			stripQualifications(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Rarg}})
		}
		return
	}

	if rt := node.GetResTarget(); rt != nil {
		stripQualifications(rt.Val)
		return
	}

	if join := node.GetJoinExpr(); join != nil {
		stripQualifications(join.Larg)
		stripQualifications(join.Rarg)
		if join.Quals != nil {
			stripQualifications(join.Quals)
		}
		return
	}

	if sub := node.GetRangeSubselect(); sub != nil {
		stripQualifications(sub.Subquery)
		return
	}

	if sl := node.GetSubLink(); sl != nil {
		stripQualifications(sl.Subselect)
		return
	}

	if expr := node.GetAExpr(); expr != nil {
		stripQualifications(expr.Lexpr)
		stripQualifications(expr.Rexpr)
		return
	}

	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		for _, arg := range boolExpr.Args {
			stripQualifications(arg)
		}
		return
	}

	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.Args {
			stripQualifications(arg)
		}
		return
	}

	if sb := node.GetSortBy(); sb != nil {
		stripQualifications(sb.Node)
		return
	}
}

// ViewDiffResult separates view DROP and CREATE/MODIFY statements.
// Drops should run before table changes, creates after.
type ViewDiffResult struct {
	DropStmts   []string // DROP VIEW / DROP MATERIALIZED VIEW (should run before table changes)
	CreateStmts []string // ALTER VIEW RENAME, CREATE OR REPLACE VIEW, CREATE MATERIALIZED VIEW, indexes, comments (should run after table changes)
}

func DiffViews(current, desired *orderedmap.Map[string, *model.View], dc DropChecker, indexConcurrently bool) (*ViewDiffResult, error) {
	dc = NormalizeDropChecker(dc)
	result := &ViewDiffResult{}

	// Detect renames
	renameStmts, current, err := detectViewRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.CreateStmts = append(result.CreateStmts, renameStmts...)

	// Track views that are recreated (DROP+CREATE) so comments can be re-applied
	recreated := make(map[string]bool)

	// New or modified views (CREATE OR REPLACE / recreate for materialized)
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)
		if !ok {
			// New view
			result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
			// Add indexes for new materialized views
			if desiredView.Materialized && desiredView.Indexes != nil {
				for _, idx := range desiredView.Indexes.CollectValues() {
					stmt, err := createIndexSQL(idx.Definition, indexConcurrently || idx.Concurrently)
					if err != nil {
						return nil, err
					}
					result.CreateStmts = append(result.CreateStmts, stmt)
				}
			}
		} else if !equalViewDef(currentView.Definition, desiredView.Definition) || currentView.Materialized != desiredView.Materialized {
			needsDropCreate := desiredView.Materialized || currentView.Materialized != desiredView.Materialized
			if needsDropCreate {
				// Materialized views or type changes (VIEW ↔ MATERIALIZED VIEW)
				// require DROP and recreate. Only proceed if drops are allowed.
				if dc.IsDropAllowed("view") {
					if currentView.Materialized {
						result.DropStmts = append(result.DropStmts, "DROP MATERIALIZED VIEW "+k+";")
					} else {
						result.DropStmts = append(result.DropStmts, "DROP VIEW "+k+";")
					}
					result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
					if desiredView.Materialized && desiredView.Indexes != nil {
						for _, idx := range desiredView.Indexes.CollectValues() {
							stmt, err := createIndexSQL(idx.Definition, indexConcurrently || idx.Concurrently)
							if err != nil {
								return nil, err
							}
							result.CreateStmts = append(result.CreateStmts, stmt)
						}
					}
					recreated[k] = true
				}
			} else {
				// Regular view: CREATE OR REPLACE
				result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
			}
		} else if desiredView.Materialized {
			// Definition unchanged, diff indexes
			viewIdxStmts, err := diffViewIndexes(currentView, desiredView, indexConcurrently)
			if err != nil {
				return nil, err
			}
			result.CreateStmts = append(result.CreateStmts, viewIdxStmts...)
		}
	}

	// Dropped views
	if dc.IsDropAllowed("view") {
		for k, v := range current.All() {
			if _, ok := desired.GetOk(k); !ok {
				if v.Materialized {
					result.DropStmts = append(result.DropStmts, "DROP MATERIALIZED VIEW "+k+";")
				} else {
					result.DropStmts = append(result.DropStmts, "DROP VIEW "+k+";")
				}
			}
		}
	}

	// Comment changes
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)

		// If the type changed (VIEW ↔ MATERIALIZED VIEW) but drop was denied,
		// the object type hasn't changed yet — skip comment diff.
		if ok && currentView.Materialized != desiredView.Materialized && !dc.IsDropAllowed("view") {
			continue
		}

		var currentComment *string
		if ok && !recreated[k] {
			// Preserve current comment for diffing only if the view was not
			// recreated. Recreated views lose their comment in PostgreSQL,
			// so currentComment stays nil to ensure re-application.
			currentComment = currentView.Comment
		}
		if !equalPtr(currentComment, desiredView.Comment) {
			objType := "VIEW"
			if desiredView.Materialized {
				objType = "MATERIALIZED VIEW"
			}
			if desiredView.Comment != nil {
				result.CreateStmts = append(result.CreateStmts, "COMMENT ON "+objType+" "+k+" IS "+model.QuoteLiteral(*desiredView.Comment)+";")
			} else {
				result.CreateStmts = append(result.CreateStmts, "COMMENT ON "+objType+" "+k+" IS NULL;")
			}
		}
	}

	return result, nil
}

// diffViewIndexes generates DDL for index changes on materialized views.
func diffViewIndexes(current, desired *model.View, concurrently bool) ([]string, error) {
	var stmts []string

	currentIndexes := orderedmap.New[string, *model.Index]()
	if current.Indexes != nil {
		currentIndexes = current.Indexes
	}
	desiredIndexes := orderedmap.New[string, *model.Index]()
	if desired.Indexes != nil {
		desiredIndexes = desired.Indexes
	}

	// Drop removed or changed indexes
	for name, currentIdx := range currentIndexes.All() {
		desiredIdx, ok := desiredIndexes.GetOk(name)
		if !ok || !equalIndexDef(currentIdx.Definition, desiredIdx.Definition) {
			useConcurrently := concurrently
			if !useConcurrently {
				if ok {
					useConcurrently = desiredIdx.Concurrently
				} else {
					useConcurrently = currentIdx.Concurrently
				}
			}
			stmt, err := dropIndexSQL(currentIdx.Schema, name, useConcurrently)
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, stmt)
		}
	}

	// Add new or changed indexes
	for name, desiredIdx := range desiredIndexes.All() {
		currentIdx, ok := currentIndexes.GetOk(name)
		if !ok || !equalIndexDef(currentIdx.Definition, desiredIdx.Definition) {
			stmt, err := createIndexSQL(desiredIdx.Definition, concurrently || desiredIdx.Concurrently)
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, stmt)
		}
	}

	return stmts, nil
}
