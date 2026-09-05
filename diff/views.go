package diff

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// equalViewDef compares two view definitions by parsing each side once,
// applying every available view normalization in place, then deparsing and
// comparing the resulting strings. proto.Equal cannot be used here because
// parse trees include source location information that differs when the
// same query has different formatting.
//
// Normalizations applied:
//   - schema/column qualification stripping (pg_get_viewdef adds
//     table-qualified columns and omits the default schema, parsed SQL is
//     the opposite),
//   - symmetric expression normalization via normalizeCheckExpr:
//     paren / text-like cast stripping and `= ANY(ARRAY[...])` -> `IN (...)`,
//   - asymmetric current-only cast alignment via alignCurrentCasts: strips
//     TypeCasts (notably 'lit'::enum_type) the catalog added but the user
//     didn't write. Top-level casts at the target list are preserved on
//     both sides since they affect the resulting view's column type.
//
// All three walk the whole statement, so a nested SELECT, a join qualifier,
// a DISTINCT ON element, a named WINDOW or a sub-query gets the same
// treatment as the target list without any of them being enumerated here.
//
// The caller convention is equalViewDef(current, desired): current is the
// pg_get_viewdef form (from the catalog), desired is the user SQL. The
// asymmetric normalizations rely on that ordering.
func equalViewDef(current, desired string) bool {
	if current == desired {
		return true
	}
	curResult, errCur := pg_query.Parse("CREATE VIEW _v AS " + current)
	desResult, errDes := pg_query.Parse("CREATE VIEW _v AS " + desired)
	if errCur != nil || errDes != nil {
		return current == desired
	}
	for _, stmt := range curResult.Stmts {
		stripQualifications(stmt.Stmt)
		stmt.Stmt = normalizeCheckExpr(stmt.Stmt)
	}
	for _, stmt := range desResult.Stmts {
		stripQualifications(stmt.Stmt)
		stmt.Stmt = normalizeCheckExpr(stmt.Stmt)
	}
	if len(curResult.Stmts) == 1 && len(desResult.Stmts) == 1 {
		curResult.Stmts[0].Stmt = alignCurrentCasts(desResult.Stmts[0].Stmt, curResult.Stmts[0].Stmt)
	}
	curStr, errCur := pg_query.Deparse(curResult)
	desStr, errDes := pg_query.Deparse(desResult)
	if errCur != nil || errDes != nil {
		return current == desired
	}
	return curStr == desStr
}

// canCreateOrReplaceView reports whether a view definition change can be
// expressed as CREATE OR REPLACE VIEW. PostgreSQL requires the new query to
// produce columns with the same names in the same order as the existing
// view; only appending new columns at the end is allowed. Removing,
// renaming, reordering, or replacing a column makes CREATE OR REPLACE fail
// with `cannot drop columns from view` (or a similar error), so the caller
// must use DROP + CREATE in that case.
//
// Returns false when either side's output columns can't be determined
// statically (SELECT *, target-list expressions without an alias, parse
// error). That conservatively routes uncertain cases through DROP +
// CREATE, which always works at the cost of briefly dropping privileges /
// dependent views; strictly better than leaving the bug in place.
//
// Known limitation: type-only changes on a same-named column (e.g.
// `SELECT n FROM t` -> `SELECT n::bigint AS n FROM t`) are reported as
// CREATE-OR-REPLACE-able because the names line up, but PostgreSQL still
// rejects them with `cannot change data type of view column`. pg_query
// doesn't perform type inference, so we can't detect this statically;
// users who hit it can resolve manually by adjusting the source DDL or
// dropping the view in a pre-step.
func canCreateOrReplaceView(current, desired string) bool {
	curCols, curOK := viewOutputColumns(current)
	desCols, desOK := viewOutputColumns(desired)
	if !curOK || !desOK {
		return false
	}
	if len(desCols) < len(curCols) {
		return false
	}
	for i, name := range curCols {
		if desCols[i] != name {
			return false
		}
	}
	return true
}

// viewOutputColumns parses a view definition's SELECT body and returns the
// ordered list of output column names. Returns ok=false when any column
// can't be named statically (SELECT *, t.*, or an expression target
// without an alias) or when parsing fails.
func viewOutputColumns(definition string) (cols []string, ok bool) {
	def := strings.TrimSpace(definition)
	def = strings.TrimSuffix(def, ";")
	result, err := pg_query.Parse("CREATE VIEW _v AS " + def)
	if err != nil || len(result.Stmts) != 1 {
		return nil, false
	}
	vs := result.Stmts[0].Stmt.GetViewStmt()
	if vs == nil || vs.Query == nil {
		return nil, false
	}
	return selectOutputColumns(vs.Query)
}

// selectOutputColumns walks a SelectStmt and returns the ordered names of
// its top-level target list. For UNION / INTERSECT / EXCEPT it recurses
// into the left arm, since PostgreSQL inherits set-operation result names
// from the first SELECT.
func selectOutputColumns(node *pg_query.Node) ([]string, bool) {
	ss := node.GetSelectStmt()
	if ss == nil {
		return nil, false
	}
	if ss.Op != pg_query.SetOperation_SETOP_NONE {
		if ss.Larg == nil {
			return nil, false
		}
		return selectOutputColumns(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Larg}})
	}
	if len(ss.TargetList) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(ss.TargetList))
	for _, t := range ss.TargetList {
		rt := t.GetResTarget()
		if rt == nil {
			return nil, false
		}
		if rt.Name != "" {
			names = append(names, rt.Name)
			continue
		}
		if rt.Val == nil {
			return nil, false
		}
		cr := rt.Val.GetColumnRef()
		if cr == nil || len(cr.Fields) == 0 {
			return nil, false
		}
		last := cr.Fields[len(cr.Fields)-1]
		if last.GetAStar() != nil {
			return nil, false
		}
		s := last.GetString_()
		if s == nil {
			return nil, false
		}
		names = append(names, s.Sval)
	}
	return names, true
}

// stripQualifications removes the schema from every RangeVar and the table
// prefix from every two-part ColumnRef in the statement. pg_get_viewdef omits
// the schema when it is on the search path and qualifies a column with its
// table, and a written view definition tends to do the opposite, so both sides
// are reduced to the bare form before they are compared.
//
// The walk reaches every node kind, which is what the older enumeration could
// not do: a reference sitting inside COALESCE, CASE, GREATEST, a subscript, a
// row or array constructor, IS TRUE, COLLATE, an XML or SQL/JSON expression,
// an aggregate ORDER BY or FILTER, an OVER clause, DISTINCT ON, GROUPING SETS
// or a sub-query nested under any of them kept its prefix and re-emitted
// CREATE OR REPLACE VIEW on every plan.
func stripQualifications(node *pg_query.Node) {
	pgast.Walk(node, pgast.WalkOptions{}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		if rv := n.GetRangeVar(); rv != nil {
			rv.Schemaname = ""
			return n
		}
		if cr := n.GetColumnRef(); cr != nil {
			// "table.column" -> "column". Only when both parts are plain
			// identifiers, so table.* keeps its prefix.
			if len(cr.Fields) == 2 && cr.Fields[1].GetString_() != nil {
				cr.Fields = cr.Fields[1:]
			}
		}
		return n
	})
}

// ViewDiffResult separates view DROP and CREATE/MODIFY statements.
// Drops should run before table changes, creates after.
type ViewDiffResult struct {
	DropStmts           []string // DROP VIEW / DROP MATERIALIZED VIEW (should run before table changes)
	CreateStmts         []string // ALTER VIEW RENAME, CREATE OR REPLACE VIEW, CREATE MATERIALIZED VIEW, indexes, comments (should run after table changes)
	DisallowedDropStmts []string // DROP VIEW / DROP MATERIALIZED VIEW / DROP INDEX (on matview) suppressed by DropChecker, with "-- skipped: " prefix
	HasConcurrently     bool     // true if any index operation uses CONCURRENTLY
}

func DiffViews(current, desired *orderedmap.Map[string, *model.View], dc DropChecker) (*ViewDiffResult, error) {
	dc = normalizeDropChecker(dc)
	result := &ViewDiffResult{}

	// Detect renames
	renameStmts, current, renamedFrom, err := detectViewRenames(current, desired)
	if err != nil {
		return nil, err
	}

	// Track views that are recreated (DROP+CREATE) so comments can be re-applied.
	recreated := make(map[string]bool)
	// Track views whose recreation was suppressed by --allow-drop. For these we
	// skip the executable comment diff too, so the output reflects "nothing
	// executable" rather than emitting half of the intended change.
	recreateDenied := make(map[string]bool)
	// A renamed view that ALSO needs DROP+CREATE (e.g. its column list is
	// changing) must not go through the ALTER ... RENAME path: that
	// statement is sequenced with CreateStmts, after DropStmts, so by
	// the time it runs the executable DROP has already failed against
	// the old name. Track these so the rename statement is dropped
	// and the DROP statement uses the old name instead.
	needsRecreateRenamed := map[string]bool{}
	for newKey := range renamedFrom {
		desiredView, dok := desired.GetOk(newKey)
		currentView, cok := current.GetOk(newKey)
		if !dok || !cok {
			continue
		}
		// detectViewRenames rejects a rename that switches between VIEW and
		// MATERIALIZED VIEW, so this cannot be reached. Kept for the day that
		// rename is allowed, when a type switch has to take the recreate path
		// rather than ALTER ... RENAME.
		// if currentView.Materialized != desiredView.Materialized {
		// 	needsRecreateRenamed[newKey] = true
		// 	continue
		// }
		if !equalViewDef(currentView.Definition, desiredView.Definition) {
			if desiredView.Materialized || !canCreateOrReplaceView(currentView.Definition, desiredView.Definition) {
				needsRecreateRenamed[newKey] = true
			}
		}
	}
	for _, stmt := range renameStmts {
		skip := false
		for newKey := range needsRecreateRenamed {
			oldKey := renamedFrom[newKey]
			desiredView, _ := desired.GetOk(newKey)
			needle := oldKey + " RENAME TO " + model.Ident(desiredView.Name) + ";"
			if strings.Contains(stmt, needle) {
				skip = true
				break
			}
		}
		if !skip {
			result.CreateStmts = append(result.CreateStmts, stmt)
		}
	}

	// New or modified views (CREATE OR REPLACE / recreate for materialized)
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)
		if !ok {
			// New view
			result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
			result.CreateStmts = append(result.CreateStmts, viewTriggerStmts(desiredView)...)
			// Add indexes for new materialized views
			if desiredView.Materialized && desiredView.Indexes != nil {
				for _, idx := range desiredView.Indexes.CollectValues() {
					stmt, err := createIndexSQL(idx.Definition, idx.Concurrently)
					if err != nil {
						return nil, fmt.Errorf("create index %s on %s: %w", model.Ident(idx.Schema, idx.Name), k, err)
					}
					result.CreateStmts = append(result.CreateStmts, stmt)
					if idx.Concurrently {
						result.HasConcurrently = true
					}
				}
			}
		} else if !equalViewDef(currentView.Definition, desiredView.Definition) || currentView.Materialized != desiredView.Materialized {
			// Materialized views always need DROP+CREATE; VIEW<->MATVIEW
			// switches do too. Regular view definition changes prefer
			// CREATE OR REPLACE; but PostgreSQL rejects it when the new
			// query removes, renames, or reorders an existing output
			// column, so detect that case and fall back to DROP+CREATE.
			needsDropCreate := desiredView.Materialized || currentView.Materialized != desiredView.Materialized
			if !needsDropCreate && !canCreateOrReplaceView(currentView.Definition, desiredView.Definition) {
				needsDropCreate = true
			}
			if needsDropCreate {
				// When the view was also renamed, drop the old name;
				// the database still has it because the ALTER RENAME
				// was suppressed above. Computed before the drop-policy
				// branch so the skipped-DROP comment matches the
				// relation that actually exists.
				dropName := k
				if oldKey, renamed := renamedFrom[k]; renamed {
					dropName = oldKey
				}
				// Materialized views or type changes (VIEW <-> MATERIALIZED VIEW)
				// require DROP and recreate. Only proceed if drops are allowed;
				// otherwise emit a commented DROP for visibility (no CREATE,
				// since recreation requires the drop).
				if dc.IsDropAllowed("view") {
					if currentView.Materialized {
						result.DropStmts = append(result.DropStmts, "DROP MATERIALIZED VIEW "+dropName+";")
					} else {
						result.DropStmts = append(result.DropStmts, "DROP VIEW "+dropName+";")
					}
					result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
					// DROP VIEW takes the view's triggers with it, so the
					// recreate has to put them back.
					result.CreateStmts = append(result.CreateStmts, viewTriggerStmts(desiredView)...)
					if desiredView.Materialized && desiredView.Indexes != nil {
						for _, idx := range desiredView.Indexes.CollectValues() {
							stmt, err := createIndexSQL(idx.Definition, idx.Concurrently)
							if err != nil {
								return nil, fmt.Errorf("create index %s on %s: %w", model.Ident(idx.Schema, idx.Name), k, err)
							}
							result.CreateStmts = append(result.CreateStmts, stmt)
							if idx.Concurrently {
								result.HasConcurrently = true
							}
						}
					}
					recreated[k] = true
				} else {
					if currentView.Materialized {
						result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP MATERIALIZED VIEW "+dropName+";")
					} else {
						result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP VIEW "+dropName+";")
					}
					recreateDenied[k] = true
				}
			} else {
				// Regular view: CREATE OR REPLACE
				result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
			}
		} else if desiredView.Materialized {
			// Definition unchanged, diff indexes
			viewIdxStmts, viewIdxDisallowed, viewIdxHasConcurrently, err := diffViewIndexes(currentView, desiredView, dc)
			if err != nil {
				return nil, err
			}
			result.CreateStmts = append(result.CreateStmts, viewIdxStmts...)
			result.DisallowedDropStmts = append(result.DisallowedDropStmts, viewIdxDisallowed...)
			if viewIdxHasConcurrently {
				result.HasConcurrently = true
			}
		} else if currentView.CheckOption != desiredView.CheckOption {
			// Definition unchanged, check option changed. A replaced
			// definition carries the clause on its CREATE OR REPLACE VIEW,
			// which replaces the options as a whole, so only this branch
			// needs an ALTER.
			result.CreateStmts = append(result.CreateStmts, model.SetCheckOptionSQL(k, desiredView.CheckOption))
		}

		// A view that stayed in place, whether its definition was replaced or
		// left alone, kept its triggers, so they are diffed. The branches that
		// created the view already emitted them, and a denied recreate left
		// the view as it was.
		if ok && !recreated[k] && !recreateDenied[k] {
			trgStmts, trgDisallowed, err := diffTriggers(k, currentView.Triggers, desiredView.Triggers, dc)
			if err != nil {
				return nil, err
			}
			result.CreateStmts = append(result.CreateStmts, trgStmts...)
			result.DisallowedDropStmts = append(result.DisallowedDropStmts, trgDisallowed...)
		}
	}

	// Dropped views. When the view-drop policy disallows it, emit a commented DROP.
	viewAllowed := dc.IsDropAllowed("view")
	for k, v := range current.All() {
		if _, ok := desired.GetOk(k); !ok {
			drop := "DROP VIEW " + k + ";"
			if v.Materialized {
				drop = "DROP MATERIALIZED VIEW " + k + ";"
			}
			if viewAllowed {
				result.DropStmts = append(result.DropStmts, drop)
			} else {
				result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: "+drop)
			}
		}
	}

	// Comment changes
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)

		// If the recreation was blocked by --allow-drop (type change or
		// matview definition change), the object on disk still matches the
		// pre-recreation shape, so skip comment diff to keep the output
		// consistent with "nothing executable was emitted for this view".
		if recreateDenied[k] {
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

// viewTriggerStmts renders the CREATE TRIGGER statements for a view that is
// about to be created. A materialized view cannot carry a trigger, so the map
// is empty there.
func viewTriggerStmts(v *model.View) []string {
	if v.Triggers == nil {
		return nil
	}
	var stmts []string
	for _, trg := range v.Triggers.CollectValues() {
		stmts = append(stmts, trg.SQL())
	}
	return stmts
}

// diffViewIndexes generates DDL for index changes on materialized views.
func diffViewIndexes(current, desired *model.View, dc DropChecker) (stmts []string, disallowed []string, hasConcurrently bool, err error) {
	dc = normalizeDropChecker(dc)
	currentIndexes := orderedmap.New[string, *model.Index]()
	if current.Indexes != nil {
		currentIndexes = current.Indexes
	}
	desiredIndexes := orderedmap.New[string, *model.Index]()
	if desired.Indexes != nil {
		desiredIndexes = desired.Indexes
	}

	sameDef := equalIndexDefs(currentIndexes, desiredIndexes)

	// Drop removed or changed indexes. Pure removals honor the index-drop
	// policy; definition changes still run DROP+CREATE.
	idxAllowed := dc.IsDropAllowed("index")
	for name, currentIdx := range currentIndexes.All() {
		desiredIdx, ok := desiredIndexes.GetOk(name)
		if !ok || !sameDef[name] {
			// Pure drops have no desired entry. Fall back to the current
			// entry's flag, which forceConcurrentlyDirectives sets when
			// --force-index-concurrently is in effect.
			useConcurrently := currentIdx.Concurrently
			if ok {
				useConcurrently = desiredIdx.Concurrently
			}
			stmt, err := dropIndexSQL(currentIdx.Schema, name, useConcurrently)
			if err != nil {
				return nil, nil, false, fmt.Errorf("drop index %s: %w", model.Ident(currentIdx.Schema, name), err)
			}
			if !ok && !idxAllowed {
				disallowed = append(disallowed, "-- skipped: "+stmt)
				continue
			}
			stmts = append(stmts, stmt)
			if useConcurrently {
				hasConcurrently = true
			}
		}
	}

	// Add new or changed indexes
	for name, desiredIdx := range desiredIndexes.All() {
		if _, ok := currentIndexes.GetOk(name); ok && sameDef[name] {
			continue
		}
		stmt, err := createIndexSQL(desiredIdx.Definition, desiredIdx.Concurrently)
		if err != nil {
			return nil, nil, false, fmt.Errorf("create index %s: %w", model.Ident(desiredIdx.Schema, name), err)
		}
		stmts = append(stmts, stmt)
		if desiredIdx.Concurrently {
			hasConcurrently = true
		}
	}

	return stmts, disallowed, hasConcurrently, nil
}
