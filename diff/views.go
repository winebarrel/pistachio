package diff

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
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
//   - symmetric expression normalization via normalizeSelectExprs:
//     paren / text-like cast stripping and `= ANY(ARRAY[...])` → `IN (...)`,
//   - asymmetric current-only cast alignment via alignSelectCasts: strips
//     TypeCasts (notably 'lit'::enum_type) the catalog added but the user
//     didn't write. Top-level casts at the target list are preserved on
//     both sides since they affect the resulting view's column type.
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
		normalizeSelectExprs(stmt.Stmt)
	}
	for _, stmt := range desResult.Stmts {
		stripQualifications(stmt.Stmt)
		normalizeSelectExprs(stmt.Stmt)
	}
	if len(curResult.Stmts) == 1 && len(desResult.Stmts) == 1 {
		alignSelectCasts(desResult.Stmts[0].Stmt, curResult.Stmts[0].Stmt)
	}
	curStr, errCur := pg_query.Deparse(curResult)
	desStr, errDes := pg_query.Deparse(desResult)
	if errCur != nil || errDes != nil {
		return current == desired
	}
	return curStr == desStr
}

// normalizeSelectExprs walks a SELECT statement (and any nested
// SELECT/JOIN/CTE) and applies normalizeCheckExpr at every position that
// holds an expression. Converts `= ANY(ARRAY[...])` back to `IN (...)` and
// strips text-like TypeCasts. Called both from equalViewDef and from
// normalizeCheckExpr's SubLink case for sub-queries inside CHECK / RLS
// expressions.
func normalizeSelectExprs(node *pg_query.Node) {
	if node == nil {
		return
	}

	if vs := node.GetViewStmt(); vs != nil {
		normalizeSelectExprs(vs.Query)
		return
	}

	if ss := node.GetSelectStmt(); ss != nil {
		if ss.WithClause != nil {
			for _, cte := range ss.WithClause.Ctes {
				if c := cte.GetCommonTableExpr(); c != nil {
					normalizeSelectExprs(c.Ctequery)
				}
			}
		}
		for _, from := range ss.FromClause {
			normalizeSelectExprs(from)
		}
		for _, target := range ss.TargetList {
			if rt := target.GetResTarget(); rt != nil && rt.Val != nil {
				rt.Val = normalizeTargetExpr(rt.Val)
			}
		}
		if ss.WhereClause != nil {
			ss.WhereClause = normalizeCheckExpr(ss.WhereClause)
		}
		if ss.HavingClause != nil {
			ss.HavingClause = normalizeCheckExpr(ss.HavingClause)
		}
		for i, gb := range ss.GroupClause {
			ss.GroupClause[i] = normalizeCheckExpr(gb)
		}
		for _, sb := range ss.SortClause {
			if s := sb.GetSortBy(); s != nil {
				s.Node = normalizeCheckExpr(s.Node)
			}
		}
		if ss.LimitCount != nil {
			ss.LimitCount = normalizeCheckExpr(ss.LimitCount)
		}
		if ss.LimitOffset != nil {
			ss.LimitOffset = normalizeCheckExpr(ss.LimitOffset)
		}
		if ss.Larg != nil {
			normalizeSelectExprs(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Larg}})
		}
		if ss.Rarg != nil {
			normalizeSelectExprs(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Rarg}})
		}
		return
	}

	if join := node.GetJoinExpr(); join != nil {
		normalizeSelectExprs(join.Larg)
		normalizeSelectExprs(join.Rarg)
		if join.Quals != nil {
			join.Quals = normalizeCheckExpr(join.Quals)
		}
		return
	}

	if sub := node.GetRangeSubselect(); sub != nil {
		normalizeSelectExprs(sub.Subquery)
		return
	}
}

// normalizeTargetExpr is normalizeCheckExpr with a top-level TypeCast
// preserved. In a SELECT target list the cast type determines the output
// column type, so symmetric text-cast stripping at that position would
// hide an intentional `SELECT col` → `SELECT col::text` change.
func normalizeTargetExpr(node *pg_query.Node) *pg_query.Node {
	if tc := node.GetTypeCast(); tc != nil {
		tc.Arg = normalizeCheckExpr(tc.Arg)
		return node
	}
	return normalizeCheckExpr(node)
}

// alignTargetCasts is alignCurrentCasts with a top-level TypeCast
// asymmetry preserved (a cast on one side but not the other surfaces
// as a diff). Below a matched top-level cast on both sides, normal
// alignment applies. Rationale matches normalizeTargetExpr.
func alignTargetCasts(desired, current *pg_query.Node) *pg_query.Node {
	dt := desired.GetTypeCast()
	ct := current.GetTypeCast()
	if dt != nil && ct != nil {
		ct.Arg = alignCurrentCasts(dt.Arg, ct.Arg)
		return current
	}
	if dt != nil || ct != nil {
		return current
	}
	return alignCurrentCasts(desired, current)
}

// alignSelectCasts performs the same parallel walk as normalizeSelectExprs but
// across two trees, applying alignCurrentCasts at each expression position
// to strip TypeCasts present only on the current side. Used by equalViewDef
// for the asymmetric current↔desired comparison, and from alignCurrentCasts'
// SubLink case for sub-queries inside CHECK / RLS expressions.
func alignSelectCasts(desired, current *pg_query.Node) {
	if desired == nil || current == nil {
		return
	}

	if dv := desired.GetViewStmt(); dv != nil {
		if cv := current.GetViewStmt(); cv != nil {
			alignSelectCasts(dv.Query, cv.Query)
		}
		return
	}

	if ds := desired.GetSelectStmt(); ds != nil {
		cs := current.GetSelectStmt()
		if cs == nil {
			return
		}
		if ds.WithClause != nil && cs.WithClause != nil && len(ds.WithClause.Ctes) == len(cs.WithClause.Ctes) {
			for i := range ds.WithClause.Ctes {
				dc := ds.WithClause.Ctes[i].GetCommonTableExpr()
				cc := cs.WithClause.Ctes[i].GetCommonTableExpr()
				if dc != nil && cc != nil {
					alignSelectCasts(dc.Ctequery, cc.Ctequery)
				}
			}
		}
		if len(ds.FromClause) == len(cs.FromClause) {
			for i := range ds.FromClause {
				alignSelectCasts(ds.FromClause[i], cs.FromClause[i])
			}
		}
		if len(ds.TargetList) == len(cs.TargetList) {
			for i := range ds.TargetList {
				dt := ds.TargetList[i].GetResTarget()
				ct := cs.TargetList[i].GetResTarget()
				if dt != nil && ct != nil && dt.Val != nil && ct.Val != nil {
					ct.Val = alignTargetCasts(dt.Val, ct.Val)
				}
			}
		}
		if ds.WhereClause != nil && cs.WhereClause != nil {
			cs.WhereClause = alignCurrentCasts(ds.WhereClause, cs.WhereClause)
		}
		if ds.HavingClause != nil && cs.HavingClause != nil {
			cs.HavingClause = alignCurrentCasts(ds.HavingClause, cs.HavingClause)
		}
		if len(ds.GroupClause) == len(cs.GroupClause) {
			for i := range ds.GroupClause {
				cs.GroupClause[i] = alignCurrentCasts(ds.GroupClause[i], cs.GroupClause[i])
			}
		}
		if len(ds.SortClause) == len(cs.SortClause) {
			for i := range ds.SortClause {
				dsb := ds.SortClause[i].GetSortBy()
				csb := cs.SortClause[i].GetSortBy()
				if dsb != nil && csb != nil {
					csb.Node = alignCurrentCasts(dsb.Node, csb.Node)
				}
			}
		}
		if ds.LimitCount != nil && cs.LimitCount != nil {
			cs.LimitCount = alignCurrentCasts(ds.LimitCount, cs.LimitCount)
		}
		if ds.LimitOffset != nil && cs.LimitOffset != nil {
			cs.LimitOffset = alignCurrentCasts(ds.LimitOffset, cs.LimitOffset)
		}
		if ds.Larg != nil && cs.Larg != nil {
			alignSelectCasts(
				&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ds.Larg}},
				&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: cs.Larg}},
			)
		}
		if ds.Rarg != nil && cs.Rarg != nil {
			alignSelectCasts(
				&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ds.Rarg}},
				&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: cs.Rarg}},
			)
		}
		return
	}

	if dj := desired.GetJoinExpr(); dj != nil {
		if cj := current.GetJoinExpr(); cj != nil {
			alignSelectCasts(dj.Larg, cj.Larg)
			alignSelectCasts(dj.Rarg, cj.Rarg)
			if dj.Quals != nil && cj.Quals != nil {
				cj.Quals = alignCurrentCasts(dj.Quals, cj.Quals)
			}
		}
		return
	}

	if drs := desired.GetRangeSubselect(); drs != nil {
		if crs := current.GetRangeSubselect(); crs != nil {
			alignSelectCasts(drs.Subquery, crs.Subquery)
		}
		return
	}
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
		// Testexpr holds the LHS of `x IN (SELECT ...)` / `x = ANY (SELECT ...)`
		// forms; stripping qualifications here matches the bare-IN A_Expr path.
		stripQualifications(sl.Testexpr)
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
	DropStmts           []string // DROP VIEW / DROP MATERIALIZED VIEW (should run before table changes)
	CreateStmts         []string // ALTER VIEW RENAME, CREATE OR REPLACE VIEW, CREATE MATERIALIZED VIEW, indexes, comments (should run after table changes)
	DisallowedDropStmts []string // DROP VIEW / DROP MATERIALIZED VIEW / DROP INDEX (on matview) suppressed by DropChecker, with "-- skipped: " prefix
	HasConcurrently     bool     // true if any index operation uses CONCURRENTLY
}

func DiffViews(current, desired *orderedmap.Map[string, *model.View], dc DropChecker) (*ViewDiffResult, error) {
	dc = normalizeDropChecker(dc)
	result := &ViewDiffResult{}

	// Detect renames
	renameStmts, current, err := detectViewRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.CreateStmts = append(result.CreateStmts, renameStmts...)

	// Track views that are recreated (DROP+CREATE) so comments can be re-applied.
	recreated := make(map[string]bool)
	// Track views whose recreation was suppressed by --allow-drop. For these we
	// skip the executable comment diff too, so the output reflects "nothing
	// executable" rather than emitting half of the intended change.
	recreateDenied := make(map[string]bool)

	// New or modified views (CREATE OR REPLACE / recreate for materialized)
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)
		if !ok {
			// New view
			result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
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
			needsDropCreate := desiredView.Materialized || currentView.Materialized != desiredView.Materialized
			if needsDropCreate {
				// Materialized views or type changes (VIEW ↔ MATERIALIZED VIEW)
				// require DROP and recreate. Only proceed if drops are allowed;
				// otherwise emit a commented DROP for visibility (no CREATE,
				// since recreation requires the drop).
				if dc.IsDropAllowed("view") {
					if currentView.Materialized {
						result.DropStmts = append(result.DropStmts, "DROP MATERIALIZED VIEW "+k+";")
					} else {
						result.DropStmts = append(result.DropStmts, "DROP VIEW "+k+";")
					}
					result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
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
						result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP MATERIALIZED VIEW "+k+";")
					} else {
						result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP VIEW "+k+";")
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

	// Drop removed or changed indexes. Pure removals honor the index-drop
	// policy; definition changes still run DROP+CREATE.
	idxAllowed := dc.IsDropAllowed("index")
	for name, currentIdx := range currentIndexes.All() {
		desiredIdx, ok := desiredIndexes.GetOk(name)
		if !ok || !equalIndexDef(currentIdx.Definition, desiredIdx.Definition) {
			useConcurrently := false
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
		currentIdx, ok := currentIndexes.GetOk(name)
		if !ok || !equalIndexDef(currentIdx.Definition, desiredIdx.Definition) {
			stmt, err := createIndexSQL(desiredIdx.Definition, desiredIdx.Concurrently)
			if err != nil {
				return nil, nil, false, fmt.Errorf("create index %s: %w", model.Ident(desiredIdx.Schema, name), err)
			}
			stmts = append(stmts, stmt)
			if desiredIdx.Concurrently {
				hasConcurrently = true
			}
		}
	}

	return stmts, disallowed, hasConcurrently, nil
}
