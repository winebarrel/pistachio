package diff

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// detectEnumRenames finds desired enums with RenameFrom that match a current enum.
func detectEnumRenames(current, desired *orderedmap.Map[string, *model.Enum]) ([]string, *orderedmap.Map[string, *model.Enum], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredEnum := range desired.All() {
		if desiredEnum.RenameFrom == nil {
			continue
		}
		oldKey := *desiredEnum.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldEnum, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldEnum.Schema != desiredEnum.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER TYPE "+oldKey+" RENAME TO "+model.Ident(desiredEnum.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldEnum
		renamed.Name = desiredEnum.Name
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// detectSequenceRenames finds desired sequences with RenameFrom that match a current sequence.
func detectSequenceRenames(current, desired *orderedmap.Map[string, *model.Sequence]) ([]string, *orderedmap.Map[string, *model.Sequence], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredSeq := range desired.All() {
		if desiredSeq.RenameFrom == nil {
			continue
		}
		oldKey := *desiredSeq.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldSeq, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldSeq.Schema != desiredSeq.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER SEQUENCE "+oldKey+" RENAME TO "+model.Ident(desiredSeq.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldSeq
		renamed.Name = desiredSeq.Name
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// detectTableRenames finds desired tables with RenameFrom that match a current table.
//
// NOTE: After a table rename, other objects that reference the old table name
// (e.g. foreign keys in other tables, view definitions) are not updated in the
// adjusted current state. PostgreSQL automatically updates these on RENAME, so
// running plan/apply a second time after a rename will produce a clean diff.
// A single plan may emit redundant DROP/CREATE for dependent objects.
func detectTableRenames(current, desired *orderedmap.Map[string, *model.Table]) ([]string, *orderedmap.Map[string, *model.Table], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredTable := range desired.All() {
		if desiredTable.RenameFrom == nil {
			continue
		}
		oldKey := *desiredTable.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldTable, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldTable.Schema != desiredTable.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER TABLE "+oldKey+" RENAME TO "+model.Ident(desiredTable.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldTable
		renamed.Name = desiredTable.Name

		// Update index definitions to reflect the new table name via pg_query parse/deparse
		if renamed.Indexes.Len() > 0 {
			newIndexes := orderedmap.New[string, *model.Index]()
			for idxName, idx := range renamed.Indexes.All() {
				idxCopy := *idx
				idxCopy.Table = desiredTable.Name
				updatedDef, err := updateIndexTableName(idx.Definition, desiredTable.Name)
				if err != nil {
					return nil, nil, err
				}
				idxCopy.Definition = updatedDef
				newIndexes.Set(idxName, &idxCopy)
			}
			renamed.Indexes = newIndexes
		}

		// Update FK table name
		if renamed.ForeignKeys.Len() > 0 {
			newFKs := orderedmap.New[string, *model.ForeignKey]()
			for fkName, fk := range renamed.ForeignKeys.All() {
				fkCopy := *fk
				fkCopy.Table = desiredTable.Name
				newFKs.Set(fkName, &fkCopy)
			}
			renamed.ForeignKeys = newFKs
		}

		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// updateIndexTableName parses an index definition, updates the table name,
// and deparses it back to canonical SQL.
func updateIndexTableName(def string, newTableName string) (string, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse index definition: %w", err)
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil || is.Relation == nil {
		return "", fmt.Errorf("failed to parse index definition: expected IndexStmt with relation")
	}
	is.Relation.Relname = newTableName
	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse index definition: %w", err)
	}
	return deparsed, nil
}

// detectViewRenames finds desired views with RenameFrom that match a current view.
func detectViewRenames(current, desired *orderedmap.Map[string, *model.View]) ([]string, *orderedmap.Map[string, *model.View], map[string]string, error) {
	var stmts []string
	adjusted := cloneMap(current)
	renamedFrom := map[string]string{}

	for newKey, desiredView := range desired.All() {
		if desiredView.RenameFrom == nil {
			continue
		}
		oldKey := *desiredView.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldView, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldView.Schema != desiredView.Schema {
			return nil, nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		if oldView.Materialized != desiredView.Materialized {
			return nil, nil, nil, fmt.Errorf("cannot rename %s to %s: view type mismatch (cannot rename between VIEW and MATERIALIZED VIEW)", oldKey, newKey)
		}

		objType := "VIEW"
		if oldView.Materialized {
			objType = "MATERIALIZED VIEW"
		}
		stmts = append(stmts, "ALTER "+objType+" "+oldKey+" RENAME TO "+model.Ident(desiredView.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldView
		renamed.Name = desiredView.Name
		adjusted.Set(newKey, &renamed)
		renamedFrom[newKey] = oldKey
	}

	return stmts, adjusted, renamedFrom, nil
}

// detectColumnRenames finds desired columns with RenameFrom that match a current column.
// Column references in same-table indexes, constraints, foreign keys and
// triggers are rewritten in the adjusted current state by
// `rewriteColumnRefsInIndexes`, `rewriteColumnRefsInConstraints`,
// `rewriteColumnRefsInForeignKeys` and `rewriteColumnRefsInTriggers`
// (called from diffTable), so a plain rename does not produce redundant
// drop/recreate operations on those dependents. View definitions and
// foreign-key references in *other* tables are still not rewritten; see
// TODO.md "Auto-rewrite of column references in views and cross-table FKs".
func detectColumnRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Column]) ([]string, *orderedmap.Map[string, *model.Column], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredCol := range desired.All() {
		if desiredCol.RenameFrom == nil {
			continue
		}
		oldName := *desiredCol.RenameFrom

		if oldName == newName {
			continue
		}

		oldCol, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source column %s not found in %s", model.Ident(oldName), fqtn)
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, fmt.Errorf("cannot rename column %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
			}
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME COLUMN "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldCol
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// detectConstraintRenames finds desired constraints with RenameFrom that match a
// current constraint. Returns (adjustedCurrent, renamedFrom map[newName]oldName,
// error). The caller renders the ALTER TABLE ... RENAME CONSTRAINT statements,
// so it can leave out the ones whose constraint is being recreated and keep the
// rest in the desired schema's order.
func detectConstraintRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Constraint]) (*orderedmap.Map[string, *model.Constraint], map[string]string, error) {
	adjusted := cloneMap(current)
	renamedFrom := map[string]string{}

	for newName, desiredCon := range desired.All() {
		if desiredCon.RenameFrom == nil {
			continue
		}
		oldName := *desiredCon.RenameFrom

		if oldName == newName {
			continue
		}

		oldCon, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source constraint %s not found in %s", model.Ident(oldName), fqtn)
		}

		if _, exists := adjusted.GetOk(newName); exists {
			return nil, nil, fmt.Errorf("cannot rename constraint %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
		}

		renamedFrom[newName] = oldName

		adjusted.Delete(oldName)
		renamed := *oldCon
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return adjusted, renamedFrom, nil
}

// detectIndexRenames finds desired indexes with RenameFrom that match a current index.
func detectIndexRenames(current, desired *orderedmap.Map[string, *model.Index]) ([]string, *orderedmap.Map[string, *model.Index], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredIdx := range desired.All() {
		if desiredIdx.RenameFrom == nil {
			continue
		}
		oldName := *desiredIdx.RenameFrom

		if oldName == newName {
			continue
		}

		oldIdx, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source index %s not found", model.Ident(oldName))
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, fmt.Errorf("cannot rename index %s to %s: destination already exists", model.Ident(oldName), model.Ident(newName))
			}
		}

		stmts = append(stmts, "ALTER INDEX "+model.Ident(oldIdx.Schema, oldName)+" RENAME TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldIdx
		renamed.Name = newName
		// Update definition to reflect the new index name via pg_query parse/deparse
		updatedDef, err := updateIndexName(renamed.Definition, newName)
		if err != nil {
			return nil, nil, err
		}
		renamed.Definition = updatedDef
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// updateIndexName parses an index definition, updates the index name, and deparses.
func updateIndexName(def string, newName string) (string, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse index definition: %w", err)
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return "", fmt.Errorf("failed to parse index definition: expected IndexStmt")
	}
	is.Idxname = newName
	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse index definition: %w", err)
	}
	return deparsed, nil
}

// detectForeignKeyRenames finds desired foreign keys with RenameFrom that match a current FK.
// Returns (renameStmts, adjustedCurrent, renamedFrom map[newName]oldName, error).
func detectForeignKeyRenames(fqtn string, current, desired *orderedmap.Map[string, *model.ForeignKey]) (*orderedmap.Map[string, *model.ForeignKey], map[string]string, error) {
	adjusted := cloneMap(current)
	renamedFrom := map[string]string{}

	for newName, desiredFK := range desired.All() {
		if desiredFK.RenameFrom == nil {
			continue
		}
		oldName := *desiredFK.RenameFrom

		if oldName == newName {
			continue
		}

		oldFK, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source foreign key %s not found in %s", model.Ident(oldName), fqtn)
		}

		if _, exists := adjusted.GetOk(newName); exists {
			return nil, nil, fmt.Errorf("cannot rename foreign key %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
		}

		renamedFrom[newName] = oldName

		adjusted.Delete(oldName)
		renamed := *oldFK
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return adjusted, renamedFrom, nil
}

// detectPolicyRenames finds desired policies with RenameFrom that match a
// current policy on the same table. Returns the rename statements, the
// adjusted current map keyed by new names, and a renamedFrom[newName] = oldName
// map so the caller can suppress the RENAME when the policy needs DROP+CREATE.
func detectPolicyRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Policy]) ([]string, *orderedmap.Map[string, *model.Policy], map[string]string, error) {
	var stmts []string
	adjusted := cloneMap(current)
	renamedFrom := map[string]string{}

	for newName, desiredPol := range desired.All() {
		if desiredPol.RenameFrom == nil {
			continue
		}
		oldName := *desiredPol.RenameFrom

		if oldName == newName {
			continue
		}

		oldPol, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, nil, fmt.Errorf("rename source policy %s not found on %s", model.Ident(oldName), fqtn)
		}

		if _, exists := adjusted.GetOk(newName); exists {
			return nil, nil, nil, fmt.Errorf("cannot rename policy %s to %s on %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
		}

		stmts = append(stmts, "ALTER POLICY "+model.Ident(oldName)+" ON "+fqtn+" RENAME TO "+model.Ident(newName)+";")
		renamedFrom[newName] = oldName

		adjusted.Delete(oldName)
		renamed := *oldPol
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, renamedFrom, nil
}

// cloneMap creates a shallow copy of an orderedmap.
func cloneMap[K comparable, V any](m *orderedmap.Map[K, V]) *orderedmap.Map[K, V] {
	clone := orderedmap.New[K, V]()
	for k, v := range m.All() {
		clone.Set(k, v)
	}
	return clone
}

// collectColumnRenames returns a map of old column name -> new column name
// for desired columns annotated with -- pista:renamed-from.
func collectColumnRenames(desired *orderedmap.Map[string, *model.Column]) map[string]string {
	renames := make(map[string]string)
	for name, col := range desired.All() {
		if col.RenameFrom != nil && *col.RenameFrom != name {
			renames[*col.RenameFrom] = name
		}
	}
	return renames
}

// rewriteColumnRefsInExpr walks an expression tree and rewrites each
// unqualified ColumnRef whose name appears in renames to the mapped new
// name, mutating the tree in place. Each ColumnRef is matched against the
// original-name set in a single pass, so chained renames (a->b alongside
// b->c) cannot cascade.
func rewriteColumnRefsInExpr(node *pg_query.Node, renames map[string]string) {
	pgast.WalkExprColumnRefs(node, func(s *pg_query.String) {
		if newName, ok := renames[s.Sval]; ok {
			s.Sval = newName
		}
	})
}

// rewriteColumnsInIndexDef returns a new index definition with column
// references rewritten according to the renames map (old -> new). Returns an
// error (and an empty string) if pg_query parse/deparse fails; callers fall
// back to the original definition.
//
// All renames are applied in a single AST walk, so chained renames (a->b and
// b->c) do not cascade; each original column name is matched once against
// the renames map.
func rewriteColumnsInIndexDef(def string, renames map[string]string) (string, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse index definition: %w", err)
	}
	if len(result.Stmts) == 0 {
		return "", fmt.Errorf("empty index definition")
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return "", fmt.Errorf("expected IndexStmt in index definition")
	}
	rewriteIndexElems := func(params []*pg_query.Node) {
		for _, p := range params {
			ie := p.GetIndexElem()
			if ie == nil {
				continue
			}
			if newName, ok := renames[ie.Name]; ok {
				ie.Name = newName
			}
			rewriteColumnRefsInExpr(ie.Expr, renames)
		}
	}
	rewriteIndexElems(is.IndexParams)
	rewriteIndexElems(is.IndexIncludingParams)
	rewriteColumnRefsInExpr(is.WhereClause, renames)
	return pg_query.Deparse(result)
}

// rewriteColumnsInConstraintDef returns a new constraint definition fragment
// (e.g. "PRIMARY KEY (id)", "CHECK ((x > 0))", "FOREIGN KEY (a) REFERENCES t(b)")
// with column references rewritten according to the renames map (old -> new).
// PkAttrs (referenced columns on the foreign side) are intentionally NOT
// rewritten because they refer to a different table.
//
// All renames are applied in a single AST walk so chained renames cannot
// cascade.
func rewriteColumnsInConstraintDef(def string, renames map[string]string) (string, error) {
	result, con, err := pgast.ParseConstraintDefStrict(def)
	if err != nil {
		return "", err
	}

	rewriteStringList := func(nodes []*pg_query.Node) {
		for _, n := range nodes {
			if s := n.GetString_(); s != nil {
				if newName, ok := renames[s.Sval]; ok {
					s.Sval = newName
				}
			}
		}
	}
	rewriteStringList(con.Keys)
	rewriteStringList(con.Including)
	rewriteStringList(con.FkAttrs)
	rewriteColumnRefsInExpr(con.RawExpr, renames)
	// EXCLUDE constraints encode each (column, operator) pair as a List node
	// with an IndexElem followed by an operator. Walk the IndexElem side.
	for _, ex := range con.Exclusions {
		list := ex.GetList()
		if list == nil {
			continue
		}
		for _, item := range list.Items {
			ie := item.GetIndexElem()
			if ie == nil {
				continue
			}
			if newName, ok := renames[ie.Name]; ok {
				ie.Name = newName
			}
			rewriteColumnRefsInExpr(ie.Expr, renames)
		}
	}

	return pgast.DeparseConstraintDef(result)
}

// rewriteColumnRefsInIndexes returns a clone of indexes with each Definition
// updated to reflect the column renames. Definitions that fail to parse/deparse
// are left unchanged so downstream comparison still functions (will fall back
// to redundant DROP/CREATE in that case).
func rewriteColumnRefsInIndexes(indexes *orderedmap.Map[string, *model.Index], renames map[string]string) *orderedmap.Map[string, *model.Index] {
	out := orderedmap.New[string, *model.Index]()
	for name, idx := range indexes.All() {
		clone := *idx
		if updated, err := rewriteColumnsInIndexDef(clone.Definition, renames); err == nil {
			clone.Definition = updated
		}
		out.Set(name, &clone)
	}
	return out
}

// rewriteColumnRefsInConstraints returns a clone of constraints with each
// Definition updated to reflect column renames.
func rewriteColumnRefsInConstraints(cons *orderedmap.Map[string, *model.Constraint], renames map[string]string) *orderedmap.Map[string, *model.Constraint] {
	out := orderedmap.New[string, *model.Constraint]()
	for name, con := range cons.All() {
		clone := *con
		if updated, err := rewriteColumnsInConstraintDef(clone.Definition, renames); err == nil {
			clone.Definition = updated
		}
		out.Set(name, &clone)
	}
	return out
}

// renameColumnKeys returns a clone of the columns map where each key in
// renames is replaced by its mapped new name. Iteration is single-pass
// (each existing key is matched once against the renames map) so chained
// renames such as a->b alongside b->c do not cascade. Order is preserved.
func renameColumnKeys(cols *orderedmap.Map[string, *model.Column], renames map[string]string) *orderedmap.Map[string, *model.Column] {
	out := orderedmap.New[string, *model.Column]()
	for name, col := range cols.All() {
		if newName, ok := renames[name]; ok {
			out.Set(newName, col)
		} else {
			out.Set(name, col)
		}
	}
	return out
}

// rewriteColumnRefsInForeignKeys returns a clone of FKs with each Definition
// updated to reflect column renames on the local side (FkAttrs). Referenced
// columns are not touched.
func rewriteColumnRefsInForeignKeys(fks *orderedmap.Map[string, *model.ForeignKey], renames map[string]string) *orderedmap.Map[string, *model.ForeignKey] {
	out := orderedmap.New[string, *model.ForeignKey]()
	for name, fk := range fks.All() {
		clone := *fk
		if updated, err := rewriteColumnsInConstraintDef(clone.Definition, renames); err == nil {
			clone.Definition = updated
		}
		out.Set(name, &clone)
	}
	return out
}

// rewriteTriggerWhenColumnRefs walks a trigger WHEN expression and rewrites
// each NEW.<col> / OLD.<col> reference whose column name appears in renames.
// NEW and OLD are the only qualifiers a WHEN clause takes, so a two-field
// ColumnRef names a column here, unlike in an index or constraint expression
// where the qualifier could be a table.
func rewriteTriggerWhenColumnRefs(node *pg_query.Node, renames map[string]string) {
	pgast.Walk(node, pgast.WalkOptions{SkipSubqueries: true}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		cr := n.GetColumnRef()
		if cr == nil || len(cr.Fields) != 2 {
			return n
		}
		qual := cr.Fields[0].GetString_()
		if qual == nil || (qual.Sval != "new" && qual.Sval != "old") {
			return n
		}
		if s := cr.Fields[1].GetString_(); s != nil {
			if newName, ok := renames[s.Sval]; ok {
				s.Sval = newName
			}
		}
		return n
	})
}

// rewriteColumnsInTriggerDef returns a new CREATE TRIGGER definition with the
// column names rewritten according to the renames map (old -> new). A trigger
// names columns in the UPDATE OF list and the WHEN expression. The arguments
// passed to the trigger function are left alone, as PostgreSQL does not
// rewrite them on RENAME COLUMN either.
//
// All renames are applied in a single pass, so chained renames (a->b and
// b->c) do not cascade.
func rewriteColumnsInTriggerDef(def string, renames map[string]string) (string, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse trigger definition: %w", err)
	}
	if len(result.Stmts) != 1 {
		return "", fmt.Errorf("unexpected parse result for trigger definition: %s", def)
	}
	ct := result.Stmts[0].Stmt.GetCreateTrigStmt()
	if ct == nil {
		return "", fmt.Errorf("expected CreateTrigStmt in trigger definition: %s", def)
	}
	for _, n := range ct.Columns {
		if s := n.GetString_(); s != nil {
			if newName, ok := renames[s.Sval]; ok {
				s.Sval = newName
			}
		}
	}
	rewriteTriggerWhenColumnRefs(ct.WhenClause, renames)
	return pg_query.Deparse(result)
}

// rewriteColumnRefsInTriggers returns a clone of triggers with each Definition
// updated to reflect the column renames. PostgreSQL applies RENAME COLUMN to a
// trigger on the same table, so without the rewrite the comparison reads the
// rename as a definition change and emits a redundant CREATE OR REPLACE
// TRIGGER. Definitions that fail to parse/deparse are left unchanged, which
// falls back to that redundant statement.
func rewriteColumnRefsInTriggers(triggers *orderedmap.Map[string, *model.Trigger], renames map[string]string) *orderedmap.Map[string, *model.Trigger] {
	out := orderedmap.New[string, *model.Trigger]()
	for name, trg := range triggers.All() {
		clone := *trg
		if updated, err := rewriteColumnsInTriggerDef(clone.Definition, renames); err == nil {
			clone.Definition = updated
		}
		out.Set(name, &clone)
	}
	return out
}

// rewriteColumnsInExpr returns a bare SQL expression with column references
// rewritten according to the renames map (old -> new). Policy USING /
// WITH CHECK clauses and stored-generated column expressions are stored this
// way, and both reference columns of their own table unqualified.
//
// All renames are applied in a single AST walk, so chained renames (a->b and
// b->c) do not cascade.
func rewriteColumnsInExpr(expr string, renames map[string]string) (string, error) {
	result, target, err := pgast.ParseExpr(expr)
	if err != nil {
		return "", fmt.Errorf("failed to parse expression: %w", err)
	}
	rewriteColumnRefsInExpr(target.Val, renames)
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse expression: %w", err)
	}
	return strings.TrimPrefix(sql, "SELECT "), nil
}

// rewriteColumnRefsInPolicies returns a clone of policies with USING and
// WITH CHECK updated to reflect the column renames. PostgreSQL applies
// RENAME COLUMN to a policy on the same table, so without the rewrite the
// comparison reads the rename as an expression change and emits a redundant
// ALTER POLICY. Expressions that fail to parse/deparse are left unchanged,
// which falls back to that redundant statement.
func rewriteColumnRefsInPolicies(policies *orderedmap.Map[string, *model.Policy], renames map[string]string) *orderedmap.Map[string, *model.Policy] {
	out := orderedmap.New[string, *model.Policy]()
	for name, pol := range policies.All() {
		clone := *pol
		clone.Using = rewriteColumnsInExprPtr(pol.Using, renames)
		clone.WithCheck = rewriteColumnsInExprPtr(pol.WithCheck, renames)
		out.Set(name, &clone)
	}
	return out
}

// rewriteColumnsInExprPtr rewrites an optional expression, returning the
// original pointer when there is nothing to rewrite or the rewrite fails.
func rewriteColumnsInExprPtr(expr *string, renames map[string]string) *string {
	if expr == nil {
		return nil
	}
	updated, err := rewriteColumnsInExpr(*expr, renames)
	if err != nil {
		return expr
	}
	return &updated
}

// rewriteColumnRefsInGenerated returns a clone of columns with each generated
// expression updated to reflect the column renames. PostgreSQL applies
// RENAME COLUMN to the expression, so without the rewrite diffColumns reads
// the rename as an expression change, which it reports as an error since a
// generated expression cannot be altered in place. A plain DEFAULT is left
// alone: it cannot reference a column, so a rename never reaches it.
//
// STORED is the only kind the desired side reaches today, since pg_query does
// not parse PostgreSQL 18's VIRTUAL yet. The test is on the column being
// generated at all, so a virtual one needs nothing here once it does.
func rewriteColumnRefsInGenerated(cols *orderedmap.Map[string, *model.Column], renames map[string]string) *orderedmap.Map[string, *model.Column] {
	out := orderedmap.New[string, *model.Column]()
	for name, col := range cols.All() {
		clone := *col
		if col.Generated.IsGeneratedColumn() && col.Default != nil {
			clone.Default = rewriteColumnsInExprPtr(col.Default, renames)
		}
		out.Set(name, &clone)
	}
	return out
}
