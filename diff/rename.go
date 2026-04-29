package diff

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
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
func detectViewRenames(current, desired *orderedmap.Map[string, *model.View]) ([]string, *orderedmap.Map[string, *model.View], error) {
	var stmts []string
	adjusted := cloneMap(current)

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
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldView.Schema != desiredView.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		if oldView.Materialized != desiredView.Materialized {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: view type mismatch (cannot rename between VIEW and MATERIALIZED VIEW)", oldKey, newKey)
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
	}

	return stmts, adjusted, nil
}

// detectColumnRenames finds desired columns with RenameFrom that match a current column.
//
// NOTE: After a column rename, constraint/index/FK definitions that reference the
// old column name are not updated in the adjusted current state. PostgreSQL
// automatically updates these on RENAME COLUMN, so running plan/apply a second
// time will produce a clean diff. A single plan may emit redundant DROP/ADD for
// dependent constraints or indexes.
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

// detectConstraintRenames finds desired constraints with RenameFrom that match a current constraint.
// Returns (renameStmts, adjustedCurrent, renamedFrom map[newName]oldName, error).
func detectConstraintRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Constraint]) ([]string, *orderedmap.Map[string, *model.Constraint], map[string]string, error) {
	var stmts []string
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
			return nil, nil, nil, fmt.Errorf("rename source constraint %s not found in %s", model.Ident(oldName), fqtn)
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, nil, fmt.Errorf("cannot rename constraint %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
			}
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME CONSTRAINT "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")
		renamedFrom[newName] = oldName

		adjusted.Delete(oldName)
		renamed := *oldCon
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, renamedFrom, nil
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
func detectForeignKeyRenames(fqtn string, current, desired *orderedmap.Map[string, *model.ForeignKey]) ([]string, *orderedmap.Map[string, *model.ForeignKey], map[string]string, error) {
	var stmts []string
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
			return nil, nil, nil, fmt.Errorf("rename source foreign key %s not found in %s", model.Ident(oldName), fqtn)
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, nil, fmt.Errorf("cannot rename foreign key %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
			}
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME CONSTRAINT "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")
		renamedFrom[newName] = oldName

		adjusted.Delete(oldName)
		renamed := *oldFK
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

// collectColumnRenames returns a map of old column name → new column name
// for desired columns annotated with -- pist:renamed-from.
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
// original-name set in a single pass, so chained renames (a→b alongside
// b→c) cannot cascade. Qualified references (table.col) are left untouched
// because the renamed column belongs to a single table and the diff context
// is local.
func rewriteColumnRefsInExpr(node *pg_query.Node, renames map[string]string) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_ColumnRef:
		if len(n.ColumnRef.Fields) == 1 {
			if s := n.ColumnRef.Fields[0].GetString_(); s != nil {
				if newName, ok := renames[s.Sval]; ok {
					s.Sval = newName
				}
			}
		}
	case *pg_query.Node_AExpr:
		rewriteColumnRefsInExpr(n.AExpr.Lexpr, renames)
		rewriteColumnRefsInExpr(n.AExpr.Rexpr, renames)
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			rewriteColumnRefsInExpr(arg, renames)
		}
	case *pg_query.Node_TypeCast:
		rewriteColumnRefsInExpr(n.TypeCast.Arg, renames)
	case *pg_query.Node_FuncCall:
		for _, arg := range n.FuncCall.Args {
			rewriteColumnRefsInExpr(arg, renames)
		}
	case *pg_query.Node_NullTest:
		rewriteColumnRefsInExpr(n.NullTest.Arg, renames)
	case *pg_query.Node_AArrayExpr:
		for _, elem := range n.AArrayExpr.Elements {
			rewriteColumnRefsInExpr(elem, renames)
		}
	case *pg_query.Node_List:
		for _, item := range n.List.Items {
			rewriteColumnRefsInExpr(item, renames)
		}
	case *pg_query.Node_CoalesceExpr:
		for _, arg := range n.CoalesceExpr.Args {
			rewriteColumnRefsInExpr(arg, renames)
		}
	case *pg_query.Node_CaseExpr:
		rewriteColumnRefsInExpr(n.CaseExpr.Arg, renames)
		rewriteColumnRefsInExpr(n.CaseExpr.Defresult, renames)
		for _, when := range n.CaseExpr.Args {
			if w := when.GetCaseWhen(); w != nil {
				rewriteColumnRefsInExpr(w.Expr, renames)
				rewriteColumnRefsInExpr(w.Result, renames)
			}
		}
	}
}

// rewriteColumnsInIndexDef returns a new index definition with column
// references rewritten according to the renames map (old → new). Returns an
// error (and an empty string) if pg_query parse/deparse fails; callers fall
// back to the original definition.
//
// All renames are applied in a single AST walk, so chained renames (a→b and
// b→c) do not cascade — each original column name is matched once against
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

const constraintDefWrapPrefix = "ALTER TABLE _t ADD CONSTRAINT _c "

// rewriteColumnsInConstraintDef returns a new constraint definition fragment
// (e.g. "PRIMARY KEY (id)", "CHECK ((x > 0))", "FOREIGN KEY (a) REFERENCES t(b)")
// with column references rewritten according to the renames map (old → new).
// PkAttrs (referenced columns on the foreign side) are intentionally NOT
// rewritten because they refer to a different table.
//
// All renames are applied in a single AST walk so chained renames cannot
// cascade.
func rewriteColumnsInConstraintDef(def string, renames map[string]string) (string, error) {
	sql := constraintDefWrapPrefix + def
	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", fmt.Errorf("failed to parse constraint definition: %w", err)
	}
	if len(result.Stmts) == 0 {
		return "", fmt.Errorf("empty constraint definition")
	}
	as := result.Stmts[0].Stmt.GetAlterTableStmt()
	if as == nil || len(as.Cmds) == 0 {
		return "", fmt.Errorf("unexpected parse result for constraint definition")
	}
	cmd := as.Cmds[0].GetAlterTableCmd()
	if cmd == nil || cmd.Def == nil {
		return "", fmt.Errorf("unexpected parse result for constraint definition")
	}
	con := cmd.Def.GetConstraint()
	if con == nil {
		return "", fmt.Errorf("unexpected parse result for constraint definition")
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

	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse constraint definition: %w", err)
	}
	deparsed = strings.TrimSuffix(deparsed, ";")
	if !strings.HasPrefix(deparsed, constraintDefWrapPrefix) {
		return "", fmt.Errorf("unexpected deparsed form: %s", deparsed)
	}
	return strings.TrimPrefix(deparsed, constraintDefWrapPrefix), nil
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
