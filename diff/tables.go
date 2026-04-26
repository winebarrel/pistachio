package diff

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
	"google.golang.org/protobuf/proto"
)

// TableDiffResult separates FK operations from other statements to allow
// correct ordering: FK drops first, then schema changes, then FK adds last.
type TableDiffResult struct {
	FKDropStmts         []string // FK drops (should run first)
	Stmts               []string // CREATE/ALTER TABLE, columns, constraints, indexes, comments
	FKAddStmts          []string // FK adds and renames (should run last)
	DropStmts           []string // DROP TABLE (separate from Stmts for ordering)
	DisallowedDropStmts []string // DROP TABLE / DROP COLUMN / FK DROP CONSTRAINT suppressed by DropChecker, with "-- skipped: " prefix
	HasConcurrently     bool     // true if any index operation uses CONCURRENTLY
}

func DiffTables(current, desired *orderedmap.Map[string, *model.Table], dc DropChecker) (*TableDiffResult, error) {
	dc = NormalizeDropChecker(dc)
	result := &TableDiffResult{}

	// Detect renames
	renameStmts, current, err := detectTableRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, renameStmts...)

	// New tables
	for k, v := range desired.All() {
		if _, ok := current.GetOk(k); !ok {
			result.Stmts = append(result.Stmts, v.SQL())
			stmts, fkStmts, extraHasConcurrently, err := newTableExtras(v)
			if err != nil {
				return nil, err
			}
			result.Stmts = append(result.Stmts, stmts...)
			result.FKAddStmts = append(result.FKAddStmts, fkStmts...)
			if extraHasConcurrently {
				result.HasConcurrently = true
			}
		}
	}

	// Modified tables
	for k, desiredTable := range desired.All() {
		if currentTable, ok := current.GetOk(k); ok {
			tableResult, err := diffTable(currentTable, desiredTable, dc)
			if err != nil {
				return nil, err
			}
			result.FKDropStmts = append(result.FKDropStmts, tableResult.FKDropStmts...)
			result.Stmts = append(result.Stmts, tableResult.Stmts...)
			result.FKAddStmts = append(result.FKAddStmts, tableResult.FKAddStmts...)
			result.DisallowedDropStmts = append(result.DisallowedDropStmts, tableResult.DisallowedDropStmts...)
			if tableResult.HasConcurrently {
				result.HasConcurrently = true
			}
		}
	}

	// Dropped tables: drop FKs on dropped tables first to avoid dependency errors.
	// When the table-drop policy disallows it, emit the same DROPs as comments
	// (with "-- " prefix) into DisallowedDropStmts for visibility.
	tableAllowed := dc.IsDropAllowed("table")
	for k, tbl := range current.All() {
		if _, ok := desired.GetOk(k); !ok {
			if tableAllowed {
				for name := range tbl.ForeignKeys.Keys() {
					result.FKDropStmts = append(result.FKDropStmts, "ALTER TABLE "+k+" DROP CONSTRAINT "+model.Ident(name)+";")
				}
				result.DropStmts = append(result.DropStmts, "DROP TABLE "+k+";")
			} else {
				for name := range tbl.ForeignKeys.Keys() {
					result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: ALTER TABLE "+k+" DROP CONSTRAINT "+model.Ident(name)+";")
				}
				result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP TABLE "+k+";")
			}
		}
	}

	return result, nil
}

// newTableExtras returns non-FK extras and FK statements separately.
func newTableExtras(t *model.Table) (stmts []string, fkStmts []string, hasConcurrently bool, err error) {
	for _, idx := range t.Indexes.CollectValues() {
		stmt, err := createIndexSQL(idx.Definition, idx.Concurrently)
		if err != nil {
			return nil, nil, false, err
		}
		stmts = append(stmts, stmt)
		if idx.Concurrently {
			hasConcurrently = true
		}
	}
	for _, fk := range t.ForeignKeys.CollectValues() {
		fkStmts = append(fkStmts, fk.SQL())
	}
	if commentSQL := t.CommentSQL(); commentSQL != "" {
		stmts = append(stmts, strings.Split(commentSQL, "\n")...)
	}
	return
}

type tableDiffResult struct {
	FKDropStmts         []string
	Stmts               []string
	FKAddStmts          []string
	DisallowedDropStmts []string
	HasConcurrently     bool
}

func diffTable(current, desired *model.Table, dc DropChecker) (*tableDiffResult, error) {
	dc = NormalizeDropChecker(dc)
	result := &tableDiffResult{}
	fqtn := desired.FQTN()

	// Partition children inherit columns and constraints from the parent,
	// so skip diffing them to avoid false DROP statements.
	if desired.PartitionOf != nil && desired.PartitionBound != nil {
		idxResult, err := diffIndexes(current.Indexes, desired.Indexes)
		if err != nil {
			return nil, err
		}
		result.Stmts = append(result.Stmts, idxResult.Stmts...)
		if idxResult.HasConcurrently {
			result.HasConcurrently = true
		}
		fkDrops, fkAdds, err := diffForeignKeys(fqtn, desired.Schema, current.ForeignKeys, desired.ForeignKeys)
		if err != nil {
			return nil, err
		}
		result.FKDropStmts = append(result.FKDropStmts, fkDrops...)
		result.FKAddStmts = append(result.FKAddStmts, fkAdds...)
		return result, nil
	}

	colStmts, colDisallowed, err := diffColumns(fqtn, current.Columns, desired.Columns, dc)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, colStmts...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, colDisallowed...)

	conStmts, err := diffConstraints(fqtn, current.Constraints, desired.Constraints)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, conStmts...)

	idxResult2, err := diffIndexes(current.Indexes, desired.Indexes)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, idxResult2.Stmts...)
	if idxResult2.HasConcurrently {
		result.HasConcurrently = true
	}

	fkDrops, fkAdds, err := diffForeignKeys(fqtn, desired.Schema, current.ForeignKeys, desired.ForeignKeys)
	if err != nil {
		return nil, err
	}
	result.FKDropStmts = append(result.FKDropStmts, fkDrops...)
	result.FKAddStmts = append(result.FKAddStmts, fkAdds...)

	result.Stmts = append(result.Stmts, diffComments(current, desired)...)

	return result, nil
}

func diffColumns(fqtn string, current, desired *orderedmap.Map[string, *model.Column], dc DropChecker) (stmts []string, disallowed []string, err error) {
	dc = NormalizeDropChecker(dc)

	// Detect renames
	renameStmts, current, err := detectColumnRenames(fqtn, current, desired)
	if err != nil {
		return nil, nil, err
	}
	stmts = append(stmts, renameStmts...)

	// Add new columns
	for name, col := range desired.All() {
		if _, ok := current.GetOk(name); !ok {
			stmts = append(stmts, addColumnSQL(fqtn, col))
		}
	}

	// Alter existing columns
	for name, desiredCol := range desired.All() {
		if currentCol, ok := current.GetOk(name); ok {
			stmts = append(stmts, alterColumnSQL(fqtn, currentCol, desiredCol)...)
		}
	}

	// Drop removed columns. When the column-drop policy disallows it, emit
	// the same DROP as a comment for visibility.
	colAllowed := dc.IsDropAllowed("column")
	for name := range current.Keys() {
		if _, ok := desired.GetOk(name); !ok {
			if colAllowed {
				stmts = append(stmts, "ALTER TABLE "+fqtn+" DROP COLUMN "+model.Ident(name)+";")
			} else {
				disallowed = append(disallowed, "-- skipped: ALTER TABLE "+fqtn+" DROP COLUMN "+model.Ident(name)+";")
			}
		}
	}

	return stmts, disallowed, nil
}

func addColumnSQL(fqtn string, col *model.Column) string {
	sql := "ALTER TABLE " + fqtn + " ADD COLUMN " + model.Ident(col.Name) + " " + col.TypeName

	if col.Collation != nil {
		sql += " COLLATE " + model.Ident(*col.Collation)
	}

	if col.Identity.IsGeneratedAlways() {
		sql += " GENERATED ALWAYS AS IDENTITY"
	} else if col.Identity.IsGeneratedByDefault() {
		sql += " GENERATED BY DEFAULT AS IDENTITY"
	} else if col.Generated.IsStoredGeneratedColumn() {
		if col.Default != nil {
			sql += " GENERATED ALWAYS AS (" + *col.Default + ") STORED"
		}
	} else if col.Default != nil {
		sql += " DEFAULT " + *col.Default
	}

	if col.NotNull && !col.Identity.IsIdentityColumn() {
		sql += " NOT NULL"
	}

	return sql + ";"
}

func alterColumnSQL(fqtn string, current, desired *model.Column) []string {
	var stmts []string
	colIdent := model.Ident(desired.Name)

	// Type change
	if !equalTypeName(current.TypeName, desired.TypeName) {
		sql := "ALTER TABLE " + fqtn + " ALTER COLUMN " + colIdent + " SET DATA TYPE " + desired.TypeName
		if desired.Collation != nil {
			sql += " COLLATE " + model.Ident(*desired.Collation)
		}
		stmts = append(stmts, sql+";")
	}

	// Default change
	if !equalDefault(current.Default, desired.Default) {
		if desired.Default != nil {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" SET DEFAULT "+*desired.Default+";")
		} else {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" DROP DEFAULT;")
		}
	}

	// NOT NULL change
	// Identity columns are implicitly NOT NULL in PostgreSQL; skip NOT NULL diff
	// when either side is an identity column to avoid spurious SET/DROP NOT NULL.
	if current.NotNull != desired.NotNull && !current.Identity.IsIdentityColumn() && !desired.Identity.IsIdentityColumn() {
		if desired.NotNull {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" SET NOT NULL;")
		} else {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" DROP NOT NULL;")
		}
	}

	return stmts
}

// normalizeConstraintDef normalizes a constraint definition by parsing
// and deparsing it through pg_query to eliminate formatting differences.
// It also normalizes AST-level differences introduced by pg_get_constraintdef
// (e.g. explicit ::text casts on string literals, = ANY(ARRAY[...]) vs IN (...)).
func normalizeConstraintDef(def string) (string, error) {
	sql := "ALTER TABLE _t ADD CONSTRAINT _c " + def
	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", err
	}
	as := result.Stmts[0].Stmt.GetAlterTableStmt()
	if as != nil {
		cmd := as.Cmds[0].GetAlterTableCmd()
		if cmd != nil {
			con := cmd.Def.GetConstraint()
			if con != nil {
				con.RawExpr = normalizeCheckExpr(con.RawExpr)
			}
		}
	}
	return pg_query.Deparse(result)
}

// normalizeCheckExpr recursively normalizes a CHECK constraint expression
// so that semantically equivalent definitions compare as equal:
//   - Strips casts to text-like types (text, varchar), which pg_get_constraintdef adds.
//   - Converts = ANY(ARRAY[...]) to IN (...) (PostgreSQL internal representation).
func normalizeCheckExpr(node *pg_query.Node) *pg_query.Node {
	if node == nil {
		return nil
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_TypeCast:
		tc := n.TypeCast
		tc.Arg = normalizeCheckExpr(tc.Arg)
		if isTextLikeTypeName(tc.TypeName) {
			return tc.Arg
		}
	case *pg_query.Node_AExpr:
		ae := n.AExpr
		ae.Lexpr = normalizeCheckExpr(ae.Lexpr)
		ae.Rexpr = normalizeCheckExpr(ae.Rexpr)
		if ae.Kind == pg_query.A_Expr_Kind_AEXPR_OP_ANY {
			if arr := ae.Rexpr.GetAArrayExpr(); arr != nil {
				ae.Kind = pg_query.A_Expr_Kind_AEXPR_IN
				ae.Rexpr = &pg_query.Node{
					Node: &pg_query.Node_List{
						List: &pg_query.List{Items: arr.Elements},
					},
				}
			}
		}
	case *pg_query.Node_BoolExpr:
		for i, arg := range n.BoolExpr.Args {
			n.BoolExpr.Args[i] = normalizeCheckExpr(arg)
		}
	case *pg_query.Node_AArrayExpr:
		for i, elem := range n.AArrayExpr.Elements {
			n.AArrayExpr.Elements[i] = normalizeCheckExpr(elem)
		}
	case *pg_query.Node_List:
		for i, item := range n.List.Items {
			n.List.Items[i] = normalizeCheckExpr(item)
		}
	case *pg_query.Node_FuncCall:
		for i, arg := range n.FuncCall.Args {
			n.FuncCall.Args[i] = normalizeCheckExpr(arg)
		}
	case *pg_query.Node_NullTest:
		n.NullTest.Arg = normalizeCheckExpr(n.NullTest.Arg)
	case *pg_query.Node_CoalesceExpr:
		for i, arg := range n.CoalesceExpr.Args {
			n.CoalesceExpr.Args[i] = normalizeCheckExpr(arg)
		}
	case *pg_query.Node_CaseExpr:
		n.CaseExpr.Arg = normalizeCheckExpr(n.CaseExpr.Arg)
		n.CaseExpr.Defresult = normalizeCheckExpr(n.CaseExpr.Defresult)
		for _, when := range n.CaseExpr.Args {
			if w := when.GetCaseWhen(); w != nil {
				w.Expr = normalizeCheckExpr(w.Expr)
				w.Result = normalizeCheckExpr(w.Result)
			}
		}
	}
	return node
}

// isTextLikeTypeName returns true if the TypeName refers to a text-like type
// (text, text[], varchar, character varying, or their array forms).
// pg_get_constraintdef adds these casts on expressions that are already
// text-typed, so stripping them avoids false diffs.
func isTextLikeTypeName(tn *pg_query.TypeName) bool {
	if tn == nil {
		return false
	}
	for _, n := range tn.Names {
		if s := n.GetString_(); s != nil {
			switch s.Sval {
			case "text", "varchar":
				return true
			}
		}
	}
	return false
}

// equalConstraintDef compares two constraint definitions by normalizing
// them through pg_query parse/deparse, so that formatting differences
// (e.g. extra parentheses, spacing) do not cause false diffs.
func equalConstraintDef(a, b string) bool {
	if a == b {
		return true
	}
	normA, errA := normalizeConstraintDef(a)
	normB, errB := normalizeConstraintDef(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return normA == normB
}

func diffConstraints(fqtn string, current, desired *orderedmap.Map[string, *model.Constraint]) ([]string, error) {
	var stmts []string

	// Detect renames
	renameStmts, current, renamedFrom, err := detectConstraintRenames(fqtn, current, desired)
	if err != nil {
		return nil, err
	}

	// Determine which renamed constraints need recreation instead of just rename
	needsRecreation := map[string]bool{}
	for name, currentCon := range current.All() {
		desiredCon, ok := desired.GetOk(name)
		if !ok {
			continue
		}
		if !equalConstraintDef(currentCon.Definition, desiredCon.Definition) || currentCon.Validated != desiredCon.Validated {
			if equalConstraintDef(currentCon.Definition, desiredCon.Definition) && !currentCon.Validated && desiredCon.Validated {
				continue
			}
			if _, renamed := renamedFrom[name]; renamed {
				needsRecreation[name] = true
			}
		}
	}

	// Only emit rename statements for constraints that don't need recreation
	for _, stmt := range renameStmts {
		skip := false
		for name := range needsRecreation {
			oldName := renamedFrom[name]
			if strings.Contains(stmt, model.Ident(oldName)+" TO "+model.Ident(name)) {
				skip = true
				break
			}
		}
		if !skip {
			stmts = append(stmts, stmt)
		}
	}

	// Drop removed or changed constraints
	for name, currentCon := range current.All() {
		desiredCon, ok := desired.GetOk(name)
		if !ok || !equalConstraintDef(currentCon.Definition, desiredCon.Definition) || currentCon.Validated != desiredCon.Validated {
			// NOT VALID → validated with same definition can use VALIDATE CONSTRAINT
			if ok && equalConstraintDef(currentCon.Definition, desiredCon.Definition) && !currentCon.Validated && desiredCon.Validated {
				continue
			}
			dropName := name
			if oldName, renamed := renamedFrom[name]; renamed {
				dropName = oldName
			}
			stmts = append(stmts, "ALTER TABLE "+fqtn+" DROP CONSTRAINT "+model.Ident(dropName)+";")
		}
	}

	// Add new or changed constraints
	for name, desiredCon := range desired.All() {
		currentCon, ok := current.GetOk(name)
		if !ok || !equalConstraintDef(currentCon.Definition, desiredCon.Definition) || currentCon.Validated != desiredCon.Validated {
			// NOT VALID → validated with same definition can use VALIDATE CONSTRAINT
			if ok && equalConstraintDef(currentCon.Definition, desiredCon.Definition) && !currentCon.Validated && desiredCon.Validated {
				stmts = append(stmts, "ALTER TABLE "+fqtn+" VALIDATE CONSTRAINT "+model.Ident(name)+";")
				continue
			}
			sql := "ALTER TABLE " + fqtn + " ADD CONSTRAINT " + model.Ident(name) + " " + desiredCon.Definition
			if !desiredCon.Validated {
				sql += " NOT VALID"
			}
			stmts = append(stmts, sql+";")
		}
	}

	return stmts, nil
}

type diffIndexesResult struct {
	Stmts           []string
	HasConcurrently bool
}

func diffIndexes(current, desired *orderedmap.Map[string, *model.Index]) (*diffIndexesResult, error) {
	result := &diffIndexesResult{}

	// Detect renames
	renameStmts, current, err := detectIndexRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, renameStmts...)

	// Drop removed or changed indexes
	for name, currentIdx := range current.All() {
		desiredIdx, ok := desired.GetOk(name)
		if !ok || !equalIndexDef(currentIdx.Definition, desiredIdx.Definition) {
			// Use CONCURRENTLY only when the desired index (when it exists and
			// is being changed) has the per-index directive. Pure drops (index
			// removed from desired) never use CONCURRENTLY because catalog-derived
			// indexes don't carry the directive.
			useConcurrently := false
			if ok {
				useConcurrently = desiredIdx.Concurrently
			}
			stmt, err := dropIndexSQL(currentIdx.Schema, name, useConcurrently)
			if err != nil {
				return nil, fmt.Errorf("drop index %s: %w", model.Ident(currentIdx.Schema, name), err)
			}
			result.Stmts = append(result.Stmts, stmt)
			if useConcurrently {
				result.HasConcurrently = true
			}
		}
	}

	// Add new or changed indexes
	for name, desiredIdx := range desired.All() {
		currentIdx, ok := current.GetOk(name)
		if !ok || !equalIndexDef(currentIdx.Definition, desiredIdx.Definition) {
			stmt, err := createIndexSQL(desiredIdx.Definition, desiredIdx.Concurrently)
			if err != nil {
				return nil, fmt.Errorf("create index %s: %w", model.Ident(desiredIdx.Schema, name), err)
			}
			result.Stmts = append(result.Stmts, stmt)
			if desiredIdx.Concurrently {
				result.HasConcurrently = true
			}
		}
	}

	return result, nil
}

// dropIndexSQL builds a DROP INDEX statement, optionally with CONCURRENTLY
// set via the pg_query AST.
func dropIndexSQL(schema, name string, concurrently bool) (string, error) {
	base := "DROP INDEX " + model.Ident(schema, name)
	if !concurrently {
		return base + ";", nil
	}

	result, err := pg_query.Parse(base)
	if err != nil {
		return "", fmt.Errorf("failed to parse drop index statement: %w", err)
	}

	ds := result.Stmts[0].Stmt.GetDropStmt()
	if ds == nil {
		return "", fmt.Errorf("expected DropStmt for: %s", base)
	}

	ds.Concurrent = true

	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse drop index statement: %w", err)
	}

	return deparsed + ";", nil
}

// createIndexSQL returns a CREATE INDEX statement with the CONCURRENTLY flag
// set via the pg_query AST when concurrently is true.
func createIndexSQL(def string, concurrently bool) (string, error) {
	if !concurrently {
		return def + ";", nil
	}

	result, err := pg_query.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse index definition: %w", err)
	}

	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return "", fmt.Errorf("expected IndexStmt for: %s", def)
	}

	is.Concurrent = true

	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse index definition: %w", err)
	}

	return deparsed + ";", nil
}

// diffForeignKeys returns (dropStmts, addStmts, error).
// Rename statements are included in addStmts (they depend on table renames being done first).
func diffForeignKeys(fqtn, schema string, current, desired *orderedmap.Map[string, *model.ForeignKey]) ([]string, []string, error) {
	var dropStmts, addStmts []string

	// Detect renames (renames go into addStmts since they may depend on table renames)
	renameStmts, current, renamedFrom, err := detectForeignKeyRenames(fqtn, current, desired)
	if err != nil {
		return nil, nil, err
	}

	// Determine which renamed FKs need recreation (drop+add) instead of just rename
	needsRecreation := map[string]bool{}
	for name := range current.All() {
		desiredFk, ok := desired.GetOk(name)
		if !ok {
			continue
		}
		currentFk := current.Get(name)
		if !equalFKDef(currentFk.Definition, desiredFk.Definition, schema) || currentFk.Validated != desiredFk.Validated {
			// NOT VALID → validated with same definition can use VALIDATE CONSTRAINT (no recreation needed)
			if equalFKDef(currentFk.Definition, desiredFk.Definition, schema) && !currentFk.Validated && desiredFk.Validated {
				continue
			}
			if _, renamed := renamedFrom[name]; renamed {
				needsRecreation[name] = true
			}
		}
	}

	// Only emit rename statements for FKs that don't need recreation
	for _, stmt := range renameStmts {
		skip := false
		for name := range needsRecreation {
			oldName := renamedFrom[name]
			if strings.Contains(stmt, model.Ident(oldName)+" TO "+model.Ident(name)) {
				skip = true
				break
			}
		}
		if !skip {
			addStmts = append(addStmts, stmt)
		}
	}

	// Drop removed or changed FKs
	for name := range current.All() {
		desiredFk, ok := desired.GetOk(name)
		currentFk := current.Get(name)
		if !ok || !equalFKDef(currentFk.Definition, desiredFk.Definition, schema) || currentFk.Validated != desiredFk.Validated {
			// NOT VALID → validated with same definition can use VALIDATE CONSTRAINT
			if ok && equalFKDef(currentFk.Definition, desiredFk.Definition, schema) && !currentFk.Validated && desiredFk.Validated {
				continue
			}
			dropName := name
			if oldName, renamed := renamedFrom[name]; renamed {
				dropName = oldName
			}
			dropStmts = append(dropStmts, "ALTER TABLE "+fqtn+" DROP CONSTRAINT "+model.Ident(dropName)+";")
		}
	}

	// Add new or changed FKs
	for name, desiredFk := range desired.All() {
		currentFk, ok := current.GetOk(name)
		if !ok || !equalFKDef(currentFk.Definition, desiredFk.Definition, schema) || currentFk.Validated != desiredFk.Validated {
			// NOT VALID → validated with same definition can use VALIDATE CONSTRAINT
			if ok && equalFKDef(currentFk.Definition, desiredFk.Definition, schema) && !currentFk.Validated && desiredFk.Validated {
				addStmts = append(addStmts, "ALTER TABLE "+fqtn+" VALIDATE CONSTRAINT "+model.Ident(name)+";")
				continue
			}
			addStmts = append(addStmts, desiredFk.SQL())
		}
	}

	return dropStmts, addStmts, nil
}

func diffComments(current, desired *model.Table) []string {
	var stmts []string
	fqtn := desired.FQTN()

	// Table comment
	if !equalPtr(current.Comment, desired.Comment) {
		if desired.Comment != nil {
			stmts = append(stmts, "COMMENT ON TABLE "+fqtn+" IS "+model.QuoteLiteral(*desired.Comment)+";")
		} else {
			stmts = append(stmts, "COMMENT ON TABLE "+fqtn+" IS NULL;")
		}
	}

	// Column comments
	for name, desiredCol := range desired.Columns.All() {
		var currentComment *string
		if currentCol, ok := current.Columns.GetOk(name); ok {
			currentComment = currentCol.Comment
		}
		if !equalPtr(currentComment, desiredCol.Comment) {
			colIdent := fqtn + "." + model.Ident(name)
			if desiredCol.Comment != nil {
				stmts = append(stmts, "COMMENT ON COLUMN "+colIdent+" IS "+model.QuoteLiteral(*desiredCol.Comment)+";")
			} else {
				stmts = append(stmts, "COMMENT ON COLUMN "+colIdent+" IS NULL;")
			}
		}
	}

	return stmts
}

func equalPtr[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// normalizeIndexDef normalizes an index definition by parsing it,
// clearing the schema, and deparsing it back to a canonical string.
// It also canonicalises the default sort order so that explicit ASC
// (the default) matches an omitted order, and explicit NULLS LAST
// for ASC / NULLS FIRST for DESC matches an omitted nulls clause.
func normalizeIndexDef(def string) (string, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return "", err
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return "", fmt.Errorf("unexpected parse result for index definition: %s", def)
	}
	if is.Relation != nil {
		is.Relation.Schemaname = ""
	}
	for _, p := range is.IndexParams {
		ie := p.GetIndexElem()
		if ie == nil {
			continue
		}
		// Canonicalise sort order: SORTBY_ASC and SORTBY_DEFAULT are equivalent.
		if ie.Ordering == pg_query.SortByDir_SORTBY_ASC {
			ie.Ordering = pg_query.SortByDir_SORTBY_DEFAULT
		}
		// Canonicalise nulls order: NULLS LAST is the default for ASC/DEFAULT,
		// NULLS FIRST is the default for DESC.
		switch ie.Ordering {
		case pg_query.SortByDir_SORTBY_DEFAULT:
			if ie.NullsOrdering == pg_query.SortByNulls_SORTBY_NULLS_LAST {
				ie.NullsOrdering = pg_query.SortByNulls_SORTBY_NULLS_DEFAULT
			}
		case pg_query.SortByDir_SORTBY_DESC:
			if ie.NullsOrdering == pg_query.SortByNulls_SORTBY_NULLS_FIRST {
				ie.NullsOrdering = pg_query.SortByNulls_SORTBY_NULLS_DEFAULT
			}
		}
	}
	return pg_query.Deparse(result)
}

// equalIndexDef compares two index definitions by normalizing them
// through pg_query parse/deparse, so that schema qualification and
// formatting differences do not cause false diffs.
func equalIndexDef(a, b string) bool {
	normA, errA := normalizeIndexDef(a)
	normB, errB := normalizeIndexDef(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return normA == normB
}

// parseFKDef parses a FK constraint definition string into a pg_query Constraint node.
func parseFKDef(def string) (*pg_query.Constraint, error) {
	sql := "ALTER TABLE _t ADD CONSTRAINT _c " + def
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	as := result.Stmts[0].Stmt.GetAlterTableStmt()
	if as == nil {
		return nil, fmt.Errorf("unexpected parse result for FK definition: %s", def)
	}
	cmd := as.Cmds[0].GetAlterTableCmd()
	if cmd == nil {
		return nil, fmt.Errorf("unexpected parse result for FK definition: %s", def)
	}
	con := cmd.Def.GetConstraint()
	if con == nil {
		return nil, fmt.Errorf("unexpected parse result for FK definition: %s", def)
	}
	return con, nil
}

// normalizeFKSchema normalizes the referenced table's schema name in a FK
// constraint node so that an empty schema (implicit via search_path) is
// treated the same as the explicit schema of the owning table.
func normalizeFKSchema(con *pg_query.Constraint, schema string) {
	if con.Pktable != nil && con.Pktable.Schemaname == "" {
		con.Pktable.Schemaname = schema
	}
}

// equalFKDef compares two FK constraint definitions by their parse trees,
// so that formatting differences do not cause false diffs.
// schema is the schema of the table that owns the FK constraint and is used
// to fill in an implicit (empty) schema on the referenced table.
func equalFKDef(a, b, schema string) bool {
	nodeA, errA := parseFKDef(a)
	nodeB, errB := parseFKDef(b)
	if errA != nil || errB != nil {
		return a == b
	}
	normalizeFKSchema(nodeA, schema)
	normalizeFKSchema(nodeB, schema)
	return proto.Equal(nodeA, nodeB)
}

// parseDefault parses a default expression string into a pg_query Node,
// stripping a top-level type cast added by pg_get_expr (e.g. 'hello'::text → 'hello').
func parseDefault(expr string) (*pg_query.Node, error) {
	result, err := pg_query.Parse("SELECT " + expr)
	if err != nil {
		return nil, err
	}
	stmt := result.Stmts[0].Stmt.GetSelectStmt()
	if stmt == nil || len(stmt.TargetList) == 0 {
		return nil, fmt.Errorf("unexpected parse result for default: %s", expr)
	}
	target := stmt.TargetList[0].GetResTarget()
	if target == nil {
		return nil, fmt.Errorf("unexpected parse result for default: %s", expr)
	}
	node := target.Val
	// Strip top-level type cast added by pg_get_expr
	if node.GetTypeCast() != nil {
		node = node.GetTypeCast().Arg
	}
	return node, nil
}

// serialBaseTypes maps serial type names to their base types.
var serialBaseTypes = map[string]string{
	"serial":      "integer",
	"bigserial":   "bigint",
	"smallserial": "smallint",
}

// equalTypeName compares two type names, treating serial types as equal to their base types.
func equalTypeName(a, b string) bool {
	if a == b {
		return true
	}
	normalize := func(t string) string {
		if base, ok := serialBaseTypes[t]; ok {
			return base
		}
		return t
	}
	return normalize(a) == normalize(b)
}

// equalDefault compares two default expressions by their parse trees.
func equalDefault(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	nodeA, errA := parseDefault(*a)
	nodeB, errB := parseDefault(*b)
	if errA != nil || errB != nil {
		return *a == *b
	}
	return proto.Equal(nodeA, nodeB)
}
