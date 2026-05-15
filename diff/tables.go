package diff

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/internal/pgast"
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
	DisallowedDropStmts []string // DROP TABLE / DROP COLUMN / DROP CONSTRAINT (incl. FK) / DROP INDEX suppressed by DropChecker, with "-- skipped: " prefix
	HasConcurrently     bool     // true if any index operation uses CONCURRENTLY
}

func DiffTables(current, desired *orderedmap.Map[string, *model.Table], dc DropChecker) (*TableDiffResult, error) {
	dc = normalizeDropChecker(dc)
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
	// (with "-- skipped: " prefix) into DisallowedDropStmts for visibility.
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
	if rlsSQL := t.RLSSQL(); rlsSQL != "" {
		stmts = append(stmts, strings.Split(rlsSQL, "\n")...)
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
	dc = normalizeDropChecker(dc)
	result := &tableDiffResult{}
	fqtn := desired.FQTN()

	// Partition children inherit columns and constraints from the parent,
	// so skip diffing them to avoid false DROP statements. RLS flags and
	// policies are owned per-relation (children do not auto-inherit them),
	// so they're still diffed here, mirroring how indexes and FKs work.
	if desired.PartitionOf != nil && desired.PartitionBound != nil {
		idxResult, err := diffIndexes(current.Indexes, desired.Indexes, dc)
		if err != nil {
			return nil, err
		}
		result.Stmts = append(result.Stmts, idxResult.Stmts...)
		result.DisallowedDropStmts = append(result.DisallowedDropStmts, idxResult.DisallowedDropStmts...)
		if idxResult.HasConcurrently {
			result.HasConcurrently = true
		}
		fkDrops, fkAdds, fkDisallowed, err := diffForeignKeys(fqtn, desired.Schema, current.ForeignKeys, desired.ForeignKeys, dc)
		if err != nil {
			return nil, err
		}
		result.FKDropStmts = append(result.FKDropStmts, fkDrops...)
		result.FKAddStmts = append(result.FKAddStmts, fkAdds...)
		result.DisallowedDropStmts = append(result.DisallowedDropStmts, fkDisallowed...)

		result.Stmts = append(result.Stmts, diffRLS(fqtn, current, desired)...)
		polStmts, polDisallowed, err := diffPolicies(fqtn, current.Policies, desired.Policies, dc)
		if err != nil {
			return nil, err
		}
		result.Stmts = append(result.Stmts, polStmts...)
		result.DisallowedDropStmts = append(result.DisallowedDropStmts, polDisallowed...)

		return result, nil
	}

	colStmts, colDisallowed, err := diffColumns(fqtn, current.Columns, desired.Columns, dc)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, colStmts...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, colDisallowed...)

	// Rewrite column references in dependent objects so that subsequent diffs
	// don't see the renamed column as a definition change. PostgreSQL applies
	// RENAME COLUMN transparently to indexes/constraints/FKs on the same table.
	// Also rekey current.Columns so diffComments looks up the right entry by
	// the new column name.
	if columnRenames := collectColumnRenames(desired.Columns); len(columnRenames) > 0 {
		clone := *current
		clone.Indexes = rewriteColumnRefsInIndexes(current.Indexes, columnRenames)
		clone.Constraints = rewriteColumnRefsInConstraints(current.Constraints, columnRenames)
		clone.ForeignKeys = rewriteColumnRefsInForeignKeys(current.ForeignKeys, columnRenames)
		clone.Columns = renameColumnKeys(current.Columns, columnRenames)
		current = &clone
	}

	conStmts, conDisallowed, err := diffConstraints(fqtn, current.Constraints, desired.Constraints, dc)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, conStmts...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, conDisallowed...)

	idxResult2, err := diffIndexes(current.Indexes, desired.Indexes, dc)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, idxResult2.Stmts...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, idxResult2.DisallowedDropStmts...)
	if idxResult2.HasConcurrently {
		result.HasConcurrently = true
	}

	fkDrops, fkAdds, fkDisallowed, err := diffForeignKeys(fqtn, desired.Schema, current.ForeignKeys, desired.ForeignKeys, dc)
	if err != nil {
		return nil, err
	}
	result.FKDropStmts = append(result.FKDropStmts, fkDrops...)
	result.FKAddStmts = append(result.FKAddStmts, fkAdds...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, fkDisallowed...)

	// RLS toggles run before CREATE POLICY so a newly enabled table has its
	// flag set when policies attach. Disabling RLS likewise comes before any
	// policy DROP that the user may have stacked alongside.
	result.Stmts = append(result.Stmts, diffRLS(fqtn, current, desired)...)

	polStmts, polDisallowed, err := diffPolicies(fqtn, current.Policies, desired.Policies, dc)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, polStmts...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, polDisallowed...)

	result.Stmts = append(result.Stmts, diffComments(current, desired)...)

	return result, nil
}

func diffColumns(fqtn string, current, desired *orderedmap.Map[string, *model.Column], dc DropChecker) (stmts []string, disallowed []string, err error) {
	dc = normalizeDropChecker(dc)

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
			cur := currentCol.Generated.IsStoredGeneratedColumn()
			des := desiredCol.Generated.IsStoredGeneratedColumn()
			if cur != des {
				return nil, nil, fmt.Errorf("column %s.%s: cannot toggle GENERATED — DROP COLUMN + ADD COLUMN is required",
					fqtn, model.Ident(name))
			}
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
		if col.NotNullName != nil {
			sql += " CONSTRAINT " + model.Ident(*col.NotNullName)
		}
		sql += " NOT NULL"
	}

	return sql + ";"
}

func alterColumnSQL(fqtn string, current, desired *model.Column) []string {
	var stmts []string
	colIdent := model.Ident(desired.Name)

	// Type or collation change. Collation is altered via SET DATA TYPE
	// because PostgreSQL has no separate "set collation" syntax — re-issuing
	// SET DATA TYPE without COLLATE reverts to the type's default collation.
	if !equalTypeName(current.TypeName, desired.TypeName) || !equalPtr(current.Collation, desired.Collation) {
		sql := "ALTER TABLE " + fqtn + " ALTER COLUMN " + colIdent + " SET DATA TYPE " + desired.TypeName
		if desired.Collation != nil {
			sql += " COLLATE " + model.Ident(*desired.Collation)
		}
		stmts = append(stmts, sql+";")
	}

	curIsIdent := current.Identity.IsIdentityColumn()
	desIsIdent := desired.Identity.IsIdentityColumn()

	// Identity transitions. Handle before default/NOT NULL diff because adding
	// IDENTITY requires the column to have no default and NOT NULL set.
	switch {
	case !curIsIdent && desIsIdent:
		// none → identity: clear default and ensure NOT NULL, then ADD IDENTITY.
		// Catalog reports Default=nil for serial/bigserial/smallserial columns
		// even though they carry a hidden nextval() default that would block
		// ADD IDENTITY, so detect those by TypeName as well.
		if current.Default != nil || isSerialType(current.TypeName) {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" DROP DEFAULT;")
		}
		if !current.NotNull {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" SET NOT NULL;")
		}
		stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" ADD GENERATED "+identityKind(desired.Identity)+" AS IDENTITY;")
	case curIsIdent && !desIsIdent:
		// identity → none: DROP IDENTITY. The column stays NOT NULL afterwards;
		// the NOT NULL diff below will emit DROP NOT NULL if desired is nullable.
		stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" DROP IDENTITY IF EXISTS;")
	case curIsIdent && desIsIdent && current.Identity != desired.Identity:
		// always ↔ by default
		stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" SET GENERATED "+identityKind(desired.Identity)+";")
	}

	// Default change. For non-generated columns, emit SET DEFAULT / DROP
	// DEFAULT as usual. For generated columns, the "default" stores the
	// generated expression and cannot be altered via SET DEFAULT — caller
	// (diffColumns) already errors on a Generated state toggle, and
	// expression-only changes within a generated column are not surfaced
	// here because catalog renders pg_get_expr-added type casts that don't
	// reliably compare with the desired-side raw expression. See TODO.md.
	// Skip when desired is identity: we explicitly DROP DEFAULT above as part
	// of the ADD IDENTITY transition.
	if !current.Generated.IsStoredGeneratedColumn() && !desired.Generated.IsStoredGeneratedColumn() && !desIsIdent {
		if !equalDefault(current.Default, desired.Default) {
			if desired.Default != nil {
				stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" SET DEFAULT "+*desired.Default+";")
			} else {
				stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" DROP DEFAULT;")
			}
		}
	}

	// NOT NULL change. Identity columns are implicitly NOT NULL, so skip when
	// the desired side is identity (the ADD IDENTITY path sets NOT NULL above).
	if current.NotNull != desired.NotNull && !desIsIdent {
		if desired.NotNull {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" SET NOT NULL;")
		} else {
			stmts = append(stmts, "ALTER TABLE "+fqtn+" ALTER COLUMN "+colIdent+" DROP NOT NULL;")
		}
	} else if current.NotNull && desired.NotNull && !desIsIdent &&
		current.NotNullName != nil && desired.NotNullName != nil &&
		*current.NotNullName != *desired.NotNullName {
		// Both sides NOT NULL with explicit but different names — rename in place.
		// Adding/removing a name on an existing NOT NULL needs PG18's standalone
		// ALTER ... ADD CONSTRAINT NOT NULL syntax (not yet supported by
		// pg_query_go); left out of v1. Identity columns are skipped to match
		// Table.SQL / addColumnSQL, which intentionally do not render
		// "CONSTRAINT <name> NOT NULL" on identity columns — emitting a rename
		// here would surface a name the dumper hides.
		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME CONSTRAINT "+model.Ident(*current.NotNullName)+" TO "+model.Ident(*desired.NotNullName)+";")
	}

	return stmts
}

func identityKind(id model.ColumnIdentity) string {
	if id.IsGeneratedAlways() {
		return "ALWAYS"
	}
	return "BY DEFAULT"
}

// isSerialType reports whether the type name corresponds to a Postgres serial
// pseudo-type. catalog/columns.go renders these explicitly as "serial" /
// "bigserial" / "smallserial" while leaving the column's Default field nil,
// so callers that need to drop the underlying nextval() default before an
// ALTER must check the type name to detect this case.
func isSerialType(typeName string) bool {
	_, ok := serialBaseTypes[typeName]
	return ok
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

// isNumericTypeName returns true if the TypeName refers to a built-in
// Postgres numeric scalar. pg_query canonicalises integer / bigint /
// smallint to int4 / int8 / int2 and double precision / real to float8 /
// float4 respectively, so both forms are accepted, but only the unqualified
// keyword (`integer`) and the `pg_catalog`-qualified canonical form
// (`pg_catalog.int4`) match — a user-defined type that happens to be named
// e.g. `myapp.int4` is not considered numeric, so a cast onto it can't be
// silently stripped or coerced.
func isNumericTypeName(tn *pg_query.TypeName) bool {
	if tn == nil || len(tn.Names) == 0 {
		return false
	}
	var name string
	switch len(tn.Names) {
	case 1:
		s := tn.Names[0].GetString_()
		if s == nil {
			return false
		}
		name = s.Sval
	case 2:
		schema := tn.Names[0].GetString_()
		s := tn.Names[1].GetString_()
		if schema == nil || s == nil || schema.Sval != "pg_catalog" {
			return false
		}
		name = s.Sval
	default:
		return false
	}
	switch name {
	case "int2", "int4", "int8", "smallint", "integer", "bigint",
		"numeric", "decimal", "float4", "float8", "real":
		return true
	}
	return false
}

// desiredIsNumericAConst reports whether a desired-side node is a bare
// numeric A_Const (Ival or Fval). Used to gate the Sval→numeric coercion
// in the asymmetric cast-strip path so that desired-side quoted literals
// (`'0'`, `'1'`) keep matching the cast-stripped Sval as before.
func desiredIsNumericAConst(n *pg_query.Node) bool {
	if n == nil {
		return false
	}
	ac := n.GetAConst()
	if ac == nil {
		return false
	}
	return ac.GetIval() != nil || ac.GetFval() != nil
}

// numericAConstFromString converts a numeric-valued string into an A_Const
// node. pg_get_constraintdef emits negative numeric literals as
// `'-40'::integer` (to dodge the unary-minus precedence trap that `-40::int`
// would hit), while user-written `-40` parses to A_Const{Ival -40}. After
// alignCurrentCasts strips the TypeCast wrapper, the bare A_Const{Sval "-40"}
// that survives would still deparse to `'-40'` and diff against `-40`. This
// helper produces the canonical Ival / Fval form so the two compare equal.
// Returns nil for non-numeric strings; callers leave the original node alone
// in that case.
func numericAConstFromString(s string) *pg_query.Node {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil && n >= -2147483648 && n <= 2147483647 {
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{
						Ival: &pg_query.Integer{Ival: int32(n)},
					},
				},
			},
		}
	}
	// ParseFloat returns ErrRange (with ±Inf as value) for syntactically
	// valid numerics that exceed float64 — but PG's `numeric` carries those
	// just fine and pg_query's Fval is just a string, so accept ErrRange as
	// "lexically a number, store the original string verbatim".
	if _, err := strconv.ParseFloat(s, 64); err == nil || errors.Is(err, strconv.ErrRange) {
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Fval{
						Fval: &pg_query.Float{Fval: s},
					},
				},
			},
		}
	}
	return nil
}

// equalConstraintDef compares two constraint definitions by normalizing
// them through pg_query parse/deparse, so that formatting differences
// (e.g. extra parentheses, spacing) do not cause false diffs.
//
// The comparison is asymmetric with respect to type casts: pg_get_constraintdef
// often adds explicit casts (e.g. '00:00:00'::time, '0'::integer) that the
// user did not write in the desired schema. When current has a TypeCast at
// a position where desired has none, the cast is stripped from current so
// the two compare equal. Non-text-like casts present in desired are
// preserved, so an explicit user-written `'0'::integer` that disagrees
// with the DB still surfaces. Text-like casts (`text`, `varchar`) are
// stripped on both sides by normalizeCheckExpr regardless of who wrote
// them, since pg_get_constraintdef adds them to anything string-typed
// and the user-written form practically never includes them.
func equalConstraintDef(current, desired string) bool {
	if current == desired {
		return true
	}
	curResult, curCon, errC := pgast.ParseConstraintDefStrict(current)
	desResult, desCon, errD := pgast.ParseConstraintDefStrict(desired)
	if errC != nil || errD != nil {
		return current == desired
	}
	curCon.RawExpr = normalizeCheckExpr(curCon.RawExpr)
	desCon.RawExpr = normalizeCheckExpr(desCon.RawExpr)
	curCon.RawExpr = alignCurrentCasts(desCon.RawExpr, curCon.RawExpr)
	curStr, errC := pg_query.Deparse(curResult)
	desStr, errD := pg_query.Deparse(desResult)
	if errC != nil || errD != nil {
		return current == desired
	}
	return curStr == desStr
}

// alignCurrentCasts walks desired and current in parallel, stripping
// TypeCast wrappers from current at positions where desired has no
// matching TypeCast. The function mutates the current tree in place
// and returns the (possibly unwrapped) current node so callers can
// reassign at parent boundaries.
func alignCurrentCasts(desired, current *pg_query.Node) *pg_query.Node {
	if desired == nil || current == nil {
		return current
	}
	for {
		ct := current.GetTypeCast()
		if ct == nil || desired.GetTypeCast() != nil {
			break
		}
		if ct.Arg == nil {
			return current
		}
		arg := ct.Arg
		// When stripping a cast on a string literal of a numeric type, coerce
		// the surviving A_Const{Sval "<n>"} back to A_Const{Ival/Fval} so it
		// matches a user-written bare numeric literal — but only when desired
		// at this position is itself a numeric (non-string) A_Const. If the
		// user wrote a quoted form (`'0'`), leave the stripped Sval intact so
		// it still compares equal to the quoted desired.
		if isNumericTypeName(ct.TypeName) && desiredIsNumericAConst(desired) {
			if ac := arg.GetAConst(); ac != nil {
				if sv := ac.GetSval(); sv != nil {
					if numeric := numericAConstFromString(sv.Sval); numeric != nil {
						arg = numeric
					}
				}
			}
		}
		current = arg
	}
	switch dn := desired.Node.(type) {
	case *pg_query.Node_TypeCast:
		if cn := current.GetTypeCast(); cn != nil {
			cn.Arg = alignCurrentCasts(dn.TypeCast.Arg, cn.Arg)
		}
	case *pg_query.Node_AExpr:
		if cn := current.GetAExpr(); cn != nil {
			cn.Lexpr = alignCurrentCasts(dn.AExpr.Lexpr, cn.Lexpr)
			cn.Rexpr = alignCurrentCasts(dn.AExpr.Rexpr, cn.Rexpr)
		}
	case *pg_query.Node_BoolExpr:
		if cn := current.GetBoolExpr(); cn != nil && len(cn.Args) == len(dn.BoolExpr.Args) {
			for i := range dn.BoolExpr.Args {
				cn.Args[i] = alignCurrentCasts(dn.BoolExpr.Args[i], cn.Args[i])
			}
		}
	case *pg_query.Node_AArrayExpr:
		if cn := current.GetAArrayExpr(); cn != nil && len(cn.Elements) == len(dn.AArrayExpr.Elements) {
			for i := range dn.AArrayExpr.Elements {
				cn.Elements[i] = alignCurrentCasts(dn.AArrayExpr.Elements[i], cn.Elements[i])
			}
		}
	case *pg_query.Node_List:
		if cn := current.GetList(); cn != nil && len(cn.Items) == len(dn.List.Items) {
			for i := range dn.List.Items {
				cn.Items[i] = alignCurrentCasts(dn.List.Items[i], cn.Items[i])
			}
		}
	case *pg_query.Node_FuncCall:
		if cn := current.GetFuncCall(); cn != nil && len(cn.Args) == len(dn.FuncCall.Args) {
			for i := range dn.FuncCall.Args {
				cn.Args[i] = alignCurrentCasts(dn.FuncCall.Args[i], cn.Args[i])
			}
		}
	case *pg_query.Node_NullTest:
		if cn := current.GetNullTest(); cn != nil {
			cn.Arg = alignCurrentCasts(dn.NullTest.Arg, cn.Arg)
		}
	case *pg_query.Node_CoalesceExpr:
		if cn := current.GetCoalesceExpr(); cn != nil && len(cn.Args) == len(dn.CoalesceExpr.Args) {
			for i := range dn.CoalesceExpr.Args {
				cn.Args[i] = alignCurrentCasts(dn.CoalesceExpr.Args[i], cn.Args[i])
			}
		}
	case *pg_query.Node_CaseExpr:
		if cn := current.GetCaseExpr(); cn != nil {
			cn.Arg = alignCurrentCasts(dn.CaseExpr.Arg, cn.Arg)
			cn.Defresult = alignCurrentCasts(dn.CaseExpr.Defresult, cn.Defresult)
			if len(cn.Args) == len(dn.CaseExpr.Args) {
				for i := range dn.CaseExpr.Args {
					dw := dn.CaseExpr.Args[i].GetCaseWhen()
					cw := cn.Args[i].GetCaseWhen()
					if dw != nil && cw != nil {
						cw.Expr = alignCurrentCasts(dw.Expr, cw.Expr)
						cw.Result = alignCurrentCasts(dw.Result, cw.Result)
					}
				}
			}
		}
	}
	return current
}

func diffConstraints(fqtn string, current, desired *orderedmap.Map[string, *model.Constraint], dc DropChecker) (stmts []string, disallowed []string, err error) {
	dc = normalizeDropChecker(dc)

	// Detect renames
	renameStmts, current, renamedFrom, err := detectConstraintRenames(fqtn, current, desired)
	if err != nil {
		return nil, nil, err
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

	// Drop removed or changed constraints. Pure removals (constraint absent
	// from desired) honor the constraint-drop policy; definition changes still
	// run DROP+ADD because PostgreSQL has no ALTER CONSTRAINT for definitions.
	conAllowed := dc.IsDropAllowed("constraint")
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
			drop := "ALTER TABLE " + fqtn + " DROP CONSTRAINT " + model.Ident(dropName) + ";"
			if !ok && !conAllowed {
				disallowed = append(disallowed, "-- skipped: "+drop)
				continue
			}
			stmts = append(stmts, drop)
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

	return stmts, disallowed, nil
}

type diffIndexesResult struct {
	Stmts               []string
	DisallowedDropStmts []string
	HasConcurrently     bool
}

func diffIndexes(current, desired *orderedmap.Map[string, *model.Index], dc DropChecker) (*diffIndexesResult, error) {
	dc = normalizeDropChecker(dc)
	result := &diffIndexesResult{}

	// Detect renames
	renameStmts, current, err := detectIndexRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, renameStmts...)

	// Drop removed or changed indexes. Pure removals (index absent from
	// desired) honor the index-drop policy; definition changes still run
	// DROP+CREATE because PostgreSQL has no ALTER INDEX for definitions.
	idxAllowed := dc.IsDropAllowed("index")
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
			if !ok && !idxAllowed {
				result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: "+stmt)
				continue
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

// diffForeignKeys returns (dropStmts, addStmts, disallowed, error).
// Rename statements are included in addStmts (they depend on table renames being done first).
// Pure FK removals (FK absent from desired while the owning table stays) honor
// --allow-drop=foreign_key; FK drops emitted because the owning table is being
// dropped follow the table policy (handled in DiffTables).
func diffForeignKeys(fqtn, schema string, current, desired *orderedmap.Map[string, *model.ForeignKey], dc DropChecker) (dropStmts, addStmts, disallowed []string, err error) {
	dc = normalizeDropChecker(dc)

	// Detect renames (renames go into addStmts since they may depend on table renames)
	renameStmts, current, renamedFrom, err := detectForeignKeyRenames(fqtn, current, desired)
	if err != nil {
		return nil, nil, nil, err
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
	fkAllowed := dc.IsDropAllowed("foreign_key")
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
			drop := "ALTER TABLE " + fqtn + " DROP CONSTRAINT " + model.Ident(dropName) + ";"
			if !ok && !fkAllowed {
				disallowed = append(disallowed, "-- skipped: "+drop)
				continue
			}
			dropStmts = append(dropStmts, drop)
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

	return dropStmts, addStmts, disallowed, nil
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
