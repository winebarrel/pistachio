// Package dsql holds what --engine dsql changes, for Amazon Aurora DSQL.
//
// The rest of pistachio stays PostgreSQL's: the current side is read and the
// diff is computed as for any server, and the functions here bring the
// catalog's values to the form the desired side uses and rewrite the
// statements the diff writes into the form DSQL runs. Each is called from a
// single branch on the engine, so the package comes out by deleting it and
// the branches the compiler then reports.
package dsql

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// btreeMethod is the access method DSQL reports for every index. DSQL has
// one, and the desired side calls it btree.
const btreeMethod = "btree_index"

// NormalizeCurrent brings the values DSQL reports to the form the desired
// side uses. DSQL reports every index's access method as btree_index, adds
// every non-key column to a primary key's index as INCLUDE, and stores every
// column compressed with lz4. None of them is something the schema says.
func NormalizeCurrent(tables *orderedmap.Map[string, *model.Table]) error {
	for _, t := range tables.CollectValues() {
		for _, col := range t.Columns.CollectValues() {
			col.Compression = ""
		}

		for _, con := range t.Constraints.CollectValues() {
			if !con.Type.IsPrimaryKeyConstraint() {
				continue
			}
			result, pk, err := pgast.ParseConstraintDefStrict(con.Definition)
			if err != nil {
				return fmt.Errorf("dsql: primary key %s: %w", con.Name, err)
			}
			pk.Including = nil
			def, err := pgast.DeparseConstraintDef(result)
			if err != nil {
				return fmt.Errorf("dsql: primary key %s: %w", con.Name, err)
			}
			con.Definition = def
		}

		for _, idx := range t.Indexes.CollectValues() {
			result, err := pg_query.Parse(idx.Definition)
			if err != nil {
				return fmt.Errorf("dsql: index %s: %w", idx.Name, err)
			}
			is := result.Stmts[0].Stmt.GetIndexStmt()
			if is.AccessMethod == btreeMethod {
				is.AccessMethod = "btree"
			}
			def, err := pg_query.Deparse(result)
			if err != nil {
				return fmt.Errorf("dsql: index %s: %w", idx.Name, err)
			}
			idx.Definition = def
		}
	}

	return nil
}

// Split rewrites the two changes DSQL takes only in another form. It runs
// before the statements are ordered, so the statements it adds are placed
// with the rest.
//
// A column added with a DEFAULT becomes ADD COLUMN and SET DEFAULT: DSQL
// rejects ADD COLUMN with a constraint. The rows already in the table keep a
// NULL, since DSQL has no DDL that fills them.
//
// A unique constraint added to a table becomes CREATE UNIQUE INDEX and ADD
// CONSTRAINT ... USING INDEX: DSQL takes no other ADD CONSTRAINT. Exec waits
// for the index between the two.
func Split(stmts []string) ([]string, error) {
	var out []string

	for _, stmt := range stmts {
		result, err := pg_query.Parse(stmt)
		if err != nil {
			return nil, fmt.Errorf("dsql: failed to parse %q: %w", stmt, err)
		}

		as := result.Stmts[0].Stmt.GetAlterTableStmt()
		if as == nil || len(as.Cmds) != 1 {
			out = append(out, stmt)
			continue
		}
		cmd := as.Cmds[0].GetAlterTableCmd()
		table := relationName(as.Relation)

		switch cmd.Subtype {
		case pg_query.AlterTableType_AT_AddColumn:
			split, ok := splitAddColumnDefault(stmt, table, cmd.Def.GetColumnDef())
			if !ok {
				out = append(out, stmt)
				continue
			}
			out = append(out, split...)
		case pg_query.AlterTableType_AT_AddConstraint:
			con := cmd.Def.GetConstraint()
			if con.Contype != pg_query.ConstrType_CONSTR_UNIQUE || con.Indexname != "" {
				out = append(out, stmt)
				continue
			}
			split, err := splitAddUnique(as.Relation, con)
			if err != nil {
				return nil, fmt.Errorf("dsql: failed to rewrite %q: %w", stmt, err)
			}
			out = append(out, split...)
		default:
			out = append(out, stmt)
		}
	}

	return out, nil
}

// splitAddColumnDefault cuts the DEFAULT out of an ADD COLUMN. A column that
// is also NOT NULL is left for Finish to refuse, and so is one with no
// DEFAULT. The DEFAULT is the last clause of the column the diff writes when
// there is no NOT NULL after it, so it runs to the end of the statement.
func splitAddColumnDefault(stmt, table string, col *pg_query.ColumnDef) ([]string, bool) {
	var def *pg_query.Constraint
	for _, c := range col.Constraints {
		con := c.GetConstraint()
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_DEFAULT:
			def = con
		case pg_query.ConstrType_CONSTR_NOTNULL:
			return nil, false
		}
	}
	if def == nil {
		return nil, false
	}

	body := strings.TrimSuffix(strings.TrimRight(stmt, " \n"), ";")
	expr := strings.TrimSpace(strings.TrimPrefix(body[def.Location:], "DEFAULT"))
	add := strings.TrimRight(body[:def.Location], " ") + ";"
	setDefault := "ALTER TABLE " + table + " ALTER COLUMN " + model.Ident(col.Colname) + " SET DEFAULT " + expr + ";"

	return []string{add, setDefault}, true
}

// splitAddUnique writes the unique index and the constraint that takes it
// over, both under the constraint's name.
func splitAddUnique(rel *pg_query.RangeVar, con *pg_query.Constraint) ([]string, error) {
	params := make([]*pg_query.Node, 0, len(con.Keys))
	for _, k := range con.Keys {
		params = append(params, indexElem(k.GetString_().Sval))
	}
	including := make([]*pg_query.Node, 0, len(con.Including))
	for _, k := range con.Including {
		including = append(including, indexElem(k.GetString_().Sval))
	}

	index, err := deparse(&pg_query.Node{Node: &pg_query.Node_IndexStmt{IndexStmt: &pg_query.IndexStmt{
		Idxname:              con.Conname,
		Relation:             rel,
		IndexParams:          params,
		IndexIncludingParams: including,
		Options:              con.Options,
		Unique:               true,
		NullsNotDistinct:     con.NullsNotDistinct,
	}}})
	if err != nil {
		return nil, err
	}

	constraint, err := deparse(&pg_query.Node{Node: &pg_query.Node_AlterTableStmt{AlterTableStmt: &pg_query.AlterTableStmt{
		Relation: rel,
		Objtype:  pg_query.ObjectType_OBJECT_TABLE,
		Cmds: []*pg_query.Node{{Node: &pg_query.Node_AlterTableCmd{AlterTableCmd: &pg_query.AlterTableCmd{
			Subtype: pg_query.AlterTableType_AT_AddConstraint,
			Def: &pg_query.Node{Node: &pg_query.Node_Constraint{Constraint: &pg_query.Constraint{
				Contype:      pg_query.ConstrType_CONSTR_UNIQUE,
				Conname:      con.Conname,
				Indexname:    con.Conname,
				Deferrable:   con.Deferrable,
				Initdeferred: con.Initdeferred,
			}}},
		}}}},
	}}})
	if err != nil {
		return nil, err
	}

	return []string{index + ";", constraint + ";"}, nil
}

func indexElem(name string) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_IndexElem{IndexElem: &pg_query.IndexElem{
		Name:          name,
		Ordering:      pg_query.SortByDir_SORTBY_DEFAULT,
		NullsOrdering: pg_query.SortByNulls_SORTBY_NULLS_DEFAULT,
	}}}
}

func deparse(stmt *pg_query.Node) (string, error) {
	return pg_query.Deparse(&pg_query.ParseResult{
		Version: pgast.ParseTreeVersion,
		Stmts:   []*pg_query.RawStmt{{Stmt: stmt}},
	})
}

// relationName writes the table a statement names. model.Ident leaves an
// empty schema out.
func relationName(rel *pg_query.RangeVar) string {
	return model.Ident(rel.Schemaname, rel.Relname)
}

// Finish rewrites the ordered statements into the form DSQL runs, and
// refuses a statement DSQL has no form of. It runs on the final list rather
// than on the table diff alone: the statements of one table diff are spread
// over several lists that only the ordering joins.
//
// It refuses CONCURRENTLY, which DSQL has no form of for CREATE INDEX or DROP
// INDEX, and the column and constraint changes DSQL takes only in CREATE
// TABLE. It drops USING btree, since DSQL rejects USING and has the one
// method, and leaves another method for DSQL to refuse. It writes CACHE 1 on
// an identity column, since DSQL rejects one without a cache size. ASYNC goes
// in last: PostgreSQL's grammar has no such word, so nothing can parse the
// statement after it.
func Finish(stmts []string) ([]string, error) {
	out := make([]string, 0, len(stmts))

	for _, stmt := range stmts {
		result, err := pg_query.Parse(stmt)
		if err != nil {
			return nil, fmt.Errorf("dsql: failed to parse %q: %w", stmt, err)
		}
		node := result.Stmts[0].Stmt

		if reason := refusal(node); reason != "" {
			return nil, fmt.Errorf("--engine dsql: DSQL does not support %s: %s", reason, stmt)
		}

		if is := node.GetIndexStmt(); is != nil {
			if is.AccessMethod == "btree" {
				is.AccessMethod = ""
			}
			def, err := pg_query.Deparse(result)
			if err != nil {
				return nil, fmt.Errorf("dsql: failed to deparse %q: %w", stmt, err)
			}
			out = append(out, addAsync(def)+";")
			continue
		}

		stmt, err = AddIdentityCache(stmt)
		if err != nil {
			return nil, err
		}
		out = append(out, stmt)
	}

	return out, nil
}

// refusal names what DSQL cannot run in a statement, or returns an empty
// string.
func refusal(node *pg_query.Node) string {
	if is := node.GetIndexStmt(); is != nil && is.Concurrent {
		return "CREATE INDEX CONCURRENTLY"
	}
	if ds := node.GetDropStmt(); ds != nil && ds.Concurrent {
		return "DROP INDEX CONCURRENTLY"
	}

	as := node.GetAlterTableStmt()
	if as == nil {
		return ""
	}
	for _, c := range as.Cmds {
		cmd := c.GetAlterTableCmd()
		switch cmd.Subtype {
		case pg_query.AlterTableType_AT_DropColumn:
			return "DROP COLUMN"
		case pg_query.AlterTableType_AT_SetNotNull:
			return "SET NOT NULL"
		case pg_query.AlterTableType_AT_AlterColumnType:
			return "a column type change"
		case pg_query.AlterTableType_AT_AddColumn:
			for _, n := range cmd.Def.GetColumnDef().Constraints {
				if n.GetConstraint().Contype == pg_query.ConstrType_CONSTR_NOTNULL {
					return "ADD COLUMN with NOT NULL"
				}
			}
		case pg_query.AlterTableType_AT_AddConstraint:
			switch cmd.Def.GetConstraint().Contype {
			case pg_query.ConstrType_CONSTR_PRIMARY:
				return "ADD CONSTRAINT ... PRIMARY KEY"
			case pg_query.ConstrType_CONSTR_CHECK:
				return "ADD CONSTRAINT ... CHECK"
			}
		}
	}

	return ""
}

// addAsync writes ASYNC after CREATE [UNIQUE] INDEX. The statement is one
// deparse wrote, so the words are where it puts them.
func addAsync(def string) string {
	for _, prefix := range []string{"CREATE UNIQUE INDEX ", "CREATE INDEX "} {
		if rest, ok := strings.CutPrefix(def, prefix); ok {
			return prefix + "ASYNC " + rest
		}
	}
	return def
}

// AddIdentityCache writes CACHE 1 on each identity column that names no
// cache size. The model leaves a cache of 1 out as the default, and DSQL
// rejects an identity column without one. sql may hold several statements
// and comments, as a dump does; the text is changed where the identity
// clause sits and nowhere else, so the layout stays as it was.
func AddIdentityCache(sql string) (string, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", fmt.Errorf("dsql: failed to parse %q: %w", sql, err)
	}

	var locations []int32
	for _, raw := range result.Stmts {
		for _, con := range identityConstraints(raw.Stmt) {
			hasCache := slices.ContainsFunc(con.Options, func(n *pg_query.Node) bool {
				return n.GetDefElem().GetDefname() == "cache"
			})
			if !hasCache {
				locations = append(locations, con.Location)
			}
		}
	}

	// From the end, so an insertion does not move the ones before it.
	slices.Sort(locations)
	for _, loc := range slices.Backward(locations) {
		sql = insertCache(sql, int(loc))
	}

	return sql, nil
}

// identityConstraints returns the identity clauses of a statement: those of
// the columns in CREATE TABLE and ADD COLUMN, and ADD GENERATED.
func identityConstraints(node *pg_query.Node) []*pg_query.Constraint {
	var cols []*pg_query.ColumnDef
	var cons []*pg_query.Constraint

	if cs := node.GetCreateStmt(); cs != nil {
		for _, elt := range cs.TableElts {
			if col := elt.GetColumnDef(); col != nil {
				cols = append(cols, col)
			}
		}
	}
	if as := node.GetAlterTableStmt(); as != nil {
		for _, c := range as.Cmds {
			cmd := c.GetAlterTableCmd()
			switch cmd.Subtype {
			case pg_query.AlterTableType_AT_AddColumn:
				cols = append(cols, cmd.Def.GetColumnDef())
			case pg_query.AlterTableType_AT_AddIdentity:
				cons = append(cons, cmd.Def.GetConstraint())
			}
		}
	}

	for _, col := range cols {
		for _, n := range col.Constraints {
			if con := n.GetConstraint(); con.Contype == pg_query.ConstrType_CONSTR_IDENTITY {
				cons = append(cons, con)
			}
		}
	}

	return cons
}

// insertCache adds CACHE 1 to the identity clause that starts at loc: inside
// its parenthesized options when it has them, as the options otherwise.
func insertCache(sql string, loc int) string {
	const word = "IDENTITY"
	end := loc + strings.Index(sql[loc:], word) + len(word)
	rest := sql[end:]
	if strings.HasPrefix(rest, " (") {
		close := end + strings.Index(rest, ")")
		return sql[:close] + " CACHE 1" + sql[close:]
	}
	return sql[:end] + " (CACHE 1)" + rest
}

// UnwaitedUniqueIndex reports a unique constraint that takes over an index
// the same statements build with ASYNC. Without the wait the index is not
// valid yet when the constraint takes it over, so --dsql-no-wait-index-build
// cannot run them. An index that already exists needs no wait.
func UnwaitedUniqueIndex(stmts []string) (string, bool, error) {
	built := map[string]bool{}

	for _, stmt := range stmts {
		if rest, ok := strings.CutPrefix(stmt, "CREATE UNIQUE INDEX ASYNC "); ok {
			result, err := pg_query.Parse("CREATE UNIQUE INDEX " + rest)
			if err != nil {
				return "", false, fmt.Errorf("dsql: failed to parse %q: %w", stmt, err)
			}
			built[result.Stmts[0].Stmt.GetIndexStmt().Idxname] = true
			continue
		}

		result, err := pg_query.Parse(stmt)
		if err != nil {
			// A statement the diff did not write, such as pre-SQL, has
			// nothing to report.
			continue
		}
		as := result.Stmts[0].Stmt.GetAlterTableStmt()
		if as == nil {
			continue
		}
		for _, c := range as.Cmds {
			con := c.GetAlterTableCmd().GetDef().GetConstraint()
			if con != nil && con.Indexname != "" && built[con.Indexname] {
				return con.Indexname, true, nil
			}
		}
	}

	return "", false, nil
}

// Conn is the part of a connection Exec uses.
type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Exec runs one statement. An index build is waited for: CREATE INDEX ASYNC
// returns the job it starts, and sys.wait_for_job returns whether the job
// succeeded. A build that fails leaves the index invalid, so the apply stops
// there.
func Exec(ctx context.Context, conn Conn, stmt string) error {
	if !strings.HasPrefix(stmt, "CREATE INDEX ASYNC ") && !strings.HasPrefix(stmt, "CREATE UNIQUE INDEX ASYNC ") {
		_, err := conn.Exec(ctx, stmt)
		return err
	}

	var jobID string
	if err := conn.QueryRow(ctx, stmt).Scan(&jobID); err != nil {
		return err
	}

	var succeeded bool
	if err := conn.QueryRow(ctx, "CALL sys.wait_for_job("+model.QuoteLiteral(jobID)+")").Scan(&succeeded); err != nil {
		return fmt.Errorf("failed to wait for index build job %s: %w", jobID, err)
	}
	if !succeeded {
		return fmt.Errorf("index build job %s failed", jobID)
	}

	return nil
}
