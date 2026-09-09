package pistachio

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// --explain puts a comment before every planned statement that reads or
// rewrites a table that already holds data, so a plan pasted into a review
// says which of its lines cost time in proportion to the table. Two facts are
// read off each statement, whether it scans or rewrites the table and what
// the lock it takes stops, and the table's size is added from pg_class:
//
//	-- rewrite, blocks reads and writes: public.events (~120000000 rows, 9629 MB, 5 indexes rebuilt)
//	ALTER TABLE public.events ALTER COLUMN amount SET DATA TYPE numeric(12,2);
//
// A statement that only changes the catalog takes no comment, whatever lock
// it holds for the instant it runs. Neither does one on a table this plan
// creates, since that table is empty.
//
// The classification is a fixed table drawn from the ALTER TABLE reference
// and tablecmds.c, applied to the parse tree of the emitted statement, and
// the diff is not involved. What the table cannot decide on its own is asked
// of the server in two small catalog reads: whether a column type change is a
// binary-coercible relabel or a conversion, and whether a default calls a
// volatile function. Both run only when the plan holds such a statement.

// touchKind says how much of the table a statement reads or writes.
type touchKind int

const (
	touchNone touchKind = iota
	// touchScan reads every row once: a constraint validation, an index
	// build, a NOT NULL check.
	touchScan
	// touchRewrite copies the table into a new file and rebuilds every index
	// on it.
	touchRewrite
)

// blockKind says what the statement's lock stops on the table for as long as
// the statement runs. It is the lock level folded to what a reader or a writer
// of the table sees.
type blockKind int

const (
	// blockNothing is SHARE UPDATE EXCLUSIVE or weaker: reads and writes go
	// on, only another DDL waits.
	blockNothing blockKind = iota
	// blockWrites is SHARE, SHARE ROW EXCLUSIVE or EXCLUSIVE.
	blockWrites
	// blockAll is ACCESS EXCLUSIVE.
	blockAll
)

func (k touchKind) String() string {
	switch k {
	case touchScan:
		return "scan"
	case touchRewrite:
		return "rewrite"
	}
	return ""
}

func (k blockKind) String() string {
	switch k {
	case blockWrites:
		return "blocks writes"
	case blockAll:
		return "blocks reads and writes"
	}
	return "blocks nothing"
}

// explainTarget is one table a statement touches. Rebuilt is the number of
// indexes the statement builds again, Descendants the partitions or INHERITS
// children the statement recurses into, with DescendantKind naming which.
type explainTarget struct {
	key            string
	rebuilt        int
	descendants    int
	descendantKind string
}

// explainEffect is the classification of one statement.
type explainEffect struct {
	touch   touchKind
	block   blockKind
	targets []explainTarget
}

// merge folds another effect in, for the several actions of one ALTER TABLE:
// the heaviest touch and the strongest lock win, and a target already listed
// keeps the larger of the two rebuild counts.
func (e *explainEffect) merge(o explainEffect) {
	e.touch = max(e.touch, o.touch)
	e.block = max(e.block, o.block)
	for _, t := range o.targets {
		e.addTarget(t)
	}
}

func (e *explainEffect) addTarget(t explainTarget) {
	for i := range e.targets {
		if e.targets[i].key == t.key {
			e.targets[i].rebuilt = max(e.targets[i].rebuilt, t.rebuilt)
			return
		}
	}
	e.targets = append(e.targets, t)
}

// explainer holds what the classification reads: the current schema, the
// size estimates and the two server answers, plus the rename directives that
// map a name the plan uses back to the table the catalog read.
type explainer struct {
	schemas []string
	current *orderedmap.Map[string, *model.Table]
	// children maps a parent's key to its partitions or INHERITS children.
	children map[string][]*model.Table
	// tableAlias maps a table's new name to its current one.
	tableAlias map[string]string
	// columnAlias maps, per new table name, a column's new name to its
	// current one.
	columnAlias map[string]map[string]string
	// domains is the desired side, read for ADD COLUMN: a column of a domain
	// that carries a constraint is verified by a rewrite.
	domains  *orderedmap.Map[string, *model.Domain]
	stats    map[string]catalog.TableStat
	types    map[catalog.TypeChange]catalog.TypeChangeInfo
	volatile map[string]bool
}

// explainStmts returns the statements with the comment prepended to each one
// that scans or rewrites a table.
func (client *Client) explainStmts(
	ctx context.Context,
	cat *catalog.Catalog,
	stmts []string,
	current, desired *orderedmap.Map[string, *model.Table],
	domains *orderedmap.Map[string, *model.Domain],
) ([]string, error) {
	ex := &explainer{
		schemas:     client.Schemas,
		current:     current,
		children:    map[string][]*model.Table{},
		tableAlias:  map[string]string{},
		columnAlias: map[string]map[string]string{},
		domains:     domains,
	}
	for _, t := range current.CollectValues() {
		if t.PartitionOf != nil {
			ex.children[*t.PartitionOf] = append(ex.children[*t.PartitionOf], t)
		}
	}
	for key, t := range desired.All() {
		if t.RenameFrom != nil && *t.RenameFrom != key {
			ex.tableAlias[key] = *t.RenameFrom
		}
		for name, col := range t.Columns.All() {
			if col.RenameFrom != nil && *col.RenameFrom != name {
				if ex.columnAlias[key] == nil {
					ex.columnAlias[key] = map[string]string{}
				}
				ex.columnAlias[key][name] = *col.RenameFrom
			}
		}
	}

	parsed := make([]*pg_query.Node, len(stmts))
	var changes []catalog.TypeChange
	var funcs []string
	seenChange := map[catalog.TypeChange]bool{}
	seenFunc := map[string]bool{}
	for i, sql := range stmts {
		parsed[i] = parseOneStmt(sql)
		if parsed[i] == nil {
			continue
		}
		for _, ch := range ex.typeChangesOf(parsed[i]) {
			if !seenChange[ch] {
				seenChange[ch] = true
				changes = append(changes, ch)
			}
		}
		for _, f := range defaultFuncsOf(parsed[i]) {
			if !seenFunc[f] {
				seenFunc[f] = true
				funcs = append(funcs, f)
			}
		}
	}

	// The type and volatility answers decide what a statement does to the
	// rows, so they come before the classification. Each is skipped when the
	// plan holds nothing that needs it.
	var err error
	if ex.types, err = cat.TypeChanges(ctx, changes); err != nil {
		return nil, err
	}
	if ex.volatile, err = cat.VolatileFunctions(ctx, funcs); err != nil {
		return nil, err
	}

	effects := make([]explainEffect, len(stmts))
	sized := false
	for i, node := range parsed {
		if node == nil {
			continue
		}
		effects[i] = ex.classify(node)
		sized = sized || len(effects[i].targets) > 0
	}

	// The sizes are read only once a statement is known to want one, so a plan
	// that creates and drops alone touches pg_class no more than plan does
	// without the flag.
	if sized {
		if ex.stats, err = cat.TableStats(ctx); err != nil {
			return nil, err
		}
	}

	out := make([]string, len(stmts))
	for i, sql := range stmts {
		out[i] = sql
		if line := ex.render(effects[i]); line != "" {
			out[i] = line + "\n" + sql
		}
	}
	return out, nil
}

// parseOneStmt parses a statement the diff emitted. Anything that does not
// parse as exactly one statement is left unexplained rather than reported:
// the plan is what matters, and the comment is an aid to reading it.
func parseOneStmt(sql string) *pg_query.Node {
	result, err := pg_query.Parse(sql)
	if err != nil || len(result.Stmts) != 1 {
		return nil
	}
	return result.Stmts[0].Stmt
}

// currentTable resolves a name the plan uses to the table the catalog read,
// through a rename directive when the plan renamed it first. A table the plan
// creates has no current entry and resolves to nil, and so does a statement
// with no relation at all.
func (ex *explainer) currentTable(rv *pg_query.RangeVar) (string, *model.Table) {
	key := model.Ident(rv.GetSchemaname(), rv.GetRelname())
	if old, ok := ex.tableAlias[key]; ok {
		if t, ok := ex.current.GetOk(old); ok {
			return key, t
		}
	}
	t, _ := ex.current.GetOk(key)
	return key, t
}

// currentColumn resolves a column of the table the plan names, through a
// column rename directive when the plan renamed it first.
func (ex *explainer) currentColumn(key string, t *model.Table, name string) *model.Column {
	if aliases, ok := ex.columnAlias[key]; ok {
		if old, ok := aliases[name]; ok {
			name = old
		}
	}
	col, _ := t.Columns.GetOk(name)
	return col
}

// target builds the entry for a table, counting the relations the statement
// recurses into when recurse is set: every partition of a partitioned table,
// every child of an INHERITS parent.
func (ex *explainer) target(key string, t *model.Table, recurse bool) explainTarget {
	tg := explainTarget{key: key}
	if !recurse {
		return tg
	}
	descendants := ex.descendants(t)
	if len(descendants) == 0 {
		return tg
	}
	tg.descendants = len(descendants)
	if t.Partitioned {
		tg.descendantKind = "partition"
	} else {
		tg.descendantKind = "child table"
	}
	return tg
}

// descendants lists every table under t, at any depth.
func (ex *explainer) descendants(t *model.Table) []*model.Table {
	var out []*model.Table
	for _, c := range ex.children[t.FQTN()] {
		out = append(out, c)
		out = append(out, ex.descendants(c)...)
	}
	return out
}

// indexCount is the number of indexes a rewrite of t builds again: the ones
// declared as indexes and the ones a PRIMARY KEY, UNIQUE or EXCLUDE
// constraint owns.
func indexCount(t *model.Table) int {
	n := t.Indexes.Len()
	for _, con := range t.Constraints.CollectValues() {
		if con.Type.IsPrimaryKeyConstraint() || con.Type.IsUniqueConstraint() || con.Type.IsExclusionConstraint() {
			n++
		}
	}
	return n
}

// classify reads one parsed statement.
func (ex *explainer) classify(node *pg_query.Node) explainEffect {
	switch {
	case node.GetAlterTableStmt() != nil:
		return ex.classifyAlterTable(node.GetAlterTableStmt())
	case node.GetIndexStmt() != nil:
		return ex.classifyIndex(node.GetIndexStmt())
	case node.GetAlterDomainStmt() != nil:
		return ex.classifyAlterDomain(node.GetAlterDomainStmt())
	case node.GetCreateTableAsStmt() != nil:
		return ex.classifyCreateTableAs(node.GetCreateTableAsStmt())
	}
	return explainEffect{}
}

func (ex *explainer) classifyAlterTable(as *pg_query.AlterTableStmt) explainEffect {
	if as.Objtype != pg_query.ObjectType_OBJECT_TABLE || as.Relation == nil {
		return explainEffect{}
	}
	key, t := ex.currentTable(as.Relation)
	if t == nil {
		return explainEffect{}
	}

	// Every node of Cmds is an AlterTableCmd, and a nil one would read as the
	// undefined subtype, which the table below does not name.
	var eff explainEffect
	for _, n := range as.Cmds {
		eff.merge(ex.classifyAlterTableCmd(key, t, as.Relation.Inh, n.GetAlterTableCmd()))
	}
	if eff.touch == touchNone {
		return explainEffect{}
	}
	return eff
}

// classifyAlterTableCmd is the table for ALTER TABLE. An action not listed
// changes the catalog alone and takes nothing.
func (ex *explainer) classifyAlterTableCmd(key string, t *model.Table, recurse bool, cmd *pg_query.AlterTableCmd) explainEffect {
	self := func(touch touchKind, block blockKind) explainEffect {
		tg := ex.target(key, t, recurse)
		if touch == touchRewrite {
			tg.rebuilt = indexCount(t)
		}
		return explainEffect{touch: touch, block: block, targets: []explainTarget{tg}}
	}

	switch cmd.GetSubtype() {
	case pg_query.AlterTableType_AT_AddColumn:
		return self(ex.addColumnTouch(cmd.GetDef().GetColumnDef()), blockAll)

	case pg_query.AlterTableType_AT_AlterColumnType:
		col := ex.currentColumn(key, t, cmd.GetName())
		tn := cmd.GetDef().GetColumnDef().GetTypeName()
		if col == nil || tn == nil {
			return explainEffect{}
		}
		return self(ex.alterTypeTouch(col.TypeName, tn), blockAll)

	case pg_query.AlterTableType_AT_SetNotNull:
		return self(touchScan, blockAll)

	case pg_query.AlterTableType_AT_SetLogged, pg_query.AlterTableType_AT_SetUnLogged:
		return self(touchRewrite, blockAll)

	case pg_query.AlterTableType_AT_ValidateConstraint:
		return self(touchScan, blockNothing)

	case pg_query.AlterTableType_AT_AddConstraint:
		con := cmd.GetDef().GetConstraint()
		switch con.GetContype() {
		case pg_query.ConstrType_CONSTR_CHECK, pg_query.ConstrType_CONSTR_NOTNULL:
			if con.GetSkipValidation() {
				return explainEffect{}
			}
			return self(touchScan, blockAll)
		case pg_query.ConstrType_CONSTR_PRIMARY, pg_query.ConstrType_CONSTR_UNIQUE, pg_query.ConstrType_CONSTR_EXCLUSION:
			// USING INDEX takes an index that already exists rather than
			// building one. pistachio does not emit it today.
			if con.GetIndexname() != "" {
				return explainEffect{}
			}
			return self(touchScan, blockAll)
		case pg_query.ConstrType_CONSTR_FOREIGN:
			if con.GetSkipValidation() {
				return explainEffect{}
			}
			// Both tables take SHARE ROW EXCLUSIVE. The referencing table
			// is scanned; the referenced one is probed through its key,
			// and every partition of it is locked. A referenced table the
			// same plan creates is named without a size.
			eff := self(touchScan, blockWrites)
			refKey, ref := ex.currentTable(con.GetPktable())
			if ref != nil {
				eff.addTarget(ex.target(refKey, ref, ref.Partitioned))
			} else {
				eff.addTarget(explainTarget{key: refKey})
			}
			return eff
		}
	}
	return explainEffect{}
}

// addColumnTouch decides what ADD COLUMN does to the rows that exist. A
// volatile default has to be evaluated per row, and so does an identity, a
// generated expression, a serial and a domain with a constraint, each of which
// is a rewrite. A NOT NULL without a default is checked against every row.
// Anything else is stored as a missing value in the catalog.
func (ex *explainer) addColumnTouch(cd *pg_query.ColumnDef) touchKind {
	if isSerialTypeName(cd.GetTypeName()) || ex.isConstrainedDomain(cd.GetTypeName()) {
		return touchRewrite
	}
	notNull := false
	hasDefault := false
	for _, n := range cd.GetConstraints() {
		con := n.GetConstraint()
		switch con.GetContype() {
		case pg_query.ConstrType_CONSTR_IDENTITY, pg_query.ConstrType_CONSTR_GENERATED:
			return touchRewrite
		case pg_query.ConstrType_CONSTR_NOTNULL:
			notNull = true
		case pg_query.ConstrType_CONSTR_DEFAULT:
			hasDefault = true
			for _, f := range funcNamesOf(con.GetRawExpr()) {
				if ex.volatile[f] {
					return touchRewrite
				}
			}
		}
	}
	if notNull && !hasDefault {
		return touchScan
	}
	return touchNone
}

// isConstrainedDomain reports whether the type names a desired domain that
// carries a NOT NULL or a CHECK.
func (ex *explainer) isConstrainedDomain(tn *pg_query.TypeName) bool {
	if ex.domains == nil {
		return false
	}
	name := typeNameString(tn)
	for key, d := range ex.domains.All() {
		if key == name || (!strings.Contains(name, ".") && d.Name == name) {
			return d.NotNull || len(d.Constraints) > 0
		}
	}
	return false
}

// alterTypeTouch decides what SET DATA TYPE does to the rows. A change the
// server answered as a plain relabel changes the catalog alone, and so does a
// wider modifier on the same type. Everything else converts every row.
func (ex *explainer) alterTypeTouch(current string, desired *pg_query.TypeName) touchKind {
	ch := catalog.TypeChange{Src: baseTypeString(current), Dst: typeNameString(desired)}
	info, ok := ex.types[ch]
	if !ok || !info.Known {
		return touchNone
	}
	if info.Constrained || strings.HasSuffix(current, "[]") != (len(desired.ArrayBounds) > 0) {
		return touchRewrite
	}
	if info.SameBase {
		if typmodWidens(info.BaseType, typmodOf(current), typmodsOf(desired)) {
			return touchNone
		}
		return touchRewrite
	}
	if info.Binary && len(desired.Typmods) == 0 {
		return touchNone
	}
	return touchRewrite
}

// typmodWidens reports whether the modifier change on one base type keeps the
// stored values as they are. These are the types whose length coercion has a
// support function that lets the planner drop it when the new modifier admits
// every value the old one did: a longer varchar or varbit, a numeric of the
// same scale and more precision, a timestamp or time of more precision. No
// modifier at all admits everything. A change on any other type, or a
// narrowing, converts every row.
func typmodWidens(baseType string, src, dst []int64) bool {
	if len(dst) == 0 {
		return true
	}
	switch baseType {
	case "character varying", "bit varying":
		return len(src) == 1 && dst[0] >= src[0]
	case "numeric":
		if len(src) < 2 {
			return false
		}
		if len(dst) < 2 {
			dst = append(dst, 0)
		}
		return dst[1] == src[1] && dst[0] >= src[0]
	case "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone":
		if len(src) == 0 {
			src = []int64{6}
		}
		return dst[0] >= src[0]
	}
	return false
}

// typmodOf reads the modifier out of a catalog type name, "(10,2)" in
// "numeric(10,2)" or "(3)" in "timestamp(3) without time zone".
func typmodOf(typeName string) []int64 {
	m := typmodRe.FindStringSubmatch(typeName)
	if m == nil {
		return nil
	}
	var mods []int64
	for part := range strings.SplitSeq(m[1], ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			return nil
		}
		mods = append(mods, n)
	}
	return mods
}

var typmodRe = regexp.MustCompile(`\(([0-9, ]+)\)`)

// typmodsOf reads the modifier off a parsed type name.
func typmodsOf(tn *pg_query.TypeName) []int64 {
	var mods []int64
	for _, n := range tn.Typmods {
		c := n.GetAConst()
		if c == nil || c.GetIval() == nil {
			return nil
		}
		mods = append(mods, int64(c.GetIval().Ival))
	}
	return mods
}

// baseTypeString strips the modifier and the array bound off a catalog type
// name, leaving what to_regtype resolves.
func baseTypeString(typeName string) string {
	s := typmodRe.ReplaceAllString(typeName, "")
	return strings.TrimSuffix(s, "[]")
}

// typeNameString spells a parsed type name the way to_regtype reads it: the
// name without its modifier or array bound, and without the pg_catalog prefix
// the grammar puts on a built-in type. A nil type name gives "", which
// to_regtype answers with NULL rather than an error.
func typeNameString(tn *pg_query.TypeName) string {
	var parts []string
	for _, n := range tn.GetNames() {
		if s := n.GetString_(); s != nil {
			parts = append(parts, s.Sval)
		}
	}
	if len(parts) == 2 && parts[0] == "pg_catalog" {
		parts = parts[1:]
	}
	return strings.Join(parts, ".")
}

func isSerialTypeName(tn *pg_query.TypeName) bool {
	switch typeNameString(tn) {
	case "serial", "serial4", "bigserial", "serial8", "smallserial", "serial2":
		return true
	}
	return false
}

// typeChangesOf collects the type changes a statement carries, so the server
// can be asked about all of them at once.
func (ex *explainer) typeChangesOf(node *pg_query.Node) []catalog.TypeChange {
	as := node.GetAlterTableStmt()
	if as == nil || as.Objtype != pg_query.ObjectType_OBJECT_TABLE || as.Relation == nil {
		return nil
	}
	key, t := ex.currentTable(as.Relation)
	if t == nil {
		return nil
	}
	var changes []catalog.TypeChange
	for _, n := range as.Cmds {
		cmd := n.GetAlterTableCmd()
		if cmd.GetSubtype() != pg_query.AlterTableType_AT_AlterColumnType {
			continue
		}
		col := ex.currentColumn(key, t, cmd.GetName())
		tn := cmd.GetDef().GetColumnDef().GetTypeName()
		if col == nil || tn == nil {
			continue
		}
		changes = append(changes, catalog.TypeChange{Src: baseTypeString(col.TypeName), Dst: typeNameString(tn)})
	}
	return changes
}

// defaultFuncsOf collects the functions the defaults of a statement's added
// columns call.
func defaultFuncsOf(node *pg_query.Node) []string {
	as := node.GetAlterTableStmt()
	if as == nil {
		return nil
	}
	var funcs []string
	for _, n := range as.Cmds {
		cmd := n.GetAlterTableCmd()
		if cmd.GetSubtype() != pg_query.AlterTableType_AT_AddColumn {
			continue
		}
		for _, c := range cmd.GetDef().GetColumnDef().GetConstraints() {
			if con := c.GetConstraint(); con.GetContype() == pg_query.ConstrType_CONSTR_DEFAULT {
				funcs = append(funcs, funcNamesOf(con.GetRawExpr())...)
			}
		}
	}
	return funcs
}

// funcNamesOf lists the functions an expression calls, by name alone.
func funcNamesOf(expr *pg_query.Node) []string {
	var names []string
	pgast.Walk(expr, pgast.WalkOptions{}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		if fc := n.GetFuncCall(); fc != nil && len(fc.Funcname) > 0 {
			if s := fc.Funcname[len(fc.Funcname)-1].GetString_(); s != nil {
				names = append(names, s.Sval)
			}
		}
		return n
	})
	return names
}

func (ex *explainer) classifyIndex(is *pg_query.IndexStmt) explainEffect {
	key, t := ex.currentTable(is.GetRelation())
	if t == nil {
		return explainEffect{}
	}
	block := blockWrites
	if is.Concurrent {
		block = blockNothing
	}
	return explainEffect{
		touch:   touchScan,
		block:   block,
		targets: []explainTarget{ex.target(key, t, is.GetRelation().GetInh() && t.Partitioned)},
	}
}

// classifyAlterDomain covers the domain changes that verify the values that
// exist: SET NOT NULL, ADD CONSTRAINT without NOT VALID and VALIDATE
// CONSTRAINT. Each opens every table with a column of the domain under a
// SHARE lock and reads it through.
func (ex *explainer) classifyAlterDomain(ad *pg_query.AlterDomainStmt) explainEffect {
	switch ad.Subtype {
	case "O", "V":
	case "C":
		if con := ad.Def.GetConstraint(); con == nil || con.SkipValidation {
			return explainEffect{}
		}
	default:
		return explainEffect{}
	}

	var parts []string
	for _, n := range ad.TypeName {
		if s := n.GetString_(); s != nil {
			parts = append(parts, s.Sval)
		}
	}
	if len(parts) == 0 {
		return explainEffect{}
	}
	domain := model.Ident(parts...)
	bare := parts[len(parts)-1]

	// A partition is left to the parent, which sums it in. It carries the
	// parent's columns, so it matches the domain on its own and would be
	// counted a second time. An INHERITS child is listed, since its parent is
	// not summed and each side holds its own rows.
	eff := explainEffect{touch: touchScan, block: blockWrites}
	for key, t := range ex.current.All() {
		if t.IsPartitionChild() {
			continue
		}
		for _, col := range t.Columns.CollectValues() {
			typ := strings.TrimSuffix(col.TypeName, "[]")
			if typ == domain || typ == bare {
				eff.addTarget(ex.target(key, t, t.Partitioned))
				break
			}
		}
	}
	if len(eff.targets) == 0 {
		return explainEffect{}
	}
	return eff
}

// classifyCreateTableAs covers CREATE MATERIALIZED VIEW ... WITH DATA, which
// runs its query over the tables it names and holds them under ACCESS SHARE.
func (ex *explainer) classifyCreateTableAs(cas *pg_query.CreateTableAsStmt) explainEffect {
	if cas.Objtype != pg_query.ObjectType_OBJECT_MATVIEW || cas.Into == nil || cas.Into.SkipData {
		return explainEffect{}
	}
	eff := explainEffect{touch: touchScan, block: blockNothing}
	pgast.Walk(cas.Query, pgast.WalkOptions{}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		rv := n.GetRangeVar()
		if rv == nil {
			return n
		}
		for _, candidate := range ex.resolveRangeVar(rv) {
			if t, ok := ex.current.GetOk(candidate); ok {
				eff.addTarget(ex.target(candidate, t, true))
				break
			}
		}
		return n
	})
	if len(eff.targets) == 0 {
		return explainEffect{}
	}
	return eff
}

// resolveRangeVar lists the keys a relation reference in a query body may
// resolve to: the qualified name as written, or the name under each target
// schema and then public when the reference carries no schema.
func (ex *explainer) resolveRangeVar(rv *pg_query.RangeVar) []string {
	if rv.Schemaname != "" {
		return []string{model.Ident(rv.Schemaname, rv.Relname)}
	}
	var keys []string
	for _, s := range ex.schemas {
		keys = append(keys, model.Ident(s, rv.Relname))
	}
	return append(keys, model.Ident("public", rv.Relname))
}

// render writes the comment line, or nothing for an effect that touches no
// rows.
func (ex *explainer) render(eff explainEffect) string {
	if eff.touch == touchNone || len(eff.targets) == 0 {
		return ""
	}
	parts := make([]string, len(eff.targets))
	for i, tg := range eff.targets {
		parts[i] = ex.renderTarget(tg)
	}
	return fmt.Sprintf("-- %s, %s: %s", eff.touch, eff.block, strings.Join(parts, ", "))
}

// renderTarget writes one table with its size. The rows and bytes of a
// partitioned table are those of its partitions, since the parent holds
// nothing itself; an INHERITS parent holds its own rows next to its
// children's. A table no VACUUM or ANALYZE has visited reads as not analyzed,
// and a table outside the managed schemas is named alone.
func (ex *explainer) renderTarget(tg explainTarget) string {
	t, ok := ex.current.GetOk(tg.key)
	if !ok {
		if old, aliased := ex.tableAlias[tg.key]; aliased {
			t, ok = ex.current.GetOk(old)
		}
	}
	if !ok {
		return tg.key
	}

	relations := []*model.Table{t}
	if tg.descendants > 0 {
		relations = append(relations, ex.descendants(t)...)
	}
	var rows, bytes int64
	analyzed := false
	for _, r := range relations {
		st, ok := ex.stats[r.FQTN()]
		if !ok || r.Partitioned {
			continue
		}
		bytes += st.Bytes
		if st.Rows >= 0 {
			rows += st.Rows
			analyzed = true
		}
	}

	var details []string
	if analyzed {
		details = append(details, "~"+pluralize(int(rows), "row"), sizePretty(bytes))
	} else {
		details = append(details, "not analyzed")
	}
	if tg.rebuilt > 0 {
		details = append(details, plural(tg.rebuilt, "index", "indexes")+" rebuilt")
	}
	if tg.descendants > 0 {
		details = append(details, pluralize(tg.descendants, tg.descendantKind))
	}
	return tg.key + " (" + strings.Join(details, ", ") + ")"
}

// plural is pluralize for a noun whose plural is not the singular plus s.
func plural(n int, singular, pluralForm string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}
	return fmt.Sprintf("%d %s", n, pluralForm)
}

// sizePretty writes a byte count the way pg_size_pretty does: in bytes up to
// 10 kB, then in the largest unit that keeps the number under 10240.
func sizePretty(bytes int64) string {
	const limit = 10 * 1024
	if bytes < limit {
		return strconv.FormatInt(bytes, 10) + " bytes"
	}
	n := bytes
	for _, unit := range []string{"kB", "MB", "GB", "TB"} {
		n = (n + 512) / 1024
		if n < limit {
			return strconv.FormatInt(n, 10) + " " + unit
		}
	}
	return strconv.FormatInt((n+512)/1024, 10) + " PB"
}
