package pistachio

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
	"github.com/winebarrel/pistachio/toposort"
)

// diffAllOptions holds the common options for diffAll.
type diffAllOptions struct {
	FilterOptions
	DropPolicy
	Desired                  *desiredInput
	DisableIndexConcurrently bool
	ForceIndexConcurrently   bool
	BulkAlter                bool
	AssumeValidated          bool
	// StateHash asks for the hash of the current side the diff read. Only a
	// run that writes a plan file needs it, so every other run pays nothing
	// for the extra encoding.
	StateHash bool
}

// desiredInput is what a run reads from the file system: the parsed desired
// schema and the pre-SQL that goes with it.
type desiredInput struct {
	schema             *parser.ParseResult
	preSQL             string
	concurrentlyPreSQL string
}

// loadDesiredInput reads and parses every file a diff needs. Plan and Apply
// call it before they connect, so a missing file, a syntax error or a
// duplicate name is reported without a database, and an exclusive apply does
// not take its lock before it can read its own input.
func (client *Client) loadDesiredInput(
	files []string,
	preSQL, preSQLFile string,
	concurrentlyPreSQL, concurrentlyPreSQLFile string,
) (*desiredInput, error) {
	// Not wrapped: the parser's message already names the file.
	schema, err := parser.ParseSQLFilesWithSchema(files, client.Schemas[0])
	if err != nil {
		return nil, err
	}

	resolvedPreSQL, err := resolvePreSQL(preSQL, preSQLFile)
	if err != nil {
		return nil, err
	}

	resolvedConcurrentlyPreSQL, err := resolveConcurrentlyPreSQL(concurrentlyPreSQL, concurrentlyPreSQLFile)
	if err != nil {
		return nil, err
	}

	return &desiredInput{
		schema:             schema,
		preSQL:             resolvedPreSQL,
		concurrentlyPreSQL: resolvedConcurrentlyPreSQL,
	}, nil
}

// diffAllResult holds the result of diffAll.
type diffAllResult struct {
	Stmts                []string
	DisallowedDrops      []string
	Ignored              []string
	PreSQL               string
	ConcurrentlyPreSQL   string
	Count                ObjectCount
	ExecuteStmts         []*parser.ExecuteStmt
	HasConcurrentlyIndex bool
	// CurrentTables is every table the catalog read, before the filters, and
	// DesiredTables and DesiredDomains the desired side after them. --explain
	// reads the current side for the tables the statements touch, including a
	// partition a filter left out, and the desired side for the rename
	// directives and the domain constraints.
	CurrentTables  *orderedmap.Map[string, *model.Table]
	DesiredTables  *orderedmap.Map[string, *model.Table]
	DesiredDomains *orderedmap.Map[string, *model.Domain]
	// DroppedViews names the views and materialized views the statements
	// drop, for the dependent check diffAll makes against the catalog. Diff
	// reads no catalog and leaves it alone.
	DroppedViews []string
	// StateHash fingerprints the current side the statements were computed
	// against. Empty unless the run asked for it.
	StateHash string
	// IgnoredObjects names what the desired schema marked -- pista:ignore, by
	// the key the current side holds it under. Ignored is the same list as the
	// comments the output carries; the plan file records these so apply-from
	// can drop them from its read before it hashes it.
	IgnoredObjects []string
}

// ignoredObjectComments renders the -- ignored: line of each name. plan writes
// them from the diff and apply-from from the plan file, so both spell it the
// same way.
func ignoredObjectComments(names []string) []string {
	comments := make([]string, len(names))
	for i, name := range names {
		comments[i] = "-- ignored: " + name
	}
	return comments
}

// schemaObjects holds one side of a diff: one map per object kind. On the
// current side diffAll reads it from the system catalogs and Diff fills it
// from a parsed schema file; the desired side comes from the desired schema,
// filtered.
//
// The two sides have the same shape, so a call that takes them as maps takes
// fourteen of them, seven pairs a reader counts out by position. The struct
// names each side instead.
type schemaObjects struct {
	Tables         *orderedmap.Map[string, *model.Table]
	Views          *orderedmap.Map[string, *model.View]
	Enums          *orderedmap.Map[string, *model.Enum]
	Domains        *orderedmap.Map[string, *model.Domain]
	CompositeTypes *orderedmap.Map[string, *model.CompositeType]
	Sequences      *orderedmap.Map[string, *model.Sequence]
	Routines       *orderedmap.Map[string, *model.Routine]
}

// objectDiffs holds what one diff produced, one result per object kind.
type objectDiffs struct {
	Tables         *diff.TableDiffResult
	Views          *diff.ViewDiffResult
	Enums          *diff.EnumDiffResult
	Domains        *diff.DomainDiffResult
	CompositeTypes *diff.CompositeTypeDiffResult
	Sequences      *diff.SequenceDiffResult
	Routines       *diff.RoutineDiffResult
}

// diffAll performs the common catalog fetch, parse, diff, and statement
// ordering logic shared by Plan and Apply.
func (client *Client) diffAll(ctx context.Context, conn *pgx.Conn, options *diffAllOptions) (*diffAllResult, error) {
	cat, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	current, err := readCurrent(ctx, cat, &options.FilterOptions)
	if err != nil {
		return nil, err
	}

	result, err := client.diffObjects(current, options)
	if err != nil {
		return nil, err
	}

	// Only a plan that drops a view pays for the dependent read, so the
	// common run makes no extra query.
	if len(result.DroppedViews) > 0 {
		if err := checkViewDependents(ctx, cat, result.DroppedViews); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// readCurrent reads every object kind the diff compares out of the system
// catalogs. apply-from reads the same way for its state hash, so the two see
// one schema rather than two readings of it.
func readCurrent(ctx context.Context, cat *catalog.Catalog, filter *FilterOptions) (*schemaObjects, error) {
	current := &schemaObjects{}
	var err error

	current.Tables, err = cat.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tables: %w", err)
	}

	current.Views, err = cat.Views(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch views: %w", err)
	}

	current.Enums, err = cat.Enums(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch enums: %w", err)
	}

	current.Domains, err = cat.Domains(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch domains: %w", err)
	}

	current.CompositeTypes, err = cat.CompositeTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch composite types: %w", err)
	}

	current.Sequences, err = cat.Sequences(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch sequences: %w", err)
	}

	// pg_proc is read only when --manage-routine asked for it. Skipping the
	// query keeps the extra round trip off every other run.
	current.Routines = orderedmap.New[string, *model.Routine]()
	if filter.ManageRoutine {
		current.Routines, err = cat.Routines(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch routines: %w", err)
		}
	}

	return current, nil
}

// count reports how many objects of each kind a run inspected, for the line
// plan and apply write above their output. The routine slot is left out
// unless routines are managed, so the line reads as it did before they were.
func (o *schemaObjects) count(schemas []string, manageRoutine bool) ObjectCount {
	count := ObjectCount{
		Schemas:        schemas,
		Tables:         o.Tables.Len(),
		Views:          o.Views.Len(),
		Enums:          o.Enums.Len(),
		Domains:        o.Domains.Len(),
		CompositeTypes: o.CompositeTypes.Len(),
		Sequences:      o.Sequences.Len(),
	}
	if manageRoutine {
		n := o.Routines.Len()
		count.Routines = &n
	}
	return count
}

// diffObjects diffs the desired schema against an already-loaded current side
// and orders the statements. diffAll hands it the catalog's view of the
// database; Diff hands it a parsed schema file.
func (client *Client) diffObjects(current *schemaObjects, options *diffAllOptions) (*diffAllResult, error) {
	currentTables := current.Tables

	desired := options.Desired.schema

	filterDesiredBySchemas(desired, client.Schemas, client.SchemaMap)

	narrowed := options.currentSide(current)
	filteredTables := narrowed.Tables
	filteredViews := narrowed.Views
	filteredEnums := narrowed.Enums
	filteredDomains := narrowed.Domains
	filteredCompositeTypes := narrowed.CompositeTypes
	filteredSequences := narrowed.Sequences
	filteredRoutines := narrowed.Routines

	desiredEnums := options.filterEnums(client.reverseRemapEnumSchemas(desired.Enums))
	desiredDomains := options.filterDomains(client.reverseRemapDomainSchemas(desired.Domains))
	desiredCompositeTypes := options.filterCompositeTypes(client.reverseRemapCompositeTypeSchemas(desired.CompositeTypes))
	desiredTables := options.filterTables(client.reverseRemapTableSchemas(desired.Tables))
	desiredViews := options.filterViews(client.reverseRemapViewSchemas(desired.Views))
	// Only standalone sequences are managed; sequences a desired CREATE
	// SEQUENCE ties to a column via OWNED BY are excluded, matching the
	// catalog side (which already drops serial/identity-owned sequences).
	desiredSequences := options.filterSequences(standaloneSequences(client.reverseRemapSequenceSchemas(desired.Sequences)))
	desiredRoutines := options.filterRoutines(client.reverseRemapRoutineSchemas(desired.Routines))

	// Objects marked -- pista:ignore are unmanaged: drop them from both the
	// desired and current sides so no create, alter, or drop is generated.
	// Their FQNs are surfaced as -- ignored: comments.
	var ignored []string
	ignored = append(ignored, removeIgnored(desiredTables, filteredTables, func(t *model.Table) bool { return t.Ignore })...)
	ignored = append(ignored, removeIgnored(desiredViews, filteredViews, func(v *model.View) bool { return v.Ignore })...)
	ignored = append(ignored, removeIgnored(desiredEnums, filteredEnums, func(e *model.Enum) bool { return e.Ignore })...)
	ignored = append(ignored, removeIgnored(desiredDomains, filteredDomains, func(d *model.Domain) bool { return d.Ignore })...)
	ignored = append(ignored, removeIgnored(desiredCompositeTypes, filteredCompositeTypes, func(ct *model.CompositeType) bool { return ct.Ignore })...)
	ignored = append(ignored, removeIgnored(desiredSequences, filteredSequences, func(s *model.Sequence) bool { return s.Ignore })...)
	ignored = append(ignored, removeIgnored(desiredRoutines, filteredRoutines, func(r *model.Routine) bool { return r.Ignore })...)
	sort.Strings(ignored)

	// Hashed here: after removeIgnored, so an object the desired schema
	// ignores is out of it, and before the transforms below, which reach into
	// the current side too (--assume-validated, --force-index-concurrently).
	// The plan file records the ignored names, and apply-from drops them from
	// its own read before hashing, so the two see the same objects.
	var currentStateHash string
	if options.StateHash {
		var err error
		if currentStateHash, err = narrowed.stateHash(); err != nil {
			return nil, err
		}
	}

	count := narrowed.count(client.Schemas, options.ManageRoutine)

	// The current side is cleared by currentSide, above, so that the state
	// hash reads what the diff compares.
	if !options.ManageStorageParam {
		clearStorageParams(desiredTables)
		clearMatViewStorageParams(desiredViews)
	}

	switch {
	case options.DisableIndexConcurrently:
		// The current side is cleared too. The catalog never sets the flag,
		// so this is a no-op for plan and apply, but Diff parses the current
		// side from a file, where a -- pista:concurrently would otherwise
		// reach a pure drop.
		clearConcurrentlyDirectives(filteredTables, filteredViews)
		clearConcurrentlyDirectives(desiredTables, desiredViews)
	case options.ForceIndexConcurrently:
		forceConcurrentlyDirectives(filteredTables, filteredViews, desiredTables, desiredViews)
	}

	if options.AssumeValidated {
		assumeValidatedConstraints(filteredTables, desiredTables, filteredDomains, desiredDomains)
	}

	enumDiff, err := diff.DiffEnums(filteredEnums, desiredEnums, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff enums: %w", err)
	}

	sequenceDiff, err := diff.DiffSequences(filteredSequences, desiredSequences, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff sequences: %w", err)
	}

	domainDiff, err := diff.DiffDomains(filteredDomains, desiredDomains, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff domains: %w", err)
	}

	compositeTypeDiff, err := diff.DiffCompositeTypes(filteredCompositeTypes, desiredCompositeTypes, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff composite types: %w", err)
	}

	tableDiff, err := diff.DiffTables(filteredTables, desiredTables, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff tables: %w", err)
	}

	// --bulk-alter merges every table; the -- pista:bulk-alter directive
	// opts in individual tables.
	bulkAlterTables := make(map[string]bool)
	for _, t := range desiredTables.CollectValues() {
		if t.BulkAlter {
			bulkAlterTables[t.FQTN()] = true
		}
	}
	if options.BulkAlter || len(bulkAlterTables) > 0 {
		tableDiff.Stmts = mergeAlterTable(tableDiff.Stmts, func(fqtn string) bool {
			return options.BulkAlter || bulkAlterTables[fqtn]
		})
	}

	viewDiff, err := diff.DiffViews(filteredViews, desiredViews, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff views: %w", err)
	}

	routineDiff, err := diff.DiffRoutines(filteredRoutines, desiredRoutines, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff routines: %w", err)
	}

	stmts := orderStatements(
		&schemaObjects{
			Tables:         filteredTables,
			Views:          filteredViews,
			Enums:          filteredEnums,
			Domains:        filteredDomains,
			CompositeTypes: filteredCompositeTypes,
			Sequences:      filteredSequences,
			Routines:       filteredRoutines,
		},
		&schemaObjects{
			Tables:         desiredTables,
			Views:          desiredViews,
			Enums:          desiredEnums,
			Domains:        desiredDomains,
			CompositeTypes: desiredCompositeTypes,
			Sequences:      desiredSequences,
			Routines:       desiredRoutines,
		},
		&objectDiffs{
			Tables:         tableDiff,
			Views:          viewDiff,
			Enums:          enumDiff,
			Domains:        domainDiff,
			CompositeTypes: compositeTypeDiff,
			Sequences:      sequenceDiff,
			Routines:       routineDiff,
		},
	)

	var disallowed []string
	disallowed = append(disallowed, viewDiff.DisallowedDropStmts...)
	disallowed = append(disallowed, tableDiff.DisallowedDropStmts...)
	disallowed = append(disallowed, domainDiff.DisallowedDropStmts...)
	disallowed = append(disallowed, compositeTypeDiff.DisallowedDropStmts...)
	disallowed = append(disallowed, enumDiff.DisallowedDropStmts...)
	disallowed = append(disallowed, sequenceDiff.DisallowedDropStmts...)
	disallowed = append(disallowed, routineDiff.DisallowedDropStmts...)

	return &diffAllResult{
		Stmts:                stmts,
		DisallowedDrops:      disallowed,
		Ignored:              ignoredObjectComments(ignored),
		IgnoredObjects:       ignored,
		PreSQL:               options.Desired.preSQL,
		ConcurrentlyPreSQL:   options.Desired.concurrentlyPreSQL,
		Count:                count,
		ExecuteStmts:         desired.ExecuteStmts,
		HasConcurrentlyIndex: tableDiff.HasConcurrently || viewDiff.HasConcurrently,
		CurrentTables:        currentTables,
		DesiredTables:        desiredTables,
		DesiredDomains:       desiredDomains,
		DroppedViews:         viewDiff.DroppedViews,
		StateHash:            currentStateHash,
	}, nil
}

// checkViewDependents fails the plan when it would drop a view or
// materialized view that another object still reads. PostgreSQL refuses that
// DROP instead of cascading, so without this the plan reads as fine and apply
// fails on it.
//
// Dependents come from the catalog, not from the desired schema, which cannot
// see a view a filter or an unmanaged schema hides.
//
// A dependent the same plan drops is no obstacle: drops run deepest first.
func checkViewDependents(ctx context.Context, cat *catalog.Catalog, dropped []string) error {
	dependents, err := cat.ViewDependents(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch view dependents: %w", err)
	}

	alsoDropped := make(map[string]bool, len(dropped))
	for _, k := range dropped {
		alsoDropped[k] = true
	}

	// Every blocked view is reported, not the first one, so one run says
	// everything that has to move rather than one view per run.
	var msgs []string
	for _, k := range dropped {
		var blockers []string
		for _, dep := range dependents[k] {
			if dep.Relation != "" && alsoDropped[dep.Relation] {
				continue
			}
			blockers = append(blockers, dep.String())
		}
		if len(blockers) == 0 {
			continue
		}
		verb := "depends"
		if len(blockers) > 1 {
			verb = "depend"
		}
		msgs = append(msgs, fmt.Sprintf("cannot drop %s: %s %s on it", k, strings.Join(blockers, ", "), verb))
	}

	if len(msgs) > 0 {
		return errors.New(strings.Join(msgs, "; "))
	}

	return nil
}

// standaloneSequences returns only the sequences not owned by a table column.
// A desired CREATE SEQUENCE with OWNED BY ties the sequence to a column, so it
// is treated as unmanaged, keeping the desired side symmetric with the catalog
// side (which already excludes serial/identity-owned sequences).
func standaloneSequences(sequences *orderedmap.Map[string, *model.Sequence]) *orderedmap.Map[string, *model.Sequence] {
	out := orderedmap.New[string, *model.Sequence]()
	for k, s := range sequences.All() {
		if !s.Owned() {
			out.Set(k, s)
		}
	}
	return out
}

// removeIgnored deletes every entry the ignored predicate matches from the
// desired map and the same key from the current map, so ignored objects
// produce no create, alter, or drop. It returns the removed keys (the object
// FQNs). Keys are collected first to avoid mutating the map while ranging.
func removeIgnored[V any](desired, current *orderedmap.Map[string, V], ignored func(V) bool) []string {
	var keys []string
	for k, v := range desired.All() {
		if ignored(v) {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		desired.Delete(k)
		current.Delete(k)
	}
	return keys
}

// clearConcurrentlyDirectives wipes the per-index Concurrently flag on every
// table and materialized view index in the given maps, used to implement
// --disable-index-concurrently.
func clearConcurrentlyDirectives(
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) {
	for _, t := range tables.CollectValues() {
		for _, idx := range t.Indexes.CollectValues() {
			idx.Concurrently = false
		}
	}
	for _, v := range views.CollectValues() {
		for _, idx := range v.Indexes.CollectValues() {
			idx.Concurrently = false
		}
	}
}

// clearStorageParams drops the storage parameters off every table in the
// given maps, used when --manage-storage-param was not passed. Clearing both
// sides is what leaves them unmanaged: no SET or RESET is planned, a WITH
// clause a desired schema writes is not carried into the CREATE TABLE, and
// dump does not write one.
//
// The map is emptied rather than dropped, so a JSON dump writes {} for a table
// whose parameters were not read, the way it writes {} for the routines it did
// not read. The SQL is the same either way: the renderer and the diff both
// take a nil map for an empty one.
func clearStorageParams(tableMaps ...*orderedmap.Map[string, *model.Table]) {
	for _, tables := range tableMaps {
		for _, t := range tables.CollectValues() {
			t.StorageParams = orderedmap.New[string, string]()
		}
	}
}

// clearMatViewStorageParams does the same for a materialized view, which
// carries the parameters a table carries and is left unmanaged with it. A plain
// view is not touched: it holds only security_barrier and security_invoker,
// which decide what the view means rather than how it is stored.
func clearMatViewStorageParams(viewMaps ...*orderedmap.Map[string, *model.View]) {
	for _, views := range viewMaps {
		for _, v := range views.CollectValues() {
			if v.Materialized {
				v.StorageParams = orderedmap.New[string, string]()
			}
		}
	}
}

// assumeValidatedConstraints marks every table constraint, domain constraint,
// and foreign key as validated on both the current and desired sides, used to
// implement --assume-validated. Forcing the validation state to true means NOT
// VALID is never emitted and validation-state differences produce no VALIDATE
// CONSTRAINT, keeping the validated flag out of the diff entirely.
func assumeValidatedConstraints(
	currentTables *orderedmap.Map[string, *model.Table],
	desiredTables *orderedmap.Map[string, *model.Table],
	currentDomains *orderedmap.Map[string, *model.Domain],
	desiredDomains *orderedmap.Map[string, *model.Domain],
) {
	for _, tables := range []*orderedmap.Map[string, *model.Table]{currentTables, desiredTables} {
		for _, t := range tables.CollectValues() {
			for _, con := range t.Constraints.CollectValues() {
				con.Validated = true
			}
			for _, fk := range t.ForeignKeys.CollectValues() {
				fk.Validated = true
			}
		}
	}
	for _, domains := range []*orderedmap.Map[string, *model.Domain]{currentDomains, desiredDomains} {
		for _, d := range domains.CollectValues() {
			for _, c := range d.Constraints {
				c.Validated = true
			}
		}
	}
}

// forceConcurrentlyDirectives sets the per-index Concurrently flag on every
// table and materialized view index in both the current and desired schemas,
// used to implement --force-index-concurrently. The current side is also
// flipped so that pure DROP INDEX paths (index absent from desired) can pick
// up the flag, since catalog-derived indexes don't carry the directive.
func forceConcurrentlyDirectives(
	currentTables *orderedmap.Map[string, *model.Table],
	currentViews *orderedmap.Map[string, *model.View],
	desiredTables *orderedmap.Map[string, *model.Table],
	desiredViews *orderedmap.Map[string, *model.View],
) {
	for _, t := range currentTables.CollectValues() {
		for _, idx := range t.Indexes.CollectValues() {
			idx.Concurrently = true
		}
	}
	for _, v := range currentViews.CollectValues() {
		for _, idx := range v.Indexes.CollectValues() {
			idx.Concurrently = true
		}
	}
	for _, t := range desiredTables.CollectValues() {
		for _, idx := range t.Indexes.CollectValues() {
			idx.Concurrently = true
		}
	}
	for _, v := range desiredViews.CollectValues() {
		for _, idx := range v.Indexes.CollectValues() {
			idx.Concurrently = true
		}
	}
}

// orderStatements uses topological sort to determine the correct execution
// order for diff statements based on object dependencies.
// Falls back to the default category-based ordering if topological sort fails.
func orderStatements(current, desired *schemaObjects, diffs *objectDiffs) []string {
	// Build topological order from desired schema for creates
	createOrder, err := toposort.OrderFromSchema(
		desired.Enums, desired.Domains, desired.CompositeTypes, desired.Tables, desired.Views, desired.Sequences, desired.Routines,
	)
	if err != nil {
		return fallbackOrder(current, desired, diffs)
	}

	createPosMap := make(map[string]int, len(createOrder))
	for i, name := range createOrder {
		createPosMap[name] = i
	}
	addIndexPositions(createPosMap, desired.Tables, desired.Views)

	// Build topological order from current schema for drops.
	// Dropped objects are not in the desired schema, so we need the current
	// schema's dependency graph to determine correct drop order.
	dropOrder, err := toposort.OrderFromSchema(
		current.Enums, current.Domains, current.CompositeTypes, current.Tables, current.Views, current.Sequences, current.Routines,
	)
	if err != nil {
		return fallbackOrder(current, desired, diffs)
	}

	dropPosMap := make(map[string]int, len(dropOrder))
	for i, name := range dropOrder {
		dropPosMap[name] = i
	}
	addIndexPositions(dropPosMap, current.Tables, current.Views)

	// Phase 1: Creates/modifications in topological order.
	// Statements whose owning object cannot be identified (pos < 0) are placed
	// before all topo-ordered statements, preserving their original relative order.
	var createStmts []taggedStmt
	createStmts = append(createStmts, tagStatements(diffs.Enums.Stmts, createPosMap)...)
	createStmts = append(createStmts, tagStatements(diffs.Domains.Stmts, createPosMap)...)
	createStmts = append(createStmts, tagStatements(diffs.CompositeTypes.Stmts, createPosMap)...)
	createStmts = append(createStmts, tagStatements(diffs.Sequences.Stmts, createPosMap)...)
	createStmts = append(createStmts, tagStatements(diffs.Routines.Stmts, createPosMap)...)
	createStmts = append(createStmts, tagStatements(diffs.Tables.Stmts, createPosMap)...)
	sort.SliceStable(createStmts, func(i, j int) bool {
		return compareTaggedPos(createStmts[i].pos, createStmts[j].pos, false)
	})

	// Phase 2: Pre-create drops in reverse topological order.
	// View drops must happen before table/column changes (views may depend on
	// columns being dropped).
	var preDropStmts []taggedStmt
	preDropStmts = append(preDropStmts, tagStatements(diffs.Views.DropStmts, dropPosMap)...)
	sort.SliceStable(preDropStmts, func(i, j int) bool {
		return compareTaggedPos(preDropStmts[i].pos, preDropStmts[j].pos, true)
	})

	// Phase 3: Post-create drops in reverse dependency order.
	// Table drops must come after table creates/alters (column type changes may
	// remove references to domains/enums). Domain/enum drops come after table
	// drops (tables must stop referencing them first).
	var postDropStmts []taggedStmt
	postDropStmts = append(postDropStmts, tagStatements(diffs.Tables.DropStmts, dropPosMap)...)
	postDropStmts = append(postDropStmts, tagStatements(diffs.Sequences.DropStmts, dropPosMap)...)
	postDropStmts = append(postDropStmts, tagStatements(diffs.Routines.DropStmts, dropPosMap)...)
	postDropStmts = append(postDropStmts, tagStatements(diffs.CompositeTypes.DropStmts, dropPosMap)...)
	postDropStmts = append(postDropStmts, tagStatements(diffs.Domains.DropStmts, dropPosMap)...)
	postDropStmts = append(postDropStmts, tagStatements(diffs.Enums.DropStmts, dropPosMap)...)
	sort.SliceStable(postDropStmts, func(i, j int) bool {
		return compareTaggedPos(postDropStmts[i].pos, postDropStmts[j].pos, true)
	})

	// Phase 4: View creates in topological order
	var viewCreateStmts []taggedStmt
	viewCreateStmts = append(viewCreateStmts, tagStatements(diffs.Views.CreateStmts, createPosMap)...)
	sort.SliceStable(viewCreateStmts, func(i, j int) bool {
		return compareTaggedPos(viewCreateStmts[i].pos, viewCreateStmts[j].pos, false)
	})

	// Assemble:
	// FK drops -> view drops -> creates/alters -> table/domain/enum drops -> FK adds -> view creates
	var stmts []string
	for _, ts := range tagStatements(diffs.Tables.FKDropStmts, dropPosMap) {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range preDropStmts {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range createStmts {
		stmts = append(stmts, ts.sql)
	}
	// The logged <-> unlogged transitions carry their own order, so they are
	// appended rather than tagged and sorted. They sit after the creates, which
	// puts them after any rename, and before the FK adds, which together with
	// the FK drops above leaves exactly the keys that stay in place.
	stmts = append(stmts, diffs.Tables.PersistenceStmts...)
	for _, ts := range postDropStmts {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range tagStatements(diffs.Tables.FKAddStmts, createPosMap) {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range viewCreateStmts {
		stmts = append(stmts, ts.sql)
	}

	return stmts
}

// fallbackOrder is the original hardcoded ordering logic used as fallback.
//
// The view statements are still sorted among themselves. A view chain has to
// come apart deepest first and go back together base first, whatever made the
// whole-schema sort fail, and checkViewDependents counts on that when it lets
// a dependent the same plan drops through.
func fallbackOrder(current, desired *schemaObjects, diffs *objectDiffs) []string {
	var stmts []string
	stmts = append(stmts, diffs.Enums.Stmts...)
	stmts = append(stmts, diffs.Domains.Stmts...)
	stmts = append(stmts, diffs.CompositeTypes.Stmts...)
	stmts = append(stmts, diffs.Sequences.Stmts...)
	stmts = append(stmts, diffs.Routines.Stmts...)
	stmts = append(stmts, sortViewStmts(diffs.Views.DropStmts, current.Views, true)...)
	stmts = append(stmts, diffs.Tables.FKDropStmts...)
	stmts = append(stmts, diffs.Tables.Stmts...)
	stmts = append(stmts, diffs.Tables.PersistenceStmts...)
	stmts = append(stmts, diffs.Tables.DropStmts...)
	stmts = append(stmts, diffs.Sequences.DropStmts...)
	stmts = append(stmts, diffs.Routines.DropStmts...)
	stmts = append(stmts, diffs.CompositeTypes.DropStmts...)
	stmts = append(stmts, diffs.Domains.DropStmts...)
	stmts = append(stmts, diffs.Enums.DropStmts...)
	stmts = append(stmts, diffs.Tables.FKAddStmts...)
	stmts = append(stmts, sortViewStmts(diffs.Views.CreateStmts, desired.Views, false)...)
	return stmts
}

// sortViewStmts orders statements by the dependency order of the views alone,
// reversed for drops. It is the fallback's stand-in for the whole-schema sort,
// which fails on a cycle the views cannot be part of: two tables with foreign
// keys to each other are one, and a schema people write.
//
// A sort that fails even so leaves the statements as they were, the way the
// fallback left every statement before.
func sortViewStmts(stmts []string, views *orderedmap.Map[string, *model.View], reverse bool) []string {
	if len(stmts) == 0 || views == nil {
		return stmts
	}

	order, err := toposort.OrderViews(views)
	if err != nil {
		return stmts
	}

	posMap := make(map[string]int, len(order))
	for i, name := range order {
		posMap[name] = i
	}
	addIndexPositions(posMap, orderedmap.New[string, *model.Table](), views)

	tagged := tagStatements(stmts, posMap)
	sort.SliceStable(tagged, func(i, j int) bool {
		return compareTaggedPos(tagged[i].pos, tagged[j].pos, reverse)
	})

	sorted := make([]string, len(tagged))
	for i, ts := range tagged {
		sorted[i] = ts.sql
	}
	return sorted
}

// addIndexPositions gives every index the position of the relation it sits on.
// COMMENT ON INDEX names the index alone, so without this it would sort as an
// unidentified statement and run before the CREATE INDEX it comments on.
func addIndexPositions(
	posMap map[string]int,
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) {
	add := func(key string, indexes *orderedmap.Map[string, *model.Index]) {
		pos, ok := posMap[key]
		if !ok || indexes == nil {
			return
		}
		for _, idx := range indexes.CollectValues() {
			posMap[model.Ident(idx.Schema, idx.Name)] = pos
		}
	}
	for k, t := range tables.All() {
		add(k, t.Indexes)
	}
	for k, v := range views.All() {
		add(k, v.Indexes)
	}
}

// taggedStmt pairs a SQL statement with a sort position derived from
// the topological order of the object it affects.
type taggedStmt struct {
	sql string
	pos int
}

// compareTaggedPos compares two tagged statement positions for sorting.
// Unknown positions (pos < 0) are placed before all known positions,
// preserving their original relative order via the stable sort.
// This ensures RENAME and INDEX statements (which can't be mapped to a
// posMap entry) execute before dependent creates/modifications.
func compareTaggedPos(posI, posJ int, reverse bool) bool {
	iUnknown := posI < 0
	jUnknown := posJ < 0

	switch {
	case iUnknown && jUnknown:
		return false // preserve original order
	case iUnknown:
		return true // unknown before known
	case jUnknown:
		return false
	default:
		if reverse {
			return posI > posJ
		}
		return posI < posJ
	}
}

// tagStatements extracts object names from SQL statements and assigns
// topological positions. Statements whose object can't be identified
// get position -1.
func tagStatements(stmts []string, posMap map[string]int) []taggedStmt {
	tagged := make([]taggedStmt, len(stmts))
	for i, sql := range stmts {
		name := extractObjectName(sql)
		pos := -1
		if p, ok := posMap[name]; ok {
			pos = p
		}
		tagged[i] = taggedStmt{sql: sql, pos: pos}
	}
	return tagged
}

// extractObjectName names the object a DDL statement belongs to, the way
// model.Ident names it, which is how the position maps are keyed. An empty
// string means no object the order knows about, and tagStatements leaves such
// a statement at position -1.
//
// The name comes from the parse tree, not from a list of statement prefixes.
// A prefix the list leaves out sorts the statement as unknown in silence:
// CREATE UNIQUE INDEX did once, CREATE UNLOGGED SEQUENCE until this.
func extractObjectName(sql string) string {
	// A statement PostgreSQL cannot read has no place in the order. These
	// are pistachio's own statements, so the run fails on it anyway.
	result, err := pg_query.Parse(sql)
	if err != nil || len(result.GetStmts()) == 0 {
		return ""
	}

	return stmtObjectName(result.GetStmts()[0].GetStmt())
}

// stmtObjectName names the object one parsed statement belongs to. An index,
// a trigger and a policy belong to the relation they sit on, and a routine to
// its name without the argument list, which is the key the graph gives it.
//
// A rename or a drop that names an index alone stays unplaced, as before:
// both run before the statements that follow them, which is what position -1
// gives them. COMMENT ON INDEX does take the position of the relation the
// index sits on, which addIndexPositions adds.
func stmtObjectName(node *pg_query.Node) string {
	switch {
	case node.GetCreateStmt() != nil:
		return rangeVarIdent(node.GetCreateStmt().GetRelation())
	case node.GetCreateSeqStmt() != nil:
		return rangeVarIdent(node.GetCreateSeqStmt().GetSequence())
	case node.GetAlterSeqStmt() != nil:
		return rangeVarIdent(node.GetAlterSeqStmt().GetSequence())
	case node.GetViewStmt() != nil:
		return rangeVarIdent(node.GetViewStmt().GetView())
	case node.GetCreateTableAsStmt() != nil:
		return rangeVarIdent(node.GetCreateTableAsStmt().GetInto().GetRel())
	case node.GetIndexStmt() != nil:
		return rangeVarIdent(node.GetIndexStmt().GetRelation())
	case node.GetCreateTrigStmt() != nil:
		return rangeVarIdent(node.GetCreateTrigStmt().GetRelation())
	case node.GetCreatePolicyStmt() != nil:
		return rangeVarIdent(node.GetCreatePolicyStmt().GetTable())
	case node.GetAlterPolicyStmt() != nil:
		return rangeVarIdent(node.GetAlterPolicyStmt().GetTable())
	case node.GetCompositeTypeStmt() != nil:
		return rangeVarIdent(node.GetCompositeTypeStmt().GetTypevar())
	case node.GetCreateEnumStmt() != nil:
		return nameIdent(node.GetCreateEnumStmt().GetTypeName())
	case node.GetAlterEnumStmt() != nil:
		return nameIdent(node.GetAlterEnumStmt().GetTypeName())
	case node.GetCreateDomainStmt() != nil:
		return nameIdent(node.GetCreateDomainStmt().GetDomainname())
	case node.GetAlterDomainStmt() != nil:
		return nameIdent(node.GetAlterDomainStmt().GetTypeName())
	case node.GetCreateFunctionStmt() != nil:
		return toposort.RoutineNode(nameIdent(node.GetCreateFunctionStmt().GetFuncname()))
	case node.GetAlterTableStmt() != nil:
		// ALTER TABLE, and the ALTER on a view, a materialized view, a
		// sequence or a composite type, all arrive here.
		return rangeVarIdent(node.GetAlterTableStmt().GetRelation())
	case node.GetRenameStmt() != nil:
		return renameObjectName(node.GetRenameStmt())
	case node.GetDropStmt() != nil:
		return dropObjectName(node.GetDropStmt())
	case node.GetCommentStmt() != nil:
		return commentObjectName(node.GetCommentStmt())
	}

	return ""
}

// renameObjectName names the object a RENAME belongs to. A rename on a
// relation, and one on a trigger, a policy, a column or a constraint, carries
// the relation; a rename on a type or a domain carries the name alone.
func renameObjectName(rs *pg_query.RenameStmt) string {
	if rs.GetRenameType() == pg_query.ObjectType_OBJECT_INDEX {
		return ""
	}

	if rs.GetRelation() != nil {
		return rangeVarIdent(rs.GetRelation())
	}

	return objectIdent(rs.GetObject())
}

// dropObjectName names the object a DROP belongs to.
func dropObjectName(ds *pg_query.DropStmt) string {
	objects := ds.GetObjects()
	if len(objects) == 0 {
		return ""
	}

	switch ds.GetRemoveType() {
	case pg_query.ObjectType_OBJECT_INDEX:
		return ""
	case pg_query.ObjectType_OBJECT_FUNCTION, pg_query.ObjectType_OBJECT_PROCEDURE:
		return toposort.RoutineNode(objectIdent(objects[0]))
	case pg_query.ObjectType_OBJECT_POLICY, pg_query.ObjectType_OBJECT_TRIGGER:
		// DROP POLICY p ON t names the policy after the relation.
		return ownerIdent(objects[0])
	default:
		return objectIdent(objects[0])
	}
}

// commentObjectName names the object a COMMENT belongs to. A comment on a
// constraint is not one of them, and nothing emits one: the form on a table
// would want ownerIdent, and the form on a domain carries the domain as a
// type name rather than as name parts.
func commentObjectName(cs *pg_query.CommentStmt) string {
	switch cs.GetObjtype() {
	case pg_query.ObjectType_OBJECT_COLUMN:
		// COMMENT ON COLUMN names the column after the table or the
		// composite type that holds it.
		return ownerIdent(cs.GetObject())
	case pg_query.ObjectType_OBJECT_FUNCTION, pg_query.ObjectType_OBJECT_PROCEDURE:
		return toposort.RoutineNode(objectIdent(cs.GetObject()))
	default:
		return objectIdent(cs.GetObject())
	}
}

// objectIdent names the object a DROP, a COMMENT or a RENAME carries. A type
// arrives as a type name, a routine as a name with an argument list, and
// everything else as a list of name parts.
func objectIdent(node *pg_query.Node) string {
	switch {
	case node.GetTypeName() != nil:
		return nameIdent(node.GetTypeName().GetNames())
	case node.GetObjectWithArgs() != nil:
		return nameIdent(node.GetObjectWithArgs().GetObjname())
	case node.GetList() != nil:
		return nameIdent(node.GetList().GetItems())
	}

	return ""
}

// ownerIdent names what holds the object, by dropping the last part of a name
// that ends in the held object's own: a column after its table, a policy
// after the relation it is on.
func ownerIdent(node *pg_query.Node) string {
	items := node.GetList().GetItems()
	if len(items) < 2 {
		return ""
	}

	return nameIdent(items[:len(items)-1])
}

// rangeVarIdent names a relation the way model.Ident names it.
func rangeVarIdent(rv *pg_query.RangeVar) string {
	return model.Ident(rv.GetSchemaname(), rv.GetRelname())
}

// nameIdent joins the parts a qualified name arrives in, each a string node,
// into one model.Ident name.
func nameIdent(names []*pg_query.Node) string {
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, n.GetString_().GetSval())
	}

	return model.Ident(parts...)
}
