package pistachio

import (
	"context"
	"fmt"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/format"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

type DumpOptions struct {
	Split      string `xor:"json-split" help:"Output each table/view/enum/domain/composite type/sequence/routine as a separate file in the specified directory."`
	OmitSchema bool   `help:"Omit schema name from the dump output."`
	NoReadOnly bool   `env:"PISTA_NO_READ_ONLY" help:"Open the database connection read-write. By default dump uses a read-only connection."`
	NoFormat   bool   `xor:"json-no-format" env:"PISTA_NO_FORMAT" help:"Write the dump as the model renders it, without the layout pista fmt applies."`
	// JSON writes the dump as JSON rather than SQL, in the shape `pista parse`
	// writes, so the same schema describes both. The flags that lay SQL out have nothing to
	// change in it, so kong refuses them alongside it.
	JSON bool `xor:"json-split,json-no-format,json-explain" env:"PISTA_DUMP_JSON" help:"Write the dump as JSON instead of SQL."`
	// Explain writes the size estimate of each table, materialized view and
	// index into the comment above it. The JSON carries no comment to hold it.
	Explain bool `xor:"json-explain" env:"PISTA_DUMP_EXPLAIN" help:"Comment each table, materialized view and index with its size estimate from pg_class."`
}

type DumpResult struct {
	Tables         *orderedmap.Map[string, *model.Table]
	Views          *orderedmap.Map[string, *model.View]
	Enums          *orderedmap.Map[string, *model.Enum]
	Domains        *orderedmap.Map[string, *model.Domain]
	CompositeTypes *orderedmap.Map[string, *model.CompositeType]
	Sequences      *orderedmap.Map[string, *model.Sequence]
	Routines       *orderedmap.Map[string, *model.Routine]
	OmitSchema     bool
	NoFormat       bool
	Count          ObjectCount
}

// stripRelationSchemaPrefix removes the schema qualification from the relation
// an index or a trigger targets, for --omit-schema. pg_get_indexdef emits
// "... ON <schema>.<rel> ..." for ordinary indexes and
// "... ON ONLY <schema>.<rel> ..." for indexes on partitioned-table parents,
// so both forms are rewritten. The two patterns are mutually exclusive per
// occurrence (the ONLY form has "ONLY " between "ON " and the relation), so
// applying both replacers never double-processes a match.
// pg_get_triggerdef writes the same "... ON <schema>.<rel> ..." shape.
func stripRelationSchemaPrefix(definition, fqrn, relName string) string {
	definition = strings.ReplaceAll(definition, " ON ONLY "+fqrn+" ", " ON ONLY "+relName+" ")
	return strings.ReplaceAll(definition, " ON "+fqrn+" ", " ON "+relName+" ")
}

func (r *DumpResult) tables() *orderedmap.Map[string, *model.Table] {
	if r.Tables == nil {
		return orderedmap.New[string, *model.Table]()
	}
	if !r.OmitSchema {
		return r.Tables
	}
	tables := orderedmap.New[string, *model.Table]()
	for _, t := range r.Tables.CollectValues() {
		copied := *t
		fqtn := t.FQTN()
		tableName := model.Ident(t.Name)
		copied.Schema = ""
		// Strip the schema only when the parent is in the one being dumped.
		// --omit-schema allows a single schema, but the parent may sit outside
		// it, and such a reference has to keep its schema to resolve.
		if copied.PartitionOf != nil {
			parent := strings.TrimPrefix(*copied.PartitionOf, model.Ident(t.Schema)+".")
			copied.PartitionOf = &parent
		}
		if copied.ForeignKeys.Len() > 0 {
			fks := orderedmap.New[string, *model.ForeignKey]()
			for _, fk := range copied.ForeignKeys.CollectValues() {
				fkCopied := *fk
				fkCopied.Schema = ""
				fks.Set(fk.Name, &fkCopied)
			}
			copied.ForeignKeys = fks
		}
		if copied.Indexes.Len() > 0 {
			idxs := orderedmap.New[string, *model.Index]()
			for _, idx := range copied.Indexes.CollectValues() {
				idxCopied := *idx
				idxCopied.Schema = ""
				idxCopied.Definition = stripRelationSchemaPrefix(idx.Definition, fqtn, tableName)
				idxs.Set(idx.Name, &idxCopied)
			}
			copied.Indexes = idxs
		}
		if copied.Policies != nil && copied.Policies.Len() > 0 {
			policies := orderedmap.New[string, *model.Policy]()
			for _, p := range copied.Policies.CollectValues() {
				pCopied := *p
				pCopied.Schema = ""
				policies.Set(p.Name, &pCopied)
			}
			copied.Policies = policies
		}
		copied.Triggers = stripTriggerSchemas(copied.Triggers, fqtn, tableName)
		tables.Set(tableName, &copied)
	}
	return tables
}

func (r *DumpResult) views() *orderedmap.Map[string, *model.View] {
	if r.Views == nil {
		return orderedmap.New[string, *model.View]()
	}
	if !r.OmitSchema {
		return r.Views
	}
	views := orderedmap.New[string, *model.View]()
	for _, v := range r.Views.CollectValues() {
		copied := *v
		fqvn := v.FQVN()
		viewName := model.Ident(v.Name)
		copied.Schema = ""
		if copied.Indexes != nil && copied.Indexes.Len() > 0 {
			idxs := orderedmap.New[string, *model.Index]()
			for _, idx := range copied.Indexes.CollectValues() {
				idxCopied := *idx
				idxCopied.Schema = ""
				idxCopied.Definition = stripRelationSchemaPrefix(idx.Definition, fqvn, viewName)
				idxs.Set(idx.Name, &idxCopied)
			}
			copied.Indexes = idxs
		}
		copied.Triggers = stripTriggerSchemas(copied.Triggers, fqvn, viewName)
		views.Set(viewName, &copied)
	}
	return views
}

// stripTriggerSchemas drops the schema from a relation's triggers, for
// --omit-schema. The trigger keeps whatever schema the definition gives the
// function it calls, which may sit outside the schema being dumped.
func stripTriggerSchemas(
	triggers *orderedmap.Map[string, *model.Trigger],
	fqrn, relName string,
) *orderedmap.Map[string, *model.Trigger] {
	if triggers == nil || triggers.Len() == 0 {
		return triggers
	}
	out := orderedmap.New[string, *model.Trigger]()
	for _, trg := range triggers.CollectValues() {
		copied := *trg
		copied.Schema = ""
		copied.Definition = stripRelationSchemaPrefix(trg.Definition, fqrn, relName)
		out.Set(trg.Name, &copied)
	}
	return out
}

func (r *DumpResult) enums() *orderedmap.Map[string, *model.Enum] {
	if r.Enums == nil {
		return orderedmap.New[string, *model.Enum]()
	}
	if !r.OmitSchema {
		return r.Enums
	}
	enums := orderedmap.New[string, *model.Enum]()
	for _, e := range r.Enums.CollectValues() {
		copied := *e
		copied.Schema = ""
		enums.Set(model.Ident(e.Name), &copied)
	}
	return enums
}

func (r *DumpResult) domains() *orderedmap.Map[string, *model.Domain] {
	if r.Domains == nil {
		return orderedmap.New[string, *model.Domain]()
	}
	if !r.OmitSchema {
		return r.Domains
	}
	domains := orderedmap.New[string, *model.Domain]()
	for _, d := range r.Domains.CollectValues() {
		copied := *d
		copied.Schema = ""
		domains.Set(model.Ident(d.Name), &copied)
	}
	return domains
}

func (r *DumpResult) compositeTypes() *orderedmap.Map[string, *model.CompositeType] {
	if r.CompositeTypes == nil {
		return orderedmap.New[string, *model.CompositeType]()
	}
	if !r.OmitSchema {
		return r.CompositeTypes
	}
	compositeTypes := orderedmap.New[string, *model.CompositeType]()
	for _, ct := range r.CompositeTypes.CollectValues() {
		copied := *ct
		copied.Schema = ""
		compositeTypes.Set(model.Ident(ct.Name), &copied)
	}
	return compositeTypes
}

func (r *DumpResult) sequences() *orderedmap.Map[string, *model.Sequence] {
	if r.Sequences == nil {
		return orderedmap.New[string, *model.Sequence]()
	}
	if !r.OmitSchema {
		return r.Sequences
	}
	sequences := orderedmap.New[string, *model.Sequence]()
	for _, s := range r.Sequences.CollectValues() {
		copied := *s
		copied.Schema = ""
		sequences.Set(model.Ident(s.Name), &copied)
	}
	return sequences
}

// routines returns the routines with the schema stripped when --omit-schema is
// set. A routine's argument types can name a type in the same schema, so those
// lose the prefix too.
func (r *DumpResult) routines() *orderedmap.Map[string, *model.Routine] {
	if r.Routines == nil {
		return orderedmap.New[string, *model.Routine]()
	}
	if !r.OmitSchema {
		return r.Routines
	}
	routines := orderedmap.New[string, *model.Routine]()
	for _, rt := range r.Routines.CollectValues() {
		copied := *rt
		copied.Schema = ""
		copied.Args = make([]*model.RoutineArg, len(rt.Args))
		for i, a := range rt.Args {
			arg := *a
			copied.Args[i] = &arg
		}
		routines.Set(copied.FQRN(), &copied)
	}
	return routines
}

// Document returns what the dump holds in the shape `pista parse` writes, so
// one JSON Schema describes both. The accessors carry --omit-schema, and a
// database holds no `-- pista:execute` statements, so that half is empty.
func (r *DumpResult) Document() *parser.ParseResult {
	return &parser.ParseResult{
		Tables:         r.tables(),
		Views:          r.views(),
		Enums:          r.enums(),
		Domains:        r.domains(),
		CompositeTypes: r.compositeTypes(),
		Sequences:      r.sequences(),
		Routines:       r.routines(),
	}
}

func (r *DumpResult) String() string {
	return r.formatSchemaSQL(r.enums(), r.domains(), r.compositeTypes(), r.sequences(), r.routines(), r.tables(), r.views())
}

// formatSchemaSQL formats enums, domains, composite types, sequences, tables,
// and views into canonical SQL output for dump.
// Order: enums -> domains -> composite types -> sequences -> tables -> views
// (enums/domains/composite types first since later objects may depend on them;
// sequences before tables since column defaults may reference them).
func (r *DumpResult) formatSchemaSQL(
	enums *orderedmap.Map[string, *model.Enum],
	domains *orderedmap.Map[string, *model.Domain],
	compositeTypes *orderedmap.Map[string, *model.CompositeType],
	sequences *orderedmap.Map[string, *model.Sequence],
	routines *orderedmap.Map[string, *model.Routine],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) string {
	var parts []string
	if enums != nil && enums.Len() > 0 {
		parts = append(parts, model.EnumsToSQL(enums))
	}
	if domains != nil && domains.Len() > 0 {
		parts = append(parts, model.DomainsToSQL(domains))
	}
	if compositeTypes != nil && compositeTypes.Len() > 0 {
		parts = append(parts, model.CompositeTypesToSQL(compositeTypes))
	}
	if sequences != nil && sequences.Len() > 0 {
		parts = append(parts, model.SequencesToSQL(sequences))
	}
	if routines != nil && routines.Len() > 0 {
		parts = append(parts, model.RoutinesToSQL(routines))
	}
	if tables != nil && tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(tables))
	}
	if views != nil && views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(views))
	}
	return strings.TrimSuffix(r.formatSQL(strings.Join(parts, "\n\n")), "\n")
}

// formatSQL lays the dump out with the formatter pista fmt uses, so the two
// agree on where the lines break and how far they are indented. --no-format
// leaves the model's own rendering alone.
//
// A model read from a catalog renders SQL that parses, so the error path is
// not reached by a real dump. A model built by hand can render something that
// does not parse, an empty sequence type for one, and that is returned as it
// was rather than failing the dump.
func (r *DumpResult) formatSQL(sql string) string {
	if r.NoFormat {
		return sql
	}

	out, err := format.Format(sql)
	if err != nil {
		return sql
	}

	return out
}

func (r *DumpResult) Files() map[string]string {
	files := make(map[string]string)
	seen := make(map[string]bool)
	for _, e := range r.enums().CollectValues() {
		name := uniqueFileName(seen, toFileName(e.Schema, e.Name))
		files[name] = r.formatSQL(model.EnumToSQL(e) + "\n")
		seen[strings.ToLower(name)] = true
	}
	for _, d := range r.domains().CollectValues() {
		name := uniqueFileName(seen, toFileName(d.Schema, d.Name))
		files[name] = r.formatSQL(model.DomainToSQL(d) + "\n")
		seen[strings.ToLower(name)] = true
	}
	for _, ct := range r.compositeTypes().CollectValues() {
		name := uniqueFileName(seen, toFileName(ct.Schema, ct.Name))
		files[name] = r.formatSQL(model.CompositeTypeToSQL(ct) + "\n")
		seen[strings.ToLower(name)] = true
	}
	for _, s := range r.sequences().CollectValues() {
		name := uniqueFileName(seen, toFileName(s.Schema, s.Name))
		files[name] = r.formatSQL(model.SequenceToSQL(s) + "\n")
		seen[strings.ToLower(name)] = true
	}
	for _, rt := range r.routines().CollectValues() {
		// Overloads share a schema-qualified name, so uniqueFileName gives
		// the second one a _2 suffix, the same as any other collision.
		name := uniqueFileName(seen, toFileName(rt.Schema, rt.Name))
		files[name] = r.formatSQL(model.RoutineToSQL(rt) + "\n")
		seen[strings.ToLower(name)] = true
	}
	for _, t := range r.tables().CollectValues() {
		name := uniqueFileName(seen, toFileName(t.Schema, t.Name))
		files[name] = r.formatSQL(model.TableToSQL(t) + "\n")
		seen[strings.ToLower(name)] = true
	}
	for _, v := range r.views().CollectValues() {
		name := uniqueFileName(seen, toFileName(v.Schema, v.Name))
		files[name] = r.formatSQL(model.ViewToSQL(v) + "\n")
		seen[strings.ToLower(name)] = true
	}
	return files
}

func uniqueFileName(seen map[string]bool, name string) string {
	if !seen[strings.ToLower(name)] {
		return name
	}
	ext := ".sql"
	base := strings.TrimSuffix(name, ext)
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s_%d%s", base, i, ext)
		if !seen[strings.ToLower(candidate)] {
			return candidate
		}
	}
}

var fileNameReplacer = strings.NewReplacer(
	`"`, "",
	" ", "_",
)

func toFileName(schema, name string) string {
	base := name
	if schema != "" {
		base = schema + "." + name
	}
	return fileNameReplacer.Replace(base) + ".sql"
}

func (client *Client) Dump(ctx context.Context, options *DumpOptions) (*DumpResult, error) {
	if err := client.validateSchemas(); err != nil {
		return nil, err
	}
	if options.OmitSchema && len(client.Schemas) > 1 {
		return nil, fmt.Errorf("--omit-schema cannot be used with multiple schemas")
	}

	conn, err := client.connect(ctx, !options.NoReadOnly)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	catalog, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	tables, err := catalog.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tables: %w", err)
	}

	views, err := catalog.Views(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch views: %w", err)
	}

	enums, err := catalog.Enums(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch enums: %w", err)
	}

	domains, err := catalog.Domains(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch domains: %w", err)
	}

	compositeTypes, err := catalog.CompositeTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch composite types: %w", err)
	}

	sequences, err := catalog.Sequences(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch sequences: %w", err)
	}

	// pg_proc is read only when --manage-routine asked for it.
	routines := orderedmap.New[string, *model.Routine]()
	if client.ManageRoutine {
		routines, err = catalog.Routines(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch routines: %w", err)
		}
	}

	// The estimates are keyed by the names the catalog read, so they are
	// attached before a --schema-map remap renames the schemas.
	if options.Explain {
		if err := explainDump(ctx, catalog, tables, views); err != nil {
			return nil, fmt.Errorf("failed to explain dump: %w", err)
		}
	}

	filteredTables := client.filterTables(client.remapTableSchemas(tables))
	filteredViews := client.filterViews(client.remapViewSchemas(views))
	filteredEnums := client.filterEnums(client.remapEnumSchemas(enums))
	filteredDomains := client.filterDomains(client.remapDomainSchemas(domains))
	filteredCompositeTypes := client.filterCompositeTypes(client.remapCompositeTypeSchemas(compositeTypes))
	filteredSequences := client.filterSequences(client.remapSequenceSchemas(sequences))
	filteredRoutines := client.filterRoutines(client.remapRoutineSchemas(routines))

	if !client.ManageStorageParam {
		clearStorageParams(filteredTables)
		clearMatViewStorageParams(filteredViews)
	}

	return &DumpResult{
		Tables:         filteredTables,
		Views:          filteredViews,
		Enums:          filteredEnums,
		Domains:        filteredDomains,
		CompositeTypes: filteredCompositeTypes,
		Sequences:      filteredSequences,
		Routines:       filteredRoutines,
		OmitSchema:     options.OmitSchema,
		NoFormat:       options.NoFormat,
		Count: ObjectCount{
			Schemas:        client.Schemas,
			Tables:         filteredTables.Len(),
			Views:          filteredViews.Len(),
			Enums:          filteredEnums.Len(),
			Domains:        filteredDomains.Len(),
			CompositeTypes: filteredCompositeTypes.Len(),
			Sequences:      filteredSequences.Len(),
			Routines:       routineCount(client.ManageRoutine, filteredRoutines),
		},
	}, nil
}

// routineCount returns the routine slot of ObjectCount: a count when routines
// are managed, nil otherwise so the summary line leaves the slot out.
func routineCount(manage bool, routines *orderedmap.Map[string, *model.Routine]) *int {
	if !manage {
		return nil
	}
	n := routines.Len()
	return &n
}
