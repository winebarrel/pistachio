package pistachio

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/toposort"
)

type DumpOptions struct {
	FilterOptions
	Split      string `xor:"split-sort-by-deps" help:"Output each table/view/enum/domain/composite type/sequence as a separate file in the specified directory."`
	OmitSchema bool   `help:"Omit schema name from the dump output."`
	SortByDeps bool   `xor:"split-sort-by-deps" help:"Order the dump output by object dependency instead of by name. Errors when the dependency graph has a cycle. Cannot be used with --split."`
	NoReadOnly bool   `env:"PISTA_NO_READ_ONLY" help:"Open the database connection read-write. By default dump uses a read-only connection."`
}

type DumpResult struct {
	Tables         *orderedmap.Map[string, *model.Table]
	Views          *orderedmap.Map[string, *model.View]
	Enums          *orderedmap.Map[string, *model.Enum]
	Domains        *orderedmap.Map[string, *model.Domain]
	CompositeTypes *orderedmap.Map[string, *model.CompositeType]
	Sequences      *orderedmap.Map[string, *model.Sequence]
	OmitSchema     bool
	SortByDeps     bool
	Count          ObjectCount
}

// stripIndexSchemaPrefix removes the schema qualification from the relation an
// index targets, for --omit-schema. pg_get_indexdef emits
// "... ON <schema>.<rel> ..." for ordinary indexes and
// "... ON ONLY <schema>.<rel> ..." for indexes on partitioned-table parents,
// so both forms are rewritten. The two patterns are mutually exclusive per
// occurrence (the ONLY form has "ONLY " between "ON " and the relation), so
// applying both replacers never double-processes a match.
func stripIndexSchemaPrefix(definition, fqrn, relName string) string {
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
				idxCopied.Definition = stripIndexSchemaPrefix(idx.Definition, fqtn, tableName)
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
				idxCopied.Definition = stripIndexSchemaPrefix(idx.Definition, fqvn, viewName)
				idxs.Set(idx.Name, &idxCopied)
			}
			copied.Indexes = idxs
		}
		views.Set(viewName, &copied)
	}
	return views
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

func (r *DumpResult) String() string {
	if r.SortByDeps {
		if sql, ok := r.dependencyOrderedSQL(); ok {
			return sql
		}
	}
	return formatSchemaSQL(r.enums(), r.domains(), r.compositeTypes(), r.sequences(), r.tables(), r.views())
}

// dumpItem carries an object's rendered SQL together with its position in the
// dependency order.
type dumpItem struct {
	pos int
	sql string
}

// dependencyOrderedSQL renders all objects in a single dependency order
// (dependencies first) rather than the category grouping used by
// formatSchemaSQL. It returns ok=false when the graph cannot be sorted. Client.Dump
// validates the same graph up front and errors on a cycle, so the ok=false path
// is not reached through the CLI; a directly built DumpResult still degrades to
// the name order rather than panicking.
//
// The topological order is computed from the original schema-qualified maps so
// foreign-key and other cross-object references resolve correctly, then applied
// to the (possibly schema-stripped) objects produced by the helper methods.
// Both iterate in the same order, so positions align by index.
func (r *DumpResult) dependencyOrderedSQL() (string, bool) {
	order, err := toposort.OrderFromSchema(
		orEmpty(r.Enums), orEmpty(r.Domains), orEmpty(r.CompositeTypes),
		orEmpty(r.Tables), orEmpty(r.Views), orEmpty(r.Sequences),
	)
	if err != nil {
		return "", false
	}
	pos := make(map[string]int, len(order))
	for i, name := range order {
		pos[name] = i
	}

	var items []dumpItem
	var ok bool
	if items, ok = appendDumpItems(items, r.Enums, r.enums().CollectValues(), pos, model.EnumToSQL); !ok {
		return "", false
	}
	if items, ok = appendDumpItems(items, r.Domains, r.domains().CollectValues(), pos, model.DomainToSQL); !ok {
		return "", false
	}
	if items, ok = appendDumpItems(items, r.CompositeTypes, r.compositeTypes().CollectValues(), pos, model.CompositeTypeToSQL); !ok {
		return "", false
	}
	if items, ok = appendDumpItems(items, r.Sequences, r.sequences().CollectValues(), pos, model.SequenceToSQL); !ok {
		return "", false
	}
	if items, ok = appendDumpItems(items, r.Tables, r.tables().CollectValues(), pos, model.TableToSQL); !ok {
		return "", false
	}
	if items, ok = appendDumpItems(items, r.Views, r.views().CollectValues(), pos, model.ViewToSQL); !ok {
		return "", false
	}

	sort.SliceStable(items, func(i, j int) bool {
		return items[i].pos < items[j].pos
	})

	parts := make([]string, len(items))
	for i, it := range items {
		parts[i] = it.sql
	}
	return strings.Join(parts, "\n\n"), true
}

// appendDumpItems zips the original schema-qualified map (source of the
// dependency-order keys) with the already schema-adjusted objects (source of
// the rendered SQL). The helper methods preserve iteration order and count, so
// the i-th key in orig corresponds to the i-th value in adjusted, and pos holds
// every key because OrderFromSchema saw the same maps.
//
// Those invariants hold today, but rather than trust them blindly it returns
// ok=false when a key is missing from pos or the counts disagree, so the caller
// falls back to the name order instead of mis-sorting or panicking.
func appendDumpItems[V any](
	items []dumpItem,
	orig *orderedmap.Map[string, V],
	adjusted []V,
	pos map[string]int,
	toSQL func(V) string,
) ([]dumpItem, bool) {
	if orig == nil {
		return items, true
	}
	i := 0
	for k := range orig.Keys() {
		p, ok := pos[k]
		if !ok || i >= len(adjusted) {
			return items, false
		}
		items = append(items, dumpItem{pos: p, sql: toSQL(adjusted[i])})
		i++
	}
	return items, true
}

// orEmpty returns an empty map when m is nil, so callers can pass optional
// DumpResult collections to code that dereferences them.
func orEmpty[V any](m *orderedmap.Map[string, V]) *orderedmap.Map[string, V] {
	if m == nil {
		return orderedmap.New[string, V]()
	}
	return m
}

// formatSchemaSQL formats enums, domains, composite types, sequences, tables,
// and views into canonical SQL output for dump.
// Order: enums -> domains -> composite types -> sequences -> tables -> views
// (enums/domains/composite types first since later objects may depend on them;
// sequences before tables since column defaults may reference them).
func formatSchemaSQL(
	enums *orderedmap.Map[string, *model.Enum],
	domains *orderedmap.Map[string, *model.Domain],
	compositeTypes *orderedmap.Map[string, *model.CompositeType],
	sequences *orderedmap.Map[string, *model.Sequence],
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
	if tables != nil && tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(tables))
	}
	if views != nil && views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(views))
	}
	return strings.Join(parts, "\n\n")
}

func (r *DumpResult) Files() map[string]string {
	files := make(map[string]string)
	seen := make(map[string]bool)
	for _, e := range r.enums().CollectValues() {
		name := uniqueFileName(seen, toFileName(e.Schema, e.Name))
		files[name] = model.EnumToSQL(e) + "\n"
		seen[strings.ToLower(name)] = true
	}
	for _, d := range r.domains().CollectValues() {
		name := uniqueFileName(seen, toFileName(d.Schema, d.Name))
		files[name] = model.DomainToSQL(d) + "\n"
		seen[strings.ToLower(name)] = true
	}
	for _, ct := range r.compositeTypes().CollectValues() {
		name := uniqueFileName(seen, toFileName(ct.Schema, ct.Name))
		files[name] = model.CompositeTypeToSQL(ct) + "\n"
		seen[strings.ToLower(name)] = true
	}
	for _, s := range r.sequences().CollectValues() {
		name := uniqueFileName(seen, toFileName(s.Schema, s.Name))
		files[name] = model.SequenceToSQL(s) + "\n"
		seen[strings.ToLower(name)] = true
	}
	for _, t := range r.tables().CollectValues() {
		name := uniqueFileName(seen, toFileName(t.Schema, t.Name))
		files[name] = model.TableToSQL(t) + "\n"
		seen[strings.ToLower(name)] = true
	}
	for _, v := range r.views().CollectValues() {
		name := uniqueFileName(seen, toFileName(v.Schema, v.Name))
		files[name] = model.ViewToSQL(v) + "\n"
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

	filteredTables := options.filterTables(client.remapTableSchemas(tables))
	filteredViews := options.filterViews(client.remapViewSchemas(views))
	filteredEnums := options.filterEnums(client.remapEnumSchemas(enums))
	filteredDomains := options.filterDomains(client.remapDomainSchemas(domains))
	filteredCompositeTypes := options.filterCompositeTypes(client.remapCompositeTypeSchemas(compositeTypes))
	filteredSequences := options.filterSequences(client.remapSequenceSchemas(sequences))

	// Validate the dependency order up front so a cycle is a hard error rather
	// than a silent fall back to name order. String() renders in this order.
	if options.SortByDeps {
		if _, err := toposort.OrderFromSchema(
			filteredEnums, filteredDomains, filteredCompositeTypes,
			filteredTables, filteredViews, filteredSequences,
		); err != nil {
			return nil, fmt.Errorf("failed to order dump by dependency: %w", err)
		}
	}

	return &DumpResult{
		Tables:         filteredTables,
		Views:          filteredViews,
		Enums:          filteredEnums,
		Domains:        filteredDomains,
		CompositeTypes: filteredCompositeTypes,
		Sequences:      filteredSequences,
		OmitSchema:     options.OmitSchema,
		SortByDeps:     options.SortByDeps,
		Count: ObjectCount{
			Schemas:        client.Schemas,
			Tables:         filteredTables.Len(),
			Views:          filteredViews.Len(),
			Enums:          filteredEnums.Len(),
			Domains:        filteredDomains.Len(),
			CompositeTypes: filteredCompositeTypes.Len(),
			Sequences:      filteredSequences.Len(),
		},
	}, nil
}
