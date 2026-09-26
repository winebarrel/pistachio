package pistachio

import (
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// filterByName keeps the objects of one type that --include, --exclude,
// --enable and --disable select.
func filterByName[V any](f *FilterOptions, objType string, m *orderedmap.Map[string, V], getName func(V) string) *orderedmap.Map[string, V] {
	if !f.IsTypeEnabled(objType) {
		return orderedmap.New[string, V]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return m
	}

	filtered := orderedmap.New[string, V]()
	for k, v := range m.All() {
		if f.MatchName(getName(v)) {
			filtered.Set(k, v)
		}
	}
	return filtered
}

func (f *FilterOptions) filterTables(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	tables = filterByName(f, "table", tables, func(t *model.Table) string { return t.Name })
	if !f.SkipPartitionChild {
		return tables
	}

	filtered := orderedmap.New[string, *model.Table]()
	for k, t := range tables.All() {
		if !t.IsPartitionChild() {
			filtered.Set(k, t)
		}
	}
	return filtered
}

func (f *FilterOptions) filterViews(views *orderedmap.Map[string, *model.View]) *orderedmap.Map[string, *model.View] {
	return filterByName(f, "view", views, func(v *model.View) string { return v.Name })
}

func (f *FilterOptions) filterEnums(enums *orderedmap.Map[string, *model.Enum]) *orderedmap.Map[string, *model.Enum] {
	return filterByName(f, "enum", enums, func(e *model.Enum) string { return e.Name })
}

func (f *FilterOptions) filterSequences(sequences *orderedmap.Map[string, *model.Sequence]) *orderedmap.Map[string, *model.Sequence] {
	return filterByName(f, "sequence", sequences, func(s *model.Sequence) string { return s.Name })
}

func (f *FilterOptions) filterCompositeTypes(compositeTypes *orderedmap.Map[string, *model.CompositeType]) *orderedmap.Map[string, *model.CompositeType] {
	return filterByName(f, "composite_type", compositeTypes, func(ct *model.CompositeType) string { return ct.Name })
}

func (f *FilterOptions) filterDomains(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	return filterByName(f, "domain", domains, func(d *model.Domain) string { return d.Name })
}

// filterRoutines returns the routines to manage. Routines are opt-in: without
// --manage-routine the map is empty, which keeps pg_proc out of the diff and
// leaves the output of every existing schema unchanged.
func (f *FilterOptions) filterRoutines(routines *orderedmap.Map[string, *model.Routine]) *orderedmap.Map[string, *model.Routine] {
	if !f.ManageRoutine {
		return orderedmap.New[string, *model.Routine]()
	}
	return filterByName(f, "routine", routines, func(r *model.Routine) string { return r.Name })
}
