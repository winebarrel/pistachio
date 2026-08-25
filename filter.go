package pistachio

import (
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func (f *FilterOptions) filterTables(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	if !f.IsTypeEnabled("table") {
		return orderedmap.New[string, *model.Table]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return tables
	}

	filtered := orderedmap.New[string, *model.Table]()
	for k, t := range tables.All() {
		if f.MatchName(t.Name) {
			filtered.Set(k, t)
		}
	}
	return filtered
}

func (f *FilterOptions) filterViews(views *orderedmap.Map[string, *model.View]) *orderedmap.Map[string, *model.View] {
	if !f.IsTypeEnabled("view") {
		return orderedmap.New[string, *model.View]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return views
	}

	filtered := orderedmap.New[string, *model.View]()
	for k, v := range views.All() {
		if f.MatchName(v.Name) {
			filtered.Set(k, v)
		}
	}
	return filtered
}

func (f *FilterOptions) filterEnums(enums *orderedmap.Map[string, *model.Enum]) *orderedmap.Map[string, *model.Enum] {
	if !f.IsTypeEnabled("enum") {
		return orderedmap.New[string, *model.Enum]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return enums
	}

	filtered := orderedmap.New[string, *model.Enum]()
	for k, e := range enums.All() {
		if f.MatchName(e.Name) {
			filtered.Set(k, e)
		}
	}
	return filtered
}

func (f *FilterOptions) filterSequences(sequences *orderedmap.Map[string, *model.Sequence]) *orderedmap.Map[string, *model.Sequence] {
	if !f.IsTypeEnabled("sequence") {
		return orderedmap.New[string, *model.Sequence]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return sequences
	}

	filtered := orderedmap.New[string, *model.Sequence]()
	for k, s := range sequences.All() {
		if f.MatchName(s.Name) {
			filtered.Set(k, s)
		}
	}
	return filtered
}

func (f *FilterOptions) filterCompositeTypes(compositeTypes *orderedmap.Map[string, *model.CompositeType]) *orderedmap.Map[string, *model.CompositeType] {
	if !f.IsTypeEnabled("composite_type") {
		return orderedmap.New[string, *model.CompositeType]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return compositeTypes
	}

	filtered := orderedmap.New[string, *model.CompositeType]()
	for k, ct := range compositeTypes.All() {
		if f.MatchName(ct.Name) {
			filtered.Set(k, ct)
		}
	}
	return filtered
}

func (f *FilterOptions) filterDomains(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	if !f.IsTypeEnabled("domain") {
		return orderedmap.New[string, *model.Domain]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return domains
	}

	filtered := orderedmap.New[string, *model.Domain]()
	for k, d := range domains.All() {
		if f.MatchName(d.Name) {
			filtered.Set(k, d)
		}
	}
	return filtered
}

// filterRoutines returns the routines to manage. Routines are opt-in: without
// --manage-routine the map is empty, which keeps pg_proc out of the diff and
// leaves the output of every existing schema unchanged.
func (f *FilterOptions) filterRoutines(routines *orderedmap.Map[string, *model.Routine]) *orderedmap.Map[string, *model.Routine] {
	if !f.ManageRoutine || !f.IsTypeEnabled("routine") {
		return orderedmap.New[string, *model.Routine]()
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return routines
	}

	filtered := orderedmap.New[string, *model.Routine]()
	for k, r := range routines.All() {
		if f.MatchName(r.Name) {
			filtered.Set(k, r)
		}
	}
	return filtered
}
