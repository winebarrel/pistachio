package pistachio

import (
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func (f *FilterOptions) filterTables(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
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

func (f *FilterOptions) filterDomains(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
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
