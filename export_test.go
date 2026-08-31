package pistachio

import (
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

const ExclusiveLockClassID = exclusiveLockClassID

var (
	ResolvePreSQL             = resolvePreSQL
	ResolveConcurrentlyPreSQL = resolveConcurrentlyPreSQL
	ExtractObjectName         = extractObjectName
	OrderStatements           = orderStatements
	CompareTaggedPos          = compareTaggedPos
	BuildDefReplacer          = buildDefReplacer
)

func FilterTables(f *FilterOptions, tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	return f.filterTables(tables)
}

func DumpResultTables(r *DumpResult) *orderedmap.Map[string, *model.Table] {
	return r.tables()
}

func DumpResultViews(r *DumpResult) *orderedmap.Map[string, *model.View] {
	return r.views()
}

func DumpResultEnums(r *DumpResult) *orderedmap.Map[string, *model.Enum] {
	return r.enums()
}

func DumpResultDomains(r *DumpResult) *orderedmap.Map[string, *model.Domain] {
	return r.domains()
}
