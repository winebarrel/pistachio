package pistachio

import (
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

var (
	ResolvePreSQL             = resolvePreSQL
	ResolveConcurrentlyPreSQL = resolveConcurrentlyPreSQL
	ExtractObjectName         = extractObjectName
	OrderStatements           = orderStatements
	CompareTaggedPos          = compareTaggedPos
	BuildDefReplacer          = buildDefReplacer
)

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
