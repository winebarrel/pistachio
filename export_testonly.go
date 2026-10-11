//go:build testonly

package pistachio

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// The names the external tests use for what is unexported. The testonly tag
// keeps them out of a build without it, which is what a release is.

var (
	BuildDefReplacer          = buildDefReplacer
	CompareTaggedPos          = compareTaggedPos
	DroppedKeys               = droppedKeys
	ExplainDump               = explainDump
	ExtractObjectName         = extractObjectName
	MergeAlterTable           = mergeAlterTable
	OrderStatements           = orderStatements
	ParseMergeableAlterTable  = parseMergeableAlterTable
	ReadPlanFile              = readPlanFile
	RemapDefaultExpr          = remapDefaultExpr
	RemapQualifiedName        = remapQualifiedName
	ResolveConcurrentlyPreSQL = resolveConcurrentlyPreSQL
	ResolvePreSQL             = resolvePreSQL
	SplitConstraintKey        = splitConstraintKey
	TryExclusive              = tryExclusive
	WritePlanFile             = writePlanFile
)

type (
	ObjectDiffs   = objectDiffs
	SchemaObjects = schemaObjects
)

const (
	ExclusiveLockClassID  = exclusiveLockClassID
	ExclusivePollInterval = exclusivePollInterval
)

// Unexported methods. Those of Engine and DumpResult are taken as method
// expressions rather than wrapped: a Validate method would make Engine a kong
// hook, and DumpResult has fields named Tables, Views, Enums and Domains.

var (
	EngineIsDSQL      = Engine.isDSQL
	EngineValidate    = Engine.validate
	DumpResultTables  = (*DumpResult).tables
	DumpResultViews   = (*DumpResult).views
	DumpResultEnums   = (*DumpResult).enums
	DumpResultDomains = (*DumpResult).domains
)

func (client *Client) Connect(ctx context.Context, readOnly bool) (*pgx.Conn, error) {
	return client.connect(ctx, readOnly)
}

func (client *Client) BuildConnConfig() (*pgx.ConnConfig, error) {
	return client.buildConnConfig()
}

func (f *FilterOptions) FilterTables(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	return f.filterTables(tables)
}
