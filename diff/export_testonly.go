//go:build testonly

package diff

// The names the external tests use for what is unexported. The testonly tag
// keeps them out of a build without it, which is what a release is.

var (
	AddColumnSQL                   = addColumnSQL
	AlignIndexCasts                = alignIndexCasts
	AlterColumnSQL                 = alterColumnSQL
	AlterSerialSequenceSQL         = alterSerialSequenceSQL
	AlterTypeName                  = alterTypeName
	CanCreateOrReplaceView         = canCreateOrReplaceView
	CollectColumnRenames           = collectColumnRenames
	ColumnStorageSQL               = columnStorageSQL
	CompareFKDef                   = compareFKDef
	CreateIndexSQL                 = createIndexSQL
	DiffColumns                    = diffColumns
	DiffComments                   = diffComments
	DiffConstraints                = diffConstraints
	DiffForeignKeys                = diffForeignKeys
	DiffIndexes                    = diffIndexes
	DiffPersistence                = diffPersistence
	DiffPolicies                   = diffPolicies
	DiffRLS                        = diffRLS
	DiffStorageParams              = diffStorageParams
	DiffTable                      = diffTable
	DiffTriggers                   = diffTriggers
	DropIndexSQL                   = dropIndexSQL
	EqualCollation                 = equalCollation
	EqualConstraintDef             = equalConstraintDef
	EqualIndexDef                  = equalIndexDef
	EqualRoles                     = equalRoles
	EqualSelectExpr                = equalSelectExpr
	EqualTriggerDef                = equalTriggerDef
	EqualViewDef                   = equalViewDef
	ExprChanged                    = exprChanged
	FoldTypeMod                    = foldTypeMod
	IdentitySeqClauses             = identitySeqClauses
	IndexColumns                   = indexColumns
	IsConstraintTrigger            = isConstraintTrigger
	IsTextLikeTypeName             = isTextLikeTypeName
	NewTableExtras                 = newTableExtras
	NormalizeDropChecker           = normalizeDropChecker
	NormalizeIndexStmt             = normalizeIndexStmt
	NormalizeRoles                 = normalizeRoles
	OrderPersistenceChanges        = orderPersistenceChanges
	ParentIndexCopies              = parentIndexCopies
	ParseFKDef                     = parseFKDef
	ParseIndexDef                  = parseIndexDef
	ParseTriggerDef                = parseTriggerDef
	PartitionDepth                 = partitionDepth
	PeerIsNumericAtTopLevel        = peerIsNumericAtTopLevel
	RenameColumnKeys               = renameColumnKeys
	RenameTriggerDef               = renameTriggerDef
	RenameTriggerRelation          = renameTriggerRelation
	ReplaceTriggerSQL              = replaceTriggerSQL
	RewriteColumnRefsInConstraints = rewriteColumnRefsInConstraints
	RewriteColumnRefsInForeignKeys = rewriteColumnRefsInForeignKeys
	RewriteColumnRefsInGenerated   = rewriteColumnRefsInGenerated
	RewriteColumnRefsInIndexes     = rewriteColumnRefsInIndexes
	RewriteColumnRefsInPolicies    = rewriteColumnRefsInPolicies
	RewriteColumnRefsInTriggers    = rewriteColumnRefsInTriggers
	RewriteColumnsInConstraintDef  = rewriteColumnsInConstraintDef
	RewriteColumnsInIndexDef       = rewriteColumnsInIndexDef
	SameConstraintDef              = sameConstraintDef
	SchemaOf                       = schemaOf
	StripDefaultTopLevelCast       = stripDefaultTopLevelCast
	UpdateIndexName                = updateIndexName
	UpdateIndexTableName           = updateIndexTableName
	UsingIndexNames                = usingIndexNames
	ViewOutputColumns              = viewOutputColumns
	WithInheritedColumns           = withInheritedColumns
)

type (
	DenyAllDrops      = denyAllDrops
	PersistenceChange = persistenceChange
)

// EqualPtr wraps equalPtr, which is generic and so cannot be assigned to a
// variable.
func EqualPtr[T comparable](a, b *T) bool { return equalPtr(a, b) }
