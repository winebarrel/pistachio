package parser

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

type ParseResult struct {
	Tables         *orderedmap.Map[string, *model.Table]
	Views          *orderedmap.Map[string, *model.View]
	Enums          *orderedmap.Map[string, *model.Enum]
	Domains        *orderedmap.Map[string, *model.Domain]
	CompositeTypes *orderedmap.Map[string, *model.CompositeType]
	Sequences      *orderedmap.Map[string, *model.Sequence]
	Routines       *orderedmap.Map[string, *model.Routine]
	ExecuteStmts   []*ExecuteStmt
}

// warnWriter receives warnings about statements pistachio does not support and
// silently ignores. Tests swap it via SetWarnWriter.
var warnWriter io.Writer = os.Stderr

// ignoredStmtSnippet returns the raw text of the ignored statement. Deparsing
// drops the comments and blank lines that surround the statement in the file.
// The raw slice is a fallback for the rare statement pg_query can parse but not
// deparse. The caller collapses whitespace, so a multi-line body from either
// path becomes one line.
func ignoredStmtSnippet(sql string, rawStmt *pg_query.RawStmt) string {
	single := &pg_query.ParseResult{Stmts: []*pg_query.RawStmt{rawStmt}}
	if deparsed, err := pg_query.Deparse(single); err == nil {
		return deparsed
	}

	start := rawStmt.StmtLocation
	end := start + rawStmt.StmtLen
	// pg_query leaves StmtLen at 0 for the final statement in the input.
	if rawStmt.StmtLen == 0 || end > int32(len(sql)) {
		end = int32(len(sql))
	}
	return sql[start:end]
}

// txHint suggests the flags that wrap the apply in a transaction, but only for
// the statements that open or close a whole-file transaction. ROLLBACK and
// SAVEPOINT reach here too, and neither is answered by wrapping the apply.
func txHint(rawStmt *pg_query.RawStmt) string {
	ts := rawStmt.Stmt.GetTransactionStmt()
	if ts == nil {
		return ""
	}
	switch ts.Kind {
	case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN,
		pg_query.TransactionStmtKind_TRANS_STMT_START,
		pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
		return " (use --with-tx or --try-tx to run the apply in a transaction)"
	default:
		return ""
	}
}

// warnIgnoredStmt reports a statement type that no parser case handles. The
// snippet is collapsed to one line and truncated on a rune boundary, so the
// warning stays short and valid UTF-8 even for a large multi-line body.
func warnIgnoredStmt(sql string, rawStmt *pg_query.RawStmt) {
	snippet := strings.Join(strings.Fields(ignoredStmtSnippet(sql, rawStmt)), " ")
	if snippet == "" {
		return
	}

	const maxRunes = 200
	if runes := []rune(snippet); len(runes) > maxRunes {
		snippet = string(runes[:maxRunes]) + "..."
	}

	fmt.Fprintf(warnWriter, "pistachio: ignored unsupported statement: %s%s\n", snippet, txHint(rawStmt)) //nolint:errcheck
}

// alterTableSupportedCmds lists the ALTER TABLE actions the parser reads into
// the model: constraints (parseAlterTableConstraints), the row-level security
// toggles (applyAlterTableRLS), the trigger states
// (applyAlterTableTriggerState) and the column storage and compression
// (applyAlterTableColumnStorage). Anything else is dropped, so warning is
// driven off this list rather than off a list of the actions to reject: an
// action PostgreSQL adds later warns instead of vanishing.
var alterTableSupportedCmds = map[pg_query.AlterTableType]bool{
	pg_query.AlterTableType_AT_AddConstraint:      true,
	pg_query.AlterTableType_AT_EnableRowSecurity:  true,
	pg_query.AlterTableType_AT_DisableRowSecurity: true,
	pg_query.AlterTableType_AT_ForceRowSecurity:   true,
	pg_query.AlterTableType_AT_NoForceRowSecurity: true,
	pg_query.AlterTableType_AT_EnableTrig:         true,
	pg_query.AlterTableType_AT_DisableTrig:        true,
	pg_query.AlterTableType_AT_EnableAlwaysTrig:   true,
	pg_query.AlterTableType_AT_EnableReplicaTrig:  true,
	pg_query.AlterTableType_AT_SetStorage:         true,
	pg_query.AlterTableType_AT_SetCompression:     true,
}

// commentTargetSupported lists the COMMENT ON targets parseCommentStmt reads
// into the model. A comment on any other object is dropped, and warns.
var commentTargetSupported = map[pg_query.ObjectType]bool{
	pg_query.ObjectType_OBJECT_TABLE:     true,
	pg_query.ObjectType_OBJECT_VIEW:      true,
	pg_query.ObjectType_OBJECT_MATVIEW:   true,
	pg_query.ObjectType_OBJECT_COLUMN:    true,
	pg_query.ObjectType_OBJECT_SEQUENCE:  true,
	pg_query.ObjectType_OBJECT_TYPE:      true,
	pg_query.ObjectType_OBJECT_DOMAIN:    true,
	pg_query.ObjectType_OBJECT_FUNCTION:  true,
	pg_query.ObjectType_OBJECT_PROCEDURE: true,
}

// warnIgnoredAlterTableCmds warns about the ALTER TABLE actions no handler
// reads. ALTER TABLE as a statement is supported, so such an action never
// reaches the unsupported-statement warning; dropping it in silence would let
// a column added this way read as absent from the desired schema and plan as
// a DROP COLUMN.
//
// The warning carries a statement rebuilt from the ignored actions alone, so
// one that mixes a supported action with an unsupported one reports only the
// latter. The rebuilt statement keeps the original location, which is what
// the snippet falls back to when deparse fails.
func warnIgnoredAlterTableCmds(sql string, rawStmt *pg_query.RawStmt, as *pg_query.AlterTableStmt) {
	var ignored []*pg_query.Node

	for _, cmdNode := range as.Cmds {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil || alterTableSupportedCmds[cmd.Subtype] {
			continue
		}
		ignored = append(ignored, cmdNode)
	}

	if len(ignored) == 0 {
		return
	}

	warnIgnoredStmt(sql, &pg_query.RawStmt{
		Stmt: &pg_query.Node{
			Node: &pg_query.Node_AlterTableStmt{
				AlterTableStmt: &pg_query.AlterTableStmt{
					Relation:  as.Relation,
					Objtype:   as.Objtype,
					MissingOk: as.MissingOk,
					Cmds:      ignored,
				},
			},
		},
		StmtLocation: rawStmt.StmtLocation,
		StmtLen:      rawStmt.StmtLen,
	})
}

func setUnique[V any](m *orderedmap.Map[string, V], key, kind string, v V) error {
	if _, ok := m.GetOk(key); ok {
		return fmt.Errorf("duplicate %s: %s", kind, key)
	}
	m.Set(key, v)
	return nil
}

func readSQLFile(path string) (string, error) {
	var data []byte
	var err error

	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}

	if err != nil {
		if path == "-" {
			return "", fmt.Errorf("failed to read SQL from stdin: %w", err)
		}
		return "", fmt.Errorf("failed to read SQL file: %w", err)
	}

	return string(data), nil
}

func ParseSQLFilesWithSchema(paths []string, defaultSchema string) (*ParseResult, error) {
	var sqls []string
	for _, path := range paths {
		sql, err := readSQLFile(path)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, sql)
	}

	return parseSQLWithSchema(strings.Join(sqls, "\n"), defaultSchema)
}

func parseSQLWithSchema(sql string, defaultSchema string) (*ParseResult, error) {
	if err := validateDirectives(sql); err != nil {
		return nil, err
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	tables := orderedmap.New[string, *model.Table]()
	views := orderedmap.New[string, *model.View]()
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	compositeTypes := orderedmap.New[string, *model.CompositeType]()
	sequences := orderedmap.New[string, *model.Sequence]()
	routines := orderedmap.New[string, *model.Routine]()

	stmtDirectives := extractStmtDirectives(sql, result.Stmts)
	concurrentlyDirectives := extractConcurrentlyDirectives(sql, result.Stmts)
	bulkAlterDirectives := extractBulkAlterDirectives(sql, result.Stmts)
	ignoreDirectives := extractIgnoreDirectives(sql, result.Stmts)
	executeStmts, executeSkipLocations, err := extractExecuteDirectives(sql, result.Stmts)
	if err != nil {
		return nil, err
	}

	for _, rawStmt := range result.Stmts {
		// Skip statements marked with -- pista:execute
		if executeSkipLocations[rawStmt.StmtLocation] {
			continue
		}

		node := rawStmt.Stmt
		renameFrom := stmtDirectives[rawStmt.StmtLocation]
		ignore := ignoreDirectives[rawStmt.StmtLocation]

		switch {
		case node.GetCreateEnumStmt() != nil:
			enum, err := parseCreateEnumStmt(node.GetCreateEnumStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				qualified := qualifyRenameFrom(renameFrom, defaultSchema)
				enum.RenameFrom = &qualified
			}

			// Extract value-level rename directives from raw SQL
			stmtEnd := rawStmt.StmtLocation + rawStmt.StmtLen
			if stmtEnd > int32(len(sql)) {
				stmtEnd = int32(len(sql))
			}
			rawStmtSQL := sql[rawStmt.StmtLocation:stmtEnd]
			valueDirectives, err := extractEnumValueDirectives(rawStmtSQL)
			if err != nil {
				return nil, err
			}
			for idx, oldVal := range valueDirectives {
				if idx < len(enum.Values) {
					if enum.ValueRenameFrom == nil {
						enum.ValueRenameFrom = make(map[string]string)
					}
					enum.ValueRenameFrom[enum.Values[idx]] = oldVal
				}
			}

			enum.Ignore = ignore
			if err := setUnique(enums, enum.FQEN(), "enum", enum); err != nil {
				return nil, err
			}

		case node.GetCreateDomainStmt() != nil:
			domain, err := parseCreateDomainStmt(node.GetCreateDomainStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				qualified := qualifyRenameFrom(renameFrom, defaultSchema)
				domain.RenameFrom = &qualified
			}
			domain.Ignore = ignore
			if err := setUnique(domains, domain.FQDN(), "domain", domain); err != nil {
				return nil, err
			}

		case node.GetCompositeTypeStmt() != nil:
			compositeType, err := parseCompositeTypeStmt(node.GetCompositeTypeStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				qualified := qualifyRenameFrom(renameFrom, defaultSchema)
				compositeType.RenameFrom = &qualified
			}

			// Extract attribute-level rename directives from raw SQL. The
			// composite type body is a column-like list, so the CREATE TABLE
			// inline-directive scanner applies unchanged.
			stmtEnd := rawStmt.StmtLocation + rawStmt.StmtLen
			if stmtEnd > int32(len(sql)) {
				stmtEnd = int32(len(sql))
			}
			rawStmtSQL := sql[rawStmt.StmtLocation:stmtEnd]
			attrDirectives := extractInlineDirectives(rawStmtSQL)
			for _, attr := range compositeType.Attributes {
				if oldName, ok := attrDirectives.Columns[attr.Name]; ok {
					old := oldName
					attr.RenameFrom = &old
				}
			}

			compositeType.Ignore = ignore
			if err := setUnique(compositeTypes, compositeType.FQCN(), "composite type", compositeType); err != nil {
				return nil, err
			}

		case node.GetCreateStmt() != nil:
			table, err := parseCreateStmt(node.GetCreateStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				qualified := qualifyRenameFrom(renameFrom, defaultSchema)
				table.RenameFrom = &qualified
			}
			if bulkAlterDirectives[rawStmt.StmtLocation] {
				table.BulkAlter = true
			}

			// Extract column/constraint-level directives from raw SQL
			stmtEnd := rawStmt.StmtLocation + rawStmt.StmtLen
			if stmtEnd > int32(len(sql)) {
				stmtEnd = int32(len(sql))
			}
			rawStmtSQL := sql[rawStmt.StmtLocation:stmtEnd]
			inlineDirectives := extractInlineDirectives(rawStmtSQL)
			for colName, oldName := range inlineDirectives.Columns {
				if col, ok := table.Columns.GetOk(colName); ok {
					old := oldName
					col.RenameFrom = &old
				}
			}
			for conName, oldName := range inlineDirectives.Constraints {
				if con, ok := table.Constraints.GetOk(conName); ok {
					old := oldName
					con.RenameFrom = &old
				} else if fk, ok := table.ForeignKeys.GetOk(conName); ok {
					old := oldName
					fk.RenameFrom = &old
				}
			}

			table.Ignore = ignore
			if err := setUnique(tables, table.FQTN(), "table", table); err != nil {
				return nil, err
			}

		case node.GetViewStmt() != nil:
			view, err := parseViewStmt(node.GetViewStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				qualified := qualifyRenameFrom(renameFrom, defaultSchema)
				view.RenameFrom = &qualified
			}
			view.Ignore = ignore
			if err := setUnique(views, view.FQVN(), "view", view); err != nil {
				return nil, err
			}

		case node.GetCreateTableAsStmt() != nil:
			as := node.GetCreateTableAsStmt()
			if as.Objtype == pg_query.ObjectType_OBJECT_MATVIEW {
				view, err := parseCreateMatViewStmt(as, defaultSchema)
				if err != nil {
					return nil, err
				}
				if renameFrom != "" {
					qualified := qualifyRenameFrom(renameFrom, defaultSchema)
					view.RenameFrom = &qualified
				}
				view.Ignore = ignore
				if err := setUnique(views, view.FQVN(), "materialized view", view); err != nil {
					return nil, err
				}
			}

		case node.GetIndexStmt() != nil:
			idx, err := parseIndexStmt(node.GetIndexStmt(), rawStmt, defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				unquoted := normalizeUnqualifiedDirective(renameFrom)
				idx.RenameFrom = &unquoted
			}
			if concurrentlyDirectives[rawStmt.StmtLocation] {
				idx.Concurrently = true
			}
			fqtn := model.Ident(idx.Schema, idx.Table)
			if t, ok := tables.GetOk(fqtn); ok {
				if err := setUnique(t.Indexes, idx.Name, "index", idx); err != nil {
					return nil, err
				}
			} else if v, ok := views.GetOk(fqtn); ok && v.Materialized {
				if err := setUnique(v.Indexes, idx.Name, "index", idx); err != nil {
					return nil, err
				}
			}

		case node.GetAlterTableStmt() != nil:
			as := node.GetAlterTableStmt()
			schema := as.Relation.Schemaname
			if schema == "" {
				schema = defaultSchema
			}
			fqtn := model.Ident(schema, as.Relation.Relname)
			t, ok := tables.GetOk(fqtn)
			if !ok {
				continue
			}

			// A table marked -- pista:ignore is out of the diff, so an
			// action dropped from it cannot mislead the plan.
			if !t.Ignore {
				warnIgnoredAlterTableCmds(sql, rawStmt, as)
			}

			// RLS toggles, trigger states, column storage and constraint
			// subcommands can coexist in one ALTER TABLE statement. Each
			// helper picks up only its own subtypes and walks the cmd list
			// independently, so run them all.
			applyAlterTableRLS(as, t)
			applyAlterTableTriggerState(as, t)
			applyAlterTableColumnStorage(as, t)

			cons, fks, err := parseAlterTableConstraints(as, defaultSchema)
			if err != nil {
				return nil, err
			}
			// A rename directive names a single old object, so it cannot be
			// applied when one statement declares several constraints.
			if renameFrom != "" && len(cons)+len(fks) > 1 {
				return nil, fmt.Errorf("pista:renamed-from is ambiguous: ALTER TABLE %s adds %d constraints in one statement", fqtn, len(cons)+len(fks))
			}
			for _, fk := range fks {
				if renameFrom != "" {
					unquoted := normalizeUnqualifiedDirective(renameFrom)
					fk.RenameFrom = &unquoted
				}
				if err := setUnique(t.ForeignKeys, fk.Name, "foreign key", fk); err != nil {
					return nil, err
				}
			}
			for _, con := range cons {
				if renameFrom != "" {
					unquoted := normalizeUnqualifiedDirective(renameFrom)
					con.RenameFrom = &unquoted
				}
				if err := setUnique(t.Constraints, con.Name, "constraint", con); err != nil {
					return nil, err
				}
			}

		case node.GetCreatePolicyStmt() != nil:
			policy, err := parseCreatePolicyStmt(node.GetCreatePolicyStmt(), defaultSchema, tables)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				unquoted := normalizeUnqualifiedDirective(renameFrom)
				policy.RenameFrom = &unquoted
			}

		case node.GetCreateTrigStmt() != nil:
			trg, err := parseCreateTrigStmt(node.GetCreateTrigStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				unquoted := normalizeUnqualifiedDirective(renameFrom)
				trg.RenameFrom = &unquoted
			}
			if err := attachTrigger(trg, tables, views); err != nil {
				return nil, err
			}

		case node.GetCreateSeqStmt() != nil:
			seq, err := parseCreateSeqStmt(node.GetCreateSeqStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				qualified := qualifyRenameFrom(renameFrom, defaultSchema)
				seq.RenameFrom = &qualified
			}
			seq.Ignore = ignore
			if err := setUnique(sequences, seq.FQN(), "sequence", seq); err != nil {
				return nil, err
			}

		case node.GetAlterSeqStmt() != nil:
			applyAlterSeqOwnedBy(node.GetAlterSeqStmt(), defaultSchema, sequences)

		case node.GetCreateFunctionStmt() != nil:
			routine, err := parseCreateFunctionStmt(node.GetCreateFunctionStmt(), defaultSchema)
			if errors.Is(err, ErrUnsupportedRoutine) {
				// A routine pistachio reads but does not manage. The catalog
				// skips the same ones, so warning and dropping it here keeps
				// both sides of the diff in step.
				warnIgnoredStmt(sql, rawStmt)
				continue
			}
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				return nil, fmt.Errorf("pista:renamed-from is not supported for routines: %s", routine.FQRN())
			}
			// The parser sets Ignore itself for a routine it reads but cannot
			// compare, so this must not clear it.
			routine.Ignore = routine.Ignore || ignore
			if err := setUnique(routines, routine.FQRN(), "routine", routine); err != nil {
				return nil, err
			}

		case node.GetCommentStmt() != nil:
			cs := node.GetCommentStmt()
			if !commentTargetSupported[cs.Objtype] {
				warnIgnoredStmt(sql, rawStmt)
				break
			}
			parseCommentStmt(cs, defaultSchema, tables, views, enums, domains, compositeTypes, sequences, routines)

		default:
			// A statement type no case above handles. It is dropped from the
			// desired schema, so warn instead of failing silently.
			warnIgnoredStmt(sql, rawStmt)
		}
	}

	if err := validateColumnRefs(tables); err != nil {
		return nil, err
	}

	parsed := &ParseResult{Tables: tables, Views: views, Enums: enums, Domains: domains, CompositeTypes: compositeTypes, Sequences: sequences, Routines: routines, ExecuteStmts: executeStmts}

	if err := validateNamespaces(parsed); err != nil {
		return nil, err
	}

	return parsed, nil
}

func parseCreateStmt(cs *pg_query.CreateStmt, defaultSchema string) (*model.Table, error) {
	schema := cs.Relation.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	table := &model.Table{
		Schema:      schema,
		Name:        cs.Relation.Relname,
		Unlogged:    cs.Relation.Relpersistence == "u",
		Partitioned: cs.Partspec != nil,
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
		Policies:    orderedmap.New[string, *model.Policy](),
		Triggers:    orderedmap.New[string, *model.Trigger](),
	}

	if cs.Tablespacename != "" {
		ts := cs.Tablespacename
		table.TableSpace = &ts
	}

	table.StorageParams = parseStorageParams(cs.Options)

	if cs.Partspec != nil {
		def, err := deparsePartitionSpec(cs)
		if err != nil {
			return nil, err
		}
		table.PartitionDef = &def
	}

	if len(cs.InhRelations) > 0 {
		rv := cs.InhRelations[0].GetRangeVar()
		if rv != nil {
			parentSchema := rv.Schemaname
			if parentSchema == "" {
				parentSchema = defaultSchema
			}
			parent := model.Ident(parentSchema, rv.Relname)
			table.PartitionOf = &parent

			if cs.Partbound != nil {
				bound, err := deparsePartitionBound(cs)
				if err != nil {
					return nil, err
				}
				table.PartitionBound = &bound
			}
		}
	}

	for _, elt := range cs.TableElts {
		switch {
		case elt.GetColumnDef() != nil:
			cd := elt.GetColumnDef()
			col, err := parseColumnDef(cd)
			if err != nil {
				return nil, err
			}
			if err := setUnique(table.Columns, col.Name, "column", col); err != nil {
				return nil, err
			}

			// Extract column-level constraints (PRIMARY KEY, UNIQUE, CHECK, FK).
			if err := extractColumnConstraints(cd, table, schema, defaultSchema); err != nil {
				return nil, err
			}

		case elt.GetConstraint() != nil:
			con := elt.GetConstraint()
			if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
				fk, err := parseInlineForeignKey(con, schema, cs.Relation.Relname, defaultSchema)
				if err != nil {
					return nil, err
				}
				if fk != nil {
					if err := setUnique(table.ForeignKeys, fk.Name, "foreign key", fk); err != nil {
						return nil, err
					}
				}
			} else {
				constraint, err := parseTableConstraint(con, table.Name)
				if err != nil {
					return nil, err
				}
				if constraint != nil {
					if err := setUnique(table.Constraints, constraint.Name, "constraint", constraint); err != nil {
						return nil, err
					}
					// PK implies NOT NULL on all key columns
					if constraint.Type.IsPrimaryKeyConstraint() {
						for _, colName := range constraint.Columns {
							if col, ok := table.Columns.GetOk(colName); ok {
								col.NotNull = true
							}
						}
					}
				}
			}
		}
	}

	return table, nil
}

// collationFromClause builds the canonical collation form (quoted, ready to
// follow COLLATE) from a COLLATE clause. It returns nil for the default
// collation, which the catalog never reports, so writing it explicitly does
// not read as a change.
func collationFromClause(cc *pg_query.CollateClause) *string {
	if cc == nil || len(cc.Collname) == 0 {
		return nil
	}

	var parts []string
	for _, n := range cc.Collname {
		if str := n.GetString_(); str != nil {
			parts = append(parts, str.Sval)
		}
	}
	if len(parts) == 0 || parts[len(parts)-1] == "default" {
		return nil
	}

	collation := model.Ident(parts...)
	return &collation
}

func parseColumnDef(cd *pg_query.ColumnDef) (*model.Column, error) {
	col := &model.Column{
		Name: cd.Colname,
	}

	if cd.TypeName != nil {
		typeName, err := deparseTypeName(cd.TypeName)
		if err != nil {
			return nil, fmt.Errorf("failed to deparse type for column %s: %w", cd.Colname, err)
		}
		col.TypeName = typeName
	}

	col.Collation = collationFromClause(cd.CollClause)
	col.StorageType = normalizeStorageKeyword(cd.StorageName)
	col.Compression = normalizeStorageKeyword(cd.Compression)

	for _, conNode := range cd.Constraints {
		con := conNode.GetConstraint()
		if con == nil {
			continue
		}
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_NOTNULL:
			col.NotNull = true
			if con.Conname != "" {
				name := con.Conname
				col.NotNullName = &name
			}
		case pg_query.ConstrType_CONSTR_DEFAULT:
			if con.RawExpr != nil {
				def, err := deparseExpr(con.RawExpr)
				if err != nil {
					return nil, fmt.Errorf("failed to deparse default for column %s: %w", cd.Colname, err)
				}
				col.Default = &def
			}
		case pg_query.ConstrType_CONSTR_IDENTITY:
			switch con.GeneratedWhen {
			case "a":
				col.Identity = model.ColumnIdentity('a')
			case "d":
				col.Identity = model.ColumnIdentity('d')
			}
			if col.Identity.IsIdentityColumn() {
				// PostgreSQL takes the sequence type from the column and
				// rejects an AS option here, so the bounds resolve against the
				// column type.
				p, err := parseSeqOptions(con.Options, col.TypeName)
				if err != nil {
					return nil, fmt.Errorf("failed to parse identity options for column %s: %w", cd.Colname, err)
				}
				col.IdentitySeq = &model.IdentitySequence{
					Start:     p.start,
					Min:       p.min,
					Max:       p.max,
					Increment: p.increment,
					Cache:     p.cache,
					Cycle:     p.cycle,
				}
			}
		case pg_query.ConstrType_CONSTR_GENERATED:
			// pg_query reports GeneratedWhen="a" (ALWAYS) for STORED generated
			// columns. PostgreSQL only supports STORED at this time, so any
			// CONSTR_GENERATED implies stored. Map to the catalog form ('s').
			col.Generated = model.ColumnGenerated('s')
			if con.RawExpr != nil {
				def, err := deparseExpr(con.RawExpr)
				if err != nil {
					return nil, fmt.Errorf("failed to deparse generated expr for column %s: %w", cd.Colname, err)
				}
				col.Default = &def
			}
		}
	}

	return col, nil
}

// normalizeStorageKeyword lowercases a STORAGE strategy or a COMPRESSION
// method and reads DEFAULT as none, which is how the catalog reports a column
// that carries neither.
func normalizeStorageKeyword(name string) string {
	if strings.EqualFold(name, "default") {
		return ""
	}
	return strings.ToLower(name)
}

// applyAlterTableColumnStorage reads the storage and compression actions onto
// the columns they name. pg_dump writes both as separate statements, so a file
// adopted from one carries them here rather than in the column definition.
func applyAlterTableColumnStorage(as *pg_query.AlterTableStmt, t *model.Table) {
	for _, cmdNode := range as.Cmds {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil {
			continue
		}
		if cmd.Subtype != pg_query.AlterTableType_AT_SetStorage &&
			cmd.Subtype != pg_query.AlterTableType_AT_SetCompression {
			continue
		}
		// A setting for a column the file does not declare has nothing to sit
		// on, the same as a trigger state on a table declared elsewhere.
		col, ok := t.Columns.GetOk(cmd.Name)
		if !ok {
			continue
		}
		value := normalizeStorageKeyword(cmd.Def.GetString_().GetSval())
		if cmd.Subtype == pg_query.AlterTableType_AT_SetStorage {
			col.StorageType = value
		} else {
			col.Compression = value
		}
	}
}

// nameDataLen mirrors PostgreSQL's NAMEDATALEN. An identifier holds at most
// nameDataLen-1 bytes.
const nameDataLen = 64

// clipIdent returns the longest prefix of s that is at most n bytes long and
// does not split a character, the way PostgreSQL's pg_mbcliplen does.
func clipIdent(s string, n int) string {
	if n >= len(s) {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// makeObjectName mirrors PostgreSQL's makeObjectName
// (src/backend/catalog/catalog.c). It joins name1, name2 and label with
// underscores, shortening name1 and name2 - the longer one first - until the
// whole name fits in nameDataLen-1 bytes. The label is never shortened, so a
// long name keeps its _pkey / _check / ... suffix instead of losing it to a
// plain truncation of the joined string. An empty name2 stands for the NULL
// PostgreSQL passes when the name has no column part; label is always given,
// and is short enough to leave room for the rest, which is what PostgreSQL
// asserts there.
func makeObjectName(name1, name2, label string) string {
	overhead := len(label) + 1
	if name2 != "" {
		overhead++ // separating underscore
	}

	name1chars := len(name1)
	name2chars := len(name2)
	availchars := nameDataLen - 1 - overhead

	for name1chars+name2chars > availchars {
		if name1chars > name2chars {
			name1chars--
		} else {
			name2chars--
		}
	}

	name := clipIdent(name1, name1chars)
	if name2 != "" {
		name += "_" + clipIdent(name2, name2chars)
	}

	return name + "_" + label
}

// autoNameConstraint generates a PostgreSQL-style constraint name for unnamed
// constraints, following the naming convention from PostgreSQL's
// ChooseConstraintName (src/backend/catalog/pg_constraint.c):
//
//	PRIMARY KEY -> {table}_pkey
//	UNIQUE      -> {table}_{col}..._key
//	CHECK       -> {table}_{col}_check (or {table}_check)
//	EXCLUSION   -> {table}_{col}..._excl
//	FOREIGN KEY -> {table}_{col}..._fkey
//
// cols holds every column the constraint keys on, in order, joined with an
// underscore the way PostgreSQL joins them. PRIMARY KEY carries no column, and
// a CHECK carries one only when its expression references exactly one, so both
// take an empty list in the other cases.
//
// A name that does not fit in an identifier is shortened by makeObjectName the
// way PostgreSQL shortens it. Without that the server would truncate the name
// it is handed instead, to something the next run no longer recognises.
//
// Duplicate name resolution is NOT handled: PostgreSQL appends a number to the
// second name (e.g. users_id_check1), which cannot be predicted from the
// desired schema alone, so a file that generates one name twice is rejected as
// a duplicate constraint name.
func autoNameConstraint(tableName string, cols []string, contype pg_query.ConstrType) string {
	colPart := strings.Join(cols, "_")
	switch contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		return makeObjectName(tableName, "", "pkey")
	case pg_query.ConstrType_CONSTR_UNIQUE:
		return makeObjectName(tableName, colPart, "key")
	case pg_query.ConstrType_CONSTR_CHECK:
		return makeObjectName(tableName, colPart, "check")
	case pg_query.ConstrType_CONSTR_EXCLUSION:
		return makeObjectName(tableName, colPart, "excl")
	case pg_query.ConstrType_CONSTR_FOREIGN:
		return makeObjectName(tableName, colPart, "fkey")
	default:
		return ""
	}
}

// figureIndexColname picks the name PostgreSQL gives an index element written
// as an expression, following FigureIndexColname and FigureColnameInternal
// (src/backend/parser/parse_target.c). The second return is PostgreSQL's
// strength: a name found deeper in the tree wins over one an enclosing cast or
// CASE falls back to, so `(a + b)::text` is named after the type while
// `coalesce(a, b)::text` keeps the function name. An empty name means
// PostgreSQL finds none and the element is called "expr".
//
// The node kinds below are the ones an index expression realistically holds.
// Anything else takes "expr", which is also what PostgreSQL does for every
// kind it has no name for.
func figureIndexColname(node *pg_query.Node) (string, int) {
	if node == nil {
		return "", 0
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_ColumnRef:
		return lastNodeName(n.ColumnRef.Fields), 2
	case *pg_query.Node_FuncCall:
		return lastNodeName(n.FuncCall.Funcname), 2
	case *pg_query.Node_AExpr:
		// NULLIF is written like a function and named like one.
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_NULLIF {
			return "nullif", 2
		}
	case *pg_query.Node_CoalesceExpr:
		return "coalesce", 2
	case *pg_query.Node_MinMaxExpr:
		if n.MinMaxExpr.Op == pg_query.MinMaxOp_IS_LEAST {
			return "least", 2
		}
		return "greatest", 2
	case *pg_query.Node_AArrayExpr:
		return "array", 2
	case *pg_query.Node_CaseExpr:
		if name, strength := figureIndexColname(n.CaseExpr.Defresult); strength > 1 {
			return name, strength
		}
		return "case", 1
	case *pg_query.Node_TypeCast:
		if name, strength := figureIndexColname(n.TypeCast.Arg); strength > 1 {
			return name, strength
		}
		if n.TypeCast.TypeName != nil {
			if name := lastNodeName(n.TypeCast.TypeName.Names); name != "" {
				return name, 1
			}
		}
	case *pg_query.Node_CollateClause:
		return figureIndexColname(n.CollateClause.Arg)
	case *pg_query.Node_AIndirection:
		// A field selection is named after the field. A subscript carries no
		// name of its own, so the argument is named instead.
		if name := lastNodeName(n.AIndirection.Indirection); name != "" {
			return name, 2
		}
		return figureIndexColname(n.AIndirection.Arg)
	}

	return "", 0
}

// lastNodeName returns the last name in a dotted or subscripted list, skipping
// subscripts and `*` the way FigureColnameInternal does. A list holding no name
// at all, such as a bare subscript, gives the empty string, which is what lets
// the A_Indirection case fall back to the argument.
func lastNodeName(nodes []*pg_query.Node) string {
	var name string
	for _, node := range nodes {
		if s := node.GetString_(); s != nil {
			name = s.Sval
		}
	}
	return name
}

// chooseIndexColumnNames returns one name per index element, the key columns
// first and then the INCLUDE list, following ChooseIndexColumnNames
// (src/backend/commands/indexcmds.c). A name repeated within one index takes a
// number, so (lower(a), lower(b)) reads lower_lower1.
func chooseIndexColumnNames(is *pg_query.IndexStmt) []string {
	var names []string

	taken := func(name string) bool {
		return slices.Contains(names, name)
	}

	for _, params := range [][]*pg_query.Node{is.IndexParams, is.IndexIncludingParams} {
		for _, node := range params {
			ie := node.GetIndexElem()

			origname := ie.GetName()
			if origname == "" {
				origname, _ = figureIndexColname(ie.GetExpr())
				if origname == "" {
					origname = "expr"
				}
			}

			curname := origname
			for i := 1; taken(curname); i++ {
				curname = origname + strconv.Itoa(i)
			}
			names = append(names, curname)
		}
	}

	return names
}

// autoNameIndex generates a PostgreSQL-style name for an index written without
// one, following ChooseRelationName (src/backend/commands/indexcmds.c):
//
//	{table}_{col}..._idx
//
// Without this the index reaches the diff with an empty name, which matches no
// index in the database, so every run creates the index again and PostgreSQL
// numbers each copy.
//
// Duplicate name resolution is NOT handled, as for constraints: PostgreSQL
// appends a number when the name is already taken in the schema, which cannot
// be predicted from the desired schema alone, so a file that generates one
// name twice is rejected as a duplicate index name.
func autoNameIndex(is *pg_query.IndexStmt) string {
	return makeObjectName(is.Relation.Relname, strings.Join(chooseIndexColumnNames(is), "_"), "idx")
}

// constraintKeyCols returns the columns a constraint keys on, in order. Keys
// holds them for PRIMARY KEY and UNIQUE; EXCLUDE keeps its elements in
// Exclusions instead, where PostgreSQL stands "expr" in for an element that is
// an expression rather than a column.
func constraintKeyCols(con *pg_query.Constraint) []string {
	var cols []string
	for _, k := range con.Keys {
		if s := k.GetString_(); s != nil {
			cols = append(cols, s.Sval)
		}
	}
	for _, ex := range con.Exclusions {
		list := ex.GetList()
		if list == nil {
			continue
		}
		for _, item := range list.Items {
			ie := item.GetIndexElem()
			if ie == nil {
				continue
			}
			if ie.Name != "" {
				cols = append(cols, ie.Name)
			} else {
				cols = append(cols, "expr")
			}
		}
	}
	return cols
}

// fkAttrCols returns the local-side columns of a foreign key, in order.
func fkAttrCols(con *pg_query.Constraint) []string {
	var cols []string
	for _, attr := range con.FkAttrs {
		if s := attr.GetString_(); s != nil {
			cols = append(cols, s.Sval)
		}
	}
	return cols
}

// checkExprCols returns the single column a CHECK expression references, or nil
// when it references none or several. PostgreSQL names such a constraint after
// the column only in the single-column case, and it reads the expression even
// for a constraint written on a column, so `a integer CHECK (a > b)` becomes
// {table}_check rather than {table}_a_check.
//
// walkExprColumnRefs does not descend into every expression node, so an exotic
// CHECK can come back empty and take the {table}_check form where PostgreSQL
// would name the column.
func checkExprCols(expr *pg_query.Node) []string {
	seen := map[string]bool{}
	var cols []string
	for _, ref := range walkExprColumnRefs(expr) {
		if seen[ref] {
			continue
		}
		seen[ref] = true
		cols = append(cols, ref)
	}
	if len(cols) != 1 {
		return nil
	}
	return cols
}

// autoNameColumnConstraint names an unnamed constraint written on a column.
func autoNameColumnConstraint(tableName, colName string, con *pg_query.Constraint) string {
	if con.Contype == pg_query.ConstrType_CONSTR_CHECK {
		return autoNameConstraint(tableName, checkExprCols(con.RawExpr), con.Contype)
	}
	return autoNameConstraint(tableName, []string{colName}, con.Contype)
}

// extractColumnConstraints extracts named constraints from a column definition
// (e.g. PRIMARY KEY, UNIQUE, CHECK, EXCLUSION, FOREIGN KEY) and adds them to
// the table. Column-attribute constraints (NOT NULL, DEFAULT, IDENTITY,
// GENERATED) are skipped as they are handled by parseColumnDef.
// Unnamed constraints are auto-named following PostgreSQL's naming convention.
func extractColumnConstraints(cd *pg_query.ColumnDef, table *model.Table, schema, defaultSchema string) error {
	for _, conNode := range cd.Constraints {
		con := conNode.GetConstraint()
		if con == nil {
			continue
		}
		// Skip column-attribute constraints (NOT NULL, DEFAULT, IDENTITY, GENERATED)
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_NOTNULL, pg_query.ConstrType_CONSTR_DEFAULT,
			pg_query.ConstrType_CONSTR_IDENTITY, pg_query.ConstrType_CONSTR_GENERATED:
			continue
		}
		if con.Conname == "" {
			con.Conname = autoNameColumnConstraint(table.Name, cd.Colname, con)
		}
		// Column-level PK/UNIQUE/EXCLUSION have no Keys; fill in the column name.
		// CHECK constraints do not use Keys (they reference columns via the expression).
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_PRIMARY:
			if len(con.Keys) == 0 {
				con.Keys = []*pg_query.Node{pg_query.MakeStrNode(cd.Colname)}
			}
			// PK implies NOT NULL
			if col, ok := table.Columns.GetOk(cd.Colname); ok {
				col.NotNull = true
			}
		case pg_query.ConstrType_CONSTR_UNIQUE, pg_query.ConstrType_CONSTR_EXCLUSION:
			if len(con.Keys) == 0 {
				con.Keys = []*pg_query.Node{pg_query.MakeStrNode(cd.Colname)}
			}
		}
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_FOREIGN:
			// Column-level FK has no FkAttrs; fill in the owning column name.
			if len(con.FkAttrs) == 0 {
				con.FkAttrs = []*pg_query.Node{pg_query.MakeStrNode(cd.Colname)}
			}
			fk, err := parseInlineForeignKey(con, schema, table.Name, defaultSchema)
			if err != nil {
				return err
			}
			if fk != nil {
				if err := setUnique(table.ForeignKeys, fk.Name, "foreign key", fk); err != nil {
					return err
				}
			}
		case pg_query.ConstrType_CONSTR_PRIMARY, pg_query.ConstrType_CONSTR_UNIQUE,
			pg_query.ConstrType_CONSTR_CHECK, pg_query.ConstrType_CONSTR_EXCLUSION:
			constraint, err := parseTableConstraint(con, table.Name)
			if err != nil {
				return err
			}
			if constraint != nil {
				if err := setUnique(table.Constraints, constraint.Name, "constraint", constraint); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func parseTableConstraint(con *pg_query.Constraint, tableName string) (*model.Constraint, error) {
	if con.Conname == "" {
		cols := constraintKeyCols(con)
		if con.Contype == pg_query.ConstrType_CONSTR_CHECK {
			cols = checkExprCols(con.RawExpr)
		}
		con.Conname = autoNameConstraint(tableName, cols, con.Contype)
	}

	var conType model.ConstraintType
	switch con.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		conType = model.ConstraintType('p')
	case pg_query.ConstrType_CONSTR_UNIQUE:
		conType = model.ConstraintType('u')
	case pg_query.ConstrType_CONSTR_CHECK:
		conType = model.ConstraintType('c')
	case pg_query.ConstrType_CONSTR_EXCLUSION:
		conType = model.ConstraintType('x')
	default:
		return nil, nil
	}

	def, err := deparseConstraintDef(con)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse constraint %s: %w", con.Conname, err)
	}

	var columns []string
	for _, k := range con.Keys {
		if s := k.GetString_(); s != nil {
			columns = append(columns, s.Sval)
		}
	}

	return &model.Constraint{
		Name:       con.Conname,
		Type:       conType,
		Definition: def,
		Columns:    columns,
		Deferrable: con.Deferrable,
		Deferred:   con.Initdeferred,
		Validated:  !con.SkipValidation,
	}, nil
}

func parseIndexStmt(is *pg_query.IndexStmt, rawStmt *pg_query.RawStmt, defaultSchema string) (*model.Index, error) {
	schema := is.Relation.Schemaname
	if schema == "" {
		schema = defaultSchema
		// Qualify the relation with the default schema before deparsing
		// so the Definition contains the fully-qualified table name.
		is.Relation.Schemaname = defaultSchema
	}

	// Capture and clear the Concurrent flag before deparsing so the stored
	// Definition is canonical (without CONCURRENTLY). Whether to emit
	// CONCURRENTLY is decided per-operation via Index.Concurrently, which
	// keeps HasConcurrently tracking and --disable-index-concurrently
	// accurate even when input SQL uses CREATE INDEX CONCURRENTLY directly.
	concurrent := is.Concurrent
	is.Concurrent = false

	// Name the index before deparsing so the stored Definition carries the
	// name PostgreSQL would have picked.
	if is.Idxname == "" {
		is.Idxname = autoNameIndex(is)
	}

	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: rawStmt.Stmt}},
	}
	def, err := pg_query.Deparse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse index: %w", err)
	}

	var tablespace *string
	if is.TableSpace != "" {
		ts := is.TableSpace
		tablespace = &ts
	}

	return &model.Index{
		Schema:       schema,
		Name:         is.Idxname,
		Table:        is.Relation.Relname,
		Definition:   def,
		TableSpace:   tablespace,
		Concurrently: concurrent,
	}, nil
}

func parseViewStmt(vs *pg_query.ViewStmt, defaultSchema string) (*model.View, error) {
	schema := vs.View.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	// Deparse the SELECT query
	selectResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: vs.Query,
		}},
	}
	def, err := pg_query.Deparse(selectResult)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse view query: %w", err)
	}

	return &model.View{
		Schema:     schema,
		Name:       vs.View.Relname,
		Definition: def,
		Indexes:    orderedmap.New[string, *model.Index](),
		Triggers:   orderedmap.New[string, *model.Trigger](),
	}, nil
}

func parseCreateMatViewStmt(as *pg_query.CreateTableAsStmt, defaultSchema string) (*model.View, error) {
	into := as.Into
	if into == nil || into.Rel == nil {
		return nil, fmt.Errorf("materialized view has no target relation")
	}

	schema := into.Rel.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	// Deparse the SELECT query
	selectResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: as.Query,
		}},
	}
	def, err := pg_query.Deparse(selectResult)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse materialized view query: %w", err)
	}

	return &model.View{
		Schema:       schema,
		Name:         into.Rel.Relname,
		Definition:   def,
		Materialized: true,
		Indexes:      orderedmap.New[string, *model.Index](),
		Triggers:     orderedmap.New[string, *model.Trigger](),
	}, nil
}

func parseCreateDomainStmt(ds *pg_query.CreateDomainStmt, defaultSchema string) (*model.Domain, error) {
	schema := defaultSchema
	name := ""

	for i, n := range ds.Domainname {
		if s := n.GetString_(); s != nil {
			if i == len(ds.Domainname)-1 {
				name = s.Sval
			} else {
				schema = s.Sval
			}
		}
	}

	// Parse the base type
	baseType := ""
	if ds.TypeName != nil {
		bt, err := deparseTypeName(ds.TypeName)
		if err != nil {
			return nil, fmt.Errorf("failed to deparse base type for domain %s: %w", name, err)
		}
		baseType = bt
	}

	domain := &model.Domain{
		Schema:   schema,
		Name:     name,
		BaseType: baseType,
	}

	// Extract collation
	domain.Collation = collationFromClause(ds.CollClause)

	// Extract constraints from deparsed SQL
	// Parse the deparsed statement to get normalized constraints
	for _, conNode := range ds.Constraints {
		con := conNode.GetConstraint()
		if con == nil {
			continue
		}
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_NOTNULL:
			domain.NotNull = true
		case pg_query.ConstrType_CONSTR_DEFAULT:
			if con.RawExpr != nil {
				def, err := deparseExpr(con.RawExpr)
				if err != nil {
					return nil, fmt.Errorf("failed to deparse default for domain %s: %w", name, err)
				}
				domain.Default = &def
			}
		case pg_query.ConstrType_CONSTR_CHECK:
			if con.Conname == "" {
				con.Conname = makeObjectName(name, "", "check")
			}
			def := ""
			if con.RawExpr != nil {
				expr, err := deparseExpr(con.RawExpr)
				if err != nil {
					return nil, fmt.Errorf("failed to deparse constraint %s for domain %s: %w", con.Conname, name, err)
				}
				def = "CHECK (" + expr + ")"
			}
			domain.Constraints = append(domain.Constraints, &model.DomainConstraint{
				Name:       con.Conname,
				Definition: def,
				Validated:  !con.SkipValidation,
			})
		}
	}

	return domain, nil
}

func parseCompositeTypeStmt(cts *pg_query.CompositeTypeStmt, defaultSchema string) (*model.CompositeType, error) {
	schema := defaultSchema
	name := ""
	if cts.Typevar != nil {
		name = cts.Typevar.Relname
		if cts.Typevar.Schemaname != "" {
			schema = cts.Typevar.Schemaname
		}
	}

	compositeType := &model.CompositeType{
		Schema: schema,
		Name:   name,
	}

	for _, node := range cts.Coldeflist {
		cd := node.GetColumnDef()
		if cd == nil {
			continue
		}

		typeName := ""
		if cd.TypeName != nil {
			tn, err := deparseTypeName(cd.TypeName)
			if err != nil {
				return nil, fmt.Errorf("failed to deparse attribute type for composite type %s: %w", name, err)
			}
			typeName = tn
		}

		attr := &model.CompositeAttribute{
			Name:     cd.Colname,
			TypeName: typeName,
		}

		attr.Collation = collationFromClause(cd.CollClause)

		compositeType.Attributes = append(compositeType.Attributes, attr)
	}

	return compositeType, nil
}

func parseCommentOnDomain(cs *pg_query.CommentStmt, defaultSchema string, domains *orderedmap.Map[string, *model.Domain]) {
	tn := cs.Object.GetTypeName()
	if tn == nil {
		return
	}
	var names []string
	for _, n := range tn.Names {
		if s := n.GetString_(); s != nil {
			names = append(names, s.Sval)
		}
	}
	if len(names) == 0 {
		return
	}
	schema := defaultSchema
	domainName := names[0]
	if len(names) >= 2 {
		schema = names[0]
		domainName = names[1]
	}
	fqdn := model.Ident(schema, domainName)
	if d, ok := domains.GetOk(fqdn); ok {
		if cs.Comment != "" {
			c := cs.Comment
			d.Comment = &c
		} else {
			d.Comment = nil
		}
	}
}

func parseCreateEnumStmt(es *pg_query.CreateEnumStmt, defaultSchema string) (*model.Enum, error) {
	schema := defaultSchema
	name := ""

	for i, n := range es.TypeName {
		if s := n.GetString_(); s != nil {
			if i == len(es.TypeName)-1 {
				name = s.Sval
			} else {
				schema = s.Sval
			}
		}
	}

	var values []string
	for _, v := range es.Vals {
		if s := v.GetString_(); s != nil {
			values = append(values, s.Sval)
		}
	}

	return &model.Enum{
		Schema: schema,
		Name:   name,
		Values: values,
	}, nil
}

// seqParams holds a sequence option list after PostgreSQL's implicit defaults
// have been filled in.
type seqParams struct {
	dataType    string
	start       int64
	min         int64
	max         int64
	increment   int64
	cache       int64
	cycle       bool
	ownerTable  *string
	ownerColumn *string
}

// parseSeqOptions reads a sequence option list, the one CREATE SEQUENCE takes
// and the one an identity column's sequence_options gives, and fills in the
// same implicit defaults PostgreSQL applies (so a bare "CREATE SEQUENCE s"
// matches the catalog values 1/1/2^63-1/1/1/false). NO MINVALUE and NO MAXVALUE
// (arg nil) are treated as "use the default", matching PostgreSQL. dataType is
// the type to resolve the bounds against, and an AS option overrides it.
func parseSeqOptions(options []*pg_query.Node, dataType string) (*seqParams, error) {
	var (
		increment                int64 = 1
		hasMin, hasMax, hasStart bool
		minVal, maxVal, startVal int64
		cacheVal                 int64 = 1
		cycle                    bool
		ownerTable, ownerColumn  *string
	)

	for _, o := range options {
		de := o.GetDefElem()
		if de == nil {
			continue
		}
		switch de.Defname {
		case "as":
			if tn := de.Arg.GetTypeName(); tn != nil {
				dt, err := deparseTypeName(tn)
				if err != nil {
					return nil, fmt.Errorf("failed to deparse sequence data type: %w", err)
				}
				dataType = dt
			}
		case "increment":
			v, ok, err := defElemInt64(de)
			if err != nil {
				return nil, err
			}
			if ok {
				increment = v
			}
		case "minvalue":
			v, ok, err := defElemInt64(de)
			if err != nil {
				return nil, err
			}
			if ok {
				minVal = v
				hasMin = true
			}
		case "maxvalue":
			v, ok, err := defElemInt64(de)
			if err != nil {
				return nil, err
			}
			if ok {
				maxVal = v
				hasMax = true
			}
		case "start":
			v, ok, err := defElemInt64(de)
			if err != nil {
				return nil, err
			}
			if ok {
				startVal = v
				hasStart = true
			}
		case "cache":
			v, ok, err := defElemInt64(de)
			if err != nil {
				return nil, err
			}
			if ok {
				cacheVal = v
			}
		case "cycle":
			if b := de.Arg.GetBoolean(); b != nil {
				cycle = b.Boolval
			}
		case "owned_by":
			ownerTable, ownerColumn = parseSeqOwnedBy(de.Arg)
		}
	}

	// Apply PostgreSQL's defaults for any options left unspecified.
	def := model.DefaultIdentitySequence(dataType, increment)
	if !hasMin {
		minVal = def.Min
	}
	if !hasMax {
		maxVal = def.Max
	}
	if !hasStart {
		if increment > 0 {
			startVal = minVal
		} else {
			startVal = maxVal
		}
	}

	return &seqParams{
		dataType:    dataType,
		start:       startVal,
		min:         minVal,
		max:         maxVal,
		increment:   increment,
		cache:       cacheVal,
		cycle:       cycle,
		ownerTable:  ownerTable,
		ownerColumn: ownerColumn,
	}, nil
}

// parseCreateSeqStmt parses a CREATE SEQUENCE statement.
func parseCreateSeqStmt(cs *pg_query.CreateSeqStmt, defaultSchema string) (*model.Sequence, error) {
	schema := cs.Sequence.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	p, err := parseSeqOptions(cs.Options, "bigint")
	if err != nil {
		return nil, err
	}

	return &model.Sequence{
		Schema:      schema,
		Name:        cs.Sequence.Relname,
		DataType:    p.dataType,
		Start:       p.start,
		Min:         p.min,
		Max:         p.max,
		Increment:   p.increment,
		Cache:       p.cache,
		Cycle:       p.cycle,
		OwnerTable:  p.ownerTable,
		OwnerColumn: p.ownerColumn,
	}, nil
}

// defElemInt64 reads an integer sequence option. pg_query encodes values that
// fit in int32 as Integer nodes and larger values (e.g. bigint bounds) as
// Float nodes carrying the decimal string. Returns ok=false when the arg is
// nil (NO MINVALUE / NO MAXVALUE).
func defElemInt64(de *pg_query.DefElem) (int64, bool, error) {
	if de.Arg == nil {
		return 0, false, nil
	}
	if i := de.Arg.GetInteger(); i != nil {
		return int64(i.Ival), true, nil
	}
	if f := de.Arg.GetFloat(); f != nil {
		v, err := strconv.ParseInt(f.Fval, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("invalid value %q for sequence option %s: %w", f.Fval, de.Defname, err)
		}
		return v, true, nil
	}
	return 0, false, fmt.Errorf("unexpected value for sequence option %s", de.Defname)
}

// applyAlterSeqOwnedBy records an ALTER SEQUENCE OWNED BY clause on the
// already-parsed sequence, marking it unmanaged. The catalog excludes owned
// sequences, so without this every plan proposes creating a sequence that
// already exists. Other ALTER SEQUENCE options are not tracked.
func applyAlterSeqOwnedBy(as *pg_query.AlterSeqStmt, defaultSchema string, sequences *orderedmap.Map[string, *model.Sequence]) {
	if as.Sequence == nil {
		return
	}
	schema := as.Sequence.Schemaname
	if schema == "" {
		schema = defaultSchema
	}
	seq, ok := sequences.GetOk(model.Ident(schema, as.Sequence.Relname))
	if !ok {
		return
	}
	for _, opt := range as.Options {
		de := opt.GetDefElem()
		if de == nil || de.Defname != "owned_by" {
			continue
		}
		seq.OwnerTable, seq.OwnerColumn = parseSeqOwnedBy(de.Arg)
	}
}

// parseSeqOwnedBy extracts the owner table and column from an OWNED BY clause.
// OWNED BY NONE (list ["none"]) yields nil owner. The pipeline only manages
// standalone sequences, so an owned sequence is filtered out downstream; the
// values here just mark it as owned.
func parseSeqOwnedBy(arg *pg_query.Node) (*string, *string) {
	list := arg.GetList()
	if list == nil {
		return nil, nil
	}
	var names []string
	for _, item := range list.Items {
		if s := item.GetString_(); s != nil {
			names = append(names, s.Sval)
		}
	}
	if len(names) == 1 && strings.EqualFold(names[0], "none") {
		return nil, nil
	}
	if len(names) < 2 {
		return nil, nil
	}
	table := names[len(names)-2]
	column := names[len(names)-1]
	return &table, &column
}

func parseCommentStmt(cs *pg_query.CommentStmt, defaultSchema string, tables *orderedmap.Map[string, *model.Table], views *orderedmap.Map[string, *model.View], enums *orderedmap.Map[string, *model.Enum], domains *orderedmap.Map[string, *model.Domain], compositeTypes *orderedmap.Map[string, *model.CompositeType], sequences *orderedmap.Map[string, *model.Sequence], routines *orderedmap.Map[string, *model.Routine]) {
	// COMMENT ON TYPE/DOMAIN uses TypeName, not a list
	if cs.Objtype == pg_query.ObjectType_OBJECT_TYPE {
		parseCommentOnType(cs, defaultSchema, enums, compositeTypes)
		return
	}
	if cs.Objtype == pg_query.ObjectType_OBJECT_DOMAIN {
		parseCommentOnDomain(cs, defaultSchema, domains)
		return
	}
	// COMMENT ON FUNCTION/PROCEDURE carries an ObjectWithArgs, not a list.
	if cs.Objtype == pg_query.ObjectType_OBJECT_FUNCTION || cs.Objtype == pg_query.ObjectType_OBJECT_PROCEDURE {
		parseCommentOnRoutine(cs, defaultSchema, routines)
		return
	}

	items := cs.Object.GetList().GetItems()
	if len(items) == 0 {
		return
	}

	var names []string
	for _, item := range items {
		if s := item.GetString_(); s != nil {
			names = append(names, s.Sval)
		}
	}

	switch cs.Objtype {
	case pg_query.ObjectType_OBJECT_TABLE:
		schema := defaultSchema
		tableName := names[0]
		if len(names) >= 2 {
			schema = names[0]
			tableName = names[1]
		}
		fqtn := model.Ident(schema, tableName)
		if t, ok := tables.GetOk(fqtn); ok {
			if cs.Comment != "" {
				c := cs.Comment
				t.Comment = &c
			} else {
				t.Comment = nil
			}
		}
	case pg_query.ObjectType_OBJECT_VIEW, pg_query.ObjectType_OBJECT_MATVIEW:
		schema := defaultSchema
		viewName := names[0]
		if len(names) >= 2 {
			schema = names[0]
			viewName = names[1]
		}
		fqvn := model.Ident(schema, viewName)
		if v, ok := views.GetOk(fqvn); ok {
			if cs.Comment != "" {
				c := cs.Comment
				v.Comment = &c
			} else {
				v.Comment = nil
			}
		}
	case pg_query.ObjectType_OBJECT_COLUMN:
		if len(names) < 2 {
			return
		}
		schema := defaultSchema
		tableName := names[0]
		colName := names[1]
		if len(names) >= 3 {
			schema = names[0]
			tableName = names[1]
			colName = names[2]
		}
		fqtn := model.Ident(schema, tableName)
		var comment *string
		if cs.Comment != "" {
			c := cs.Comment
			comment = &c
		}
		if t, ok := tables.GetOk(fqtn); ok {
			if col, ok := t.Columns.GetOk(colName); ok {
				col.Comment = comment
				return
			}
			// A partition child declares no columns of its own, but comments
			// are per-relation and can be set on an inherited column. Record
			// the comment against a column entry so the diff can see it.
			// Restricted to true partition children: an INHERITS-style child
			// still goes through the regular column diff, where a bodyless
			// entry would be mistaken for a new column.
			if t.IsPartitionChild() {
				t.Columns.Set(colName, &model.Column{Name: colName, Comment: comment})
			}
			return
		}
		// COMMENT ON COLUMN also targets composite type attributes
		// (schema.type.attribute).
		if ct, ok := compositeTypes.GetOk(fqtn); ok {
			for _, attr := range ct.Attributes {
				if attr.Name == colName {
					attr.Comment = comment
					break
				}
			}
		}
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		schema := defaultSchema
		seqName := names[0]
		if len(names) >= 2 {
			schema = names[0]
			seqName = names[1]
		}
		fqn := model.Ident(schema, seqName)
		if seq, ok := sequences.GetOk(fqn); ok {
			if cs.Comment != "" {
				c := cs.Comment
				seq.Comment = &c
			} else {
				seq.Comment = nil
			}
		}
	}
}

func parseCommentOnType(cs *pg_query.CommentStmt, defaultSchema string, enums *orderedmap.Map[string, *model.Enum], compositeTypes *orderedmap.Map[string, *model.CompositeType]) {
	tn := cs.Object.GetTypeName()
	if tn == nil {
		return
	}
	var names []string
	for _, n := range tn.Names {
		if s := n.GetString_(); s != nil {
			names = append(names, s.Sval)
		}
	}
	if len(names) == 0 {
		return
	}
	schema := defaultSchema
	typeName := names[0]
	if len(names) >= 2 {
		schema = names[0]
		typeName = names[1]
	}
	// COMMENT ON TYPE names both enums and composite types; set on whichever
	// this file defines.
	fqn := model.Ident(schema, typeName)
	var comment *string
	if cs.Comment != "" {
		c := cs.Comment
		comment = &c
	}
	if e, ok := enums.GetOk(fqn); ok {
		e.Comment = comment
	}
	if ct, ok := compositeTypes.GetOk(fqn); ok {
		ct.Comment = comment
	}
}

// parseAlterTableConstraints collects every ADD CONSTRAINT subcommand of an
// ALTER TABLE statement. PostgreSQL accepts comma-separated actions in one
// statement, so a single statement can declare several constraints.
func parseAlterTableConstraints(as *pg_query.AlterTableStmt, defaultSchema string) ([]*model.Constraint, []*model.ForeignKey, error) {
	var constraints []*model.Constraint
	var fks []*model.ForeignKey

	for _, cmdNode := range as.Cmds {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil || cmd.Subtype != pg_query.AlterTableType_AT_AddConstraint {
			continue
		}
		con := cmd.Def.GetConstraint()
		if con == nil {
			continue
		}

		schema := as.Relation.Schemaname
		if schema == "" {
			schema = defaultSchema
		}

		// A foreign key added here does not pass through
		// parseInlineForeignKey, so name it before the definition is
		// deparsed. Without this an unnamed one reaches the diff with an
		// empty name and plans as `ADD CONSTRAINT  FOREIGN KEY ...`.
		if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN && con.Conname == "" {
			con.Conname = autoNameConstraint(as.Relation.Relname, fkAttrCols(con), con.Contype)
		}

		def, err := deparseConstraintDef(con)
		if err != nil {
			return nil, nil, err
		}

		if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
			var refSchema, refTable *string
			if con.Pktable != nil {
				rs := con.Pktable.Schemaname
				if rs == "" {
					rs = defaultSchema
				}
				refSchema = &rs
				rt := con.Pktable.Relname
				refTable = &rt
			}

			var columns []string
			for _, attr := range con.FkAttrs {
				if s := attr.GetString_(); s != nil {
					columns = append(columns, s.Sval)
				}
			}

			fks = append(fks, &model.ForeignKey{
				Constraint: model.Constraint{
					Name:       con.Conname,
					Type:       model.ConstraintType('f'),
					Definition: def,
					Columns:    columns,
					Deferrable: con.Deferrable,
					Deferred:   con.Initdeferred,
					Validated:  !con.SkipValidation,
				},
				Schema:    schema,
				Table:     as.Relation.Relname,
				RefSchema: refSchema,
				RefTable:  refTable,
			})

			continue
		}

		// Non-FK constraint (PRIMARY KEY, UNIQUE, CHECK, etc.)
		constraint, err := parseTableConstraint(con, as.Relation.Relname)
		if err != nil {
			return nil, nil, err
		}

		constraints = append(constraints, constraint)
	}

	return constraints, fks, nil
}

// parseInlineForeignKey builds a ForeignKey from an inline FOREIGN KEY
// constraint inside a CREATE TABLE statement.
func parseInlineForeignKey(con *pg_query.Constraint, schema, table, defaultSchema string) (*model.ForeignKey, error) {
	if con.Conname == "" {
		con.Conname = autoNameConstraint(table, fkAttrCols(con), con.Contype)
	}

	def, err := deparseConstraintDef(con)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse constraint %s: %w", con.Conname, err)
	}

	var refSchema, refTable *string
	if con.Pktable != nil {
		rs := con.Pktable.Schemaname
		if rs == "" {
			rs = defaultSchema
		}
		refSchema = &rs
		rt := con.Pktable.Relname
		refTable = &rt
	}

	var columns []string
	for _, attr := range con.FkAttrs {
		if s := attr.GetString_(); s != nil {
			columns = append(columns, s.Sval)
		}
	}

	return &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       con.Conname,
			Type:       model.ConstraintType('f'),
			Definition: def,
			Columns:    columns,
			Deferrable: con.Deferrable,
			Deferred:   con.Initdeferred,
			Validated:  !con.SkipValidation,
		},
		Schema:    schema,
		Table:     table,
		RefSchema: refSchema,
		RefTable:  refTable,
	}, nil
}

// Deparse helpers

func deparseTypeName(tn *pg_query.TypeName) (string, error) {
	// pg_query's deparse places typmod after "with/without time zone" for
	// timestamp/time variants, producing invalid SQL like
	// "timestamp without time zone(6)". Format these four types directly
	// from the AST so the precision lands in the right spot.
	if s, ok := formatTimeTypeName(tn); ok {
		return s, nil
	}
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{
				Node: &pg_query.Node_CreateStmt{
					CreateStmt: &pg_query.CreateStmt{
						Relation: pg_query.MakeSimpleRangeVar("_t", 0),
						TableElts: []*pg_query.Node{
							pg_query.MakeSimpleColumnDefNode("_c", tn, nil, 0),
						},
					},
				},
			},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse type name: %w", err)
	}

	const marker = "_c "
	idx := strings.Index(sql, marker)
	if idx == -1 {
		return "", fmt.Errorf("unexpected deparse output for type: %s", sql)
	}
	rest := sql[idx+len(marker):]
	lastParen := strings.LastIndex(rest, ")")
	if lastParen < 0 {
		return "", fmt.Errorf("unexpected deparse output for type: %s", sql)
	}
	typeName := strings.TrimSpace(rest[:lastParen])
	// pg_query may qualify built-in types with "pg_catalog." (e.g. json -> pg_catalog.json).
	// Strip the prefix so the result matches format_type() output.
	typeName = strings.TrimPrefix(typeName, "pg_catalog.")
	return normalizeTypeName(typeName), nil
}

func formatTimeTypeName(tn *pg_query.TypeName) (string, bool) {
	if len(tn.Names) == 0 || len(tn.Names) > 2 {
		return "", false
	}
	if len(tn.Names) == 2 {
		q := tn.Names[0].GetString_()
		if q == nil || q.GetSval() != "pg_catalog" {
			return "", false
		}
	}
	last := tn.Names[len(tn.Names)-1].GetString_()
	if last == nil {
		return "", false
	}
	var bare, zone string
	switch last.GetSval() {
	case "timestamp":
		bare, zone = "timestamp", "without time zone"
	case "timestamptz":
		bare, zone = "timestamp", "with time zone"
	case "time":
		bare, zone = "time", "without time zone"
	case "timetz":
		bare, zone = "time", "with time zone"
	default:
		return "", false
	}
	prec := ""
	if len(tn.Typmods) > 0 {
		c := tn.Typmods[0].GetAConst()
		if c == nil {
			return "", false
		}
		ival := c.GetIval()
		if ival == nil {
			return "", false
		}
		prec = fmt.Sprintf("(%d)", ival.GetIval())
	}
	var arr strings.Builder
	for _, b := range tn.ArrayBounds {
		// pg_query encodes "[]" as Ival=-1; positive values are explicit
		// array bounds like "[3]". Anything else (e.g. a non-Integer node)
		// means we don't know how to format it; fall back to deparse.
		i := b.GetInteger()
		if i == nil {
			return "", false
		}
		if i.GetIval() < 0 {
			arr.WriteString("[]")
		} else {
			fmt.Fprintf(&arr, "[%d]", i.GetIval())
		}
	}
	return bare + prec + " " + zone + arr.String(), true
}

var typeAliases = map[string]string{
	"int":         "integer",
	"int4":        "integer",
	"int2":        "smallint",
	"int8":        "bigint",
	"float4":      "real",
	"float8":      "double precision",
	"bool":        "boolean",
	"varchar":     "character varying",
	"char":        "character",
	"timestamp":   "timestamp without time zone",
	"timestamptz": "timestamp with time zone",
	"time":        "time without time zone",
	"timetz":      "time with time zone",
	"varbit":      "bit varying",
	"decimal":     "numeric",
	"float":       "double precision",
}

func normalizeTypeName(name string) string {
	// Handle types with modifiers like "varchar(255)" -> "character varying(255)"
	base := name
	suffix := ""
	if idx := strings.Index(name, "("); idx != -1 {
		base = name[:idx]
		suffix = name[idx:]
	} else if idx := strings.Index(name, "["); idx != -1 {
		base = name[:idx]
		suffix = name[idx:]
	}

	// Normalize spacing in type modifiers: "numeric(10, 2)" -> "numeric(10,2)"
	suffix = strings.ReplaceAll(suffix, ", ", ",")

	if canonical, ok := typeAliases[base]; ok {
		return canonical + suffix
	}
	return base + suffix
}

func deparseExpr(node *pg_query.Node) (string, error) {
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{
				Node: &pg_query.Node_SelectStmt{
					SelectStmt: &pg_query.SelectStmt{
						TargetList: []*pg_query.Node{
							pg_query.MakeResTargetNodeWithVal(node, 0),
						},
					},
				},
			},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse expression: %w", err)
	}
	const prefix = "SELECT "
	if !strings.HasPrefix(sql, prefix) {
		return "", fmt.Errorf("unexpected deparse output for expression: %s", sql)
	}
	return strings.TrimSpace(sql[len(prefix):]), nil
}

func deparseConstraintDef(con *pg_query.Constraint) (string, error) {
	// Temporarily clear SkipValidation so "NOT VALID" is not included in the
	// definition string (it is tracked separately via the Validated field).
	origSkipValidation := con.SkipValidation
	con.SkipValidation = false
	defer func() { con.SkipValidation = origSkipValidation }()

	// Work around a libpg_query deparse bug: a single key column named "value"
	// is dropped from the deparsed column list, even though the parse tree
	// keeps it. Swap each key column name for a collision-free placeholder that
	// deparses reliably, then substitute the real identifiers back into the
	// output. Only UNIQUE/PRIMARY KEY constraints populate Keys, so other
	// constraint types are unaffected.
	//
	// Upstream: https://github.com/pganalyze/pg_query_go/issues/148
	// Revert this workaround once that bug is fixed.
	swapped := make([]*pg_query.String, 0, len(con.Keys))
	origKeys := make([]string, 0, len(con.Keys))
	repl := make(map[string]string, len(con.Keys))
	for i, k := range con.Keys {
		s := k.GetString_()
		if s == nil {
			continue
		}
		// The trailing "_e" delimits the index so one placeholder is never a
		// substring of another (e.g. "..._1_e" vs "..._10_e"), which would
		// corrupt replacement for constraints with ten or more key columns.
		placeholder := fmt.Sprintf("pistachio_key_placeholder_%d_e", i)
		swapped = append(swapped, s)
		origKeys = append(origKeys, s.Sval)
		repl[placeholder] = model.Ident(s.Sval)
		s.Sval = placeholder
	}
	defer func() {
		for i, s := range swapped {
			s.Sval = origKeys[i]
		}
	}()

	alterCmd := &pg_query.AlterTableCmd{
		Subtype: pg_query.AlterTableType_AT_AddConstraint,
		Def:     &pg_query.Node{Node: &pg_query.Node_Constraint{Constraint: con}},
	}
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{
				Node: &pg_query.Node_AlterTableStmt{
					AlterTableStmt: &pg_query.AlterTableStmt{
						Relation: pg_query.MakeSimpleRangeVar("_t", 0),
						Cmds: []*pg_query.Node{{
							Node: &pg_query.Node_AlterTableCmd{AlterTableCmd: alterCmd},
						}},
						Objtype: pg_query.ObjectType_OBJECT_TABLE,
					},
				},
			},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse constraint: %w", err)
	}

	restorePlaceholders := func(def string) string {
		for placeholder, ident := range repl {
			def = strings.ReplaceAll(def, placeholder, ident)
		}
		return def
	}

	if con.Conname != "" {
		marker := "CONSTRAINT " + model.Ident(con.Conname) + " "
		idx := strings.Index(sql, marker)
		if idx != -1 {
			return restorePlaceholders(strings.TrimSpace(sql[idx+len(marker):])), nil
		}
	}

	const fallbackMarker = " ADD "
	idx := strings.LastIndex(sql, fallbackMarker)
	if idx != -1 {
		return restorePlaceholders(strings.TrimSpace(sql[idx+len(fallbackMarker):])), nil
	}

	return "", fmt.Errorf("could not extract constraint definition from: %s", sql)
}

func deparsePartitionSpec(cs *pg_query.CreateStmt) (string, error) {
	minCS := &pg_query.CreateStmt{
		Relation: pg_query.MakeSimpleRangeVar("_t", 0),
		TableElts: []*pg_query.Node{
			pg_query.MakeSimpleColumnDefNode("_c", &pg_query.TypeName{
				Names: []*pg_query.Node{pg_query.MakeStrNode("integer")},
			}, nil, 0),
		},
		Partspec: cs.Partspec,
	}
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{Node: &pg_query.Node_CreateStmt{CreateStmt: minCS}},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse partition spec: %w", err)
	}
	const prefix = "PARTITION BY "
	idx := strings.Index(sql, prefix)
	if idx == -1 {
		return "", fmt.Errorf("could not extract partition spec from: %s", sql)
	}
	return strings.TrimSpace(sql[idx+len(prefix):]), nil
}

func deparsePartitionBound(cs *pg_query.CreateStmt) (string, error) {
	minCS := &pg_query.CreateStmt{
		Relation: pg_query.MakeSimpleRangeVar("_t", 0),
		InhRelations: []*pg_query.Node{
			{Node: &pg_query.Node_RangeVar{RangeVar: pg_query.MakeSimpleRangeVar("_parent", 0)}},
		},
		Partbound: cs.Partbound,
	}
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{Node: &pg_query.Node_CreateStmt{CreateStmt: minCS}},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse partition bound: %w", err)
	}
	const prefix = "PARTITION OF _parent "
	idx := strings.Index(sql, prefix)
	if idx == -1 {
		return "", fmt.Errorf("could not extract partition bound from: %s", sql)
	}
	return strings.TrimSpace(sql[idx+len(prefix):]), nil
}

// parseStorageParams reads the WITH clause of a CREATE TABLE. A parameter of
// the TOAST relation arrives with `toast` as its namespace and is keyed under
// the same `toast.` prefix the catalog reports it with.
func parseStorageParams(options []*pg_query.Node) *orderedmap.Map[string, string] {
	params := make(map[string]string, len(options))
	for _, o := range options {
		de := o.GetDefElem()
		name := de.GetDefname()
		if ns := de.GetDefnamespace(); ns != "" {
			name = ns + "." + name
		}
		// WITH (oids = false) is what every pg_dump before 12 writes on every
		// table. PostgreSQL still accepts it in a CREATE TABLE and stores
		// nothing for it, while ALTER TABLE ... SET rejects the name, so it is
		// not a parameter. oids = true is rejected by the CREATE itself.
		if name == "oids" {
			continue
		}
		params[name] = storageParamValue(de)
	}
	return model.SortedStorageParams(params)
}

// storageParamValue reads a storage parameter's value as text. A parameter
// written without one asks for true, which is what PostgreSQL stores for it.
func storageParamValue(de *pg_query.DefElem) string {
	switch n := de.GetArg().GetNode().(type) {
	case *pg_query.Node_String_:
		return n.String_.Sval
	case *pg_query.Node_Integer:
		return strconv.FormatInt(int64(n.Integer.Ival), 10)
	case *pg_query.Node_Float:
		return n.Float.Fval
	case *pg_query.Node_TypeName:
		// A value that reads as a bare identifier, `autovacuum_enabled = off`,
		// arrives as a type name: the grammar accepts a type there. Its parts
		// carry the word PostgreSQL stores.
		var parts []string
		for _, name := range n.TypeName.Names {
			parts = append(parts, name.GetString_().GetSval())
		}
		return strings.Join(parts, ".")
	default:
		return "true"
	}
}
