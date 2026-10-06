package parser

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

type ParseResult struct {
	Tables         *orderedmap.Map[string, *model.Table]         `json:"tables"`
	Views          *orderedmap.Map[string, *model.View]          `json:"views"`
	Enums          *orderedmap.Map[string, *model.Enum]          `json:"enums"`
	Domains        *orderedmap.Map[string, *model.Domain]        `json:"domains"`
	CompositeTypes *orderedmap.Map[string, *model.CompositeType] `json:"composite_types"`
	Sequences      *orderedmap.Map[string, *model.Sequence]      `json:"sequences"`
	Routines       *orderedmap.Map[string, *model.Routine]       `json:"routines"`
	ExecuteStmts   []*ExecuteStmt                                `json:"execute_stmts"`
	// LintIgnores holds the rules that -- pista:lint-ignore turns off, by the
	// object it is written before. It is kept out of the model and of the
	// JSON document: it means nothing to plan or apply, and the state hash
	// that a plan file records is taken over the model.
	LintIgnores map[LintTarget][]string `json:"-"`
	// Positions holds where each table, column, index and foreign key is
	// declared, for pista lint to report. It is empty for SQL that came from
	// no file. Like LintIgnores, it is kept out of the model and the document.
	Positions map[LintTarget]Position `json:"-"`
}

// Position is a place in a schema file.
type Position struct {
	File   string
	Line   int
	Column int
}

func (p Position) String() string {
	return fmt.Sprintf("%s:%d:%d", p.File, p.Line, p.Column)
}

// LintKind is a kind of object a lint rule checks.
type LintKind string

const (
	LintTable      LintKind = "table"
	LintColumn     LintKind = "column"
	LintIndex      LintKind = "index"
	LintForeignKey LintKind = "foreign_key"
)

// LintTarget names one object a lint rule checks. Table is the qualified name
// of the table, or of the materialized view an index is on. Name is the name
// of the column, index or foreign key, and empty for a table.
type LintTarget struct {
	Kind  LintKind
	Table string
	Name  string
}

// warnWriter receives warnings about statements pistachio does not support and
// silently ignores. Tests swap it via setWarnWriter.
var warnWriter io.Writer = os.Stderr

// ignoredStmtSnippet returns the raw text of the ignored statement. Deparsing
// drops the comments and blank lines that surround the statement in the file.
// The raw slice is a fallback for the rare statement pg_query can parse but not
// deparse. The caller collapses whitespace, so a multi-line body from either
// path becomes one line.
func ignoredStmtSnippet(sql string, rawStmt *pg_query.RawStmt) string {
	single := &pg_query.ParseResult{Version: pgast.ParseTreeVersion, Stmts: []*pg_query.RawStmt{rawStmt}}
	if deparsed, err := pg_query.Deparse(single); err == nil {
		return deparsed
	}

	return stmtRegion(sql, rawStmt)
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
//
// The statement's file, line and column lead the message when the parse came
// from files:
//
//	pista: schema/items.sql:12:1: ignored unsupported statement: DROP TABLE public.items
func warnIgnoredStmt(sql string, spans []fileSpan, rawStmt *pg_query.RawStmt) {
	snippet := strings.Join(strings.Fields(ignoredStmtSnippet(sql, rawStmt)), " ")
	if snippet == "" {
		return
	}

	const maxRunes = 200
	if runes := []rune(snippet); len(runes) > maxRunes {
		snippet = string(runes[:maxRunes]) + "..."
	}

	at := ""
	if pos, ok := locate(sql, spans, int(stmtStart(sql, rawStmt))); ok {
		at = pos.String() + ": "
	}

	fmt.Fprintf(warnWriter, "pista: %signored unsupported statement: %s%s\n", at, snippet, txHint(rawStmt)) //nolint:errcheck
}

// createTableLikeError rejects a LIKE clause, which the parser does not
// expand. The columns it would copy would read as absent from the desired
// schema, so a table created this way would plan a DROP COLUMN for each of
// them, or be created with no columns.
func createTableLikeError(cs *pg_query.CreateStmt, fqtn string, offset int32) error {
	for _, elt := range cs.TableElts {
		if elt.GetTableLikeClause() != nil {
			return &locatedError{msg: "CREATE TABLE " + fqtn + ": LIKE is not supported (list the columns instead)", offset: int(offset)}
		}
	}

	return nil
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
	pg_query.ObjectType_OBJECT_TABLE:         true,
	pg_query.ObjectType_OBJECT_VIEW:          true,
	pg_query.ObjectType_OBJECT_MATVIEW:       true,
	pg_query.ObjectType_OBJECT_COLUMN:        true,
	pg_query.ObjectType_OBJECT_INDEX:         true,
	pg_query.ObjectType_OBJECT_SEQUENCE:      true,
	pg_query.ObjectType_OBJECT_TYPE:          true,
	pg_query.ObjectType_OBJECT_DOMAIN:        true,
	pg_query.ObjectType_OBJECT_FUNCTION:      true,
	pg_query.ObjectType_OBJECT_PROCEDURE:     true,
	pg_query.ObjectType_OBJECT_TABCONSTRAINT: true,
	pg_query.ObjectType_OBJECT_DOMCONSTRAINT: true,
	pg_query.ObjectType_OBJECT_TRIGGER:       true,
	pg_query.ObjectType_OBJECT_POLICY:        true,
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
func warnIgnoredAlterTableCmds(sql string, spans []fileSpan, rawStmt *pg_query.RawStmt, as *pg_query.AlterTableStmt) {
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

	warnIgnoredStmt(sql, spans, &pg_query.RawStmt{
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

// setUnique records v under key and rejects a name the map already holds.
// on names the object the key belongs to, empty for a name that stands on its
// own in a schema. offset is where in the parsed SQL the message points, so
// the error can name the file, line and column of the repeat.
func setUnique[V any](m *orderedmap.Map[string, V], key, kind string, v V, on string, offset int32) error {
	if _, ok := m.GetOk(key); ok {
		scope := ""
		if on != "" {
			scope = " on " + on
		}
		return &locatedError{
			msg:    fmt.Sprintf("duplicate %s: %s%s", kind, key, scope),
			offset: int(offset),
		}
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
	sources := make([]Source, 0, len(paths))
	for _, path := range paths {
		sql, err := readSQLFile(path)
		if err != nil {
			return nil, err
		}
		name := path
		if path == "-" {
			name = "<stdin>"
		}
		sources = append(sources, Source{Name: name, SQL: sql})
	}

	return ParseSQLSourcesWithSchema(sources, defaultSchema)
}

// Source is one piece of desired-schema SQL and the name a message calls it
// by. A file's name is its path; SQL read out of a git repository carries the
// revision as well, so an error says which version of the file it is in.
type Source struct {
	Name string
	SQL  string
}

// ParseSQLSourcesWithSchema parses SQL that is already in memory, the way
// ParseSQLFilesWithSchema parses the files it reads. A caller that did not
// read a file uses it to keep the file name in an error and a warning.
func ParseSQLSourcesWithSchema(sources []Source, defaultSchema string) (*ParseResult, error) {
	sqls := make([]string, 0, len(sources))
	spans := make([]fileSpan, 0, len(sources))
	offset := 0
	for _, source := range sources {
		spans = append(spans, fileSpan{path: source.Name, start: offset})
		sqls = append(sqls, source.SQL)
		offset += len(source.SQL) + 1 // the "\n" the join puts between sources
	}

	joined := strings.Join(sqls, "\n")
	result, err := parseSQLWithSchema(joined, defaultSchema, spans)
	if err != nil {
		return nil, annotateError(err, joined, spans)
	}
	return result, nil
}

// parseSQLWithSchema parses the SQL every desired-schema file was joined
// into. spans says where each file starts, so a warning can name the file a
// statement is in. A caller parsing a string passes none, and the warning
// carries no position.
func parseSQLWithSchema(sql string, defaultSchema string, spans []fileSpan) (*ParseResult, error) {
	if err := validateDirectives(sql); err != nil {
		return nil, err
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if err := validateDirectivePlacement(sql, spans); err != nil {
		return nil, err
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
	lintIgnoreDirectives := extractLintIgnoreDirectives(sql, result.Stmts)
	lintIgnores := map[LintTarget][]string{}
	addLintIgnores := func(target LintTarget, rules []string) {
		if len(rules) > 0 {
			lintIgnores[target] = append(lintIgnores[target], rules...)
		}
	}
	positions := map[LintTarget]Position{}
	addPosition := func(target LintTarget, offset int32) {
		if pos, ok := locate(sql, spans, int(offset)); ok {
			positions[target] = Position{File: pos.path, Line: pos.line, Column: pos.col}
		}
	}
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
		lintIgnore := lintIgnoreDirectives[rawStmt.StmtLocation]
		// Where a duplicate-name error points when the repeat is the
		// statement itself rather than a part of one.
		stmtOffset := stmtStart(sql, rawStmt)

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
			rawStmtSQL := stmtRegion(sql, rawStmt)
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
			if err := setUnique(enums, enum.FQEN(), "enum", enum, "", stmtOffset); err != nil {
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
			if err := setUnique(domains, domain.FQDN(), "domain", domain, "", stmtOffset); err != nil {
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
			rawStmtSQL := stmtRegion(sql, rawStmt)
			attrDirectives := extractInlineDirectives(rawStmtSQL)
			for _, attr := range compositeType.Attributes {
				if oldName, ok := attrDirectives.Columns[attr.Name]; ok {
					old := oldName
					attr.RenameFrom = &old
				}
			}

			compositeType.Ignore = ignore
			if err := setUnique(compositeTypes, compositeType.FQCN(), "composite type", compositeType, "", stmtOffset); err != nil {
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
			rawStmtSQL := stmtRegion(sql, rawStmt)
			inlineDirectives := extractInlineDirectives(rawStmtSQL)
			for colName, oldName := range inlineDirectives.Columns {
				if col, ok := table.Columns.GetOk(colName); ok {
					old := oldName
					col.RenameFrom = &old
				}
			}
			for colName, expr := range inlineDirectives.RetypeUsing {
				if col, ok := table.Columns.GetOk(colName); ok {
					col.RetypeUsing = &expr
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
			addLintIgnores(LintTarget{Kind: LintTable, Table: table.FQTN()}, lintIgnore)
			addCreateTablePositions(addPosition, node.GetCreateStmt(), table, stmtOffset)
			for colName, rules := range inlineDirectives.LintIgnoreColumns {
				if _, ok := table.Columns.GetOk(colName); ok {
					addLintIgnores(LintTarget{Kind: LintColumn, Table: table.FQTN(), Name: colName}, rules)
				}
			}
			for conName, rules := range inlineDirectives.LintIgnoreConstraints {
				if _, ok := table.ForeignKeys.GetOk(conName); ok {
					addLintIgnores(LintTarget{Kind: LintForeignKey, Table: table.FQTN(), Name: conName}, rules)
				}
			}

			table.Ignore = ignore
			if !table.Ignore {
				if err := createTableLikeError(node.GetCreateStmt(), table.FQTN(), stmtOffset); err != nil {
					return nil, err
				}
			}
			if err := setUnique(tables, table.FQTN(), "table", table, "", stmtOffset); err != nil {
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
			if err := setUnique(views, view.FQVN(), "view", view, "", stmtOffset); err != nil {
				return nil, err
			}

		case node.GetCreateTableAsStmt() != nil:
			as := node.GetCreateTableAsStmt()
			// CREATE TABLE AS shares this statement type and is not read.
			if as.Objtype != pg_query.ObjectType_OBJECT_MATVIEW {
				warnIgnoredStmt(sql, spans, rawStmt)
				break
			}
			view, err := parseCreateMatViewStmt(as, defaultSchema)
			if err != nil {
				return nil, err
			}
			if renameFrom != "" {
				qualified := qualifyRenameFrom(renameFrom, defaultSchema)
				view.RenameFrom = &qualified
			}
			view.Ignore = ignore
			if err := setUnique(views, view.FQVN(), "materialized view", view, "", stmtOffset); err != nil {
				return nil, err
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
			fqtn := idx.FQTN()
			addLintIgnores(LintTarget{Kind: LintIndex, Table: fqtn, Name: idx.Name}, lintIgnore)
			addPosition(LintTarget{Kind: LintIndex, Table: fqtn, Name: idx.Name}, stmtOffset)
			if t, ok := tables.GetOk(fqtn); ok {
				if err := setUnique(t.Indexes, idx.Name, "index", idx, fqtn, stmtOffset); err != nil {
					return nil, err
				}
			} else if v, ok := views.GetOk(fqtn); ok {
				// PostgreSQL refuses an index on a plain view.
				if !v.Materialized {
					return nil, &locatedError{msg: "CREATE INDEX " + idx.Name + ": " + fqtn + " is a view, which cannot hold an index", offset: int(stmtOffset)}
				}
				if err := setUnique(v.Indexes, idx.Name, "index", idx, fqtn, stmtOffset); err != nil {
					return nil, err
				}
			} else {
				return nil, undeclared("CREATE INDEX "+idx.Name, "table or materialized view", fqtn, stmtOffset)
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
				// ALTER INDEX, ALTER VIEW and ALTER MATERIALIZED VIEW share
				// this statement type, name no table, and are not read. Nor
				// is ALTER TABLE on a view, a materialized view or a
				// sequence, which PostgreSQL accepts and pg_dump writes for
				// a view column's default. IF EXISTS is about the database,
				// not the file, so it does not excuse a missing declaration.
				_, isView := views.GetOk(fqtn)
				_, isSeq := sequences.GetOk(fqtn)
				if as.Objtype != pg_query.ObjectType_OBJECT_TABLE || isView || isSeq {
					warnIgnoredStmt(sql, spans, rawStmt)
					continue
				}
				return nil, undeclared("ALTER TABLE "+fqtn, "table", fqtn, stmtOffset)
			}

			// A table marked -- pista:ignore is out of the diff, so an
			// action dropped from it, or a trigger or column it does not
			// declare, cannot mislead the plan.
			if !t.Ignore {
				if err := checkAlterTableTargets(as, t, fqtn, stmtOffset); err != nil {
					return nil, err
				}
				warnIgnoredAlterTableCmds(sql, spans, rawStmt, as)
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
				if err := setUnique(t.ForeignKeys, fk.Name, "foreign key", fk, fqtn, stmtOffset); err != nil {
					return nil, err
				}
				addLintIgnores(LintTarget{Kind: LintForeignKey, Table: fqtn, Name: fk.Name}, lintIgnore)
				addPosition(LintTarget{Kind: LintForeignKey, Table: fqtn, Name: fk.Name}, stmtOffset)
			}
			for _, con := range cons {
				if renameFrom != "" {
					unquoted := normalizeUnqualifiedDirective(renameFrom)
					con.RenameFrom = &unquoted
				}
				if err := setUnique(t.Constraints, con.Name, "constraint", con, fqtn, stmtOffset); err != nil {
					return nil, err
				}
				applyPrimaryKeyNotNull(t, con)
			}

		case node.GetCreatePolicyStmt() != nil:
			policy, err := parseCreatePolicyStmt(node.GetCreatePolicyStmt(), defaultSchema, tables, stmtOffset)
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
			if err := attachTrigger(trg, tables, views, stmtOffset); err != nil {
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
			if err := setUnique(sequences, seq.FQN(), "sequence", seq, "", stmtOffset); err != nil {
				return nil, err
			}

		case node.GetAlterSeqStmt() != nil:
			seq, err := applyAlterSeqOwnedBy(node.GetAlterSeqStmt(), defaultSchema, sequences, stmtOffset)
			if err != nil {
				return nil, err
			}
			// A sequence marked -- pista:ignore is out of the diff, so an
			// option dropped from it cannot mislead the plan.
			if seq != nil && !seq.Ignore {
				warnIgnoredAlterSeqOptions(sql, spans, rawStmt, node.GetAlterSeqStmt())
			}

		case node.GetCreateFunctionStmt() != nil:
			routine, err := parseCreateFunctionStmt(node.GetCreateFunctionStmt(), defaultSchema)
			if errors.Is(err, ErrUnsupportedRoutine) {
				// A routine pistachio reads but does not manage. The catalog
				// skips the same ones, so warning and dropping it here keeps
				// both sides of the diff in step.
				warnIgnoredStmt(sql, spans, rawStmt)
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
			if err := setUnique(routines, routine.FQRN(), "routine", routine, "", stmtOffset); err != nil {
				return nil, err
			}

		case node.GetAlterDomainStmt() != nil:
			ok, err := parseAlterDomainAddCheck(node.GetAlterDomainStmt(), defaultSchema, domains, stmtOffset)
			if err != nil {
				return nil, err
			}
			if !ok {
				warnIgnoredStmt(sql, spans, rawStmt)
			}

		case node.GetCommentStmt() != nil:
			cs := node.GetCommentStmt()
			if !commentTargetSupported[cs.Objtype] {
				warnIgnoredStmt(sql, spans, rawStmt)
				break
			}
			parseCommentStmt(cs, defaultSchema, tables, views, enums, domains, compositeTypes, sequences, routines)

		default:
			// A statement type no case above handles. It is dropped from the
			// desired schema, so warn instead of failing silently.
			warnIgnoredStmt(sql, spans, rawStmt)
		}
	}

	if err := applyUsingIndexPrimaryKeyNotNull(tables); err != nil {
		return nil, err
	}

	if err := fillFKRefColumns(tables); err != nil {
		return nil, err
	}

	if err := validateColumnRefs(tables); err != nil {
		return nil, err
	}

	parsed := &ParseResult{Tables: tables, Views: views, Enums: enums, Domains: domains, CompositeTypes: compositeTypes, Sequences: sequences, Routines: routines, ExecuteStmts: executeStmts, LintIgnores: lintIgnores, Positions: positions}

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
			if err := setUnique(table.Columns, col.Name, "column", col, table.FQTN(), cd.Location); err != nil {
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
					if err := setUnique(table.ForeignKeys, fk.Name, "foreign key", fk, table.FQTN(), con.Location); err != nil {
						return nil, err
					}
				}
			} else {
				constraint, err := parseTableConstraint(con, table.Name)
				if err != nil {
					return nil, err
				}
				if constraint != nil {
					if err := setUnique(table.Constraints, constraint.Name, "constraint", constraint, table.FQTN(), con.Location); err != nil {
						return nil, err
					}
					applyPrimaryKeyNotNull(table, constraint)
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

// applyPrimaryKeyNotNull marks the key columns of a primary key NOT NULL.
// PostgreSQL sets the flag itself whichever way the key arrives, inline with
// the table or in a later ALTER TABLE, and the catalog reports it, so the
// desired side has to read both forms the same way. Reading only the inline
// one planned a DROP NOT NULL that PostgreSQL then refuses with "column is in
// a primary key".
func applyPrimaryKeyNotNull(table *model.Table, con *model.Constraint) {
	if !con.Type.IsPrimaryKeyConstraint() {
		return
	}
	for _, colName := range con.Columns {
		if col, ok := table.Columns.GetOk(colName); ok {
			col.NotNull = true
		}
	}
}

// applyUsingIndexPrimaryKeyNotNull marks the columns of a primary key written
// USING INDEX NOT NULL. The key lists no columns, so they are read from its
// index, and the index must be declared. The index can come later in the
// files, so this runs after every statement is read. A table marked
// -- pista:ignore is out of the diff, so it is skipped.
func applyUsingIndexPrimaryKeyNotNull(tables *orderedmap.Map[string, *model.Table]) error {
	for fqtn, t := range tables.All() {
		if t.Ignore {
			continue
		}
		for con := range t.Constraints.Values() {
			if !con.Type.IsPrimaryKeyConstraint() || con.IndexName == "" {
				continue
			}
			if _, ok := t.Indexes.GetOk(con.IndexName); !ok {
				return fmt.Errorf("index %s used by primary key %s on table %s is not declared", model.Ident(con.IndexName), model.Ident(con.Name), fqtn)
			}
			for _, colName := range primaryKeyColumns(t) {
				if col, ok := t.Columns.GetOk(colName); ok {
					col.NotNull = true
				}
			}
		}
	}
	return nil
}

// serialTypes lists the pseudo-types that expand to a column plus a sequence.
var serialTypes = map[string]bool{
	"serial":      true,
	"bigserial":   true,
	"smallserial": true,
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

	// serial expands to a NOT NULL column plus a sequence and a default, so a
	// declaration that leaves the words off still describes a NOT NULL column.
	// The catalog reports it that way, so reading it as nullable planned a
	// DROP NOT NULL right after the table was created.
	if serialTypes[col.TypeName] {
		col.NotNull = true
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
				def = parenthesizeDefault(def)
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

// checkAlterTableTargets refuses a trigger state naming a trigger, or a
// storage setting naming a column, that the table does not declare before
// the statement. It runs before anything in the statement is applied or
// warned about. The ALL and USER trigger forms are subtypes of their own and
// name no trigger. A partition child declares no columns of its own, so its
// storage settings are not checked; dump writes none, pg_dump may.
func checkAlterTableTargets(as *pg_query.AlterTableStmt, t *model.Table, fqtn string, offset int32) error {
	for _, cmdNode := range as.Cmds {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil {
			continue
		}
		switch cmd.Subtype {
		case pg_query.AlterTableType_AT_EnableTrig, pg_query.AlterTableType_AT_DisableTrig,
			pg_query.AlterTableType_AT_EnableAlwaysTrig, pg_query.AlterTableType_AT_EnableReplicaTrig:
			if _, ok := t.Triggers.GetOk(cmd.Name); !ok {
				return undeclared("ALTER TABLE "+fqtn, "trigger", cmd.Name, offset)
			}
		case pg_query.AlterTableType_AT_SetStorage, pg_query.AlterTableType_AT_SetCompression:
			if _, ok := t.Columns.GetOk(cmd.Name); !ok && !t.IsPartitionChild() {
				return undeclared("ALTER TABLE "+fqtn, "column", cmd.Name, offset)
			}
		}
	}
	return nil
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
		// checkAlterTableTargets has refused a column the table does not
		// declare, so the lookup fails only on a partition child, which
		// declares none, or on an ignored table, which is not checked.
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
// (src/backend/commands/indexcmds.c). It joins name1, name2 and label with
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

// MakeObjectName exports makeObjectName. The catalog uses it to compare a
// stored name with the name PostgreSQL would generate.
func MakeObjectName(name1, name2, label string) string {
	return makeObjectName(name1, name2, label)
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
	return chooseIndexElemNames(is.IndexParams, is.IndexIncludingParams)
}

// chooseIndexElemNames is chooseIndexColumnNames over the element lists
// themselves, so a constraint's elements can be named the same way.
func chooseIndexElemNames(lists ...[]*pg_query.Node) []string {
	var names []string

	taken := func(name string) bool {
		return slices.Contains(names, name)
	}

	for _, params := range lists {
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

// AutoNameIndex generates a PostgreSQL-style name for an index written without
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
//
// The diff uses it too: the copy of a parent's index that PostgreSQL creates
// on a new partition takes this name.
func AutoNameIndex(is *pg_query.IndexStmt) string {
	return makeObjectName(is.Relation.Relname, strings.Join(chooseIndexColumnNames(is), "_"), "idx")
}

// constraintNameParts returns the names PostgreSQL builds an unnamed constraint's
// name from, in order. The constraint is backed by an index, and the name comes
// from that index's elements: the key columns, which Keys holds for PRIMARY
// KEY and UNIQUE and Exclusions for EXCLUDE, followed by the INCLUDE columns.
// An element that is an expression is named the way an index element is, so
// lower(a) reads lower and a repeated name takes a number.
func constraintNameParts(con *pg_query.Constraint) []string {
	var elems []*pg_query.Node
	for _, k := range con.Keys {
		if s := k.GetString_(); s != nil {
			elems = append(elems, indexElemNode(s.Sval))
		}
	}
	for _, ex := range con.Exclusions {
		list := ex.GetList()
		if list == nil {
			continue
		}
		for _, item := range list.Items {
			if item.GetIndexElem() != nil {
				elems = append(elems, item)
			}
		}
	}
	for _, inc := range con.Including {
		if s := inc.GetString_(); s != nil {
			elems = append(elems, indexElemNode(s.Sval))
		}
	}
	return chooseIndexElemNames(elems)
}

// indexElemNode wraps a column name as the index element a constraint's key
// list implies, so the naming can read keys and exclusions alike.
func indexElemNode(name string) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_IndexElem{IndexElem: &pg_query.IndexElem{Name: name}}}
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
	if err := foldConstraintAttrs(cd.Constraints); err != nil {
		return err
	}
	for _, conNode := range cd.Constraints {
		con := conNode.GetConstraint()
		if con == nil {
			continue
		}
		// Skip column-attribute constraints (NOT NULL, DEFAULT, IDENTITY,
		// GENERATED) and the DEFERRABLE attributes folded in above.
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_NOTNULL, pg_query.ConstrType_CONSTR_DEFAULT,
			pg_query.ConstrType_CONSTR_IDENTITY, pg_query.ConstrType_CONSTR_GENERATED,
			pg_query.ConstrType_CONSTR_ATTR_DEFERRABLE, pg_query.ConstrType_CONSTR_ATTR_NOT_DEFERRABLE,
			pg_query.ConstrType_CONSTR_ATTR_DEFERRED, pg_query.ConstrType_CONSTR_ATTR_IMMEDIATE:
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
				if err := setUnique(table.ForeignKeys, fk.Name, "foreign key", fk, table.FQTN(), con.Location); err != nil {
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
				if err := setUnique(table.Constraints, constraint.Name, "constraint", constraint, table.FQTN(), con.Location); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// foldConstraintAttrs applies the DEFERRABLE and INITIALLY clauses written on
// a column to the constraint before them, and rejects the ones PostgreSQL
// rejects, as its transformConstraintAttrs does. The grammar gives each clause
// its own node in ColumnDef.Constraints, so the parse lets them all through.
// INITIALLY DEFERRED implies DEFERRABLE.
func foldConstraintAttrs(nodes []*pg_query.Node) error {
	var last *pg_query.Constraint
	var sawDeferrability, sawInitially bool
	for _, n := range nodes {
		con := n.GetConstraint()
		clause, ok := constraintAttrClauses[con.GetContype()]
		if !ok {
			last = con
			sawDeferrability, sawInitially = false, false
			continue
		}
		fail := func(msg string) error {
			return &locatedError{msg: msg, offset: int(con.Location)}
		}
		if !supportsConstraintAttrs(last) {
			return fail("misplaced " + clause + " clause")
		}
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_ATTR_DEFERRABLE, pg_query.ConstrType_CONSTR_ATTR_NOT_DEFERRABLE:
			if sawDeferrability {
				return fail("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed")
			}
			sawDeferrability = true
			last.Deferrable = con.Contype == pg_query.ConstrType_CONSTR_ATTR_DEFERRABLE
		default:
			if sawInitially {
				return fail("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed")
			}
			sawInitially = true
			last.Initdeferred = con.Contype == pg_query.ConstrType_CONSTR_ATTR_DEFERRED
			if last.Initdeferred && !sawDeferrability {
				last.Deferrable = true
			}
		}
		if last.Initdeferred && !last.Deferrable {
			return fail("constraint declared INITIALLY DEFERRED must be DEFERRABLE")
		}
	}
	return nil
}

var constraintAttrClauses = map[pg_query.ConstrType]string{
	pg_query.ConstrType_CONSTR_ATTR_DEFERRABLE:     "DEFERRABLE",
	pg_query.ConstrType_CONSTR_ATTR_NOT_DEFERRABLE: "NOT DEFERRABLE",
	pg_query.ConstrType_CONSTR_ATTR_DEFERRED:       "INITIALLY DEFERRED",
	pg_query.ConstrType_CONSTR_ATTR_IMMEDIATE:      "INITIALLY IMMEDIATE",
}

// supportsConstraintAttrs reports whether con takes DEFERRABLE and INITIALLY.
func supportsConstraintAttrs(con *pg_query.Constraint) bool {
	switch con.GetContype() {
	case pg_query.ConstrType_CONSTR_PRIMARY, pg_query.ConstrType_CONSTR_UNIQUE,
		pg_query.ConstrType_CONSTR_EXCLUSION, pg_query.ConstrType_CONSTR_FOREIGN:
		return true
	}
	return false
}

func parseTableConstraint(con *pg_query.Constraint, tableName string) (*model.Constraint, error) {
	if con.Conname == "" {
		if con.Indexname != "" {
			// ADD UNIQUE USING INDEX without CONSTRAINT: PostgreSQL names
			// the constraint after the index.
			con.Conname = con.Indexname
		} else {
			cols := constraintNameParts(con)
			if con.Contype == pg_query.ConstrType_CONSTR_CHECK {
				cols = checkExprCols(con.RawExpr)
			}
			con.Conname = autoNameConstraint(tableName, cols, con.Contype)
		}
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
		IndexName:  con.Indexname,
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

	// IF NOT EXISTS says how to run the statement, not what the index is.
	// pg_get_indexdef never writes it, so leaving it in would make the two
	// sides differ and plan a drop and a create on every run.
	is.IfNotExists = false

	// Name the index before deparsing so the stored Definition carries the
	// name PostgreSQL would have picked.
	if is.Idxname == "" {
		is.Idxname = AutoNameIndex(is)
	}

	result := &pg_query.ParseResult{
		Version: pgast.ParseTreeVersion,
		Stmts:   []*pg_query.RawStmt{{Stmt: rawStmt.Stmt}},
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

	columnNames := applyColumnNames(vs.Query, vs.Aliases)

	// Deparse the SELECT query
	selectResult := &pg_query.ParseResult{
		Version: pgast.ParseTreeVersion,
		Stmts: []*pg_query.RawStmt{{
			Stmt: vs.Query,
		}},
	}
	def, err := pg_query.Deparse(selectResult)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse view query: %w", err)
	}

	return &model.View{
		Schema:         schema,
		Name:           vs.View.Relname,
		Definition:     def,
		ColumnNames:    columnNames,
		CheckOption:    viewCheckOption(vs),
		StorageParams:  parseViewStorageParams(vs.Options),
		Indexes:        orderedmap.New[string, *model.Index](),
		Triggers:       orderedmap.New[string, *model.Trigger](),
		ColumnComments: orderedmap.New[string, string](),
	}, nil
}

// applyColumnNames writes the column list of CREATE VIEW v (x, y) onto the
// target list of the query, which is where PostgreSQL keeps it and where
// pg_get_viewdef writes it back, so the definition compares with the catalog.
// A set operation takes its names from its leftmost SELECT. A name equal to
// the column a target reads is left off, as pg_get_viewdef leaves it off.
//
// It returns the names it could not write, to be kept as a column list: all of
// them when the query has no target list (VALUES), when a star sits among the
// named targets, when there are more names than targets, or when the query
// refers to a name that the rewrite would take away or add, as in ORDER BY z.
func applyColumnNames(query *pg_query.Node, colNames []*pg_query.Node) []string {
	if len(colNames) == 0 {
		return nil
	}
	names := make([]string, len(colNames))
	for i, n := range colNames {
		names[i] = n.GetString_().GetSval()
	}

	ss := query.GetSelectStmt()
	for ss != nil && ss.Op != pg_query.SetOperation_SETOP_NONE {
		ss = ss.Larg
	}
	if ss == nil || len(ss.TargetList) < len(names) {
		return names
	}
	targets := make([]*pg_query.ResTarget, len(names))
	for i := range names {
		rt := ss.TargetList[i].GetResTarget()
		if rt == nil || isStarTarget(rt) {
			return names
		}
		targets[i] = rt
	}
	if refersToChangedName(query.GetSelectStmt(), ss, targets, names) {
		return names
	}

	for i, rt := range targets {
		rt.Name = names[i]
		if columnRefName(rt) == names[i] {
			rt.Name = ""
		}
	}
	return nil
}

// refersToChangedName reports whether the query refers by a bare name to an
// output name that applyColumnNames would change. An alias the query wrote
// goes away, so ORDER BY z or GROUP BY z breaks. A new name is resolved before
// an input column of the same name in ORDER BY and DISTINCT ON, so
// SELECT b FROM t ORDER BY a with the list (a) would sort by b instead of t.a.
// GROUP BY resolves an input column first, so a new name there is harmless.
// The name a column reference gives its target is not counted: once it goes,
// the bare name still finds the same input column.
//
// Output names are visible only in these clauses, of the SELECT that carries
// the targets and, for a set operation, of the whole, and not in a sub-query
// under them.
func refersToChangedName(top, ss *pg_query.SelectStmt, targets []*pg_query.ResTarget, names []string) bool {
	removed := map[string]bool{}
	added := map[string]bool{}
	for i, rt := range targets {
		cur := rt.Name
		if cur == "" {
			cur = columnRefName(rt)
		}
		if cur == names[i] {
			continue
		}
		if rt.Name != "" {
			removed[rt.Name] = true
		}
		added[names[i]] = true
	}
	either := maps.Clone(removed)
	maps.Copy(either, added)
	found := false
	refers := func(clause []*pg_query.Node, names map[string]bool) {
		for _, c := range clause {
			pgast.Walk(c, pgast.WalkOptions{SkipSubqueries: true}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
				if cr := n.GetColumnRef(); cr != nil && len(cr.Fields) == 1 && names[cr.Fields[0].GetString_().GetSval()] {
					found = true
				}
				return n
			})
		}
	}
	for _, s := range []*pg_query.SelectStmt{top, ss} {
		refers(s.SortClause, either)
		refers(s.DistinctClause, either)
		refers(s.GroupClause, removed)
	}
	return found
}

// columnRefName returns the name of the column a target reads, or "" when the
// target is not a column reference.
func columnRefName(rt *pg_query.ResTarget) string {
	fields := rt.Val.GetColumnRef().GetFields()
	if len(fields) == 0 {
		return ""
	}
	return fields[len(fields)-1].GetString_().GetSval()
}

// isStarTarget reports whether a target is * or t.*.
func isStarTarget(rt *pg_query.ResTarget) bool {
	fields := rt.Val.GetColumnRef().GetFields()
	return len(fields) > 0 && fields[len(fields)-1].GetAStar() != nil
}

// parseViewStorageParams reads the WITH (...) that precedes AS in a CREATE VIEW
// or CREATE MATERIALIZED VIEW. check_option is left out: it is read as the
// view's check option instead, which is also where the catalog side puts it.
func parseViewStorageParams(options []*pg_query.Node) *orderedmap.Map[string, string] {
	rest := make([]*pg_query.Node, 0, len(options))
	for _, o := range options {
		if o.GetDefElem().GetDefname() == "check_option" {
			continue
		}
		rest = append(rest, o)
	}
	return parseStorageParams(rest)
}

// viewCheckOption reads a view's check option from the trailing
// WITH [LOCAL | CASCADED] CHECK OPTION, which pg_dump and pista dump write, or
// from a check_option entry in the WITH (...) before AS. A bare WITH CHECK
// OPTION is CASCADED. PostgreSQL rejects a statement that writes both, so the
// order here does not matter.
func viewCheckOption(vs *pg_query.ViewStmt) string {
	switch vs.WithCheckOption {
	case pg_query.ViewCheckOption_LOCAL_CHECK_OPTION:
		return "local"
	case pg_query.ViewCheckOption_CASCADED_CHECK_OPTION:
		return "cascaded"
	}
	for _, opt := range vs.Options {
		de := opt.GetDefElem()
		if de == nil || de.Defname != "check_option" || de.Arg == nil {
			continue
		}
		// A quoted value arrives as a string, a bare one as a type name.
		if s := de.Arg.GetString_(); s != nil {
			return strings.ToLower(s.Sval)
		}
		if tn := de.Arg.GetTypeName(); tn != nil && len(tn.Names) > 0 {
			return strings.ToLower(tn.Names[len(tn.Names)-1].GetString_().GetSval())
		}
	}
	return ""
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

	columnNames := applyColumnNames(as.Query, into.ColNames)

	// Deparse the SELECT query
	selectResult := &pg_query.ParseResult{
		Version: pgast.ParseTreeVersion,
		Stmts: []*pg_query.RawStmt{{
			Stmt: as.Query,
		}},
	}
	def, err := pg_query.Deparse(selectResult)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse materialized view query: %w", err)
	}

	return &model.View{
		Schema:         schema,
		Name:           into.Rel.Relname,
		Definition:     def,
		ColumnNames:    columnNames,
		Materialized:   true,
		WithNoData:     into.SkipData,
		StorageParams:  parseViewStorageParams(into.Options),
		Indexes:        orderedmap.New[string, *model.Index](),
		Triggers:       orderedmap.New[string, *model.Trigger](),
		ColumnComments: orderedmap.New[string, string](),
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
				def = parenthesizeDefault(def)
				domain.Default = &def
			}
		case pg_query.ConstrType_CONSTR_CHECK:
			dc, err := parseDomainCheck(con, name)
			if err != nil {
				return nil, err
			}
			domain.Constraints = append(domain.Constraints, dc)
		}
	}

	return domain, nil
}

// parseDomainCheck converts a domain CHECK constraint. An unnamed one gets the
// name PostgreSQL would give it.
func parseDomainCheck(con *pg_query.Constraint, domainName string) (*model.DomainConstraint, error) {
	if con.Conname == "" {
		con.Conname = makeObjectName(domainName, "", "check")
	}
	def := ""
	if con.RawExpr != nil {
		expr, err := deparseExpr(con.RawExpr)
		if err != nil {
			return nil, fmt.Errorf("failed to deparse constraint %s for domain %s: %w", con.Conname, domainName, err)
		}
		def = "CHECK (" + expr + ")"
	}
	return &model.DomainConstraint{
		Name:       con.Conname,
		Definition: def,
		Validated:  !con.SkipValidation,
	}, nil
}

// parseAlterDomainAddCheck reads ALTER DOMAIN ... ADD CONSTRAINT ... CHECK,
// which is how a NOT VALID domain constraint is written. It returns false for
// any other ALTER DOMAIN, which the caller warns about.
func parseAlterDomainAddCheck(
	ads *pg_query.AlterDomainStmt,
	defaultSchema string,
	domains *orderedmap.Map[string, *model.Domain],
	offset int32,
) (bool, error) {
	con := ads.GetDef().GetConstraint()
	if ads.Subtype != "C" || con == nil || con.Contype != pg_query.ConstrType_CONSTR_CHECK {
		return false, nil
	}

	schema := defaultSchema
	name := ""
	for i, n := range ads.TypeName {
		if s := n.GetString_(); s != nil {
			if i == len(ads.TypeName)-1 {
				name = s.Sval
			} else {
				schema = s.Sval
			}
		}
	}
	fqdn := model.Ident(schema, name)
	d, ok := domains.GetOk(fqdn)
	if !ok {
		return false, undeclared("ALTER DOMAIN "+fqdn, "domain", fqdn, offset)
	}

	dc, err := parseDomainCheck(con, name)
	if err != nil {
		return false, err
	}
	for _, c := range d.Constraints {
		if c.Name == dc.Name {
			return false, &locatedError{
				msg:    fmt.Sprintf("duplicate domain constraint: %s on %s", dc.Name, fqdn),
				offset: int(offset),
			}
		}
	}
	d.Constraints = append(d.Constraints, dc)
	return true, nil
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
	if d := findDomain(cs.Object.GetTypeName(), defaultSchema, domains); d != nil {
		d.Comment = commentPtr(cs.Comment)
	}
}

// parseCommentOnDomainConstraint reads COMMENT ON CONSTRAINT ... ON DOMAIN,
// whose object is the domain as a type name followed by the constraint's name.
func parseCommentOnDomainConstraint(cs *pg_query.CommentStmt, defaultSchema string, domains *orderedmap.Map[string, *model.Domain]) {
	items := cs.Object.GetList().GetItems()
	d := findDomain(items[0].GetTypeName(), defaultSchema, domains)
	if d == nil {
		return
	}
	name := items[1].GetString_().GetSval()
	for _, c := range d.Constraints {
		if c.Name == name {
			c.Comment = commentPtr(cs.Comment)
			return
		}
	}
}

// findDomain returns the domain a type name names, or nil when the file does
// not define it.
func findDomain(tn *pg_query.TypeName, defaultSchema string, domains *orderedmap.Map[string, *model.Domain]) *model.Domain {
	var names []string
	for _, n := range tn.Names {
		if s := n.GetString_(); s != nil {
			names = append(names, s.Sval)
		}
	}
	schema, domainName := schemaName(names, defaultSchema)
	return domains.Get(model.Ident(schema, domainName))
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
		Unlogged:    cs.Sequence.Relpersistence == "u",
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
// already-parsed sequence. OWNED BY NONE clears the owner. Other ALTER
// SEQUENCE options are not tracked.
func applyAlterSeqOwnedBy(as *pg_query.AlterSeqStmt, defaultSchema string, sequences *orderedmap.Map[string, *model.Sequence], offset int32) (*model.Sequence, error) {
	if as.Sequence == nil {
		return nil, nil
	}
	schema := as.Sequence.Schemaname
	if schema == "" {
		schema = defaultSchema
	}
	fqn := model.Ident(schema, as.Sequence.Relname)
	seq, ok := sequences.GetOk(fqn)
	if !ok {
		return nil, undeclared("ALTER SEQUENCE "+fqn, "sequence", fqn, offset)
	}
	for _, opt := range as.Options {
		de := opt.GetDefElem()
		if de == nil || de.Defname != "owned_by" {
			continue
		}
		seq.OwnerTable, seq.OwnerColumn = parseSeqOwnedBy(de.Arg)
	}
	return seq, nil
}

// warnIgnoredAlterSeqOptions warns about the ALTER SEQUENCE options other
// than OWNED BY, which the parser does not read, the way
// warnIgnoredAlterTableCmds does for an ALTER TABLE action: the warning
// carries a statement rebuilt from the dropped options alone.
func warnIgnoredAlterSeqOptions(sql string, spans []fileSpan, rawStmt *pg_query.RawStmt, as *pg_query.AlterSeqStmt) {
	var ignored []*pg_query.Node

	for _, opt := range as.Options {
		if de := opt.GetDefElem(); de != nil && de.Defname == "owned_by" {
			continue
		}
		ignored = append(ignored, opt)
	}

	if len(ignored) == 0 {
		return
	}

	warnIgnoredStmt(sql, spans, &pg_query.RawStmt{
		Stmt: &pg_query.Node{
			Node: &pg_query.Node_AlterSeqStmt{
				AlterSeqStmt: &pg_query.AlterSeqStmt{
					Sequence:  as.Sequence,
					Options:   ignored,
					MissingOk: as.MissingOk,
				},
			},
		},
		StmtLocation: rawStmt.StmtLocation,
		StmtLen:      rawStmt.StmtLen,
	})
}

// parseSeqOwnedBy extracts the owner table and column from an OWNED BY clause.
// OWNED BY NONE (list ["none"]) yields nil owner. The table's schema is
// dropped, because PostgreSQL requires it to be the sequence's schema.
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
	if len(names) < 2 {
		return nil, nil
	}
	table := names[len(names)-2]
	column := names[len(names)-1]
	return &table, &column
}

func parseCommentStmt(cs *pg_query.CommentStmt, defaultSchema string, tables *orderedmap.Map[string, *model.Table], views *orderedmap.Map[string, *model.View], enums *orderedmap.Map[string, *model.Enum], domains *orderedmap.Map[string, *model.Domain], compositeTypes *orderedmap.Map[string, *model.CompositeType], sequences *orderedmap.Map[string, *model.Sequence], routines *orderedmap.Map[string, *model.Routine]) {
	// COMMENT ON TYPE/DOMAIN uses TypeName, not a list. COMMENT ON TYPE also
	// names a domain.
	if cs.Objtype == pg_query.ObjectType_OBJECT_TYPE {
		parseCommentOnType(cs, defaultSchema, enums, compositeTypes)
		parseCommentOnDomain(cs, defaultSchema, domains)
		return
	}
	if cs.Objtype == pg_query.ObjectType_OBJECT_DOMAIN {
		parseCommentOnDomain(cs, defaultSchema, domains)
		return
	}
	if cs.Objtype == pg_query.ObjectType_OBJECT_DOMCONSTRAINT {
		parseCommentOnDomainConstraint(cs, defaultSchema, domains)
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
		schema, tableName := schemaName(names, defaultSchema)
		fqtn := model.Ident(schema, tableName)
		if t, ok := tables.GetOk(fqtn); ok {
			t.Comment = commentPtr(cs.Comment)
		}
	case pg_query.ObjectType_OBJECT_VIEW, pg_query.ObjectType_OBJECT_MATVIEW:
		schema, viewName := schemaName(names, defaultSchema)
		fqvn := model.Ident(schema, viewName)
		if v, ok := views.GetOk(fqvn); ok {
			v.Comment = commentPtr(cs.Comment)
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
		comment := commentPtr(cs.Comment)
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
		// COMMENT ON COLUMN also targets the columns of a view or a
		// materialized view. The column list is not checked: the view's
		// columns come from its query, which the parser does not resolve.
		if v, ok := views.GetOk(fqtn); ok {
			if comment != nil {
				v.ColumnComments.Set(colName, *comment)
			} else {
				v.ColumnComments.Delete(colName)
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
	case pg_query.ObjectType_OBJECT_INDEX:
		schema, idxName := schemaName(names, defaultSchema)
		// COMMENT ON INDEX names the index alone, so the relation it sits on
		// is found by scanning. An index name is unique within its schema, and
		// the index a constraint owns is not in the model on either side.
		if idx := findIndex(tables, views, schema, idxName); idx != nil {
			idx.Comment = commentPtr(cs.Comment)
		}
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		schema, seqName := schemaName(names, defaultSchema)
		fqn := model.Ident(schema, seqName)
		if seq, ok := sequences.GetOk(fqn); ok {
			seq.Comment = commentPtr(cs.Comment)
		}
	case pg_query.ObjectType_OBJECT_TABCONSTRAINT:
		// A foreign key is a constraint too, and the two maps share the
		// namespace PostgreSQL gives constraint names on a table.
		t := tables.Get(relationIdent(names, defaultSchema))
		if t == nil {
			return
		}
		name := names[len(names)-1]
		if con, ok := t.Constraints.GetOk(name); ok {
			con.Comment = commentPtr(cs.Comment)
		} else if fk, ok := t.ForeignKeys.GetOk(name); ok {
			fk.Comment = commentPtr(cs.Comment)
		}
	case pg_query.ObjectType_OBJECT_TRIGGER:
		// A view carries INSTEAD OF triggers. Every table and view the parser
		// builds holds a trigger map, so neither is checked for nil.
		fqn := relationIdent(names, defaultSchema)
		name := names[len(names)-1]
		var trg *model.Trigger
		if t, ok := tables.GetOk(fqn); ok {
			trg = t.Triggers.Get(name)
		} else if v, ok := views.GetOk(fqn); ok {
			trg = v.Triggers.Get(name)
		}
		if trg != nil {
			trg.Comment = commentPtr(cs.Comment)
		}
	case pg_query.ObjectType_OBJECT_POLICY:
		if t, ok := tables.GetOk(relationIdent(names, defaultSchema)); ok {
			if p := t.Policies.Get(names[len(names)-1]); p != nil {
				p.Comment = commentPtr(cs.Comment)
			}
		}
	}
}

// relationIdent names the relation a constraint, a trigger or a policy sits
// on from the name parts COMMENT ON gives it: the relation, one- or
// two-part, followed by the object's own name.
func relationIdent(names []string, defaultSchema string) string {
	schema, name := schemaName(names[:len(names)-1], defaultSchema)
	return model.Ident(schema, name)
}

// commentPtr returns a pointer to a copy of the COMMENT ON text, or nil for an
// empty string, which removes the comment.
func commentPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// schemaName splits a one- or two-part object name into its schema and name,
// taking def as the schema when the name is unqualified.
func schemaName(names []string, def string) (schema, name string) {
	schema = def
	name = names[0]
	if len(names) >= 2 {
		schema = names[0]
		name = names[1]
	}
	return schema, name
}

// findIndex returns the index of the given schema and name from the tables and
// views that carry one, or nil when no relation declares it. Every table and
// view the parser builds holds an index map, so neither is checked for nil.
func findIndex(tables *orderedmap.Map[string, *model.Table], views *orderedmap.Map[string, *model.View], schema, name string) *model.Index {
	for _, t := range tables.CollectValues() {
		if idx, ok := t.Indexes.GetOk(name); ok && idx.Schema == schema {
			return idx
		}
	}
	for _, v := range views.CollectValues() {
		if idx, ok := v.Indexes.GetOk(name); ok && idx.Schema == schema {
			return idx
		}
	}
	return nil
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
	schema, typeName := schemaName(names, defaultSchema)
	// COMMENT ON TYPE names both enums and composite types; set on whichever
	// this file defines.
	fqn := model.Ident(schema, typeName)
	comment := commentPtr(cs.Comment)
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

		if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
			fk, err := parseInlineForeignKey(con, schema, as.Relation.Relname, defaultSchema)
			if err != nil {
				return nil, nil, err
			}
			fks = append(fks, fk)
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

// parseInlineForeignKey builds a ForeignKey from a FOREIGN KEY constraint
// written inside CREATE TABLE or added by ALTER TABLE.
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

	return &model.ForeignKey{
		Name:       con.Conname,
		Type:       model.ConstraintType('f'),
		Definition: def,
		Columns:    fkAttrCols(con),
		Deferrable: con.Deferrable,
		Deferred:   con.Initdeferred,
		Validated:  !con.SkipValidation,
		Schema:     schema,
		Table:      table,
		RefSchema:  refSchema,
		RefTable:   refTable,
	}, nil
}

// fillFKRefColumns fills in the referenced columns of a foreign key that
// leaves them out, taking the referenced table's primary key as PostgreSQL
// does. The catalog prints those columns, so a key without them never matches
// it. This runs after every statement is read, since the referenced table or
// its primary key may come later. A bare table name is looked up in the first
// target schema and then in the key's own, the two schemas the diff matches a
// bare name against. A key to a table the schema does not declare, or to one
// with no primary key, is left as written.
func fillFKRefColumns(tables *orderedmap.Map[string, *model.Table]) error {
	for t := range tables.Values() {
		for fk := range t.ForeignKeys.Values() {
			con := pgast.ParseConstraintDef(fk.Definition)
			if con == nil || len(con.PkAttrs) > 0 {
				continue
			}
			ref, ok := tables.GetOk(model.Ident(*fk.RefSchema, *fk.RefTable))
			if !ok && con.Pktable.Schemaname == "" {
				ref, ok = tables.GetOk(model.Ident(t.Schema, *fk.RefTable))
			}
			if !ok {
				continue
			}
			pkCols := primaryKeyColumns(ref)
			if len(pkCols) == 0 {
				continue
			}
			for _, col := range pkCols {
				con.PkAttrs = append(con.PkAttrs, pg_query.MakeStrNode(col))
			}
			def, err := deparseConstraintDef(con)
			if err != nil {
				return fmt.Errorf("failed to deparse constraint %s: %w", fk.Name, err)
			}
			fk.Definition = def
		}
	}
	return nil
}

// primaryKeyColumns returns the columns of a table's primary key, or nil when
// it has none. A key written USING INDEX lists no columns, so they are read
// from the index it takes over.
func primaryKeyColumns(t *model.Table) []string {
	for con := range t.Constraints.Values() {
		if !con.Type.IsPrimaryKeyConstraint() {
			continue
		}
		if con.IndexName == "" {
			return con.Columns
		}
		idx, ok := t.Indexes.GetOk(con.IndexName)
		if !ok {
			return nil
		}
		result, err := pg_query.Parse(idx.Definition)
		if err != nil {
			return nil
		}
		var cols []string
		for _, param := range result.Stmts[0].Stmt.GetIndexStmt().GetIndexParams() {
			cols = append(cols, param.GetIndexElem().GetName())
		}
		return cols
	}
	return nil
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
		Version: pgast.ParseTreeVersion,
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
	_, rest, ok := strings.Cut(sql, marker)
	if !ok {
		return "", fmt.Errorf("unexpected deparse output for type: %s", sql)
	}
	typeName, _, ok := strings.CutLast(rest, ")")
	if !ok {
		return "", fmt.Errorf("unexpected deparse output for type: %s", sql)
	}
	typeName = strings.TrimSpace(typeName)
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
	"serial2":     "smallserial",
	"serial4":     "serial",
	"serial8":     "bigserial",
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
		base = canonical
	}

	// The array marker is separated so a modifier on an array type is reached:
	// numeric(5)[] is stored as numeric(5,0)[].
	mod, array := splitTypeSuffix(suffix)
	if base == "numeric" {
		mod = fillNumericScale(mod)
	}

	return base + mod + array
}

// splitTypeSuffix separates a type's modifier from its array marker:
// "(10,2)[]" becomes "(10,2)" and "[]".
func splitTypeSuffix(suffix string) (mod, array string) {
	if strings.HasPrefix(suffix, "(") {
		if end := strings.Index(suffix, ")"); end != -1 {
			return suffix[:end+1], suffix[end+1:]
		}
		return suffix, ""
	}
	return "", suffix
}

// fillNumericScale writes the scale a numeric modifier leaves out. PostgreSQL
// takes "numeric(5)" as "numeric(5,0)" and format_type prints both digits, so
// without this the declaration and the catalog never compare equal.
func fillNumericScale(mod string) string {
	if mod == "" || strings.Contains(mod, ",") {
		return mod
	}
	precision := strings.TrimSuffix(strings.TrimPrefix(mod, "("), ")")
	if precision == "" {
		return mod
	}
	for _, r := range precision {
		if r < '0' || r > '9' {
			return mod
		}
	}
	return "(" + precision + ",0)"
}

func deparseExpr(node *pg_query.Node) (string, error) {
	result := &pg_query.ParseResult{
		Version: pgast.ParseTreeVersion,
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
	expr, ok := strings.CutPrefix(sql, "SELECT ")
	if !ok {
		return "", fmt.Errorf("unexpected deparse output for expression: %s", sql)
	}
	return strings.TrimSpace(expr), nil
}

// parenthesizeDefault wraps a deparsed column or domain DEFAULT expression in
// parentheses when it does not stand there on its own. The grammar takes a
// restricted expression after DEFAULT, and deparseExpr renders a SELECT
// target, so AT TIME ZONE, AND and the like come out without the parentheses
// DEFAULT needs. A trailing COLLATE parses, but as the column's collation
// rather than part of the default. Any other expression is left as
// deparseExpr writes it.
//
// The answer is kept per expression, since most tables repeat the same few
// defaults and each check is a parse through cgo.
func parenthesizeDefault(def string) string {
	if v, ok := parenthesizedDefaults.Load(def); ok {
		return v.(string)
	}

	v := def
	tree, err := pg_query.Parse("CREATE TABLE t (c int DEFAULT " + def + ")")
	if err != nil || tree.Stmts[0].Stmt.GetCreateStmt().TableElts[0].GetColumnDef().CollClause != nil {
		v = "(" + def + ")"
	}
	parenthesizedDefaults.Store(def, v)

	return v
}

var parenthesizedDefaults sync.Map

func deparseConstraintDef(con *pg_query.Constraint) (string, error) {
	// Temporarily clear SkipValidation so "NOT VALID" is not included in the
	// definition string (it is tracked separately via the Validated field).
	origSkipValidation := con.SkipValidation
	con.SkipValidation = false
	defer func() { con.SkipValidation = origSkipValidation }()

	// Clear the name too, so the definition is whatever follows the fixed
	// "ALTER TABLE _t ADD " prefix. Looking for the name in the output fails
	// when the deparser quotes it and model.Ident does not, as with "time".
	origConname := con.Conname
	con.Conname = ""
	defer func() { con.Conname = origConname }()

	alterCmd := &pg_query.AlterTableCmd{
		Subtype: pg_query.AlterTableType_AT_AddConstraint,
		Def:     &pg_query.Node{Node: &pg_query.Node_Constraint{Constraint: con}},
	}
	result := &pg_query.ParseResult{
		Version: pgast.ParseTreeVersion,
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

	def, ok := strings.CutPrefix(sql, "ALTER TABLE _t ADD ")
	if !ok {
		return "", fmt.Errorf("could not extract constraint definition from: %s", sql)
	}
	return strings.TrimSpace(def), nil
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
		Version: pgast.ParseTreeVersion,
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{Node: &pg_query.Node_CreateStmt{CreateStmt: minCS}},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse partition spec: %w", err)
	}
	const prefix = "PARTITION BY "
	_, after, ok := strings.Cut(sql, prefix)
	if !ok {
		return "", fmt.Errorf("could not extract partition spec from: %s", sql)
	}
	return strings.TrimSpace(after), nil
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
		Version: pgast.ParseTreeVersion,
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{Node: &pg_query.Node_CreateStmt{CreateStmt: minCS}},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse partition bound: %w", err)
	}
	const prefix = "PARTITION OF _parent "
	_, after, ok := strings.Cut(sql, prefix)
	if !ok {
		return "", fmt.Errorf("could not extract partition bound from: %s", sql)
	}
	return strings.TrimSpace(after), nil
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

// addCreateTablePositions records where a table, its columns and its foreign
// keys are declared. A foreign key written as a table constraint is placed at
// its first keyword. parseCreateStmt has named an unnamed one by then. A key
// written on a column is placed at the column, and any other at the table.
func addCreateTablePositions(add func(LintTarget, int32), cs *pg_query.CreateStmt, table *model.Table, stmtOffset int32) {
	fqtn := table.FQTN()
	add(LintTarget{Kind: LintTable, Table: fqtn}, stmtOffset)

	namedFKs := map[string]int32{}
	columnFKs := map[string]int32{}
	for _, elt := range cs.TableElts {
		if cd := elt.GetColumnDef(); cd != nil {
			add(LintTarget{Kind: LintColumn, Table: fqtn, Name: cd.Colname}, cd.Location)
			for _, c := range cd.Constraints {
				if con := c.GetConstraint(); con != nil && con.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
					columnFKs[cd.Colname] = cd.Location
				}
			}
		} else if con := elt.GetConstraint(); con != nil && con.Contype == pg_query.ConstrType_CONSTR_FOREIGN && con.Conname != "" {
			namedFKs[con.Conname] = con.Location
		}
	}

	for name, fk := range table.ForeignKeys.All() {
		offset := stmtOffset
		if loc, ok := namedFKs[name]; ok {
			offset = loc
		} else if len(fk.Columns) == 1 {
			if loc, ok := columnFKs[fk.Columns[0]]; ok {
				offset = loc
			}
		}
		add(LintTarget{Kind: LintForeignKey, Table: fqtn, Name: name}, offset)
	}
}
