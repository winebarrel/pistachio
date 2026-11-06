package dsqlproxy

import (
	"regexp"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v18"
)

// The messages come from DSQL.md, which records what a live cluster
// answered. TestConformance checks them against the cluster.
const (
	msgAsyncIndex       = "unsupported mode. please use CREATE INDEX ASYNC."
	msgIndexUsing       = "USING not supported for CREATE INDEX"
	msgIndexConcurrent  = "CONCURRENTLY not supported for CREATE INDEX"
	msgDropConcurrent   = "DROP INDEX CONCURRENTLY must be first action in transaction"
	msgAddColumn        = "ALTER TABLE ADD COLUMN with constraint not supported"
	msgCollate          = "COLLATE clause not supported"
	msgCompression      = "COMPRESSION clause not supported"
	msgSetNotNull       = "unsupported ALTER TABLE ALTER COLUMN ... SET NOT NULL statement"
	msgSetDataType      = "unsupported ALTER TABLE ALTER COLUMN ... SET DATA TYPE statement"
	msgSetCompression   = "unsupported ALTER TABLE ALTER COLUMN ... SET COMPRESSION statement"
	msgAddConstraint    = "unsupported ALTER TABLE ADD CONSTRAINT statement"
	msgValidate         = "unsupported ALTER TABLE VALIDATE CONSTRAINT statement"
	msgAsyncAlter       = "ALTER TABLE ASYNC not supported"
	msgRowSecurity      = "unsupported ALTER TABLE ENABLE ROW SECURITY statement"
	msgPartitionBy      = "PARTITION BY clause not supported for CREATE TABLE"
	msgPartitionOf      = "PARTITION OF clause not supported for CREATE TABLE"
	msgIdentityCache    = "identity column is not supported without an explicit cache size. please define CACHE greater than or equal to 65536 or equal to 1"
	msgSequenceCache    = "CREATE SEQUENCE is not supported without an explicit cache size. please define CACHE greater than or equal to 65536 or equal to 1"
	msgDomainCheck      = "ADD CONSTRAINT clause not supported for ALTER DOMAIN"
	msgDomainNotNull    = "SET NOT NULL clause not supported for ALTER DOMAIN"
	msgMultipleDDL      = "multiple ddl statements not supported in a transaction"
	msgDefaultReadOnly  = `setting configuration parameter "default_transaction_read_only" not supported`
	msgAdvisoryLockFunc = "function %s not supported"
)

var (
	asyncIndexRe = regexp.MustCompile(`(?is)^(\s*create\s+(?:unique\s+)?index)\s+async\b(.*)$`)
	asyncAlterRe = regexp.MustCompile(`(?is)^(\s*alter\s+table)\s+async\b(.*)$`)
	indexUsingRe = regexp.MustCompile(`(?is)\bon\s+(?:only\s+)?[^(]*\busing\b`)

	// The catalog values DSQL reports differently are read through the
	// functions setup.sql defines in their place.
	catalogFuncRe  = regexp.MustCompile(`(?i)\b(?:pg_catalog\.)?(pg_get_constraintdef|pg_get_indexdef)\s*\(`)
	attcompressRe  = regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)\.attcompression\b`)
	advisoryLockRe = regexp.MustCompile(`(?i)\b(pg_(?:try_)?advisory_(?:xact_)?lock(?:_shared)?)\s*\(`)
)

// session is the state of one client connection the rules need: how many DDL
// statements the current transaction has run.
type session struct {
	inTx     func() bool
	ddlCount int
}

// rewrite returns the text to send to the backend in place of sql, and for
// each statement in it the command tag DSQL answers with, or "" to keep the
// backend's. A statement that needs no change is sent as it is.
func (s *session) rewrite(sql string) (string, []string) {
	stmts, err := pg_query.SplitWithScanner(sql, true)
	if err != nil || len(stmts) == 0 {
		return sql, []string{""}
	}

	changed := false
	out := make([]string, len(stmts))
	tags := make([]string, len(stmts))
	for i, stmt := range stmts {
		// The statements of one query string run in one transaction.
		out[i], tags[i] = s.rewriteStmt(stmt, i > 0)
		changed = changed || out[i] != stmt
	}
	if !changed {
		return sql, tags
	}
	return strings.Join(out, ";\n"), tags
}

func (s *session) rewriteStmt(stmt string, implicitTx bool) (string, string) {
	if m := asyncIndexRe.FindStringSubmatch(stmt); m != nil {
		body := m[1] + m[2]
		if msg := asyncIndexRefusal(body); msg != "" {
			return refuse(msg), ""
		}
		if msg := s.countDDL(implicitTx); msg != "" {
			return refuse(msg), ""
		}
		return runJob("INDEX_BUILD", body), "CREATE INDEX"
	}

	if m := asyncAlterRe.FindStringSubmatch(stmt); m != nil {
		body := m[1] + m[2]
		if !isValidate(body) {
			return refuse(msgAsyncAlter), ""
		}
		if msg := s.countDDL(implicitTx); msg != "" {
			return refuse(msg), ""
		}
		return runJob("VALIDATE_CONSTRAINT", body), "ALTER TABLE"
	}

	result, err := pg_query.Parse(stmt)
	if err != nil || len(result.Stmts) != 1 {
		// The backend reports the error.
		return stmt, ""
	}
	node := result.Stmts[0].Stmt

	if msg := refusal(node); msg != "" {
		return refuse(msg), ""
	}

	if isDDL(node) {
		if msg := s.countDDL(implicitTx); msg != "" {
			return refuse(msg), ""
		}
		if guard := dropPkeyGuard(node); guard != "" {
			return "DO $dsql_proxy$BEGIN " + guard + " EXECUTE " + quoteLiteral(stmt) + "; END$dsql_proxy$", "ALTER TABLE"
		}
		return stmt, ""
	}

	if node.GetSelectStmt() != nil {
		if m := advisoryLockRe.FindStringSubmatch(stmt); m != nil {
			return refuse(strings.Replace(msgAdvisoryLockFunc, "%s", strings.ToLower(m[1]), 1)), ""
		}
		stmt = catalogFuncRe.ReplaceAllString(stmt, "dsql_proxy.$1(")
		stmt = attcompressRe.ReplaceAllString(stmt, "dsql_proxy.attcompression($1)")
	}

	return stmt, ""
}

// countDDL counts a DDL statement and refuses the second one in a
// transaction.
func (s *session) countDDL(implicitTx bool) string {
	s.ddlCount++
	if s.ddlCount > 1 && (implicitTx || s.inTx()) {
		return msgMultipleDDL
	}
	return ""
}

// txEnded starts the count again once the backend is out of a transaction.
func (s *session) txEnded() {
	s.ddlCount = 0
}

func asyncIndexRefusal(body string) string {
	result, err := pg_query.Parse(body)
	if err != nil || len(result.Stmts) != 1 {
		return ""
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return ""
	}
	if is.GetConcurrent() {
		return msgIndexConcurrent
	}
	if indexUsingRe.MatchString(body) {
		return msgIndexUsing
	}
	return ""
}

func isValidate(body string) bool {
	result, err := pg_query.Parse(body)
	if err != nil || len(result.Stmts) != 1 {
		return false
	}
	at := result.Stmts[0].Stmt.GetAlterTableStmt()
	if at == nil || len(at.GetCmds()) != 1 {
		return false
	}
	return at.GetCmds()[0].GetAlterTableCmd().GetSubtype() == pg_query.AlterTableType_AT_ValidateConstraint
}

// refusal returns the message DSQL refuses the statement with, or "" when
// DSQL takes it.
func refusal(node *pg_query.Node) string {
	switch {
	case node.GetIndexStmt() != nil:
		if node.GetIndexStmt().GetConcurrent() {
			return msgIndexConcurrent
		}
		return msgAsyncIndex

	case node.GetDropStmt() != nil:
		ds := node.GetDropStmt()
		if ds.GetConcurrent() && ds.GetRemoveType() == pg_query.ObjectType_OBJECT_INDEX {
			return msgDropConcurrent
		}

	case node.GetAlterTableStmt() != nil:
		at := node.GetAlterTableStmt()
		if at.GetObjtype() != pg_query.ObjectType_OBJECT_TABLE {
			return ""
		}
		for _, c := range at.GetCmds() {
			if msg := alterTableCmdRefusal(c.GetAlterTableCmd()); msg != "" {
				return msg
			}
		}

	case node.GetCreateStmt() != nil:
		cs := node.GetCreateStmt()
		if cs.GetPartspec() != nil {
			return msgPartitionBy
		}
		if cs.GetPartbound() != nil {
			return msgPartitionOf
		}
		for _, elt := range cs.GetTableElts() {
			col := elt.GetColumnDef()
			if col == nil {
				continue
			}
			if col.GetCollClause() != nil {
				return msgCollate
			}
			if col.GetCompression() != "" {
				return msgCompression
			}
			for _, c := range col.GetConstraints() {
				con := c.GetConstraint()
				if con.GetContype() == pg_query.ConstrType_CONSTR_IDENTITY && !hasOption(con.GetOptions(), "cache") {
					return msgIdentityCache
				}
			}
		}

	case node.GetCreateSeqStmt() != nil:
		if !hasOption(node.GetCreateSeqStmt().GetOptions(), "cache") {
			return msgSequenceCache
		}

	case node.GetAlterDomainStmt() != nil:
		switch node.GetAlterDomainStmt().GetSubtype() {
		case "C":
			return msgDomainCheck
		case "O":
			return msgDomainNotNull
		}

	case node.GetVariableSetStmt() != nil:
		if node.GetVariableSetStmt().GetName() == "default_transaction_read_only" {
			return msgDefaultReadOnly
		}

	case node.GetCreateEnumStmt() != nil:
		return "CREATE TYPE not supported"
	case node.GetCompositeTypeStmt() != nil:
		return "unsupported statement: CompositeType"
	case node.GetCreatePolicyStmt() != nil:
		return "unsupported statement: CreatePolicy"
	case node.GetCreateExtensionStmt() != nil:
		return "unsupported statement: CreateExtension"
	case node.GetCreateTableAsStmt() != nil:
		return "unsupported statement: CreateTableAs"
	}
	return ""
}

func alterTableCmdRefusal(cmd *pg_query.AlterTableCmd) string {
	switch cmd.GetSubtype() {
	case pg_query.AlterTableType_AT_AddColumn:
		col := cmd.GetDef().GetColumnDef()
		if col.GetCollClause() != nil {
			return msgCollate
		}
		if col.GetCompression() != "" {
			return msgCompression
		}
		if len(col.GetConstraints()) > 0 {
			return msgAddColumn
		}
	case pg_query.AlterTableType_AT_SetNotNull:
		return msgSetNotNull
	case pg_query.AlterTableType_AT_AlterColumnType:
		return msgSetDataType
	case pg_query.AlterTableType_AT_SetCompression:
		return msgSetCompression
	case pg_query.AlterTableType_AT_ValidateConstraint:
		return msgValidate
	case pg_query.AlterTableType_AT_EnableRowSecurity, pg_query.AlterTableType_AT_ForceRowSecurity:
		return msgRowSecurity
	case pg_query.AlterTableType_AT_AddConstraint:
		con := cmd.GetDef().GetConstraint()
		switch con.GetContype() {
		case pg_query.ConstrType_CONSTR_UNIQUE:
			if con.GetIndexname() == "" {
				return msgAddConstraint
			}
		case pg_query.ConstrType_CONSTR_CHECK, pg_query.ConstrType_CONSTR_FOREIGN:
			if !con.GetSkipValidation() {
				return msgAddConstraint
			}
		default:
			return msgAddConstraint
		}
	}
	return ""
}

// dropPkeyGuard returns the call that refuses the drop of a primary key, for
// an ALTER TABLE that drops a constraint. Whether the constraint is the
// primary key is known only to the backend.
func dropPkeyGuard(node *pg_query.Node) string {
	at := node.GetAlterTableStmt()
	if at == nil || at.GetObjtype() != pg_query.ObjectType_OBJECT_TABLE {
		return ""
	}
	rel := quoteIdent(at.GetRelation().GetRelname())
	if s := at.GetRelation().GetSchemaname(); s != "" {
		rel = quoteIdent(s) + "." + rel
	}
	var guard strings.Builder
	for _, c := range at.GetCmds() {
		cmd := c.GetAlterTableCmd()
		if cmd.GetSubtype() == pg_query.AlterTableType_AT_DropConstraint {
			guard.WriteString("PERFORM dsql_proxy.refuse_drop_pkey(")
			guard.WriteString(quoteLiteral(rel))
			guard.WriteString(", ")
			guard.WriteString(quoteLiteral(cmd.GetName()))
			guard.WriteString(");")
		}
	}
	return guard.String()
}

// isDDL reports a statement that DSQL counts toward its one DDL statement per
// transaction.
func isDDL(node *pg_query.Node) bool {
	switch {
	case node.GetSelectStmt() != nil,
		node.GetInsertStmt() != nil,
		node.GetUpdateStmt() != nil,
		node.GetDeleteStmt() != nil,
		node.GetMergeStmt() != nil,
		node.GetTransactionStmt() != nil,
		node.GetVariableSetStmt() != nil,
		node.GetVariableShowStmt() != nil,
		node.GetCallStmt() != nil,
		node.GetExplainStmt() != nil,
		node.GetCopyStmt() != nil,
		node.GetPrepareStmt() != nil,
		node.GetExecuteStmt() != nil,
		node.GetDeallocateStmt() != nil,
		node.GetDiscardStmt() != nil,
		node.GetLockStmt() != nil,
		node.GetDoStmt() != nil:
		return false
	}
	return true
}

func hasOption(opts []*pg_query.Node, name string) bool {
	return slices.ContainsFunc(opts, func(o *pg_query.Node) bool {
		return o.GetDefElem().GetDefname() == name
	})
}

func runJob(jobType, stmt string) string {
	return "SELECT job_id FROM dsql_proxy.run_job(" + quoteLiteral(jobType) + ", " + quoteLiteral(stmt) + ")"
}

func refuse(msg string) string {
	return "SELECT dsql_proxy.refuse('0A000', " + quoteLiteral(msg) + ")"
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
