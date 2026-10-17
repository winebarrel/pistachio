package pistachio

import (
	"errors"
	"fmt"

	"github.com/winebarrel/pistachio/dsql"
	"github.com/winebarrel/pistachio/parser"
)

// Engine is the server a run targets. postgres is PostgreSQL itself. dsql is
// Amazon Aurora DSQL: the current side is read the same way and the diff is
// PostgreSQL's, and the dsql package brings the two to the form DSQL takes.
type Engine string

const (
	EnginePostgres Engine = "postgres"
	EngineDSQL     Engine = "dsql"
)

// isDSQL is the one place the engine is compared, so a caller never spells
// the value. An empty engine is postgres: a library caller that builds the
// options by hand, and a plan file written before the engine was recorded.
func (e Engine) isDSQL() bool {
	return e == EngineDSQL
}

// validate refuses an engine the CLI would not have accepted. Only a plan file
// edited by hand reaches it.
func (e Engine) validate() error {
	switch e {
	case "", EnginePostgres, EngineDSQL:
		return nil
	}
	return fmt.Errorf("unknown engine %q", e)
}

// dsqlRefused is an option DSQL has no use for, and whether it was given.
type dsqlRefused struct {
	flag string
	set  bool
}

// refuseWithDSQL returns an error naming the first option given under
// --engine dsql. kong's xor tag cannot say this: it looks at whether a flag
// was given, not at its value, so it would refuse --engine postgres too.
func refuseWithDSQL(engine Engine, opts ...dsqlRefused) error {
	if !engine.isDSQL() {
		return nil
	}
	for _, o := range opts {
		if o.set {
			return fmt.Errorf("--%s cannot be used with --engine dsql", o.flag)
		}
	}
	return nil
}

// planRefused lists the plan options DSQL has no use for. --explain reads the
// size estimates in pg_class, which DSQL does not keep, and --refuse-hazard
// judges the locks a statement takes, which DSQL does not take. --bulk-alter saves the
// lock and the rewrite a combined ALTER TABLE spares, and DSQL has neither.
// DSQL has no CONCURRENTLY.
func planRefused(o *PlanOptions) []dsqlRefused {
	return []dsqlRefused{
		{"explain", o.Explain},
		{"refuse-hazard", len(o.RefuseHazard) > 0},
		{"bulk-alter", o.BulkAlter},
		{"not-null-via-check", o.NotNullViaCheck},
		{"force-index-concurrently", o.ForceIndexConcurrently},
		{"disable-index-concurrently", o.DisableIndexConcurrently},
		{"concurrently-pre-sql", o.ConcurrentlyPreSQL != ""},
		{"concurrently-pre-sql-file", o.ConcurrentlyPreSQLFile != ""},
	}
}

// execRefused lists the ExecOptions DSQL cannot run. DSQL takes one DDL
// statement per transaction and has no advisory lock. It refuses SET
// lock_timeout and SET statement_timeout, and with no locks there is no lock
// timeout to retry. --lock-retry-delay has a default, so it is always set and
// cannot be told apart from one given; without --lock-retries it does nothing.
func execRefused(o *ExecOptions) []dsqlRefused {
	return []dsqlRefused{
		{"with-tx", o.WithTx},
		{"try-tx", o.TryTx},
		{"exclusive", o.Exclusive},
		{"exclusive-wait", o.ExclusiveWait != nil},
		{"lock-timeout", o.LockTimeout != nil},
		{"lock-retries", o.LockRetries > 0},
		{"statement-timeout", o.StatementTimeout != nil},
	}
}

// requireDSQL refuses --dsql-no-wait-job without --engine dsql. The
// flag is DSQL's, and does nothing anywhere else.
func requireDSQL(engine Engine, o *ExecOptions) error {
	if o.DSQLNoWaitJob && !engine.isDSQL() {
		return errors.New("--dsql-no-wait-job requires --engine dsql")
	}
	return nil
}

// ValidatePlanEngine refuses the plan options --engine dsql cannot use.
func ValidatePlanEngine(engine Engine, o *PlanOptions) error {
	return refuseWithDSQL(engine, planRefused(o)...)
}

// ValidateApplyEngine refuses the apply options --engine dsql cannot use.
func ValidateApplyEngine(engine Engine, o *ApplyOptions) error {
	if err := requireDSQL(engine, &o.ExecOptions); err != nil {
		return err
	}
	return refuseWithDSQL(engine, append(planRefused(&PlanOptions{
		RefuseHazard:             o.RefuseHazard,
		BulkAlter:                o.BulkAlter,
		NotNullViaCheck:          o.NotNullViaCheck,
		ForceIndexConcurrently:   o.ForceIndexConcurrently,
		DisableIndexConcurrently: o.DisableIndexConcurrently,
		ConcurrentlyPreSQL:       o.ConcurrentlyPreSQL,
		ConcurrentlyPreSQLFile:   o.ConcurrentlyPreSQLFile,
	}), execRefused(&o.ExecOptions)...)...)
}

// ValidateApplyFromEngine refuses the apply-from options the plan file's
// engine cannot use. The engine is the plan file's, so the file is read here
// as well as where it is applied.
func ValidateApplyFromEngine(o *ApplyFromOptions) error {
	plan, err := readPlanFile(o.PlanFile)
	if err != nil {
		return err
	}
	engine := plan.Scope.Engine
	if err := engine.validate(); err != nil {
		return fmt.Errorf("plan file %s: %w", o.PlanFile, err)
	}
	if err := requireDSQL(engine, &o.ExecOptions); err != nil {
		return err
	}
	return refuseWithDSQL(engine, execRefused(&o.ExecOptions)...)
}

// ValidateDumpEngine refuses the dump options --engine dsql cannot use.
func ValidateDumpEngine(engine Engine, o *DumpOptions) error {
	return refuseWithDSQL(engine, dsqlRefused{"explain", o.Explain})
}

// parseStrippingAsync parses the desired schema files with the ASYNC of each
// CREATE INDEX ASYNC blanked out. ASYNC is how DSQL builds an index, not part
// of its definition, and the plan adds it to every index it creates.
func parseStrippingAsync(files []string, defaultSchema string) (*parser.ParseResult, error) {
	sources, err := parser.ReadSQLFiles(files)
	if err != nil {
		return nil, err
	}
	for i := range sources {
		sources[i].SQL = dsql.StripAsync(sources[i].SQL)
	}
	return parser.ParseSQLSourcesWithSchema(sources, defaultSchema)
}
