package parser

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// parseCreateTrigStmt converts a CreateTrigStmt into a model.Trigger. The
// relation is qualified with defaultSchema when the statement leaves it bare,
// so the stored definition names the same relation whatever search_path the
// plan runs under. OR REPLACE is dropped: it says how to reach the state, not
// what the state is, and the diff decides that.
func parseCreateTrigStmt(ct *pg_query.CreateTrigStmt, defaultSchema string) (*model.Trigger, error) {
	if ct.Relation == nil {
		return nil, fmt.Errorf("CREATE TRIGGER %s: missing table reference", ct.Trigname)
	}
	schema := ct.Relation.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	// The statement is deparsed from the node itself. Nothing reads the parse
	// tree after this case, so the two fields are set in place rather than on
	// a copy.
	ct.Relation.Schemaname = schema
	ct.Replace = false

	def, err := pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{Stmt: &pg_query.Node{Node: &pg_query.Node_CreateTrigStmt{CreateTrigStmt: ct}}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("CREATE TRIGGER %s: failed to deparse: %w", ct.Trigname, err)
	}

	return &model.Trigger{
		Schema:     schema,
		Table:      ct.Relation.Relname,
		Name:       ct.Trigname,
		Definition: def,
		State:      model.TriggerStateDefault,
	}, nil
}

// attachTrigger files a parsed trigger under the table or view it is on. A
// trigger names its relation, so the relation has to be declared first, the
// way CREATE POLICY needs its table. offset is where the statement starts,
// which a duplicate-name error points at.
func attachTrigger(
	trg *model.Trigger,
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
	offset int32,
) error {
	fqtn := trg.FQTN()
	if t, ok := tables.GetOk(fqtn); ok {
		return setUnique(t.Triggers, trg.Name, "trigger", trg, fqtn, offset)
	}
	if v, ok := views.GetOk(fqtn); ok {
		return setUnique(v.Triggers, trg.Name, "trigger", trg, fqtn, offset)
	}
	return fmt.Errorf("CREATE TRIGGER %s: relation %s not defined", trg.Name, fqtn)
}

// applyAlterTableTriggerState picks up the ENABLE / DISABLE TRIGGER
// subcommands and records the state on the named trigger. CREATE TRIGGER has
// no syntax for a disabled trigger, so this is the only way a desired schema
// can ask for one, and it is what dump writes. The ALL and USER forms name no
// single trigger and are left alone.
func applyAlterTableTriggerState(as *pg_query.AlterTableStmt, t *model.Table) {
	for _, cmdNode := range as.Cmds {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil {
			continue
		}
		var state model.TriggerState
		switch cmd.Subtype {
		case pg_query.AlterTableType_AT_EnableTrig:
			state = model.TriggerStateDefault
		case pg_query.AlterTableType_AT_DisableTrig:
			state = 'D'
		case pg_query.AlterTableType_AT_EnableAlwaysTrig:
			state = 'A'
		case pg_query.AlterTableType_AT_EnableReplicaTrig:
			state = 'R'
		default:
			continue
		}
		// A state for a trigger the file does not declare has nothing to sit
		// on, the same as an index on a table declared elsewhere.
		if trg, ok := t.Triggers.GetOk(cmd.Name); ok {
			trg.State = state
		}
	}
}
