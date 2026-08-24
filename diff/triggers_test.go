package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func newTrigger(name, def string, opts ...func(*model.Trigger)) *model.Trigger {
	trg := &model.Trigger{
		Schema:     "public",
		Table:      "events",
		Name:       name,
		Definition: def,
		State:      model.TriggerStateDefault,
	}
	for _, o := range opts {
		o(trg)
	}
	return trg
}

func withState(s model.TriggerState) func(*model.Trigger) {
	return func(trg *model.Trigger) { trg.State = s }
}

func withTriggerRenameFrom(s string) func(*model.Trigger) {
	return func(trg *model.Trigger) { trg.RenameFrom = &s }
}

func triggers(list ...*model.Trigger) *orderedmap.Map[string, *model.Trigger] {
	m := orderedmap.New[string, *model.Trigger]()
	for _, trg := range list {
		m.Set(trg.Name, trg)
	}
	return m
}

const (
	insertDef = "CREATE TRIGGER events_stamp BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()"
	updateDef = "CREATE TRIGGER events_stamp BEFORE UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()"
	conDef    = "CREATE CONSTRAINT TRIGGER events_stamp AFTER INSERT ON public.events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stamp()"
)

func allowTriggerDrops() DropChecker {
	return &fakeDropChecker{allowed: map[string]bool{"trigger": true}}
}

type fakeDropChecker struct {
	allowed map[string]bool
}

func (f *fakeDropChecker) IsDropAllowed(objectType string) bool { return f.allowed[objectType] }

func TestDiffTriggers_NoChange(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef))
	des := triggers(newTrigger("events_stamp", insertDef))
	stmts, disallowed, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Empty(t, disallowed)
}

// The catalog leaves out a schema search_path reaches and spells out the parts
// the deparser omits. None of that is a change.
func TestDiffTriggers_NoChangeAcrossRenderings(t *testing.T) {
	cur := triggers(newTrigger("events_batch",
		"CREATE TRIGGER events_batch AFTER INSERT ON events REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION stamp()"))
	des := triggers(newTrigger("events_batch",
		"CREATE TRIGGER events_batch AFTER INSERT ON public.events REFERENCING NEW TABLE added EXECUTE FUNCTION stamp()"))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

// pg_get_triggerdef casts a string literal in the WHEN expression the way
// pg_get_constraintdef casts one in a CHECK body, and writes = ANY (ARRAY[...])
// where the file says IN. Neither is a change.
func TestDiffTriggers_NoChangeAcrossWhenRenderings(t *testing.T) {
	cases := []struct {
		name string
		cur  string
		des  string
	}{
		{
			"text cast",
			"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW WHEN (new.name <> 'skip'::text) EXECUTE FUNCTION stamp()",
			"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW WHEN (new.name <> 'skip') EXECUTE FUNCTION stamp()",
		},
		{
			"any array",
			"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW WHEN (new.kind = ANY (ARRAY[1, 2])) EXECUTE FUNCTION stamp()",
			"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW WHEN (new.kind IN (1, 2)) EXECUTE FUNCTION stamp()",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := triggers(newTrigger("events_audit", tc.cur))
			des := triggers(newTrigger("events_audit", tc.des))
			stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
			require.NoError(t, err)
			assert.Empty(t, stmts)
		})
	}
}

// A WHEN expression that really differs is a definition change.
func TestDiffTriggers_WhenChanged(t *testing.T) {
	cur := triggers(newTrigger("events_audit",
		"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW WHEN (new.name <> 'skip'::text) EXECUTE FUNCTION stamp()"))
	des := triggers(newTrigger("events_audit",
		"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW WHEN (new.name <> 'keep') EXECUTE FUNCTION stamp()"))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{
		"CREATE OR REPLACE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW WHEN (new.name <> 'keep') EXECUTE FUNCTION stamp();",
	}, stmts)
}

// The catalog renders every trigger argument as a string, so an integer in the
// file is not a change.
func TestDiffTriggers_NoChangeAcrossArgumentRenderings(t *testing.T) {
	cur := triggers(newTrigger("events_audit",
		"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION stamp('a', '1')"))
	des := triggers(newTrigger("events_audit",
		"CREATE TRIGGER events_audit AFTER UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION stamp('a', 1)"))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffTriggers_Add(t *testing.T) {
	stmts, _, err := diffTriggers("public.events", triggers(), triggers(newTrigger("events_stamp", insertDef)), allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{insertDef + ";"}, stmts)
}

func TestDiffTriggers_AddDisabled(t *testing.T) {
	des := triggers(newTrigger("events_stamp", insertDef, withState('D')))
	stmts, _, err := diffTriggers("public.events", triggers(), des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{
		insertDef + ";",
		"ALTER TABLE public.events DISABLE TRIGGER events_stamp;",
	}, stmts)
}

func TestDiffTriggers_Drop(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef))
	stmts, disallowed, err := diffTriggers("public.events", cur, triggers(), allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP TRIGGER events_stamp ON public.events;"}, stmts)
	assert.Empty(t, disallowed)
}

func TestDiffTriggers_DropDisallowed(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef))
	stmts, disallowed, err := diffTriggers("public.events", cur, triggers(), nil)
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Equal(t, []string{"-- skipped: DROP TRIGGER events_stamp ON public.events;"}, disallowed)
}

func TestDiffTriggers_Replace(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef))
	des := triggers(newTrigger("events_stamp", updateDef))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{
		"CREATE OR REPLACE TRIGGER events_stamp BEFORE UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION stamp();",
	}, stmts)
}

// CREATE OR REPLACE TRIGGER re-enables the trigger, so a state other than the
// default has to be set again behind it.
func TestDiffTriggers_ReplaceKeepsState(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef, withState('D')))
	des := triggers(newTrigger("events_stamp", updateDef, withState('D')))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{
		"CREATE OR REPLACE TRIGGER events_stamp BEFORE UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION stamp();",
		"ALTER TABLE public.events DISABLE TRIGGER events_stamp;",
	}, stmts)
}

func TestDiffTriggers_StateOnly(t *testing.T) {
	cases := []struct {
		name  string
		cur   model.TriggerState
		des   model.TriggerState
		stmts []string
	}{
		{"enable", 'D', model.TriggerStateDefault, []string{"ALTER TABLE public.events ENABLE TRIGGER events_stamp;"}},
		{"disable", model.TriggerStateDefault, 'D', []string{"ALTER TABLE public.events DISABLE TRIGGER events_stamp;"}},
		{"always", model.TriggerStateDefault, 'A', []string{"ALTER TABLE public.events ENABLE ALWAYS TRIGGER events_stamp;"}},
		{"replica", model.TriggerStateDefault, 'R', []string{"ALTER TABLE public.events ENABLE REPLICA TRIGGER events_stamp;"}},
		{"unchanged", 'A', 'A', nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := triggers(newTrigger("events_stamp", insertDef, withState(tc.cur)))
			des := triggers(newTrigger("events_stamp", insertDef, withState(tc.des)))
			stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
			require.NoError(t, err)
			assert.Equal(t, tc.stmts, stmts)
		})
	}
}

// PostgreSQL rejects CREATE OR REPLACE TRIGGER across the constraint-trigger
// line, so the change runs as DROP and CREATE.
func TestDiffTriggers_ConstraintSwitch(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", conDef))
	des := triggers(newTrigger("events_stamp", insertDef))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{
		"DROP TRIGGER events_stamp ON public.events;",
		insertDef + ";",
	}, stmts)
}

// A denied recreate leaves the trigger exactly as it was: no DROP, no
// CREATE, nothing else touches it either.
func TestDiffTriggers_ConstraintSwitchDenied(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", conDef))
	des := triggers(newTrigger("events_stamp", insertDef))
	stmts, disallowed, err := diffTriggers("public.events", cur, des, nil)
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Equal(t, []string{"-- skipped: DROP TRIGGER events_stamp ON public.events;"}, disallowed)
}

// A rename bundled with a denied constraint switch keeps the old name too:
// the RENAME is suppressed the same way it is for a recreate that goes
// through, and nothing steps in to run it on its own.
func TestDiffTriggers_RenameWithRecreateDenied(t *testing.T) {
	cur := triggers(newTrigger("events_old", conDef))
	des := triggers(newTrigger("events_new",
		"CREATE TRIGGER events_new AFTER INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()",
		withTriggerRenameFrom("events_old")))
	stmts, disallowed, err := diffTriggers("public.events", cur, des, nil)
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Equal(t, []string{"-- skipped: DROP TRIGGER events_old ON public.events;"}, disallowed)
}

func TestDiffTriggers_Rename(t *testing.T) {
	cur := triggers(newTrigger("events_old",
		"CREATE TRIGGER events_old BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()"))
	des := triggers(newTrigger("events_new",
		"CREATE TRIGGER events_new BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()",
		withTriggerRenameFrom("events_old")))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TRIGGER events_old ON public.events RENAME TO events_new;"}, stmts)
}

// A rename that also crosses the constraint-trigger line drops the old name
// and creates the new one, so the RENAME would have nothing left to rename.
func TestDiffTriggers_RenameWithRecreate(t *testing.T) {
	cur := triggers(newTrigger("events_old",
		"CREATE CONSTRAINT TRIGGER events_old AFTER INSERT ON public.events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stamp()"))
	des := triggers(newTrigger("events_new",
		"CREATE TRIGGER events_new AFTER INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()",
		withTriggerRenameFrom("events_old")))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Equal(t, []string{
		"DROP TRIGGER events_old ON public.events;",
		"CREATE TRIGGER events_new AFTER INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp();",
	}, stmts)
}

// The directive stays in the file after the rename has been applied, so the
// source being gone while the destination exists is not an error.
func TestDiffTriggers_RenameAlreadyApplied(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef))
	des := triggers(newTrigger("events_stamp", insertDef, withTriggerRenameFrom("events_old")))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffTriggers_RenameSameName(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef))
	des := triggers(newTrigger("events_stamp", insertDef, withTriggerRenameFrom("events_stamp")))
	stmts, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffTriggers_RenameSourceMissing(t *testing.T) {
	des := triggers(newTrigger("events_new", insertDef, withTriggerRenameFrom("events_old")))
	_, _, err := diffTriggers("public.events", triggers(), des, allowTriggerDrops())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source trigger events_old not found")
}

func TestDiffTriggers_RenameDestinationExists(t *testing.T) {
	cur := triggers(
		newTrigger("events_old", insertDef),
		newTrigger("events_new", insertDef),
	)
	des := triggers(newTrigger("events_new", insertDef, withTriggerRenameFrom("events_old")))
	_, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

// The rename runs before any comparison, so a current definition that does not
// parse surfaces there rather than in equalTriggerDef.
func TestDiffTriggers_RenameUnparseableCurrent(t *testing.T) {
	cur := triggers(newTrigger("events_old", "NOT A TRIGGER"))
	des := triggers(newTrigger("events_new", insertDef, withTriggerRenameFrom("events_old")))
	_, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse the current trigger definition")
}

func TestDiffTriggers_NilMaps(t *testing.T) {
	stmts, disallowed, err := diffTriggers("public.events", nil, nil, allowTriggerDrops())
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Empty(t, disallowed)
}

func TestDiffTriggers_UnparseableDefinition(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", "NOT A TRIGGER"))
	des := triggers(newTrigger("events_stamp", insertDef))
	_, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse the current trigger definition")
}

func TestDiffTriggers_UnparseableDesiredDefinition(t *testing.T) {
	cur := triggers(newTrigger("events_stamp", insertDef))
	des := triggers(newTrigger("events_stamp", "NOT A TRIGGER"))
	_, _, err := diffTriggers("public.events", cur, des, allowTriggerDrops())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse the desired trigger definition")
}

// A definition that parses as something other than CREATE TRIGGER cannot be
// compared, and neither can a file holding more than one statement.
func TestParseTriggerDef_WrongStatement(t *testing.T) {
	_, _, err := parseTriggerDef("SELECT 1", "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected parse result")

	_, _, err = parseTriggerDef(insertDef+"; "+insertDef, "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected parse result")
}

func TestIsConstraintTrigger(t *testing.T) {
	assert.True(t, isConstraintTrigger(conDef))
	assert.False(t, isConstraintTrigger(insertDef))
	assert.False(t, isConstraintTrigger("NOT A TRIGGER"))
}

func TestReplaceTriggerSQL_Unparseable(t *testing.T) {
	_, err := replaceTriggerSQL("NOT A TRIGGER")
	require.Error(t, err)

	_, err = replaceTriggerSQL("SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected parse result")

	_, err = replaceTriggerSQL(insertDef + "; " + insertDef)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected parse result")
}

func TestRenameTriggerDef_Unparseable(t *testing.T) {
	_, err := renameTriggerDef("NOT A TRIGGER", "x")
	require.Error(t, err)

	_, err = renameTriggerDef("SELECT 1", "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected parse result")

	_, err = renameTriggerDef(insertDef+"; "+insertDef, "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected parse result")
}
