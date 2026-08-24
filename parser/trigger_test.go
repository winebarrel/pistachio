package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/model"
)

func TestParseSQL_Trigger(t *testing.T) {
	sql := `CREATE TABLE public.t (id int, name text);
CREATE TRIGGER t_audit AFTER UPDATE OF name ON public.t FOR EACH ROW WHEN (OLD.name IS DISTINCT FROM NEW.name) EXECUTE FUNCTION public.stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	trg, ok := result.Tables.Get("public.t").Triggers.GetOk("t_audit")
	require.True(t, ok)
	assert.Equal(t, "public", trg.Schema)
	assert.Equal(t, "t", trg.Table)
	assert.Equal(t,
		"CREATE TRIGGER t_audit AFTER UPDATE OF name ON public.t FOR EACH ROW WHEN (old.name IS DISTINCT FROM new.name) EXECUTE FUNCTION public.stamp()",
		trg.Definition)
	assert.True(t, trg.State.IsDefault())
}

func TestParseSQL_Trigger_DefaultSchemaFallback(t *testing.T) {
	// The relation is written bare, so the schema falls back to the default
	// one and the stored definition comes out qualified.
	sql := `CREATE TABLE public.t (id int);
CREATE TRIGGER t_ins BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	trg := result.Tables.Get("public.t").Triggers.Get("t_ins")
	assert.Equal(t, "public", trg.Schema)
	assert.Equal(t,
		"CREATE TRIGGER t_ins BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp()",
		trg.Definition)
}

func TestParseSQL_Trigger_ConstraintTrigger(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
CREATE CONSTRAINT TRIGGER t_check AFTER INSERT ON public.t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	trg := result.Tables.Get("public.t").Triggers.Get("t_check")
	assert.Equal(t,
		"CREATE CONSTRAINT TRIGGER t_check AFTER INSERT ON public.t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stamp()",
		trg.Definition)
}

func TestParseSQL_Trigger_OrReplaceNormalized(t *testing.T) {
	// OR REPLACE says how to get to the state, not what the state is, so the
	// stored definition drops it and the diff decides.
	sql := `CREATE TABLE public.t (id int);
CREATE OR REPLACE TRIGGER t_ins BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	trg := result.Tables.Get("public.t").Triggers.Get("t_ins")
	assert.Equal(t,
		"CREATE TRIGGER t_ins BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp()",
		trg.Definition)
}

// EXECUTE PROCEDURE is the spelling PostgreSQL kept for compatibility, and
// several real schemas still use it. The catalog always writes EXECUTE
// FUNCTION, and so does the deparser, so the two meet.
func TestParseSQL_Trigger_ExecuteProcedure(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
CREATE TRIGGER t_ins BEFORE INSERT ON public.t FOR EACH ROW EXECUTE PROCEDURE stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	trg := result.Tables.Get("public.t").Triggers.Get("t_ins")
	assert.Equal(t,
		"CREATE TRIGGER t_ins BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp()",
		trg.Definition)
}

func TestParseSQL_Trigger_OnView(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
CREATE VIEW public.v AS SELECT id FROM public.t;
CREATE TRIGGER v_ins INSTEAD OF INSERT ON public.v FOR EACH ROW EXECUTE FUNCTION stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	trg, ok := result.Views.Get("public.v").Triggers.GetOk("v_ins")
	require.True(t, ok)
	assert.Equal(t, "v", trg.Table)
}

func TestParseSQL_Trigger_UnknownRelation(t *testing.T) {
	sql := `CREATE TRIGGER t_ins BEFORE INSERT ON public.missing FOR EACH ROW EXECUTE FUNCTION stamp();`
	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "public.missing")
}

func TestParseSQL_Trigger_Duplicate(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
CREATE TRIGGER t_ins BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();
CREATE TRIGGER t_ins BEFORE DELETE ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();`
	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate trigger")
}

// The same trigger name on two tables is legal: a trigger name is unique
// within its relation, not within the schema.
func TestParseSQL_Trigger_SameNameOnTwoTables(t *testing.T) {
	sql := `CREATE TABLE public.a (id int);
CREATE TABLE public.b (id int);
CREATE TRIGGER stamped BEFORE INSERT ON public.a FOR EACH ROW EXECUTE FUNCTION stamp();
CREATE TRIGGER stamped BEFORE INSERT ON public.b FOR EACH ROW EXECUTE FUNCTION stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	assert.Equal(t, "a", result.Tables.Get("public.a").Triggers.Get("stamped").Table)
	assert.Equal(t, "b", result.Tables.Get("public.b").Triggers.Get("stamped").Table)
}

func TestParseSQL_Trigger_EnableState(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
CREATE TRIGGER t_off BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();
CREATE TRIGGER t_always BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();
CREATE TRIGGER t_replica BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();
CREATE TRIGGER t_on BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();
ALTER TABLE public.t DISABLE TRIGGER t_off;
ALTER TABLE public.t ENABLE ALWAYS TRIGGER t_always;
ALTER TABLE public.t ENABLE REPLICA TRIGGER t_replica;
ALTER TABLE public.t ENABLE TRIGGER t_on;`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	triggers := result.Tables.Get("public.t").Triggers
	assert.Equal(t, model.TriggerState('D'), triggers.Get("t_off").State)
	assert.Equal(t, model.TriggerState('A'), triggers.Get("t_always").State)
	assert.Equal(t, model.TriggerState('R'), triggers.Get("t_replica").State)
	assert.Equal(t, model.TriggerStateDefault, triggers.Get("t_on").State)
}

// An enable state named for a trigger the file does not declare is ignored,
// the way an index on an undeclared table is.
func TestParseSQL_Trigger_EnableStateUnknownTrigger(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
ALTER TABLE public.t DISABLE TRIGGER nowhere;`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	assert.Zero(t, result.Tables.Get("public.t").Triggers.Len())
}

func TestParseSQL_Trigger_RenamedFrom(t *testing.T) {
	sql := `CREATE TABLE public.t (id int);
-- pista:renamed-from t_old
CREATE TRIGGER t_new BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION stamp();`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	trg := result.Tables.Get("public.t").Triggers.Get("t_new")
	require.NotNil(t, trg.RenameFrom)
	assert.Equal(t, "t_old", *trg.RenameFrom)
}
