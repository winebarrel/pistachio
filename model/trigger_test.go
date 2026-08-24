package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func TestTriggerState(t *testing.T) {
	assert.True(t, model.TriggerStateDefault.IsDefault())
	// A trigger built without a state reads as enabled.
	assert.True(t, model.TriggerState(0).IsDefault())
	assert.False(t, model.TriggerState('D').IsDefault())

	assert.Equal(t, "ENABLE", model.TriggerStateDefault.Action())
	assert.Equal(t, "ENABLE", model.TriggerState(0).Action())
	assert.Equal(t, "DISABLE", model.TriggerState('D').Action())
	assert.Equal(t, "ENABLE REPLICA", model.TriggerState('R').Action())
	assert.Equal(t, "ENABLE ALWAYS", model.TriggerState('A').Action())
}

func TestTrigger_SQL(t *testing.T) {
	trg := model.Trigger{
		Schema:     "public",
		Table:      "events",
		Name:       "events_stamp",
		Definition: "CREATE TRIGGER events_stamp BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION stamp()",
	}
	assert.Equal(t, "public.events", trg.FQTN())
	assert.Equal(t,
		"CREATE TRIGGER events_stamp BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION stamp();",
		trg.SQL())
}

func TestTrigger_StateSQL(t *testing.T) {
	trg := model.Trigger{Schema: "public", Table: "events", Name: "events_stamp"}

	// The default state is what CREATE TRIGGER leaves behind, so it needs no
	// statement of its own.
	assert.Empty(t, trg.StateSQL())

	trg.State = model.TriggerStateDefault
	assert.Empty(t, trg.StateSQL())

	trg.State = 'D'
	assert.Equal(t, "ALTER TABLE public.events DISABLE TRIGGER events_stamp;", trg.StateSQL())

	trg.State = 'A'
	assert.Equal(t, "ALTER TABLE public.events ENABLE ALWAYS TRIGGER events_stamp;", trg.StateSQL())
}

func TestTrigger_String(t *testing.T) {
	trg := &model.Trigger{Name: "events_stamp"}
	assert.Contains(t, trg.String(), "events_stamp")
}

// orderedmapOf files triggers under their names, the way the parser and the
// catalog do.
func orderedmapOf(list ...*model.Trigger) *orderedmap.Map[string, *model.Trigger] {
	m := orderedmap.New[string, *model.Trigger]()
	for _, trg := range list {
		m.Set(trg.Name, trg)
	}
	return m
}

func TestTable_TrigSQL(t *testing.T) {
	tbl := &model.Table{
		Schema:      "public",
		Name:        "events",
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		Indexes:     orderedmap.New[string, *model.Index](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Triggers: orderedmapOf(
			&model.Trigger{Schema: "public", Table: "events", Name: "a", Definition: "CREATE TRIGGER a BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()"},
			&model.Trigger{Schema: "public", Table: "events", Name: "b", Definition: "CREATE TRIGGER b BEFORE UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION stamp()", State: 'D'},
		),
	}
	assert.Equal(t,
		"CREATE TRIGGER a BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION stamp();\n"+
			"CREATE TRIGGER b BEFORE UPDATE ON public.events FOR EACH ROW EXECUTE FUNCTION stamp();\n"+
			"ALTER TABLE public.events DISABLE TRIGGER b;",
		tbl.TrigSQL())
	assert.Contains(t, model.TableToSQL(tbl), "CREATE TRIGGER a BEFORE INSERT")
}

func TestTable_TrigSQL_None(t *testing.T) {
	tbl := &model.Table{Schema: "public", Name: "events"}
	assert.Empty(t, tbl.TrigSQL())
}

func TestView_TrigSQL(t *testing.T) {
	v := &model.View{
		Schema:     "public",
		Name:       "recent_events",
		Definition: "SELECT id FROM events",
		Triggers:   orderedmapOf(&model.Trigger{Schema: "public", Table: "recent_events", Name: "v_ins", Definition: "CREATE TRIGGER v_ins INSTEAD OF INSERT ON recent_events FOR EACH ROW EXECUTE FUNCTION stamp()"}),
	}
	assert.Equal(t,
		"CREATE TRIGGER v_ins INSTEAD OF INSERT ON recent_events FOR EACH ROW EXECUTE FUNCTION stamp();",
		v.TrigSQL())
	assert.Contains(t, model.ViewToSQL(v), "CREATE TRIGGER v_ins INSTEAD OF INSERT")
}

func TestView_TrigSQL_None(t *testing.T) {
	v := &model.View{Schema: "public", Name: "recent_events", Definition: "SELECT 1"}
	assert.Empty(t, v.TrigSQL())
}
