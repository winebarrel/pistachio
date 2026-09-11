package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func marshalJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestColumnIdentity_MarshalJSON(t *testing.T) {
	assert.Equal(t, `"always"`, marshalJSON(t, model.ColumnIdentity('a')))
	assert.Equal(t, `"by_default"`, marshalJSON(t, model.ColumnIdentity('d')))
	assert.Equal(t, `""`, marshalJSON(t, model.ColumnIdentity(0)))
}

func TestColumnGenerated_MarshalJSON(t *testing.T) {
	assert.Equal(t, `"stored"`, marshalJSON(t, model.ColumnGenerated('s')))
	assert.Equal(t, `"virtual"`, marshalJSON(t, model.ColumnGenerated('v')))
	assert.Equal(t, `""`, marshalJSON(t, model.ColumnGenerated(0)))
}

func TestConstraintType_MarshalJSON(t *testing.T) {
	assert.Equal(t, `"check"`, marshalJSON(t, model.ConstraintType('c')))
	assert.Equal(t, `"foreign_key"`, marshalJSON(t, model.ConstraintType('f')))
	assert.Equal(t, `"not_null"`, marshalJSON(t, model.ConstraintType('n')))
	assert.Equal(t, `"primary_key"`, marshalJSON(t, model.ConstraintType('p')))
	assert.Equal(t, `"unique"`, marshalJSON(t, model.ConstraintType('u')))
	assert.Equal(t, `"exclusion"`, marshalJSON(t, model.ConstraintType('x')))
	assert.Equal(t, `""`, marshalJSON(t, model.ConstraintType(0)))
}

func TestPolicyCommand_MarshalJSON(t *testing.T) {
	assert.Equal(t, `"ALL"`, marshalJSON(t, model.PolicyCommand('*')))
	assert.Equal(t, `"SELECT"`, marshalJSON(t, model.PolicyCommand('r')))
	assert.Equal(t, `"INSERT"`, marshalJSON(t, model.PolicyCommand('a')))
	assert.Equal(t, `"UPDATE"`, marshalJSON(t, model.PolicyCommand('w')))
	assert.Equal(t, `"DELETE"`, marshalJSON(t, model.PolicyCommand('d')))
	assert.Equal(t, `""`, marshalJSON(t, model.PolicyCommand(0)))
}

func TestTriggerState_MarshalJSON(t *testing.T) {
	assert.Equal(t, `"enabled"`, marshalJSON(t, model.TriggerState(0)))
	assert.Equal(t, `"enabled"`, marshalJSON(t, model.TriggerState('O')))
	assert.Equal(t, `"disabled"`, marshalJSON(t, model.TriggerState('D')))
	assert.Equal(t, `"replica"`, marshalJSON(t, model.TriggerState('R')))
	assert.Equal(t, `"always"`, marshalJSON(t, model.TriggerState('A')))
}

// The golden strings below pin the JSON field names, which are the external
// contract of `pista parse`. A change here is a breaking change for its
// consumers.

func TestColumn_MarshalJSON(t *testing.T) {
	def := "0"
	col := &model.Column{Name: "id", TypeName: "bigint", NotNull: true, Default: &def, Identity: 'a'}
	assert.JSONEq(t,
		`{"name":"id","type":"bigint","not_null":true,"default":"0","identity":"always"}`,
		marshalJSON(t, col))
}

func TestColumn_MarshalJSON_OmitsZeroValues(t *testing.T) {
	col := &model.Column{Name: "note", TypeName: "text"}
	assert.JSONEq(t, `{"name":"note","type":"text"}`, marshalJSON(t, col))
}

func TestConstraint_MarshalJSON(t *testing.T) {
	con := &model.Constraint{
		Name:       "t_pkey",
		Type:       'p',
		Definition: "PRIMARY KEY (id)",
		Columns:    []string{"id"},
		Validated:  true,
	}
	assert.JSONEq(t,
		`{"name":"t_pkey","type":"primary_key","definition":"PRIMARY KEY (id)","columns":["id"],"validated":true}`,
		marshalJSON(t, con))
}

func TestForeignKey_MarshalJSON(t *testing.T) {
	ref := "public"
	refTable := "users"
	fk := &model.ForeignKey{
		Name:       "posts_user_id_fkey",
		Type:       'f',
		Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)",
		Validated:  true,
		Schema:     "public",
		Table:      "posts",
		RefSchema:  &ref,
		RefTable:   &refTable,
	}
	assert.JSONEq(t,
		`{"name":"posts_user_id_fkey","type":"foreign_key",`+
			`"definition":"FOREIGN KEY (user_id) REFERENCES public.users(id)","validated":true,`+
			`"schema":"public","table":"posts","ref_schema":"public","ref_table":"users"}`,
		marshalJSON(t, fk))
}

func TestTable_MarshalJSON_OmitsOID(t *testing.T) {
	tbl := &model.Table{OID: 42, Schema: "public", Name: "items"}
	s := marshalJSON(t, tbl)
	assert.NotContains(t, s, "42")
	assert.NotContains(t, s, "OID")
	assert.Contains(t, s, `"schema":"public"`)
	assert.Contains(t, s, `"name":"items"`)
}

func TestPolicy_MarshalJSON(t *testing.T) {
	using := "user_id = current_user_id()"
	p := &model.Policy{
		Name:       "p_items",
		Schema:     "public",
		Table:      "items",
		Permissive: true,
		Command:    'r',
		Roles:      []string{"app"},
		Using:      &using,
	}
	assert.JSONEq(t,
		`{"name":"p_items","schema":"public","table":"items","permissive":true,`+
			`"command":"SELECT","roles":["app"],"using":"user_id = current_user_id()"}`,
		marshalJSON(t, p))
}

func TestTrigger_MarshalJSON(t *testing.T) {
	trg := &model.Trigger{
		Schema:     "public",
		Table:      "items",
		Name:       "trg",
		Definition: "CREATE TRIGGER trg BEFORE INSERT ON public.items FOR EACH ROW EXECUTE FUNCTION f()",
	}
	assert.JSONEq(t,
		`{"schema":"public","table":"items","name":"trg",`+
			`"definition":"CREATE TRIGGER trg BEFORE INSERT ON public.items FOR EACH ROW EXECUTE FUNCTION f()",`+
			`"state":"enabled"}`,
		marshalJSON(t, trg))
}

func TestSequence_MarshalJSON(t *testing.T) {
	seq := &model.Sequence{
		Schema:    "public",
		Name:      "seq",
		DataType:  "bigint",
		Start:     1,
		Min:       1,
		Max:       9223372036854775807,
		Increment: 1,
		Cache:     1,
	}
	assert.JSONEq(t,
		`{"schema":"public","name":"seq","data_type":"bigint","start":1,"min":1,`+
			`"max":9223372036854775807,"increment":1,"cache":1}`,
		marshalJSON(t, seq))
}

func TestEnum_MarshalJSON(t *testing.T) {
	e := &model.Enum{Schema: "public", Name: "status", Values: []string{"active", "archived"}}
	assert.JSONEq(t,
		`{"schema":"public","name":"status","values":["active","archived"]}`,
		marshalJSON(t, e))
}

func TestIndex_MarshalJSON(t *testing.T) {
	idx := &model.Index{
		Schema:       "public",
		Name:         "idx_items_name",
		Table:        "items",
		Definition:   "CREATE INDEX idx_items_name ON public.items USING btree (name)",
		Concurrently: true,
	}
	assert.JSONEq(t,
		`{"schema":"public","name":"idx_items_name","table":"items",`+
			`"definition":"CREATE INDEX idx_items_name ON public.items USING btree (name)","concurrently":true}`,
		marshalJSON(t, idx))
}

func TestView_MarshalJSON(t *testing.T) {
	v := &model.View{
		Schema:       "public",
		Name:         "recent",
		Definition:   "SELECT * FROM items",
		Materialized: true,
		Indexes:      orderedmap.New[string, *model.Index](),
		Triggers:     orderedmap.New[string, *model.Trigger](),
	}
	assert.JSONEq(t,
		`{"schema":"public","name":"recent","definition":"SELECT * FROM items",`+
			`"materialized":true,"indexes":{},"triggers":{}}`,
		marshalJSON(t, v))
}

func TestCompositeType_MarshalJSON(t *testing.T) {
	old := "streets"
	ct := &model.CompositeType{
		Schema: "public",
		Name:   "address",
		Attributes: []*model.CompositeAttribute{
			{Name: "street", TypeName: "text", RenameFrom: &old},
		},
	}
	assert.JSONEq(t,
		`{"schema":"public","name":"address",`+
			`"attributes":[{"name":"street","type":"text","rename_from":"streets"}]}`,
		marshalJSON(t, ct))
}

func TestRoutine_MarshalJSON(t *testing.T) {
	r := &model.Routine{
		Schema:     "public",
		Name:       "add",
		Args:       []*model.RoutineArg{{Name: "a", Type: "integer"}, {Name: "b", Type: "integer"}},
		ReturnType: "integer",
		Language:   "sql",
		Body:       "SELECT a + b",
		Volatility: "IMMUTABLE",
		Parallel:   "SAFE",
		Config:     []*model.RoutineConfig{{Name: "search_path", Args: []string{"public"}}},
	}
	assert.JSONEq(t,
		`{"schema":"public","name":"add",`+
			`"args":[{"name":"a","type":"integer"},{"name":"b","type":"integer"}],`+
			`"return_type":"integer","language":"sql","body":"SELECT a + b",`+
			`"volatility":"IMMUTABLE","parallel":"SAFE",`+
			`"config":[{"name":"search_path","args":["public"]}]}`,
		marshalJSON(t, r))
}

func TestDomain_MarshalJSON(t *testing.T) {
	d := &model.Domain{
		Schema:   "public",
		Name:     "email",
		BaseType: "text",
		NotNull:  true,
		Constraints: []*model.DomainConstraint{
			{Name: "email_check", Definition: "CHECK (VALUE ~ '@')", Validated: true},
		},
	}
	assert.JSONEq(t,
		`{"schema":"public","name":"email","base_type":"text","not_null":true,`+
			`"constraints":[{"name":"email_check","definition":"CHECK (VALUE ~ '@')","validated":true}]}`,
		marshalJSON(t, d))
}
