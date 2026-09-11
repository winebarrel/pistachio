package model_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// marshalJSON encodes with json/v2, which is what the parse command writes
// with. v1 escapes <, > and & in a string, so a golden taken through it would
// not be the bytes the command produces.
func marshalJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

func TestColumnIdentity_MarshalJSON(t *testing.T) {
	assert.JSONEq(t, `"always"`, marshalJSON(t, model.ColumnIdentity('a')))
	assert.JSONEq(t, `"by_default"`, marshalJSON(t, model.ColumnIdentity('d')))
	assert.JSONEq(t, `""`, marshalJSON(t, model.ColumnIdentity(0)))
}

func TestColumnGenerated_MarshalJSON(t *testing.T) {
	assert.JSONEq(t, `"stored"`, marshalJSON(t, model.ColumnGenerated('s')))
	assert.JSONEq(t, `"virtual"`, marshalJSON(t, model.ColumnGenerated('v')))
	assert.JSONEq(t, `""`, marshalJSON(t, model.ColumnGenerated(0)))
}

func TestConstraintType_MarshalJSON(t *testing.T) {
	assert.JSONEq(t, `"check"`, marshalJSON(t, model.ConstraintType('c')))
	assert.JSONEq(t, `"foreign_key"`, marshalJSON(t, model.ConstraintType('f')))
	assert.JSONEq(t, `"not_null"`, marshalJSON(t, model.ConstraintType('n')))
	assert.JSONEq(t, `"primary_key"`, marshalJSON(t, model.ConstraintType('p')))
	assert.JSONEq(t, `"unique"`, marshalJSON(t, model.ConstraintType('u')))
	assert.JSONEq(t, `"exclusion"`, marshalJSON(t, model.ConstraintType('x')))
	assert.JSONEq(t, `""`, marshalJSON(t, model.ConstraintType(0)))
}

func TestPolicyCommand_MarshalJSON(t *testing.T) {
	assert.JSONEq(t, `"ALL"`, marshalJSON(t, model.PolicyCommand('*')))
	assert.JSONEq(t, `"SELECT"`, marshalJSON(t, model.PolicyCommand('r')))
	assert.JSONEq(t, `"INSERT"`, marshalJSON(t, model.PolicyCommand('a')))
	assert.JSONEq(t, `"UPDATE"`, marshalJSON(t, model.PolicyCommand('w')))
	assert.JSONEq(t, `"DELETE"`, marshalJSON(t, model.PolicyCommand('d')))
	assert.JSONEq(t, `""`, marshalJSON(t, model.PolicyCommand(0)))
}

func TestTriggerState_MarshalJSON(t *testing.T) {
	assert.JSONEq(t, `"enabled"`, marshalJSON(t, model.TriggerState(0)))
	assert.JSONEq(t, `"enabled"`, marshalJSON(t, model.TriggerState('O')))
	assert.JSONEq(t, `"disabled"`, marshalJSON(t, model.TriggerState('D')))
	assert.JSONEq(t, `"replica"`, marshalJSON(t, model.TriggerState('R')))
	assert.JSONEq(t, `"always"`, marshalJSON(t, model.TriggerState('A')))
}

// The goldens below pin the field names and the shape of the output, which is
// what a consumer of `pista parse` reads. Every field is written, whatever it
// holds, so a key never appears or disappears with the input.

func TestColumn_MarshalJSON(t *testing.T) {
	def := "0"
	col := &model.Column{Name: "id", TypeName: "bigint", NotNull: true, Default: &def, Identity: 'a'}
	assert.JSONEq(t, `{
		"name": "id",
		"rename_from": null,
		"type": "bigint",
		"serial_sequence": null,
		"not_null": true,
		"not_null_name": null,
		"default": "0",
		"identity": "always",
		"identity_sequence": null,
		"generated": "",
		"collation": null,
		"storage_type": "",
		"type_storage": "",
		"compression": "",
		"comment": null
	}`, marshalJSON(t, col))
}

func TestColumn_MarshalJSON_ZeroValues(t *testing.T) {
	col := &model.Column{Name: "note", TypeName: "text"}
	assert.JSONEq(t, `{
		"name": "note",
		"rename_from": null,
		"type": "text",
		"serial_sequence": null,
		"not_null": false,
		"not_null_name": null,
		"default": null,
		"identity": "",
		"identity_sequence": null,
		"generated": "",
		"collation": null,
		"storage_type": "",
		"type_storage": "",
		"compression": "",
		"comment": null
	}`, marshalJSON(t, col))
}

// A generated column keeps its expression in Default, next to generated, which
// is where the model holds it.
func TestColumn_MarshalJSON_Generated(t *testing.T) {
	expr := "lower(email)"
	col := &model.Column{Name: "norm", TypeName: "text", Generated: 's', Default: &expr}
	s := marshalJSON(t, col)
	assert.Contains(t, s, `"generated":"stored"`)
	assert.Contains(t, s, `"default":"lower(email)"`)
}

func TestColumn_MarshalJSON_IdentitySequence(t *testing.T) {
	col := &model.Column{
		Name:        "id",
		TypeName:    "bigint",
		Identity:    'd',
		IdentitySeq: &model.IdentitySequence{Start: 1, Min: 1, Max: 100, Increment: 1, Cache: 1},
	}
	s := marshalJSON(t, col)
	assert.Contains(t, s, `"identity":"by_default"`)
	assert.Contains(t, s, `"identity_sequence":{"start":1,"min":1,"max":100,"increment":1,"cache":1,"cycle":false}`)
}

func TestConstraint_MarshalJSON(t *testing.T) {
	con := &model.Constraint{
		Name:       "t_pkey",
		Type:       'p',
		Definition: "PRIMARY KEY (id)",
		Columns:    []string{"id"},
		Validated:  true,
	}
	assert.JSONEq(t, `{
		"oid": 0,
		"name": "t_pkey",
		"rename_from": null,
		"type": "primary_key",
		"definition": "PRIMARY KEY (id)",
		"columns": ["id"],
		"deferrable": false,
		"deferred": false,
		"validated": true,
		"inherited": false,
		"index_name": ""
	}`, marshalJSON(t, con))
}

// A check constraint the schema wrote NOT VALID reports validated false, which
// is why the field is written rather than left out when it is false.
func TestConstraint_MarshalJSON_NotValid(t *testing.T) {
	con := &model.Constraint{Name: "t_check", Type: 'c', Definition: "CHECK (n > 0)"}
	assert.Contains(t, marshalJSON(t, con), `"validated":false`)
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
	assert.JSONEq(t, `{
		"oid": 0,
		"name": "posts_user_id_fkey",
		"rename_from": null,
		"type": "foreign_key",
		"definition": "FOREIGN KEY (user_id) REFERENCES public.users(id)",
		"columns": [],
		"deferrable": false,
		"deferred": false,
		"validated": true,
		"inherited": false,
		"index_name": "",
		"schema": "public",
		"table": "posts",
		"ref_schema": "public",
		"ref_table": "users"
	}`, marshalJSON(t, fk))
}

func TestTable_MarshalJSON(t *testing.T) {
	tbl := &model.Table{
		OID:           42,
		Schema:        "public",
		Name:          "items",
		StorageParams: orderedmap.New[string, string](),
		Columns:       orderedmap.New[string, *model.Column](),
		Constraints:   orderedmap.New[string, *model.Constraint](),
		ForeignKeys:   orderedmap.New[string, *model.ForeignKey](),
		Indexes:       orderedmap.New[string, *model.Index](),
		Policies:      orderedmap.New[string, *model.Policy](),
		Triggers:      orderedmap.New[string, *model.Trigger](),
	}
	assert.JSONEq(t, `{
		"oid": 42,
		"schema": "public",
		"name": "items",
		"rename_from": null,
		"bulk_alter": false,
		"ignore": false,
		"table_space": null,
		"storage_params": {},
		"unlogged": false,
		"partitioned": false,
		"partition_def": null,
		"partition_of": null,
		"partition_bound": null,
		"row_security": false,
		"force_row_security": false,
		"columns": {},
		"constraints": {},
		"foreign_keys": {},
		"indexes": {},
		"policies": {},
		"triggers": {},
		"comment": null
	}`, marshalJSON(t, tbl))
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
	assert.JSONEq(t, `{
		"name": "p_items",
		"rename_from": null,
		"schema": "public",
		"table": "items",
		"permissive": true,
		"command": "SELECT",
		"roles": ["app"],
		"using": "user_id = current_user_id()",
		"with_check": null
	}`, marshalJSON(t, p))
}

func TestTrigger_MarshalJSON(t *testing.T) {
	trg := &model.Trigger{
		Schema:     "public",
		Table:      "items",
		Name:       "trg",
		Definition: "CREATE TRIGGER trg BEFORE INSERT ON public.items FOR EACH ROW EXECUTE FUNCTION f()",
	}
	assert.JSONEq(t, `{
		"schema": "public",
		"table": "items",
		"name": "trg",
		"rename_from": null,
		"definition": "CREATE TRIGGER trg BEFORE INSERT ON public.items FOR EACH ROW EXECUTE FUNCTION f()",
		"state": "enabled"
	}`, marshalJSON(t, trg))
}

func TestIndex_MarshalJSON(t *testing.T) {
	idx := &model.Index{
		Schema:       "public",
		Name:         "idx_items_name",
		Table:        "items",
		Definition:   "CREATE INDEX idx_items_name ON public.items USING btree (name)",
		Concurrently: true,
	}
	assert.JSONEq(t, `{
		"oid": 0,
		"schema": "public",
		"name": "idx_items_name",
		"rename_from": null,
		"table": "items",
		"definition": "CREATE INDEX idx_items_name ON public.items USING btree (name)",
		"table_space": null,
		"concurrently": true,
		"comment": null
	}`, marshalJSON(t, idx))
}

func TestView_MarshalJSON(t *testing.T) {
	v := &model.View{
		Schema:        "public",
		Name:          "recent",
		Definition:    "SELECT * FROM items",
		Materialized:  true,
		StorageParams: orderedmap.New[string, string](),
		Indexes:       orderedmap.New[string, *model.Index](),
		Triggers:      orderedmap.New[string, *model.Trigger](),
	}
	assert.JSONEq(t, `{
		"oid": 0,
		"schema": "public",
		"name": "recent",
		"rename_from": null,
		"definition": "SELECT * FROM items",
		"materialized": true,
		"check_option": "",
		"storage_params": {},
		"indexes": {},
		"triggers": {},
		"comment": null,
		"ignore": false
	}`, marshalJSON(t, v))
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
	assert.JSONEq(t, `{
		"oid": 0,
		"schema": "public",
		"name": "seq",
		"data_type": "bigint",
		"start": 1,
		"min": 1,
		"max": 9223372036854775807,
		"increment": 1,
		"cache": 1,
		"cycle": false,
		"unlogged": false,
		"owner_table": null,
		"owner_column": null,
		"rename_from": null,
		"comment": null,
		"ignore": false
	}`, marshalJSON(t, seq))
}

func TestEnum_MarshalJSON(t *testing.T) {
	e := &model.Enum{Schema: "public", Name: "status", Values: []string{"active", "archived"}}
	assert.JSONEq(t, `{
		"oid": 0,
		"schema": "public",
		"name": "status",
		"rename_from": null,
		"values": ["active", "archived"],
		"value_rename_from": {},
		"comment": null,
		"ignore": false
	}`, marshalJSON(t, e))
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
	assert.JSONEq(t, `{
		"oid": 0,
		"schema": "public",
		"name": "email",
		"rename_from": null,
		"base_type": "text",
		"not_null": true,
		"default": null,
		"collation": null,
		"constraints": [
			{"name": "email_check", "definition": "CHECK (VALUE ~ '@')", "validated": true}
		],
		"comment": null,
		"ignore": false
	}`, marshalJSON(t, d))
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
	assert.JSONEq(t, `{
		"oid": 0,
		"schema": "public",
		"name": "address",
		"rename_from": null,
		"attributes": [
			{
				"name": "street",
				"type": "text",
				"collation": null,
				"rename_from": "streets",
				"comment": null
			}
		],
		"comment": null,
		"ignore": false
	}`, marshalJSON(t, ct))
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
	assert.JSONEq(t, `{
		"oid": 0,
		"schema": "public",
		"name": "add",
		"procedure": false,
		"args": [
			{"mode": "", "name": "a", "type": "integer", "default": ""},
			{"mode": "", "name": "b", "type": "integer", "default": ""}
		],
		"return_type": "integer",
		"returns_set": false,
		"language": "sql",
		"body": "SELECT a + b",
		"obj_file": "",
		"volatility": "IMMUTABLE",
		"strict": false,
		"security_definer": false,
		"leakproof": false,
		"parallel": "SAFE",
		"cost": null,
		"rows": null,
		"config": [{"name": "search_path", "args": ["public"]}],
		"comment": null,
		"ignore": false
	}`, marshalJSON(t, r))
}

// An expression with a > must not come back HTML escaped, the way v1 writes it.
func TestMarshalJSON_NoHTMLEscape(t *testing.T) {
	con := &model.Constraint{Name: "c", Type: 'c', Definition: "CHECK (amt > 0 AND a < b)"}
	assert.Contains(t, marshalJSON(t, con), `"CHECK (amt > 0 AND a < b)"`)
}
