package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func TestView_FQVN(t *testing.T) {
	v := model.View{Schema: "public", Name: "active_users"}
	assert.Equal(t, "public.active_users", v.FQVN())
}

func TestView_SQL(t *testing.T) {
	v := model.View{Schema: "public", Name: "active_users", Definition: "SELECT id, name FROM users WHERE active = true"}
	expected := "CREATE OR REPLACE VIEW public.active_users AS\nSELECT id, name FROM users WHERE active = true;"
	assert.Equal(t, expected, v.SQL())
}

func TestView_SQL_trims(t *testing.T) {
	v := model.View{Schema: "public", Name: "v1", Definition: "  SELECT 1;  "}
	assert.Equal(t, "CREATE OR REPLACE VIEW public.v1 AS\nSELECT 1;", v.SQL())
}

func TestView_CommentSQL(t *testing.T) {
	comment := "Active users view"
	v := model.View{Schema: "public", Name: "active_users", Comment: &comment}
	assert.Equal(t, "COMMENT ON VIEW public.active_users IS 'Active users view';", v.CommentSQL())
}

func TestView_CommentSQL_nil(t *testing.T) {
	v := model.View{Schema: "public", Name: "active_users"}
	assert.Empty(t, v.CommentSQL())
}

func TestViewsToSQL(t *testing.T) {
	comment := "my view"
	views := orderedmap.New[string, *model.View]()
	views.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: &comment})
	views.Set("public.v2", &model.View{Schema: "public", Name: "v2", Definition: "SELECT 2"})

	got := model.ViewsToSQL(views)
	expected := `-- public.v1
CREATE OR REPLACE VIEW public.v1 AS
SELECT 1;
COMMENT ON VIEW public.v1 IS 'my view';

-- public.v2
CREATE OR REPLACE VIEW public.v2 AS
SELECT 2;`
	assert.Equal(t, expected, got)
}

func TestView_SQL_materialized(t *testing.T) {
	v := model.View{Schema: "public", Name: "mv", Materialized: true, Definition: "SELECT count(*) AS cnt FROM users"}
	expected := "CREATE MATERIALIZED VIEW public.mv AS\nSELECT count(*) AS cnt FROM users;"
	assert.Equal(t, expected, v.SQL())
}

func TestView_CommentSQL_materialized(t *testing.T) {
	comment := "stats"
	v := model.View{Schema: "public", Name: "mv", Materialized: true, Comment: &comment}
	assert.Equal(t, "COMMENT ON MATERIALIZED VIEW public.mv IS 'stats';", v.CommentSQL())
}

func TestViewToSQL_materializedWithIndex(t *testing.T) {
	indexes := orderedmap.New[string, *model.Index]()
	indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	v := &model.View{Schema: "public", Name: "mv", Materialized: true, Definition: "SELECT 1 AS n", Indexes: indexes}
	got := model.ViewToSQL(v)
	assert.Contains(t, got, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, got, "CREATE INDEX idx_mv_n")
}

func TestView_SQL_checkOption(t *testing.T) {
	v := model.View{Schema: "public", Name: "v1", Definition: "SELECT id FROM t WHERE ok", CheckOption: "cascaded"}
	assert.Equal(t, "CREATE OR REPLACE VIEW public.v1 AS\nSELECT id FROM t WHERE ok\n  WITH CASCADED CHECK OPTION;", v.SQL())
	v.CheckOption = "local"
	assert.Equal(t, "CREATE OR REPLACE VIEW public.v1 AS\nSELECT id FROM t WHERE ok\n  WITH LOCAL CHECK OPTION;", v.SQL())
}

func TestView_SQL_storageParams(t *testing.T) {
	v := model.View{
		Schema: "public", Name: "v1", Definition: "SELECT id FROM t",
		StorageParams: model.SortedStorageParams(map[string]string{"security_invoker": "true", "security_barrier": "true"}),
		CheckOption:   "local",
	}
	// The clause precedes AS, the check option follows the query, and the
	// parameters come out in name order.
	assert.Equal(t,
		"CREATE OR REPLACE VIEW public.v1 WITH (security_barrier='true', security_invoker='true') AS\nSELECT id FROM t\n  WITH LOCAL CHECK OPTION;",
		v.SQL())

	mv := model.View{
		Schema: "public", Name: "mv1", Definition: "SELECT id FROM t", Materialized: true,
		StorageParams: model.SortedStorageParams(map[string]string{"fillfactor": "70"}),
	}
	assert.Equal(t, "CREATE MATERIALIZED VIEW public.mv1 WITH (fillfactor='70') AS\nSELECT id FROM t;", mv.SQL())
}

func TestView_SQL_storageParamsEmpty(t *testing.T) {
	v := model.View{Schema: "public", Name: "v1", Definition: "SELECT id FROM t"}
	assert.Equal(t, "CREATE OR REPLACE VIEW public.v1 AS\nSELECT id FROM t;", v.SQL())
	v.StorageParams = model.SortedStorageParams(nil)
	assert.Equal(t, "CREATE OR REPLACE VIEW public.v1 AS\nSELECT id FROM t;", v.SQL())
}

func TestView_ObjType(t *testing.T) {
	assert.Equal(t, "VIEW", model.View{}.ObjType())
	assert.Equal(t, "MATERIALIZED VIEW", model.View{Materialized: true}.ObjType())
}

func TestViewStorageParamsSQL(t *testing.T) {
	assert.Equal(t,
		"ALTER VIEW public.v1 SET (security_barrier='true');",
		model.SetViewStorageParamsSQL("public.v1", "VIEW", []string{"security_barrier='true'"}))
	assert.Equal(t,
		"ALTER MATERIALIZED VIEW public.mv1 SET (autovacuum_enabled='off', fillfactor='90');",
		model.SetViewStorageParamsSQL("public.mv1", "MATERIALIZED VIEW", []string{"autovacuum_enabled='off'", "fillfactor='90'"}))
	assert.Equal(t,
		"ALTER VIEW public.v1 RESET (security_invoker);",
		model.ResetViewStorageParamsSQL("public.v1", "VIEW", []string{"security_invoker"}))
	assert.Equal(t,
		"ALTER MATERIALIZED VIEW public.mv1 RESET (fillfactor);",
		model.ResetViewStorageParamsSQL("public.mv1", "MATERIALIZED VIEW", []string{"fillfactor"}))
}

func TestSetCheckOptionSQL(t *testing.T) {
	assert.Equal(t, "ALTER VIEW public.v1 SET (check_option='local');", model.SetCheckOptionSQL("public.v1", "local"))
	assert.Equal(t, "ALTER VIEW public.v1 RESET (check_option);", model.SetCheckOptionSQL("public.v1", ""))
}
