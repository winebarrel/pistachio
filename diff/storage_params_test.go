package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/model"
)

func paramsTable(params map[string]string) *model.Table {
	t := &model.Table{Schema: "public", Name: "users"}
	if params != nil {
		t.StorageParams = model.SortedStorageParams(params)
	}
	return t
}

func TestDiffStorageParams_noChange(t *testing.T) {
	params := map[string]string{"fillfactor": "70"}
	assert.Empty(t, diffStorageParams("public.users", paramsTable(params), paramsTable(params)))
}

func TestDiffStorageParams_neitherSideCarriesTheMap(t *testing.T) {
	assert.Empty(t, diffStorageParams("public.users", paramsTable(nil), paramsTable(nil)))
}

func TestDiffStorageParams_add(t *testing.T) {
	assert.Equal(t,
		[]string{"ALTER TABLE public.users SET (autovacuum_enabled='off', fillfactor='70');"},
		diffStorageParams("public.users",
			paramsTable(nil),
			paramsTable(map[string]string{"fillfactor": "70", "autovacuum_enabled": "off"})))
}

func TestDiffStorageParams_changeValue(t *testing.T) {
	assert.Equal(t,
		[]string{"ALTER TABLE public.users SET (fillfactor='90');"},
		diffStorageParams("public.users",
			paramsTable(map[string]string{"fillfactor": "70"}),
			paramsTable(map[string]string{"fillfactor": "90"})))
}

func TestDiffStorageParams_reset(t *testing.T) {
	assert.Equal(t,
		[]string{"ALTER TABLE public.users RESET (autovacuum_enabled, fillfactor);"},
		diffStorageParams("public.users",
			paramsTable(map[string]string{"fillfactor": "70", "autovacuum_enabled": "off"}),
			paramsTable(nil)))
}

// One parameter arriving next to another leaving is the shape a rewritten
// WITH clause takes. SET comes first so a table never sits without either.
func TestDiffStorageParams_setAndReset(t *testing.T) {
	assert.Equal(t,
		[]string{
			"ALTER TABLE public.users SET (toast.autovacuum_enabled='off');",
			"ALTER TABLE public.users RESET (autovacuum_enabled);",
		},
		diffStorageParams("public.users",
			paramsTable(map[string]string{"autovacuum_enabled": "off"}),
			paramsTable(map[string]string{"toast.autovacuum_enabled": "off"})))
}
