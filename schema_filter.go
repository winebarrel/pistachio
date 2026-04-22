package pistachio

import (
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

// filterDesiredBySchemas removes objects from the parse result whose schema
// is not in the target schemas list. This prevents objects from unrelated
// schemas in the SQL file from being treated as desired state.
func filterDesiredBySchemas(result *parser.ParseResult, schemas []string, schemaMap map[string]string) {
	schemaSet := make(map[string]bool, len(schemas))
	for _, s := range schemas {
		schemaSet[s] = true
	}
	// Also include schema-map destinations (desired SQL may use mapped names)
	for _, v := range schemaMap {
		schemaSet[v] = true
	}

	result.Tables = filterMapBySchema(result.Tables, schemaSet, func(t *model.Table) string { return t.Schema })
	result.Views = filterMapBySchema(result.Views, schemaSet, func(v *model.View) string { return v.Schema })
	result.Enums = filterMapBySchema(result.Enums, schemaSet, func(e *model.Enum) string { return e.Schema })
	result.Domains = filterMapBySchema(result.Domains, schemaSet, func(d *model.Domain) string { return d.Schema })
}

func filterMapBySchema[V any](m *orderedmap.Map[string, V], schemas map[string]bool, getSchema func(V) string) *orderedmap.Map[string, V] {
	filtered := orderedmap.New[string, V]()
	for k, v := range m.All() {
		if schemas[getSchema(v)] {
			filtered.Set(k, v)
		}
	}
	return filtered
}
