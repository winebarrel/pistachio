package diff

import (
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// diffStorageParams returns the statements that bring a table's storage
// parameters in line with the desired schema: one SET for the parameters that
// arrive or change value, one RESET for the ones the desired schema no longer
// names. Neither rewrites the table. Both sides are keyed in name order, so
// the statements come out the same on every run.
//
// The caller clears the parameters on both sides unless --manage-storage-param
// asked for them, so an unmanaged run reaches here with two empty maps and
// plans nothing.
//
// A partitioned table needs no special case, since PostgreSQL rejects every
// parameter on a relation that holds no storage. A partition holds its own,
// which it does not take from the parent, and is diffed like any other table.
func diffStorageParams(fqtn string, current, desired *model.Table) []string {
	currentParams := storageParams(current)
	desiredParams := storageParams(desired)

	var stmts []string

	var set []string
	for name, value := range desiredParams.All() {
		if cur, ok := currentParams.GetOk(name); !ok || cur != value {
			set = append(set, name+"="+model.QuoteLiteral(value))
		}
	}
	if len(set) > 0 {
		stmts = append(stmts, model.SetStorageParamsSQL(fqtn, set))
	}

	var reset []string
	for name := range currentParams.Keys() {
		if _, ok := desiredParams.GetOk(name); !ok {
			reset = append(reset, name)
		}
	}
	if len(reset) > 0 {
		stmts = append(stmts, model.ResetStorageParamsSQL(fqtn, reset))
	}

	return stmts
}

// storageParams reads a table's parameters, standing in an empty map for one
// built without any. The catalog and the parser always set the map, other
// callers may not.
func storageParams(t *model.Table) *orderedmap.Map[string, string] {
	if t.StorageParams == nil {
		return orderedmap.New[string, string]()
	}
	return t.StorageParams
}
