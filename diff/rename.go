package diff

import (
	"fmt"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// detectEnumRenames finds desired enums with RenameFrom that match a current enum.
// Returns RENAME statements and a new "current" map where the renamed entry's key
// has been updated to the new FQEN.
func detectEnumRenames(current, desired *orderedmap.Map[string, *model.Enum]) ([]string, *orderedmap.Map[string, *model.Enum], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredEnum := range desired.All() {
		if desiredEnum.RenameFrom == nil {
			continue
		}
		oldKey := *desiredEnum.RenameFrom

		oldEnum, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldEnum.Schema != desiredEnum.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER TYPE "+oldKey+" RENAME TO "+model.Ident(desiredEnum.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldEnum
		renamed.Name = desiredEnum.Name
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// detectTableRenames finds desired tables with RenameFrom that match a current table.
func detectTableRenames(current, desired *orderedmap.Map[string, *model.Table]) ([]string, *orderedmap.Map[string, *model.Table], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredTable := range desired.All() {
		if desiredTable.RenameFrom == nil {
			continue
		}
		oldKey := *desiredTable.RenameFrom

		oldTable, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldTable.Schema != desiredTable.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		oldFQTN := oldTable.FQTN()
		newFQTN := model.Ident(oldTable.Schema, desiredTable.Name)

		stmts = append(stmts, "ALTER TABLE "+oldKey+" RENAME TO "+model.Ident(desiredTable.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldTable
		renamed.Name = desiredTable.Name

		// Update index definitions to reflect the new table name
		if renamed.Indexes.Len() > 0 {
			newIndexes := orderedmap.New[string, *model.Index]()
			for idxName, idx := range renamed.Indexes.All() {
				idxCopy := *idx
				idxCopy.Table = desiredTable.Name
				idxCopy.Definition = strings.ReplaceAll(idx.Definition, " ON "+oldFQTN+" ", " ON "+newFQTN+" ")
				newIndexes.Set(idxName, &idxCopy)
			}
			renamed.Indexes = newIndexes
		}

		// Update FK definitions to reflect the new table name
		if renamed.ForeignKeys.Len() > 0 {
			newFKs := orderedmap.New[string, *model.ForeignKey]()
			for fkName, fk := range renamed.ForeignKeys.All() {
				fkCopy := *fk
				fkCopy.Table = desiredTable.Name
				newFKs.Set(fkName, &fkCopy)
			}
			renamed.ForeignKeys = newFKs
		}

		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// detectViewRenames finds desired views with RenameFrom that match a current view.
func detectViewRenames(current, desired *orderedmap.Map[string, *model.View]) ([]string, *orderedmap.Map[string, *model.View], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredView := range desired.All() {
		if desiredView.RenameFrom == nil {
			continue
		}
		oldKey := *desiredView.RenameFrom

		oldView, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldView.Schema != desiredView.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER VIEW "+oldKey+" RENAME TO "+model.Ident(desiredView.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldView
		renamed.Name = desiredView.Name
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// detectColumnRenames finds desired columns with RenameFrom that match a current column.
func detectColumnRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Column]) ([]string, *orderedmap.Map[string, *model.Column], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredCol := range desired.All() {
		if desiredCol.RenameFrom == nil {
			continue
		}
		oldName := *desiredCol.RenameFrom

		oldCol, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source column %s not found in %s", model.Ident(oldName), fqtn)
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME COLUMN "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldCol
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// detectConstraintRenames finds desired constraints with RenameFrom that match a current constraint.
func detectConstraintRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Constraint]) ([]string, *orderedmap.Map[string, *model.Constraint], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredCon := range desired.All() {
		if desiredCon.RenameFrom == nil {
			continue
		}
		oldName := *desiredCon.RenameFrom

		oldCon, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source constraint %s not found in %s", model.Ident(oldName), fqtn)
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME CONSTRAINT "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldCon
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// detectIndexRenames finds desired indexes with RenameFrom that match a current index.
func detectIndexRenames(current, desired *orderedmap.Map[string, *model.Index]) ([]string, *orderedmap.Map[string, *model.Index], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredIdx := range desired.All() {
		if desiredIdx.RenameFrom == nil {
			continue
		}
		oldName := *desiredIdx.RenameFrom

		oldIdx, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source index %s not found", model.Ident(oldName))
		}

		stmts = append(stmts, "ALTER INDEX "+model.Ident(oldIdx.Schema, oldName)+" RENAME TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldIdx
		renamed.Name = newName
		// Update definition to reflect the new index name
		renamed.Definition = strings.Replace(renamed.Definition, model.Ident(oldName), model.Ident(newName), 1)
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// detectForeignKeyRenames finds desired foreign keys with RenameFrom that match a current FK.
func detectForeignKeyRenames(fqtn string, current, desired *orderedmap.Map[string, *model.ForeignKey]) ([]string, *orderedmap.Map[string, *model.ForeignKey], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredFK := range desired.All() {
		if desiredFK.RenameFrom == nil {
			continue
		}
		oldName := *desiredFK.RenameFrom

		oldFK, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source foreign key %s not found in %s", model.Ident(oldName), fqtn)
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME CONSTRAINT "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldFK
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// cloneMap creates a shallow copy of an orderedmap.
func cloneMap[K comparable, V any](m *orderedmap.Map[K, V]) *orderedmap.Map[K, V] {
	clone := orderedmap.New[K, V]()
	for k, v := range m.All() {
		clone.Set(k, v)
	}
	return clone
}
