package diff

import (
	"fmt"
	"slices"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

type DomainDiffResult struct {
	Stmts     []string
	DropStmts []string
}

func DiffDomains(current, desired *orderedmap.Map[string, *model.Domain], dc DropChecker) (*DomainDiffResult, error) {
	result := &DomainDiffResult{}

	// Detect renames
	renameStmts, current, err := detectDomainRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, renameStmts...)

	// New domains
	for k, desiredDomain := range desired.All() {
		if _, ok := current.GetOk(k); !ok {
			result.Stmts = append(result.Stmts, desiredDomain.SQL())
			if commentSQL := desiredDomain.CommentSQL(); commentSQL != "" {
				result.Stmts = append(result.Stmts, commentSQL)
			}
		}
	}

	// Modified domains
	for k, desiredDomain := range desired.All() {
		currentDomain, ok := current.GetOk(k)
		if !ok {
			continue
		}

		stmts, err := diffDomain(k, currentDomain, desiredDomain)
		if err != nil {
			return nil, err
		}
		result.Stmts = append(result.Stmts, stmts...)
	}

	// Dropped domains
	if dc.IsDropAllowed("domain") {
		for k := range current.Keys() {
			if _, ok := desired.GetOk(k); !ok {
				result.DropStmts = append(result.DropStmts, "DROP DOMAIN "+k+";")
			}
		}
	}

	return result, nil
}

func diffDomain(fqdn string, current, desired *model.Domain) ([]string, error) {
	var stmts []string

	// Base type change is not supported by PostgreSQL ALTER DOMAIN
	if current.BaseType != desired.BaseType {
		return nil, fmt.Errorf("cannot change base type of domain %s from %s to %s: PostgreSQL does not support this", fqdn, current.BaseType, desired.BaseType)
	}

	// Collation change is not supported by PostgreSQL ALTER DOMAIN
	if !equalPtr(current.Collation, desired.Collation) {
		return nil, fmt.Errorf("cannot change collation of domain %s: PostgreSQL does not support this", fqdn)
	}

	// Default change (use AST comparison to handle type alias differences)
	if !equalDefault(current.Default, desired.Default) {
		if desired.Default != nil {
			stmts = append(stmts, "ALTER DOMAIN "+fqdn+" SET DEFAULT "+*desired.Default+";")
		} else {
			stmts = append(stmts, "ALTER DOMAIN "+fqdn+" DROP DEFAULT;")
		}
	}

	// NOT NULL change
	if current.NotNull != desired.NotNull {
		if desired.NotNull {
			stmts = append(stmts, "ALTER DOMAIN "+fqdn+" SET NOT NULL;")
		} else {
			stmts = append(stmts, "ALTER DOMAIN "+fqdn+" DROP NOT NULL;")
		}
	}

	// Constraint changes
	stmts = append(stmts, diffDomainConstraints(fqdn, current.Constraints, desired.Constraints)...)

	// Comment change
	if !equalPtr(current.Comment, desired.Comment) {
		if desired.Comment != nil {
			stmts = append(stmts, "COMMENT ON DOMAIN "+fqdn+" IS "+model.QuoteLiteral(*desired.Comment)+";")
		} else {
			stmts = append(stmts, "COMMENT ON DOMAIN "+fqdn+" IS NULL;")
		}
	}

	return stmts, nil
}

func diffDomainConstraints(fqdn string, current, desired []*model.DomainConstraint) []string {
	var stmts []string

	currentByName := make(map[string]string)
	for _, c := range current {
		currentByName[c.Name] = c.Definition
	}

	desiredByName := make(map[string]string)
	for _, c := range desired {
		desiredByName[c.Name] = c.Definition
	}

	// Drop removed or changed constraints
	for _, c := range current {
		desiredDef, ok := desiredByName[c.Name]
		if !ok || !equalConstraintDef(c.Definition, desiredDef) {
			stmts = append(stmts, "ALTER DOMAIN "+fqdn+" DROP CONSTRAINT "+model.Ident(c.Name)+";")
		}
	}

	// Add new or changed constraints
	for _, c := range desired {
		currentDef, ok := currentByName[c.Name]
		if !ok || !equalConstraintDef(currentDef, c.Definition) {
			stmts = append(stmts, "ALTER DOMAIN "+fqdn+" ADD CONSTRAINT "+model.Ident(c.Name)+" "+c.Definition+";")
		}
	}

	return stmts
}

// detectDomainRenames finds desired domains with RenameFrom that match a current domain.
func detectDomainRenames(current, desired *orderedmap.Map[string, *model.Domain]) ([]string, *orderedmap.Map[string, *model.Domain], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredDomain := range desired.All() {
		if desiredDomain.RenameFrom == nil {
			continue
		}
		oldKey := *desiredDomain.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldDomain, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldDomain.Schema != desiredDomain.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER DOMAIN "+oldKey+" RENAME TO "+model.Ident(desiredDomain.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldDomain
		renamed.Name = desiredDomain.Name
		renamed.Constraints = slices.Clone(oldDomain.Constraints)
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}
