package diff

import (
	"fmt"
	"slices"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

type DomainDiffResult struct {
	Stmts               []string
	DropStmts           []string
	DisallowedDropStmts []string
}

func DiffDomains(current, desired *orderedmap.Map[string, *model.Domain], dc DropChecker) (*DomainDiffResult, error) {
	dc = normalizeDropChecker(dc)
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
			result.Stmts = append(result.Stmts, desiredDomain.NotValidConSQL()...)
			if commentSQL := desiredDomain.CommentSQL(); commentSQL != "" {
				result.Stmts = append(result.Stmts, commentSQL)
			}
			result.Stmts = append(result.Stmts, desiredDomain.ConstraintCommentSQL()...)
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

	// Dropped domains. When the domain-drop policy disallows it, emit a commented DROP.
	drops, skipped := dropMissing(current, desired, "DOMAIN", dc.IsDropAllowed("domain"))
	result.DropStmts = append(result.DropStmts, drops...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, skipped...)

	return result, nil
}

func diffDomain(fqdn string, current, desired *model.Domain) ([]string, error) {
	var stmts []string

	// Base type change is not supported by PostgreSQL ALTER DOMAIN. The
	// comparison folds the modifier, so a base type the catalog reports in
	// mixed case (geometry(Polygon,4326)) matches the declaration it was read
	// from, and strips the domain's own schema, since format_type writes a base
	// type on the search path unqualified while a file may name it in full.
	if !equalTypeName(current.BaseType, desired.BaseType, schemaOf(fqdn)) {
		return nil, fmt.Errorf("cannot change base type of domain %s from %s to %s: PostgreSQL does not support this", fqdn, current.BaseType, desired.BaseType)
	}

	// Collation change is not supported by PostgreSQL ALTER DOMAIN
	if !equalCollation(current.Collation, desired.Collation) {
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
		stmts = append(stmts, commentOnSQL("DOMAIN "+fqdn, desired.Comment))
	}

	return stmts, nil
}

func diffDomainConstraints(fqdn string, current, desired []*model.DomainConstraint) []string {
	var stmts []string

	currentByName := make(map[string]*model.DomainConstraint)
	for _, c := range current {
		currentByName[c.Name] = c
	}

	desiredByName := make(map[string]*model.DomainConstraint)
	for _, c := range desired {
		desiredByName[c.Name] = c
	}

	// A validated constraint the desired schema writes NOT VALID is dropped
	// and added back, as a table's is, since nothing clears the flag.
	change := func(cur, des *model.DomainConstraint) definitionChange {
		return newDefinitionChange(equalConstraintDef(cur.Definition, des.Definition), cur.Validated, des.Validated)
	}

	// Drop removed or changed constraints. One that only needs validating is
	// kept.
	for _, c := range current {
		if d, ok := desiredByName[c.Name]; ok {
			if ch := change(c, d); !ch.changed || ch.validateOnly {
				continue
			}
		}
		stmts = append(stmts, "ALTER DOMAIN "+fqdn+" DROP CONSTRAINT "+model.Ident(c.Name)+";")
	}

	// Add new or changed constraints, or validate a NOT VALID one. A
	// constraint added here has no comment yet.
	currentComments := map[string]*string{}
	for _, c := range desired {
		if cur, ok := currentByName[c.Name]; ok {
			ch := change(cur, c)
			if !ch.changed || ch.validateOnly {
				currentComments[c.Name] = cur.Comment
			}
			if !ch.changed {
				continue
			}
			if ch.validateOnly {
				stmts = append(stmts, "ALTER DOMAIN "+fqdn+" VALIDATE CONSTRAINT "+model.Ident(c.Name)+";")
				continue
			}
		}
		sql := "ALTER DOMAIN " + fqdn + " ADD CONSTRAINT " + model.Ident(c.Name) + " " + c.Definition
		if !c.Validated {
			sql += " NOT VALID"
		}
		stmts = append(stmts, sql+";")
	}

	for _, c := range desired {
		if !equalPtr(currentComments[c.Name], c.Comment) {
			stmts = append(stmts, commentOnSQL("CONSTRAINT "+model.Ident(c.Name)+" ON DOMAIN "+fqdn, c.Comment))
		}
	}

	return stmts
}

// detectDomainRenames finds desired domains with RenameFrom that match a current domain.
func detectDomainRenames(current, desired *orderedmap.Map[string, *model.Domain]) ([]string, *orderedmap.Map[string, *model.Domain], error) {
	return detectRenames(current, desired, "DOMAIN",
		func(d *model.Domain) (string, string, *string) { return d.Schema, d.Name, d.RenameFrom },
		func(old *model.Domain, name string) *model.Domain {
			renamed := *old
			renamed.Name = name
			renamed.Constraints = slices.Clone(old.Constraints)
			return &renamed
		})
}
