package lint

import (
	"encoding/json/v2"
	"fmt"
	"slices"

	"cel.dev/cel-go/common/types"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/document"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

// Violation is one object for which a rule is false. Position is where the
// object is declared, and empty for SQL that came from no file.
type Violation struct {
	Position string
	Object   string
	Rule     string
	Message  string
}

func (v Violation) String() string {
	return withPosition(v.Position, v.Object+": "+v.Rule+": "+v.Message)
}

func withPosition(pos, s string) string {
	if pos == "" {
		return s
	}
	return pos + ": " + s
}

// target is one object to check, with the variables a rule of its kind reads.
type target struct {
	key   parser.LintTarget
	label string
	vars  map[string]any
}

// Run checks every object of the parse result against the rules, in the order
// the files declare the objects, and returns the objects a rule is false for.
// A table marked -- pista:ignore is not checked, nor is anything on it.
// schemas resolves an unqualified domain name, as document.New does.
func (l *Linter) Run(r *parser.ParseResult, schemas []string) ([]Violation, error) {
	targets, err := collectTargets(r, schemas)
	if err != nil {
		return nil, err
	}

	var violations []Violation
	for _, t := range targets {
		ignored := r.LintIgnores[t.key]
		var pos string
		if p, ok := r.Positions[t.key]; ok {
			pos = p.String()
		}
		for _, rule := range l.Rules {
			if rule.On != t.key.Kind || slices.Contains(ignored, rule.Name) {
				continue
			}

			l.trace.object, l.trace.rule = t.label, rule.Name
			out, _, err := rule.program.Eval(t.vars)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", withPosition(pos, t.label+": "+rule.Name), err)
			}
			ok, isBool := out.(types.Bool)
			if !isBool {
				return nil, fmt.Errorf("%s: assert returned %s, not bool", withPosition(pos, t.label+": "+rule.Name), out.Type().TypeName())
			}
			if !ok {
				violations = append(violations, Violation{Position: pos, Object: t.label, Rule: rule.Name, Message: rule.Message})
			}
		}
	}

	return violations, nil
}

// collectTargets lists the objects to check. Each object is read from the JSON
// document that pista parse writes, so a rule sees the same fields. The
// document's maps carry no order once decoded, so the order comes from the
// parse result.
func collectTargets(r *parser.ParseResult, schemas []string) ([]target, error) {
	doc := document.New(r, schemas)
	encoded, err := json.Marshal(doc, json.Deterministic(true), model.JSONMarshalers)
	if err != nil {
		return nil, err
	}
	var root map[string]any
	if err := json.Unmarshal(encoded, &root); err != nil {
		return nil, err
	}

	jsonTables, _ := root["tables"].(map[string]any)
	jsonViews, _ := root["views"].(map[string]any)

	var targets []target
	add := func(key parser.LintTarget, label string, vars map[string]any) {
		vars["doc"] = root
		targets = append(targets, target{key: key, label: label, vars: vars})
	}

	if doc.Tables != nil {
		for fqtn, t := range doc.Tables.All() {
			if t.Ignore {
				continue
			}
			table, _ := jsonTables[fqtn].(map[string]any)
			add(parser.LintTarget{Kind: parser.LintTable, Table: fqtn}, "table "+fqtn, map[string]any{"table": table})

			columns, _ := table["columns"].([]any)
			for _, c := range columns {
				column, _ := c.(map[string]any)
				name, _ := column["name"].(string)
				add(parser.LintTarget{Kind: parser.LintColumn, Table: fqtn, Name: name},
					"column "+fqtn+"."+model.Ident(name), map[string]any{"table": table, "column": column})
			}

			addIndexes(add, fqtn, table, t.Indexes)

			fks, _ := table["foreign_keys"].(map[string]any)
			if t.ForeignKeys != nil {
				for name := range t.ForeignKeys.Keys() {
					add(parser.LintTarget{Kind: parser.LintForeignKey, Table: fqtn, Name: name},
						"foreign key "+model.Ident(name)+" on "+fqtn, map[string]any{"table": table, "fk": fks[name]})
				}
			}
		}
	}

	// An index on a materialized view is checked with the view as table.
	if doc.Views != nil {
		for fqvn, v := range doc.Views.All() {
			if v.Ignore || !v.Materialized {
				continue
			}
			view, _ := jsonViews[fqvn].(map[string]any)
			addIndexes(add, fqvn, view, v.Indexes)
		}
	}

	return targets, nil
}

func addIndexes(add func(parser.LintTarget, string, map[string]any), fqtn string, table map[string]any, indexes *orderedmap.Map[string, *document.Index]) {
	if indexes == nil {
		return
	}

	jsonIndexes, _ := table["indexes"].(map[string]any)
	for name, idx := range indexes.All() {
		add(parser.LintTarget{Kind: parser.LintIndex, Table: fqtn, Name: name},
			"index "+model.Ident(idx.Schema, idx.Name), map[string]any{"table": table, "index": jsonIndexes[name]})
	}
}
