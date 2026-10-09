// Package lint checks the objects that schema files declare against rules the
// user writes. A rule is a CEL expression that must be true for every object
// of one kind. Its when expression can limit it to some of those objects. The
// rule reads the object as pista parse writes it in JSON.
package lint

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"cel.dev/cel-go/cel"
	celast "cel.dev/cel-go/common/ast"
	"github.com/winebarrel/pistachio/parser"
	"gopkg.in/yaml.v3"
)

// Rule is one rule read from a rule file.
type Rule struct {
	Name        string
	On          parser.LintKind
	Description string
	Let         []Let
	When        string
	Assert      string
	Message     string
	File        string
	// when is nil when the rule has no when, so it checks every object of
	// its kind.
	when    cel.Program
	program cel.Program
}

// Let is one value that let names. assert reads it by Name.
type Let struct {
	Name    string
	Expr    string
	program cel.Program
}

type ruleFile struct {
	Rules []ruleSpec `yaml:"rules"`
}

type ruleSpec struct {
	Name        string    `yaml:"name"`
	On          string    `yaml:"on"`
	Description string    `yaml:"description"`
	Let         []letSpec `yaml:"let"`
	When        string    `yaml:"when"`
	Assert      string    `yaml:"assert"`
	Message     string    `yaml:"message"`
}

type letSpec struct {
	Name string `yaml:"name"`
	Expr string `yaml:"expr"`
}

// kinds lists the values of on, in the order the documentation gives them.
var kinds = []parser.LintKind{parser.LintTable, parser.LintColumn, parser.LintIndex, parser.LintForeignKey}

// Linter holds the rules that Load read.
type Linter struct {
	Rules []*Rule
	trace *trace
}

// Load reads the rules in paths. A path is a rule file, or a directory whose
// .yml and .yaml files are read in name order. Subdirectories are not read.
// Two rules with the same name are an error. stderr receives what the debug()
// function prints and the warnings Run writes.
func Load(paths []string, stderr io.Writer) (*Linter, error) {
	var files []string
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			files = append(files, path)
			continue
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			ext := strings.ToLower(filepath.Ext(e.Name()))
			if ext != ".yml" && ext != ".yaml" {
				continue
			}
			// Stat follows a symbolic link, so a link to a rule file is read.
			file := filepath.Join(path, e.Name())
			info, err := os.Stat(file)
			if err != nil {
				return nil, err
			}
			if !info.IsDir() {
				files = append(files, file)
			}
		}
	}

	tr := &trace{w: stderr}
	envs := newEnvs(tr)

	var rules []*Rule
	seen := map[string]string{}
	for _, file := range files {
		specs, err := readRuleFile(file)
		if err != nil {
			return nil, err
		}
		for _, spec := range specs {
			rule, err := compileRule(spec, file, envs)
			if err != nil {
				return nil, err
			}
			if prev, ok := seen[rule.Name]; ok {
				return nil, fmt.Errorf("%s: rule %s is also defined in %s", file, rule.Name, prev)
			}
			seen[rule.Name] = file
			rules = append(rules, rule)
		}
	}

	if len(rules) == 0 {
		return nil, errors.New("no rules found in " + strings.Join(paths, ", "))
	}

	return &Linter{Rules: rules, trace: tr}, nil
}

func readRuleFile(file string) ([]ruleSpec, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var rf ruleFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&rf); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	// A second document after --- would be skipped without a word.
	var next yaml.Node
	if err := dec.Decode(&next); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: a rule file holds one YAML document", file)
	}

	return rf.Rules, nil
}

func compileRule(spec ruleSpec, file string, envs map[parser.LintKind]*cel.Env) (*Rule, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("%s: a rule has no name", file)
	}
	// -- pista:lint-ignore separates names with commas and spaces.
	if strings.ContainsAny(spec.Name, ", \t") {
		return nil, fmt.Errorf("%s: rule name %q has a comma or a space", file, spec.Name)
	}
	where := fmt.Sprintf("%s: rule %s", file, spec.Name)

	kind := parser.LintKind(spec.On)
	if !slices.Contains(kinds, kind) {
		return nil, fmt.Errorf("%s: on must be one of table, column, index or foreign_key, not %q", where, spec.On)
	}
	if spec.Assert == "" {
		return nil, fmt.Errorf("%s: assert is empty", where)
	}
	if spec.Message == "" {
		return nil, fmt.Errorf("%s: message is empty", where)
	}

	// when cannot read let, since let is evaluated only where when is true.
	env := envs[kind]
	var when cel.Program
	if spec.When != "" {
		var err error
		when, err = compileExpr(env, spec.When, "when", where)
		if err != nil {
			return nil, err
		}
	}
	lets, env, err := compileLets(env, spec.Let, kind, where)
	if err != nil {
		return nil, err
	}
	program, err := compileExpr(env, spec.Assert, "assert", where)
	if err != nil {
		return nil, err
	}

	return &Rule{
		Name:        spec.Name,
		On:          kind,
		Description: spec.Description,
		Let:         lets,
		When:        spec.When,
		Assert:      spec.Assert,
		Message:     spec.Message,
		File:        file,
		when:        when,
		program:     program,
	}, nil
}

// compileLets compiles the values of let in order, each one with the values
// before it declared. It returns env with every value declared, for assert.
func compileLets(env *cel.Env, specs []letSpec, kind parser.LintKind, where string) ([]Let, *cel.Env, error) {
	defined := append([]string{"doc"}, variables[kind]...)
	var lets []Let
	for _, spec := range specs {
		if spec.Name == "" {
			return nil, nil, fmt.Errorf("%s: a let has no name", where)
		}
		if !isIdent(env, spec.Name) {
			return nil, nil, fmt.Errorf("%s: let name %q is not a CEL identifier", where, spec.Name)
		}
		if slices.Contains(defined, spec.Name) {
			return nil, nil, fmt.Errorf("%s: let %s is already defined", where, spec.Name)
		}
		if spec.Expr == "" {
			return nil, nil, fmt.Errorf("%s: let %s: expr is empty", where, spec.Name)
		}

		ast, iss := env.Compile(spec.Expr)
		if iss.Err() != nil {
			return nil, nil, fmt.Errorf("%s: let %s: %w", where, spec.Name, iss.Err())
		}
		// The value keeps the type CEL found, so assert x on an int fails to
		// compile rather than when it runs.
		env = must(env.Extend(cel.Variable(spec.Name, ast.OutputType())))
		lets = append(lets, Let{Name: spec.Name, Expr: spec.Expr, program: must(env.Program(ast))})
		defined = append(defined, spec.Name)
	}

	return lets, env, nil
}

// isIdent reports whether name parses as one CEL identifier, so CEL decides
// which words are reserved.
func isIdent(env *cel.Env, name string) bool {
	parsed, iss := env.Parse(name)
	if iss.Err() != nil {
		return false
	}
	e := parsed.NativeRep().Expr()
	return e.Kind() == celast.IdentKind && e.AsIdent() == name
}

// compileExpr compiles the CEL expression of the field assert or when. Its
// result must be a boolean.
func compileExpr(env *cel.Env, expr, field, where string) (cel.Program, error) {
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("%s: %s: %w", where, field, iss.Err())
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("%s: %s is %s, not bool", where, field, t)
	}
	return must(env.Program(ast)), nil
}
