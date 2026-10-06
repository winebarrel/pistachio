// Package lint checks the objects that schema files declare against rules the
// user writes. A rule is a CEL expression that must be true for every object
// of one kind. The rule reads the object as pista parse writes it in JSON.
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
	"github.com/winebarrel/pistachio/parser"
	"gopkg.in/yaml.v3"
)

// Rule is one rule read from a rule file.
type Rule struct {
	Name    string
	On      parser.LintKind
	Assert  string
	Message string
	File    string
	program cel.Program
}

type ruleFile struct {
	Rules []ruleSpec `yaml:"rules"`
}

type ruleSpec struct {
	Name    string `yaml:"name"`
	On      string `yaml:"on"`
	Assert  string `yaml:"assert"`
	Message string `yaml:"message"`
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

	env := envs[kind]
	ast, iss := env.Compile(spec.Assert)
	if iss.Err() != nil {
		return nil, fmt.Errorf("%s: %w", where, iss.Err())
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("%s: assert is %s, not bool", where, t)
	}
	return &Rule{
		Name:    spec.Name,
		On:      kind,
		Assert:  spec.Assert,
		Message: spec.Message,
		File:    file,
		program: must(env.Program(ast)),
	}, nil
}
