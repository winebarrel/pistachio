package command_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/cmd/command"
)

const lintRules = `rules:
  - name: require-primary-key
    on: table
    assert: table.constraints.values().exists(c, c.type == "primary_key")
    message: no primary key
`

func TestLint_AfterApply_TrimsSchemas(t *testing.T) {
	cmd := &command.Lint{Schemas: []string{" app"}}
	require.NoError(t, cmd.AfterApply())
	assert.Equal(t, []string{"app"}, cmd.Schemas)
}

func TestLint_Run(t *testing.T) {
	rules := writeSQLFile(t, "rules.yml", lintRules)
	path := writeSQLFile(t, "schema.sql", `
create table logs (msg text);
create table users (id bigint not null, constraint users_pkey primary key (id));
`)

	var buf bytes.Buffer
	cmd := &command.Lint{Files: []string{path}, Rules: []string{rules}, Schemas: []string{"public"}}
	require.ErrorIs(t, cmd.Run(&buf), command.ErrLintViolations)
	assert.Equal(t, "table public.logs: require-primary-key: no primary key\n", buf.String())
}

func TestLint_Run_NoViolations(t *testing.T) {
	rules := writeSQLFile(t, "rules.yml", lintRules)
	path := writeSQLFile(t, "schema.sql", "create table users (id bigint not null, constraint users_pkey primary key (id));")

	var buf bytes.Buffer
	cmd := &command.Lint{Files: []string{path}, Rules: []string{rules}, Schemas: []string{"public"}}
	require.NoError(t, cmd.Run(&buf))
	assert.Empty(t, buf.String())
}

// An unqualified name is qualified with the first schema, as parse does.
func TestLint_Run_Schema(t *testing.T) {
	rules := writeSQLFile(t, "rules.yml", lintRules)
	path := writeSQLFile(t, "schema.sql", "create table logs (msg text);")

	var buf bytes.Buffer
	cmd := &command.Lint{Files: []string{path}, Rules: []string{rules}, Schemas: []string{"app", "public"}}
	require.ErrorIs(t, cmd.Run(&buf), command.ErrLintViolations)
	assert.Equal(t, "table app.logs: require-primary-key: no primary key\n", buf.String())
}

func TestLint_Run_Errors(t *testing.T) {
	rules := writeSQLFile(t, "rules.yml", lintRules)
	badRules := writeSQLFile(t, "bad.yml", "rules:\n  - {name: r, on: view, assert: 'true', message: m}\n")
	schema := writeSQLFile(t, "schema.sql", "create table t (id int);")
	broken := writeSQLFile(t, "broken.sql", "create table (")
	runtime := writeSQLFile(t, "runtime.yml", "rules:\n  - {name: r, on: table, assert: 'table.nope', message: m}\n")

	for name, tc := range map[string]struct {
		rules, file, want string
	}{
		"a bad rule file": {badRules, schema, "on must be one of"},
		"a broken schema": {rules, broken, "syntax error"},
		"a failing rule":  {runtime, schema, "table public.t: r: no such key: nope"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := &command.Lint{Files: []string{tc.file}, Rules: []string{tc.rules}, Schemas: []string{"public"}}
			require.ErrorContains(t, cmd.Run(&bytes.Buffer{}), tc.want)
		})
	}
}
