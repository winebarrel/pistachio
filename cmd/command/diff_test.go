package command_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/cmd/command"
)

func TestDiff_Run(t *testing.T) {
	current := writeSQLFile(t, "current.sql", `
create table users (
    id integer not null,
    constraint users_pkey primary key (id)
);
`)
	desired := writeSQLFile(t, "desired.sql", `
create table users (
    id integer not null,
    email text,
    constraint users_pkey primary key (id)
);
`)

	var buf bytes.Buffer
	cmd := &command.Diff{
		Schemas: []string{"public"},
		Current: current,
		Desired: desired,
	}
	require.NoError(t, cmd.Run(&buf))

	out := buf.String()
	assert.Equal(t, "-- Diff for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)", firstLine(out))
	assert.Contains(t, out, "ALTER TABLE public.users ADD COLUMN email text;")
}

func TestDiff_Run_NoChanges(t *testing.T) {
	sql := "create table users (id integer not null, constraint users_pkey primary key (id));"
	current := writeSQLFile(t, "current.sql", sql)
	desired := writeSQLFile(t, "desired.sql", sql)

	var buf bytes.Buffer
	cmd := &command.Diff{
		Schemas: []string{"public"},
		Current: current,
		Desired: desired,
	}
	require.NoError(t, cmd.Run(&buf))
	assert.Contains(t, buf.String(), "-- No changes")
}

// --check reports changes as ErrDiffChanges, which main maps to exit code 2.
// The diff has still been written.
func TestDiff_Run_Check(t *testing.T) {
	current := writeSQLFile(t, "current.sql", "")
	desired := writeSQLFile(t, "desired.sql", "create table users (id integer);")

	var buf bytes.Buffer
	cmd := &command.Diff{
		Schemas: []string{"public"},
		Current: current,
		Desired: desired,
		Check:   true,
	}
	err := cmd.Run(&buf)
	require.ErrorIs(t, err, command.ErrDiffChanges)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestDiff_Run_CheckNoChanges(t *testing.T) {
	sql := "create table users (id integer);"
	current := writeSQLFile(t, "current.sql", sql)
	desired := writeSQLFile(t, "desired.sql", sql)

	var buf bytes.Buffer
	cmd := &command.Diff{
		Schemas: []string{"public"},
		Current: current,
		Desired: desired,
		Check:   true,
	}
	require.NoError(t, cmd.Run(&buf))
	assert.Contains(t, buf.String(), "-- No changes")
}

// A drop the policy does not allow is a comment, not an executable change, so
// it does not trip --check; it still has to be written in the no-SQL case.
func TestDiff_Run_DisallowedDropIsNotAChange(t *testing.T) {
	current := writeSQLFile(t, "current.sql", "create table users (id integer);")
	desired := writeSQLFile(t, "desired.sql", "")

	var buf bytes.Buffer
	cmd := &command.Diff{
		Schemas: []string{"public"},
		Current: current,
		Desired: desired,
		Check:   true,
	}
	require.NoError(t, cmd.Run(&buf))
	assert.Contains(t, buf.String(), "-- skipped: DROP TABLE public.users;")
	assert.Contains(t, buf.String(), "-- No changes")
}

// With changes present, ignored objects and skipped drops still follow the
// SQL as comments.
func TestDiff_Run_ChangesWithIgnoredAndSkippedDrop(t *testing.T) {
	current := writeSQLFile(t, "current.sql", `
create table users (id integer);
create table legacy (id integer);
create table gone (id integer);
`)
	desired := writeSQLFile(t, "desired.sql", `
create table users (id integer, email text);
-- pista:ignore
create table legacy (id integer);
`)

	var buf bytes.Buffer
	cmd := &command.Diff{
		Schemas: []string{"public"},
		Current: current,
		Desired: desired,
	}
	require.NoError(t, cmd.Run(&buf))
	out := buf.String()
	assert.Contains(t, out, "ALTER TABLE public.users ADD COLUMN email text;")
	assert.Contains(t, out, "-- ignored: public.legacy")
	assert.Contains(t, out, "-- skipped: DROP TABLE public.gone;")
}

func TestDiff_Run_ParseError(t *testing.T) {
	current := writeSQLFile(t, "current.sql", "CREATE TABLE (;")
	desired := writeSQLFile(t, "desired.sql", "")

	var buf bytes.Buffer
	cmd := &command.Diff{
		Schemas: []string{"public"},
		Current: current,
		Desired: desired,
	}
	require.Error(t, cmd.Run(&buf))
}
