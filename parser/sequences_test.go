package parser_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

func parseOneSequence(t *testing.T, sql string) *model.Sequence {
	t.Helper()
	f := filepath.Join(t.TempDir(), "seq.sql")
	require.NoError(t, os.WriteFile(f, []byte(sql), 0o644))
	result, err := parser.ParseSQLFilesWithSchema([]string{f}, "public")
	require.NoError(t, err)
	require.Equal(t, 1, result.Sequences.Len())
	return result.Sequences.CollectValues()[0]
}

func TestParseCreateSeqStmt_Defaults(t *testing.T) {
	seq := parseOneSequence(t, "CREATE SEQUENCE public.s;")
	assert.Equal(t, "public", seq.Schema)
	assert.Equal(t, "s", seq.Name)
	assert.Equal(t, "bigint", seq.DataType)
	assert.Equal(t, int64(1), seq.Start)
	assert.Equal(t, int64(1), seq.Min)
	assert.Equal(t, int64(9223372036854775807), seq.Max)
	assert.Equal(t, int64(1), seq.Increment)
	assert.Equal(t, int64(1), seq.Cache)
	assert.False(t, seq.Cycle)
	assert.Nil(t, seq.OwnerTable)
}

func TestParseCreateSeqStmt_Descending(t *testing.T) {
	seq := parseOneSequence(t, "CREATE SEQUENCE public.s INCREMENT BY -1;")
	assert.Equal(t, int64(-1), seq.Increment)
	assert.Equal(t, int64(-9223372036854775808), seq.Min)
	assert.Equal(t, int64(-1), seq.Max)
	assert.Equal(t, int64(-1), seq.Start)
}

func TestParseCreateSeqStmt_TypeBounds(t *testing.T) {
	small := parseOneSequence(t, "CREATE SEQUENCE public.s AS smallint;")
	assert.Equal(t, "smallint", small.DataType)
	assert.Equal(t, int64(32767), small.Max)

	integer := parseOneSequence(t, "CREATE SEQUENCE public.s AS integer;")
	assert.Equal(t, "integer", integer.DataType)
	assert.Equal(t, int64(2147483647), integer.Max)
}

func TestParseCreateSeqStmt_NoMinvalueUsesDefault(t *testing.T) {
	seq := parseOneSequence(t, "CREATE SEQUENCE public.s NO MINVALUE NO MAXVALUE;")
	assert.Equal(t, int64(1), seq.Min)
	assert.Equal(t, int64(9223372036854775807), seq.Max)
}

func TestParseCreateSeqStmt_ExplicitBigValues(t *testing.T) {
	// Values beyond int32 arrive from pg_query as Float nodes carrying the
	// decimal string, exercising defElemInt64's Float branch.
	seq := parseOneSequence(t, "CREATE SEQUENCE public.s MINVALUE -9223372036854775808 MAXVALUE 9223372036854775807;")
	assert.Equal(t, int64(-9223372036854775808), seq.Min)
	assert.Equal(t, int64(9223372036854775807), seq.Max)
}

func TestParseCreateSeqStmt_AllOptions(t *testing.T) {
	seq := parseOneSequence(t, `CREATE SEQUENCE public.s
		AS integer INCREMENT BY 5 MINVALUE 10 MAXVALUE 10000 START WITH 20 CACHE 3 CYCLE;`)
	assert.Equal(t, "integer", seq.DataType)
	assert.Equal(t, int64(5), seq.Increment)
	assert.Equal(t, int64(10), seq.Min)
	assert.Equal(t, int64(10000), seq.Max)
	assert.Equal(t, int64(20), seq.Start)
	assert.Equal(t, int64(3), seq.Cache)
	assert.True(t, seq.Cycle)
}

func TestParseCreateSeqStmt_OwnedBy(t *testing.T) {
	seq := parseOneSequence(t, "CREATE SEQUENCE public.s OWNED BY public.t.id;")
	require.NotNil(t, seq.OwnerTable)
	assert.Equal(t, "t", *seq.OwnerTable)
	require.NotNil(t, seq.OwnerColumn)
	assert.Equal(t, "id", *seq.OwnerColumn)
	assert.True(t, seq.Owned())
}

func TestParseCreateSeqStmt_OwnedByNone(t *testing.T) {
	seq := parseOneSequence(t, "CREATE SEQUENCE public.s OWNED BY NONE;")
	assert.Nil(t, seq.OwnerTable)
	assert.False(t, seq.Owned())
}
