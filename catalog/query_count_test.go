package catalog_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// queryCounter counts the statements a connection sends.
type queryCounter struct {
	n int
}

func (c *queryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.n++
	return ctx
}

func (c *queryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func connectWithCounter(t *testing.T, ctx context.Context) (*pgx.Conn, *queryCounter) {
	t.Helper()
	cfg, err := pgx.ParseConfig(testutil.ConnString())
	require.NoError(t, err)
	counter := &queryCounter{}
	cfg.Tracer = counter
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	return conn, counter
}

// Reading tables costs a fixed number of statements: growing the schema must
// not grow the statement count.
func TestTables_QueryCountDoesNotGrowWithTables(t *testing.T) {
	ctx := context.Background()
	setupConn := testutil.ConnectDB(t)
	defer setupConn.Close(ctx)

	ddl := func(n int) string {
		sql := ""
		for i := range n {
			name := "t" + string(rune('a'+i))
			sql += `
				CREATE TABLE public.` + name + ` (
					id bigint NOT NULL,
					owner text NOT NULL,
					CONSTRAINT ` + name + `_pkey PRIMARY KEY (id),
					CONSTRAINT ` + name + `_owner_check CHECK (owner <> '')
				);
				ALTER TABLE public.` + name + ` ENABLE ROW LEVEL SECURITY;
				CREATE POLICY ` + name + `_sel ON public.` + name + ` FOR SELECT USING (owner = current_user);
			`
		}
		return sql
	}

	count := func(t *testing.T, n int) int {
		t.Helper()
		testutil.SetupDB(t, ctx, setupConn, ddl(n))
		conn, counter := connectWithCounter(t, ctx)
		defer conn.Close(ctx)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)
		require.Equal(t, n, tables.Len())
		return counter.n
	}

	few := count(t, 2)
	many := count(t, 20)
	assert.Equal(t, few, many, "statement count grew with the number of tables")
}

// Domains and composite types are read the same way, so their statement count
// is fixed too.
func TestTypes_QueryCountDoesNotGrowWithTypes(t *testing.T) {
	ctx := context.Background()
	setupConn := testutil.ConnectDB(t)
	defer setupConn.Close(ctx)

	ddl := func(n int) string {
		sql := ""
		for i := range n {
			name := "d" + string(rune('a'+i))
			sql += `
				CREATE DOMAIN public.` + name + ` AS text
					CONSTRAINT ` + name + `_len CHECK (length(VALUE) > 0)
					CONSTRAINT ` + name + `_low CHECK (VALUE = lower(VALUE));
				CREATE TYPE public.c` + name + ` AS (a integer, b text, c timestamptz);
			`
		}
		return sql
	}

	count := func(t *testing.T, n int) int {
		t.Helper()
		testutil.SetupDB(t, ctx, setupConn, ddl(n))
		conn, counter := connectWithCounter(t, ctx)
		defer conn.Close(ctx)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		domains, err := cat.Domains(ctx)
		require.NoError(t, err)
		require.Equal(t, n, domains.Len())
		compositeTypes, err := cat.CompositeTypes(ctx)
		require.NoError(t, err)
		require.Equal(t, n, compositeTypes.Len())
		return counter.n
	}

	few := count(t, 2)
	many := count(t, 20)
	assert.Equal(t, few, many, "statement count grew with the number of types")
}
