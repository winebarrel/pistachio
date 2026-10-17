package pistachio

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
)

const refuseHazardInit = `
CREATE TABLE public.orders (
    id integer NOT NULL,
    amount integer,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);`

const refuseHazardDesired = `
CREATE TABLE public.orders (
    id integer NOT NULL,
    amount integer NOT NULL,
    note text,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);`

// A refused plan is printed but not written for apply-from.
func TestPlan_RefuseHazardWritesNoPlanFile(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, refuseHazardInit)

	dir := t.TempDir()
	desiredFile := filepath.Join(dir, "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(refuseHazardDesired), 0o644))
	out := filepath.Join(dir, "plan.json")

	client := NewClient(&Options{ConnString: testutil.ConnString(), Schemas: []string{"public"}})
	result, err := client.Plan(ctx, &PlanOptions{
		Files:        []string{desiredFile},
		RefuseHazard: []string{"scan"},
		Out:          out,
	})
	require.NoError(t, err)
	require.NotNil(t, result.Refused)
	assert.Equal(t, "1 statement refused by --refuse-hazard=scan", result.Refused.Error())
	assert.Contains(t, result.SQL, "-- refused: scan, blocks reads and writes: public.orders\nALTER TABLE public.orders ALTER COLUMN amount SET NOT NULL;")
	assert.NoFileExists(t, out)

	// A plan nothing is refused in is written as usual.
	result, err = client.Plan(ctx, &PlanOptions{
		Files:        []string{desiredFile},
		RefuseHazard: []string{"rewrite"},
		Out:          out,
	})
	require.NoError(t, err)
	assert.Nil(t, result.Refused)
	assert.FileExists(t, out)
}

// apply runs nothing when a statement is refused, and names the refused
// statements in the error, since it writes no plan to show them in.
func TestApply_RefuseHazard(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, refuseHazardInit)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(refuseHazardDesired), 0o644))

	client := NewClient(&Options{ConnString: testutil.ConnString(), Schemas: []string{"public"}})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &ApplyOptions{
		Files:        []string{desiredFile},
		PreSQL:       "CREATE TABLE public.pre_sql_ran (id integer);",
		RefuseHazard: []string{"blocks-reads", "blocks-writes"},
	}, &buf)
	var refused *HazardRefusedError
	require.ErrorAs(t, err, &refused)
	assert.Equal(t, `1 statement refused by --refuse-hazard=blocks-reads,blocks-writes:
-- refused: scan, blocks reads and writes: public.orders
ALTER TABLE public.orders ALTER COLUMN amount SET NOT NULL;`, err.Error())
	assert.Empty(t, buf.String())

	// Nothing ran: not the pre-SQL, and not the ADD COLUMN that was not
	// refused.
	var n int
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'note'`).Scan(&n))
	assert.Equal(t, 0, n)
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'pre_sql_ran'`).Scan(&n))
	assert.Equal(t, 0, n)

	// A kind no statement is of lets the apply through.
	result, err := client.Apply(ctx, &ApplyOptions{
		Files:        []string{desiredFile},
		RefuseHazard: []string{"rewrite"},
	}, &buf)
	require.NoError(t, err)
	assert.True(t, result.Applied)
}
