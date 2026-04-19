package command_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/cmd/command"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestPlan_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestDump_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Dump{}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestPlan_Run_Error(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
}

func TestDump_Run_Error(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Dump{}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
}

func TestApply_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}
