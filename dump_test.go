package pistachio_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

type dumpTestCase struct {
	Init string `yaml:"init"`
	Dump string `yaml:"dump"`
}

func TestDump_InvalidConnString(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.Error(t, err)
}

func TestDump(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	files, err := filepath.Glob("testdata/dump/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			tc := loadYAML[dumpTestCase](t, file)
			testutil.SetupDB(t, ctx, conn, tc.Init)
			client := pistachio.NewClient(&pistachio.Options{
				ConnString: conn.Config().ConnString(),
				Schemas:    []string{"public"},
			})
			got, err := client.Dump(ctx, &pistachio.DumpOptions{})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Dump), strings.TrimSpace(got))
		})
	}
}
