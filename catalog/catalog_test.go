package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
	"github.com/winebarrel/pistachio/model"
)

func TestNewCatalog(t *testing.T) {
	conn := testutil.ConnectDB(t)
	ctx := context.Background()
	defer conn.Close(ctx)

	t.Run("with schemas", func(t *testing.T) {
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		assert.NotNil(t, cat)
	})

	t.Run("empty schemas", func(t *testing.T) {
		_, err := catalog.NewCatalog(conn, []string{})
		require.Error(t, err)
	})
}

// A dropped connection surfaces as an error from every catalog reader rather
// than a panic or an empty result.
func TestCatalog_ClosedConnection(t *testing.T) {
	conn := testutil.ConnectDB(t)
	ctx := context.Background()

	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)
	require.NoError(t, conn.Close(ctx))

	// A non-zero OID so the batched readers reach the connection instead of
	// returning early on an empty table list.
	tbls := []*model.Table{{OID: 1, Schema: "public", Name: "users"}}

	tests := []struct {
		name string
		call func() error
	}{
		{"Tables", func() error { _, err := cat.Tables(ctx); return err }},
		{"ListTables", func() error { _, err := cat.ListTables(ctx); return err }},
		{"Views", func() error { _, err := cat.Views(ctx); return err }},
		{"ListViews", func() error { _, err := cat.ListViews(ctx); return err }},
		{"Enums", func() error { _, err := cat.Enums(ctx); return err }},
		{"ListEnums", func() error { _, err := cat.ListEnums(ctx); return err }},
		{"Domains", func() error { _, err := cat.Domains(ctx); return err }},
		{"ListDomains", func() error { _, err := cat.ListDomains(ctx); return err }},
		{"CompositeTypes", func() error { _, err := cat.CompositeTypes(ctx); return err }},
		{"ListCompositeTypes", func() error { _, err := cat.ListCompositeTypes(ctx); return err }},
		{"Sequences", func() error { _, err := cat.Sequences(ctx); return err }},
		{"ListSequences", func() error { _, err := cat.ListSequences(ctx); return err }},
		{"Routines", func() error { _, err := cat.Routines(ctx); return err }},
		{"ListRoutines", func() error { _, err := cat.ListRoutines(ctx); return err }},
		{"ListIndexes", func() error { _, err := cat.ListIndexes(ctx); return err }},
		{"ListTriggers", func() error { _, err := cat.ListTriggers(ctx); return err }},
		{"ListColumnsByTables", func() error { _, err := cat.ListColumnsByTables(ctx, tbls); return err }},
		{"ListConstraintsByTables", func() error { _, _, err := cat.ListConstraintsByTables(ctx, tbls); return err }},
		{"ListPoliciesByTables", func() error { _, err := cat.ListPoliciesByTables(ctx, tbls); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.call())
		})
	}
}
