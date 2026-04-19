package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
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
