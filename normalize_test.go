package pistachio_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
	"github.com/winebarrel/pistachio/model"
)

func TestNormalizeDesiredViewDefs_ClosedConn(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	conn.Close(ctx)

	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	// Should not panic; silently returns on conn.Begin failure
	pistachio.NormalizeDesiredViewDefs(ctx, conn, current, desired)

	v, _ := desired.GetOk("public.v1")
	assert.Equal(t, "SELECT 1", v.Definition)
}

func TestNormalizeDesiredViewDefs_QueryRowError(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.items (
    id integer NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_items AS SELECT id FROM items;
`)

	current := orderedmap.New[string, *model.View]()
	// FQVN "nonexistent.active_items" exists in current so normalization is attempted,
	// but CREATE VIEW succeeds under public while pg_get_viewdef('nonexistent.active_items'::regclass) fails.
	current.Set("nonexistent.active_items", &model.View{Schema: "nonexistent", Name: "active_items", Definition: "SELECT id FROM items"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("nonexistent.active_items", &model.View{Schema: "nonexistent", Name: "active_items", Definition: "SELECT id FROM items"})

	pistachio.NormalizeDesiredViewDefs(ctx, conn, current, desired)

	// tx.Exec fails (nonexistent schema), definition unchanged
	v, _ := desired.GetOk("nonexistent.active_items")
	assert.Equal(t, "SELECT id FROM items", v.Definition)
}

func TestNormalizeDesiredViewDefs_SavepointIsolation(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.items (
    id integer NOT NULL,
    label text NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
CREATE VIEW public.good_view AS SELECT id, label FROM items WHERE label = 'active';
`)

	current := orderedmap.New[string, *model.View]()
	// First view: will fail (references nonexistent column)
	current.Set("public.bad_view", &model.View{Schema: "public", Name: "bad_view", Definition: "SELECT missing_col FROM items"})
	// Second view: should still succeed despite first failure
	current.Set("public.good_view", &model.View{Schema: "public", Name: "good_view", Definition: "SELECT id, label FROM items WHERE label = 'active'"})

	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.bad_view", &model.View{Schema: "public", Name: "bad_view", Definition: "SELECT missing_col FROM items"})
	desired.Set("public.good_view", &model.View{Schema: "public", Name: "good_view", Definition: "SELECT id, label FROM items WHERE label = 'active'"})

	pistachio.NormalizeDesiredViewDefs(ctx, conn, current, desired)

	// bad_view: definition unchanged (CREATE VIEW failed)
	bad, _ := desired.GetOk("public.bad_view")
	assert.Equal(t, "SELECT missing_col FROM items", bad.Definition)

	// good_view: definition normalized despite bad_view failing first
	good, _ := desired.GetOk("public.good_view")
	assert.Contains(t, good.Definition, "'active'::text")
}

func TestNormalizeDesiredViewDefs_SkipsNewViews(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	testutil.SetupDB(t, ctx, conn, "")

	current := orderedmap.New[string, *model.View]()
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	pistachio.NormalizeDesiredViewDefs(ctx, conn, current, desired)

	// New view not in current — definition should remain unchanged
	v, _ := desired.GetOk("public.v1")
	assert.Equal(t, "SELECT 1", v.Definition)
}
