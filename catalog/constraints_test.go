package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestListConstraintsByTable(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("primary key", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		assert.Equal(t, 1, tbl.Constraints.Len())

		con, ok := tbl.Constraints.GetOk("users_pkey")
		require.True(t, ok)
		assert.True(t, con.Type.IsPrimaryKeyConstraint())
		assert.Equal(t, []string{"id"}, con.Columns)
		assert.True(t, con.Validated)
	})

	t.Run("unique", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				email text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id),
				CONSTRAINT users_email_key UNIQUE (email)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		assert.Equal(t, 2, tbl.Constraints.Len())

		pk, ok := tbl.Constraints.GetOk("users_pkey")
		require.True(t, ok)
		assert.Equal(t, []string{"id"}, pk.Columns)

		con, ok := tbl.Constraints.GetOk("users_email_key")
		require.True(t, ok)
		assert.True(t, con.Type.IsUniqueConstraint())
		assert.Equal(t, []string{"email"}, con.Columns)
	})

	t.Run("check", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.products (
				id integer NOT NULL,
				price numeric(10,2) NOT NULL,
				CONSTRAINT products_pkey PRIMARY KEY (id),
				CONSTRAINT products_price_check CHECK (price > 0)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.products")
		con, ok := tbl.Constraints.GetOk("products_price_check")
		require.True(t, ok)
		assert.True(t, con.Type.IsCheckConstraint())
		assert.Contains(t, con.Definition, "price")
		// Single-column CHECK: only that column appears in Columns.
		assert.Equal(t, []string{"price"}, con.Columns)
	})

	t.Run("foreign key", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.posts (
				id integer NOT NULL,
				user_id integer NOT NULL,
				CONSTRAINT posts_pkey PRIMARY KEY (id)
			);
			ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.posts")
		assert.Equal(t, 1, tbl.ForeignKeys.Len())

		fk, ok := tbl.ForeignKeys.GetOk("posts_user_id_fkey")
		require.True(t, ok)
		assert.True(t, fk.Type.IsForeignKeyConstraint())
		assert.Equal(t, "posts", fk.Table)
		assert.Equal(t, "public", fk.Schema)
		require.NotNil(t, fk.RefSchema)
		assert.Equal(t, "public", *fk.RefSchema)
		require.NotNil(t, fk.RefTable)
		assert.Equal(t, "users", *fk.RefTable)
		assert.Equal(t, []string{"user_id"}, fk.Columns)

		// PK on the same table must report only its own column. This is the
		// regression scenario for the column_t CTE bug; the previous CTE
		// grouped by attrelid, so posts_pkey would have inherited {id, user_id}
		// from the FK-row's conkey.
		pk, ok := tbl.Constraints.GetOk("posts_pkey")
		require.True(t, ok)
		assert.Equal(t, []string{"id"}, pk.Columns)
	})

	t.Run("foreign key with actions", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.posts (
				id integer NOT NULL,
				user_id integer NOT NULL,
				CONSTRAINT posts_pkey PRIMARY KEY (id)
			);
			ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey
				FOREIGN KEY (user_id) REFERENCES users(id)
				ON UPDATE CASCADE ON DELETE SET NULL;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.posts")
		fk, ok := tbl.ForeignKeys.GetOk("posts_user_id_fkey")
		require.True(t, ok)
		assert.Contains(t, fk.Definition, "ON UPDATE CASCADE")
		assert.Contains(t, fk.Definition, "ON DELETE SET NULL")
	})

	t.Run("deferrable constraint", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.posts (
				id integer NOT NULL,
				user_id integer NOT NULL,
				CONSTRAINT posts_pkey PRIMARY KEY (id)
			);
			ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey
				FOREIGN KEY (user_id) REFERENCES users(id)
				DEFERRABLE INITIALLY DEFERRED;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.posts")
		fk, ok := tbl.ForeignKeys.GetOk("posts_user_id_fkey")
		require.True(t, ok)
		assert.True(t, fk.Deferrable)
		assert.True(t, fk.Deferred)
	})

	// Regression: prior CTE grouped by attrelid, so every constraint on the
	// same table received the union of all conkey columns (and on PG18 the
	// contype='n' rows added duplicates). This test pins each constraint to
	// only its own columns. If the CTE is ever re-broken, these Equal
	// assertions will fail with the "leaked from sibling" array.
	t.Run("multiple constraints isolated columns", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.t (
				id integer NOT NULL,
				email text NOT NULL,
				age integer,
				code text,
				CONSTRAINT t_pkey PRIMARY KEY (id),
				CONSTRAINT t_email_key UNIQUE (email),
				CONSTRAINT t_age_check CHECK (age > 0),
				CONSTRAINT t_code_key UNIQUE (code)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.t")

		want := map[string][]string{
			"t_pkey":      {"id"},
			"t_email_key": {"email"},
			"t_age_check": {"age"},
			"t_code_key":  {"code"},
		}
		for name, cols := range want {
			con, ok := tbl.Constraints.GetOk(name)
			require.Truef(t, ok, "constraint %s missing", name)
			assert.Equalf(t, cols, con.Columns, "constraint %s columns", name)
		}
	})

	// Regression: with a composite PK on (b, a), the CTE must aggregate in
	// conkey order (b then a). A sibling UNIQUE on (c) is included so a
	// broken attrelid-grouped CTE; which would mix c into the PK's array;
	// is caught here too, not just by the cross-contamination test above.
	t.Run("composite primary key preserves conkey order", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.t (
				a integer NOT NULL,
				b integer NOT NULL,
				c integer NOT NULL,
				CONSTRAINT t_pkey PRIMARY KEY (b, a),
				CONSTRAINT t_c_key UNIQUE (c)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.t")
		pk, ok := tbl.Constraints.GetOk("t_pkey")
		require.True(t, ok)
		assert.Equal(t, []string{"b", "a"}, pk.Columns)

		uniq, ok := tbl.Constraints.GetOk("t_c_key")
		require.True(t, ok)
		assert.Equal(t, []string{"c"}, uniq.Columns)
	})

	// Regression: composite UNIQUE preserves declaration order in conkey, and
	// a composite FK preserves declaration order on the local side.
	t.Run("composite unique and composite foreign key preserve order", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.parent (
				a integer NOT NULL,
				b integer NOT NULL,
				CONSTRAINT parent_pkey PRIMARY KEY (a, b)
			);
			CREATE TABLE public.child (
				x integer NOT NULL,
				y integer NOT NULL,
				z integer NOT NULL,
				CONSTRAINT child_pkey PRIMARY KEY (x),
				CONSTRAINT child_yz_key UNIQUE (z, y)
			);
			ALTER TABLE ONLY public.child ADD CONSTRAINT child_parent_fkey
				FOREIGN KEY (z, y) REFERENCES public.parent(a, b);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		child := tables.Get("public.child")

		uniq, ok := child.Constraints.GetOk("child_yz_key")
		require.True(t, ok)
		assert.Equal(t, []string{"z", "y"}, uniq.Columns)

		fk, ok := child.ForeignKeys.GetOk("child_parent_fkey")
		require.True(t, ok)
		assert.Equal(t, []string{"z", "y"}, fk.Columns)
	})

	// CHECK constraints with multiple column references must aggregate every
	// referenced column in conkey order. The previous attrelid-grouped CTE
	// would have produced the same union for any other constraint on the
	// table, masking ordering or selection bugs in the multi-column case.
	t.Run("multi-column CHECK preserves all referenced columns", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.t (
				id integer NOT NULL,
				lo integer NOT NULL,
				hi integer NOT NULL,
				CONSTRAINT t_pkey PRIMARY KEY (id),
				CONSTRAINT t_range_check CHECK (hi > lo)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.t")
		chk, ok := tbl.Constraints.GetOk("t_range_check")
		require.True(t, ok)
		assert.True(t, chk.Type.IsCheckConstraint())
		// PG stores CHECK conkey in expression-reference order, so
		// `CHECK (hi > lo)` yields conkey={hi.attnum, lo.attnum}.
		assert.Equal(t, []string{"hi", "lo"}, chk.Columns)
	})

	// CHECK constraints whose expression references no columns (e.g.
	// a constant or a function call) have conkey=NULL. The LEFT JOIN
	// to column_t therefore produces no row, and Constraint.Columns
	// must come back empty rather than picking up a sibling constraint's
	// array (which the prior attrelid-grouped CTE would have done).
	t.Run("CHECK with no column refs has empty Columns", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.t (
				id integer NOT NULL,
				email text NOT NULL,
				CONSTRAINT t_pkey PRIMARY KEY (id),
				CONSTRAINT t_constant_check CHECK (1 > 0)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.t")
		chk, ok := tbl.Constraints.GetOk("t_constant_check")
		require.True(t, ok)
		assert.True(t, chk.Type.IsCheckConstraint())
		assert.Empty(t, chk.Columns)
	})

	// Regression: exclusion constraints populate conkey too, and must not
	// leak column entries to sibling constraints on the same table.
	t.Run("exclusion constraint isolated columns", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE EXTENSION IF NOT EXISTS btree_gist;
			CREATE TABLE public.bookings (
				id integer NOT NULL,
				room integer NOT NULL,
				during tstzrange NOT NULL,
				CONSTRAINT bookings_pkey PRIMARY KEY (id),
				CONSTRAINT bookings_no_overlap EXCLUDE USING gist (room WITH =, during WITH &&)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.bookings")

		pk, ok := tbl.Constraints.GetOk("bookings_pkey")
		require.True(t, ok)
		assert.Equal(t, []string{"id"}, pk.Columns)

		excl, ok := tbl.Constraints.GetOk("bookings_no_overlap")
		require.True(t, ok)
		assert.True(t, excl.Type.IsExclusionConstraint())
		assert.Equal(t, []string{"room", "during"}, excl.Columns)
	})
}
