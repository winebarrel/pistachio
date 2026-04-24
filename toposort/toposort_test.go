package toposort_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/toposort"
)

func TestGraph_Sort_Simple(t *testing.T) {
	g := toposort.NewGraph()
	g.AddNode("a")
	g.AddNode("b")
	g.AddNode("c")
	g.AddEdge("c", "b") // c depends on b
	g.AddEdge("b", "a") // b depends on a

	result, err := g.Sort()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestGraph_Sort_Independent(t *testing.T) {
	g := toposort.NewGraph()
	g.AddNode("c")
	g.AddNode("a")
	g.AddNode("b")

	result, err := g.Sort()
	require.NoError(t, err)
	// Alphabetical order for independent nodes
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestGraph_Sort_Cycle(t *testing.T) {
	g := toposort.NewGraph()
	g.AddEdge("a", "b")
	g.AddEdge("b", "a")

	_, err := g.Sort()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle detected")
}

func TestGraph_Sort_Diamond(t *testing.T) {
	g := toposort.NewGraph()
	g.AddEdge("d", "b") // d depends on b
	g.AddEdge("d", "c") // d depends on c
	g.AddEdge("b", "a") // b depends on a
	g.AddEdge("c", "a") // c depends on a

	result, err := g.Sort()
	require.NoError(t, err)
	assert.Equal(t, "a", result[0])
	assert.Equal(t, "d", result[3])
	// b and c can be in either order, but alphabetical
	assert.Equal(t, []string{"a", "b", "c", "d"}, result)
}

func TestExtractDeps_EnumDomainTableView(t *testing.T) {
	sql := `
		CREATE TYPE public.status AS ENUM ('active', 'inactive');
		CREATE DOMAIN public.user_status AS public.status;
		CREATE TABLE public.users (
			id integer NOT NULL,
			status public.user_status,
			CONSTRAINT users_pkey PRIMARY KEY (id)
		);
		CREATE VIEW public.active_users AS SELECT id FROM public.users WHERE status = 'active';
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 4)

	// enum: no deps
	assert.Equal(t, "public.status", stmts[0].Name)
	assert.Empty(t, stmts[0].Deps)

	// domain: depends on enum
	assert.Equal(t, "public.user_status", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.status")

	// table: depends on domain
	assert.Equal(t, "public.users", stmts[2].Name)
	assert.Contains(t, stmts[2].Deps, "public.user_status")

	// view: depends on table
	assert.Equal(t, "public.active_users", stmts[3].Name)
	assert.Contains(t, stmts[3].Deps, "public.users")
}

func TestExtractDeps_ForeignKey(t *testing.T) {
	sql := `
		CREATE TABLE public.users (
			id integer NOT NULL,
			CONSTRAINT users_pkey PRIMARY KEY (id)
		);
		CREATE TABLE public.posts (
			id integer NOT NULL,
			user_id integer NOT NULL,
			CONSTRAINT posts_pkey PRIMARY KEY (id),
			CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id)
		);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.users", stmts[0].Name)
	assert.Empty(t, stmts[0].Deps)

	assert.Equal(t, "public.posts", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.users")
}

func TestExtractDeps_ViewWithJoin(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL);
		CREATE TABLE public.posts (id integer NOT NULL, user_id integer NOT NULL);
		CREATE VIEW public.user_posts AS
			SELECT u.id, p.id AS post_id
			FROM public.users u
			JOIN public.posts p ON u.id = p.user_id;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 3)

	assert.Equal(t, "public.user_posts", stmts[2].Name)
	assert.Contains(t, stmts[2].Deps, "public.users")
	assert.Contains(t, stmts[2].Deps, "public.posts")
}

func TestExtractDeps_IgnoresBuiltinTypes(t *testing.T) {
	sql := `
		CREATE TABLE public.users (
			id integer NOT NULL,
			name text NOT NULL,
			created_at timestamp NOT NULL
		);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Empty(t, stmts[0].Deps)
}

func TestExtractDeps_IgnoresExternalRefs(t *testing.T) {
	// Reference to a table not defined in this SQL should not be a dep
	sql := `
		CREATE TABLE public.posts (
			id integer NOT NULL,
			user_id integer NOT NULL,
			CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id)
		);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Empty(t, stmts[0].Deps)
}

func TestSortSQL_BasicOrder(t *testing.T) {
	// Deliberately in reverse dependency order
	sql := `
		CREATE VIEW public.active_users AS SELECT id FROM public.users;
		CREATE TABLE public.users (
			id integer NOT NULL,
			status public.status,
			CONSTRAINT users_pkey PRIMARY KEY (id)
		);
		CREATE TYPE public.status AS ENUM ('active', 'inactive');
	`

	sorted, err := toposort.SortSQL(sql)
	require.NoError(t, err)
	require.Len(t, sorted, 3)

	// enum first, then table, then view
	assert.Contains(t, sorted[0], "CREATE TYPE")
	assert.Contains(t, sorted[1], "CREATE TABLE")
	assert.Contains(t, sorted[2], "SELECT")
}

func TestSortSQL_ComplexDeps(t *testing.T) {
	sql := `
		CREATE VIEW public.active_users AS SELECT id, name FROM public.users WHERE status = 'active';
		CREATE TABLE public.posts (
			id integer NOT NULL,
			user_id integer NOT NULL,
			CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id)
		);
		CREATE TABLE public.users (
			id integer NOT NULL,
			name text NOT NULL,
			status public.status,
			CONSTRAINT users_pkey PRIMARY KEY (id)
		);
		CREATE DOMAIN public.user_status AS public.status;
		CREATE TYPE public.status AS ENUM ('active', 'inactive');
	`

	sorted, err := toposort.SortSQL(sql)
	require.NoError(t, err)
	require.Len(t, sorted, 5)

	// Build index for checking relative order
	idx := make(map[string]int)
	for i, s := range sorted {
		switch {
		case strings.Contains(s, "CREATE TYPE"):
			idx["enum"] = i
		case strings.Contains(s, "CREATE DOMAIN"):
			idx["domain"] = i
		case strings.Contains(s, "CREATE TABLE") && strings.Contains(s, "public.posts"):
			idx["posts"] = i
		case strings.Contains(s, "CREATE TABLE") && strings.Contains(s, "public.users"):
			idx["users"] = i
		case strings.Contains(s, "SELECT"):
			idx["view"] = i
		}
	}

	assert.Less(t, idx["enum"], idx["domain"], "enum before domain")
	assert.Less(t, idx["enum"], idx["users"], "enum before users (type dep)")
	assert.Less(t, idx["users"], idx["posts"], "users before posts (FK dep)")
	assert.Less(t, idx["users"], idx["view"], "users before view")
}

func TestSortSQL_CyclicError(t *testing.T) {
	// This creates a cycle through FKs: a → b → a
	sql := `
		CREATE TABLE public.a (
			id integer NOT NULL,
			b_id integer NOT NULL,
			CONSTRAINT a_b_fk FOREIGN KEY (b_id) REFERENCES public.b(id)
		);
		CREATE TABLE public.b (
			id integer NOT NULL,
			a_id integer NOT NULL,
			CONSTRAINT b_a_fk FOREIGN KEY (a_id) REFERENCES public.a(id)
		);
	`

	_, err := toposort.SortSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle detected")
}

func TestSortSQL_Empty(t *testing.T) {
	sorted, err := toposort.SortSQL("")
	require.NoError(t, err)
	assert.Nil(t, sorted)
}

func TestSortSQL_ParseError(t *testing.T) {
	_, err := toposort.SortSQL("NOT VALID SQL {{{}}")
	require.Error(t, err)
}

func TestExtractDeps_ParseError(t *testing.T) {
	_, err := toposort.ExtractDeps("NOT VALID SQL {{{}}")
	require.Error(t, err)
}

func TestExtractDeps_ViewWithUnion(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL);
		CREATE TABLE public.admins (id integer NOT NULL);
		CREATE VIEW public.all_people AS
			SELECT id FROM public.users
			UNION
			SELECT id FROM public.admins;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 3)

	assert.Equal(t, "public.all_people", stmts[2].Name)
	assert.Contains(t, stmts[2].Deps, "public.users")
	assert.Contains(t, stmts[2].Deps, "public.admins")
}

func TestExtractDeps_ViewWithSubselect(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL, active boolean);
		CREATE VIEW public.active_count AS
			SELECT count(*) FROM (SELECT id FROM public.users WHERE active) sub;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.active_count", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.users")
}

func TestExtractDeps_InlineForeignKey(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL);
		CREATE TABLE public.posts (
			id integer NOT NULL,
			user_id integer REFERENCES public.users(id)
		);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.posts", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.users")
}

func TestExtractDeps_ArrayType(t *testing.T) {
	sql := `
		CREATE TYPE public.status AS ENUM ('a', 'b');
		CREATE TABLE public.users (
			id integer NOT NULL,
			statuses public.status[]
		);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	// The parser should handle the array type reference
	assert.Equal(t, "public.users", stmts[1].Name)
}

func TestExtractDeps_PartitionParent(t *testing.T) {
	sql := `
		CREATE TABLE public.events (id integer, created_at date) PARTITION BY RANGE (created_at);
		CREATE TABLE public.events_2024 PARTITION OF public.events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.events_2024", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.events")
}

func TestExtractDeps_SkipsNonCreateStatements(t *testing.T) {
	// ALTER TABLE and other non-CREATE statements should be ignored
	sql := `
		CREATE TABLE public.users (id integer NOT NULL);
		ALTER TABLE public.users ADD COLUMN name text;
		CREATE TABLE public.posts (id integer NOT NULL);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	// Only CREATE statements are extracted
	require.Len(t, stmts, 2)
	assert.Equal(t, "public.users", stmts[0].Name)
	assert.Equal(t, "public.posts", stmts[1].Name)
}

func TestExtractDeps_UnqualifiedEnumType(t *testing.T) {
	// Enum type referenced without schema prefix in column type
	sql := `
		CREATE TYPE public.status AS ENUM ('a', 'b');
		CREATE TABLE public.users (
			id integer NOT NULL,
			status status
		);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.users", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.status")
}

func TestExtractDeps_UnqualifiedDomainBaseType(t *testing.T) {
	// Domain with unqualified base type
	sql := `
		CREATE TYPE public.status AS ENUM ('a', 'b');
		CREATE DOMAIN public.user_status AS status;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.user_status", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.status")
}

func TestExtractDeps_TableWithNoColumns(t *testing.T) {
	// Edge case: partition child with no explicit columns
	sql := `
		CREATE TABLE public.parent (id integer) PARTITION BY RANGE (id);
		CREATE TABLE public.child PARTITION OF public.parent FOR VALUES FROM (1) TO (100);
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.child", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.parent")
}

func TestExtractDeps_ViewWithSchemalessTable(t *testing.T) {
	// View referencing a table without schema prefix
	sql := `
		CREATE TABLE public.users (id integer NOT NULL);
		CREATE VIEW public.v AS SELECT id FROM users;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.v", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.users")
}

func TestExtractDeps_NonPublicSchema(t *testing.T) {
	sql := `
		CREATE TYPE myapp.status AS ENUM ('a', 'b');
		CREATE TABLE myapp.users (
			id integer NOT NULL,
			status status
		);
	`

	stmts, err := toposort.ExtractDeps(sql, "myapp")
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "myapp.status", stmts[0].Name)
	assert.Equal(t, "myapp.users", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "myapp.status")
}

func TestSortSQL_NonPublicSchema(t *testing.T) {
	sql := `
		CREATE TABLE myapp.posts (
			id integer NOT NULL,
			user_id integer NOT NULL,
			CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES myapp.users(id)
		);
		CREATE TABLE myapp.users (id integer NOT NULL);
	`

	sorted, err := toposort.SortSQL(sql, "myapp")
	require.NoError(t, err)
	require.Len(t, sorted, 2)
	assert.Contains(t, sorted[0], "myapp.users")
	assert.Contains(t, sorted[1], "myapp.posts")
}

func TestExtractDeps_ViewWithWhereSubquery(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL, active boolean);
		CREATE TABLE public.config (key text, val integer);
		CREATE VIEW public.v AS SELECT id FROM public.users WHERE id > (SELECT val FROM public.config WHERE key = 'min_id');
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 3)

	assert.Equal(t, "public.v", stmts[2].Name)
	assert.Contains(t, stmts[2].Deps, "public.users")
	assert.Contains(t, stmts[2].Deps, "public.config")
}

func TestExtractDeps_ViewWithCTE(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL);
		CREATE VIEW public.v AS WITH active AS (SELECT id FROM public.users) SELECT id FROM active;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	assert.Equal(t, "public.v", stmts[1].Name)
	assert.Contains(t, stmts[1].Deps, "public.users")
}

func TestExtractDeps_JoinWithSubqueryInQuals(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL);
		CREATE TABLE public.config (key text, val integer);
		CREATE TABLE public.posts (id integer NOT NULL, user_id integer);
		CREATE VIEW public.v AS
			SELECT u.id FROM public.users u
			JOIN public.posts p ON u.id = p.user_id
				AND p.id > (SELECT val FROM public.config WHERE key = 'min_post');
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 4)

	assert.Equal(t, "public.v", stmts[3].Name)
	assert.Contains(t, stmts[3].Deps, "public.users")
	assert.Contains(t, stmts[3].Deps, "public.posts")
	assert.Contains(t, stmts[3].Deps, "public.config")
}

func TestExtractDeps_ViewWithCoalesceSubquery(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL, name text);
		CREATE TABLE public.defaults (val text);
		CREATE VIEW public.v AS SELECT id, COALESCE(name, (SELECT val FROM public.defaults LIMIT 1)) FROM public.users;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 3)

	assert.Equal(t, "public.v", stmts[2].Name)
	assert.Contains(t, stmts[2].Deps, "public.users")
	assert.Contains(t, stmts[2].Deps, "public.defaults")
}

func TestExtractDeps_ViewWithCaseSubquery(t *testing.T) {
	sql := `
		CREATE TABLE public.users (id integer NOT NULL, active boolean);
		CREATE TABLE public.config (val text);
		CREATE VIEW public.v AS SELECT id, CASE WHEN active THEN (SELECT val FROM public.config LIMIT 1) ELSE 'none' END FROM public.users;
	`

	stmts, err := toposort.ExtractDeps(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 3)

	assert.Equal(t, "public.v", stmts[2].Name)
	assert.Contains(t, stmts[2].Deps, "public.users")
	assert.Contains(t, stmts[2].Deps, "public.config")
}
