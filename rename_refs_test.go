package pistachio

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

const renameRefsInitSchema = `
CREATE TYPE renschema.mood AS ENUM ('ok', 'sad');
CREATE SEQUENCE renschema.counter;
CREATE TABLE renschema.users (
    id integer DEFAULT nextval('renschema.counter') NOT NULL,
    m renschema.mood,
    ms renschema.mood[],
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW renschema.user_moods AS SELECT id, m FROM renschema.users;
`

// A type or sequence outside the search_path reaches the diff qualified, and
// on the path bare. Either way the rename alone is planned, and the apply
// succeeds although a view reads the retyped column.
func TestPlan_RenameReferencesOutsidePublic(t *testing.T) {
	for _, tt := range []struct {
		name       string
		searchPath *string
		counter    string
	}{
		{name: "qualified", counter: "renschema.user_counter"},
		{name: "on search_path", searchPath: new("renschema"), counter: "user_counter"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			connString := setupSchemaDB(t, ctx, "renschema", renameRefsInitSchema)

			desiredFile := filepath.Join(t.TempDir(), "desired.sql")
			require.NoError(t, os.WriteFile(desiredFile, []byte(`
-- pista:renamed-from renschema.mood
CREATE TYPE renschema.feeling AS ENUM ('ok', 'sad');
-- pista:renamed-from renschema.counter
CREATE SEQUENCE renschema.user_counter;
CREATE TABLE renschema.users (
    id integer DEFAULT nextval('`+tt.counter+`'::regclass) NOT NULL,
    m renschema.feeling,
    ms renschema.feeling[],
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW renschema.user_moods AS SELECT id, m FROM renschema.users;
`), 0o600))

			client := NewClient(&Options{
				ConnString: connString,
				Schemas:    []string{"renschema"},
				SearchPath: tt.searchPath,
			})

			got, err := client.Plan(ctx, &PlanOptions{Files: []string{desiredFile}})
			require.NoError(t, err)
			assert.Equal(t, "ALTER TYPE renschema.mood RENAME TO feeling;\nALTER SEQUENCE renschema.counter RENAME TO user_counter;", got.SQL)

			_, err = client.Apply(ctx, &ApplyOptions{Files: []string{desiredFile}}, io.Discard)
			require.NoError(t, err)

			got, err = client.Plan(ctx, &PlanOptions{Files: []string{desiredFile}})
			require.NoError(t, err)
			assert.Empty(t, got.SQL)
		})
	}
}

func TestSplitSearchPath(t *testing.T) {
	assert.Equal(t, []string{"public"}, splitSearchPath("public"))
	assert.Equal(t, []string{"$user", "public"}, splitSearchPath(`"$user", public`))
	assert.Equal(t, []string{"app", "My Schema", `a"b`}, splitSearchPath(`app,"My Schema" , "a""b"`))
	assert.Empty(t, splitSearchPath(""))
}

func TestReferenceRenames_RenameType(t *testing.T) {
	r := &referenceRenames{
		types: []objectRename{
			{schema: "public", from: "status", to: "user_status"},
			{schema: "app", from: "Mood", to: "Feeling"},
		},
		searchPath: []string{"public"},
	}

	for _, tt := range []struct {
		in, want string
		changed  bool
	}{
		{"status", "user_status", true},
		{"public.status", "public.user_status", true},
		{"status[]", "user_status[]", true},
		{"status[][]", "user_status[][]", true},
		{`app."Mood"`, `app."Feeling"`, true},
		{`app."Mood"[]`, `app."Feeling"[]`, true},
		// app is not on the path, so a bare name is some other type.
		{`"Mood"`, `"Mood"`, false},
		{"app.status", "app.status", false},
		{"integer", "integer", false},
		{"character varying(10)[]", "character varying(10)[]", false},
	} {
		got, changed := r.renameType(tt.in)
		assert.Equal(t, tt.want, got, tt.in)
		assert.Equal(t, tt.changed, changed, tt.in)
	}
}

func TestReferenceRenames_RenameDefault(t *testing.T) {
	r := &referenceRenames{
		sequences: []objectRename{
			{schema: "public", from: "seq", to: "id_seq"},
			{schema: "app", from: "It's", to: "Its"},
		},
		searchPath: []string{"public"},
	}

	for _, tt := range []struct {
		in, want string
		changed  bool
	}{
		{"nextval('seq'::regclass)", "nextval('id_seq'::regclass)", true},
		{"nextval('public.seq'::regclass)", "nextval('public.id_seq'::regclass)", true},
		{"nextval('seq')", "nextval('id_seq')", true},
		{`nextval('app."It''s"'::regclass)`, `nextval('app."Its"'::regclass)`, true},
		{"(nextval('seq'::regclass) * 2)", "(nextval('id_seq'::regclass) * 2)", true},
		{"nextval('other'::regclass)", "nextval('other'::regclass)", false},
		{"nextval('app.seq'::regclass)", "nextval('app.seq'::regclass)", false},
		{"'seq'::text", "'seq'::text", false},
	} {
		got, changed := r.renameDefault(&tt.in)
		assert.Equal(t, tt.want, *got, tt.in)
		assert.Equal(t, tt.changed, changed, tt.in)
	}

	got, changed := r.renameDefault(nil)
	assert.Nil(t, got)
	assert.False(t, changed)

	def := "nextval('seq'::regclass)"
	got, changed = (&referenceRenames{types: r.sequences, searchPath: r.searchPath}).renameDefault(&def)
	assert.Same(t, &def, got)
	assert.False(t, changed)
}

// The current side is shared with orderStatements, so the renames go into
// copies, and an object with nothing to rename is not copied.
func TestReferenceRenames_ApplyLeavesInputAlone(t *testing.T) {
	r := &referenceRenames{
		types:      []objectRename{{schema: "public", from: "status", to: "user_status"}},
		sequences:  []objectRename{{schema: "public", from: "seq", to: "id_seq"}},
		searchPath: []string{"public"},
	}

	seqDefault := "nextval('seq'::regclass)"
	users := &model.Table{Schema: "public", Name: "users", Columns: orderedmap.New[string, *model.Column]()}
	users.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", Default: &seqDefault})
	users.Columns.Set("s", &model.Column{Name: "s", TypeName: "status"})
	users.Columns.Set("n", &model.Column{Name: "n", TypeName: "text"})
	other := &model.Table{Schema: "public", Name: "other", Columns: orderedmap.New[string, *model.Column]()}
	other.Columns.Set("n", &model.Column{Name: "n", TypeName: "text"})
	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.users", users)
	tables.Set("public.other", other)

	out := r.applyTables(tables)
	assert.Equal(t, "user_status", out.Get("public.users").Columns.Get("s").TypeName)
	assert.Equal(t, "nextval('id_seq'::regclass)", *out.Get("public.users").Columns.Get("id").Default)
	assert.Same(t, users.Columns.Get("n"), out.Get("public.users").Columns.Get("n"))
	assert.Same(t, other, out.Get("public.other"))
	assert.Equal(t, "status", users.Columns.Get("s").TypeName)
	assert.Equal(t, "nextval('seq'::regclass)", *users.Columns.Get("id").Default)

	domDefault := "nextval('seq'::regclass)"
	dom := &model.Domain{Schema: "public", Name: "d", BaseType: "status", Default: &domDefault}
	plain := &model.Domain{Schema: "public", Name: "p", BaseType: "integer"}
	domains := orderedmap.New[string, *model.Domain]()
	domains.Set("public.d", dom)
	domains.Set("public.p", plain)

	outDomains := r.applyDomains(domains)
	assert.Equal(t, "user_status", outDomains.Get("public.d").BaseType)
	assert.Equal(t, "nextval('id_seq'::regclass)", *outDomains.Get("public.d").Default)
	assert.Same(t, plain, outDomains.Get("public.p"))
	assert.Equal(t, "status", dom.BaseType)
	assert.Equal(t, "nextval('seq'::regclass)", *dom.Default)

	attr := &model.CompositeAttribute{Name: "a", TypeName: "status[]"}
	keep := &model.CompositeAttribute{Name: "b", TypeName: "text"}
	ct := &model.CompositeType{Schema: "public", Name: "pair", Attributes: []*model.CompositeAttribute{attr, keep}}
	plainCT := &model.CompositeType{Schema: "public", Name: "plain", Attributes: []*model.CompositeAttribute{keep}}
	cts := orderedmap.New[string, *model.CompositeType]()
	cts.Set("public.pair", ct)
	cts.Set("public.plain", plainCT)

	outCTs := r.applyCompositeTypes(cts)
	assert.Equal(t, "user_status[]", outCTs.Get("public.pair").Attributes[0].TypeName)
	assert.Same(t, keep, outCTs.Get("public.pair").Attributes[1])
	assert.Same(t, plainCT, outCTs.Get("public.plain"))
	assert.Equal(t, "status[]", attr.TypeName)
	assert.Same(t, attr, ct.Attributes[0])
}

func TestCollectRenames(t *testing.T) {
	get := func(e *model.Enum) (string, string, *string) { return e.Schema, e.Name, e.RenameFrom }
	from := func(s string) *string { return &s }

	current := orderedmap.New[string, *model.Enum]()
	current.Set("public.a", &model.Enum{Schema: "public", Name: "a"})
	current.Set("public.b", &model.Enum{Schema: "public", Name: "b"})
	current.Set("public.taken", &model.Enum{Schema: "public", Name: "taken"})

	desired := orderedmap.New[string, *model.Enum]()
	// Renamed.
	desired.Set("public.a2", &model.Enum{Schema: "public", Name: "a2", RenameFrom: from("public.a")})
	// Already applied: the old name is gone.
	desired.Set("public.b", &model.Enum{Schema: "public", Name: "b", RenameFrom: from("public.gone")})
	// The new name is in use; DiffEnums reports it.
	desired.Set("public.taken", &model.Enum{Schema: "public", Name: "taken", RenameFrom: from("public.b")})
	// Renamed from itself.
	desired.Set("public.self", &model.Enum{Schema: "public", Name: "self", RenameFrom: from("public.self")})
	// No directive.
	desired.Set("public.plain", &model.Enum{Schema: "public", Name: "plain"})

	assert.Equal(t, []objectRename{{schema: "public", from: "a", to: "a2"}}, collectRenames(current, desired, get))
}
