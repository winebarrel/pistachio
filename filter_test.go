package pistachio_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/model"
)

func TestValidatePatterns(t *testing.T) {
	t.Run("valid patterns", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user*", "post?"}, Exclude: []string{"tmp_*"}}
		assert.NoError(t, o.ValidatePatterns())
	})

	t.Run("invalid include pattern", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"[invalid"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--include")
	})

	t.Run("invalid exclude pattern", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"[invalid"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--exclude")
	})

	t.Run("empty", func(t *testing.T) {
		o := &pistachio.FilterOptions{}
		assert.NoError(t, o.ValidatePatterns())
	})

	t.Run("valid regexp patterns", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{`/^posts_\d+$/`}, Exclude: []string{"//"}}
		assert.NoError(t, o.ValidatePatterns())
	})

	t.Run("invalid include regexp", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"/[invalid/"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--include")
	})

	t.Run("invalid exclude regexp", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"/*bad/"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--exclude")
	})
}

func TestFilterOptions_AfterApply_Valid(t *testing.T) {
	o := &pistachio.FilterOptions{Include: []string{"user*"}}
	assert.NoError(t, o.AfterApply())
}

func TestFilterOptions_AfterApply_Invalid(t *testing.T) {
	o := &pistachio.FilterOptions{Include: []string{"[bad"}}
	err := o.AfterApply()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--include")
}

func TestFilterOptions_AfterApply_TrimsWhitespace(t *testing.T) {
	o := &pistachio.FilterOptions{
		Include: []string{" user* ", "\tposts\n"},
		Exclude: []string{"  tmp_* "},
	}
	require.NoError(t, o.AfterApply())
	assert.Equal(t, []string{"user*", "posts"}, o.Include)
	assert.Equal(t, []string{"tmp_*"}, o.Exclude)
}

// Trimming runs before the pattern is classified, so a regexp survives the
// surrounding whitespace a shell or a config file leaves behind.
func TestFilterOptions_AfterApply_TrimsRegexp(t *testing.T) {
	o := &pistachio.FilterOptions{Include: []string{" /^users$/ "}}
	require.NoError(t, o.AfterApply())
	assert.Equal(t, []string{"/^users$/"}, o.Include)
	assert.True(t, o.MatchName("users"))
	assert.False(t, o.MatchName("posts"))
}

func TestMatchName(t *testing.T) {
	t.Run("no filters", func(t *testing.T) {
		o := &pistachio.FilterOptions{}
		assert.True(t, o.MatchName("users"))
	})

	t.Run("include match", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"users"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("include wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user*"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("user_roles"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("exclude match", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"posts"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("exclude wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"tmp_*"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("tmp_backup"))
	})

	t.Run("include and exclude", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user*"}, Exclude: []string{"user_tmp"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("user_roles"))
		assert.False(t, o.MatchName("user_tmp"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("multiple include patterns", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"users", "posts"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("posts"))
		assert.False(t, o.MatchName("orders"))
	})

	t.Run("question mark wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user?"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("user_roles"))
	})

	t.Run("include regexp", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{`/^posts_\d+$/`}}
		assert.True(t, o.MatchName("posts_1"))
		assert.True(t, o.MatchName("posts_2026"))
		assert.False(t, o.MatchName("posts_old"))
		assert.False(t, o.MatchName("users"))
	})

	t.Run("exclude regexp", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{`/^(tmp|scratch)_/`}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("tmp_backup"))
		assert.False(t, o.MatchName("scratch_1"))
	})

	t.Run("regexp is not anchored", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"/user/"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("superuser"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("regexp and wildcard together", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user*", `/^audit_(log|trail)$/`}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("audit_log"))
		assert.True(t, o.MatchName("audit_trail"))
		assert.False(t, o.MatchName("audit_other"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("empty regexp matches everything", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"//"}}
		assert.False(t, o.MatchName("users"))
	})

	t.Run("one slash is a wildcard, not a regexp", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"/user"}}
		assert.False(t, o.MatchName("users"))
		assert.True(t, o.MatchName("/user"))
	})

	t.Run("regexp metacharacter in a wildcard is literal", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"users+"}}
		assert.False(t, o.MatchName("users"))
		assert.True(t, o.MatchName("users+"))
	})

	t.Run("trailing slash alone is a wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user/"}}
		assert.False(t, o.MatchName("users"))
		assert.True(t, o.MatchName("user/"))
	})

	t.Run("a lone slash is a wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"/"}}
		assert.False(t, o.MatchName("users"))
		assert.True(t, o.MatchName("/"))
	})

	t.Run("slash inside a regexp", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"/^a/b$/"}}
		assert.True(t, o.MatchName("a/b"))
		assert.False(t, o.MatchName("ab"))
	})

	t.Run("exclude regexp beats include regexp", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"/^user/"}, Exclude: []string{"/_tmp$/"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("user_tmp"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("multiple exclude patterns of both forms", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"tmp_*", `/^posts_\d+$/`}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("tmp_backup"))
		assert.False(t, o.MatchName("posts_1"))
		assert.True(t, o.MatchName("posts_old"))
	})

	// A library caller can build FilterOptions without going through
	// AfterApply, so MatchName sees a pattern nothing has validated.
	t.Run("invalid regexp matches nothing", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"/[bad/"}}
		assert.False(t, o.MatchName("users"))
	})
}

func TestIsTypeEnabled_Disable(t *testing.T) {
	t.Run("disable table", func(t *testing.T) {
		f := &pistachio.FilterOptions{Disable: []string{"table"}}
		assert.False(t, f.IsTypeEnabled("table"))
		assert.True(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.True(t, f.IsTypeEnabled("domain"))
	})

	t.Run("disable multiple", func(t *testing.T) {
		f := &pistachio.FilterOptions{Disable: []string{"table", "view"}}
		assert.False(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.True(t, f.IsTypeEnabled("domain"))
	})

	t.Run("enable takes precedence over disable", func(t *testing.T) {
		f := &pistachio.FilterOptions{Enable: []string{"enum"}, Disable: []string{"table"}}
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.False(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
	})
}

func TestIsTypeEnabled_Enable(t *testing.T) {
	t.Run("empty (all enabled)", func(t *testing.T) {
		f := &pistachio.FilterOptions{}
		assert.True(t, f.IsTypeEnabled("table"))
		assert.True(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.True(t, f.IsTypeEnabled("domain"))
	})

	t.Run("only table", func(t *testing.T) {
		f := &pistachio.FilterOptions{Enable: []string{"table"}}
		assert.True(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
		assert.False(t, f.IsTypeEnabled("enum"))
		assert.False(t, f.IsTypeEnabled("domain"))
	})

	t.Run("multiple types", func(t *testing.T) {
		f := &pistachio.FilterOptions{Enable: []string{"table", "enum"}}
		assert.True(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.False(t, f.IsTypeEnabled("domain"))
	})
}

func partitionChildTable(name, parent, bound string) *model.Table {
	return &model.Table{Schema: "public", Name: name, PartitionOf: &parent, PartitionBound: &bound}
}

func TestFilterTables_SkipPartitionChild(t *testing.T) {
	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.events", &model.Table{Schema: "public", Name: "events", Partitioned: true})
	tables.Set("public.events_2024", partitionChildTable("events_2024", "public.events", "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"))
	// A middle level is both partitioned and a child, and goes with the rest.
	sub := partitionChildTable("events_2025", "public.events", "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")
	sub.Partitioned = true
	tables.Set("public.events_2025", sub)
	// An INHERITS child carries no bound, so it is not a partition child.
	parent := "public.events"
	tables.Set("public.events_old", &model.Table{Schema: "public", Name: "events_old", PartitionOf: &parent})
	tables.Set("public.users", &model.Table{Schema: "public", Name: "users"})

	t.Run("off", func(t *testing.T) {
		f := &pistachio.FilterOptions{}
		got := pistachio.FilterTables(f, tables)
		assert.Equal(t, []string{"public.events", "public.events_2024", "public.events_2025", "public.events_old", "public.users"}, got.CollectKeys())
	})

	t.Run("on", func(t *testing.T) {
		f := &pistachio.FilterOptions{SkipPartitionChild: true}
		got := pistachio.FilterTables(f, tables)
		assert.Equal(t, []string{"public.events", "public.events_old", "public.users"}, got.CollectKeys())
	})

	t.Run("with exclude", func(t *testing.T) {
		f := &pistachio.FilterOptions{SkipPartitionChild: true, Exclude: []string{"users"}}
		got := pistachio.FilterTables(f, tables)
		assert.Equal(t, []string{"public.events", "public.events_old"}, got.CollectKeys())
	})

	t.Run("with disabled table type", func(t *testing.T) {
		f := &pistachio.FilterOptions{SkipPartitionChild: true, Disable: []string{"table"}}
		assert.Equal(t, 0, pistachio.FilterTables(f, tables).Len())
	})
}
