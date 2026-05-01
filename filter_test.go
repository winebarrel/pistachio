package pistachio_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
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
