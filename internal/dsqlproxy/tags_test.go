package dsqlproxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTagsQuery(t *testing.T) {
	tg := newTags()
	tg.query([]string{"", "CREATE INDEX"})
	assert.Empty(t, tg.complete())
	assert.Equal(t, "CREATE INDEX", tg.complete())
	assert.Empty(t, tg.complete())
	tg.ready()
}

func TestTagsExtended(t *testing.T) {
	tg := newTags()
	tg.parse("s1", []string{"CREATE INDEX"})
	tg.parse("", []string{""})
	tg.bind("p", "s1")
	tg.execute("p")
	tg.bind("", "")
	tg.execute("")
	tg.sync()
	assert.Equal(t, "CREATE INDEX", tg.complete())
	assert.Empty(t, tg.complete())
	tg.ready()
}

func TestTagsErrorSkipsBatch(t *testing.T) {
	tg := newTags()
	// The first statement fails, so the second never completes.
	tg.query([]string{"", "ALTER TABLE"})
	tg.ready()
	tg.query([]string{"CREATE INDEX"})
	assert.Equal(t, "CREATE INDEX", tg.complete())
}

func TestTagsPipelined(t *testing.T) {
	tg := newTags()
	tg.parse("", []string{"ALTER TABLE"})
	tg.bind("", "")
	tg.execute("")
	tg.sync()
	tg.query([]string{"CREATE INDEX"})
	assert.Equal(t, "ALTER TABLE", tg.complete())
	tg.ready()
	assert.Equal(t, "CREATE INDEX", tg.complete())
	tg.ready()
}
