package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqualPtr(t *testing.T) {
	assert.True(t, equalPtr[string](nil, nil))
	assert.False(t, equalPtr(new("a"), nil))
	assert.False(t, equalPtr(nil, new("a")))
	assert.True(t, equalPtr(new("a"), new("a")))
	assert.False(t, equalPtr(new("a"), new("b")))
}
