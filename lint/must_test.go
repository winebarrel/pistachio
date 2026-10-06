package lint

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMust(t *testing.T) {
	assert.Equal(t, 1, must(1, nil))
	assert.PanicsWithError(t, "boom", func() { must(0, errors.New("boom")) })
}
