package lint_test

import (
	"errors"
	"testing"

	"github.com/winebarrel/pistachio/lint"

	"github.com/stretchr/testify/assert"
)

func TestMust(t *testing.T) {
	assert.Equal(t, 1, lint.Must(1, nil))
	assert.PanicsWithError(t, "boom", func() { lint.Must(0, errors.New("boom")) })
}
