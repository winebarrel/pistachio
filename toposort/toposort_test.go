package toposort_test

import (
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
