package toposort

import (
	"fmt"
	"sort"
)

// Graph is a directed acyclic graph for topological sorting.
type Graph struct {
	nodes map[string]bool
	edges map[string][]string // from → [to ...] meaning "from depends on to"
}

// NewGraph creates a new empty graph.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[string]bool),
		edges: make(map[string][]string),
	}
}

// AddNode adds a node to the graph.
func (g *Graph) AddNode(name string) {
	g.nodes[name] = true
}

// AddEdge adds a dependency edge: "from" depends on "to".
// Both nodes are implicitly added.
func (g *Graph) AddEdge(from, to string) {
	g.nodes[from] = true
	g.nodes[to] = true
	g.edges[from] = append(g.edges[from], to)
}

// Sort performs topological sort using Kahn's algorithm.
// Returns nodes in dependency order (dependencies first).
// Returns an error if a cycle is detected.
// Nodes with no dependency ordering are sorted alphabetically for stability.
func (g *Graph) Sort() ([]string, error) {
	// Build in-degree map and reverse adjacency list
	inDegree := make(map[string]int)
	dependents := make(map[string][]string) // to → [from ...] (who depends on to)

	for n := range g.nodes {
		inDegree[n] = 0
	}

	for from, tos := range g.edges {
		for _, to := range tos {
			// "from depends on to" means edge to → from in execution order
			inDegree[from]++
			dependents[to] = append(dependents[to], from)
		}
	}

	// Collect nodes with no dependencies
	var queue []string
	for n := range g.nodes {
		if inDegree[n] == 0 {
			queue = append(queue, n)
		}
	}
	sort.Strings(queue)

	var result []string
	for len(queue) > 0 {
		// Pop first (alphabetically smallest for stability)
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)

		// Reduce in-degree for dependents
		deps := dependents[node]
		sort.Strings(deps)
		for _, dep := range deps {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
		sort.Strings(queue)
	}

	if len(result) != len(g.nodes) {
		return nil, fmt.Errorf("cycle detected: sorted %d of %d nodes", len(result), len(g.nodes))
	}

	return result, nil
}
