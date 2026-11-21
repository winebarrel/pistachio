package command

import "strings"

// trimSchemas trims each schema name in place.
func trimSchemas(schemas []string) {
	for i, s := range schemas {
		schemas[i] = strings.TrimSpace(s)
	}
}
