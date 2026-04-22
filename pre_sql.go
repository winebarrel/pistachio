package pistachio

import (
	"fmt"
	"os"
)

// resolvePreSQL returns the pre-SQL string from either the direct string
// or the file path. The direct string takes precedence.
func resolvePreSQL(preSQL, preSQLFile string) (string, error) {
	if preSQL != "" {
		return preSQL, nil
	}
	if preSQLFile != "" {
		data, err := os.ReadFile(preSQLFile)
		if err != nil {
			return "", fmt.Errorf("failed to read pre-SQL file: %s: %w", preSQLFile, err)
		}
		return string(data), nil
	}
	return "", nil
}
