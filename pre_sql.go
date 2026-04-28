package pistachio

import (
	"fmt"
	"os"
)

// resolvePreSQL returns the pre-SQL string from either the direct string
// or the file path. The direct string takes precedence.
func resolvePreSQL(preSQL, preSQLFile string) (string, error) {
	return resolveSQLOpt(preSQL, preSQLFile, "pre-SQL")
}

// resolveConcurrentlyPreSQL returns the concurrently-pre-SQL string from
// either the direct string or the file path. The direct string takes
// precedence.
func resolveConcurrentlyPreSQL(preSQL, preSQLFile string) (string, error) {
	return resolveSQLOpt(preSQL, preSQLFile, "concurrently-pre-SQL")
}

func resolveSQLOpt(sql, file, label string) (string, error) {
	if sql != "" {
		return sql, nil
	}
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("failed to read %s file: %s: %w", label, file, err)
		}
		return string(data), nil
	}
	return "", nil
}
