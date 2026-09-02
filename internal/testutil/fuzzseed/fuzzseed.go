// Package fuzzseed loads the SQL corpus the fuzz targets start from.
package fuzzseed

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

// Schemas returns the schema files the restore fidelity check uses, plus a few
// short inputs that exercise the directive scanner. The fidelity schemas are
// the broadest SQL pistachio is known to handle, which makes them a better
// starting corpus than anything written by hand for the fuzzer.
//
// The paths are resolved from this file's own location, so a target can call
// this from any package without knowing where the test runs from.
func Schemas() ([]string, error) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("cannot locate the fuzzseed source file")
	}

	root := filepath.Join(filepath.Dir(self), "..", "..", "..")
	pattern := filepath.Join(root, "test", "fidelity", "schemas", "*.sql")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no seed schemas matched %s; has the directory moved?", pattern)
	}
	sort.Strings(paths)

	seeds := make([]string, 0, len(paths)+len(directives))
	for _, path := range paths {
		sql, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, string(sql))
	}

	return append(seeds, directives...), nil
}

// directives covers the hand-written comment scanner the fidelity schemas
// never reach: pg_query drops comments, so a directive only becomes reachable
// once the seed corpus puts one in front of the fuzzer.
var directives = []string{
	"",
	"-- pista:ignore\nCREATE TABLE public.t (id int);",
	"-- pista:renamed-from old_t\nCREATE TABLE public.t (id int);",
	"CREATE TABLE public.t (\n  id int, -- pista:renamed-from old_id\n  v text\n);",
	"CREATE TABLE public.t (id int);\n-- pista:concurrently\nCREATE INDEX i ON public.t (id);",
	"-- pista:bulk-alter\nCREATE TABLE public.t (id int);",
	"-- pista:execute SELECT 1\nGRANT SELECT ON public.t TO r;",
	"-- pista:execute-first\nGRANT SELECT ON public.t TO r;",
	"CREATE TYPE public.e AS ENUM ('a', 'b'); -- pista:renamed-from old_e",
}
