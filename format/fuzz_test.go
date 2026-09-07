package format_test

import (
	"testing"

	"github.com/winebarrel/pistachio/format"
	"github.com/winebarrel/pistachio/internal/testutil/fuzzseed"
)

// FuzzFormat feeds arbitrary text to the formatter and holds it to the two
// properties the command rests on: the result carries the tokens the input
// carried, which Format checks itself and reports as an error, and formatting
// an already formatted file changes nothing.
//
// A file that does not parse is rejected, so most inputs stop at the first
// error. The value is in the ones the mutator builds out of the seeds, which
// reach the layout rules.
func FuzzFormat(f *testing.F) {
	seeds, err := fuzzseed.Schemas()
	if err != nil {
		f.Fatal(err)
	}
	for _, sql := range seeds {
		f.Add(sql)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		out, err := format.Format(sql)
		if err != nil {
			return
		}

		again, err := format.Format(out)
		if err != nil {
			t.Fatalf("formatted output does not format: %s\ninput: %q\noutput: %q", err, sql, out)
		}
		if again != out {
			t.Fatalf("format is not idempotent\ninput: %q\nfirst: %q\nsecond: %q", sql, out, again)
		}
	})
}
