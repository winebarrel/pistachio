package format

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestProf(t *testing.T) {
	b, err := os.ReadFile("/private/tmp/claude-501/-Volumes-Enclosure-ghq-github-com-winebarrel-pistachio/bb03f5f8-4d67-4e4d-9346-c6dafc2040fd/scratchpad/bench/schema.sql")
	if err != nil {
		t.Skip(err)
	}
	sql := string(b)
	start := time.Now()
	out, err := Format(sql)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("bytes=%d elapsed=%s\n", len(sql), time.Since(start).Round(time.Millisecond))
	_ = out
}
