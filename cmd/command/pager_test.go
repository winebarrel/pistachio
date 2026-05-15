package command_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/winebarrel/pistachio/cmd/command"
)

func TestStartPager_NoPagerFlag(t *testing.T) {
	t.Setenv("PISTA_PAGER", "cat")
	restore := command.SetIsTerminalForTest(func(*os.File) bool { return true })
	defer restore()

	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })

	w, closer, err := command.StartPager(f, true)
	if err != nil {
		t.Fatalf("StartPager: %v", err)
	}
	defer closer()
	if w != io.Writer(f) {
		t.Fatalf("noPager=true should return writer unchanged, got %T", w)
	}
}

func TestStartPager_NonFileWriter(t *testing.T) {
	t.Setenv("PISTA_PAGER", "cat")
	restore := command.SetIsTerminalForTest(func(*os.File) bool { return true })
	defer restore()

	var buf bytes.Buffer
	w, closer, err := command.StartPager(&buf, false)
	if err != nil {
		t.Fatalf("StartPager: %v", err)
	}
	defer closer()
	if w != io.Writer(&buf) {
		t.Fatalf("non-*os.File writer should be returned unchanged, got %T", w)
	}
}

func TestStartPager_NotATerminal(t *testing.T) {
	t.Setenv("PISTA_PAGER", "cat")
	restore := command.SetIsTerminalForTest(func(*os.File) bool { return false })
	defer restore()

	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })

	w, closer, err := command.StartPager(f, false)
	if err != nil {
		t.Fatalf("StartPager: %v", err)
	}
	defer closer()
	if w != io.Writer(f) {
		t.Fatalf("non-TTY writer should be returned unchanged")
	}
}

func TestStartPager_EnvUnset(t *testing.T) {
	t.Setenv("PISTA_PAGER", "")
	restore := command.SetIsTerminalForTest(func(*os.File) bool { return true })
	defer restore()

	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })

	w, closer, err := command.StartPager(f, false)
	if err != nil {
		t.Fatalf("StartPager: %v", err)
	}
	defer closer()
	if w != io.Writer(f) {
		t.Fatalf("empty PISTA_PAGER should disable paging, got %T", w)
	}
}

func TestStartPager_SpawnsAndPipesThrough(t *testing.T) {
	t.Setenv("PISTA_PAGER", "tr '[:lower:]' '[:upper:]'")
	restore := command.SetIsTerminalForTest(func(*os.File) bool { return true })
	defer restore()

	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })

	w, closer, err := command.StartPager(f, false)
	if err != nil {
		t.Fatalf("StartPager: %v", err)
	}
	if w == io.Writer(f) {
		t.Fatalf("expected pager pipe writer, got original file")
	}

	if _, err := io.WriteString(w, "hello pager"); err != nil {
		t.Fatalf("write to pager: %v", err)
	}
	closer()

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "HELLO PAGER") {
		t.Errorf("expected pager output to contain %q, got %q", "HELLO PAGER", got)
	}
}

func TestStartPager_FailsOnBadCommand(t *testing.T) {
	// `sh -c` itself starts fine, so to force a Start failure we point
	// at a non-existent executable path that exec.Command can resolve
	// and fail on directly.
	t.Setenv("PISTA_PAGER", "/no/such/pager/binary-zzz9999")
	restore := command.SetIsTerminalForTest(func(*os.File) bool { return true })
	defer restore()

	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })

	// `sh -c` will start successfully and the missing binary surfaces as
	// a non-zero exit on Wait, not an error from Start. Verify StartPager
	// still returns a usable writer and tolerates the pager exiting early.
	w, closer, err := command.StartPager(f, false)
	if err != nil {
		t.Fatalf("StartPager unexpectedly errored: %v", err)
	}
	if w == io.Writer(f) {
		t.Fatalf("expected pager pipe writer even when subprocess will exit non-zero")
	}
	// Write may EPIPE once the subprocess dies; that's fine, ignore error.
	io.WriteString(w, "data")
	closer()
}
