package command

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/winebarrel/pistachio/format"
)

// ErrFormatDiff is returned by Fmt.Run when --check is set and a file is not
// formatted. main maps it to exit code 2, the same as plan --check.
var ErrFormatDiff = errors.New("files need formatting")

type Fmt struct {
	Files []string `arg:"" help:"Path to the schema SQL file(s)."`
	Check bool     `env:"PISTA_FMT_CHECK" help:"Report the files that are not formatted instead of writing them. Exits with code 2 when there are any."`
}

func (cmd *Fmt) Run(w io.Writer) error {
	var failed, diff int

	for _, path := range cmd.Files {
		changed, err := cmd.formatFile(path, w)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pista: %s: %s\n", path, err) //nolint:errcheck
			failed++
			continue
		}
		if changed {
			diff++
		}
	}

	if failed > 0 {
		return fmt.Errorf("failed to format %d file(s)", failed)
	}

	if cmd.Check && diff > 0 {
		return ErrFormatDiff
	}

	return nil
}

// formatFile reports whether path was not formatted already. The file is
// rewritten unless --check is set, and its name is printed either way; a file
// that is already formatted is left alone and says nothing.
//
// A symlink is resolved once, up front, and the file it points at is what is
// read and written. Renaming over the link itself would replace it with a
// plain file and leave the file it pointed at as it was, and resolving after
// the read would let a link retargeted in between carry one file's text onto
// another.
func (cmd *Fmt) formatFile(path string, w io.Writer) (bool, error) {
	target, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false, err
	}

	src, err := os.ReadFile(target)
	if err != nil {
		return false, err
	}

	out, err := format.Format(string(src))
	if err != nil {
		return false, err
	}

	if out == string(src) {
		return false, nil
	}

	if !cmd.Check {
		info, err := os.Stat(target)
		if err != nil {
			return false, err
		}
		if err := writeFileAtomic(target, out, info.Mode().Perm()); err != nil {
			return false, err
		}
	}

	fmt.Fprintln(w, path) //nolint:errcheck

	return true, nil
}

// writeFileAtomic writes content next to path and renames it over path, so a
// failure partway through leaves the original file as it was. The checks run
// in order: a rename that follows a failed write would put a half-written file
// where the original was. The rename replaces the name, not the inode, so a
// hard link to the file keeps the old content; the atomicity and the other
// names cannot both be kept.
func writeFileAtomic(path, content string, perm os.FileMode) error {
	dir, base := filepath.Dir(path), filepath.Base(path)

	tmp, err := os.CreateTemp(dir, base+".pista-*")
	if err != nil {
		return err
	}
	name := tmp.Name()

	defer func() {
		tmp.Close()     //nolint:errcheck
		os.Remove(name) //nolint:errcheck
	}()

	if _, err := tmp.WriteString(content); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(name, perm); err != nil {
		return err
	}

	return os.Rename(name, path)
}
