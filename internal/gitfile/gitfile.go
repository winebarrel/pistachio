// Package gitfile reads schema files out of a git repository, so a diff can
// compare two revisions of a file without the caller writing them out first.
// It shells out to git rather than reading the object store, which keeps
// every revision spelling git accepts working, worktrees and submodules
// included.
package gitfile

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// Range is the pair of revisions a diff reads. Desired is empty when the
// desired side is the working tree rather than a revision.
type Range struct {
	Current string
	Desired string
}

// ParseRange turns a git range into the two revisions to compare, the way
// git diff reads one:
//
//	A..B    A against B
//	A...B   B against its merge base with A
//	A       A against the working tree
//
// An omitted side of A..B or A...B is HEAD. git is never given the range
// itself, so any spelling it accepts for a single revision works, HEAD^ and
// HEAD@{yesterday} included; a ref name cannot hold "..", so splitting on it
// does not cut one in half.
//
// The revisions are verified here, which separates a revision that does not
// exist from a file that is not in it.
func ParseRange(spec string) (*Range, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, errors.New("empty git range")
	}

	left, right, mergeBased := splitRange(spec)

	// Both endpoints are verified before the merge base is asked for, so a
	// revision that does not exist is reported the same way whichever form
	// the range was written in.
	for _, rev := range []string{left, right} {
		if rev == "" {
			continue
		}
		if err := verify(rev); err != nil {
			return nil, err
		}
	}

	if !mergeBased {
		return &Range{Current: left, Desired: right}, nil
	}

	base, err := mergeBase(left, right)
	if err != nil {
		return nil, err
	}

	return &Range{Current: base, Desired: right}, nil
}

// splitRange returns the two endpoints of the range and whether the current
// side is their merge base. A bare revision has no desired revision: the
// working tree stands in for it, which the empty string says.
func splitRange(spec string) (left, right string, mergeBased bool) {
	switch {
	case strings.Contains(spec, "..."):
		l, r, _ := strings.Cut(spec, "...")
		return orHEAD(l), orHEAD(r), true
	case strings.Contains(spec, ".."):
		l, r, _ := strings.Cut(spec, "..")
		return orHEAD(l), orHEAD(r), false
	default:
		return spec, "", false
	}
}

// Read returns the contents of path at rev. ok is false when rev does not
// hold the path, which a diff takes as an empty file: the path was added or
// removed between the two revisions.
func Read(rev, path string) (string, bool, error) {
	object := rev + ":" + gitPath(path)

	kind, found := objectType(object)
	if !found {
		return "", false, nil
	}
	if kind != "blob" {
		return "", false, fmt.Errorf("%s is not a file in %s", path, rev)
	}

	sql, err := run("show", "--end-of-options", object)
	if err != nil {
		return "", false, err
	}

	return sql, true, nil
}

// gitPath writes path the way git resolves it against the working directory
// rather than against the repository root, so the path on the command line is
// the path git looks up. An absolute path is made relative first, since the
// "./" prefix that asks for it cannot carry one.
func gitPath(path string) string {
	if filepath.IsAbs(path) {
		if wd, err := filepath.Abs("."); err == nil {
			if rel, err := filepath.Rel(wd, path); err == nil {
				path = rel
			}
		}
	}

	return "./" + filepath.ToSlash(path)
}

// objectType returns the type of the object, and false when git does not find
// it. The revision has been verified by then, so an object git cannot reach
// is a path the revision does not hold.
func objectType(object string) (string, bool) {
	out, err := run("cat-file", "-t", "--end-of-options", object)
	if err != nil {
		return "", false
	}

	return strings.TrimSpace(out), true
}

func orHEAD(rev string) string {
	if rev == "" {
		return "HEAD"
	}

	return rev
}

// verify fails unless rev names a commit git can reach.
func verify(rev string) error {
	if _, err := run("rev-parse", "--verify", "--end-of-options", rev+"^{commit}"); err != nil {
		return fmt.Errorf("git revision %s: %w", rev, err)
	}

	return nil
}

func mergeBase(left, right string) (string, error) {
	out, err := run("merge-base", "--end-of-options", left, right)
	if err != nil {
		return "", fmt.Errorf("git merge-base %s %s: %w", left, right, err)
	}

	return strings.TrimSpace(out), nil
}

// run returns what git wrote to stdout, byte for byte: the output is a file's
// contents as often as it is a revision.
func run(args ...string) (string, error) {
	cmd := exec.Command("git", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", errors.New(strings.TrimPrefix(msg, "fatal: "))
		}
		return "", err
	}

	return stdout.String(), nil
}
