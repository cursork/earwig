package snapshot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzSafePath(f *testing.F) {
	root := f.TempDir()

	// Seed corpus: known interesting inputs
	f.Add("")
	f.Add(".")
	f.Add("..")
	f.Add("../etc/passwd")
	f.Add("..\\..\\etc\\passwd")
	f.Add("/etc/passwd")
	f.Add("a/b/c.txt")
	f.Add("a/../b.txt")
	f.Add("a/../../etc/shadow")
	f.Add("normal.txt")
	f.Add(string([]byte{0}))
	f.Add("foo\x00bar.txt")
	f.Add("foo\x00")
	f.Add("\x00")
	f.Add("a/b/../../../etc/passwd")
	f.Add(".earwig/earwig.db")
	f.Add("a/" + strings.Repeat("b/", 200) + "c.txt")
	f.Add(strings.Repeat("a", 4096))
	f.Add("a/\x00/b")
	f.Add("./a/./b/../c")
	f.Add("a//b//c")

	f.Fuzz(func(t *testing.T, relPath string) {
		result, err := safePath(root, relPath)
		if err != nil {
			return // rejected — that's fine
		}

		// Invariant 1: result must be absolute
		if !filepath.IsAbs(result) {
			t.Fatalf("safePath returned non-absolute path: %q", result)
		}

		// Invariant 2: result must be strictly under root (not equal to it)
		cleanRoot := filepath.Clean(root)
		if result == cleanRoot {
			t.Fatalf("safePath returned root itself: %q", result)
		}
		if !strings.HasPrefix(result, cleanRoot+string(filepath.Separator)) {
			t.Fatalf("safePath returned path outside root: result=%q root=%q", result, cleanRoot)
		}

		// Invariant 3: result must not contain NUL
		if strings.ContainsRune(result, 0) {
			t.Fatalf("safePath returned path with NUL: %q", result)
		}

		// Invariant 4: result must be clean (no //, /./, /../)
		if result != filepath.Clean(result) {
			t.Fatalf("safePath returned non-clean path: %q (clean: %q)", result, filepath.Clean(result))
		}

		// Invariant 5: the relative path from root to result should not
		// have ".." as a path component — that would mean it escaped root.
		// (Note: on Unix, backslashes are literal filename chars, so
		// "..\\..\\etc\\passwd" is a single valid component, not traversal.)
		rel, err := filepath.Rel(cleanRoot, result)
		if err != nil {
			t.Fatalf("filepath.Rel failed: %v", err)
		}
		for _, part := range strings.Split(rel, string(filepath.Separator)) {
			if part == ".." {
				t.Fatalf("relative path from root contains '..' component: %q", rel)
			}
		}

		// Invariant 6: if we can stat the parent dir, the result path
		// must still be under root at the OS level (resolve symlinks).
		// This catches cases where filepath.Clean gives a different
		// answer than the OS would.
		evalRoot, err := filepath.EvalSymlinks(cleanRoot)
		if err != nil {
			return // can't check
		}
		dir := filepath.Dir(result)
		if _, err := os.Stat(dir); err == nil {
			evalDir, err := filepath.EvalSymlinks(dir)
			if err == nil {
				if !strings.HasPrefix(evalDir, evalRoot) && evalDir != evalRoot {
					t.Fatalf("resolved parent escapes root: eval=%q evalRoot=%q", evalDir, evalRoot)
				}
			}
		}
	})
}
