package snapshot

import (
	"path/filepath"
	"strings"
	"testing"
)

func FuzzSplitPath(f *testing.F) {
	// Seed corpus
	f.Add("")
	f.Add(".")
	f.Add("a")
	f.Add("a/b/c")
	f.Add("a/b/c/")
	f.Add("a/b/c/d/e/f/g/h")
	f.Add("a//b")
	f.Add("/a/b/c")
	f.Add("./a/b")
	f.Add("a/./b/../c")
	f.Add("/")
	f.Add(strings.Repeat("a/", 100) + "z")

	f.Fuzz(func(t *testing.T, path string) {
		parts := splitPath(path)

		// Invariant 1: no empty strings in result
		for i, p := range parts {
			if p == "" {
				t.Fatalf("splitPath(%q) has empty string at index %d", path, i)
			}
		}

		// Invariant 2: no "." components in result (Clean eliminates them)
		for i, p := range parts {
			if p == "." {
				t.Fatalf("splitPath(%q) has '.' at index %d", path, i)
			}
		}

		// Invariant 3: for relative paths, Join(parts...) reconstructs the cleaned path
		if len(parts) > 0 && !filepath.IsAbs(path) {
			joined := filepath.Join(parts...)
			cleaned := filepath.Clean(path)
			if joined != cleaned {
				t.Fatalf("splitPath(%q): Join=%q != Clean=%q", path, joined, cleaned)
			}
		}

		// Invariant 4: if input is non-empty, non-absolute, and cleans to
		// something other than ".", parts should be non-empty
		cleaned := filepath.Clean(path)
		if path != "" && !filepath.IsAbs(path) && cleaned != "." && len(parts) == 0 {
			t.Fatalf("splitPath(%q) returned nil but Clean=%q", path, cleaned)
		}

		// Invariant 5: must terminate (implicit — if we get here, no infinite loop)
	})
}
