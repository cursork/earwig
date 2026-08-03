package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsEarwigWatchArgs(t *testing.T) {
	cases := []struct {
		name string
		argv []string
		want bool
	}{
		{"abs path + watch", []string{"/usr/local/bin/earwig", "watch"}, true},
		{"bare name + watch", []string{"earwig", "watch"}, true},
		{"relative + watch + flag", []string{"./earwig", "watch", "-detach"}, true},
		{"watch as later arg", []string{"earwig", "-detach", "watch"}, true},
		{"no watch arg", []string{"earwig"}, false},
		{"different subcommand", []string{"earwig", "log"}, false},
		{"earwig-gen is not earwig", []string{"/usr/bin/earwig-gen", "watch"}, false},
		{"editor on a notes file", []string{"vim", "earwig-watch-notes.txt"}, false},
		{"empty argv", nil, false},
		{"watch substring is not watch", []string{"earwig", "watching"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isEarwigWatchArgs(tc.argv); got != tc.want {
				t.Errorf("isEarwigWatchArgs(%q) = %v, want %v", tc.argv, got, tc.want)
			}
		})
	}
}

func TestFindRootFrom(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".earwig"), 0700); err != nil {
		t.Fatal(err)
	}
	deep := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(deep, 0755); err != nil {
		t.Fatal(err)
	}

	t.Run("found from root", func(t *testing.T) {
		got, levels, err := findRootFrom(root)
		if err != nil {
			t.Fatal(err)
		}
		if got != root || levels != 0 {
			t.Errorf("findRootFrom(root) = (%q, %d), want (%q, 0)", got, levels, root)
		}
	})

	t.Run("found from nested subdir", func(t *testing.T) {
		got, levels, err := findRootFrom(deep)
		if err != nil {
			t.Fatal(err)
		}
		if got != root || levels != 2 {
			t.Errorf("findRootFrom(deep) = (%q, %d), want (%q, 2)", got, levels, root)
		}
	})

	t.Run("not found returns error", func(t *testing.T) {
		// A directory with no .earwig anywhere up to the filesystem root.
		// Use a nested temp dir whose ancestors are unlikely to be earwig roots.
		other := t.TempDir()
		if _, _, err := findRootFrom(other); err == nil {
			t.Errorf("findRootFrom(%q) = nil error, want 'not an earwig directory'", other)
		}
	})
}
