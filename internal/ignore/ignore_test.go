package ignore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltinIgnores(t *testing.T) {
	m, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path    string
		ignored bool
	}{
		{".earwig", true},
		{".earwig/earwig.db", true},
		{".git", true},
		{".git/config", true},
		{"src/main.go", false},
		{"README.md", false},
	}

	for _, tc := range cases {
		got := m.Match(tc.path)
		if got != tc.ignored {
			t.Errorf("Match(%q) = %v, want %v", tc.path, got, tc.ignored)
		}
	}
}

func TestCustomPatterns(t *testing.T) {
	dir := t.TempDir()
	ignoreFile := filepath.Join(dir, "ignore")
	if err := os.WriteFile(ignoreFile, []byte("*.log\nbuild/\n"), 0644); err != nil {
		t.Fatal(err)
	}

	m, err := New([]string{ignoreFile})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path    string
		ignored bool
	}{
		{"debug.log", true},
		{"logs/app.log", true},
		{"build/output.bin", true},
		{"src/main.go", false},
	}

	for _, tc := range cases {
		got := m.Match(tc.path)
		if got != tc.ignored {
			t.Errorf("Match(%q) = %v, want %v", tc.path, got, tc.ignored)
		}
	}
}

func TestMultipleIgnoreFiles(t *testing.T) {
	dir := t.TempDir()

	file1 := filepath.Join(dir, "a")
	if err := os.WriteFile(file1, []byte("*.log\n"), 0644); err != nil {
		t.Fatal(err)
	}

	file2 := filepath.Join(dir, "b")
	if err := os.WriteFile(file2, []byte("*.tmp\n"), 0644); err != nil {
		t.Fatal(err)
	}

	m, err := New([]string{file1, file2})
	if err != nil {
		t.Fatal(err)
	}

	if !m.Match("foo.log") {
		t.Error("expected foo.log to be ignored (from file1)")
	}
	if !m.Match("bar.tmp") {
		t.Error("expected bar.tmp to be ignored (from file2)")
	}
	if m.Match("main.go") {
		t.Error("expected main.go to not be ignored")
	}
}

func TestMissingIgnoreFile(t *testing.T) {
	m, err := New([]string{"/nonexistent/file"})
	if err != nil {
		t.Fatal(err)
	}

	// Should still work with just builtins
	if !m.Match(".earwig") {
		t.Error("expected .earwig to be ignored even with missing ignore file")
	}
	if m.Match("src/main.go") {
		t.Error("expected src/main.go to not be ignored")
	}
}

func TestNonENOENTError(t *testing.T) {
	// A directory at the ignore path makes os.ReadFile fail with a non-ENOENT
	// error (EISDIR) for EVERY user, including root — unlike chmod 0000, which
	// root bypasses (so this test also holds when run in the Docker container,
	// where tests run as root). New must surface it, not silently skip.
	dir := t.TempDir()
	ignorePath := filepath.Join(dir, "ignore-is-a-directory")
	if err := os.Mkdir(ignorePath, 0755); err != nil {
		t.Fatal(err)
	}

	_, err := New([]string{ignorePath})
	if err == nil {
		t.Fatal("expected error for unreadable ignore file, got nil")
	}
	if !strings.Contains(err.Error(), "reading ignore file") {
		t.Fatalf("expected 'reading ignore file' error, got: %v", err)
	}
}

func TestNegationPattern(t *testing.T) {
	dir := t.TempDir()
	ignoreFile := filepath.Join(dir, "ignore")
	if err := os.WriteFile(ignoreFile, []byte("*.log\n!important.log\n"), 0644); err != nil {
		t.Fatal(err)
	}

	m, err := New([]string{ignoreFile})
	if err != nil {
		t.Fatal(err)
	}

	if !m.Match("debug.log") {
		t.Error("expected debug.log to be ignored")
	}
	if m.Match("important.log") {
		t.Error("expected important.log to NOT be ignored (negation)")
	}
}

// writeIgnoreFiles writes each name→content pair into a temp dir and returns the
// paths in the same order, mirroring how main.go passes [.earwig/ignore, .gitignore].
func writeIgnoreFiles(t *testing.T, files ...[2]string) []string {
	t.Helper()
	dir := t.TempDir()
	var paths []string
	for _, f := range files {
		// Distinct filenames so paths don't collide in the shared temp dir.
		p := filepath.Join(dir, f[0])
		if err := os.WriteFile(p, []byte(f[1]), 0644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	return paths
}

// The headline feature: a `!` keep in earwig's own ignore file overrides an
// exclude declared by an external source (.gitignore). gitignore's own `!`
// only re-includes within one file; an earwig keep wins across files.
func TestKeepOverridesGitignore(t *testing.T) {
	paths := writeIgnoreFiles(t,
		[2]string{"earwig_ignore", "!runs/\n"}, // .earwig/ignore — keep
		[2]string{"gitignore", "runs/\n"},      // .gitignore — exclude
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path    string
		ignored bool
	}{
		{"runs/data.json", false},      // kept despite .gitignore runs/
		{"runs/sub/report.html", false}, // kept, nested
		{"runs", false},                 // dir itself: not pruned by the walk
		{"runs/latest", false},          // kept symlink-ish entry
		{"main.go", false},              // unrelated, never ignored
	}
	for _, tc := range cases {
		if got := m.Match(tc.path); got != tc.ignored {
			t.Errorf("Match(%q) = %v, want %v", tc.path, got, tc.ignored)
		}
	}
}

// Order independence: keeps win regardless of which file (or order) declares
// them, so reversing the file order still keeps runs/.
func TestKeepOrderIndependent(t *testing.T) {
	paths := writeIgnoreFiles(t,
		[2]string{"gitignore", "runs/\n"},
		[2]string{"earwig_ignore", "!runs/\n"},
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}
	if m.Match("runs/data.json") {
		t.Error("expected runs/data.json kept regardless of file order")
	}
}

// A keep can protect a single file inside a directory excluded by a
// trailing-slash pattern (which does not prune the dir), while its siblings
// stay ignored.
func TestKeepSelectiveFileInExcludedDir(t *testing.T) {
	paths := writeIgnoreFiles(t,
		[2]string{"earwig_ignore", "!build/keep.txt\n"},
		[2]string{"gitignore", "build/\n"},
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}

	if m.Match("build/keep.txt") {
		t.Error("expected build/keep.txt to be kept")
	}
	if !m.Match("build/other.txt") {
		t.Error("expected build/other.txt to remain ignored")
	}
	if m.Match("build") {
		t.Error("expected build dir entry not pruned (trailing-slash pattern)")
	}
}

// SAFETY: keeps must NOT be able to re-include the builtin ignores. Otherwise
// `!.earwig` would let earwig snapshot its own database, and `!.git` would put
// git internals under restore's deletion management.
func TestKeepCannotOverrideBuiltins(t *testing.T) {
	paths := writeIgnoreFiles(t,
		[2]string{"earwig_ignore", "!.earwig\n!.earwig/\n!.git\n!.git/\n"},
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range []string{".earwig", ".earwig/earwig.db", ".git", ".git/config", ".git/objects/ab/cd"} {
		if !m.Match(p) {
			t.Errorf("Match(%q) = false, want true (builtins must never be kept)", p)
		}
	}
}

// Matcher correctness for the documented limitation: a keep on a child does not
// un-ignore a directory excluded by a BARE-NAME pattern. The dir path itself
// stays ignored, so the tree walk hard-prunes it before the child keep can be
// reached. (Use a trailing-slash exclude or keep the dir itself instead.)
func TestKeepBareNameDirStillPrunes(t *testing.T) {
	paths := writeIgnoreFiles(t,
		[2]string{"earwig_ignore", "!secret/keep.txt\n"},
		[2]string{"gitignore", "secret\n"}, // bare name → prunes the dir
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}

	if !m.Match("secret") {
		t.Error("expected bare-name-excluded dir 'secret' to remain ignored (walk prunes it)")
	}
}

// extractKeeps must skip comments and blank lines and tolerate surrounding
// whitespace around the `!`.
func TestKeepIgnoresCommentsAndBlanks(t *testing.T) {
	content := "# a comment\n\n  !runs/  \n# !not-a-keep\n"
	paths := writeIgnoreFiles(t,
		[2]string{"earwig_ignore", content},
		[2]string{"gitignore", "runs/\nnot-a-keep/\n"},
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}

	if m.Match("runs/x") {
		t.Error("expected runs/x kept (whitespace around '!runs/' tolerated)")
	}
	if !m.Match("not-a-keep/x") {
		t.Error("expected not-a-keep/x ignored ('!' inside a comment is not a keep)")
	}
}

// Keeps from multiple ignore files all apply.
func TestKeepFromMultipleFiles(t *testing.T) {
	paths := writeIgnoreFiles(t,
		[2]string{"earwig_ignore", "!runs/\n"},
		[2]string{"gitignore", "runs/\nlogs/\n"},
		[2]string{"extra_ignore", "!logs/\n"},
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}
	if m.Match("runs/a") {
		t.Error("expected runs/a kept (keep from file 1)")
	}
	if m.Match("logs/b") {
		t.Error("expected logs/b kept (keep from file 3)")
	}
}

// A keep does not leak onto unrelated excluded paths.
func TestKeepDoesNotLeak(t *testing.T) {
	paths := writeIgnoreFiles(t,
		[2]string{"earwig_ignore", "!runs/\n"},
		[2]string{"gitignore", "runs/\n*.log\nrunsy/\n"},
	)
	m, err := New(paths)
	if err != nil {
		t.Fatal(err)
	}
	if m.Match("runs/keep") {
		t.Error("expected runs/keep kept")
	}
	if !m.Match("debug.log") {
		t.Error("expected debug.log still ignored (unrelated to keep)")
	}
	if !m.Match("runsy/x") {
		t.Error("expected runsy/x still ignored (keep 'runs/' must not match 'runsy/')")
	}
}
