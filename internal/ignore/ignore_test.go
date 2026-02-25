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
	// Create a file, then make it unreadable (non-ENOENT error)
	dir := t.TempDir()
	ignoreFile := filepath.Join(dir, "ignore")
	if err := os.WriteFile(ignoreFile, []byte("*.log\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ignoreFile, 0000); err != nil {
		t.Fatal(err)
	}

	_, err := New([]string{ignoreFile})
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
