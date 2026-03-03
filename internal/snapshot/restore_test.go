package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// ── splitPath tests ───────────────────────────────────────

func TestSplitPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want []string
	}{
		{"three components", "a/b/c", []string{"a", "b", "c"}},
		{"single component", "a", []string{"a"}},
		{"empty string", "", nil},
		{"dot", ".", nil},
		{"five components", "a/b/c/d/e", []string{"a", "b", "c", "d", "e"}},
		{"trailing slash", "a/b/c/", []string{"a", "b", "c"}},
		{"double slash", "a//b", []string{"a", "b"}},
		{"nested with dot", "a/./b", []string{"a", "b"}},
		{"absolute path terminates", "/a/b/c", []string{"a", "b", "c"}},
		{"root only", "/", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitPath(tt.path)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitPath(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

// ── hashFile tests ────────────────────────────────────────

func TestHashFile(t *testing.T) {
	t.Run("normal file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "hello.txt")
		content := []byte("hello world")
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatal(err)
		}

		got, err := hashFile(path)
		if err != nil {
			t.Fatalf("hashFile: %v", err)
		}

		h := sha256.Sum256(content)
		want := hex.EncodeToString(h[:])
		if got != want {
			t.Fatalf("hashFile = %q, want %q", got, want)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "empty.txt")
		if err := os.WriteFile(path, nil, 0644); err != nil {
			t.Fatal(err)
		}

		got, err := hashFile(path)
		if err != nil {
			t.Fatalf("hashFile: %v", err)
		}

		h := sha256.Sum256(nil)
		want := hex.EncodeToString(h[:])
		if got != want {
			t.Fatalf("hashFile(empty) = %q, want %q", got, want)
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := hashFile("/tmp/earwig-nonexistent-file-12345")
		if err == nil {
			t.Fatal("expected error for nonexistent file")
		}
	})

	t.Run("directory", func(t *testing.T) {
		dir := t.TempDir()
		// hashFile opens and reads — for a directory, io.Copy will fail
		// or produce a hash of nothing useful. The key is it doesn't panic.
		_, err := hashFile(dir)
		if err == nil {
			// On some OSes reading a directory via io.Copy may succeed
			// but produce an empty/wrong hash. Either an error or wrong
			// hash is acceptable — the point is no panic.
			t.Log("hashFile on directory returned no error (platform-dependent)")
		}
	})
}
