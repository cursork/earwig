package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nk/earwig/internal/ignore"
	"github.com/nk/earwig/internal/store"
)

func setup(t *testing.T) (*store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	dbDir := filepath.Join(dir, ".earwig")
	os.MkdirAll(dbDir, 0755)
	s, err := store.Open(filepath.Join(dbDir, "earwig.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	os.MkdirAll(filepath.Dir(path), 0755)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestTakeSnapshot(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")
	writeFile(t, dir, "sub/b.txt", "world")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap, err := c.TakeSnapshot(nil, "test")
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("expected snapshot, got nil")
	}

	files, _ := s.GetSnapshotFiles(snap.ID)
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	paths := map[string]bool{}
	for _, f := range files {
		paths[f.Path] = true
	}
	if !paths["a.txt"] || !paths["sub/b.txt"] {
		t.Fatalf("unexpected files: %v", files)
	}
}

func TestSnapshotDedup(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "same content")
	writeFile(t, dir, "b.txt", "same content")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap, _ := c.TakeSnapshot(nil, "test")
	files, _ := s.GetSnapshotFiles(snap.ID)

	if files[0].BlobHash != files[1].BlobHash {
		t.Fatal("identical files should share the same blob hash")
	}
}

func TestSnapshotIgnoresEarwig(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "tracked")
	writeFile(t, dir, ".earwig/earwig.db", "should be ignored")
	writeFile(t, dir, ".git/config", "should be ignored")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap, _ := c.TakeSnapshot(nil, "test")
	files, _ := s.GetSnapshotFiles(snap.ID)

	if len(files) != 1 {
		t.Fatalf("expected 1 tracked file, got %d", len(files))
	}
	if files[0].Path != "a.txt" {
		t.Fatalf("expected a.txt, got %s", files[0].Path)
	}
}

func TestSnapshotIgnoresCustomPatterns(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "tracked")
	writeFile(t, dir, "debug.log", "should be ignored")
	writeFile(t, dir, ".earwigignore", "*.log\n")

	ig, _ := ignore.New([]string{filepath.Join(dir, ".earwigignore")})
	c := NewCreator(s, dir, ig)

	snap, _ := c.TakeSnapshot(nil, "test")
	files, _ := s.GetSnapshotFiles(snap.ID)

	if len(files) != 2 { // a.txt + .earwigignore
		names := []string{}
		for _, f := range files {
			names = append(names, f.Path)
		}
		t.Fatalf("expected 2 tracked files, got %d: %v", len(files), names)
	}
}

func TestSkipIdenticalSnapshot(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap1, _ := c.TakeSnapshot(nil, "first")
	snap2, err := c.TakeSnapshot(&snap1.ID, "second")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap2 != nil {
		t.Fatal("expected nil snapshot when nothing changed")
	}
}

func TestSnapshotDetectsChanges(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap1, _ := c.TakeSnapshot(nil, "first")

	writeFile(t, dir, "a.txt", "modified")

	snap2, err := c.TakeSnapshot(&snap1.ID, "second")
	if err != nil {
		t.Fatal(err)
	}
	if snap2 == nil {
		t.Fatal("expected new snapshot after file change")
	}
	if snap2.ParentID == nil || *snap2.ParentID != snap1.ID {
		t.Fatal("second snapshot should reference first as parent")
	}
}
