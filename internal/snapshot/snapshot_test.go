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

func TestIncrementalSnapshotModify(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")
	writeFile(t, dir, "b.txt", "world")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap1, _ := c.TakeSnapshot(nil, "first")

	// Modify only a.txt
	writeFile(t, dir, "a.txt", "modified")

	snap2, err := c.TakeIncrementalSnapshot(snap1.ID, map[string]bool{"a.txt": true}, "incr")
	if err != nil {
		t.Fatal(err)
	}
	if snap2 == nil {
		t.Fatal("expected new snapshot")
	}

	files, _ := s.GetSnapshotFiles(snap2.ID)
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	// b.txt should have same hash as in snap1
	snap1Files, _ := s.GetSnapshotFiles(snap1.ID)
	snap1Map := map[string]string{}
	for _, f := range snap1Files {
		snap1Map[f.Path] = f.BlobHash
	}
	snap2Map := map[string]string{}
	for _, f := range files {
		snap2Map[f.Path] = f.BlobHash
	}

	if snap2Map["b.txt"] != snap1Map["b.txt"] {
		t.Fatal("b.txt should be unchanged")
	}
	if snap2Map["a.txt"] == snap1Map["a.txt"] {
		t.Fatal("a.txt should have changed")
	}
}

func TestIncrementalSnapshotDelete(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")
	writeFile(t, dir, "b.txt", "world")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap1, _ := c.TakeSnapshot(nil, "first")

	// Delete b.txt
	os.Remove(filepath.Join(dir, "b.txt"))

	snap2, err := c.TakeIncrementalSnapshot(snap1.ID, map[string]bool{"b.txt": true}, "incr")
	if err != nil {
		t.Fatal(err)
	}
	if snap2 == nil {
		t.Fatal("expected new snapshot")
	}

	files, _ := s.GetSnapshotFiles(snap2.ID)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Path != "a.txt" {
		t.Fatalf("expected a.txt, got %s", files[0].Path)
	}
}

func TestIncrementalSnapshotDeleteDir(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")
	writeFile(t, dir, "sub/b.txt", "world")
	writeFile(t, dir, "sub/c.txt", "foo")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap1, _ := c.TakeSnapshot(nil, "first")

	// Delete the whole sub directory
	os.RemoveAll(filepath.Join(dir, "sub"))

	// fsnotify would report "sub" as removed
	snap2, err := c.TakeIncrementalSnapshot(snap1.ID, map[string]bool{"sub": true}, "incr")
	if err != nil {
		t.Fatal(err)
	}
	if snap2 == nil {
		t.Fatal("expected new snapshot")
	}

	files, _ := s.GetSnapshotFiles(snap2.ID)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Path != "a.txt" {
		t.Fatalf("expected a.txt, got %s", files[0].Path)
	}
}

func TestIncrementalSnapshotNoChange(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	snap1, _ := c.TakeSnapshot(nil, "first")

	// Report a.txt as changed but don't actually change it
	snap2, err := c.TakeIncrementalSnapshot(snap1.ID, map[string]bool{"a.txt": true}, "incr")
	if err != nil {
		t.Fatal(err)
	}
	if snap2 != nil {
		t.Fatal("expected nil snapshot when nothing actually changed")
	}
}

func TestRestoreSkipsUnchangedFiles(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "original")
	writeFile(t, dir, "b.txt", "unchanged")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, _ := c.TakeSnapshot(nil, "first")

	// Modify a.txt, leave b.txt alone
	writeFile(t, dir, "a.txt", "modified")

	// Record b.txt's mod time before restore
	bPath := filepath.Join(dir, "b.txt")
	bInfoBefore, _ := os.Stat(bPath)

	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap1.ID); err != nil {
		t.Fatal(err)
	}

	// a.txt should be restored
	data, _ := os.ReadFile(filepath.Join(dir, "a.txt"))
	if string(data) != "original" {
		t.Fatalf("expected 'original', got %q", data)
	}

	// b.txt should not have been rewritten (mod time preserved)
	bInfoAfter, _ := os.Stat(bPath)
	if !bInfoBefore.ModTime().Equal(bInfoAfter.ModTime()) {
		t.Fatal("b.txt was rewritten even though content matches")
	}
}

func TestSafePath(t *testing.T) {
	root := "/tmp/earwig-test-root"

	tests := []struct {
		name    string
		relPath string
		wantErr bool
	}{
		{"normal file", "a.txt", false},
		{"nested file", "src/app.go", false},
		{"dotdot escape", "../etc/passwd", true},
		{"nested dotdot escape", "foo/../../etc/passwd", true},
		{"absolute path", "/etc/passwd", true},
		{"empty path", "", true},
		{"just dot", ".", true},
		{"dotdot only", "..", true},
		{"deep dotdot", "a/b/c/../../../..", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := safePath(root, tt.relPath)
			if tt.wantErr && err == nil {
				t.Errorf("safePath(%q, %q) = nil error, want error", root, tt.relPath)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("safePath(%q, %q) = %v, want nil", root, tt.relPath, err)
			}
		})
	}
}

func TestRestoreRejectsTraversalPaths(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "legit")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap, _ := c.TakeSnapshot(nil, "first")

	// Manually insert a malicious snapshot with a ".." path
	blobHash, _ := s.PutBlob([]byte("malicious content"))
	malSnap, _ := s.CreateSnapshot(&snap.ID, []store.SnapshotFile{
		{Path: "a.txt", BlobHash: blobHash, Mode: 0644, Size: 17},
		{Path: "../escape.txt", BlobHash: blobHash, Mode: 0644, Size: 17},
	}, "malicious")

	// Create a canary file outside the root
	canaryPath := filepath.Join(filepath.Dir(dir), "escape.txt")
	os.WriteFile(canaryPath, []byte("canary"), 0644)
	defer os.Remove(canaryPath)

	// Restore should fail due to the traversal path
	restorer := NewRestorer(s, dir, ig)
	err := restorer.Restore(malSnap.ID)
	if err == nil {
		t.Fatal("expected error restoring snapshot with path traversal, got nil")
	}

	// Canary must be untouched
	data, _ := os.ReadFile(canaryPath)
	if string(data) != "canary" {
		t.Fatal("canary file was modified by restore with traversal path")
	}
}

func TestIncrementalSnapshotRejectsTraversalPaths(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "hello")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap, _ := c.TakeSnapshot(nil, "first")

	// Try an incremental snapshot with a malicious changedPaths entry
	// safePath should reject it silently (skip, not crash)
	snap2, err := c.TakeIncrementalSnapshot(snap.ID, map[string]bool{
		"../etc/passwd": true,
	}, "malicious")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No actual changes happened, so should be nil
	if snap2 != nil {
		t.Fatal("expected nil snapshot for traversal-only changed paths")
	}
}
