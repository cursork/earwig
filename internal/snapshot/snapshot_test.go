package snapshot

import (
	"os"
	"path/filepath"
	"strings"
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
	earwigDir := filepath.Join(dir, ".earwig")
	os.MkdirAll(earwigDir, 0755)
	writeFile(t, dir, filepath.Join(".earwig", "ignore"), "*.log\n")

	ig, _ := ignore.New([]string{filepath.Join(earwigDir, "ignore")})
	c := NewCreator(s, dir, ig)

	snap, _ := c.TakeSnapshot(nil, "test")
	files, _ := s.GetSnapshotFiles(snap.ID)

	if len(files) != 1 { // a.txt only (.earwig/ is always ignored)
		names := []string{}
		for _, f := range files {
			names = append(names, f.Path)
		}
		t.Fatalf("expected 1 tracked file, got %d: %v", len(files), names)
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

func TestRestoreRemovesSymlinkAtFilePath(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "original")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap, _ := c.TakeSnapshot(nil, "first")

	// Create a canary file outside the root and replace a.txt with a symlink to it
	canaryPath := filepath.Join(t.TempDir(), "canary")
	os.WriteFile(canaryPath, []byte("do not touch"), 0644)

	aPath := filepath.Join(dir, "a.txt")
	os.Remove(aPath)
	os.Symlink(canaryPath, aPath)

	// Restore should remove the symlink and write the regular file
	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap.ID); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// a.txt should be a regular file with the original content
	info, err := os.Lstat(aPath)
	if err != nil {
		t.Fatalf("a.txt missing after restore: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("a.txt is still a symlink after restore")
	}
	data, _ := os.ReadFile(aPath)
	if string(data) != "original" {
		t.Fatalf("a.txt has wrong content: %q", data)
	}

	// Canary must be untouched
	canary, _ := os.ReadFile(canaryPath)
	if string(canary) != "do not touch" {
		t.Fatalf("canary was modified: %q", canary)
	}
}

func TestRestoreRemovesSymlinkInDirPath(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "sub/a.txt", "original")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap, _ := c.TakeSnapshot(nil, "first")

	// Replace the "sub" directory with a symlink to an outside directory
	outsideDir := t.TempDir()
	os.WriteFile(filepath.Join(outsideDir, "a.txt"), []byte("do not touch"), 0644)

	os.RemoveAll(filepath.Join(dir, "sub"))
	os.Symlink(outsideDir, filepath.Join(dir, "sub"))

	// Restore should remove the symlink, recreate the dir, and write the file
	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap.ID); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// sub should be a real directory, not a symlink
	info, err := os.Lstat(filepath.Join(dir, "sub"))
	if err != nil {
		t.Fatalf("sub missing after restore: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("sub is still a symlink after restore")
	}

	// sub/a.txt should have the snapshot content
	data, _ := os.ReadFile(filepath.Join(dir, "sub", "a.txt"))
	if string(data) != "original" {
		t.Fatalf("sub/a.txt has wrong content: %q", data)
	}

	// Outside directory's file must be untouched
	outside, _ := os.ReadFile(filepath.Join(outsideDir, "a.txt"))
	if string(outside) != "do not touch" {
		t.Fatalf("outside file was modified: %q", outside)
	}
}

func TestSymlinkRoundTrip(t *testing.T) {
	s, dir := setup(t)

	// Create a regular file and a symlink
	writeFile(t, dir, "real.txt", "real content")
	target := filepath.Join(t.TempDir(), "external")
	os.WriteFile(target, []byte("external"), 0644)
	os.Symlink(target, filepath.Join(dir, "link.txt"))

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	// Snapshot should capture both the file and the symlink
	snap, err := c.TakeSnapshot(nil, "with-symlink")
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	files, _ := s.GetSnapshotFiles(snap.ID)
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	fileMap := map[string]store.SnapshotFile{}
	for _, f := range files {
		fileMap[f.Path] = f
	}

	if fileMap["real.txt"].Type != "file" {
		t.Fatalf("real.txt should be type 'file', got %q", fileMap["real.txt"].Type)
	}
	if fileMap["link.txt"].Type != "symlink" {
		t.Fatalf("link.txt should be type 'symlink', got %q", fileMap["link.txt"].Type)
	}

	// Delete everything and restore
	os.RemoveAll(filepath.Join(dir, "real.txt"))
	os.Remove(filepath.Join(dir, "link.txt"))

	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap.ID); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// real.txt should be a regular file
	data, _ := os.ReadFile(filepath.Join(dir, "real.txt"))
	if string(data) != "real content" {
		t.Fatalf("real.txt content: %q", data)
	}

	// link.txt should be a symlink pointing to the original target
	linkPath := filepath.Join(dir, "link.txt")
	info, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("link.txt missing: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("link.txt should be a symlink after restore")
	}
	got, _ := os.Readlink(linkPath)
	if got != target {
		t.Fatalf("link.txt target: got %q, want %q", got, target)
	}
}

func TestFilesEqualTypeChange(t *testing.T) {
	// Same content/path but different type should not be equal
	h := "abc123"
	a := []store.SnapshotFile{{Path: "link.txt", BlobHash: h, Mode: 0644, Type: "file"}}
	b := []store.SnapshotFile{{Path: "link.txt", BlobHash: h, Mode: 0644, Type: "symlink"}}
	if filesEqual(a, b) {
		t.Fatal("filesEqual should return false when type differs")
	}
}

func TestFilesEqualModeChange(t *testing.T) {
	// Same content/path/type but different mode should not be equal
	h := "abc123"
	a := []store.SnapshotFile{{Path: "exec.sh", BlobHash: h, Mode: 0644, Type: "file"}}
	b := []store.SnapshotFile{{Path: "exec.sh", BlobHash: h, Mode: 0755, Type: "file"}}
	if filesEqual(a, b) {
		t.Fatal("filesEqual should return false when mode differs")
	}
}

func TestRestoreRegularFileToSymlink(t *testing.T) {
	s, dir := setup(t)

	// Snapshot 1: regular file
	writeFile(t, dir, "target.txt", "regular content")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, err := c.TakeSnapshot(nil, "as-file")
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot 2: symlink at the same path
	os.Remove(filepath.Join(dir, "target.txt"))
	linkTarget := filepath.Join(t.TempDir(), "elsewhere")
	os.WriteFile(linkTarget, []byte("external"), 0644)
	os.Symlink(linkTarget, filepath.Join(dir, "target.txt"))

	snap2, err := c.TakeSnapshot(&snap1.ID, "as-symlink")
	if err != nil {
		t.Fatal(err)
	}

	// Restore snap1 (regular file) then snap2 (symlink)
	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap1.ID); err != nil {
		t.Fatalf("Restore to snap1: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "target.txt"))
	if string(data) != "regular content" {
		t.Fatalf("expected regular content, got %q", data)
	}

	// Now restore to symlink — this is the critical test.
	// The regular file must be removed before os.Symlink.
	if err := restorer.Restore(snap2.ID); err != nil {
		t.Fatalf("Restore to snap2 (file->symlink): %v", err)
	}
	info, err := os.Lstat(filepath.Join(dir, "target.txt"))
	if err != nil {
		t.Fatalf("target.txt missing: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("target.txt should be a symlink after restore")
	}
	got, _ := os.Readlink(filepath.Join(dir, "target.txt"))
	if got != linkTarget {
		t.Fatalf("symlink target: got %q, want %q", got, linkTarget)
	}
}

func TestRestoreOverwritesReadOnlyFile(t *testing.T) {
	s, dir := setup(t)

	writeFile(t, dir, "ro.txt", "version1")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, err := c.TakeSnapshot(nil, "v1")
	if err != nil {
		t.Fatal(err)
	}

	// Write new content and make it read-only
	writeFile(t, dir, "ro.txt", "version2")
	os.Chmod(filepath.Join(dir, "ro.txt"), 0444)

	snap2, err := c.TakeSnapshot(&snap1.ID, "v2")
	if err != nil {
		t.Fatal(err)
	}
	_ = snap2

	// Restore to snap1 — must overwrite the read-only file
	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap1.ID); err != nil {
		t.Fatalf("Restore over read-only file: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "ro.txt"))
	if string(data) != "version1" {
		t.Fatalf("expected 'version1', got %q", data)
	}
}

func TestRestoreDeletesFileInReadOnlyDir(t *testing.T) {
	s, dir := setup(t)

	writeFile(t, dir, "ro-dir/keep.txt", "keep")
	writeFile(t, dir, "ro-dir/delete-me.txt", "gone")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, err := c.TakeSnapshot(nil, "with-both")
	if err != nil {
		t.Fatal(err)
	}

	// Remove the file from disk and take a new snapshot
	os.Remove(filepath.Join(dir, "ro-dir", "delete-me.txt"))
	snap2, err := c.TakeSnapshot(&snap1.ID, "without-delete-me")
	if err != nil {
		t.Fatal(err)
	}

	// Restore to snap1 (both files), then make dir read-only
	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap1.ID); err != nil {
		t.Fatalf("Restore to snap1: %v", err)
	}
	os.Chmod(filepath.Join(dir, "ro-dir"), 0555)
	t.Cleanup(func() {
		// Ensure cleanup can remove the dir
		os.Chmod(filepath.Join(dir, "ro-dir"), 0755)
	})

	// Restore to snap2 — must delete delete-me.txt from read-only dir
	if err := restorer.Restore(snap2.ID); err != nil {
		t.Fatalf("Restore with read-only parent dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ro-dir", "delete-me.txt")); err == nil {
		t.Fatal("delete-me.txt should have been removed from read-only dir")
	}
	data, _ := os.ReadFile(filepath.Join(dir, "ro-dir", "keep.txt"))
	if string(data) != "keep" {
		t.Fatalf("keep.txt expected 'keep', got %q", data)
	}
}

// S3: safePath rejects NUL bytes in paths.
func TestSafePathRejectsNUL(t *testing.T) {
	root := "/tmp/earwig-test-root"
	_, err := safePath(root, "foo\x00bar.txt")
	if err == nil {
		t.Fatal("expected error for path with NUL byte, got nil")
	}
	if !strings.Contains(err.Error(), "NUL") {
		t.Fatalf("expected NUL error, got: %v", err)
	}
}

// S3: Restore rejects crafted snapshot with NUL byte in path.
func TestRestoreRejectsNULInPath(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "a.txt", "legit")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap, _ := c.TakeSnapshot(nil, "first")

	// Manually insert a snapshot with a NUL byte in the path
	blobHash, _ := s.PutBlob([]byte("evil"))
	malSnap, _ := s.CreateSnapshot(&snap.ID, []store.SnapshotFile{
		{Path: "a.txt", BlobHash: blobHash, Mode: 0644, Size: 4, Type: "file"},
		{Path: "evil\x00.txt", BlobHash: blobHash, Mode: 0644, Size: 4, Type: "file"},
	}, "nul-attack")

	restorer := NewRestorer(s, dir, ig)
	err := restorer.Restore(malSnap.ID)
	if err == nil {
		t.Fatal("expected error restoring snapshot with NUL in path, got nil")
	}
}

// S9: readFile rejects non-regular files (via IsRegular check after fstat).
// Symlink filtering is handled by the caller (Lstat), but if a directory is
// passed to readFile it should be caught by the fstat check.
func TestReadFileRejectsDirectory(t *testing.T) {
	s, dir := setup(t)
	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)

	subDir := filepath.Join(dir, "subdir")
	os.MkdirAll(subDir, 0755)

	_, err := c.readFile(subDir, "subdir")
	if err == nil {
		t.Fatal("readFile should reject directories")
	}
	if !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("expected 'not a regular file' error, got: %v", err)
	}
}

// S10: Restore preserves read-only parent dir permissions after deleting a file.
func TestRestorePreservesReadOnlyDirPerms(t *testing.T) {
	s, dir := setup(t)

	writeFile(t, dir, "ro-dir/keep.txt", "keep")
	writeFile(t, dir, "ro-dir/delete-me.txt", "gone")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, _ := c.TakeSnapshot(nil, "with-both")

	// Remove delete-me.txt and snapshot
	os.Remove(filepath.Join(dir, "ro-dir", "delete-me.txt"))
	snap2, err := c.TakeSnapshot(&snap1.ID, "without-delete-me")
	if err != nil {
		t.Fatal(err)
	}

	// Restore snap1 (both files), then make dir read-only
	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(snap1.ID); err != nil {
		t.Fatalf("Restore to snap1: %v", err)
	}
	roDirPath := filepath.Join(dir, "ro-dir")
	os.Chmod(roDirPath, 0555)
	t.Cleanup(func() { os.Chmod(roDirPath, 0755) })

	// Restore snap2 — must delete delete-me.txt from read-only dir
	if err := restorer.Restore(snap2.ID); err != nil {
		t.Fatalf("Restore with read-only parent dir: %v", err)
	}

	// delete-me.txt should be gone
	if _, err := os.Stat(filepath.Join(roDirPath, "delete-me.txt")); err == nil {
		t.Fatal("delete-me.txt should have been removed")
	}

	// The directory should still be 0555 (permissions restored after chmod)
	info, err := os.Stat(roDirPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0555 {
		t.Fatalf("ro-dir should be 0555 after restore, got %04o", info.Mode().Perm())
	}
}

func TestPreviewCategorizesChanges(t *testing.T) {
	s, dir := setup(t)

	// Create initial state: a.txt, b.txt, c.txt
	writeFile(t, dir, "a.txt", "original-a")
	writeFile(t, dir, "b.txt", "original-b")
	writeFile(t, dir, "c.txt", "original-c")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, err := c.TakeSnapshot(nil, "first")
	if err != nil {
		t.Fatal(err)
	}

	// Modify filesystem: change a.txt, delete b.txt, add d.txt
	writeFile(t, dir, "a.txt", "changed-a")
	os.Remove(filepath.Join(dir, "b.txt"))
	writeFile(t, dir, "d.txt", "new-d")

	// Preview restoring to snap1
	restorer := NewRestorer(s, dir, ig)
	plan, err := restorer.Preview(snap1.ID)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}

	// d.txt should be in Delete (on disk but not in snapshot)
	if len(plan.Delete) != 1 || plan.Delete[0] != "d.txt" {
		t.Fatalf("expected Delete=[d.txt], got %v", plan.Delete)
	}
	// b.txt should be in Write (in snapshot but not on disk)
	if len(plan.Write) != 1 || plan.Write[0] != "b.txt" {
		t.Fatalf("expected Write=[b.txt], got %v", plan.Write)
	}
	// a.txt should be in Modify (different content)
	if len(plan.Modify) != 1 || plan.Modify[0] != "a.txt" {
		t.Fatalf("expected Modify=[a.txt], got %v", plan.Modify)
	}
	// c.txt should be Unchanged
	if plan.Unchanged != 1 {
		t.Fatalf("expected Unchanged=1, got %d", plan.Unchanged)
	}
	// HasChanges should be true
	if !plan.HasChanges() {
		t.Fatal("HasChanges should be true")
	}
}

func TestPreviewDetectsChmod(t *testing.T) {
	s, dir := setup(t)

	writeFile(t, dir, "script.sh", "#!/bin/sh")
	os.Chmod(filepath.Join(dir, "script.sh"), 0755)

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, err := c.TakeSnapshot(nil, "executable")
	if err != nil {
		t.Fatal(err)
	}

	// Change mode to 0644
	os.Chmod(filepath.Join(dir, "script.sh"), 0644)

	restorer := NewRestorer(s, dir, ig)
	plan, err := restorer.Preview(snap1.ID)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}

	if len(plan.Chmod) != 1 {
		t.Fatalf("expected 1 chmod entry, got %d", len(plan.Chmod))
	}
	if plan.Chmod[0].Path != "script.sh" {
		t.Fatalf("expected chmod for script.sh, got %s", plan.Chmod[0].Path)
	}
	if plan.Chmod[0].OldMode != 0644 {
		t.Fatalf("expected old mode 0644, got %04o", plan.Chmod[0].OldMode)
	}
	if plan.Chmod[0].NewMode != 0755 {
		t.Fatalf("expected new mode 0755, got %04o", plan.Chmod[0].NewMode)
	}
}

func TestPreviewNoChanges(t *testing.T) {
	s, dir := setup(t)

	writeFile(t, dir, "a.txt", "hello")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap1, err := c.TakeSnapshot(nil, "first")
	if err != nil {
		t.Fatal(err)
	}

	restorer := NewRestorer(s, dir, ig)
	plan, err := restorer.Preview(snap1.ID)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}

	if plan.HasChanges() {
		t.Fatalf("expected no changes, got Delete=%v Write=%v Modify=%v Chmod=%v",
			plan.Delete, plan.Write, plan.Modify, plan.Chmod)
	}
	if plan.Unchanged != 1 {
		t.Fatalf("expected 1 unchanged, got %d", plan.Unchanged)
	}
}

func TestRestoreSkipsIgnoredPaths(t *testing.T) {
	s, dir := setup(t)

	writeFile(t, dir, "a.txt", "tracked")

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig)
	snap, err := c.TakeSnapshot(nil, "first")
	if err != nil {
		t.Fatal(err)
	}

	// Manually insert a malicious snapshot with a .earwig/ path
	blobHash, _ := s.PutBlob([]byte("malicious"))
	malSnap, err := s.CreateSnapshot(&snap.ID, []store.SnapshotFile{
		{Path: "a.txt", BlobHash: blobHash, Mode: 0644, Size: 9, Type: "file"},
		{Path: ".earwig/evil.txt", BlobHash: blobHash, Mode: 0644, Size: 9, Type: "file"},
	}, "malicious")
	if err != nil {
		t.Fatal(err)
	}

	restorer := NewRestorer(s, dir, ig)
	if err := restorer.Restore(malSnap.ID); err != nil {
		t.Fatal(err)
	}

	// .earwig/evil.txt must NOT have been written
	evilPath := filepath.Join(dir, ".earwig", "evil.txt")
	if _, err := os.Stat(evilPath); err == nil {
		t.Fatal(".earwig/evil.txt should not exist — ignore matcher bypass")
	}

	// a.txt should be restored normally
	data, _ := os.ReadFile(filepath.Join(dir, "a.txt"))
	if string(data) != "malicious" {
		t.Fatalf("a.txt expected 'malicious', got %q", data)
	}
}
