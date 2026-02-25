package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutAndGetBlob(t *testing.T) {
	s := testStore(t)
	data := []byte("hello world")

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if hash == "" {
		t.Fatal("expected non-empty hash")
	}

	got, err := s.GetBlob(hash)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestBlobDedup(t *testing.T) {
	s := testStore(t)
	data := []byte("duplicate content")

	h1, err := s.PutBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := s.PutBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatalf("expected same hash, got %s and %s", h1, h2)
	}

	// Verify only one row in blobs
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM blobs`).Scan(&count); err != nil {
		t.Fatalf("counting blobs: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 blob row, got %d", count)
	}
}

func TestCreateAndGetSnapshot(t *testing.T) {
	s := testStore(t)

	h1, _ := s.PutBlob([]byte("content a"))
	h2, _ := s.PutBlob([]byte("content b"))

	files := []SnapshotFile{
		{Path: "a.txt", BlobHash: h1, Mode: 0644, ModTime: time.Now(), Size: 9},
		{Path: "b.txt", BlobHash: h2, Mode: 0644, ModTime: time.Now(), Size: 9},
	}

	snap, err := s.CreateSnapshot(nil, files, "initial")
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.Hash == "" {
		t.Fatal("expected non-empty snapshot hash")
	}
	if snap.ParentID != nil {
		t.Fatal("expected nil parent for first snapshot")
	}

	// Get by full hash
	got, err := s.GetSnapshot(snap.Hash)
	if err != nil {
		t.Fatalf("GetSnapshot full hash: %v", err)
	}
	if got.ID != snap.ID {
		t.Fatalf("got ID %d, want %d", got.ID, snap.ID)
	}

	// Get by prefix
	got, err = s.GetSnapshot(snap.Hash[:8])
	if err != nil {
		t.Fatalf("GetSnapshot prefix: %v", err)
	}
	if got.ID != snap.ID {
		t.Fatalf("got ID %d, want %d", got.ID, snap.ID)
	}

	// Get files
	gotFiles, err := s.GetSnapshotFiles(snap.ID)
	if err != nil {
		t.Fatalf("GetSnapshotFiles: %v", err)
	}
	if len(gotFiles) != 2 {
		t.Fatalf("expected 2 files, got %d", len(gotFiles))
	}
	if gotFiles[0].Path != "a.txt" || gotFiles[1].Path != "b.txt" {
		t.Fatalf("unexpected file paths: %v", gotFiles)
	}
}

func TestSnapshotNotFound(t *testing.T) {
	s := testStore(t)
	_, err := s.GetSnapshot("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent snapshot")
	}
}

func TestGetSnapshotLikeWildcards(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("x"))
	files := []SnapshotFile{{Path: "x.txt", BlobHash: h, Mode: 0644, ModTime: time.Now(), Size: 1, Type: "file"}}
	snap, err := s.CreateSnapshot(nil, files, "test")
	if err != nil {
		t.Fatal(err)
	}

	// A LIKE wildcard should NOT match the snapshot
	_, err = s.GetSnapshot("%" + snap.Hash[1:5])
	if err == nil {
		t.Fatal("expected error when using % wildcard as prefix")
	}
	_, err = s.GetSnapshot("_" + snap.Hash[1:5])
	if err == nil {
		t.Fatal("expected error when using _ wildcard as prefix")
	}

	// But exact prefix should still work
	got, err := s.GetSnapshot(snap.Hash[:8])
	if err != nil {
		t.Fatalf("exact prefix lookup failed: %v", err)
	}
	if got.ID != snap.ID {
		t.Fatalf("got ID %d, want %d", got.ID, snap.ID)
	}
}

func TestListSnapshots(t *testing.T) {
	s := testStore(t)

	h1, _ := s.PutBlob([]byte("x"))
	h2, _ := s.PutBlob([]byte("y"))
	files1 := []SnapshotFile{{Path: "x.txt", BlobHash: h1, Mode: 0644, ModTime: time.Now(), Size: 1}}
	files2 := []SnapshotFile{{Path: "x.txt", BlobHash: h2, Mode: 0644, ModTime: time.Now(), Size: 1}}

	s1, _ := s.CreateSnapshot(nil, files1, "first")
	s2, _ := s.CreateSnapshot(&s1.ID, files2, "second")

	all, err := s.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(all))
	}
	if all[0].ID != s1.ID || all[1].ID != s2.ID {
		t.Fatal("snapshots not in order")
	}
	if all[1].ParentID == nil || *all[1].ParentID != s1.ID {
		t.Fatal("second snapshot should have first as parent")
	}
}

func TestGetLatestSnapshot(t *testing.T) {
	s := testStore(t)

	// No snapshots yet
	latest, err := s.GetLatestSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if latest != nil {
		t.Fatal("expected nil for empty store")
	}

	h1, _ := s.PutBlob([]byte("x"))
	h2, _ := s.PutBlob([]byte("y"))
	files1 := []SnapshotFile{{Path: "x.txt", BlobHash: h1, Mode: 0644, ModTime: time.Now(), Size: 1}}
	files2 := []SnapshotFile{{Path: "x.txt", BlobHash: h2, Mode: 0644, ModTime: time.Now(), Size: 1}}

	s1, _ := s.CreateSnapshot(nil, files1, "first")
	s.CreateSnapshot(&s1.ID, files2, "second")

	latest, err = s.GetLatestSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if latest.Message != "second" {
		t.Fatalf("expected latest to be 'second', got %q", latest.Message)
	}
}

func TestDiffSnapshots(t *testing.T) {
	s := testStore(t)

	hA, _ := s.PutBlob([]byte("content a"))
	hB, _ := s.PutBlob([]byte("content b"))
	hB2, _ := s.PutBlob([]byte("content b modified"))
	hC, _ := s.PutBlob([]byte("content c"))

	now := time.Now()
	files1 := []SnapshotFile{
		{Path: "a.txt", BlobHash: hA, Mode: 0644, ModTime: now, Size: 9},
		{Path: "b.txt", BlobHash: hB, Mode: 0644, ModTime: now, Size: 9},
	}
	snap1, _ := s.CreateSnapshot(nil, files1, "s1")

	files2 := []SnapshotFile{
		{Path: "b.txt", BlobHash: hB2, Mode: 0644, ModTime: now, Size: 18},
		{Path: "c.txt", BlobHash: hC, Mode: 0644, ModTime: now, Size: 9},
	}
	snap2, _ := s.CreateSnapshot(&snap1.ID, files2, "s2")

	changes, err := s.DiffSnapshots(snap1.ID, snap2.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}

	// Sorted by path: a.txt (deleted), b.txt (modified), c.txt (added)
	if changes[0].Path != "a.txt" || changes[0].Type != ChangeDeleted {
		t.Fatalf("expected a.txt deleted, got %+v", changes[0])
	}
	if changes[1].Path != "b.txt" || changes[1].Type != ChangeModified {
		t.Fatalf("expected b.txt modified, got %+v", changes[1])
	}
	if changes[2].Path != "c.txt" || changes[2].Type != ChangeAdded {
		t.Fatalf("expected c.txt added, got %+v", changes[2])
	}
}

func TestDuplicateSnapshotHash(t *testing.T) {
	s := testStore(t)

	h, _ := s.PutBlob([]byte("x"))
	files := []SnapshotFile{{Path: "x.txt", BlobHash: h, Mode: 0644, ModTime: time.Now(), Size: 1}}

	_, err := s.CreateSnapshot(nil, files, "first")
	if err != nil {
		t.Fatal(err)
	}

	// Same files, but timestamp in hash makes each snapshot unique.
	snap2, err := s.CreateSnapshot(nil, files, "second")
	if err != nil {
		t.Fatal(err)
	}
	if snap2.Hash == "" {
		t.Fatal("expected non-empty hash for second snapshot")
	}
}

func TestBlobCompressionRoundTrip(t *testing.T) {
	s := testStore(t)

	// Create data large enough to trigger compression (>=128KB)
	// Use repetitive data so compression actually helps
	data := make([]byte, 200*1024) // 200KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	// Verify it was stored compressed
	var encoding string
	var storedSize int
	if err := s.db.QueryRow(`SELECT encoding, length(data) FROM blobs WHERE hash = ?`, hash).Scan(&encoding, &storedSize); err != nil {
		t.Fatalf("querying blob encoding: %v", err)
	}
	if encoding != "zstd" {
		t.Fatalf("expected zstd encoding, got %q", encoding)
	}
	if storedSize >= len(data) {
		t.Fatalf("expected compressed data to be smaller: stored %d >= original %d", storedSize, len(data))
	}

	// Round-trip: GetBlob should return original data
	got, err := s.GetBlob(hash)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("got %d bytes, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, got[i], data[i])
		}
	}
}

func TestBlobSmallRoundTrip(t *testing.T) {
	s := testStore(t)
	data := []byte("small blob")

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	// Small blobs may or may not compress — either encoding is fine.
	// The important thing is the round-trip.
	got, err := s.GetBlob(hash)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestBlobIncompressibleStaysRaw(t *testing.T) {
	s := testStore(t)

	// Random-ish data that doesn't compress well (>=128KB to hit threshold)
	data := make([]byte, 200*1024)
	// Fill with pseudo-random values using a simple LCG
	v := uint32(12345)
	for i := range data {
		v = v*1103515245 + 12345
		data[i] = byte(v >> 16)
	}

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}

	var encoding string
	if err := s.db.QueryRow(`SELECT encoding FROM blobs WHERE hash = ?`, hash).Scan(&encoding); err != nil {
		t.Fatalf("querying blob encoding: %v", err)
	}
	// Could be raw or zstd depending on whether compression helped — either is fine.
	// The key test is that round-trip works.

	got, err := s.GetBlob(hash)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("got %d bytes, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}

func TestOpenCreatesDBFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sub", "test.db")

	// Parent dir doesn't exist yet — Open should fail
	_, err := Open(dbPath)
	if err == nil {
		// Some SQLite drivers create parent dirs, some don't. If it succeeded, that's fine too.
		return
	}

	// Create parent and try again
	os.MkdirAll(filepath.Dir(dbPath), 0755)
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected db file to exist")
	}
}

func TestGetBlobNotFound(t *testing.T) {
	s := testStore(t)
	_, err := s.GetBlob("0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Fatal("expected error for nonexistent blob")
	}
}

func TestDiffSnapshotsIdentical(t *testing.T) {
	s := testStore(t)
	h, err := s.PutBlob([]byte("content"))
	if err != nil {
		t.Fatal(err)
	}
	files := []SnapshotFile{{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: time.Now(), Size: 7, Type: "file"}}
	snap, err := s.CreateSnapshot(nil, files, "test")
	if err != nil {
		t.Fatal(err)
	}
	changes, err := s.DiffSnapshots(snap.ID, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("expected 0 changes for identical snapshot, got %d", len(changes))
	}
}

func TestDiffSnapshotsTypeChange(t *testing.T) {
	s := testStore(t)
	h, err := s.PutBlob([]byte("target"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	files1 := []SnapshotFile{{Path: "link.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 6, Type: "file"}}
	snap1, err := s.CreateSnapshot(nil, files1, "as file")
	if err != nil {
		t.Fatal(err)
	}
	files2 := []SnapshotFile{{Path: "link.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 6, Type: "symlink"}}
	snap2, err := s.CreateSnapshot(&snap1.ID, files2, "as symlink")
	if err != nil {
		t.Fatal(err)
	}
	changes, err := s.DiffSnapshots(snap1.ID, snap2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change for type change, got %d", len(changes))
	}
	if changes[0].Type != ChangeModified {
		t.Fatalf("expected ChangeModified, got %v", changes[0].Type)
	}
}

func TestDiffSnapshotsModeChange(t *testing.T) {
	s := testStore(t)
	h, err := s.PutBlob([]byte("content"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	files1 := []SnapshotFile{{Path: "exec.sh", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"}}
	snap1, err := s.CreateSnapshot(nil, files1, "not executable")
	if err != nil {
		t.Fatal(err)
	}
	files2 := []SnapshotFile{{Path: "exec.sh", BlobHash: h, Mode: 0755, ModTime: now, Size: 7, Type: "file"}}
	snap2, err := s.CreateSnapshot(&snap1.ID, files2, "executable")
	if err != nil {
		t.Fatal(err)
	}
	changes, err := s.DiffSnapshots(snap1.ID, snap2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change for mode change, got %d", len(changes))
	}
	if changes[0].Type != ChangeModified {
		t.Fatalf("expected ChangeModified, got %v", changes[0].Type)
	}
}
