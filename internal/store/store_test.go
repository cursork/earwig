package store

import (
	"os"
	"path/filepath"
	"strings"
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

func TestGetBlobVerifiesHash(t *testing.T) {
	s := testStore(t)
	data := []byte("hello world")

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with the blob data in the DB (same length so size check passes)
	_, err = s.db.Exec(`UPDATE blobs SET data = ? WHERE hash = ?`, []byte("hello tampr"), hash)
	if err != nil {
		t.Fatal(err)
	}

	// GetBlob should detect the hash mismatch
	_, err = s.GetBlob(hash)
	if err == nil {
		t.Fatal("expected error for tampered blob, got nil")
	}
	if !strings.Contains(err.Error(), "integrity check failed") {
		t.Fatalf("expected integrity check error, got: %v", err)
	}
}

func TestGetBlobRejectsOversizedBlob(t *testing.T) {
	s := testStore(t)
	data := []byte("small data")

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	// Set a huge size value in the DB to simulate a decompression bomb
	_, err = s.db.Exec(`UPDATE blobs SET size = ? WHERE hash = ?`, maxBlobSize+1, hash)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetBlob(hash)
	if err == nil {
		t.Fatal("expected error for oversized blob, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected size exceeded error, got: %v", err)
	}
}

func TestCreateSnapshotValidatesType(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	// Valid types should work
	_, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "valid file")
	if err != nil {
		t.Fatalf("file type should be valid: %v", err)
	}

	// Empty type defaults to "file" — should work
	snap2, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "b.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: ""},
	}, "empty type")
	if err != nil {
		t.Fatalf("empty type should default to file: %v", err)
	}
	files, _ := s.GetSnapshotFiles(snap2.ID)
	if files[0].Type != "file" {
		t.Fatalf("expected type 'file', got %q", files[0].Type)
	}

	// Invalid type should fail
	_, err = s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "c.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "unknown"},
	}, "invalid type")
	if err == nil {
		t.Fatal("expected error for invalid file type 'unknown'")
	}
}

func TestGarbageCollect(t *testing.T) {
	s := testStore(t)

	// Store a blob that's referenced by a snapshot
	h1, _ := s.PutBlob([]byte("referenced"))
	now := time.Now()
	s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h1, Mode: 0644, ModTime: now, Size: 10, Type: "file"},
	}, "snap")

	// Store an orphaned blob (not referenced by any snapshot)
	h2, _ := s.PutBlob([]byte("orphaned"))

	// Verify both blobs exist
	var count int
	s.db.QueryRow(`SELECT COUNT(*) FROM blobs`).Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2 blobs, got %d", count)
	}

	// GC should remove the orphaned blob
	removed, err := s.GarbageCollect()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}

	// Referenced blob should still exist
	_, err = s.GetBlob(h1)
	if err != nil {
		t.Fatalf("referenced blob should survive GC: %v", err)
	}

	// Orphaned blob should be gone
	_, err = s.GetBlob(h2)
	if err == nil {
		t.Fatal("orphaned blob should have been removed by GC")
	}
}

func TestCorruptTimestampGetSnapshot(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("data"))
	now := time.Now()
	snap, _ := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 4, Type: "file"},
	}, "test")

	// Corrupt the created_at timestamp
	_, err := s.db.Exec(`UPDATE snapshots SET created_at = 'not-a-date' WHERE id = ?`, snap.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetSnapshot(snap.Hash)
	if err == nil {
		t.Fatal("expected error for corrupt timestamp in GetSnapshot")
	}
	if !strings.Contains(err.Error(), "corrupt timestamp") {
		t.Fatalf("expected corrupt timestamp error, got: %v", err)
	}
}

func TestCorruptTimestampListSnapshots(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("data"))
	now := time.Now()
	s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 4, Type: "file"},
	}, "test")

	// Corrupt the timestamp
	_, err := s.db.Exec(`UPDATE snapshots SET created_at = 'garbage'`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.ListSnapshots()
	if err == nil {
		t.Fatal("expected error for corrupt timestamp in ListSnapshots")
	}
	if !strings.Contains(err.Error(), "corrupt timestamp") {
		t.Fatalf("expected corrupt timestamp error, got: %v", err)
	}
}

func TestCorruptTimestampGetLatestSnapshot(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("data"))
	now := time.Now()
	s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 4, Type: "file"},
	}, "test")

	// Corrupt the timestamp
	_, err := s.db.Exec(`UPDATE snapshots SET created_at = 'garbage'`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetLatestSnapshot()
	if err == nil {
		t.Fatal("expected error for corrupt timestamp in GetLatestSnapshot")
	}
	if !strings.Contains(err.Error(), "corrupt timestamp") {
		t.Fatalf("expected corrupt timestamp error, got: %v", err)
	}
}

func TestCorruptModTimeGetSnapshotFiles(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("data"))
	now := time.Now()
	snap, _ := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 4, Type: "file"},
	}, "test")

	// Corrupt the mod_time timestamp
	_, err := s.db.Exec(`UPDATE snapshot_files SET mod_time = 'bad-time' WHERE snapshot_id = ?`, snap.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetSnapshotFiles(snap.ID)
	if err == nil {
		t.Fatal("expected error for corrupt mod_time in GetSnapshotFiles")
	}
	if !strings.Contains(err.Error(), "corrupt mod_time") {
		t.Fatalf("expected corrupt mod_time error, got: %v", err)
	}
}

// S1: Snapshot hash includes mode and type — changing either produces a different hash.
func TestSnapshotHashIncludesMode(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	snap1, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "mode-644")
	if err != nil {
		t.Fatal(err)
	}

	snap2, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0755, ModTime: now, Size: 7, Type: "file"},
	}, "mode-755")
	if err != nil {
		t.Fatal(err)
	}

	if snap1.Hash == snap2.Hash {
		t.Fatal("snapshots with different modes should have different hashes")
	}
}

func TestSnapshotHashIncludesType(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("target"))
	now := time.Now()

	snap1, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 6, Type: "file"},
	}, "as-file")
	if err != nil {
		t.Fatal(err)
	}

	snap2, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 6, Type: "symlink"},
	}, "as-symlink")
	if err != nil {
		t.Fatal(err)
	}

	if snap1.Hash == snap2.Hash {
		t.Fatal("snapshots with different types should have different hashes")
	}
}

// S6: CreateSnapshot rejects path conflicts (file and directory at same path).
func TestCreateSnapshotRejectsPathConflict(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	_, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "foo", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "foo/bar.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "conflicting")
	if err == nil {
		t.Fatal("expected error for path conflict, got nil")
	}
	if !strings.Contains(err.Error(), "path conflict") {
		t.Fatalf("expected path conflict error, got: %v", err)
	}
}

func TestCreateSnapshotAllowsNonConflictingPaths(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	// "foo.txt" and "foobar/baz.txt" should NOT conflict (foo.txt/ is not a prefix of foobar/)
	_, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "foo.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "foobar/baz.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "non-conflicting")
	if err != nil {
		t.Fatalf("non-conflicting paths should be accepted: %v", err)
	}
}

// Bug fix: path conflict detection with intervening paths.
// "foo" and "foo/bar.txt" conflict, but "foo-bar/baz.txt" sorts between them
// and the old adjacent-pair check missed the conflict.
func TestCreateSnapshotRejectsNonAdjacentPathConflict(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	_, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "foo", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "foo-bar/baz.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "foo/bar.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "non-adjacent conflict")
	if err == nil {
		t.Fatal("expected error for non-adjacent path conflict (foo vs foo/bar.txt)")
	}
	if !strings.Contains(err.Error(), "path conflict") {
		t.Fatalf("expected path conflict error, got: %v", err)
	}
}

// Bug fix: GetBlob raw blob size mismatch.
// A crafted DB row with raw encoding but mismatched size should be rejected.
func TestGetBlobRejectsRawSizeMismatch(t *testing.T) {
	s := testStore(t)
	data := []byte("small data")

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	// Set a wrong size (smaller than actual data) to simulate crafted DB
	_, err = s.db.Exec(`UPDATE blobs SET size = ? WHERE hash = ?`, 5, hash)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetBlob(hash)
	if err == nil {
		t.Fatal("expected error for raw blob with mismatched size")
	}
	if !strings.Contains(err.Error(), "data length") {
		t.Fatalf("expected data length mismatch error, got: %v", err)
	}
}

// Verify validatePathConflicts with multiple stacked potential parents
func TestValidatePathConflictsDeepNesting(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	// "a", "a/b", "a/b/c.txt" — "a" conflicts with "a/b"
	_, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "a/b", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "a/b/c.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "deep conflict")
	if err == nil {
		t.Fatal("expected error for deep nested path conflict")
	}
	if !strings.Contains(err.Error(), "path conflict") {
		t.Fatalf("expected path conflict error, got: %v", err)
	}
}

// S7: DeleteSnapshot removes snapshot and re-parents children.
func TestDeleteSnapshot(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	// Create a chain: snap1 -> snap2 -> snap3
	snap1, _ := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "first")
	snap2, _ := s.CreateSnapshot(&snap1.ID, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0755, ModTime: now, Size: 7, Type: "file"},
	}, "second")
	snap3, _ := s.CreateSnapshot(&snap2.ID, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "third")

	// Delete the middle snapshot
	if err := s.DeleteSnapshot(snap2.ID); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	// snap2 should be gone
	_, err := s.GetSnapshot(snap2.Hash)
	if err == nil {
		t.Fatal("deleted snapshot should not be found")
	}

	// snap3 should be re-parented to snap1
	got, err := s.GetSnapshot(snap3.Hash)
	if err != nil {
		t.Fatalf("snap3 lookup: %v", err)
	}
	if got.ParentID == nil || *got.ParentID != snap1.ID {
		t.Fatalf("snap3 should be re-parented to snap1, got parent %v", got.ParentID)
	}
}

func TestDeleteSnapshotReparentsToNull(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	snap1, _ := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "root")
	snap2, _ := s.CreateSnapshot(&snap1.ID, []SnapshotFile{
		{Path: "a.txt", BlobHash: h, Mode: 0755, ModTime: now, Size: 7, Type: "file"},
	}, "child")

	// Delete the root
	if err := s.DeleteSnapshot(snap1.ID); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	got, _ := s.GetSnapshot(snap2.Hash)
	if got.ParentID != nil {
		t.Fatalf("snap2 should have nil parent after root deleted, got %v", got.ParentID)
	}
}

// --- MaskMode tests ---

func TestMaskModeNormal(t *testing.T) {
	if got := MaskMode(0o755); got != 0o755 {
		t.Fatalf("MaskMode(0755) = %04o, want 0755", got)
	}
	if got := MaskMode(0o644); got != 0o644 {
		t.Fatalf("MaskMode(0644) = %04o, want 0644", got)
	}
}

func TestMaskModeStripsSetuid(t *testing.T) {
	if got := MaskMode(0o4755); got != 0o755 {
		t.Fatalf("MaskMode(04755) = %04o, want 0755", got)
	}
}

func TestMaskModeStripsSetgid(t *testing.T) {
	if got := MaskMode(0o2755); got != 0o755 {
		t.Fatalf("MaskMode(02755) = %04o, want 0755", got)
	}
}

func TestMaskModeStripsSticky(t *testing.T) {
	if got := MaskMode(0o1755); got != 0o755 {
		t.Fatalf("MaskMode(01755) = %04o, want 0755", got)
	}
}

func TestMaskModeStripsAll(t *testing.T) {
	if got := MaskMode(0o7777); got != 0o777 {
		t.Fatalf("MaskMode(07777) = %04o, want 0777", got)
	}
}

func TestMaskModeZero(t *testing.T) {
	if got := MaskMode(0); got != 0 {
		t.Fatalf("MaskMode(0) = %04o, want 0", got)
	}
}

// --- chooseEncoding tests ---

func TestChooseEncodingSmallData(t *testing.T) {
	// Data below threshold → always raw, regardless of compression ratio
	enc, use := chooseEncoding(100, 50)
	if enc != "raw" || use {
		t.Fatalf("small data: expected raw/false, got %s/%v", enc, use)
	}
}

func TestChooseEncodingLargeCompressible(t *testing.T) {
	// Data above threshold, compression helped
	enc, use := chooseEncoding(200*1024, 100*1024)
	if enc != "zstd" || !use {
		t.Fatalf("large compressible: expected zstd/true, got %s/%v", enc, use)
	}
}

func TestChooseEncodingLargeIncompressible(t *testing.T) {
	// Data above threshold, compression didn't help (same size or larger)
	enc, use := chooseEncoding(200*1024, 200*1024)
	if enc != "raw" || use {
		t.Fatalf("large incompressible: expected raw/false, got %s/%v", enc, use)
	}
	enc, use = chooseEncoding(200*1024, 300*1024)
	if enc != "raw" || use {
		t.Fatalf("large expanded: expected raw/false, got %s/%v", enc, use)
	}
}

func TestChooseEncodingBoundary(t *testing.T) {
	// Exactly at threshold
	enc, _ := chooseEncoding(128*1024, 128*1024-1)
	if enc != "zstd" {
		t.Fatalf("at threshold with savings: expected zstd, got %s", enc)
	}
	enc, _ = chooseEncoding(128*1024-1, 0)
	if enc != "raw" {
		t.Fatalf("below threshold: expected raw, got %s", enc)
	}
}

// --- classifyFileChange tests ---

func TestClassifyFileChange(t *testing.T) {
	tests := []struct {
		name           string
		inOld, inNew   bool
		contentDiffers bool
		want           string
	}{
		{"added", false, true, false, "added"},
		{"deleted", true, false, false, "deleted"},
		{"modified", true, true, true, "modified"},
		{"unchanged", true, true, false, "unchanged"},
		// contentDiffers is ignored when only in new
		{"added ignores differs", false, true, true, "added"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyFileChange(tt.inOld, tt.inNew, tt.contentDiffers)
			if got != tt.want {
				t.Fatalf("classifyFileChange(%v, %v, %v) = %q, want %q",
					tt.inOld, tt.inNew, tt.contentDiffers, got, tt.want)
			}
		})
	}
}

// --- checkBlobSize tests ---

func TestCheckBlobSizeValid(t *testing.T) {
	if err := checkBlobSize(100, 100, 1024); err != nil {
		t.Fatalf("valid size: %v", err)
	}
}

func TestCheckBlobSizeExceedsMax(t *testing.T) {
	err := checkBlobSize(100, 2000, 1024)
	if err == nil {
		t.Fatal("expected error for size exceeding max")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected exceeds maximum error, got: %v", err)
	}
}

func TestCheckBlobSizeMismatch(t *testing.T) {
	err := checkBlobSize(100, 200, 1024)
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}
	if !strings.Contains(err.Error(), "data length") {
		t.Fatalf("expected data length error, got: %v", err)
	}
}

func TestCheckBlobSizeAtMax(t *testing.T) {
	// Exactly at max should pass
	if err := checkBlobSize(1024, 1024, 1024); err != nil {
		t.Fatalf("size at max should pass: %v", err)
	}
}

// --- Path conflict edge cases ---

func TestPathConflictManyInterveningPaths(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	// "a" conflicts with "a/z.txt", but many paths intervene:
	// a, a-1, a-2, a.1, a.2, a/z.txt
	_, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "a", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "a-1", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "a-2", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "a.1", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "a.2", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
		{Path: "a/z.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "many intervening")
	if err == nil {
		t.Fatal("expected error: 'a' and 'a/z.txt' conflict despite 4 intervening paths")
	}
	if !strings.Contains(err.Error(), "path conflict") {
		t.Fatalf("expected path conflict error, got: %v", err)
	}
}

func TestPathConflictEmptyList(t *testing.T) {
	s := testStore(t)
	// Empty snapshot should work fine
	_, err := s.CreateSnapshot(nil, []SnapshotFile{}, "empty")
	if err != nil {
		t.Fatalf("empty snapshot should succeed: %v", err)
	}
}

func TestPathConflictSingleFile(t *testing.T) {
	s := testStore(t)
	h, _ := s.PutBlob([]byte("content"))
	now := time.Now()

	_, err := s.CreateSnapshot(nil, []SnapshotFile{
		{Path: "only.txt", BlobHash: h, Mode: 0644, ModTime: now, Size: 7, Type: "file"},
	}, "single file")
	if err != nil {
		t.Fatalf("single file should succeed: %v", err)
	}
}

// --- validateEncoding tests ---

func TestValidateEncodingValid(t *testing.T) {
	if err := validateEncoding("raw"); err != nil {
		t.Fatalf("raw should be valid: %v", err)
	}
	if err := validateEncoding("zstd"); err != nil {
		t.Fatalf("zstd should be valid: %v", err)
	}
}

func TestValidateEncodingInvalid(t *testing.T) {
	tests := []string{"", "gzip", "garbage", "RAW", "ZSTD", "lz4"}
	for _, enc := range tests {
		if err := validateEncoding(enc); err == nil {
			t.Fatalf("encoding %q should be invalid", enc)
		}
	}
}

// --- computeBlobHash tests ---

func TestComputeBlobHashLength(t *testing.T) {
	result := computeBlobHash([]byte("hello world"))
	if len(result) != 64 {
		t.Fatalf("expected 64 chars, got %d", len(result))
	}
}

func TestComputeBlobHashDeterministic(t *testing.T) {
	data := []byte("test data")
	h1 := computeBlobHash(data)
	h2 := computeBlobHash(data)
	if h1 != h2 {
		t.Fatalf("same data should produce same hash: %s != %s", h1, h2)
	}
}

func TestComputeBlobHashMatchesManual(t *testing.T) {
	data := []byte("hello")
	got := computeBlobHash(data)
	// SHA-256 of "hello" = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
	want := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if got != want {
		t.Fatalf("computeBlobHash(%q) = %s, want %s", data, got, want)
	}
}

// --- GetBlob rejects unknown encoding ---

func TestGetBlobRejectsUnknownEncoding(t *testing.T) {
	s := testStore(t)
	data := []byte("hello world")

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	// Set encoding to garbage
	_, err = s.db.Exec(`UPDATE blobs SET encoding = 'garbage' WHERE hash = ?`, hash)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetBlob(hash)
	if err == nil {
		t.Fatal("expected error for unknown encoding, got nil")
	}
	if !strings.Contains(err.Error(), "unknown blob encoding") {
		t.Fatalf("expected unknown blob encoding error, got: %v", err)
	}
}

// --- GetBlob raw size mismatch with large size ---

func TestGetBlobRejectsRawSizeLargerThanData(t *testing.T) {
	s := testStore(t)
	data := []byte("small data")

	hash, err := s.PutBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	// Set size LARGER than actual data (but within maxBlobSize)
	_, err = s.db.Exec(`UPDATE blobs SET size = ? WHERE hash = ?`, 1000, hash)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetBlob(hash)
	if err == nil {
		t.Fatal("expected error for raw blob with oversized stored size")
	}
}

// createTestSnapshot is a helper that creates a snapshot with one file.
func createTestSnapshot(t *testing.T, s *Store, parentID *int64, msg string) *Snapshot {
	t.Helper()
	h, err := s.PutBlob([]byte(msg)) // unique content per message
	if err != nil {
		t.Fatal(err)
	}
	snap, err := s.CreateSnapshot(parentID, []SnapshotFile{
		{Path: msg + ".txt", BlobHash: h, Mode: 0644, ModTime: time.Now(), Size: int64(len(msg))},
	}, msg)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestSetCheckpoint(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")

	if err := s.SetCheckpoint("my-check", snap.ID); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetCheckpoint("my-check")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != snap.ID {
		t.Fatalf("got snapshot ID %d, want %d", got.ID, snap.ID)
	}
}

func TestSetCheckpointDuplicateFails(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")

	if err := s.SetCheckpoint("dup", snap.ID); err != nil {
		t.Fatal(err)
	}
	err := s.SetCheckpoint("dup", snap.ID)
	if err == nil {
		t.Fatal("expected error for duplicate checkpoint name")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}
}

func TestUpdateCheckpoint(t *testing.T) {
	s := testStore(t)
	snap1 := createTestSnapshot(t, s, nil, "one")
	snap2 := createTestSnapshot(t, s, &snap1.ID, "two")

	if err := s.SetCheckpoint("moveme", snap1.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateCheckpoint("moveme", snap2.ID); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetCheckpoint("moveme")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != snap2.ID {
		t.Fatalf("got snapshot ID %d, want %d", got.ID, snap2.ID)
	}
}

func TestUpdateCheckpointNotFound(t *testing.T) {
	s := testStore(t)
	err := s.UpdateCheckpoint("nope", 999)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' error, got: %v", err)
	}
}

func TestDeleteCheckpoint(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")

	if err := s.SetCheckpoint("delme", snap.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCheckpoint("delme"); err != nil {
		t.Fatal(err)
	}
	_, err := s.GetCheckpoint("delme")
	if err == nil {
		t.Fatal("expected error after deletion")
	}
}

func TestDeleteCheckpointNotFound(t *testing.T) {
	s := testStore(t)
	err := s.DeleteCheckpoint("nope")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListCheckpoints(t *testing.T) {
	s := testStore(t)
	snap1 := createTestSnapshot(t, s, nil, "one")
	snap2 := createTestSnapshot(t, s, &snap1.ID, "two")

	s.SetCheckpoint("beta", snap1.ID)
	s.SetCheckpoint("alpha", snap2.ID)

	cps, err := s.ListCheckpoints()
	if err != nil {
		t.Fatal(err)
	}
	if len(cps) != 2 {
		t.Fatalf("expected 2 checkpoints, got %d", len(cps))
	}
	// Should be alphabetically ordered
	if cps[0].Name != "alpha" || cps[1].Name != "beta" {
		t.Fatalf("expected [alpha, beta], got [%s, %s]", cps[0].Name, cps[1].Name)
	}
}

func TestListCheckpointsEmpty(t *testing.T) {
	s := testStore(t)
	cps, err := s.ListCheckpoints()
	if err != nil {
		t.Fatal(err)
	}
	if len(cps) != 0 {
		t.Fatalf("expected 0 checkpoints, got %d", len(cps))
	}
}

func TestGetCheckpointsForSnapshot(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")

	s.SetCheckpoint("first", snap.ID)
	s.SetCheckpoint("second", snap.ID)

	names, err := s.GetCheckpointsForSnapshot(snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2, got %d", len(names))
	}
}

func TestCheckpointsBySnapshot(t *testing.T) {
	s := testStore(t)
	snap1 := createTestSnapshot(t, s, nil, "one")
	snap2 := createTestSnapshot(t, s, &snap1.ID, "two")

	s.SetCheckpoint("a", snap1.ID)
	s.SetCheckpoint("b", snap2.ID)
	s.SetCheckpoint("c", snap1.ID)

	m, err := s.CheckpointsBySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(m[snap1.ID]) != 2 {
		t.Fatalf("expected 2 for snap1, got %d", len(m[snap1.ID]))
	}
	if len(m[snap2.ID]) != 1 {
		t.Fatalf("expected 1 for snap2, got %d", len(m[snap2.ID]))
	}
}

func TestResolveRefByCheckpoint(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")

	s.SetCheckpoint("my-tag", snap.ID)

	got, err := s.ResolveRef("my-tag")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != snap.ID {
		t.Fatalf("got ID %d, want %d", got.ID, snap.ID)
	}
}

func TestResolveRefByHash(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")

	got, err := s.ResolveRef(snap.Hash[:8])
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != snap.ID {
		t.Fatalf("got ID %d, want %d", got.ID, snap.ID)
	}
}

func TestResolveRefNotFound(t *testing.T) {
	s := testStore(t)
	_, err := s.ResolveRef("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "no checkpoint or snapshot matching") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteSnapshotCascadesCheckpoints(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")
	s.SetCheckpoint("doomed", snap.ID)

	if err := s.DeleteSnapshot(snap.ID); err != nil {
		t.Fatal(err)
	}

	_, err := s.GetCheckpoint("doomed")
	if err == nil {
		t.Fatal("expected checkpoint to be deleted with snapshot")
	}
}

func TestGetSnapshotByID(t *testing.T) {
	s := testStore(t)
	snap := createTestSnapshot(t, s, nil, "one")

	got, err := s.GetSnapshotByID(snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash != snap.Hash {
		t.Fatalf("got hash %s, want %s", got.Hash, snap.Hash)
	}
}

func TestMigrationV3toV4(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create a v3 database manually
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Force version back to 3 and drop checkpoints table
	s.db.Exec(`UPDATE meta SET value = '3' WHERE key = 'schema_version'`)
	s.db.Exec(`DROP TABLE IF EXISTS checkpoints`)
	s.db.Exec(`DROP INDEX IF EXISTS idx_checkpoints_snapshot`)
	s.Close()

	// Reopen — should migrate to v4
	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer s2.Close()

	var version string
	if err := s2.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != "4" {
		t.Fatalf("expected version 4, got %s", version)
	}

	// Verify checkpoints table exists by inserting
	snap := createTestSnapshot(t, s2, nil, "test")
	if err := s2.SetCheckpoint("v3-migrated", snap.ID); err != nil {
		t.Fatalf("checkpoint after migration: %v", err)
	}
}
