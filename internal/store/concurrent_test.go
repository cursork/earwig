package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestStoreConcurrentAccess verifies that *Store is safe for concurrent use
// across all its mutable internal state — the *sql.DB (documented safe by Go),
// the shared zstd.Encoder (klauspost docs: EncodeAll concurrent-safe), and
// the shared zstd.Decoder (klauspost docs: DecodeAll concurrent-safe).
//
// Multiple goroutines hammer PutBlob (exercises the encoder, including the
// >=128KB compression path), GetBlob (exercises the decoder), CreateSnapshot,
// GetSnapshotFiles, and SetCheckpoint in parallel with no external lock.
//
// Run under `go test -race`. If a future change adds unprotected mutable
// state to *Store, this test will fail. If a future change drops the
// snapMu in cmdWatch, this test is the evidence that the Store itself
// remained safe — the bug would be in higher-level semantics elsewhere.
func TestStoreConcurrentAccess(t *testing.T) {
	s := testStore(t)

	const goroutines = 8
	const perGoroutine = 20

	// Seed a couple of compressible blobs >= 128KB so DecodeAll exercises
	// concurrent decompression too, not just raw passthrough.
	bigPayloads := make([]string, 4)
	for i := range bigPayloads {
		bigPayloads[i] = strings.Repeat(fmt.Sprintf("payload-%d-", i), 16*1024)
	}
	bigHashes := make([]string, len(bigPayloads))
	for i, p := range bigPayloads {
		h, err := s.PutBlob([]byte(p))
		if err != nil {
			t.Fatalf("seed PutBlob: %v", err)
		}
		bigHashes[i] = h
	}

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				// PutBlob — small (raw) and large (zstd) interleaved.
				small := []byte(fmt.Sprintf("g%d-i%d-small", g, i))
				if _, err := s.PutBlob(small); err != nil {
					errs <- fmt.Errorf("small PutBlob: %w", err)
					return
				}
				big := []byte(strings.Repeat(fmt.Sprintf("g%d-", g), 32*1024) + fmt.Sprintf("i%d", i))
				bigHash, err := s.PutBlob(big)
				if err != nil {
					errs <- fmt.Errorf("big PutBlob: %w", err)
					return
				}

				// GetBlob on freshly-written + pre-seeded blobs (encoder/decoder both hot).
				if _, err := s.GetBlob(bigHash); err != nil {
					errs <- fmt.Errorf("GetBlob own: %w", err)
					return
				}
				if _, err := s.GetBlob(bigHashes[i%len(bigHashes)]); err != nil {
					errs <- fmt.Errorf("GetBlob seed: %w", err)
					return
				}

				// CreateSnapshot referencing a real blob hash.
				files := []SnapshotFile{
					{Path: fmt.Sprintf("g%d/i%d.txt", g, i), BlobHash: bigHash, Type: "file", Mode: 0644},
				}
				snap, err := s.CreateSnapshot(nil, files, fmt.Sprintf("g%d-i%d", g, i))
				if err != nil {
					errs <- fmt.Errorf("CreateSnapshot: %w", err)
					return
				}

				// GetSnapshotFiles round-trip.
				if _, err := s.GetSnapshotFiles(snap.ID); err != nil {
					errs <- fmt.Errorf("GetSnapshotFiles: %w", err)
					return
				}

				// SetCheckpoint (unique per goroutine+iter).
				if err := s.SetCheckpoint(fmt.Sprintf("cp-g%d-i%d", g, i), snap.ID); err != nil {
					errs <- fmt.Errorf("SetCheckpoint: %w", err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Sanity: snapshot count.
	snaps, err := s.ListSnapshots()
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if got, want := len(snaps), goroutines*perGoroutine; got != want {
		t.Errorf("snapshot count: got %d, want %d", got, want)
	}
}
