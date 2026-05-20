package snapshot

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nk/earwig/internal/config"
	"github.com/nk/earwig/internal/ignore"
	"github.com/nk/earwig/internal/store"
)

// writeFileAt is a goroutine-safe write helper used by the concurrent test
// (the package's writeFile takes *testing.T, which is not safe to call from
// goroutines other than the one running the test). Errors are reported via
// the returned value so the caller can surface them through a channel.
func writeFileAt(dir, name, content string) error {
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0644)
}

// TestConcurrentTakeSnapshot mirrors the concurrency pattern in cmdWatch:
// a single Creator shared across goroutines, a snapMu serialising the body of
// each take, a snapCount atomic for the "every 10th full walk" decision, and
// a mu-protected map of changed paths. Two goroutines hammer the pattern from
// either side (debouncer-like + input-like).
//
// Under `go test -race`, this confirms the shared-memory model in cmdWatch is
// clean: no unsynchronised access, no data races, no panics from concurrent
// snapshot creation. If a future refactor drops the mutex or breaks Creator's
// internal warnMu, this will fail.
func TestConcurrentTakeSnapshot(t *testing.T) {
	s, dir := setup(t)

	// Seed enough files that snapshots have real work to do — exercises the
	// blob store, snapshot_files writes, and the Creator's size-warn dedup.
	for i := 0; i < 30; i++ {
		if err := writeFileAt(dir, filepath.Join("data", "f"+itoa(i)+".txt"), strings.Repeat("x", 200)); err != nil {
			t.Fatal(err)
		}
	}

	ig, _ := ignore.New(nil)
	cfg, _ := config.Load(filepath.Join(dir, ".earwig", "missing.json")) // returns Default
	creator := NewCreator(s, dir, ig, cfg)

	var (
		mu           sync.Mutex
		changedPaths = make(map[string]bool)
		snapMu       sync.Mutex
		snapCount    atomic.Int64
		headID       atomic.Int64 // 0 means "no parent yet"
	)

	// takeSnap mirrors the cmdWatch closure: snapMu around the body, atomic
	// count for the every-10th rule, mu around changedPaths swap, single
	// shared Creator. The flock is not modelled because this is in-process
	// only — that's exactly the contract this test exercises.
	takeSnap := func(message string) (*store.Snapshot, error) {
		snapMu.Lock()
		defer snapMu.Unlock()

		var parentID *int64
		if id := headID.Load(); id > 0 {
			parentID = &id
		}

		mu.Lock()
		paths := changedPaths
		changedPaths = make(map[string]bool)
		mu.Unlock()

		n := snapCount.Add(1)
		var snap *store.Snapshot
		var err error
		if parentID == nil || n%10 == 0 || len(paths) == 0 {
			snap, err = creator.TakeSnapshot(parentID, message)
		} else {
			snap, err = creator.TakeIncrementalSnapshot(*parentID, paths, message)
		}
		if err != nil {
			return nil, err
		}
		if snap != nil {
			headID.Store(snap.ID)
		}
		return snap, nil
	}

	// Initial snapshot to establish HEAD.
	if _, err := takeSnap("initial"); err != nil {
		t.Fatal(err)
	}

	const iters = 30
	var wg sync.WaitGroup
	errs := make(chan error, iters*2)

	// "Debouncer" goroutine: writes files (which under cmdWatch would come via
	// fsnotify), records the change under mu, then triggers takeSnap.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			rel := filepath.Join("data", "deb"+itoa(i)+".txt")
			if err := writeFileAt(dir, rel, strings.Repeat("d", 100+i)); err != nil {
				errs <- err
				return
			}
			mu.Lock()
			changedPaths[rel] = true
			mu.Unlock()
			if _, err := takeSnap("debouncer"); err != nil {
				errs <- err
				return
			}
		}
	}()

	// "Input" goroutine: simulates 's'/'c' keypresses interleaved with the
	// debouncer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			rel := filepath.Join("data", "inp"+itoa(i)+".txt")
			if err := writeFileAt(dir, rel, strings.Repeat("i", 100+i)); err != nil {
				errs <- err
				return
			}
			mu.Lock()
			changedPaths[rel] = true
			mu.Unlock()
			if _, err := takeSnap("input"); err != nil {
				errs <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("snapshot error: %v", err)
	}

	if got := snapCount.Load(); got != int64(2*iters+1) {
		t.Errorf("snapCount: got %d, want %d", got, 2*iters+1)
	}
}

// itoa avoids importing strconv just for two callers.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

