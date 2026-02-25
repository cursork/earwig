package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nk/earwig/internal/ignore"
	"github.com/nk/earwig/internal/snapshot"
	"github.com/nk/earwig/internal/store"
	"github.com/nk/earwig/internal/watcher"
)

var commands = map[string]func([]string) error{
	"init":     cmdInit,
	"snapshot": cmdSnapshot,
	"log":      cmdLog,
	"show":     cmdShow,
	"watch":    cmdWatch,
	"restore":  cmdRestore,
	"gc":       cmdGC,
	"forget":   cmdForget,
	"_files":   cmdFiles,
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd, ok := commands[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}

	if err := cmd(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: earwig <command> [args]\n\nCommands:\n")
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if strings.HasPrefix(name, "_") {
			continue
		}
		fmt.Fprintf(os.Stderr, "  %s\n", name)
	}
}

// findRoot walks up from cwd looking for .earwig/
func findRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cwd := dir
	levels := 0
	for {
		if _, err := os.Stat(filepath.Join(dir, ".earwig")); err == nil {
			if levels > 2 {
				rel, _ := filepath.Rel(dir, cwd)
				fmt.Fprintf(os.Stderr, "warning: earwig root is %d levels above cwd (%s from %s)\n", levels, dir, rel)
			}
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("not an earwig directory (or any parent): .earwig not found")
		}
		dir = parent
		levels++
	}
}

func openStore() (*store.Store, string, error) {
	root, err := findRoot()
	if err != nil {
		return nil, "", err
	}
	checkRestoreRecovery(root)
	s, err := store.Open(filepath.Join(root, ".earwig", "earwig.db"))
	if err != nil {
		return nil, "", err
	}
	return s, root, nil
}

// checkRestoreRecovery warns if a previous restore was interrupted.
func checkRestoreRecovery(root string) {
	markerPath := filepath.Join(root, ".earwig", "RESTORING")
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return
	}
	hash := strings.TrimSpace(string(data))
	fmt.Fprintf(os.Stderr, "warning: a previous restore was interrupted. Pre-restore state saved as snapshot %s.\n", hash)
	fmt.Fprintf(os.Stderr, "Run 'earwig restore %s' to recover, or delete .earwig/RESTORING to dismiss.\n", hash)
}

func loadIgnore(root string) (*ignore.Matcher, error) {
	var files []string
	for _, name := range []string{filepath.Join(".earwig", "ignore"), ".gitignore"} {
		p := filepath.Join(root, name)
		if _, err := os.Stat(p); err == nil {
			files = append(files, p)
		}
	}
	return ignore.New(files)
}

func cmdInit(args []string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	earwigDir := filepath.Join(cwd, ".earwig")
	if _, err := os.Stat(earwigDir); err == nil {
		return fmt.Errorf("earwig already initialized in %s", cwd)
	}

	if err := os.MkdirAll(earwigDir, 0700); err != nil {
		return err
	}

	s, err := store.Open(filepath.Join(earwigDir, "earwig.db"))
	if err != nil {
		os.RemoveAll(earwigDir)
		return fmt.Errorf("creating database: %w", err)
	}
	s.Close()

	fmt.Printf("Initialized earwig in %s\n", cwd)
	return nil
}

func cmdSnapshot(args []string) error {
	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	ig, err := loadIgnore(root)
	if err != nil {
		return err
	}

	parentID, err := readHead(root, s)
	if err != nil {
		return err
	}

	c := snapshot.NewCreator(s, root, ig)
	snap, err := c.TakeSnapshot(parentID, "manual")
	if err != nil {
		return err
	}
	if snap == nil {
		fmt.Println("No changes to snapshot.")
		return nil
	}

	if err := writeHead(root, snap.ID); err != nil {
		return fmt.Errorf("writing HEAD: %w", err)
	}
	fmt.Printf("Snapshot %s\n", shortHash(snap.Hash))
	return nil
}

func cmdLog(args []string) error {
	s, _, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	snapshots, err := s.ListSnapshots()
	if err != nil {
		return err
	}

	if len(snapshots) == 0 {
		fmt.Println("No snapshots yet.")
		return nil
	}

	// Build children map to detect branches
	children := make(map[int64][]int64)
	for _, snap := range snapshots {
		if snap.ParentID != nil {
			children[*snap.ParentID] = append(children[*snap.ParentID], snap.ID)
		}
	}

	for i := len(snapshots) - 1; i >= 0; i-- {
		snap := snapshots[i]
		branchMark := ""
		if snap.ParentID != nil {
			if siblings := children[*snap.ParentID]; len(siblings) > 1 {
				branchMark = " (branch)"
			}
		}
		fmt.Printf("* %s  %s  %s%s\n",
			shortHash(snap.Hash),
			snap.CreatedAt.Format("2006-01-02 15:04:05"),
			snap.Message,
			branchMark,
		)
	}
	return nil
}

func cmdShow(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: earwig show <hash>")
	}

	s, _, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	snap, err := s.GetSnapshot(args[0])
	if err != nil {
		return err
	}

	fmt.Printf("Snapshot %s\n", shortHash(snap.Hash))
	fmt.Printf("Date:    %s\n", snap.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("Message: %s\n\n", snap.Message)

	if snap.ParentID == nil {
		files, err := s.GetSnapshotFiles(snap.ID)
		if err != nil {
			return err
		}
		for _, f := range files {
			fmt.Printf("  A %s\n", f.Path)
		}
		return nil
	}

	changes, err := s.DiffSnapshots(*snap.ParentID, snap.ID)
	if err != nil {
		return err
	}

	if len(changes) == 0 {
		fmt.Println("  (no changes)")
		return nil
	}

	for _, c := range changes {
		switch c.Type {
		case store.ChangeAdded:
			fmt.Printf("  A %s\n", c.Path)
		case store.ChangeModified:
			fmt.Printf("  M %s\n", c.Path)
		case store.ChangeDeleted:
			fmt.Printf("  D %s\n", c.Path)
		}
	}
	return nil
}

func cmdFiles(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: earwig _files <hash>")
	}

	s, _, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	snap, err := s.GetSnapshot(args[0])
	if err != nil {
		return err
	}

	files, err := s.GetSnapshotFiles(snap.ID)
	if err != nil {
		return err
	}

	for _, f := range files {
		fmt.Printf("%s\t%d\t%s\t%s\n", f.Path, f.Size, f.BlobHash, f.Type)
	}
	return nil
}

// HEAD tracking

func readHead(root string, s *store.Store) (*int64, error) {
	headPath := filepath.Join(root, ".earwig", "HEAD")
	data, err := os.ReadFile(headPath)
	if err == nil {
		idStr := strings.TrimSpace(string(data))
		var id int64
		if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
			return nil, fmt.Errorf("corrupt HEAD file (content: %q): %w", idStr, err)
		}
		return &id, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading HEAD: %w", err)
	}
	// No HEAD file — fall back to latest snapshot
	latest, err := s.GetLatestSnapshot()
	if err != nil {
		return nil, err
	}
	if latest != nil {
		return &latest.ID, nil
	}
	return nil, nil
}

func writeHead(root string, id int64) error {
	headPath := filepath.Join(root, ".earwig", "HEAD")
	tmpPath := fmt.Sprintf("%s.tmp.%d", headPath, os.Getpid())
	if err := os.WriteFile(tmpPath, []byte(fmt.Sprintf("%d", id)), 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, headPath)
}

// File lock: prevents watcher from snapshotting during restore.
// Uses syscall.Flock for real mutual exclusion — no TOCTOU race.
// The flock file is persistent (never removed) so it can always be locked.

func flockPath(root string) string {
	return filepath.Join(root, ".earwig", "flock")
}

// acquireFlock acquires an exclusive file lock on .earwig/flock.
// If blocking is true, waits until the lock is available.
// Returns the locked file (caller must Close() to release) or nil if
// non-blocking and the lock is held by another process.
func acquireFlock(root string, blocking bool) (*os.File, error) {
	p := flockPath(root)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening flock: %w", err)
	}
	how := syscall.LOCK_EX
	if !blocking {
		how |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		f.Close()
		if !blocking && (errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)) {
			return nil, nil // lock held by another process
		}
		return nil, fmt.Errorf("acquiring flock: %w", err)
	}
	return f, nil
}

func cmdWatch(args []string) error {
	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	ig, err := loadIgnore(root)
	if err != nil {
		return err
	}

	var (
		mu           sync.Mutex
		changedPaths = make(map[string]bool)
		snapCount    int
	)

	takeSnap := func() {
		// Acquire flock non-blocking — if restore holds it, skip this cycle.
		flockFile, err := acquireFlock(root, false)
		if err != nil {
			log.Printf("error acquiring flock: %v", err)
			return
		}
		if flockFile == nil {
			return // restore in progress
		}
		defer flockFile.Close()

		parentID, err := readHead(root, s)
		if err != nil {
			log.Printf("error reading HEAD: %v", err)
			return
		}

		c := snapshot.NewCreator(s, root, ig)

		var snap *store.Snapshot

		// Swap out changed paths
		mu.Lock()
		paths := changedPaths
		changedPaths = make(map[string]bool)
		mu.Unlock()

		// Every 10th snapshot or if no parent, do a full walk for consistency
		snapCount++
		if parentID == nil || snapCount%10 == 0 || len(paths) == 0 {
			snap, err = c.TakeSnapshot(parentID, "auto")
		} else {
			snap, err = c.TakeIncrementalSnapshot(*parentID, paths, "auto")
		}
		if err != nil {
			log.Printf("error taking snapshot: %v", err)
			return
		}
		if snap == nil {
			return // No changes
		}

		if err := writeHead(root, snap.ID); err != nil {
			log.Printf("error writing HEAD: %v", err)
			return
		}
		fmt.Printf("[%s] Snapshot %s\n", snap.CreatedAt.Format("15:04:05"), shortHash(snap.Hash))
	}

	// Initial snapshot (always full walk)
	takeSnap()

	debouncer := watcher.NewDebouncer(1 * time.Minute)
	defer debouncer.Stop()

	w, err := watcher.New(root, ig)
	if err != nil {
		return err
	}
	defer w.Close()

	w.OnEvent = func(relPath string) {
		mu.Lock()
		changedPaths[relPath] = true
		mu.Unlock()
		debouncer.Trigger(takeSnap)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Watching %s for changes (Ctrl+C to stop)\n", root)
	return w.Run(ctx)
}

func cmdRestore(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: earwig restore <hash>")
	}

	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	snap, err := s.GetSnapshot(args[0])
	if err != nil {
		return err
	}

	ig, err := loadIgnore(root)
	if err != nil {
		return err
	}

	// Acquire exclusive flock — blocks until watcher snapshot finishes.
	flockFile, err := acquireFlock(root, true)
	if err != nil {
		return fmt.Errorf("acquiring lock: %w", err)
	}
	defer flockFile.Close()

	// Auto-snapshot current state before restore so the user can undo
	parentID, err := readHead(root, s)
	if err != nil {
		return err
	}
	c := snapshot.NewCreator(s, root, ig)
	preSnap, err := c.TakeSnapshot(parentID, "pre-restore")
	if err != nil {
		return fmt.Errorf("pre-restore snapshot: %w", err)
	}
	if preSnap != nil {
		if err := writeHead(root, preSnap.ID); err != nil {
			return fmt.Errorf("writing HEAD: %w", err)
		}
		fmt.Printf("Saved current state as %s\n", shortHash(preSnap.Hash))
	}

	// Write RESTORING marker so a crash midway can be detected on next run.
	restoreMarker := filepath.Join(root, ".earwig", "RESTORING")
	if preSnap != nil {
		if err := os.WriteFile(restoreMarker, []byte(shortHash(preSnap.Hash)), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not write crash recovery marker: %v\n", err)
		}
	}

	restorer := snapshot.NewRestorer(s, root, ig)
	if err := restorer.Restore(snap.ID); err != nil {
		return err
	}

	// Restore succeeded — remove the marker.
	os.Remove(restoreMarker)

	if err := writeHead(root, snap.ID); err != nil {
		return fmt.Errorf("writing HEAD: %w", err)
	}
	fmt.Printf("Restored to snapshot %s (%s)\n", shortHash(snap.Hash), snap.CreatedAt.Format("2006-01-02 15:04:05"))
	return nil
}

func cmdForget(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: earwig forget <hash>")
	}

	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	flockFile, err := acquireFlock(root, true)
	if err != nil {
		return fmt.Errorf("acquiring lock: %w", err)
	}
	defer flockFile.Close()

	snap, err := s.GetSnapshot(args[0])
	if err != nil {
		return err
	}

	// Don't allow forgetting the current HEAD snapshot.
	headID, err := readHead(root, s)
	if err != nil {
		return err
	}
	if headID != nil && *headID == snap.ID {
		return fmt.Errorf("cannot forget the current HEAD snapshot")
	}

	if err := s.DeleteSnapshot(snap.ID); err != nil {
		return err
	}
	fmt.Printf("Forgot snapshot %s\n", shortHash(snap.Hash))

	// Run GC to clean up any blobs orphaned by this deletion.
	count, err := s.GarbageCollect()
	if err != nil {
		return err
	}
	if count > 0 {
		fmt.Printf("Removed %d orphaned blob(s).\n", count)
	}
	return nil
}

func cmdGC(args []string) error {
	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	flockFile, err := acquireFlock(root, true)
	if err != nil {
		return fmt.Errorf("acquiring lock: %w", err)
	}
	defer flockFile.Close()

	count, err := s.GarbageCollect()
	if err != nil {
		return err
	}
	if count == 0 {
		fmt.Println("No orphaned blobs.")
	} else {
		fmt.Printf("Removed %d orphaned blob(s).\n", count)
	}
	return nil
}
