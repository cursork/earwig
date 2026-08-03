package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pmezard/go-difflib/difflib"
	"golang.org/x/sys/unix"

	"github.com/nk/earwig/internal/config"
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
	"diff":     cmdDiff,
	"gc":       cmdGC,
	"forget":   cmdForget,
	"check":     cmdCheck,
	"checks":    cmdChecks,
	"grep":      cmdGrep,
	"tui":       cmdTUI,
	"processes": cmdProcesses,
	"db":        cmdDB,
	"_files":    cmdFiles,
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
	fmt.Fprintf(os.Stderr, `usage: earwig <command> [args]

Commands:
  init                    Create .earwig/ and database
  watch [-detach]         Watch for changes and auto-snapshot
  snapshot                Take a manual snapshot
  log [file]              Show snapshot history (optionally filter by file)
  show <hash> [file...]   Show changes in a snapshot, or print file contents
  diff <hash>             Show what a restore would change vs current state
  restore [-y] <hash>     Restore filesystem to a snapshot
  grep <pattern> [glob]   Search file contents across snapshots
  check [name] [hash]     Create a checkpoint (random name if omitted)
  check -d <name>         Delete a checkpoint
  check -u <name> [hash]  Move a checkpoint to a different snapshot
  checks                  List all checkpoints
  forget <hash>           Delete a snapshot (re-parents children, runs GC)
  gc                      Remove orphaned blobs
  tui                     Interactive snapshot browser
  processes               List running earwig watchers
  db [sql]                Open SQLite shell, or run a query
`)
}

// findRootFrom walks up from startDir looking for a .earwig/ directory and
// returns the directory that contains it plus how many levels up it was found.
// Quiet (no warnings), so it can also resolve another process's watched root
// during the single-watcher startup check.
func findRootFrom(startDir string) (string, int, error) {
	dir := startDir
	levels := 0
	for {
		if _, err := os.Stat(filepath.Join(dir, ".earwig")); err == nil {
			return dir, levels, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", 0, fmt.Errorf("not an earwig directory (or any parent): .earwig not found")
		}
		dir = parent
		levels++
	}
}

// findRoot walks up from cwd looking for .earwig/ and warns if the root is more
// than two levels above cwd (scope-amplification risk).
func findRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir, levels, err := findRootFrom(cwd)
	if err != nil {
		return "", err
	}
	if levels > 2 {
		rel, _ := filepath.Rel(dir, cwd)
		fmt.Fprintf(os.Stderr, "warning: earwig root is %d levels above cwd (%s from %s)\n", levels, dir, rel)
	}
	return dir, nil
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

func loadConfig(root string) (*config.Config, error) {
	return config.Load(filepath.Join(root, ".earwig", "config.json"))
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
		if rmErr := os.RemoveAll(earwigDir); rmErr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not clean up %s: %v\n", earwigDir, rmErr)
		}
		return fmt.Errorf("creating database: %w", err)
	}
	if err := s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: closing database: %v\n", err)
	}

	addToGitExclude(cwd)

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

	cfg, err := loadConfig(root)
	if err != nil {
		return err
	}

	parentID, err := readHead(root, s)
	if err != nil {
		return err
	}

	c := snapshot.NewCreator(s, root, ig, cfg)
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
	s, root, err := openStore()
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

	headID, err := readHead(root, s)
	if err != nil {
		return err
	}

	// Load checkpoint names for display
	cpMap, err := s.CheckpointsBySnapshot()
	if err != nil {
		return err
	}

	// File filter mode: earwig log <file>
	if len(args) > 0 {
		filterPath := filepath.ToSlash(args[0])
		return cmdLogFiltered(s, snapshots, headID, cpMap, filterPath)
	}

	// Graph state: each column tracks which snapshot ID it's tracing toward.
	// 0 means the slot is free.
	var columns []int64

	// Pre-seed column 0 with HEAD so its lineage stays leftmost.
	if headID != nil {
		columns = []int64{*headID}
	}

	// Process newest-first
	for i := len(snapshots) - 1; i >= 0; i-- {
		snap := snapshots[i]

		// Find all columns targeting this snapshot
		var matchCols []int
		for c, target := range columns {
			if target == snap.ID {
				matchCols = append(matchCols, c)
			}
		}

		// If no column tracks this snapshot, it's a branch tip — allocate a column
		if len(matchCols) == 0 {
			col := -1
			for c, target := range columns {
				if target == 0 {
					col = c
					break
				}
			}
			if col == -1 {
				col = len(columns)
				columns = append(columns, 0)
			}
			columns[col] = snap.ID
			matchCols = []int{col}
		}

		ownCol := matchCols[0]

		// If multiple columns converge here, draw merge separator line(s).
		// Each row moves merging columns one position closer to ownCol.
		if len(matchCols) > 1 {
			extraCols := matchCols[1:]
			// Animate: each step moves every extra column one position left
			maxDist := 0
			for _, mc := range extraCols {
				if d := mc - ownCol; d > maxDist {
					maxDist = d
				}
			}
			for step := 1; step <= maxDist; step++ {
				fmt.Println(strings.TrimRight(
					drawMergeLine(columns, ownCol, extraCols, step),
					" "))
			}
			// Free the extra columns
			for _, mc := range extraCols {
				columns[mc] = 0
			}
		}

		// Trim trailing empty columns before drawing commit line
		for len(columns) > 0 && columns[len(columns)-1] == 0 {
			columns = columns[:len(columns)-1]
		}

		// Build the commit line graph prefix
		prefix := drawGraphPrefix(columns, ownCol)

		// Build A/M/D change summary
		changeSummary := changeSummaryFor(s, &snap)

		// Checkpoint names
		cpLabel := ""
		if names, ok := cpMap[snap.ID]; ok {
			cpLabel = "  (" + strings.Join(names, ", ") + ")"
		}

		// HEAD marker
		headMark := ""
		if headID != nil && snap.ID == *headID {
			headMark = "  <- HERE"
		}

		fmt.Printf("%s%s  %s  %s%s%s%s\n",
			prefix,
			shortHash(snap.Hash),
			snap.CreatedAt.Format("2006-01-02 15:04:05"),
			snap.Message,
			changeSummary,
			cpLabel,
			headMark,
		)

		// Update the kept column to trace toward this snapshot's parent
		if snap.ParentID != nil {
			columns[ownCol] = *snap.ParentID
		} else {
			columns[ownCol] = 0 // root — free the column
		}

		// Trim trailing empty columns
		for len(columns) > 0 && columns[len(columns)-1] == 0 {
			columns = columns[:len(columns)-1]
		}
	}
	return nil
}

// cmdLogFiltered shows a flat list of snapshots that changed the given file.
func cmdLogFiltered(s *store.Store, snapshots []store.Snapshot, headID *int64, cpMap map[int64][]string, filterPath string) error {
	found := false
	// Process newest-first
	for i := len(snapshots) - 1; i >= 0; i-- {
		snap := snapshots[i]

		if !snapshotTouchesFile(s, &snap, filterPath) {
			continue
		}
		found = true

		cpLabel := ""
		if names, ok := cpMap[snap.ID]; ok {
			cpLabel = "  (" + strings.Join(names, ", ") + ")"
		}

		headMark := ""
		if headID != nil && snap.ID == *headID {
			headMark = "  <- HERE"
		}

		changeSummary := changeSummaryFor(s, &snap)

		fmt.Printf("* %s  %s  %s%s%s%s\n",
			shortHash(snap.Hash),
			snap.CreatedAt.Format("2006-01-02 15:04:05"),
			snap.Message,
			changeSummary,
			cpLabel,
			headMark,
		)
	}
	if !found {
		fmt.Printf("No snapshots touching %s\n", filterPath)
	}
	return nil
}

// snapshotTouchesFile returns true if the snapshot added, modified, or deleted the given file.
func snapshotTouchesFile(s *store.Store, snap *store.Snapshot, path string) bool {
	if snap.ParentID == nil {
		// Root snapshot — file is touched if it exists in the snapshot
		files, err := s.GetSnapshotFiles(snap.ID)
		if err != nil {
			return false
		}
		for _, f := range files {
			if f.Path == path {
				return true
			}
		}
		return false
	}
	changes, err := s.DiffSnapshots(*snap.ParentID, snap.ID)
	if err != nil {
		return false
	}
	for _, c := range changes {
		if c.Path == path {
			return true
		}
	}
	return false
}

// drawGraphPrefix builds the "* | | " prefix for a commit line.
func drawGraphPrefix(columns []int64, ownCol int) string {
	var b strings.Builder
	for c := 0; c < len(columns); c++ {
		if c == ownCol {
			b.WriteByte('*')
		} else if columns[c] != 0 {
			b.WriteByte('|')
		} else {
			b.WriteByte(' ')
		}
		b.WriteByte(' ')
	}
	return b.String()
}

// drawMergeLine draws one row of the merge animation.
// Each extra column moves `step` positions to the left toward ownCol.
// Uses character-level positioning so "/" appears right next to "|".
func drawMergeLine(columns []int64, ownCol int, extraCols []int, step int) string {
	width := len(columns)
	// Total character positions: each column gets 2 chars (symbol + space)
	chars := make([]byte, width*2)
	for i := range chars {
		chars[i] = ' '
	}
	// Draw continuing columns
	for c := 0; c < width; c++ {
		if columns[c] != 0 {
			isExtra := false
			for _, mc := range extraCols {
				if mc == c {
					isExtra = true
					break
				}
			}
			if !isExtra {
				chars[c*2] = '|'
			}
		}
	}
	// Draw the merging "/" indicators at their animated positions
	for _, mc := range extraCols {
		pos := mc - step
		if pos <= ownCol {
			pos = ownCol
		}
		// Place "/" at the character position. If pos == ownCol, it goes at
		// ownCol*2+1 (right after the "|") to get the "|\/" look.
		// Otherwise at pos*2 to show diagonal movement.
		if pos == ownCol {
			chars[ownCol*2+1] = '/'
		} else {
			chars[pos*2] = '/'
		}
	}
	return string(chars)
}

func changeSummaryFor(s *store.Store, snap *store.Snapshot) string {
	var parts []string
	if snap.ParentID == nil {
		files, err := s.GetSnapshotFiles(snap.ID)
		if err != nil {
			return ""
		}
		for _, f := range files {
			parts = append(parts, "A "+filepath.Base(f.Path))
		}
	} else {
		changes, err := s.DiffSnapshots(*snap.ParentID, snap.ID)
		if err != nil {
			return ""
		}
		for _, c := range changes {
			var prefix string
			switch c.Type {
			case store.ChangeAdded:
				prefix = "A"
			case store.ChangeModified:
				prefix = "M"
			case store.ChangeDeleted:
				prefix = "D"
			}
			parts = append(parts, prefix+" "+filepath.Base(c.Path))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	summary := "  [" + strings.Join(parts, ", ")
	const maxLen = 50
	if len(summary) > maxLen {
		summary = summary[:maxLen-3] + "..."
	}
	summary += "]"
	return summary
}

func cmdShow(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: earwig show <hash> [file ...]")
	}

	s, _, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	snap, err := s.ResolveRef(args[0])
	if err != nil {
		return err
	}

	// earwig show <hash> <file> [file ...] — print file contents
	if len(args) > 1 {
		return showFiles(s, snap, args[1:])
	}

	// earwig show <hash> — summary
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

func showFiles(s *store.Store, snap *store.Snapshot, paths []string) error {
	files, err := s.GetSnapshotFiles(snap.ID)
	if err != nil {
		return err
	}
	fileMap := make(map[string]store.SnapshotFile, len(files))
	for _, f := range files {
		fileMap[f.Path] = f
	}

	multi := len(paths) > 1
	for i, path := range paths {
		path = filepath.ToSlash(path)
		f, ok := fileMap[path]
		if !ok {
			return fmt.Errorf("file not found in snapshot: %s", path)
		}
		data, err := s.GetBlob(f.BlobHash)
		if err != nil {
			return err
		}
		if multi && i > 0 {
			fmt.Println()
		}
		if multi {
			fmt.Printf("==> %s <==\n", path)
		}
		os.Stdout.Write(data)
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

	snap, err := s.ResolveRef(args[0])
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

func cmdDB(args []string) error {
	root, err := findRoot()
	if err != nil {
		return err
	}
	dbPath := filepath.Join(root, ".earwig", "earwig.db")

	sqlite3, err := exec.LookPath("sqlite3")
	if err != nil {
		return fmt.Errorf("sqlite3 not found in PATH")
	}

	if len(args) == 0 {
		// Interactive: exec sqlite3 (replaces process)
		return syscall.Exec(sqlite3, []string{"sqlite3", dbPath}, os.Environ())
	}

	// Non-interactive: run query
	cmd := exec.Command(sqlite3, dbPath, strings.Join(args, " "))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
		if closeErr := f.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "warning: closing flock file: %v\n", closeErr)
		}
		if !blocking && (errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)) {
			return nil, nil // lock held by another process
		}
		return nil, fmt.Errorf("acquiring flock: %w", err)
	}
	return f, nil
}

func cmdWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	detach := fs.Bool("detach", false, "run watcher in background (survives terminal close)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *detach {
		return detachWatcher()
	}

	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	// One watcher per directory. A second watcher is harmless but pointless, so
	// refuse to start and point at the one already running. Best-effort (ps-based,
	// so slightly racy on simultaneous starts) — acceptable for a not-ideal case.
	if pid, running := watcherAlreadyRunning(root); running {
		return fmt.Errorf("another earwig watcher is already running in %s (PID %d); only one watcher per directory is supported", root, pid)
	}

	ig, err := loadIgnore(root)
	if err != nil {
		return err
	}

	cfg, err := loadConfig(root)
	if err != nil {
		return err
	}

	// Interactive cbreak-mode keystrokes when stdin is a TTY. cbreak (not full
	// raw) disables line buffering + echo but keeps OPOST and ISIG on, so
	// snapshot/warning output still renders normally and Ctrl+C still signals.
	var (
		oldTermios *unix.Termios
		outMu      sync.Mutex // serializes prints + terminal-mode transitions
	)
	stdinFd := int(os.Stdin.Fd())
	if isTerminal(stdinFd) {
		if old, err := makeCBreak(stdinFd); err == nil {
			oldTermios = old
			defer restoreTermios(stdinFd, oldTermios)
		}
	}
	interactive := oldTermios != nil

	var (
		mu           sync.Mutex
		changedPaths = make(map[string]bool)
		snapCount    atomic.Int64
		// snapMu serialises in-process takeSnap calls between the debouncer
		// goroutine and the input goroutine. Distinct from the flock, which
		// is cross-process (vs. `earwig restore` in another shell). Holding
		// snapMu also makes safe our use of the shared snapshot.Creator —
		// only one goroutine is inside Creator.Take(Incremental)Snapshot at
		// a time, so its non-Creator-owned state (the *store.Store) doesn't
		// need its own locking discipline here.
		snapMu sync.Mutex
	)

	// One Creator for the whole watcher lifetime so per-path warning dedup
	// (Creator.warnedSizes) actually persists across snapshots.
	creator := snapshot.NewCreator(s, root, ig, cfg)

	takeSnap := func() *store.Snapshot {
		// In-process: only one snapshot at a time. Cross-process: flock
		// keeps us out of restore's way. Mutex first; if it's contended
		// (other goroutine running a snapshot now), we wait for that to
		// finish — by which point changedPaths will have been swapped out
		// and our walk will find genuinely-no-changes if there are none.
		// That's better than the previous bail-out behaviour, which made
		// 's' under contention print "(no changes)" misleadingly.
		snapMu.Lock()
		defer snapMu.Unlock()

		// Acquire flock non-blocking — if restore holds it, skip this cycle.
		flockFile, err := acquireFlock(root, false)
		if err != nil {
			log.Printf("error acquiring flock: %v", err)
			return nil
		}
		if flockFile == nil {
			return nil // restore in progress
		}
		defer flockFile.Close()

		parentID, err := readHead(root, s)
		if err != nil {
			log.Printf("error reading HEAD: %v", err)
			return nil
		}

		var snap *store.Snapshot

		// Swap out changed paths
		mu.Lock()
		paths := changedPaths
		changedPaths = make(map[string]bool)
		mu.Unlock()

		// Every 10th snapshot or if no parent, do a full walk for consistency
		n := snapCount.Add(1)
		if parentID == nil || n%10 == 0 || len(paths) == 0 {
			snap, err = creator.TakeSnapshot(parentID, "auto")
		} else {
			snap, err = creator.TakeIncrementalSnapshot(*parentID, paths, "auto")
		}
		if err != nil {
			log.Printf("error taking snapshot: %v", err)
			return nil
		}
		if snap == nil {
			return nil // No changes
		}

		if err := writeHead(root, snap.ID); err != nil {
			log.Printf("error writing HEAD: %v", err)
			return nil
		}
		return snap
	}

	printSnap := func(snap *store.Snapshot) {
		if snap == nil {
			return
		}
		fmt.Printf("[%s] Snapshot %s\n", snap.CreatedAt.Format("15:04:05"), shortHash(snap.Hash))
	}

	// Initial snapshot (always full walk)
	if snap := takeSnap(); snap != nil {
		outMu.Lock()
		printSnap(snap)
		outMu.Unlock()
	}

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
		debouncer.Trigger(func() {
			snap := takeSnap()
			if snap != nil {
				outMu.Lock()
				printSnap(snap)
				outMu.Unlock()
			}
		})
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Clean up PID file on shutdown (best-effort, may not exist)
	defer os.Remove(filepath.Join(root, ".earwig", "PID"))

	hint := "  (Ctrl+C to stop)"
	if interactive {
		hint = "  [c]heckpoint [s]napshot [l]og [t]ui [/]grep [?]help [q]uit"
	}
	fmt.Printf("Watching %s%s\n", root, hint)

	inputDone := make(chan struct{})
	if interactive {
		go func() {
			defer close(inputDone)
			runWatchInput(ctx, stdinFd, oldTermios, &outMu, s, root, takeSnap, printSnap, cancel)
		}()
	} else {
		close(inputDone)
	}

	runErr := w.Run(ctx)

	// Wait for the input dispatcher to exit before our deferred closes
	// (s.Close, restoreTermios, debouncer.Stop) run. The dispatcher holds
	// references to s and creator — without this wait, a key processed
	// between ctx-cancel and process exit could touch a closed store.
	<-inputDone
	return runErr
}

// readLineFromStdin reads bytes from os.Stdin one at a time until newline or
// EOF and returns the trimmed line. Used during the '/' prompt instead of
// bufio.NewReader, which would buffer extra bytes past the newline and lose
// them when its scope ends — those bytes belong to runWatchInput's next read.
func readLineFromStdin() string {
	var b []byte
	one := make([]byte, 1)
	for {
		n, err := os.Stdin.Read(one)
		if err != nil || n == 0 {
			break
		}
		if one[0] == '\n' || one[0] == '\r' {
			break
		}
		b = append(b, one[0])
	}
	return strings.TrimSpace(string(b))
}

// stdinByteChan returns a channel that yields one byte per stdin read, and
// exits cleanly when ctx is cancelled. The TTY's os.Stdin.Read is otherwise
// uncancellable; the self-pipe trick (poll on stdin + a pipe whose write-end
// closes on ctx-done) is the portable way to unblock it without closing
// stdin itself. No timer-based polling, no goroutine leak.
//
// On the rare error path where the pipe can't be created we fall back to a
// plain blocking read — the goroutine then leaks at process exit, same as
// before this commit. Logged so it's visible.
func stdinByteChan(ctx context.Context) <-chan byte {
	ch := make(chan byte)

	pr, pw, perr := os.Pipe()
	if perr != nil {
		log.Printf("stdinByteChan: pipe creation failed (%v); falling back to leaking blocking read", perr)
		go func() {
			buf := make([]byte, 1)
			for {
				n, err := os.Stdin.Read(buf)
				if err != nil || n == 0 {
					return
				}
				select {
				case ch <- buf[0]:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch
	}

	// Closer goroutine: when ctx is done, close pw so the poll on pr wakes up.
	go func() {
		<-ctx.Done()
		pw.Close()
	}()

	// Reader goroutine: poll on stdin + pipe, read stdin on POLLIN, exit on
	// ctx-pipe event or any error.
	go func() {
		defer close(ch)
		defer pr.Close()
		defer pw.Close()

		stdinFd := int32(os.Stdin.Fd())
		pipeFd := int32(pr.Fd())
		buf := make([]byte, 1)

		for {
			fds := []unix.PollFd{
				{Fd: stdinFd, Events: unix.POLLIN},
				{Fd: pipeFd, Events: unix.POLLIN},
			}
			_, err := unix.Poll(fds, -1)
			if err != nil {
				if err == unix.EINTR {
					select {
					case <-ctx.Done():
						return
					default:
						continue
					}
				}
				return
			}
			// ctx-done pipe became readable (closed-write-end) → exit.
			if fds[1].Revents != 0 {
				return
			}
			if fds[0].Revents&unix.POLLIN == 0 {
				// POLLHUP / POLLERR on stdin without POLLIN — nothing more to read.
				if fds[0].Revents&(unix.POLLHUP|unix.POLLERR|unix.POLLNVAL) != 0 {
					return
				}
				continue
			}
			n, err := os.Stdin.Read(buf)
			if err != nil || n == 0 {
				return
			}
			select {
			case ch <- buf[0]:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// runWatchInput dispatches single-byte keystrokes to commands. For 'l', 't',
// and '/', it temporarily restores cooked mode so cmdLog / cmdTUI / cmdGrep
// render normally and line-editing works.
//
// outMu is held for the full duration of l/t/'/' subcommands by design: the
// terminal-mode transition (cbreak → cooked → cbreak) and the subcommand's
// output must not interleave with debouncer-driven snapshot prints, or the
// stream gets garbled and the cooked-mode subcommand sees half-set termios.
//
// The dispatcher exits cleanly on ctx-done (signal or 'q'), so cmdWatch can
// wait for it before running deferred closes that would invalidate the
// closure-captured *store.Store and Creator.
func runWatchInput(
	ctx context.Context,
	fd int,
	oldTermios *unix.Termios,
	outMu *sync.Mutex,
	s *store.Store,
	root string,
	takeSnap func() *store.Snapshot,
	printSnap func(*store.Snapshot),
	cancel context.CancelFunc,
) {
	keys := stdinByteChan(ctx)
	for {
		var b byte
		select {
		case <-ctx.Done():
			return
		case k, ok := <-keys:
			if !ok {
				return
			}
			b = k
		}
		switch b {
		case 'q', 'Q':
			cancel()
			return
		case 'c', 'C':
			outMu.Lock()
			snap := takeSnap()
			printSnap(snap)
			// Pin the checkpoint target to snap.ID when we have one. Re-reading
			// HEAD here would race with the debouncer: it can take its own
			// snapshot and rewrite HEAD between our take and our read, leaving
			// us checkpointing a different snapshot than the one we printed.
			// Fall back to HEAD only when takeSnap returned nil (no changes).
			var (
				snapID   int64
				snapHash string
			)
			if snap != nil {
				snapID = snap.ID
				snapHash = snap.Hash
			} else {
				headID, herr := readHead(root, s)
				if herr != nil {
					fmt.Printf("checkpoint: %v\n", herr)
					outMu.Unlock()
					continue
				}
				if headID == nil {
					outMu.Unlock()
					continue
				}
				snapID = *headID
			}
			var name string
			var setErr error
			for i := 0; i < 20; i++ {
				var nameErr error
				name, nameErr = randomCheckpointName()
				if nameErr != nil {
					fmt.Printf("checkpoint name: %v\n", nameErr)
					break
				}
				setErr = s.SetCheckpoint(name, snapID)
				if setErr == nil {
					if snapHash == "" {
						fmt.Printf("Checkpoint %s\n", name)
					} else {
						fmt.Printf("Checkpoint %s -> %s\n", name, shortHash(snapHash))
					}
					break
				}
				if !strings.Contains(setErr.Error(), "already exists") {
					fmt.Printf("checkpoint: %v\n", setErr)
					break
				}
			}
			outMu.Unlock()
		case 's', 'S':
			outMu.Lock()
			snap := takeSnap()
			if snap == nil {
				fmt.Println("(no changes)")
			} else {
				printSnap(snap)
			}
			outMu.Unlock()
		case 'l', 'L':
			outMu.Lock()
			restoreTermios(fd, oldTermios)
			if err := cmdLog(nil); err != nil {
				fmt.Printf("log: %v\n", err)
			}
			if newOld, err := makeCBreak(fd); err == nil {
				oldTermios = newOld
			}
			outMu.Unlock()
		case 't', 'T':
			outMu.Lock()
			restoreTermios(fd, oldTermios)
			if err := cmdTUI(nil); err != nil {
				fmt.Printf("tui: %v\n", err)
			}
			if newOld, err := makeCBreak(fd); err == nil {
				oldTermios = newOld
			}
			outMu.Unlock()
		case '/':
			outMu.Lock()
			restoreTermios(fd, oldTermios)
			fmt.Print("grep> ")
			line := readLineFromStdin()
			if line != "" {
				if err := cmdGrep([]string{line}); err != nil {
					fmt.Printf("grep: %v\n", err)
				}
			}
			if newOld, err := makeCBreak(fd); err == nil {
				oldTermios = newOld
			}
			outMu.Unlock()
		case '?', 'h', 'H':
			outMu.Lock()
			fmt.Println("Keys: c=checkpoint  s=snapshot  l=log  t=tui  /=grep  ?=help  q=quit")
			outMu.Unlock()
		}
	}
}

func detachWatcher() error {
	root, err := findRoot()
	if err != nil {
		return err
	}

	// Refuse before spawning, so we don't print a bogus "started" line for a
	// child that would immediately exit on the same one-watcher-per-dir check.
	if pid, running := watcherAlreadyRunning(root); running {
		return fmt.Errorf("another earwig watcher is already running in %s (PID %d); only one watcher per directory is supported", root, pid)
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding executable path: %w", err)
	}

	logPath := filepath.Join(root, ".earwig", "watch.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}

	cmd := &exec.Cmd{
		Path:   exe,
		Args:   []string{exe, "watch"},
		Dir:    root,
		Stdout: logFile,
		Stderr: logFile,
		SysProcAttr: &syscall.SysProcAttr{
			Setsid: true,
		},
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("starting watcher: %w", err)
	}
	logFile.Close()

	pid := cmd.Process.Pid

	// Release the child process so the parent doesn't wait/zombie
	cmd.Process.Release()

	pidPath := filepath.Join(root, ".earwig", "PID")
	if err := os.WriteFile(pidPath, []byte(fmt.Sprintf("%d\n", pid)), 0644); err != nil {
		return fmt.Errorf("writing PID file: %w", err)
	}

	fmt.Printf("Watcher started (PID %d), logging to .earwig/watch.log\n", pid)
	return nil
}

func cmdRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	yes := fs.Bool("y", false, "skip confirmation prompt")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: earwig restore [-y] <hash>")
	}

	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	snap, err := s.ResolveRef(fs.Arg(0))
	if err != nil {
		return err
	}

	ig, err := loadIgnore(root)
	if err != nil {
		return err
	}

	cfg, err := loadConfig(root)
	if err != nil {
		return err
	}

	// Acquire exclusive flock — blocks until watcher snapshot finishes.
	flockFile, err := acquireFlock(root, true)
	if err != nil {
		return fmt.Errorf("acquiring lock: %w", err)
	}
	defer flockFile.Close()

	// Preview what the restore would do
	restorer := snapshot.NewRestorer(s, root, ig)
	plan, err := restorer.Preview(snap.ID)
	if err != nil {
		return fmt.Errorf("computing restore plan: %w", err)
	}

	if !plan.HasChanges() {
		fmt.Println("Already at target state. Nothing to do.")
		return nil
	}

	// Display the plan
	printPlan(plan, snap)

	// Confirm unless -y
	if !*yes {
		if !confirm("Proceed? [y/N]") {
			fmt.Println("Restore cancelled.")
			return nil
		}
	}

	// Auto-snapshot current state before restore so the user can undo
	parentID, err := readHead(root, s)
	if err != nil {
		return err
	}
	c := snapshot.NewCreator(s, root, ig, cfg)
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

func cmdDiff(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: earwig diff <hash>")
	}

	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	snap, err := s.ResolveRef(args[0])
	if err != nil {
		return err
	}

	ig, err := loadIgnore(root)
	if err != nil {
		return err
	}

	result, err := formatRestoreDiffStr(s, root, ig, snap)
	if err != nil {
		return err
	}
	fmt.Print(result)
	return nil
}

// GrepMatch represents a single line match from blob content search.
type GrepMatch struct {
	SnapshotHash string
	Path         string
	LineNum      int
	Line         string
}

// grepBlobs searches blob contents for a regex pattern.
// Returns matches grouped by snapshot (newest-first) then path.
// Each unique blob is fetched and searched at most once (dedup).
// maxSize limits the file size to search (0 = no limit).
func grepBlobs(s *store.Store, pattern *regexp.Regexp, snapshotIDs []int64, fileGlob string, maxSize int64) ([]GrepMatch, error) {
	refs, err := s.BlobRefs(snapshotIDs, maxSize)
	if err != nil {
		return nil, err
	}

	// Build snapshot ID -> hash lookup
	snapshots, err := s.ListSnapshots()
	if err != nil {
		return nil, err
	}
	snapHashByID := make(map[int64]string, len(snapshots))
	snapOrderByID := make(map[int64]int, len(snapshots))
	for i, snap := range snapshots {
		snapHashByID[snap.ID] = snap.Hash
		snapOrderByID[snap.ID] = i
	}

	var matches []GrepMatch

	for blobHash, blobRefs := range refs {
		// Filter by file glob if specified
		var filteredRefs []store.BlobRef
		for _, ref := range blobRefs {
			if fileGlob != "" {
				matched, _ := filepath.Match(fileGlob, filepath.Base(ref.Path))
				if !matched {
					// Also try matching the full path
					matched, _ = filepath.Match(fileGlob, ref.Path)
				}
				if !matched {
					continue
				}
			}
			filteredRefs = append(filteredRefs, ref)
		}
		if len(filteredRefs) == 0 {
			continue
		}

		data, err := s.GetBlob(blobHash)
		if err != nil {
			continue // skip unreadable blobs
		}
		if isBinaryContent(data) {
			continue
		}

		// Search line by line
		lines := strings.Split(string(data), "\n")
		var lineMatches []struct {
			num  int
			line string
		}
		for i, line := range lines {
			if pattern.MatchString(line) {
				lineMatches = append(lineMatches, struct {
					num  int
					line string
				}{i + 1, line})
			}
		}
		if len(lineMatches) == 0 {
			continue
		}

		// Emit matches for every (snapshot, path) that references this blob
		for _, ref := range filteredRefs {
			snapHash, ok := snapHashByID[ref.SnapshotID]
			if !ok {
				continue
			}
			for _, lm := range lineMatches {
				matches = append(matches, GrepMatch{
					SnapshotHash: snapHash,
					Path:         ref.Path,
					LineNum:      lm.num,
					Line:         lm.line,
				})
			}
		}
	}

	// Sort by snapshot order (newest first), then path, then line number
	sort.Slice(matches, func(i, j int) bool {
		oi := snapOrderByID[snapIDByHash(snapHashByID, matches[i].SnapshotHash)]
		oj := snapOrderByID[snapIDByHash(snapHashByID, matches[j].SnapshotHash)]
		if oi != oj {
			return oi > oj // newest first (higher index = newer)
		}
		if matches[i].Path != matches[j].Path {
			return matches[i].Path < matches[j].Path
		}
		return matches[i].LineNum < matches[j].LineNum
	})

	return matches, nil
}

func snapIDByHash(hashToID map[int64]string, hash string) int64 {
	for id, h := range hashToID {
		if h == hash {
			return id
		}
	}
	return 0
}

func cmdGrep(args []string) error {
	fs := flag.NewFlagSet("grep", flag.ContinueOnError)
	caseInsensitive := fs.Bool("i", false, "case-insensitive search")
	listOnly := fs.Bool("l", false, "list matching files only")
	limit := fs.Int("n", 0, "limit to N most recent snapshots")
	maxSizeMB := fs.Int("max-size", 10, "skip files larger than N MB (0 = no limit)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: earwig grep [-i] [-l] [-n count] [-max-size MB] <pattern> [file-glob]")
	}

	patternStr := fs.Arg(0)
	if *caseInsensitive {
		patternStr = "(?i)" + patternStr
	}
	re, err := regexp.Compile(patternStr)
	if err != nil {
		return fmt.Errorf("invalid pattern: %w", err)
	}

	fileGlob := ""
	if fs.NArg() >= 2 {
		fileGlob = fs.Arg(1)
	}

	s, _, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	// Determine which snapshots to search
	var snapshotIDs []int64
	if *limit > 0 {
		snapshots, err := s.ListSnapshots()
		if err != nil {
			return err
		}
		start := len(snapshots) - *limit
		if start < 0 {
			start = 0
		}
		for i := start; i < len(snapshots); i++ {
			snapshotIDs = append(snapshotIDs, snapshots[i].ID)
		}
	}

	var maxSize int64
	if *maxSizeMB > 0 {
		maxSize = int64(*maxSizeMB) * 1024 * 1024
	}

	matches, err := grepBlobs(s, re, snapshotIDs, fileGlob, maxSize)
	if err != nil {
		return err
	}

	if len(matches) == 0 {
		fmt.Println("No matches.")
		return nil
	}

	if *listOnly {
		// Deduplicate snapshot:path pairs
		seen := make(map[string]bool)
		for _, m := range matches {
			key := shortHash(m.SnapshotHash) + "  " + m.Path
			if !seen[key] {
				seen[key] = true
				fmt.Println(key)
			}
		}
		return nil
	}

	for _, m := range matches {
		fmt.Printf("%s  %s:%d:  %s\n", shortHash(m.SnapshotHash), m.Path, m.LineNum, m.Line)
	}
	return nil
}

// Checkpoint name validation: alphanumeric, hyphens, underscores, dots.
// Must not be pure hex (ambiguous with hashes).
var validCheckpointName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
var pureHex = regexp.MustCompile(`^[0-9a-f]+$`)

func validateCheckpointName(name string) error {
	if !validCheckpointName.MatchString(name) {
		return fmt.Errorf("invalid checkpoint name %q (use alphanumeric, hyphens, underscores, dots; max 64 chars)", name)
	}
	if pureHex.MatchString(name) {
		return fmt.Errorf("checkpoint name %q looks like a hash prefix (must contain non-hex characters)", name)
	}
	return nil
}

func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	del := fs.Bool("d", false, "delete a checkpoint")
	update := fs.Bool("u", false, "move an existing checkpoint")
	if err := fs.Parse(args); err != nil {
		return err
	}

	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	// Delete mode: earwig check -d <name>
	if *del {
		if fs.NArg() < 1 {
			return fmt.Errorf("usage: earwig check -d <name>")
		}
		name := fs.Arg(0)
		if err := s.DeleteCheckpoint(name); err != nil {
			return err
		}
		fmt.Printf("Deleted checkpoint %s\n", name)
		return nil
	}

	// Update mode: earwig check -u <name> [hash]
	if *update {
		if fs.NArg() < 1 {
			return fmt.Errorf("usage: earwig check -u <name> [hash]")
		}
		name := fs.Arg(0)
		var snap *store.Snapshot
		if fs.NArg() >= 2 {
			snap, err = s.ResolveRef(fs.Arg(1))
			if err != nil {
				return err
			}
		} else {
			snap, err = snapshotForCheck(s, root)
			if err != nil {
				return err
			}
		}
		if err := s.UpdateCheckpoint(name, snap.ID); err != nil {
			return err
		}
		fmt.Printf("Moved checkpoint %s -> %s\n", name, shortHash(snap.Hash))
		return nil
	}

	// Create mode
	switch fs.NArg() {
	case 0:
		// earwig check — random name, snapshot first
		snap, err := snapshotForCheck(s, root)
		if err != nil {
			return err
		}
		// Generate a unique random name
		var name string
		for i := 0; i < 20; i++ {
			name, err = randomCheckpointName()
			if err != nil {
				return err
			}
			err = s.SetCheckpoint(name, snap.ID)
			if err == nil {
				fmt.Printf("Checkpoint %s -> %s\n", name, shortHash(snap.Hash))
				return nil
			}
			// If it's a uniqueness error, retry; otherwise surface
			if !strings.Contains(err.Error(), "already exists") {
				return err
			}
		}
		return fmt.Errorf("could not generate a unique checkpoint name after 20 attempts")

	case 1:
		// earwig check <name> — named checkpoint, snapshot first
		name := fs.Arg(0)
		if err := validateCheckpointName(name); err != nil {
			return err
		}
		snap, err := snapshotForCheck(s, root)
		if err != nil {
			return err
		}
		if err := s.SetCheckpoint(name, snap.ID); err != nil {
			return err
		}
		fmt.Printf("Checkpoint %s -> %s\n", name, shortHash(snap.Hash))
		return nil

	case 2:
		// earwig check <name> <hash> — named checkpoint on specific snapshot
		name := fs.Arg(0)
		if err := validateCheckpointName(name); err != nil {
			return err
		}
		snap, err := s.ResolveRef(fs.Arg(1))
		if err != nil {
			return err
		}
		if err := s.SetCheckpoint(name, snap.ID); err != nil {
			return err
		}
		fmt.Printf("Checkpoint %s -> %s\n", name, shortHash(snap.Hash))
		return nil

	default:
		return fmt.Errorf("usage: earwig check [name] [hash]")
	}
}

// snapshotForCheck takes a snapshot of the current filesystem state.
// If there are changes, returns the new snapshot. If no changes, returns HEAD.
func snapshotForCheck(s *store.Store, root string) (*store.Snapshot, error) {
	ig, err := loadIgnore(root)
	if err != nil {
		return nil, err
	}
	cfg, err := loadConfig(root)
	if err != nil {
		return nil, err
	}
	parentID, err := readHead(root, s)
	if err != nil {
		return nil, err
	}
	c := snapshot.NewCreator(s, root, ig, cfg)
	snap, err := c.TakeSnapshot(parentID, "check")
	if err != nil {
		return nil, err
	}
	if snap != nil {
		if err := writeHead(root, snap.ID); err != nil {
			return nil, fmt.Errorf("writing HEAD: %w", err)
		}
		fmt.Printf("Snapshot %s\n", shortHash(snap.Hash))
		return snap, nil
	}
	// No changes — checkpoint HEAD
	if parentID == nil {
		return nil, fmt.Errorf("no snapshots yet")
	}
	return s.GetSnapshotByID(*parentID)
}

func cmdChecks(args []string) error {
	s, _, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	checkpoints, err := s.ListCheckpoints()
	if err != nil {
		return err
	}

	if len(checkpoints) == 0 {
		fmt.Println("No checkpoints.")
		return nil
	}

	for _, cp := range checkpoints {
		fmt.Printf("%-20s  %s  %s  %s\n",
			cp.Name,
			shortHash(cp.Snapshot.Hash),
			cp.Snapshot.CreatedAt.Format("2006-01-02 15:04:05"),
			cp.Snapshot.Message,
		)
	}
	return nil
}

// formatRestoreDiffStr returns the full diff of a snapshot vs current filesystem.
func formatRestoreDiffStr(s *store.Store, root string, ig *ignore.Matcher, snap *store.Snapshot) (string, error) {
	restorer := snapshot.NewRestorer(s, root, ig)
	plan, err := restorer.Preview(snap.ID)
	if err != nil {
		return "", err
	}

	if !plan.HasChanges() {
		return "No differences. Current state matches snapshot.\n", nil
	}

	// Build map of snapshot files for blob lookups
	targetFiles, err := s.GetSnapshotFiles(snap.ID)
	if err != nil {
		return "", err
	}
	targetMap := make(map[string]store.SnapshotFile, len(targetFiles))
	for _, f := range targetFiles {
		targetMap[f.Path] = f
	}

	var b strings.Builder
	b.WriteString(formatPlan(plan, snap))

	for _, path := range plan.Delete {
		old, oldLabel := readDiskContent(root, path)
		b.WriteString(formatUnifiedDiff(old, "", "a/"+path, "/dev/null", oldLabel, ""))
	}

	for _, path := range plan.Write {
		f, ok := targetMap[path]
		if !ok {
			continue
		}
		nw, newLabel := readBlobContent(s, f)
		b.WriteString(formatUnifiedDiff("", nw, "/dev/null", "b/"+path, "", newLabel))
	}

	for _, path := range plan.Modify {
		f, ok := targetMap[path]
		if !ok {
			continue
		}
		old, oldLabel := readDiskContent(root, path)
		nw, newLabel := readBlobContent(s, f)
		b.WriteString(formatUnifiedDiff(old, nw, "a/"+path, "b/"+path, oldLabel, newLabel))
	}

	return b.String(), nil
}

// formatParentDiffStr returns the full diff of a snapshot vs its parent.
func formatParentDiffStr(s *store.Store, snap *store.Snapshot) (string, error) {
	if snap.ParentID == nil {
		// Root snapshot: all files are added
		files, err := s.GetSnapshotFiles(snap.ID)
		if err != nil {
			return "", err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "Snapshot %s (root):\n\n", shortHash(snap.Hash))
		for _, f := range files {
			fmt.Fprintf(&b, "  A %s\n", f.Path)
		}
		b.WriteByte('\n')
		for _, f := range files {
			nw, newLabel := readBlobContent(s, f)
			b.WriteString(formatUnifiedDiff("", nw, "/dev/null", "b/"+f.Path, "", newLabel))
		}
		return b.String(), nil
	}

	changes, err := s.DiffSnapshots(*snap.ParentID, snap.ID)
	if err != nil {
		return "", err
	}

	if len(changes) == 0 {
		return "No changes vs parent.\n", nil
	}

	parentFiles, err := s.GetSnapshotFiles(*snap.ParentID)
	if err != nil {
		return "", err
	}
	parentMap := make(map[string]store.SnapshotFile, len(parentFiles))
	for _, f := range parentFiles {
		parentMap[f.Path] = f
	}

	snapFiles, err := s.GetSnapshotFiles(snap.ID)
	if err != nil {
		return "", err
	}
	snapMap := make(map[string]store.SnapshotFile, len(snapFiles))
	for _, f := range snapFiles {
		snapMap[f.Path] = f
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Changes in %s vs parent:\n\n", shortHash(snap.Hash))

	for _, c := range changes {
		switch c.Type {
		case store.ChangeAdded:
			fmt.Fprintf(&b, "  A %s\n", c.Path)
		case store.ChangeModified:
			fmt.Fprintf(&b, "  M %s\n", c.Path)
		case store.ChangeDeleted:
			fmt.Fprintf(&b, "  D %s\n", c.Path)
		}
	}
	b.WriteByte('\n')

	for _, c := range changes {
		switch c.Type {
		case store.ChangeDeleted:
			if f, ok := parentMap[c.Path]; ok {
				old, oldLabel := readBlobContent(s, f)
				b.WriteString(formatUnifiedDiff(old, "", "a/"+c.Path, "/dev/null", oldLabel, ""))
			}
		case store.ChangeAdded:
			if f, ok := snapMap[c.Path]; ok {
				nw, newLabel := readBlobContent(s, f)
				b.WriteString(formatUnifiedDiff("", nw, "/dev/null", "b/"+c.Path, "", newLabel))
			}
		case store.ChangeModified:
			oldF, okOld := parentMap[c.Path]
			newF, okNew := snapMap[c.Path]
			if okOld && okNew {
				old, oldLabel := readBlobContent(s, oldF)
				nw, newLabel := readBlobContent(s, newF)
				b.WriteString(formatUnifiedDiff(old, nw, "a/"+c.Path, "b/"+c.Path, oldLabel, newLabel))
			}
		}
	}

	return b.String(), nil
}

// readDiskContent reads the current content of a file on disk.
// Returns the content string and a label hint (e.g. "(binary)" or "(symlink)").
func readDiskContent(root, relPath string) (string, string) {
	absPath := filepath.Join(root, filepath.FromSlash(relPath))
	info, err := os.Lstat(absPath)
	if err != nil {
		return "", ""
	}
	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(absPath)
		if err != nil {
			return "", ""
		}
		return target + "\n", "(symlink)"
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return "", ""
	}
	if isBinaryContent(data) {
		return "", "(binary)"
	}
	return string(data), ""
}

// readBlobContent reads content from the blob store for a snapshot file.
func readBlobContent(s *store.Store, f store.SnapshotFile) (string, string) {
	data, err := s.GetBlob(f.BlobHash)
	if err != nil {
		return "", ""
	}
	if f.Type == "symlink" {
		return string(data) + "\n", "(symlink)"
	}
	if isBinaryContent(data) {
		return "", "(binary)"
	}
	return string(data), ""
}

// isBinaryContent returns true if data contains NUL bytes (indicating binary).
func isBinaryContent(data []byte) bool {
	// Check up to first 8KB
	limit := len(data)
	if limit > 8192 {
		limit = 8192
	}
	for i := 0; i < limit; i++ {
		if data[i] == 0 {
			return true
		}
	}
	return false
}

// formatUnifiedDiff returns a unified diff between old and new content as a string.
func formatUnifiedDiff(oldContent, newContent, oldName, newName, oldLabel, newLabel string) string {
	// Binary file detection
	if oldLabel == "(binary)" || newLabel == "(binary)" {
		name := strings.TrimPrefix(oldName, "a/")
		if name == "/dev/null" {
			name = strings.TrimPrefix(newName, "b/")
		}
		return fmt.Sprintf("Binary file %s differs\n", name)
	}

	// Symlink annotation
	if oldLabel == "(symlink)" {
		oldName += " " + oldLabel
	}
	if newLabel == "(symlink)" {
		newName += " " + newLabel
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(oldContent),
		B:        difflib.SplitLines(newContent),
		FromFile: oldName,
		ToFile:   newName,
		Context:  3,
	}
	text, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return fmt.Sprintf("warning: diff failed for %s: %v\n", oldName, err)
	}
	return text
}

func formatPlan(plan *snapshot.RestorePlan, snap *store.Snapshot) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Restore to %s (%s):\n\n", shortHash(snap.Hash), snap.CreatedAt.Format("2006-01-02 15:04:05"))

	if len(plan.Delete) > 0 {
		fmt.Fprintf(&b, "  Delete %d file(s):\n", len(plan.Delete))
		for _, p := range plan.Delete {
			fmt.Fprintf(&b, "    D %s\n", p)
		}
		b.WriteByte('\n')
	}

	if len(plan.Write) > 0 {
		fmt.Fprintf(&b, "  Write %d file(s):\n", len(plan.Write))
		for _, p := range plan.Write {
			fmt.Fprintf(&b, "    A %s\n", p)
		}
		b.WriteByte('\n')
	}

	if len(plan.Modify) > 0 {
		fmt.Fprintf(&b, "  Modify %d file(s):\n", len(plan.Modify))
		for _, p := range plan.Modify {
			fmt.Fprintf(&b, "    M %s\n", p)
		}
		b.WriteByte('\n')
	}

	if len(plan.Chmod) > 0 {
		fmt.Fprintf(&b, "  Chmod %d file(s):\n", len(plan.Chmod))
		for _, c := range plan.Chmod {
			fmt.Fprintf(&b, "    C %s (%04o → %04o)\n", c.Path, c.OldMode, c.NewMode)
		}
		b.WriteByte('\n')
	}

	if plan.Unchanged > 0 {
		fmt.Fprintf(&b, "  Unchanged: %d file(s)\n\n", plan.Unchanged)
	}

	return b.String()
}

func printPlan(plan *snapshot.RestorePlan, snap *store.Snapshot) {
	fmt.Print(formatPlan(plan, snap))
}

func confirm(prompt string) bool {
	fmt.Print(prompt + " ")
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(strings.ToLower(line)) == "y"
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

	snap, err := s.ResolveRef(args[0])
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

	// Check for checkpoints that will be cascade-deleted.
	cpNames, _ := s.GetCheckpointsForSnapshot(snap.ID)

	if err := s.DeleteSnapshot(snap.ID); err != nil {
		return err
	}
	fmt.Printf("Forgot snapshot %s\n", shortHash(snap.Hash))
	for _, name := range cpNames {
		fmt.Printf("Deleted checkpoint %s\n", name)
	}

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

// watcherProc is a live `earwig watch` process discovered via ps.
type watcherProc struct {
	PID   int
	Etime string
	Cwd   string // best-effort working directory; "" if unknown
}

// isEarwigWatchArgs reports whether a command line (argv fields) is an
// `earwig watch` invocation: argv[0]'s basename is "earwig" and some later
// argument is "watch". Tighter than a substring match, so an unrelated process
// that merely mentions "earwig" and "watch" (e.g. an editor on a notes file) is
// not mistaken for a watcher.
func isEarwigWatchArgs(argv []string) bool {
	if len(argv) == 0 || filepath.Base(argv[0]) != "earwig" {
		return false
	}
	for _, a := range argv[1:] {
		if a == "watch" {
			return true
		}
	}
	return false
}

// scanWatchers returns running `earwig watch` processes other than this one,
// via ps. Cwd is filled in best-effort (may be "").
func scanWatchers() ([]watcherProc, error) {
	out, err := exec.Command("ps", "-eo", "pid,etime,args").Output()
	if err != nil {
		return nil, fmt.Errorf("running ps: %w", err)
	}

	myPID := os.Getpid()
	var procs []watcherProc
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		fields := strings.Fields(strings.TrimSpace(scanner.Text()))
		if len(fields) < 3 {
			continue
		}
		var pid int
		if _, err := fmt.Sscanf(fields[0], "%d", &pid); err != nil {
			continue
		}
		if pid == myPID || !isEarwigWatchArgs(fields[2:]) {
			continue
		}
		procs = append(procs, watcherProc{PID: pid, Etime: fields[1], Cwd: processCwd(pid)})
	}
	return procs, nil
}

func cmdProcesses(args []string) error {
	procs, err := scanWatchers()
	if err != nil {
		return err
	}
	if len(procs) == 0 {
		fmt.Println("No earwig watchers running.")
		return nil
	}
	for _, p := range procs {
		dir := p.Cwd
		if dir == "" {
			dir = "(unknown directory)"
		}
		fmt.Printf("PID %-8d  %-14s  %s\n", p.PID, p.Etime, dir)
	}
	return nil
}

// watcherAlreadyRunning reports whether another live `earwig watch` process is
// already watching root. Each candidate's watched root is resolved from its cwd
// (which may be a subdirectory of root), so the check holds regardless of where
// each watcher was launched. The launcher of a `-detach` watcher (our parent) is
// excluded. Best-effort: a ps failure returns false rather than blocking start.
func watcherAlreadyRunning(root string) (int, bool) {
	procs, err := scanWatchers()
	if err != nil {
		return 0, false
	}
	root = resolveDir(root)
	ppid := os.Getppid()
	for _, p := range procs {
		if p.PID == ppid || p.Cwd == "" {
			continue
		}
		r, _, err := findRootFrom(p.Cwd)
		if err != nil {
			continue
		}
		if resolveDir(r) == root {
			return p.PID, true
		}
	}
	return 0, false
}

// resolveDir canonicalizes a directory path (symlinks resolved) for comparing
// watched roots. os.Getwd may yield /var/... while ps/lsof report the resolved
// /private/var/... on macOS, so both sides must be canonicalized or the compare
// misses. Falls back to Clean if the path can't be resolved.
func resolveDir(p string) string {
	if rp, err := filepath.EvalSymlinks(p); err == nil {
		return rp
	}
	return filepath.Clean(p)
}

func processCwd(pid int) string {
	if runtime.GOOS == "linux" {
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
		if err != nil {
			return ""
		}
		return target
	}
	// macOS: use lsof
	out, err := exec.Command("lsof", "-a", "-p", fmt.Sprintf("%d", pid), "-d", "cwd", "-Fn").Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "n") {
			return line[1:]
		}
	}
	return ""
}

func addToGitExclude(dir string) {
	gitDir := filepath.Join(dir, ".git")
	if info, err := os.Stat(gitDir); err != nil || !info.IsDir() {
		return
	}

	excludeFile := filepath.Join(gitDir, "info", "exclude")
	existing, err := os.ReadFile(excludeFile)
	if err != nil {
		return
	}

	for _, line := range strings.Split(string(existing), "\n") {
		if strings.TrimRight(line, "\r") == ".earwig" {
			return
		}
	}

	fmt.Printf("Add .earwig to %s? [Y/n] ", excludeFile)
	reader := bufio.NewReader(os.Stdin)
	answer, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "" && answer != "y" && answer != "yes" {
		return
	}

	f, err := os.OpenFile(excludeFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not open %s: %v\n", excludeFile, err)
		return
	}
	defer f.Close()

	if len(existing) > 0 && existing[len(existing)-1] != '\n' {
		if _, err := f.WriteString("\n"); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not write to %s: %v\n", excludeFile, err)
			return
		}
	}

	if _, err := f.WriteString(".earwig\n"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not write to %s: %v\n", excludeFile, err)
		return
	}

	fmt.Printf("Added .earwig to %s\n", excludeFile)
}
