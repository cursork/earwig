package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
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

func usage() {
	fmt.Fprintf(os.Stderr, "usage: earwig <command> [args]\n\nCommands:\n")
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(os.Stderr, "  %s\n", name)
	}
}

// findRoot walks up from cwd looking for .earwig/
func findRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".earwig")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("not an earwig directory (or any parent): .earwig not found")
		}
		dir = parent
	}
}

func openStore() (*store.Store, string, error) {
	root, err := findRoot()
	if err != nil {
		return nil, "", err
	}
	s, err := store.Open(filepath.Join(root, ".earwig", "earwig.db"))
	if err != nil {
		return nil, "", err
	}
	return s, root, nil
}

func loadIgnore(root string) (*ignore.Matcher, error) {
	var files []string
	for _, name := range []string{".earwigignore", ".gitignore"} {
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

	if err := os.MkdirAll(earwigDir, 0755); err != nil {
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

	writeHead(root, snap.ID)
	fmt.Printf("Snapshot %s\n", snap.Hash[:12])
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
			snap.Hash[:12],
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

	fmt.Printf("Snapshot %s\n", snap.Hash[:12])
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

// HEAD tracking

func readHead(root string, s *store.Store) (*int64, error) {
	headPath := filepath.Join(root, ".earwig", "HEAD")
	data, err := os.ReadFile(headPath)
	if err == nil {
		idStr := strings.TrimSpace(string(data))
		var id int64
		if _, err := fmt.Sscanf(idStr, "%d", &id); err == nil {
			return &id, nil
		}
	}
	// Fall back to latest snapshot
	latest, err := s.GetLatestSnapshot()
	if err != nil {
		return nil, err
	}
	if latest != nil {
		return &latest.ID, nil
	}
	return nil, nil
}

func writeHead(root string, id int64) {
	headPath := filepath.Join(root, ".earwig", "HEAD")
	os.WriteFile(headPath, []byte(fmt.Sprintf("%d", id)), 0644)
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

	takeSnap := func() {
		parentID, err := readHead(root, s)
		if err != nil {
			log.Printf("error reading HEAD: %v", err)
			return
		}

		c := snapshot.NewCreator(s, root, ig)
		snap, err := c.TakeSnapshot(parentID, "auto")
		if err != nil {
			log.Printf("error taking snapshot: %v", err)
			return
		}
		if snap == nil {
			return // No changes
		}

		writeHead(root, snap.ID)
		fmt.Printf("[%s] Snapshot %s\n", snap.CreatedAt.Format("15:04:05"), snap.Hash[:12])
	}

	// Initial snapshot
	takeSnap()

	debouncer := watcher.NewDebouncer(1 * time.Minute)
	defer debouncer.Stop()

	w, err := watcher.New(root, ig)
	if err != nil {
		return err
	}

	w.OnEvent = func() {
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

	restorer := snapshot.NewRestorer(s, root, ig)
	if err := restorer.Restore(snap.ID); err != nil {
		return err
	}

	writeHead(root, snap.ID)
	fmt.Printf("Restored to snapshot %s (%s)\n", snap.Hash[:12], snap.CreatedAt.Format("2006-01-02 15:04:05"))
	return nil
}
